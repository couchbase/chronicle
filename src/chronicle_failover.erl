%% @author Couchbase <info@couchbase.com>
%% @copyright 2020 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
-module(chronicle_failover).

-include("chronicle.hrl").

-behavior(gen_server).

-export([start_link/0]).
-export([failover/1, failover/2, try_cancel/2]).

-export([init/1, handle_call/3, handle_cast/2]).

-export_type([failover_result/0, try_cancel_result/0]).

-define(SERVER, ?SERVER_NAME(?MODULE)).

-define(STORE_BRANCH_TIMEOUT, 15000).
-define(WAIT_LEADER_TIMEOUT, 10000).
-define(CANCEL_BRANCH_TIMEOUT, 15000).
-define(CLEANUP_BRANCH_TIMEOUT, 5000).

-record(state, {}).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

-type failover_result() :: ok | {error, failover_error()}.
-type failover_error() :: {not_in_peers, chronicle:peer(), [chronicle:peer()]}
                        | {aborted, #{diverged_peers => [chronicle:peer()],
                                      failed_peers => [chronicle:peer()]}}
                        | no_leader.

-spec failover([chronicle:peer()]) -> failover_result().
failover(KeepPeers) ->
    failover(KeepPeers, undefined).

-spec failover([chronicle:peer()], Opaque::any()) -> failover_result().
failover(KeepPeers, Opaque) ->
    case gen_server:call(?SERVER, {failover, KeepPeers, Opaque}, infinity) of
        {ok, HistoryId} ->
            %% Make sure chronicle_leader is aware of the branch stored locally.
            ok = chronicle_leader:sync(),

            %% Wait for leader to get elected. This doesn't guarantee that
            %% everything went smoothly. But it's a close approximation.
            try chronicle_leader:wait_for_leader(?WAIT_LEADER_TIMEOUT) of
                {_Leader, {LeaderHistoryId, _}}
                  when LeaderHistoryId =:= HistoryId ->
                    ok;
                _ ->
                    {error, no_leader}
            catch
                exit:no_leader ->
                    {error, no_leader}
            end;
        {error, _} = Error ->
            Error
    end.

-type try_cancel_result() :: ok
                           | {error, {failed_peers, [chronicle:peer()]}}.

-spec try_cancel(chronicle:history_id(), [chronicle:peer()]) ->
          ok | {error, {failed_peers, [chronicle:peer()]}}.
try_cancel(BranchId, Peers) ->
    gen_server:call(?SERVER, {try_cancel, BranchId, Peers}, infinity).

%% gen_server callbacks
init([]) ->
    {ok, #state{}}.

handle_call({failover, KeepPeers, Opaque}, _From, State) ->
    handle_failover(KeepPeers, Opaque, State);
handle_call({try_cancel, BranchId, Peers}, _From, State) ->
    handle_try_cancel(BranchId, Peers, State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast ~p.~nState:~n~p",
             [Cast, State]),
    {noreply, State}.

%% internal
handle_failover(KeepPeers, Opaque, State) ->
    Metadata = chronicle_agent:get_metadata(),
    NewHistoryId = chronicle_utils:random_uuid(),
    Reply = prepare_branch(KeepPeers, Opaque, NewHistoryId, Metadata),
    {reply, Reply, State}.

handle_try_cancel(BranchId, Peers, State) ->
    {reply, cancel_branch(BranchId, Peers), State}.

prepare_branch(KeepPeers, Opaque, NewHistoryId, Metadata) ->
    #metadata{peer = Self, history_id = OldHistoryId} = Metadata,
    case lists:member(Self, KeepPeers) of
        true ->
            Branch = #branch{history_id = NewHistoryId,
                             old_history_id = OldHistoryId,
                             coordinator = Self,
                             peers = KeepPeers,
                             opaque = Opaque},
            Followers = KeepPeers -- [Self],

            case store_branch(Followers, Branch) of
                ok ->
                    case local_store_branch(Branch) of
                        ok ->
                            {ok, NewHistoryId};
                        {error, Error} ->
                            ?WARNING("Failed to store branch locallly.~n"
                                     "Branch:~n~p~n"
                                     "Error: ~p",
                                     [Branch, Error]),

                            %% All errors are clean errors currently. So we
                            %% make an attempt to undo the branch on the
                            %% followers.
                            cleanup_branch(Branch, Followers),
                            {error, {aborted, #{failed_peers => [Self]}}}
                    end;
                {error, _} = Error ->
                    %% Attempt to undo the branch.
                    cleanup_branch(Branch, Followers),
                    Error
            end;
        false ->
            {error, {not_in_peers, Self, KeepPeers}}
    end.

local_store_branch(Branch) ->
    ?DEBUG("Setting local brach:~n~p", [Branch]),
    chronicle_agent:local_store_branch(Branch, ?STORE_BRANCH_TIMEOUT).

store_branch(Peers, Branch) ->
    ?DEBUG("Setting branch.~n"
           "Peers: ~w~n"
           "Branch:~n~p",
           [Peers, Branch]),

    {_Ok, Errors} =
        chronicle_agent:store_branch(Peers, Branch, ?STORE_BRANCH_TIMEOUT),
    case maps:size(Errors) =:= 0 of
        true ->
            ok;
        false ->
            ?WARNING("Failed to store branch on some peers.~n"
                     "Branch:~n~p~n"
                     "Errors:~n~p",
                     [Branch, Errors]),
            {error, {aborted, massage_errors(Errors)}}
    end.

massage_errors(Errors) ->
    chronicle_utils:groupby_map(
      fun ({Peer, Error}) ->
              case Error of
                  {error, {history_mismatch, _}} ->
                      {diverged_peers, Peer};
                  _ ->
                      {failed_peers, Peer}
              end
      end, maps:to_list(Errors)).

cancel_branch(BranchId, Peers) ->
    undo_branch(BranchId, Peers, ?CANCEL_BRANCH_TIMEOUT).

cleanup_branch(#branch{history_id = BranchId}, Peers) ->
    _ = undo_branch(BranchId, Peers, ?CLEANUP_BRANCH_TIMEOUT),
    ok.

undo_branch(BranchId, Peers, Timeout) ->
    ?DEBUG("Undoing branch.~n"
           "Branch id: ~w~n"
           "Peers: ~w",
           [BranchId, Peers]),

    {_Ok, Bad} = chronicle_agent:undo_branch(Peers, BranchId, Timeout),
    Errors = maps:filter(
               fun (_, Error) ->
                       case Error of
                           {error, no_branch} ->
                               %% No branch found.
                               false;
                           {error, {bad_branch, _}} ->
                               %% Branch superseded by another one.
                               false;
                           _ ->
                               true
                       end
               end, Bad),

    case maps:size(Errors) =:= 0 of
        true ->
            ?DEBUG("Branch undone successfully."),
            ok;
        false ->
            ?WARNING("Failed to undo branch on some nodes:~n~p", [Errors]),
            {error, {failed_peers, [maps:keys(Errors)]}}
    end.
