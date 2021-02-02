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

-compile(export_all).
-behavior(gen_server).

-include("chronicle.hrl").

-define(SERVER, ?SERVER_NAME(?MODULE)).

-record(state, {}).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

failover(RemainingPeers) ->
    failover(RemainingPeers, undefined).

failover(RemainingPeers, Opaque) ->
    gen_server:call(?SERVER, {failover, RemainingPeers, Opaque}).

%% gen_server callbacks
init([]) ->
    {ok, #state{}}.

handle_call({failover, RemainingPeers, Opaque}, _From, State) ->
    handle_failover(RemainingPeers, Opaque, State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast ~p.~nState:~n~p",
             [Cast, State]),
    {noreply, State}.

%% internal
handle_failover(RemainingPeers, Opaque, State) ->
    #metadata{history_id = HistoryId} = chronicle_agent:get_metadata(),
    NewHistoryId = chronicle_utils:random_uuid(),
    Reply = prepare_branch(HistoryId, NewHistoryId, RemainingPeers, Opaque),
    {reply, Reply, State}.

prepare_branch(OldHistoryId, NewHistoryId, Peers, Opaque) ->
    Branch = #branch{history_id = NewHistoryId,
                     old_history_id = OldHistoryId,
                     coordinator = ?PEER(),
                     peers = Peers,
                     status = unknown,
                     opaque = Opaque},

    %% Store a branch record locally first, so we can recover even if we crash
    %% somewhere in the middle.
    case local_store_branch(Branch) of
        {ok, _Metadata} ->
            prepare_branch_on_followers(Branch);
        {error, _} = Error ->
            Error
    end.

prepare_branch_on_followers(#branch{coordinator = Self,
                                    peers = Peers} = Branch) ->
    unknown = Branch#branch.status,

    Followers = Peers -- [Self],
    Result = store_branch(Followers, Branch),
    update_branch_status(Branch, Result),
    Result.

local_store_branch(Branch) ->
    ?DEBUG("Setting local brach:~n~p", [Branch]),
    chronicle_agent:local_store_branch(Branch).

store_branch(Peers, Branch) ->
    ?DEBUG("Setting branch.~n"
           "Peers: ~w~n"
           "Branch:~n~p",
           [Peers, Branch]),

    {_Ok, Bad} = chronicle_agent:store_branch(Peers, Branch),
    case maps:size(Bad) =:= 0 of
        true ->
            ok;
        false ->
            Errors = maps:to_list(Bad),
            ?WARNING("Failed to store branch on some peers.~n"
                     "Branch:~n~p~n"
                     "Errors:~n~p",
                     [Branch, Errors]),
            {error, Errors}
    end.

update_branch_status(Branch, Result) ->
    unknown = Branch#branch.status,
    case branch_status_from_result(Result) of
        unknown ->
            ok;
        NewStatus ->
            NewBranch = Branch#branch{status = NewStatus},
            {ok, _} = local_store_branch(NewBranch),
            ok
    end.

branch_status_from_result(Result) ->
    case Result of
        ok ->
            ok;
        _ ->
            failed
    end.
