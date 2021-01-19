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

-import(chronicle_utils, [parallel_mapfold/4]).

-define(SERVER, ?SERVER_NAME(?MODULE)).

-record(state, {}).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

failover(RemainingPeers) ->
    gen_server:call(?SERVER, {failover, RemainingPeers}).

%% gen_server callbacks
init([]) ->
    {ok, #state{}}.

handle_call({failover, RemainingPeers}, _From, State) ->
    handle_failover(RemainingPeers, State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast ~p.~nState:~n~p",
             [Cast, State]),
    {noreply, State}.

%% internal
handle_failover(RemainingPeers, State) ->
    #metadata{history_id = HistoryId} = chronicle_agent:get_metadata(),
    NewHistoryId = chronicle_utils:random_uuid(),
    {reply, prepare_branch(HistoryId, NewHistoryId, RemainingPeers), State}.

prepare_branch(OldHistoryId, NewHistoryId, Peers) ->
    Branch = #branch{history_id = NewHistoryId,
                     old_history_id = OldHistoryId,
                     coordinator = self,
                     peers = Peers,
                     status = unknown},

    %% Store a branch record locally first, so we can recover even if we crash
    %% somewhere in the middle.
    case store_branch(Branch) of
        {ok, Metadata} ->
            FinalBranch = Metadata#metadata.pending_branch,
            prepare_branch_on_followers(FinalBranch);
        {error, _} = Error ->
            Error
    end.

prepare_branch_on_followers(#branch{coordinator = Self,
                                    peers = Peers} = Branch) ->
    unknown = Branch#branch.status,

    Followers = Peers -- [Self],
    Result = prepare_branch_on_peers(Followers, Branch),
    update_branch_status(Branch, Result),
    Result.

store_branch(Branch) ->
    store_branch(?SELF_PEER, Branch).

store_branch(Peer, Branch) ->
    ?DEBUG("Sending store_branch to ~p.~n"
           "Branch:~n~p",
           [Peer, Branch]),
    chronicle_agent:store_branch(Peer, Branch).

prepare_branch_on_peers(Peers, Branch) ->
    CallFun = fun (Peer) -> store_branch(Peer, Branch) end,
    HandleFun =
        fun (_Peer, Response, _Acc) ->
                case Response of
                    {ok, _Metadata} ->
                        {continue, ok};
                    {error, _} = Error ->
                        {stop, Error}
                end
        end,

    Result = parallel_mapfold(CallFun, HandleFun, ok, Peers),
    case Result of
        ok ->
            ok;
        {error, _} = Error ->
            ?WARNING("Failed to prepare branch: ~p.~nBranch:~n~p",
                     [Error, Branch]),
            Error
    end.

update_branch_status(Branch, Result) ->
    unknown = Branch#branch.status,
    case branch_status_from_result(Result) of
        unknown ->
            ok;
        NewStatus ->
            NewBranch = Branch#branch{status = NewStatus},
            {ok, _} = store_branch(NewBranch),
            ok
    end.

branch_status_from_result(Result) ->
    case Result of
        ok ->
            ok;
        {error, {concurrent_branch, _} = Status} ->
            Status;
        {error, {history_mismatch, _} = Status} ->
            Status;
        _ ->
            unknown
    end.
