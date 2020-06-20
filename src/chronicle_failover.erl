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

-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).

-record(state, {}).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

failover(Peer, HistoryId, RemainingPeers) ->
    gen_server:call(?SERVER(Peer), {failover, HistoryId, RemainingPeers}).

retry_failover(Peer, HistoryId) ->
    gen_server:call(?SERVER(Peer), {retry_failover, HistoryId}).

%% gen_server callbacks
init([]) ->
    {ok, #state{}}.

handle_call({failover, HistoryId, RemainingPeers}, _From, State) ->
    handle_failover(HistoryId, RemainingPeers, State);
handle_call({retry_failover, HistoryId}, _From, State) ->
    handle_retry_failover(HistoryId, State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast ~p.~nState:~n~p",
             [Cast, State]),
    {noreply, State}.

%% internal
handle_failover(HistoryId, RemainingPeers, State) ->
    {reply, prepare_branch(HistoryId, RemainingPeers), State}.

handle_retry_failover(HistoryId, State) ->
    case chronicle_agent:get_metadata() of
        {ok, Metadata} ->
            Self = ?PEER(),
            #metadata{pending_branch = Branch} = Metadata,
            case Branch of
                #branch{coordinator = Coordinator,
                        history_id = BranchId,
                        status = Status}
                  when Coordinator =:= Self
                       andalso Status =:= unknown
                       andalso BranchId =:= HistoryId ->
                    {reply, prepare_branch_on_followers(Metadata, Branch)};
                _ ->
                    %% TODO: consider making the error more specific to the
                    %% exact cause of failure
                    {reply, {error, {bad_failover, Branch}}, State}
            end;
        {error, _} = Error ->
            {reply, {failed_to_get_metadata, Error}, State}
    end.

prepare_branch(HistoryId, Peers) ->
    %% TODO: check that Self is in peers?
    Self = ?PEER(),

    Branch = #branch{history_id = HistoryId,
                     coordinator = Self,
                     peers = Peers,
                     status = unknown},

    %% Store a branch record locally first, so we can recover even if we crash
    %% somewhere in the middle.
    case store_branch(Self, Branch) of
        {ok, Metadata} ->
            prepare_branch_on_followers(Metadata, Branch);
        {error, _} = Error ->
            Error
    end.

prepare_branch_on_followers(SelfMetadata, #branch{coordinator = Self,
                                                  peers = Peers} = Branch) ->
    unknown = Branch#branch.status,

    Followers = Peers -- [Self],
    Result =
        case prepare_branch_on_peers(Followers, Branch) of
            {ok, PeerResults0} ->
                PeerResults = [{Self, SelfMetadata} | PeerResults0],
                check_prepare_branch_results(PeerResults);
            {error, _} = Error ->
                Error
        end,
    update_branch_status(Self, Branch, Result),
    Result.

store_branch(Peer, Branch) ->
    ?DEBUG("Sending store_branch to ~p. Branch:~n~p", [Peer, Branch]),
    chronicle_agent:store_branch(Peer, Branch).

prepare_branch_on_peers(Peers, Branch) ->
    CallFun = fun (Peer) -> store_branch(Peer, Branch) end,
    HandleFun =
        fun (Peer, Response, Acc) ->
                case Response of
                    {ok, Metadata} ->
                        {continue, [{Peer, Metadata} | Acc]};
                    {error, _} = Error ->
                        {stop, Error}
                end
        end,

    Result = parallel_mapfold(CallFun, HandleFun, [], Peers),
    case Result of
        {error, _} = Error ->
            ?WARNING("Failed to prepare branch: ~p.~nBranch:~n~p",
                     [Error, Branch]),
            Error;
        Results when is_list(Results) ->
            {ok, Results}
    end.

check_prepare_branch_results(Results) ->
    %% TODO: take pending branch into account
    Histories = chronicle_utils:groupby(
                  fun ({_Peer, Metadata}) ->
                          Metadata#metadata.history_id
                  end, Results),
    case maps:size(Histories) =:= 1 of
        true ->
            %% All peers are on the same history so we are good to go.
            ok;
        false ->
            %% Some peers are on different histories. So can't proceed.
            ConflictingPeerGroups =
                maps:map(
                  fun (_HistoryId, HistoryPeers) ->
                          [Peer || {Peer, _Metadata} <- HistoryPeers]
                  end, Histories),
            {error, {incompatible_histories,
                     maps:to_list(ConflictingPeerGroups)}}
    end.

update_branch_status(Self, Branch, Result) ->
    unknown = Branch#branch.status,
    case branch_status_from_result(Result) of
        unknown ->
            ok;
        NewStatus ->
            NewBranch = Branch#branch{status = NewStatus},
            {ok, _} = store_branch(Self, NewBranch)
    end.

branch_status_from_result(Result) ->
    case Result of
        ok ->
            ok;
        {error, {concurrent_branch, _} = Status} ->
            Status;
        {error, {incompatible_histories, _} = Status} ->
            Status;
        _ ->
            unknown
    end.
