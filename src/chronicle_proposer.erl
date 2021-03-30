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
-module(chronicle_proposer).

-behavior(gen_statem).
-compile(export_all).

-include("chronicle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(chronicle_utils, [get_all_peers/1,
                          get_establish_quorum/1,
                          get_position/1,
                          get_quorum_peers/1,
                          have_quorum/2,
                          is_quorum_feasible/3,
                          log_entry_revision/1,
                          term_number/1,
                          sanitize_reason/1]).

-define(SERVER, ?SERVER_NAME(?MODULE)).

%% TODO: move these to the config
-define(STOP_TIMEOUT,
        chronicle_settings:get({proposer, stop_timeout}, 10000)).
-define(ESTABLISH_TERM_TIMEOUT,
        chronicle_settings:get({proposer, establish_term_timeout}, 10000)).
-define(NO_QUORUM_TIMEOUT,
        chronicle_settings:get({proposer, no_quorum_timeout}, 15000)).
-define(CHECK_PEERS_INTERVAL,
        chronicle_settings:get({proposer, check_peers_interval}, 1000)).
-define(MAX_INFLIGHT,
        chronicle_settings:get({proposer, max_inflight}, 500)).

-record(data, { parent,

                peer,

                %% TODO: reconsider what's needed and what's not needed here
                history_id,
                term,
                quorum,
                peers,
                quorum_peers,
                machines,
                config,

                high_seqno,
                committed_seqno,

                %% Committed seqno known to the agent on this node.
                local_committed_seqno,

                %% Don't consider seqno-s that are lesser than this seqno
                %% committed even if we replicated the corresponding entries
                %% to a quorum of nodes. Initialized to the seqno at which the
                %% first entry is proposed in the current term.
                safe_commit_seqno,

                being_removed,

                peer_statuses,
                monitors_peers,
                monitors_refs,

                %% Used only when the state is 'establish_term'.
                votes,
                failed_votes,
                branch,
                position,

                %% Used when the state is 'proposing'.
                pending_entries,
                catchup_pid,

                sync_round,
                acked_sync_round,
                sync_requests,

                config_change_reply_to,
                postponed_config_requests }).

-record(peer_status, {
                      %% The following fields are only valid when peer's state
                      %% is active.
                      acked_seqno,
                      acked_commit_seqno,
                      sent_seqno,
                      sent_commit_seqno,

                      %% These fields are valid irrespective of the state.
                      sent_sync_round,
                      acked_sync_round,

                      state :: active | catchup | status_requested }).

-record(sync_request, { round,
                        reply_to,
                        ok_reply }).

start_link(HistoryId, Term) ->
    Self = self(),
    gen_statem:start_link(?START_NAME(?MODULE),
                          ?MODULE, [Self, HistoryId, Term], []).

stop(Pid) ->
    gen_statem:call(Pid, stop, ?STOP_TIMEOUT).

sync_quorum(Pid, ReplyTo) ->
    gen_statem:cast(Pid, {sync_quorum, ReplyTo}).

query(Pid, ReplyTo, Query) ->
    gen_statem:cast(Pid, {query, ReplyTo, Query}).

cas_config(Pid, ReplyTo, NewConfig, Revision) ->
    gen_statem:cast(Pid, {cas_config, ReplyTo, NewConfig, Revision}).

append_commands(Pid, Commands) ->
    gen_statem:cast(Pid, {append_commands, Commands}).

%% gen_statem callbacks
callback_mode() ->
    [handle_event_function, state_enter].

format_status(Opt, [_PDict, State, Data]) ->
    case Opt of
        normal ->
            [{data, [{"State", {State, Data}}]}];
        terminate ->
            {State,
             case Data of
                 #data{} ->
                     sanitize_data(Data);
                 _ ->
                     %% During gen_statem initialization Data may be undefined.
                     Data
             end}
    end.

sanitize_event(cast, {append_commands, _}) ->
    {cast, {append_commands, '...'}};
sanitize_event(Type, Event) ->
    {Type, Event}.

init([Parent, HistoryId, Term]) ->
    chronicle_peers:monitor(),

    PeerStatuses = ets:new(peer_statuses, [protected, set]),
    Data = #data{ parent = Parent,
                  history_id = HistoryId,
                  term = Term,
                  peer_statuses = PeerStatuses,
                  monitors_peers = #{},
                  monitors_refs = #{},

                  votes = [],
                  failed_votes = [],
                  pending_entries = queue:new(),

                  sync_requests = queue:new(),
                  sync_round = 0,
                  acked_sync_round = 0,

                  postponed_config_requests = []},

    {ok, establish_term, Data}.

handle_event(enter, _OldState, NewState, Data) ->
    handle_state_enter(NewState, Data);
handle_event(state_timeout, establish_term_timeout, State, Data) ->
    handle_establish_term_timeout(State, Data);
handle_event(info, check_peers, State, Data) ->
    case State of
        proposing ->
            {keep_state, check_peers(Data)};
        {stopped, _} ->
            keep_state_and_data
    end;
handle_event(info, {check_quorum, CommittedSeqno, SyncRound}, State, Data) ->
    handle_check_quorum(CommittedSeqno, SyncRound, State, Data);
handle_event(info, {{agent_response, Ref, Peer, Request}, Result}, State,
             #data{peers = Peers} = Data) ->
    case lists:member(Peer, Peers) of
        true ->
            case get_peer_monitor(Peer, Data) of
                {ok, OurRef} when OurRef =:= Ref ->
                    handle_agent_response(Peer, Request, Result, State, Data);
                _ ->
                    ?DEBUG("Ignoring a stale response "
                           "from peer ~w. Request: ~w",
                           [Peer, Request]),
                    keep_state_and_data
            end;
        false ->
            ?INFO("Ignoring a response from a removed peer ~w. Request: ~w~n"
                  "Peers: ~w",
                  [Peer, Request, Peers]),
            keep_state_and_data
    end;
handle_event(info, {nodeup, Peer, Info}, State, Data) ->
    handle_nodeup(Peer, Info, State, Data);
handle_event(info, {nodedown, Peer, Info}, State, Data) ->
    handle_nodedown(Peer, Info, State, Data);
handle_event(info, {'DOWN', MRef, process, Pid, Reason}, State, Data) ->
    handle_down(MRef, Pid, Reason, State, Data);
handle_event(cast, {sync_quorum, ReplyTo}, State, Data) ->
    handle_sync_quorum(ReplyTo, State, Data);
handle_event(cast, {query, ReplyTo, Query} = Request, proposing, Data) ->
    maybe_postpone_config_request(
      Request, Data,
      fun () ->
              handle_query(ReplyTo, Query, Data)
      end);
handle_event(cast, {query, ReplyTo, _}, {stopped, _}, _Data) ->
    reply_not_leader(ReplyTo),
    keep_state_and_data;
handle_event(cast,
             {cas_config, ReplyTo, NewConfig, Revision} = Request,
             proposing, Data) ->
    maybe_postpone_config_request(
      Request, Data,
      fun () ->
              handle_cas_config(ReplyTo, NewConfig, Revision, Data)
      end);
handle_event(cast, {cas_config, ReplyTo, _, _}, {stopped, _}, _Data) ->
    reply_not_leader(ReplyTo),
    keep_state_and_data;
handle_event(cast, {append_commands, Commands}, State, Data) ->
    handle_append_commands(Commands, State, Data);
handle_event({call, From}, stop, State, Data) ->
    handle_stop(From, State, Data);
handle_event({call, From}, _Call, _State, _Data) ->
    {keep_state_and_data, [{reply, From, nack}]};
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event ~p", [{Type, Event}]),
    keep_state_and_data.

%% internal
handle_state_enter(establish_term,
                   #data{history_id = HistoryId, term = Term} = Data) ->
    %% Establish term locally first. This ensures that the metadata we're
    %% going to be using won't change (unless another node starts a higher
    %% term) between when we get it here and when we get a majority of votes.
    case chronicle_agent:establish_local_term(HistoryId, Term) of
        {ok, #metadata{peer = Self} = Metadata} ->
            Quorum = get_establish_quorum(Metadata),
            Peers = get_quorum_peers(Quorum),
            case lists:member(Self, get_quorum_peers(Quorum)) of
                true ->
                    NewData = Data#data{peer = Self},
                    establish_term_init(Metadata, NewData);
                false ->
                    ?INFO("Refusing to start a term ~w in history id ~p. "
                          "We're not a voting member anymore.~n"
                          "Peers: ~w",
                          [Term, HistoryId, Peers]),
                    {stop, {not_voter, Peers}}
            end;
        {error, Error} ->
            ?DEBUG("Error trying to establish local term. Stepping down.~n"
                   "History id: ~w~n"
                   "Term: ~w~n"
                   "Error: ~w",
                   [HistoryId, Term, Error]),
            {stop, {local_establish_term_failed, HistoryId, Term, Error}}
    end;
handle_state_enter(proposing, Data) ->
    NewData0 = start_catchup_process(Data),
    NewData1 = maybe_resolve_branch(NewData0),
    NewData2 = maybe_complete_config_transition(NewData1),
    NewData = propose_noop(NewData2),

    announce_proposer_ready(NewData),

    {keep_state, schedule_check_quorum(check_peers(NewData))};
handle_state_enter({stopped, _}, _Data) ->
    keep_state_and_data.

start_catchup_process(#data{peer = Self,
                            history_id = HistoryId, term = Term} = Data) ->
    case chronicle_catchup:start_link(Self, HistoryId, Term) of
        {ok, Pid} ->
            Data#data{catchup_pid = Pid};
        {error, Error} ->
            exit({failed_to_start_catchup_process, Error})
    end.

stop_catchup_process(#data{catchup_pid = Pid} = Data) ->
    case Pid of
        undefined ->
            Data;
        _ ->
            chronicle_catchup:stop(Pid),
            Data#data{catchup_pid = undefined}
    end.

preload_pending_entries(#data{history_id = HistoryId,
                              term = Term,
                              high_seqno = HighSeqno,
                              local_committed_seqno = LocalCommittedSeqno}
                        = Data) ->
    case HighSeqno > LocalCommittedSeqno of
        true ->
            case chronicle_agent:get_log(HistoryId, Term,
                                         LocalCommittedSeqno + 1, HighSeqno) of
                {ok, Entries} ->
                    Data#data{pending_entries = queue:from_list(Entries)};
                {error, Error} ->
                    ?WARNING("Encountered an error while fetching "
                             "uncommitted entries from local agent.~n"
                             "History id: ~p~n"
                             "Term: ~w~n"
                             "Committed seqno: ~w~n"
                             "High seqno: ~w~n"
                             "Error: ~w",
                             [HistoryId, Term,
                              LocalCommittedSeqno, HighSeqno, Error]),
                    exit({preload_pending_entries_failed, Error})
            end;
        false ->
            Data
    end.

propose_noop(Data) ->
    %% If everything from previous term is committed, there's no need to
    %% propose a noop. But for the sake of simplicity we'll propose it all the
    %% time whena term is established.
    {NoopEntry, NewData} = propose_value(noop, Data),
    NewData#data{safe_commit_seqno = NoopEntry#log_entry.seqno}.

announce_proposer_ready(#data{parent = Parent,
                              history_id = HistoryId,
                              term = Term,
                              high_seqno = HighSeqno}) ->
    chronicle_server:proposer_ready(Parent, HistoryId, Term, HighSeqno).

establish_term_init(Metadata,
                    #data{peer = Self,
                          history_id = HistoryId, term = Term} = Data) ->
    Quorum = require_self_quorum(get_establish_quorum(Metadata), Data),
    QuorumPeers = get_quorum_peers(Quorum),
    AllPeers = require_self_peer(get_all_peers(Metadata), Data),

    ?DEBUG("Going to establish term ~w (history id ~p).~n"
           "Quorum peers: ~w~n"
           "Metadata:~n~p",
           [Term, HistoryId, QuorumPeers, Metadata]),

    #metadata{config = ConfigEntry,
              high_seqno = HighSeqno,
              committed_seqno = CommittedSeqno,
              pending_branch = PendingBranch} = Metadata,

    OtherQuorumPeers = QuorumPeers -- [Self],

    %% Send a fake response to update our state with the knowledge that we've
    %% established the term locally. Initally, I wasn't planning to use such
    %% somewhat questionable approach and instead would update the state
    %% here. But if our local peer is the only peer, then we need to
    %% transition to propsing state immediately. But brain-dead gen_statem
    %% won't let you transition to a different state from a state_enter
    %% callback. So here we are.
    Position = get_position(Metadata),
    NewData0 = send_local_establish_term(Metadata, Data),
    {NewData1, BusyPeers} = send_establish_term(OtherQuorumPeers,
                                                Position, NewData0),
    log_busy_peers(establish_term, BusyPeers),

    case is_quorum_feasible(QuorumPeers, BusyPeers, Quorum) of
        true ->
            NewData = NewData1#data{peer = Self,
                                    peers = AllPeers,
                                    quorum_peers = QuorumPeers,
                                    quorum = Quorum,
                                    machines = get_machines(ConfigEntry),
                                    votes = [],
                                    failed_votes = BusyPeers,
                                    position = Position,
                                    config = ConfigEntry,
                                    high_seqno = HighSeqno,
                                    committed_seqno = CommittedSeqno,
                                    local_committed_seqno = CommittedSeqno,
                                    branch = PendingBranch,
                                    being_removed = false},
            {keep_state,
             preload_pending_entries(NewData),
             {state_timeout,
              ?ESTABLISH_TERM_TIMEOUT, establish_term_timeout}};
        false ->
            %% This should be a rare situation. That's because to be
            %% elected a leader we need to get a quorum of votes. So
            %% at least a quorum of nodes should be alive.
            ?WARNING("Can't establish term ~w, history id ~p. "
                     "Too many busy peers.~n"
                     "Quorum peers: ~w~n"
                     "Busy peers: ~w~n"
                     "Quorum: ~w",
                     [Term, HistoryId, QuorumPeers, BusyPeers, Quorum]),
            {stop, {error, no_quorum}}
    end.

handle_establish_term_timeout(establish_term = _State, #data{term = Term}) ->
    ?ERROR("Failed to establish term ~w after ~bms",
           [Term, ?ESTABLISH_TERM_TIMEOUT]),
    {stop, establish_term_timeout}.

check_peers(#data{peers = Peers,
                  sync_round = SyncRound,
                  acked_sync_round = AckedSyncRound} = Data) ->
    erlang:send_after(?CHECK_PEERS_INTERVAL, self(), check_peers),

    PeersToCheck =
        lists:filter(
          fun (Peer) ->
                  case get_peer_status(Peer, Data) of
                      {ok, #peer_status{sent_sync_round = PeerSentSyncRound}} ->
                          %% If we failed to send a heartbeat to the node due
                          %% to it being busy AND we haven't got enough
                          %% responses from other nodes, then we'll try
                          %% sending another heartbeat here.
                          PeerSentSyncRound < SyncRound
                              andalso SyncRound =/= AckedSyncRound;
                      not_found ->
                          true
                  end
          end, Peers),

    NewData = send_heartbeat(PeersToCheck, Data),

    %% Some peers may be behind because we didn't replicate to them due to
    %% chronicle_agent:append() returning 'nosuspend'.
    replicate(NewData).

handle_check_quorum(OldCommittedSeqno, OldSyncRound, State,
                    #data{acked_sync_round = SyncRound,
                          committed_seqno = CommittedSeqno} = Data) ->
    case State of
        proposing ->
            case SyncRound =:= OldSyncRound
                andalso CommittedSeqno =:= OldCommittedSeqno of
                true ->
                    stop(no_quorum, State, Data);
                false ->
                    {keep_state, schedule_check_quorum(Data)}
            end;
        {stopped, _} ->
            keep_state_and_data
    end.

schedule_check_quorum(#data{sync_round = SyncRound,
                            acked_sync_round = AckedSyncRound,
                            committed_seqno = CommittedSeqno,
                            high_seqno = HighSeqno} = Data) ->
    NewData =
        case SyncRound =/= AckedSyncRound orelse CommittedSeqno =/= HighSeqno of
            true ->
                %% There are some outstanding requests, so just piggy-back on
                %% them.
                Data;
            false ->
                send_heartbeat(Data#data{sync_round = SyncRound + 1})
        end,

    erlang:send_after(?NO_QUORUM_TIMEOUT, self(),
                      {check_quorum, CommittedSeqno, AckedSyncRound}),
    NewData.

handle_agent_response(Peer,
                      establish_term,
                      Result, State, Data) ->
    handle_establish_term_result(Peer, Result, State, Data);
handle_agent_response(Peer,
                      {append, _, _} = Request,
                      Result, State, Data) ->
    handle_append_result(Peer, Request, Result, State, Data);
handle_agent_response(Peer, {heartbeat, Round}, Result, State, Data) ->
    handle_heartbeat_result(Peer, Round, Result, State, Data);
handle_agent_response(Peer, catchup, Result, State, Data) ->
    handle_catchup_result(Peer, Result, State, Data).

handle_establish_term_result(Peer, Result, State, Data) ->
    case Result of
        {ok, #metadata{committed_seqno = CommittedSeqno} = Metadata} ->
            set_peer_active(Peer, Metadata, Data),
            establish_term_handle_vote(Peer, {ok, CommittedSeqno}, State, Data);
        {error, Error} ->
            case handle_common_error(Peer, Error, Data) of
                {stop, Reason} ->
                    stop(Reason, State, Data);
                ignored ->
                    #data{history_id = HistoryId,
                          term = Term,
                          position = Position} = Data,

                    ?WARNING("Failed to establish "
                             "term ~w (history id ~p, log position ~w) "
                             "on peer ~w: ~w",
                             [Term, HistoryId, Position, Peer, Error]),

                    Ignore =
                        case Error of
                            {behind, _} ->
                                %% We keep going despite the fact we're behind
                                %% this peer because we still might be able to
                                %% get a majority of votes.
                                true;
                            {conflicting_term, _} ->
                                %% Some conflicting_term errors are ignored by
                                %% handle_common_error. If we hit one, we
                                %% record a failed vote, but keep going.
                                true;
                            {bad_state, _} ->
                                true;
                            _ ->
                                false
                        end,

                    case Ignore of
                        true ->
                            remove_peer_status(Peer, Data),
                            establish_term_handle_vote(Peer,
                                                       failed, State, Data);
                        false ->
                            stop({unexpected_error, Peer, Error}, State, Data)
                    end
            end
    end.

handle_common_error(Peer, Error,
                    #data{history_id = HistoryId, term = Term}) ->
    case Error of
        {conflicting_term, OtherTerm} ->
            OurTermNumber = term_number(Term),
            OtherTermNumber = term_number(OtherTerm),

            case OtherTermNumber > OurTermNumber of
                true ->
                    ?INFO("Conflicting term on peer ~w. Stopping.~n"
                          "History id: ~p~n"
                          "Our term: ~w~n"
                          "Conflicting term: ~w",
                          [Peer, HistoryId, Term, OtherTerm]),
                    {stop, {conflicting_term, Term, OtherTerm}};
                false ->
                    %% This is most likely to happen when two different nodes
                    %% try to start a term of the same number at around the
                    %% same time. If one of the nodes manages to establish the
                    %% term on a quorum of nodes, despite conflicts, it'll be
                    %% able propose and replicate just fine. So we ignore such
                    %% conflicts.
                    true = (OurTermNumber =:= OtherTermNumber),
                    ?INFO("Conflicting term on peer ~w. Ignoring.~n"
                          "History id: ~p~n"
                          "Our term: ~w~n"
                          "Conflicting term: ~w~n",
                          [Peer, HistoryId, Term, OtherTerm]),

                    ignored
            end;
        {history_mismatch, OtherHistoryId} ->
            ?INFO("Saw history mismatch when talking to peer ~w.~n"
                  "Our history id: ~p~n"
                  "Conflicting history id: ~p",
                  [Peer, HistoryId, OtherHistoryId]),

            %% The system has undergone a partition. Either we are part of the
            %% new partition but haven't received the corresponding branch
            %% record yet. Or alternatively, we've been partitioned out. In
            %% the latter case we, probably, shouldn't continue to operate.
            %%
            %% TODO: handle the latter case better
            {stop, {history_mismatch, HistoryId, OtherHistoryId}};
        _ ->
            ignored
    end.

establish_term_handle_vote(Peer, Status, proposing = State, Data) ->
    case Status of
        {ok, _} ->
            %% Though not very likely, it's possible that this peer knew that
            %% some seqno was committed, while none of the nodes we talked to
            %% before successfully establishing the term did. In such case
            %% receiving this response may advance our idea of what is
            %% committed (though at the moment the committed seqno returned by
            %% the peer is ignored). That's why we evaluate the committed
            %% seqno here instead of simply replicating to the peer.
            check_committed_seqno_advanced(
              #{must_replicate_to => Peer}, State, Data);
        failed ->
            %% This is not exactly clean. But the intention is the
            %% following. We got some error that we chose to ignore. But since
            %% we are already proposing, we need to know this peer's
            %% position.
            {keep_state, send_heartbeat(Peer, Data)}
    end;
establish_term_handle_vote(Peer, Status, establish_term = State,
                           #data{high_seqno = HighSeqno,
                                 committed_seqno = OurCommittedSeqno,
                                 votes = Votes,
                                 failed_votes = FailedVotes} = Data) ->
    false = lists:member(Peer, Votes),
    false = lists:member(Peer, FailedVotes),

    NewData =
        case Status of
            {ok, CommittedSeqno} ->
                NewCommittedSeqno = max(OurCommittedSeqno, CommittedSeqno),
                case NewCommittedSeqno =/= OurCommittedSeqno of
                    true ->
                        true = (HighSeqno >= NewCommittedSeqno),
                        ?INFO("Discovered new committed seqno from peer ~w.~n"
                              "Old committed seqno: ~b~n"
                              "New committed seqno: ~b",
                              [Peer, OurCommittedSeqno, NewCommittedSeqno]);
                    false ->
                        ok
                end,

                Data#data{votes = [Peer | Votes],
                          committed_seqno = NewCommittedSeqno};
            failed ->
                Data#data{failed_votes = [Peer | FailedVotes]}
        end,

    establish_term_maybe_transition(State, NewData).

establish_term_maybe_transition(establish_term = State,
                                #data{term = Term,
                                      history_id = HistoryId,
                                      quorum_peers = QuorumPeers,
                                      votes = Votes,
                                      failed_votes = FailedVotes,
                                      quorum = Quorum} = Data) ->
    case have_quorum(Votes, Quorum) of
        true ->
            ?DEBUG("Established term ~w (history id ~p) successfully.~n"
                   "Votes: ~w",
                   [Term, HistoryId, Votes]),

            {next_state, proposing, Data};
        false ->
            case is_quorum_feasible(QuorumPeers, FailedVotes, Quorum) of
                true ->
                    {keep_state, Data};
                false ->
                    ?WARNING("Couldn't establish term ~w, history id ~p.~n"
                             "Votes received: ~w~n"
                             "Quorum: ~w~n",
                             [Term, HistoryId, Votes, Quorum]),
                    stop({error, no_quorum}, State, Data)
            end
    end.

maybe_resolve_branch(#data{branch = undefined} = Data) ->
    Data;
maybe_resolve_branch(#data{high_seqno = HighSeqno,
                           committed_seqno = CommittedSeqno,
                           branch = Branch,
                           config = #log_entry{value = Config},
                           pending_entries = PendingEntries} = Data) ->
    %% Some of the pending entries may actually be committed, but our local
    %% agent doesn't know yet. So those need to be preserved.
    NewPendingEntries =
        chronicle_utils:queue_takewhile(
          fun (#log_entry{seqno = Seqno}) ->
                  Seqno =< CommittedSeqno
          end, PendingEntries),
    NewData = Data#data{branch = undefined,
                        %% Note, that this essintially truncates any
                        %% uncommitted entries. This is acceptable/safe to do
                        %% for the following reasons:
                        %%
                        %%  1. We are going through a quorum failover, so data
                        %%  inconsistencies are expected.
                        %%
                        %%  2. Since a unanimous quorum is required for
                        %%  resolving quorum failovers, the leader is
                        %%  guaranteed to know the highest committed seqno
                        %%  observed by the surviving part of the cluster. In
                        %%  other words, we won't truncate something that was
                        %%  known to have been committed.
                        high_seqno = CommittedSeqno,
                        pending_entries = NewPendingEntries},

    %% Note, that the new config may be based on an uncommitted config that
    %% will get truncated from the history. This can be confusing and it's
    %% possible to deal with this situation better. But for the time being I
    %% decided not to bother.
    NewConfig = chronicle_config:branch(Branch, Config),

    ?INFO("Resolving a branch.~n"
          "High seqno: ~p~n"
          "Committed seqno: ~p~n"
          "Branch:~n~p~n"
          "Latest known config:~n~p~n"
          "New config:~n~p",
          [HighSeqno, CommittedSeqno, Branch, Config, NewConfig]),

    force_propose_config(NewConfig, NewData).

handle_append_result(Peer, Request, Result, proposing = State, Data) ->
    {append, CommittedSeqno, HighSeqno} = Request,

    case Result of
        ok ->
            NewData = maybe_drop_pending_entries(Peer, CommittedSeqno, Data),
            handle_append_ok(Peer, HighSeqno, CommittedSeqno, State, NewData);
        {error, Error} ->
            handle_replication_error(Peer, Error, append, State, Data)
    end.

maybe_drop_pending_entries(Peer, NewCommittedSeqno, #data{peer = Self} = Data)
  when Peer =:= Self ->
    OldCommittedSeqno = Data#data.local_committed_seqno,
    case OldCommittedSeqno =:= NewCommittedSeqno of
        true ->
            Data;
        false ->
            PendingEntries = Data#data.pending_entries,
            NewPendingEntries =
                chronicle_utils:queue_dropwhile(
                  fun (Entry) ->
                          Entry#log_entry.seqno =< NewCommittedSeqno
                  end, PendingEntries),

            Data#data{pending_entries = NewPendingEntries,
                      local_committed_seqno = NewCommittedSeqno}
    end;
maybe_drop_pending_entries(_, _, Data) ->
    Data.

handle_replication_error(Peer, Error, Op, State, Data) ->
    handle_replication_error(Peer, Error, Op,
                             fun (_) -> false end,
                             State, Data).

handle_replication_error(Peer, Error, Op, ErrorPred,
                         proposing = State, #data{peer = Self} = Data) ->
    case handle_common_error(Peer, Error, Data) of
        {stop, Reason} ->
            stop(Reason, State, Data);
        ignored ->
            ?WARNING("Error on peer ~w while sending ~w: ~w",
                     [Peer, Op, Error]),

            Ignore =
                case Error of
                    {bad_state, _} ->
                        %% The peer may have gotten wiped or didn't get
                        %% initialized properly before being added to the
                        %% cluster. Attempting to ingore under the assumption
                        %% that eventually the condition that lead to the
                        %% error will get resolved.
                        Peer =/= Self;
                    {missing_entries, _} ->
                        %% This should not happen normally. But I couldn't
                        %% convince myself that erlang guarantees that the
                        %% following isn't possible:
                        %%
                        %% 1. Connection to remote node disappears.
                        %%
                        %% 2. We send more stuff to the node without realizing
                        %% it.
                        %%
                        %% 3. This reestablishes the connection and the agent
                        %% sends us back a response.
                        %%
                        %% 4. We process this response before DOWN message
                        %% about (1) is delivered.
                        Peer =/= Self;
                    _ ->
                        ErrorPred(Error)
                end,

            case Ignore of
                true ->
                    maybe_cancel_peer_catchup(Peer, Data),
                    remove_peer_status(Peer, Data),
                    {keep_state, demonitor_agents([Peer], Data)};
                false ->
                    stop({unexpected_error, Peer, Error}, State, Data)
            end
    end.

handle_append_ok(Peer, PeerHighSeqno,
                 PeerCommittedSeqno, proposing = State, Data) ->
    set_peer_acked_seqnos(Peer, PeerHighSeqno, PeerCommittedSeqno, Data),
    check_committed_seqno_advanced(
      %% Always check the peer we got the response from. So if we didn't
      %% replicate some entries to it because there were too many in flight,
      %% we do this as soon as possible.
      #{must_replicate_to => Peer},
      State, Data).

check_committed_seqno_advanced(Options, State, Data) ->
    #data{committed_seqno = CommittedSeqno,
          safe_commit_seqno = SafeCommitSeqno} = Data,

    QuorumSeqno = deduce_quorum_seqno(Data),
    case QuorumSeqno > CommittedSeqno andalso QuorumSeqno >= SafeCommitSeqno of
        true ->
            NewData0 = Data#data{committed_seqno = QuorumSeqno},

            case handle_config_post_append(Data, NewData0) of
                {ok, NewData, Effects} ->
                    {keep_state, replicate(NewData), Effects};
                {stop, Reason, NewData} ->
                    stop(Reason, State, replicate(NewData))
            end;
        false ->
            %% Note, that it's possible for the deduced committed seqno to go
            %% backwards with respect to our own committed seqno here. This
            %% may happen for multiple reasons. The simplest scenario is where
            %% some nodes go down at which point their peer statuses are
            %% erased. If the previous committed seqno was acknowledged only
            %% by a minimal majority of nodes, any of them going down will
            %% result in the deduced seqno going backwards.
            %%
            %% Another possibility is when a new topology is adopted. Since
            %% deduce_committed_seqno/1 always uses the most up-to-date
            %% topology, what was committed in the old topooogy, might not yet
            %% have a quorum in the new topology. In such case the deduced
            %% committed sequence number will be ?NO_SEQNO.
            NewData =
                case maps:find(must_replicate_to, Options) of
                    {ok, Peers} ->
                        replicate(Peers, Data);
                    error ->
                        Data
                end,

            {keep_state, NewData}
    end.

handle_heartbeat_result(Peer, Round, Result, proposing = State, Data) ->
    case Result of
        {ok, Metadata} ->
            maybe_set_peer_active(Peer, Metadata, Data),
            set_peer_acked_sync_round(Peer, Round, Data),

            NewData = check_sync_round_advanced(Data),

            %% We need to check if the committed seqno has advanced because
            %% it's possible that we previously replicated something to this
            %% peer but never got a response because the connection got
            %% dropped. Once a new connection is created, there'll be nothing
            %% more to replicate to the peer. So we won't go through the usual
            %% append code path (unless there are more mutations to
            %% replicate). But if this peer is required by the quorum (like if
            %% there are only two nodes in the cluster), we won't be able to
            %% detect that what was replicated before the connection drop is
            %% committed.
            check_committed_seqno_advanced(
              #{must_replicate_to => Peer}, State, NewData);
        {error, Error} ->
            handle_replication_error(Peer, Error, heartbeat, State, Data)
    end.

check_sync_round_advanced(#data{acked_sync_round = AckedRound} = Data) ->
    NewAckedRound = deduce_acked_sync_round(Data),
    case NewAckedRound > AckedRound of
        true ->
            NewData = Data#data{acked_sync_round = NewAckedRound},
            sync_quorum_maybe_reply(NewData);
        false ->
            Data
    end.

sync_quorum_maybe_reply(#data{acked_sync_round = AckedRound,
                              sync_requests = Requests} = Data) ->
    {_, NewRequests} =
        chronicle_utils:queue_takefold(
          fun (Request, _Acc) ->
                  case Request#sync_request.round =< AckedRound of
                      true ->
                          reply_request(Request#sync_request.reply_to,
                                        Request#sync_request.ok_reply),
                          {true, unused};
                      false ->
                          false
                  end
          end, unused, Requests),

    Data#data{sync_requests = NewRequests}.

sync_quorum_reply_not_leader(#data{sync_requests = SyncRequests} = Data) ->
    chronicle_utils:queue_foreach(
      fun (#sync_request{reply_to = ReplyTo}) ->
              reply_not_leader(ReplyTo)
      end, SyncRequests),

    Data#data{sync_requests = queue:new()}.

handle_catchup_result(Peer, Result, proposing = State, Data) ->
    case Result of
        {ok, SentCommittedSeqno} ->
            ?DEBUG("Caught up peer ~w to seqno ~b", [Peer, SentCommittedSeqno]),
            set_peer_catchup_done(Peer, SentCommittedSeqno, Data),
            {keep_state, replicate(Peer, Data)};
        {compacted, PeerSeqno} ->
            ?INFO("Catchup to peer ~w failed because "
                  "log/snapshots got compacted. Last sent seqno: ~b",
                  [Peer, PeerSeqno]),
            set_peer_catchup_done(Peer, PeerSeqno, Data),
            {keep_state, replicate(Peer, Data)};
        {error, Error} ->
            handle_catchup_error(Peer, Error, State, Data)
    end.

handle_catchup_error(Peer, Error, State, Data) ->
    handle_replication_error(Peer, Error, catchup,
                             fun (Err) ->
                                     case Err of
                                         %% Catchup failed for an unknown
                                         %% reason, attempt to ignore.
                                         {catchup_failed, _} ->
                                             true;
                                         _ ->
                                             false
                                     end
                             end, State, Data).

maybe_complete_config_transition(#data{config = ConfigEntry} = Data) ->
    Config = ConfigEntry#log_entry.value,
    case chronicle_config:is_stable(Config) of
        true ->
            Data;
        false ->
            case is_config_committed(Data) of
                true ->
                    NextConfig = chronicle_config:next_config(Config),
                    %% Preserve config_change_from if any.
                    ReplyTo = Data#data.config_change_reply_to,
                    propose_config(NextConfig, ReplyTo, Data);
                false ->
                    Data
            end
    end.

maybe_reply_config_change(#data{config = ConfigEntry,
                                config_change_reply_to = ReplyTo} = Data) ->
    case chronicle_config:is_stable(ConfigEntry#log_entry.value) of
        true ->
            case ReplyTo =/= undefined of
                true ->
                    true = is_config_committed(Data),
                    Revision = log_entry_revision(ConfigEntry),
                    reply_request(ReplyTo, {ok, Revision}),
                    Data#data{config_change_reply_to = undefined};
                false ->
                    Data
            end;
        false ->
            %% We only reply once the stable config gets committed.
            Data
    end.

maybe_postpone_config_request(Request, Data, Fun) ->
    case is_config_committed(Data) of
        true ->
            Fun();
        false ->
            #data{postponed_config_requests = Postponed} = Data,
            NewPostponed = [{cast, Request} | Postponed],
            {keep_state, Data#data{postponed_config_requests = NewPostponed}}
    end.

unpostpone_config_requests(#data{postponed_config_requests =
                                     Postponed} = Data) ->
    NewData = Data#data{postponed_config_requests = []},
    Effects = [{next_event, Type, Request} ||
                  {Type, Request} <- lists:reverse(Postponed)],
    {NewData, Effects}.

check_leader_got_removed(#data{being_removed = BeingRemoved} = Data) ->
    BeingRemoved andalso is_config_committed(Data).

handle_config_post_append(OldData,
                          #data{peer = Peer,
                                config = ConfigEntry} = NewData) ->
    ConfigRevision = log_entry_revision(ConfigEntry),
    GotCommitted =
        not is_revision_committed(ConfigRevision, OldData)
        andalso is_revision_committed(ConfigRevision, NewData),

    case GotCommitted of
        true ->
            %% Stop replicating to nodes that might have been removed.
            NewData0 = reset_peers(NewData),

            NewData1 = maybe_reply_config_change(NewData0),
            NewData2 = maybe_complete_config_transition(NewData1),

            case check_leader_got_removed(NewData2) of
                true ->
                    ?INFO("Shutting down because leader ~w "
                          "got removed from peers.~n"
                          "Peers: ~w",
                          [Peer, NewData2#data.quorum_peers]),
                    {stop, leader_removed, NewData2};
                false ->
                    %% Deliver postponed config changes again. We've postponed
                    %% them all the way till this moment to be able to return
                    %% an error that includes the revision of the conflicting
                    %% config. That way the caller can wait to receive the
                    %% conflicting config before retrying.
                    {NewData3, Effects} = unpostpone_config_requests(NewData2),
                    {ok, NewData3, Effects}
            end;
        false ->
            %% Nothing changed, so nothing to do.
            {ok, NewData, []}
    end.

reset_peers(#data{config = ConfigEntry} = Data) ->
    Peers = require_self_peer(config_peers(ConfigEntry), Data),
    NewData = Data#data{peers = Peers},
    handle_new_peers(Data, NewData).

is_config_committed(#data{config = ConfigEntry} = Data) ->
    ConfigRevision = log_entry_revision(ConfigEntry),
    is_revision_committed(ConfigRevision, Data).

is_revision_committed({_, Seqno}, #data{committed_seqno = CommittedSeqno}) ->
    Seqno =< CommittedSeqno.

get_machines(ConfigEntry) ->
    maps:keys(chronicle_config:get_rsms(ConfigEntry#log_entry.value)).

handle_nodeup(Peer, _Info, State, #data{peers = Peers} = Data) ->
    ?INFO("Peer ~w came up", [Peer]),
    case State of
        establish_term ->
            %% Note, no attempt is made to send establish_term requests to
            %% peers that come up while we're in establish_term state. The
            %% motivation is as follows:
            %%
            %%  1. We go through this state only once right after an election,
            %%  so normally there should be a quorum of peers available anyway.
            %%
            %%  2. Since peers can flip back and forth, it's possible that
            %%  we've already sent an establish_term request to this peer and
            %%  we'll get an error when we try to do this again.
            %%
            %%  3. In the worst case, we won't be able to establish the
            %%  term. This will trigger another election and once and if we're
            %%  elected again, we'll retry with a new set of live peers.
            keep_state_and_data;
        {stopped, _} ->
            keep_state_and_data;
        proposing ->
            case lists:member(Peer, Peers) of
                true ->
                    case get_peer_status(Peer, Data) of
                        {ok, _} ->
                            %% We are already in contact with the peer
                            %% (likely, check_peers initiated the connection
                            %% and that's why we got this message). Nothing
                            %% needs to be done.
                            keep_state_and_data;
                        not_found ->
                            {keep_state,
                             send_heartbeat(Peer, Data)}
                    end;
                false ->
                    ?INFO("Peer ~w is not in peers: ~w", [Peer, Peers]),
                    keep_state_and_data
            end
    end.

handle_nodedown(Peer, Info, _State, _Data) ->
    %% If there was an outstanding request, we'll also receive a DOWN message
    %% and handle everything there. Otherwise, we don't care.
    ?INFO("Peer ~p went down: ~p", [Peer, Info]),
    keep_state_and_data.

handle_down(MRef, Pid, Reason, State, #data{peer = Self} = Data) ->
    {ok, Peer, NewData} = take_monitor(MRef, Data),
    ?INFO("Observed agent ~w on peer ~w "
          "go down with reason ~W",
          [Pid, Peer, sanitize_reason(Reason), 10]),

    case Peer =:= Self of
        true ->
            ?ERROR("Terminating proposer "
                   "because local agent ~w terminated", [Pid]),
            stop(local_agent_terminated, State, Data);
        false ->
            maybe_cancel_peer_catchup(Peer, NewData),
            remove_peer_status(Peer, NewData),

            case State of
                establish_term ->
                    handle_down_establish_term(Peer, State, NewData);
                proposing ->
                    {keep_state, NewData}
            end
    end.

handle_down_establish_term(Peer,
                           establish_term = State,
                           #data{votes = Votes,
                                 failed_votes = FailedVotes} = Data) ->
    %% We might have already gotten a response from this peer before it went
    %% down.
    HasVoted = lists:member(Peer, Votes)
        orelse lists:member(Peer, FailedVotes),

    case HasVoted of
        true ->
            {keep_state, Data};
        false ->
            establish_term_handle_vote(Peer, failed, State, Data)
    end.

handle_append_commands(Commands, {stopped, _}, _Data) ->
    %% Proposer has stopped. Reject any incoming commands.
    reply_commands_not_leader(Commands),
    keep_state_and_data;
handle_append_commands(Commands, proposing, #data{being_removed = true}) ->
    %% Node is being removed. Don't accept new commands.
    reply_commands_not_leader(Commands),
    keep_state_and_data;
handle_append_commands(Commands,
                       proposing,
                       #data{high_seqno = HighSeqno,
                             pending_entries = PendingEntries} = Data) ->
    {NewHighSeqno, NewPendingEntries, NewData0} =
        lists:foldl(
          fun ({ReplyTo, Command}, {PrevSeqno, AccEntries, AccData} = Acc) ->
                  Seqno = PrevSeqno + 1,
                  case handle_command(Command, Seqno, AccData) of
                      {ok, LogEntry, NewAccData} ->
                          reply_request(ReplyTo, {accepted, Seqno}),
                          {Seqno, queue:in(LogEntry, AccEntries), NewAccData};
                      {reject, Error} ->
                          reply_request(ReplyTo, Error),
                          Acc
                  end
          end,
          {HighSeqno, PendingEntries, Data}, Commands),

    NewData1 = NewData0#data{pending_entries = NewPendingEntries,
                             high_seqno = NewHighSeqno},

    {keep_state, replicate(NewData1)}.

handle_command({rsm_command, RSMName, Command}, Seqno,
               #data{machines = Machines} = Data) ->
    case lists:member(RSMName, Machines) of
        true ->
            RSMCommand = #rsm_command{rsm_name = RSMName,
                                      command = Command},
            {ok, make_log_entry(Seqno, RSMCommand, Data), Data};
        false ->
            ?WARNING("Received a command "
                     "referencing a non-existing RSM: ~w", [RSMName]),
            {reject, {error, {unknown_rsm, RSMName}}}
    end.

reply_commands_not_leader(Commands) ->
    {ReplyTos, _} = lists:unzip(Commands),
    lists:foreach(fun reply_not_leader/1, ReplyTos).

handle_sync_quorum(ReplyTo, {stopped, _}, _Data) ->
    reply_not_leader(ReplyTo),
    keep_state_and_data;
handle_sync_quorum(ReplyTo, proposing, Data) ->
    start_sync_quorum(ReplyTo, ok, Data).

start_sync_quorum(ReplyTo, OkReply,
                  #data{sync_round = Round,
                        sync_requests = SyncRequests} = Data) ->
    NewRound = Round + 1,
    Request = #sync_request{reply_to = ReplyTo,
                            ok_reply = OkReply,
                            round = NewRound},
    NewSyncRequests = queue:in(Request, SyncRequests),
    NewData = Data#data{sync_round = NewRound,
                        sync_requests = NewSyncRequests},

    {keep_state, send_heartbeat(NewData)}.

handle_query(ReplyTo, Query, Data) ->
    case Query of
        get_config ->
            handle_get_config(ReplyTo, Data);
        get_cluster_info ->
            handle_get_cluster_info(ReplyTo, Data);
        _ ->
            reply_request(ReplyTo, {error, unknown_query})
    end.

handle_get_config(ReplyTo, #data{config = ConfigEntry} = Data) ->
    true = is_config_committed(Data),

    Config = ConfigEntry#log_entry.value,
    true = chronicle_config:is_stable(Config),
    ConfigRevision = log_entry_revision(ConfigEntry),

    Reply = {ok, Config, ConfigRevision},
    start_sync_quorum(ReplyTo, Reply, Data).

handle_get_cluster_info(ReplyTo,
                        #data{history_id = HistoryId,
                              config = ConfigEntry,
                              committed_seqno = CommittedSeqno} = Data) ->
    true = is_config_committed(Data),
    Info = #{history_id => HistoryId,
             committed_seqno => CommittedSeqno,
             config => ConfigEntry},
    start_sync_quorum(ReplyTo, Info, Data).

handle_cas_config(ReplyTo, NewConfig, CasRevision, Data) ->
    case check_cas_config(NewConfig, CasRevision, Data) of
        {ok, OldConfig} ->
            FinalConfig = chronicle_config:transition(NewConfig, OldConfig),
            NewData = propose_config(FinalConfig, ReplyTo, Data),
            {keep_state, replicate(NewData)};
        {error, _} = Error ->
            reply_request(ReplyTo, Error),
            keep_state_and_data
    end.

check_cas_config(NewConfig, CasRevision, #data{config = ConfigEntry}) ->
    #log_entry{value = Config} = ConfigEntry,
    ConfigRevision = log_entry_revision(ConfigEntry),
    true = chronicle_config:is_stable(Config),

    case CasRevision =:= ConfigRevision of
        true ->
            case chronicle_config:is_config(NewConfig)
                andalso chronicle_config:is_stable(NewConfig) of
                true ->
                    {ok, Config};
                false ->
                    {error, {invalid_config, NewConfig}}
            end;
        false ->
            {error, {cas_failed, ConfigRevision}}
    end.

handle_stop(From, State,
            #data{history_id = HistoryId, term = Term} = Data) ->
    ?INFO("Proposer for term ~w "
          "in history ~p is terminating.", [Term, HistoryId]),
    case State of
        {stopped, Reason} ->
            {stop_and_reply,
             {shutdown, Reason},
             {reply, From, ok}};
        _ ->
            stop(stop, [postpone], State, Data)
    end.

make_log_entry(Seqno, Value, #data{history_id = HistoryId, term = Term}) ->
    #log_entry{history_id = HistoryId,
               term = Term,
               seqno = Seqno,
               value = Value}.

update_config(ConfigEntry, #data{peer = Self,
                                 quorum_peers = OldQuorumPeers} = Data) ->
    RawQuorum = get_append_quorum(ConfigEntry),
    BeingRemoved = not lists:member(Self, get_quorum_peers(RawQuorum)),

    %% Always require include local to acknowledge writes, even if the node is
    %% being removed.
    Quorum = require_self_quorum(RawQuorum, Data),
    QuorumPeers = get_quorum_peers(Quorum),
    AllPeers = require_self_peer(config_peers(ConfigEntry), Data),

    %% When nodes are being removed, attempt to notify them about the new
    %% config that removes them. This is just a best-effort approach. If nodes
    %% are down -- they are not going to get notified.
    NewPeers = lists:usort(OldQuorumPeers ++ AllPeers),
    NewData = Data#data{config = ConfigEntry,
                        being_removed = BeingRemoved,
                        quorum = Quorum,
                        quorum_peers = QuorumPeers,
                        peers = NewPeers,
                        machines = get_machines(ConfigEntry)},

    handle_new_peers(Data, NewData).

handle_new_peers(#data{peers = OldPeers},
                 #data{peers = NewPeers,
                       quorum_peers = NewQuorumPeers} = NewData) ->
    [] = (NewQuorumPeers -- NewPeers),

    RemovedPeers = OldPeers -- NewPeers,
    AddedPeers = NewPeers -- OldPeers,

    %% If some quorum peers were removed, we might have enough votes in the
    %% new quorum.
    NewData1 = check_sync_round_advanced(NewData),

    handle_added_peers(AddedPeers,
                       handle_removed_peers(RemovedPeers, NewData1)).

handle_removed_peers(Peers, Data) ->
    %% Attempt to replicate to the removed nodes. In the normal case where the
    %% removed nodes are healthy, this will notify them about the config that
    %% got them removed (via reset_peers()).
    NewData = replicate(Peers, Data),
    remove_peer_statuses(Peers, NewData),
    demonitor_agents(Peers, NewData).

handle_added_peers(Peers, Data) ->
    send_heartbeat(Peers, Data).

force_propose_config(Config,
                     #data{config_change_reply_to = undefined} = Data) ->
    %% This function doesn't check that the current config is committed, which
    %% should be the case for regular config transitions. It's only meant to
    %% be used after resolving a branch.
    do_propose_config(Config, undefined, Data).

propose_config(Config, ReplyTo, Data) ->
    true = is_config_committed(Data),
    do_propose_config(Config, ReplyTo, Data).

%% TODO: right now when this function is called we replicate the proposal in
%% its own batch. But it can be coalesced with user batches.
do_propose_config(Config, ReplyTo, Data) ->
    {LogEntry, NewData0} = propose_value(Config, Data),
    NewData = NewData0#data{config_change_reply_to = ReplyTo},
    update_config(LogEntry, NewData).

propose_value(Value,
              #data{high_seqno = HighSeqno,
                    pending_entries = Entries} = Data) ->
    Seqno = HighSeqno + 1,
    LogEntry = make_log_entry(Seqno, Value, Data),

    NewEntries = queue:in(LogEntry, Entries),
    NewData = Data#data{pending_entries = NewEntries, high_seqno = Seqno},

    {LogEntry, NewData}.

get_peer_status(Peer, #data{peer_statuses = Tab}) ->
    case ets:lookup(Tab, Peer) of
        [{_, PeerStatus}] ->
            {ok, PeerStatus};
        [] ->
            not_found
    end.

get_peer_state(Peer, Data) ->
    case get_peer_status(Peer, Data) of
        {ok, #peer_status{state = PeerState}} ->
            {ok, PeerState};
        not_found ->
            not_found
    end.

put_peer_status(Peer, PeerStatus, #data{peer_statuses = Tab}) ->
    ets:insert(Tab, {Peer, PeerStatus}).

update_peer_status(Peer, Fun, #data{peer_statuses = Tab} = Data) ->
    {ok, PeerStatus} = get_peer_status(Peer, Data),
    ets:insert(Tab, {Peer, Fun(PeerStatus)}).

set_peer_status_requested(Peer, Data) ->
    true = do_set_peer_status_requested(Peer, Data).

maybe_set_peer_status_requested(Peer, Data) ->
    _ = do_set_peer_status_requested(Peer, Data),
    ok.

do_set_peer_status_requested(Peer, #data{peer_statuses = Tab}) ->
    PeerStatus = #peer_status{state = status_requested,
                              sent_sync_round = 0,
                              acked_sync_round = 0},
    ets:insert_new(Tab, {Peer, PeerStatus}).

maybe_set_peer_active(Peer, Metadata, Data) ->
    {ok, PeerState} = get_peer_state(Peer, Data),
    case PeerState of
        status_requested ->
            set_peer_active(Peer, Metadata, Data);
        _ ->
            ok
    end.

set_peer_active(Peer, Metadata, Data) ->
    {ok, PeerStatus} = get_peer_status(Peer, Data),
    %% We should never overwrite an existing peer status.
    status_requested = PeerStatus#peer_status.state,
    set_peer_active(Peer, PeerStatus, Metadata, Data).

set_peer_active(Peer, PeerStatus, Metadata,
                #data{committed_seqno = CommittedSeqno,
                      high_seqno = HighSeqno} = Data) ->
    #metadata{committed_seqno = PeerCommittedSeqno,
              high_term = PeerHighTerm,
              high_seqno = PeerHighSeqno} = Metadata,

    PeerCompatible =
        case PeerHighSeqno =< HighSeqno of
            true ->
                case get_term_for_seqno(PeerHighSeqno, Data) of
                    {ok, Term} ->
                        Term =:= PeerHighTerm;
                    {error, compacted} ->
                        true = (PeerHighSeqno =< CommittedSeqno),
                        %% The peer fell too far behind. Compatible or not,
                        %% we'll need to send a snapshot to it.
                        false
                end;
            false ->
                false
        end,

    {SentCommittedSeqno, SentHighSeqno} =
        case PeerCompatible of
            true ->
                {PeerCommittedSeqno, PeerHighSeqno};
            false ->
                %% Peer's got some entries that are incompatible with our view
                %% of the history. Instead of trying to find the point where
                %% our histories diverged, we'll resend all entries starting
                %% from peer's committed seqno (which must be compatible).
                {PeerCommittedSeqno, PeerCommittedSeqno}
        end,

    set_peer_active(Peer, PeerStatus, SentHighSeqno, SentCommittedSeqno, Data).

set_peer_active(Peer, PeerStatus, HighSeqno, CommittedSeqno, Data) ->
    NewPeerStatus = PeerStatus#peer_status{
                      acked_seqno = HighSeqno,
                      sent_seqno = HighSeqno,
                      acked_commit_seqno = CommittedSeqno,
                      sent_commit_seqno = CommittedSeqno,
                      state = active},
    put_peer_status(Peer, NewPeerStatus, Data).

set_peer_sent_seqnos(Peer, HighSeqno, CommittedSeqno, Data) ->
    update_peer_status(
      Peer,
      fun (#peer_status{acked_seqno = AckedSeqno,
                        state = PeerState} = PeerStatus) ->
              active = PeerState,
              true = (HighSeqno >= AckedSeqno),
              true = (HighSeqno >= CommittedSeqno),

              PeerStatus#peer_status{sent_seqno = HighSeqno,
                                     sent_commit_seqno = CommittedSeqno}
      end, Data).

set_peer_acked_seqnos(Peer, HighSeqno, CommittedSeqno, Data) ->
    update_peer_status(
      Peer,
      fun (#peer_status{sent_seqno = SentHighSeqno,
                        sent_commit_seqno = SentCommittedSeqno,
                        state = PeerState} = PeerStatus) ->
              active = PeerState,
              true = (SentHighSeqno >= HighSeqno),
              true = (SentCommittedSeqno >= CommittedSeqno),

              PeerStatus#peer_status{acked_seqno = HighSeqno,
                                     acked_commit_seqno = CommittedSeqno}
      end, Data).

set_peer_catchup(Peer, Data) ->
    update_peer_status(
      Peer,
      fun (#peer_status{state = active} = PeerStatus) ->
              PeerStatus#peer_status{state = catchup}
      end, Data).

set_peer_catchup_done(Peer, SentCommittedSeqno, Data) ->
    {ok, PeerStatus} = get_peer_status(Peer, Data),
    catchup = PeerStatus#peer_status.state,
    set_peer_active(Peer, PeerStatus,
                    SentCommittedSeqno, SentCommittedSeqno, Data).

set_peer_acked_sync_round(Peer, Round, Data) ->
    update_peer_status(
      Peer,
      fun (#peer_status{sent_sync_round = SentRound,
                        acked_sync_round = AckedRound} = PeerStatus) ->
              true = (Round =< SentRound),
              true = (Round >= AckedRound),
              PeerStatus#peer_status{acked_sync_round = Round}
      end, Data).

set_peer_sent_sync_round(Peer, Round, Data) ->
    update_peer_status(
      Peer,
      fun (#peer_status{sent_sync_round = SentRound} = PeerStatus) ->
              true = (Round >= SentRound),
              PeerStatus#peer_status{sent_sync_round = Round}
      end, Data).

remove_peer_status(Peer, Data) ->
    remove_peer_statuses([Peer], Data).

remove_peer_statuses(Peers, #data{peer_statuses = Tab}) ->
    lists:foreach(
      fun (Peer) ->
              ets:delete(Tab, Peer)
      end, Peers).

maybe_send_requests(Peers, Data, Fun) ->
    NewData = monitor_agents(Peers, Data),
    NotSent =
        lists:filtermap(
          fun (Peer) ->
                  {ok, Ref} = get_peer_monitor(Peer, NewData),
                  ServerRef = chronicle_agent:server_ref(Peer, Data#data.peer),
                  case Fun(Peer, Ref, ServerRef) of
                      true ->
                          false;
                      false ->
                          true;
                      {false, Reason} ->
                          {true, {Peer, Reason}}
                  end
          end, Peers),

    {NewData, NotSent}.

make_agent_opaque(Ref, Peer, Request) ->
    {agent_response, Ref, Peer, Request}.

send_requests(Peers, Data, Fun) ->
    {NewData, []} =
        maybe_send_requests(
          Peers, Data,
          fun (Peer, PeerRef, ServerRef) ->
                  Fun(Peer, PeerRef, ServerRef),
                  true
          end),
    NewData.

send_local_establish_term(Metadata, #data{peer = Self} = Data) ->
    Peers = [Self],
    set_peer_status_requested(Self, Data),

    send_requests(
      Peers, Data,
      fun (Peer, PeerRef, _ServerRef) ->
              Opaque = make_agent_opaque(PeerRef, Peer, establish_term),
              self() ! {Opaque, {ok, Metadata}}
      end).

send_establish_term(Peers, Position,
                    #data{history_id = HistoryId, term = Term} = Data) ->
    maybe_send_requests(
      Peers, Data,
      fun (Peer, PeerRef, ServerRef) ->
              ?DEBUG("Sending establish_term request to peer ~w. "
                     "Term = ~w. History Id: ~p. "
                     "Log position: ~w.",
                     [Peer, Term, HistoryId, Position]),

              Opaque = make_agent_opaque(PeerRef, Peer, establish_term),
              case chronicle_agent:establish_term(ServerRef, Opaque,
                                                  HistoryId, Term, Position,
                                                  [nosuspend]) of
                  ok ->
                      set_peer_status_requested(Peer, Data),
                      true;
                  nosuspend ->
                      false
              end
      end).

replicate(#data{peers = Peers} = Data) ->
    replicate(Peers, Data).

replicate(Peers, Data) when is_list(Peers) ->
    replicate_to_peers(Peers, Data);
replicate(Peer, Data) when is_atom(Peer) ->
    replicate([Peer], Data).

replicate_to_peers(Peers, Data) ->
    {NewData, NotSent} = send_append(Peers, Data),
    BusyPeers = [Peer || {Peer, busy} <- NotSent],
    CatchupPeers = [{Peer, PeerSeqno} ||
                       {Peer, {need_catchup, PeerSeqno}} <- NotSent],

    log_busy_peers(append, BusyPeers),
    catchup_peers(CatchupPeers, NewData).

should_replicate_to(Peer, HighSeqno, CommitSeqno, MaxInflight, Data) ->
    case get_peer_status(Peer, Data) of
        {ok, #peer_status{acked_seqno = AckedSeqno,
                          sent_seqno = PeerSentSeqno,
                          sent_commit_seqno = PeerSentCommitSeqno,
                          state = active}} ->
            InFlight = (PeerSentSeqno - AckedSeqno),
            Credit = MaxInflight - InFlight,
            DoSync =
                Credit > 0
                andalso (HighSeqno > PeerSentSeqno
                         orelse CommitSeqno > PeerSentCommitSeqno),

            case DoSync of
                true ->
                    {true, PeerSentSeqno, Credit};
                false ->
                    false
            end;
        _ ->
            false
    end.

send_append(Peers, #data{history_id = HistoryId,
                         term = Term,
                         high_seqno = HighSeqno,
                         committed_seqno = CommittedSeqno} = Data) ->
    MaxInflight = ?MAX_INFLIGHT,
    maybe_send_requests(
      Peers, Data,
      fun (Peer, PeerRef, ServerRef) ->
              case should_replicate_to(Peer, HighSeqno, CommittedSeqno,
                                       MaxInflight, Data) of
                  {true, PeerSeqno, PeerCredit} ->
                      send_append_to_peer(Peer, PeerCredit, PeerRef, PeerSeqno,
                                          ServerRef, HistoryId, Term, Data);
                  false ->
                      %% Nothing to be sent, so exclude from "not sent" list.
                      true
              end
      end).

send_append_to_peer(Peer, PeerCredit, PeerRef, PeerSeqno,
                    ServerRef, HistoryId, Term, Data) ->
    case get_entries(PeerSeqno, PeerCredit, Data) of
        {ok, AtTerm, CommittedSeqno, HighSeqno, Entries} ->
            Request = {append, CommittedSeqno, HighSeqno},
            Opaque = make_agent_opaque(PeerRef, Peer, Request),
            case chronicle_agent:append(ServerRef, Opaque, HistoryId,
                                        Term, CommittedSeqno,
                                        AtTerm, PeerSeqno, Entries,
                                        [nosuspend]) of
                ok ->
                    set_peer_sent_seqnos(Peer, HighSeqno,
                                         CommittedSeqno, Data),
                    true;
                nosuspend ->
                    {false, busy}
            end;
        need_catchup ->
            {false, {need_catchup, PeerSeqno}}
    end.

catchup_peers(Peers, #data{catchup_pid = Pid} = Data) ->
    %% TODO: demonitor_agents() is needed to make sure that if there are any
    %% outstanding requests to the peers, we'll ignore their responses if we
    %% wind up receiving them. Consider doing something cleaner than this.
    JustPeers = [Peer || {Peer, _} <- Peers],
    NewData = monitor_agents(JustPeers, demonitor_agents(JustPeers, Data)),
    lists:foreach(
      fun ({Peer, PeerSeqno}) ->
              set_peer_catchup(Peer, NewData),

              {ok, Ref} = get_peer_monitor(Peer, NewData),
              Opaque = make_agent_opaque(Ref, Peer, catchup),

              ?DEBUG("Catching up peer ~w from seqno ~b", [Peer, PeerSeqno]),
              chronicle_catchup:catchup_peer(Pid, Opaque, Peer, PeerSeqno)
      end, Peers),

    NewData.

maybe_cancel_peer_catchup(Peer, #data{catchup_pid = Pid} = Data) ->
    case get_peer_status(Peer, Data) of
        {ok, #peer_status{state = catchup}} ->
            chronicle_catchup:cancel_catchup(Pid, Peer);
        _ ->
            ok
    end.

get_entries(PeerSeqno, _, _Data) when PeerSeqno =:= ?NO_SEQNO ->
    need_catchup;
get_entries(PeerSeqno, MaxEntries, Data) ->
    case get_term_for_seqno(PeerSeqno, Data) of
        {ok, Term} ->
            get_entries_with_term(PeerSeqno, MaxEntries, Term, Data);
        {error, compacted} ->
            need_catchup
    end.

get_entries_with_term(PeerSeqno, MaxEntries, Term,
                      #data{high_seqno = HighSeqno} = Data) ->
    StartSeqno = PeerSeqno + 1,
    EndSeqno = min(PeerSeqno + MaxEntries, HighSeqno),
    CommittedSeqno = min(EndSeqno, Data#data.committed_seqno),

    case StartSeqno =< EndSeqno of
        true ->
            case do_get_entries(StartSeqno, EndSeqno, Data) of
                {ok, Entries} ->
                    {ok, Term, CommittedSeqno, EndSeqno, Entries};
                need_catchup ->
                    need_catchup
            end;
        false ->
            {ok, Term, CommittedSeqno, EndSeqno, []}
    end.

do_get_entries(StartSeqno, EndSeqno,
               #data{pending_entries = PendingEntries,
                     local_committed_seqno = LocalCommittedSeqno}) ->
    case StartSeqno =< LocalCommittedSeqno of
        true ->
            %% TODO: consider triggerring catchup even if we've got all the
            %% entries to send, but there more than some configured number of
            %% them.
            case EndSeqno =< LocalCommittedSeqno of
                true ->
                    get_local_log(StartSeqno, EndSeqno);
                false ->
                    case get_local_log(StartSeqno, LocalCommittedSeqno) of
                        {ok, BackfillEntries} ->
                            QueuedEntries =
                                get_entries_from_queue(LocalCommittedSeqno + 1,
                                                       EndSeqno,
                                                       PendingEntries),

                            {ok, BackfillEntries ++ QueuedEntries};
                        need_catchup ->
                            need_catchup
                    end
            end;
        false ->
            {ok, get_entries_from_queue(StartSeqno, EndSeqno, PendingEntries)}
    end.

%% TODO: consider using a different data structure for pending entries
get_entries_from_queue(StartSeqno, EndSeqno, Q) ->
    true = (EndSeqno >= StartSeqno),
    get_entries_from_queue_drop_tail(StartSeqno, EndSeqno, Q).

get_entries_from_queue_drop_tail(StartSeqno, EndSeqno, Q) ->
    {{value, Entry}, NewQ} = queue:out_r(Q),
    EntrySeqno = Entry#log_entry.seqno,
    case EntrySeqno > EndSeqno of
        true ->
            get_entries_from_queue_drop_tail(StartSeqno, EndSeqno, NewQ);
        false ->
            get_entries_from_queue_acc(StartSeqno, NewQ, [Entry])
    end.

get_entries_from_queue_acc(StartSeqno, Q, Acc) ->
    case queue:out_r(Q) of
        {{value, Entry}, NewQ} ->
            case Entry#log_entry.seqno >= StartSeqno of
                true ->
                    get_entries_from_queue_acc(StartSeqno, NewQ, [Entry|Acc]);
                false ->
                    Acc
            end;
        {empty, _} ->
            Acc
    end.

get_term_for_seqno(Seqno, #data{term = Term,
                                local_committed_seqno = LocalCommittedSeqno,
                                safe_commit_seqno = SafeCommitSeqno} = Data) ->
    case Seqno >= SafeCommitSeqno of
        true ->
            %% Should be the most common case when we are replicating entries
            %% from current term.
            {ok, Term};
        false ->
            case Seqno =< LocalCommittedSeqno of
                true ->
                    chronicle_agent:get_term_for_seqno(Seqno);
                false ->
                    get_term_for_seqno_pending(Seqno, Data)
            end
    end.

get_term_for_seqno_pending(Seqno, #data{pending_entries = PendingEntries}) ->
    Tail = chronicle_utils:queue_dropwhile(
             fun (Entry) ->
                     Entry#log_entry.seqno < Seqno
             end, PendingEntries),

    %% We must always find an entry.
    {value, Entry} = queue:peek(Tail),
    true = (Entry#log_entry.seqno =:= Seqno),

    {ok, Entry#log_entry.term}.

get_local_log(StartSeqno, EndSeqno) ->
    case chronicle_agent:get_log_committed(StartSeqno, EndSeqno) of
        {ok, _} = Ok ->
            Ok;
        {error, compacted} ->
            need_catchup
    end.

send_ensure_term(Peers, Request,
                 #data{history_id = HistoryId, term = Term} = Data) ->
    maybe_send_requests(
      Peers, Data,
      fun (Peer, PeerRef, ServerRef) ->
              Opaque = make_agent_opaque(PeerRef, Peer, Request),
              case chronicle_agent:ensure_term(ServerRef,
                                               Opaque, HistoryId, Term,
                                               [nosuspend]) of
                  ok ->
                      true;
                  nosuspend ->
                      false
              end
      end).

send_heartbeat(#data{quorum_peers = QuorumPeers} = Data) ->
    send_heartbeat(QuorumPeers, Data).

send_heartbeat(Peers, #data{sync_round = Round} = Data) when is_list(Peers) ->
    {NewData, BusyPeers} = send_ensure_term(Peers, {heartbeat, Round}, Data),
    SentPeers = Peers -- BusyPeers,
    log_busy_peers(heartbeat, BusyPeers),

    lists:foreach(
      fun (Peer) ->
              maybe_set_peer_status_requested(Peer, NewData),
              set_peer_sent_sync_round(Peer, Round, Data)
      end, SentPeers),

    NewData;
send_heartbeat(Peer, Data) when is_atom(Peer) ->
    send_heartbeat([Peer], Data).

log_busy_peers(Op, BusyPeers) ->
    case BusyPeers of
        [] ->
            ok;
        _ ->
            ?WARNING("Didn't send ~w request to some peers due "
                     "to distribution connection being busy.~n"
                     "Peers: ~w",
                     [Op, BusyPeers])
    end.

reply_request(ReplyTo, Reply) ->
    chronicle_server:reply_request(ReplyTo, Reply).

monitor_agents(Peers,
               #data{peer = Self,
                     monitors_peers = MPeers, monitors_refs = MRefs} = Data) ->
    {NewMPeers, NewMRefs} =
        lists:foldl(
          fun (Peer, {AccMPeers, AccMRefs} = Acc) ->
                  case maps:is_key(Peer, AccMPeers) of
                      true ->
                          %% already monitoring
                          Acc;
                      false ->
                          ServerRef = chronicle_agent:server_ref(Peer, Self),
                          MRef = chronicle_agent:monitor(ServerRef),
                          {AccMPeers#{Peer => MRef}, AccMRefs#{MRef => Peer}}
                  end
          end, {MPeers, MRefs}, Peers),

    Data#data{monitors_peers = NewMPeers, monitors_refs = NewMRefs}.

demonitor_agents(Peers,
                 #data{monitors_peers = MPeers, monitors_refs = MRefs} =
                     Data) ->
    {NewMPeers, NewMRefs} =
        lists:foldl(
          fun (Peer, {AccMPeers, AccMRefs} = Acc) ->
                  case maps:take(Peer, AccMPeers) of
                      {MRef, NewAccMPeers} ->
                          erlang:demonitor(MRef, [flush]),
                          {NewAccMPeers, maps:remove(MRef, AccMRefs)};
                      error ->
                          Acc
                  end
          end, {MPeers, MRefs}, Peers),

    Data#data{monitors_peers = NewMPeers, monitors_refs = NewMRefs}.

take_monitor(MRef,
             #data{monitors_peers = MPeers, monitors_refs = MRefs} = Data) ->
    case maps:take(MRef, MRefs) of
        {Peer, NewMRefs} ->
            NewMPeers = maps:remove(Peer, MPeers),
            {ok, Peer, Data#data{monitors_peers = NewMPeers,
                                 monitors_refs = NewMRefs}};
        error ->
            not_found
    end.

get_peer_monitor(Peer, #data{monitors_peers = MPeers}) ->
    case maps:find(Peer, MPeers) of
        {ok, _} = Ok ->
            Ok;
        error ->
            not_found
    end.

deduce_acked_sync_round(#data{quorum = Quorum, quorum_peers = Peers} = Data) ->
    PeerRounds =
        lists:filtermap(
          fun (Peer) ->
                  case get_peer_status(Peer, Data) of
                      {ok, #peer_status{acked_sync_round = Round}} ->
                          {true, {Peer, Round}};
                      not_found ->
                          false
                  end
          end, Peers),

    deduce_quorum_value(PeerRounds, 0, Quorum).

deduce_quorum_seqno(#data{quorum = Quorum,
                          quorum_peers = Peers} = Data) ->
    PeerSeqnos =
        lists:filtermap(
          fun (Peer) ->
                  case get_peer_status(Peer, Data) of
                      {ok, #peer_status{acked_seqno = Seqno,
                                        state = active}} ->
                          %% Note, that peers in catchup are ignored. They are
                          %% far behind and can't contribute anything useful.
                          {true, {Peer, Seqno}};
                      _ ->
                          false
                  end
          end, Peers),

    deduce_quorum_value(PeerSeqnos, ?NO_SEQNO, Quorum).

deduce_quorum_value(PeerValues0, Default, Quorum) ->
    PeerValues =
        %% Order peers in the decreasing order of their seqnos.
        lists:sort(fun ({_PeerA, ValueA}, {_PeerB, ValueB}) ->
                           ValueA >= ValueB
                   end, PeerValues0),

    deduce_quorum_value_loop(PeerValues, Default, Quorum, sets:new()).

deduce_quorum_value_loop([], Default, _Quroum, _Votes) ->
    Default;
deduce_quorum_value_loop([{Peer, Value} | Rest], Default, Quorum, Votes) ->
    NewVotes = sets:add_element(Peer, Votes),
    case have_quorum(NewVotes, Quorum) of
        true ->
            Value;
        false ->
            deduce_quorum_value_loop(Rest, Default, Quorum, NewVotes)
    end.

-ifdef(TEST).
deduce_quorum_value_test() ->
    Deduce = fun (PeerValues, Quorum) ->
                     deduce_quorum_value(PeerValues, 0, Quorum)
             end,

    Peers = [a, b, c, d, e],
    Quorum = {joint,
              {all, sets:from_list([a])},
              {majority, sets:from_list(Peers)}},

    ?assertEqual(0, Deduce([], Quorum)),
    ?assertEqual(0, Deduce([{a, 1}, {b, 3}], Quorum)),
    ?assertEqual(1, Deduce([{a, 1}, {b, 1}, {c, 3}, {d, 1}, {e, 2}], Quorum)),
    ?assertEqual(1, Deduce([{a, 1}, {b, 1}, {c, 3}, {d, 3}, {e, 2}], Quorum)),
    ?assertEqual(2, Deduce([{a, 2}, {b, 1}, {c, 3}, {d, 3}, {e, 2}], Quorum)),
    ?assertEqual(1, Deduce([{a, 1}, {b, 3}, {c, 3}, {d, 3}, {e, 2}], Quorum)),
    ?assertEqual(3, Deduce([{a, 3}, {b, 3}, {c, 3}, {d, 3}, {e, 2}], Quorum)),

    NewPeers = [a, b, c],
    JointQuorum = {joint,
                   {all, sets:from_list([a])},
                   {joint,
                    {majority, sets:from_list(Peers)},
                    {majority, sets:from_list(NewPeers)}}},

    ?assertEqual(0, Deduce([{c, 1}, {d, 1}, {e, 1}], JointQuorum)),
    ?assertEqual(1, Deduce([{a, 1}, {b, 1},
                            {c, 2}, {d, 2}, {e, 2}], JointQuorum)),
    ?assertEqual(1, Deduce([{a, 2}, {b, 2},
                            {c, 1}, {d, 1}, {e, 1}], JointQuorum)),
    ?assertEqual(1, Deduce([{a, 1}, {b, 2}, {c, 2},
                            {d, 3}, {e, 1}], JointQuorum)),
    ?assertEqual(2, Deduce([{a, 2}, {b, 2}, {c, 1},
                            {d, 3}, {e, 1}], JointQuorum)).
-endif.

stop(Reason, State, Data) ->
    stop(Reason, [], State, Data).

stop(Reason, ExtraEffects, State,
     #data{parent = Pid,
           peers = Peers,
           config_change_reply_to = ConfigReplyTo} = Data)
  when State =:= establish_term;
       State =:= proposing ->
    {NewData0, Effects} = unpostpone_config_requests(Data),

    %% Demonitor all agents so we don't process any more requests from them.
    NewData1 = demonitor_agents(Peers, NewData0),

    %% Reply to all in-flight sync_quorum requests
    NewData2 = sync_quorum_reply_not_leader(NewData1),

    case ConfigReplyTo of
        undefined ->
            ok;
        _ ->
            reply_request(ConfigReplyTo, {error, {leader_error, leader_lost}})
    end,

    NewData3 =
        case State =:= proposing of
            true ->
                %% Make an attempt to notify local agent about the latest
                %% committed seqno, so chronicle_rsm-s can reply to clients
                %% whose commands got committed.
                %%
                %% But this can be and needs to be done only if we've
                %% established the term on a quorum of nodes (that is, our
                %% state is 'proposing').
                sync_local_agent(NewData2),
                stop_catchup_process(NewData2);
            false ->
                NewData2
        end,

    chronicle_server:proposer_stopping(Pid, Reason),
    {next_state, {stopped, Reason}, NewData3, Effects ++ ExtraEffects};
stop(_Reason, ExtraEffects, {stopped, _}, Data) ->
    {keep_state, Data, ExtraEffects}.

sync_local_agent(#data{history_id = HistoryId,
                       term = Term,
                       committed_seqno = CommittedSeqno}) ->
    Result =
        (catch chronicle_agent:local_mark_committed(HistoryId,
                                                    Term, CommittedSeqno)),
    case Result of
        ok ->
            ok;
        Other ->
            ?DEBUG("Failed to synchronize with local agent.~n"
                   "History id: ~p~n"
                   "Term: ~w~n"
                   "Committed seqno: ~b~n"
                   "Error: ~w",
                   [HistoryId, Term, CommittedSeqno, Other])
    end.

reply_not_leader(ReplyTo) ->
    reply_request(ReplyTo, {error, {leader_error, not_leader}}).

require_self_quorum(Quorum, #data{peer = Self}) ->
    {joint, {all, sets:from_list([Self])}, Quorum}.

require_self_peer(Peers, #data{peer = Self}) ->
    lists:usort([Self | Peers]).

get_append_quorum(#log_entry{value = Config}) ->
    chronicle_config:get_quorum(Config).

config_peers(#log_entry{value = Config}) ->
    chronicle_config:get_peers(Config).

sanitize_data(#data{pending_entries = PendingQ} = Data) ->
    Pending = queue:to_list(PendingQ),
    Sanitized = chronicle_utils:sanitize_entries(Pending),

    Data#data{pending_entries = {sanitized,
                                 length(Pending),
                                 Sanitized}}.
