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

-import(chronicle_utils, [get_position/1]).

-define(SERVER, ?SERVER_NAME(?MODULE)).

%% TODO: move these to the config
-define(ESTABLISH_TERM_TIMEOUT, 10000).

-record(data, { parent,

                %% TODO: reconsider what's needed and what's not needed here
                history_id,
                term,
                quorum,
                peers,
                config,
                config_revision,
                high_seqno,
                committed_seqno,

                peer_statuses,
                monitors,

                %% Used only when the state is 'establish_term'.
                %% TODO: consider using different data records for
                %% establish_term and proposing states
                votes,
                failed_votes,
                branch,

                %% Used when the state is 'proposing'.
                pending_entries,
                pending_high_seqno,
                pending_froms,
                sync_requests}).

-record(peer_status, { peer,
                       acked_seqno,
                       sent_seqno,
                       sent_commit_seqno }).

-record(sync_request, { ref,
                        votes,
                        failed_votes }).

start_link(HistoryId, Term) ->
    Self = self(),
    gen_statem:start_link(?START_NAME(?MODULE),
                          ?MODULE, [Self, HistoryId, Term], []).

deliver_commands(Pid, Commands) ->
    gen_statem:cast(Pid, {deliver_commands, Commands}).

%% gen_statem callbacks
callback_mode() ->
    [handle_event_function, state_enter].

init([Parent, HistoryId, Term]) ->
    chronicle_peers:monitor(),

    PeerStatuses = ets:new(peer_statuses,
                           [protected, set, {keypos, #peer_status.peer}]),
    SyncRequests = ets:new(sync_requests,
                           [protected, set, {keypos, #sync_request.ref}]),
    Data = #data{ parent = Parent,
                  history_id = HistoryId,
                  term = Term,
                  peer_statuses = PeerStatuses,
                  monitors = #{},
                  %% TODO: store votes, failed_votes and peers as sets
                  votes = [],
                  failed_votes = [],
                  pending_entries = queue:new(),
                  pending_froms = queue:new(),
                  sync_requests = SyncRequests},

    {ok, establish_term, Data}.

handle_event(enter, _OldState, NewState, Data) ->
    handle_state_enter(NewState, Data);
handle_event(state_timeout, establish_term_timeout, State, Data) ->
    handle_establish_term_timeout(State, Data);
handle_event(info, {{agent_response, Ref, Peer, Request}, Result}, State,
             #data{peers = Peers} = Data) ->
    case lists:member(Peer, Peers) of
        true ->
            case get_peer_monitor(Peer, Data) of
                {ok, OurRef} when OurRef =:= Ref ->
                    handle_agent_response(Peer, Request, Result, State, Data);
                _ ->
                    ?DEBUG("Ignoring a stale response from peer ~p.~n"
                           "Request:~n~p",
                           [Peer, Request]),
                    keep_state_and_data
            end;
        false ->
            ?INFO("Ignoring a response from a removed peer ~p.~n"
                  "Peers:~n~p~n"
                  "Request:~n~p",
                  [Peer, Peers, Request]),
            keep_state_and_data
    end;
handle_event(info, {nodeup, Peer, Info}, State, Data) ->
    handle_nodeup(Peer, Info, State, Data);
handle_event(info, {nodedown, Peer, Info}, State, Data) ->
    handle_nodedown(Peer, Info, State, Data);
handle_event(info, {'DOWN', MRef, process, Pid, Reason}, State, Data) ->
    handle_down(MRef, Pid, Reason, State, Data);
handle_event(cast, {deliver_commands, Commands}, State, Data) ->
    handle_deliver_commands(Commands, State, Data);
handle_event({call, From}, _Call, _State, _Data) ->
    {keep_state_and_data, [{reply, From, nack}]};
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event ~p", [{Type, Event}]),
    keep_state_and_data.

%% internal
handle_state_enter(establish_term,
                   #data{history_id = HistoryId, term = Term} = Data) ->
    {ok, Metadata} = chronicle_agent:get_metadata(),
    CurrentHistoryId = chronicle_agent:get_history_id(Metadata),
    case CurrentHistoryId =:= HistoryId of
        true ->
            Peers = get_establish_peers(Metadata),
            Quorum = get_establish_quorum(Metadata),
            LivePeers = chronicle_peers:get_live_peers(Peers),
            DeadPeers = Peers -- LivePeers,

            ?DEBUG("Going to establish term ~p (history id ~p).~n"
                   "Metadata:~n~p~n"
                   "Live peers:~n~p",
                   [Term, HistoryId, Metadata, LivePeers]),

            #metadata{config = Config,
                      config_revision = ConfigRevision,
                      high_seqno = HighSeqno,
                      committed_seqno = CommittedSeqno,
                      pending_branch = PendingBranch} = Metadata,

            case is_quorum_feasible(Peers, DeadPeers, Quorum) of
                true ->
                    NewData0 = send_establish_term(LivePeers, Metadata, Data),
                    NewData = NewData0#data{peers = Peers,
                                            quorum = Quorum,
                                            votes = [],
                                            failed_votes = DeadPeers,
                                            config = Config,
                                            config_revision = ConfigRevision,
                                            high_seqno = HighSeqno,
                                            pending_high_seqno = HighSeqno,
                                            committed_seqno = CommittedSeqno,
                                            branch = PendingBranch},
                    {keep_state,
                     NewData,
                     {state_timeout,
                      ?ESTABLISH_TERM_TIMEOUT, establish_term_timeout}};
                false ->
                    %% This should be a rare situation. That's because to be
                    %% elected a leader we need to get a quorum of votes. So
                    %% at least a quorum of nodes should be alive.
                    ?WARNING("Can't establish term ~p, history id ~p.~n"
                             "Not enough peers are alive to achieve quorum.~n"
                             "Peers: ~p~n"
                             "Live peers: ~p~n"
                             "Quorum: ~p",
                             [Term, HistoryId, Peers, LivePeers, Quorum]),
                    {stop, {error, no_quorum}}
            end;
        false ->
            ?DEBUG("History id changed since election. Stepping down.~n"
                   "Election history id: ~p~n"
                   "Current history id: ~p",
                   [HistoryId, CurrentHistoryId]),
            {stop, {history_changed, HistoryId, CurrentHistoryId}}
    end;
handle_state_enter(proposing, Data) ->
    replicate(Data).

handle_establish_term_timeout(establish_term = _State, #data{term = Term}) ->
    ?ERROR("Failed to establish term ~p after ~bms",
           [Term, ?ESTABLISH_TERM_TIMEOUT]),
    {stop, establish_term_timeout}.

handle_agent_response(Peer,
                      {establish_term, _, _, _} = Request,
                      Result, State, Data) ->
    handle_establish_term_result(Peer, Request, Result, State, Data);
handle_agent_response(Peer,
                      {append, _, _, _, _} = Request,
                      Result, State, Data) ->
    handle_append_result(Peer, Request, Result, State, Data);
handle_agent_response(Peer,
                      {sync_quorum, _} = Request,
                      Result, State, Data) ->
    handle_sync_quorum_result(Peer, Request, Result, State, Data).

handle_establish_term_result(Peer,
                             {establish_term, HistoryId, Term, Position},
                             Result, State, Data) ->
    true = (HistoryId =:= Data#data.history_id),
    true = (Term =:= Data#data.term),

    case Result of
        {ok, #metadata{committed_seqno = CommittedSeqno}} ->
            init_peer_status(Peer, CommittedSeqno, Data),
            establish_term_handle_vote(Peer, ok, State, Data);
        {error, Error} ->
            case handle_common_error(Peer, Error, Data) of
                {stop, _} = Stop ->
                    Stop;
                ignored ->
                    ?WARNING("Failed to establish "
                             "term ~p (history id ~p, log position ~p) "
                             "on peer ~p: ~p",
                             [Term, HistoryId, Position, Peer, Error]),
                    establish_term_handle_vote(Peer, failed, State, Data)
            end
    end.

handle_common_error(Peer, Error,
                    #data{history_id = HistoryId, term = Term}) ->
    case Error of
        {conflicting_term, OtherTerm} ->
            ?INFO("Saw term conflict when trying on peer ~p.~n"
                  "History id: ~p~n"
                  "Our term: ~p~n"
                  "Conflicting term: ~p",
                  [Peer, HistoryId, Term, OtherTerm]),
            {stop, {conflicting_term, Term, OtherTerm}};
        {history_mismatch, OtherHistoryId} ->
            ?INFO("Saw history mismatch when trying on peer ~p.~n"
                  "Our history id: ~p~n"
                  "Conflicting history id: ~n",
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

establish_term_handle_vote(_Peer, _Status, proposing, Data) ->
    %% We'are already proposing. So nothing needs to be done.
    {keep_state, Data};
establish_term_handle_vote(Peer, Status, establish_term,
                           #data{votes = Votes,
                                 failed_votes = FailedVotes} = Data) ->
    NewData =
        case Status of
            ok ->
                Data#data{votes = [Peer | Votes]};
            failed ->
                Data#data{failed_votes = [Peer | FailedVotes]}
        end,

    establish_term_maybe_transition(NewData).

establish_term_maybe_transition(#data{term = Term,
                                      history_id = HistoryId,
                                      peers = Peers,
                                      votes = Votes,
                                      failed_votes = FailedVotes,
                                      quorum = Quorum} = Data) ->
    case have_quorum(Votes, Quorum) of
        true ->
            ?DEBUG("Established term ~p (history id ~p) successfully.~n"
                   "Votes: ~p~n",
                   [Term, HistoryId, Votes]),

            NewData = maybe_resolve_branch(Data),
            {next_state, proposing, NewData};
        false ->
            case is_quorum_feasible(Peers, FailedVotes, Quorum) of
                true ->
                    {keep_state, Data};
                false ->
                    ?WARNING("Couldn't establish term ~p, history id ~p.~n"
                             "Votes received: ~p~n"
                             "Quorum: ~p~n",
                             [Term, HistoryId, Votes, Quorum]),
                    {stop, {error, no_quorum}}
            end
    end.

maybe_resolve_branch(#data{branch = undefined} = Data) ->
    Data;
maybe_resolve_branch(#data{branch = Branch,
                           config = Config} = Data) ->
    NewConfig = Config#config{voters = Branch#branch.peers},

    ?INFO("New config after resolving a branch.~n"
          "Branch:~n~p~n"
          "Latest known config:~n~p~n"
          "New config:~n~p",
          [Branch, Config, NewConfig]),

    force_propose_config(NewConfig, Data#data{branch = undefined}).

handle_append_result(Peer, Request, Result, proposing, Data) ->
    {append, HistoryId, Term, CommittedSeqno, HighSeqno} = Request,

    true = (HistoryId =:= Data#data.history_id),
    true = (Term =:= Data#data.term),

    case Result of
        ok ->
            handle_append_ok(Peer, HighSeqno, CommittedSeqno, Data);
        {error, Error} ->
            case handle_common_error(Peer, Error, Data) of
                {stop, _} = Stop ->
                    Stop;
                ignored ->
                    ?WARNING("Append failed on peer ~p: ~p", [Peer, Error]),
                    %% TODO: handle this, specifically, handle missing_entries
                    %% error
                    {stop, Error}
            end
    end.

handle_append_ok(Peer, PeerHighSeqno, PeerCommittedSeqno,
                 #data{committed_seqno = CommittedSeqno,
                       pending_entries = PendingEntries} = Data) ->
    ?DEBUG("Append ok on peer ~p.~n"
           "High Seqno: ~p~n"
           "Committed Seqno: ~p",
           [Peer, PeerHighSeqno, PeerCommittedSeqno]),
    set_peer_acked_seqno(Peer, PeerHighSeqno, Data),

    case deduce_committed_seqno(Data) of
        {ok, NewCommittedSeqno}
          when NewCommittedSeqno > CommittedSeqno ->
            ?DEBUG("Committed seqno advanced.~n"
                   "New committed seqno: ~p~n"
                   "Old committed seqno: ~p",
                   [NewCommittedSeqno, CommittedSeqno]),
            NewPendingEntries =
                queue_dropwhile(
                  fun (Entry) ->
                          Entry#log_entry.seqno =< NewCommittedSeqno
                  end, PendingEntries),

            NewData =
                reply_committed(
                  Data#data{committed_seqno = NewCommittedSeqno,
                            high_seqno = NewCommittedSeqno,
                            pending_entries = NewPendingEntries}),

            replicate(NewData);
        {ok, NewCommittedSeqno} ->
            true = (NewCommittedSeqno =:= CommittedSeqno),
            keep_state_and_data;
        no_quorum ->
            %% This case is possible because deduce_committed_seqno/1 always
            %% uses the most up-to-date config. So what was committed in the
            %% old config, might not yet have a quorum in the current
            %% configuration.
            keep_state_and_data
    end.

handle_sync_quorum_result(Peer, {sync_quorum, Ref}, Result, proposing,
                          #data{sync_requests = SyncRequests} = Data) ->
    ?DEBUG("Sync quorum response from ~p: ~p", [Peer, Result]),
    case ets:lookup(SyncRequests, Ref) of
        [] ->
            keep_state_and_data;
        [#sync_request{} = Request] ->
            case Result of
                {ok, _} ->
                    sync_quorum_handle_vote(Peer, ok, Request, Data),
                    keep_state_and_data;
                {error, Error} ->
                    case handle_common_error(Peer, Error, Data) of
                        {stop, _} = Stop ->
                            Stop;
                        ignored ->
                            sync_quorum_handle_vote(Peer,
                                                    failed, Request, Data),
                            keep_state_and_data
                    end
            end
    end.

sync_quorum_handle_vote(Peer, Status,
                        #sync_request{ref = Ref,
                                      votes = Votes,
                                      failed_votes = FailedVotes} = Request,
                        #data{sync_requests = Requests} = Data) ->
    NewRequest =
        case Status of
            ok ->
                Request#sync_request{votes = [Peer | Votes]};
            failed ->
                Request#sync_request{failed_votes = [Peer | FailedVotes]}
        end,

    case sync_quorum_maybe_reply(NewRequest, Data) of
        continue ->
            ets:insert(Requests, NewRequest);
        done ->
            ets:delete(Requests, Ref)
    end.

sync_quorum_maybe_reply(Request, Data) ->
    case sync_quorum_check_result(Request, Data) of
        continue ->
            continue;
        Result ->
            reply_command(Request#sync_request.ref, Result, Data),
            done
    end.

sync_quorum_check_result(#sync_request{votes = Votes,
                                       failed_votes = FailedVotes},
                         #data{quorum = Quorum, peers = Peers}) ->
    case have_quorum(Votes, Quorum) of
        true ->
            ok;
        false ->
            case is_quorum_feasible(Peers, FailedVotes, Quorum) of
                true ->
                    continue;
                false ->
                    {error, no_quorum}
            end
    end.

sync_quorum_handle_peer_down(Peer, #data{sync_requests = Tab} = Data) ->
    lists:foreach(
      fun (#sync_request{votes = Votes,
                         failed_votes = FailedVotes} = Request) ->
              HasVoted = lists:member(Peer, Votes)
                  orelse lists:member(Peer, FailedVotes),

              case HasVoted of
                  true ->
                      ok;
                  false ->
                      sync_quorum_handle_vote(Peer, failed, Request, Data)
              end
      end, ets:tab2list(Tab)).

sync_quorum_on_config_update(AddedPeers, #data{sync_requests = Tab} = Data) ->
    lists:foldl(
      fun (#sync_request{ref = Ref} = Request, AccData) ->
              %% We might have a quorum in the new configuration. If that's
              %% the case, reply to the request immediately.
              case sync_quorum_maybe_reply(Request, AccData) of
                  done ->
                      ets:delete(Tab, Ref),
                      AccData;
                  continue ->
                      %% If there are new peers, we need to send extra
                      %% ensure_term requests to them. Otherwise, we might not
                      %% ever get enough responses to reach quorum.
                      send_ensure_term(AddedPeers, {sync_quorum, Ref}, AccData)
              end
      end, Data, ets:tab2list(Tab)).

maybe_complete_config_transition(#data{config = Config} = Data) ->
    case Config of
        #config{} ->
            Data;
        #transition{future_config = FutureConfig} ->
            case is_config_committed(Data) of
                true ->
                    propose_config(FutureConfig, Data);
                false ->
                    Data
            end
    end.

is_config_committed(#data{config_revision = {_, _, ConfigSeqno},
                          committed_seqno = CommittedSeqno}) ->
    ConfigSeqno =< CommittedSeqno.

replicate(Data0) ->
    Data = maybe_complete_config_transition(Data0),
    #data{committed_seqno = CommittedSeqno,
          pending_high_seqno = HighSeqno} = Data,

    case get_peers_to_replicate(HighSeqno, CommittedSeqno, Data) of
        [] ->
            {keep_state, Data};
        Peers ->
            {keep_state, send_append(Peers, Data)}
    end.

get_peers_to_replicate(HighSeqno, CommitSeqno, #data{peers = Peers} = Data) ->
    LivePeers = chronicle_peers:get_live_peers(Peers),

    lists:filtermap(
      fun (Peer) ->
              case get_peer_status(Peer, Data) of
                  {ok, #peer_status{sent_seqno = PeerSentSeqno,
                                    sent_commit_seqno = PeerSentCommitSeqno}} ->
                      case HighSeqno > PeerSentSeqno orelse
                          CommitSeqno > PeerSentCommitSeqno of
                          true ->
                              {true, {Peer, PeerSentSeqno}};
                          false ->
                              false
                      end;
                  not_found ->
                      %% TODO: should request peers position instead of
                      %% blindly replicating everything
                      {true, {Peer, ?NO_SEQNO}}
              end
      end, LivePeers).

config_peers(#config{voters = Voters}) ->
    Voters;
config_peers(#transition{current_config = Current,
                         future_config = Future}) ->
    lists:usort(config_peers(Current) ++ config_peers(Future)).

get_quorum(Config) ->
    {joint,
     %% Include local agent in all quorums.
     {all, sets:from_list([?PEER()])},
     do_get_quorum(Config)}.

do_get_quorum(#config{voters = Voters}) ->
    {majority, sets:from_list(Voters)};
do_get_quorum(#transition{current_config = Current, future_config = Future}) ->
    {joint, do_get_quorum(Current), do_get_quorum(Future)}.

have_quorum(AllVotes, Quorum)
  when is_list(AllVotes) ->
    do_have_quorum(sets:from_list(AllVotes), Quorum);
have_quorum(AllVotes, Quorum) ->
    do_have_quorum(AllVotes, Quorum).

do_have_quorum(AllVotes, {joint, Quorum1, Quorum2}) ->
    do_have_quorum(AllVotes, Quorum1) andalso do_have_quorum(AllVotes, Quorum2);
do_have_quorum(AllVotes, {all, QuorumNodes}) ->
    MissingVotes = sets:subtract(QuorumNodes, AllVotes),
    sets:size(MissingVotes) =:= 0;
do_have_quorum(AllVotes, {majority, QuorumNodes}) ->
    Votes = sets:intersection(AllVotes, QuorumNodes),
    sets:size(Votes) * 2 > sets:size(QuorumNodes).

is_quorum_feasible(Peers, FailedVotes, Quorum) ->
    PossibleVotes = Peers -- FailedVotes,
    have_quorum(PossibleVotes, Quorum).

%% TODO: find a better place for the following functions
get_establish_quorum(Metadata) ->
    case Metadata#metadata.pending_branch of
        undefined ->
            get_quorum(Metadata#metadata.config);
        #branch{peers = BranchPeers} ->
            {all, sets:from_list(BranchPeers)}
    end.

get_establish_peers(Metadata) ->
    case Metadata#metadata.pending_branch of
        undefined ->
            config_peers(Metadata#metadata.config);
        #branch{peers = BranchPeers} ->
            BranchPeers
    end.

handle_nodeup(Peer, _Info, State, Data) ->
    ?INFO("Peer ~p came up", [Peer]),
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
        proposing ->
            replicate(Data)
    end.

handle_nodedown(Peer, Info, _State, _Data) ->
    %% If there was an outstanding request, we'll also receive a DOWN message
    %% and handle everything there. Otherwise, we don't care.
    ?INFO("Peer ~p went down: ~p", [Peer, Info]),
    keep_state_and_data.

handle_down(MRef, Pid, Reason, State, Data) ->
    {ok, Peer, NewData} = take_monitor(MRef, Data),
    ?INFO("Observed agent ~p on peer ~p "
          "go down with reason ~p", [Pid, Peer, Reason]),

    case Peer =:= ?PEER() of
        true ->
            ?ERROR("Terminating proposer because local "
                   "agent ~p terminated with reason ~p",
                   [Pid, Reason]),
            {stop, {agent_terminated, Reason}};
        false ->
            case State of
                establish_term ->
                    establish_term_handle_vote(Peer, failed, State, NewData);
                proposing ->
                    reset_peer_status(Peer, NewData),
                    {keep_state, NewData}
            end
    end.

handle_deliver_commands(Commands0, proposing,
                        #data{pending_high_seqno = PendingHighSeqno,
                              pending_froms = PendingFroms,
                              pending_entries = PendingEntries} = Data0) ->
    %% TODO: rework this
    {Syncs, Commands} =
        lists:partition(
          fun ({_, Command}) ->
                  Command =:= sync_quorum
          end, Commands0),

    Data = lists:foldl(
             fun ({Ref, sync_quorum}, AccData) ->
                     handle_sync_quorum(Ref, AccData)
             end, Data0, Syncs),

    {NewPendingHighSeqno, NewPendingFroms, NewPendingEntries, NewData0} =
        lists:foldl(
          fun ({From, Command},
               {PrevSeqno, AccFroms, AccEntries, AccData} = Acc) ->
                  Seqno = PrevSeqno + 1,
                  case handle_command(Command, Seqno, AccData) of
                      {ok, LogEntry, NewAccData} ->
                          {Seqno,
                           queue:in({Seqno, From}, AccFroms),
                           queue:in(LogEntry, AccEntries),
                           NewAccData};
                      {reply, Reply} ->
                          reply_command(From, Reply, Data),
                          Acc
                  end
          end,
          {PendingHighSeqno, PendingFroms, PendingEntries, Data}, Commands),

    NewData1 = NewData0#data{pending_entries = NewPendingEntries,
                             pending_high_seqno = NewPendingHighSeqno,
                             pending_froms = NewPendingFroms},

    replicate(NewData1).

handle_command({cas_config, NewConfig, CasRevision}, Seqno, Data) ->
    handle_cas_config(NewConfig, CasRevision, Seqno, Data);
handle_command({rsm_command, RSMName, CommandId, Command}, Seqno, Data) ->
    handle_rsm_command(RSMName, CommandId, Command, Seqno, Data).

handle_cas_config(NewConfig, CasRevision, Seqno,
                  #data{config = Config,
                        config_revision = ConfigRevision} = Data) ->
    %% TODO: this protects against the client proposing transition. But in
    %% reality, it should be solved in some other way
    #config{} = NewConfig,
    case CasRevision =:= ConfigRevision of
        true ->
            %% TODO: need to backfill new nodes
            Transition =
                #transition{current_config = Config,
                            future_config = NewConfig},

            LogEntry = make_log_entry(Seqno, Transition, Data),
            Revision = log_entry_revision(LogEntry),
            NewData = update_config(Transition, Revision, Data),

            {ok, LogEntry, NewData};
        false ->
            {reply, {error, {cas_failed, CasRevision, ConfigRevision}}}
    end.

handle_rsm_command(RSMName, CommandId, Command, Seqno, Data) ->
    %% TODO: check that the given rsm exists
    RSMCommand = #rsm_command{rsm_name = RSMName,
                              id = CommandId,
                              command = Command},
    {ok, make_log_entry(Seqno, RSMCommand, Data), Data}.

handle_sync_quorum(Ref, #data{peers = Peers,
                              sync_requests = SyncRequests} = Data) ->
    %% TODO: timeouts
    LivePeers = chronicle_peers:get_live_peers(Peers),
    DeadPeers = Peers -- LivePeers,

    Request = #sync_request{ref = Ref, votes = [], failed_votes = DeadPeers},
    case sync_quorum_maybe_reply(Request, Data) of
        continue ->
            ets:insert_new(SyncRequests, Request),
            send_ensure_term(LivePeers, {sync_quorum, Ref}, Data);
        done ->
            Data
    end.

make_log_entry(Seqno, Value, #data{history_id = HistoryId, term = Term}) ->
    #log_entry{history_id = HistoryId,
               term = Term,
               seqno = Seqno,
               value = Value}.

update_config(Config, Revision, Data) ->
    NewPeers = config_peers(Config),
    NewData = Data#data{config = Config,
                        config_revision = Revision,
                        peers = NewPeers,
                        quorum = get_quorum(Config)},

    OldPeers = config_peers(Data#data.config),

    RemovedPeers = OldPeers -- NewPeers,
    AddedPeers = NewPeers -- OldPeers,

    handle_added_peers(AddedPeers, handle_removed_peers(RemovedPeers, NewData)).

handle_removed_peers(Peers, Data) ->
    remove_peer_statuses(Peers, Data),
    demonitor_agents(Peers, Data).

handle_added_peers(Peers, Data) ->
    sync_quorum_on_config_update(Peers, Data).

log_entry_revision(#log_entry{history_id = HistoryId,
                              term = Term, seqno = Seqno}) ->
    {HistoryId, Term, Seqno}.

force_propose_config(Config, Data) ->
    %% This function doesn't check that the current config is committed, which
    %% should be the case for regular config transitions. It's only meant to
    %% be used after resolving a branch.
    do_propose_config(Config, Data).

propose_config(Config, Data) ->
    true = is_config_committed(Data),
    do_propose_config(Config, Data).

%% TODO: right now when this function is called we replicate the proposal in
%% its own batch. But it can be coalesced with user batches.
do_propose_config(Config, #data{pending_high_seqno = HighSeqno,
                                pending_entries = Entries} = Data) ->
    Seqno = HighSeqno + 1,
    LogEntry = make_log_entry(Seqno, Config, Data),
    Revision = log_entry_revision(LogEntry),

    NewEntries = queue:in(LogEntry, Entries),
    NewData = Data#data{pending_entries = NewEntries,
                        pending_high_seqno = Seqno},
    update_config(Config, Revision, NewData).

get_peer_status(Peer, #data{peer_statuses = Tab}) ->
    case ets:lookup(Tab, Peer) of
        [#peer_status{} = PeerStatus] ->
            {ok, PeerStatus};
        [] ->
            not_found
    end.

update_peer_status(Peer, Fun, #data{peer_statuses = Tab} = Data) ->
    Current =
        case get_peer_status(Peer, Data) of
            {ok, PeerStatus} ->
                PeerStatus;
            not_found ->
                %% TODO: this shouldn't be needed once peer statuses are
                %% initialized properly
                #peer_status{peer = Peer,
                             acked_seqno = ?NO_SEQNO,
                             sent_seqno = ?NO_SEQNO,
                             sent_commit_seqno = ?NO_SEQNO}
        end,

    ets:insert(Tab, Fun(Current)).

init_peer_status(Peer, Seqno, #data{peer_statuses = Tab}) ->
    true = ets:insert_new(Tab, #peer_status{peer = Peer,
                                            acked_seqno = Seqno,
                                            sent_commit_seqno = Seqno,
                                            sent_seqno = Seqno}).

reset_peer_status(Peer, #data{peer_statuses = Tab} = Data) ->
    case get_peer_status(Peer, Data) of
        {ok, #peer_status{acked_seqno = AckedSeqno} = PeerStatus} ->
            NewPeerStatus =
                PeerStatus#peer_status{sent_seqno = AckedSeqno,
                                       sent_commit_seqno = AckedSeqno},
            ets:insert(Tab, NewPeerStatus);
        not_found ->
            ok
    end.

set_peer_sent_seqnos(Peer, HighSeqno, CommittedSeqno, Data) ->
    update_peer_status(
      Peer,
      fun (#peer_status{acked_seqno = AckedSeqno} = PeerStatus) ->
              true = (HighSeqno >= AckedSeqno),
              true = (HighSeqno >= CommittedSeqno),
              PeerStatus#peer_status{sent_seqno = HighSeqno,
                                     sent_commit_seqno = CommittedSeqno}
      end, Data).

set_peer_acked_seqno(Peer, Seqno, Data) ->
    update_peer_status(
      Peer,
      fun (#peer_status{sent_seqno = SentSeqno} = PeerStatus) ->
              true = (SentSeqno >= Seqno),
              PeerStatus#peer_status{acked_seqno = Seqno}
      end, Data).

remove_peer_statuses(Peers, #data{peer_statuses = Tab}) ->
    lists:foreach(
      fun (Peer) ->
              ets:delete(Tab, Peer)
      end, Peers).

send_requests(Peers, Request, Data, Fun) ->
    NewData = monitor_agents(Peers, Data),
    lists:foreach(
      fun (Peer) ->
              {ok, Ref} = get_peer_monitor(Peer, NewData),
              Opaque = {agent_response, Ref, Peer, Request},
              Fun(Peer, Opaque)
      end, Peers),

    NewData.

send_establish_term(Peers, Metadata,
                    #data{history_id = HistoryId, term = Term} = Data) ->
    Position = get_position(Metadata),
    Request = {establish_term, HistoryId, Term, Position},
    send_requests(
      Peers, Request, Data,
      fun (Peer, Opaque) ->
              ?DEBUG("Sending establish_term request to peer ~p. "
                     "Term = ~p. History Id: ~p. "
                     "Log position: ~p.",
                     [Peer, Term, HistoryId, Position]),

              chronicle_agent:establish_term(Peer, Opaque,
                                             HistoryId, Term, Position)
      end).

send_append(PeersInfo0,
            #data{history_id = HistoryId,
                  term = Term,
                  committed_seqno = CommittedSeqno,
                  pending_high_seqno = HighSeqno} = Data) ->
    Request = {append, HistoryId, Term, CommittedSeqno, HighSeqno},

    PeersInfo = maps:from_list(PeersInfo0),
    Peers = maps:keys(PeersInfo),

    send_requests(
      Peers, Request, Data,
      fun (Peer, Opaque) ->
              PeerSeqno = maps:get(Peer, PeersInfo),
              Entries = get_entries(PeerSeqno, Data),
              set_peer_sent_seqnos(Peer, HighSeqno, CommittedSeqno, Data),
              ?DEBUG("Sending append request to peer ~p.~n"
                     "History Id: ~p~n"
                     "Term: ~p~n"
                     "Committed Seqno: ~p~n"
                     "Entries:~n~p",
                     [Peer, HistoryId, Term, CommittedSeqno, Entries]),

              chronicle_agent:append(Peer, Opaque,
                                     HistoryId, Term, CommittedSeqno, Entries)
      end).

%% TODO: think about how to backfill peers properly
get_entries(Seqno, #data{high_seqno = HighSeqno,
                         pending_entries = PendingEntries} = Data) ->
    BackfillEntries =
        case Seqno < HighSeqno of
            true ->
                get_local_log(Seqno + 1, HighSeqno, Data);
            false ->
                []
        end,

    %% TODO: consider storing pending entries more effitiently, so we don't
    %% have to traverse them here
    Entries =
        queue_dropwhile(
          fun (Entry) ->
                  Entry#log_entry.seqno =< Seqno
          end, PendingEntries),

    BackfillEntries ++ queue:to_list(Entries).

get_local_log(StartSeqno, EndSeqno,
              #data{history_id = HistoryId, term = Term}) ->
    %% TODO: handle errors better
    {ok, Log} = chronicle_agent:get_log(HistoryId, Term, StartSeqno, EndSeqno),
    Log.

send_ensure_term(Peers, Request,
                 #data{history_id = HistoryId, term = Term} = Data) ->
    send_requests(
      Peers, Request, Data,
      fun (Peer, Opaque) ->
              chronicle_agent:ensure_term(Peer, Opaque, HistoryId, Term)
      end).

reply_committed(#data{history_id = HistoryId,
                      term = Term,
                      committed_seqno = CommittedSeqno,
                      pending_froms = Froms} = Data) ->
    {Replies, NewFroms} =
        queue_takefold(
          fun ({FromSeqno, From}, Acc) ->
                  case FromSeqno =< CommittedSeqno of
                      true ->
                          Revision = {HistoryId, Term, FromSeqno},
                          Reply = {ok, Revision},
                          {true, [{From, Reply} | Acc]};
                      false ->
                          false
                  end
          end, [], Froms),

    case Replies of
        [] ->
            Data;
        _ ->
            reply_commands(Replies, Data),
            Data#data{pending_froms = NewFroms}
    end.

queue_takefold(Fun, Acc, Queue) ->
    case queue:out(Queue) of
        {empty, _} ->
            {Acc, Queue};
        {{value, Value}, NewQueue} ->
            case Fun(Value, Acc) of
                {true, NewAcc} ->
                    queue_takefold(Fun, NewAcc, NewQueue);
                false ->
                    {Acc, Queue}
            end
    end.

-ifdef(TEST).
queue_takefold_test() ->
    Q = queue:from_list(lists:seq(1, 10)),
    MkFun = fun (CutOff) ->
                    fun (V, Acc) ->
                            case V =< CutOff of
                                true ->
                                    {true, Acc+V};
                                false ->
                                    false
                            end
                    end
            end,

    Test = fun (ExpectedSum, ExpectedTail, CutOff) ->
                   {Sum, NewQ} = queue_takefold(MkFun(CutOff), 0, Q),
                   ?assertEqual(ExpectedSum, Sum),
                   ?assertEqual(ExpectedTail, queue:to_list(NewQ))
           end,

    Test(0, lists:seq(1,10), 0),
    Test(15, lists:seq(6,10), 5),
    Test(55, [], 42).
-endif.

queue_dropwhile(Pred, Queue) ->
    {_, NewQueue} =
        queue_takefold(
          fun (Value, _) ->
                  case Pred(Value) of
                      true ->
                          {true, unused};
                      false ->
                          false
                  end
          end, unused, Queue),
    NewQueue.

-ifdef(TEST).
queue_dropwhile_test() ->
    Q = queue:from_list(lists:seq(1, 10)),
    Test = fun (Expected, CutOff) ->
                   NewQ = queue_dropwhile(fun (V) -> V =< CutOff end, Q),
                   ?assertEqual(Expected, queue:to_list(NewQ))
           end,
    Test(lists:seq(1,10), 0),
    Test(lists:seq(6,10), 5),
    Test([], 42).
-endif.

reply_command(From, Reply, Data) ->
    reply_commands([{From, Reply}], Data).

reply_commands(Replies, #data{parent = Parent}) ->
    chronicle_server:reply_commands(Parent, Replies).

monitor_agents(Peers, #data{monitors = Monitors} = Data) ->
    NewMonitors =
        lists:foldl(
          fun (Peer, Acc) ->
                  case maps:is_key(Peer, Acc) of
                      true ->
                          %% already monitoring
                          Monitors;
                      false ->
                          MRef = chronicle_agent:monitor(Peer),
                          Acc#{Peer => MRef, MRef => Peer}
                  end
          end, Monitors, Peers),

    Data#data{monitors = NewMonitors}.

demonitor_agents(Peers, #data{monitors = Monitors} = Data) ->
    NewMonitors =
        lists:foldl(
          fun (Peer, Acc) ->
                  case maps:take(Peer, Acc) of
                      {MRef, NewAcc} ->
                          erlang:demonitor(MRef, [flush]),
                          maps:remove(MRef, NewAcc);
                      error ->
                          Acc
                  end
          end, Monitors, Peers),

    Data#data{monitors = NewMonitors}.

take_monitor(MRef, #data{monitors = Monitors} = Data) ->
    case maps:take(MRef, Monitors) of
        {Peer, NewMonitors0} ->
            NewMonitors = maps:remove(Peer, NewMonitors0),
            {ok, Peer, Data#data{monitors = NewMonitors}};
        error ->
            not_found
    end.

get_peer_monitor(Peer, #data{monitors = Monitors}) ->
    case maps:find(Peer, Monitors) of
        {ok, _} = Ok ->
            Ok;
        error ->
            not_found
    end.

deduce_committed_seqno(#data{peers = Peers,
                             quorum = Quorum} = Data) ->
    PeerSeqnos =
        lists:filtermap(
          fun (Peer) ->
                  case get_peer_status(Peer, Data) of
                      {ok, #peer_status{acked_seqno = AckedSeqno}}
                        when AckedSeqno =/= ?NO_SEQNO ->
                          {true, {Peer, AckedSeqno}};
                      _ ->
                          false
                  end
          end, Peers),

    deduce_committed_seqno(PeerSeqnos, Quorum).

deduce_committed_seqno(PeerSeqnos0, Quorum) ->
    PeerSeqnos =
        %% Order peers in the decreasing order of their seqnos.
        lists:sort(fun ({_PeerA, SeqnoA}, {_PeerB, SeqnoB}) ->
                           SeqnoA >= SeqnoB
                   end, PeerSeqnos0),

    deduce_committed_seqno_loop(PeerSeqnos, Quorum, sets:new()).

deduce_committed_seqno_loop([], _Quroum, _Votes) ->
    no_quorum;
deduce_committed_seqno_loop([{Peer, Seqno} | Rest], Quorum, Votes) ->
    NewVotes = sets:add_element(Peer, Votes),
    case have_quorum(NewVotes, Quorum) of
        true ->
            {ok, Seqno};
        false ->
            deduce_committed_seqno_loop(Rest, Quorum, NewVotes)
    end.

-ifdef(TEST).
deduce_committed_seqno_test() ->
    Peers = [a, b, c, d, e],
    Quorum = {joint,
              {all, sets:from_list([a])},
              {majority, sets:from_list(Peers)}},

    ?assertEqual(no_quorum, deduce_committed_seqno([], Quorum)),
    ?assertEqual(no_quorum, deduce_committed_seqno([{a, 1}, {b, 3}], Quorum)),
    ?assertEqual({ok, 1},
                 deduce_committed_seqno([{a, 1}, {b, 1},
                                         {c, 3}, {d, 1}, {e, 2}], Quorum)),
    ?assertEqual({ok, 1},
                 deduce_committed_seqno([{a, 1}, {b, 1},
                                         {c, 3}, {d, 3}, {e, 2}], Quorum)),
    ?assertEqual({ok, 2},
                 deduce_committed_seqno([{a, 2}, {b, 1},
                                         {c, 3}, {d, 3}, {e, 2}], Quorum)),
    ?assertEqual({ok, 1},
                 deduce_committed_seqno([{a, 1}, {b, 3},
                                         {c, 3}, {d, 3}, {e, 2}], Quorum)),
    ?assertEqual({ok, 3},
                 deduce_committed_seqno([{a, 3}, {b, 3},
                                         {c, 3}, {d, 3}, {e, 2}], Quorum)),

    NewPeers = [a, b, c],
    JointQuorum = {joint,
                   {all, sets:from_list([a])},
                   {joint,
                    {majority, sets:from_list(Peers)},
                    {majority, sets:from_list(NewPeers)}}},

    ?assertEqual(no_quorum,
                 deduce_committed_seqno([{c, 1}, {d, 1}, {e, 1}], JointQuorum)),
    ?assertEqual({ok, 1},
                 deduce_committed_seqno([{a, 1}, {b, 1},
                                         {c, 2}, {d, 2}, {e, 2}], JointQuorum)),
    ?assertEqual({ok, 1},
                 deduce_committed_seqno([{a, 2}, {b, 2},
                                         {c, 1}, {d, 1}, {e, 1}], JointQuorum)),
    ?assertEqual({ok, 1},
                 deduce_committed_seqno([{a, 1}, {b, 2}, {c, 2},
                                         {d, 3}, {e, 1}], JointQuorum)),
    ?assertEqual({ok, 2},
                 deduce_committed_seqno([{a, 2}, {b, 2}, {c, 1},
                                         {d, 3}, {e, 1}], JointQuorum)).
-endif.
