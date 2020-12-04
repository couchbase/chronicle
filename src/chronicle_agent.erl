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
%% TODO: pull is needed to detect situations where a node gets shunned
%% TODO: check more state invariants
-module(chronicle_agent).

-compile(export_all).

-export_type([provision_result/0, reprovision_result/0, wipe_result/0,
              prepare_join_result/0, join_cluster_result/0]).

-behavior(gen_statem).
-include("chronicle.hrl").

-import(chronicle_utils, [call/2, call/3,
                          call_async/4,
                          config_peers/1,
                          next_term/2,
                          term_number/1,
                          compare_positions/2]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer),
        case Peer of
            ?SELF_PEER ->
                ?SERVER;
            _ ->
                ?SERVER_NAME(Peer, ?MODULE)
        end).

-define(PROVISION_TIMEOUT, 10000).
-define(ESTABLISH_LOCAL_TERM_TIMEOUT, 10000).
-define(LOCAL_MARK_COMMITTED_TIMEOUT, 5000).
-define(STORE_BRANCH_TIMEOUT, 15000).
-define(PREPARE_JOIN_TIMEOUT, 10000).
-define(JOIN_CLUSTER_TIMEOUT, 120000).

-define(INSTALL_SNAPSHOT_TIMEOUT, 120000).

-define(SNAPSHOT_TIMEOUT, 60000).
-define(SNAPSHOT_RETRIES, 5).
-define(SNAPSHOT_RETRY_AFTER, 10000).
-define(SNAPSHOT_INTERVAL, 1024).

%% Used to indicate that a function will send a message with the provided Tag
%% back to the caller when the result is ready. And the result type is
%% _ReplyType. This is entirely useless for dializer, but is usefull for
%% documentation purposes.
-type maybe_replies(_Tag, _ReplyType) :: chronicle_utils:send_result().
-type peer() :: ?SELF_PEER | chronicle:peer().

-record(snapshot_state, { tref,
                          seqno,
                          config,

                          remaining_rsms,
                          savers }).

-record(join_cluster, { from, seqno }).

%% TODO: get rid of the duplication between #data{} and #metadata{}.
-record(data, { storage,
                rsms_by_name,
                rsms_by_mref,

                snapshot_readers,

                snapshot_attempts = 0,
                snapshot_state :: undefined
                                | {retry, reference()}
                                | #snapshot_state{}}).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

-spec monitor(peer()) -> reference().
monitor(Peer) ->
    chronicle_utils:monitor_process(?SERVER(Peer)).

-spec get_system_state() ->
          not_provisioned |
          {joining_cluster, chronicle:history_id()} |
          {provisioned, #metadata{}}.
get_system_state() ->
    call(?SERVER, get_system_state).

-spec get_metadata() -> #metadata{}.
get_metadata() ->
    case get_system_state() of
        {provisioned, Metadata} ->
            Metadata;
        _ ->
            exit(not_provisioned)
    end.

check_grant_vote(HistoryId, Position) ->
    call(?SERVER, {check_grant_vote, HistoryId, Position}).

register_rsm(Name, Pid) ->
    call(?SERVER, {register_rsm, Name, Pid}, 10000).

save_rsm_snapshot(Name, Seqno, Snapshot) ->
    Pid = self(),
    case call(?SERVER, {get_rsm_snapshot_saver, Name, Pid, Seqno}, 10000) of
        {ok, SaverPid} ->
            SaverPid ! {snapshot, Snapshot};
        {error, rejected} ->
            ?INFO("Snapshot for RSM ~p at "
                  "sequence number ~p got rejected.", [Name, Seqno])
    end.

get_rsm_snapshot(Name) ->
    with_latest_snapshot(
      fun (Seqno, Config, Storage) ->
              get_rsm_snapshot(Name, Seqno, Config, Storage)
      end).

get_rsm_snapshot(Name, Seqno, Config, Storage) ->
    RSMs = get_rsms(Config),
    case maps:is_key(Name, RSMs) of
        true ->
            Snapshot = read_rsm_snapshot(Name, Seqno, Storage),
            {ok, Seqno, Snapshot};
        false ->
            {no_snapshot, Seqno}
    end.

get_full_snapshot() ->
    with_latest_snapshot(
      fun (Seqno, Config, Storage) ->
              get_full_snapshot(Seqno, Config, Storage)
      end).

get_full_snapshot(Seqno, Config, Storage) ->
    RSMs = get_rsms(Config),
    RSMSnapshots =
        maps:map(
          fun (Name, _) ->
                  Snapshot = read_rsm_snapshot(Name, Seqno, Storage),
                  term_to_binary(Snapshot, [compressed])
          end, RSMs),
    {ok, Seqno, Config, RSMSnapshots}.

with_latest_snapshot(Fun) ->
    case call(?SERVER, {get_latest_snapshot, self()}, 10000) of
        {ok, Ref, Seqno, Config, Storage} ->
            try
                Fun(Seqno, Config, Storage)
            after
                gen_statem:cast(?SERVER, {release_snapshot, Ref})
            end;
        {error, no_snapshot} ->
            {no_snapshot, ?NO_SEQNO}
    end.

get_log() ->
    call(?SERVER, get_log).

get_log_committed(StartSeqno, EndSeqno) ->
    chronicle_storage:get_log_committed(StartSeqno, EndSeqno).

get_log_for_rsm(Name, StartSeqno, EndSeqno) ->
    %% TODO: avoid O(NumberOfStateMachines * NumberOfEntries) complexity.
    case get_log_committed(StartSeqno, EndSeqno) of
        {ok, Entries} ->
            RSMEntries =
                lists:filter(
                  fun (#log_entry{value = Value}) ->
                          case Value of
                              #rsm_command{rsm_name = Name} ->
                                  true;
                              #config{} ->
                                  true;
                              _ ->
                                  false
                          end
                  end, Entries),
            {ok, RSMEntries};
        {error, _} = Error ->
            Error
    end.

-spec get_log(chronicle:history_id(),
              chronicle:leader_term(),
              chronicle:seqno(),
              chronicle:seqno()) ->
          {ok, [#log_entry{}]} |
          {error, Error} when
      Error :: {history_mismatch, chronicle:history_id()} |
               {conflicting_term, chronicle:leader_term()} |
               bad_range.
get_log(HistoryId, Term, StartSeqno, EndSeqno) ->
    call(?SERVER, {get_log, HistoryId, Term, StartSeqno, EndSeqno}).

-spec get_history_id(#metadata{}) -> chronicle:history_id().
get_history_id(#metadata{history_id = CommittedHistoryId,
                         pending_branch = undefined}) ->
    CommittedHistoryId;
get_history_id(#metadata{pending_branch =
                             #branch{history_id = PendingHistoryId}}) ->
    PendingHistoryId.

-type provision_result() :: ok | {error, joining_cluster | provisioned}.
-spec provision([Machine]) -> provision_result() when
      Machine :: {Name :: atom(), Mod :: module(), Args :: [any()]}.
provision(Machines) ->
    case call(?SERVER, {provision, Machines}, ?PROVISION_TIMEOUT) of
        ok ->
            ok = chronicle_secondary_sup:sync_system_state_change();
        Other ->
            Other
    end.

-type reprovision_result() :: ok
                            | {error, reprovision_error()}.
-type reprovision_error() :: not_provisioned
                           | {unstable_config, #transition{}}
                           | {bad_config, peer(), [peer()], [peer()]}.

-spec reprovision() -> reprovision_result().
reprovision() ->
    case call(?SERVER, reprovision, ?PROVISION_TIMEOUT) of
        ok ->
            ok = chronicle_secondary_sup:sync_system_state_change();
        Other ->
            Other
    end.

-type wipe_result() :: ok.
-spec wipe() -> wipe_result().
wipe() ->
    case call(?SERVER, wipe) of
        ok ->
            ok = chronicle_secondary_sup:sync_system_state_change();
        Other ->
            Other
    end.

-type prepare_join_result() :: ok | {error, prepare_join_error()}.
-type prepare_join_error() :: provisioned
                            | joining_cluster
                            | bad_cluster_info.
-spec prepare_join(chronicle:cluster_info()) -> prepare_join_result().
prepare_join(ClusterInfo) ->
    call(?SERVER, {prepare_join, ClusterInfo}, ?PREPARE_JOIN_TIMEOUT).

-type join_cluster_result() :: ok | {error, join_cluster_error()}.
-type join_cluster_error() :: not_prepared
                            | {history_mismatch, chronicle:history_id()}
                            | {not_in_peers,
                               chronicle:peer(), [chronicle:peer()]}.
-spec join_cluster(chronicle:cluster_info()) -> join_cluster_result().
join_cluster(ClusterInfo) ->
    case call(?SERVER, {join_cluster, ClusterInfo}, ?JOIN_CLUSTER_TIMEOUT) of
        ok ->
            ok = chronicle_secondary_sup:sync_system_state_change();
        Other ->
            Other
    end.

-type establish_term_result() ::
        {ok, #metadata{}} |
        {error, establish_term_error()}.

-type establish_term_error() ::
        not_provisioned |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {behind, chronicle:peer_position()}.

-spec establish_local_term(chronicle:history_id(),
                           chronicle:leader_term()) ->
          establish_term_result().
establish_local_term(HistoryId, Term) ->
    call(?SERVER, {establish_term, HistoryId, Term},
         ?ESTABLISH_LOCAL_TERM_TIMEOUT).

-spec establish_term(peer(),
                     Opaque,
                     chronicle:history_id(),
                     chronicle:leader_term(),
                     chronicle:peer_position(),
                     chronicle_utils:send_options()) ->
          maybe_replies(Opaque, establish_term_result()).
establish_term(Peer, Opaque, HistoryId, Term, Position, Options) ->
    %% TODO: don't abuse gen_server calls here and everywhere else
    call_async(?SERVER(Peer), Opaque,
               {establish_term, HistoryId, Term, Position},
               Options).

-type ensure_term_result() ::
        {ok, #metadata{}} |
        {error, ensure_term_error()}.

-type ensure_term_error() ::
        not_provisioned |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()}.

-spec ensure_term(peer(),
                  Opaque,
                  chronicle:history_id(),
                  chronicle:leader_term(),
                  chronicle_utils:send_options()) ->
          maybe_replies(Opaque, ensure_term_result()).
ensure_term(Peer, Opaque, HistoryId, Term, Options) ->
    call_async(?SERVER(Peer), Opaque,
               {ensure_term, HistoryId, Term},
               Options).

-type append_result() :: ok | {error, append_error()}.
-type append_error() ::
        not_provisioned |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {missing_entries, #metadata{}} |
        {protocol_error, any()}.

-spec append(peer(),
             Opaque,
             chronicle:history_id(),
             chronicle:leader_term(),
             chronicle:seqno(),
             chronicle:seqno(),
             [#log_entry{}],
             chronicle_utils:send_options()) ->
          maybe_replies(Opaque, append_result()).
append(Peer, Opaque, HistoryId, Term,
       CommittedSeqno, AtSeqno, Entries, Options) ->
    call_async(?SERVER(Peer), Opaque,
               {append, HistoryId, Term, CommittedSeqno, AtSeqno, Entries},
               Options).

-type install_snapshot_result() :: ok | {error, install_snapshot_error()}.
-type install_snapshot_error() ::
        not_provisioned |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {snapshot_rejected, #metadata{}} |
        {protocol_error, any()}.

-spec install_snapshot(peer(),
                       chronicle:history_id(),
                       chronicle:leader_term(),
                       chronicle:seqno(),
                       ConfigEntry::#log_entry{},
                       #{RSM::atom() => RSMSnapshot::binary()}) ->
          install_snapshot_result().
install_snapshot(Peer, HistoryId, Term, Seqno, ConfigEntry, RSMSnapshots) ->
    call(?SERVER(Peer),
         {install_snapshot, HistoryId, Term, Seqno, ConfigEntry, RSMSnapshots},
         ?INSTALL_SNAPSHOT_TIMEOUT).

-type local_mark_committed_result() ::
        ok | {error, local_mark_committed_error()}.
-type local_mark_committed_error() ::
        not_provisioned |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {protocol_error, any()}.

-spec local_mark_committed(chronicle:history_id(),
                           chronicle:leader_term(),
                           chronicle:seqno()) ->
          local_mark_committed_result().
local_mark_committed(HistoryId, Term, CommittedSeqno) ->
    call(?SERVER,
         {local_mark_committed, HistoryId, Term, CommittedSeqno},
         ?LOCAL_MARK_COMMITTED_TIMEOUT).

-type store_branch_result() ::
        {ok, #metadata{}} |
        {error, store_branch_error()}.
-type store_branch_error() ::
        not_provisioned |
        {coordinator_not_in_peers, chronicle:peer(), [chronicle:peer()]} |
        {concurrent_branch, OurBranch::#branch{}}.

-spec store_branch(peer(), #branch{}) -> store_branch_result().
store_branch(Peer, Branch) ->
    call(?SERVER(Peer), {store_branch, Branch}, ?STORE_BRANCH_TIMEOUT).

-spec undo_branch(peer(), chronicle:history_id()) -> ok | {error, Error} when
      Error :: no_branch |
               {bad_branch, OurBranch::#branch{}}.
undo_branch(Peer, BranchId) ->
    call(?SERVER(Peer), {undo_branch, BranchId}).

%% gen_statem callbacks
callback_mode() ->
    handle_event_function.

init([]) ->
    Data = init_data(),
    State =
        case get_meta(?META_STATE, Data) of
            ?META_STATE_PROVISIONED ->
                provisioned;
            ?META_STATE_PREPARE_JOIN ->
                prepare_join;
            {?META_STATE_JOIN_CLUSTER, Seqno} ->
                #join_cluster{seqno = Seqno};
            ?META_STATE_NOT_PROVISIONED ->
                not_provisioned
        end,

    {FinalState, FinalData} =
        case State of
            #join_cluster{} ->
                %% It's possible the agent crashed right before logging that
                %% we got provisioned. So we need to check again.
                check_join_cluster_done(State, Data);
            _ ->
                {State, Data}
        end,

    {ok, FinalState, FinalData}.

handle_event({call, From}, Call, State, Data) ->
    handle_call(Call, From, State, Data);
handle_event(cast, {release_snapshot, Ref}, State, Data) ->
    handle_release_snapshot(Ref, State, Data);
handle_event(info, {'DOWN', MRef, process, Pid, Reason}, State, Data) ->
    handle_down(MRef, Pid, Reason, State, Data);
handle_event(info, {snapshot_result, Pid, RSM, Result}, State, Data) ->
    handle_snapshot_result(RSM, Pid, Result, State, Data);
handle_event(info, snapshot_timeout, State, Data) ->
    handle_snapshot_timeout(State, Data);
handle_event(info, retry_snapshot, State, Data) ->
    handle_retry_snapshot(State, Data);
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event of type ~p: ~p", [Type, Event]),
    keep_state_and_data.

handle_call(get_system_state, From, State, Data) ->
    handle_get_system_state(From, State, Data);
handle_call({check_grant_vote, PeerHistoryId, PeerPosition},
            From, State, Data) ->
    handle_check_grant_vote(PeerHistoryId, PeerPosition, From, State, Data);
handle_call(get_log, From, _State, _Data) ->
    %% TODO: get rid of this
    {keep_state_and_data,
     {reply, From, {ok, chronicle_storage:get_log()}}};
handle_call({get_log, HistoryId, Term, StartSeqno, EndSeqno},
            From, State, Data) ->
    handle_get_log(HistoryId, Term, StartSeqno, EndSeqno, From, State, Data);
handle_call({register_rsm, Name, Pid}, From, State, Data) ->
    handle_register_rsm(Name, Pid, From, State, Data);
handle_call({get_latest_snapshot, Pid}, From, State, Data) ->
    handle_get_latest_snapshot(Pid, From, State, Data);
handle_call({provision, Machines}, From, State, Data) ->
    handle_provision(Machines, From, State, Data);
handle_call(reprovision, From, State, Data) ->
    handle_reprovision(From, State, Data);
handle_call(wipe, From, State, Data) ->
    handle_wipe(From, State, Data);
handle_call({prepare_join, ClusterInfo}, From, State, Data) ->
    handle_prepare_join(ClusterInfo, From, State, Data);
handle_call({join_cluster, ClusterInfo}, From, State, Data) ->
    handle_join_cluster(ClusterInfo, From, State, Data);
handle_call({establish_term, HistoryId, Term}, From, State, Data) ->
    %% TODO: consider simply skipping the position check for this case
    Position = {get_meta(?META_TERM_VOTED, Data), get_high_seqno(Data)},
    handle_establish_term(HistoryId, Term, Position, From, State, Data);
handle_call({establish_term, HistoryId, Term, Position}, From, State, Data) ->
    handle_establish_term(HistoryId, Term, Position, From, State, Data);
handle_call({ensure_term, HistoryId, Term}, From, State, Data) ->
    handle_ensure_term(HistoryId, Term, From, State, Data);
handle_call({append, HistoryId, Term, CommittedSeqno, AtSeqno, Entries},
            From, State, Data) ->
    handle_append(HistoryId, Term,
                  CommittedSeqno, AtSeqno, Entries, From, State, Data);
handle_call({local_mark_committed, HistoryId, Term, CommittedSeqno},
            From, State, Data) ->
    handle_local_mark_committed(HistoryId, Term,
                                CommittedSeqno, From, State, Data);
handle_call({install_snapshot,
             HistoryId, Term, Seqno, ConfigEntry, RSMSnapshotsMeta},
            From, State, Data) ->
    handle_install_snapshot(HistoryId, Term, Seqno,
                            ConfigEntry, RSMSnapshotsMeta, From, State, Data);
handle_call({store_branch, Branch}, From, State, Data) ->
    handle_store_branch(Branch, From, State, Data);
handle_call({undo_branch, BranchId}, From, State, Data) ->
    handle_undo_branch(BranchId, From, State, Data);
handle_call({get_rsm_snapshot_saver, RSM, RSMPid, Seqno}, From, State, Data) ->
    handle_get_rsm_snapshot_saver(RSM, RSMPid, Seqno, From, State, Data);
handle_call(_Call, From, _State, _Data) ->
    {keep_state_and_data,
     {reply, From, nack}}.

terminate(_Reason, Data) ->
    maybe_cancel_snapshot(Data).

%% internal
handle_get_system_state(From, State, Data) ->
    Reply =
        case get_external_state(State) of
            provisioned = ExtState ->
                {ExtState, build_metadata(Data)};
            joining_cluster = ExtState ->
                {ExtState, get_effective_history_id(Data)};
            ExtState ->
                ExtState
        end,
    {keep_state_and_data, {reply, From, Reply}}.

get_external_state(State) ->
    case State of
        provisioned ->
            provisioned;
        prepare_join ->
            joining_cluster;
        #join_cluster{} ->
            joining_cluster;
        not_provisioned ->
            not_provisioned
    end.

build_metadata(Data) ->
    #{?META_PEER := Peer,
      ?META_HISTORY_ID := HistoryId,
      ?META_TERM := Term,
      ?META_TERM_VOTED := TermVoted,
      ?META_COMMITTED_SEQNO := CommittedSeqno,
      ?META_PENDING_BRANCH := PendingBranch} = get_meta(Data),

    ConfigEntry = get_config(Data),
    {Config, ConfigRevision} =
        case ConfigEntry of
            undefined ->
                {undefined, undefined};
            #log_entry{value = Value} ->
                {Value, chronicle_utils:log_entry_revision(ConfigEntry)}
        end,

    #metadata{peer = Peer,
              history_id = HistoryId,
              term = Term,
              term_voted = TermVoted,
              high_seqno = get_high_seqno(Data),
              committed_seqno = CommittedSeqno,
              config = Config,
              config_revision = ConfigRevision,
              pending_branch = PendingBranch}.

handle_check_grant_vote(PeerHistoryId, PeerPosition, From, State, Data) ->
    OurHistoryId = get_effective_history_id(Data),
    Reply =
        case ?CHECK(check_prepared(State),
                    check_history_id(PeerHistoryId, OurHistoryId),
                    check_peer_current(PeerPosition, Data)) of
            ok ->
                {ok, get_meta(?META_TERM, Data)};
            {error, _} = Error ->
                Error
        end,

    {keep_state_and_data, {reply, From, Reply}}.

check_prepared(State) ->
    case State of
        not_provisioned ->
            {error, not_provisioned};
        _ ->
            ok
    end.

handle_get_log(HistoryId, Term, StartSeqno, EndSeqno, From, State, Data) ->
    Reply =
        case check_get_log(HistoryId, Term,
                           StartSeqno, EndSeqno, State, Data) of
            ok ->
                Entries = chronicle_storage:get_log(StartSeqno, EndSeqno),
                {ok, Entries};
            {error, _} = Error ->
                Error
        end,

    {keep_state_and_data, {reply, From, Reply}}.

check_get_log(HistoryId, Term, StartSeqno, EndSeqno, State, Data) ->
    ?CHECK(check_provisioned(State),
           check_history_id(HistoryId, Data),
           check_same_term(Term, Data),
           check_log_range(StartSeqno, EndSeqno, Data)).

handle_register_rsm(Name, Pid, From, State,
                    #data{rsms_by_name = RSMs,
                          rsms_by_mref = MRefs} = Data) ->
    case check_register_rsm(Name, State, Data) of
        ok ->
            ?DEBUG("Registering RSM ~p with pid ~p", [Name, Pid]),

            MRef = erlang:monitor(process, Pid),
            NewRSMs = RSMs#{Name => {MRef, Pid}},
            NewMRefs = MRefs#{MRef => Name},

            CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
            Info0 = #{committed_seqno => CommittedSeqno},
            Info1 = case need_rsm_snapshot(Name, Data) of
                        {true, NeedSnapshotSeqno} ->
                            Info0#{need_snapshot_seqno => NeedSnapshotSeqno};
                        false ->
                            Info0
                    end,

            Reply = {ok, Info1},
            NewData = Data#data{rsms_by_name = NewRSMs,
                                rsms_by_mref = NewMRefs},

            {keep_state, NewData, {reply, From, Reply}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_register_rsm(Name, State, #data{rsms_by_name = RSMs}) ->
    case check_provisioned(State) of
        ok ->
            case maps:find(Name, RSMs) of
                {ok, {OtherPid, _}} ->
                    {error, {already_registered, Name, OtherPid}};
                error ->
                    ok
            end;
        {error, _} = Error ->
            Error
    end.

handle_get_latest_snapshot(Pid, From, _State,
                           #data{snapshot_readers = Readers,
                                 storage = Storage} = Data) ->
    case get_and_hold_latest_snapshot(Data) of
        {{Seqno, Config}, NewData0} ->
            MRef = erlang:monitor(process, Pid),
            NewReaders = Readers#{MRef => Seqno},
            NewData = NewData0#data{snapshot_readers = NewReaders},
            Reply = {ok, MRef, Seqno, Config, Storage},

            {keep_state,
             NewData,
             {reply, From, Reply}};
        no_snapshot ->
            {keep_state_and_data,
             {reply, From, {error, no_snapshot}}}
    end.

handle_release_snapshot(MRef, _State,
                        #data{snapshot_readers = Readers} = Data) ->
    {SnapshotSeqno, NewReaders} = maps:take(MRef, Readers),
    erlang:demonitor(MRef, [flush]),
    {keep_state, release_snapshot(SnapshotSeqno,
                                  Data#data{snapshot_readers = NewReaders})}.

handle_down(MRef, Pid, Reason, State,
            #data{rsms_by_name = RSMs,
                  rsms_by_mref = MRefs,
                  snapshot_readers = Readers} = Data) ->
    case maps:take(MRef, MRefs) of
        error ->
            case maps:is_key(MRef, Readers) of
                true ->
                    handle_release_snapshot(MRef, State, Data);
                false ->
                    {stop, {unexpected_process_down, MRef, Pid, Reason}, Data}
            end;
        {Name, NewMRefs} ->
            ?DEBUG("RSM ~p~p terminated with reason: ~p", [Name, Pid, Reason]),

            NewRSMs = maps:remove(Name, RSMs),
            {keep_state, Data#data{rsms_by_name = NewRSMs,
                                   rsms_by_mref = NewMRefs}}
    end.

handle_snapshot_result(RSM, Pid, Result, State, Data) ->
    case Result of
        ok ->
            handle_snapshot_ok(RSM, Pid, State, Data);
        failed ->
            handle_snapshot_failed(State, Data)
    end.

handle_snapshot_ok(RSM, Pid, _State,
                   #data{snapshot_state = SnapshotState} = Data) ->
    #snapshot_state{tref = TRef,
                    seqno = Seqno,
                    config = Config,
                    savers = Savers} = SnapshotState,
    NewSavers = maps:remove(Pid, Savers),

    ?DEBUG("Saved a snapshot for RSM ~p at seqno ~p", [RSM, Seqno]),

    case maps:size(NewSavers) =:= 0 of
        true ->
            ?DEBUG("All RSM snapshots at seqno ~p saved. "
                   "Recording the snapshot.~n"
                   "Config:~n~p",
                   [Seqno, Config]),

            cancel_snapshot_timer(TRef),
            NewData = Data#data{snapshot_state = undefined},
            {keep_state, record_snapshot(Seqno, Config, NewData)};
        false ->
            NewSnapshotState = SnapshotState#snapshot_state{savers = NewSavers},
            {keep_state, Data#data{snapshot_state = NewSnapshotState}}
    end.

handle_snapshot_failed(_State,
                       #data{snapshot_state = SnapshotState,
                             snapshot_attempts = Attempts} = Data) ->
    #snapshot_state{seqno = Seqno} = SnapshotState,

    NewAttempts = Attempts + 1,
    AttemptsRemaining = ?SNAPSHOT_RETRIES - NewAttempts,

    ?ERROR("Failed to take snapshot at seqno ~p. "
           "~p attempts remaining.~nSnapshot state:~n~p",
           [Seqno, AttemptsRemaining, SnapshotState]),
    NewData = Data#data{snapshot_attempts = NewAttempts},
    case AttemptsRemaining > 0 of
        true ->
            {keep_state, schedule_retry_snapshot(NewData)};
        false ->
            {stop, {snapshot_failed, SnapshotState}, NewData}
    end.

handle_snapshot_timeout(_State, #data{snapshot_state = SnapshotState} = Data) ->
    ?ERROR("Timeout while taking snapshot.~n"
           "Snapshot state:~n~p", [SnapshotState]),
    {stop, {snapshot_timeout, SnapshotState}, Data}.

handle_retry_snapshot(_State, Data) ->
    {retry, _} = Data#data.snapshot_state,
    {keep_state, initiate_snapshot(Data#data{snapshot_state = undefined})}.

foreach_rsm(Fun, #data{rsms_by_name = RSMs}) ->
    chronicle_utils:maps_foreach(
      fun (Name, {_, Pid}) ->
              Fun(Name, Pid)
      end, RSMs).

handle_reprovision(From, State, Data) ->
    case check_reprovision(State, Data) of
        {ok, Config} ->
            #{?META_HISTORY_ID := HistoryId,
              ?META_TERM := Term} = get_meta(Data),

            HighSeqno = get_high_seqno(Data),
            Peer = get_peer_name(),
            NewTerm = next_term(Term, Peer),
            NewConfig = Config#config{voters = [Peer]},
            Seqno = HighSeqno + 1,

            ConfigEntry = #log_entry{history_id = HistoryId,
                                     term = NewTerm,
                                     seqno = Seqno,
                                     value = NewConfig},

            ?DEBUG("Reprovisioning peer with config:~n~p", [ConfigEntry]),

            NewData = append_entry(ConfigEntry,
                                   #{?META_PEER => Peer,
                                     ?META_TERM => NewTerm,
                                     ?META_TERM_VOTED => NewTerm,
                                     ?META_COMMITTED_SEQNO => Seqno},
                                   Data),

            announce_system_reprovisioned(NewData),
            announce_new_config(NewData),
            announce_committed_seqno(Seqno, NewData),

            {keep_state, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_reprovision(State, Data) ->
    case check_provisioned(State) of
        ok ->
            Peer = get_meta(?META_PEER, Data),
            ConfigEntry = get_config(Data),
            Config = ConfigEntry#log_entry.value,
            case Config of
                #config{voters = Voters,
                        replicas = Replicas} ->
                    case Voters =:= [Peer] andalso Replicas =:= [] of
                        true ->
                            {ok, Config};
                        _ ->
                            {error, {bad_config, Peer, Voters, Replicas}}
                    end;
                #transition{} ->
                    {error, {unstable_config, Config}}
            end;
        Error ->
            Error
    end.

handle_provision(Machines0, From, State, Data) ->
    case check_not_provisioned(State) of
        ok ->
            Peer = get_peer_name(),
            HistoryId = chronicle_utils:random_uuid(),
            Term = next_term(?NO_TERM, Peer),
            Seqno = 1,

            Machines = maps:from_list(
                         [{Name, #rsm_config{module = Module, args = Args}} ||
                             {Name, Module, Args} <- Machines0]),

            Config = #config{voters = [Peer],
                             replicas = [],
                             state_machines = Machines},
            ConfigEntry = #log_entry{history_id = HistoryId,
                                     term = Term,
                                     seqno = Seqno,
                                     value = Config},

            ?DEBUG("Provisioning with history ~p. Config:~n~p",
                   [HistoryId, Config]),

            NewData = append_entry(ConfigEntry,
                                   #{?META_STATE => ?META_STATE_PROVISIONED,
                                     ?META_PEER => Peer,
                                     ?META_HISTORY_ID => HistoryId,
                                     ?META_TERM => Term,
                                     ?META_TERM_VOTED => Term,
                                     ?META_COMMITTED_SEQNO => Seqno},
                                   Data),

            announce_system_provisioned(NewData),

            {next_state, provisioned, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data,
             {reply, From, Error}}
    end.

check_not_provisioned(State) ->
    case State of
        not_provisioned ->
            ok;
        _ ->
            {error, get_external_state(State)}
    end.

check_provisioned(State) ->
    case State of
        provisioned ->
            ok;
        _ ->
            {error, not_provisioned}
    end.

handle_wipe(From, State, Data) ->
    case State of
        not_provisioned ->
            {keep_state_and_data, {reply, From, ok}};
        _ ->
            announce_system_state(not_provisioned),
            %% TODO: There might be snapshots held by some of the RSMs. Wiping
            %% without ensuring that all of those are stopped is therefore
            %% unsafe.
            {next_state,
             not_provisioned, perform_wipe(Data),
             {reply, From, ok}}
    end.

perform_wipe(Data) ->
    NewData = maybe_cancel_snapshot(Data),

    ?INFO("Wiping"),
    chronicle_storage:close(NewData#data.storage),
    chronicle_storage:wipe(),
    ?INFO("Wiped successfully"),

    init_data().

handle_prepare_join(ClusterInfo, From, State, Data) ->
    case check_not_provisioned(State) of
        ok ->
            case ClusterInfo of
                #{history_id := HistoryId} ->
                    Peer = get_peer_name(),
                    NewData =
                        store_meta(#{?META_HISTORY_ID => HistoryId,
                                     ?META_PEER => Peer,
                                     ?META_STATE => ?META_STATE_PREPARE_JOIN},
                                   Data),
                    announce_joining_cluster(NewData),
                    {next_state, prepare_join, NewData, {reply, From, ok}};
                _ ->
                    {keep_state_and_data,
                     {reply, From, {error, bad_cluster_info}}}
            end;
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

handle_join_cluster(ClusterInfo, From, State, Data) ->
    case check_join_cluster(ClusterInfo, State, Data) of
        {ok, Seqno} ->
            Meta = #{?META_STATE => {?META_STATE_JOIN_CLUSTER, Seqno}},
            NewData = store_meta(Meta, Data),
            NewState = #join_cluster{from = From, seqno = Seqno},

            %% We might already have all entries we need.
            {FinalState, FinalData} =
                check_join_cluster_done(NewState, NewData),

            {next_state, FinalState, FinalData};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_join_cluster(ClusterInfo, State, Data) ->
    case State of
        prepare_join ->
            case ClusterInfo of
                #{history_id := HistoryId,
                  committed_seqno := CommittedSeqno,
                  peers := Peers} ->
                    case ?CHECK(check_history_id(HistoryId, Data),
                                check_peer(Peers, Data)) of
                        ok ->
                            {ok, CommittedSeqno};
                        {error, _} = Error ->
                            Error
                    end;
                _ ->
                    {error, bad_cluster_info}
            end;
        _ ->
            {error, not_prepared}
    end.

check_peer(Peers, Data) ->
    Peer = get_meta(?META_PEER, Data),
    true = (Peer =/= ?NO_PEER),
    case lists:member(Peer, Peers) of
        true ->
            ok;
        false ->
            {error, {not_in_peers, Peer, Peers}}
    end.

check_join_cluster_done(State, Data) ->
    case State of
        #join_cluster{seqno = WaitedSeqno, from = From} ->
            CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
            case CommittedSeqno >= WaitedSeqno of
                true ->
                    NewData = store_meta(#{?META_STATE =>
                                               ?META_STATE_PROVISIONED}, Data),
                    announce_system_state(provisioned, build_metadata(NewData)),

                    case From =/= undefined of
                        true ->
                            gen_statem:reply(From, ok);
                        false ->
                            ok
                    end,

                    {provisioned, NewData};
                false ->
                    {State, Data}
            end;
        _ ->
            {State, Data}
    end.

handle_establish_term(HistoryId, Term, Position, From, State, Data) ->
    assert_valid_history_id(HistoryId),
    assert_valid_term(Term),

    case check_establish_term(HistoryId, Term, Position, State, Data) of
        ok ->
            NewData = store_meta(#{?META_TERM => Term}, Data),
            announce_term_established(State, Term),
            ?DEBUG("Accepted term ~p in history ~p", [Term, HistoryId]),

            Reply = {ok, build_metadata(Data)},
            {keep_state, NewData, {reply, From, Reply}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_establish_term(HistoryId, Term, Position, State, Data) ->
    ?CHECK(check_prepared(State),
           check_history_id(HistoryId, Data),
           check_later_term(Term, Data),
           check_peer_current(Position, Data)).

check_later_term(Term, Data) ->
    CurrentTerm = get_meta(?META_TERM, Data),
    case term_number(Term) > term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

check_peer_current(Position, Data) ->
    OurTermVoted = get_meta(?META_TERM_VOTED, Data),
    OurHighSeqno = get_high_seqno(Data),
    OurPosition = {OurTermVoted, OurHighSeqno},
    case compare_positions(Position, OurPosition) of
        lt ->
            {error, {behind, OurPosition}};
        _ ->
            ok
    end.

handle_ensure_term(HistoryId, Term, From, State, Data) ->
    Reply =
        case ?CHECK(check_prepared(State),
                    check_history_id(HistoryId, Data),
                    check_not_earlier_term(Term, Data)) of
            ok ->
                {ok, build_metadata(Data)};
            {error, _} = Error ->
                Error
        end,

    {keep_state_and_data, {reply, From, Reply}}.

handle_append(HistoryId, Term,
              CommittedSeqno, AtSeqno, Entries, From, State, Data) ->
    assert_valid_history_id(HistoryId),
    assert_valid_term(Term),

    case check_append(HistoryId, Term,
                      CommittedSeqno, AtSeqno, Entries, State, Data) of
        {ok, Info} ->
            complete_append(HistoryId, Term, Info, From, State, Data);
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

extract_latest_config(Entries) ->
    lists:foldl(
      fun (Entry, Acc) ->
              case Entry#log_entry.value of
                  #config{} ->
                      Entry;
                  #transition{} ->
                      Entry;
                  #rsm_command{} ->
                      Acc
              end
      end, false, Entries).

complete_append(HistoryId, Term, Info, From, State, Data) ->
    #{entries := Entries,
      start_seqno := StartSeqno,
      end_seqno := EndSeqno,
      committed_seqno := NewCommittedSeqno,
      truncate := Truncate} = Info,

    PreMetadata =
        #{?META_HISTORY_ID => HistoryId,
          ?META_TERM => Term,
          ?META_TERM_VOTED => Term,
          ?META_PENDING_BRANCH => undefined},
    PostMetadata = #{?META_COMMITTED_SEQNO => NewCommittedSeqno},

    %% When resolving a branch, we must never delete the branch record without
    %% also logging a new config. Therefore the update needs to be atomic.
    Atomic = (get_meta(?META_PENDING_BRANCH, Data) =/= undefined),
    NewData0 = append_entries(StartSeqno, EndSeqno, Entries, PreMetadata,
                              PostMetadata, Truncate, Atomic, Data),

    ?DEBUG("Appended entries.~n"
           "History id: ~p~n"
           "Term: ~p~n"
           "High Seqno: ~p~n"
           "Committed Seqno: ~p~n"
           "Entries: ~p~n"
           "Config: ~p",
           [HistoryId, Term, EndSeqno,
            NewCommittedSeqno, Entries, get_config(NewData0)]),

    %% TODO: in-progress snapshots might need to be canceled if any of the
    %% state machines get deleted.

    {NewState, NewData} =
        case State of
            provisioned ->
                maybe_announce_term_established(Term, Data),
                maybe_announce_new_config(Data, NewData0),
                maybe_announce_committed_seqno(Data, NewData0),

                {State, NewData0};
            _ ->
                check_join_cluster_done(State, NewData0)
        end,


    {next_state,
     NewState, maybe_initiate_snapshot(NewState, NewData),
     {reply, From, ok}}.

check_append(HistoryId, Term, CommittedSeqno, AtSeqno, Entries, State, Data) ->
    ?CHECK(check_prepared(State),
           check_append_history_id(HistoryId, Entries, Data),
           check_not_earlier_term(Term, Data),
           check_append_obsessive(Term, CommittedSeqno,
                                  AtSeqno, Entries, Data)).

check_append_history_id(HistoryId, Entries, Data) ->
    case check_history_id(HistoryId, Data) of
        ok ->
            OldHistoryId = get_meta(?META_HISTORY_ID, Data),
            case HistoryId =:= OldHistoryId of
                true ->
                    ok;
                false ->
                    NewHistoryEntries =
                        lists:dropwhile(
                          fun (#log_entry{history_id = EntryHistoryId}) ->
                                  EntryHistoryId =:= OldHistoryId
                          end, Entries),

                    %% The first entry in any history must be a config entry
                    %% committing that history.
                    case NewHistoryEntries of
                        [#log_entry{history_id = HistoryId,
                                    value = #config{}} | _] ->
                            ok;
                        _ ->
                            %% TODO: sanitize entries
                            ?ERROR("Malformed entries in an append "
                                   "request starting a new history.~n"
                                   "Old history id: ~p~n"
                                   "New history id: ~p~n"
                                   "Entries:~n~p",
                                   [OldHistoryId, HistoryId, Entries]),
                            {error, {protocol_error,
                                     {missing_config_starting_history,
                                      OldHistoryId, HistoryId, Entries}}}
                    end
            end;
        {error, _} = Error ->
            Error
    end.

check_append_obsessive(Term, CommittedSeqno, AtSeqno, Entries, Data) ->
    case get_entries_seqnos(AtSeqno, Entries) of
        {ok, StartSeqno, EndSeqno} ->
            #{?META_TERM_VOTED := OurTermVoted,
              ?META_COMMITTED_SEQNO := OurCommittedSeqno} = get_meta(Data),
            OurHighSeqno = get_high_seqno(Data),

            {SafeHighSeqno, NewTerm} =
                case Term =:= OurTermVoted of
                    true ->
                        {OurHighSeqno, false};
                    false ->
                        %% Last we received any entries was in a different
                        %% term. So any uncommitted entries might actually be
                        %% from alternative histories and they need to be
                        %% truncated.
                        {OurCommittedSeqno, true}
                end,

            case StartSeqno > SafeHighSeqno + 1 of
                true ->
                    %% TODO: add more information here?

                    %% There's a gap between what entries we've got and what
                    %% we were given. So the leader needs to send us more.
                    {error, {missing_entries, build_metadata(Data)}};
                false ->
                    case EndSeqno < SafeHighSeqno of
                        true ->
                            %% Currently, this should never happen, because
                            %% proposer always sends all history it has. But
                            %% conceptually it doesn't have to be this way. If
                            %% proposer starts chunking appends into
                            %% sub-appends in the future, in combination with
                            %% message loss, this case will be normal. But
                            %% consider this a protocol error for now.
                            {error,
                             {protocol_error,
                              {stale_proposer, EndSeqno, SafeHighSeqno}}};
                        false ->
                            case check_committed_seqno(Term, CommittedSeqno,
                                                       EndSeqno, Data) of
                                {ok, FinalCommittedSeqno} ->
                                    case drop_known_entries(
                                           Entries, EndSeqno,
                                           SafeHighSeqno, OurHighSeqno,
                                           NewTerm, Data) of
                                        {ok,
                                         FinalStartSeqno,
                                         Truncate,
                                         FinalEntries} ->
                                            {ok,
                                             #{entries => FinalEntries,
                                               start_seqno => FinalStartSeqno,
                                               end_seqno => EndSeqno,
                                               committed_seqno =>
                                                   FinalCommittedSeqno,
                                               truncate => Truncate}};
                                        {error, _} = Error ->
                                            Error
                                    end;
                                {error, _} = Error ->
                                    Error
                            end
                    end
            end;
        {error, {malformed, Entry}} ->
            %% TODO: remove logging of the entries
            ?ERROR("Received an ill-formed append request in term ~p.~n"
                   "Stumbled upon this entry: ~p~n"
                   "All entries:~n~p",
                   [Term, Entry, Entries]),
            {error, {protocol_error,
                     {malformed_append, Entry, AtSeqno, Entries}}}

    end.

drop_known_entries(Entries, EntriesEndSeqno,
                   SafeHighSeqno, HighSeqno, NewTerm, Data) ->
    {SafeEntries, UnsafeEntries} = split_entries(SafeHighSeqno, Entries),

    %% All safe entries must match.
    case check_entries_match(SafeEntries, Data) of
        ok ->
            {PreHighSeqnoEntries, PostHighSeqnoEntries} =
                split_entries(HighSeqno, UnsafeEntries),

            case check_entries_match(PreHighSeqnoEntries, Data) of
                ok ->
                    case EntriesEndSeqno < HighSeqno of
                        true ->
                            %% The entries sent by the leader match our
                            %% entries, but our history is longer. This may
                            %% only happen in a new term. The tail of the log
                            %% needs to be truncated.

                            true = NewTerm,
                            [] = PostHighSeqnoEntries,

                            ?DEBUG("Going to drop log tail past ~p",
                                   [EntriesEndSeqno]),

                            {ok, EntriesEndSeqno + 1, true, []};
                        false ->
                            {ok, HighSeqno + 1, false, PostHighSeqnoEntries}
                    end;
                {mismatch, MismatchSeqno, _OurEntry, Remaining} ->
                    true = NewTerm,

                    ?DEBUG("Mismatch at seqno ~p. "
                           "Going to drop the log tail.", [MismatchSeqno]),
                    {ok, MismatchSeqno, true, Remaining ++ PostHighSeqnoEntries}
            end;
        {mismatch, Seqno, OurEntry, Remaining} ->
            %% TODO: don't log entries
            ?ERROR("Unexpected mismatch in entries sent by the proposer.~n"
                   "Seqno: ~p~n"
                   "Our entry:~n~p~n"
                   "Remaining entries:~n~p",
                   [Seqno, OurEntry, Remaining]),
            {error, {protocol_error,
                     {mismatched_entry, Remaining, OurEntry}}}
    end.

split_entries(Seqno, Entries) ->
    lists:splitwith(
      fun (#log_entry{seqno = EntrySeqno}) ->
              EntrySeqno =< Seqno
      end, Entries).

check_entries_match([], _Data) ->
    ok;
check_entries_match([Entry | Rest] = Entries, Data) ->
    EntrySeqno = Entry#log_entry.seqno,
    {ok, OurEntry} = get_log_entry(EntrySeqno, Data),

    %% TODO: it should be enough to compare histories and terms here. But for
    %% now let's compare complete entries to be doubly confident.
    case Entry =:= OurEntry of
        true ->
            check_entries_match(Rest, Data);
        false ->
            {mismatch, EntrySeqno, OurEntry, Entries}
    end.

get_entries_seqnos(AtSeqno, Entries) ->
    get_entries_seqnos_loop(Entries, AtSeqno + 1, AtSeqno).

get_entries_seqnos_loop([], StartSeqno, EndSeqno) ->
    {ok, StartSeqno, EndSeqno};
get_entries_seqnos_loop([Entry|Rest], StartSeqno, EndSeqno) ->
    Seqno = Entry#log_entry.seqno,

    case Seqno =:= EndSeqno + 1 of
        true ->
            get_entries_seqnos_loop(Rest, StartSeqno, Seqno);
        false ->
            {error, {malformed, Entry}}
    end.

check_committed_seqno(Term, CommittedSeqno, HighSeqno, Data) ->
    ?CHECK(check_committed_seqno_known(CommittedSeqno, HighSeqno, Data),
           check_committed_seqno_rollback(Term, CommittedSeqno, Data)).

check_committed_seqno_rollback(Term, CommittedSeqno, Data) ->
    #{?META_TERM_VOTED := OurTermVoted,
      ?META_COMMITTED_SEQNO := OurCommittedSeqno} = get_meta(Data),
    case CommittedSeqno < OurCommittedSeqno of
        true ->
            case Term =:= OurTermVoted of
                true ->
                    ?ERROR("Refusing to lower our committed "
                           "seqno ~p to ~p in term ~p. "
                           "This should never happen.",
                           [OurCommittedSeqno, CommittedSeqno, Term]),
                    {error, {protocol_error,
                             {committed_seqno_rollback,
                              CommittedSeqno, OurCommittedSeqno}}};
                false ->
                    %% If this append establishes a new term, it's possible
                    %% that the leader doesn't know the latest committed
                    %% seqno. This is normal, in a sense that the leader will
                    %% re-commit the same value again. The receiving peer will
                    %% keep its committed seqno unchanged.
                    ?INFO("Leader in term ~p believes seqno ~p "
                          "to be committed, "
                          "while we know that ~p was committed in term ~p. "
                          "Keeping our committed seqno intact.",
                          [Term, CommittedSeqno,
                           OurCommittedSeqno, OurTermVoted]),
                    {ok, OurCommittedSeqno}
            end;
        false ->
            {ok, CommittedSeqno}
    end.

check_committed_seqno_known(CommittedSeqno, HighSeqno, Data) ->
    case CommittedSeqno > HighSeqno of
        true ->
            %% TODO: add more information here?
            {error, {missing_entries, build_metadata(Data)}};
        false ->
            ok
    end.

check_not_earlier_term(Term, Data) ->
    CurrentTerm = get_meta(?META_TERM, Data),
    case term_number(Term) >= term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

handle_local_mark_committed(HistoryId, Term,
                            CommittedSeqno, From, State, Data) ->
    case check_local_mark_committed(HistoryId, Term,
                                    CommittedSeqno, State, Data) of
        ok ->
            OurCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
            NewData =
                case OurCommittedSeqno =:= CommittedSeqno of
                    true ->
                        Data;
                    false ->
                        NewData0 =
                            store_meta(#{?META_COMMITTED_SEQNO =>
                                             CommittedSeqno}, Data),

                        announce_committed_seqno(CommittedSeqno, NewData0),

                        ?DEBUG("Marked ~p seqno committed", [CommittedSeqno]),
                        NewData0
                end,

            {keep_state, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_local_mark_committed(HistoryId, Term, CommittedSeqno, State, Data) ->
    HighSeqno = get_high_seqno(Data),

    ?CHECK(check_provisioned(State),
           check_history_id(HistoryId, Data),
           check_same_term(Term, Data),
           case check_committed_seqno(Term, CommittedSeqno, HighSeqno, Data) of
               {ok, FinalCommittedSeqno} ->
                   %% This is only ever called by the local leader, so there
                   %% never should be a possibility of rollback.
                   true = (FinalCommittedSeqno =:= CommittedSeqno),
                   ok;
               {error, _} = Error ->
                   Error
           end).

handle_install_snapshot(HistoryId, Term, SnapshotSeqno,
                        ConfigEntry, RSMSnapshots, From, State, Data) ->
    case check_install_snapshot(HistoryId, Term, SnapshotSeqno,
                                ConfigEntry, RSMSnapshots, State, Data) of
        ok ->
            Metadata = #{?META_HISTORY_ID => HistoryId,
                         ?META_TERM => Term,
                         ?META_TERM_VOTED => Term,
                         ?META_PENDING_BRANCH => undefined,
                         ?META_COMMITTED_SEQNO => SnapshotSeqno},

            NewData0 = install_snapshot(SnapshotSeqno, ConfigEntry,
                                        RSMSnapshots, Metadata, Data),

            {NewState, NewData} =
                case State of
                    provisioned ->
                        maybe_announce_term_established(Term, Data),
                        maybe_announce_new_config(Data, NewData0),
                        maybe_announce_committed_seqno(Data, NewData0),

                        {State, maybe_cancel_snapshot(NewData0)};
                    _ ->
                        check_join_cluster_done(State, NewData0)
                end,

            {next_state,
             NewState, NewData,
             {reply, From, {ok, build_metadata(NewData)}}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_install_snapshot(HistoryId, Term,
                       SnapshotSeqno, ConfigEntry, RSMSnapshots, State, Data) ->
    ?CHECK(check_prepared(State),
           check_history_id(HistoryId, Data),
           check_not_earlier_term(Term, Data),
           check_snapshot_seqno(SnapshotSeqno, Data),
           check_snapshot_config(ConfigEntry, RSMSnapshots)).

check_snapshot_seqno(SnapshotSeqno, Data) ->
    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),

    case SnapshotSeqno > CommittedSeqno of
        true ->
            ok;
        false ->
            {error, {snapshot_rejected, build_metadata(Data)}}
    end.

check_snapshot_config(Config, RSMSnapshots) ->
    ConfigRSMs = maps:keys(get_rsms(Config)),
    SnapshotRSMs = maps:keys(RSMSnapshots),

    case ConfigRSMs -- SnapshotRSMs of
        [] ->
            ok;
        MissingRSMs ->
            ?ERROR("Inconsistent install_snapshot request.~n"
                   "Config:~n~p"
                   "Missing snapshots: ~p",
                   [Config, MissingRSMs]),
            {error, {protocol_error,
                     {missing_snapshots, Config, MissingRSMs}}}
    end.

handle_store_branch(Branch, From, State, Data) ->
    assert_valid_branch(Branch),

    case ?CHECK(check_provisioned(State),
                check_branch_compatible(Branch, Data),
                check_branch_coordinator(Branch, Data)) of
        {ok, FinalBranch} ->
            NewData =
                store_meta(#{?META_PENDING_BRANCH => FinalBranch}, Data),

            case get_meta(?META_PENDING_BRANCH, Data) of
                undefined ->
                    %% New branch, announce history change.
                    announce_new_history(NewData);
                _ ->
                    ok
            end,

            ?DEBUG("Stored a branch record:~n~p", [FinalBranch]),
            {keep_state,
             NewData,
             {reply, From, {ok, build_metadata(NewData)}}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_branch_compatible(NewBranch, Data) ->
    PendingBranch = get_meta(?META_PENDING_BRANCH, Data),
    case PendingBranch =:= undefined of
        true ->
            ok;
        false ->
            PendingId = PendingBranch#branch.history_id,
            NewId = NewBranch#branch.history_id,

            case PendingId =:= NewId of
                true ->
                    ok;
                false ->
                    {error, {concurrent_branch, PendingBranch}}
            end
    end.

check_branch_coordinator(Branch, Data) ->
    Peer = get_meta(?META_PEER, Data),
    Coordinator =
        case Branch#branch.coordinator of
            self ->
                Peer;
            Other ->
                Other
        end,

    FinalBranch = Branch#branch{coordinator = Coordinator},
    Peers = Branch#branch.peers,

    case lists:member(Coordinator, Peers) of
        true ->
            {ok, FinalBranch};
        false ->
            {error, {coordinator_not_in_peers, Coordinator, Peers}}
    end.

handle_undo_branch(BranchId, From, _State, Data) ->
    assert_valid_history_id(BranchId),
    case check_branch_id(BranchId, Data) of
        ok ->
            NewData = store_meta(#{?META_PENDING_BRANCH => undefined}, Data),
            announce_new_history(NewData),

            ?DEBUG("Undid branch ~p", [BranchId]),
            {keep_state, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

handle_get_rsm_snapshot_saver(RSM, RSMPid, Seqno, From, _State, Data) ->
    case need_rsm_snapshot(RSM, Seqno, Data) of
        true ->
            {Pid, NewData} =
                spawn_rsm_snapshot_saver(RSM, RSMPid, Seqno, Data),
            {keep_state,
             NewData,
             {reply, From, {ok, Pid}}};
        false ->
            {keep_state_and_data,
             {reply, From, {error, rejected}}}
    end.

spawn_rsm_snapshot_saver(RSM, RSMPid, Seqno,
                         #data{snapshot_state = SnapshotState,
                               storage = Storage} = Data) ->
    #snapshot_state{savers = Savers,
                    remaining_rsms = RemainingRSMs} = SnapshotState,

    Parent = self(),
    Pid = proc_lib:spawn_link(
            fun () ->
                    Result =
                        try rsm_snapshot_saver(RSM, RSMPid, Seqno, Storage) of
                            R ->
                                R
                        catch
                            T:E:Stacktrace ->
                                ?ERROR("Exception while taking "
                                       "snapshot for RSM ~p~p at seqno ~p: ~p~n"
                                       "Stacktrace:~n~p",
                                       [RSM, RSMPid,
                                        Seqno, {T, E}, Stacktrace]),
                                failed
                        end,

                    %% Make sure to change flush_snapshot_results() if the
                    %% format of the message is modified.
                    Parent ! {snapshot_result, self(), RSM, Result}
            end),

    NewSnapshotState =
        SnapshotState#snapshot_state{
          savers = Savers#{Pid => RSM},
          remaining_rsms = sets:del_element(RSM, RemainingRSMs)},

    {Pid, Data#data{snapshot_state = NewSnapshotState}}.

flush_snapshot_results() ->
    ?FLUSH({snapshot_result, _, _, _}),
    ok.

rsm_snapshot_saver(RSM, RSMPid, Seqno, Storage) ->
    MRef = erlang:monitor(process, RSMPid),

    receive
        {snapshot, Snapshot} ->
            chronicle_storage:save_rsm_snapshot(Seqno, RSM, Snapshot, Storage);
        {'DOWN', MRef, process, RSMPid, Reason} ->
            ?ERROR("RSM ~p~p died with reason ~p "
                   "before passing a snapshot for seqno ~p.",
                   [RSM, RSMPid, Reason, Seqno]),
            failed
    end.

need_rsm_snapshot(RSM, #data{snapshot_state = SnapshotState}) ->
    case SnapshotState of
        undefined ->
            false;
        {retry, _} ->
            false;
        #snapshot_state{seqno = SnapshotSeqno,
                        remaining_rsms = RemainingRSMs} ->
            case sets:is_element(RSM, RemainingRSMs) of
                true ->
                    {true, SnapshotSeqno};
                false ->
                    false
            end
    end.

need_rsm_snapshot(RSM, Seqno, Data) ->
    case need_rsm_snapshot(RSM, Data) of
        {true, NeedSnapshotSeqno} ->
            NeedSnapshotSeqno =:= Seqno;
        false ->
            false
    end.

check_branch_id(BranchId, Data) ->
    OurBranch = get_meta(?META_PENDING_BRANCH, Data),
    case OurBranch of
        undefined ->
            {error, no_branch};
        #branch{history_id = OurBranchId} ->
            case OurBranchId =:= BranchId of
                true ->
                    ok;
                false ->
                    {error, {bad_branch, OurBranch}}
            end
    end.

check_history_id(HistoryId, #data{} = Data) ->
    OurHistoryId = get_effective_history_id(Data),
    true = (OurHistoryId =/= ?NO_HISTORY),
    check_history_id(HistoryId, OurHistoryId);
check_history_id(HistoryId, OurHistoryId) ->
    case HistoryId =:= OurHistoryId of
        true ->
            ok;
        false ->
            {error, {history_mismatch, OurHistoryId}}
    end.

get_effective_history_id(Data) ->
    #{?META_HISTORY_ID := CommittedHistoryId,
      ?META_PENDING_BRANCH := PendingBranch} = get_meta(Data),

    case PendingBranch of
        undefined ->
            CommittedHistoryId;
        #branch{history_id = PendingHistoryId} ->
            PendingHistoryId
    end.

check_same_term(Term, Data) ->
    OurTerm = get_meta(?META_TERM, Data),
    case Term =:= OurTerm of
        true ->
            ok;
        false ->
            {error, {conflicting_term, OurTerm}}
    end.

check_log_range(StartSeqno, EndSeqno, Data) ->
    HighSeqno = get_high_seqno(Data),
    case StartSeqno > HighSeqno
        orelse EndSeqno > HighSeqno
        orelse StartSeqno > EndSeqno of
        true ->
            {error, bad_range};
        false ->
            ok
    end.

init_data() ->
    #data{storage = storage_open(),
          rsms_by_name = #{},
          rsms_by_mref = #{},
          snapshot_readers = #{}}.

get_state_path() ->
    case application:get_env(chronicle, data_dir) of
        {ok, Dir} ->
            {ok, filename:join(Dir, "agent_state")};
        undefined ->
            undefined
    end.

assert_valid_history_id(HistoryId) ->
    true = is_binary(HistoryId).

assert_valid_term(Term) ->
    {TermNumber, _TermLeader} = Term,
    true = is_integer(TermNumber).

assert_valid_branch(#branch{history_id = HistoryId,
                            coordinator = Coordinator}) ->
    assert_valid_history_id(HistoryId),
    assert_valid_peer(Coordinator).

assert_valid_peer(_Coordinator) ->
    %% TODO
    ok.

announce_new_history(Data) ->
    HistoryId = get_effective_history_id(Data),
    Metadata = build_metadata(Data),
    chronicle_events:sync_notify({new_history, HistoryId, Metadata}).

maybe_announce_term_established(Term, Data) ->
    OldTerm = get_meta(?META_TERM, Data),
    case Term =:= OldTerm of
        true ->
            ok;
        false ->
            announce_term_established(Term)
    end.

announce_term_established(State, Term) ->
    case State of
        provisioned ->
            announce_term_established(Term);
        _ ->
            ok
    end.

announce_term_established(Term) ->
    chronicle_events:sync_notify({term_established, Term}).

maybe_announce_new_config(OldData, NewData) ->
    case get_config(OldData) =:= get_config(NewData) of
        true ->
            ok;
        false ->
            announce_new_config(NewData)
    end.

announce_new_config(Data) ->
    Metadata = build_metadata(Data),
    ConfigEntry = get_config(Data),
    Config = ConfigEntry#log_entry.value,
    chronicle_events:sync_notify({new_config, Config, Metadata}).

maybe_announce_committed_seqno(OldData, NewData) ->
    OldCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, OldData),
    NewCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, NewData),
    case OldCommittedSeqno =:= NewCommittedSeqno of
        true ->
            ok;
        false ->
            announce_committed_seqno(NewCommittedSeqno, NewData)
    end.

announce_committed_seqno(CommittedSeqno, Data) ->
    foreach_rsm(
      fun (_Name, Pid) ->
              chronicle_rsm:note_seqno_committed(Pid, CommittedSeqno)
      end, Data).

announce_system_state(SystemState) ->
    announce_system_state(SystemState, no_extra).

announce_system_state(SystemState, Extra) ->
    chronicle_events:sync_notify({system_state, SystemState, Extra}).

announce_joining_cluster(Data) ->
    HistoryId = get_effective_history_id(Data),
    true = (HistoryId =/= ?NO_HISTORY),
    announce_system_state(joining_cluster, HistoryId).

announce_system_provisioned(Data) ->
    announce_system_state(provisioned, build_metadata(Data)).

announce_system_reprovisioned(Data) ->
    chronicle_events:sync_notify({system_event,
                                  reprovisioned, build_metadata(Data)}).

storage_open() ->
    Storage0 = chronicle_storage:open(),
    Meta = chronicle_storage:get_meta(Storage0),
    Storage1 =
        case maps:size(Meta) > 0 of
            true ->
                Storage0;
            false ->
                SeedMeta = #{?META_STATE => not_provisioned,
                             ?META_PEER => ?NO_PEER,
                             ?META_HISTORY_ID => ?NO_HISTORY,
                             ?META_TERM => ?NO_TERM,
                             ?META_TERM_VOTED => ?NO_TERM,
                             ?META_COMMITTED_SEQNO => ?NO_SEQNO,
                             ?META_PENDING_BRANCH => undefined},
                ?INFO("Found empty storage. "
                      "Seeding it with default metadata:~n~p", [SeedMeta]),
                chronicle_storage:store_meta(SeedMeta, Storage0)
        end,

    %% Sync storage to make sure that whatever state is exposed to the outside
    %% world is durable: theoretically it's possible for agent to crash after
    %% writing an update out to storage but before making it durable. This is
    %% meant to deal with such possibility.
    chronicle_storage:sync(Storage1),
    publish_storage(Storage1).

publish_storage(Storage) ->
    chronicle_storage:publish(Storage).

append_entry(Entry, Meta, #data{storage = Storage} = Data) ->
    Seqno = Entry#log_entry.seqno,
    NewStorage = chronicle_storage:append(Seqno, Seqno,
                                          [Entry], #{meta => Meta},
                                          Storage),
    chronicle_storage:sync(NewStorage),
    Data#data{storage = publish_storage(NewStorage)}.

store_meta(Meta, #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:store_meta(Meta, Storage),
    chronicle_storage:sync(NewStorage),
    Data#data{storage = publish_storage(NewStorage)}.

append_entries(StartSeqno, EndSeqno, Entries,
               PreMetadata, PostMetadata, Truncate, Atomic,
               #data{storage = Storage} = Data) ->
    NewStorage0 =
        case Truncate of
            true ->
                %% It's important to truncate diverged entries before logging
                %% metadata. We only truncate on the first append in a new
                %% term. So the metadata will include the new term_voted
                %% value. But if truncate happens after this metadata gets
                %% logged, if the process crashes, upon recovery we might end
                %% up with an untruncated log and the new term_voted. Which
                %% would be in violation of the following invariant: all nodes
                %% with the same term_voted agree on the longest common prefix
                %% of their histories.
                chronicle_storage:truncate(StartSeqno - 1, Storage);
            false ->
                Storage
        end,

    NewStorage  =
        %% TODO: simply make everything atomic?
        case Atomic of
            true ->
                Metadata = maps:merge(PreMetadata, PostMetadata),
                chronicle_storage:append(StartSeqno, EndSeqno,
                                         Entries,
                                         #{meta => Metadata}, NewStorage0);
            false ->
                S0 = chronicle_storage:store_meta(PreMetadata, NewStorage0),
                S1 = chronicle_storage:append(StartSeqno, EndSeqno,
                                              Entries, #{}, S0),
                chronicle_storage:store_meta(PostMetadata, S1)
        end,

    chronicle_storage:sync(NewStorage),
    Data#data{storage = publish_storage(NewStorage)}.

record_snapshot(Seqno, ConfigEntry, #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:record_snapshot(Seqno, ConfigEntry, Storage),
    Data#data{storage = NewStorage}.

install_snapshot(Seqno, ConfigEntry, RSMSnapshots, Metadata,
                 #data{storage = Storage} = Data) ->
    lists:foreach(
      fun ({RSM, RSMSnapshotBinary}) ->
              RSMSnapshot = binary_to_term(RSMSnapshotBinary),
              chronicle_storage:save_rsm_snapshot(Seqno, RSM,
                                                  RSMSnapshot, Storage)
      end, maps:to_list(RSMSnapshots)),

    NewStorage = chronicle_storage:install_snapshot(Seqno, ConfigEntry,
                                                    Metadata, Storage),
    chronicle_storage:sync(NewStorage),
    Data#data{storage = publish_storage(NewStorage)}.

get_peer_name() ->
    Peer = ?PEER(),
    case Peer =:= ?NO_PEER of
        true ->
            exit(nodistribution);
        false ->
            Peer
    end.

get_meta(#data{storage = Storage}) ->
    chronicle_storage:get_meta(Storage).

get_meta(Key, Data) ->
    maps:get(Key, get_meta(Data)).

get_high_seqno(#data{storage = Storage}) ->
    chronicle_storage:get_high_seqno(Storage).

get_config(#data{storage = Storage}) ->
    chronicle_storage:get_config(Storage).

get_config_for_seqno(Seqno, #data{storage = Storage}) ->
    chronicle_storage:get_config_for_seqno(Seqno, Storage).

get_latest_snapshot_seqno(#data{storage = Storage}) ->
    chronicle_storage:get_latest_snapshot_seqno(Storage).

get_and_hold_latest_snapshot(#data{storage = Storage} = Data) ->
    case chronicle_storage:get_and_hold_latest_snapshot(Storage) of
        {Snapshot, NewStorage} ->
            {Snapshot, Data#data{storage = NewStorage}};
        no_snapshot ->
            no_snapshot
    end.

release_snapshot(Seqno, #data{storage = Storage} = Data) ->
    Data#data{storage = chronicle_storage:release_snapshot(Seqno, Storage)}.

get_log_entry(Seqno, #data{storage = Storage}) ->
    chronicle_storage:get_log_entry(Seqno, Storage).

maybe_initiate_snapshot(State, Data) ->
    case State of
        provisioned ->
            maybe_initiate_snapshot(Data);
        _ ->
            %% State machines aren't running before the agent moves to state
            %% 'provisioned'.
            Data
    end.

maybe_initiate_snapshot(#data{snapshot_state = #snapshot_state{}} = Data) ->
    Data;
maybe_initiate_snapshot(#data{snapshot_state = {retry, _}} = Data) ->
    Data;
maybe_initiate_snapshot(Data) ->
    LatestSnapshotSeqno = get_latest_snapshot_seqno(Data),
    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),

    case CommittedSeqno - LatestSnapshotSeqno >= ?SNAPSHOT_INTERVAL of
        true ->
            initiate_snapshot(Data);
        false ->
            Data
    end.

initiate_snapshot(Data) ->
    undefined = Data#data.snapshot_state,

    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
    CommittedConfig = get_config_for_seqno(CommittedSeqno, Data),
    CurrentConfig = get_config(Data),

    CommittedRSMs = get_rsms(CommittedConfig),
    CurrentRSMs = get_rsms(CurrentConfig),

    %% TODO: depending on specifics of how RSM deletions are handled,
    %% it may not be safe when there's an uncommitted config deleting
    %% an RSM. Deal with this once RSM deletions are supported. For
    %% now we are asserting that the set of RSMs is the same in both
    %% configs.
    true = (CommittedRSMs =:= CurrentRSMs),

    TRef = start_snapshot_timer(),

    RSMs = sets:from_list(maps:keys(CommittedRSMs)),
    SnapshotState = #snapshot_state{tref = TRef,
                                    seqno = CommittedSeqno,
                                    config = CommittedConfig,
                                    remaining_rsms = RSMs,
                                    savers = #{}},

    foreach_rsm(
      fun (_Name, Pid) ->
              chronicle_rsm:take_snapshot(Pid, CommittedSeqno)
      end, Data),

    ?INFO("Taking snapshot at seqno ~p.~n"
          "Config:~n~p",
          [CommittedSeqno, CommittedConfig]),

    Data#data{snapshot_state = SnapshotState}.

start_snapshot_timer() ->
    erlang:send_after(?SNAPSHOT_TIMEOUT, self(), snapshot_timeout).

cancel_snapshot_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ?FLUSH(snapshot_timeout).

schedule_retry_snapshot(Data) ->
    NewData = cancel_snapshot(Data),
    TRef = erlang:send_after(?SNAPSHOT_RETRY_AFTER, self(), retry_snapshot),
    NewData#data{snapshot_state = {retry, TRef}}.

cancel_snapshot_retry(Data) ->
    {retry, TRef} = Data#data.snapshot_state,

    _ = erlang:cancel_timer(TRef),
    ?FLUSH(retry_snapshot),
    Data#data{snapshot_state = undefined}.

maybe_cancel_snapshot(#data{snapshot_state = SnapshotState} = Data) ->
    case SnapshotState of
        undefined ->
            Data;
        {retry, _TRef} ->
            cancel_snapshot_retry(Data);
        #snapshot_state{} ->
            cancel_snapshot(Data)
    end.

cancel_snapshot(#data{snapshot_state = SnapshotState,
                      storage = Storage} = Data) ->
    #snapshot_state{tref = TRef,
                    savers = Savers,
                    seqno = SnapshotSeqno} = SnapshotState,

    cancel_snapshot_timer(TRef),
    chronicle_utils:maps_foreach(
      fun (Pid, _RSM) ->
              chronicle_utils:terminate_linked_process(Pid, kill)
      end, Savers),

    flush_snapshot_results(),

    ?INFO("Snapshot at seqno ~p canceled.", [SnapshotSeqno]),
    chronicle_storage:delete_snapshot(SnapshotSeqno, Storage),

    Data#data{snapshot_state = undefined}.

get_rsms(#log_entry{value = Config}) ->
    chronicle_utils:config_rsms(Config).

read_rsm_snapshot(Name, Seqno, Storage) ->
    case chronicle_storage:read_rsm_snapshot(Name, Seqno, Storage) of
        {ok, Snapshot} ->
            Snapshot;
        {error, Error} ->
            exit({get_rsm_snapshot_failed, Name, Seqno, Error})
    end.
