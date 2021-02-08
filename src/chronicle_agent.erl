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
%% TODO: check more state invariants
-module(chronicle_agent).

-compile(export_all).

-export_type([provision_result/0, reprovision_result/0, wipe_result/0,
              prepare_join_result/0, join_cluster_result/0]).

-behavior(gen_statem).
-include("chronicle.hrl").

-import(chronicle_utils, [call/2, call/3, call/4,
                          call_async/4,
                          next_term/2,
                          term_number/1,
                          compare_positions/2,
                          max_position/2,
                          sanitize_entry/1,
                          sanitize_entries/1,
                          sanitize_reason/1,
                          sanitize_stacktrace/1]).

-define(NAME, ?MODULE).
-define(SERVER, ?SERVER_NAME(?NAME)).
-define(SERVER(Peer),
        case Peer of
            ?SELF_PEER ->
                ?SERVER;
            _ ->
                ?SERVER_NAME(Peer, ?NAME)
        end).

-define(PROVISION_TIMEOUT,
        chronicle_settings:get({agent, provision_timeout}, 10000)).
-define(ESTABLISH_LOCAL_TERM_TIMEOUT,
        chronicle_settings:get({agent, establish_local_term_timeout}, 10000)).
-define(LOCAL_MARK_COMMITTED_TIMEOUT,
        chronicle_settings:get({agent, local_mark_committed_timeout}, 5000)).
-define(PREPARE_JOIN_TIMEOUT,
        chronicle_settings:get({agent, prepare_join_timeout}, 10000)).
-define(JOIN_CLUSTER_TIMEOUT,
        chronicle_settings:get({agent, join_cluster_timeout}, 120000)).

-define(INSTALL_SNAPSHOT_TIMEOUT,
        chronicle_settings:get({agent, install_snapshot_timeout}, 120000)).

-define(SNAPSHOT_TIMEOUT,
        chronicle_settings:get({agent, snapshot_timeout}, 60000)).
-define(SNAPSHOT_RETRIES,
        chronicle_settings:get({agent, snapshot_retries}, 5)).
-define(SNAPSHOT_RETRY_AFTER,
        chronicle_settings:get({agent, snapshot_retry_after}, 10000)).
-define(SNAPSHOT_INTERVAL,
        chronicle_settings:get({agent, snapshot_interval}, 100)).

%% Used to indicate that a function will send a message with the provided Tag
%% back to the caller when the result is ready. And the result type is
%% _ReplyType. This is entirely useless for dializer, but is usefull for
%% documentation purposes.
-type maybe_replies(_Tag, _ReplyType) :: chronicle_utils:send_result().
-type peer() :: ?SELF_PEER | chronicle:peer().

-record(snapshot_state, { tref,
                          seqno,
                          history_id,
                          term,
                          config,

                          remaining_rsms,
                          savers }).

-record(prepare_join, { config }).
-record(join_cluster, { from, config, seqno }).

-record(data, { storage,
                rsms_by_name,
                rsms_by_mref,

                snapshot_readers,

                snapshot_attempts = 0,
                snapshot_state :: undefined
                                | {retry, reference()}
                                | #snapshot_state{},

                wipe_state = false :: false | {wiping, From::any()}
              }).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

-spec monitor(peer()) -> reference().
monitor(Peer) ->
    chronicle_utils:monitor_process(?SERVER(Peer)).

-spec get_system_state() ->
          not_provisioned |
          {joining_cluster, chronicle:history_id()} |
          {provisioned, #metadata{}} |
          {removed, #metadata{}}.
get_system_state() ->
    call(?SERVER, get_system_state).

-spec get_metadata() -> #metadata{}.
get_metadata() ->
    case get_system_state() of
        {State, Metadata} when State =:= provisioned;
                               State =:= removed ->
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
      fun (Seqno, _HistoryId,_Term, Config, Storage) ->
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
    with_latest_snapshot(fun get_full_snapshot/5).

get_full_snapshot(Seqno, HistoryId, Term, Config, Storage) ->
    RSMs = get_rsms(Config),
    RSMSnapshots =
        maps:map(
          fun (Name, _) ->
                  Snapshot = read_rsm_snapshot(Name, Seqno, Storage),
                  term_to_binary(Snapshot, [compressed])
          end, RSMs),
    {ok, Seqno, HistoryId, Term, Config, RSMSnapshots}.

with_latest_snapshot(Fun) ->
    case call(?SERVER, {get_latest_snapshot, self()}, 10000) of
        {ok, Ref, Seqno, HistoryId, Term, Config, Storage} ->
            try
                Fun(Seqno, HistoryId, Term, Config, Storage)
            after
                gen_statem:cast(?SERVER, {release_snapshot, Ref})
            end;
        {error, no_snapshot} ->
            {no_snapshot, ?NO_SEQNO};
        {error, Error} ->
            exit(Error)
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

get_term_for_seqno(Seqno) ->
    case get_log_committed(Seqno, Seqno) of
        {ok, [#log_entry{term = Term}]} ->
            {ok, Term};
        {error, compacted} ->
            %% We might still have a snapshot at the given seqno.
            case call(?SERVER, {get_term_for_seqno, Seqno}) of
                {ok, _} = Ok ->
                    Ok;
                {error, compacted} = Error ->
                    Error;
                {error, Error} ->
                    exit(Error)
            end;
        {error, Error} ->
            exit(Error)
    end.

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
            sync_system_state_change();
        Other ->
            Other
    end.

-type reprovision_result() :: ok
                            | {error, reprovision_error()}.
-type reprovision_error() :: {bad_state,
                              not_provisioned | joining_cluster | removed}
                           | {bad_config, peer(), #config{}}.

-spec reprovision() -> reprovision_result().
reprovision() ->
    case call(?SERVER, reprovision, ?PROVISION_TIMEOUT) of
        ok ->
            sync_system_state_change(),

            %% Make sure that chronicle_leader updates published leader
            %% information, so writes don't fail once reprovision() returns.
            sync_leader();
        Other ->
            Other
    end.

-type wipe_result() :: ok.
-spec wipe() -> wipe_result().
wipe() ->
    call(?SERVER, wipe, infinity).

-type prepare_join_result() :: ok | {error, prepare_join_error()}.
-type prepare_join_error() :: provisioned
                            | joining_cluster
                            | {bad_cluster_info, any()}.
-spec prepare_join(chronicle:cluster_info()) -> prepare_join_result().
prepare_join(ClusterInfo) ->
    call(?SERVER, {prepare_join, ClusterInfo}, ?PREPARE_JOIN_TIMEOUT).

-type join_cluster_result() :: ok | {error, join_cluster_error()}.
-type join_cluster_error() :: not_prepared
                            | {history_mismatch, chronicle:history_id()}
                            | {not_in_peers,
                               chronicle:peer(), [chronicle:peer()]}
                            | wipe_requested
                            | {bad_cluster_info, any()}.
-spec join_cluster(chronicle:cluster_info()) -> join_cluster_result().
join_cluster(ClusterInfo) ->
    case call(?SERVER, {join_cluster, ClusterInfo}, ?JOIN_CLUSTER_TIMEOUT) of
        ok ->
            sync_system_state_change();
        Other ->
            Other
    end.

-type establish_term_result() ::
        {ok, #metadata{}} |
        {error, establish_term_error()}.

-type establish_term_error() ::
        {bad_state, not_provisioned | removed} |
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
        {bad_state, not_provisioned | removed} |
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
        {bad_state, not_provisioned | removed} |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {missing_entries, #metadata{}} |
        {protocol_error, any()}.

-spec append(peer(),
             Opaque,
             chronicle:history_id(),
             chronicle:leader_term(),
             chronicle:seqno(),
             chronicle:leader_term(),
             chronicle:seqno(),
             [#log_entry{}],
             chronicle_utils:send_options()) ->
          maybe_replies(Opaque, append_result()).
append(Peer, Opaque, HistoryId, Term,
       CommittedSeqno,
       AtTerm, AtSeqno, Entries, Options) ->
    call_async(?SERVER(Peer), Opaque,
               {append, HistoryId, Term,
                CommittedSeqno, AtTerm, AtSeqno, Entries},
               Options).

-type install_snapshot_result() :: ok | {error, install_snapshot_error()}.
-type install_snapshot_error() ::
        {bad_state, not_provisioned | removed} |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {protocol_error, any()}.

-spec install_snapshot(peer(),
                       chronicle:history_id(),
                       chronicle:leader_term(),
                       chronicle:seqno(),
                       chronicle:history_id(),
                       chronicle:leader_term(),
                       ConfigEntry::#log_entry{},
                       #{RSM::atom() => RSMSnapshot::binary()}) ->
          install_snapshot_result().
install_snapshot(Peer, HistoryId, Term,
                 SnapshotSeqno, SnapshotHistoryId,
                 SnapshotTerm, SnapshotConfig, RSMSnapshots) ->
    call(?SERVER(Peer),
         {install_snapshot,
          HistoryId, Term,
          SnapshotSeqno, SnapshotHistoryId,
          SnapshotTerm, SnapshotConfig, RSMSnapshots},
         install_snapshot,
         ?INSTALL_SNAPSHOT_TIMEOUT).

-type local_mark_committed_result() ::
        ok | {error, local_mark_committed_error()}.
-type local_mark_committed_error() ::
        {bad_state, not_provisioned | joining_cluster | removed} |
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

-type store_branch_error() ::
        {bad_state, not_provisioned | joining_cluster | removed} |
        {not_in_peers, chronicle:peer(), [chronicle:peer()]} |
        {history_mismatch, OurHistory::chronicle:history_id()}.

-type local_store_branch_result() :: ok | {error, store_branch_error()}.

-spec local_store_branch(#branch{}, timeout()) -> local_store_branch_result().
local_store_branch(Branch, Timeout) ->
    call(?SERVER, {store_branch, Branch}, Timeout).

-spec store_branch([chronicle:peer()], #branch{}, timeout()) ->
          chronicle_utils:multi_call_result(ok, {error, store_branch_error()}).
store_branch(Peers, Branch, Timeout) ->
    chronicle_utils:multi_call(Peers, ?NAME,
                               {store_branch, Branch},
                               fun (Result) ->
                                       case Result of
                                           ok ->
                                               true;
                                           _ ->
                                               false
                                       end
                               end,
                               Timeout).

-type undo_branch_error() :: no_branch
                           | {bad_branch, TheirBranch::#branch{}}.
-type undo_branch_result() :: chronicle_utils:multi_call_result(
                                ok,
                                {error, undo_branch_error()}).

-spec undo_branch([chronicle:peer()], chronicle:history_id(), timeout()) ->
          undo_branch_result().
undo_branch(Peers, BranchId, Timeout) ->
    chronicle_utils:multi_call(Peers, ?NAME,
                               {undo_branch, BranchId},
                               fun (Result) ->
                                       Result =:= ok
                               end,
                               Timeout).

sync_system_state_change() ->
    ok = chronicle_secondary_sup:sync().

sync_leader() ->
    ok = chronicle_leader:sync().

is_wipe_requested() ->
    call(?SERVER, is_wipe_requested).

mark_removed(Peer, PeerId) ->
    call(?SERVER, {mark_removed, Peer, PeerId}).

check_member(HistoryId, Peer, PeerId, PeerSeqno) ->
    call(?SERVER, {check_member, HistoryId, Peer, PeerId, PeerSeqno}).

%% gen_statem callbacks
callback_mode() ->
    handle_event_function.

sanitize_event({call, _} = Type,
               {append, HistoryId, Term, CommittedSeqno, AtTerm, AtSeqno, _}) ->
    {Type, {append, HistoryId, Term, CommittedSeqno, AtTerm, AtSeqno, '...'}};
sanitize_event({call, _} = Type,
               {install_snapshot,
                HistoryId, Term,
                SnapshotSeqno, SnapshotTerm, SnapshotConfig, _}) ->
    {Type, {install_snapshot,
            HistoryId, Term,
            SnapshotSeqno, SnapshotTerm, SnapshotConfig, '...'}};
sanitize_event(Type, Event) ->
    {Type, Event}.

init([]) ->
    Data = init_data(),
    State =
        case get_meta(?META_STATE, Data) of
            ?META_STATE_PROVISIONED ->
                provisioned;
            {?META_STATE_PREPARE_JOIN, #{config := Config}} ->
                #prepare_join{config = Config};
            {?META_STATE_JOIN_CLUSTER, #{config := Config, seqno := Seqno}} ->
                #join_cluster{config = Config, seqno = Seqno};
            ?META_STATE_NOT_PROVISIONED ->
                not_provisioned;
            ?META_STATE_REMOVED ->
                removed
        end,

    %% It's possible the agent crashed right before logging the new state to
    %% disk.
    {FinalState, FinalData} = check_state_transitions(State, Data),

    %% Make sure clients are woken up to check the latest state. Important if
    %% chronicle_agent restarts and forgets to send out some notifications.
    announce_system_state_changed(),
    publish_settings(FinalData),

    {ok, FinalState, FinalData}.

handle_event({call, From}, Call, State, Data) ->
    handle_call(Call, From, State, Data);
handle_event(cast, {release_snapshot, Ref}, State, Data) ->
    handle_release_snapshot(Ref, State, Data);
handle_event(cast, prepare_wipe_done, State, Data) ->
    handle_prepare_wipe_done(State, Data);
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
handle_call({get_term_for_seqno, Seqno}, From, State, Data) ->
    handle_get_term_for_seqno(Seqno, From, State, Data);
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
handle_call(is_wipe_requested, From, State, Data) ->
    handle_is_wipe_requested(From, State, Data);
handle_call({prepare_join, ClusterInfo}, From, State, Data) ->
    handle_prepare_join(ClusterInfo, From, State, Data);
handle_call({join_cluster, ClusterInfo}, From, State, Data) ->
    handle_join_cluster(ClusterInfo, From, State, Data);
handle_call({establish_term, HistoryId, Term}, From, State, Data) ->
    %% TODO: consider simply skipping the position check for this case
    Position = {get_high_term(Data), get_high_seqno(Data)},
    handle_establish_term(HistoryId, Term, Position, From, State, Data);
handle_call({establish_term, HistoryId, Term, Position}, From, State, Data) ->
    handle_establish_term(HistoryId, Term, Position, From, State, Data);
handle_call({ensure_term, HistoryId, Term}, From, State, Data) ->
    handle_ensure_term(HistoryId, Term, From, State, Data);
handle_call({append, HistoryId, Term, CommittedSeqno, AtTerm, AtSeqno, Entries},
            From, State, Data) ->
    handle_append(HistoryId, Term, CommittedSeqno,
                  AtTerm, AtSeqno, Entries, From, State, Data);
handle_call({local_mark_committed, HistoryId, Term, CommittedSeqno},
            From, State, Data) ->
    handle_local_mark_committed(HistoryId, Term,
                                CommittedSeqno, From, State, Data);
handle_call({install_snapshot,
             HistoryId, Term,
             SnapshotSeqno, SnapshotHistoryId,
             SnapshotTerm, SnapshotConfig, RSMSnapshots},
            From, State, Data) ->
    handle_install_snapshot(HistoryId, Term,
                            SnapshotSeqno, SnapshotHistoryId,
                            SnapshotTerm, SnapshotConfig,
                            RSMSnapshots, From, State, Data);
handle_call({store_branch, Branch}, From, State, Data) ->
    handle_store_branch(Branch, From, State, Data);
handle_call({undo_branch, BranchId}, From, State, Data) ->
    handle_undo_branch(BranchId, From, State, Data);
handle_call({get_rsm_snapshot_saver, RSM, RSMPid, Seqno}, From, State, Data) ->
    handle_get_rsm_snapshot_saver(RSM, RSMPid, Seqno, From, State, Data);
handle_call({mark_removed, Peer, PeerId}, From, State, Data) ->
    handle_mark_removed(Peer, PeerId, From, State, Data);
handle_call({check_member, HistoryId, Peer, PeerId, PeerSeqno},
            From, State, Data) ->
    handle_check_member(HistoryId, Peer, PeerId, PeerSeqno, From, State, Data);
handle_call(_Call, From, _State, _Data) ->
    {keep_state_and_data,
     {reply, From, nack}}.

terminate(_Reason, Data) ->
    maybe_cancel_snapshot(Data).

%% internal
handle_get_system_state(From, State, Data) ->
    ExtState = get_external_state(State),
    Reply =
        case ExtState of
            joining_cluster ->
                HistoryId = get_meta(?META_HISTORY_ID, Data),
                true = (HistoryId =/= ?NO_HISTORY),

                {ExtState, HistoryId};
            _ when ExtState =:= provisioned;
                   ExtState =:= removed ->
                {ExtState, build_metadata(Data)};
            _ ->
                ExtState
        end,
    {keep_state_and_data, {reply, From, Reply}}.

get_external_state(State) ->
    case State of
        provisioned ->
            provisioned;
        #prepare_join{} ->
            joining_cluster;
        #join_cluster{} ->
            joining_cluster;
        not_provisioned ->
            not_provisioned;
        removed ->
            removed
    end.

build_metadata(Data) ->
    #{?META_PEER := Peer,
      ?META_PEER_ID := PeerId,
      ?META_HISTORY_ID := HistoryId,
      ?META_TERM := Term,
      ?META_COMMITTED_SEQNO := CommittedSeqno,
      ?META_PENDING_BRANCH := PendingBranch} = get_meta(Data),

    #metadata{peer = Peer,
              peer_id = PeerId,
              history_id = HistoryId,
              term = Term,
              high_term = get_high_term(Data),
              high_seqno = get_high_seqno(Data),
              committed_seqno = CommittedSeqno,
              config = get_config(Data),
              pending_branch = PendingBranch}.

handle_check_grant_vote(PeerHistoryId, PeerPosition, From, State, Data) ->
    Reply =
        case ?CHECK(check_prepared(State),
                    check_history_id(PeerHistoryId, Data),
                    check_peer_current(PeerPosition, State, Data)) of
            ok ->
                {ok, get_meta(?META_TERM, Data)};
            {error, _} = Error ->
                Error
        end,

    {keep_state_and_data, {reply, From, Reply}}.

check_prepared(State) ->
    case State of
        _ when State =:= not_provisioned;
               State =:= removed ->
            {error, {bad_state, get_external_state(State)}};
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

handle_get_term_for_seqno(Seqno, From, _State,
                          #data{storage = Storage} = Data) ->
    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
    Reply =
        case Seqno =< CommittedSeqno of
            true ->
                chronicle_storage:get_term_for_seqno(Seqno, Storage);
            false ->
                {error, {uncommitted, Seqno, CommittedSeqno}}
        end,

    {keep_state_and_data, {reply, From, Reply}}.

handle_register_rsm(Name, Pid, From, State,
                    #data{rsms_by_name = RSMs,
                          rsms_by_mref = MRefs} = Data) ->
    case check_register_rsm(Name, State, Data) of
        ok ->
            NewData0 = maybe_cleanup_old_rsm(Name, Data),

            ?DEBUG("Registering RSM ~p with pid ~p", [Name, Pid]),

            MRef = erlang:monitor(process, Pid),
            NewRSMs = RSMs#{Name => {MRef, Pid}},
            NewMRefs = MRefs#{MRef => Name},

            CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, NewData0),
            Info0 = #{committed_seqno => CommittedSeqno},
            Info1 = case need_rsm_snapshot(Name, NewData0) of
                        {true, NeedSnapshotSeqno} ->
                            Info0#{need_snapshot_seqno => NeedSnapshotSeqno};
                        false ->
                            Info0
                    end,

            Reply = {ok, Info1},
            NewData = NewData0#data{rsms_by_name = NewRSMs,
                                    rsms_by_mref = NewMRefs},

            {keep_state, NewData, {reply, From, Reply}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_provisioned_or_removed(State) ->
    case State of
        provisioned ->
            ok;
        removed ->
            ok;
        _ ->
            {error, {bad_state, get_external_state(State)}}
    end.

check_register_rsm(Name, State, #data{rsms_by_name = RSMs}) ->
    case check_provisioned_or_removed(State) of
        ok ->
            case maps:find(Name, RSMs) of
                {ok, {_, OtherPid}} ->
                    case is_process_alive(OtherPid) of
                        true ->
                            {error, {already_registered, Name, OtherPid}};
                        false ->
                            ok
                    end;
                error ->
                    ok
            end;
        {error, _} = Error ->
            Error
    end.

maybe_cleanup_old_rsm(Name, #data{rsms_by_name = RSMs} = Data) ->
    case maps:find(Name, RSMs) of
        {ok, {MRef, Pid}} ->
            handle_rsm_down(Name, MRef, Pid, Data);
        error ->
            Data
    end.

handle_get_latest_snapshot(Pid, From, State,
                           #data{snapshot_readers = Readers,
                                 storage = Storage} = Data) ->
    case check_provisioned_or_removed(State) of
        ok ->
            case get_and_hold_latest_snapshot(Data) of
                {{Seqno, HistoryId, Term, Config}, NewData0} ->
                    MRef = erlang:monitor(process, Pid),
                    NewReaders = Readers#{MRef => Seqno},
                    NewData = NewData0#data{snapshot_readers = NewReaders},
                    Reply = {ok, MRef, Seqno, HistoryId, Term, Config, Storage},

                    {keep_state,
                     NewData,
                     {reply, From, Reply}};
                no_snapshot ->
                    {keep_state_and_data,
                     {reply, From, {error, no_snapshot}}}
            end;
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

handle_release_snapshot(MRef, _State,
                        #data{snapshot_readers = Readers} = Data) ->
    {SnapshotSeqno, NewReaders} = maps:take(MRef, Readers),
    erlang:demonitor(MRef, [flush]),
    {keep_state, release_snapshot(SnapshotSeqno,
                                  Data#data{snapshot_readers = NewReaders})}.

handle_down(MRef, Pid, Reason, State,
            #data{rsms_by_mref = MRefs,
                  snapshot_readers = Readers} = Data) ->
    case maps:find(MRef, MRefs) of
        error ->
            case maps:is_key(MRef, Readers) of
                true ->
                    handle_release_snapshot(MRef, State, Data);
                false ->
                    {stop, {unexpected_process_down, MRef, Pid, Reason}, Data}
            end;
        {ok, Name} ->
            {keep_state, handle_rsm_down(Name, MRef, Pid, Data)}
    end.

handle_rsm_down(Name, MRef, Pid, #data{rsms_by_name = RSMs,
                                       rsms_by_mref = MRefs} = Data) ->
    ?DEBUG("RSM ~p~p terminated", [Name, Pid]),

    erlang:demonitor(MRef, [flush]),
    NewRSMs = maps:remove(Name, RSMs),
    NewMRefs = maps:remove(MRef, MRefs),

    Data#data{rsms_by_name = NewRSMs, rsms_by_mref = NewMRefs}.

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
                    history_id = HistoryId,
                    term = Term,
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
            {keep_state,
             record_snapshot(Seqno, HistoryId, Term, Config, NewData)};
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
        {ok, OldPeer, Config} ->
            #{?META_HISTORY_ID := HistoryId,
              ?META_TERM := Term} = get_meta(Data),

            HighSeqno = get_high_seqno(Data),
            Peer = get_peer_name(),
            NewTerm = next_term(Term, Peer),
            NewConfig = chronicle_config:reinit(Peer, OldPeer, Config),
            Seqno = HighSeqno + 1,

            ConfigEntry = #log_entry{history_id = HistoryId,
                                     term = NewTerm,
                                     seqno = Seqno,
                                     value = NewConfig},

            ?DEBUG("Reprovisioning peer with config:~n~p", [ConfigEntry]),

            NewData = append_entry(ConfigEntry,
                                   #{?META_PEER => Peer,
                                     ?META_TERM => NewTerm,
                                     ?META_COMMITTED_SEQNO => Seqno},
                                   Data),

            announce_system_reprovisioned(NewData),
            handle_new_config(NewData),
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
            case chronicle_config:is_stable(Config) of
                true ->
                    Voters = chronicle_config:get_voters(Config),
                    Replicas = chronicle_config:get_replicas(Config),

                    case Voters =:= [Peer] andalso Replicas =:= [] of
                        true ->
                            {ok, Peer, Config};
                        false ->
                            {error, {bad_config, Peer, Config}}
                    end;
                false ->
                    {error, {bad_config, Peer, Config}}
            end;
        Error ->
            Error
    end.

handle_provision(Machines, From, State, Data) ->
    case check_not_provisioned(State) of
        ok ->
            Peer = get_peer_name(),
            HistoryId = chronicle_utils:random_uuid(),
            Term = next_term(?NO_TERM, Peer),
            Seqno = 1,

            Config = chronicle_config:init(Peer, Machines),
            ConfigEntry = #log_entry{history_id = HistoryId,
                                     term = Term,
                                     seqno = Seqno,
                                     value = Config},

            {ok, PeerId} = chronicle_config:get_peer_id(Peer, Config),

            ?DEBUG("Provisioning with history ~p. Config:~n~p",
                   [HistoryId, Config]),

            NewData = append_entry(ConfigEntry,
                                   #{?META_STATE => ?META_STATE_PROVISIONED,
                                     ?META_PEER => Peer,
                                     ?META_PEER_ID => PeerId,
                                     ?META_HISTORY_ID => HistoryId,
                                     ?META_TERM => Term,
                                     ?META_COMMITTED_SEQNO => Seqno},
                                   Data),

            announce_system_provisioned(NewData),
            handle_new_config(NewData),

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
            {error, {bad_state, get_external_state(State)}}
    end.

handle_wipe(From, State, #data{wipe_state = WipeState} = Data) ->
    case State of
        not_provisioned ->
            false = WipeState,
            {keep_state_and_data, {reply, From, ok}};
        _ ->
            case WipeState of
                {wiping, _} ->
                    %% Wipe is already in progress. Postpone the event, it'll
                    %% get responded to once the state becomes
                    %% 'not_provisioned'.
                    postpone;
                false ->
                    ?INFO("Wipe requested."),

                    announce_system_wiping(),

                    %% Wait for chronicle_secondary_sup to terminate all
                    %% downstream processes. They may be holding snapshots, so
                    %% we wouldn't be able to safely delete those. Waiting
                    %% needs to be done without blocking the gen_statem loop,
                    %% because some of the processes may be blocked on
                    %% synchronous calls to chronicle_agent itself.
                    Self = self(),
                    proc_lib:spawn_link(
                      fun () ->
                              sync_system_state_change(),
                              gen_statem:cast(Self, prepare_wipe_done)
                      end),

                    NewData = Data#data{wipe_state = {wiping, From}},
                    {keep_state, maybe_cancel_snapshot(NewData)}
            end
    end.

handle_is_wipe_requested(From, _State, Data) ->
    {keep_state_and_data, {reply, From, is_wipe_requested(Data)}}.

is_wipe_requested(#data{wipe_state = WipeState}) ->
    case WipeState of
        {wiping, _} ->
            true;
        false ->
            false
    end.

handle_prepare_wipe_done(State, Data) ->
    ?INFO("All secondary processes have terminated."),

    true = (State =/= not_provisioned),
    undefined = Data#data.snapshot_state,

    {wiping, From} = Data#data.wipe_state,

    %% All RSMs and snapshot readers should be stopped by now, but it's
    %% possible(?) that we haven't processed all DOWN messages yet.
    MRefs =
        maps:keys(Data#data.snapshot_readers) ++
        maps:keys(Data#data.rsms_by_mref),
    lists:foreach(
      fun (MRef) ->
              erlang:demonitor(MRef, [flush])
      end, MRefs),

    ?INFO("Wiping"),
    chronicle_storage:close(Data#data.storage),
    chronicle_storage:wipe(),
    ?INFO("Wiped successfully"),

    gen_statem:reply(From, ok),

    NewData = init_data(),
    publish_settings(NewData),
    {next_state, not_provisioned, NewData}.

handle_prepare_join(ClusterInfo, From, State, Data) ->
    case ?CHECK(check_not_provisioned(State),
                check_cluster_info(ClusterInfo)) of
        ok ->
            #{history_id := HistoryId,
              config := Config} = ClusterInfo,
            Peer = get_peer_name(),
            Meta = #{?META_HISTORY_ID => HistoryId,
                     ?META_PEER => Peer,
                     ?META_STATE => {?META_STATE_PREPARE_JOIN,
                                     #{config => Config}}},
            NewData = store_meta(Meta, Data),
            announce_joining_cluster(HistoryId),
            {next_state,
             #prepare_join{config = Config}, NewData,
             {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_cluster_info(ClusterInfo) ->
    case ClusterInfo of
        #{history_id := HistoryId,
          committed_seqno := CommittedSeqno,
          config := #log_entry{value = #config{}}}
          when is_binary(HistoryId),
               is_integer(CommittedSeqno) ->
            ok;
        _ ->
            {error, {bad_cluster_info, ClusterInfo}}
    end.

handle_join_cluster(ClusterInfo, From, State, Data) ->
    case check_join_cluster(ClusterInfo, State, Data) of
        {ok, Config, Seqno} ->
            Peer = get_meta(?META_PEER, Data),
            {ok, PeerId} =
                chronicle_config:get_peer_id(Peer, Config#log_entry.value),

            Meta = #{?META_PEER_ID => PeerId,
                     ?META_STATE => {?META_STATE_JOIN_CLUSTER,
                                     #{seqno => Seqno,
                                       config => Config}}},
            NewData = store_meta(Meta, Data),
            NewState = #join_cluster{from = From,
                                     seqno = Seqno,
                                     config = Config},

            %% We might already have all entries we need.
            {FinalState, FinalData} =
                check_state_transitions(NewState, NewData),

            {next_state, FinalState, FinalData};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_join_cluster(ClusterInfo, State, Data) ->
    case State of
        #prepare_join{} ->
            case check_cluster_info(ClusterInfo) of
                ok ->
                    #{history_id := HistoryId,
                      config := Config,
                      committed_seqno := Seqno} = ClusterInfo,

                    case ?CHECK(check_history_id(HistoryId, Data),
                                check_in_peers(Config, Data),
                                check_not_wipe_requested(Data)) of
                        ok ->
                            {ok, Config, Seqno};
                        {error, _} = Error ->
                            Error
                    end
            end;
        _ ->
            {error, not_prepared}
    end.

check_not_wipe_requested(Data) ->
    case is_wipe_requested(Data) of
        false ->
            ok;
        true ->
            {error, wipe_requested}
    end.

check_in_peers(#log_entry{value = Config}, Data) ->
    Peers = chronicle_config:get_peers(Config),

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
            %% Don't transition if wipe is in progress. This will confuse
            %% chronicle_secondary_sup which expects that once wipe has
            %% started, eventually the system will transition to
            %% not_provisioned state without any intermediate states.
            %%
            %% TODO: this extreme coupling between the two processes is
            %% super-annoying. Do something once there's more time.
            NotWipeRequested = not is_wipe_requested(Data),

            CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
            case CommittedSeqno >= WaitedSeqno andalso NotWipeRequested of
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
           check_peer_current(Position, State, Data)).

check_later_term(Term, Data) ->
    CurrentTerm = get_meta(?META_TERM, Data),
    case term_number(Term) > term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

check_peer_current(Position, State, Data) ->
    RequiredPosition = get_required_peer_position(State, Data),
    case compare_positions(Position, RequiredPosition) of
        lt ->
            {error, {behind, RequiredPosition}};
        _ ->
            ok
    end.

get_position(Data) ->
    OurHighTerm = get_high_term(Data),
    OurHighSeqno = get_high_seqno(Data),
    {OurHighTerm, OurHighSeqno}.

get_required_peer_position(State, Data) ->
    OurPosition = get_position(Data),

    %% When a node is being added to a cluster, it starts in an empty
    %% state. So it'll accept vote solicitations from any node with the
    %% correct history id. But it's possible that the node will get contacted
    %% by nodes that were previsouly removed from the cluster (if those don't
    %% know that they got removed). For this to happen the node being added
    %% must have previsouly been part of the cluster together with the removed
    %% nodes. The latter nodes will have the correct history id and will be
    %% able to get the added node to vote for them. Which may give one of
    %% those nodes enough votes to become the leader, which must never happen.
    %%
    %% To protect against this the node will reject vote solicitations from
    %% nodes that are not aware of the latest config as of when the node add
    %% sequence was initiated.
    case config_position(State) of
        {ok, ConfigPosition} ->
            max_position(ConfigPosition, OurPosition);
        false ->
            OurPosition
    end.

config_position(#prepare_join{config = Config}) ->
    {ok, entry_position(Config)};
config_position(#join_cluster{config = Config}) ->
    {ok, entry_position(Config)};
config_position(_) ->
    false.

entry_position(#log_entry{term = Term, seqno = Seqno}) ->
    {Term, Seqno}.

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
              CommittedSeqno, AtTerm, AtSeqno, Entries, From, State, Data) ->
    assert_valid_history_id(HistoryId),
    assert_valid_term(Term),

    case check_append(HistoryId, Term,
                      CommittedSeqno, AtTerm, AtSeqno, Entries, State, Data) of
        {ok, Info} ->
            complete_append(HistoryId, Term, Info, From, State, Data);
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

complete_append(HistoryId, Term, Info, From, State, Data) ->
    #{entries := Entries,
      start_seqno := StartSeqno,
      end_seqno := EndSeqno,
      committed_seqno := NewCommittedSeqno,
      truncate := Truncate,
      commit_branch := CommitBranch} = Info,

    Metadata0 = #{?META_TERM => Term,
                  ?META_COMMITTED_SEQNO => NewCommittedSeqno},
    Metadata =
        case CommitBranch of
            true ->
                Metadata0#{?META_HISTORY_ID => HistoryId,
                           ?META_PENDING_BRANCH => undefined};
            false ->
                Metadata0
        end,

    NewData0 =
        case Entries =/= [] orelse Truncate of
            true ->
                append_entries(StartSeqno, EndSeqno,
                               Entries, Metadata, Truncate, Data);
            false ->
                store_meta(Metadata, Data)
        end,

    %% TODO: in-progress snapshots might need to be canceled if any of the
    %% state machines get deleted.

    {NewState, NewData} =
        post_append(Term, NewCommittedSeqno, State, Data, NewData0),

    {next_state, NewState, NewData, {reply, From, ok}}.

post_append(Term, NewCommittedSeqno, State, OldData, NewData) ->
    {FinalState, FinalData} =
        case State of
            provisioned ->
                maybe_announce_term_established(Term, OldData),
                check_new_config(OldData, NewData),

                OldCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, OldData),
                case OldCommittedSeqno =:= NewCommittedSeqno of
                    true ->
                        {State, NewData};
                    false ->
                        announce_committed_seqno(NewCommittedSeqno, NewData),
                        check_got_removed(OldCommittedSeqno,
                                          NewCommittedSeqno, State, NewData)
                end;
            _ ->
                check_state_transitions(State, NewData)
        end,

    {FinalState, maybe_initiate_snapshot(FinalState, FinalData)}.

check_append(HistoryId, Term, CommittedSeqno,
             AtTerm, AtSeqno, Entries, State, Data) ->
    ?CHECK(check_prepared(State),
           check_append_history_id(HistoryId, Entries, Data),
           check_not_earlier_term(Term, Data),
           check_append_obsessive(HistoryId, Term, CommittedSeqno,
                                  AtTerm, AtSeqno, Entries, Data)).

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
                        [] ->
                            %% Partial append.
                            ok;
                        [#log_entry{history_id = HistoryId,
                                    value = #config{}} | _] ->
                            ok;
                        _ ->
                            ?ERROR("Malformed entries in an append "
                                   "request starting a new history.~n"
                                   "Old history id: ~p~n"
                                   "New history id: ~p~n"
                                   "Entries:~n~p",
                                   [OldHistoryId, HistoryId,
                                    sanitize_entries(NewHistoryEntries)]),
                            {error, {protocol_error,
                                     {missing_config_starting_history,
                                      OldHistoryId, HistoryId,
                                      sanitize_entries(NewHistoryEntries)}}}
                    end
            end;
        {error, _} = Error ->
            Error
    end.

check_append_obsessive(HistoryId, Term,
                       CommittedSeqno, AtTerm, AtSeqno, Entries, Data) ->
    OurHighSeqno = get_high_seqno(Data),
    case AtSeqno > OurHighSeqno of
        true ->
            {error, {missing_entries, build_metadata(Data)}};
        false ->
            OurCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
            case check_at_seqno(AtTerm, AtSeqno, OurCommittedSeqno, Data) of
                ok ->
                    case preprocess_entries(HistoryId, AtSeqno, Entries,
                                            OurCommittedSeqno, OurHighSeqno,
                                            Data) of
                        {ok,
                         StartSeqno, EndSeqno,
                         CommitBranch, Truncate, FinalEntries} ->
                            case check_committed_seqno(Term, CommittedSeqno,
                                                       EndSeqno, Data) of
                                {ok, FinalCommittedSeqno} ->
                                    {ok,
                                     #{entries => FinalEntries,
                                       start_seqno => StartSeqno,
                                       end_seqno => EndSeqno,
                                       committed_seqno => FinalCommittedSeqno,
                                       truncate => Truncate,
                                       commit_branch => CommitBranch}};
                                {error, _} = Error ->
                                    Error
                            end;
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

check_at_seqno(AtTerm, AtSeqno, CommittedSeqno,
               #data{storage = Storage} = Data) ->
    case chronicle_storage:get_term_for_seqno(AtSeqno, Storage) of
        {ok, Term}
          when Term =:= AtTerm ->
            ok;
        {ok, Term} ->
            case AtSeqno > CommittedSeqno of
                true ->
                    {error, {missing_entries, build_metadata(Data)}};
                false ->
                    %% We got a term mismatch on a committed entry. This must
                    %% not happen.
                    ?ERROR("Term mismatch at seqno ~p when "
                           "the entry is committed.~n"
                           "Our term: ~p~n"
                           "Leader term: ~p",
                           [AtSeqno, Term, AtTerm]),

                    {error,
                     {protocol_error,
                      {at_seqno_mismatch,
                       AtTerm, AtSeqno, CommittedSeqno, Term}}}
            end;
        {error, compacted} ->
            true = (AtSeqno =< CommittedSeqno),

            %% Since AtSeqno is committed, consider the terms match.
            ok
    end.

preprocess_entries(HistoryId, AtSeqno, Entries,
                   CommittedSeqno, HighSeqno, Data) ->
    case preprocess_entries_loop(AtSeqno, Entries,
                                 CommittedSeqno, HighSeqno, Data) of
        {ok, StartSeqno, EndSeqno, LastHistoryId, Truncate, FinalEntries} ->
            CommitBranch = (LastHistoryId =:= HistoryId),
            {ok, StartSeqno, EndSeqno, CommitBranch, Truncate, FinalEntries};
        {error, {malformed, Entry}} ->
            ?ERROR("Received an ill-formed append request.~n"
                   "At seqno: ~p"
                   "Stumbled upon this entry: ~p~n"
                   "Some entries:~n~p",
                   [AtSeqno,
                    sanitize_entry(Entry),
                    sanitize_entries(Entries)]),
            {error, {protocol_error,
                     {malformed_append,
                      sanitize_entry(Entry),
                      AtSeqno,
                      sanitize_entries(Entries)}}};
        {error, _} = Error ->
            Error
    end.

preprocess_entries_loop(PrevSeqno, [], _CommittedSeqno, _HighSeqno, _Data) ->
    {ok, PrevSeqno + 1, PrevSeqno, ?NO_HISTORY, false, []};
preprocess_entries_loop(PrevSeqno,
                        [Entry | RestEntries] = Entries,
                        CommittedSeqno, HighSeqno, Data) ->
    EntrySeqno = Entry#log_entry.seqno,
    case PrevSeqno + 1 =:= EntrySeqno of
        true ->
            case EntrySeqno > HighSeqno of
                true ->
                    case get_entries_seqnos(PrevSeqno, Entries) of
                        {ok, StartSeqno, EndSeqno, HistoryId} ->
                            {ok,
                             StartSeqno, EndSeqno,
                             HistoryId, false, Entries};
                        {error, _} = Error ->
                            Error
                    end;
                false ->
                    case get_log_entry(EntrySeqno, Data) of
                        %% TODO: it should be enough to compare histories and
                        %% terms here. But for now let's compare complete
                        %% entries to be doubly confident.
                        {ok, OurEntry}
                          when Entry =:= OurEntry ->
                            preprocess_entries_loop(EntrySeqno, RestEntries,
                                                    CommittedSeqno, HighSeqno,
                                                    Data);
                        {ok, OurEntry} ->
                            case EntrySeqno > CommittedSeqno of
                                true ->
                                    case get_entries_seqnos(PrevSeqno,
                                                            Entries) of
                                        {ok, StartSeqno, EndSeqno, HistoryId} ->
                                            {ok,
                                             StartSeqno, EndSeqno, HistoryId,
                                             true, Entries};
                                        {error, _} = Error ->
                                            Error
                                    end;
                                false ->
                                    ?ERROR("Unexpected mismatch in entries "
                                           "sent by the proposer.~n"
                                           "Seqno: ~p~n"
                                           "Committed seqno: ~p~n"
                                           "Our entry:~n~p~n"
                                           "Received entry:~n~p",
                                           [EntrySeqno, CommittedSeqno,
                                            sanitize_entry(OurEntry),
                                            sanitize_entry(Entry)]),
                                    {error,
                                     {protocol_error,
                                      {mismatched_entry,
                                       EntrySeqno, CommittedSeqno,
                                       sanitize_entry(OurEntry),
                                       sanitize_entry(Entry)}}}
                            end;
                        {error, not_found} ->
                            %% The entry must have been compacted.
                            true = (EntrySeqno =< CommittedSeqno),

                            %% Assume the entries matched
                            preprocess_entries_loop(EntrySeqno, RestEntries,
                                                    CommittedSeqno, HighSeqno,
                                                    Data)
                    end
            end;
        false ->
            {error, {malformed, Entry}}
    end.

get_entries_seqnos(AtSeqno, Entries) ->
    get_entries_seqnos_loop(Entries, AtSeqno + 1, AtSeqno, ?NO_HISTORY).

get_entries_seqnos_loop([], StartSeqno, EndSeqno, HistoryId) ->
    {ok, StartSeqno, EndSeqno, HistoryId};
get_entries_seqnos_loop([Entry|Rest], StartSeqno, EndSeqno, _HistoryId) ->
    Seqno = Entry#log_entry.seqno,
    HistoryId = Entry#log_entry.history_id,

    case Seqno =:= EndSeqno + 1 of
        true ->
            get_entries_seqnos_loop(Rest, StartSeqno, Seqno, HistoryId);
        false ->
            {error, {malformed, Entry}}
    end.

check_committed_seqno(Term, CommittedSeqno, HighSeqno, Data) ->
    ?CHECK(check_committed_seqno_known(CommittedSeqno, HighSeqno, Data),
           check_committed_seqno_rollback(Term, CommittedSeqno, Data)).

check_committed_seqno_rollback(Term, CommittedSeqno, Data) ->
    OurCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
    case CommittedSeqno < OurCommittedSeqno of
        true ->
            HighTerm = get_high_term(Data),
            case Term =:= HighTerm of
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
                          "while we know that ~p was committed. "
                          "Keeping our committed seqno intact.",
                          [Term, CommittedSeqno, OurCommittedSeqno]),
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

handle_install_snapshot(HistoryId, Term,
                        SnapshotSeqno, SnapshotHistoryId,
                        SnapshotTerm, SnapshotConfig,
                        RSMSnapshots, From, State, Data) ->
    case check_install_snapshot(HistoryId, Term,
                                SnapshotConfig,
                                RSMSnapshots, State, Data) of
        ok ->
            case get_snapshot_action(SnapshotSeqno, SnapshotTerm, Data) of
                skip ->
                    {keep_state_and_data,
                     {reply, From, {ok, build_metadata(Data)}}};
                Action ->
                    Metadata0 = #{?META_TERM => Term,
                                  ?META_COMMITTED_SEQNO => SnapshotSeqno},
                    Metadata =
                        case HistoryId =:= SnapshotHistoryId of
                            true ->
                                Metadata0#{?META_HISTORY_ID => HistoryId,
                                           ?META_PENDING_BRANCH => undefined};
                            false ->
                                %% Make sure that the branch record gets
                                %% cleaned up only once we get a record
                                %% committed with the new history id.
                                Metadata0
                        end,

                    NewData0 =
                        case Action of
                            install ->
                                install_snapshot(SnapshotSeqno,
                                                 SnapshotHistoryId,
                                                 SnapshotTerm, SnapshotConfig,
                                                 RSMSnapshots, Metadata, Data);
                            commit_seqno ->
                                store_meta(Metadata, Data)
                        end,
                    NewData1 = maybe_cancel_snapshot(NewData0),

                    {NewState, NewData} =
                        post_append(Term, SnapshotSeqno, State, Data, NewData1),

                    {next_state, NewState, NewData,
                     {reply, From, {ok, build_metadata(NewData)}}}
            end;
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

get_snapshot_action(SnapshotSeqno, SnapshotTerm,
                    #data{storage = Storage} = Data) ->
    HighSeqno = get_high_seqno(Data),
    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),

    if
        SnapshotSeqno > HighSeqno ->
            install;
        SnapshotSeqno =< CommittedSeqno ->
            skip;
        true ->
            %% CommittedSeqno < SnapshotSeqno =< HighSeqno
            %%
            %% Install snapshot always tosses away all entries in the log and
            %% replaces them with the snapshot. In this case, some or all of
            %% the entries following the snapshot might ultimately be
            %% committed and must be preserved. Otherwise in certain
            %% situations it's possible for a committed entry to get
            %% overwritten.
            %%
            %% It's only possible for the tail of the log after the snapshot
            %% to have "valid" entries if the entry at SnapshotSeqno is
            %% valid. We check if that's indeed the case by comparing the
            %% entry's term and the snapshot term. If they match, then there's
            %% no need to install the snapshot. We can simply mark everything
            %% up to the snapshot seqno committed.
            {ok, Term} =
                chronicle_storage:get_term_for_seqno(SnapshotSeqno, Storage),
            case Term =:= SnapshotTerm of
                true ->
                    commit_seqno;
                false ->
                    install
            end
    end.

check_install_snapshot(HistoryId, Term,
                       SnapshotConfig, RSMSnapshots, State, Data) ->
    ?CHECK(check_prepared(State),
           check_history_id(HistoryId, Data),
           check_not_earlier_term(Term, Data),
           check_snapshot_config(SnapshotConfig, RSMSnapshots)).

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
                check_branch_peers(Branch, Data)) of
        ok ->
            NewData = store_meta(#{?META_PENDING_BRANCH => Branch}, Data),

            PendingBranch = get_meta(?META_PENDING_BRANCH, Data),
            case PendingBranch of
                undefined ->
                    ?DEBUG("Stored a branch record:~n~p", [Branch]);
                #branch{} ->
                    ?WARNING("Pending branch overriden by a new branch.~n"
                             "Pending branch:~n~p~n"
                             "New branch:~n~p",
                             [PendingBranch, Branch])
            end,

            %% New branch, announce history change.
            announce_new_history(NewData),

            {keep_state, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_branch_compatible(NewBranch, Data) ->
    HistoryId = NewBranch#branch.old_history_id,
    check_committed_history_id(HistoryId, Data).

check_branch_peers(Branch, Data) ->
    Peer = get_meta(?META_PEER, Data),
    Coordinator = Branch#branch.coordinator,
    Peers = Branch#branch.peers,

    case lists:member(Coordinator, Peers) of
        true ->
            case lists:member(Peer, Peers) of
                true ->
                    ok;
                false ->
                    {error, {not_in_peers, Peer, Peers}}
            end;
        false ->
            {error, {not_in_peers, Coordinator, Peers}}
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
                                        Seqno, {T, E},
                                        sanitize_stacktrace(Stacktrace)]),
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
        {'DOWN', MRef, process, RSMPid, _Reason} ->
            ?ERROR("RSM ~p~p died "
                   "before passing a snapshot for seqno ~p.",
                   [RSM, RSMPid, Seqno]),
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

handle_mark_removed(Peer, PeerId, From, State, Data) ->
    case check_mark_removed(Peer, PeerId, State, Data) of
        ok ->
            {NewState, NewData} = mark_removed(Data),
            {next_state, NewState, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_mark_removed(Peer, PeerId, State, Data) ->
    ?CHECK(check_provisioned(State),
           check_not_wipe_requested(Data),
           check_peer_and_id(Peer, PeerId, Data)).

check_peer_and_id(Peer, PeerId, Data) ->
    #{?META_PEER := OurPeer,
      ?META_PEER_ID := OurPeerId} = get_meta(Data),

    Theirs = {Peer, PeerId},
    Ours = {OurPeer, OurPeerId},

    case Ours =:= Theirs of
        true ->
            ok;
        false ->
            {error, {peer_mismatch, Ours, Theirs}}
    end.

handle_check_member(HistoryId, Peer, PeerId, PeerSeqno, From, State, Data) ->
    Reply =
        case ?CHECK(check_provisioned(State),
                    check_history_id(HistoryId, Data)) of
            ok ->
                CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
                case CommittedSeqno >= PeerSeqno of
                    true ->
                        ConfigEntry = get_config_for_seqno(CommittedSeqno,
                                                           Data),
                        Config = ConfigEntry#log_entry.value,
                        IsPeer = chronicle_config:is_peer(Peer, PeerId, Config),
                        {ok, IsPeer};
                    false ->
                        {error, {peer_ahead, PeerSeqno, CommittedSeqno}}
                end;
            {error, _} = Error ->
                Error
        end,

    {keep_state_and_data, {reply, From, Reply}}.

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

check_history_id(HistoryId, Data) ->
    OurHistoryId = get_effective_history_id(Data),
    true = (OurHistoryId =/= ?NO_HISTORY),
    do_check_history_id(HistoryId, OurHistoryId).

check_committed_history_id(HistoryId, Data) ->
    OurHistoryId = get_meta(?META_HISTORY_ID, Data),
    true = (OurHistoryId =/= ?NO_HISTORY),
    do_check_history_id(HistoryId, OurHistoryId).

do_check_history_id(HistoryId, OurHistoryId) ->
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

check_new_config(OldData, NewData) ->
    case get_config_revision(OldData) =:= get_config_revision(NewData) of
        true ->
            ok;
        false ->
            handle_new_config(NewData)
    end.

get_config_revision(Data) ->
    Config = get_config(Data),
    #log_entry{history_id = HistoryId,
               term = Term,
               seqno = Seqno} = Config,
    {HistoryId, Term, Seqno}.

handle_new_config(Data) ->
    publish_settings(Data),
    announce_new_config(Data).

publish_settings(Data) ->
    Settings =
        case get_config(Data) of
            undefined ->
                #{};
            #log_entry{value = Config} ->
                chronicle_config:get_settings(Config)
        end,

    chronicle_settings:set_settings(Settings).

announce_new_config(Data) ->
    Metadata = build_metadata(Data),
    ConfigEntry = get_config(Data),
    Config = ConfigEntry#log_entry.value,
    chronicle_events:sync_notify({new_config, Config, Metadata}).

announce_committed_seqno(CommittedSeqno, Data) ->
    foreach_rsm(
      fun (_Name, Pid) ->
              chronicle_rsm:note_seqno_committed(Pid, CommittedSeqno)
      end, Data).

announce_system_state(SystemState) ->
    announce_system_state(SystemState, no_extra).

announce_system_state(SystemState, Extra) ->
    chronicle_events:sync_notify({system_state, SystemState, Extra}),
    announce_system_state_changed().

announce_system_state_changed() ->
    chronicle_utils:announce_important_change(system_state).

announce_joining_cluster(HistoryId) ->
    announce_system_state(joining_cluster, HistoryId).

announce_system_provisioned(Data) ->
    announce_system_state(provisioned, build_metadata(Data)).

announce_system_reprovisioned(Data) ->
    announce_system_event(reprovisioned, build_metadata(Data)).

announce_system_wiping() ->
    announce_system_event(wiping).

announce_system_event(Event) ->
    announce_system_event(Event, no_extra).

announce_system_event(Event, Extra) ->
    chronicle_events:sync_notify({system_event, Event, Extra}).

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
                             ?META_PEER_ID => ?NO_PEER_ID,
                             ?META_HISTORY_ID => ?NO_HISTORY,
                             ?META_TERM => ?NO_TERM,
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

append_entry(Entry, Meta, Data) ->
    Seqno = Entry#log_entry.seqno,
    append_entries(Seqno, Seqno, [Entry], Meta, false, Data).

store_meta(Meta, #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:store_meta(Meta, Storage),
    chronicle_storage:sync(NewStorage),
    Data#data{storage = publish_storage(NewStorage)}.

append_entries(StartSeqno, EndSeqno, Entries, Metadata, Truncate,
               #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:append(StartSeqno, EndSeqno,
                                          Entries,
                                          #{meta => Metadata,
                                            truncate => Truncate}, Storage),
    chronicle_storage:sync(NewStorage),
    Data#data{storage = publish_storage(NewStorage)}.

record_snapshot(Seqno, HistoryId, Term, ConfigEntry,
                #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:record_snapshot(Seqno, HistoryId, Term,
                                                   ConfigEntry, Storage),
    Data#data{storage = NewStorage}.

install_snapshot(SnapshotSeqno, SnapshotHistoryId, SnapshotTerm, SnapshotConfig,
                 RSMSnapshots, Metadata, #data{storage = Storage} = Data) ->
    lists:foreach(
      fun ({RSM, RSMSnapshotBinary}) ->
              RSMSnapshot = binary_to_term(RSMSnapshotBinary),
              chronicle_storage:save_rsm_snapshot(SnapshotSeqno, RSM,
                                                  RSMSnapshot, Storage)
      end, maps:to_list(RSMSnapshots)),

    NewStorage =
        chronicle_storage:install_snapshot(SnapshotSeqno,
                                           SnapshotHistoryId, SnapshotTerm,
                                           SnapshotConfig, Metadata, Storage),
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

get_high_term(#data{storage = Storage}) ->
    chronicle_storage:get_high_term(Storage).

get_config(#data{storage = Storage}) ->
    chronicle_storage:get_config(Storage).

get_config_for_seqno(Seqno, #data{storage = Storage}) ->
    chronicle_storage:get_config_for_seqno(Seqno, Storage).

get_config_for_seqno_range(FromSeqno, ToSeqno, #data{storage = Storage}) ->
    chronicle_storage:get_config_for_seqno_range(FromSeqno, ToSeqno, Storage).

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

    case CommittedSeqno - LatestSnapshotSeqno >= ?SNAPSHOT_INTERVAL
        andalso not is_wipe_requested(Data) of
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

    {ok,
     #log_entry{term = Term,
                history_id = HistoryId}} = get_log_entry(CommittedSeqno, Data),

    %% TODO: depending on specifics of how RSM deletions are handled,
    %% it may not be safe when there's an uncommitted config deleting
    %% an RSM. Deal with this once RSM deletions are supported. For
    %% now we are asserting that the set of RSMs is the same in both
    %% configs.
    true = (CommittedRSMs =:= CurrentRSMs),

    TRef = start_snapshot_timer(),

    RSMs = sets:from_list(maps:keys(CommittedRSMs)),
    SnapshotState = #snapshot_state{tref = TRef,
                                    history_id = HistoryId,
                                    term = Term,
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
    chronicle_config:get_rsms(Config).

read_rsm_snapshot(Name, Seqno, Storage) ->
    case chronicle_storage:read_rsm_snapshot(Name, Seqno, Storage) of
        {ok, Snapshot} ->
            Snapshot;
        {error, Error} ->
            exit({get_rsm_snapshot_failed, Name, Seqno, Error})
    end.

check_got_removed(OldCommittedSeqno, NewCommittedSeqno, State, Data) ->
    case get_config_for_seqno_range(OldCommittedSeqno + 1,
                                    NewCommittedSeqno, Data) of
        {ok, NewConfig} ->
            check_got_removed_with_config(NewConfig, State, Data);
        false ->
            {State, Data}
    end.

check_got_removed(State, Data) ->
    case State of
        provisioned ->
            CommittedConfig = get_committed_config(Data),
            check_got_removed_with_config(CommittedConfig, State, Data);
        _ ->
            {State, Data}
    end.

check_got_removed_with_config(#log_entry{value = Config}, State, Data) ->
    #{?META_PEER := Peer, ?META_PEER_ID := PeerId} = get_meta(Data),
    case chronicle_config:is_peer(Peer, PeerId, Config) of
        true ->
            {State, Data};
        false ->
            case is_wipe_requested(Data) of
                true ->
                    ?DEBUG("Got removed when wipe is requested. "
                           "Not changing the state."),
                    {State, Data};
                false ->
                    mark_removed(Data)
            end
    end.

mark_removed(Data) ->
    NewData = store_meta(#{?META_STATE => ?META_STATE_REMOVED}, Data),
    announce_system_state(removed, build_metadata(Data)),
    {removed, NewData}.

get_committed_config(Data) ->
    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
    get_config_for_seqno(CommittedSeqno, Data).

check_state_transitions(State, Data) ->
    {NewState, NewData} = check_join_cluster_done(State, Data),
    check_got_removed(NewState, NewData).
