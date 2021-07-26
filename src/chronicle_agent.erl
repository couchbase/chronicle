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

-behavior(gen_statem).
-include("chronicle.hrl").

-export([start_link/0]).
-export([server_ref/2, monitor/1,
         get_system_state/0, get_metadata/0,
         get_peer_info/0, get_peer_infos/1,
         check_grant_vote/2,
         get_info_for_rsm/1, save_rsm_snapshot/3, get_rsm_snapshot/1,
         get_full_snapshot/1, release_snapshot/1,
         snapshot_ok/1, snapshot_failed/1,
         get_log/0, get_log/4,
         get_log_committed/1, get_log_committed/2, get_log_for_rsm/3,
         get_term_for_seqno/1, get_history_id/1,
         provision/1, reprovision/0,
         wipe/0, is_wipe_requested/0,
         prepare_join/1, join_cluster/1,
         establish_local_term/2, establish_term/6, ensure_term/5,
         append/7, append/9,
         install_snapshot/8,
         local_mark_committed/3,
         local_store_branch/2, store_branch/3, undo_branch/3,
         mark_removed/2, check_member/4,
         force_snapshot/0, export_snapshot/1]).

-export([callback_mode/0, sanitize_event/2,
         init/1, handle_event/4, terminate/3]).

-export_type([provision_result/0, reprovision_result/0, wipe_result/0,
              prepare_join_result/0, join_cluster_result/0,
              force_snapshot_result/0, export_snapshot_result/0]).

-import(chronicle_utils, [call/2, call/3, call/4,
                          call_async/4,
                          next_term/2,
                          term_number/1,
                          compare_positions/2,
                          max_position/2,
                          sanitize_entry/1,
                          sanitize_entries/1]).

-define(NAME, ?MODULE).
-define(SERVER, ?SERVER_NAME(?NAME)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?NAME)).

-define(GET_PEER_INFO_TIMEOUT,
        chronicle_settings:get({agent, get_peer_info_timeout}, 15000)).

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
-define(JOIN_CLUSTER_NO_PROGRESS_TIMEOUT,
        chronicle_settings:get(
          {agent, join_cluster_no_progress_timeout}, 10000)).

-define(APPEND_TIMEOUT,
        chronicle_settings:get({agent, append_timeout}, 120000)).
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

-define(FORCE_SNAPSHOT_TIMEOUT,
        chronicle_settings:get({agent, force_snapshot_timeout}, 60000)).

%% Used to indicate that a function will send a message with the provided Tag
%% back to the caller when the result is ready. And the result type is
%% _ReplyType. This is entirely useless for dializer, but is usefull for
%% documentation purposes.
-type maybe_replies(_Tag, _ReplyType) :: chronicle_utils:send_result().

-record(snapshot_state, { tref,
                          seqno,
                          history_id,
                          term,
                          config,

                          waiters,
                          write_pending }).

-record(prepare_join, { config }).
-record(join_cluster, { from, config, seqno }).

-record(data, { storage,
                pending_appends = 0,

                snapshot_attempts = 0,
                snapshot_state :: undefined
                                | {retry, reference()}
                                | #snapshot_state{},

                wipe_state = false :: false | {wiping, From::any()},
                join_cluster_tref
              }).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

-type server_ref() :: any().

-spec server_ref(chronicle:peer(), chronicle:peer()) -> server_ref().
server_ref(Peer, SelfPeer) ->
    case Peer =:= SelfPeer of
        true ->
            ?SERVER;
        false ->
            ?SERVER_NAME(Peer, ?NAME)
    end.

-spec monitor(server_ref()) -> reference().
monitor(ServerRef) ->
    chronicle_utils:monitor_process(ServerRef).

-spec get_system_state() ->
          not_provisioned |
          {joining_cluster, #metadata{}} |
          {provisioned, #metadata{}} |
          {removed, #metadata{}}.
get_system_state() ->
    case chronicle_ets:get(agent_state) of
        {ok, SystemState} ->
            SystemState;
        not_found ->
            exit(no_agent)
    end.

-spec get_metadata() -> #metadata{}.
get_metadata() ->
    case get_system_state() of
        {State, Metadata} when State =:= provisioned;
                               State =:= removed ->
            Metadata;
        _ ->
            exit(not_provisioned)
    end.

-type peer_info() :: #{peer := chronicle:peer(),
                       peer_id := chronicle:peer_id(),
                       supported_compat_version := chronicle:compat_version()}.
-type get_peer_info_error() :: {error, not_initialized}.
-type get_peer_info_result() :: {ok, peer_info()} | get_peer_info_error().

-spec get_peer_info() -> get_peer_info_result().
get_peer_info() ->
    call(?SERVER, get_peer_info, ?GET_PEER_INFO_TIMEOUT).

-spec get_peer_infos([chronicle:peer()]) ->
          {ok, #{chronicle:peer() => peer_info()}} |
          {error, #{chronicle:peer() => get_peer_info_error()}}.
get_peer_infos(Peers) ->
    {Good, Bad} =
        chronicle_utils:multi_call(Peers, ?NAME,
                                   get_peer_info,
                                   fun (Result) ->
                                           case Result of
                                               {ok, _} ->
                                                   true;
                                               _ ->
                                                   false
                                           end
                                   end,
                                   ?GET_PEER_INFO_TIMEOUT),

    case maps:size(Bad) =:= 0 of
        true ->
            {ok, maps:map(fun (_Peer, {ok, Info}) ->
                                  Info
                          end, Good)};
        false ->
            {error, Bad}
    end.

-type check_grant_error() :: {bad_state, not_provisioned | removed}
                           | {history_mismatch, chronicle:history_id()}
                           | {behind, chronicle:peer_position()}.

-spec check_grant_vote(chronicle:history_id(), chronicle:peer_positoin()) ->
          {ok, chronicle:leader_term()} |
          {error, check_grant_error()}.
check_grant_vote(HistoryId, Position) ->
    case get_system_state() of
        not_provisioned ->
            {error, {bad_state, not_provisioned}};
        {removed, _} ->
            {error, {bad_state, removed}};
        {_, Metadata} ->
            OurHistoryId = get_history_id(Metadata),
            OurPosition = chronicle_utils:get_position(Metadata),

            %% Note that this is a weaker check than what establish_term does
            %% when the state is joining_cluster (we don't take into account
            %% the cluster config passed to us as part of
            %% prepare_join/join_cluster). But that's ok, because the
            %% prospective leader will still have to go through the agent to
            %% when it gets enough votes from chronicle_leader.
            case ?CHECK(do_check_history_id(HistoryId, OurHistoryId),
                        check_peer_current(Position, OurPosition)) of
                ok ->
                    {ok, Metadata#metadata.term};
                {error, _} = Error ->
                    Error
            end
    end.

get_info_for_rsm(Name) ->
    case get_system_state() of
        {State, Metadata} when State =:= provisioned;
                               State =:= removed ->
            %% TODO: change it to use committed config
            case check_existing_rsm(Name, Metadata#metadata.config) of
                ok ->
                    Info0 =
                        #{committed_seqno => Metadata#metadata.committed_seqno},
                    Info =
                        case chronicle_snapshot_mgr:need_snapshot(Name) of
                            {true, Seqno} ->
                                Info0#{need_snapshot_seqno => Seqno};
                            false ->
                                Info0
                        end,

                    {ok, Info};
                {error, no_rsm} = Error ->
                    Error
            end;
        {State, _} ->
            exit({bad_state, State});
        State ->
            exit({bad_state, State})
    end.

save_rsm_snapshot(Name, Seqno, Snapshot) ->
    chronicle_snapshot_mgr:save_snapshot(Name, Seqno, Snapshot).

get_rsm_snapshot(Name) ->
    case with_latest_snapshot(fun (Seqno, _HistoryId, _Term, Config) ->
                                      get_rsm_snapshot(Name, Seqno, Config)
                              end) of
        {error, no_snapshot} ->
            {no_snapshot, ?NO_SEQNO, undefined};
        Other ->
            Other
    end.

get_rsm_snapshot(Name, Seqno, Config) ->
    RSMs = get_rsms(Config),
    case maps:is_key(Name, RSMs) of
        true ->
            Snapshot = read_rsm_snapshot(Name, Seqno),
            {ok, Seqno, Config#log_entry.value, Snapshot};
        false ->
            {no_snapshot, Seqno, Config#log_entry.value}
    end.

get_full_snapshot(HaveSeqno) ->
    with_latest_snapshot(
      fun (SnapshotSeqno, HistoryId, Term, Config) ->
              case SnapshotSeqno > HaveSeqno of
                  true ->
                      get_full_snapshot(SnapshotSeqno, HistoryId, Term, Config);
                  false ->
                      {error, no_snapshot}
              end
      end).

get_full_snapshot(Seqno, HistoryId, Term, Config) ->
    RSMs = get_rsms(Config),
    RSMSnapshots =
        maps:map(
          fun (Name, _) ->
                  Snapshot = read_rsm_snapshot(Name, Seqno),
                  term_to_binary(Snapshot, [compressed])
          end, RSMs),
    {ok, Seqno, HistoryId, Term, Config, RSMSnapshots}.

with_latest_snapshot(Fun) ->
    case chronicle_snapshot_mgr:get_latest_snapshot() of
        {ok, Ref, Seqno, HistoryId, Term, Config} ->
            try
                Fun(Seqno, HistoryId, Term, Config)
            after
                chronicle_snapshot_mgr:release_snapshot(Ref)
            end;
        {error, no_snapshot} = Error->
            Error;
        {error, Error} ->
            exit(Error)
    end.

release_snapshot(Seqno) ->
    chronicle_utils:send(?SERVER,
                         {snapshot_mgr, {release_snapshot, Seqno}}, []).

snapshot_ok(Seqno) ->
    snapshot_result(Seqno, ok).

snapshot_failed(Seqno) ->
    snapshot_result(Seqno, failed).

snapshot_result(Seqno, Result) ->
    chronicle_utils:send(?SERVER,
                         {snapshot_mgr, {snapshot_result, Seqno, Result}}, []).

%% For debugging only.
get_log() ->
    call(?SERVER, get_log).

-spec get_log_committed(chronicle:seqno()) ->
          {ok, chronicle:seqno(), [#log_entry{}]} | {error, compacted}.
get_log_committed(StartSeqno) ->
    #metadata{committed_seqno = CommittedSeqno} = get_metadata(),
    case get_log_committed(StartSeqno, CommittedSeqno) of
        {ok, Entries} ->
            {ok, CommittedSeqno, Entries};
        {error, compacted} = Error ->
            Error
    end.

-spec get_log_committed(chronicle:seqno(), chronicle:seqno()) ->
          {ok, [#log_entry{}]} | {error, compacted}.
get_log_committed(StartSeqno, EndSeqno) ->
    case StartSeqno > EndSeqno of
        true ->
            {ok, []};
        false ->
            chronicle_storage:get_log_committed(StartSeqno, EndSeqno)
    end.

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
    chronicle_storage:get_term_for_committed_seqno(Seqno).

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
                           | {bad_config, chronicle:peer(), #config{}}.

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
                            | {bad_cluster_info, any()}
                            | {unsupported_compat_version,
                               ClusterVersion::chronicle:compat_version(),
                               OurVersion::chronicle:compat_version()}.
-spec prepare_join(chronicle:cluster_info()) -> prepare_join_result().
prepare_join(ClusterInfo) ->
    call(?SERVER, {prepare_join, ClusterInfo}, ?PREPARE_JOIN_TIMEOUT).

-type join_cluster_result() :: ok | {error, join_cluster_error()}.
-type join_cluster_error() :: not_prepared
                            | {history_mismatch, chronicle:history_id()}
                            | {not_in_peers,
                               chronicle:peer(), [chronicle:peer()]}
                            | wipe_requested
                            | {bad_cluster_info, any()}
                            | no_progress.
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

-spec establish_term(server_ref(),
                     Opaque,
                     chronicle:history_id(),
                     chronicle:leader_term(),
                     chronicle:peer_position(),
                     chronicle_utils:send_options()) ->
          maybe_replies(Opaque, establish_term_result()).
establish_term(ServerRef, Opaque, HistoryId, Term, Position, Options) ->
    call_async(ServerRef, Opaque,
               {establish_term, HistoryId, Term, Position},
               Options).

-type ensure_term_result() ::
        {ok, #metadata{}} |
        {error, ensure_term_error()}.

-type ensure_term_error() ::
        {bad_state, not_provisioned | removed} |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()}.

-spec ensure_term(server_ref(),
                  Opaque,
                  chronicle:history_id(),
                  chronicle:leader_term(),
                  chronicle_utils:send_options()) ->
          maybe_replies(Opaque, ensure_term_result()).
ensure_term(ServerRef, Opaque, HistoryId, Term, Options) ->
    call_async(ServerRef, Opaque,
               {ensure_term, HistoryId, Term},
               Options).

-type append_result() :: ok | {error, append_error()}.
-type append_error() ::
        {bad_state, not_provisioned | removed} |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {missing_entries, #metadata{}} |
        {protocol_error, any()}.

-spec append(server_ref(),
             Opaque,
             chronicle:history_id(),
             chronicle:leader_term(),
             chronicle:seqno(),
             chronicle:leader_term(),
             chronicle:seqno(),
             [#log_entry{}],
             chronicle_utils:send_options()) ->
          maybe_replies(Opaque, append_result()).
append(ServerRef, Opaque, HistoryId, Term,
       CommittedSeqno,
       AtTerm, AtSeqno, Entries, Options) ->
    call_async(ServerRef, Opaque,
               {append, HistoryId, Term,
                CommittedSeqno, AtTerm, AtSeqno, Entries},
               Options).

-spec append(server_ref(),
             chronicle:history_id(),
             chronicle:leader_term(),
             chronicle:seqno(),
             chronicle:leader_term(),
             chronicle:seqno(),
             [#log_entry{}]) -> append_result().
append(ServerRef, HistoryId, Term,
       CommittedSeqno, AtTerm, AtSeqno, Entries) ->
    call(ServerRef,
         {append, HistoryId, Term, CommittedSeqno, AtTerm, AtSeqno, Entries},
         ?APPEND_TIMEOUT).

-type install_snapshot_result() :: {ok, #metadata{}}
                                 | {error, install_snapshot_error()}.
-type install_snapshot_error() ::
        {bad_state, not_provisioned | removed} |
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {protocol_error, any()}.

-spec install_snapshot(server_ref(),
                       chronicle:history_id(),
                       chronicle:leader_term(),
                       chronicle:seqno(),
                       chronicle:history_id(),
                       chronicle:leader_term(),
                       ConfigEntry::#log_entry{},
                       #{RSM::atom() => RSMSnapshot::binary()}) ->
          install_snapshot_result().
install_snapshot(ServerRef, HistoryId, Term,
                 SnapshotSeqno, SnapshotHistoryId,
                 SnapshotTerm, SnapshotConfig, RSMSnapshots) ->
    call(ServerRef,
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

-spec mark_removed(chronicle:peer(), chronicle:peer_id()) ->
          ok | {error, Error} when
      Error :: {bad_state, joining_cluster | not_provisioned | removed}
             | wipe_requested
             | {peer_mismatch,
                {chronicle:peer(), chronicle:peer_id()},
                {chronicle:peer(), chronicle:peer_id()}}.
mark_removed(Peer, PeerId) ->
    call(?SERVER, {mark_removed, Peer, PeerId}).

-type check_member_error() ::
        {bad_state, joining_cluster | removed | not_provisioned} |
        {history_mismatch, OurHistory::chronicle:history_id()} |
        {peer_ahead,
         PeerSeqno::chronicle:seqno(),
         CommittedSeqno::chronicle:seqno()}.

-spec check_member(chronicle:history_id(),
                   chronicle:peer(), chronicle:peer_id(),
                   chronicle:seqno()) ->
          {ok, boolean()} |
          {error, check_member_error()}.
check_member(HistoryId, Peer, PeerId, PeerSeqno) ->
    case get_system_state() of
        {provisioned, Metadata} ->
            OurHistoryId = get_history_id(Metadata),
            case do_check_history_id(HistoryId, OurHistoryId) of
                ok ->
                    #metadata{committed_seqno = CommittedSeqno,
                              committed_config = ConfigEntry} = Metadata,
                    case CommittedSeqno >= PeerSeqno of
                        true ->
                            Config = ConfigEntry#log_entry.value,
                            IsPeer =
                                chronicle_config:is_peer(Peer, PeerId, Config),
                            {ok, IsPeer};
                        false ->
                            {error, {peer_ahead, PeerSeqno, CommittedSeqno}}
                    end;
                {error, _} = Error ->
                    Error
            end;
        {State, _} ->
            {error, {bad_state, State}};
        State ->
            {error, {bad_state, State}}
    end.

-type force_snapshot_error() :: snapshot_failed
                              | snapshot_canceled
                              | wipe_requested
                              | {bad_state, joining_cluster | not_provisioned}.
-type force_snapshot_result() :: {ok, file:name()}
                               | {error, force_snapshot_error()}.
-spec force_snapshot() -> force_snapshot_result().
force_snapshot() ->
    case call(?SERVER, force_snapshot, ?FORCE_SNAPSHOT_TIMEOUT) of
        {ok, Seqno} ->
            {ok, chronicle_storage:snapshot_dir(Seqno)};
        {error, _} = Error ->
            Error
    end.

-type export_snapshot_error() ::
        force_snapshot_error()
      | {link_failed | copy_failed, file:posix(), file:name(), file:name()}
      | {mkdir_failed, file:posix(), file:name()}.
-type export_snapshot_result() :: ok | {error, export_snapshot_error()}.

-spec export_snapshot(file:name()) -> export_snapshot_result().
export_snapshot(Path) ->
    case chronicle_utils:mkdir_p(Path) of
        ok ->
            case force_snapshot() of
                {ok, _} ->
                    Result = with_latest_snapshot(
                               fun (Seqno, _, _, Config) ->
                                       chronicle_storage:copy_snapshot(
                                         Path, Seqno, Config)
                               end),

                    case Result of
                        {error, no_snapshot} ->
                            %% Should not happen because we just forced a
                            %% snapshot.
                            exit(no_snapshot);
                        _ ->
                            Result
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, Error} ->
            {error, {mkdir_failed, Error, Path}}
    end.

%% gen_statem callbacks
callback_mode() ->
    handle_event_function.

sanitize_event({call, _} = Type,
               {append, HistoryId, Term, CommittedSeqno, AtTerm, AtSeqno, _}) ->
    {Type, {append, HistoryId, Term, CommittedSeqno, AtTerm, AtSeqno, '...'}};
sanitize_event({call, _} = Type,
               {install_snapshot,
                HistoryId, Term,
                SnapshotSeqno, SnapshotHistoryId,
                SnapshotTerm, SnapshotConfig, _}) ->
    {Type, {install_snapshot,
            HistoryId, Term,
            SnapshotSeqno, SnapshotHistoryId,
            SnapshotTerm, SnapshotConfig, '...'}};
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

    ok = chronicle_ets:register_writer([agent_state]),
    publish_state(State, Data),

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
handle_event(cast, prepare_wipe_done, State, Data) ->
    handle_prepare_wipe_done(State, Data);
handle_event(info, {chronicle_storage, Event}, State, Data) ->
    handle_storage_event(Event, State, Data);
handle_event(info, snapshot_timeout, State, Data) ->
    handle_snapshot_timeout(State, Data);
handle_event(info, retry_snapshot, State, Data) ->
    handle_retry_snapshot(State, Data);
handle_event(info, {snapshot_mgr, Msg}, State, Data) ->
    case Msg of
        {release_snapshot, Seqno} ->
            handle_release_snapshot(Seqno, State, Data);
        {snapshot_result, Seqno, Result} ->
            handle_snapshot_result(Seqno, Result, State, Data)
    end;
handle_event(info, join_cluster_timeout, State, Data) ->
    handle_join_cluster_timeout(State, Data);
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event of type ~p: ~p", [Type, Event]),
    keep_state_and_data.

handle_call(get_peer_info, From, State, Data) ->
    handle_get_peer_info(From, State, Data);
handle_call(get_log, From, _State, _Data) ->
    {keep_state_and_data,
     {reply, From, {ok, chronicle_storage:get_log()}}};
handle_call({get_log, HistoryId, Term, StartSeqno, EndSeqno},
            From, State, Data) ->
    handle_get_log(HistoryId, Term, StartSeqno, EndSeqno, From, State, Data);
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
    Position = get_position(Data),
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
handle_call({mark_removed, Peer, PeerId}, From, State, Data) ->
    handle_mark_removed(Peer, PeerId, From, State, Data);
handle_call(force_snapshot, From, State, Data) ->
    handle_force_snapshot(From, State, Data);
handle_call(_Call, From, _State, _Data) ->
    {keep_state_and_data,
     {reply, From, nack}}.

terminate(_Reason, State, Data) ->
    maybe_cancel_snapshot(State, Data).

%% internal
publish_state(State, Data) ->
    %% TODO: It's pretty wasteful to publish this on every change when most
    %% information doesn't change most of the time. Consider optimizing it
    %% later.
    ExtState = get_external_state(State),
    FullState =
        case ExtState of
            _ when ExtState =:= provisioned;
                   ExtState =:= joining_cluster;
                   ExtState =:= removed ->
                {ExtState, build_metadata(Data)};
            _ ->
                ExtState
        end,

    chronicle_ets:put(agent_state, FullState).

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
              committed_config = get_committed_config(Data),
              pending_branch = PendingBranch}.

check_prepared(State) ->
    case State of
        _ when State =:= not_provisioned;
               State =:= removed ->
            {error, {bad_state, get_external_state(State)}};
        _ ->
            ok
    end.

handle_get_peer_info(From, _State, Data) ->
    Meta = get_meta(Data),
    Reply =
        case Meta of
            #{?META_PEER := Peer,
              ?META_PEER_ID := PeerId} when Peer =/= ?NO_PEER,
                                            PeerId =/= ?NO_PEER_ID ->
                {ok, #{peer => Peer,
                       peer_id => PeerId,
                       supported_compat_version => ?COMPAT_VERSION}};
            _ ->
                {error, not_initialized}
        end,

    {keep_state_and_data, {reply, From, Reply}}.

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

check_existing_rsm(Name, Config) ->
    case Config of
        undefined ->
            {error, no_rsm};
        Config ->
            case maps:is_key(Name, get_rsms(Config)) of
                true ->
                    ok;
                false ->
                    {error, no_rsm}
            end
    end.

handle_release_snapshot(SnapshotSeqno, _State, Data) ->
    {keep_state, release_snapshot(SnapshotSeqno, Data)}.

handle_snapshot_result(Seqno, Result, State,
                       #data{snapshot_state = SnapshotState} = Data) ->
    true = (Seqno =:= SnapshotState#snapshot_state.seqno),

    case Result of
        ok ->
            handle_snapshot_ok(State, Data);
        failed ->
            handle_snapshot_failed(State, Data)
    end.

handle_snapshot_ok(_State, #data{snapshot_state = SnapshotState} = Data) ->
    false = SnapshotState#snapshot_state.write_pending,
    #snapshot_state{tref = TRef,
                    seqno = Seqno,
                    history_id = HistoryId,
                    term = Term,
                    config = Config} = SnapshotState,

    ?DEBUG("Recording snapshot at seqno ~p.~n"
           "Config:~n~p",
           [Seqno, Config]),

    cancel_snapshot_timer(TRef),

    NewSnapshotState = SnapshotState#snapshot_state{write_pending = true},
    NewData = record_snapshot(Seqno, HistoryId, Term, Config,
                              Data#data{snapshot_state = NewSnapshotState}),

    {keep_state, NewData}.

handle_snapshot_failed(_State,
                       #data{snapshot_state = SnapshotState,
                             snapshot_attempts = Attempts,
                             storage = Storage} = Data) ->
    false = SnapshotState#snapshot_state.write_pending,
    #snapshot_state{seqno = Seqno} = SnapshotState,

    ?ERROR("Failed to take a snapshot at seqno ~b", [Seqno]),

    reply_snapshot_failed(Data),
    chronicle_storage:delete_snapshot(Seqno, Storage),

    case need_snapshot(Data) of
        true ->
            NewAttempts = Attempts + 1,
            AttemptsRemaining = ?SNAPSHOT_RETRIES - NewAttempts,
            NewData = Data#data{snapshot_attempts = NewAttempts},

            case AttemptsRemaining > 0 of
                true ->
                    ?INFO("Scheduling a snapshot retry. "
                          "~b attempts remaining.", [AttemptsRemaining]),
                    {keep_state, schedule_retry_snapshot(NewData)};
                false ->
                    {stop, {snapshot_failed, Seqno}, NewData}
            end;
        false ->
            %% This might have been a user-initiated snapshot and strictly
            %% speaking we don't need a snapshot currently.
            {keep_state, reset_snapshot_state(Data)}
    end.

handle_snapshot_timeout(_State, #data{snapshot_state = SnapshotState} = Data) ->
    false = SnapshotState#snapshot_state.write_pending,
    reply_snapshot_failed(Data),
    #snapshot_state{seqno = Seqno} = SnapshotState,
    ?ERROR("Timeout while taking snapshot at seqno ~b.", [Seqno]),
    {stop, {snapshot_timeout, Seqno}, Data}.

handle_retry_snapshot(_State, Data) ->
    {keep_state, retry_snapshot(Data)}.

handle_storage_event(Event, State, Data) ->
    case chronicle_storage:handle_event(Event, Data#data.storage) of
        {ok, NewStorage} ->
            {keep_state, Data#data{storage = NewStorage}};
        {ok, NewStorage, Requests} ->
            NewData0 = Data#data{storage = NewStorage},
            NewData = handle_completed_requests(Requests, State,
                                                Data, NewData0),
            {keep_state, NewData}
    end.

handle_completed_requests(Requests, State, OldData, NewData) ->
    {Froms, Count, HaveSnapshot} =
        lists:foldr(
          fun (Request, {AccFroms, AccCount, AccSnapshot} = Acc) ->
                  case Request of
                      {append, From} ->
                          {[From | AccFroms], AccCount + 1, AccSnapshot};
                      {record_snapshot, Seqno} ->
                          false = AccSnapshot,

                          SnapshotState = NewData#data.snapshot_state,
                          true = SnapshotState#snapshot_state.write_pending,
                          true = (Seqno =:= SnapshotState#snapshot_state.seqno),
                          {AccFroms, AccCount, true};
                      none ->
                          Acc
                  end
          end, {[], 0, false}, Requests),

    FinalData =
        case HaveSnapshot of
            true ->
                handle_snapshot_recorded(NewData);
            false ->
                NewData
        end,

    handle_completed_appends(Froms, Count, State, OldData, FinalData).

handle_snapshot_recorded(#data{snapshot_state = SnapshotState} = Data) ->
    Seqno = SnapshotState#snapshot_state.seqno,
    ?DEBUG("Snapshot at seqno ~p recorded.", [Seqno]),
    reply_snapshot_ok(Seqno, Data),
    reset_snapshot_state(Data).

handle_completed_appends([], _Count, _State, _OldData, NewData) ->
    NewData;
handle_completed_appends(Froms, Count, State, OldData, NewData) ->
    NewData0 = dec_pending_appends(Count, NewData),
    NewData1 = post_append(State, OldData, NewData0),
    lists:foreach(
      fun (From) ->
              gen_statem:reply(From, ok)
      end, Froms),

    NewData1.

handle_reprovision(From, State, Data) ->
    case check_reprovision(State, Data) of
        {ok, OldPeer, Config} ->
            #{?META_HISTORY_ID := HistoryId,
              ?META_TERM := Term} = get_pending_meta(Data),

            HighSeqno = get_pending_high_seqno(Data),
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
                                   State, Data),

            publish_state(State, NewData),
            announce_system_reprovisioned(NewData),
            handle_new_config(NewData),
            announce_committed_seqno(Seqno),

            {keep_state, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_reprovision(State, Data) ->
    case check_provisioned(State) of
        ok ->
            Peer = get_pending_meta(?META_PEER, Data),
            ConfigEntry = get_pending_config(Data),
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

            Config = chronicle_config:init(HistoryId, Peer, Machines),
            ConfigEntry = #log_entry{history_id = HistoryId,
                                     term = Term,
                                     seqno = Seqno,
                                     value = Config},

            {ok, PeerId} = chronicle_config:get_peer_id(Peer, Config),

            ?DEBUG("Provisioning with history ~p. Config:~n~p",
                   [HistoryId, Config]),

            NewState = provisioned,
            NewData = append_entry(ConfigEntry,
                                   #{?META_STATE => ?META_STATE_PROVISIONED,
                                     ?META_PEER => Peer,
                                     ?META_PEER_ID => PeerId,
                                     ?META_HISTORY_ID => HistoryId,
                                     ?META_TERM => Term,
                                     ?META_COMMITTED_SEQNO => Seqno},
                                   State, Data),

            publish_state(NewState, NewData),

            announce_system_provisioned(NewData),
            handle_new_config(NewData),

            {next_state, NewState, NewData, {reply, From, ok}};
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

check_provisioned_or_removed(State) ->
    case State of
        provisioned ->
            ok;
        removed ->
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
                    {keep_state, maybe_cancel_snapshot(State, NewData)}
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

    chronicle_snapshot_mgr:wipe(),
    flush_snapshot_mgr_msgs(),

    ?INFO("Wiping"),
    chronicle_storage:close(Data#data.storage),
    chronicle_storage:wipe(),
    ?INFO("Wiped successfully"),

    gen_statem:reply(From, ok),

    NewState = not_provisioned,
    NewData = init_data(),
    publish_settings(NewData),
    publish_state(NewState, NewData),

    {next_state, NewState, NewData}.

flush_snapshot_mgr_msgs() ->
    ?FLUSH({snapshot_mgr, _}).

handle_prepare_join(ClusterInfo, From, State, Data) ->
    case ?CHECK(check_not_provisioned(State),
                check_cluster_info(ClusterInfo),
                check_compat_version(ClusterInfo)) of
        ok ->
            #{history_id := HistoryId,
              config := Config} = ClusterInfo,
            Peer = get_peer_name(),
            Meta = #{?META_HISTORY_ID => HistoryId,
                     ?META_PEER => Peer,
                     ?META_STATE => {?META_STATE_PREPARE_JOIN,
                                     #{config => Config}}},

            NewData = store_meta(Meta, State, Data),
            NewState = #prepare_join{config = Config},

            publish_state(NewState, NewData),
            announce_joining_cluster(HistoryId),

            {next_state, NewState, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_cluster_info(ClusterInfo) ->
    case ClusterInfo of
        #{history_id := HistoryId,
          committed_seqno := CommittedSeqno,
          compat_version := CompatVersion,
          config := #log_entry{value = #config{}}}
          when is_binary(HistoryId),
               is_integer(CommittedSeqno),
               is_integer(CompatVersion) ->
            ok;
        _ ->
            {error, {bad_cluster_info, ClusterInfo}}
    end.

check_compat_version(#{compat_version := Version}) ->
    case Version =< ?COMPAT_VERSION of
        true ->
            ok;
        false ->
            {error, {unsupported_compat_version, Version, ?COMPAT_VERSION}}
    end.

handle_join_cluster(ClusterInfo, From, State, Data) ->
    case check_join_cluster(ClusterInfo, State, Data) of
        {ok, Config, Seqno} ->
            Peer = get_pending_meta(?META_PEER, Data),
            {ok, PeerId} =
                chronicle_config:get_peer_id(Peer, Config#log_entry.value),

            Meta = #{?META_PEER_ID => PeerId,
                     ?META_STATE => {?META_STATE_JOIN_CLUSTER,
                                     #{seqno => Seqno,
                                       config => Config}}},

            NewData0 = store_meta(Meta, State, Data),
            NewState = #join_cluster{from = From,
                                     seqno = Seqno,
                                     config = Config},
            NewData = start_join_cluster_timer(NewState, NewData0),
            publish_state(NewState, NewData),

            %% We might already have all entries we need.
            {FinalState, FinalData} =
                check_state_transitions(NewState, NewData),

            {next_state, FinalState, FinalData};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

handle_join_cluster_timeout(State,
                            #data{pending_appends = PendingAppends} = Data) ->
    #join_cluster{from = From} = State,

    NewData = Data#data{join_cluster_tref = undefined},
    case PendingAppends =:= 0 of
        true ->
            ?ERROR("Join cluster made no progress for ~bms.",
                   [?JOIN_CLUSTER_NO_PROGRESS_TIMEOUT]),

            NewState = State#join_cluster{from = undefined},

            {next_state, NewState, NewData,
             {reply, From, {error, no_progress}}};
        false ->
            %% There are still some appends pending. Don't interrupt join.
            ?WARNING("Join cluster timeout "
                     "when ~b appends are pending", [PendingAppends]),
            {keep_state, NewData}
    end.

start_join_cluster_timer(State, Data) ->
    case State of
        #join_cluster{from = From} when From =/= undefined ->
            NewData = cancel_join_cluster_timer(Data),
            TRef = erlang:send_after(?JOIN_CLUSTER_NO_PROGRESS_TIMEOUT,
                                     self(),
                                     join_cluster_timeout),
            NewData#data{join_cluster_tref = TRef};
        _ ->
            Data
    end.

cancel_join_cluster_timer(#data{join_cluster_tref = TRef} = Data) ->
    case TRef of
        undefined ->
            Data;
        _ ->
            _ = erlang:cancel_timer(TRef),
            ?FLUSH(join_cluster_timeout),
            Data#data{join_cluster_tref = undefined}
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

    Peer = get_pending_meta(?META_PEER, Data),
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

            CommittedSeqno = get_pending_meta(?META_COMMITTED_SEQNO, Data),
            case CommittedSeqno >= WaitedSeqno andalso NotWipeRequested of
                true ->
                    NewState = provisioned,
                    NewData = store_meta(#{?META_STATE =>
                                               ?META_STATE_PROVISIONED},
                                         State, Data),
                    publish_state(NewState, NewData),
                    announce_system_state(provisioned, build_metadata(NewData)),

                    case From =/= undefined of
                        true ->
                            gen_statem:reply(From, ok);
                        false ->
                            ok
                    end,

                    {NewState, cancel_join_cluster_timer(NewData)};
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
            NewData = store_meta(#{?META_TERM => Term}, State, Data),
            publish_state(State, NewData),

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
    CurrentTerm = get_pending_meta(?META_TERM, Data),
    case term_number(Term) > term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

check_peer_current(Position, State, Data) ->
    RequiredPosition = get_required_peer_position(State, Data),
    check_peer_current(Position, RequiredPosition).

check_peer_current(Position, OurPosition) ->
    case compare_positions(Position, OurPosition) of
        lt ->
            {error, {behind, OurPosition}};
        _ ->
            ok
    end.

get_position(Data) ->
    OurHighTerm = get_pending_high_term(Data),
    OurHighSeqno = get_pending_high_seqno(Data),
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

    Request = {append, From},
    NewData0 =
        case Entries =/= [] orelse Truncate of
            true ->
                append_entries_async(Request,
                                     StartSeqno, EndSeqno,
                                     Entries, Metadata, Truncate, Data);
            false ->
                store_meta_async(Request, Metadata, Data)
        end,
    NewData1 = inc_pending_appends(NewData0),

    %% TODO: in-progress snapshots might need to be canceled if any of the
    %% state machines get deleted.

    {NewState, NewData} = check_state_transitions(State, Data, NewData1),
    {next_state, NewState, NewData}.

post_append(State, OldData, NewData) ->
    publish_state(State, NewData),

    FinalData =
        case State of
            provisioned ->
                maybe_announce_term_established(OldData, NewData),
                check_new_config(OldData, NewData),

                NewCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, NewData),
                OldCommittedSeqno = get_meta(?META_COMMITTED_SEQNO, OldData),
                case OldCommittedSeqno =:= NewCommittedSeqno of
                    true ->
                        ok;
                    false ->
                        announce_committed_seqno(NewCommittedSeqno)
                end,
                NewData;
            _ ->
                start_join_cluster_timer(State, NewData)
        end,

    maybe_initiate_snapshot(State, FinalData).

check_append(HistoryId, Term, CommittedSeqno,
             AtTerm, AtSeqno, Entries, State, Data) ->
    ?CHECK(check_prepared(State),
           check_history_id(HistoryId, Data),
           check_not_earlier_term(Term, Data),
           check_append_obsessive(HistoryId, Term, CommittedSeqno,
                                  AtTerm, AtSeqno, Entries, Data)).

check_append_history_invariants(HistoryId, Entries, Info, Data) ->
    OldHistoryId = get_pending_meta(?META_HISTORY_ID, Data),
    case HistoryId =:= OldHistoryId of
        true ->
            {ok, Info};
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
                    {ok, Info};
                [#log_entry{history_id = HistoryId, value = #config{}} | _] ->
                    {ok, Info};
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
    end.

check_append_obsessive(HistoryId, Term,
                       CommittedSeqno, AtTerm, AtSeqno, Entries, Data) ->
    OurHighSeqno = get_pending_high_seqno(Data),
    case AtSeqno > OurHighSeqno of
        true ->
            {error, {missing_entries, build_metadata(Data)}};
        false ->
            OurCommittedSeqno = get_pending_meta(?META_COMMITTED_SEQNO, Data),
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
                                    Info = #{entries => FinalEntries,
                                             start_seqno => StartSeqno,
                                             end_seqno => EndSeqno,
                                             committed_seqno =>
                                                 FinalCommittedSeqno,
                                             truncate => Truncate,
                                             commit_branch => CommitBranch},
                                    check_append_history_invariants(
                                      HistoryId, FinalEntries, Info, Data);
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
            ?ERROR("Received a malformed append request.~n"
                   "At seqno: ~p~n"
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
    OurCommittedSeqno = get_pending_meta(?META_COMMITTED_SEQNO, Data),
    case CommittedSeqno < OurCommittedSeqno of
        true ->
            HighTerm = get_pending_high_term(Data),
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
            {error, {missing_entries, build_metadata(Data)}};
        false ->
            ok
    end.

check_not_earlier_term(Term, Data) ->
    CurrentTerm = get_pending_meta(?META_TERM, Data),
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
            OurPendingCommittedSeqno =
                get_pending_meta(?META_COMMITTED_SEQNO, Data),

            NewData =
                if
                    OurCommittedSeqno =:= CommittedSeqno ->
                        Data;
                    OurPendingCommittedSeqno =:= CommittedSeqno ->
                        %% The committed seqno update is in-flight. Need to
                        %% wait before responding.
                        sync_storage(State, Data);
                    true ->
                        NewData0 =
                            store_meta(#{?META_COMMITTED_SEQNO =>
                                             CommittedSeqno},
                                       State, Data),

                        publish_state(State, NewData0),
                        announce_committed_seqno(CommittedSeqno),

                        NewData0
                end,

            ?DEBUG("Marked seqno ~p committed", [CommittedSeqno]),
            {keep_state, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_local_mark_committed(HistoryId, Term, CommittedSeqno, State, Data) ->
    HighSeqno = get_pending_high_seqno(Data),

    ?CHECK(check_provisioned_or_removed(State),
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
                    %% Make sure the response is based on fully persisted state.
                    NewData = sync_storage(State, Data),
                    {keep_state,
                     NewData,
                     {reply, From, {ok, build_metadata(NewData)}}};
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
                                maybe_cancel_snapshot(
                                  %% Reply with the installed snapshot seqno.
                                  {ok, SnapshotSeqno},
                                  State,
                                  do_install_snapshot(
                                    SnapshotSeqno, SnapshotHistoryId,
                                    SnapshotTerm, SnapshotConfig,
                                    RSMSnapshots, Metadata, State, Data));
                            commit_seqno ->
                                store_meta(Metadata, State, Data)
                        end,

                    {NewState, NewData1} =
                        check_state_transitions(State, Data, NewData0),
                    NewData = post_append(NewState, Data, NewData1),

                    {next_state, NewState, NewData,
                     {reply, From, {ok, build_metadata(NewData)}}}
            end;
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

get_snapshot_action(SnapshotSeqno, SnapshotTerm,
                    #data{storage = Storage} = Data) ->
    HighSeqno = get_pending_high_seqno(Data),
    CommittedSeqno = get_pending_meta(?META_COMMITTED_SEQNO, Data),

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
            NewData = store_meta(#{?META_PENDING_BRANCH => Branch},
                                 State, Data),

            PendingBranch = get_pending_meta(?META_PENDING_BRANCH, Data),
            case PendingBranch of
                undefined ->
                    ?DEBUG("Stored a branch record:~n~p", [Branch]);
                #branch{} ->
                    ?WARNING("Pending branch overriden by a new branch.~n"
                             "Pending branch:~n~p~n"
                             "New branch:~n~p",
                             [PendingBranch, Branch])
            end,

            publish_state(State, NewData),

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
    Peer = get_pending_meta(?META_PEER, Data),
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

handle_undo_branch(BranchId, From, State, Data) ->
    assert_valid_history_id(BranchId),
    case check_branch_id(BranchId, Data) of
        ok ->
            NewData = store_meta(#{?META_PENDING_BRANCH => undefined},
                                 State, Data),
            publish_state(State, NewData),
            announce_new_history(NewData),

            ?DEBUG("Undid branch ~p", [BranchId]),
            {keep_state, NewData, {reply, From, ok}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

handle_mark_removed(Peer, PeerId, From, State, Data) ->
    case check_mark_removed(Peer, PeerId, State, Data) of
        ok ->
            {NewState, NewData} = do_mark_removed(State, Data),
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
      ?META_PEER_ID := OurPeerId} = get_pending_meta(Data),

    Theirs = {Peer, PeerId},
    Ours = {OurPeer, OurPeerId},

    case Ours =:= Theirs of
        true ->
            ok;
        false ->
            {error, {peer_mismatch, Ours, Theirs}}
    end.

handle_force_snapshot(From, State, Data) ->
    case check_force_snapshot(State, Data) of
        ok ->
            LatestSnapshotSeqno = get_current_snapshot_seqno(Data),
            CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),

            case LatestSnapshotSeqno =:= CommittedSeqno of
                true ->
                    {keep_state_and_data,
                     {reply, From, {ok, CommittedSeqno}}};
                false ->
                    {keep_state, force_snapshot(From, Data)}
            end;
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_force_snapshot(State, Data) ->
    ?CHECK(check_provisioned_or_removed(State),
           check_not_wipe_requested(Data)).

check_branch_id(BranchId, Data) ->
    OurBranch = get_pending_meta(?META_PENDING_BRANCH, Data),
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
    OurHistoryId = get_pending_meta(?META_HISTORY_ID, Data),
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
      ?META_PENDING_BRANCH := PendingBranch} = get_pending_meta(Data),

    case PendingBranch of
        undefined ->
            CommittedHistoryId;
        #branch{history_id = PendingHistoryId} ->
            PendingHistoryId
    end.

check_same_term(Term, Data) ->
    %% The caller must have waited for the term to be established. Hence,
    %% get_meta() is used as opposed to get_pending_meta().
    OurTerm = get_pending_meta(?META_TERM, Data),
    case Term =:= OurTerm of
        true ->
            ok;
        false ->
            {error, {conflicting_term, OurTerm}}
    end.

check_log_range(StartSeqno, EndSeqno, Data) ->
    %% Don't return entries that have not been persisted yet.
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
    Storage = chronicle_storage:open(
                fun chronicle_snapshot_mgr:store_snapshot/1),
    maybe_seed_storage(#data{storage = Storage}).

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

maybe_announce_term_established(OldData, NewData) ->
    OldTerm = get_meta(?META_TERM, OldData),
    NewTerm = get_meta(?META_TERM, NewData),
    case NewTerm =:= OldTerm of
        true ->
            ok;
        false ->
            announce_term_established(NewTerm)
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
    case get_config(Data) of
        undefined ->
            undefined;
        #log_entry{history_id = HistoryId,
                   term = Term,
                   seqno = Seqno} ->
            {HistoryId, Term, Seqno}
    end.

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

announce_committed_seqno(CommittedSeqno) ->
    chronicle_events:notify(?RSM_EVENTS, {seqno_committed, CommittedSeqno}).

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

maybe_seed_storage(Data) ->
    Meta = get_meta(Data),
    case maps:size(Meta) > 0 of
        true ->
            Data;
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
            NewData = store_meta_async(none, SeedMeta, Data),

            %% There may not be any outstanding requests. So it's ok to sync
            %% storage directly.
            {[none], NewStorage} = chronicle_storage:sync(NewData#data.storage),
            NewData#data{storage = NewStorage}
    end.

append_entry(Entry, Meta, State, Data) ->
    Seqno = Entry#log_entry.seqno,
    NewData = append_entries_async(none,
                                   Seqno, Seqno,
                                   [Entry], Meta, false, Data),
    sync_storage(State, NewData).

store_meta_async(Request, Meta, #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:store_meta(Request, Meta, Storage),
    Data#data{storage = NewStorage}.

store_meta(Meta, State, Data) ->
    NewData = store_meta_async(none, Meta, Data),
    sync_storage(State, NewData).

append_entries_async(Request,
                     StartSeqno, EndSeqno, Entries, Metadata, Truncate,
                     #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:append(Request,
                                          StartSeqno, EndSeqno,
                                          Entries,
                                          #{meta => Metadata,
                                            truncate => Truncate}, Storage),
    Data#data{storage = NewStorage}.

record_snapshot(Seqno, HistoryId, Term, ConfigEntry,
                #data{storage = Storage} = Data) ->
    NewStorage = chronicle_storage:record_snapshot({record_snapshot, Seqno},
                                                   Seqno, HistoryId, Term,
                                                   ConfigEntry, Storage),
    Data#data{storage = NewStorage}.

do_install_snapshot(SnapshotSeqno, SnapshotHistoryId,
                    SnapshotTerm, SnapshotConfig,
                    RSMSnapshots, Metadata,
                    State, #data{storage = Storage} = Data) ->
    ok = chronicle_storage:prepare_snapshot(SnapshotSeqno),
    lists:foreach(
      fun ({RSM, RSMSnapshotBinary}) ->
              RSMSnapshot = binary_to_term(RSMSnapshotBinary),
              chronicle_storage:save_rsm_snapshot(SnapshotSeqno,
                                                  RSM, RSMSnapshot)
      end, maps:to_list(RSMSnapshots)),

    {Requests, NewStorage} =
        chronicle_storage:install_snapshot(SnapshotSeqno,
                                           SnapshotHistoryId, SnapshotTerm,
                                           SnapshotConfig, Metadata, Storage),

    %% install_snapshot() is synchronous to make the implementation of
    %% chronicle_storage (slightly) simpler. So we need to process any
    %% requests that might have completed.
    NewData0 = Data#data{storage = NewStorage},
    handle_completed_requests(Requests, State, Data, NewData0).

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

get_pending_meta(#data{storage = Storage}) ->
    chronicle_storage:get_pending_meta(Storage).

get_pending_meta(Key, Data) ->
    maps:get(Key, get_pending_meta(Data)).

get_high_seqno(#data{storage = Storage}) ->
    chronicle_storage:get_high_seqno(Storage).

get_pending_high_seqno(#data{storage = Storage}) ->
    chronicle_storage:get_pending_high_seqno(Storage).

get_high_term(#data{storage = Storage}) ->
    chronicle_storage:get_high_term(Storage).

get_pending_high_term(#data{storage = Storage}) ->
    chronicle_storage:get_pending_high_term(Storage).

get_config(#data{storage = Storage}) ->
    chronicle_storage:get_config(Storage).

get_committed_config(#data{storage = Storage}) ->
    chronicle_storage:get_committed_config(Storage).

get_pending_config(#data{storage = Storage}) ->
    chronicle_storage:get_pending_config(Storage).

get_pending_committed_config(#data{storage = Storage}) ->
    chronicle_storage:get_pending_committed_config(Storage).

get_current_snapshot_seqno(#data{storage = Storage}) ->
    chronicle_storage:get_current_snapshot_seqno(Storage).

release_snapshot(Seqno, #data{storage = Storage} = Data) ->
    Data#data{storage = chronicle_storage:release_snapshot(Seqno, Storage)}.

get_log_entry(Seqno, #data{storage = Storage}) ->
    chronicle_storage:get_log_entry(Seqno, Storage).

maybe_initiate_snapshot(State, Data) ->
    case State of
        _ when State =:= provisioned;
               State =:= removed ->
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
    case need_snapshot(Data) andalso not is_wipe_requested(Data) of
        true ->
            initiate_snapshot(Data);
        false ->
            Data
    end.

need_snapshot(Data) ->
    LatestSnapshotSeqno = get_current_snapshot_seqno(Data),
    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
    CommittedSeqno - LatestSnapshotSeqno >= ?SNAPSHOT_INTERVAL.

force_snapshot(From, #data{snapshot_state = SnapshotState} = Data) ->
    NewData =
        case SnapshotState of
            #snapshot_state{} ->
                Data;
            {retry, _} ->
                retry_snapshot(Data);
            undefined ->
                initiate_snapshot(Data)
        end,

    add_snapshot_waiter(From, NewData).

retry_snapshot(Data) ->
    {retry, TRef} = Data#data.snapshot_state,
    cancel_snapshot_retry_timer(TRef),
    initiate_snapshot(Data#data{snapshot_state = undefined}).

add_snapshot_waiter(From, #data{snapshot_state = SnapshotState} = Data) ->
    #snapshot_state{waiters = Waiters} = SnapshotState,
    NewWaiters = [From | Waiters],
    NewSnapshotState = SnapshotState#snapshot_state{waiters = NewWaiters},
    Data#data{snapshot_state = NewSnapshotState}.

reply_snapshot_waiters(Reply, #data{snapshot_state = SnapshotState}) ->
    #snapshot_state{waiters = Waiters} = SnapshotState,
    lists:foreach(
      fun (From) ->
              gen_statem:reply(From, Reply)
      end, Waiters).

reply_snapshot_ok(Seqno, Data) ->
    reply_snapshot_waiters({ok, Seqno}, Data).

reply_snapshot_failed(Data) ->
    reply_snapshot_waiters({error, snapshot_failed}, Data).

initiate_snapshot(Data) ->
    undefined = Data#data.snapshot_state,

    CommittedSeqno = get_meta(?META_COMMITTED_SEQNO, Data),
    CommittedConfig = get_committed_config(Data),
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
    SnapshotState = #snapshot_state{tref = TRef,
                                    history_id = HistoryId,
                                    term = Term,
                                    seqno = CommittedSeqno,
                                    config = CommittedConfig,
                                    waiters = [],
                                    write_pending = false},

    RSMs = maps:keys(CommittedRSMs),
    chronicle_snapshot_mgr:pending_snapshot(CommittedSeqno, RSMs),
    chronicle_events:notify(?RSM_EVENTS, {take_snapshot, CommittedSeqno}),

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
    cancel_snapshot_retry_timer(TRef),
    reset_snapshot_state(Data).

cancel_snapshot_retry_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ?FLUSH(retry_snapshot),
    ok.

maybe_cancel_snapshot(State, Data) ->
    maybe_cancel_snapshot({error, snapshot_canceled}, State, Data).

maybe_cancel_snapshot(Reply, State,
                      #data{snapshot_state = SnapshotState} = Data) ->
    case SnapshotState of
        undefined ->
            Data;
        {retry, _TRef} ->
            cancel_snapshot_retry(Data);
        #snapshot_state{write_pending = true} ->
            %% We've already issued a write to record the snapshot. This can't
            %% be canceled. So we wait until the write goes through instead.
            NewData = sync_storage(State, Data),
            undefined = NewData#data.snapshot_state,
            NewData;
        #snapshot_state{} ->
            reply_snapshot_waiters(Reply, Data),
            cancel_snapshot(Data)
    end.

cancel_snapshot(#data{snapshot_state = SnapshotState,
                      storage = Storage} = Data) ->
    false = SnapshotState#snapshot_state.write_pending,
    #snapshot_state{tref = TRef,
                    seqno = SnapshotSeqno} = SnapshotState,

    cancel_snapshot_timer(TRef),

    chronicle_snapshot_mgr:cancel_pending_snapshot(SnapshotSeqno),
    flush_snapshot_mgr_msgs(),

    ?INFO("Snapshot at seqno ~p canceled.", [SnapshotSeqno]),
    chronicle_storage:delete_snapshot(SnapshotSeqno, Storage),
    reset_snapshot_state(Data).

reset_snapshot_state(Data) ->
    Data#data{snapshot_state = undefined, snapshot_attempts = 0}.

get_rsms(#log_entry{value = Config}) ->
    chronicle_config:get_rsms(Config).

read_rsm_snapshot(Name, Seqno) ->
    case chronicle_storage:read_rsm_snapshot(Name, Seqno) of
        {ok, Snapshot} ->
            Snapshot;
        {error, Error} ->
            exit({get_rsm_snapshot_failed, Name, Seqno, Error})
    end.

check_got_removed(State, OldData, NewData) ->
    OldConfig = get_pending_committed_config(OldData),
    NewConfig = get_pending_committed_config(NewData),

    OldRevision = chronicle_utils:log_entry_revision(OldConfig),
    NewRevision = chronicle_utils:log_entry_revision(NewConfig),

    case OldRevision =:= NewRevision of
        true ->
            {State, NewData};
        false ->
            check_got_removed_with_config(NewConfig, State, NewData)
    end.

check_got_removed(State, Data) ->
    case State of
        provisioned ->
            CommittedConfig = get_pending_committed_config(Data),
            check_got_removed_with_config(CommittedConfig, State, Data);
        _ ->
            {State, Data}
    end.

check_got_removed_with_config(#log_entry{value = Config}, State, Data) ->
    #{?META_PEER := Peer, ?META_PEER_ID := PeerId} = get_pending_meta(Data),
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
                    do_mark_removed(State, Data)
            end
    end.

do_mark_removed(State, Data) ->
    NewState = removed,
    NewData = store_meta(#{?META_STATE => ?META_STATE_REMOVED}, State, Data),
    publish_state(NewState, NewData),
    announce_system_state(removed, build_metadata(NewData)),
    {NewState, NewData}.

check_state_transitions(State, Data) ->
    {NewState, NewData} = check_join_cluster_done(State, Data),
    check_got_removed(NewState, NewData).

check_state_transitions(State, OldData, NewData) ->
    case State of
        provisioned ->
            OldSeqno = get_pending_meta(?META_COMMITTED_SEQNO, OldData),
            NewSeqno = get_pending_meta(?META_COMMITTED_SEQNO, NewData),

            case OldSeqno =:= NewSeqno of
                true ->
                    {State, NewData};
                false ->
                    check_got_removed(State, OldData, NewData)
            end;
        _ ->
            check_state_transitions(State, NewData)
    end.

sync_storage(State, #data{storage = Storage} = Data) ->
    {Requests, NewStorage} = chronicle_storage:sync(Storage),
    NewData = Data#data{storage = NewStorage},
    handle_completed_requests(Requests, State, Data, NewData).

inc_pending_appends(#data{pending_appends = Count} = Data) ->
    Data#data{pending_appends = Count + 1}.

dec_pending_appends(By, #data{pending_appends = Count} = Data) ->
    true = (Count >= By),
    Data#data{pending_appends = Count - By}.
