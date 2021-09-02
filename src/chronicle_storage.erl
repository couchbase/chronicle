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
-module(chronicle_storage).

-include("chronicle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([open/1, wipe/0,
         get_meta/1, get_pending_meta/1,
         get_high_seqno/1, get_pending_high_seqno/1,
         get_high_term/1, get_pending_high_term/1,
         get_config/1, get_pending_config/1,
         get_committed_config/1, get_pending_committed_config/1,
         store_meta/3, append/6, sync/1, close/1,
         install_snapshot/6, record_snapshot/6, delete_snapshot/2,
         prepare_snapshot/1, copy_snapshot/3,
         read_rsm_snapshot/1, read_rsm_snapshot/2, save_rsm_snapshot/3,
         get_current_snapshot/1, get_current_snapshot_seqno/1,
         release_snapshot/2,
         get_term_for_committed_seqno/1, get_term_for_seqno/2,
         get_log/0, get_log/2, get_log_committed/2, get_log_entry/2,
         ensure_rsm_dir/1, snapshot_dir/1,
         handle_event/2,
         map_append/2]).

-ifdef(TEST).
-define(WRITER, list_to_atom("chronicle_storage_writer-" ++
                                 atom_to_list(vnet:vnode()))).
-else.
-define(WRITER, chronicle_storage_writer).
-endif.

-define(MEM_LOG_INFO_TAB, ?ETS_TABLE(chronicle_mem_log_info)).
-define(MEM_LOG_TAB, ?ETS_TABLE(chronicle_mem_log)).

-define(RANGE_KEY, '$range').
-define(SNAPSHOT_KEY, '$snapshot').

-define(HISTO_METRIC(Op), {<<"chronicle_disk_latency">>, [{op, Op}]}).
-define(HISTO_MAX, 5000).
-define(HISTO_UNIT, millisecond).

-define(TIME_OK(Op, StartTS),
        begin
            __EndTS = erlang:monotonic_time(?HISTO_UNIT),
            __Diff = __EndTS - StartTS,
            chronicle_stats:report_histo(
              ?HISTO_METRIC(Op), ?HISTO_MAX, ?HISTO_UNIT, __Diff)
        end).

-define(TIME(Op, Body), ?TIME(Op, ok, Body)).
-define(TIME(Op, OkPattern, Body),
        begin
            __StartTS = erlang:monotonic_time(?HISTO_UNIT),
            __Result = Body,
            case __Result of
                OkPattern ->
                    ?TIME_OK(Op, __StartTS);
                _ ->
                    ok
            end,

            __Result
        end).

-type meta_state() :: ?META_STATE_PROVISIONED
                    | ?META_STATE_NOT_PROVISIONED
                    | {?META_STATE_PREPARE_JOIN,
                       #{config := #log_entry{}}}
                    | {?META_STATE_JOIN_CLUSTER,
                       #{seqno := chronicle:seqno(),
                         config := #log_entry{}}}
                    | ?META_STATE_REMOVED.
-type meta() :: #{ ?META_STATE => meta_state(),
                   ?META_PEER => chronicle:peer(),
                   ?META_PEER_ID => chronicle:peer_id(),
                   ?META_HISTORY_ID => chronicle:history_id(),
                   ?META_TERM => chronicle:leader_term(),
                   ?META_COMMITTED_SEQNO => chronicle:seqno(),
                   ?META_PENDING_BRANCH => undefined | #branch{},
                   ?META_VERSION => chronicle:compat_version() }.

-record(storage, { current_log_ix,
                   current_log_path,
                   current_log_start_seqno,
                   current_log_data_size,

                   low_seqno,
                   high_seqno,
                   meta,

                   pending_high_seqno,
                   pending_meta,

                   config,
                   committed_config,

                   pending_configs,
                   pending_committed_config,

                   current_snapshot,
                   extra_snapshots,

                   data_dir,

                   log_info_tab,
                   log_tab,

                   log_segments,

                   writer,
                   requests = queue:new(),
                   rollover_pending = false,

                   publish_snapshot_fun
                  }).

open(PublishSnapshotFun) ->
    _ = ets:new(?MEM_LOG_INFO_TAB,
                [protected, set, named_table, {read_concurrency, true}]),
    _ = ets:new(?MEM_LOG_TAB,
                [protected, set, named_table,
                 {keypos, #log_entry.seqno}, {read_concurrency, true}]),
    Storage0 = #storage{log_info_tab = ets:whereis(?MEM_LOG_INFO_TAB),
                        log_tab = ets:whereis(?MEM_LOG_TAB),
                        publish_snapshot_fun = PublishSnapshotFun},

    DataDir = chronicle_env:data_dir(),
    maybe_complete_wipe(DataDir),
    ensure_dirs(DataDir),
    check_version(DataDir),

    Storage1 = Storage0#storage{data_dir = DataDir},
    Storage2 = open_logs(Storage1),
    validate_state(Storage2),
    Storage3 = spawn_writer(Storage2),
    Storage4 = compact(Storage3),

    publish_snapshot(Storage4),
    Storage5 = cleanup_snapshots(Storage4),

    publish(Storage5).

open_logs(#storage{data_dir = DataDir} = Storage) ->
    {Orphans, Sealed, Current} = find_logs(DataDir),

    NewStorage0 = Storage#storage{meta = #{},
                                  pending_meta = #{},
                                  config = undefined,
                                  committed_config = undefined,
                                  pending_configs = [],
                                  low_seqno = ?NO_SEQNO + 1,
                                  high_seqno = ?NO_SEQNO,
                                  pending_high_seqno = ?NO_SEQNO,
                                  current_snapshot = no_snapshot,
                                  extra_snapshots = [],
                                  log_segments = []},

    %% TODO: Be more robust when dealing with corrupt log files. As long as a
    %% consistent state can be recovered, corrupt logs should not be a fatal
    %% error.
    NewStorage1 =
        lists:foldl(
          fun ({_LogIx, LogPath}, Acc0) ->
                  Acc = Acc0#storage{current_log_path = LogPath},
                  case chronicle_log:read_log(LogPath,
                                              fun handle_user_data/2,
                                              fun handle_log_entry/2,
                                              Acc) of
                      {ok, NewAcc} ->
                          #storage{
                             log_segments = LogSegments,
                             current_log_start_seqno = LogStartSeqno} = NewAcc,

                          LogSegment = {LogPath, LogStartSeqno},
                          NewLogSegments = [LogSegment | LogSegments],
                          NewAcc#storage{log_segments = NewLogSegments};
                      {error, Error} ->
                          ?ERROR("Failed to read log ~p: ~p", [LogPath, Error]),
                          exit({failed_to_read_log, LogPath, Error})
                  end
          end, NewStorage0, Sealed),

    {CurrentLogIx, CurrentLogPath} = Current,
    Result = open_current_log(CurrentLogIx, CurrentLogPath, NewStorage1),

    maybe_delete_orphans(Orphans),

    Result.

open_current_log(LogIx, LogPath, Storage0) ->
    Storage = Storage0#storage{current_log_ix = LogIx,
                               current_log_path = LogPath},
    {Log, NewStorage} =
        case chronicle_log:open(LogPath,
                                fun handle_user_data/2,
                                fun handle_log_entry/2,
                                Storage) of
            {ok, L, NewStorage0} ->
                {L, NewStorage0};
            {error, Error} when Error =:= enoent;
                                Error =:= no_header ->
                ?INFO("Error while opening log file ~p: ~p", [LogPath, Error]),
                #storage{meta = Meta,
                         config = Config,
                         high_seqno = HighSeqno} = Storage,
                L = create_log(Config, Meta, HighSeqno, LogPath),
                {L, Storage#storage{current_log_start_seqno = HighSeqno + 1}};
            {error, Error} ->
                ?ERROR("Failed to open log ~p: ~p", [LogPath, Error]),
                exit({failed_to_open_log, LogPath, Error})
        end,

    log_sync(Log),
    {ok, DataSize} = chronicle_log:data_size(Log),
    ok = chronicle_log:close(Log),

    NewStorage#storage{current_log_data_size = DataSize}.

maybe_delete_orphans([]) ->
    ok;
maybe_delete_orphans(Orphans) ->
    Paths = [LogPath || {_, LogPath} <- Orphans],
    ?WARNING("Found orphan logs. Going to delete them. Logs:~n~p", [Paths]),
    try_delete_files(Paths).

try_delete_files(Paths) ->
    lists:foreach(
      fun (Path) ->
              case chronicle_utils:delete_recursive(Path) of
                  ok ->
                      ok;
                  {error, Error} ->
                      ?WARNING("Failed to delete file ~p: ~p", [Path, Error])
              end
      end, Paths).

rollover(#storage{rollover_pending = true} = Storage) ->
    Storage;
rollover(#storage{current_log_ix = CurrentLogIx,
                  data_dir = DataDir,
                  pending_meta = Meta,
                  pending_high_seqno = HighSeqno,
                  writer = Writer} = Storage) ->
    Config = get_pending_config(Storage),
    NewLogPath = log_path(DataDir, CurrentLogIx + 1),
    Writer ! {rollover, Config, Meta, HighSeqno, NewLogPath},
    Storage#storage{rollover_pending = true}.

create_log(Config, Meta, HighSeqno, LogPath) ->
    ?INFO("Creating log file ~s (high seqno = ~b)", [LogPath, HighSeqno]),

    LogData = #{config => Config, meta => Meta, high_seqno => HighSeqno},
    case chronicle_log:create(LogPath, LogData) of
        {ok, Log} ->
            Log;
        {error, Error} ->
            exit({create_log_failed, LogPath, Error})
    end.

publish(#storage{log_info_tab = LogInfoTab,
                 low_seqno = LowSeqno,
                 high_seqno = HighSeqno,
                 meta = Meta} = Storage) ->
    OldPublishedRange = get_published_seqno_range(),

    CommittedSeqno = get_committed_seqno(Meta),
    ets:insert(LogInfoTab, {?RANGE_KEY, LowSeqno, HighSeqno, CommittedSeqno}),

    case OldPublishedRange of
        not_published ->
            false;
        {PublishedLowSeqno, PublishedHighSeqno, _} ->
            case PublishedLowSeqno < LowSeqno of
                true ->
                    mem_log_delete_range(PublishedLowSeqno,
                                         min(LowSeqno - 1, PublishedHighSeqno),
                                         Storage);
                false ->
                    ok
            end,

            case PublishedHighSeqno > HighSeqno of
                true ->
                    mem_log_delete_range(HighSeqno + 1,
                                         PublishedHighSeqno, Storage);
                false ->
                    ok
            end
    end,

    Storage.

ensure_dirs(DataDir) ->
    ok = chronicle_utils:mkdir_p(logs_dir(DataDir)),
    ok = chronicle_utils:mkdir_p(snapshots_dir(DataDir)),
    ok = chronicle_utils:mkdir_p(rsms_dir(DataDir)),

    sync_dir(chronicle_dir(DataDir)),
    sync_dir(DataDir).

chronicle_dir(DataDir) ->
    filename:join(DataDir, "chronicle").

logs_dir(DataDir) ->
    filename:join(chronicle_dir(DataDir), "logs").

snapshots_dir(DataDir) ->
    filename:join(chronicle_dir(DataDir), "snapshots").

rsms_dir(DataDir) ->
    filename:join(chronicle_dir(DataDir), "rsms").

snapshot_dir(Seqno) ->
    snapshot_dir(chronicle_env:data_dir(), Seqno).

snapshot_dir(DataDir, Seqno) ->
    filename:join(snapshots_dir(DataDir), integer_to_list(Seqno)).

wipe() ->
    DataDir = chronicle_env:data_dir(),
    ChronicleDir = chronicle_dir(DataDir),
    case file_exists(ChronicleDir, directory) of
        true ->
            ok = chronicle_utils:create_marker(wipe_marker(DataDir)),
            complete_wipe(DataDir);
        false ->
            ok
    end.

complete_wipe(DataDir) ->
    case chronicle_utils:delete_recursive(chronicle_dir(DataDir)) of
        ok ->
            sync_dir(DataDir),
            ok = chronicle_utils:delete_marker(wipe_marker(DataDir));
        {error, Error} ->
            exit({wipe_failed, Error})
    end.

maybe_complete_wipe(DataDir) ->
    Marker = wipe_marker(DataDir),
    case file_exists(Marker, regular) of
        true ->
            ?INFO("Found wipe marker file ~p. Completing wipe.", [Marker]),
            complete_wipe(DataDir);
        false ->
            ok
    end.

wipe_marker(DataDir) ->
    filename:join(DataDir, "chronicle.wipe").

handle_user_data(UserData, Storage) ->
    #{config := Config, meta := Meta, high_seqno := HighSeqno} = UserData,

    #storage{config = StorageConfig,
             meta = StorageMeta,
             high_seqno = StorageHighSeqno,
             current_log_path = LogPath} = Storage,

    NewStorage = Storage#storage{current_log_start_seqno = HighSeqno + 1},
    case StorageConfig of
        undefined ->
            NewStorage1 =
                NewStorage#storage{meta = Meta,
                                   pending_meta = Meta,
                                   high_seqno = HighSeqno,
                                   pending_high_seqno = HighSeqno,
                                   low_seqno = HighSeqno + 1},
            case Config of
                undefined ->
                    NewStorage1;
                #log_entry{} ->
                    NewStorage2 = append_configs([Config], NewStorage1),
                    sync_pending(NewStorage2)
            end;
        _ ->
            case Config =:= StorageConfig
                andalso Meta =:= StorageMeta
                andalso HighSeqno =:= StorageHighSeqno of
                true ->
                    NewStorage;
                false ->
                    exit({inconsistent_log,
                          LogPath,
                          {config, Config, StorageConfig},
                          {meta, Meta, StorageMeta},
                          {high_seqno, HighSeqno, StorageHighSeqno}})
            end
    end.

%% TODO: In many respects log entry handlers duplicate the code in functions
%% that log the corresponding entries. Get rid of this duplication.
handle_log_entry(Entry, Storage) ->
    case Entry of
        {append, StartSeqno, EndSeqno, Meta, Truncate, Entries} ->
            handle_log_append(StartSeqno, EndSeqno,
                              Meta, Truncate, Entries, Storage);
        {meta, Meta} ->
            sync_pending(compact_configs(add_pending_meta(Meta, Storage)));
        {snapshot, Seqno, HistoryId, Term, Config} ->
            add_snapshot(Seqno, HistoryId, Term, Config, Storage);
        {install_snapshot, Seqno, HistoryId, Term, Config, Meta} ->
            ets:delete_all_objects(Storage#storage.log_tab),
            do_install_snapshot(Seqno, HistoryId, Term, Config, Meta, Storage)
    end.

handle_log_append(StartSeqno, EndSeqno, Meta, Truncate, Entries,
                  #storage{low_seqno = LowSeqno,
                           high_seqno = HighSeqno} = Storage) ->
    NewStorage0 =
        case Truncate of
            true ->
                %% Need to do this explicitly, since currently prepare_append()
                %% doesn't truncate until publish() is called.
                mem_log_delete_range(StartSeqno, HighSeqno, Storage),

                %% For the first log segment it's possible for truncate to go
                %% below the low seqno. So it needs to be adjusted accordingly.
                case StartSeqno < LowSeqno of
                    true ->
                        Storage#storage{low_seqno = StartSeqno};
                    false ->
                        Storage
                end;
            false ->
                case StartSeqno =:= HighSeqno + 1 of
                    true ->
                        ok;
                    false ->
                        exit({inconsistent_log,
                              Storage#storage.current_log_path,
                              {append, StartSeqno, EndSeqno, HighSeqno}})
                end,

                Storage
        end,

    NewStorage1 = prepare_append(StartSeqno, EndSeqno, Meta,
                                 Truncate, Entries, NewStorage0),
    sync_pending(NewStorage1).

sync_pending(#storage{
                pending_meta = Meta,
                pending_high_seqno = HighSeqno,
                pending_committed_config = CommittedConfig} = Storage) ->
    Storage#storage{meta = Meta,
                    high_seqno = HighSeqno,
                    config = get_pending_config(Storage),
                    committed_config = CommittedConfig}.

add_meta(Meta, #storage{meta = CurrentMeta} = Storage) ->
    Storage#storage{meta = maps:merge(CurrentMeta, Meta)}.

add_pending_meta(Meta, #storage{pending_meta = CurrentMeta} = Storage) ->
    Storage#storage{pending_meta = maps:merge(CurrentMeta, Meta)}.

add_snapshot(Seqno, HistoryId, Term, Config, Storage) ->
    Snapshot = {Seqno, HistoryId, Term, Config},
    NewStorage = retire_snapshot(Storage),
    NewStorage#storage{current_snapshot = Snapshot}.

retire_snapshot(#storage{current_snapshot = CurrentSnapshot,
                         extra_snapshots = ExtraSnapshots} = Storage) ->
    case CurrentSnapshot of
        no_snapshot ->
            Storage;
        _ ->
            Storage#storage{extra_snapshots =
                                [CurrentSnapshot | ExtraSnapshots]}
    end.

is_config_entry(#log_entry{value = Value}) ->
    chronicle_config:is_config(Value).

-spec get_meta(#storage{}) -> meta().
get_meta(Storage) ->
    Storage#storage.meta.

-spec get_pending_meta(#storage{}) -> meta().
get_pending_meta(Storage) ->
    Storage#storage.pending_meta.

get_committed_seqno(#{?META_COMMITTED_SEQNO := CommittedSeqno}) ->
    CommittedSeqno;
get_committed_seqno(_) ->
    ?NO_SEQNO.

get_high_seqno(Storage) ->
    Storage#storage.high_seqno.

get_pending_high_seqno(Storage) ->
    Storage#storage.pending_high_seqno.

get_high_term(Storage) ->
    {ok, Term} = get_term_for_seqno(get_high_seqno(Storage), Storage),
    Term.

get_pending_high_term(Storage) ->
    {ok, Term} = get_term_for_seqno(get_pending_high_seqno(Storage), Storage),
    Term.

get_term_for_committed_seqno(?NO_SEQNO) ->
    {ok, ?NO_TERM};
get_term_for_committed_seqno(Seqno) ->
    case get_log_committed(Seqno, Seqno) of
        {ok, [#log_entry{term = Term}]} ->
            {ok, Term};
        {error, compacted} ->
            case get_published_snapshot() of
                {SnapSeqno, SnapTerm} when Seqno =:= SnapSeqno ->
                    {ok, SnapTerm};
                _ ->
                    {error, compacted}
            end
    end.

get_term_for_seqno(Seqno, #storage{low_seqno = LowSeqno,
                                   pending_high_seqno = HighSeqno} = Storage) ->
    case Seqno =< HighSeqno of
        true ->
            if
                Seqno >= LowSeqno ->
                    {ok, Entry} = get_log_entry(Seqno, Storage),
                    {ok, Entry#log_entry.term};
                Seqno =:= ?NO_SEQNO ->
                    {ok, ?NO_TERM};
                true ->
                    case get_current_snapshot(Storage) of
                        {SnapSeqno, _, SnapTerm, _} when Seqno =:= SnapSeqno ->
                            {ok, SnapTerm};
                        _ ->
                            {error, compacted}
                    end
            end;
        false ->
            {error, {invalid_seqno, Seqno, LowSeqno, HighSeqno}}
    end.

get_config(Storage) ->
    Storage#storage.config.

get_committed_config(Storage) ->
    Storage#storage.committed_config.

get_pending_config(#storage{pending_configs = PendingConfigs} = Storage) ->
    case PendingConfigs of
        [] ->
            get_pending_committed_config(Storage);
        [Config|_] ->
            Config
    end.

get_pending_committed_config(Storage) ->
    Storage#storage.pending_committed_config.

-spec store_meta(any(), meta(), #storage{}) -> #storage{}.
store_meta(Opaque, Updates, Storage) ->
    Meta = dedup_meta(Updates, Storage),
    case maps:size(Meta) > 0 of
        true ->
            NewStorage0 = add_pending_meta(Meta, Storage),
            NewStorage = compact_configs(NewStorage0),
            Record = {meta, Meta},
            Data = {meta, Meta, get_pending_committed_config(NewStorage)},
            writer_append(Opaque, Record, Data, NewStorage);
        false ->
            writer_sync(Opaque, Storage)
    end.

dedup_meta(Updates, #storage{pending_meta = Meta}) ->
    maps:filter(
      fun (Key, Value) ->
              case maps:find(Key, Meta) of
                  {ok, CurrentValue} ->
                      Value =/= CurrentValue;
                  error ->
                      true
              end
      end, Updates).

append(Opaque, StartSeqno, EndSeqno, Entries, Opts, Storage) ->
    Truncate = append_handle_truncate(StartSeqno, Opts, Storage),
    Meta = append_handle_meta(Opts, Storage),
    LogEntry = {append, StartSeqno, EndSeqno, Meta, Truncate, Entries},

    NewStorage = prepare_append(StartSeqno, EndSeqno,
                                Meta, Truncate, Entries, Storage),
    AppendData = {append,
                  EndSeqno, Meta,
                  get_pending_config(NewStorage),
                  get_pending_committed_config(NewStorage)},

    writer_append(Opaque, LogEntry, AppendData, NewStorage).

prepare_append(StartSeqno, EndSeqno, Meta, Truncate, Entries, Storage) ->
    NewStorage0 =
        case Truncate of
            true ->
                truncate_configs(StartSeqno, Storage);
            false ->
                Storage
        end,

    NewStorage1 = mem_log_append(EndSeqno, Entries, NewStorage0),
    NewStorage2 = add_pending_meta(Meta, NewStorage1),
    append_configs(Entries, NewStorage2).

append_handle_meta(Opts, Storage) ->
    case maps:find(meta, Opts) of
        {ok, Meta} ->
            dedup_meta(Meta, Storage);
        error ->
            #{}
    end.

append_handle_truncate(StartSeqno, Opts,
                       #storage{pending_meta = Meta,
                                low_seqno = LowSeqno,
                                pending_high_seqno = HighSeqno}) ->
    Truncate = maps:get(truncate, Opts, false),
    case Truncate of
        true ->
            CommittedSeqno = get_committed_seqno(Meta),

            true = (StartSeqno > CommittedSeqno),
            true = (StartSeqno =< HighSeqno + 1),
            true = (StartSeqno >= LowSeqno);
        false ->
            true = (StartSeqno =:= HighSeqno + 1)
    end,

    Truncate.

sync(Storage) ->
    sync_loop([], Storage).

sync_loop(Acc, #storage{requests = Requests} = Storage) ->
    case queue:is_empty(Requests) of
        true ->
            {lists:append(lists:reverse(Acc)), Storage};
        false ->
            receive
                {?MODULE, Event} ->
                    case handle_event(Event, Storage) of
                        {ok, NewStorage} ->
                            sync_loop(Acc, NewStorage);
                        {ok, NewStorage, Opaques} ->
                            sync_loop([Opaques|Acc], NewStorage)
                    end
            end
    end.

close(#storage{log_tab = LogTab,
               log_info_tab = LogInfoTab,
               writer = Writer}) ->
    ets:delete(LogTab),
    ets:delete(LogInfoTab),

    case Writer of
        undefined ->
            ok;
        _ ->
            chronicle_utils:terminate_linked_process(Writer, shutdown),
            ?FLUSH({?MODULE, _}),
            ok
    end.

handle_event(Event, Storage) ->
    case Event of
        {append_done, BytesWritten, Requests} ->
            handle_append_done(BytesWritten, Requests, Storage);
        {rollover_done, HighSeqno} ->
            handle_rollover_done(HighSeqno, Storage)
    end.

handle_append_done(BytesWritten, DoneRefs,
                   #storage{current_log_data_size = DataSize,
                            requests = Requests} = Storage) ->
    NewDataSize = DataSize + BytesWritten,
    {NewRequests, Opaques, ReqDatas} = extract_requests(Requests, DoneRefs),

    NewStorage0 = Storage#storage{requests = NewRequests,
                                  current_log_data_size = NewDataSize},

    NewStorage = lists:foldl(
                   fun (Req, Acc) ->
                           handle_request_done(Req, Acc)
                   end, NewStorage0, ReqDatas),

    {ok, NewStorage, Opaques}.

handle_request_done(sync, Storage) ->
    Storage;
handle_request_done({meta, Meta, CommittedConfig}, Storage) ->
    NewStorage0 = add_meta(Meta, Storage),
    NewStorage = NewStorage0#storage{committed_config = CommittedConfig},
    publish(NewStorage);
handle_request_done({append,
                     HighSeqno, Meta,
                     Config, CommittedConfig}, Storage) ->
    NewStorage0 = add_meta(Meta, Storage),
    NewStorage1 = NewStorage0#storage{high_seqno = HighSeqno,
                                      config = Config,
                                      committed_config = CommittedConfig},
    publish(NewStorage1);
handle_request_done({snapshot, Seqno, HistoryId, Term, Config}, Storage) ->
    NewStorage = add_snapshot(Seqno, HistoryId, Term, Config, Storage),
    publish_snapshot(NewStorage),
    publish(compact(NewStorage));
handle_request_done(none, Storage) ->
    Storage.

extract_requests(Requests, DoneRefs) ->
    extract_requests(Requests, DoneRefs, [], []).

extract_requests(Requests, [], AccOpaques, AccReqDatas) ->
    {Requests,
     lists:reverse(AccOpaques),
     lists:reverse(AccReqDatas)};
extract_requests(Requests, [Ref|Rest], AccOpaques, AccReqDatas) ->
    {{value, {Ref, Opaque, ReqData}}, NewRequests} = queue:out(Requests),
    extract_requests(NewRequests, Rest,
                     [Opaque|AccOpaques], [ReqData|AccReqDatas]).

handle_rollover_done(HighSeqno,
                     #storage{data_dir = DataDir,

                              current_log_path = CurrentLogPath,
                              current_log_start_seqno = CurrentLogStartSeqno,
                              current_log_ix = CurrentLogIx,
                              log_segments = LogSegments} = Storage) ->
    LogSegment = {CurrentLogPath, CurrentLogStartSeqno},
    NewLogSegments = [LogSegment | LogSegments],

    NewLogIx = CurrentLogIx + 1,
    NewLogPath = log_path(DataDir, NewLogIx),
    {ok, Storage#storage{
           current_log_ix = NewLogIx,
           current_log_path = NewLogPath,
           current_log_data_size = 0,
           current_log_start_seqno = HighSeqno + 1,
           log_segments = NewLogSegments,
           rollover_pending = false}}.

find_logs(DataDir) ->
    LogsDir = logs_dir(DataDir),
    Logs0 = list_dir(LogsDir,
                     "^([[:digit:]]\+).log$", regular,
                     fun (Path, [Index]) ->
                             {list_to_integer(Index), Path}
                     end),
    Logs1 = lists:keysort(1, Logs0),
    case Logs1 of
        [] ->
            {[], [], {0, log_path(DataDir, 0)}};
        _ ->
            {Orphans, NonOrphans} = find_orphan_logs(Logs1),
            {Orphans, lists:droplast(NonOrphans), lists:last(NonOrphans)}
    end.

cleanup_snapshots(#storage{extra_snapshots = Snapshots} = Storage) ->
    case Snapshots of
        [] ->
            ok;
        _ ->
            Seqnos = [Seqno || {Seqno, _, _, _} <- Snapshots],
            ?DEBUG("Going to delete redundant snapshots. Seqnos: ~w", [Seqnos])
    end,

    cleanup_orphan_snapshots(Storage#storage{extra_snapshots = []}).

cleanup_orphan_snapshots(#storage{data_dir = DataDir} = Storage) ->
    SnapshotSeqno = get_current_snapshot_seqno(Storage),
    DiskSnapshots = find_disk_snapshots(DataDir),
    OrphanSnapshots =
        lists:filtermap(
          fun ({Seqno, Path}) ->
                  case Seqno =:= SnapshotSeqno of
                      true ->
                          false;
                      false ->
                          {true, Path}
                  end
          end, DiskSnapshots),

    case OrphanSnapshots of
        [] ->
            ok;
        _ ->
            ?WARNING("Found orphan snapshots.~n"
                     "Curent snapshot seqno: ~p~n"
                     "Orphan snapshot files:~n~p",
                     [SnapshotSeqno, OrphanSnapshots]),
            try_delete_files(OrphanSnapshots)
    end,

    Storage.

find_disk_snapshots(DataDir) ->
    SnapshotsDir = snapshots_dir(DataDir),
    list_dir(SnapshotsDir,
             "^([[:digit:]]\+)$", directory,
             fun (Path, [Seqno]) ->
                     {list_to_integer(Seqno), Path}
             end).

list_dir(Dir, RegExp, Type, Fun) ->
    case file:list_dir(Dir) of
        {ok, Children} ->
            lists:filtermap(
              fun (Name) ->
                      Path = filename:join(Dir, Name),
                      case list_dir_handle_child(Path, Name,
                                                 RegExp, Type, Fun) of
                          {ok, Result} ->
                              {true, Result};
                          {error, Error} ->
                              ?WARNING("Ignoring unexpected file ~p: ~p",
                                       [Path, Error]),
                              false
                      end
              end, Children);
        {error, Error} ->
            exit({list_dir_failed, Dir, Error})
    end.

list_dir_handle_child(Path, Name, RegExp, Type, Fun) ->
    case re:run(Name, RegExp,
                [{capture, all_but_first, list}]) of
        {match, Match} ->
            case chronicle_utils:check_file_exists(Path, Type) of
                ok ->
                    {ok, Fun(Path, Match)};
                {error, {wrong_file_type, _, _}} = Error ->
                    Error;
                {error, _} = Error ->
                    exit({check_file_exists_failed, Path, Error})
            end;
        nomatch ->
            {error, unknown_file}
    end.

find_orphan_logs(Logs) ->
    [Last | Rest] = lists:reverse(Logs),
    find_orphan_logs_loop(Rest, [Last]).

find_orphan_logs_loop([], Acc) ->
    {[], Acc};
find_orphan_logs_loop([{LogIx, _} = Log | Rest] = Logs,
                      [{PrevIx, _} | _] = Acc) ->
    case LogIx + 1 =:= PrevIx of
        true ->
            find_orphan_logs_loop(Rest, [Log | Acc]);
        false ->
            {lists:reverse(Logs), Acc}
    end.

-ifdef(TEST).
find_orphan_logs_test() ->
    Logs0 = [{0, "0.log"}, {1, "1.log"}, {2, "2.log"}],
    ?assertEqual({[], Logs0}, find_orphan_logs(Logs0)),

    Logs1 = [{4, "4.log"}, {5, "5.log"}],
    ?assertEqual({[], Logs1}, find_orphan_logs(Logs1)),

    ?assertEqual({Logs0, Logs1}, find_orphan_logs(Logs0 ++ Logs1)),

    Logs2 = [{7, "7.log"}],
    ?assertEqual({Logs0 ++ Logs1, Logs2},
                 find_orphan_logs(Logs0 ++ Logs1 ++ Logs2)).
-endif.

log_path(DataDir, LogIndex) ->
    filename:join(logs_dir(DataDir), integer_to_list(LogIndex) ++ ".log").

writer_append(Opaque, Record, Data, Storage) ->
    writer_request(Opaque, {append, Record}, Data, Storage).

writer_sync(Opaque, Storage) ->
    writer_request(Opaque, sync, sync, Storage).

writer_request(Opaque, Msg, RequestData,
               #storage{writer = Writer,
                        requests = Requests} = Storage) ->
    Ref = make_ref(),
    NewRequests = queue:in({Ref, Opaque, RequestData}, Requests),

    Writer ! {Ref, Msg},
    Storage#storage{requests = NewRequests}.

truncate_configs(StartSeqno,
                 #storage{pending_configs = PendingConfigs} = Storage) ->
    NewPendingConfigs =
        lists:dropwhile(fun (#log_entry{seqno = Seqno}) ->
                                Seqno >= StartSeqno
                        end, PendingConfigs),

    Storage#storage{pending_configs = NewPendingConfigs}.

append_configs(Entries, Storage) ->
    ConfigEntries = lists:filter(fun is_config_entry/1, Entries),
    case ConfigEntries of
        [] ->
            Storage;
        _ ->
            PendingConfigs = Storage#storage.pending_configs,
            NewPendingConfigs = lists:reverse(ConfigEntries) ++ PendingConfigs,
            NewStorage = Storage#storage{pending_configs = NewPendingConfigs},
            compact_configs(NewStorage)
    end.

compact_configs(#storage{pending_configs = PendingConfigs} = Storage) ->
    case PendingConfigs of
        [] ->
            Storage;
        _ ->
            CommittedSeqno = get_committed_seqno(Storage#storage.pending_meta),
            case lists:splitwith(fun (#log_entry{seqno = Seqno}) ->
                                         Seqno > CommittedSeqno
                                 end, PendingConfigs) of
                {[_|_], []} ->
                    Storage;
                {NewPendingConfigs, [NewCommittedConfig|_]} ->
                    Storage#storage{
                      pending_configs = NewPendingConfigs,
                      pending_committed_config = NewCommittedConfig}
            end
    end.

mem_log_append(EndSeqno, Entries,
               #storage{log_tab = LogTab} = Storage) ->
    ets:insert(LogTab, Entries),
    Storage#storage{pending_high_seqno = EndSeqno}.

mem_log_delete_range(FromSeqno, ToSeqno, #storage{log_tab = LogTab}) ->
    StartTS = erlang:monotonic_time(),
    delete_table_range(LogTab, FromSeqno, ToSeqno),
    EndTS = erlang:monotonic_time(),

    ?DEBUG("Deleted log range from seqno ~p to ~p in ~pus",
           [FromSeqno, ToSeqno,
            erlang:convert_time_unit(EndTS - StartTS, native, microsecond)]).

delete_table_range(_Tab, FromSeqno, ToSeqno)
  when FromSeqno > ToSeqno ->
    ok;
delete_table_range(LogTab, FromSeqno, ToSeqno) ->
    ets:delete(LogTab, FromSeqno),
    delete_table_range(LogTab, FromSeqno + 1, ToSeqno).

file_exists(Path, Type) ->
    case chronicle_utils:check_file_exists(Path, Type) of
        ok ->
            true;
        {error, enoent} ->
            false;
        {error, Error} ->
            exit({file_exists_failed, Path, Type, Error})
    end.

sync_dir(Dir) ->
    Result = ?TIME(<<"sync_dir">>, chronicle_utils:sync_dir(Dir)),
    case Result of
        ok ->
            ok;
        {error, Error} ->
            exit({sync_dir_failed, Dir, Error})
    end.

get_log() ->
    {LogLowSeqno, LogHighSeqno, _} = get_published_seqno_range(),
    get_log(LogLowSeqno, LogHighSeqno).

get_log(StartSeqno, EndSeqno) ->
    {LogLowSeqno, LogHighSeqno, _} = get_published_seqno_range(),
    true = (StartSeqno >= LogLowSeqno),
    true = (EndSeqno =< LogHighSeqno),

    case get_log_loop(StartSeqno, EndSeqno, []) of
        {ok, Entries} ->
            Entries;
        {missing_entry, MissingSeqno} ->
            %% This function is only supposed to be called in a synchronized
            %% fashion, so there should be no possibility of intervening
            %% compaction.
            exit({missing_entry,
                  MissingSeqno, StartSeqno, EndSeqno,
                  LogLowSeqno, LogHighSeqno})
    end.

get_log_committed(StartSeqno, EndSeqno) ->
    true = (StartSeqno =< EndSeqno),
    {LogLowSeqno, LogHighSeqno, LogCommittedSeqno} =
        get_published_seqno_range(),
    case EndSeqno > LogCommittedSeqno of
        true ->
            exit({uncommitted, StartSeqno, EndSeqno, LogCommittedSeqno});
        false ->
            case in_range(StartSeqno, EndSeqno, LogLowSeqno, LogHighSeqno) of
                true ->
                    do_get_log_committed(StartSeqno, EndSeqno);
                false ->
                    {error, compacted}
            end
    end.

do_get_log_committed(StartSeqno, EndSeqno) ->
    case get_log_loop(StartSeqno, EndSeqno, []) of
        {ok, _} = Ok ->
            Ok;
        {missing_entry, MissingSeqno} ->
            {LogLowSeqno, LogHighSeqno, _} = get_published_seqno_range(),

            %% Compaction must have happened while we were reading the
            %% entries. So start and end seqnos must now be out of range. If
            %% they are still in range, there must be a bug somewhere.
            case in_range(StartSeqno, EndSeqno, LogLowSeqno, LogHighSeqno) of
                true ->
                    exit({missing_entry,
                          MissingSeqno, StartSeqno, EndSeqno,
                          LogLowSeqno, LogHighSeqno});
                false ->
                    {error, compacted}
            end
    end.

in_range(StartSeqno, EndSeqno, LowSeqno, HighSeqno) ->
    seqno_in_range(StartSeqno, LowSeqno, HighSeqno)
        andalso seqno_in_range(EndSeqno, LowSeqno, HighSeqno).

seqno_in_range(Seqno, LowSeqno, HighSeqno) ->
    Seqno >= LowSeqno andalso Seqno =< HighSeqno.

get_log_loop(StartSeqno, EndSeqno, Acc)
  when EndSeqno < StartSeqno ->
    {ok, Acc};
get_log_loop(StartSeqno, EndSeqno, Acc) ->
    case ets:lookup(?MEM_LOG_TAB, EndSeqno) of
        [Entry] ->
            get_log_loop(StartSeqno, EndSeqno - 1, [Entry | Acc]);
        [] ->
            {missing_entry, EndSeqno}
    end.

get_log_entry(Seqno, #storage{log_tab = Tab}) ->
    case ets:lookup(Tab, Seqno) of
        [Entry] ->
            {ok, Entry};
        [] ->
            {error, not_found}
    end.

get_published_seqno_range() ->
    case ets:lookup(?MEM_LOG_INFO_TAB, ?RANGE_KEY) of
        [] ->
            not_published;
        [{_, LowSeqno, HighSeqno, CommittedSeqno}] ->
            {LowSeqno, HighSeqno, CommittedSeqno}
    end.

get_published_snapshot() ->
    case ets:lookup(?MEM_LOG_INFO_TAB, ?SNAPSHOT_KEY) of
        [] ->
            no_snapshot;
        [{_, Seqno, Term}] ->
            {Seqno, Term}
    end.

record_snapshot(Opaque, Seqno, HistoryId, Term, Config, Storage) ->
    record_snapshot(Opaque, Seqno,
                    {snapshot, Seqno, HistoryId, Term, Config}, Storage).

record_snapshot(Opaque, Seqno, LogRecord, Storage) ->
    record_snapshot(Opaque, Seqno, LogRecord, LogRecord, Storage).

record_snapshot(Opaque, Seqno, LogRecord, Data, Storage) ->
    LatestSnapshotSeqno = get_current_snapshot_seqno(Storage),
    true = (Seqno > LatestSnapshotSeqno),

    NewStorage0 = rollover(Storage),
    writer_append(Opaque, LogRecord, Data, NewStorage0).

publish_snapshot(Storage) ->
    case get_current_snapshot(Storage) of
        no_snapshot ->
            ok;
        Snapshot ->
            publish_snapshot(Snapshot, Storage)
    end.

publish_snapshot({Seqno, _, Term, _} = Snapshot,
                 #storage{log_info_tab = LogInfoTab,
                          publish_snapshot_fun = PublishSnapshotFun}) ->
    ets:insert(LogInfoTab, {?SNAPSHOT_KEY, Seqno, Term}),
    PublishSnapshotFun(Snapshot),
    ok.

install_snapshot(Seqno, HistoryId, Term, Config, Meta, Storage) ->
    %% TODO: move managing of this metadata to chronicle_storage
    Seqno = get_committed_seqno(Meta),
    NewStorage0 = record_snapshot(none, Seqno,
                                  {install_snapshot,
                                   Seqno, HistoryId, Term, Config, Meta},
                                  none,
                                  Storage),
    {DoneRequests, NewStorage1} = sync(NewStorage0),
    none = lists:last(DoneRequests),

    NewStorage2 = do_install_snapshot(Seqno, HistoryId,
                                      Term, Config, Meta, NewStorage1),
    publish_snapshot(NewStorage2),
    {lists:droplast(DoneRequests), publish(compact(NewStorage2))}.

do_install_snapshot(Seqno, HistoryId, Term, Config, Meta, Storage) ->
    #storage{meta = CurrentMeta} = Storage,
    NewMeta = maps:merge(CurrentMeta, Meta),
    NewStorage = Storage#storage{low_seqno = Seqno + 1,
                                 high_seqno = Seqno,
                                 pending_high_seqno = Seqno,
                                 config = Config,
                                 committed_config = Config,
                                 pending_committed_config = Config,
                                 pending_configs = [],
                                 meta = NewMeta,
                                 pending_meta = NewMeta},
    add_snapshot(Seqno, HistoryId, Term, Config, NewStorage).

rsm_snapshot_path(SnapshotDir, RSM) ->
    filename:join(SnapshotDir, [RSM, ".snapshot"]).

prepare_snapshot(Seqno) ->
    SnapshotDir = snapshot_dir(Seqno),
    ?TIME(<<"prepare_snapshot">>, chronicle_utils:mkdir_p(SnapshotDir)).

copy_snapshot(Path, Seqno, Config) ->
    SnapshotDir = snapshot_dir(Seqno),
    RSMs = maps:keys(chronicle_config:get_rsms(Config#log_entry.value)),
    copy_snapshot_loop(Path, SnapshotDir, RSMs).

copy_snapshot_loop(_Path, _SnapshotDir, []) ->
    ok;
copy_snapshot_loop(Path, SnapshotDir, [RSM | Rest]) ->
    SrcPath = rsm_snapshot_path(SnapshotDir, RSM),
    DstPath = rsm_snapshot_path(Path, RSM),

    case link_or_copy(SrcPath, DstPath) of
        ok ->
            copy_snapshot_loop(Path, SnapshotDir, Rest);
        {error, _} = Error ->
            Error
    end.

link_or_copy(SrcPath, DstPath) ->
    case file:make_link(SrcPath, DstPath) of
        ok ->
            ok;
        {error, Error}
          when Error =:= exdev;
               Error =:= enotsup ->
            case file:copy(SrcPath, DstPath) of
                {ok, _} ->
                    ok;
                {error, Error} ->
                    {error, {copy_failed, Error, SrcPath, DstPath}}
            end;
        {error, Error} ->
            {error, {link_failed, Error, SrcPath, DstPath}}
    end.

save_rsm_snapshot(Seqno, RSM, RSMState) ->
    SnapshotDir = snapshot_dir(Seqno),

    Path = rsm_snapshot_path(SnapshotDir, RSM),
    Data = term_to_binary(RSMState, [{compressed, 9}]),
    Crc = erlang:crc32(Data),

    %% We don't really care about atomicity that much here. But it also
    %% doesn't hurt.
    Result = chronicle_utils:atomic_write_file(
               Path,
               fun (File) ->
                       ok = file:write(File, <<Crc:?CRC_BITS>>),
                       ok = file:write(File, Data)
               end),

    case Result of
        ok ->
            ok;
        {error, Error} ->
            exit({snapshot_failed, Path, Error})
    end.

validate_rsm_snapshot(SnapshotDir, RSM) ->
    Path = rsm_snapshot_path(SnapshotDir, RSM),
    case read_rsm_snapshot_data(Path) of
        {ok, _Data} ->
            ok;
        {error, {invalid_snapshot, _Reason}} = Error ->
            Error;
        {error, enoent} = Error ->
            %% While we shouldn't see this error under any normal
            %% circumstances, it's benign enough to allow. It also allows
            %% deleting snapshot files by hand without causing everything to
            %% crash.
            Error;
        {error, Reason} ->
            exit({snapshot_read_failed, Path, Reason})
    end.

read_rsm_snapshot_data(Path) ->
    case file:read_file(Path) of
        {ok, Binary} ->
            case Binary of
                <<Crc:?CRC_BITS, Data/binary>> ->
                    DataCrc = erlang:crc32(Data),
                    case Crc =:= DataCrc of
                        true ->
                            {ok, Data};
                        false ->
                            {error, {invalid_snapshot, crc_mismatch}}
                    end;
                _ ->
                    {error, {invalid_snapshot, unexpected_eof}}
            end;
        {error, _} = Error ->
            Error
    end.

read_rsm_snapshot(RSM, Seqno) ->
    SnapshotDir = snapshot_dir(Seqno),
    Path = rsm_snapshot_path(SnapshotDir, RSM),
    read_rsm_snapshot(Path).

read_rsm_snapshot(Path) ->
    case read_rsm_snapshot_data(Path) of
        {ok, Data} ->
            {ok, binary_to_term(Data)};
        {error, _} = Error ->
            Error
    end.

validate_snapshot(_DataDir, no_snapshot) ->
    ok;
validate_snapshot(DataDir, {Seqno, _, _, Config}) ->
    SnapshotDir = snapshot_dir(DataDir, Seqno),
    case chronicle_utils:check_file_exists(SnapshotDir, directory) of
        ok ->
            validate_existing_snapshot(SnapshotDir, Config);
        {error, Error} ->
            exit({validate_snapshot_failed, SnapshotDir, Error})
    end.

validate_existing_snapshot(SnapshotDir, Config) ->
    RSMs = chronicle_config:get_rsms(Config#log_entry.value),
    Errors =
        lists:filtermap(
          fun (RSM) ->
                  case validate_rsm_snapshot(SnapshotDir, RSM) of
                      ok ->
                          false;
                      {error, Error} ->
                          {true, {RSM, Error}}
                  end
          end, maps:keys(RSMs)),

    case Errors =:= [] of
        true ->
            ok;
        false ->
            exit({validate_snapshot_failed, SnapshotDir, Errors})
    end.

validate_state(#storage{low_seqno = LowSeqno,
                        data_dir = DataDir} = Storage) ->
    validate_snapshot(DataDir, get_current_snapshot(Storage)),

    SnapshotSeqno = get_current_snapshot_seqno(Storage),
    case SnapshotSeqno + 1 >= LowSeqno of
        true ->
            ok;
        false ->
            ?ERROR("Last snapshot at seqno ~p is below our low seqno ~p",
                   [SnapshotSeqno, LowSeqno]),
            exit({missing_snapshot, SnapshotSeqno, LowSeqno})
    end.

release_snapshot(Seqno, #storage{extra_snapshots = ExtraSnapshots} = Storage) ->
    case lists:keytake(Seqno, 1, ExtraSnapshots) of
        false ->
            Storage;
        {value, _, NewExtraSnapshot} ->
            delete_snapshot(Seqno, Storage),
            Storage#storage{extra_snapshots = NewExtraSnapshot}
    end.

delete_snapshot(SnapshotSeqno, #storage{data_dir = DataDir}) ->
    SnapshotDir = snapshot_dir(DataDir, SnapshotSeqno),
    ?INFO("Deleting snapshot at seqno ~p: ~s", [SnapshotSeqno, SnapshotDir]),
    Result = ?TIME(<<"delete_snapshot">>,
                   chronicle_utils:delete_recursive(SnapshotDir)),
    case Result of
        ok ->
            ok;
        {error, Error} ->
            exit({delete_snapshot_failed, Error})
    end.

get_current_snapshot(#storage{current_snapshot = MaybeSnapshot}) ->
    MaybeSnapshot.

get_current_snapshot_seqno(Storage) ->
    case get_current_snapshot(Storage) of
        no_snapshot ->
            ?NO_SEQNO;
        {Seqno, _, _Term, _Config} ->
            Seqno
    end.

compact(#storage{log_segments = LogSegments} = Storage) ->
    case LogSegments of
        [] ->
            Storage;
        _ ->
            do_compact(Storage)
    end.

do_compact(#storage{low_seqno = LowSeqno,
                    log_segments = LogSegments,

                    writer = Writer} = Storage) ->
    SnapshotSeqno = get_current_snapshot_seqno(Storage),

    {Keep, Delete} = classify_logs(SnapshotSeqno, LogSegments),
    case Delete of
        [] ->
            ok;
        _ ->
            ?DEBUG("Going to delete the following log "
                   "files (preserved snapshot seqno: ~p):~n"
                   "~p",
                   [SnapshotSeqno, Delete]),

            LogPaths = [LogPath || {LogPath, _} <- Delete],
            Writer ! {delete_logs, LogPaths},
            ok
    end,

    {_LogPath, LogStartSeqno} = lists:last(Keep),

    %% Our current low seqno may actually be greater then the start seqno of
    %% the log file. That's because log files are preserved even when a
    %% snapshot is transferred and installed from another node. In such case,
    %% the low seqno will be the seqno of the snapshot. But we do still keep
    %% the log (with lower start seqnos) around.
    NewLowSeqno = max(LowSeqno, LogStartSeqno),

    NewStorage = Storage#storage{log_segments = Keep,
                                 low_seqno = NewLowSeqno},

    NewStorage.

classify_logs(SnapshotSeqno, Logs) ->
    classify_logs_loop(SnapshotSeqno, Logs, []).

classify_logs_loop(_, [], Acc) ->
    {lists:reverse(Acc), []};
classify_logs_loop(SnapshotSeqno, [{_, LogStartSeqno} = Log | RestLogs], Acc) ->
    case LogStartSeqno > SnapshotSeqno of
        true ->
            classify_logs_loop(SnapshotSeqno, RestLogs, [Log | Acc]);
        false ->
            {lists:reverse([Log | Acc]), RestLogs}
    end.

-ifdef(TEST).
classify_logs_test() ->
    Logs = [{log1, 200}, {log2, 150}, {log3, 200}, {log4, 140}, {log5, 100}],
    [Log1, Log2, Log3, Log4, Log5] = Logs,

    ?assertEqual({[Log1], [Log2, Log3, Log4, Log5]}, classify_logs(250, Logs)),
    ?assertEqual({[Log1], [Log2, Log3, Log4, Log5]}, classify_logs(200, Logs)),
    ?assertEqual({Logs, []}, classify_logs(50, Logs)),
    ?assertEqual({[Log1, Log2], [Log3, Log4, Log5]}, classify_logs(175, Logs)),
    ?assertEqual({[Log1, Log2, Log3, Log4], [Log5]}, classify_logs(145, Logs)),
    ?assertEqual({Logs, []}, classify_logs(125, Logs)).
-endif.

ensure_rsm_dir(Name) ->
    RSMsDir = rsms_dir(chronicle_env:data_dir()),
    Dir = filename:join(RSMsDir, Name),
    ok = chronicle_utils:mkdir_p(Dir),
    Dir.

map_append(Fun, Entry) ->
    case Entry of
        {append, StartSeqno, EndSeqno, Meta, Truncate, Terms} ->
            {append,
             StartSeqno, EndSeqno, Meta, Truncate,
             lists:map(Fun, Terms)};
        _ ->
            Entry
    end.

check_version(DataDir) ->
    Path = filename:join(chronicle_dir(DataDir), "version"),
    Version = chronicle_utils:read_int_from_file(Path, -1),

    if
        Version =:= ?VERSION ->
            ok;
        Version < ?VERSION ->
            ok = chronicle_utils:store_int_to_file(Path, ?VERSION);
        true ->
            ?ERROR("Found unsupported storage version ~b (our version is ~b)",
                   [Version, ?VERSION]),
            exit({unsupported_storage_version, Version, ?VERSION})
    end.

-record(writer, { parent,
                  log,
                  next_msg
                }).

spawn_writer(#storage{current_log_path = LogPath} = Storage) ->
    Writer = #writer{parent = self()},
    Pid = proc_lib:spawn_link(
            fun () ->
                    register(?WRITER, self()),
                    process_flag(trap_exit, true),

                    {ok, Log} = chronicle_log:open(LogPath),
                    writer_loop(Writer#writer{log = Log})
            end),

    Storage#storage{writer = Pid}.

writer_recv_msg(#writer{next_msg = NextMsg} = Writer) ->
    case NextMsg of
        undefined ->
            Msg = receive M -> M end,
            {Writer, Msg};
        _ ->
            {Writer#writer{next_msg = undefined}, NextMsg}
    end.

writer_unrecv_msg(Msg, #writer{next_msg = undefined} = Writer) ->
    Writer#writer{next_msg = Msg}.

writer_loop(Writer) ->
    {NewWriter0, Msg} = writer_recv_msg(Writer),

    NewWriter =
        case Msg of
            {Req, {append, Record}} ->
                writer_handle_append([Req], [Record], 1, NewWriter0);
            {Req, sync} ->
                writer_handle_append([Req], [], 0, NewWriter0);
            {rollover, Config, Meta, HighSeqno, Path} ->
                writer_handle_rollover(Config, Meta,
                                       HighSeqno, Path, NewWriter0);
            {delete_logs, Paths} ->
                writer_handle_delete_logs(Paths, NewWriter0);
            {'EXIT', Pid, Reason} ->
                ?DEBUG("Got exit from ~w with reason ~w",
                       [Pid, chronicle_utils:sanitize_reason(Reason)]),
                exit(normal);
            _ ->
                exit({unexpected_message, Msg})
        end,

    writer_loop(NewWriter).

writer_handle_append(Reqs, Batch, N, Writer) ->
    receive
        {Req, {append, Record}} ->
            writer_handle_append([Req | Reqs], [Record | Batch], N + 1, Writer);
        {Req, sync} ->
            writer_handle_append([Req | Reqs], Batch, N, Writer);
        Msg ->
            NewWriter = writer_unrecv_msg(Msg, Writer),
            writer_append_batch(Reqs, Batch, N, NewWriter)
    after
        0 ->
            writer_append_batch(Reqs, Batch, N, Writer)
    end.

writer_append_batch(Requests0, Batch0, Size,
                    #writer{log = Log} = Writer) ->
    Requests = lists:reverse(Requests0),

    BytesWritten =
        case Batch0 of
            [] ->
                %% sync-s only
                0;
            _ ->
                Batch = lists:reverse(Batch0),
                Written = writer_log_append(Log, Batch),
                log_sync(Log),
                Written
        end,

    report_batch_stats(Size),
    notify_parent({append_done, BytesWritten, Requests}, Writer),
    Writer.

report_batch_stats(Size) ->
    chronicle_stats:report_counter(<<"chronicle_append_num">>, Size),
    chronicle_stats:report_max(<<"chronicle_append_batch_size_1m_max">>,
                               60000, 10000, Size).

writer_log_append(Log, Batch) ->
    Result = ?TIME(<<"append">>, {ok, _}, chronicle_log:append(Log, Batch)),
    case Result of
        {ok, BytesWritten} ->
            BytesWritten;
        {error, Error} ->
            exit({append_failed, Error})
    end.

log_sync(Log) ->
    Result = ?TIME(<<"sync">>, chronicle_log:sync(Log)),
    case Result of
        ok ->
            ok;
        {error, Error} ->
            exit({sync_failed, Error})
    end.

writer_handle_rollover(Config, Meta, HighSeqno, Path,
                       #writer{log = Log} = Writer) ->
    ok = chronicle_log:close(Log),
    NewLog = create_log(Config, Meta, HighSeqno, Path),

    notify_parent({rollover_done, HighSeqno}, Writer),
    Writer#writer{log = NewLog}.

writer_handle_delete_logs(Paths, Writer) ->
    delete_logs(Paths),
    Writer.

notify_parent(Msg, #writer{parent = Parent}) ->
    Parent ! {?MODULE, Msg},
    ok.

delete_logs(Paths) ->
    lists:foreach(
      fun (Path) ->
              Result = ?TIME(<<"delete">>, file:delete(Path)),
              case Result of
                  ok ->
                      ?INFO("Deleted ~s", [Path]),
                      true;
                  {error, Error} ->
                      ?ERROR("Failed to delete ~s: ~p",
                             [Path, Error]),
                      error({delete_log_failed, Path, Error})
              end
      end, Paths).
