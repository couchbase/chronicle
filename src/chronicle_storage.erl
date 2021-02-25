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

-compile(export_all).

-include("chronicle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MEM_LOG_INFO_TAB, ?ETS_TABLE(chronicle_mem_log_info)).
-define(MEM_LOG_TAB, ?ETS_TABLE(chronicle_mem_log)).

-define(RANGE_KEY, '$range').

-define(LOG_MAX_SIZE,
        chronicle_settings:get({storage, log_max_size}, 1024 * 1024)).
-define(MAX_LOG_SEGMENTS,
        chronicle_settings:get({storage, max_log_segments}, 20)).
-define(MAX_SNAPSHOTS,
        chronicle_settings:get({storage, max_snapshots}, 1)).

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
                   ?META_PENDING_BRANCH => undefined | #branch{} }.

-record(storage, { current_log,
                   current_log_ix,
                   current_log_path,
                   current_log_start_seqno,
                   current_log_data_size,
                   low_seqno,
                   high_seqno,
                   meta,

                   config,
                   committed_config,
                   pending_configs,

                   snapshots,
                   snapshots_in_use,

                   data_dir,

                   log_info_tab,
                   log_tab,

                   log_segments
                  }).

open() ->
    _ = ets:new(?MEM_LOG_INFO_TAB,
                [protected, set, named_table, {read_concurrency, true}]),
    _ = ets:new(?MEM_LOG_TAB,
                [protected, set, named_table,
                 {keypos, #log_entry.seqno}, {read_concurrency, true}]),
    Storage0 = #storage{log_info_tab = ets:whereis(?MEM_LOG_INFO_TAB),
                        log_tab = ets:whereis(?MEM_LOG_TAB),
                        low_seqno = ?NO_SEQNO + 1,
                        high_seqno = ?NO_SEQNO,
                        meta = #{},
                        snapshots_in_use = gb_sets:new()},

    try
        DataDir = chronicle_env:data_dir(),
        maybe_complete_wipe(DataDir),
        ensure_dirs(DataDir),

        Storage1 = Storage0#storage{data_dir = DataDir},
        Storage2 = compact(validate_state(open_logs(Storage1))),

        cleanup_orphan_snapshots(Storage2),

        Storage2
    catch
        T:E:Stack ->
            close(Storage0),
            erlang:raise(T, E, Stack)
    end.

open_logs(#storage{data_dir = DataDir} = Storage) ->
    {Orphans, Sealed, Current} = find_logs(DataDir),

    NewStorage0 = Storage#storage{meta = #{},
                                  config = undefined,
                                  committed_config = undefined,
                                  pending_configs = [],
                                  low_seqno = ?NO_SEQNO + 1,
                                  high_seqno = ?NO_SEQNO,
                                  snapshots = [],
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
    FinalStorage = open_current_log(CurrentLogIx, CurrentLogPath, NewStorage1),

    maybe_delete_orphans(Orphans),

    FinalStorage.

open_current_log(LogIx, LogPath, Storage0) ->
    Storage = Storage0#storage{current_log_ix = LogIx,
                               current_log_path = LogPath},
    NewStorage =
        case chronicle_log:open(LogPath,
                                fun handle_user_data/2,
                                fun handle_log_entry/2,
                                Storage) of
            {ok, Log, NewStorage0} ->
                NewStorage0#storage{current_log = Log};
            {error, Error} when Error =:= enoent;
                                Error =:= no_header ->
                ?INFO("Error while opening log file ~p: ~p", [LogPath, Error]),
                #storage{meta = Meta,
                         config = Config,
                         high_seqno = HighSeqno} = Storage,
                Log = create_log(Config, Meta, HighSeqno, LogPath),
                Storage#storage{current_log = Log,
                                current_log_start_seqno = HighSeqno + 1};
            {error, Error} ->
                ?ERROR("Failed to open log ~p: ~p", [LogPath, Error]),
                exit({failed_to_open_log, LogPath, Error})
        end,

    {ok, DataSize} = chronicle_log:data_size(NewStorage#storage.current_log),
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

maybe_rollover(#storage{current_log_data_size = LogDataSize} = Storage) ->
    case LogDataSize > ?LOG_MAX_SIZE of
        true ->
            compact(rollover(Storage));
        false ->
            Storage
    end.

rollover(#storage{current_log = CurrentLog,
                  current_log_ix = CurrentLogIx,
                  current_log_path = CurrentLogPath,
                  current_log_start_seqno = CurrentLogStartSeqno,
                  data_dir = DataDir,
                  config = Config,
                  meta = Meta,
                  high_seqno = HighSeqno,
                  log_segments = LogSegments} = Storage) ->
    sync(Storage),
    ok = chronicle_log:close(CurrentLog),

    LogSegment = {CurrentLogPath, CurrentLogStartSeqno},
    NewLogSegments = [LogSegment | LogSegments],

    NewLogIx = CurrentLogIx + 1,
    NewLogPath = log_path(DataDir, NewLogIx),
    NewLog = create_log(Config, Meta, HighSeqno, NewLogPath),
    Storage#storage{current_log = NewLog,
                    current_log_ix = NewLogIx,
                    current_log_path = NewLogPath,
                    current_log_data_size = 0,
                    current_log_start_seqno = HighSeqno + 1,
                    log_segments = NewLogSegments}.

create_log(Config, Meta, HighSeqno, LogPath) ->
    ?INFO("Creating log file ~p.~n"
          "Config:~n~p~n"
          "Metadata:~n~p",
          [LogPath, Config, Meta]),

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
    ok = chronicle_utils:mkdir_p(snapshots_dir(DataDir)).

chronicle_dir(DataDir) ->
    filename:join(DataDir, "chronicle").

logs_dir(DataDir) ->
    filename:join(chronicle_dir(DataDir), "logs").

snapshots_dir(DataDir) ->
    filename:join(chronicle_dir(DataDir), "snapshots").

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
            NewStorage1 = NewStorage#storage{meta = Meta,
                                             high_seqno = HighSeqno,
                                             low_seqno = HighSeqno + 1},
            case Config of
                undefined ->
                    NewStorage1;
                #log_entry{} ->
                    append_configs([Config], NewStorage1)
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
            compact_configs(add_meta(Meta, Storage));
        {snapshot, Seqno, HistoryId, Term, Config} ->
            add_snapshot(Seqno, HistoryId, Term, Config, Storage);
        {install_snapshot, Seqno, HistoryId, Term, Config, Meta} ->
            ets:delete_all_objects(Storage#storage.log_tab),

            #storage{meta = CurrentMeta} = Storage,
            NewStorage = Storage#storage{low_seqno = Seqno + 1,
                                         high_seqno = Seqno,
                                         config = Config,
                                         committed_config = Config,
                                         pending_configs = [],
                                         meta = maps:merge(CurrentMeta, Meta)},
            add_snapshot(Seqno, HistoryId, Term, Config, NewStorage)
    end.

handle_log_append(StartSeqno, EndSeqno, Meta, Truncate, Entries,
                  #storage{low_seqno = LowSeqno,
                           high_seqno = HighSeqno} = Storage) ->
    NewStorage =
        case Truncate of
            true ->
                %% Need to do this explicitly, since currently do_append()
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

    do_append(StartSeqno, EndSeqno, Meta, Truncate, Entries, NewStorage).

add_meta(Meta, #storage{meta = CurrentMeta} = Storage) ->
    Storage#storage{meta = maps:merge(CurrentMeta, Meta)}.

add_snapshot(Seqno, HistoryId, Term, Config,
             #storage{snapshots = CurrentSnapshots} = Storage) ->
    Storage#storage{snapshots =
                        [{Seqno, HistoryId, Term, Config} | CurrentSnapshots]}.

is_config_entry(#log_entry{value = Value}) ->
    chronicle_config:is_config(Value).

-spec get_meta(#storage{}) -> meta().
get_meta(Storage) ->
    Storage#storage.meta.

get_committed_seqno(#{?META_COMMITTED_SEQNO := CommittedSeqno}) ->
    CommittedSeqno.

get_high_seqno(Storage) ->
    Storage#storage.high_seqno.

get_high_term(#storage{high_seqno = HighSeqno} = Storage) ->
    {ok, Term} = get_term_for_seqno(HighSeqno, Storage),
    Term.

get_term_for_seqno(Seqno, #storage{high_seqno = HighSeqno,
                                   low_seqno = LowSeqno,
                                   snapshots = Snapshots} = Storage) ->
    case Seqno =< HighSeqno of
        true ->
            if
                Seqno >= LowSeqno ->
                    {ok, Entry} = get_log_entry(Seqno, Storage),
                    {ok, Entry#log_entry.term};
                Seqno =:= ?NO_SEQNO ->
                    {ok, ?NO_TERM};
                true ->
                    case lists:keyfind(Seqno, 1, Snapshots) of
                        {_, _, Term, _} ->
                            {ok, Term};
                        false ->
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

-spec store_meta(meta(), #storage{}) -> #storage{}.
store_meta(Updates, Storage) ->
    Meta = dedup_meta(Updates, Storage),
    case maps:size(Meta) > 0 of
        true ->
            NewStorage0 = add_meta(Meta, Storage),
            NewStorage = compact_configs(NewStorage0),
            maybe_rollover(log_append([{meta, Meta}], NewStorage));
        false ->
            Storage
    end.

dedup_meta(Updates, #storage{meta = Meta}) ->
    maps:filter(
      fun (Key, Value) ->
              case maps:find(Key, Meta) of
                  {ok, CurrentValue} ->
                      Value =/= CurrentValue;
                  error ->
                      true
              end
      end, Updates).

append(StartSeqno, EndSeqno, Entries, Opts, Storage) ->
    Truncate = append_handle_truncate(StartSeqno, Opts, Storage),
    Meta = append_handle_meta(Opts, Storage),
    LogEntry = {append, StartSeqno, EndSeqno, Meta, Truncate, Entries},
    NewStorage0 = log_append([LogEntry], Storage),
    NewStorage1 = do_append(StartSeqno, EndSeqno, Meta,
                            Truncate, Entries, NewStorage0),
    maybe_rollover(NewStorage1).

do_append(StartSeqno, EndSeqno, Meta, Truncate, Entries, Storage) ->
    NewStorage0 =
        case Truncate of
            true ->
                truncate_configs(StartSeqno, Storage);
            false ->
                Storage
        end,

    NewStorage1 = mem_log_append(EndSeqno, Entries, NewStorage0),
    NewStorage2 = add_meta(Meta, NewStorage1),
    append_configs(Entries, NewStorage2).

append_handle_meta(Opts, Storage) ->
    case maps:find(meta, Opts) of
        {ok, Meta} ->
            dedup_meta(Meta, Storage);
        error ->
            #{}
    end.

append_handle_truncate(StartSeqno, Opts,
                       #storage{meta = Meta,
                                low_seqno = LowSeqno,
                                high_seqno = HighSeqno}) ->
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

sync(#storage{current_log = Log}) ->
    case chronicle_log:sync(Log) of
        ok ->
            ok;
        {error, Error} ->
            exit({sync_failed, Error})
    end.

close(#storage{current_log = Log,
               log_tab = LogTab,
               log_info_tab = LogInfoTab}) ->
    ets:delete(LogTab),
    ets:delete(LogInfoTab),

    case Log of
        undefined ->
            ok;
        _ ->
            ok = chronicle_log:close(Log)
    end.

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

cleanup_orphan_snapshots(#storage{data_dir = DataDir,
                                  snapshots = Snapshots}) ->
    SnapshotSeqnos = [Seqno || {Seqno, _, _, _} <- Snapshots],
    DiskSnapshots = find_disk_snapshots(DataDir),
    OrphanSnapshots =
        lists:filtermap(
          fun ({SnapshotSeqno, Path}) ->
                  case lists:member(SnapshotSeqno, SnapshotSeqnos) of
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
                     "Known snapshots seqnos: ~p~n"
                     "Orphan snapshot files:~n~p",
                     [SnapshotSeqnos, OrphanSnapshots]),
            try_delete_files(OrphanSnapshots)
    end.

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

log_append(Records, #storage{current_log = Log,
                             current_log_data_size = LogDataSize} = Storage) ->
    case chronicle_log:append(Log, Records) of
        {ok, BytesWritten} ->
            NewLogDataSize = LogDataSize + BytesWritten,
            Storage#storage{current_log_data_size = NewLogDataSize};
        {error, Error} ->
            exit({append_failed, Error})
    end.

truncate_configs(StartSeqno,
                 #storage{pending_configs = PendingConfigs,
                          committed_config = CommittedConfig} = Storage) ->
    case lists:dropwhile(fun (#log_entry{seqno = Seqno}) ->
                                 Seqno >= StartSeqno
                         end, PendingConfigs) of
        [] ->
            Storage#storage{pending_configs = [],
                            config = CommittedConfig};
        [NewConfig|_] = NewPendingConfigs ->
            Storage#storage{pending_configs = NewPendingConfigs,
                            config = NewConfig}
    end.

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
            CommittedSeqno = get_committed_seqno(Storage#storage.meta),
            case lists:splitwith(fun (#log_entry{seqno = Seqno}) ->
                                         Seqno > CommittedSeqno
                                 end, PendingConfigs) of
                {[_|_], []} ->
                    Storage;
                {[], [NewCommittedConfig|_]}->
                    Storage#storage{pending_configs = [],
                                    config = NewCommittedConfig,
                                    committed_config = NewCommittedConfig};
                {[NewConfig|_] = NewPendingConfigs, [NewCommittedConfig|_]} ->
                    Storage#storage{pending_configs = NewPendingConfigs,
                                    config = NewConfig,
                                    committed_config = NewCommittedConfig}
            end
    end.

delete_ordered_table_range(Table, FromKey, ToKey) ->
    StartKey =
        case ets:member(Table, FromKey) of
            true ->
                FromKey;
            false ->
                ets:next(Table, FromKey)
        end,

    delete_ordered_table_range_loop(Table, StartKey, ToKey).

delete_ordered_table_range_loop(Table, Key, ToKey) ->
    case Key of
        '$end_of_table' ->
            ok;
        _ when Key =< ToKey ->
            NextKey = ets:next(Table, Key),
            ets:delete(Table, Key),
            delete_ordered_table_range_loop(Table, NextKey, ToKey);
        _ ->
            true = (Key > ToKey),
            ok
    end.

mem_log_append(EndSeqno, Entries,
               #storage{log_tab = LogTab} = Storage) ->
    ets:insert(LogTab, Entries),
    Storage#storage{high_seqno = EndSeqno}.

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
    case chronicle_utils:sync_dir(Dir) of
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

record_snapshot(Seqno, HistoryId, Term, Config, Storage) ->
    record_snapshot(Seqno, HistoryId, Term, Config,
                    {snapshot, Seqno, HistoryId, Term, Config}, Storage).

record_snapshot(Seqno, HistoryId, Term, Config, LogRecord,
                #storage{data_dir = DataDir,
                         snapshots = Snapshots} = Storage) ->
    LatestSnapshotSeqno = get_latest_snapshot_seqno(Storage),
    true = (Seqno > LatestSnapshotSeqno),

    SnapshotsDir = snapshots_dir(DataDir),
    sync_dir(SnapshotsDir),

    NewStorage0 = log_append([LogRecord], Storage),

    NewSnapshots = [{Seqno, HistoryId, Term, Config} | Snapshots],
    NewStorage1 = NewStorage0#storage{snapshots = NewSnapshots},
    compact(NewStorage1).

install_snapshot(Seqno, HistoryId, Term, Config, Meta,
                 #storage{meta = OldMeta} = Storage) ->
    NewStorage = record_snapshot(Seqno, HistoryId, Term, Config,
                                 {install_snapshot,
                                  Seqno, HistoryId, Term, Config, Meta},
                                 Storage),

    %% TODO: move managing of this metadata to chronicle_storage
    Seqno = get_committed_seqno(Meta),
    NewStorage#storage{meta = maps:merge(OldMeta, Meta),
                       low_seqno = Seqno + 1,
                       high_seqno = Seqno,
                       config = Config,
                       committed_config = Config,
                       pending_configs = []}.

rsm_snapshot_path(SnapshotDir, RSM) ->
    filename:join(SnapshotDir, [RSM, ".snapshot"]).

save_rsm_snapshot(Seqno, RSM, RSMState) ->
    SnapshotDir = snapshot_dir(chronicle_env:data_dir(), Seqno),
    ok = chronicle_utils:mkdir_p(SnapshotDir),

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
    case read_rsm_snapshot_data(SnapshotDir, RSM) of
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

read_rsm_snapshot_data(SnapshotDir, RSM) ->
    Path = rsm_snapshot_path(SnapshotDir, RSM),
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
    SnapshotDir = snapshot_dir(chronicle_env:data_dir(), Seqno),
    case read_rsm_snapshot_data(SnapshotDir, RSM) of
        {ok, Data} ->
            {ok, binary_to_term(Data)};
        {error, _} = Error ->
            Error
    end.

validate_snapshot(DataDir, Seqno, Config) ->
    SnapshotDir = snapshot_dir(DataDir, Seqno),
    case chronicle_utils:check_file_exists(SnapshotDir, directory) of
        ok ->
            validate_existing_snapshot(SnapshotDir, Config);
        {error, enoent} ->
            missing;
        {error, Error} ->
            error({validate_snapshot_failed, SnapshotDir, Error})
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
            {error, Errors}
    end.

validate_state(#storage{low_seqno = LowSeqno,
                        snapshots = Snapshots,
                        data_dir = DataDir} = Storage) ->
    {ValidSnapshots0, InvalidSnapshots} =
        lists:foldl(
          fun ({Seqno, _, _, Config} = Snapshot,
               {AccValid, AccInvalid} = Acc) ->
                  case validate_snapshot(DataDir, Seqno, Config) of
                      ok ->
                          {[Snapshot | AccValid], AccInvalid};
                      missing ->
                          Acc;
                      {error, _} = Error ->
                          {AccValid, [{Snapshot, Error} | AccInvalid]}
                  end
          end, {[], []}, Snapshots),

    case InvalidSnapshots =:= [] of
        true ->
            ok;
        false ->
            ?WARNING("Found some snapshots to be invalid.~n~p",
                     [InvalidSnapshots])
    end,

    ValidSnapshots = lists:reverse(ValidSnapshots0),
    NewStorage = Storage#storage{snapshots = ValidSnapshots},
    SnapshotSeqno = get_latest_snapshot_seqno(NewStorage),

    case SnapshotSeqno + 1 >= LowSeqno of
        true ->
            ok;
        false ->
            ?ERROR("Last snapshot at seqno ~p is below our low seqno ~p",
                   [SnapshotSeqno, LowSeqno]),
            exit({missing_snapshot, SnapshotSeqno, LowSeqno})
    end,

    NewStorage.

get_and_hold_latest_snapshot(Storage) ->
    case get_latest_snapshot(Storage) of
        no_snapshot ->
            no_snapshot;
        {Seqno, _, _, _} = Snapshot ->
            {Snapshot, hold_snapshot(Seqno, Storage)}
    end.

hold_snapshot(Seqno, #storage{snapshots_in_use = Snapshots} = Storage) ->
    NewSnapshots = gb_sets:add_element(Seqno, Snapshots),
    Storage#storage{snapshots_in_use = NewSnapshots}.

release_snapshot(Seqno, #storage{snapshots_in_use = Snapshots} = Storage) ->
    NewSnapshots = gb_sets:del_element(Seqno, Snapshots),
    compact(Storage#storage{snapshots_in_use = NewSnapshots}).

get_used_snapshot_seqno(#storage{snapshots_in_use = Snapshots}) ->
    case gb_sets:is_empty(Snapshots) of
        true ->
            no_seqno;
        false ->
            gb_sets:smallest(Snapshots)
    end.

get_latest_snapshot(#storage{snapshots = Snapshots}) ->
    case Snapshots of
        [] ->
            no_snapshot;
        [{_, _, _, _} = Snapshot| _] ->
            Snapshot
    end.

get_latest_snapshot_seqno(Storage) ->
    case get_latest_snapshot(Storage) of
        no_snapshot ->
            ?NO_SEQNO;
        {Seqno, _, _Term, _Config} ->
            Seqno
    end.

compact(Storage) ->
    compact_log(compact_snapshots(Storage)).

compact_snapshots(#storage{snapshots = Snapshots} = Storage) ->
    NumSnapshots = length(Snapshots),
    case NumSnapshots > ?MAX_SNAPSHOTS of
        true ->
            %% Sync the log file to make sure we don't forget about any of the
            %% existing snapshots.
            sync(Storage),

            {KeepSnapshots0, DeleteSnapshots0} =
                lists:split(?MAX_SNAPSHOTS, Snapshots),

            {KeepSnapshots, DeleteSnapshots} =
                case get_used_snapshot_seqno(Storage) of
                    no_seqno ->
                        {KeepSnapshots0, DeleteSnapshots0};
                    UsedSeqno ->
                        {Used, Unused} = lists:splitwith(
                                           fun ({Seqno, _, _, _}) ->
                                                   Seqno >= UsedSeqno
                                           end, DeleteSnapshots0),

                        case Used of
                            [] ->
                                ok;
                            [_] ->
                                %% The agent will only release the previous
                                %% snapshot once the new one is recorded. So
                                %% it's normal that one extra snapshot may
                                %% still be held.
                                ok;
                            _ ->
                                ?DEBUG("Won't delete some snapshots because "
                                       "they are in use (used seqno ~p):~n~p",
                                       [UsedSeqno, Used])
                        end,

                        {KeepSnapshots0 ++ Used, Unused}
                end,

            lists:foreach(
              fun ({SnapshotSeqno, _, _, _}) ->
                      delete_snapshot(SnapshotSeqno, Storage)
              end, DeleteSnapshots),

            Storage#storage{snapshots = KeepSnapshots};
        false ->
            Storage
    end.

delete_snapshot(SnapshotSeqno, #storage{data_dir = DataDir}) ->
    SnapshotDir = snapshot_dir(DataDir, SnapshotSeqno),
    ?INFO("Deleting snapshot at seqno ~p: ~s", [SnapshotSeqno, SnapshotDir]),
    case chronicle_utils:delete_recursive(SnapshotDir) of
        ok ->
            ok;
        {error, Error} ->
            exit({delete_snapshot_failed, Error})
    end.

compact_log(#storage{log_segments = LogSegments} = Storage) ->
    NumLogSegments = length(LogSegments),
    case NumLogSegments > ?MAX_LOG_SEGMENTS of
        true ->
            do_compact_log(Storage);
        false ->
            Storage
    end.

do_compact_log(#storage{low_seqno = LowSeqno,
                        log_segments = LogSegments} = Storage) ->
    %% Make sure we don't lose the snapshots
    %%
    %% TODO: consider moving all syncing to chronicle_storage, so no extra
    %% syncing is needed here.
    sync(Storage),

    %% Don't delete past our earliest snapshot.
    SnapshotSeqno = get_latest_snapshot_seqno(Storage),

    {CannotDelete, CanDelete} = classify_logs(SnapshotSeqno, LogSegments),
    LogSegments = CannotDelete ++ CanDelete,

    {Keep, Delete} =
        case length(CannotDelete) > ?MAX_LOG_SEGMENTS of
            true ->
                WarnLogs = lists:nthtail(?MAX_LOG_SEGMENTS, CannotDelete),

                %% This shouldn't happen assuming snapshots are taken
                %% regularly enough. But if it does, for the sake of
                %% simplicity, we'll just wait until the next snapshot
                %% (instead of requesting a snapshot explicitly).
                ?WARNING("Can't delete some required log segments. "
                         "Snapshot seqno ~p.~n"
                         "All log segments:~n~p~n"
                         "Extraneous log segments:~n~p",
                         [SnapshotSeqno, LogSegments, WarnLogs]),

                {CannotDelete, CanDelete};
            false ->
                lists:split(?MAX_LOG_SEGMENTS, LogSegments)
        end,

    case Delete of
        [] ->
            ok;
        _ ->
            ?DEBUG("Going to delete the following log "
                   "files (preserved snapshot seqno: ~p):~n"
                   "~p",
                   [SnapshotSeqno, Delete]),

            lists:foreach(
              fun ({LogPath, _}) ->
                      case file:delete(LogPath) of
                          ok ->
                              ?INFO("Deleted ~s", [LogPath]),
                              true;
                          {error, Error} ->
                              ?ERROR("Failed to delete ~s: ~p",
                                     [LogPath, Error]),
                              error({compact_log_failed, LogPath, Error})
                      end
              end, Delete)
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
classify_logs_loop() ->
    Logs = [{log1, 200}, {log2, 150}, {log3, 200}, {log4, 150}, {log5, 100}],
    ?assertEqual({[], Logs}, classify_logs(250, Logs)),
    ?assertEqual({[], Logs}, classify_logs(200, Logs)),
    ?assertEqual({Logs, []}, classify_logs(50, Logs)),

    [Log1, Log2, Log3, Log4, Log5] = Logs,
    ?assertEqual({[Log1, Log2], [Log3, Log4, Log5]}, classify_logs(175, Logs)),
    ?assertEqual({[Log1, Log2, Log3, Log4], [Log5]}, classify_logs(125, Logs)).
-endif.
