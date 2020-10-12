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

-define(MEM_LOG_INFO_TAB, ?ETS_TABLE(chronicle_mem_log_info)).
-define(MEM_LOG_TAB, ?ETS_TABLE(chronicle_mem_log)).
-define(CONFIG_INDEX, ?ETS_TABLE(chronicle_config_index)).

-define(RANGE_KEY, '$range').

-define(READ_CHUNK_SIZE, 1024 * 1024).

-record(storage, { current_log,
                   low_seqno,
                   high_seqno,
                   committed_seqno,
                   meta,
                   config,
                   snapshots,

                   persist,
                   data_dir,

                   log_info_tab,
                   log_tab,
                   config_index_tab
                  }).

open() ->
    Persist = chronicle_env:persist(),

    ets:new(?MEM_LOG_INFO_TAB,
            [protected, set, named_table, {read_concurrency, true}]),
    ets:new(?MEM_LOG_TAB,
            [protected, set, named_table,
             {keypos, #log_entry.seqno}, {read_concurrency, true}]),
    ets:new(?CONFIG_INDEX,
            [protected, ordered_set, named_table, {keypos, #log_entry.seqno}]),
    Storage = #storage{log_info_tab = ets:whereis(?MEM_LOG_INFO_TAB),
                       log_tab = ets:whereis(?MEM_LOG_TAB),
                       config_index_tab = ets:whereis(?CONFIG_INDEX),
                       persist = Persist,
                       low_seqno = ?NO_SEQNO + 1,
                       high_seqno = ?NO_SEQNO,
                       committed_seqno = ?NO_SEQNO,
                       meta = #{}},

    try
        case Persist of
            true ->
                DataDir = chronicle_env:data_dir(),
                maybe_complete_wipe(DataDir),
                ensure_dirs(DataDir),
                validate_state(open_logs(Storage#storage{data_dir = DataDir}));
            false ->
                Storage
        end
    catch
        T:E:Stack ->
            close(Storage),
            erlang:raise(T, E, Stack)
    end.

open_logs(#storage{data_dir = DataDir} = Storage) ->
    {Sealed, Current} =
        case find_logs(DataDir) of
            [] ->
                {[], 0};
            Logs ->
                {lists:droplast(Logs), lists:last(Logs)}
        end,

    InitState = #{meta => #{},
                  config => undefined,
                  low_seqno => ?NO_SEQNO + 1,
                  high_seqno => ?NO_SEQNO,
                  snapshots => []},
    SealedState =
        lists:foldl(
          fun (LogIndex, Acc) ->
                  LogPath = log_path(DataDir, LogIndex),
                  HandleEntryFun = make_handle_log_entry_fun(LogPath, Storage),

                  case chronicle_log:read_log(LogPath, HandleEntryFun, Acc) of
                      {ok, NewAcc} ->
                          NewAcc;
                      {error, Error} ->
                          ?ERROR("Failed to read log ~p: ~p", [LogPath, Error]),
                          exit({failed_to_read_log, LogPath, Error})
                  end
          end, InitState, Sealed),

    CurrentLogPath = log_path(DataDir, Current),
    case chronicle_log:open(CurrentLogPath,
                            make_handle_log_entry_fun(CurrentLogPath, Storage),
                            SealedState) of
        {ok, CurrentLog, FinalState} ->
            #{meta := Meta, config := Config,
              low_seqno := LowSeqno, high_seqno := HighSeqno,
              snapshots := Snapshots} = FinalState,
            Storage#storage{current_log = CurrentLog,
                            low_seqno = LowSeqno,
                            high_seqno = HighSeqno,
                            meta = Meta,
                            config = Config,
                            snapshots = Snapshots};
        {error, Error} ->
            ?ERROR("Failed to open log ~p: ~p", [CurrentLogPath, Error]),
            exit({failed_to_open_log, CurrentLogPath, Error})
    end.

publish(#storage{log_info_tab = LogInfoTab,
                 low_seqno = LowSeqno,
                 high_seqno = HighSeqno,
                 committed_seqno = CommittedSeqno} = Storage) ->
    ets:insert(LogInfoTab, {?RANGE_KEY, LowSeqno, HighSeqno, CommittedSeqno}),
    Storage.

set_committed_seqno(Seqno, #storage{
                              low_seqno = LowSeqno,
                              high_seqno = HighSeqno,
                              committed_seqno = CommittedSeqno} = Storage) ->
    true = (Seqno >= CommittedSeqno),
    true = (Seqno >= LowSeqno - 1),
    true = (Seqno =< HighSeqno),

    Storage#storage{committed_seqno = Seqno}.

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
    filename:join(snapshots_dir(DataDir), io_lib:format("~16.16.0b", [Seqno])).

wipe() ->
    case chronicle_env:persist() of
        true ->
            DataDir = chronicle_env:data_dir(),
            ChronicleDir = chronicle_dir(DataDir),
            case file_exists(ChronicleDir, directory) of
                true ->
                    ok = chronicle_utils:create_marker(wipe_marker(DataDir)),
                    complete_wipe(DataDir);
                false ->
                    ok
            end;
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

make_handle_log_entry_fun(LogPath, Storage) ->
    fun (Entry, State) ->
            handle_log_entry(LogPath, Storage, Entry, State)
    end.

handle_log_entry(LogPath, Storage, Entry, State) ->
    case Entry of
        {atomic, Entries} ->
            lists:foldl(
              fun (SubEntry, Acc) ->
                      handle_log_entry(LogPath, Storage, SubEntry, Acc)
              end, State, Entries);
        {meta, KVs} ->
            maps:update_with(meta,
                             fun (CurrentMeta) ->
                                     maps:merge(CurrentMeta, KVs)
                             end, State);
        {truncate, Seqno} ->
            NewConfig = truncate_table(Storage#storage.config_index_tab, Seqno),
            State#{config => NewConfig,
                   high_seqno => Seqno};
        {snapshot, Seqno, Config} ->
            maps:update_with(snapshots,
                             fun (CurrentSnapshots) ->
                                     [{Seqno, Config} | CurrentSnapshots]
                             end, State);
        {install_snapshot, Seqno, Config, Meta} ->
            CurrentMeta = maps:get(meta, State),
            State#{low_seqno => Seqno + 1,
                   high_seqno => Seqno,
                   config => Config,
                   meta => maps:merge(CurrentMeta, Meta)};
        #log_entry{seqno = Seqno} ->
            #{low_seqno := LowSeqno,
              high_seqno := PrevSeqno,
              config := Config} = State,

            case Seqno =:= PrevSeqno + 1 orelse PrevSeqno =:= ?NO_SEQNO of
                true ->
                    ok;
                false ->
                    exit({inconsistent_log, LogPath, Entry, PrevSeqno})
            end,

            NewLowSeqno =
                case LowSeqno =/= ?NO_SEQNO of
                    true ->
                        LowSeqno;
                    false ->
                        Seqno
                end,

            ets:insert(Storage#storage.log_tab, Entry),
            NewConfig  =
                case is_config_entry(Entry) of
                    true ->
                        ets:insert(Storage#storage.config_index_tab, Entry),
                        Entry;
                    false ->
                        Config
                end,

            State#{config => NewConfig,
                   low_seqno => NewLowSeqno,
                   high_seqno => Seqno}
    end.

is_config_entry(#log_entry{value = Value}) ->
    case Value of
        #config{} ->
            true;
        #transition{} ->
            true;
        _ ->
            false
    end.

get_meta(Storage) ->
    Storage#storage.meta.

get_high_seqno(Storage) ->
    Storage#storage.high_seqno.

get_config(Storage) ->
    Storage#storage.config.

get_config_for_seqno(Seqno, #storage{config_index_tab = Tab}) ->
    case ets:prev(Tab, Seqno + 1) of
        '$end_of_table' ->
            exit({no_config_for_seqno, Seqno});
        ConfigSeqno ->
            [Config] = ets:lookup(Tab, ConfigSeqno),
            Config
    end.

store_meta(Updates, Storage) ->
    case store_meta_prepare(Updates, Storage) of
        {ok, DedupedUpdates, NewStorage} ->
            log_append([{meta, DedupedUpdates}], Storage),
            NewStorage;
        not_needed ->
            Storage
    end.

store_meta_prepare(Updates, #storage{meta = Meta} = Storage) ->
    Deduped = maps:filter(
                fun (Key, Value) ->
                        case maps:find(Key, Meta) of
                            {ok, CurrentValue} ->
                                Value =/= CurrentValue;
                            error ->
                                true
                        end
                end, Updates),
    case maps:size(Deduped) > 0 of
        true ->
            {ok, Deduped, Storage#storage{meta = maps:merge(Meta, Deduped)}};
        false ->
            not_needed
    end.

truncate(Seqno, #storage{high_seqno = HighSeqno,
                         committed_seqno = CommittedSeqno} = Storage) ->
    true = (Seqno >= CommittedSeqno),
    true = (Seqno =< HighSeqno),

    log_append([{truncate, Seqno}], Storage),
    NewStorage = config_index_truncate(Seqno, Storage),
    NewStorage#storage{high_seqno = Seqno}.

append(StartSeqno, EndSeqno, Entries, Opts,
       #storage{high_seqno = HighSeqno} = Storage) ->
    true = (StartSeqno =:= HighSeqno + 1),
    {DiskEntries, NewStorage0} = append_handle_meta(Storage, Entries, Opts),
    log_append(DiskEntries, NewStorage0),
    NewStorage1 = mem_log_append(EndSeqno, Entries, NewStorage0),
    config_index_append(Entries, NewStorage1).

append_handle_meta(Storage, Entries, Opts) ->
    case maps:find(meta, Opts) of
        {ok, Meta} ->
            case store_meta_prepare(Meta, Storage) of
                {ok, DedupedMeta, NewStorage} ->
                    NewEntries = [{atomic, [{meta, DedupedMeta} | Entries]}],
                    {NewEntries, NewStorage};
                not_needed ->
                    {Entries, Storage}
            end;
        error ->
            {Entries, Storage}
    end.

sync(#storage{current_log = Log, persist = true}) ->
    case chronicle_log:sync(Log) of
        ok ->
            ok;
        {error, Error} ->
            exit({sync_failed, Error})
    end;
sync(#storage{persist = false}) ->
    ok.

close(#storage{current_log = Log,
               log_tab = LogTab,
               log_info_tab = LogInfoTab,
               config_index_tab = ConfigIndexTab}) ->
    ets:delete(LogTab),
    ets:delete(LogInfoTab),
    ets:delete(ConfigIndexTab),

    case Log of
        undefined ->
            ok;
        _ ->
            ok = chronicle_log:close(Log)
    end.

find_logs(DataDir) ->
    LogsDir = logs_dir(DataDir),
    Candidates = filelib:wildcard(filename:join(LogsDir, "*.log")),
    Logs = lists:filtermap(
             fun (Candidate) ->
                     Name = filename:basename(Candidate),
                     case re:run(Name,
                                 "^([[:digit:]]\+).log$",
                                 [{capture, all_but_first, list}]) of
                         {match, [Index]} ->
                             {true, list_to_integer(Index)};
                         nomatch ->
                             ?WARNING("Ignoring unexpected file on "
                                      "log directory: ~p", [Candidate]),
                             false
                     end
             end, Candidates),

    lists:sort(Logs).

log_path(DataDir, LogIndex) ->
    filename:join(logs_dir(DataDir), integer_to_list(LogIndex) ++ ".log").

log_append(Records, #storage{current_log = Log, persist = true}) ->
    case chronicle_log:append(Log, Records) of
        ok ->
            ok;
        {error, Error} ->
            exit({append_failed, Error})
    end;
log_append(_Records, #storage{persist = false}) ->
    ok.

config_index_truncate(Seqno, #storage{config_index_tab = Tab} = Storage) ->
    NewConfig = truncate_table(Tab, Seqno),
    Storage#storage{config = NewConfig}.

config_index_append(Entries, #storage{config_index_tab = ConfigIndex,
                                      config = Config} = Storage) ->
    ConfigEntries = lists:filter(fun is_config_entry/1, Entries),
    NewConfig =
        case ConfigEntries of
            [] ->
                Config;
            _ ->
                ets:insert(ConfigIndex, ConfigEntries),
                lists:last(ConfigEntries)
        end,

    Storage#storage{config = NewConfig}.

truncate_table(Table, Seqno) ->
    truncate_table_loop(Table, ets:last(Table), Seqno).

truncate_table_loop(Table, Last, Seqno) ->
    case Last of
        '$end_of_table' ->
            undefined;
        EntrySeqno ->
            case EntrySeqno > Seqno of
                true ->
                    Prev = ets:prev(Table, EntrySeqno),
                    ets:delete(Table, EntrySeqno),
                    truncate_table_loop(Table, Prev, Seqno);
                false ->
                    [Entry] = ets:lookup(Table, EntrySeqno),
                    Entry
            end
    end.

mem_log_append(EndSeqno, Entries,
               #storage{log_tab = LogTab} = Storage) ->
    ets:insert(LogTab, Entries),
    Storage#storage{high_seqno = EndSeqno}.

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
    {LogLowSeqno, LogHighSeqno} = get_seqno_range(),
    get_log_loop(LogLowSeqno, LogHighSeqno, []).

get_log(StartSeqno, EndSeqno) ->
    {LogLowSeqno, LogHighSeqno} = get_seqno_range(),
    true = (StartSeqno >= LogLowSeqno),
    true = (EndSeqno =< LogHighSeqno),

    get_log_loop(StartSeqno, EndSeqno, []).

get_log_loop(StartSeqno, EndSeqno, Acc)
  when EndSeqno < StartSeqno ->
    Acc;
get_log_loop(StartSeqno, EndSeqno, Acc) ->
    %% TODO: need to handle entries being deleted underneath us
    [Entry] = ets:lookup(?MEM_LOG_TAB, EndSeqno),
    get_log_loop(StartSeqno, EndSeqno - 1, [Entry | Acc]).

get_log_entry(Seqno, #storage{log_tab = Tab}) ->
    case ets:lookup(Tab, Seqno) of
        [Entry] ->
            {ok, Entry};
        [] ->
            {error, not_found}
    end.

get_seqno_range() ->
    [{_, LowSeqno, HighSeqno, _}] = ets:lookup(?MEM_LOG_INFO_TAB, ?RANGE_KEY),
    {LowSeqno, HighSeqno}.

record_snapshot(Seqno, Config, #storage{data_dir = DataDir,
                                        snapshots = Snapshots} = Storage) ->
    LatestSnapshotSeqno = get_latest_snapshot_seqno(Storage),
    true = (Seqno > LatestSnapshotSeqno),

    SnapshotsDir = snapshots_dir(DataDir),
    sync_dir(SnapshotsDir),

    log_append([{snapshot, Seqno, Config}], Storage),
    Storage#storage{snapshots = [{Seqno, Config} | Snapshots]}.

install_snapshot(Seqno, Config, Meta,
                 #storage{high_seqno = HighSeqno,
                          meta = OldMeta} = Storage) ->
    true = (Seqno > HighSeqno),

    Seqno = get_latest_snapshot_seqno(Storage),
    log_append([{install_snapshot, Seqno, Config, Meta}], Storage),

    %% TODO: The log entries in the ets table need to be cleaned up as
    %% well. Deal with this as part of compaction.
    %% TODO: config index also needs to be reset
    Storage#storage{meta = maps:merge(OldMeta, Meta),
                    low_seqno = Seqno + 1,
                    high_seqno = Seqno,
                    config = Config}.

rsm_snapshot_path(SnapshotDir, RSM) ->
    filename:join(SnapshotDir, [RSM, ".snapshot"]).

save_rsm_snapshot(Seqno, RSM, RSMState,
                  #storage{data_dir = DataDir, snapshots = Snapshots}) ->
    %% Make sure we are not overwriting an existing snapshot.
    false = lists:keymember(Seqno, 1, Snapshots),

    SnapshotDir = snapshot_dir(DataDir, Seqno),
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
    case file:open(Path, [read, raw, binary]) of
        {ok, File} ->
            case chronicle_utils:read_full(File, ?CRC_BYTES) of
                {ok, <<Crc:?CRC_BITS>>} ->
                    validate_rsm_snapshot_loop(Path, File, Crc, 0);
                eof ->
                    {error, unexpected_eof};
                {error, Error} ->
                    exit({read_failed, Path, Error})
            end;
        {error, not_found} ->
            {error, not_found};
        {error, Error} ->
            exit({open_failed, Path, Error})
    end.

validate_rsm_snapshot_loop(Path, File, Crc, AccCrc) ->
    case file:read(File, ?READ_CHUNK_SIZE) of
        {ok, Data} ->
            validate_rsm_snapshot_loop(Path, File, Crc,
                                       erlang:crc32(AccCrc, Data));
        eof ->
            case Crc =:= AccCrc of
                true ->
                    ok;
                false ->
                    {error, crc_mismatch}
            end;
        {error, Error} ->
            exit({read_failed, Path, Error})
    end.

validate_snapshot(DataDir, Seqno, Config) ->
    SnapshotDir = snapshot_dir(DataDir, Seqno),
    RSMs = chronicle_utils:config_rsms(Config#log_entry.value),
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
          fun ({Seqno, Config} = Snapshot, {AccValid, AccInvalid}) ->
                  case validate_snapshot(DataDir, Seqno, Config) of
                      ok ->
                          {[Snapshot | AccValid], AccInvalid};
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
    LastSnapshotSeqno =
        case ValidSnapshots of
            [] ->
                ?NO_SEQNO + 1;
            [{Seqno, _} | _] ->
                Seqno
        end,

    case LastSnapshotSeqno >= LowSeqno of
        true ->
            ok;
        false ->
            ?ERROR("Last snapshot at seqno ~p is below our low seqno ~p",
                   [LastSnapshotSeqno, LowSeqno]),
            exit({missing_snapshot, LastSnapshotSeqno, LowSeqno})
    end,

    Storage#storage{snapshots = ValidSnapshots}.

get_latest_snapshot_seqno(#storage{snapshots = Snapshots}) ->
    case Snapshots of
        [] ->
            ?NO_SEQNO;
        [{Seqno, _} | _] ->
            Seqno
    end.
