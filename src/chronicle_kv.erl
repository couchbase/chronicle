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
-module(chronicle_kv).
-compile(export_all).

-include("chronicle.hrl").

-import(chronicle_utils, [start_timeout/1]).

%% TODO: make configurable
-define(DEFAULT_TIMEOUT, 15000).

-record(data, {name,
               meta_table,
               kv_table,
               event_mgr,
               initialized}).

event_manager(Name) ->
    ?SERVER_NAME(event_manager_name(Name)).

add(Name, Key, Value) ->
    add(Name, Key, Value, #{}).

add(Name, Key, Value, Opts) ->
    submit_command(Name, {add, Key, Value}, get_timeout(Opts), Opts).

set(Name, Key, Value) ->
    set(Name, Key, Value, any).

set(Name, Key, Value, ExpectedRevision) ->
    set(Name, Key, Value, ExpectedRevision, #{}).

set(Name, Key, Value, ExpectedRevision, Opts) ->
    submit_command(Name,
                   {set, Key, Value, ExpectedRevision},
                   get_timeout(Opts), Opts).

update(Name, Key, Fun) ->
    update(Name, Key, Fun, #{}).

update(Name, Key, Fun, Opts) ->
    case get(Name, Key) of
        {ok, {Value, Revision}} ->
            set(Name, Key, Fun(Value), Revision, Opts);
        {error, _} = Error ->
            Error
    end.

delete(Name, Key) ->
    delete(Name, Key, any).

delete(Name, Key, ExpectedRevision) ->
    delete(Name, Key, ExpectedRevision, #{}).

delete(Name, Key, ExpectedRevision, Opts) ->
    submit_command(Name,
                   {delete, Key, ExpectedRevision},
                   get_timeout(Opts), Opts).

transaction(Name, Keys, Fun) ->
    transaction(Name, Keys, Fun, #{}).

transaction(Name, Keys, Fun, Opts) ->
    TRef = start_timeout(get_timeout(Opts)),
    {ok, {Snapshot, Missing}} = get_snapshot(Name, Keys, TRef, Opts),
    case Fun(Snapshot) of
        {commit, Updates} ->
            Conditions = transaction_conditions(Snapshot, Missing),
            submit_transaction(Name, Conditions, Updates, TRef, Opts);
        {commit, Updates, Extra} ->
            Conditions = transaction_conditions(Snapshot, Missing),
            case submit_transaction(Name, Conditions, Updates, TRef, Opts) of
                {ok, Revision} ->
                    {ok, Revision, Extra};
                Other ->
                    Other
            end;
        {abort, Result} ->
            Result
    end.

transaction_conditions(Snapshot, Missing) ->
    ConditionsMissing = [{missing, Key} || Key <- Missing],
    maps:fold(
      fun (Key, {_, Revision}, Acc) ->
              [{revision, Key, Revision} | Acc]
      end, ConditionsMissing, Snapshot).

submit_transaction(Name, Conditions, Updates) ->
    submit_transaction(Name, Conditions, Updates, #{}).

submit_transaction(Name, Conditions, Updates, Opts) ->
    submit_transaction(Name, Conditions, Updates, get_timeout(Opts), Opts).

submit_transaction(Name, Conditions, Updates, Timeout, Opts) ->
    submit_command(Name, {transaction, Conditions, Updates}, Timeout, Opts).

get(Name, Key) ->
    get(Name, Key, #{}).

get(Name, Key, Opts) ->
    optimistic_query(
      Name, {get, Key}, get_timeout(Opts), Opts,
      fun () ->
              case get_fast_path(Name, Key) of
                  {ok, _} = Ok ->
                      Ok;
                  {error, no_table} ->
                      %% This may happen in multiple situations:
                      %%
                      %% 1. The chronicle_kv process is still initializing.
                      %% 2. New snapshot is installed around the time when we
                      %%    attempt to read from the table.
                      %% 3. The process is dead/restarting.
                      %%
                      %% It's not possible to distinguish between these cases
                      %% easily. In some cases (2), we could simply retry, in
                      %% others (3), retrying is not likely to help. For
                      %% simplicity all these cases are dealt with by falling
                      %% back to a query to the chronicle_kv process.
                      use_slow_path;
                  {error, _} = Error ->
                      Error
              end
      end).

get_fast_path(Name, Key) ->
    case get_kv_table(Name) of
        {ok, Table} ->
            get_from_kv_table(Table, Key);
        {error, no_table} = Error ->
            Error
    end.

rewrite(Name, Fun) ->
    rewrite(Name, Fun, #{}).

rewrite(Name, Fun, Opts) ->
    TRef = start_timeout(get_timeout(Opts)),
    case submit_query(Name, {rewrite, Fun}, TRef, Opts) of
        {ok, Conditions, Updates} ->
            submit_transaction(Name, Conditions, Updates, TRef, Opts);
        {error, _} = Error ->
            Error
    end.

get_full_snapshot(Name) ->
    get_full_snapshot(Name, #{}).

get_full_snapshot(Name, Opts) ->
    submit_query(Name, get_full_snapshot, get_timeout(Opts), Opts).

get_snapshot(Name, Keys) ->
    get_snapshot(Name, Keys, #{}).

get_snapshot(Name, Keys, Opts) ->
    get_snapshot(Name, Keys, get_timeout(Opts), Opts).

get_snapshot(Name, Keys, Timeout, Opts) ->
    optimistic_query(
      Name, {get_snapshot, Keys}, Timeout, Opts,
      fun () ->
              get_snapshot_fast_path(Name, Keys)
      end).

get_snapshot_fast_path(Name, Keys) ->
    case get_kv_table(Name) of
        {ok, Table} ->
            {_, TableSeqno} = get_revision(Name),
            get_snapshot_fast_path_loop(Table, TableSeqno, Keys, #{});
        {error, no_table} ->
            use_slow_path
    end.

get_snapshot_fast_path_loop(_Table, _TableSeqno, [], Snapshot) ->
    {ok, {Snapshot, []}};
get_snapshot_fast_path_loop(Table, TableSeqno, [Key | RestKeys], Snapshot) ->
    case get_from_kv_table(Table, Key) of
        {ok, {_, {_HistoryId, Seqno}} = ValueRev} ->
            case Seqno =< TableSeqno of
                true ->
                    get_snapshot_fast_path_loop(
                      Table, TableSeqno, RestKeys,
                      maps:put(Key, ValueRev, Snapshot));
                false ->
                    %% The table revision is updated strictly after pushing
                    %% all changes to the ets table. So as long as all values
                    %% that we read have revisions that are not greater than
                    %% the table revision, we are guaranteed to have observed
                    %% a consistent snapshot. Otherwise, there's no such
                    %% guarantee and we need to fall back to the slow path.
                    use_slow_path
            end;
        {error, Error} when Error =:= no_table;
                            Error =:= not_found ->
            %% Note that any key that we fail to find forces the slow
            %% path. This could be avoided if deletion tombstones were kept in
            %% the ets table. Then we could simply use the revision of the
            %% tombstone to find out if this read could violate snapshot
            %% isolation just like in the normal case above. But since there
            %% are no tombstones, we need to use the slow path. Essentially,
            %% we assume that in most cases the keys that the caller is
            %% interested in will exist and therefore this optimization will
            %% be benefitial.
            use_slow_path
    end.

get_revision(Name) ->
    chronicle_rsm:get_local_revision(Name).

sync(Name, Type) ->
    sync(Name, Type, ?DEFAULT_TIMEOUT).

sync(Name, Type, Timeout) ->
    case Type of
        local ->
            ok;
        _ ->
            chronicle_rsm:sync(Name, Type, Timeout)
    end.

%% callbacks
specs(Name, _Args) ->
    EventName = event_manager_name(Name),
    Spec = #{id => EventName,
             start => {gen_event, start_link, [?START_NAME(EventName)]},
             restart => permanent,
             shutdown => brutal_kill,
             type => worker},
    [Spec].

init(Name, []) ->
    %% In order to prevent client from reading from not fully initialized ets
    %% table an internal table is used. The state is restored into this
    %% internal table which is then published (by renaming) in post_init/3
    %% callback.
    MetaTable = ets:new(?ETS_TABLE(Name),
                        [protected, named_table, {read_concurrency, true}]),
    KvTable = create_kv_table(Name),
    {ok, #{}, #data{name = Name,
                    initialized = false,
                    event_mgr = event_manager(Name),
                    meta_table = ets:whereis(MetaTable),
                    kv_table = KvTable}}.

post_init(_Revision, _State, #data{kv_table = KvTable} = Data) ->
    publish_kv_table(KvTable, Data),
    {ok, Data#data{initialized = true}}.

apply_snapshot(SnapshotRevision, SnapshotState, _OldRevision, _OldState,
               #data{name = Name,
                     kv_table = OldKvTable,
                     initialized = Initialized} = Data) ->
    KvTable = create_kv_table(Name),
    chronicle_utils:maps_foreach(
      fun (Key, ValueRev) ->
              true = ets:insert_new(KvTable, {Key, ValueRev})
      end, SnapshotState),

    case Initialized of
        true ->
            publish_kv_table(KvTable, Data);
        false ->
            %% post_init() will publish the table once fully initialized
            ok
    end,
    ets:delete(OldKvTable),
    notify_snapshot_installed(SnapshotRevision, Data),
    {ok, Data#data{kv_table = KvTable}}.

handle_command(_, _StateRevision, _State, Data) ->
    {apply, Data}.

handle_query({rewrite, Fun}, StateRevision, State, Data) ->
    handle_rewrite(Fun, StateRevision, State, Data);
handle_query({get, Key}, _StateRevision, State, Data) ->
    handle_get(Key, State, Data);
handle_query(get_full_snapshot, StateRevision, State, Data) ->
    handle_get_full_snapshot(StateRevision, State, Data);
handle_query({get_snapshot, Keys}, _StateRevision, State, Data) ->
    handle_get_snapshot(Keys, State, Data).

apply_command({add, Key, Value}, Revision, StateRevision, State, Data) ->
    apply_add(Key, Value, Revision, StateRevision, State, Data);
apply_command({set, Key, Value, ExpectedRevision}, Revision,
              StateRevision, State, Data) ->
    apply_set(Key, Value, ExpectedRevision,
              Revision, StateRevision, State, Data);
apply_command({delete, Key, ExpectedRevision}, Revision,
              StateRevision, State, Data) ->
    apply_delete(Key, ExpectedRevision, Revision, StateRevision, State, Data);
apply_command({transaction, Conditions, Updates}, Revision,
              StateRevision, State, Data) ->
    apply_transaction(Conditions, Updates,
                      Revision, StateRevision, State, Data).

handle_info(Msg, _StateRevision, _State, Data) ->
    ?WARNING("Unexpected message: ~p", [Msg]),
    {noreply, Data}.

terminate(_Reason, _StateRevision, _State, _Data) ->
    ok.

%% internal
get_timeout(Opts) ->
    maps:get(timeout, Opts, ?DEFAULT_TIMEOUT).

optimistic_query(Name, Query, Timeout, Opts, Body) ->
    TRef = start_timeout(Timeout),
    case handle_read_consistency(Name, TRef, Opts) of
        ok ->
            case Body() of
                {ok, _} = Ok ->
                    Ok;
                {error, _} = Error ->
                    Error;
                use_slow_path ->
                    chronicle_rsm:query(Name, Query, TRef)
            end;
        {error, _} = Error ->
            Error
    end.

submit_query(Name, Query, Timeout, Opts) ->
    TRef = start_timeout(Timeout),
    case handle_read_consistency(Name, TRef, Opts) of
        ok ->
            chronicle_rsm:query(Name, Query, TRef);
        {error, _} = Error ->
            Error
    end.

handle_read_consistency(Name, Timeout, Opts) ->
    Consistency = maps:get(read_consistency, Opts, local),
    sync(Name, Consistency, Timeout).

submit_command(Name, Command, Timeout, Opts) ->
    TRef = start_timeout(Timeout),
    Result = chronicle_rsm:command(Name, Command, TRef),
    handle_read_own_writes(Name, Result, TRef, Opts),
    Result.

handle_read_own_writes(Name, Result, TRef, Opts) ->
    case get_read_own_writes_revision(Result, Opts) of
        {ok, Revision} ->
            chronicle_rsm:sync_revision(Name, Revision, TRef);
        no_revision ->
            ok
    end.

get_read_own_writes_revision(Result, Opts) ->
    case maps:get(read_own_writes, Opts, true) of
        true ->
            case Result of
                {ok, Revision} ->
                    {ok, Revision};
                {error, {conflict, Revision}} ->
                    {ok, Revision};
                _Other ->
                    no_revision
            end;
        false ->
            no_revision
    end.

handle_rewrite(Fun, StateRevision, State, Data) ->
    Updates =
        maps:fold(
          fun (Key, {Value, _Rev}, Acc) ->
                  case Fun(Key, Value) of
                      {update, NewValue} ->
                          [{set, Key, NewValue} | Acc];
                      {update, NewKey, NewValue} ->
                          case Key =:= NewKey of
                              true ->
                                  [{set, Key, NewValue} | Acc];
                              false ->
                                  [{delete, Key},
                                   {set, NewKey, NewValue} | Acc]
                          end;
                      keep ->
                          Acc;
                      delete ->
                          [{delete, Key} | Acc]
                  end
          end, [], State),

    Conditions = [{state_revision, StateRevision}],
    {reply, {ok, Conditions, Updates}, Data}.

handle_get(Key, State, Data) ->
    Reply =
        case maps:find(Key, State) of
            {ok, _} = Ok ->
                Ok;
            error ->
                {error, not_found}
        end,

    {reply, Reply, Data}.

handle_get_full_snapshot(StateRevision, State, Data) ->
    {reply, {ok, {State, StateRevision}}, Data}.

handle_get_snapshot(Keys, State, Data) ->
    Snapshot = maps:with(Keys, State),
    Missing = Keys -- maps:keys(Snapshot),
    {reply, {ok, {Snapshot, Missing}}, Data}.

apply_add(Key, Value, Revision, StateRevision, State, Data) ->
    case check_condition({missing, Key}, Revision, StateRevision, State) of
        ok ->
            NewState = handle_update(Key, Value, Revision, State, Data),
            Reply = {ok, Revision},
            {reply, Reply, NewState, Data};
        {error, _} = Error ->
            {reply, Error, State, Data}
    end.

apply_set(Key, Value, ExpectedRevision, Revision, StateRevision, State, Data) ->
    case check_condition({revision, Key, ExpectedRevision},
                         Revision, StateRevision, State) of
        ok ->
            NewState = handle_update(Key, Value, Revision, State, Data),
            Reply = {ok, Revision},
            {reply, Reply, NewState, Data};
        {error, _} = Error ->
            {reply, Error, State, Data}
    end.

apply_delete(Key, ExpectedRevision, Revision, StateRevision, State, Data) ->
    case check_condition({revision, Key, ExpectedRevision},
                         Revision, StateRevision, State) of
        ok ->
            NewState = handle_delete(Key, Revision, State, Data),
            Reply = {ok, Revision},
            {reply, Reply, NewState, Data};
        {error, _} = Error ->
            {reply, Error, State, Data}
    end.

apply_transaction(Conditions, Updates, Revision, StateRevision, State, Data) ->
    case check_transaction(Conditions, Revision, StateRevision, State) of
        ok ->
            NewState =
                apply_transaction_updates(Updates, Revision, State, Data),
            Reply = {ok, Revision},
            {reply, Reply, NewState, Data};
        {error, _} = Error ->
            {reply, Error, State, Data}
    end.

check_transaction(Conditions, Revision, StateRevision, State) ->
    check_transaction_loop(Conditions, Revision, StateRevision, State).

check_transaction_loop([], _, _, _) ->
    ok;
check_transaction_loop([Condition | Rest], Revision, StateRevision, State) ->
    case check_condition(Condition, Revision, StateRevision, State) of
        ok ->
            check_transaction_loop(Rest, Revision, StateRevision, State);
        {error, _} = Error ->
            Error
    end.

apply_transaction_updates(Updates, Revision, State, Data) ->
    lists:foldl(
      fun (Update, AccState) ->
              case Update of
                  {set, Key, Value} ->
                      handle_update(Key, Value, Revision, AccState, Data);
                  {delete, Key} ->
                      handle_delete(Key, Revision, AccState, Data)
              end
      end, State, Updates).

check_condition(Condition, Revision, StateRevision, State) ->
    case condition_holds(Condition, StateRevision, State) of
        true ->
            ok;
        false ->
            {error, {conflict, Revision}}
    end.

condition_holds({state_revision, Revision}, StateRevision, _State) ->
    Revision =:= StateRevision;
condition_holds({missing, Key}, _StateRevision, State) ->
    not maps:is_key(Key, State);
condition_holds({revision, _Key, any}, _StateRevision, _State) ->
    true;
condition_holds({revision, Key, Revision}, _StateRevision, State) ->
    case maps:find(Key, State) of
        {ok, {_, OurRevision}} ->
            OurRevision =:= Revision;
        error ->
            false
    end.

event_manager_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "-events").

handle_update(Key, Value, Revision, State, #data{kv_table = Table} = Data) ->
    ValueRev = {Value, Revision},
    ets:insert(Table, {Key, ValueRev}),
    notify_updated(Key, Revision, Value, Data),
    maps:put(Key, ValueRev, State).

handle_delete(Key, Revision, State, #data{kv_table = Table} = Data) ->
    ets:delete(Table, Key),
    notify_deleted(Key, Revision, Data),
    maps:remove(Key, State).

notify(Event, #data{initialized = true, event_mgr = Mgr}) ->
    gen_event:notify(Mgr, Event);
notify(_Event, _Data) ->
    ok.

notify_key(Key, Revision, Event, Data) ->
    notify({{key, Key}, Revision, Event}, Data).

notify_deleted(Key, Revision, Data) ->
    notify_key(Key, Revision, deleted, Data).

notify_updated(Key, Revision, Value, Data) ->
    notify_key(Key, Revision, {updated, Value}, Data).

notify_snapshot_installed(Revision, Data) ->
    notify({snapshot_installed, Revision}, Data).

get_kv_table(Name) ->
    case ets:lookup(?ETS_TABLE(Name), table) of
        [{_, TableRef}] ->
            {ok, TableRef};
        [] ->
            %% The process is going through init.
            {error, no_table}
    end.

create_kv_table(Name) ->
    ets:new(Name, [protected, {read_concurrency, true}]).

publish_kv_table(KvTable, #data{meta_table = MetaTable}) ->
    ets:insert(MetaTable, {table, KvTable}).

get_from_kv_table(Table, Key) ->
    try ets:lookup(Table, Key) of
        [] ->
            {error, not_found};
        [{_, ValueRev}] ->
            {ok, ValueRev}
    catch
        error:badarg ->
            {error, no_table}
    end.
