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

-include("chronicle.hrl").

-import(chronicle_utils, [start_timeout/1,
                          sanitize_stacktrace/1]).

-export([format_state/1]).

%% APIs
-export([event_manager/1]).
-export([add/3, add/4]).
-export([set/3, set/4, set/5]).
-export([update/3, update/4]).
-export([delete/2, delete/3, delete/4]).
-export([multi/2, multi/3]).
-export([transaction/3, transaction/4]).
-export([get/2, get/3]).
-export([rewrite/2, rewrite/3]).
-export([get_snapshot/2, get_snapshot/3]).
-export([get_full_snapshot/1, get_full_snapshot/2]).
-export([get_revision/1]).
-export([sync/1, sync/2]).

-export([txn/2, txn/3, ro_txn/2, ro_txn/3]).
-export([txn_get/2, txn_get_many/2]).

%% callbacks
-export([specs/2,
         init/2, post_init/3, state_enter/4,
         apply_snapshot/5, apply_command/6,
         handle_query/4, handle_info/4,
         handle_config/5,
         terminate/4]).

%% TODO: make configurable
-define(DEFAULT_TIMEOUT, 15000).
-define(TXN_RETRIES, 10).

-type name() :: atom().
-type key() :: any().
-type value() :: any().
-type revision() :: chronicle:revision().
-type revisionreq() :: any | revision().

-type read_consistency() :: local | quorum.
-type options() :: #{timeout => timeout(),
                     read_consistency => read_consistency(),
                     read_own_writes => boolean()}.
-type op_result() :: {ok, revision()}
                   | {error, {conflict, revision()}}.

-record(data, {name,
               meta_table,
               kv_table,
               event_mgr,
               initialized}).

-ifndef(TEST).
-type event_manager_name() :: atom().
-else.
-type event_manager_name() :: any().
-endif.

format_state(State) ->
    chronicle_dump:raw(maps:to_list(State)).

-spec event_manager(name()) -> event_manager_name().
event_manager(Name) ->
    ?SERVER_NAME(event_manager_name(Name)).

-spec add(name(), key(), value()) -> op_result().
add(Name, Key, Value) ->
    add(Name, Key, Value, #{}).

-spec add(name(), key(), value(), options()) -> op_result().
add(Name, Key, Value, Opts) ->
    submit_command(Name, {add, Key, Value}, get_timeout(Opts), Opts).

-spec set(name(), key(), value()) -> op_result().
set(Name, Key, Value) ->
    set(Name, Key, Value, any).

-spec set(name(), key(), value(), revisionreq()) -> op_result().
set(Name, Key, Value, ExpectedRevision) ->
    set(Name, Key, Value, ExpectedRevision, #{}).

-spec set(name(), key(), value(), revisionreq(), options()) -> op_result().
set(Name, Key, Value, ExpectedRevision, Opts) ->
    submit_command(Name,
                   {set, Key, Value, ExpectedRevision},
                   get_timeout(Opts), Opts).

-type update_result() :: {ok, revision()}
                       | {error, not_found | exceeded_retries}.
-type update_fun() :: fun ((value()) -> value()).

-spec update(name(), key(), update_fun()) -> update_result().
update(Name, Key, Fun) ->
    update(Name, Key, Fun, #{}).

-spec update(name(), key(), update_fun(), txn_options()) -> update_result().
update(Name, Key, Fun, Opts) ->
    txn(Name,
        fun (Txn) ->
                case txn_get(Key, Txn) of
                    {ok, {Value, _}} ->
                        {commit, [{set, Key, Fun(Value)}]};
                    {error, not_found} = Error ->
                        {abort, Error}
                end
        end, Opts).

-spec delete(name(), key()) -> op_result().
delete(Name, Key) ->
    delete(Name, Key, any).

-spec delete(name(), key(), revisionreq()) -> op_result().
delete(Name, Key, ExpectedRevision) ->
    delete(Name, Key, ExpectedRevision, #{}).

-spec delete(name(), key(), revisionreq(), options()) -> op_result().
delete(Name, Key, ExpectedRevision, Opts) ->
    submit_command(Name,
                   {delete, Key, ExpectedRevision},
                   get_timeout(Opts), Opts).

-type multi_ops() :: [multi_op()].
-type multi_op() :: {add, key(), value()}
                  | {set, key(), value()}
                  | {set, key(), value(), revisionreq()}
                  | {delete, key()}
                  | {delete, key(), revisionreq()}.

-spec multi(name(), multi_ops()) -> op_result().
multi(Name, Updates) ->
    multi(Name, Updates, #{}).

-spec multi(name(), multi_ops(), options()) -> op_result().
multi(Name, Updates, Opts) ->
    {TxnConditions, TxnUpdates} = multi_to_txn(Updates),
    submit_transaction(Name, TxnConditions, TxnUpdates, Opts).

multi_to_txn(Updates) ->
    multi_to_txn_loop(Updates, [], []).

multi_to_txn_loop([], TxnConditions, TxnUpdates) ->
    {TxnConditions, TxnUpdates};
multi_to_txn_loop([Update | Updates], TxnConditions, TxnUpdates) ->
    case Update of
        {add, Key, Value} ->
            multi_to_txn_loop(Updates,
                              [{missing, Key} | TxnConditions],
                              [{set, Key, Value} | TxnUpdates]);
        {set, Key, Value} ->
            multi_to_txn_loop(Updates,
                              TxnConditions,
                              [{set, Key, Value} | TxnUpdates]);
        {set, Key, Value, Revision} ->
            multi_to_txn_loop(Updates,
                              [{revision, Key, Revision} | TxnConditions],
                              [{set, Key, Value} | TxnUpdates]);
        {delete, Key} ->
            multi_to_txn_loop(Updates,
                              TxnConditions,
                              [{delete, Key} | TxnUpdates]);
        {delete, Key, Revision} ->
            multi_to_txn_loop(Updates,
                              [{revision, Revision} | TxnConditions],
                              [{delete, Key} | TxnUpdates])
    end.

-type snapshot() :: #{key() => {value(), revision()}}.
-type txn_ops() :: [txn_op()].
-type txn_op() :: {set, key(), value()}
                | {delete, key()}.
-type txn_options() :: options() | #{retries => non_neg_integer()}.
-type txn_fun(Data) :: fun ((Data) ->
                               {commit, txn_ops()} |
                               {commit, txn_ops(), Extra::any()} |
                               {abort, Result::any()}).
-type txn_result() :: {ok, revision()}
                    | {ok, revision(), Extra::any()}
                    | {error, exceeded_retries}
                    | (Aborted::any()).

-type txn_opaque() ::
        {txn_fast, Table::reference(), TableSeqno::chronicle:seqno()} |
        {txn_slow, snapshot()}.

-spec transaction(name(), [key()], txn_fun(snapshot())) -> txn_result().
transaction(Name, Keys, Fun) ->
    transaction(Name, Keys, Fun, #{}).

-spec transaction(name(), [key()], txn_fun(snapshot()), txn_options()) ->
          txn_result().
transaction(Name, Keys, Fun, Opts) ->
    txn(Name,
        fun (Txn) ->
                Snapshot = txn_get_many(Keys, Txn),
                Fun(Snapshot)
        end, Opts).

-spec txn_get(key(), txn_opaque()) -> get_result().
txn_get(Key, Txn) ->
    case Txn of
        {txn_fast, Table, TableSeqno} ->
            txn_get_from_table(Key, Table, TableSeqno);
        {txn_slow, Map} ->
            txn_get_from_map(Key, Map)
    end.

-spec txn_get_many([key()], txn_opaque()) -> snapshot().
txn_get_many(Keys, Txn) ->
    case Txn of
        {txn_fast, Table, TableSeqno} ->
            txn_get_from_table_many(Keys, Table, TableSeqno);
        {txn_slow, Map} ->
            txn_get_from_map_many(Keys, Map)
    end.

-spec txn(name(), txn_fun(txn_opaque())) -> txn_result().
txn(Name, Fun) ->
    txn(Name, Fun, #{}).

-spec txn(name(), txn_fun(txn_opaque()), txn_options()) -> txn_result().
txn(Name, Fun, Opts) ->
    TRef = start_timeout(get_timeout(Opts)),
    Retries = maps:get(retries, Opts, ?TXN_RETRIES),
    txn_loop(Name, Fun, Opts, TRef, Retries).

txn_loop(Name, Fun, Opts, TRef, Retries) ->
    case prepare_txn(Name, Fun, TRef, Opts) of
        {ok, {TxnResult, Conditions, _Revision}} ->
            case TxnResult of
                {commit, Updates} ->
                    txn_loop_commit(Name, Fun, Opts, TRef, Retries,
                                    Updates, Conditions, no_extra);
                {commit, Updates, Extra} ->
                    txn_loop_commit(Name, Fun, Opts, TRef, Retries,
                                    Updates, Conditions, {extra, Extra});
                {abort, Result} ->
                    Result
            end;
        {error, {raised, T, E, Stack}} ->
            erlang:raise(T, E, Stack)
    end.

txn_loop_commit(Name, Fun, Opts, TRef, Retries, Updates, Conditions, Extra) ->
    case submit_transaction(Name, Conditions, Updates, TRef, Opts) of
        {ok, Revision} = Ok ->
            case Extra of
                no_extra ->
                    Ok;
                {extra, ActualExtra} ->
                    {ok, Revision, ActualExtra}
            end;
        {error, {conflict, Revision}} ->
            case Retries > 0 of
                true ->
                    %% If read_own_writes is true, the following should
                    %% normally hit the fast path. So it shouldn't be a big
                    %% deal that we're doing this "redundant" sync.
                    chronicle_rsm:sync_revision(Name, Revision, TRef),

                    %% Don't trigger extra synchronization when retrying.
                    NewOpts = Opts#{read_consistency => local},

                    txn_loop(Name, Fun, NewOpts,
                             TRef, Retries - 1);
                false ->
                    {error, exceeded_retries}
            end;
        Other ->
            Other
    end.

prepare_txn(Name, Fun, TRef, Opts) ->
    optimistic_query(Name, {prepare_txn, Fun}, TRef, Opts,
                     fun () ->
                             prepare_txn_fast(Name, Fun)
                     end).

prepare_txn_fast(Name, Fun) ->
    case get_kv_table(Name) of
        {ok, Table} ->
            {_, TableSeqno} = Revision = get_revision(Name),

            try
                Result = txn_with_conditions(
                           Revision,
                           fun () ->
                                   Fun({txn_fast, Table, TableSeqno})
                           end),
                {ok, Result}
            catch
                throw:use_slow_path ->
                    use_slow_path
            end;
        {error, no_table} ->
            use_slow_path
    end.

submit_transaction(Name, Conditions, Updates, Opts) ->
    submit_transaction(Name, Conditions, Updates, get_timeout(Opts), Opts).

submit_transaction(Name, Conditions, Updates, Timeout, Opts) ->
    submit_command(Name, {transaction, Conditions, Updates}, Timeout, Opts).

-type ro_txn_fun() :: fun ((txn_opaque()) -> TxnResult::any()).
-type ro_txn_result() :: {ok, {TxnResult::any(), revision()}}.

-spec ro_txn(name(), ro_txn_fun()) -> ro_txn_result().
ro_txn(Name, Fun) ->
    ro_txn(Name, Fun, #{}).

-spec ro_txn(name(), ro_txn_fun(), options()) -> ro_txn_result().
ro_txn(Name, Fun, Opts) ->
    TRef = start_timeout(get_timeout(Opts)),
    case prepare_txn(Name, Fun, TRef, Opts) of
        {ok, {TxnResult, _Conditions, Revision}} ->
            {ok, {TxnResult, Revision}};
        {error, {raised, T, E, Stack}} ->
            erlang:raise(T, E, Stack)
    end.

-type get_result() :: {ok, {value(), revision()}} | {error, not_found}.

-spec get(name(), key()) -> get_result().
get(Name, Key) ->
    get(Name, Key, #{}).

-spec get(name(), key(), options()) -> get_result().
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

-type rewrite_fun_result() ::
        {update, NewValue::value()} |
        {update, NewKey::key(), NewValue::value} |
        keep |
        delete.
-type rewrite_fun() :: fun ((key(), value()) -> rewrite_fun_result()).

-spec rewrite(name(), rewrite_fun()) -> op_result().
rewrite(Name, Fun) ->
    rewrite(Name, Fun, #{}).

-spec rewrite(name(), rewrite_fun(), options()) -> op_result().
rewrite(Name, Fun, Opts) ->
    TRef = start_timeout(get_timeout(Opts)),
    case submit_query(Name, {rewrite, Fun}, TRef, Opts) of
        {ok, Conditions, Updates} ->
            submit_transaction(Name, Conditions, Updates, TRef, Opts);
        {error, _} = Error ->
            Error
    end.

-type get_snapshot_result() :: {ok, {snapshot(), revision()}}.

-spec get_full_snapshot(name()) -> get_snapshot_result().
get_full_snapshot(Name) ->
    get_full_snapshot(Name, #{}).

-spec get_full_snapshot(name(), options()) -> get_snapshot_result().
get_full_snapshot(Name, Opts) ->
    submit_query(Name, get_full_snapshot, get_timeout(Opts), Opts).

-spec get_snapshot(name(), [key()]) -> get_snapshot_result().
get_snapshot(Name, Keys) ->
    get_snapshot(Name, Keys, #{}).

-spec get_snapshot(name(), [key()], options()) -> get_snapshot_result().
get_snapshot(Name, Keys, Opts) ->
    optimistic_query(
      Name, {get_snapshot, Keys}, get_timeout(Opts), Opts,
      fun () ->
              get_snapshot_fast_path(Name, Keys)
      end).

get_snapshot_fast_path(Name, Keys) ->
    case get_kv_table(Name) of
        {ok, Table} ->
            {_, TableSeqno} = Revision = get_revision(Name),
            case get_snapshot_fast_path_loop(Table, TableSeqno, Keys, #{}) of
                {ok, Snapshot} ->
                    {ok, {Snapshot, Revision}};
                use_slow_path ->
                    use_slow_path
            end;
        {error, no_table} ->
            use_slow_path
    end.

get_snapshot_fast_path_loop(_Table, _TableSeqno, [], Snapshot) ->
    {ok, Snapshot};
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

-spec get_revision(name()) -> revision().
get_revision(Name) ->
    chronicle_rsm:get_local_revision(Name).

-spec sync(name()) -> ok.
sync(Name) ->
    sync(Name, ?DEFAULT_TIMEOUT).

-spec sync(name(), timeout()) -> ok.
sync(Name, Timeout) ->
    chronicle_rsm:sync(Name, Timeout).

-spec sync(name(), read_consistency(), timeout()) -> ok.
sync(Name, Type, Timeout) ->
    case Type of
        local ->
            ok;
        quorum ->
            chronicle_rsm:sync(Name, Timeout)
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

state_enter(_, _, _, Data) ->
    {ok, Data}.

apply_snapshot(SnapshotRevision, SnapshotState, _OldRevision, OldState,
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

    %% TODO: This is a temprorary kludge making ns_server's life a bit
    %% easier. Get rid of it.
    notify_snapshot_diff(SnapshotRevision, SnapshotState, OldState, Data),

    notify_snapshot_installed(SnapshotRevision, Data),
    {ok, Data#data{kv_table = KvTable}}.

handle_query({rewrite, Fun}, StateRevision, State, Data) ->
    handle_rewrite(Fun, StateRevision, State, Data);
handle_query({get, Key}, _StateRevision, State, Data) ->
    handle_get(Key, State, Data);
handle_query(get_full_snapshot, StateRevision, State, Data) ->
    handle_get_full_snapshot(StateRevision, State, Data);
handle_query({get_snapshot, Keys}, StateRevision, State, Data) ->
    handle_get_snapshot(Keys, StateRevision, State, Data);
handle_query({prepare_txn, Fun}, StateRevision, State, Data) ->
    handle_prepare_txn(Fun, StateRevision, State, Data).

apply_command(_, {add, Key, Value}, Revision, StateRevision, State, Data) ->
    apply_add(Key, Value, Revision, StateRevision, State, Data);
apply_command(_, {set, Key, Value, ExpectedRevision}, Revision,
              StateRevision, State, Data) ->
    apply_set(Key, Value, ExpectedRevision,
              Revision, StateRevision, State, Data);
apply_command(_, {delete, Key, ExpectedRevision}, Revision,
              StateRevision, State, Data) ->
    apply_delete(Key, ExpectedRevision, Revision, StateRevision, State, Data);
apply_command(_, {transaction, Conditions, Updates}, Revision,
              StateRevision, State, Data) ->
    apply_transaction(Conditions, Updates,
                      Revision, StateRevision, State, Data).

handle_config(ConfigEntry, Revision, _StateRevision, State, Data) ->
    #log_entry{value = Config} = ConfigEntry,
    case chronicle_config:get_branch_opaque(Config) of
        {ok, Opaque} ->
            NewState = handle_update('$failover_opaque',
                                     Opaque, Revision, State, Data),
            {ok, NewState, Data, []};
        no_branch ->
            {ok, State, Data, []}
    end.

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
    ok = handle_read_consistency(Name, TRef, Opts),
    case Body() of
        {ok, _} = Ok ->
            Ok;
        {error, _} = Error ->
            Error;
        use_slow_path ->
            chronicle_rsm:query(Name, Query, TRef)
    end.

submit_query(Name, Query, Timeout, Opts) ->
    TRef = start_timeout(Timeout),
    ok = handle_read_consistency(Name, TRef, Opts),
    chronicle_rsm:query(Name, Query, TRef).

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

handle_get_snapshot(Keys, StateRevision, State, Data) ->
    Snapshot = maps:with(Keys, State),
    {reply, {ok, {Snapshot, StateRevision}}, Data}.

handle_prepare_txn(Fun, StateRevision, State, Data) ->
    Result =
        try txn_with_conditions(StateRevision,
                                fun () -> Fun({txn_slow, State}) end)
        of R -> {ok, R}
        catch
            T:E:Stack ->
                {error, {raised, T, E, sanitize_stacktrace(Stack)}}
        end,

    {reply, Result, Data}.

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

key_updated_event(Key, Revision, Value) ->
    {{key, Key}, Revision, {updated, Value}}.

key_deleted_event(Key, Revision) ->
    {{key, Key}, Revision, deleted}.

notify_deleted(Key, Revision, Data) ->
    notify(key_deleted_event(Key, Revision), Data).

notify_updated(Key, Revision, Value, Data) ->
    notify(key_updated_event(Key, Revision, Value), Data).

notify_snapshot_installed(Revision, Data) ->
    notify({snapshot_installed, Revision}, Data).

notify_snapshot_diff(Revision, NewSnapshot, OldSnapshot,
                     #data{initialized = true, event_mgr = Mgr}) ->
    chronicle_utils:maps_foreach(
      fun (Key, {Value, Rev}) ->
              Notify =
                  case maps:find(Key, OldSnapshot) of
                      {ok, OldRev} ->
                          Rev =:= OldRev;
                      error ->
                          true
                  end,

              case Notify of
                  true ->
                      %% Note that the snapshot revision is used here instead
                      %% of the revisions of the individual keys. This to
                      %% ensure that revisions are aways in monotonically
                      %% increasing order.
                      Event = key_updated_event(Key, Revision, Value),
                      gen_event:notify(Mgr, Event);
                  false ->
                      ok
              end
      end, NewSnapshot),

    chronicle_utils:maps_foreach(
      fun (Key, _) ->
              case maps:is_key(Key, NewSnapshot) of
                  true ->
                      ok;
                  false ->
                      Event = key_deleted_event(Key, Revision),
                      gen_event:notify(Mgr, Event)
              end
      end, OldSnapshot);
notify_snapshot_diff(_, _, _, _) ->
    ok.

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

txn_get_from_table(Key, Table, TableSeqno) ->
    case get_from_kv_table(Table, Key) of
        {ok, {_Value, {_HistoryId, Seqno} = Revision}} = Ok ->
            case Seqno =< TableSeqno of
                true ->
                    txn_require_revision(Key, Revision),
                    Ok;
                false ->
                    throw(use_slow_path)
            end;
        {error, Error} when Error =:= no_table;
                            Error =:= not_found ->
            throw(use_slow_path)
    end.

txn_get_from_map(Key, Map) ->
    case maps:find(Key, Map) of
        {ok, {_, Revision}} = Ok ->
            txn_require_revision(Key, Revision),
            Ok;
        error ->
            txn_require_missing(Key),
            {error, not_found}
    end.

txn_get_from_map_many(Keys, Map) ->
    lists:foldl(
      fun (Key, Acc) ->
              case txn_get_from_map(Key, Map) of
                  {ok, ValueRev} ->
                      Acc#{Key => ValueRev};
                  {error, not_found} ->
                      Acc
              end
      end, #{}, Keys).

txn_get_from_table_many(Keys, Table, TableSeqno) ->
    lists:foldl(
      fun (Key, Acc) ->
              {ok, ValueRev} = txn_get_from_table(Key, Table, TableSeqno),
              Acc#{Key => ValueRev}
      end, #{}, Keys).

-define(TXN_CONDITIONS, '$txn_conditions').

txn_add_condition(Condition) ->
    Conditions = erlang:get(?TXN_CONDITIONS),
    erlang:put(?TXN_CONDITIONS, [Condition | Conditions]),
    ok.

txn_require_revision(Key, Revision) ->
    txn_add_condition({revision, Key, Revision}).

txn_require_missing(Key) ->
    txn_add_condition({missing, Key}).

txn_init_conditions() ->
    case erlang:put(?TXN_CONDITIONS, []) of
        undefined ->
            ok;
        false ->
            error(nested_transactions_detected)
    end.

txn_take_conditions() ->
    erlang:erase(?TXN_CONDITIONS).

txn_with_conditions(Revision, Body) ->
    txn_init_conditions(),
    try Body() of
        Result ->
            {Result, txn_take_conditions(), Revision}
    after
        _ = txn_take_conditions()
    end.
