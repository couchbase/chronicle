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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(chronicle_utils, [start_timeout/1,
                          sanitize_stacktrace/1]).

-export([sanitize_state/2, sanitize_command/2]).
-export([format_state/1]).

%% APIs
-export([event_manager/1]).
-export([add/3, add/4, add/5]).
-export([set/3, set/4, set/5, set/6]).
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
-export([txn_get/2, txn_get_key_opts/2, txn_get_many/2]).
-export([get_txn_default_timeout/0]).

%% internal
-export([expire/3]).

%% for use by expire handlers
-export([expire_updates/2]).

-export_type([key/0, full_snapshot/0]).

%% callbacks
-export([specs/2,
         init/2, post_init/3, state_enter/4,
         apply_snapshot/5, apply_command/6,
         handle_query/4, handle_info/4,
         handle_config/5,
         terminate/4]).

-define(DEFAULT_TIMEOUT, 15000).
-define(DEFAULT_RETRIES, 10).
-define(DEFAULT_MAX_TTL, 30 * 24 * 60 * 60 * 1000).
-define(TTL_COMPAT_VERSION, 1).

-type name() :: atom().
-type key() :: any().
-type value() :: any().
-type revision() :: chronicle:revision().
-type revisionreq() :: any | revision().

-type read_consistency() :: local | quorum.
-type write_options() :: #{timeout => timeout()}.
-type read_options() :: #{timeout => timeout(),
                          read_consistency => read_consistency()}.
%% ttl is the minimum time in milliseconds that a key lives for, up to the
%% max_ttl setting. Refused until the cluster compat version is at least 1
-type key_options() :: #{ttl => pos_integer()}.
-type op_result() :: {ok, revision()}
                   | {error, {conflict, revision()}}.

-record(data, {name,
               meta_table,
               kv_table,
               event_mgr,
               initialized,
               %% We track if we are the leader node and only pass ttl writes
               %% for tracking if true
               is_leader = false}).

-ifndef(TEST).
-type event_manager_name() :: atom().
-else.
-type event_manager_name() :: any().
-endif.

sanitize_state(Fun, State) ->
    maps:map(
      fun (Key, Entry) ->
              setelement(1, Entry, Fun(Key, element(1, Entry)))
      end, State).

sanitize_command(Fun, Command) ->
    case Command of
        {add, Key, Value} ->
            {add, Key, Fun(Key, Value)};
        {add, Key, Value, KeyOpts} ->
            {add, Key, Fun(Key, Value), KeyOpts};
        {set, Key, Value, ExpectedRevision} ->
            {set, Key, Fun(Key, Value), ExpectedRevision};
        {set, Key, Value, ExpectedRevision, KeyOpts} ->
            {set, Key, Fun(Key, Value), ExpectedRevision, KeyOpts};
        {transaction, Conditions, Updates} ->
            {transaction, Conditions,
             lists:map(
               fun (Update) ->
                       case Update of
                           {set, Key, Value} ->
                               {set, Key, Fun(Key, Value)};
                           {set, Key, Value, KeyOpts} ->
                               {set, Key, Fun(Key, Value), KeyOpts};
                           _ ->
                               Update
                       end
               end, Updates)};
        _ ->
            Command
    end.

format_state(State) ->
    chronicle_dump:raw(maps:to_list(State)).

-spec event_manager(name()) -> event_manager_name().
event_manager(Name) ->
    ?SERVER_NAME(event_manager_name(Name)).

-spec add(name(), key(), value()) -> op_result().
add(Name, Key, Value) ->
    add(Name, Key, Value, #{}).

-spec add(name(), key(), value(), write_options()) -> op_result().
add(Name, Key, Value, Opts) ->
    add(Name, Key, Value, #{}, Opts).

-spec add(name(), key(), value(), key_options(), write_options()) ->
          op_result().
add(Name, Key, Value, KeyOpts, Opts) ->
    submit_command(Name, with_key_opts(Name, {add, Key, Value}, KeyOpts),
                   get_timeout(Name, Opts)).

-spec set(name(), key(), value()) -> op_result().
set(Name, Key, Value) ->
    set(Name, Key, Value, any).

-spec set(name(), key(), value(), revisionreq()) -> op_result().
set(Name, Key, Value, ExpectedRevision) ->
    set(Name, Key, Value, ExpectedRevision, #{}).

-spec set(name(), key(), value(), revisionreq(), write_options()) ->
          op_result().
set(Name, Key, Value, ExpectedRevision, Opts) ->
    set(Name, Key, Value, ExpectedRevision, #{}, Opts).

-spec set(name(), key(), value(), revisionreq(), key_options(),
          write_options()) ->
          op_result().
set(Name, Key, Value, ExpectedRevision, KeyOpts, Opts) ->
    submit_command(Name,
                   with_key_opts(Name, {set, Key, Value, ExpectedRevision},
                                 KeyOpts),
                   get_timeout(Name, Opts)).

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

-spec delete(name(), key(), revisionreq(), write_options()) -> op_result().
delete(Name, Key, ExpectedRevision, Opts) ->
    submit_command(Name,
                   {delete, Key, ExpectedRevision},
                   get_timeout(Name, Opts)).

-type multi_ops() :: [multi_op()].
-type multi_op() :: {add, key(), value()}
                  | {add, key(), value(), key_options()}
                  | {set, key(), value()}
                  | {set, key(), value(), key_options()}
                  | {set, key(), value(), revisionreq()}
                  | {set, key(), value(), revisionreq(), key_options()}
                  | {delete, key()}
                  | {delete, key(), revisionreq()}.

-spec multi(name(), multi_ops()) -> op_result().
multi(Name, Updates) ->
    multi(Name, Updates, #{}).

-spec multi(name(), multi_ops(), write_options()) -> op_result().
multi(Name, Updates, Opts) ->
    {TxnConditions, TxnUpdates} = multi_to_txn(Updates),
    check_updates_key_opts(Name, TxnUpdates),
    submit_transaction(Name, TxnConditions, TxnUpdates,
                       get_timeout(Name, Opts)).

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
        {add, Key, Value, KeyOpts} ->
            multi_to_txn_loop(Updates,
                              [{missing, Key} | TxnConditions],
                              [{set, Key, Value, KeyOpts} | TxnUpdates]);
        {set, Key, Value} ->
            multi_to_txn_loop(Updates,
                              TxnConditions,
                              [{set, Key, Value} | TxnUpdates]);
        {set, Key, Value, KeyOpts} when is_map(KeyOpts) ->
            multi_to_txn_loop(Updates,
                              TxnConditions,
                              [{set, Key, Value, KeyOpts} | TxnUpdates]);
        {set, Key, Value, Revision} ->
            multi_to_txn_loop(Updates,
                              [{revision, Key, Revision} | TxnConditions],
                              [{set, Key, Value} | TxnUpdates]);
        {set, Key, Value, Revision, KeyOpts} ->
            multi_to_txn_loop(Updates,
                              [{revision, Key, Revision} | TxnConditions],
                              [{set, Key, Value, KeyOpts} | TxnUpdates]);
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
%% Entries carry key options only when the key has some
-type full_snapshot() :: #{key() => {value(), revision()}
                                   | {value(), revision(), key_options()}}.
-type txn_ops() :: [txn_op()].
-type txn_op() :: {set, key(), value()}
                | {set, key(), value(), key_options()}
                | {delete, key()}
                | {expire, key()}.
-type txn_options() :: read_options() | #{retries => non_neg_integer()}.
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
    case txn_get_entry(Key, Txn) of
        {ok, Entry} ->
            {ok, value_rev(Entry)};
        {error, not_found} = Error ->
            Error
    end.

-spec txn_get_key_opts(key(), txn_opaque()) ->
          {ok, key_options()} | {error, not_found}.
txn_get_key_opts(Key, Txn) ->
    case txn_get_entry(Key, Txn) of
        {ok, Entry} ->
            {ok, key_opts(Entry)};
        {error, not_found} = Error ->
            Error
    end.

txn_get_entry(Key, Txn) ->
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

-spec get_txn_default_timeout() -> pos_integer().
get_txn_default_timeout() ->
    ?DEFAULT_TIMEOUT.

-spec txn(name(), txn_fun(txn_opaque())) -> txn_result().
txn(Name, Fun) ->
    txn(Name, Fun, #{}).

-spec txn(name(), txn_fun(txn_opaque()), txn_options()) -> txn_result().
txn(Name, Fun, Opts) ->
    BuildTxn = fun (TRef, TxnOpts) ->
                       case prepare_txn(Name, Fun, TRef, TxnOpts) of
                           {ok, {TxnResult, Conditions, _Revision}} ->
                               check_txn_max_ttl(Name, TxnResult),
                               {ok, {TxnResult, Conditions}};
                           Other ->
                               Other
                       end
               end,
    perform_txn(Name, BuildTxn, Opts).

perform_txn(Name, BuildTxn, Opts) ->
    TRef = start_timeout(get_timeout(Name, Opts)),
    Retries = get_retries(Name, Opts),
    txn_loop(Name, BuildTxn, Opts, TRef, Retries).

txn_loop(Name, BuildTxn, Opts, TRef, Retries) ->
    case BuildTxn(TRef, Opts) of
        {ok, {TxnResult, Conditions}} ->
            case TxnResult of
                {commit, Updates} ->
                    txn_loop_commit(Name, BuildTxn, Opts, TRef, Retries,
                                    Updates, Conditions, no_extra);
                {commit, Updates, Extra} ->
                    txn_loop_commit(Name, BuildTxn, Opts, TRef, Retries,
                                    Updates, Conditions, {extra, Extra});
                {abort, Result} ->
                    Result
            end;
        {error, {raised, T, E, Stack}} ->
            erlang:raise(T, E, Stack)
    end.

txn_loop_commit(Name, BuildTxn, Opts, TRef,
                Retries, Updates, Conditions, Extra) ->
    case submit_transaction(Name, Conditions, Updates, TRef) of
        {ok, Revision} = Ok ->
            case Extra of
                no_extra ->
                    Ok;
                {extra, ActualExtra} ->
                    {ok, Revision, ActualExtra}
            end;
        {error, {conflict, _Revision}} ->
            case Retries > 0 of
                true ->
                    %% Don't trigger extra synchronization when retrying.
                    NewOpts = Opts#{read_consistency => local},

                    txn_loop(Name, BuildTxn, NewOpts,
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

submit_transaction(Name, Conditions, Updates, Timeout) ->
    submit_command(Name, {transaction, Conditions, Updates}, Timeout).

-type ro_txn_fun() :: fun ((txn_opaque()) -> TxnResult::any()).
-type ro_txn_result() :: {ok, {TxnResult::any(), revision()}}.

-spec ro_txn(name(), ro_txn_fun()) -> ro_txn_result().
ro_txn(Name, Fun) ->
    ro_txn(Name, Fun, #{}).

-spec ro_txn(name(), ro_txn_fun(), read_options()) -> ro_txn_result().
ro_txn(Name, Fun, Opts) ->
    TRef = start_timeout(get_timeout(Name, Opts)),
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

-spec get(name(), key(), read_options()) -> get_result().
get(Name, Key, Opts) ->
    optimistic_query(
      Name, {get, Key}, get_timeout(Name, Opts), Opts,
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

-spec rewrite(name(), rewrite_fun()) -> txn_result().
rewrite(Name, Fun) ->
    rewrite(Name, Fun, #{}).

-spec rewrite(name(), rewrite_fun(), txn_options()) -> txn_result().
rewrite(Name, Fun, Opts) ->
    BuildTxn = fun (TRef, TxnOpts) ->
                       submit_query(Name, {rewrite, Fun}, TRef, TxnOpts)
               end,
    perform_txn(Name, BuildTxn, Opts).

-type get_snapshot_result() :: {ok, {snapshot(), revision()}}.
-type get_full_snapshot_result() :: {ok, {full_snapshot(), revision()}}.

-spec get_full_snapshot(name()) -> get_full_snapshot_result().
get_full_snapshot(Name) ->
    get_full_snapshot(Name, #{}).

-spec get_full_snapshot(name(), read_options()) ->
          get_full_snapshot_result().
get_full_snapshot(Name, Opts) ->
    submit_query(Name, get_full_snapshot, get_timeout(Name, Opts), Opts).

-spec get_snapshot(name(), [key()]) -> get_snapshot_result().
get_snapshot(Name, Keys) ->
    get_snapshot(Name, Keys, #{}).

-spec get_snapshot(name(), [key()], read_options()) ->
          get_snapshot_result().
get_snapshot(Name, Keys, Opts) ->
    optimistic_query(
      Name, {get_snapshot, Keys}, get_timeout(Name, Opts), Opts,
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
    sync(Name, get_opt_default(Name, timeout, ?DEFAULT_TIMEOUT)).

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
    ExpirySpec = #{id => chronicle_kv_expiry,
                   start => {chronicle_kv_expiry, start_link, [Name]},
                   restart => permanent,
                   shutdown => brutal_kill,
                   type => worker},
    [Spec, ExpirySpec].

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

state_enter(leader, _, State, #data{name = Name} = Data) ->
    chronicle_kv_expiry:activate(Name, ttl_keys(State)),
    {ok, Data#data{is_leader = true}};
state_enter(_, _, _, #data{name = Name} = Data) ->
    chronicle_kv_expiry:deactivate(Name),
    {ok, Data#data{is_leader = false}}.

apply_snapshot(SnapshotRevision, SnapshotState, _OldRevision, OldState,
               #data{name = Name,
                     kv_table = OldKvTable,
                     initialized = Initialized,
                     is_leader = IsLeader} = Data) ->
    KvTable = create_kv_table(Name),
    chronicle_utils:maps_foreach(
      fun (Key, Entry) ->
              true = ets:insert_new(KvTable, {Key, Entry})
      end, SnapshotState),

    case IsLeader of
        true ->
            chronicle_kv_expiry:activate(Name, ttl_keys(SnapshotState));
        false ->
            ok
    end,

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
    handle_prepare_txn(Fun, StateRevision, State, Data);
handle_query(_, _, _, Data) ->
    {reply, {error, unknown_query}, Data}.

apply_command(_, {add, Key, Value}, Revision, StateRevision, State, Data) ->
    apply_add(Key, Value, #{}, Revision, StateRevision, State, Data);
apply_command(_, {add, Key, Value, KeyOpts}, Revision,
              StateRevision, State, Data) ->
    apply_add(Key, Value, KeyOpts, Revision, StateRevision, State, Data);
apply_command(_, {set, Key, Value, ExpectedRevision}, Revision,
              StateRevision, State, Data) ->
    apply_set(Key, Value, #{}, ExpectedRevision,
              Revision, StateRevision, State, Data);
apply_command(_, {set, Key, Value, ExpectedRevision, KeyOpts}, Revision,
              StateRevision, State, Data) ->
    apply_set(Key, Value, KeyOpts, ExpectedRevision,
              Revision, StateRevision, State, Data);
apply_command(_, {delete, Key, ExpectedRevision}, Revision,
              StateRevision, State, Data) ->
    apply_delete(Key, ExpectedRevision, Revision, StateRevision, State, Data);
apply_command(_, {transaction, Conditions, Updates}, Revision,
              StateRevision, State, Data) ->
    apply_transaction(Conditions, Updates,
                      Revision, StateRevision, State, Data);
apply_command(_, _, _, _, State, Data) ->
    {reply, {error, unknown_command}, State, Data}.

handle_config(ConfigEntry, Revision, _StateRevision, State, Data) ->
    #log_entry{value = Config} = ConfigEntry,
    case chronicle_config:get_branch_opaque(Config) of
        {ok, Opaque} ->
            NewState = handle_update('$failover_opaque', Opaque,
                                     #{}, Revision, State, Data),
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
get_opt(RSMName, OptName, Opts, Default) ->
    case maps:find(OptName, Opts) of
        {ok, Value} ->
            Value;
        error ->
            get_opt_default(RSMName, OptName, Default)
    end.

get_opt_default(RSMName, OptName, Default) ->
    chronicle_settings:get({chronicle_kv, RSMName, OptName}, Default).

get_timeout(Name, Opts) ->
    get_opt(Name, timeout, Opts, ?DEFAULT_TIMEOUT).

get_retries(Name, Opts) ->
    get_opt(Name, retries, Opts, ?DEFAULT_RETRIES).

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

with_key_opts(Name, Command, KeyOpts) ->
    check_key_opts(Name, KeyOpts),

    %% Commands without key options keep their original form, so old nodes
    %% can still apply them
    case map_size(KeyOpts) of
        0 ->
            Command;
        _ ->
            erlang:append_element(Command, KeyOpts)
    end.

check_key_opts(Name, KeyOpts) ->
    case valid_key_opts(KeyOpts) of
        true ->
            ok;
        false ->
            error({bad_key_opts, KeyOpts})
    end,
    check_max_ttl(Name, KeyOpts).

check_updates_key_opts(Name, Updates) ->
    lists:foreach(
      fun ({set, _Key, _Value, KeyOpts}) ->
              check_key_opts(Name, KeyOpts);
          (_) ->
              ok
      end, Updates).

valid_key_opts(KeyOpts) when is_map(KeyOpts) ->
    lists:all(fun valid_key_opt/1, maps:to_list(KeyOpts));
valid_key_opts(_) ->
    false.

valid_key_opt({ttl, TTL}) when is_integer(TTL), TTL > 0 ->
    true;
valid_key_opt(_) ->
    false.

%% Checked before submitting rather than on apply, as the setting may differ
%% between nodes
check_max_ttl(Name, #{ttl := TTL} = KeyOpts) ->
    case TTL =< get_opt_default(Name, max_ttl, ?DEFAULT_MAX_TTL) of
        true ->
            ok;
        false ->
            error({bad_key_opts, KeyOpts})
    end;
check_max_ttl(_Name, _KeyOpts) ->
    ok.

check_updates_max_ttl(Name, Updates) when is_list(Updates) ->
    lists:foreach(
      fun ({set, _Key, _Value, KeyOpts}) ->
              check_max_ttl(Name, KeyOpts);
          (_) ->
              ok
      end, Updates);
check_updates_max_ttl(_Name, _Updates) ->
    ok.

check_txn_max_ttl(Name, TxnResult) ->
    case TxnResult of
        {commit, Updates} ->
            check_updates_max_ttl(Name, Updates);
        {commit, Updates, _Extra} ->
            check_updates_max_ttl(Name, Updates);
        {abort, _} ->
            ok
    end.

-type expire_handler() :: fun (([{key(), revision()}], txn_opaque()) ->
                                     txn_ops()).

%% Runs on the leader. The handler builds the updates so that an RSM can
%% change other keys in the same transaction as the expiry
-spec expire(name(), [{key(), revision()}], timeout()) -> txn_result().
expire(Name, Keys, Timeout) ->
    Handler = get_expire_handler(Name),
    txn(Name,
        fun (Txn) ->
                case Handler(Keys, Txn) of
                    [] ->
                        {abort, {ok, nothing_to_expire}};
                    Updates ->
                        {commit, Updates}
                end
        end, #{timeout => Timeout}).

-spec get_expire_handler(name()) -> expire_handler().
get_expire_handler(Name) ->
    case chronicle_env:kv_expire_handler(Name) of
        {ok, Handler} ->
            Handler;
        undefined ->
            fun expire_updates/2;
        {error, Error} ->
            error(Error)
    end.

%% Expires the keys that haven't been rewritten since they were picked
-spec expire_updates([{key(), revision()}], txn_opaque()) -> txn_ops().
expire_updates(Keys, Txn) ->
    lists:filtermap(
      fun ({Key, Revision}) ->
              case txn_get(Key, Txn) of
                  {ok, {_, Revision}} ->
                      {true, {expire, Key}};
                  _ ->
                      false
              end
      end, Keys).

submit_command(Name, Command, Timeout) ->
    check_command_supported(Command),
    TRef = start_timeout(Timeout),
    case chronicle_rsm:command(Name, Command, TRef) of
        {error, {raise, Error}} ->
            error(Error);
        Other ->
            Other
    end.

%% Old nodes reject ttl commands whilst new nodes apply them, so they are
%% refused until the cluster compat version shows every node supports them
check_command_supported(Command) ->
    case is_ttl_command(Command) of
        true ->
            check_ttl_supported();
        false ->
            ok
    end.

is_ttl_command({add, _Key, _Value, _KeyOpts}) ->
    true;
is_ttl_command({set, _Key, _Value, _ExpectedRevision, _KeyOpts}) ->
    true;
is_ttl_command({transaction, _Conditions, Updates}) when is_list(Updates) ->
    lists:any(fun is_ttl_update/1, Updates);
is_ttl_command(_Command) ->
    false.

is_ttl_update({set, _Key, _Value, _KeyOpts}) ->
    true;
is_ttl_update({expire, _Key}) ->
    true;
is_ttl_update(_Update) ->
    false.

check_ttl_supported() ->
    #metadata{committed_config = ConfigEntry} = chronicle_agent:get_metadata(),
    Version = chronicle_config:get_compat_version(ConfigEntry#log_entry.value),
    case Version >= ?TTL_COMPAT_VERSION of
        true ->
            ok;
        false ->
            error({unsupported_compat_version, Version, ?TTL_COMPAT_VERSION})
    end.

handle_rewrite(Fun, StateRevision, State, Data) ->
    Reply =
        try build_rewrite_updates(Fun, State) of
            Updates ->
                Conditions = [{state_revision, StateRevision}],
                {ok, {{commit, Updates}, Conditions}}
        catch
            T:E:Stack ->
                {error, {raised, T, E, sanitize_stacktrace(Stack)}}
        end,

    {reply, Reply, Data}.

build_rewrite_updates(Fun, State) ->
    maps:fold(
      fun (Key, Entry, Acc) ->
              {Value, _Rev} = value_rev(Entry),
              KeyOpts = key_opts(Entry),
              case Fun(Key, Value) of
                  {update, NewValue} ->
                      [set_update(Key, NewValue, KeyOpts) | Acc];
                  {update, NewKey, NewValue} ->
                      case Key =:= NewKey of
                          true ->
                              [set_update(Key, NewValue, KeyOpts) | Acc];
                          false ->
                              [{delete, Key},
                               set_update(NewKey, NewValue, KeyOpts) | Acc]
                      end;
                  keep ->
                      Acc;
                  delete ->
                      [{delete, Key} | Acc]
              end
      end, [], State).

%% Preserve key options so that a rewrite doesn't drop a ttl
set_update(Key, Value, KeyOpts) when map_size(KeyOpts) =:= 0 ->
    {set, Key, Value};
set_update(Key, Value, KeyOpts) ->
    {set, Key, Value, KeyOpts}.

handle_get(Key, State, Data) ->
    Reply =
        case maps:find(Key, State) of
            {ok, Entry} ->
                {ok, value_rev(Entry)};
            error ->
                {error, not_found}
        end,

    {reply, Reply, Data}.

handle_get_full_snapshot(StateRevision, State, Data) ->
    {reply, {ok, {State, StateRevision}}, Data}.

handle_get_snapshot(Keys, StateRevision, State, Data) ->
    Snapshot = maps:map(fun (_, Entry) -> value_rev(Entry) end,
                        maps:with(Keys, State)),
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

apply_add(Key, Value, KeyOpts, Revision, StateRevision, State, Data) ->
    case check_condition({missing, Key}, Revision, StateRevision, State) of
        ok ->
            NewState =
                handle_update(Key, Value, KeyOpts, Revision, State, Data),
            Reply = {ok, Revision},
            {reply, Reply, NewState, Data};
        {error, _} = Error ->
            {reply, Error, State, Data}
    end.

apply_set(Key, Value, KeyOpts, ExpectedRevision,
          Revision, StateRevision, State, Data) ->
    case check_condition({revision, Key, ExpectedRevision},
                         Revision, StateRevision, State) of
        ok ->
            NewState =
                handle_update(Key, Value, KeyOpts, Revision, State, Data),
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
    case check_transaction(Conditions, Updates,
                           Revision, StateRevision, State) of
        ok ->
            NewState =
                apply_transaction_updates(Updates, Revision, State, Data),
            Reply = {ok, Revision},
            {reply, Reply, NewState, Data};
        {error, _} = Error ->
            {reply, Error, State, Data}
    end.

check_transaction(Conditions, Updates, Revision, StateRevision, State) ->
    case check_transaction_updates(Updates) of
        ok ->
            check_transaction_conditions(Conditions,
                                         Revision, StateRevision, State);
        {error, _} = Error ->
            Error
    end.

check_transaction_updates(Updates) ->
    Valid =
        case is_list(Updates) of
            true ->
                lists:all(
                  fun (Update) ->
                          case Update of
                              {delete, _} ->
                                  true;
                              {set, _, _} ->
                                  true;
                              {set, _, _, KeyOpts} ->
                                  valid_key_opts(KeyOpts);
                              {expire, _} ->
                                  true;
                              _ ->
                                  false
                          end
                  end, Updates);
            false ->
                false
        end,

    case Valid of
        true ->
            ok;
        false ->
            {error, {raise, {bad_updates, Updates}}}
    end.

check_transaction_conditions([], _, _, _) ->
    ok;
check_transaction_conditions([Condition | Rest],
                             Revision, StateRevision, State) ->
    case check_condition(Condition, Revision, StateRevision, State) of
        ok ->
            check_transaction_conditions(Rest, Revision, StateRevision, State);
        {error, _} = Error ->
            Error
    end.

apply_transaction_updates(Updates, Revision, State, Data) ->
    lists:foldl(
      fun (Update, AccState) ->
              case Update of
                  {set, Key, Value} ->
                      handle_update(Key, Value, #{}, Revision, AccState, Data);
                  {set, Key, Value, KeyOpts} ->
                      handle_update(Key, Value, KeyOpts, Revision,
                                    AccState, Data);
                  {delete, Key} ->
                      handle_delete(Key, Revision, AccState, Data);
                  {expire, Key} ->
                      case maps:is_key(Key, AccState) of
                          true ->
                              handle_delete(Key, Revision, AccState, Data);
                          false ->
                              AccState
                      end
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
        {ok, Entry} ->
            {_, OurRevision} = value_rev(Entry),
            OurRevision =:= Revision;
        error ->
            false
    end.

event_manager_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "-events").

%% State entries carry key options only when there are some, so states
%% without them keep the format older nodes understand
make_entry(Value, Revision, KeyOpts) when map_size(KeyOpts) =:= 0 ->
    {Value, Revision};
make_entry(Value, Revision, KeyOpts) ->
    {Value, Revision, KeyOpts}.

value_rev({_Value, _Revision} = ValueRev) ->
    ValueRev;
value_rev({Value, Revision, _KeyOpts}) ->
    {Value, Revision}.

key_opts({_Value, _Revision}) ->
    #{};
key_opts({_Value, _Revision, KeyOpts}) ->
    KeyOpts.

ttl_keys(State) ->
    maps:fold(
      fun (Key, {_, Revision, #{ttl := TTL}}, Acc) ->
              Acc#{Key => {Revision, TTL}};
          (_, _, Acc) ->
              Acc
      end, #{}, State).

handle_update(Key, Value, KeyOpts, Revision, State,
              #data{kv_table = Table} = Data) ->
    Entry = make_entry(Value, Revision, KeyOpts),
    ets:insert(Table, {Key, Entry}),
    notify_updated(Key, Revision, Value, Data),
    case maps:find(ttl, KeyOpts) of
        {ok, TTL} ->
            track_ttl(Key, Revision, TTL, Data);
        error ->
            untrack_ttl(Key, State, Data)
    end,
    maps:put(Key, Entry, State).

handle_delete(Key, Revision, State, #data{kv_table = Table} = Data) ->
    ets:delete(Table, Key),
    notify_deleted(Key, Revision, Data),
    untrack_ttl(Key, State, Data),
    maps:remove(Key, State).

track_ttl(Key, Revision, TTL, #data{is_leader = true, name = Name}) ->
    chronicle_kv_expiry:track(Name, Key, Revision, TTL);
track_ttl(_Key, _Revision, _TTL, _Data) ->
    ok.

%% State is from before the update, as only a key that had a ttl is tracked
untrack_ttl(Key, State, #data{is_leader = true, name = Name}) ->
    case maps:find(Key, State) of
        {ok, {_, _, #{ttl := _}}} ->
            chronicle_kv_expiry:untrack(Name, Key);
        _ ->
            ok
    end;
untrack_ttl(_Key, _State, _Data) ->
    ok.

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
      fun (Key, Entry) ->
              {Value, Rev} = value_rev(Entry),
              Notify =
                  case maps:find(Key, OldSnapshot) of
                      {ok, OldEntry} ->
                          {_OldValue, OldRev} = value_rev(OldEntry),
                          Rev =/= OldRev;
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

-ifdef(TEST).
is_ttl_command_test() ->
    ?assert(is_ttl_command({add, k, v, #{ttl => 1}})),
    ?assert(is_ttl_command({set, k, v, any, #{ttl => 1}})),
    ?assert(is_ttl_command({transaction, [], [{set, k, v, #{}}]})),
    ?assert(is_ttl_command({transaction, [], [{delete, k}, {expire, k}]})),
    ?assertNot(is_ttl_command({add, k, v})),
    ?assertNot(is_ttl_command({set, k, v, any})),
    ?assertNot(is_ttl_command({delete, k, any})),
    ?assertNot(is_ttl_command({transaction, [], [{set, k, v}, {delete, k}]})).

notify_snapshot_diff_test() ->
    Rev = {<<"history">>, 10},
    OldSnapshot = #{unchanged => {a, {<<"history">>, 1}},
                    changed => {b, {<<"history">>, 2}},
                    deleted => {c, {<<"history">>, 3}},
                    ttl_unchanged => {e, {<<"history">>, 4}, #{ttl => 1000}},
                    ttl_changed => {f, {<<"history">>, 5}, #{ttl => 1000}}},
    NewSnapshot = #{unchanged => {a, {<<"history">>, 1}},
                    changed => {b2, {<<"history">>, 8}},
                    added => {d, {<<"history">>, 9}},
                    ttl_unchanged => {e, {<<"history">>, 4}, #{ttl => 1000}},
                    ttl_changed => {f2, {<<"history">>, 7}, #{ttl => 1000}}},

    notify_snapshot_diff(Rev, NewSnapshot, OldSnapshot,
                         #data{initialized = true, event_mgr = self()}),

    ?assertEqual(lists:sort([{{key, changed}, Rev, {updated, b2}},
                             {{key, ttl_changed}, Rev, {updated, f2}},
                             {{key, added}, Rev, {updated, d}},
                             {{key, deleted}, Rev, deleted}]),
                 lists:sort(collect_notify_events())).

collect_notify_events() ->
    receive
        {notify, Event} ->
            [Event | collect_notify_events()]
    after 0 ->
            []
    end.
-endif.

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
    case get_entry_from_kv_table(Table, Key) of
        {ok, Entry} ->
            {ok, value_rev(Entry)};
        {error, _} = Error ->
            Error
    end.

get_entry_from_kv_table(Table, Key) ->
    try ets:lookup(Table, Key) of
        [] ->
            {error, not_found};
        [{_, Entry}] ->
            {ok, Entry}
    catch
        error:badarg ->
            {error, no_table}
    end.

txn_get_from_table(Key, Table, TableSeqno) ->
    case get_entry_from_kv_table(Table, Key) of
        {ok, Entry} = Ok ->
            {_Value, {_HistoryId, Seqno} = Revision} = value_rev(Entry),
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
        {ok, Entry} = Ok ->
            {_, Revision} = value_rev(Entry),
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
                  {ok, Entry} ->
                      Acc#{Key => value_rev(Entry)};
                  {error, not_found} ->
                      Acc
              end
      end, #{}, Keys).

txn_get_from_table_many(Keys, Table, TableSeqno) ->
    lists:foldl(
      fun (Key, Acc) ->
              {ok, Entry} = txn_get_from_table(Key, Table, TableSeqno),
              Acc#{Key => value_rev(Entry)}
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
