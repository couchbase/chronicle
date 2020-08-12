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

-import(chronicle_utils, [with_timeout/2]).

%% TODO: make configurable
-define(DEFAULT_TIMEOUT, 15000).

-record(state, {table}).
-record(data, {event_mgr}).
-record(kv, {key, value, revision}).

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
    with_timeout(
      get_timeout(Opts),
      fun (TRef) ->
              {ok, {Snapshot, Missing}} = get_snapshot(Name, Keys, TRef, Opts),
              case Fun(Snapshot) of
                  {commit, Updates} ->
                      ConditionsMissing = [{missing, Key} || Key <- Missing],
                      Conditions =
                          maps:fold(
                            fun (Key, {_, Revision}, Acc) ->
                                    [{revision, Key, Revision} | Acc]
                            end, ConditionsMissing, Snapshot),
                      submit_transaction(Name, Conditions, Updates, TRef, Opts);
                  {abort, Result} ->
                      Result
              end
      end).

submit_transaction(Name, Conditions, Updates) ->
    submit_transaction(Name, Conditions, Updates, #{}).

submit_transaction(Name, Conditions, Updates, Opts) ->
    submit_transaction(Name, Conditions, Updates, get_timeout(Opts), Opts).

submit_transaction(Name, Conditions, Updates, Timeout, Opts) ->
    submit_command(Name, {transaction, Conditions, Updates}, Timeout, Opts).

get(Name, Key) ->
    get(Name, Key, #{}).

get(Name, Key, Opts) ->
    Timeout = get_timeout(Opts),
    case handle_read_consistency(Name, Timeout, Opts) of
        ok ->
            handle_get(?ETS_TABLE(Name), Key);
        {error, _} = Error ->
            Error
    end.

rewrite(Name, Fun) ->
    rewrite(Name, Fun, #{}).

rewrite(Name, Fun, Opts) ->
    with_timeout(get_timeout(Opts),
                 fun (TRef) ->
                         case submit_query(Name, {rewrite, Fun}, TRef, Opts) of
                             {ok, Conditions, Updates} ->
                                 submit_transaction(Name,
                                                    Conditions, Updates,
                                                    TRef, Opts);
                             {error, _} = Error ->
                                 Error
                         end
                 end).

%% For debugging only.
get_snapshot(Name) ->
    submit_query(Name, get_snapshot, ?DEFAULT_TIMEOUT, #{}).

get_snapshot(Name, Keys) ->
    get_snapshot(Name, Keys, #{}).

get_snapshot(Name, Keys, Opts) ->
    get_snapshot(Name, Keys, get_timeout(Opts), Opts).

get_snapshot(Name, Keys, Timeout, Opts) ->
    %% TODO: consider implementing optimistic snapshots
    submit_query(Name, {get_snapshot, Keys}, Timeout, Opts).

get_revision(Name) ->
    chronicle_rsm:get_local_revision(Name).

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
    EventMgr = event_manager(Name),
    Table = ets:new(?ETS_TABLE(Name),
                    [protected, named_table, {keypos, #kv.key}]),
    {ok, #state{table = Table}, #data{event_mgr = EventMgr}}.

handle_command(_, _StateRevision, _State, Data) ->
    {apply, Data}.

handle_query({rewrite, Fun}, StateRevision, State, Data) ->
    handle_rewrite(Fun, StateRevision, State, Data);
handle_query(get_snapshot, _StateRevision, State, Data) ->
    handle_get_snapshot(State, Data);
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

submit_query(Name, Query, Timeout, Opts) ->
    with_timeout(Timeout,
                 fun (TRef) ->
                         case handle_read_consistency(Name, TRef, Opts) of
                             ok ->
                                 chronicle_rsm:query(Name, Query, TRef);
                             {error, _} = Error ->
                                 Error
                         end
                 end).

handle_read_consistency(Name, Timeout, Opts) ->
    case maps:get(read_consistency, Opts, local) of
        local ->
            ok;
        Consistency
          when Consistency =:= leader;
               Consistency =:= quorum ->
            chronicle_rsm:sync(Name, Consistency, Timeout)
    end.

submit_command(Name, Command, Timeout, Opts) ->
    with_timeout(Timeout,
                 fun (TRef) ->
                         Result = chronicle_rsm:command(Name, Command, TRef),
                         handle_read_own_writes(Name, Result, TRef, Opts),
                         Result
                 end).

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

handle_rewrite(Fun, StateRevision, #state{table = Table}, Data) ->
    Updates =
        ets:foldl(
          fun (#kv{key = Key, value = Value}, Acc) ->
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
          end, [], Table),

    Conditions = [{state_revision, StateRevision}],
    {reply, {ok, Conditions, Updates}, Data}.

handle_get_snapshot(#state{table = Table}, Data) ->
    {reply, ets:tab2list(Table), Data}.

handle_get_snapshot(Keys, #state{table = Table}, Data) ->
    Result =
        lists:foldl(
          fun (Key, {AccSnapshot, AccMissing}) ->
                  case handle_get(Table, Key) of
                      {ok, ValueRev} ->
                          {AccSnapshot#{Key => ValueRev}, AccMissing};
                      {error, not_found} ->
                          {AccSnapshot, [Key | AccMissing]}
                  end
          end, {#{}, []}, Keys),

    {reply, {ok, Result}, Data}.

apply_add(Key, Value, Revision,
          StateRevision, #state{table = Table} = State, Data) ->
    Reply =
        case check_condition({missing, Key}, Revision, StateRevision, Table) of
            ok ->
                handle_update(Key, Value, Revision, State, Data),
                {ok, Revision};
            {error, _} = Error ->
                Error
        end,
    {reply, Reply, State, Data}.

apply_set(Key, Value, ExpectedRevision, Revision,
          StateRevision, #state{table = Table} = State, Data) ->
    Reply =
        case check_condition(
               {revision, Key, ExpectedRevision}, Revision,
               StateRevision, Table) of
            ok ->
                handle_update(Key, Value, Revision, State, Data),
                {ok, Revision};
            {error, _} = Error ->
                Error
        end,

    {reply, Reply, State, Data}.

apply_delete(Key, ExpectedRevision, Revision,
             StateRevision, #state{table = Table} = State, Data) ->
    Reply =
        case check_condition(
               {revision, Key, ExpectedRevision}, Revision,
               StateRevision, Table) of
            ok ->
                handle_delete(Key, Revision, State, Data),
                {ok, Revision};
            {error, _} = Error ->
                Error
        end,
    {reply, Reply, State, Data}.

apply_transaction(Conditions, Updates, Revision, StateRevision, State, Data) ->
    Reply =
        case check_transaction(Conditions, Revision, StateRevision, State) of
            ok ->
                apply_transaction_updates(Updates, Revision, State, Data),
                {ok, Revision};
            {error, _} = Error ->
                Error
        end,
    {reply, Reply, State, Data}.

check_transaction(Conditions, Revision, StateRevision, #state{table = Table}) ->
    check_transaction_loop(Conditions, Revision, StateRevision, Table).

check_transaction_loop([], _, _, _) ->
    ok;
check_transaction_loop([Condition | Rest], Revision, StateRevision, Table) ->
    case check_condition(Condition, Revision, StateRevision, Table) of
        ok ->
            check_transaction_loop(Rest, Revision, StateRevision, Table);
        {error, _} = Error ->
            Error
    end.

apply_transaction_updates(Updates, Revision, State, Data) ->
    lists:foreach(
      fun (Update) ->
              case Update of
                  {set, Key, Value} ->
                      handle_update(Key, Value, Revision, State, Data);
                  {delete, Key} ->
                      handle_delete(Key, Revision, State, Data)
              end
      end, Updates).

check_condition(Condition, Revision, StateRevision, Table) ->
    case condition_holds(Condition, StateRevision, Table) of
        true ->
            ok;
        false ->
            {error, {conflict, Revision}}
    end.

condition_holds({state_revision, Revision}, StateRevision, _Table) ->
    Revision =:= StateRevision;
condition_holds({missing, Key}, _StateRevision, Table) ->
    not ets:member(Table, Key);
condition_holds({revision, _Key, any}, _StateRevision, _Table) ->
    true;
condition_holds({revision, Key, Revision}, _StateRevision, Table) ->
    try ets:lookup_element(Table, Key, #kv.revision) of
        OurRevision when OurRevision =:= Revision ->
            true;
        _ ->
            false
    catch
        error:badarg ->
            %% The key is missing.
            false
    end.

event_manager_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "-events").

handle_get(Table, Key) ->
    case ets:lookup(Table, Key) of
        [] ->
            {error, not_found};
        [#kv{value = Value, revision = Revision}] ->
            {ok, {Value, Revision}}
    end.

handle_update(Key, Value, Revision, #state{table = Table}, Data) ->
    KV = #kv{key = Key, value = Value, revision = Revision},
    ets:insert(Table, KV),
    notify_key(Key, Revision, {updated, Value}, Data).

handle_delete(Key, Revision, #state{table = Table}, Data) ->
    ets:delete(Table, Key),
    notify_key(Key, Revision, deleted, Data).

notify(Event, #data{event_mgr = Mgr}) ->
    gen_event:notify(Mgr, Event).

notify_key(Key, Revision, Event, Data) ->
    notify({{key, Key}, Revision, Event}, Data).
