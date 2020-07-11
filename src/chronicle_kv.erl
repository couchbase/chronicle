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

-record(state, {table}).
-record(data, {event_mgr}).
-record(kv, {key, value, revision}).

event_manager(Name) ->
    ?SERVER_NAME(event_manager_name(Name)).

add(Name, Key, Value) ->
    chronicle_rsm:command(Name, {add, Key, Value}).

set(Name, Key, Value) ->
    chronicle_rsm:command(Name, {set, Key, Value}).

set(Name, Key, Value, ExpectedRevision) ->
    chronicle_rsm:command(Name, {set, Key, Value, ExpectedRevision}).

delete(Name, Key) ->
    chronicle_rsm:command(Name, {delete, Key}).

delete(Name, Key, ExpectedRevision) ->
    chronicle_rsm:command(Name, {delete, Key, ExpectedRevision}).

get(Name, Key) ->
    case ets:lookup(?ETS_TABLE(Name), Key) of
        [] ->
            {error, not_found};
        [#kv{value = Value}] ->
            {ok, Value}
    end.

get_snapshot(Name) ->
    chronicle_rsm:query(Name, get_snapshot).

transaction(Name, Conditions, Updates) ->
    chronicle_rsm:command(Name, {transaction, Conditions, Updates}).

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

handle_command(_, _State, Data) ->
    {apply, Data}.

handle_query(get_snapshot, State, Data) ->
    handle_get_snapshot(State, Data).

apply_command({add, Key, Value}, Revision, State, Data) ->
    apply_add(Key, Value, Revision, State, Data);
apply_command({set, Key, Value}, Revision, State, Data) ->
    apply_set(Key, Value, any, Revision, State, Data);
apply_command({set, Key, Value, ExpectedRevision}, Revision, State, Data) ->
    apply_set(Key, Value, ExpectedRevision, Revision, State, Data);
apply_command({delete, Key}, Revision, State, Data) ->
    apply_delete(Key, any, Revision, State, Data);
apply_command({delete, Key, ExpectedRevision}, Revision, State, Data) ->
    apply_delete(Key, ExpectedRevision, Revision, State, Data);
apply_command({transaction, Conditions, Updates}, Revision, State, Data) ->
    apply_transaction(Conditions, Updates, Revision, State, Data).

handle_info(Msg, State, Data) ->
    ?WARNING("Unexpected message: ~p", [Msg]),
    {noreply, Data}.

terminate(_Reason, _State, _Data) ->
    ok.

%% internal
handle_get_snapshot(#state{table = Table}, Data) ->
    {reply, ets:tab2list(Table), Data}.

apply_add(Key, Value, Revision, #state{table = Table} = State, Data) ->
    KV = #kv{key = Key, value = Value, revision = Revision},
    Reply =
        case ets:insert_new(Table, KV) of
            true ->
                {ok, Revision};
            false ->
                {error, already_exists}
        end,
    {reply, Reply, State, Data}.

apply_set(Key, Value, ExpectedRevision, Revision,
          #state{table = Table} = State, Data) ->
    Reply =
        case check_revision(Key, ExpectedRevision, Revision, State) of
            ok ->
                KV = #kv{key = Key, value = Value, revision = Revision},
                ets:insert(Table, KV),
                {ok, Revision};
            {error, _} = Error ->
                Error
        end,

    {reply, Reply, State, Data}.

apply_delete(Key, ExpectedRevision, Revision,
             #state{table = Table} = State, Data) ->
    Reply =
        case check_revision(Key, ExpectedRevision, Revision, State) of
            ok ->
                ets:delete(Table, Key),
                ok;
            {error, _} = Error ->
                Error
        end,
    {reply, Reply, State, Data}.

apply_transaction(Conditions, Updates, Revision, State, Data) ->
    Reply =
        case check_transaction(Conditions, Revision, State) of
            ok ->
                apply_transaction_updates(Updates, Revision, State),
                {ok, Revision};
            {error, _} = Error ->
                Error
        end,
    {reply, Reply, State, Data}.

check_transaction(Conditions, AppliedRevision, State) ->
    check_transaction_loop(Conditions, AppliedRevision, State).

check_transaction_loop([], _, _) ->
    ok;
check_transaction_loop([{Key, ExpectedRevision} | Rest],
                       AppliedRevision, State) ->
    case check_revision(Key, ExpectedRevision, AppliedRevision, State) of
        ok ->
            check_transaction_loop(Rest, AppliedRevision, State);
        {error, _} = Error ->
            Error
    end.

apply_transaction_updates(Updates, Revision, #state{table = Table}) ->
    lists:foreach(
      fun (Update) ->
              case Update of
                  {set, Key, Value} ->
                      KV = #kv{key = Key, value = Value, revision = Revision},
                      ets:insert(Table, KV);
                  {delete, Key} ->
                      ets:delete(Table, Key)
              end
      end, Updates).

check_revision(Key, ExpectedRevision, AppliedRevision, #state{table = Table}) ->
    case ExpectedRevision of
        any ->
            ok;
        _ ->
            try ets:lookup_element(Table, Key, #kv.revision) of
                OurRevision when OurRevision =:= ExpectedRevision ->
                    ok;
                _ ->
                    {error, {conflict, AppliedRevision}}
            catch
                error:badarg ->
                    {error, {conflict, AppliedRevision}}
            end
    end.

event_manager_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "-events").
