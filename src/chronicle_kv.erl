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

-record(data, {}).

set(Name, Key, Value) ->
    chronicle_rsm:command(Name, {set, Key, Value}).

delete(Name, Key) ->
    chronicle_rsm:command(Name, {delete, Key}).

get(Name, Key) ->
    chronicle_rsm:query(Name, {get, Key}).

get_snapshot(Name) ->
    chronicle_rsm:query(Name, get_snapshot).

%% callbacks
init(_Name, []) ->
    {ok, #{}, #data{}}.

handle_command(_, _State, Data) ->
    {apply, Data}.

handle_query({get, Key}, State, Data) ->
    handle_get(Key, State, Data);
handle_query(get_snapshot, State, Data) ->
    handle_get_snapshot(State, Data).

apply_command({set, Key, Value}, Revision, State, Data) ->
    apply_set(Key, Value, Revision, State, Data);
apply_command({delete, Key}, _Revision, State, Data) ->
    apply_delete(Key, State, Data).

handle_info(Msg, State, Data) ->
    ?WARNING("Unexpected message: ~p", [Msg]),
    {noreply, Data}.

terminate(_Reason, _State, _Data) ->
    ok.

%% internal
handle_get(Key, State, Data) ->
    {reply, maps:find(Key, State), Data}.

handle_get_snapshot(State, Data) ->
    {reply, State, Data}.

apply_set(Key, Value, Revision, State, Data) ->
    {reply, {ok, Revision}, maps:put(Key, Value, State), Data}.

apply_delete(Key, State, Data) ->
    {reply, ok, maps:remove(Key, State), Data}.
