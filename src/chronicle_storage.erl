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
-behavior(gen_server).

-include("chronicle.hrl").

-define(SERVER, ?SERVER_NAME(?MODULE)).

-record(state, { metadata }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

%% TODO: consider batching it later
persist_metadata(Key, Value) ->
    gen_server:call(?SERVER, {persist_metadata, Key, Value}, infinity).

fetch_metadata(Key) ->
    gen_server:call(?SERVER, {fetch_metadata, Key}, infinity).

%% gen_server callbacks
init(_) ->
    {ok, #state{metadata = #{}}}.

handle_call({fetch_metadata, Key}, _From, #state{metadata = Meta} = State) ->
    RV = case maps:find(Key, Meta) of
             {ok, Value} ->
                 {ok, Value};
             error ->
                 {error, not_found}
         end,
    {reply, RV, State};
handle_call({persist_metadata, Key, Value}, _From,
            #state{metadata = Meta} = State) ->
    {reply, ok, State#state{metadata = maps:put(Key, Value, Meta)}};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.
