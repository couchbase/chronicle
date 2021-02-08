%% @author Couchbase <info@couchbase.com>
%% @copyright 2021 Couchbase, Inc.
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
-module(chronicle_settings).

-behavior(gen_server).

-include("chronicle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).
-export([get/2, get_settings/0, set_settings/1, set_local_settings/1]).

-export([init/1, handle_call/3, handle_cast/2]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(TABLE, ?ETS_TABLE(?MODULE)).

-record(state, { settings = #{},
                 local_settings = #{},
                 effective_settings = #{} }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

get(Name, Default) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            Default;
        [{_, Value}] ->
            Value
    end.

get_settings() ->
    gen_server:call(?SERVER, get_settings).

set_settings(Settings) ->
    gen_server:call(?SERVER, {set_settings, Settings}).

set_local_settings(Settings) ->
    gen_server:call(?SERVER, {set_local_settings, Settings}).

%% callbacks
init([]) ->
    _ = ets:new(?TABLE, [protected, named_table,
                         {read_concurrency, true},
                         {write_concurrency, true}]),

    LocalSettings =
        case application:get_env(chronicle, settings) of
            {ok, Settings} ->
                Settings;
            undefined ->
                #{}
        end,

    State = handle_new_settings(#{}, LocalSettings, #state{}),

    {ok, State}.

handle_call(get_settings, _From, #state{effective_settings =
                                            EffectiveSettings} = State) ->
    {reply, EffectiveSettings, State};
handle_call({set_settings, Settings}, _From,
            #state{local_settings = LocalSettings} = State) ->
    NewState = handle_new_settings(Settings, LocalSettings, State),
    {reply, ok, NewState};
handle_call({set_local_settings, LocalSettings}, _From,
            #state{settings = Settings} = State) ->
    NewState = handle_new_settings(Settings, LocalSettings, State),
    {reply, ok, NewState};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast:~n~p", [Cast]),
    {noreply, State}.

%% internal
handle_new_settings(Settings, LocalSettings,
                    #state{effective_settings = OldEffectiveSettings} =
                        State) ->
    NewEffectiveSettings = maps:merge(Settings, LocalSettings),
    {ToDelete, ToSet} =
        diff_settings(OldEffectiveSettings, NewEffectiveSettings),

    lists:foreach(
      fun (Name) ->
              ets:delete(?TABLE, Name)
      end, ToDelete),

    ets:insert(?TABLE, ToSet),

    State#state{settings = Settings,
                local_settings = LocalSettings,
                effective_settings = NewEffectiveSettings}.

diff_settings(OldSettings, NewSettings) ->
    ToDelete =
        maps:fold(
          fun (Name, _, Acc) ->
                  case maps:is_key(Name, NewSettings) of
                      true ->
                          Acc;
                      false ->
                          [Name | Acc]
                  end
          end, [], OldSettings),

    ToSet =
        maps:fold(
          fun (Name, Value, Acc) ->
                  case maps:find(Name, OldSettings) of
                      {ok, OldValue}
                        when OldValue =:= Value ->
                          Acc;
                      _ ->
                          [{Name, Value} | Acc]
                  end
          end, [], NewSettings),

    {ToDelete, ToSet}.

-ifdef(TEST).
diff_settings_test() ->
    OldSettings = #{a => 42, c => fortytwo},
    NewSettings = #{a => 43, d => fortythree},

    {ToDelete, ToSet} = diff_settings(OldSettings, NewSettings),

    ?assertEqual([c], ToDelete),
    ?assertEqual([{a, 43}, {d, fortythree}], lists:sort(ToSet)).
-endif.
