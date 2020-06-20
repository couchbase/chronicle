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
%% A supervisor for processes that require the system to be provisioned to
%% run.
-module(chronicle_rsm_sup_worker).

-behavior(gen_server).

-include("chronicle.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2]).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

%% callbacks
init([]) ->
    Self = self(),
    chronicle_events:subscribe(
      fun (Event) ->
              case Event of
                  {metadata, _} ->
                      %% TODO: this is going to wake up the process needlessly
                      %% all the time; having more granular events
                      gen_server:cast(Self, Event);
                  _ ->
                      ok
              end
      end),

    {ok, Metadata} = chronicle_agent:get_metadata(),
    {ok, handle_new_metadata(Metadata, #{})}.

handle_call(Call, _From, _State) ->
    {stop, {unexpected_call, Call}}.

handle_cast({metadata, Metadata}, RSMs) ->
    {noreply, handle_new_metadata(Metadata, RSMs)};
handle_cast(Cast, _State) ->
    {stop, {unexpected_cast, Cast}}.

%% internal
handle_new_metadata(Metadata, OldRSMs) ->
    NewRSMs = get_rsms(Metadata),

    OldList = maps:to_list(OldRSMs),
    NewList = maps:to_list(NewRSMs),

    Removed = OldList -- NewList,
    Added = NewList -- OldList,

    lists:foreach(
      fun ({Name, _}) ->
              ok = chronicle_rsm_sup:stop_rsm(Name)
      end, Removed),

    lists:foreach(
      fun ({Name, #rsm_config{module = Module, args = Args}}) ->
              %% TODO: Be more lenient to errors here? So as not to affect
              %% other RSMs.
              ok = chronicle_rsm_sup:start_rsm(Name, Module, Args)
      end, Added),

    NewRSMs.

get_rsms(#metadata{config = Config}) ->
    get_rsms_from_config(Config).

get_rsms_from_config(#config{state_machines = RSMs}) ->
    RSMs;
get_rsms_from_config(#transition{current_config = Config}) ->
    get_rsms_from_config(Config).
