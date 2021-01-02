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
-module(chronicle_sup).

-behavior(supervisor).

-include("chronicle.hrl").

-define(SERVER, ?SERVER_NAME(?MODULE)).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?START_NAME(?MODULE), ?MODULE, []).

%% callbacks
init([]) ->
    %% TODO: reconsider the strategy
    Flags = #{strategy => rest_for_one,
              intensity => 3,
              period => 10},
    {ok, {Flags, child_specs()}}.

%% TODO: revise shutdown specifications
child_specs() ->
    Peers = #{id => chronicle_peers,
              start => {chronicle_peers, start_link, []},
              restart => permanent,
              shutdown => brutal_kill,
              type => worker},

    Events = #{id => chronicle_events,
               start => {chronicle_events, start_link, []},
               restart => permanent,
               shutdown => 1000,
               type => worker},

    Ets = #{id => chronicle_ets,
            start => {chronicle_ets, start_link, []},
            restart => permanent,
            shutdown => brutal_kill,
            type => worker},

    Agent = #{id => chronicle_agent,
              start => {chronicle_agent, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => worker},

    ExtEvents = #{id => chronicle_external_events,
                  start => {gen_event, start_link,
                            [?START_NAME(?EXTERNAL_EVENTS)]},
                  restart => permanent,
                  shutdown => brutal_kill,
                  type => worker},

    SecondarySup = #{id => chronicle_secondary_sup,
                     start => {chronicle_secondary_sup, start_link, []},
                     restart => permanent,
                     type => supervisor},

    [Peers, Events, Ets, Agent, ExtEvents, SecondarySup].
