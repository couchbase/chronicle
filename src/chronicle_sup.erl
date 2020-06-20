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
-export([on_system_state_change/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?START_NAME(?MODULE), ?MODULE, []).

on_system_state_change(State) ->
    case State of
        provisioned ->
            on_system_provisioned();
        unprovisioned ->
            on_system_unprovisioned()
    end.

on_system_provisioned() ->
    case supervisor:restart_child(?SERVER, chronicle_provisioned_sup) of
        {ok, _} ->
            ok;
        {error, running} ->
            %% shouldn't normally happen, but is possible if the system state
            %% changes in between when chronicle_sup_worker and
            %% chronicle_provisioned_sup start.
            ok;
        {error, restarting} ->
            %% may happen under similar circumstances as described above
            ok;
        {error, _} = Error ->
            exit(Error)
    end.

on_system_unprovisioned() ->
    ok = supervisor:terminate_child(?SERVER, chronicle_provisioned_sup).

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

    Storage = #{id => chronicle_storage,
                start => {chronicle_storage, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker},

    Agent = #{id => chronicle_agent,
              start => {chronicle_agent, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => worker},

    Provisioner = #{id => chronicle_provisioner,
                    start => {chronicle_provisioner, start_link, []},
                    restart => permanent,
                    type => supervisor},

    [Peers, Events, Storage, Agent, Provisioner].
