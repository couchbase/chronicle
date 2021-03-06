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
-module(chronicle_secondary_sup).

-behavior(dynamic_supervisor).

-include("chronicle.hrl").

-export([start_link/0]).
-export([sync/0]).
-export([init/1, handle_event/2, child_specs/1]).

start_link() ->
    dynamic_supervisor:start_link(?START_NAME(?MODULE), ?MODULE, []).

sync() ->
    ok = dynamic_supervisor:sync(?SERVER_NAME(?MODULE), 20000).

%% callbacks
init([]) ->
    Self = self(),
    chronicle_events:subscribe(
      fun (Event) ->
              case Event of
                  {system_event, wiping, _} ->
                      dynamic_supervisor:send_event(Self, wipe_requested);
                  {system_state, NewState, _} ->
                      dynamic_supervisor:send_event(Self, {state, NewState});
                  _ ->
                      ok
              end
      end),

    State =
        case chronicle_agent:is_wipe_requested() of
            true ->
                not_provisioned;
            false ->
                case chronicle_agent:get_system_state() of
                    {provisioned, _} ->
                        provisioned;
                    {removed, _} ->
                        removed;
                    {joining_cluster, _} ->
                        joining_cluster;
                    not_provisioned ->
                        not_provisioned
                end
        end,

    %% TODO: reconsider the strategy
    Flags = #{strategy => one_for_all,
              intensity => 3,
              period => 10},
    {ok, Flags, State}.

handle_event(wipe_requested, _) ->
    {noreply, not_provisioned};
handle_event({state, NewState}, _) ->
    {noreply, NewState}.

%% TODO: revise shutdown specifications
child_specs(not_provisioned) ->
    [];
child_specs(joining_cluster) ->
    Leader = #{id => chronicle_leader,
               start => {chronicle_leader, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker},

    [Leader];
child_specs(removed) ->
    child_specs(provisioned);
child_specs(provisioned) ->
    RestartableSup = #{id => chronicle_secondary_restartable_sup,
                       start => {chronicle_secondary_restartable_sup,
                                 start_link, []},
                       restart => permanent,
                       shutdown => infinity,
                       type => supervisor},

    Server = #{id => chronicle_server,
               start => {chronicle_server, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker},

    RSMSup = #{id => chronicle_rsm_sup,
               start => {chronicle_rsm_sup, start_link, []},
               restart => permanent,
               shutdown => infinity,
               type => supervisor},

    child_specs(joining_cluster) ++ [RestartableSup, Server, RSMSup].
