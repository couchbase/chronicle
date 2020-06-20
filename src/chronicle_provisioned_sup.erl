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
-module(chronicle_provisioned_sup).

-behavior(supervisor).

-include("chronicle.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?START_NAME(?MODULE), ?MODULE, []).

init([]) ->
    %% TODO: reconsider the strategy
    Flags = #{strategy => one_for_all,
              intensity => 3,
              period => 10},
    {ok, {Flags, child_specs()}}.

%% TODO: revise shutdown specifications
child_specs() ->
    Leader = #{id => chronicle_leader,
               start => {chronicle_leader, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker},

    Coordinator = #{id => chronicle_server,
                    start => {chronicle_server, start_link, []},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker},

    Failover = #{id => chronicle_failover,
                 start => {chronicle_failover, start_link, []},
                 restart => permanent,
                 shutdown => 5000,
                 type => worker},

    RSMSupWorker =
        #{id => chronicle_rsm_sup_worker,
          start => {chronicle_rsm_sup_worker, start_link, []},
          restart => permanent,
          shutdown => 1000,
          type => worker},

    RSMSup = #{id => chronicle_rsm_sup,
               start => {chronicle_rsm_sup, start_link, []},
               restart => permanent,
               shutdown => infinity,
               type => supervisor},

    [Leader, Coordinator, Failover, RSMSup, RSMSupWorker].
