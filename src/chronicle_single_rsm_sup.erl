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
-module(chronicle_single_rsm_sup).

-behavior(supervisor).
-export([start_link/3]).
-export([init/1]).

start_link(Name, Module, Args) ->
    supervisor:start_link(?MODULE, [Name, Module, Args]).

%% supervisor callbacks
init([Name, Module, Args]) ->
    Flags = #{strategy => one_for_all,
              intensity => 10,
              period => 10},
    %% TODO: make this optional
    ExtraSpecs = Module:specs(Name, Args),
    RSM = #{id => Name,
            start => {chronicle_rsm, start_link, [Name, Module, Args]},
            restart => permanent,
            shutdown => 5000,
            type => worker},

    {ok, {Flags, ExtraSpecs ++ [RSM]}}.
