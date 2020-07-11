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
-module(chronicle_rsm_sup).

-behavior(supervisor).

-include("chronicle.hrl").

-define(SERVER, ?SERVER_NAME(?MODULE)).

-export([start_link/0]).
-export([start_rsm/3, stop_rsm/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?START_NAME(?MODULE), ?MODULE, []).

start_rsm(Name, Module, Args) ->
    Spec = #{id => Name,
             start => {chronicle_single_rsm_sup,
                       start_link,
                       [Name, Module, Args]},
             restart => permanent,
             type => supervisor},
    case supervisor:start_child(?SERVER, Spec) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

stop_rsm(Name) ->
    case supervisor:terminate_child(?SERVER, Name) of
        ok ->
            ok = supervisor:delete_child(?SERVER, Name);
        {error, _} = Error ->
            Error
    end.

%% supervisor callback
init([]) ->
    %% TODO: reconsider the strategy
    Flags = #{strategy => one_for_one,
              intensity => 3,
              period => 10},
    {ok, {Flags, []}}.
