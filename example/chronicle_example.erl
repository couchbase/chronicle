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
-module(chronicle_example).

-include_lib("kernel/include/logger.hrl").

-export([start/1]).

start([IndexStr]) ->
    net_kernel:start([node(), longnames]),
    logger:set_primary_config(level, debug),
    Index = list_to_integer(IndexStr),
    ?LOG_DEBUG("starting chronicle"),
    ok = application:start(chronicle),
    ok = application:start(asn1),
    ok = application:start(crypto),
    ok = application:start(public_key),
    ok = application:start(ssl),
    ok = application:start(ranch),
    ?LOG_DEBUG("starting cowboy rest server: ~p", [Index]),
    rest_app:start_rest_server(8080 + Index),
    ?LOG_DEBUG("cookie: ~p", [erlang:get_cookie()]),
    ?LOG_DEBUG("node: ~p, nodes: ~p", [node(), nodes()]),
    ok.
