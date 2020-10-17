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
-module(chronicle_peers).

-export([start_link/0]).
-export([get_live_peers/0, get_live_peers/1, monitor/0]).

-ifndef(TEST).

start_link() ->
    ignore.

get_live_peers() ->
    lists:sort(nodes([this, visible])).

monitor() ->
    ok = net_kernel:monitor_nodes(true, [nodedown_reason]).

-else.                                          % -ifndef(TEST)

start_link() ->
    chronicle_peers_vnet:start_link().

get_live_peers() ->
    chronicle_peers_vnet:get_live_peers().

monitor() ->
    chronicle_peers_vnet:monitor().

-endif.

get_live_peers(Peers) ->
    ordsets:intersection(get_live_peers(), lists:usort(Peers)).
