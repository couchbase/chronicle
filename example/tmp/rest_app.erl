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

%% @private
-module(rest_app).

%% API.
-export([start_rest_server/1]).
-export([stop/0]).

%% API.

start_rest_server(Port) ->
    Opts = rest_server:get_options(),
    {ok, _} = cowboy:start_clear(http, [{port, Port}], Opts).

stop() ->
    ok = cowboy:stop_listener(http).

