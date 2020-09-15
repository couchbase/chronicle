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
-module(misc_tests).

-include("chronicle.hrl").

-include_lib("eunit/include/eunit.hrl").

flush_test() ->
    self() ! {test, 1},
    self() ! {test2, 1},
    self() ! {test, 2},
    self() ! {test, 3},
    self() ! {test2, 2},

    3 = ?FLUSH({test, _}),
    2 = ?FLUSH({test2, _}).
