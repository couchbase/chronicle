%% @author Couchbase <info@couchbase.com>
%% @copyright 2021 Couchbase, Inc.
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
-module(chronicle_log_tests).

-include_lib("eunit/include/eunit.hrl").

get_dir() ->
    {ok, CurDir} = file:get_cwd(),
    filename:join([CurDir, "tmp", "log_tests"]).

prepare_dir() ->
    Dir = get_dir(),
    ok = chronicle_utils:delete_recursive(Dir),
    ok = chronicle_utils:mkdir_p(Dir).

prepare() ->
    prepare_dir(),
    chronicle_env:setup_logger().

simple_test_() ->
    {spawn, {setup, fun prepare/0, fun simple_test__/0}}.

simple_test__() ->
    Dir = get_dir(),
    Path1 = filename:join([Dir, "log1"]),
    Path2 = filename:join([Dir, "log2"]),
    {ok, Log} = chronicle_log:create(Path1, user_data),
    {ok, _} = chronicle_log:append(Log, [1, 2]),
    {ok, _} = chronicle_log:append(Log, [3, 4, 5]),
    ok = chronicle_log:close(Log),

    ?assertEqual({ok, [user_data, 1, 2, 3, 4, 5]}, read_log(Path1)),

    {ok, _} = file:copy(Path1, Path2),
    {ok, Fd1} = file:open(Path1, [raw, binary, read, write]),

    {ok, Pos} = file:position(Fd1, eof),
    {ok, <<Byte>>} = file:pread(Fd1, Pos div 2, 1),
    ok = file:pwrite(Fd1, Pos div 2, <<(Byte + 1)>>),
    ok = file:close(Fd1),

    ?assertMatch({error, {corrupt_log, _}}, read_log(Path1)),

    {ok, Fd2} = file:open(Path2, [raw, binary, read, write]),
    {ok, _} = file:position(Fd2, 3 * (Pos div 4)),
    ok = file:truncate(Fd2),
    ok = file:close(Fd2),

    ?assertMatch({error, {unexpected_eof, _}}, read_log(Path2)),

    ok = repair_log(Path2),
    {ok, Items} = read_log(Path2),
    ?assert(lists:prefix(Items, [user_data, 1, 2, 3, 4, 5])),

    ok.

read_log(Path) ->
    Append = fun (Item, Acc) -> [Item | Acc] end,
    case chronicle_log:read_log(Path, Append, Append, []) of
        {ok, Items} ->
            {ok, lists:reverse(Items)};
        Other ->
            Other
    end.

repair_log(Path) ->
    {ok, Log, state} = chronicle_log:open(Path,
                                          fun (_, S) -> S end,
                                          fun (_, S) -> S end,
                                          state),
    ok = chronicle_log:close(Log).
