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
-module(chronicle_tests).

-include_lib("eunit/include/eunit.hrl").
-include("chronicle.hrl").

-export([debug_log/4]).

debug_log(Level, Fmt, Args, _Info) ->
    Time = format_time(os:timestamp()),
    ?debugFmt("~s [~p|~p] " ++ Fmt, [Time, Level, ?PEER() | Args]).

format_time(Time) ->
    LocalTime = calendar:now_to_local_time(Time),
    UTCTime = calendar:now_to_universal_time(Time),

    {{Year, Month, Day}, {Hour, Minute, Second}} = LocalTime,
    Millis = erlang:element(3, Time) div 1000,
    TimeHdr0 = io_lib:format("~B-~2.10.0B-~2.10.0BT~2.10.0B:"
                             "~2.10.0B:~2.10.0B.~3.10.0B",
                             [Year, Month, Day, Hour, Minute, Second, Millis]),
    [TimeHdr0 | get_timezone_hdr(LocalTime, UTCTime)].

get_timezone_hdr(LocalTime, UTCTime) ->
    case get_timezone_offset(LocalTime, UTCTime) of
        utc ->
            "Z";
        {Sign, DiffH, DiffM} ->
            io_lib:format("~s~2.10.0B:~2.10.0B", [Sign, DiffH, DiffM])
    end.

%% Return offset of local time zone w.r.t UTC
get_timezone_offset(LocalTime, UTCTime) ->
    %% Do the calculations in terms of # of seconds
    DiffInSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
        calendar:datetime_to_gregorian_seconds(UTCTime),
    case DiffInSecs =:= 0 of
        true ->
            %% UTC
            utc;
        false ->
            %% Convert back to hours and minutes
            {DiffH, DiffM, _ } = calendar:seconds_to_time(abs(DiffInSecs)),
            case DiffInSecs < 0 of
                true ->
                    %% Time Zone behind UTC
                    {"-", DiffH, DiffM};
                false ->
                    %% Time Zone ahead of UTC
                    {"+", DiffH, DiffM}
            end
    end.

simple_test_() ->
    Nodes = [a, b, c, d],

    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,
     fun () -> simple_test__(Nodes) end}.

setup_vnet(Nodes) ->
    {ok, _} = vnet:start_link(Nodes),
    lists:map(
      fun (N) ->
              {ok, Sup} =
                  rpc_node(
                    N, fun () ->
                               prepare_vnode_dir(),
                               chronicle_env:set_env(logger_function,
                                                     {?MODULE, debug_log}),
                               _ = application:load(chronicle),
                               ok = chronicle_env:setup(),

                               {ok, P} = chronicle_sup:start_link(),
                               unlink(P),
                               {ok, P}
                       end),
              Sup
      end, Nodes).

get_tmp_dir() ->
    {ok, BaseDir} = file:get_cwd(),
    filename:join(BaseDir, "tmp").

get_vnode_dir() ->
    Node = ?PEER(),
    filename:join(get_tmp_dir(), Node).

prepare_vnode_dir() ->
    Dir = get_vnode_dir(),
    ok = chronicle_utils:delete_recursive(Dir),
    ok = chronicle_utils:mkdir_p(Dir),
    chronicle_env:set_env(data_dir, Dir).

teardown_vnet(Sups) ->
    lists:foreach(
      fun (Sup) ->
              %% Supervisors are gen_server-s under the hood, hence the
              %% following works. Can't simply an exit signal, because it'll
              %% get ignored.
              gen_server:stop(Sup)
      end, Sups),
    vnet:stop().

add_voters(ViaNode, Voters) ->
    PreClusterInfo = rpc_node(ViaNode, fun chronicle:get_cluster_info/0),
    ok = rpc_nodes(Voters,
                   fun () ->
                           ok = chronicle:prepare_join(PreClusterInfo)
                   end),
    ok = rpc_node(ViaNode,
                  fun () ->
                          chronicle:add_voters(Voters)
                  end),
    PostClusterInfo = rpc_node(ViaNode, fun chronicle:get_cluster_info/0),
    ok = rpc_nodes(Voters,
                   fun () ->
                           ok = chronicle:join_cluster(PostClusterInfo)
                   end).

simple_test__(Nodes) ->
    Machines = [{kv, chronicle_kv, []}],
    ok = rpc_node(a,
                  fun () ->
                          chronicle:provision(Machines)
                  end),

    OtherNodes = Nodes -- [a],
    add_voters(a, OtherNodes),

    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:remove_peer(d),
                          Voters = chronicle:get_voters(),
                          ?DEBUG("Voters: ~p", [Voters]),

                          ok = chronicle_failover:failover([a, b],
                                                           pending_failover)
                  end),

    ok = vnet:disconnect(a, c),
    ok = vnet:disconnect(a, d),
    ok = vnet:disconnect(b, c),
    ok = vnet:disconnect(b, d),

    ok = rpc_node(b,
                  fun () ->
                          {ok, {pending_failover, _}} =
                              chronicle_kv:get(kv, '$failover_opaque',
                                               #{read_consistency => quorum}),
                          {ok, _} = chronicle_kv:delete(kv, pending_failover),

                          {ok, Rev} = chronicle_kv:add(kv, a, b),
                          {error, {conflict, _}} = chronicle_kv:add(kv, a, c),
                          {ok, {b, _}} = chronicle_kv:get(kv, a),
                          {ok, Rev2} = chronicle_kv:set(kv, a, c, Rev),
                          {error, _} = chronicle_kv:set(kv, a, d, Rev),
                          {ok, _} = chronicle_kv:set(kv, b, d),
                          {error, _} = chronicle_kv:delete(kv, a, Rev),
                          {ok, _} = chronicle_kv:delete(kv, a, Rev2),

                          {error, not_found} = chronicle_kv:get(kv, a,
                                                                #{read_consistency => quorum}),

                          {ok, _} = chronicle_kv:multi(kv,
                                                       [{set, a, 84},
                                                        {set, c, 42}]),
                          {error, {conflict, _}} =
                              chronicle_kv:multi(kv,
                                                 [{set, a, 1234, Rev2}]),

                          {ok, _, blah} =
                              chronicle_kv:transaction(
                                kv, [a, c],
                                fun (#{a := {A, _}, c := {C, _}}) ->
                                        84 = A,
                                        42 = C,
                                        {commit, [{set, a, A+1},
                                                  {delete, c}], blah}
                                end,
                                #{read_consistency => quorum}),

                          {ok, _} =
                              chronicle_kv:txn(
                                kv,
                                fun (Txn) ->
                                        case chronicle_kv:txn_get(a, Txn) of
                                            {ok, {A, _}} ->
                                                #{b := {B, _}} = Vs = chronicle_kv:txn_get_many([b, c], Txn),
                                                error = maps:find(c, Vs),
                                                85 = A,
                                                d = B,

                                                {commit, [{set, c, 42}]};
                                            {error, not_found} ->
                                                {abort, aborted}
                                        end
                                end),

                          {ok, {Snap, SnapRev}} =
                              chronicle_kv:get_snapshot(kv, [a, b, c]),
                          {85, _} = maps:get(a, Snap),
                          {d, _} = maps:get(b, Snap),
                          {42, _} = maps:get(c, Snap),

                          %% Snap and SnapRev are bound above
                          {ok, {Snap, SnapRev}} =
                              chronicle_kv:ro_txn(
                                kv,
                                fun (Txn) ->
                                        chronicle_kv:txn_get_many(
                                          [a, b, c], Txn)
                                end),

                          {ok, {42, _}} = chronicle_kv:get(kv, c),

                          {ok, _} = chronicle_kv:update(kv, a, fun (V) -> V+1 end),
                          {ok, {86, _}} = chronicle_kv:get(kv, a),

                          {ok, _} = chronicle_kv:set(kv, c, 1234),
                          {ok, _} = chronicle_kv:set(kv, d, 4321),
                          {ok, _} =
                              chronicle_kv:rewrite(
                                kv,
                                fun (Key, Value) ->
                                        case Key of
                                            a ->
                                                {update, 87};
                                            b ->
                                                {update, new_b, Value};
                                            c ->
                                                delete;
                                            _ ->
                                                keep
                                        end
                                end),

                          {ok, {d, _}} = chronicle_kv:get(kv, new_b),
                          {error, not_found} = chronicle_kv:get(kv, b),
                          {error, not_found} = chronicle_kv:get(kv, c),

                          ?assertError({bad_updates, _},
                                       chronicle_kv:transaction(
                                         kv, [a],
                                         fun (_) ->
                                                 {commit, sldkjflk}
                                         end)),

                          ?assertError({bad_updates, _},
                                       chronicle_kv:transaction(
                                         kv, [a],
                                         fun (_) ->
                                                 {commit, sldkjflk, extra}
                                         end)),

                          ?assertError({bad_updates, _},
                                       chronicle_kv:transaction(
                                         kv, [a],
                                         fun (_) ->
                                                 {commit, [sldkjflk], extra}
                                         end)),

                          ?assertError({bad_updates, _},
                                       chronicle_kv:transaction(
                                         kv, [a],
                                         fun (_) ->
                                                 {commit, [{set, {sldkjflk,v}}]}
                                         end)),

                          ok
                  end),

    timer:sleep(1000),


    ?debugFmt("~nStates:~n~p~n",
              [[{N, rpc_node(N, fun () ->
                                        chronicle_agent:get_metadata()
                                end)} || N <- Nodes]]),

    ?debugFmt("~nLogs:~n~p~n",
              [[{N, rpc_node(N, fun () ->
                                        chronicle_agent:get_log()
                                end)} || N <- Nodes]]),

    ?debugFmt("~nKV snapshots:~n~p~n",
              [[{N, rpc_node(N, fun () ->
                                        chronicle_kv:get_full_snapshot(kv)
                                end)} || N <- [a, b]]]),

    ok.

rpc_node(Node, Fun) ->
    vnet:rpc(Node, erlang, apply, [Fun, []]).

rpc_nodes(Nodes, Fun) ->
    lists:foreach(
      fun (N) ->
              ok = rpc_node(N, Fun)
      end, Nodes).

leader_transfer_test_() ->
    Nodes = [a, b],

    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,
     {timeout, 20, fun () -> leader_transfer_test__(Nodes) end}}.

leader_transfer_test__(Nodes) ->
    Machines = [{kv, chronicle_kv, []}],
    ok = rpc_node(a,
                  fun () ->
                          chronicle:provision(Machines)
                  end),
    add_voters(a, Nodes -- [a]),
    {ok, Leader} =
        rpc_node(a,
                 fun () ->
                         {L, _} = chronicle_leader:wait_for_leader(),
                         {ok, L}
                 end),

    [OtherNode] = Nodes -- [Leader],

    ok = rpc_node(
           OtherNode,
           fun () ->
                   RPid =
                       spawn_link(fun () ->
                                          timer:sleep(rand:uniform(500)),
                                          ok = chronicle:remove_peer(Leader)
                                  end),

                   Pids =
                       [spawn_link(
                          fun () ->
                                  timer:sleep(rand:uniform(500)),
                                  {ok, _} = chronicle_kv:set(kv, I, 2*I)
                          end) || I <- lists:seq(1, 20000)],

                   lists:foreach(
                     fun (Pid) ->
                             ok = chronicle_utils:wait_for_process(Pid, 100000)
                     end, [RPid | Pids])
           end),

    ok.

failover_recovery_test_() ->
    Nodes = [a, b, c],
    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,
     {timeout, 10, fun () -> failover_recovery_test__(Nodes) end}}.

failover_recovery_test__(Nodes) ->
    [Node|RestNodes] = Nodes,
    ok = rpc_node(a,
                  fun () ->
                               chronicle:provision([{kv, chronicle_kv, []}])
                  end),
    add_voters(Node, RestNodes),
    ok = rpc_node(Node,
                  fun () ->
                          {ok, _} = chronicle_kv:add(kv, a, 42),
                          ok
                  end),

    lists:foreach(
      fun (OtherNode) ->
              ok = vnet:disconnect(Node, OtherNode)
      end, RestNodes),

    {exit, timeout} =
        rpc_node(Node,
                 fun () ->
                         try
                             chronicle_kv:add(kv, b, 84,
                                              #{timeout => 100})
                         catch
                             T:E ->
                                 {T, E}
                         end
                 end),

    ok = rpc_node(Node,
                  fun () ->
                          ok = chronicle:failover([Node]),
                          {ok, {42, _}} = chronicle_kv:get(kv, a),
                          {error, not_found} = chronicle_kv:get(kv, b),
                          ok
                  end).

reprovision_test_() ->
    Nodes = [a],
    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,
     {timeout, 20, fun reprovision_test__/0}}.

reprovision_test__() ->
    Machines = [{kv, chronicle_kv, []}],
    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:provision(Machines),
                          {ok, _} = chronicle_kv:set(kv, a, 42),
                          ok
                  end),
    reprovision_test_loop(100).

reprovision_test_loop(0) ->
    ok;
reprovision_test_loop(I) ->
    Child = spawn_link(
              fun Loop() ->
                      %% Sequentially consistent gets should work even during
                      %% reprovisioning.
                      {ok, _} = rpc_node(a, fun () ->
                                                    chronicle_kv:get(kv, a)
                                            end),
                      Loop()
              end),

    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:reprovision(),

                          %% Make sure writes succeed after reprovision()
                          %% returns.
                          {ok, _} = chronicle_kv:set(kv, a, 42),
                          ok
                  end),

    unlink(Child),
    chronicle_utils:terminate_and_wait(Child, shutdown),

    reprovision_test_loop(I - 1).

reprovision_rewrite_test_() ->
    Nodes = [a],
    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,
     {timeout, 20, fun reprovision_rewrite_test__/0}}.

reprovision_rewrite_test__() ->
    Machines = [{kv, chronicle_kv, []}],
    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:provision(Machines),
                          {ok, _} = chronicle_kv:set(kv, a, 42),
                          ok
                  end),
    reprovision_rewrite_test_loop(100).

reprovision_rewrite_test_loop(0) ->
    ok;
reprovision_rewrite_test_loop(I) ->
    ok = rpc_node(
           a,
           fun () ->
                   ok = chronicle:reprovision(),

                   {ok, _} =
                       chronicle_kv:rewrite(
                         kv,
                         fun (_Key, Value) ->
                                 {update, Value + 1}
                         end),
                   ok
           end),
    reprovision_rewrite_test_loop(I - 1).

partition_test_() ->
    Nodes = [a, b, c, d],

    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,

     %% TODO: currently a hefty timeout is required here because nodeup events
     %% are not produced when vnet is used. So when the partition is lifted,
     %% we have to rely on check_peers() logic in chronicle_proposer to
     %% attempt to reestablish connection to the partitioned node. But that
     %% only happens at 5 second intervals.
     {timeout, 20, fun () -> partition_test__(Nodes) end}}.

partition_test__(Nodes) ->
    Machines = [{kv, chronicle_kv, []}],
    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:provision(Machines)
                  end),

    OtherNodes = Nodes -- [a],
    add_voters(a, OtherNodes),

    ok = rpc_node(a,
                  fun () ->
                          {ok, _} = chronicle_kv:set(kv, a, 42),
                          {a, _} = chronicle_leader:get_leader(),
                          ok
                  end),

    [ok = vnet:disconnect(a, N) || N <- OtherNodes],

    Pid = rpc_node(a,
                   fun () ->
                           spawn(
                             fun () ->
                                     chronicle_kv:add(kv, b, 43)
                             end)
                   end),

    ok = rpc_node(b,
                  fun () ->
                          {ok, _} = chronicle_kv:add(kv, b, 44),
                          ok
                  end),

    chronicle_utils:terminate_and_wait(Pid, shutdown),

    [ok = vnet:connect(a, N) || N <- OtherNodes],

    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle_kv:sync(kv, 10000),
                          {ok, {42, _}} = chronicle_kv:get(kv, a),
                          {ok, {44, _}} = chronicle_kv:get(kv, b),
                          ok
                  end),

    ok.
