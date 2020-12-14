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
-module(chronicle_utils).

-include_lib("kernel/include/file.hrl").
-include("chronicle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(export_all).
-export_type([batch/0, send_options/0, send_result/0]).

-ifdef(HAVE_SYNC_DIR).
-on_load(init_sync_nif/0).
-endif.

groupby(Fun, List) ->
    lists:foldl(
      fun (Elem, Acc) ->
              Key = Fun(Elem),
              maps:update_with(
                Key,
                fun (Elems) ->
                        [Elem | Elems]
                end, [Elem], Acc)
      end, #{}, List).

parallel_mapfold(MapFun, FoldFun, Acc, List) ->
    {_, false} = process_info(self(), trap_exit),

    Ref = make_ref(),
    Parent = self(),
    proc_lib:spawn_link(
      fun () ->
              Reqs = start_maps(MapFun, List),
              Result = parallel_mapfold_loop(MapFun, FoldFun, Reqs, Acc),
              Parent ! {Ref, Result}
      end),

    receive
        {Ref, Result} ->
            Result
    end.

start_maps(MapFun, List) ->
    start_maps(MapFun, List, #{}).

start_maps(MapFun, List, Refs) ->
    lists:foldl(
      fun (Elem, AccRefs) ->
              Ref = start_map(fun () ->
                                      MapFun(Elem)
                              end),
              AccRefs#{Ref => Elem}
      end, Refs, List).

start_map(Fun) ->
    Ref = make_ref(),
    Parent = self(),
    spawn_link(
      fun () ->
              Result =
                  try
                      Fun()
                  catch
                      T:E ->
                          {error, {T, E}}
                  end,

              unlink(Parent),
              Parent ! {Ref, Result}
      end),

    Ref.

parallel_mapfold_loop(_MapFun, _FoldFun, Refs, Acc)
  when map_size(Refs) =:= 0 ->
    Acc;
parallel_mapfold_loop(MapFun, FoldFun, Refs, Acc) ->
    receive
        {Ref, Result} when is_reference(Ref) ->
            {Elem, NewRefs0} = maps:take(Ref, Refs),
            case FoldFun(Elem, Result, Acc) of
                {stop, FinalResult} ->
                    FinalResult;
                {continue, NewAcc} ->
                    parallel_mapfold_loop(MapFun, FoldFun, NewRefs0, NewAcc);
                {continue, NewAcc, MoreElems} ->
                    NewRefs = start_maps(MapFun, MoreElems, NewRefs0),
                    parallel_mapfold_loop(MapFun, FoldFun, NewRefs, NewAcc)
            end;
        Msg ->
            exit({unexpected_message, Msg})
    end.

terminate(Pid, normal) ->
    terminate(Pid, shutdown);
terminate(Pid, Reason) ->
    exit(Pid, Reason).

wait_for_process(PidOrName, Timeout) ->
    MRef = erlang:monitor(process, PidOrName),
    receive
        {'DOWN', MRef, process, _, _Reason} ->
            ok
    after Timeout ->
            erlang:demonitor(MRef, [flush]),
            {error, timeout}
    end.

-ifdef(TEST).
wait_for_process_test_() ->
    {spawn,
     fun () ->
             %% Normal
             ok = wait_for_process(spawn(fun() -> ok end), 100),
             %% Timeout
             {error, timeout} =
                 wait_for_process(spawn(fun() ->
                                                timer:sleep(100), ok end),
                                  1),
             %% Process that exited before we went.
             Pid = spawn(fun() -> ok end),
             ok = wait_for_process(Pid, 100),
             ok = wait_for_process(Pid, 100)
     end}.
-endif.

terminate_and_wait(Pid, Reason) when is_pid(Pid) ->
    terminate(Pid, Reason),
    ok = wait_for_process(Pid, infinity).

terminate_linked_process(Pid, Reason) when is_pid(Pid) ->
    with_trap_exit(
      fun () ->
              terminate(Pid, Reason),
              unlink(Pid),
              ?FLUSH({'EXIT', Pid, _})
      end),

    ok = wait_for_process(Pid, infinity).

with_trap_exit(Fun) ->
    Old = process_flag(trap_exit, true),
    try
        Fun()
    after
        case Old of
            true ->
                ok;
            false ->
                process_flag(trap_exit, false),
                with_trap_exit_maybe_exit()
        end
    end.

with_trap_exit_maybe_exit() ->
    receive
        {'EXIT', _Pid, normal} = Exit ->
            ?DEBUG("Ignoring exit message with reason normal: ~p", [Exit]),
            with_trap_exit_maybe_exit();
        {'EXIT', _Pid, Reason} = Exit ->
            ?DEBUG("Terminating due to exit message ~p", [Exit]),
            %% exit/2 is used instead of exit/1, so it can't be caught by a
            %% try..catch block.
            exit(self(), Reason)
    after
        0 ->
            ok
    end.

next_term({TermNo, _}, Peer) ->
    {TermNo + 1, Peer}.

-type send_options() :: [nosuspend | noconnect].
-type send_result() :: ok | nosuspend | noconnect.

-spec call_async(ServerRef :: any(),
                 Tag :: any(),
                 Request :: any(),
                 send_options()) -> send_result().
call_async(ServerRef, Tag, Request, Options) ->
    send(ServerRef, {'$gen_call', {self(), Tag}, Request}, Options).

-spec send(any(), any(), send_options()) -> send_result().
-ifdef(TEST).
send(Name, Msg, Options) ->
    {via, vnet, _} = Name,
    try
        vnet:send(element(3, Name), Msg),
        ok
    catch
        exit:{badarg, {_, _}} ->
            %% vnet:send() may fail with this error when Name can't be
            %% resolved. This is different from how erlang:send/3 behaves, so
            %% we are just catching the error.

            %% Force dialyzer to believe that nosuspend and noconnect
            %% are valid return values.
            case Options of
                [] ->
                    ok;
                [Other|_]->
                    Other
            end
    end.
-else.
send(Name, Msg, Options) ->
    erlang:send(Name, Msg, Options).
-endif.

call(ServerRef, Call) ->
    call(ServerRef, Call, 5000).

%% A version of gen_{server,statem}:call/3 function that can take a timeout in
%% the form of {timeout, StartTime, Timeout} tuple in place of a literal timeout
%% value.
call(ServerRef, Call, Timeout) ->
    do_call(ServerRef, Call, read_timeout(Timeout)).

do_call(ServerRef, Call, Timeout) ->
    try gen:call(ServerRef, '$gen_call', Call, Timeout) of
        {ok, Reply} ->
            Reply
    catch
        Class:Reason:Stack ->
            erlang:raise(
              Class,
              {Reason, {gen, call, [ServerRef, Call, Timeout]}},
              Stack)
    end.

start_timeout({timeout, _, _} = Timeout) ->
    Timeout;
start_timeout(infinity) ->
    infinity;
start_timeout(Timeout)
  when is_integer(Timeout), Timeout >= 0 ->
    NowTs = erlang:monotonic_time(),
    {timeout, NowTs, Timeout}.

read_timeout({timeout, StartTs, Timeout}) ->
    NowTs = erlang:monotonic_time(),
    Passed = erlang:convert_time_unit(NowTs - StartTs, native, millisecond),
    Remaining = Timeout - Passed,
    max(0, Remaining);
read_timeout(infinity) ->
    infinity;
read_timeout(Timeout) when is_integer(Timeout) ->
    Timeout.

term_number({TermNumber, _TermLeader}) ->
    TermNumber.

term_leader({_TermNumber, TermLeader}) ->
    TermLeader.

get_position(#metadata{high_term = HighTerm, high_seqno = HighSeqno}) ->
    {HighTerm, HighSeqno}.

compare_positions({TermVotedA, HighSeqnoA}, {TermVotedB, HighSeqnoB}) ->
    TermVotedNoA = term_number(TermVotedA),
    TermVotedNoB = term_number(TermVotedB),

    if
        TermVotedNoA > TermVotedNoB ->
            gt;
        TermVotedNoA =:= TermVotedNoB ->
            true = (TermVotedA =:= TermVotedB),

            if
                HighSeqnoA > HighSeqnoB ->
                    gt;
                HighSeqnoA =:= HighSeqnoB ->
                    eq;
                true ->
                    lt
            end;
        true ->
            lt
    end.

max_position(PositionA, PositionB) ->
    case compare_positions(PositionA, PositionB) of
        gt ->
            PositionA;
        _ ->
            PositionB
    end.

%% A version of erlang:monitor(process, ...) that knows how to deal with {via,
%% Registry, Name} processes that are used by vnet.
monitor_process({via, Registry, Name}) ->
    assert_is_test(),

    case Registry:whereis_name(Name) of
        undefined ->
            MRef = make_ref(),

            %% This is malformed DOWN message because there's no Pid or a
            %% process name included. The caller MUST node use it. It's only
            %% meant to be used for tests, so it's ok.
            self() ! {'DOWN', MRef, process, undefined, noproc},
            MRef;
        Pid when is_pid(Pid) ->
            monitor_process(Pid)
    end;
monitor_process(Process) ->
    erlang:monitor(process, Process).

-ifdef(TEST).
assert_is_test() ->
    ok.
-else.
-spec assert_is_test() -> no_return().
assert_is_test() ->
    error(not_test).
-endif.

-record(batch, { id :: any(),
                 reqs :: list(),
                 timer :: undefined | reference(),
                 max_age :: non_neg_integer() }).

-type batch() :: #batch{}.

make_batch(Id, MaxAge) ->
    #batch{id = Id,
           reqs = [],
           max_age = MaxAge}.

batch_enq(Req, #batch{id = Id,
                      reqs = Reqs,
                      timer = Timer,
                      max_age = MaxAge} = Batch) ->
    NewTimer =
        case Timer of
            undefined ->
                erlang:send_after(MaxAge, self(), {batch_ready, Id});
            _ when is_reference(Timer) ->
                Timer
        end,

    Batch#batch{reqs = [Req | Reqs], timer = NewTimer}.

batch_flush(#batch{id = Id,
                   reqs = Reqs,
                   timer = Timer} = Batch) ->
    case Timer of
        undefined ->
            ok;
        _ when is_reference(Timer) ->
            _ = erlang:cancel_timer(Timer),
            ?FLUSH({batch_ready, Id}),
            ok
    end,
    {lists:reverse(Reqs),
     Batch#batch{reqs = [], timer = undefined}}.

gb_trees_filter(Pred, Tree) ->
    Iter = gb_trees:iterator(Tree),
    gb_trees_filter_loop(Pred, Iter, []).

gb_trees_filter_loop(Pred, Iter, Acc) ->
    case gb_trees:next(Iter) of
        {Key, Value, NewIter} ->
            NewAcc =
                case Pred(Key, Value) of
                    true ->
                        [{Key, Value} | Acc];
                    false ->
                        Acc
                end,

            gb_trees_filter_loop(Pred, NewIter, NewAcc);
        none ->
            gb_trees:from_orddict(lists:reverse(Acc))
    end.

-ifdef(TEST).
gb_trees_filter_test() ->
    IsEven = fun (Key, _Value) ->
                     Key rem 2 =:= 0
             end,

    Tree = gb_trees:from_orddict([{1,2}, {2,3}, {3,4}, {4,5}]),
    ?assertEqual([{2,3}, {4,5}],
                 gb_trees:to_list(gb_trees_filter(IsEven, Tree))),

    ?assertEqual([],
                 gb_trees:to_list(gb_trees_filter(IsEven, gb_trees:empty()))).

-endif.

random_uuid() ->
    hexify(crypto:strong_rand_bytes(16)).

hexify(Binary) ->
    << <<(hexify_digit(High)), (hexify_digit(Low))>>
       || <<High:4, Low:4>> <= Binary >>.

hexify_digit(0) -> $0;
hexify_digit(1) -> $1;
hexify_digit(2) -> $2;
hexify_digit(3) -> $3;
hexify_digit(4) -> $4;
hexify_digit(5) -> $5;
hexify_digit(6) -> $6;
hexify_digit(7) -> $7;
hexify_digit(8) -> $8;
hexify_digit(9) -> $9;
hexify_digit(10) -> $a;
hexify_digit(11) -> $b;
hexify_digit(12) -> $c;
hexify_digit(13) -> $d;
hexify_digit(14) -> $e;
hexify_digit(15) -> $f.

%% TODO: Make this configurable
-define(LEADER_RETRIES, 1).

with_leader(Timeout, Fun) ->
    with_leader(Timeout, ?LEADER_RETRIES, Fun).

with_leader(Timeout, Retries, Fun) ->
    TRef = start_timeout(Timeout),
    with_leader_loop(TRef, any, Retries, Fun).

with_leader_loop(TRef, Incarnation, Retries, Fun) ->
    {Leader, NewIncarnation} =
        chronicle_leader:wait_for_leader(Incarnation, TRef),
    Result = Fun(TRef, Leader, NewIncarnation),
    case Result of
        {error, {leader_error, not_leader}} when Retries > 0 ->
            with_leader_loop(TRef, NewIncarnation, Retries - 1, Fun);
        {error, {leader_error, _} = Error} ->
            exit({Leader, Error});
        _ ->
            Result
    end.

get_config(#metadata{config = ConfigEntry}) ->
    ConfigEntry#log_entry.value.

get_all_peers(Metadata) ->
    case Metadata#metadata.pending_branch of
        undefined ->
            config_peers(get_config(Metadata));
        #branch{peers = BranchPeers} ->
            BranchPeers
    end.

get_establish_quorum(Metadata) ->
    case Metadata#metadata.pending_branch of
        undefined ->
            get_append_quorum(get_config(Metadata));
        #branch{peers = BranchPeers} ->
            {all, sets:from_list(BranchPeers)}
    end.

get_establish_peers(Metadata) ->
    get_quorum_peers(get_establish_quorum(Metadata)).

get_append_quorum(#config{voters = Voters}) ->
    {majority, sets:from_list(Voters)};
get_append_quorum(#transition{current_config = Current,
                              future_config = Future}) ->
    {joint,
     get_append_quorum(Current),
     get_append_quorum(Future)}.

get_quorum_peers(Quorum) ->
    sets:to_list(do_get_quorum_peers(Quorum)).

do_get_quorum_peers({majority, Peers}) ->
    Peers;
do_get_quorum_peers({all, Peers}) ->
    Peers;
do_get_quorum_peers({joint, Quorum1, Quorum2}) ->
    sets:union(do_get_quorum_peers(Quorum1),
               do_get_quorum_peers(Quorum2)).

have_quorum(AllVotes, Quorum)
  when is_list(AllVotes) ->
    do_have_quorum(sets:from_list(AllVotes), Quorum);
have_quorum(AllVotes, Quorum) ->
    do_have_quorum(AllVotes, Quorum).

do_have_quorum(AllVotes, {joint, Quorum1, Quorum2}) ->
    do_have_quorum(AllVotes, Quorum1) andalso do_have_quorum(AllVotes, Quorum2);
do_have_quorum(AllVotes, {all, QuorumNodes}) ->
    MissingVotes = sets:subtract(QuorumNodes, AllVotes),
    sets:size(MissingVotes) =:= 0;
do_have_quorum(AllVotes, {majority, QuorumNodes}) ->
    Votes = sets:intersection(AllVotes, QuorumNodes),
    sets:size(Votes) * 2 > sets:size(QuorumNodes).

is_quorum_feasible(Peers, FailedVotes, Quorum) ->
    PossibleVotes = Peers -- FailedVotes,
    have_quorum(PossibleVotes, Quorum).

config_peers(#config{voters = Voters,
                     replicas = Replicas}) ->
    Voters ++ Replicas;
config_peers(#transition{current_config = Current,
                         future_config = Future}) ->
    lists:usort(config_peers(Current) ++ config_peers(Future)).

config_rsms(#config{state_machines = RSMs}) ->
    RSMs;
config_rsms(#transition{current_config = Config}) ->
    %% TODO: currently there's no way to change the set of state machines once
    %% the cluster is provisioned, so this is correct. Reconsider once state
    %% machines can be added dynamically.
    config_rsms(Config).

-ifdef(HAVE_SYNC_DIR).

init_sync_nif() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, _} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    erlang:load_nif(filename:join(PrivDir, "sync_nif"), 0).

encode_path(Path) ->
    Encoded =
        if
            is_binary(Path) ->
                Path;
            is_list(Path) ->
                case file:native_name_encoding() of
                    latin1 ->
                        list_to_binary(Path);
                    utf8 ->
                        case unicode:characters_to_nfc_binary(Path) of
                            {error, _, _} ->
                                error(badarg);
                            Binary ->
                                Binary
                        end
                end;
            true ->
                error(badarg)
        end,

    %% Null-terminate.
    <<Encoded/binary, 0>>.

sync_dir(Path) ->
    do_sync_dir(encode_path(Path)).

do_sync_dir(_Path) ->
    erlang:nif_error(sync_nif_not_loaded).

-else.                                          % -ifdef(HAVE_SYNC_DIR)

sync_dir(_Path) ->
    ok.

-endif.

atomic_write_file(Path, Body) ->
    TmpPath = Path ++ ".tmp",
    case file:open(TmpPath, [write, raw]) of
        {ok, File} ->
            try Body(File) of
                ok ->
                    atomic_write_file_commit(File, Path, TmpPath);
                Error ->
                    atomic_write_file_cleanup(File, TmpPath),
                    Error
            catch
                T:E:Stack ->
                    atomic_write_file_cleanup(File, TmpPath),
                    erlang:raise(T, E, Stack)
            end;
        Error ->
            Error
    end.

atomic_write_file_cleanup(File, TmpPath) ->
    ok = file:close(File),
    ok = file:delete(TmpPath).

atomic_write_file_commit(File, Path, TmpPath) ->
    Dir = filename:dirname(Path),
    ok = file:sync(File),
    ok = file:close(File),
    ok = file:rename(TmpPath, Path),
    ok = sync_dir(Dir).

create_marker(Path) ->
    create_marker(Path, <<>>).

create_marker(Path, Content) ->
    atomic_write_file(Path,
                      fun (File) ->
                              file:write(File, Content)
                      end).

delete_marker(Path) ->
    Dir = filename:dirname(Path),
    case file:delete(Path) of
        ok ->
            sync_dir(Dir);
        {error, enoent} ->
            sync_dir(Dir);
        {error, _} = Error ->
            Error
    end.

mkdir_p(Path) ->
    case filelib:ensure_dir(Path) of
        ok ->
            case check_file_exists(Path, directory) of
                ok ->
                    ok;
                {error, enoent} ->
                    file:make_dir(Path);
                {error, {wrong_file_type, _, _}} ->
                    {error, eexist};
                {error, _} = Error ->
                    Error
            end;
        Error ->
            Error
    end.

check_file_exists(Path, Type) ->
    case file:read_file_info(Path) of
        {ok, Info} ->
            ActualType = Info#file_info.type,
            case ActualType =:= Type of
                true ->
                    ok;
                false ->
                    {error, {wrong_file_type, Type, ActualType}}
            end;
        Error ->
            Error
    end.

delete_recursive(Path) ->
    case filelib:is_dir(Path) of
        true ->
            case file:list_dir(Path) of
                {ok, Children} ->
                    delete_recursive_loop(Path, Children);
                {error, enoent} ->
                    ok;
                {error, Error} ->
                    {error, {Error, Path}}
            end;
        false ->
            delete(Path, regular)
    end.

delete_recursive_loop(Dir, []) ->
    delete(Dir, directory);
delete_recursive_loop(Dir, [Path|Paths]) ->
    FullPath = filename:join(Dir, Path),
    case delete_recursive(FullPath) of
        ok ->
            delete_recursive_loop(Dir, Paths);
        {error, _} = Error ->
            Error
    end.

delete(Path, Type) ->
    Result =
        case Type of
            directory ->
                file:del_dir(Path);
            regular ->
                file:delete(Path)
        end,

    case Result of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Error} ->
            {error, {Error, Path}}
    end.

read_full(Fd, Size) ->
    case file:read(Fd, Size) of
        {ok, Data} when byte_size(Data) < Size ->
            eof;
        Other ->
            Other
    end.

maps_foreach(Fun, Map) ->
    maps_foreach_loop(Fun, maps:iterator(Map)).

maps_foreach_loop(Fun, MapIter) ->
    case maps:next(MapIter) of
        none ->
            ok;
        {Key, Value, NewMapIter} ->
            Fun(Key, Value),
            maps_foreach_loop(Fun, NewMapIter)
    end.

-ifdef(TEST).
maps_foreach_test() ->
    Map = #{1 => 2, 3 => 4, 5 => 6},
    Ref = make_ref(),
    erlang:put(Ref, []),
    maps_foreach(
      fun (Key, Value) ->
              erlang:put(Ref, [{Key, Value} | erlang:get(Ref)])
      end, Map),
    Elems = erlang:erase(Ref),
    ?assertEqual(lists:sort(maps:to_list(Map)), lists:sort(Elems)).
-endif.

queue_foreach(Fun, Queue) ->
    case queue:out(Queue) of
        {empty, _} ->
            ok;
        {{value, Value}, NewQueue} ->
            Fun(Value),
            queue_foreach(Fun, NewQueue)
    end.

-ifdef(TEST).
queue_foreach_test() ->
    Q = queue:from_list([1,2,3,4,5]),
    queue_foreach(
      fun (Elem) ->
              self() ! Elem
      end, Q),

    Rcv = fun () ->
                  receive
                      Msg -> Msg
                  after
                      0 ->
                          exit(no_msg)
                  end
          end,

    ?assertEqual(1, Rcv()),
    ?assertEqual(2, Rcv()),
    ?assertEqual(3, Rcv()),
    ?assertEqual(4, Rcv()),
    ?assertEqual(5, Rcv()).
-endif.

queue_takefold(Fun, Acc, Queue) ->
    case queue:out(Queue) of
        {empty, _} ->
            {Acc, Queue};
        {{value, Value}, NewQueue} ->
            case Fun(Value, Acc) of
                {true, NewAcc} ->
                    queue_takefold(Fun, NewAcc, NewQueue);
                false ->
                    {Acc, Queue}
            end
    end.

-ifdef(TEST).
queue_takefold_test() ->
    Q = queue:from_list(lists:seq(1, 10)),
    MkFun = fun (CutOff) ->
                    fun (V, Acc) ->
                            case V =< CutOff of
                                true ->
                                    {true, Acc+V};
                                false ->
                                    false
                            end
                    end
            end,

    Test = fun (ExpectedSum, ExpectedTail, CutOff) ->
                   {Sum, NewQ} = queue_takefold(MkFun(CutOff), 0, Q),
                   ?assertEqual(ExpectedSum, Sum),
                   ?assertEqual(ExpectedTail, queue:to_list(NewQ))
           end,

    Test(0, lists:seq(1,10), 0),
    Test(15, lists:seq(6,10), 5),
    Test(55, [], 42).
-endif.

queue_takewhile(Pred, Queue) ->
    {Result, _} =
        queue_takefold(
          fun (Value, Acc) ->
                  case Pred(Value) of
                      true ->
                          {true, queue:in(Value, Acc)};
                      false ->
                          false
                  end
          end, queue:new(), Queue),
    Result.

-ifdef(TEST).
queue_takewhile_test() ->
    Q = queue:from_list(lists:seq(1, 10)),
    ?assertEqual(lists:seq(1, 5),
                 queue:to_list(queue_takewhile(
                                 fun (V) ->
                                         V =< 5
                                 end, Q))).
-endif.

queue_dropwhile(Pred, Queue) ->
    {_, NewQueue} =
        queue_takefold(
          fun (Value, _) ->
                  case Pred(Value) of
                      true ->
                          {true, unused};
                      false ->
                          false
                  end
          end, unused, Queue),
    NewQueue.

-ifdef(TEST).
queue_dropwhile_test() ->
    Q = queue:from_list(lists:seq(1, 10)),
    Test = fun (Expected, CutOff) ->
                   NewQ = queue_dropwhile(fun (V) -> V =< CutOff end, Q),
                   ?assertEqual(Expected, queue:to_list(NewQ))
           end,
    Test(lists:seq(1,10), 0),
    Test(lists:seq(6,10), 5),
    Test([], 42).
-endif.

log_entry_revision(#log_entry{history_id = HistoryId, seqno = Seqno}) ->
    {HistoryId, Seqno}.
