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

-include("chronicle.hrl").

-compile(export_all).
-export_type([batch/0]).

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
    wait_for_process(Pid, infinity).

terminate_linked_process(Pid, Reason) when is_pid(Pid) ->
    terminate(Pid, Reason),
    receive
        {'EXIT', Pid, _} ->
            ok
    end.

next_term({TermNo, _}, Peer) ->
    {TermNo + 1, Peer}.

call_async(ServerRef, Request) ->
    Ref = make_ref(),
    call_async(ServerRef, Ref, Request).

call_async(ServerRef, Tag, Request) ->
    %% TODO: consider setting noconnect
    ?SEND(ServerRef, {'$gen_call', {self(), Tag}, Request}),
    Tag.

call(ServerRef, Call) ->
    call(ServerRef, Call, 5000).

%% A version of gen_{server,statem}:call/3 function that can take a timeout
%% reference in place of a literal timeout value.
call(ServerRef, Call, Timeout) ->
    do_call(ServerRef, Call, read_timeout(Timeout)).

do_call(ServerRef, Call, Timeout) ->
    try gen:call(ServerRef, '$gen_call', Call, Timeout) of
        {ok, Reply} ->
            Reply
    catch
        Class:Reason ->
            erlang:raise(
              Class,
              {Reason, {gen, call, [ServerRef, Call, Timeout]}},
              erlang:get_stacktrace())
    end.

with_timeout(TRef, Fun)
  when is_reference(TRef) ->
    Fun(TRef);
with_timeout(infinity, Fun) ->
    Fun(infinity);
with_timeout(Timeout, Fun) ->
    TRef = erlang:start_timer(Timeout, self(), timeout),
    try
        Fun(TRef)
    after
        erlang:cancel_timer(TRef),
        ?FLUSH({timeout, TRef, _})
    end.

read_timeout(infinity) ->
    infinity;
read_timeout(Timeout) when is_integer(Timeout) ->
    Timeout;
read_timeout(TRef) when is_reference(TRef) ->
    case erlang:read_timer(TRef) of
        false ->
            0;
        T when is_integer(T) ->
            T
    end.

term_number({TermNumber, _TermLeader}) ->
    TermNumber.

term_leader({_TermNumber, TermLeader}) ->
    TermLeader.

%% TODO: include committed_seqno
get_position(#metadata{term_voted = TermVoted, high_seqno = HighSeqno}) ->
    {TermVoted, HighSeqno}.

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

loop(Fun, Acc) ->
    case Fun(Acc) of
        {continue, NewAcc} ->
            loop(Fun, NewAcc);
        {stop, NewAcc} ->
            NewAcc
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
assert_is_test() ->
    error(not_test).
-endif.

run_on_process(Fun) ->
    run_on_process(Fun, infinity).

run_on_process(Fun, Timeout) ->
    Parent = self(),
    Ref = make_ref(),
    {Pid, MRef} =
        spawn_monitor(
          fun () ->
                  try Fun() of
                      Result ->
                          Parent ! {Ref, {ok, Result}}
                  catch
                      T:E ->
                          %% TODO: don't use get_stacktrace()
                          Stack = erlang:get_stacktrace(),
                          Parent ! {Ref, {raised, T, E, Stack}}
                  end
          end),

    receive
        {Ref, Result} ->
            erlang:demonitor(MRef, [flush]),
            case Result of
                {ok, Reply} ->
                    Reply;
                {raised, T, E, Stack} ->
                    erlang:raise(T, E, Stack)
            end;
        {'DOWN', MRef, process, Pid, Reason} ->
            exit(Reason)
    after
        Timeout ->
            erlang:demonitor(MRef, [flush]),
            exit(Pid, shutdown),
            exit(timeout)
    end.

-ifdef(TEST).
run_on_process_test() ->
    ?assertExit(timeout, run_on_process(fun () -> timer:sleep(1000) end, 100)),
    ?assertEqual(42, run_on_process(fun () -> 42 end)).
-endif.

-record(batch, { name :: atom(),
                 reqs :: list(),
                 timer :: undefined | reference(),
                 max_age :: non_neg_integer() }).

-type batch() :: #batch{}.

make_batch(Name, MaxAge) ->
    #batch{name = Name,
           reqs = [],
           max_age = MaxAge}.

batch_enq(Req, #batch{name = Name,
                      reqs = Reqs,
                      timer = Timer,
                      max_age = MaxAge} = Batch) ->
    NewTimer =
        case Timer of
            undefined ->
                erlang:send_after(MaxAge, self(), {batch_ready, Name});
            _ when is_reference(Timer) ->
                Timer
        end,

    Batch#batch{reqs = [Req | Reqs], timer = NewTimer}.

batch_flush(#batch{name = Name,
                   reqs = Reqs,
                   timer = Timer} = Batch) ->
    case Timer of
        undefined ->
            ok;
        _ when is_reference(Timer) ->
            erlang:cancel_timer(Timer),
            ?FLUSH({batch_ready, Name})
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
    with_timeout(Timeout,
                 fun (TRef) ->
                         with_leader_loop(TRef, any, Retries, Fun)
                 end).

with_leader_loop(TRef, Incarnation, Retries, Fun) ->
    {Leader, NewIncarnation} =
        chronicle_leader:wait_for_leader(Incarnation, TRef),
    Result = Fun(TRef, Leader),
    case Result of
        {error, {leader_error, not_leader}} when Retries > 0 ->
            with_leader_loop(TRef, NewIncarnation, Retries - 1, Fun);
        {error, {leader_error, _} = Error} ->
            exit({Leader, Error});
        _ ->
            Result
    end.

get_establish_quorum(Metadata) ->
    case Metadata#metadata.pending_branch of
        undefined ->
            get_append_quorum(Metadata#metadata.config);
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

config_peers(#config{voters = Voters}) ->
    Voters;
config_peers(#transition{current_config = Current,
                         future_config = Future}) ->
    lists:usort(config_peers(Current) ++ config_peers(Future)).
