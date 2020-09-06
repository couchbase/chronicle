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
-module(chronicle_server).

-compile(export_all).
-behavior(gen_statem).

-include("chronicle.hrl").

-import(chronicle_utils, [call/3, call_async/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).
-define(COMMANDS_BATCH_AGE, 20).
-define(SYNCS_BATCH_AGE, 5).

-record(follower, {}).
-record(leader, { history_id, term, status, seqno }).

%% TODO: reconsider the decision to have proposer run in a separate process
%% TOOD: this record contains fields only used when the state is leader
-record(data, { proposer,
                commands_batch,
                syncs_batch,
                rsms = #{} }).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

register_rsm(Name, Pid) ->
    gen_statem:call(?SERVER, {register_rsm, Name, Pid}, 10000).

get_config(Leader, Timeout) ->
    call(?SERVER(Leader), get_config, Timeout).

%% TODO: think more about what CasRevision should be.
%%
%% Specifically, should it or should it not include a term number.
cas_config(Leader, NewConfig, CasRevision, Timeout) ->
    call(?SERVER(Leader), {cas_config, NewConfig, CasRevision}, Timeout).

sync_quorum(Tag, HistoryId, Term) ->
    gen_statem:cast(?SERVER, {sync_quorum, self(), Tag, HistoryId, Term}).

%% Used locally by corresponding chronicle_rsm instance.
rsm_command(Tag, HistoryId, Term, RSMName, Command) ->
    gen_statem:cast(?SERVER, {rsm_command,
                              self(), Tag, HistoryId, Term, RSMName, Command}).

%% Meant to only be used by chronicle_proposer.
proposer_ready(Pid, HistoryId, Term, HighSeqno) ->
    Pid ! {proposer_msg, {proposer_ready, HistoryId, Term, HighSeqno}}.

proposer_stopping(Pid, Reason) ->
    Pid ! {proposer_msg, {proposer_stopping, Reason}}.

%% gen_server callbacks
callback_mode() ->
    [handle_event_function, state_enter].

init([]) ->
    process_flag(trap_exit, true),

    Self = self(),
    chronicle_events:subscribe(
      fun (Event) ->
              case is_interesting_event(Event) of
                  true ->
                      Self ! {chronicle_event, Event};
                  false ->
                      ok
              end
      end),

    chronicle_leader:announce_leader_status(),
    {ok, #follower{}, #data{}}.

handle_event(enter, OldState, State, Data) ->
    case should_trigger_state_events(OldState, State) of
        true ->
            LeaveData = handle_state_leave(OldState, Data),
            EnterData = handle_state_enter(State, LeaveData),
            {keep_state, EnterData};
        false ->
            keep_state_and_data
    end;
handle_event(info, {chronicle_event, Event}, State, Data) ->
    handle_chronicle_event(Event, State, Data);
handle_event(info, {'EXIT', Pid, Reason}, State, Data) ->
    handle_process_exit(Pid, Reason, State, Data);
handle_event(info, {'DOWN', MRef, process, Pid, Reason}, State, Data) ->
    handle_process_down(MRef, Pid, Reason, State, Data);
handle_event(info, {proposer_msg, Msg}, #leader{} = State, Data) ->
    handle_proposer_msg(Msg, State, Data);
handle_event(info, {batch_ready, BatchField}, State, Data) ->
    handle_batch_ready(BatchField, State, Data);
handle_event({call, From}, get_config, State, Data) ->
    handle_get_config(From, State, Data);
handle_event({call, From}, {cas_config, NewConfig, Revision}, State, Data) ->
    handle_cas_config(NewConfig, Revision, From, State, Data);
handle_event({call, From}, {register_rsm, Name, Pid}, State, Data) ->
    handle_register_rsm(Name, Pid, From, State, Data);
handle_event(cast,
             {rsm_command, Pid, Tag, HistoryId, Term, RSMName, RSMCommand},
             State, Data) ->
    ReplyTo = {send, Pid, Tag},
    Command = {rsm_command, RSMName, RSMCommand},
    batch_leader_request(Command, {HistoryId, Term}, ReplyTo,
                         #data.commands_batch, State, Data);
handle_event(cast, {sync_quorum, Pid, Tag, HistoryId, Term}, State, Data) ->
    ReplyTo = {send, Pid, Tag},
    batch_leader_request(sync_quorum, {HistoryId, Term},
                         ReplyTo, #data.syncs_batch, State, Data);
handle_event({call, From}, _Call, _State, _Data) ->
    {keep_state_and_data, {reply, From, nack}};
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event ~p", [{Type, Event}]),
    keep_state_and_data.

terminate(_Reason, State, Data) ->
    handle_state_leave(State, Data).

%% internal
is_interesting_event({leader_status, _}) ->
    true;
is_interesting_event(_) ->
    false.

should_trigger_state_events(OldState, NewState) ->
    %% Don't generate state events if everything that changes is leader status.
    case {OldState, NewState} of
        {#leader{history_id = HistoryId, term = Term},
         #leader{history_id = HistoryId, term = Term}} ->
            false;
        _ ->
            %% Otherwise rely on gen_statem default logic of comparing states
            %% structurally.
            true
    end.

handle_state_leave(#leader{} = State, Data) ->
    NewData = cleanup_after_proposer(terminate_proposer(Data)),
    announce_term_finished(State, NewData),
    NewData;
handle_state_leave(_OldState, Data) ->
    Data.

handle_state_enter(#leader{history_id = HistoryId, term = Term}, Data) ->
    {ok, Proposer} = chronicle_proposer:start_link(HistoryId, Term),

    undefined = Data#data.commands_batch,
    undefined = Data#data.syncs_batch,

    CommandsBatch =
        chronicle_utils:make_batch(#data.commands_batch, ?COMMANDS_BATCH_AGE),
    SyncsBatch =
        chronicle_utils:make_batch(#data.syncs_batch, ?SYNCS_BATCH_AGE),
    NewData = Data#data{proposer = Proposer,
                        commands_batch = CommandsBatch,
                        syncs_batch = SyncsBatch},
    NewData;
handle_state_enter(_State, Data) ->
    Data.

handle_chronicle_event({leader_status, LeaderInfo}, State, Data) ->
    handle_leader_status(LeaderInfo, State, Data).

handle_leader_status(not_leader, _State, Data) ->
    {next_state, #follower{}, Data};
handle_leader_status(LeaderInfo, State, Data) ->
    #{history_id := HistoryId, term := Term} = LeaderInfo,

    case State of
        #leader{history_id = OurHistoryId, term = OurTerm}
          when HistoryId =:= OurHistoryId andalso Term =:= OurTerm ->
            %% We've already reacted to a similar event.
            keep_state_and_data;
        _ ->
            {next_state,
             #leader{history_id = HistoryId,
                     term = Term,
                     status = not_ready},
             Data}
    end.

handle_process_exit(Pid, Reason, _State, #data{proposer = Proposer} = Data) ->
    case Pid =:= Proposer of
        true ->
            ?INFO("Proposer terminated with reason ~p", [Reason]),
            {stop, {proposer_terminated, Reason},
             Data#data{proposer = undefined}};
        false ->
            {stop, {linked_process_died, Pid, Reason}}
    end.

handle_proposer_msg({proposer_ready, HistoryId, Term, HighSeqno},
                    State, Data) ->
    handle_proposer_ready(HistoryId, Term, HighSeqno, State, Data);
handle_proposer_msg({proposer_stopping, Reason}, State, Data) ->
    handle_proposer_stopping(Reason, State, Data).

handle_proposer_ready(HistoryId, Term, HighSeqno,
                      #leader{history_id = HistoryId,
                              term = Term,
                              status = not_ready} = State,
                      Data) ->
    NewState = State#leader{status = ready,
                            seqno = HighSeqno},
    announce_term_established(NewState, Data),
    {next_state, NewState, Data}.

handle_proposer_stopping(Reason,
                         #leader{history_id = HistoryId, term = Term},
                         Data) ->
    ?INFO("Proposer for term ~p in history ~p is terminating. Reason: ~p",
          [Term, HistoryId, Reason]),
    {next_state, #follower{}, Data}.

reply_request(ReplyTo, Reply) ->
    case ReplyTo of
        noreply ->
            ok;
        {from, From} ->
            gen_statem:reply(From, Reply);
        {send, Pid, Tag} ->
            Pid ! {Tag, Reply};
        {many, ReplyTos} ->
            lists:foreach(
              fun (To) ->
                      reply_request(To, Reply)
              end, ReplyTos)
    end.

handle_leader_request(_, ReplyTo, #follower{}, _Fun) ->
    %% TODO
    reply_request(ReplyTo, {error, {leader_error, not_leader}}),
    keep_state_and_data;
handle_leader_request(HistoryAndTerm, ReplyTo,
                      #leader{history_id = OurHistoryId, term = OurTerm},
                      Fun) ->
    case HistoryAndTerm =:= any
        orelse HistoryAndTerm =:= {OurHistoryId, OurTerm} of
        true ->
            Fun();
        false ->
            reply_request(ReplyTo, {error, {leader_error, not_leader}}),
            keep_state_and_data
    end.

batch_leader_request(Req, HistoryAndTerm,
                     ReplyTo, BatchField, State, Data) ->
    handle_leader_request(
      HistoryAndTerm, ReplyTo, State,
      fun () ->
              NewData =
                  update_batch(
                    BatchField, Data,
                    fun (Batch) ->
                            chronicle_utils:batch_enq({ReplyTo, Req}, Batch)
                    end),
              {keep_state, NewData}
      end).

handle_batch_ready(BatchField, #leader{} = State, Data) ->
    postpone_if_not_ready(
      State,
      fun () ->
              {keep_state, deliver_batch(BatchField, Data)}
      end).

postpone_if_not_ready(#leader{status = Status}, Fun) ->
    case Status of
        ready ->
            Fun();
        not_ready ->
            {keep_state_and_data, postpone}
    end.

update_batch(BatchField, Data, Fun) ->
    Batch = element(BatchField, Data),
    NewBatch = Fun(Batch),
    setelement(BatchField, Data, NewBatch).

deliver_batch(BatchField, Data) ->
    Batch = element(BatchField, Data),
    {Requests, NewBatch} = chronicle_utils:batch_flush(Batch),
    NewData = setelement(BatchField, Data, NewBatch),
    deliver_requests(Requests, BatchField, NewData),
    NewData.

deliver_requests([], _Batch, _Data) ->
    ok;
deliver_requests(Requests, Batch, Data) ->
    case Batch of
        #data.syncs_batch ->
            deliver_syncs(Requests, Data);
        #data.commands_batch ->
            deliver_commands(Requests, Data)
    end.

deliver_syncs(Syncs, #data{proposer = Proposer}) ->
    {ReplyTos, _} = lists:unzip(Syncs),
    ReplyTo = {many, ReplyTos},

    chronicle_proposer:sync_quorum(Proposer, ReplyTo).

deliver_commands(Commands, #data{proposer = Proposer}) ->
    chronicle_proposer:append_commands(Proposer, Commands).

handle_get_config(From, State, Data) ->
    ReplyTo = {from, From},
    handle_leader_request(
      any, ReplyTo, State,
      fun () ->
              postpone_if_not_ready(
                State,
                fun () ->
                        deliver_get_config(ReplyTo, Data),
                        keep_state_and_data
                end)
      end).

deliver_get_config(ReplyTo, #data{proposer = Proposer}) ->
    chronicle_proposer:get_config(Proposer, ReplyTo).

handle_cas_config(NewConfig, Revision, From, State, Data) ->
    ReplyTo = {from, From},
    handle_leader_request(
      any, ReplyTo, State,
      fun () ->
              postpone_if_not_ready(
                State,
                fun () ->
                        deliver_cas_config(NewConfig, Revision, ReplyTo, Data),
                        keep_state_and_data
                end)
      end).

deliver_cas_config(NewConfig, Revision, ReplyTo, #data{proposer = Proposer}) ->
    chronicle_proposer:cas_config(Proposer, ReplyTo, NewConfig, Revision).

handle_register_rsm(Name, Pid, From, State, #data{rsms = RSMs} = Data) ->
    ?DEBUG("Registering RSM ~p with pid ~p", [Name, Pid]),
    MRef = erlang:monitor(process, Pid),
    NewRSMs = maps:put(MRef, {Name, Pid}, RSMs),
    NewData = Data#data{rsms = NewRSMs},
    Reply =
        case State of
            #leader{history_id = HistoryId,
                    term = Term,
                    seqno = Seqno,
                    status = ready} ->
                {ok, HistoryId, Term, Seqno};
            _ ->
                no_term
        end,

    {keep_state, NewData, {reply, From, Reply}}.

handle_process_down(MRef, Pid, Reason, _State, #data{rsms = RSMs} = Data) ->
    case maps:take(MRef, RSMs) of
        error ->
            {stop, {unexpected_process_down, MRef, Pid, Reason}};
        {{Name, RSMPid}, NewRSMs} ->
            true = (Pid =:= RSMPid),
            ?DEBUG("RSM ~p~p terminated with reason: ~p",
                   [Name, RSMPid, Reason]),
            {keep_state, Data#data{rsms = NewRSMs}}
    end.

terminate_proposer(#data{proposer = Proposer} = Data) ->
    case Proposer =:= undefined of
        true ->
            Data;
        false ->
            ok = chronicle_proposer:stop(Proposer),
            unlink(Proposer),
            ?FLUSH({'EXIT', Proposer, _}),

            ?INFO("Proposer ~p stopped", [Proposer]),
            Data#data{proposer = undefined}
    end.

cleanup_after_proposer(#data{commands_batch = CommandsBatch,
                             syncs_batch = SyncsBatch} = Data) ->
    ?FLUSH({proposer_msg, _}),
    {PendingCommands, _} = chronicle_utils:batch_flush(CommandsBatch),
    {PendingSyncs, _} = chronicle_utils:batch_flush(SyncsBatch),
    %% TODO: more detailed error
    Reply = {error, {leader_error, not_leader}},

    lists:foreach(
      fun ({ReplyTo, _Command}) ->
              reply_request(ReplyTo, Reply)
      end, PendingSyncs ++ PendingCommands),

    Data#data{syncs_batch = undefined,
              commands_batch = undefined}.

announce_term_established(#leader{history_id = HistoryId,
                                  term = Term,
                                  status = ready,
                                  seqno = Seqno},
                          Data) ->
    %% RSMs need to be notified first, so they are ready to handle requests by
    %% the time other nodes know about the new leader.
    foreach_rsm(
      fun (Pid) ->
              chronicle_rsm:note_term_established(Pid, HistoryId, Term, Seqno)
      end, Data),

    chronicle_leader:note_term_established(HistoryId, Term).

announce_term_finished(#leader{history_id = HistoryId, term = Term}, Data) ->
    foreach_rsm(
      fun (Pid) ->
              chronicle_rsm:note_term_finished(Pid, HistoryId, Term)
      end, Data),

    chronicle_leader:note_term_finished(HistoryId, Term).

foreach_rsm(Fun, #data{rsms = RSMs}) ->
    maps:fold(
      fun (_MRef, {_Name, Pid}, _) ->
              Fun(Pid)
      end, unused, RSMs).

-ifdef(TEST).

simple_test_() ->
    Nodes = [a, b, c, d],

    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,
     fun () -> simple_test__(Nodes) end}.

setup_vnet(Nodes) ->
    {ok, _} = vnet:start_link(Nodes),
    lists:foreach(
      fun (N) ->
              ok = rpc_node(
                     N, fun () ->
                                application:set_env(chronicle, persist, false),
                                {ok, P} = chronicle_sup:start_link(),
                                unlink(P),
                                ok
                        end)
      end, Nodes).

teardown_vnet(_) ->
    vnet:stop().

simple_test__(Nodes) ->
    Machines = [{kv, chronicle_kv, []}],
    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:provision(Machines)
                  end),

    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:add_voters(Nodes),
                          ok = chronicle:remove_voters([d]),
                          {ok, Voters} = chronicle:get_voters(),
                          ?DEBUG("Voters: ~p", [Voters]),

                          ok = chronicle_failover:failover([a, b])
                  end),

    ok = vnet:disconnect(a, c),
    ok = vnet:disconnect(a, d),
    ok = vnet:disconnect(b, c),
    ok = vnet:disconnect(b, d),

    ok = rpc_node(a,
                  fun () ->
                          {error, {bad_failover, _}} =
                              chronicle_failover:retry_failover(<<"failover">>),
                          ok
                  end),

    ok = rpc_node(b,
                  fun () ->
                          {ok, Rev} = chronicle_kv:add(kv, a, b),
                          {error, {conflict, _}} = chronicle_kv:add(kv, a, c),
                          {ok, {b, _}} = chronicle_kv:get(kv, a),
                          {ok, Rev2} = chronicle_kv:set(kv, a, c, Rev),
                          {error, _} = chronicle_kv:set(kv, a, d, Rev),
                          {ok, _} = chronicle_kv:set(kv, b, d),
                          {error, _} = chronicle_kv:delete(kv, a, Rev),
                          {ok, _} = chronicle_kv:delete(kv, a, Rev2),

                          {error, not_found} = chronicle_kv:get(kv, a,
                                                                #{read_cosistency => quorum}),

                          {ok, _} = chronicle_kv:submit_transaction(
                                         kv, [], [{set, a, 84},
                                                  {set, c, 42}]),
                          {error, {conflict, _}} =
                              chronicle_kv:submit_transaction(kv,
                                                              [{revision, a, Rev2}],
                                                              [{set, a, 1234}]),

                          {ok, _, blah} =
                              chronicle_kv:transaction(
                                kv, [a],
                                fun (#{a := {A, _}}) ->
                                        84 = A,
                                        {commit, [{set, a, A+1},
                                                  {delete, c}], blah}
                                end,
                                #{read_consistency => leader}),

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
                                        chronicle_kv:get_snapshot(kv)
                                end)} || N <- [a, b]]]),

    ok.

rpc_node(Node, Fun) ->
    vnet:rpc(Node, erlang, apply, [Fun, []]).

leader_transfer_test_() ->
    Nodes = [a, b],

    {setup,
     fun () -> setup_vnet(Nodes) end,
     fun teardown_vnet/1,
     {timeout, 10, fun () -> leader_transfer_test__(Nodes) end}}.

leader_transfer_test__(Nodes) ->
    Machines = [{kv, chronicle_kv, []}],
    {ok, Leader} =
        rpc_node(a,
                 fun () ->
                         ok = chronicle:provision(Machines),
                         ok = chronicle:add_voters(Nodes),
                         {L, _} = chronicle_leader:wait_for_leader(),
                         {ok, L}
                 end),

    [OtherNode] = Nodes -- [Leader],

    ok = rpc_node(
           OtherNode,
           fun () ->
                   RPid =
                       spawn_link(fun () ->
                                          ok = chronicle:remove_voters([Leader])
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

-endif.
