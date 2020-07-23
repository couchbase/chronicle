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
-record(leader, { history_id, term }).

%% TODO: reconsider the decision to have proposer run in a separate process
%% TOOD: this record contains fields only used when the state is leader
-record(data, { proposer,
                proposer_ready,
                commands_batch,
                syncs_batch,
                requests_in_flight }).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

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
rsm_command(HistoryId, Term, RSMName, CommandId, Command) ->
    gen_statem:cast(?SERVER, {rsm_command,
                              HistoryId, Term, RSMName, CommandId, Command}).

%% Meant to only be used by chronicle_proposer.
proposer_ready(Pid, HistoryId, Term, HighSeqno) ->
    Pid ! {proposer_msg, {proposer_ready, HistoryId, Term, HighSeqno}}.

reply_requests(Pid, Replies0) ->
    %% Ignore replies to commands that didn't request a reply.
    Replies = [Reply || {Ref, _} = Reply <- Replies0, Ref =/= noreply],
    Pid ! {proposer_msg, {reply_requests, Replies}}.

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

    chronicle_leader:announce_leader(),
    {ok, #follower{}, #data{}}.

handle_event(enter, OldState, State, Data) ->
    LeaveData = handle_state_leave(OldState, Data),
    EnterData = handle_state_enter(State, LeaveData),
    {keep_state, EnterData};
handle_event(info, {chronicle_event, Event}, State, Data) ->
    handle_chronicle_event(Event, State, Data);
handle_event(info, {'EXIT', Pid, Reason}, State, Data) ->
    handle_process_exit(Pid, Reason, State, Data);
handle_event(info, {proposer_msg, Msg}, #leader{} = State, Data) ->
    handle_proposer_msg(Msg, State, Data);
handle_event(info, {batch_ready, BatchField}, State, Data) ->
    handle_batch_ready(BatchField, State, Data);
handle_event({call, From}, get_config, State, Data) ->
    handle_get_config(From, State, Data);
handle_event({call, From}, {cas_config, NewConfig, Revision}, State, Data) ->
    handle_cas_config(NewConfig, Revision, From, State, Data);
handle_event(cast,
             {rsm_command, HistoryId, Term, RSMName, CommandId, RSMCommand},
             State, Data) ->
    Command = {rsm_command, RSMName, CommandId, RSMCommand},
    batch_leader_request(Command, {HistoryId, Term}, noreply,
                         #data.commands_batch, State, Data);
handle_event(cast, {sync_quorum, Pid, Tag, HistoryId, Term}, State, Data) ->
    %% TODO: shouldn't be batched, or should be batched separately from RSM
    %% commands
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
is_interesting_event({leader, _}) ->
    true;
is_interesting_event(_) ->
    false.

handle_state_leave(#leader{} = State, Data) ->
    announce_term_finished(State),
    cleanup_after_proposer(terminate_proposer(Data));
handle_state_leave(_OldState, Data) ->
    Data.

handle_state_enter(#leader{history_id = HistoryId,
                           term = Term} = State,
                   Data) ->
    {ok, Proposer} = chronicle_proposer:start_link(HistoryId, Term),

    undefined = Data#data.commands_batch,
    undefined = Data#data.syncs_batch,

    CommandsBatch =
        chronicle_utils:make_batch(#data.commands_batch, ?COMMANDS_BATCH_AGE),
    SyncsBatch =
        chronicle_utils:make_batch(#data.syncs_batch, ?SYNCS_BATCH_AGE),
    NewData = Data#data{proposer = Proposer,
                        proposer_ready = false,
                        commands_batch = CommandsBatch,
                        syncs_batch = SyncsBatch,
                        requests_in_flight = #{}},
    NewData;
handle_state_enter(_State, Data) ->
    Data.

handle_chronicle_event({leader, LeaderInfo}, State, Data) ->
    handle_new_leader(LeaderInfo, State, Data).

handle_new_leader(no_leader, _State, Data) ->
    {next_state, #follower{}, Data};
handle_new_leader(LeaderInfo, _State, Data) ->
    #{leader := Leader, history_id := HistoryId, term := Term} = LeaderInfo,

    case Leader =:= ?PEER() of
        true ->
            {next_state, #leader{history_id = HistoryId, term = Term}, Data};
        false ->
            {next_state, #follower{}, Data}
    end.

handle_process_exit(Pid, Reason, _State, #data{proposer = Proposer} = Data) ->
    case Pid =:= Proposer of
        true ->
            ?INFO("Proposer terminated with reason ~p", [Reason]),
            {next_state, #follower{}, Data#data{proposer = undefined,
                                                proposer_ready = false}};
        false ->
            {stop, {linked_process_died, Pid, Reason}}
    end.

handle_proposer_msg({proposer_ready, HistoryId, Term, HighSeqno},
                    State, Data) ->
    handle_proposer_ready(HistoryId, Term, HighSeqno, State, Data);
handle_proposer_msg({reply_requests, Replies}, State, Data) ->
    handle_reply_requests(Replies, State, Data).

handle_proposer_ready(HistoryId, Term, HighSeqno,
                      #leader{history_id = HistoryId, term = Term} = State,
                      #data{proposer_ready = false} = Data) ->
    NewData = Data#data{proposer_ready = true},
    announce_term_established(HighSeqno, State, NewData),
    {keep_state, NewData}.

handle_reply_requests(Replies, #leader{},
                      #data{requests_in_flight = InFlight} = Data) ->
    NewInFlight =
        lists:foldl(
          fun ({Ref, Reply}, Acc) ->
                  {ReplyTo, NewAcc} = maps:take(Ref, Acc),
                  reply_request(ReplyTo, Reply),
                  NewAcc
          end, InFlight, Replies),

    {keep_state, Data#data{requests_in_flight = NewInFlight}}.

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

handle_batch_ready(BatchField, #leader{}, Data) ->
    {keep_state, deliver_batch(BatchField, Data)}.

update_batch(BatchField, Data, Fun) ->
    Batch = element(BatchField, Data),
    NewBatch = Fun(Batch),
    setelement(BatchField, Data, NewBatch).

deliver_batch(BatchField, Data) ->
    Batch = element(BatchField, Data),
    {Requests, NewBatch} = chronicle_utils:batch_flush(Batch),
    NewData = setelement(BatchField, Data, NewBatch),
    deliver_requests(Requests, BatchField, NewData).

deliver_requests([], _Batch, Data) ->
    Data;
deliver_requests(Requests, Batch, Data) ->
    case Batch of
        #data.syncs_batch ->
            deliver_syncs(Requests, Data);
        #data.commands_batch ->
            deliver_commands(Requests, Data)
    end.

deliver_syncs(Syncs, #data{proposer = Proposer} = Data) ->
    Ref = make_ref(),
    chronicle_proposer:sync_quorum(Proposer, Ref),

    {ReplyTos, _} = lists:unzip(Syncs),
    store_request(Ref, {many, ReplyTos}, Data).

deliver_commands(Commands, #data{proposer = Proposer} = Data) ->
    %% All rsm commands don't require a reply. So we don't need to store them.
    BareCommands = [Command || {_From, Command} <- Commands],
    chronicle_proposer:append_commands(Proposer, BareCommands),
    Data.

store_request(Ref, ReplyTo, Data) ->
    store_requests([{Ref, ReplyTo}], Data).

store_requests(Requests, #data{requests_in_flight = InFlight} = Data) ->
    NewInFlight =
        lists:foldl(
          fun ({Ref, ReplyTo}, Acc) ->
                  true = (ReplyTo =/= noreply),
                  Acc#{Ref => ReplyTo}
          end, InFlight, Requests),

    Data#data{requests_in_flight = NewInFlight}.

handle_get_config(From, State, Data) ->
    ReplyTo = {from, From},
    handle_leader_request(
      any, ReplyTo, State,
      fun () ->
              {keep_state, deliver_get_config(ReplyTo, Data)}
      end).

deliver_get_config(ReplyTo, #data{proposer = Proposer} = Data) ->
    Ref = make_ref(),
    chronicle_proposer:get_config(Proposer, Ref),
    store_request(Ref, ReplyTo, Data).

handle_cas_config(NewConfig, Revision, From, State, Data) ->
    ReplyTo = {from, From},
    handle_leader_request(
      any, ReplyTo, State,
      fun () ->
              NewData = deliver_cas_config(NewConfig, Revision, ReplyTo, Data),
              {keep_state, NewData}
      end).

deliver_cas_config(NewConfig, Revision, ReplyTo,
                   #data{proposer = Proposer} = Data) ->
    Ref = make_ref(),
    chronicle_proposer:cas_config(Proposer, Ref, NewConfig, Revision),
    store_request(Ref, ReplyTo, Data).

terminate_proposer(#data{proposer = Proposer} = Data) ->
    case Proposer =:= undefined of
        true ->
            Data;
        false ->
            chronicle_utils:terminate_linked_process(Proposer, kill),
            Data#data{proposer = undefined}
    end.

cleanup_after_proposer(#data{commands_batch = CommandsBatch,
                             syncs_batch = SyncsBatch,
                             requests_in_flight = InFlight} = Data) ->
    ?FLUSH({proposer_msg, _}),
    {PendingCommands, _} = chronicle_utils:batch_flush(CommandsBatch),
    {PendingSyncs, _} = chronicle_utils:batch_flush(SyncsBatch),
    %% TODO: more detailed error
    Reply = {error, {leader_error, leader_lost}},

    lists:foreach(
      fun ({ReplyTo, _Command}) ->
              reply_request(ReplyTo, Reply)
      end, PendingSyncs ++ PendingCommands),

    lists:foreach(
      fun ({_Ref, ReplyTo}) ->
              reply_request(ReplyTo, Reply)
      end, maps:to_list(InFlight)),

    Data#data{requests_in_flight = #{},
              syncs_batch = undefined,
              commands_batch = undefined}.

announce_term_established(HighSeqno,
                          #leader{history_id = HistoryId, term = Term},
                          #data{proposer_ready = true}) ->
    chronicle_leader:note_term_established(HistoryId, Term, HighSeqno).

announce_term_finished(#leader{history_id = HistoryId, term = Term}) ->
    chronicle_leader:note_term_finished(HistoryId, Term).

-ifdef(TEST).

simple_test_() ->
    {timeout, 10, fun simple_test__/0}.

simple_test__() ->
    Nodes = [a, b, c, d],
    {ok, _} = vnet:start_link(Nodes),
    lists:foreach(
      fun (N) ->
              rpc_node(N, fun () ->
                                  {ok, P} = chronicle_sup:start_link(),
                                  unlink(P)
                          end)
      end, Nodes),

    Machines = [{kv, chronicle_kv, []}],
    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:provision(Machines)
                  end),

    timer:sleep(1000),

    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle:add_voters(Nodes),
                          ok = chronicle:remove_voters([d]),
                          {ok, Voters} = chronicle:get_voters(),
                          ?DEBUG("Voters: ~p", [Voters]),
                          ok
                  end),

    ok = chronicle_failover:failover(a, <<"failover">>, [a, b]),

    ok = vnet:disconnect(a, c),
    ok = vnet:disconnect(a, d),
    ok = vnet:disconnect(b, c),
    ok = vnet:disconnect(b, d),

    {error, {bad_failover, _}} =
        chronicle_failover:retry_failover(a, <<"failover">>),

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

                          {ok, _} =
                              chronicle_kv:transaction(
                                kv, [a],
                                fun (#{a := {A, _}}) ->
                                        84 = A,
                                        {commit, [{set, a, A+1},
                                                  {delete, c}]}
                                end,
                                #{read_consistency => leader}),

                          {ok, _} = chronicle_kv:update(kv, a, fun (V) -> V+1 end),
                          {ok, {86, _}} = chronicle_kv:get(kv, a),

                          {ok, _} = chronicle_kv:set(kv, c, 1234),
                          {ok, _} = chronicle_kv:set(kv, d, 4321),
                          {ok, _} =
                              chronicle_kv:rewrite(
                                kv,
                                fun (Key, _Value) ->
                                        case Key of
                                            a ->
                                                {update, 87};
                                            c ->
                                                delete;
                                            _ ->
                                                keep
                                        end
                                end),

                          {error, not_found} = chronicle_kv:get(kv, c),

                          ok
                  end),

    timer:sleep(1000),


    ?debugFmt("~nStates:~n~p~n",
              [[{N, chronicle_agent:get_metadata(N)} || N <- Nodes]]),

    ?debugFmt("~nLogs:~n~p~n",
              [[{N, chronicle_agent:get_log(N)} || N <- Nodes]]),

    ?debugFmt("~nKV snapshots:~n~p~n",
              [[{N, rpc_node(N, fun () -> chronicle_kv:get_snapshot(kv) end)} || N <- [a, b]]]),

    ok.

rpc_node(Node, Fun) ->
    vnet:rpc(Node, erlang, apply, [Fun, []]).

-endif.
