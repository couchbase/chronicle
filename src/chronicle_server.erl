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
%% TODO: notify about committed values
%% TODO: timeout after some time when failing to get a quorum
-module(chronicle_server).

-compile(export_all).
-behavior(gen_statem).

-include("chronicle.hrl").

-import(chronicle_utils, [call_async/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(chronicle_utils, [parallel_mapfold/4]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).
-define(COMMANDS_BATCH_AGE, 20).

-record(follower, {}).
-record(leader, { history_id, term }).

%% TODO: reconsider the decision to have proposer run in a separate process
%% TOOD: this record contains fields only used when the state is leader
-record(data, { proposer,
                commands_pending,
                commands_in_flight }).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

%% TODO: think more about what CasRevision should be.
%%
%% Specifically, should it or should it not include a term number.
cas_config(Peer, NewConfig, CasRevision) ->
    gen_statem:call(?SERVER(Peer), {cas_config, NewConfig, CasRevision}).

sync_quorum(Tag, HistoryId, Term) ->
    gen_statem:cast(?SERVER, {sync_quorum, self(), Tag, HistoryId, Term}).

%% Used locally by corresponding chronicle_rsm instance.
rsm_command(HistoryId, Term, RSMName, CommandId, Command) ->
    gen_statem:cast(?SERVER, {rsm_command,
                              HistoryId, Term, RSMName, CommandId, Command}).

%% Meant to only be used by chronicle_proposer.
reply_commands(Pid, Replies0) ->
    %% Ignore replies to commands that didn't request a reply.
    Replies = [Reply || {Ref, _} = Reply <- Replies0, Ref =/= noreply],
    Pid ! {proposer_msg, {reply_commands, Replies}}.

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
handle_event(info, {batch_ready, commands}, State, Data) ->
    handle_batch_ready(State, Data);
handle_event({call, From}, {cas_config, _, _} = Command, State, Data) ->
    handle_leader_command(Command, any, {from, From}, State, Data);
handle_event(cast,
             {rsm_command, HistoryId, Term, RSMName, CommandId, RSMCommand},
             State, Data) ->
    Command = {rsm_command, RSMName, CommandId, RSMCommand},
    handle_leader_command(Command, {HistoryId, Term}, noreply, State, Data);
handle_event(cast, {sync_quorum, Pid, Tag, HistoryId, Term}, State, Data) ->
    %% TODO: shouldn't be batched, or should be batched separately from RSM
    %% commands
    ReplyTo = {send, Pid, Tag},
    handle_leader_command(sync_quorum, {HistoryId, Term}, ReplyTo, State, Data);
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

handle_state_leave(#leader{}, Data) ->
    cleanup_after_proposer(terminate_proposer(Data));
handle_state_leave(_OldState, Data) ->
    Data.

handle_state_enter(#leader{history_id = HistoryId, term = Term}, Data) ->
    {ok, Proposer} = chronicle_proposer:start_link(HistoryId, Term),

    undefined = Data#data.commands_pending,
    CommandsBatch = chronicle_utils:make_batch(commands, ?COMMANDS_BATCH_AGE),
    Data#data{proposer = Proposer,
              commands_pending = CommandsBatch,
              commands_in_flight = #{}};
handle_state_enter(_State, Data) ->
    Data.

handle_chronicle_event({leader, LeaderInfo}, State, Data) ->
    handle_new_leader(LeaderInfo, State, Data).

handle_new_leader(no_leader, _State, Data) ->
    {next_state, #follower{}, Data};
handle_new_leader({Leader, HistoryId, Term}, _State, Data) ->
    case Leader =:= ?PEER() of
        true ->
            {next_state, #leader{history_id = HistoryId, term = Term}, Data};
        false ->
            {next_state, #follower{}, Data}
    end.

handle_process_exit(Pid, Reason, State, #data{proposer = Proposer} = Data) ->
    case Pid =:= Proposer of
        true ->
            ?INFO("Proposer terminated with reason ~p", [Reason]),
            announce_term_failed(State),
            {next_state, #follower{}, Data#data{proposer = undefined}};
        false ->
            {stop, {linked_process_died, Pid, Reason}}
    end.

handle_proposer_msg({reply_commands, Replies}, State, Data) ->
    handle_reply_commands(Replies, State, Data).

handle_reply_commands(Replies, #leader{},
                      #data{commands_in_flight = InFlight} = Data) ->
    NewInFlight =
        lists:foldl(
          fun ({Ref, Reply}, Acc) ->
                  {ReplyTo, NewAcc} = maps:take(Ref, Acc),
                  reply_command(ReplyTo, Reply),
                  NewAcc
          end, InFlight, Replies),

    {keep_state, Data#data{commands_in_flight = NewInFlight}}.

reply_command(ReplyTo, Reply) ->
    case ReplyTo of
        noreply ->
            ok;
        {from, From} ->
            gen_statem:reply(From, Reply);
        {send, Pid, Tag} ->
            Pid ! {Tag, Reply}
    end.

handle_leader_command(_Command, _, ReplyTo, #follower{}, _Data) ->
    %% TODO
    reply_command(ReplyTo, {error, not_leader}),
    keep_state_and_data;
handle_leader_command(Command, HistoryAndTerm, ReplyTo,
                      #leader{history_id = OurHistoryId, term = OurTerm},
                      #data{commands_pending = Commands} = Data) ->
    case HistoryAndTerm =:= any
        orelse HistoryAndTerm =:= {OurHistoryId, OurTerm} of
        true ->
            NewCommands =
                chronicle_utils:batch_enq({ReplyTo, Command}, Commands),
            {keep_state, Data#data{commands_pending = NewCommands}};
        false ->
            reply_command(ReplyTo, {error, not_leader})
    end.

handle_batch_ready(#leader{}, #data{commands_pending = Batch} = Data) ->
    {Commands, NewBatch} = chronicle_utils:batch_flush(Batch),
    true = (Commands =/= []),
    {keep_state,
     deliver_commands(Commands, Data#data{commands_pending = NewBatch})}.

deliver_commands(Commands, #data{proposer = Proposer,
                                 commands_in_flight = InFlight} = Data) ->
    {ProcessedCommands, NewInFlight} =
        lists:foldl(
          fun ({ReplyTo, Command}, {AccCommands, AccInFlight}) ->
                  case ReplyTo of
                      noreply ->
                          %% Since this command doesn't require a
                          %% reply, don't store it in
                          %% command_in_flight.
                          NewAccCommands = [{noreply, Command} | AccCommands],
                          {NewAccCommands, AccInFlight};
                      _ ->
                          Ref = make_ref(),
                          NewAccCommands = [{Ref, Command} | AccCommands],
                          NewAccInFlight = AccInFlight#{Ref => ReplyTo},
                          {NewAccCommands, NewAccInFlight}
                  end
          end, {[], InFlight}, Commands),

    chronicle_proposer:deliver_commands(Proposer, ProcessedCommands),
    Data#data{commands_in_flight = NewInFlight}.

terminate_proposer(#data{proposer = Proposer} = Data) ->
    case Proposer =:= undefined of
        true ->
            Data;
        false ->
            chronicle_utils:terminate_linked_process(Proposer, kill),
            Data#data{proposer = undefined}
    end.

cleanup_after_proposer(#data{commands_pending = PendingBatch,
                             commands_in_flight = InFlight} = Data) ->
    ?FLUSH({proposer_msg, _}),
    {PendingCommands, _} = chronicle_utils:batch_flush(PendingBatch),
    Reply = {error, leader_gone},

    lists:foreach(
      fun ({ReplyTo, _Command}) ->
              reply_command(ReplyTo, Reply)
      end, PendingCommands),

    lists:foreach(
      fun ({_Ref, ReplyTo}) ->
              reply_command(ReplyTo, Reply)
      end, maps:to_list(InFlight)),

    Data#data{commands_in_flight = #{},
              commands_pending = undefined}.

announce_term_failed(#leader{history_id = HistoryId, term = Term}) ->
    chronicle_events:notify({term_failed, HistoryId, Term}).

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

    Machines = #{kv => #rsm_config{module = chronicle_kv}},
    rpc_node(a,
             fun () ->
                     ok = chronicle_agent:provision(
                            <<"history">>, {1, ''},
                            #config{voters = [a],
                                    state_machines = Machines})
             end),

    timer:sleep(1000),

    {ok, _} = chronicle_server:cas_config(a,
                                          #config{voters = Nodes,
                                                  state_machines = Machines},
                                          {<<"history">>, {1, ''}, 1}),

    timer:sleep(1000),

    ok = chronicle_failover:failover(a, <<"failover">>, [a, b]),

    timer:sleep(2000),

    ok = vnet:disconnect(a, c),
    ok = vnet:disconnect(a, d),
    ok = vnet:disconnect(b, c),
    ok = vnet:disconnect(b, d),

    {error, {bad_failover, _}} =
        chronicle_failover:retry_failover(a, <<"failover">>),

    ok = rpc_node(b,
                  fun () ->
                          {ok, Rev} = chronicle_kv:set(kv, a, b),
                          ok = chronicle_rsm:sync_revision(kv, Rev, 10000),
                          {ok, b} = chronicle_kv:get(kv, a),
                          {ok, _} = chronicle_kv:set(kv, a, c),
                          {ok, _} = chronicle_kv:set(kv, b, d),
                          ok = chronicle_kv:delete(kv, a),

                          ok = chronicle_rsm:sync(kv, leader, 10000),
                          ok = chronicle_rsm:sync(kv, quorum, 10000),
                          error = chronicle_kv:get(kv, a),

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
