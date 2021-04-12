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

-behavior(gen_statem).

-include("chronicle.hrl").

-export([start_link/0]).
-export([register_rsm/2,
         get_config/2, get_cluster_info/2, cas_config/4, check_quorum/2,
         sync_quorum/3, rsm_command/3, rsm_command/4,
         proposer_ready/3, proposer_stopping/2, reply_request/2]).

-export([callback_mode/0,
         format_status/2, sanitize_event/2,
         init/1, handle_event/4, terminate/3]).

-import(chronicle_utils, [call/3, sanitize_reason/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([debug_log/4]).
-endif.

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).
-define(COMMANDS_BATCH_AGE,
        chronicle_settings:get({proposer, commands_batch_age}, 20)).
-define(SYNCS_BATCH_AGE,
        chronicle_settings:get({proposer, syncs_batch_age}, 5)).

-record(no_leader, {}).
-record(follower, { leader, history_id, term }).
-record(leader, { history_id, term, status }).

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
    leader_query(Leader, get_config, Timeout).

get_cluster_info(Leader, Timeout) ->
    leader_query(Leader, get_cluster_info, Timeout).

leader_query(Leader, Query, Timeout) ->
    call(?SERVER(Leader), {leader_query, Query}, Timeout).

cas_config(Leader, NewConfig, CasRevision, Timeout) ->
    call(?SERVER(Leader), {cas_config, NewConfig, CasRevision}, Timeout).

check_quorum(Leader, Timeout) ->
    try call(?SERVER(Leader), check_quorum, Timeout) of
        {ok, _Revision} ->
            ok;
        Other ->
            Other
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

sync_quorum(Tag, HistoryId, Term) ->
    gen_statem:cast(?SERVER, {sync_quorum, self(), Tag, HistoryId, Term}).

%% Used locally by corresponding chronicle_rsm instance.
rsm_command(HistoryId, Term, Command) ->
    gen_statem:cast(?SERVER, {rsm_command, HistoryId, Term, Command}).

rsm_command(Tag, HistoryId, Term, Command) ->
    gen_statem:cast(?SERVER,
                    {rsm_command, self(), Tag, HistoryId, Term, Command}).

%% Meant to only be used by chronicle_proposer.
proposer_ready(Pid, HistoryId, Term) ->
    Pid ! {proposer_msg, {proposer_ready, HistoryId, Term}},
    ok.

proposer_stopping(Pid, Reason) ->
    Pid ! {proposer_msg, {proposer_stopping, Reason}},
    ok.

reply_request(ReplyTo, Reply) ->
    case ReplyTo of
        noreply ->
            ok;
        {from, From} ->
            gen_statem:reply(From, Reply);
        {send, Pid, Tag} ->
            Pid ! {Tag, Reply},
            ok;
        {many, ReplyTos} ->
            lists:foreach(
              fun (To) ->
                      reply_request(To, Reply)
              end, ReplyTos)
    end.

%% gen_server callbacks
callback_mode() ->
    [handle_event_function, state_enter].

format_status(Opt, [_PDict, State, Data]) ->
    case Opt of
        normal ->
            [{data, [{"State", {State, Data}}]}];
        terminate ->
            {State,
             case Data of
                 #data{} ->
                     sanitize_data(Data);
                 _ ->
                     %% During gen_statem initialization Data may be undefined.
                     Data
             end}
    end.

sanitize_event(cast, {rsm_command, Pid, Tag, HistoryId, Term,
                      #rsm_command{payload = {command, _}} = Command}) ->
    {cast, {rsm_command, Pid, Tag, HistoryId, Term,
            Command#rsm_command{payload = {command, '...'}}}};
sanitize_event(Type, Event) ->
    {Type, Event}.

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

    {ok, #no_leader{}, #data{}}.

handle_event(enter, OldState, State, Data) ->
    %% RSMs need to be notified about leader status changes before
    %% chronicle_leader exposes the leader to other nodes.
    announce_leader_status(State, Data),
    NewData0 = handle_state_leave(OldState, State, Data),
    NewData = handle_state_enter(OldState, State, NewData0),
    {keep_state, NewData};
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
handle_event({call, From}, {leader_query, Query}, State, Data) ->
    handle_leader_query(Query, From, State, Data);
handle_event({call, From}, {cas_config, NewConfig, Revision}, State, Data) ->
    handle_cas_config(NewConfig, Revision, From, State, Data);
handle_event({call, From}, {register_rsm, Name, Pid}, State, Data) ->
    handle_register_rsm(Name, Pid, From, State, Data);
handle_event({call, From}, check_quorum, State, Data) ->
    handle_check_quorum(From, State, Data);
handle_event(cast, {rsm_command, HistoryId, Term, RSMCommand}, State, Data) ->
    handle_rsm_command(HistoryId, Term, RSMCommand, noreply, State, Data);
handle_event(cast,
             {rsm_command, Pid, Tag, HistoryId, Term, RSMCommand},
             State, Data) ->
    ReplyTo = {send, Pid, Tag},
    handle_rsm_command(HistoryId, Term, RSMCommand, ReplyTo, State, Data);
handle_event(cast, {sync_quorum, Pid, Tag, HistoryId, Term}, State, Data) ->
    ReplyTo = {send, Pid, Tag},
    batch_leader_request(sync_quorum, {HistoryId, Term},
                         ReplyTo, #data.syncs_batch, State, Data);
handle_event({call, From}, _Call, _State, _Data) ->
    {keep_state_and_data, {reply, From, nack}};
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event ~w", [{Type, Event}]),
    keep_state_and_data.

terminate(_Reason, State, Data) ->
    handle_event(enter, State, #no_leader{}, Data).

%% internal
is_interesting_event({leader_status, _}) ->
    true;
is_interesting_event(_) ->
    false.

same_state(OldState, NewState) ->
    %% Don't generate state events if everything that changes is leader status.
    case {OldState, NewState} of
        {#leader{history_id = HistoryId, term = Term},
         #leader{history_id = HistoryId, term = Term}} ->
            true;
        _ ->
            %% Otherwise rely on gen_statem default logic of comparing states
            %% structurally.
            OldState =:= NewState
    end.

handle_state_leave(OldState, State, Data) ->
    case OldState of
        #leader{} ->
            case same_state(OldState, State) of
                true ->
                    %% Only status has changed, nothing to cleanup.
                    Data;
                false ->
                    handle_leader_leave(OldState, Data)
            end;
        _ ->
            Data
    end.

handle_leader_leave(#leader{status = Status} = State, Data) ->
    case Status of
        ready ->
            announce_term_finished(State);
        not_ready ->
            ok
    end,

    cleanup_after_proposer(terminate_proposer(Data)).

handle_state_enter(_OldState, State, Data) ->
    case State of
        #leader{status = not_ready} ->
            handle_leader_enter(State, Data);
        #leader{status = ready} ->
            announce_term_established(State),
            Data;
        _ ->
            Data
    end.

handle_leader_enter(#leader{history_id = HistoryId, term = Term}, Data) ->
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
    NewData.

handle_chronicle_event({leader_status, LeaderInfo}, State, Data) ->
    handle_leader_status(LeaderInfo, State, Data).

handle_leader_status(no_leader, _State, Data) ->
    {next_state, #no_leader{}, Data};
handle_leader_status({follower, LeaderInfo}, _State, Data) ->
    #{leader := Leader,
      history_id := HistoryId,
      term := Term,
      status := Status} = LeaderInfo,

    case Status of
        established ->
            {next_state, #follower{leader = Leader,
                                   history_id = HistoryId,
                                   term = Term},
             Data};
        tentative ->
            {next_state, #no_leader{}, Data}
    end;
handle_leader_status({leader, LeaderInfo}, State, Data) ->
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

handle_process_exit(Pid, Reason, State, #data{proposer = Proposer} = Data) ->
    case Pid =:= Proposer of
        true ->
            handle_proposer_exit(Pid, Reason, State, Data);
        false ->
            {stop, {linked_process_died, Pid, Reason}}
    end.

handle_proposer_exit(Pid, Reason, #leader{status = Status}, Data) ->
    ?INFO("Proposer ~w terminated:~n~p",
          [Pid, sanitize_reason(Reason)]),

    NewData = Data#data{proposer = undefined},
    case Status of
        not_ready ->
            %% The proposer terminated before fully initializing, and before
            %% any requests were sent to it. This is not abnormal and may
            %% happen if the proposer fails to get enough votes when
            %% establishing the term. There's no reason to terminate
            %% chronicle_server in such case.
            {next_state, #no_leader{}, NewData};
        ready ->
            %% Some requests may have never been processed by the proposer and
            %% the only way to indicate to the callers that something went
            %% wrong is to terminate chronicle_server.
            {stop, proposer_terminated, NewData}
    end.

handle_proposer_msg({proposer_ready, HistoryId, Term}, State, Data) ->
    handle_proposer_ready(HistoryId, Term, State, Data);
handle_proposer_msg({proposer_stopping, Reason}, State, Data) ->
    handle_proposer_stopping(Reason, State, Data).

handle_proposer_ready(HistoryId, Term,
                      #leader{history_id = HistoryId,
                              term = Term,
                              status = not_ready} = State,
                      Data) ->
    NewState = State#leader{status = ready},
    {next_state, NewState, Data}.

handle_proposer_stopping(Reason,
                         #leader{history_id = HistoryId, term = Term},
                         #data{proposer = Proposer} = Data) ->
    ?INFO("Proposer ~w for term ~w in history ~p is terminating:~n~p",
          [Proposer, Term, HistoryId, sanitize_reason(Reason)]),
    {next_state, #no_leader{}, Data}.

reply_not_leader(ReplyTo) ->
    reply_request(ReplyTo, {error, {leader_error, not_leader}}).

handle_leader_request(HistoryAndTerm, _ReplyTo,
                      #leader{history_id = OurHistoryId, term = OurTerm,
                              status = ready},
                      Fun)
  when HistoryAndTerm =:= any;
       HistoryAndTerm =:= {OurHistoryId, OurTerm} ->
    Fun();
handle_leader_request(_HistoryAndTerm, ReplyTo, _State, _Fun) ->
    reply_not_leader(ReplyTo),
    keep_state_and_data.

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

handle_batch_ready(BatchField, #leader{status = ready}, Data) ->
    {keep_state, deliver_batch(BatchField, Data)}.

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

handle_leader_query(Query, From, State, Data) ->
    ReplyTo = {from, From},
    handle_leader_request(
      any, ReplyTo, State,
      fun () ->
              deliver_leader_query(Query, ReplyTo, Data),
              keep_state_and_data
      end).

deliver_leader_query(Query, ReplyTo, #data{proposer = Proposer}) ->
    chronicle_proposer:query(Proposer, ReplyTo, Query).

handle_cas_config(NewConfig, Revision, From, State, Data) ->
    ReplyTo = {from, From},
    handle_leader_request(
      any, ReplyTo, State,
      fun () ->
              deliver_cas_config(NewConfig, Revision, ReplyTo, Data),
              keep_state_and_data
      end).

deliver_cas_config(NewConfig, Revision, ReplyTo, #data{proposer = Proposer}) ->
    chronicle_proposer:cas_config(Proposer, ReplyTo, NewConfig, Revision).

handle_register_rsm(Name, Pid, From, State, #data{rsms = RSMs} = Data) ->
    ?DEBUG("Registering RSM ~w with pid ~w", [Name, Pid]),
    MRef = erlang:monitor(process, Pid),
    NewRSMs = maps:put(MRef, {Name, Pid}, RSMs),
    NewData = Data#data{rsms = NewRSMs},
    {keep_state, NewData, {reply, From, leader_status(State)}}.

announce_leader_status(State, Data) ->
    Status = leader_status(State),
    foreach_rsm(
      fun (Pid) ->
              chronicle_rsm:note_leader_status(Pid, Status)
      end, Data).

leader_status(#no_leader{}) ->
    no_leader;
leader_status(#follower{leader = Leader,
                        history_id = HistoryId, term = Term}) ->
    {follower, Leader, HistoryId, Term};
leader_status(#leader{history_id = HistoryId, term = Term, status = Status}) ->
    case Status of
        ready ->
            {leader, HistoryId, Term};
        not_ready ->
            no_leader
    end.

handle_check_quorum(From, State, #data{proposer = Proposer}) ->
    ReplyTo = {from, From},
    handle_leader_request(
      any, ReplyTo, State,
      fun () ->
              chronicle_proposer:sync_quorum(Proposer, ReplyTo),
              keep_state_and_data
      end).

handle_process_down(MRef, Pid, Reason, _State, #data{rsms = RSMs} = Data) ->
    case maps:take(MRef, RSMs) of
        error ->
            {stop, {unexpected_process_down, MRef, Pid, Reason}};
        {{Name, RSMPid}, NewRSMs} ->
            true = (Pid =:= RSMPid),
            ?DEBUG("RSM ~w~w terminated", [Name, RSMPid]),
            {keep_state, Data#data{rsms = NewRSMs}}
    end.

handle_rsm_command(HistoryId, Term, RSMCommand, ReplyTo, State, Data) ->
    Command = {rsm_command, RSMCommand},
    batch_leader_request(Command, {HistoryId, Term}, ReplyTo,
                         #data.commands_batch, State, Data).

terminate_proposer(#data{proposer = Proposer} = Data) ->
    case Proposer =:= undefined of
        true ->
            Data;
        false ->
            ok = chronicle_proposer:stop(Proposer),
            receive
                {'EXIT', Proposer, _} ->
                    ok
            after
                5000 ->
                    exit({proposer_failed_to_terminate, Proposer})
            end,

            ?INFO("Proposer ~w stopped", [Proposer]),
            Data#data{proposer = undefined}
    end.

cleanup_after_proposer(#data{commands_batch = CommandsBatch,
                             syncs_batch = SyncsBatch} = Data) ->
    ?FLUSH({proposer_msg, _}),
    {PendingCommands, _} = chronicle_utils:batch_flush(CommandsBatch),
    {PendingSyncs, _} = chronicle_utils:batch_flush(SyncsBatch),

    lists:foreach(
      fun ({ReplyTo, _Command}) ->
              reply_not_leader(ReplyTo)
      end, PendingSyncs ++ PendingCommands),

    Data#data{syncs_batch = undefined,
              commands_batch = undefined}.

announce_term_established(#leader{history_id = HistoryId,
                                  term = Term,
                                  status = ready}) ->
    chronicle_leader:note_term_established(HistoryId, Term).

announce_term_finished(#leader{history_id = HistoryId, term = Term}) ->
    chronicle_leader:note_term_finished(HistoryId, Term).

foreach_rsm(Fun, #data{rsms = RSMs}) ->
    chronicle_utils:maps_foreach(
      fun (_MRef, {_Name, Pid}) ->
              Fun(Pid)
      end, RSMs).

sanitize_data(#data{commands_batch = undefined} = Data) ->
    Data;
sanitize_data(#data{commands_batch = Commands} = Data) ->
    SanitizedCommands =
        chronicle_utils:batch_map(
          fun (Requests) ->
                  [{ReplyTo, {rsm_command, ommitted}} ||
                      {ReplyTo, {rsm_command, _}} <- Requests]
          end, Commands),

    Data#data{commands_batch = SanitizedCommands}.

-ifdef(TEST).

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
    lists:foreach(
      fun (N) ->
              ok = rpc_node(
                     N, fun () ->
                                prepare_vnode_dir(),
                                chronicle_env:set_env(logger_function,
                                                      {?MODULE, debug_log}),
                                _ = application:load(chronicle),
                                ok = chronicle_env:setup(),

                                {ok, P} = chronicle_sup:start_link(),
                                unlink(P),
                                ok
                        end)
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

teardown_vnet(_) ->
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
                          {ok, Voters} = chronicle:get_voters(),
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
                                     chronicle_kv:set(kv, b, 43)
                             end)
                   end),

    ok = rpc_node(b,
                  fun () ->
                          {ok, _} = chronicle_kv:set(kv, c, 44),
                          ok
                  end),

    chronicle_utils:terminate_and_wait(Pid, shutdown),

    [ok = vnet:connect(a, N) || N <- OtherNodes],

    ok = rpc_node(a,
                  fun () ->
                          ok = chronicle_kv:sync(kv, 10000),
                          {ok, {42, _}} = chronicle_kv:get(kv, a),
                          {error, not_found} = chronicle_kv:get(kv, b),
                          {ok, {44, _}} = chronicle_kv:get(kv, c),
                          ok
                  end),

    ok.

-endif.
