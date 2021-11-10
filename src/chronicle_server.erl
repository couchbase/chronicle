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
-export([register_rsm/2, cas_config/2, sync_quorum/3, rsm_command/3,
         proposer_ready/3, proposer_stopping/2, reply_request/2]).

-export([callback_mode/0,
         format_status/2, sanitize_event/2,
         init/1, handle_event/4, terminate/3]).

-import(chronicle_utils, [call/3, sanitize_reason/1]).

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

cas_config(NewConfig, CasRevision) ->
    gen_statem:cast(?SERVER, {cas_config, NewConfig, CasRevision}).

sync_quorum(Tag, HistoryId, Term) ->
    gen_statem:cast(?SERVER, {sync_quorum, self(), Tag, HistoryId, Term}).

%% Used locally by corresponding chronicle_rsm instance.
rsm_command(HistoryId, Term, Command) ->
    gen_statem:cast(?SERVER, {rsm_command, HistoryId, Term, Command}).

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

sanitize_event(cast, {rsm_command, HistoryId, Term,
                      #rsm_command{payload = {command, _}} = Command}) ->
    {cast, {rsm_command, HistoryId, Term,
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
handle_event({call, From}, {register_rsm, Name, Pid}, State, Data) ->
    handle_register_rsm(Name, Pid, From, State, Data);
handle_event(cast, {cas_config, Config, Revision}, State, Data) ->
    handle_cas_config(Config, Revision, State, Data);
handle_event(cast, {rsm_command, HistoryId, Term, RSMCommand}, State, Data) ->
    handle_rsm_command(HistoryId, Term, RSMCommand, State, Data);
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

handle_leader_leave(#leader{} = State, Data) ->
    announce_term_finished(State),
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

handle_proposer_exit(Pid, Reason, #leader{}, Data) ->
    ?INFO("Proposer ~w terminated:~n~p", [Pid, sanitize_reason(Reason)]),
    {next_state, #no_leader{}, Data#data{proposer = undefined}}.

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
    {_, ActualCommands} = lists:unzip(Commands),
    chronicle_proposer:append_commands(Proposer, ActualCommands).

handle_cas_config(NewConfig, Revision, State, Data) ->
    handle_leader_request(
      any, noreply, State,
      fun () ->
              deliver_cas_config(NewConfig, Revision, Data),
              keep_state_and_data
      end).

deliver_cas_config(NewConfig, Revision, #data{proposer = Proposer}) ->
    chronicle_proposer:cas_config(Proposer, NewConfig, Revision).

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

handle_process_down(MRef, Pid, Reason, _State, #data{rsms = RSMs} = Data) ->
    case maps:take(MRef, RSMs) of
        error ->
            {stop, {unexpected_process_down, MRef, Pid, Reason}};
        {{Name, RSMPid}, NewRSMs} ->
            true = (Pid =:= RSMPid),
            ?DEBUG("RSM ~w~w terminated", [Name, RSMPid]),
            {keep_state, Data#data{rsms = NewRSMs}}
    end.

handle_rsm_command(HistoryId, Term, RSMCommand, State, Data) ->
    Command = {rsm_command, RSMCommand},
    batch_leader_request(Command, {HistoryId, Term}, noreply,
                         #data.commands_batch, State, Data).

terminate_proposer(#data{proposer = Proposer} = Data) ->
    case Proposer =:= undefined of
        true ->
            Data;
        false ->
            ok = chronicle_proposer:stop(Proposer),
            Data#data{proposer = undefined}
    end.

cleanup_after_proposer(#data{commands_batch = CommandsBatch,
                             syncs_batch = SyncsBatch} = Data) ->
    ?FLUSH({proposer_msg, _}),

    %% Commands don't expect any response.
    %%
    %% Sync requests do expect a response, but it's possible that they've
    %% already been responded to by the proposer. It's ok not to send any
    %% response to those that the proposer didn't respond to: the RSM on the
    %% leader will clean up once it realizes that the node is not leader
    %% anymore; the RSM on the follower will retry once new leader is known.
    _ = chronicle_utils:batch_flush(CommandsBatch),
    _ = chronicle_utils:batch_flush(SyncsBatch),

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
