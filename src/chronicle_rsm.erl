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
-module(chronicle_rsm).
-compile(export_all).

-include("chronicle.hrl").

-import(chronicle_utils, [call/3, read_timeout/1,
                          with_leader/2, with_timeout/2]).

-define(RSM_TAG, '$rsm').
-define(SERVER(Name), ?SERVER_NAME(Name)).
-define(SERVER(Peer, Name), ?SERVER_NAME(Peer, Name)).

-type pending_client() ::
        {From :: any(),
         Type :: command
               | {sync, chronicle:revision()}}.
-type pending_clients() :: #{reference() => pending_client()}.

-type sync_revision_requests() ::
        gb_trees:tree(
          {chronicle:seqno(), reference()},
          {From :: any(), Timer :: reference(), chronicle:history_id()}).

-type leader_status() :: {wait_for_seqno, chronicle:seqno()} | established.

-record(follower, {}).
-record(leader, { history_id :: chronicle:history_id(),
                  term :: chronicle:leader_term(),
                  status :: leader_status() }).

-record(data, { name :: atom(),

                applied_history_id :: chronicle:history_id(),
                applied_seqno :: chronicle:seqno(),
                available_seqno :: chronicle:seqno(),

                pending_clients :: pending_clients(),
                sync_revision_requests :: sync_revision_requests(),
                reader :: undefined | pid(),
                reader_mref :: undefined | reference(),

                mod :: atom(),
                mod_state :: any(),
                mod_data :: any() }).

start_link(Name, Mod, ModArgs) ->
    gen_statem:start_link(?START_NAME(Name), ?MODULE, [Name, Mod, ModArgs], []).

command(Name, Command) ->
    command(Name, Command, 5000).

command(Name, Command, Timeout) ->
    unwrap_command_reply(
      with_leader(Timeout,
                  fun (TRef, Leader) ->
                          command(Leader, Name, Command, TRef)
                  end)).

command(Leader, Name, Command, Timeout) ->
    ?DEBUG("Sending Command to ~p: ~p", [Leader, Command]),
    call(?SERVER(Leader, Name), {command, Command}, Timeout).

query(Name, Query) ->
    query(Name, Query, 5000).

query(Name, Query, Timeout) ->
    call(?SERVER(Name), {query, Query}, Timeout).

get_applied_revision(Name, Type, Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader) ->
                        get_applied_revision(Leader, Name, Type, TRef)
                end).

get_applied_revision(Leader, Name, Type, Timeout) ->
    call(?SERVER(Leader, Name), {get_applied_revision, Type}, Timeout).

sync_revision(Name, Revision, Timeout0) ->
    %% TODO: add fast path
    Timeout = read_timeout(Timeout0),
    Request = {sync_revision, Revision, Timeout},
    case call(?SERVER(Name), Request, infinity) of
        ok ->
            ok;
        {error, timeout} ->
            exit({timeout, {sync_revision, Name, Revision, Timeout}})
    end.

sync(Name, Type, Timeout) ->
    with_timeout(Timeout,
                 fun (TRef) ->
                         case get_applied_revision(Name, Type, TRef) of
                             {ok, Revision} ->
                                 sync_revision(Name, Revision, TRef);
                             {error, _} = Error ->
                                 Error
                         end
                 end).

%% gen_statem callbacks
callback_mode() ->
    handle_event_function.

init([Name, Mod, ModArgs]) ->
    case Mod:init(Name, ModArgs) of
        {ok, ModState, ModData} ->
            Self = self(),
            chronicle_events:subscribe(
              fun (Event) ->
                      case is_interesting_event(Event) of
                          true ->
                              Self ! {?RSM_TAG, chronicle_event, Event};
                          false ->
                              ok
                      end
              end),

            chronicle_leader:announce_leader(),

            {ok, Metadata} = chronicle_agent:get_metadata(),
            #metadata{committed_seqno = CommittedSeqno} = Metadata,

            Data = #data{name = Name,
                         applied_history_id = ?NO_HISTORY,
                         applied_seqno = ?NO_SEQNO,
                         available_seqno = CommittedSeqno,
                         pending_clients = #{},
                         sync_revision_requests = gb_trees:empty(),
                         mod = Mod,
                         mod_state = ModState,
                         mod_data = ModData},

            {ok, #follower{}, maybe_start_reader(Data)};
        {stop, _} = Stop ->
            Stop
    end.

handle_event({call, From}, {command, Command}, State, Data) ->
    handle_command(Command, From, State, Data);
handle_event({call, From}, {query, Query}, State, Data) ->
    handle_query(Query, From, State, Data);
handle_event({call, From}, {sync_revision, Revision, Timeout}, State, Data) ->
    handle_sync_revision(Revision, Timeout, From, State, Data);
handle_event({call, From}, {get_applied_revision, Type}, State, Data) ->
    handle_get_applied_revision(Type, From, State, Data);
handle_event(cast, {entries, HighSeqno, Entries}, State, Data) ->
    handle_entries(HighSeqno, Entries, State, Data);
handle_event(info, {?RSM_TAG, chronicle_event, Event}, State, Data) ->
    handle_chronicle_event(Event, State, Data);
handle_event(info, {{?RSM_TAG, sync_quorum, Ref}, Result}, State, Data) ->
    handle_sync_quorum_result(Ref, Result, State, Data);
handle_event(info, {?RSM_TAG, sync_revision_timeout, Request}, State, Data) ->
    handle_sync_revision_timeout(Request, State, Data);
handle_event(info,
            {'DOWN', _, process, Pid, Reason}, _State, #data{reader = Reader})
  when Reader =:= Pid ->
    {stop, {reader_died, Pid, Reason}};
handle_event(info, Msg, _State, #data{mod = Mod,
                                      mod_state = ModState,
                                      mod_data = ModData} = Data) ->
    case Mod:handle_info(Msg, ModState, ModData) of
        {noreply, NewModData} ->
            {keep_state, set_mod_data(NewModData, Data)};
        {stop, _} = Stop ->
            Stop
    end;
handle_event({call, From}, Call, _State, _Data) ->
    ?WARNING("Unexpected call ~p", [Call]),
    {keep_state_and_data, [{reply, From, nack}]};
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event of type ~p: ~p", [Type, Event]),
    keep_state_and_data.

terminate(Reason, _State, #data{mod = Mod,
                                mod_state = ModState,
                                mod_data = ModData}) ->
    Mod:terminate(Reason, ModState, ModData).

%% internal
handle_command(_Command, From, #follower{}, _Data) ->
    {keep_state_and_data,
     {reply, From, {error, {leader_error, not_leader}}}};
handle_command(Command, From, #leader{} = State, Data) ->
    handle_command_leader(Command, From, State, Data).

handle_command_leader(Command, From, State, #data{mod = Mod,
                                                  mod_state = ModState,
                                                  mod_data = ModData} = Data) ->
    case Mod:handle_command(Command, ModState, ModData) of
        {apply, NewModData} ->
            NewData = set_mod_data(NewModData, Data),
            {keep_state, submit_command(Command, From, State, NewData)};
        {reject, Reply, NewModData} ->
            {keep_state,
             set_mod_data(NewModData, Data),
             {reply, From, Reply}}
    end.

handle_query(Query, From, _State, #data{mod = Mod,
                                        mod_state = ModState,
                                        mod_data = ModData} = Data) ->
    {reply, Reply, NewModData} = Mod:handle_query(Query, ModState, ModData),
    {keep_state, set_mod_data(NewModData, Data), {reply, From, Reply}}.

handle_sync_revision({HistoryId, Seqno}, Timeout, From,
                     _State,
                     #data{applied_history_id = AppliedHistoryId,
                           applied_seqno = AppliedSeqno} = Data) ->
    case HistoryId =:= AppliedHistoryId of
        true ->
            case Seqno =< AppliedSeqno of
                true ->
                    {keep_state_and_data, {reply, From, ok}};
                false ->
                    {keep_state,
                     sync_revision_add_request(HistoryId, Seqno,
                                               Timeout, From, Data)}
            end;
        false ->
            %% We may hit this case even if we in fact do have the revision
            %% that the client passed. To handle such cases properly, we'd
            %% have to not only keep track of the current history id, but also
            %% of all history ids that we've seen and corresponding ranges of
            %% sequence numbers where they apply. Since this case is pretty
            %% rare, for the sake of simplicity we'll just tolerate a
            %% possibility of sync_revision() call failing unnecessarily.
            {keep_state_and_data,
             {reply, From, {error, history_mismatch}}}
    end.

sync_revision_add_request(HistoryId, Seqno, Timeout, From,
                          #data{sync_revision_requests = Requests} = Data) ->
    Request = {Seqno, make_ref()},
    TRef = sync_revision_start_timer(Request, Timeout),
    RequestData = {From, TRef, HistoryId},
    NewRequests = gb_trees:insert(Request, RequestData, Requests),
    Data#data{sync_revision_requests = NewRequests}.

sync_revision_requests_reply(#data{applied_seqno = Seqno,
                                   sync_revision_requests = Requests} = Data) ->
    NewRequests = sync_revision_requests_reply_loop(Seqno, Requests),
    Data#data{sync_revision_requests = NewRequests}.

sync_revision_requests_reply_loop(Seqno, Requests) ->
    case gb_trees:is_empty(Requests) of
        true ->
            Requests;
        false ->
            {{ReqSeqno, _} = Request, RequestData, NewRequests} =
                gb_trees:take_smallest(Requests),
            case ReqSeqno =< Seqno of
                true ->
                    sync_revision_request_reply(Request, RequestData, ok),
                    sync_revision_requests_reply_loop(Seqno, NewRequests);
                false ->
                    Requests
            end
    end.

sync_revision_request_reply(Request, {From, TRef, _HistoryId}, Reply) ->
    sync_revision_cancel_timer(Request, TRef),
    gen_statem:reply(From, Reply).

sync_revision_drop_diverged_requests(#data{applied_history_id = HistoryId,
                                           sync_revision_requests = Requests} =
                                         Data) ->
    NewRequests =
        chronicle_utils:gb_trees_filter(
          fun (Request, {_, _, ReqHistoryId} = RequestData) ->
                  case ReqHistoryId =:= HistoryId of
                      true ->
                          true;
                      false ->
                          Reply = {error, history_mismatch},
                          sync_revision_request_reply(Request,
                                                      RequestData, Reply),
                          false
                  end
          end, Requests),

    Data#data{sync_revision_requests = NewRequests}.

sync_revision_start_timer(Request, Timeout) ->
    erlang:send_after(Timeout, self(),
                      {?RSM_TAG, sync_revision_timeout, Request}).

sync_revision_cancel_timer(Request, TRef) ->
    erlang:cancel_timer(TRef),
    ?FLUSH({?RSM_TAG, sync_revision_timeout, Request}).

handle_sync_revision_timeout(Request, _State,
                             #data{sync_revision_requests = Requests} = Data) ->
    {{From, _, _}, NewRequests} = gb_trees:take(Request, Requests),
    gen_statem:reply(From, {error, timeout}),
    {keep_state, Data#data{sync_revision_requests = NewRequests}}.

handle_entries(HighSeqno, Entries, State, #data{reader = Reader,
                                                reader_mref = MRef} = Data) ->
    true = is_pid(Reader),
    true = is_reference(MRef),

    erlang:demonitor(MRef, [flush]),

    NewData0 = Data#data{reader = undefined, reader_mref = undefined},
    NewData1 = apply_entries(HighSeqno, Entries, State, NewData0),
    NewData = maybe_start_reader(NewData1),

    {next_state,
     maybe_mark_leader_established(State, NewData),
     NewData}.

%% Mark the leader established when applied seqno catches up with the high
%% seqno as of when the term was established.
maybe_mark_leader_established(State, Data) ->
    case State of
        #follower{} ->
            State;
        #leader{status = established} ->
            State;
        #leader{status = {wait_for_seqno, Seqno}} ->
            case Data#data.applied_seqno >= Seqno of
                true ->
                    State#leader{status = established};
                false ->
                    State
            end
    end.

apply_entries(HighSeqno, Entries, State, #data{applied_history_id = HistoryId,
                                               mod_state = ModState,
                                               mod_data = ModData} = Data) ->
    {NewHistoryId, NewModState, NewModData, Replies} =
        lists:foldl(
          fun (Entry, Acc) ->
                  apply_entry(Entry, Acc, Data)
          end, {HistoryId, ModState, ModData, []}, Entries),

    ?DEBUG("Applied commands to rsm '~p'.~n"
           "New applied seqno: ~p~n"
           "Commands:~n~p~n"
           "Replies:~n~p",
           [Data#data.name, HighSeqno, Entries, Replies]),

    NewData0 = Data#data{mod_state = NewModState,
                         mod_data = NewModData,
                         applied_history_id = NewHistoryId,
                         applied_seqno = HighSeqno},
    NewData1 =
        case HistoryId =:= NewHistoryId of
            true ->
                NewData0;
            false ->
                %% Drop requests that have the history id different from the
                %% one we just adopted. See the comment in
                %% handle_sync_revision/4 for more context.
                sync_revision_drop_diverged_requests(NewData0)
        end,

    NewData = sync_revision_requests_reply(NewData1),
    pending_commands_reply(Replies, State, NewData).

apply_entry(Entry, {HistoryId, ModState, ModData, Replies} = Acc,
            #data{mod = Mod} = Data) ->
    #log_entry{value = Value} = Entry,
    case Value of
        #rsm_command{id = Id, rsm_name = Name, command = Command} ->
            true = (Name =:= Data#data.name),
            Revision = {Entry#log_entry.history_id, Entry#log_entry.seqno},

            {reply, Reply, NewModState, NewModData} =
                Mod:apply_command(Command, Revision, ModState, ModData),

            EntryTerm = Entry#log_entry.term,
            NewReplies = [{Id, EntryTerm, Reply} | Replies],
            {HistoryId, NewModState, NewModData, NewReplies};
        #config{} ->
            %% TODO: have an explicit indication in the log that an entry
            %% starts a new history
            %%
            %% The current workaround: only configs may start a new history.
            EntryHistoryId = Entry#log_entry.history_id,
            case EntryHistoryId =:= HistoryId of
                true ->
                    Acc;
                false ->
                    {EntryHistoryId, ModState, ModData, Replies}
            end
    end.

pending_commands_reply(_Replies, #follower{}, Data) ->
    Data;
pending_commands_reply(Replies,
                       #leader{term = OurTerm},
                       #data{pending_clients = Clients} = Data) ->
    NewClients =
        lists:foldl(
          fun ({Ref, Term, Reply}, Acc) ->
                  pending_command_reply(Ref, Term, Reply, OurTerm, Acc)
          end, Clients, Replies),

    Data#data{pending_clients = NewClients}.

pending_command_reply(Ref, Term, Reply, OurTerm, Clients) ->
    %% References are not guaranteed to be unique across restarts. So
    %% theoretically it's possible that we'll reply to the wrong client. So we
    %% are also checking that the corresponding entry was proposed in our
    %% term.
    case Term =:= OurTerm of
        true ->
            case maps:take(Ref, Clients) of
                {{From, command}, NewClients} ->
                    gen_statem:reply(From, {command_reply, Reply}),
                    NewClients;
                error ->
                    Clients
            end;
        false ->
            Clients
    end.

handle_get_applied_revision(_Type, From, #follower{}, _Data) ->
    {keep_state_and_data, {reply, From, {error, {leader_error, not_leader}}}};
handle_get_applied_revision(Type, From,
                            #leader{status = Status} = State, Data) ->
    case Status of
        established ->
            handle_get_applied_revision_leader(Type, From, State, Data);
        {wait_for_seqno, _} ->
            %% When we've just become the leader, we are guaranteed to have
            %% all mutations that might have been committed by the old leader,
            %% but there's no way to know what was and what wasn't
            %% committed. So we need to wait until all uncommitted entries
            %% that we have get committed.
            %%
            %% Note, that we can't simply return TermSeqno to the caller. That
            %% is because the log entry at TermSeqno might not have been
            %% committed. And if another leader immediately takes over, it may
            %% not have that entry and may never commit it. So the caller will
            %% essentially get stuck. So we need to postpone handling the call
            %% until we know that TermSeqno is committed.
            {keep_state_and_data, postpone}
    end.

handle_get_applied_revision_leader(Type, From, State, Data) ->
    established = State#leader.status,
    #data{applied_seqno = AppliedSeqno,
          applied_history_id = AppliedHistoryId} = Data,
    Revision = {AppliedHistoryId, AppliedSeqno},
    case Type of
        leader ->
            {keep_state_and_data, {reply, From, {ok, Revision}}};
        quorum ->
            {keep_state, sync_quorum(Revision, From, State, Data)}
    end.

sync_quorum(Revision, From,
            #leader{history_id = HistoryId, term = Term}, Data) ->
    Ref = make_ref(),
    Tag = {?RSM_TAG, sync_quorum, Ref},
    chronicle_server:sync_quorum(Tag, HistoryId, Term),
    add_pending_client(Ref, From, {sync, Revision}, Data).

handle_sync_quorum_result(Ref, Result, _State,
                          #data{pending_clients = Requests} = Data) ->
    case maps:take(Ref, Requests) of
        {{From, {sync, Revision}}, NewRequests} ->
            %% TODO: do I need to go anything else with the result?
            Reply =
                case Result of
                    ok ->
                        {ok, Revision};
                    {error, _} ->
                        Result
                end,
            gen_statem:reply(From, Reply),
            {keep_state, Data#data{pending_clients = NewRequests}};
        error ->
            %% Possible if we got notified that the leader has changed before
            %% local proposer was terminated.
            keep_state_and_data
    end.

is_interesting_event({leader, _}) ->
    true;
is_interesting_event({metadata, _}) ->
    true;
is_interesting_event(_) ->
    false.

handle_chronicle_event({leader, LeaderInfo}, State, Data) ->
    handle_leader_info(LeaderInfo, State, Data);
handle_chronicle_event({metadata, Metadata}, State, Data) ->
    handle_new_metadata(Metadata, State, Data).

handle_leader_info(LeaderInfo, State, Data) ->
    case should_become_leader(LeaderInfo) of
        true ->
            #{history_id := HistoryId,
              term := Term,
              status := {established, HighSeqno}} = LeaderInfo,
            Status =
                case Data#data.applied_seqno >= HighSeqno of
                    true ->
                        established;
                    false ->
                        %% Some of the entries before and including HighSeqno
                        %% may or may not be committed. So we need to wait
                        %% till their status resolves (that is, till they get
                        %% committed), before we can respond to certain
                        %% operations (get_applied_revision specifically).
                        {wait_for_seqno, HighSeqno}
                end,

            {next_state,
             #leader{history_id = HistoryId,
                     term = Term,
                     status = Status},
             Data};
        false ->
            maybe_step_down(State, Data)
    end.

should_become_leader(no_leader) ->
    false;
should_become_leader(#{leader := Leader, status := Status}) ->
    case Status of
        {established, _} ->
            Leader =:= ?PEER();
        _ ->
            false
    end.

maybe_step_down(#follower{}, _Data) ->
    keep_state_and_data;
maybe_step_down(#leader{}, Data) ->
    {next_state,
     #follower{},
     %% TODO: More detailed reason for why leader is gone
     flush_pending_clients({error, {leader_error, leader_lost}}, Data)}.

handle_new_metadata(#metadata{committed_seqno = CommittedSeqno},
                    _State, Data) ->
    NewData = Data#data{available_seqno = CommittedSeqno},
    {keep_state, maybe_start_reader(NewData)}.

maybe_start_reader(#data{reader = undefined,
                         applied_seqno = AppliedSeqno,
                         available_seqno = AvailableSeqno} = Data) ->
    case AvailableSeqno > AppliedSeqno of
        true ->
            start_reader(Data);
        false ->
            Data
    end;
maybe_start_reader(Data) ->
    Data.

start_reader(Data) ->
    Self = self(),
    {Pid, MRef} = spawn_monitor(fun () -> reader(Self, Data) end),
    Data#data{reader = Pid, reader_mref = MRef}.

reader(Parent, Data) ->
    {HighSeqno, Commands} = get_log(Data),
    gen_statem:cast(Parent, {entries, HighSeqno, Commands}).

get_log(#data{name = Name,
              applied_seqno = AppliedSeqno,
              available_seqno = AvailableSeqno}) ->
    %% TODO: replace this with a dedicated call
    {ok, Log} = chronicle_agent:get_log(?PEER()),
    Entries =
        lists:filter(
          fun (#log_entry{seqno = Seqno, value = Value}) ->
                  case Value of
                      #rsm_command{rsm_name = Name} ->
                          Seqno > AppliedSeqno andalso Seqno =< AvailableSeqno;
                      #config{} ->
                          true;
                      _ ->
                          false
                  end
          end, Log),

    {AvailableSeqno, Entries}.

submit_command(Command, From,
               #leader{history_id = HistoryId, term = Term},
               #data{name = Name} = Data) ->
    Ref = make_ref(),
    chronicle_server:rsm_command(HistoryId, Term, Name, Ref, Command),
    add_pending_client(Ref, From, command, Data).

add_pending_client(Ref, From, ClientData,
                   #data{pending_clients = Clients} = Data) ->
    Data#data{pending_clients = maps:put(Ref, {From, ClientData}, Clients)}.

flush_pending_clients(Reply, #data{pending_clients = Clients} = Data) ->
    maps:fold(
      fun (_, {From, _}, _) ->
              gen_statem:reply(From, Reply)
      end, unused, Clients),
    Data#data{pending_clients = #{}}.

set_mod_data(ModData, Data) ->
    Data#data{mod_data = ModData}.

unwrap_command_reply(Reply) ->
    case Reply of
        {command_reply, R} ->
            R;
        _ ->
            Reply
    end.
