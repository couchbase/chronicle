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
%% TODO: turn {error, history_mismatch} into an exception.
-module(chronicle_rsm).
-compile(export_all).

-include("chronicle.hrl").

-import(chronicle_utils, [call/2, call/3, read_timeout/1,
                          with_leader/2, start_timeout/1]).

-define(RSM_TAG, '$rsm').
-define(SERVER(Name), ?SERVER_NAME(Name)).
-define(SERVER(Peer, Name), ?SERVER_NAME(Peer, Name)).

-define(LOCAL_REVISION_KEY(Name), {?RSM_TAG, Name, local_revision}).

-type pending_client() ::
        {From :: any(),
         Type :: command
               | command_accepted
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

-record(snapshot, { applied_history_id :: chronicle:history_id(),
                    applied_seqno :: chronicle:seqno(),
                    mod_state :: any() }).

-record(data, { name :: atom(),

                applied_history_id :: chronicle:history_id(),
                applied_seqno :: chronicle:seqno(),
                read_seqno :: chronicle:seqno(),

                pending_clients :: pending_clients(),
                sync_revision_requests :: sync_revision_requests(),

                mod :: atom(),
                mod_state :: any(),
                mod_data :: any() }).

start_link(Name, Mod, ModArgs) ->
    gen_statem:start_link(?START_NAME(Name), ?MODULE, [Name, Mod, ModArgs], []).

%% TODO: Commands need to be tagged with current history. If that's not the
%% case, then after a quorum failover, the failed over part of the cluster may
%% continue sending commands to the leader in the non-failed over part for
%% some time.
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

get_local_revision(Name) ->
    case get_local_revision_fast(Name) of
        {ok, Revision} ->
            Revision;
        use_slow_path ->
            %% The process might still be initializing. So fall back to a call.
            call(?SERVER(Name), get_local_revision)
    end.

get_local_revision_fast(Name) ->
    case chronicle_ets:get(?LOCAL_REVISION_KEY(Name)) of
        {ok, _Revision} = Ok->
            Ok;
        not_found ->
            use_slow_path
    end.

sync_revision(Name, Revision, Timeout0) ->
    case sync_revision_fast(Name, Revision) of
        ok ->
            ok;
        use_slow_path ->
            Timeout = read_timeout(Timeout0),
            Request = {sync_revision, Revision, Timeout},
            case call(?SERVER(Name), Request, infinity) of
                ok ->
                    ok;
                {error, timeout} ->
                    exit({timeout, {sync_revision, Name, Revision, Timeout}})
            end
    end.

sync_revision_fast(Name, {RevHistoryId, RevSeqno}) ->
    case get_local_revision_fast(Name) of
        {LocalHistoryId, LocalSeqno}
          when LocalHistoryId =:= RevHistoryId andalso LocalSeqno >= RevSeqno ->
            ok;
        _ ->
            use_slow_path
    end.

sync(Name, Type, Timeout) ->
    TRef = start_timeout(Timeout),
    case get_applied_revision(Name, Type, TRef) of
        {ok, Revision} ->
            sync_revision(Name, Revision, TRef);
        {error, _} = Error ->
            Error
    end.

note_term_established(Pid, HistoryId, Term, Seqno) ->
    gen_statem:cast(Pid, {term_established, HistoryId, Term, Seqno}).

note_term_finished(Pid, HistoryId, Term) ->
    gen_statem:cast(Pid, {term_finished, HistoryId, Term}).

note_seqno_committed(Pid, Seqno) ->
    gen_statem:cast(Pid, {seqno_committed, Seqno}).

take_snapshot(Pid, Seqno) ->
    gen_statem:cast(Pid, {take_snapshot, Seqno}).

%% gen_statem callbacks
callback_mode() ->
    handle_event_function.

init([Name, Mod, ModArgs]) ->
    case Mod:init(Name, ModArgs) of
        {ok, ModState, ModData} ->
            State = #follower{},
            Data0 = #data{name = Name,
                          applied_history_id = ?NO_HISTORY,
                          applied_seqno = ?NO_SEQNO,
                          read_seqno = ?NO_SEQNO,
                          pending_clients = #{},
                          sync_revision_requests = gb_trees:empty(),
                          mod = Mod,
                          mod_state = ModState,
                          mod_data = ModData},

            Data = register_with_agent(State, Data0),
            Effects =
                case chronicle_server:register_rsm(Name, self()) of
                    {ok, HistoryId, Term, Seqno} ->
                        {next_event, cast,
                         {term_established, HistoryId, Term, Seqno}};
                    no_term ->
                        []
                end,

            ok = chronicle_ets:register_writer([?LOCAL_REVISION_KEY(Name)]),
            publish_local_revision(Data),

            case call_callback(post_init, Data) of
                {ok, NewModData} ->
                    {ok, State, set_mod_data(NewModData, Data), Effects};
                {stop, _} = Stop ->
                    Stop
            end;
        {stop, _} = Stop ->
            Stop
    end.

handle_event({call, From}, {command, Command}, State, Data) ->
    handle_command(Command, From, State, Data);
handle_event({call, From}, {query, Query}, State, Data) ->
    handle_query(Query, From, State, Data);
handle_event({call, From}, get_local_revision, State, Data) ->
    handle_get_local_revision(From, State, Data);
handle_event({call, From}, {sync_revision, Revision, Timeout}, State, Data) ->
    handle_sync_revision(Revision, Timeout, From, State, Data);
handle_event({call, From}, {get_applied_revision, Type}, State, Data) ->
    handle_get_applied_revision(Type, From, State, Data);
handle_event(cast, {term_established, HistoryId, Term, Seqno}, State, Data) ->
    handle_term_established(HistoryId, Term, Seqno, State, Data);
handle_event(cast, {term_finished, HistoryId, Term}, State, Data) ->
    handle_term_finished(HistoryId, Term, State, Data);
handle_event(cast, {seqno_committed, Seqno}, State, Data) ->
    handle_seqno_committed(Seqno, State, Data);
handle_event(cast, {take_snapshot, Seqno}, State, Data) ->
    handle_take_snapshot(Seqno, State, Data);
handle_event(info, {{?RSM_TAG, command, Ref}, Result}, State, Data) ->
    handle_command_result(Ref, Result, State, Data);
handle_event(info, {{?RSM_TAG, sync_quorum, Ref}, Result}, State, Data) ->
    handle_sync_quorum_result(Ref, Result, State, Data);
handle_event(info, {?RSM_TAG, sync_revision_timeout, Request}, State, Data) ->
    handle_sync_revision_timeout(Request, State, Data);
handle_event(info, Msg, _State, Data) ->
    case call_callback(handle_info, [Msg], Data) of
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

terminate(Reason, _State, Data) ->
    call_callback(terminate, [Reason], Data).

%% internal
handle_command(_Command, From, #follower{}, _Data) ->
    {keep_state_and_data,
     {reply, From, {error, {leader_error, not_leader}}}};
handle_command(Command, From, #leader{} = State, Data) ->
    handle_command_leader(Command, From, State, Data).

handle_command_leader(Command, From, State, Data) ->
    case call_callback(handle_command, [Command], Data) of
        {apply, NewModData} ->
            NewData = set_mod_data(NewModData, Data),
            {keep_state, submit_command(Command, From, State, NewData)};
        {reject, Reply, NewModData} ->
            {keep_state,
             set_mod_data(NewModData, Data),
             {reply, From, Reply}}
    end.

handle_query(Query, From, _State, Data) ->
    {reply, Reply, NewModData} = call_callback(handle_query, [Query], Data),
    {keep_state, set_mod_data(NewModData, Data), {reply, From, Reply}}.

handle_get_local_revision(From, _State, Data) ->
    {keep_state_and_data,
     {reply, From, local_revision(Data)}}.

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
            %%
            %% TODO: This may actually pose more issues that I thought. Post
            %% quorum failover it will take a non-zero amount of time for all
            %% rsm-s to catch up with the new history. So during this time
            %% attempts to sync those rsm-s will be failing. Which is probably
            %% undesirable.
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
                                               applied_seqno = AppliedSeqno,
                                               mod_state = ModState,
                                               mod_data = ModData} = Data) ->
    {NewHistoryId, NewAppliedSeqno, NewModState, NewModData, Replies} =
        lists:foldl(
          fun (Entry, Acc) ->
                  apply_entry(Entry, Acc, Data)
          end, {HistoryId, AppliedSeqno, ModState, ModData, []}, Entries),

    ?DEBUG("Applied commands to rsm '~p'.~n"
           "New applied seqno: ~p~n"
           "New read seqno: ~p~n"
           "Commands:~n~p~n"
           "Replies:~n~p",
           [Data#data.name, NewAppliedSeqno, HighSeqno, Entries, Replies]),

    NewData0 = Data#data{mod_state = NewModState,
                         mod_data = NewModData,
                         applied_history_id = NewHistoryId,
                         applied_seqno = NewAppliedSeqno,
                         read_seqno = HighSeqno},
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

apply_entry(Entry, {HistoryId, Seqno, ModState, ModData, Replies} = Acc,
            #data{mod = Mod} = Data) ->
    #log_entry{value = Value,
               history_id = EntryHistoryId,
               seqno = EntrySeqno} = Entry,
    case Value of
        #rsm_command{rsm_name = Name, command = Command} ->
            true = (Name =:= Data#data.name),
            true = (HistoryId =:= EntryHistoryId),
            AppliedRevision = {HistoryId, Seqno},
            Revision = {HistoryId, EntrySeqno},

            {reply, Reply, NewModState, NewModData} =
                Mod:apply_command(Command,
                                  Revision, AppliedRevision, ModState, ModData),

            EntryTerm = Entry#log_entry.term,
            NewReplies = [{EntryTerm, EntrySeqno, Reply} | Replies],
            {HistoryId, EntrySeqno, NewModState, NewModData, NewReplies};
        #config{} ->
            %% TODO: have an explicit indication in the log that an entry
            %% starts a new history
            %%
            %% The current workaround: only configs may start a new history.
            case EntryHistoryId =:= HistoryId of
                true ->
                    Acc;
                false ->
                    {EntryHistoryId, EntrySeqno, ModState, ModData, Replies}
            end
    end.

pending_commands_reply(_Replies, #follower{}, Data) ->
    Data;
pending_commands_reply(Replies,
                       #leader{term = OurTerm},
                       #data{pending_clients = Clients} = Data) ->
    NewClients =
        lists:foldl(
          fun ({Term, Seqno, Reply}, Acc) ->
                  pending_command_reply(Term, Seqno, Reply, OurTerm, Acc)
          end, Clients, Replies),

    Data#data{pending_clients = NewClients}.

pending_command_reply(Term, Seqno, Reply, OurTerm, Clients) ->
    %% Since chronicle_agent doesn't terminate all leader activities in a lock
    %% step, when some other nodes establishes a term, there's a short window
    %% of time when chronicle_rsm will continue to believe it's still in a
    %% leader state. So theoretically it's possible the new leader committs
    %% something a one of the seqnos we are waiting for. So we need to check
    %% that entry's term matches our term.
    %%
    %% TODO: consider making sure that chronicle_agent terminates everything
    %% synchronously.
    case Term =:= OurTerm of
        true ->
            case maps:take(Seqno, Clients) of
                {{From, command_accepted}, NewClients} ->
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

handle_sync_quorum_result(Ref, Result, State,
                          #data{pending_clients = Requests} = Data) ->
    {{From, {sync, Revision}}, NewRequests} = maps:take(Ref, Requests),

    Reply =
        case Result of
            ok ->
                #leader{} = State,
                {ok, Revision};
            {error, _} ->
                Result
        end,
    gen_statem:reply(From, Reply),
    {keep_state, Data#data{pending_clients = NewRequests}}.

handle_command_result(Ref, Result, State,
                      #data{pending_clients = Requests} = Data) ->
    {{From, command}, NewRequests0}  = maps:take(Ref, Requests),
    NewRequests =
        case Result of
            {accepted, Seqno} ->
                #leader{} = State,
                false = maps:is_key(Seqno, Requests),
                maps:put(Seqno, {From, command_accepted}, NewRequests0);
            {error, _} = Error ->
                gen_statem:reply(From, Error),
                NewRequests0
        end,

    {keep_state, Data#data{pending_clients = NewRequests}}.

handle_term_established(HistoryId, Term, Seqno, #follower{}, Data) ->
    Status =
        case Data#data.applied_seqno >= Seqno of
            true ->
                established;
            false ->
                %% Some of the entries before and including Seqno may or may
                %% not be committed. So we need to wait till their status
                %% resolves (that is, till they get committed), before we can
                %% respond to certain operations (get_applied_revision
                %% specifically).
                {wait_for_seqno, Seqno}
        end,

    {next_state,
     #leader{history_id = HistoryId,
             term = Term,
             status = Status},
     Data}.

handle_term_finished(HistoryId, Term, State, Data) ->
    case State of
        #leader{} ->
            true = (HistoryId =:= State#leader.history_id),
            true = (Term =:= State#leader.term),

            %% By the time chronicle_rsm receives the notification that the
            %% term has terminated, it must have already processed all
            %% notifications from chronicle_agent about commands that are
            %% known to have been committed by the outgoing leader. So there's
            %% not a need to synchronize with chronicle_agent as it was done
            %% previously.
            {next_state,
             #follower{},

             %% We only respond to accepted commands here. Other in flight
             %% requests will get a propoer response from chronicle_server.
             flush_accepted_commands({error, {leader_error, leader_lost}},
                                     Data)};
        #follower{} ->
            keep_state_and_data
    end.

handle_seqno_committed(CommittedSeqno, State,
                       #data{read_seqno = ReadSeqno} = Data) ->
    true = (CommittedSeqno > ReadSeqno),
    NewData = read_log(CommittedSeqno, State, Data),
    {next_state, maybe_mark_leader_established(State, NewData), NewData}.

handle_take_snapshot(Seqno, _State, Data) ->
    save_snapshot(Seqno, Data),
    keep_state_and_data.

save_snapshot(Seqno, #data{name = Name,
                           applied_history_id = AppliedHistoryId,
                           applied_seqno = AppliedSeqno,
                           read_seqno = ReadSeqno,
                           mod_state = ModState}) ->
    true = (Seqno =:= ReadSeqno),

    Snapshot = #snapshot{applied_history_id = AppliedHistoryId,
                         applied_seqno = AppliedSeqno,
                         mod_state = ModState},
    chronicle_agent:save_rsm_snapshot(Name, Seqno, Snapshot).

read_log(EndSeqno, State, Data) ->
    Entries = get_log(EndSeqno, Data),
    NewData = apply_entries(EndSeqno, Entries, State, Data),
    publish_local_revision(NewData),
    NewData.

get_log(EndSeqno, #data{name = Name, read_seqno = ReadSeqno}) ->
    %% TODO: replace this with a dedicated call
    {ok, Log} = chronicle_agent:get_log(),
    lists:filter(
      fun (#log_entry{seqno = Seqno, value = Value}) ->
              case Seqno > ReadSeqno andalso Seqno =< EndSeqno of
                  true ->
                      case Value of
                          #rsm_command{rsm_name = Name} ->
                              true;
                          #config{} ->
                              true;
                          _ ->
                              false
                      end;
                  false ->
                      false
              end
      end, Log).

submit_command(Command, From,
               #leader{history_id = HistoryId, term = Term},
               #data{name = Name} = Data) ->
    Ref = make_ref(),
    Tag = {?RSM_TAG, command, Ref},
    chronicle_server:rsm_command(Tag, HistoryId, Term, Name, Command),
    add_pending_client(Ref, From, command, Data).

add_pending_client(Ref, From, ClientData,
                   #data{pending_clients = Clients} = Data) ->
    Data#data{pending_clients = maps:put(Ref, {From, ClientData}, Clients)}.

flush_accepted_commands(Reply, #data{pending_clients = Clients} = Data) ->
    NewClients =
        maps:filter(
          fun (_, {From, Type}) ->
                  case Type of
                      command_accepted ->
                          gen_statem:reply(From, Reply),
                          false;
                      _ ->
                          true
                  end
          end, Clients),
    Data#data{pending_clients = NewClients}.

set_mod_data(ModData, Data) ->
    Data#data{mod_data = ModData}.

unwrap_command_reply(Reply) ->
    case Reply of
        {command_reply, R} ->
            R;
        _ ->
            Reply
    end.

call_callback(Callback, Data) ->
    call_callback(Callback, [], Data).

call_callback(Callback, Args, #data{mod = Mod,
                                    mod_state = ModState,
                                    mod_data = ModData,
                                    applied_history_id = AppliedHistoryId,
                                    applied_seqno = AppliedSeqno}) ->
    AppliedRevision = {AppliedHistoryId, AppliedSeqno},
    erlang:apply(Mod, Callback, Args ++ [AppliedRevision, ModState, ModData]).

publish_local_revision(#data{name = Name} = Data) ->
    chronicle_ets:put(?LOCAL_REVISION_KEY(Name), local_revision(Data)).

local_revision(#data{applied_history_id = AppliedHistoryId,
                     applied_seqno = AppliedSeqno}) ->
    {AppliedHistoryId, AppliedSeqno}.

register_with_agent(State, #data{name = Name} = Data0) ->
    {ok, Info} = chronicle_agent:register_rsm(Name, self()),
    #{committed_seqno := CommittedSeqno} = Info,

    Data = maybe_restore_snapshot(Data0),
    true = (Data#data.read_seqno =< CommittedSeqno),

    NewData =
        case maps:find(need_snapshot_seqno, Info) of
            {ok, SnapshotSeqno} ->
                true = (SnapshotSeqno =< CommittedSeqno),

                Data1 = read_log(SnapshotSeqno, State, Data),
                save_snapshot(SnapshotSeqno, Data1),
                Data1;
            error ->
                Data
        end,

    read_log(CommittedSeqno, State, NewData).

maybe_restore_snapshot(#data{name = Name} = Data) ->
    case chronicle_agent:get_rsm_snapshot(Name) of
        {ok, SnapshotSeqno, Snapshot} ->
            apply_snapshot(SnapshotSeqno, Snapshot, Data);
        {no_snapshot, SnapshotSeqno} ->
            Data#data{read_seqno = SnapshotSeqno}
    end.

apply_snapshot(Seqno, Snapshot, Data) ->
    #snapshot{applied_history_id = AppliedHistoryId,
              applied_seqno = AppliedSeqno,
              mod_state = ModState} = Snapshot,
    Revision = {AppliedHistoryId, AppliedSeqno},
    {ok, ModData} = call_callback(apply_snapshot, [Revision, ModState], Data),
    Data#data{applied_history_id = AppliedHistoryId,
              applied_seqno = AppliedSeqno,
              read_seqno = Seqno,
              mod_state = ModState,
              mod_data = ModData}.
