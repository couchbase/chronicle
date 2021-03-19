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

-behavior(gen_statem).

%% If it takes longer than this time to initialize, then we probably hit some
%% bug.
-define(INIT_TIMEOUT, chronicle_settings:get({rsm, init_timeout}, 60000)).

-include("chronicle.hrl").

-import(chronicle_utils, [call/2, call/3, call/4,
                          read_timeout/1,
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

-record(init, { wait_for_seqno }).
-record(no_leader, {}).
-record(follower, { leader :: chronicle:peer(),
                    history_id :: chronicle:history_id(),
                    term :: chronicle:leader_term() }).
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

command(Name, Command) ->
    command(Name, Command, 5000).

command(Name, Command, Timeout) ->
    PackedCommand = pack_command(Command),
    unwrap_command_reply(
      with_leader(Timeout,
                  fun (TRef, Leader, {HistoryId, _Term}) ->
                          command(Leader, Name, HistoryId, PackedCommand, TRef)
                  end)).

command(Leader, Name, HistoryId, Command, Timeout) ->
    call(?SERVER(Leader, Name),
         {command, HistoryId, Command}, command, Timeout).

query(Name, Query) ->
    query(Name, Query, 5000).

query(Name, Query, Timeout) ->
    call(?SERVER(Name), {query, Query}, query, Timeout).

get_applied_revision(Name, Type, Timeout)
  when Type =:= leader;
       Type =:= quorum ->
    with_leader(Timeout,
                fun (TRef, Leader, {HistoryId, _Term}) ->
                        get_applied_revision(Leader, Name,
                                             HistoryId, Type, TRef)
                end).

get_applied_revision(Leader, Name, HistoryId, Type, Timeout) ->
    call(?SERVER(Leader, Name),
         {get_applied_revision, HistoryId, Type}, Timeout).

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
                    exit({timeout, {sync_revision, Name, Revision, Timeout}});
                {error, history_mismatch} ->
                    exit({history_mismatch,
                          {sync_revision, Name, Revision, Timeout}})
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
        {error, Error} ->
            exit({Error, {sync, Name, Type, Timeout}})
    end.

note_leader_status(Pid, LeaderStatus) ->
    gen_statem:cast(Pid, {leader_status, LeaderStatus}).

note_seqno_committed(Name, Seqno) ->
    gen_statem:cast(?SERVER(Name), {seqno_committed, Seqno}).

take_snapshot(Name, Seqno) ->
    gen_statem:cast(?SERVER(Name), {take_snapshot, Seqno}).

%% gen_statem callbacks
callback_mode() ->
    handle_event_function.

format_status(Opt, [_PDict, State, Data]) ->
    case Opt of
        normal ->
            [{data, [{"State", {State, Data}}]}];
        terminate ->
            {State,
             case Data of
                 #data{} ->
                     Data#data{mod_state = omitted};
                 _ ->
                     %% During gen_statem initialization Data may be undefined.
                     Data
             end}
    end.

sanitize_event({call, _} = Type, {command, HistoryId, _}) ->
    {Type, {command, HistoryId, '...'}};
sanitize_event({call, _} = Type, {query, _}) ->
    {Type, {query, '...'}};
sanitize_event(Type, Event) ->
    {Type, Event}.

init([Name, Mod, ModArgs]) ->
    case Mod:init(Name, ModArgs) of
        {ok, ModState, ModData} ->
            ok = chronicle_ets:register_writer([?LOCAL_REVISION_KEY(Name)]),

            Data0 = #data{name = Name,
                          applied_history_id = ?NO_HISTORY,
                          applied_seqno = ?NO_SEQNO,
                          read_seqno = ?NO_SEQNO,
                          pending_clients = #{},
                          sync_revision_requests = gb_trees:empty(),
                          mod = Mod,
                          mod_state = ModState,
                          mod_data = ModData},
            Data = maybe_restore_snapshot(Data0),
            {State, Effects} = init_from_agent(Data0),

            {ok, State, Data,
             [{state_timeout, ?INIT_TIMEOUT, init_timeout} | Effects]};
        {stop, _} = Stop ->
            Stop
    end.

complete_init(#init{}, #data{name = Name} = Data) ->
    publish_local_revision(Data),
    LeaderStatus = chronicle_server:register_rsm(Name, self()),
    Effects = [{next_event, cast, {leader_status, LeaderStatus}}],

    case call_callback(post_init, Data) of
        {ok, NewModData} ->
            {next_state, #no_leader{}, set_mod_data(NewModData, Data), Effects};
        {stop, _} = Stop ->
            Stop
    end.

handle_event(state_timeout, init_timeout,
             #init{wait_for_seqno = WaitedSeqno},
             #data{read_seqno = ReadSeqno}) ->
    ?ERROR("Couldn't initialize in ~pms.~n"
           "Seqno we're waiting for: ~p~n"
           "Read seqno: ~p",
           [?INIT_TIMEOUT, WaitedSeqno, ReadSeqno]),
    {stop, init_timeout};
handle_event({call, From}, Call, State, Data) ->
    case State of
        #init{} ->
            {keep_state_and_data, postpone};
        _ ->
            handle_call(Call, From, State, Data)
    end;
handle_event(cast, {leader_status, LeaderStatus}, State, Data) ->
    handle_leader_status(LeaderStatus, State, Data);
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
handle_event(Type, Event, State, _Data) ->
    ?WARNING("Unexpected event of type ~p: ~p.~n"
             "Current state: ~p", [Type, Event, State]),
    keep_state_and_data.

terminate(Reason, _State, Data) ->
    call_callback(terminate, [Reason], Data).

%% internal
handle_call({command, HistoryId, Command}, From, State, Data) ->
    handle_command(HistoryId, Command, From, State, Data);
handle_call({query, Query}, From, State, Data) ->
    handle_query(Query, From, State, Data);
handle_call(get_local_revision, From, State, Data) ->
    handle_get_local_revision(From, State, Data);
handle_call({sync_revision, Revision, Timeout}, From, State, Data) ->
    handle_sync_revision(Revision, Timeout, From, State, Data);
handle_call({get_applied_revision, HistoryId, Type}, From, State, Data) ->
    handle_get_applied_revision(HistoryId, Type, From, State, Data);
handle_call(Call, From, _State, _Data) ->
    ?WARNING("Unexpected call ~p", [Call]),
    {keep_state_and_data, [{reply, From, nack}]}.

handle_command(HistoryId, Command, From,
               #leader{history_id = OurHistoryId} = State, Data)
  when HistoryId =:= OurHistoryId ->
    handle_command_leader(Command, From, State, Data);
handle_command(_HistoryId, _Command, From, _, _Data) ->
    {keep_state_and_data,
     {reply, From, {error, {leader_error, not_leader}}}}.

handle_command_leader(Command, From, State, Data) ->
    {keep_state, submit_command(Command, From, State, Data)}.

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
    _ = erlang:cancel_timer(TRef),
    ?FLUSH({?RSM_TAG, sync_revision_timeout, Request}).

handle_sync_revision_timeout(Request, _State,
                             #data{sync_revision_requests = Requests} = Data) ->
    {{From, _, _}, NewRequests} = gb_trees:take(Request, Requests),
    gen_statem:reply(From, {error, timeout}),
    {keep_state, Data#data{sync_revision_requests = NewRequests}}.

handle_seqno_committed_next_state(State,
                                  #data{read_seqno = ReadSeqno} = Data) ->
    case State of
        #init{wait_for_seqno = Seqno} ->
            case ReadSeqno >= Seqno of
                true ->
                    complete_init(State, Data);
                false ->
                    {keep_state, Data}
            end;
        #leader{status = {wait_for_seqno, Seqno}} ->
            %% Mark the leader established when applied seqno catches up with
            %% the high seqno as of when the term was established.
            case ReadSeqno >= Seqno of
                true ->
                    {next_state, State#leader{status = established}, Data};
                false ->
                    {keep_state, Data}
            end;
        _ ->
            {keep_state, Data}
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

apply_entry(Entry, {HistoryId, Seqno, ModState, ModData, Replies},
            #data{mod = Mod} = Data) ->
    #log_entry{value = Value,
               history_id = EntryHistoryId,
               seqno = EntrySeqno} = Entry,
    AppliedRevision = {HistoryId, Seqno},
    Revision = {HistoryId, EntrySeqno},

    case Value of
        #rsm_command{rsm_name = Name, command = Command} ->
            true = (Name =:= Data#data.name),
            true = (HistoryId =:= EntryHistoryId),

            {reply, Reply, NewModState, NewModData} =
                Mod:apply_command(unpack_command(Command),
                                  Revision, AppliedRevision, ModState, ModData),

            EntryTerm = Entry#log_entry.term,
            NewReplies = [{EntryTerm, EntrySeqno, Reply} | Replies],
            {HistoryId, EntrySeqno, NewModState, NewModData, NewReplies};
        #config{} = Config ->
            {ok, NewModState, NewModData} =
                Mod:handle_config(Config,
                                  Revision, AppliedRevision,
                                  ModState, ModData),
            {EntryHistoryId, EntrySeqno, NewModState, NewModData, Replies}
    end.

pending_commands_reply(Replies,
                       #leader{term = OurTerm},
                       #data{pending_clients = Clients} = Data) ->
    NewClients =
        lists:foldl(
          fun ({Term, Seqno, Reply}, Acc) ->
                  pending_command_reply(Term, Seqno, Reply, OurTerm, Acc)
          end, Clients, Replies),

    Data#data{pending_clients = NewClients};
pending_commands_reply(_Replies, _State, Data) ->
    Data.

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

handle_get_applied_revision(HistoryId, Type, From,
                            #leader{history_id = OurHistoryId,
                                    status = Status} = State, Data)
  when HistoryId =:= OurHistoryId ->
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
    end;
handle_get_applied_revision(_HistoryId, _Type, From, _State, _Data) ->
    {keep_state_and_data, {reply, From, {error, {leader_error, not_leader}}}}.

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

handle_leader_status(Status, State, Data) ->
    case Status of
        {leader, HistoryId, Term, Seqno} ->
            handle_became_leader(HistoryId, Term, Seqno, State, Data);
        {follower, Leader, HistoryId, Term} ->
            NewState = #follower{leader = Leader,
                                 history_id = HistoryId,
                                 term = Term},
            handle_not_leader(State, NewState, Data);
        no_leader ->
            handle_not_leader(State, #no_leader{}, Data)
    end.

handle_became_leader(HistoryId, Term, Seqno, State, Data) ->
    true = is_record(State, no_leader) orelse is_record(State, follower),

    Status =
        case Data#data.read_seqno >= Seqno of
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

handle_not_leader(OldState, NewState, Data) ->
    NewData =
        case OldState of
            #leader{} ->
                %% By the time chronicle_rsm receives the notification that
                %% the term has terminated, it must have already processed all
                %% notifications from chronicle_agent about commands that are
                %% known to have been committed by the outgoing leader. So
                %% there's not a need to synchronize with chronicle_agent as
                %% it was done previously.

                %% We only respond to accepted commands here. Other in flight
                %% requests will get a propoer response from chronicle_server.
                flush_accepted_commands({error, {leader_error, leader_lost}},
                                        Data);
            _ ->
                Data
        end,

    {next_state, NewState, NewData}.

handle_seqno_committed(CommittedSeqno, State,
                       #data{read_seqno = ReadSeqno} = Data) ->
    case CommittedSeqno >= ReadSeqno of
        true ->
            NewData = read_log(CommittedSeqno, State, Data),
            maybe_publish_local_revision(State, NewData),
            handle_seqno_committed_next_state(State, NewData);
        false ->
            ?DEBUG("Ignoring seqno_committed ~p "
                   "when read seqno is ~p", [CommittedSeqno, ReadSeqno]),
            keep_state_and_data
    end.

handle_take_snapshot(Seqno, _State, #data{read_seqno = ReadSeqno} = Data) ->
    case Seqno < ReadSeqno of
        true ->
            ?DEBUG("Ignoring stale take_snapshot "
                   "at ~p when read seqno is ~p", [Seqno, ReadSeqno]);
        false ->
            save_snapshot(Seqno, Data)
    end,

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

read_log(EndSeqno, State, #data{read_seqno = ReadSeqno} = Data) ->
    StartSeqno = ReadSeqno + 1,
    case get_log(StartSeqno, EndSeqno, Data) of
        {ok, Entries} ->
            apply_entries(EndSeqno, Entries, State, Data);
        {error, compacted} ->
            %% We should only get this error when a new snapshot was installed
            %% by the leader. So we always expect to have a snapshot
            %% available, and the snapshot seqno is expected to be greater
            %% than our read seqno.
            {ok, SnapshotSeqno, Snapshot} = get_snapshot(Data),
            true = (SnapshotSeqno >= StartSeqno),

            ?DEBUG("Got log compacted when reading seqnos ~p to ~p. "
                   "Applying snapshot at seqno ~p",
                   [StartSeqno, EndSeqno, SnapshotSeqno]),

            NewData = apply_snapshot(SnapshotSeqno, Snapshot, Data),
            case EndSeqno > SnapshotSeqno of
                true ->
                    %% There are more entries to read.
                    read_log(EndSeqno, State, NewData);
                false ->
                    NewData
            end
    end.

get_log(StartSeqno, EndSeqno, #data{name = Name}) ->
    case chronicle_agent:get_log_for_rsm(Name, StartSeqno, EndSeqno) of
        {ok, _} = Ok ->
            Ok;
        {error, compacted} = Error ->
            Error
    end.

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

maybe_publish_local_revision(#init{}, _Data) ->
    %% Don't expose the revision while we're still initializing
    ok;
maybe_publish_local_revision(_, Data) ->
    publish_local_revision(Data).

publish_local_revision(#data{name = Name} = Data) ->
    chronicle_ets:put(?LOCAL_REVISION_KEY(Name), local_revision(Data)).

local_revision(#data{applied_history_id = AppliedHistoryId,
                     applied_seqno = AppliedSeqno}) ->
    {AppliedHistoryId, AppliedSeqno}.

init_from_agent(#data{name = Name} = Data) ->
    %% TODO: deal with {error, no_rsm}
    {ok, Info} = chronicle_agent:get_info_for_rsm(Name),
    #{committed_seqno := CommittedSeqno} = Info,

    true = (Data#data.read_seqno =< CommittedSeqno),

    Effects0 = [{next_event, cast, {seqno_committed, CommittedSeqno}}],
    Effects1 =
        case maps:find(need_snapshot_seqno, Info) of
            {ok, SnapshotSeqno} ->
                true = (SnapshotSeqno =< CommittedSeqno),
                [{next_event, cast, {seqno_committed, SnapshotSeqno}},
                 {next_event, cast, {take_snapshot, SnapshotSeqno}} |
                 Effects0];
            error ->
                Effects0
        end,

    {#init{wait_for_seqno = CommittedSeqno}, Effects1}.

get_snapshot(#data{name = Name}) ->
    chronicle_agent:get_rsm_snapshot(Name).

maybe_restore_snapshot(Data) ->
    case get_snapshot(Data) of
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

pack_command(Command) ->
    {binary, term_to_binary(Command, [{compressed, 1}])}.

unpack_command(PackedCommand) ->
    {binary, Binary} = PackedCommand,
    binary_to_term(Binary).
