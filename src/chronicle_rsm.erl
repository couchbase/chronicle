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
          {From :: any(), Timer :: reference(), chronicle:revision()}).

-record(state, { name :: atom(),
                 leader_info :: no_leader |
                                {node(),
                                 chronicle:history_id(),
                                 chronicle:leader_term()},

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
    gen_server:start_link(?START_NAME(Name), ?MODULE, [Name, Mod, ModArgs], []).

command(Name, Command) ->
    command(Name, Command, 5000).

command(Name, Command, Timeout) ->
    %% TODO: deal with errors
    {ok, {Leader, _, _}} = chronicle_leader:get_leader(),
    ?DEBUG("Sending Command to ~p: ~p", [Leader, Command]),
    gen_server:call(?SERVER(Leader, Name), {command, Command}, Timeout).

query(Name, Query) ->
    query(Name, Query, 5000).

query(Name, Query, Timeout) ->
    gen_server:call(?SERVER(Name), {query, Query}, Timeout).

get_applied_revision(Name, Type, Timeout) ->
    %% TODO: deal with errors
    {ok, {Leader, _, _}} = chronicle_leader:get_leader(),
    gen_server:call(?SERVER(Leader, Name),
                    {get_applied_revision, Type}, Timeout).

sync_revision(Name, Revision, Timeout) ->
    case gen_server:call(?SERVER(Name), {sync_revision, Revision, Timeout}) of
        ok ->
            ok;
        {error, Timeout} ->
            exit({timeout, {sync_revision, Name, Revision, Timeout}})
    end.

sync(Name, Type, Timeout) ->
    chronicle_utils:run_on_process(
      fun () ->
              case get_applied_revision(Name, Type, infinity) of
                  {ok, Revision} ->
                      %% TODO: this use of timeout is ugly
                      sync_revision(Name, Revision, Timeout + 1000);
                  {error, _} = Error ->
                      %% TODO: deal with not_leader errors
                      Error
              end
      end, Timeout).

%% gen_server callbacks
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

            State = #state{name = Name,
                           leader_info = no_leader,
                           applied_history_id = ?NO_HISTORY,
                           applied_seqno = ?NO_SEQNO,
                           available_seqno = CommittedSeqno,
                           pending_clients = #{},
                           sync_revision_requests = gb_trees:empty(),
                           mod = Mod,
                           mod_state = ModState,
                           mod_data = ModData},

            {ok, maybe_start_reader(State)};
        Other ->
            Other
    end.

handle_call({command, Command}, From, State) ->
    handle_command(Command, From, State);
handle_call({query, Query}, _From, State) ->
    handle_query(Query, State);
handle_call({sync_revision, Revision, Timeout}, From, State) ->
    handle_sync_revision(Revision, Timeout, From, State);
handle_call({get_applied_revision, Type}, From, State) ->
    handle_get_applied_revision(Type, From, State);
handle_call(Call, _From, State) ->
    ?WARNING("Unexpected call ~p", [Call]),
    {reply, nack, State}.

handle_cast({entries, HighSeqno, Entries}, State) ->
    handle_entries(HighSeqno, Entries, State);
handle_cast(Cast, _State) ->
    {stop, {unexpected_cast, Cast}}.

handle_info({?RSM_TAG, chronicle_event, Event}, State) ->
    handle_chronicle_event(Event, State);
handle_info({{?RSM_TAG, sync_quorum, Ref}, Result}, State) ->
    handle_sync_quorum_result(Ref, Result, State);
handle_info({?RSM_TAG, sync_revision_timeout, Request}, State) ->
    handle_sync_revision_timeout(Request, State);
handle_info({'DOWN', _, process, Pid, Reason}, #state{reader = Reader})
  when Reader =:= Pid ->
    {stop, {reader_died, Pid, Reason}};
handle_info(Msg, #state{mod = Mod,
                        mod_state = ModState,
                        mod_data = ModData} = State) ->
    case Mod:handle_info(Msg, ModState, ModData) of
        {noreply, NewModData} ->
            {noreply, set_mod_data(NewModData, State)};
        {stop, _} = Stop ->
            Stop
    end.

terminate(Reason, #state{mod = Mod,
                         mod_state = ModState,
                         mod_data = ModData}) ->
    Mod:terminate(Reason, ModState, ModData).

%% internal
handle_command(Command, From, State) ->
    case is_leader(State) of
        true ->
            handle_command_leader(Command, From, State);
        false ->
            {reply, {error, not_leader}, State}
    end.

handle_command_leader(Command, From, #state{mod = Mod,
                                            mod_state = ModState,
                                            mod_data = ModData} = State) ->
    case Mod:handle_command(Command, ModState, ModData) of
        {apply, NewModData} ->
            NewState = set_mod_data(NewModData, State),
            {noreply, submit_command(Command, From, NewState)};
        {reject, Reply, NewData} ->
            {reply, Reply, set_mod_data(NewData, State)}
    end.

handle_query(Query, #state{mod = Mod,
                           mod_state = ModState,
                           mod_data = ModData} = State) ->
    {reply, Reply, NewModData} = Mod:handle_query(Query, ModState, ModData),
    {reply, Reply, set_mod_data(NewModData, State)}.

handle_sync_revision({HistoryId, Seqno}, Timeout, From,
                     #state{applied_history_id = AppliedHistoryId,
                            applied_seqno = AppliedSeqno} = State) ->
    case HistoryId =:= AppliedHistoryId of
        true ->
            case Seqno =< AppliedSeqno of
                true ->
                    {reply, ok, State};
                false ->
                    {noreply,
                     sync_revision_add_request(Seqno, Timeout, From, State)}
            end;
        false ->
            %% We may hit this case even if we in fact do have the revision
            %% that the client passed. To handle such cases properly, we'd
            %% have to not only keep track of the current history id, but also
            %% of all history ids that we've seen and corresponding ranges of
            %% sequence numbers where they apply. Since this case is pretty
            %% rare, for the sake of simplicity we'll just tolerate a
            %% possibility of sync_revision() call failing unnecessarily.
            {reply, {error, history_mismatch}, State}
    end.

sync_revision_add_request({HistoryId, Seqno}, Timeout, From,
                          #state{sync_revision_requests = Requests} = State) ->
    Request = {Seqno, make_ref()},
    TRef = sync_revision_start_timer(Request, Timeout),
    RequestData = {From, TRef, HistoryId},
    NewRequests = gb_trees:insert(Request, RequestData, Requests),
    State#state{sync_revision_requests = NewRequests}.

sync_revision_requests_reply(#state{applied_seqno = Seqno,
                                    sync_revision_requests = Requests} =
                                 State) ->
    NewRequests = sync_revision_requests_reply_loop(Seqno, Requests),
    State#state{sync_revision_requests = NewRequests}.

sync_revision_requests_reply_loop(Seqno, Requests) ->
    case gb_trees:is_empty(Requests) of
        true ->
            Requests;
        false ->
            {{{ReqSeqno, _} = Request, RequestData}, NewRequests} =
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
    gen_server:reply(From, Reply).

sync_revision_drop_diverged_requests(#state{applied_history_id = HistoryId,
                                            sync_revision_requests = Requests} =
                                         State) ->
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

    State#state{sync_revision_requests = NewRequests}.

sync_revision_start_timer(Request, Timeout) ->
    erlang:send_after(Timeout, self(),
                      {?RSM_TAG, sync_revision_timeout, Request}).

sync_revision_cancel_timer(Request, TRef) ->
    erlang:cancel_timer(TRef),
    ?FLUSH({?RSM_TAG, sync_revision_timeout, Request}).

handle_sync_revision_timeout(Request,
                             #state{sync_revision_requests = Requests} =
                                 State) ->
    {{From, _}, NewRequests} = gb_trees:take(Request, Requests),
    gen_server:reply(From, {error, timeout}),
    {noreply, State#state{sync_revision_requests = NewRequests}}.

handle_entries(HighSeqno, Entries, #state{reader = Reader,
                                          reader_mref = MRef} = State) ->
    true = is_pid(Reader),
    true = is_reference(MRef),

    erlang:demonitor(MRef, [flush]),

    NewState0 = State#state{reader = undefined, reader_mref = undefined},
    NewState = apply_entries(HighSeqno, Entries, NewState0),
    {noreply, maybe_start_reader(NewState)}.

apply_entries(HighSeqno, Entries, #state{applied_history_id = HistoryId,
                                         mod_state = ModState,
                                         mod_data = ModData} = State) ->
    {NewHistoryId, NewModState, NewModData, Replies} =
        lists:foldl(
          fun (Entry, Acc) ->
                  apply_entry(Entry, Acc, State)
          end, {HistoryId, ModState, ModData, []}, Entries),

    ?DEBUG("Applied commands to rsm '~p'.~n"
           "New applied seqno: ~p~n"
           "Commands:~n~p~n"
           "Replies:~n~p",
           [State#state.name, HighSeqno, Entries, Replies]),

    NewState0 = State#state{mod_state = NewModState,
                            mod_data = NewModData,
                            applied_history_id = NewHistoryId,
                            applied_seqno = HighSeqno},
    NewState1 =
        case HistoryId =:= NewHistoryId of
            true ->
                NewState0;
            false ->
                %% Drop requests that have the history id different from the
                %% one we just adopted. See the comment in
                %% handle_sync_revision/4 for more context.
                sync_revision_drop_diverged_requests(NewState0)
        end,

    NewState = sync_revision_requests_reply(NewState1),
    pending_commands_reply(Replies, NewState).

apply_entry(Entry, {HistoryId, ModState, ModData, Replies} = Acc,
            #state{mod = Mod} = State) ->
    #log_entry{value = Value} = Entry,
    case Value of
        #rsm_command{id = Id, rsm_name = Name, command = Command} ->
            true = (Name =:= State#state.name),
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

pending_commands_reply(Replies, #state{pending_clients = Clients} = State) ->
    case is_leader(State) of
        true ->
            #state{leader_info = {_, _, OurTerm}} = State,

            NewClients =
                lists:foldl(
                  fun ({Ref, Term, Reply}, Acc) ->
                          pending_command_reply(Ref, Term, Reply, OurTerm, Acc)
                  end, Clients, Replies),

            State#state{pending_clients = NewClients};
        false ->
            State
    end.

pending_command_reply(Ref, Term, Reply, OurTerm, Clients) ->
    %% References are not guaranteed to be unique across restarts. So
    %% theoretically it's possible that we'll reply to the wrong client. So we
    %% are also checking that the corresponding entry was proposed in our
    %% term.
    case Term =:= OurTerm of
        true ->
            case maps:take(Ref, Clients) of
                {{From, command}, NewClients} ->
                    gen_server:reply(From, Reply),
                    NewClients;
                error ->
                    Clients
            end;
        false ->
            Clients
    end.

handle_get_applied_revision(Type, From, State) ->
    case is_leader(State) of
        true ->
            handle_get_applied_revision_leader(Type, From, State);
        false ->
            {reply, {error, not_leader}, State}
    end.

handle_get_applied_revision_leader(Type, From, State) ->
    #state{applied_seqno = Seqno, applied_history_id = HistoryId} = State,
    Revision = {HistoryId, Seqno},

    case Type of
        leader ->
            {reply, {ok, Revision}, State};
        quorum ->
            {noreply, sync_quorum(Revision, From, State)}
    end.

sync_quorum(Revision, From,
            #state{leader_info = {_, HistoryId, Term}} = State) ->
    Ref = make_ref(),
    Tag = {?RSM_TAG, sync_quorum, Ref},
    chronicle_server:sync_quorum(Tag, HistoryId, Term),
    add_pending_client(Ref, From, {sync, Revision}, State).

handle_sync_quorum_result(Ref, Result,
                          #state{pending_clients = Requests} = State) ->
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
            gen_server:reply(From, Reply),
            {noreply, State#state{pending_clients = NewRequests}};
        error ->
            %% Possible if we got notified that the leader has changed before
            %% local proposer was terminated.
            {noreply, State}
    end.

is_interesting_event({leader, _}) ->
    true;
is_interesting_event({metadata, _}) ->
    true;
is_interesting_event(_) ->
    false.

handle_chronicle_event({leader, LeaderInfo}, State) ->
    handle_new_leader(LeaderInfo, State);
handle_chronicle_event({metadata, Metadata}, State) ->
    handle_new_metadata(Metadata, State).

handle_new_leader(NewLeaderInfo, #state{leader_info = OldLeaderInfo} = State) ->
    case NewLeaderInfo =:= OldLeaderInfo of
        true ->
            {noreply, State};
        false ->
            NewState = State#state{leader_info = NewLeaderInfo},
            {noreply, flush_pending_clients({error, leader_gone}, NewState)}
    end.

handle_new_metadata(#metadata{committed_seqno = CommittedSeqno}, State) ->
    NewState = State#state{available_seqno = CommittedSeqno},
    {noreply, maybe_start_reader(NewState)}.

maybe_start_reader(#state{reader = undefined,
                          applied_seqno = AppliedSeqno,
                          available_seqno = AvailableSeqno} = State) ->
    case AvailableSeqno > AppliedSeqno of
        true ->
            start_reader(State);
        false ->
            State
    end;
maybe_start_reader(State) ->
    State.

start_reader(State) ->
    Self = self(),
    {Pid, MRef} = spawn_monitor(fun () -> reader(Self, State) end),
    State#state{reader = Pid, reader_mref = MRef}.

reader(Parent, State) ->
    {HighSeqno, Commands} = get_log(State),
    gen_server:cast(Parent, {entries, HighSeqno, Commands}).

get_log(#state{name = Name,
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

is_leader(#state{leader_info = LeaderInfo}) ->
    case LeaderInfo of
        no_leader ->
            false;
        {Leader, _, _} ->
            Leader =:= ?PEER()
    end.

submit_command(Command, From,
               #state{name = Name,
                      leader_info = {_, HistoryId, Term}} = State) ->
    Ref = make_ref(),
    chronicle_server:rsm_command(HistoryId, Term, Name, Ref, Command),
    add_pending_client(Ref, From, command, State).

add_pending_client(Ref, From, Data,
                   #state{pending_clients = Clients} = State) ->
    State#state{pending_clients = maps:put(Ref, {From, Data}, Clients)}.

flush_pending_clients(Reply, #state{pending_clients = Clients} = State) ->
    maps:fold(
      fun (_, {From, _}, _) ->
              gen_server:reply(From, Reply)
      end, unused, Clients),
    State#state{pending_clients = #{}}.

set_mod_data(ModData, State) ->
    State#state{mod_data = ModData}.
