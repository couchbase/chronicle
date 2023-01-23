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

-behavior(gen_statem).

-include("chronicle.hrl").

-export([start_link/4]).
-export([command/2, command/3, query/2, query/3,
         get_local_revision/1, sync_revision/3, sync/2,
         note_leader_status/2]).

-export([unpack_payload/2]).
-export([map_snapshot/2, format_snapshot/1]).
-export([callback_mode/0,
         format_status/2, sanitize_event/2,
         init/1, handle_event/4, terminate/3]).

-import(chronicle_utils, [call/2, call/3, call/4,
                          read_deadline/1, start_timeout/1]).

%% If it takes longer than this time to initialize, then we probably hit some
%% bug.
-define(INIT_TIMEOUT, chronicle_settings:get({rsm, init_timeout}, 60000)).

-define(RETRY_AFTER, chronicle_settings:get({rsm, retry_after}, 100)).
-define(MAX_BACKOFF, chronicle_settings:get({rsm, max_backoff}, 32)).
-define(DOWN_INTERVAL, chronicle_settings:get({rsm, down_interval}, 10000)).
-define(NOOP_INTERVAL, chronicle_settings:get({rsm, noop_interval}, 10000)).

-define(MAX_REPLIES_PER_PEER,
        chronicle_settings:get({rsm, max_replies_per_peer}, 500)).

-define(RSM_TAG, '$rsm').
-define(SERVER(Name), ?SERVER_NAME(Name)).
-define(SERVER(Peer, Name), ?SERVER_NAME(Peer, Name)).

-define(LOCAL_REVISION_KEY(Name), {?RSM_TAG, Name, local_revision}).

-type sync_revision_requests() ::
        gb_trees:tree(
          {chronicle:seqno(), reference()},
          {From :: any(), Timer :: reference(), chronicle:history_id()}).

-record(init, { wait_for_seqno }).
-record(no_leader, {}).
-record(follower, { leader :: chronicle:peer(),
                    history_id :: chronicle:history_id(),
                    term :: chronicle:leader_term() }).
-record(leader, { history_id :: chronicle:history_id(),
                  term :: chronicle:leader_term() }).

-record(peer_state, { incarnation :: chronicle:incarnation(),
                      min_serial :: chronicle:serial(),
                      replies :: #{chronicle:serial() => any()} }).

-type peer_states() :: #{chronicle:peer_id() => #peer_state{}}.

-record(snapshot, { applied_history_id :: chronicle:history_id(),
                    applied_seqno :: chronicle:seqno(),
                    mod :: module(),
                    mod_state :: any(),
                    peer_states :: peer_states() }).

-record(request, { ref,
                   tref,
                   deadline,
                   reply_to,
                   request
                 }).

-record(data, { name :: atom(),

                peer_id :: chronicle:peer_id(),
                incarnation :: chronicle:incarnation(),

                serial :: chronicle:serial(),

                min_serial_in_use :: chronicle:serial(),
                max_serial_to_forward :: chronicle:serial(),
                serials_in_use :: gb_trees:tree(chronicle:serial(),
                                                reference()),

                applied_history_id :: chronicle:history_id(),
                applied_seqno :: chronicle:seqno(),
                read_seqno :: chronicle:seqno(),

                sync_revision_requests :: sync_revision_requests(),

                leader_requests,
                leader_sender,
                leader_status = down :: active | down,

                leader_backoff,
                leader_last_retry,

                local_requests,

                config :: undefined | #config{},
                config_peers :: sets:set(chronicle:peer_id()),
                peer_states :: peer_states(),

                max_replies_per_peer,

                mod :: atom(),
                mod_state :: any(),
                mod_data :: any() }).

start_link(Name, PeerId, Mod, ModArgs) ->
    gen_statem:start_link(?START_NAME(Name), ?MODULE,
                          [Name, PeerId, Mod, ModArgs], []).

command(Name, Command) ->
    command(Name, Command, 5000).

command(Name, Command, Timeout) ->
    leader_request(Name, {command, pack_command(Command)}, Timeout).

query(Name, Query) ->
    query(Name, Query, 5000).

query(Name, Query, Timeout) ->
    call(?SERVER(Name), {query, Query}, query, Timeout).

sync(Name, Timeout) ->
    leader_request(Name, sync, Timeout).

leader_request(Name, Request, TRef) ->
    Deadline = read_deadline(TRef),
    case call(?SERVER(Name),
              {leader_request, Request, Deadline}, leader_request,
              infinity) of
        ok ->
            ok;
        {ok, Reply} ->
            Reply;
        {error, Error} ->
            exit(Error)
    end.

get_local_revision(Name) ->
    case get_published_revision(Name) of
        {ok, {HistoryId, Seqno, _}} ->
            {HistoryId, Seqno};
        not_found ->
            %% The process might still be initializing. So fall back to a call.
            call(?SERVER(Name), get_local_revision)
    end.

get_published_revision(Name) ->
    chronicle_ets:get(?LOCAL_REVISION_KEY(Name)).

sync_revision(Name, Revision, Timeout) ->
    case sync_revision_fast(Name, Revision) of
        ok ->
            ok;
        use_slow_path ->
            Request = {sync_revision, Revision, read_deadline(Timeout)},
            case call(?SERVER(Name), Request, infinity) of
                ok ->
                    ok;
                {error, Error} ->
                    exit(Error)
            end
    end.

sync_revision_fast(Name, {RevHistoryId, RevSeqno}) ->
    case get_published_revision(Name) of
        {ok, {LocalHistoryId, _, LocalSeqno}}
          when LocalHistoryId =:= RevHistoryId andalso LocalSeqno >= RevSeqno ->
            ok;
        _ ->
            use_slow_path
    end.

note_leader_status(Pid, LeaderStatus) ->
    gen_statem:cast(Pid, {leader_status, LeaderStatus}).

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
                     Data#data{mod_state = omitted,
                               leader_requests = omitted,
                               peer_states = #{}};
                 _ ->
                     %% During gen_statem initialization Data may be undefined.
                     Data
             end}
    end.

map_snapshot(Fun, #snapshot{mod = Mod, mod_state = ModState} = Snapshot) ->
   Snapshot#snapshot{mod_state = Fun(Mod, ModState)}.

format_snapshot(#snapshot{applied_history_id = HistoryId,
                          applied_seqno = Seqno,
                          peer_states = PeerStates,
                          mod = Mod,
                          mod_state = ModState}) ->
    [{"History id", HistoryId},
     {"Seqno", Seqno},
     {"Peer states", format_peer_states(PeerStates)},
     {"RSM module", Mod},
     {"RSM state", format_mod_state(Mod, ModState)}].

format_peer_states(PeerStates) ->
    maps:to_list(
      maps:map(
        fun (_PeerId, #peer_state{incarnation = Incarnation,
                                 min_serial = MinSerial,
                                 replies = Replies}) ->
                [{"Incarnation", Incarnation},
                 {"Min serial", MinSerial},
                 {"Recent replies", Replies}]
        end, PeerStates)).

format_mod_state(Mod, ModState) ->
    case chronicle_utils:is_function_exported(Mod, format_state, 1) of
        true ->
            Mod:format_state(ModState);
        false ->
            chronicle_dump:raw(ModState)
    end.

sanitize_event({call, _} = Type,
               {leader_request, Request, Deadline} = LeaderRequest) ->
    case Request of
        {command, _} ->
            {Type, {leader_request, {command, '...'}, Deadline}};
        _ ->
            {Type, LeaderRequest}
    end;
sanitize_event(cast, {leader_request, ReplyTo, _}) ->
    {cast, {leader_request, ReplyTo, '...'}};
sanitize_event(cast, {leader_request_result, Ref, _}) ->
    {cast, {leader_request_result, Ref, '...'}};
sanitize_event({call, _} = Type, {query, _}) ->
    {Type, {query, '...'}};
sanitize_event(Type, Event) ->
    {Type, Event}.

init([Name, PeerId, Mod, ModArgs]) ->
    case Mod:init(Name, ModArgs) of
        {ok, ModState, ModData} ->
            ok = chronicle_ets:register_writer([?LOCAL_REVISION_KEY(Name)]),

            MaxRepliesPerPeer = ?MAX_REPLIES_PER_PEER,

            RSMDir = chronicle_storage:ensure_rsm_dir(Name),
            Incarnation = get_incarnation(RSMDir),

            Data0 = #data{name = Name,

                          peer_id = PeerId,
                          incarnation = Incarnation,
                          serial = 0,

                          min_serial_in_use = 0,
                          max_serial_to_forward = MaxRepliesPerPeer,
                          serials_in_use = gb_trees:empty(),

                          max_replies_per_peer = MaxRepliesPerPeer,

                          applied_history_id = ?NO_HISTORY,
                          applied_seqno = ?NO_SEQNO,
                          read_seqno = ?NO_SEQNO,
                          sync_revision_requests = gb_trees:empty(),
                          leader_requests = #{},
                          local_requests = #{},

                          config = undefined,
                          config_peers = sets:new(),
                          peer_states = #{},

                          mod = Mod,
                          mod_state = ModState,
                          mod_data = ModData},

            Self = self(),
            chronicle_events:subscribe(
              ?RSM_EVENTS,
              fun (Event) ->
                      gen_statem:cast(Self, {event, Event})
              end),

            Data = maybe_restore_snapshot(Data0),
            {State, Effects} = init_from_agent(Data0),

            {ok, State, Data,
             [{state_timeout, ?INIT_TIMEOUT, init_timeout} | Effects]};
        {stop, _} = Stop ->
            Stop
    end.

complete_init(Effects, #init{}, #data{name = Name} = Data) ->
    publish_local_revision(Data),
    LeaderStatus = chronicle_server:register_rsm(Name, self()),
    FinalEffects =
        [{next_event, cast, {leader_status, LeaderStatus}} | Effects],

    case call_callback(post_init, Data) of
        {ok, NewModData} ->
            {next_state,
             #no_leader{},
             set_mod_data(NewModData, Data),
             FinalEffects};
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
handle_event(state_timeout, retry_leader, State, Data) ->
    handle_retry_leader(State, Data);
handle_event(state_timeout, maybe_send_noop, State, Data) ->
    handle_maybe_send_noop(State, Data);
handle_event({call, From}, Call, State, Data) ->
    case State of
        #init{} ->
            {keep_state_and_data, postpone};
        _ ->
            handle_call(Call, From, State, Data)
    end;
handle_event(cast, {leader_status, LeaderStatus}, State, Data) ->
    handle_leader_status(LeaderStatus, State, Data);
handle_event(cast, {event, Event}, State, Data) ->
    case Event of
        {seqno_committed, Seqno} ->
            handle_seqno_committed(Seqno, State, Data);
        {take_snapshot, Seqno} ->
            handle_take_snapshot(Seqno, State, Data)
    end;
handle_event(cast, {noop, PeerId, Incarnation, SeenSerial}, State, Data) ->
    handle_noop(PeerId, Incarnation, SeenSerial, State, Data);
handle_event(cast, {leader_request, ReplyTo, Request}, State, Data) ->
    handle_leader_request(ReplyTo, Request, State, Data);
handle_event(cast, {leader_request_result, Ref, Result}, State, Data) ->
    handle_leader_request_result(Ref, Result, State, Data);
handle_event(info, {?RSM_TAG, request_timeout, Ref}, State, Data) ->
    handle_request_timeout(Ref, State, Data);
handle_event(info, {?RSM_TAG, leader_down}, State, Data) ->
    handle_leader_down(State, Data);
handle_event(info, {{?RSM_TAG, sync_quorum, ReplyTo}, Result}, State, Data) ->
    handle_sync_quorum_result(ReplyTo, Result, State, Data);
handle_event(info, {?RSM_TAG, sync_revision_timeout, Request}, State, Data) ->
    handle_sync_revision_timeout(Request, State, Data);
handle_event(info, {'EXIT', Pid, _}, _State, #data{leader_sender = Sender})
  when Pid =:= Sender ->
    %% In case the callback module set trap_exit to true
    {stop, {leader_sender_died, Pid}};
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

terminate(Reason, State, Data) ->
    _ = cleanup_after_leader(State, Data),
    call_callback(terminate, [Reason], Data).

%% internal
handle_call({leader_request, Request, Deadline}, From, State, Data) ->
    handle_leader_request_call(Request, Deadline, From, State, Data);
handle_call({query, Query}, From, State, Data) ->
    handle_query(Query, From, State, Data);
handle_call(get_local_revision, From, State, Data) ->
    handle_get_local_revision(From, State, Data);
handle_call({sync_revision, Revision, Deadline}, From, State, Data) ->
    handle_sync_revision(Revision, Deadline, From, State, Data);
handle_call(Call, From, _State, _Data) ->
    ?WARNING("Unexpected call ~p", [Call]),
    {keep_state_and_data, [{reply, From, nack}]}.

add_request(RawRequest, Deadline, ReplyTo, Field, Data) ->
    add_request(make_ref(), RawRequest, Deadline, ReplyTo, Field, Data).

add_request(Ref, RawRequest, Deadline, ReplyTo, Field, Data) ->
    TRef =
        case Deadline of
            infinity ->
                undefined;
            _ when is_integer(Deadline) ->
                erlang:send_after(Deadline, self(),
                                  {?RSM_TAG, request_timeout, Ref},
                                  [{abs, true}])
        end,

    Request = #request{ref = Ref,
                       tref = TRef,
                       deadline = Deadline,
                       reply_to = ReplyTo,
                       request = RawRequest},

    Requests = element(Field, Data),
    NewRequests = Requests#{Ref => Request},

    {Request, setelement(Field, Data, NewRequests)}.

cancel_request_timer(#request{ref = Ref, tref = TRef}) ->
    case TRef of
        undefined ->
            ok;
        _ ->
            _ = erlang:cancel_timer(TRef),
            ?FLUSH({?RSM_TAG, request_timeout, Ref}),
            ok
    end.

add_leader_request(Request, Deadline, From, Data) ->
    add_request(Request, Deadline, {from, From}, #data.leader_requests, Data).

get_requests_to_forward(#data{leader_requests = LeaderRequests,
                              min_serial_in_use = MinSerial,
                              max_serial_to_forward = MaxSerial}) ->
    SeenSerial = MinSerial - 1,
    maps:fold(
      fun (_, #request{request = RawRequest} = Request, Acc) ->
              case RawRequest of
                  {command, _, _, Serial, _, _} ->
                      case Serial =< MaxSerial of
                          true ->
                              [update_seen_serial(Request, SeenSerial) | Acc];
                          false ->
                              Acc
                      end;
                  _ ->
                      [Request | Acc]
              end
      end, [], LeaderRequests).

update_seen_serial(#request{request = RawRequest} = Request, SeenSerial) ->
    {command, PeerId, Incarnation, Serial, _, Command} = RawRequest,
    NewRawRequest = {command, PeerId, Incarnation, Serial, SeenSerial, Command},
    Request#request{request = NewRawRequest}.

get_requests_by_serials(From, To, #data{serials_in_use = Serials,
                                        min_serial_in_use = MinSerial,
                                        leader_requests = Requests}) ->
    SeenSerial = MinSerial - 1,
    Iter = gb_trees:iterator_from(From, Serials),
    get_requests_by_serials_loop(Iter, To, Requests, SeenSerial, []).

get_requests_by_serials_loop(Iter, To, Requests, SeenSerial, Acc) ->
    case gb_trees:next(Iter) of
        none ->
            Acc;
        {Serial, Ref, NewIter} ->
            case Serial =< To of
                true ->
                    Request0 = maps:get(Ref, Requests),
                    Request = update_seen_serial(Request0, SeenSerial),
                    NewAcc = [Request | Acc],
                    get_requests_by_serials_loop(NewIter, To,
                                                 Requests, SeenSerial, NewAcc);
                false ->
                    Acc
            end
    end.

maybe_update_serials(#request{request = Request},
                     State,
                     #data{serial = NextSerial,
                           min_serial_in_use = MinSerial,
                           max_serial_to_forward = MaxSerial,
                           serials_in_use = Serials,
                           max_replies_per_peer = MaxRepliesPerPeer} = Data) ->
    case Request of
        {command, _, _, Serial, _, _} ->
            {_, NewSerials} = gb_trees:take(Serial, Serials),
            NewMinSerial =
                case gb_trees:is_empty(NewSerials) of
                    true ->
                        NextSerial;
                    false ->
                        {S, _} = gb_trees:smallest(NewSerials),
                        S
                end,
            NewData = Data#data{serials_in_use = NewSerials},
            case MinSerial =:= NewMinSerial of
                true ->
                    {NewData, []};
                false ->
                    NewMaxSerial = NewMinSerial + MaxRepliesPerPeer,
                    Requests = get_requests_by_serials(
                                 MaxSerial + 1, NewMaxSerial, NewData),
                    {NewData#data{min_serial_in_use = NewMinSerial,
                                  max_serial_to_forward = NewMaxSerial},
                     maybe_forward_requests(Requests, State, NewData)}
            end;
        _ ->
            {Data, []}
    end.

add_local_request(Request, ReplyTo, Data) ->
    add_local_request(make_ref(), Request, ReplyTo, Data).

add_local_request(Id, Request, ReplyTo, Data) ->
    add_request(Id, Request, infinity, ReplyTo, #data.local_requests, Data).

take_request(Ref, Field, Data) ->
    Requests = element(Field, Data),
    case maps:take(Ref, Requests) of
        error ->
            no_request;
        {Request, NewRequests} ->
            {Request, setelement(Field, Data, NewRequests)}
    end.

take_leader_request(Ref, Data) ->
    take_request(Ref, #data.leader_requests, Data).

take_local_request(Ref, Data) ->
    take_request(Ref, #data.local_requests, Data).

handle_leader_request_call(RawRequest, Deadline, From, State, Data) ->
    case RawRequest of
        {command, _} ->
            handle_leader_request_command(RawRequest,
                                          Deadline, From, State, Data);
        _ ->
            handle_leader_request_other(RawRequest, Deadline, From, State, Data)
    end.

handle_leader_request_command({command, Command}, Deadline, From, State,
                              #data{peer_id = PeerId,
                                    incarnation = Incarnation,
                                    serial = Serial,
                                    min_serial_in_use = MinSerialInUse,
                                    max_serial_to_forward = MaxSerialToForward,
                                    serials_in_use = Serials} = Data) ->
    SeenSerial = MinSerialInUse - 1,
    RawRequest = {command, PeerId, Incarnation, Serial, SeenSerial, Command},
    {Request, NewData0} = add_leader_request(RawRequest, Deadline, From, Data),

    NewSerials = gb_trees:insert(Serial, Request#request.ref, Serials),
    NewSerial = Serial + 1,

    NewData = NewData0#data{serial = NewSerial,
                            serials_in_use = NewSerials},

    case Serial =< MaxSerialToForward of
        true ->
            maybe_forward_leader_request(Request, State, NewData);
        false ->
            {keep_state, NewData}
    end.

handle_leader_request_other(RawRequest, Deadline, From, State, Data) ->
    {Request, NewData} = add_leader_request(RawRequest, Deadline, From, Data),
    maybe_forward_leader_request(Request, State, NewData).

get_forward_mode(State, Data) ->
    case State of
        #leader{} ->
            local;
        #follower{} ->
            case Data#data.leader_status of
                active ->
                    remote;
                _ ->
                    none
            end;
        _ ->
            none
    end.

maybe_forward_leader_request(Request, State, Data) ->
    case get_forward_mode(State, Data) of
        local ->
            {keep_state, Data, leader_request_next_event(Request)};
        remote ->
            leader_sender_forward(Data#data.leader_sender, Request),
            {keep_state, Data};
        none ->
            {keep_state, Data}
    end.

leader_request_next_event(#request{ref = Ref, request = Request}) ->
    ReplyTo = {internal, Ref},
    {next_event, cast, {leader_request, ReplyTo, Request}}.

maybe_forward_pending_leader_requests(State, Data) ->
    maybe_forward_pending_leader_requests([], State, Data).

maybe_forward_pending_leader_requests(Effects, State, Data) ->
    Requests = get_requests_to_forward(Data),
    ForwardEffects = maybe_forward_requests(Requests, State, Data),
    {next_state, State, Data, ForwardEffects ++ Effects}.

maybe_forward_requests(Requests, State, Data) ->
    case get_forward_mode(State, Data) of
        local ->
            [leader_request_next_event(Request) || Request <- Requests];
        remote ->
            Sender = Data#data.leader_sender,
            lists:foreach(
              fun (Request) ->
                      leader_sender_forward(Sender, Request)
              end, Requests),
            [];
        none ->
            []
    end.

handle_request_timeout(Ref, State, Data) ->
    {Request, NewData0} = take_leader_request(Ref, Data),
    {NewData, Effects} = maybe_update_serials(Request, State, NewData0),
    {from, From} = Request#request.reply_to,
    {keep_state, NewData, [{reply, From, {error, timeout}} | Effects]}.

handle_leader_request_result(Ref, Result, State, Data) ->
    case take_leader_request(Ref, Data) of
        {#request{reply_to = ReplyTo} = Request, NewData0} ->
            cancel_request_timer(Request),
            {NewData, Effects} = maybe_update_serials(Request, State, NewData0),
            {from, From} = ReplyTo,

            case leader_request_sync_revision(Request, Result, From, NewData) of
                {reply, Reply} ->
                    {keep_state, NewData, [{reply, From, Reply} | Effects]};
                {noreply, FinalData} ->
                    {keep_state, FinalData, Effects}
            end;
        no_request ->
            keep_state_and_data
    end.

leader_request_sync_revision(Request, Result, From, Data) ->
    case Request#request.request of
        {command, _, _, _, _, _} ->
            case Result of
                {ok, Revision, Reply} ->
                    Deadline = Request#request.deadline,
                    do_handle_sync_revision({ok, Reply},
                                            Revision, Deadline, From, Data);
                _ ->
                    {reply, Result}
            end;
        sync ->
            case Result of
                {ok, Revision} ->
                    Deadline = Request#request.deadline,
                    do_handle_sync_revision(ok, Revision, Deadline, From, Data);
                _ ->
                    {reply, Result}
            end
    end.

handle_leader_down(State, #data{leader_last_retry = LastRetry,
                                leader_sender = Sender} = Data) ->
    #follower{} = State,

    unlink(Sender),
    ?FLUSH({'EXIT', Sender, _}),

    Now = erlang:monotonic_time(millisecond),
    Backoff =
        case LastRetry of
            undefined ->
                1;
            _ ->
                SinceRetry = Now - LastRetry,
                case SinceRetry < ?DOWN_INTERVAL of
                    true ->
                        min(?MAX_BACKOFF, Data#data.leader_backoff * 2);
                    false ->
                        1
                end
        end,

    RetryAfter = ?RETRY_AFTER * Backoff,
    {keep_state,
     Data#data{leader_status = down,
               leader_backoff = Backoff,
               leader_sender = undefined},
     {state_timeout, RetryAfter, retry_leader}}.

handle_retry_leader(State, Data) ->
    Now = erlang:monotonic_time(millisecond),
    NewData0 = Data#data{leader_last_retry = Now, leader_status = active},
    NewData = spawn_leader_sender(State, NewData0),
    maybe_forward_pending_leader_requests(State, NewData).

handle_maybe_send_noop(State, #data{peer_id = PeerId,
                                    incarnation = Incarnation,
                                    min_serial_in_use = MinSerialInUse,
                                    leader_requests = LeaderRequests} = Data) ->
    true = is_record(State, leader) orelse is_record(State, follower),

    SendNoop =
        case maps:size(LeaderRequests) =:= 0 of
            true ->
                case find_peer_state(PeerId, Incarnation, Data) of
                    {ok, #peer_state{min_serial = MinSerial}} ->
                        MinSerialInUse > MinSerial;
                    not_found ->
                        false
                end;
            false ->
                false
        end,

    Effects = schedule_maybe_send_noop(),
    case SendNoop of
        true ->
            {keep_state_and_data, send_noop(Effects, State, Data)};
        false ->
            {keep_state_and_data, Effects}
    end.

schedule_maybe_send_noop() ->
    [{state_timeout, ?NOOP_INTERVAL, maybe_send_noop}].

send_noop(Effects, State,
          #data{peer_id = PeerId,
                incarnation = Incarnation,
                min_serial_in_use = MinSerialInUse} = Data) ->
    SeenSerial = MinSerialInUse - 1,
    case get_forward_mode(State, Data) of
        local ->
            [{next_event, cast,
              {noop, PeerId, Incarnation, SeenSerial}} | Effects];
        remote ->
            Sender = Data#data.leader_sender,
            leader_sender_cast(Sender, {noop, PeerId, Incarnation, SeenSerial}),
            Effects;
        none ->
            Effects
    end.

leader_sender_forward(Sender, Request) ->
    Sender ! {request, Request},
    ok.

leader_sender_cast(Sender, Cast) ->
    Sender ! {cast, Cast},
    ok.

leader_sender(Leader, Parent, #data{name = Name}) ->
    ServerRef = ?SERVER(Leader, Name),
    MRef = chronicle_utils:monitor_process(ServerRef),
    leader_sender_loop(ServerRef, Parent, MRef).

leader_sender_loop(ServerRef, Parent, MRef) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            Parent ! {?RSM_TAG, leader_down};
        {request, Request} ->
            send_leader_request(ServerRef, Parent, Request),
            leader_sender_loop(ServerRef, Parent, MRef);
        {cast, Cast} ->
            gen_statem:cast(ServerRef, Cast),
            leader_sender_loop(ServerRef, Parent, MRef)
    end.

send_leader_request(ServerRef, Parent,
                    #request{ref = RequestRef, request = Request}) ->
    ReplyTo = {cast, Parent, RequestRef},
    gen_statem:cast(ServerRef, {leader_request, ReplyTo, Request}).

handle_leader_request(ReplyTo, Request, State, Data) ->
    case State of
        #leader{} ->
            do_leader_request(Request, ReplyTo, State, Data);
        _ ->
            keep_state_and_data
    end.

reply_leader_request(ReplyTo, Reply) ->
    reply_leader_request(ReplyTo, Reply, []).

reply_leader_request(ReplyTo, Reply, Effects) ->
    case ReplyTo of
        {cast, Pid, Tag} ->
            gen_statem:cast(Pid, {leader_request_result, Tag, Reply}),
            Effects;
        {internal, Tag} ->
            [{next_event, cast, {leader_request_result, Tag, Reply}} | Effects]
    end.

do_leader_request(Request, ReplyTo, State, Data) ->
    case Request of
        {command, PeerId, Incarnation, Serial, SeenSerial, Command} ->
            handle_command(PeerId, Incarnation,
                           Serial, SeenSerial, Command, ReplyTo, State, Data);
        sync ->
            handle_sync(ReplyTo, State, Data)
    end.

handle_command(PeerId, Incarnation, Serial, SeenSerial, Command, ReplyTo,
               #leader{history_id = HistoryId, term = Term},
               #data{name = Name} = Data) ->
    case dedup_command(PeerId, Incarnation, Serial, Data) of
        accept ->
            Id = {PeerId, Incarnation, Serial},

            %% We may be overriding an existing request. But that might only
            %% happen if the client retransmits the request.
            {_, NewData} = add_local_request(Id, command, ReplyTo, Data),

            RSMCommand = #rsm_command{rsm_name = Name,
                                      peer_id = PeerId,
                                      peer_incarnation = Incarnation,
                                      serial = Serial,
                                      seen_serial = SeenSerial,
                                      payload = {command, Command}},
            chronicle_server:rsm_command(HistoryId, Term, RSMCommand),
            {keep_state, NewData};
        {reply, Reply} ->
            {keep_state_and_data, reply_leader_request(ReplyTo, Reply)};
        noreply ->
            %% This is a retransmit of a command that is still pending
            %% response.
            Id = {PeerId, Incarnation, Serial},
            {_, NewData} = add_local_request(Id, command, ReplyTo, Data),
            {keep_state, NewData}
    end.

handle_noop(PeerId, Incarnation, SeenSerial,
            #leader{history_id = HistoryId, term = Term},
            #data{name = Name} = Data) ->
    Submit =
        case find_peer_state(PeerId, Incarnation, Data) of
            {ok, #peer_state{min_serial = MinSerial}} ->
                SeenSerial >= MinSerial;
            not_found ->
                true;
            stale ->
                false
        end,

    case Submit of
        true ->
            RSMCommand = #rsm_command{rsm_name = Name,
                                      peer_id = PeerId,
                                      peer_incarnation = Incarnation,
                                      serial = undefined,
                                      seen_serial = SeenSerial,
                                      payload = noop},
            chronicle_server:rsm_command(HistoryId, Term, RSMCommand);
        false ->
            ok
    end,

    keep_state_and_data;
handle_noop(_, _, _, _, _) ->
    %% not leader
    keep_state_and_data.

dedup_command(PeerId, Incarnation, Serial, Data) ->
    dedup_command(PeerId, Incarnation, Serial, undefined, Data).

dedup_command(PeerId, Incarnation, Serial, Peers, Data) ->
    case find_peer_state(PeerId, Incarnation, Data) of
        {ok, #peer_state{min_serial = MinSerial, replies = Replies}} ->
            case Serial >= MinSerial of
                true ->
                    case maps:find(Serial, Replies) of
                        {ok, Reply} ->
                            Reply;
                        error ->
                            accept
                    end;
                false ->
                    {reply, {error, ambiguous_write}}
            end;
        not_found ->
            case Peers of
                undefined ->
                    accept;
                _ ->
                    case sets:is_element(PeerId, Peers) of
                        true ->
                            accept;
                        false ->
                            {reply, {error, not_peer}}
                    end
            end;
        stale ->
            {reply, {error, ambiguous_write}}
    end.

record_command(PeerId, Incarnation, Serial, SeenSerial, Reply,
               #data{peer_states = PeerStates} = Data) ->
    NewPeerState =
        case find_peer_state(PeerId, Incarnation, Data) of
            {ok, PeerState} ->
                peer_state_add_reply(Serial, SeenSerial,
                                     Reply, PeerState, Data);
            not_found ->
                peer_state_init(Incarnation, Serial, Reply, Data)
        end,

    NewPeerStates = PeerStates#{PeerId => NewPeerState},
    Data#data{peer_states = NewPeerStates}.

record_delayed_replies(Replies, Data) ->
    lists:foldl(
      fun ({Id, Reply}, Acc) ->
              record_delayed_reply(Id, Reply, Acc)
      end, Data, Replies).

record_delayed_reply({PeerId, Incarnation, Serial}, Reply,
                     #data{peer_states = PeerStates} = Data) ->
    case find_peer_state(PeerId, Incarnation, Data) of
        {ok, #peer_state{replies = Replies} = PeerState} ->
            case maps:find(Serial, Replies) of
                {ok, noreply} ->
                    NewReplies = Replies#{Serial => {reply, Reply}},
                    NewPeerState = PeerState#peer_state{replies = NewReplies},
                    Data#data{peer_states =
                                  PeerStates#{PeerId => NewPeerState}};
                _ ->
                    Data
            end;
        stale ->
            Data;
        not_found ->
            Data
    end.

peer_state_add_reply(Serial, SeenSerial, Reply,
                     #peer_state{min_serial = MinSerial,
                                 replies = Replies} = PeerState,
                     #data{max_replies_per_peer = MaxRepliesPerPeer}) ->
    true = (Serial >= MinSerial),
    NewReplies = Replies#{Serial => Reply},
    case SeenSerial >= MinSerial orelse
        Serial > MinSerial + MaxRepliesPerPeer of
        true ->
            NewMinSerial = max(SeenSerial + 1, Serial - MaxRepliesPerPeer),
            PeerState#peer_state{
              min_serial = NewMinSerial,
              replies = prune_replies(NewReplies, MinSerial, NewMinSerial)};
        false ->
            PeerState#peer_state{replies = NewReplies}
    end.

prune_replies(Replies, OldMinSerial, NewMinSerial) ->
    case OldMinSerial >= NewMinSerial of
        true ->
            Replies;
        false ->
            prune_replies(maps:remove(OldMinSerial, Replies),
                          OldMinSerial + 1, NewMinSerial)
    end.

peer_state_init(Incarnation, Serial, Reply,
                #data{max_replies_per_peer = MaxRepliesPerPeer}) ->
    MinSerial = max(0, Serial - MaxRepliesPerPeer),
    #peer_state{incarnation = Incarnation,
                min_serial = MinSerial,
                replies = #{Serial => Reply}}.

find_peer_state(PeerId, Incarnation, #data{peer_states = PeerStates}) ->
    case maps:find(PeerId, PeerStates) of
        {ok, #peer_state{incarnation = SeenIncarnation} = PeerState} ->
            if
                SeenIncarnation =:= Incarnation ->
                    {ok, PeerState};
                Incarnation > SeenIncarnation ->
                    not_found;
                true ->
                    stale
            end;
        error ->
            not_found
    end.

handle_sync(ReplyTo, State, Data) ->
    {keep_state, sync_quorum(ReplyTo, State, Data)}.

sync_quorum(ReplyTo, #leader{history_id = HistoryId, term = Term}, Data) ->
    {Request, NewData} = add_local_request(sync_quorum, ReplyTo, Data),
    Tag = {?RSM_TAG, sync_quorum, Request#request.ref},
    chronicle_server:sync_quorum(Tag, HistoryId, Term),
    NewData.

handle_sync_quorum_result(Ref, Result, State, Data) ->
    case take_local_request(Ref, Data) of
        {#request{reply_to = ReplyTo,
                  request = sync_quorum}, NewData} ->
            case Result of
                {ok, _Revision} ->
                    #leader{} = State,
                    {keep_state, NewData,
                     reply_leader_request(ReplyTo, Result)};
                {error, {leader_error, _}} ->
                    {keep_state, NewData}
            end;
        no_request ->
            keep_state_and_data
    end.

handle_query(Query, From, _State, Data) ->
    {reply, Reply, NewModData} = call_callback(handle_query, [Query], Data),
    {keep_state, set_mod_data(NewModData, Data), {reply, From, Reply}}.

handle_get_local_revision(From, _State,
                          #data{applied_history_id = HistoryId,
                                applied_seqno = Seqno}) ->
    {keep_state_and_data, {reply, From, {HistoryId, Seqno}}}.

handle_sync_revision(Revision, Deadline, From, _State, Data) ->
    case do_handle_sync_revision(ok, Revision, Deadline, From, Data) of
        {reply, Reply} ->
            {keep_state_and_data, {reply, From, Reply}};
        {noreply, NewData} ->
            {keep_state, NewData}
    end.

do_handle_sync_revision(OkReply, {_, Seqno} = Revision, Deadline, From,
                        #data{read_seqno = ReadSeqno, config = Config} =
                            Data) ->
    case check_revision_compatible(Revision, ReadSeqno, Config) of
        ok ->
            case Seqno =< ReadSeqno of
                true ->
                    {reply, OkReply};
                false ->
                    {noreply,
                     sync_revision_add_request(OkReply, Seqno,
                                               Revision, Deadline, From, Data)}
            end;
        {error, _} = Error ->
            {reply, Error}
    end.

check_revision_compatible(Revision, HighSeqno, Config) ->
    case chronicle_config:is_compatible_revision(Revision, HighSeqno, Config) of
        true ->
            ok;
        {false, Info} ->
            {error, {history_mismatch, Info}}
    end.

sync_revision_add_request(OkReply, Seqno, Revision, Deadline, From,
                          #data{sync_revision_requests = Requests} = Data) ->
    Request = {Seqno, make_ref()},
    TRef = sync_revision_start_timer(Request, Deadline),
    RequestData = {From, TRef, Revision, OkReply},
    NewRequests = gb_trees:insert(Request, RequestData, Requests),
    Data#data{sync_revision_requests = NewRequests}.

manage_sync_revision_requests(OldData, NewData) ->
    OldHistoryId = OldData#data.applied_history_id,
    NewHistoryId = NewData#data.applied_history_id,

    FinalData =
        case OldHistoryId =:= NewHistoryId of
            true ->
                NewData;
            false ->
                %% Drop requests that have the history id different from the
                %% one we just adopted. See the comment in
                %% handle_sync_revision/4 for more context.
                sync_revision_drop_diverged_requests(NewData)
        end,

    sync_revision_requests_reply(FinalData).

sync_revision_requests_reply(#data{read_seqno = Seqno,
                                   sync_revision_requests = Requests,
                                   config = Config} = Data) ->
    NewRequests = sync_revision_requests_reply_loop(Seqno, Config, Requests),
    Data#data{sync_revision_requests = NewRequests}.

sync_revision_requests_reply_loop(Seqno, Config, Requests) ->
    case gb_trees:is_empty(Requests) of
        true ->
            Requests;
        false ->
            {{ReqSeqno, _} = Request,
             {_, _, Revision, OkReply} = RequestData, NewRequests} =
                gb_trees:take_smallest(Requests),
            case ReqSeqno =< Seqno of
                true ->
                    Reply =
                        case check_revision_compatible(Revision,
                                                       Seqno, Config) of
                            ok ->
                                OkReply;
                            Other ->
                                Other
                        end,
                    sync_revision_request_reply(Request, RequestData, Reply),
                    sync_revision_requests_reply_loop(Seqno,
                                                      Config, NewRequests);
                false ->
                    Requests
            end
    end.

sync_revision_request_reply(Request, {From, TRef, _Revision, _}, Reply) ->
    sync_revision_cancel_timer(Request, TRef),
    gen_statem:reply(From, Reply).

sync_revision_drop_diverged_requests(#data{read_seqno = ReadSeqno,
                                           config = Config,
                                           sync_revision_requests = Requests} =
                                         Data) ->
    NewRequests =
        chronicle_utils:gb_trees_filter(
          fun (Request, {_, _, Revision, _} = RequestData) ->
                  case check_revision_compatible(Revision,
                                                 ReadSeqno, Config) of
                      ok ->
                          true;
                      {error, _} = Error ->
                          sync_revision_request_reply(Request,
                                                      RequestData, Error),
                          false
                  end
          end, Requests),

    Data#data{sync_revision_requests = NewRequests}.

sync_revision_start_timer(_Request, infinity) ->
    undefined;
sync_revision_start_timer(Request, Deadline) ->
    erlang:send_after(Deadline, self(),
                      {?RSM_TAG, sync_revision_timeout, Request},
                      [{abs, true}]).

sync_revision_cancel_timer(_, undefined) ->
    ok;
sync_revision_cancel_timer(Request, TRef) ->
    _ = erlang:cancel_timer(TRef),
    ?FLUSH({?RSM_TAG, sync_revision_timeout, Request}),
    ok.

handle_sync_revision_timeout(Request, _State,
                             #data{sync_revision_requests = Requests} = Data) ->
    {{From, _, _, _}, NewRequests} = gb_trees:take(Request, Requests),
    gen_statem:reply(From, {error, timeout}),
    {keep_state, Data#data{sync_revision_requests = NewRequests}}.

handle_seqno_committed_next_state(Effects, State,
                                  #data{read_seqno = ReadSeqno} = Data) ->
    case State of
        #init{wait_for_seqno = Seqno} ->
            case ReadSeqno >= Seqno of
                true ->
                    complete_init(Effects, State, Data);
                false ->
                    {keep_state, Data, Effects}
            end;
        _ ->
            {keep_state, Data, Effects}
    end.

apply_entries(HighSeqno, Entries, State, Data) ->
    {NewData, Replies} =
        lists:foldl(
          fun (Entry, Acc) ->
                  apply_entry(Entry, Acc)
          end, {Data#data{read_seqno = HighSeqno}, []}, Entries),

    pending_commands_reply(Replies, State, NewData).

apply_entry(Entry, {Data, Replies}) ->
    #data{mod = Mod,
          applied_history_id = HistoryId,
          applied_seqno = Seqno,
          mod_state = ModState,
          mod_data = ModData} = Data,
    #log_entry{value = Value,
               history_id = EntryHistoryId,
               seqno = EntrySeqno} = Entry,
    AppliedRevision = {HistoryId, Seqno},
    Revision = {HistoryId, EntrySeqno},

    case Value of
        #rsm_command{payload = {command, _}} ->
            true = (HistoryId =:= EntryHistoryId),
            {NewData0, NewReplies} =
                apply_command(Value, AppliedRevision, Revision, Replies, Data),
            NewData = NewData0#data{applied_seqno = EntrySeqno},

            {NewData, NewReplies};
        #rsm_command{payload = noop} ->
            true = (HistoryId =:= EntryHistoryId),
            {apply_noop(Value, Data), Replies};
        #config{} = NewConfig ->
            case chronicle_config:is_stable(NewConfig) of
                true ->
                    {ok, NewModState, NewModData, ExtraReplies0} =
                        Mod:handle_config(Entry,
                                          Revision, AppliedRevision,
                                          ModState, ModData),

                    NewData0 = Data#data{applied_seqno = EntrySeqno,
                                         applied_history_id = EntryHistoryId,
                                         config = NewConfig,
                                         config_peers = peer_ids(NewConfig),
                                         mod_state = NewModState,
                                         mod_data = NewModData},
                    ExtraReplies =
                        [{Id, {ok, Reply}} || {Id, Reply} <- ExtraReplies0],
                    NewData = record_delayed_replies(ExtraReplies, NewData0),
                    {prune_peer_states(NewData), ExtraReplies ++ Replies};
                false ->
                    {Data, Replies}
            end
    end.

apply_command(RSMCommand, AppliedRevision, Revision, Replies,
              #data{config_peers = Peers,
                    mod = Mod,
                    mod_state = ModState,
                    mod_data = ModData} = Data) ->
    #rsm_command{rsm_name = Name,
                 peer_id = PeerId,
                 peer_incarnation = Incarnation,
                 serial = Serial,
                 seen_serial = SeenSerial,
                 payload = {command, Command}} = RSMCommand,
    true = (Name =:= Data#data.name),

    Id = {PeerId, Incarnation, Serial},
    case dedup_command(PeerId, Incarnation, Serial, Peers, Data) of
        accept ->
            {Reply, NewReplies, NewModState, NewModData} =
                case Mod:apply_command(Id, unpack_command(Command),
                                       Revision, AppliedRevision,
                                       ModState, ModData) of
                    {reply, Reply0, NewModState0, NewModData0} ->
                        Reply1 = {ok, Revision, Reply0},
                        Replies0 = [{Id, Reply1} | Replies],
                        {{reply, Reply1}, Replies0,
                         NewModState0, NewModData0};
                    {noreply, NewModState0, NewModData0} ->
                        {noreply, Replies, NewModState0, NewModData0}
                end,

            NewData0 = Data#data{mod_state = NewModState,
                                 mod_data = NewModData},
            NewData = record_command(PeerId, Incarnation,
                                     Serial, SeenSerial, Reply, NewData0),
            {NewData, NewReplies};
        {reply, Reply} ->
            {Data, [{Id, Reply} | Replies]};
        noreply ->
            {Data, Replies}
    end.

apply_noop(RSMCommand,
           #data{peer_states = PeerStates} = Data) ->
    #rsm_command{rsm_name = Name,
                 peer_id = PeerId,
                 peer_incarnation = Incarnation,
                 seen_serial = SeenSerial,
                 payload = noop} = RSMCommand,
    true = (Name =:= Data#data.name),

    case find_peer_state(PeerId, Incarnation, Data) of
        {ok, #peer_state{min_serial = MinSerial,
                         replies = Replies} = PeerState} ->
            case SeenSerial >= MinSerial of
                true ->
                    NewMinSerial = SeenSerial + 1,
                    NewReplies = prune_replies(Replies,
                                               MinSerial, NewMinSerial),
                    NewPeerState = PeerState#peer_state{
                                     min_serial = NewMinSerial,
                                     replies = NewReplies},
                    NewPeerStates = PeerStates#{PeerId => NewPeerState},
                    Data#data{peer_states = NewPeerStates};
                false ->
                    Data
            end;
        stale ->
            Data;
        not_found ->
            Data
    end.

prune_peer_states(#data{peer_states = PeerStates,
                        config_peers = Peers} = Data) ->
    NewPeerStates =
        maps:filter(
          fun (Peer, _) ->
                  sets:is_element(Peer, Peers)
          end, PeerStates),
    Data#data{peer_states = NewPeerStates}.

pending_commands_reply(Replies, #leader{}, Data) ->
    lists:foldr(
      fun ({Id, Reply}, Acc) ->
              pending_command_reply(Id, Reply, Acc)
      end, {Data, []}, Replies);
pending_commands_reply(_Replies, _State, Data) ->
    {Data, []}.

pending_command_reply(Id, Reply, {Data, Effects} = Acc) ->
    case take_local_request(Id, Data) of
        {#request{reply_to = ReplyTo, request = command}, NewData} ->
            NewEffects = reply_leader_request(ReplyTo, Reply, Effects),
            {NewData, NewEffects};
        no_request ->
            Acc
    end.

handle_leader_status(Status, State, Data) ->
    NewData = cleanup_after_leader(State, Data),

    case Status of
        {leader, HistoryId, Term} ->
            handle_became_leader(HistoryId, Term, State, Data);
        {follower, Leader, HistoryId, Term} ->
            NewState = #follower{leader = Leader,
                                 history_id = HistoryId,
                                 term = Term},
            handle_became_follower(NewState, NewData);
        no_leader ->
            {next_state, #no_leader{}, call_state_enter(no_leader, NewData)}
    end.

handle_became_leader(HistoryId, Term, State, Data) ->
    true = is_record(State, no_leader) orelse is_record(State, follower),

    NewState = #leader{history_id = HistoryId, term = Term},
    NewData = call_state_enter(leader, Data#data{leader_status = active}),
    maybe_forward_pending_leader_requests(schedule_maybe_send_noop(),
                                          NewState, NewData).

handle_became_follower(State, Data) ->
    NewData0 = Data#data{leader_status = active,
                         leader_backoff = 1,
                         leader_last_retry = undefined},
    NewData1 = spawn_leader_sender(State, NewData0),
    NewData = call_state_enter(follower, NewData1),
    maybe_forward_pending_leader_requests(schedule_maybe_send_noop(),
                                          State, NewData).

spawn_leader_sender(State, Data) ->
    #follower{leader = Leader} = State,
    undefined = Data#data.leader_sender,

    Self = self(),
    Sender = proc_lib:spawn_link(
               fun () ->
                       leader_sender(Leader, Self, Data)
               end),

    Data#data{leader_sender = Sender}.

cleanup_after_leader(State, Data) ->
    case State of
        #leader{} ->
            %% By the time chronicle_rsm receives the notification that the
            %% term has finished, it must have already processed all
            %% notifications from chronicle_agent about commands that are
            %% known to have been committed by the outgoing leader. So there's
            %% not a need to synchronize with chronicle_agent as it was done
            %% previously.

            %% For simplicity, we don't respond to inflight commands. This is
            %% because eventually all other nodes should realize that this
            %% node is not the leader anymore and will retry the requests.
            Data#data{local_requests = #{}};
        #follower{} ->
            Sender = Data#data.leader_sender,

            case Sender of
                undefined ->
                    ok;
                _ when is_pid(Sender) ->
                    chronicle_utils:terminate_linked_process(Sender, shutdown),
                    ?FLUSH({?RSM_TAG, leader_down}),
                    ok
            end,

            Data#data{leader_sender = undefined};
        _ ->
            Data
    end.

handle_seqno_committed(CommittedSeqno, State,
                       #data{read_seqno = ReadSeqno} = Data) ->
    case CommittedSeqno >= ReadSeqno of
        true ->
            {NewData0, Effects} = read_log(CommittedSeqno, State, Data),
            NewData = manage_sync_revision_requests(Data, NewData0),
            maybe_publish_local_revision(State, NewData),
            handle_seqno_committed_next_state(Effects, State, NewData);
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
                           mod = Mod,
                           mod_state = ModState,
                           peer_states = PeerStates}) ->
    true = (Seqno =:= ReadSeqno),

    Snapshot = #snapshot{applied_history_id = AppliedHistoryId,
                         applied_seqno = AppliedSeqno,
                         mod = Mod,
                         mod_state = ModState,
                         peer_states = PeerStates},
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
            {ok, SnapshotSeqno, Config, Snapshot} = get_snapshot(Data),
            true = (SnapshotSeqno >= StartSeqno),

            ?DEBUG("Got log compacted when reading seqnos ~p to ~p. "
                   "Applying snapshot at seqno ~p",
                   [StartSeqno, EndSeqno, SnapshotSeqno]),

            NewData = apply_snapshot(SnapshotSeqno, Config, Snapshot, Data),
            case EndSeqno > SnapshotSeqno of
                true ->
                    %% There are more entries to read.
                    read_log(EndSeqno, State, NewData);
                false ->
                    {NewData, []}
            end
    end.

get_log(StartSeqno, EndSeqno, #data{name = Name}) ->
    case chronicle_agent:get_log_for_rsm(Name, StartSeqno, EndSeqno) of
        {ok, _} = Ok ->
            Ok;
        {error, compacted} = Error ->
            Error
    end.

set_mod_data(ModData, Data) ->
    Data#data{mod_data = ModData}.

call_callback(Callback, Data) ->
    call_callback(Callback, [], Data).

call_callback(Callback, Args, #data{mod = Mod,
                                    mod_state = ModState,
                                    mod_data = ModData,
                                    applied_history_id = AppliedHistoryId,
                                    applied_seqno = AppliedSeqno}) ->
    AppliedRevision = {AppliedHistoryId, AppliedSeqno},
    erlang:apply(Mod, Callback, Args ++ [AppliedRevision, ModState, ModData]).

call_state_enter(State, Data) ->
    {ok, NewModData} = call_callback(state_enter, [State], Data),
    set_mod_data(NewModData, Data).

maybe_publish_local_revision(#init{}, _Data) ->
    %% Don't expose the revision while we're still initializing
    ok;
maybe_publish_local_revision(_, Data) ->
    publish_local_revision(Data).

publish_local_revision(#data{name = Name,
                             applied_history_id = AppliedHistoryId,
                             applied_seqno = AppliedSeqno,
                             read_seqno = ReadSeqno}) ->
    chronicle_ets:put(?LOCAL_REVISION_KEY(Name),
                      {AppliedHistoryId, AppliedSeqno, ReadSeqno}).

init_from_agent(#data{name = Name} = Data) ->
    %% TODO: deal with {error, no_rsm}
    {ok, Info} = chronicle_agent:get_info_for_rsm(Name),
    #{committed_seqno := CommittedSeqno} = Info,

    true = (Data#data.read_seqno =< CommittedSeqno),

    Effects0 = [{next_event, cast, {event, {seqno_committed, CommittedSeqno}}}],
    Effects1 =
        case maps:find(need_snapshot_seqno, Info) of
            {ok, SnapshotSeqno} ->
                true = (SnapshotSeqno =< CommittedSeqno),
                [{next_event, cast, {event, {seqno_committed, SnapshotSeqno}}},
                 {next_event, cast, {event, {take_snapshot, SnapshotSeqno}}} |
                 Effects0];
            error ->
                Effects0
        end,

    {#init{wait_for_seqno = CommittedSeqno}, Effects1}.

get_snapshot(#data{name = Name}) ->
    chronicle_agent:get_rsm_snapshot(Name).

maybe_restore_snapshot(Data) ->
    case get_snapshot(Data) of
        {ok, SnapshotSeqno, Config, Snapshot} ->
            apply_snapshot(SnapshotSeqno, Config, Snapshot, Data);
        {no_snapshot, SnapshotSeqno, Config} ->
            Data#data{read_seqno = SnapshotSeqno, config = Config}
    end.

apply_snapshot(Seqno, Config, Snapshot, Data) ->
    #snapshot{applied_history_id = AppliedHistoryId,
              applied_seqno = AppliedSeqno,
              mod_state = ModState,
              peer_states = PeerStates} = Snapshot,
    Revision = {AppliedHistoryId, AppliedSeqno},
    {ok, ModData} = call_callback(apply_snapshot, [Revision, ModState], Data),

    Data#data{applied_history_id = AppliedHistoryId,
              applied_seqno = AppliedSeqno,
              read_seqno = Seqno,
              mod_state = ModState,
              mod_data = ModData,
              config = Config,
              config_peers = peer_ids(Config),
              peer_states = PeerStates}.

peer_ids(Config) ->
    sets:from_list(chronicle_config:get_peer_ids(Config)).

pack_command(Command) ->
    term_to_binary(Command, [{compressed, 1}]).

unpack_command(PackedCommand) ->
    binary_to_term(PackedCommand).

unpack_payload(Fun, #rsm_command{payload = Payload} = RSMCommand) ->
    case Payload of
        noop ->
            RSMCommand;
        {command, Command} ->
            RSMCommand#rsm_command{
              payload = {command, Fun(unpack_command(Command))}}
    end.

get_incarnation(RSMDir) ->
    Path = filename:join(RSMDir, "incarnation"),
    Incarnation =
        case chronicle_utils:read_int_from_file(Path, no_incarnation) of
            no_incarnation ->
                ?INFO("No incarnation file found at '~s'", [Path]),
                0;
            V when is_integer(V) ->
                V + 1
        end,

    ok = chronicle_utils:store_int_to_file(Path, Incarnation),
    Incarnation.
