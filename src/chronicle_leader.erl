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
%% TODO: incorporate priorities?
-module(chronicle_leader).

-include("chronicle.hrl").

-behavior(gen_statem).
-compile(export_all).

-import(chronicle_utils, [parallel_mapfold/4,
                          term_number/1, term_leader/1,
                          get_position/1, compare_positions/2]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).

-define(TABLE, ?ETS_TABLE(?MODULE)).

-record(leader, { history_id, term }).
-record(follower, { leader, history_id, term }).
-record(observer, {}).
-record(candidate, {}).

-record(data, { metadata,

                %% Since heartbeats are sent frequently, keep a precomputed
                %% list of our peers.
                peers,

                %% used only when the state is #candidate{}
                election_worker,

                %% used to track timers that get auto-canceled when the state
                %% changes
                state_timers = #{},

                leader_waiters = #{} }).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

get_leader() ->
    case get_published_leader() of
        {ok, _} = Ok ->
            Ok;
        {error, no_leader} ->
            %% TODO: timeout
            R = gen_statem:call(?SERVER, {wait_for_leader, 5000}, infinity),
            R
    end.

announce_leader() ->
    gen_statem:cast(?SERVER, announce_leader).

request_vote(Peer, Candidate, HistoryId, Position) ->
    gen_statem:call(?SERVER(Peer),
                    {request_vote, Candidate, HistoryId, Position}, infinity).

%% gen_statem callbacks
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

    ets:new(?TABLE, [named_table, protected, {read_concurrency, true}]),
    {ok, Metadata} = chronicle_agent:get_metadata(),

    %% TODO
    {ok, #observer{}, metadata2data(Metadata)}.

handle_event(enter, OldState, State, Data) ->
    NewData0 = maybe_publish_leader(OldState, State, Data),
    NewData1 = handle_state_leave(OldState, NewData0),
    handle_state_enter(State, NewData1);
handle_event(info, {chronicle_event, Event}, State, Data) ->
    handle_chronicle_event(Event, State, Data);
handle_event(info, {heartbeat, Leader, HistoryId, Term}, State, Data) ->
    handle_heartbeat(Leader, HistoryId, Term, State, Data);
handle_event(info, {'EXIT', Pid, Reason}, State, Data) ->
    handle_process_exit(Pid, Reason, State, Data);
handle_event(info, {timeout, TRef, leader_wait}, State, Data) ->
    handle_leader_wait_timeout(TRef, State, Data);
handle_event(info, {state_timer, Name}, _State, Data) ->
    {ok, _, NewData} = take_state_timer(Name, Data),
    {keep_state, NewData, {next_event, internal, {state_timer, Name}}};
handle_event(internal, {state_timer, election}, State, Data) ->
    handle_election_timeout(State, Data);
handle_event(internal, {state_timer, send_heartbeat}, State, Data) ->
    handle_send_heartbeat(State, Data);
handle_event(cast, announce_leader, State, Data) ->
    handle_announce_leader(State, Data);
handle_event({call, From},
             {request_vote, Candidate, HistoryId, Position}, State, Data) ->
    handle_request_vote(Candidate, HistoryId, Position, From, State, Data);
handle_event({call, From}, {wait_for_leader, Timeout}, State, Data) ->
    handle_wait_for_leader(Timeout, From, State, Data);
handle_event({call, From}, _Call, _State, _Data) ->
    {keep_state_and_data, [{reply, From, nack}]};
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event ~p", [{Type, Event}]),
    keep_state_and_data.

terminate(_Reason, State, Data) ->
    handle_state_leave(State, Data),
    %% Reply with {error, no_leader} to leader waiters, if any.
    reply_to_leader_waiters(State, Data),
    publish_leader(no_leader).

%% internal
handle_state_leave(OldState, #data{election_worker = Worker} = Data) ->
    NewData = cancel_all_state_timers(Data),

    case Worker of
        undefined ->
            NewData;
        _ when is_pid(Worker) ->
            #candidate{} = OldState,
            chronicle_utils:terminate_linked_process(Worker, kill),
            NewData#data{election_worker = undefined}
    end.

handle_state_enter(State, Data) ->
    NewData = start_state_timers(State, Data),

    case State of
        #candidate{} ->
            {keep_state, start_election_worker(NewData)};
        _ ->
            {keep_state, NewData}
    end.

start_state_timers(State, Data) ->
    lists:foldl(
      fun (Timer, AccData) ->
              case Timer of
                  send_heartbeat ->
                      %% schedule to send a heartbeat immediately
                      schedule_send_heartbeat(0, AccData);
                  election ->
                      start_election_timer(State, AccData)
              end
      end, Data, state_timers(State)).

state_timers(#leader{}) ->
    [send_heartbeat];
state_timers(_) ->
    [election].

start_election_timer(State, Data) ->
    start_state_timer(election, get_election_timeout(State, Data), Data).

get_election_timeout(_State, _Data) ->
    %% TODO
    500.

schedule_send_heartbeat(Data) ->
    schedule_send_heartbeat(get_heartbeat_interval(), Data).

schedule_send_heartbeat(Timeout, Data) ->
    start_state_timer(send_heartbeat, Timeout, Data).

get_heartbeat_interval() ->
    %% TODO
    100.

is_interesting_event({metadata, _Metadata}) ->
    true;
is_interesting_event({term_finished, _HistoryId, _Term}) ->
    true;
is_interesting_event(_) ->
    false.

handle_chronicle_event({metadata, Metadata}, State, Data) ->
    handle_new_metadata(Metadata, State, Data);
handle_chronicle_event({term_finished, HistoryId, Term}, State, Data) ->
    handle_term_finished(HistoryId, Term, State, Data).

handle_new_metadata(Metadata, State, Data) ->
    NewData = metadata2data(Metadata, Data),

    case should_become_observer(NewData, Data, State) of
        {true, Reason} ->
            ?INFO("Becoming an observer "
                  "because of a metadata change: ~p", [Reason]),

            {next_state, #observer{}, NewData};
        false ->
            {keep_state, NewData}
    end.

should_become_observer(NewData, OldData, State) ->
    OldHistoryId = get_history_id(OldData),
    NewHistoryId = get_history_id(NewData),

    OldTerm = get_established_term(OldData),
    NewTerm = get_established_term(NewData),

    case {OldHistoryId =:= NewHistoryId, OldTerm =:= NewTerm} of
        {true, true} ->
            false;
        {true, false} ->
            Invalidate =
                case get_active_leader_term(State) of
                    {ok, LeaderTerm} ->
                        %% Some other node might have established a new term
                        %% before our prospective leader.
                        LeaderTerm =/= NewTerm;
                    no_leader ->
                        %% Some node established a new term when we don't know
                        %% that that node is the leader. Reset the state to
                        %% prevent election timeout from expiring and
                        %% interfering with that node. Hopefully we'll receive
                        %% a heartbeat from it soon.
                        true
                end,

            case Invalidate of
                true ->
                    {true, {term_changed, OldTerm, NewTerm}};
                false ->
                    false
            end;
        {false, _} ->
            {true, {history_changed, OldHistoryId, NewHistoryId}}
    end.


metadata2data(Metadata) ->
    metadata2data(Metadata, #data{}).

metadata2data(Metadata, Data) ->
    %% TODO
    Peers = chronicle_proposer:get_establish_peers(Metadata),

    Data#data{metadata = Metadata,
              peers = Peers -- [?PEER()]}.

handle_term_finished(HistoryId, Term, State, Data) ->
    case check_is_leader(HistoryId, Term, State) of
        ok ->
            ?INFO("Term ~p has finished. Stepping down.", [Term]),
            {next_state, #observer{}, Data};
        {error, _} = Error ->
            ?DEBUG("Received stale term_finished message: ~p", [Error]),
            keep_state_and_data
    end.

handle_election_timeout(State, Data) ->
    ?DEBUG("Election timeout when state is: ~p", [State]),

    NewState =
        case State of
            #observer{} ->
                #candidate{};
            _ ->
                #observer{}
        end,

    {next_state, NewState, Data}.

handle_heartbeat(Peer, HistoryId, Term, State, Data) ->
    case ?CHECK(check_history_id(HistoryId, Data),
                check_accept_heartbeat(Term, State, Data)) of
        ok ->
            NewState = #follower{leader = Peer,
                                 history_id = HistoryId,
                                 term = Term},

            %% TODO: it's somwhat ugly that I have to start election timer in
            %% two different places.
            {next_state, NewState, start_election_timer(NewState, Data)};
        Error ->
            %% TODO: this may be too much to log
            ?DEBUG("Rejecting heartbeat ~p: ~p",
                   [{Peer, HistoryId, Term}, Error]),
            keep_state_and_data
    end.

handle_process_exit(Pid, Reason, State,
                    #data{election_worker = Worker} = Data) ->
    case Pid =:= Worker of
        true ->
            handle_election_worker_exit(Reason, State, Data);
        false ->
            {stop, {linked_process_died, Pid, Reason}}
    end.

handle_election_worker_exit(Reason, #candidate{}, Data) ->
    Result =
        case Reason of
            {shutdown, {election_result, R}} ->
                R;
            _ ->
                {error, {election_worker_crashed, Reason}}
        end,

    NewData = Data#data{election_worker = undefined},
    case Result of
        {ok, HistoryId, Term} ->
            NewTerm = chronicle_utils:next_term(Term),
            ?INFO("Going to become a leader in term ~p (history id ~p)",
                  [NewTerm, HistoryId]),
            NewState = #leader{history_id = HistoryId, term = NewTerm},
            {next_state, NewState, NewData};
        {error, _} = Error ->
            ?INFO("Election failed: ~p", [Error]),
            {next_state, #observer{}, NewData}
    end.

handle_request_vote(Candidate, HistoryId, Position, From, State, Data) ->
    case ?CHECK(check_history_id(HistoryId, Data),
                check_grant_vote(Position, State, Data)) of
        ok ->
            LatestTerm = get_established_term(Data),
            NewState = #follower{leader = Candidate,
                                 history_id = HistoryId,
                                 term = tentative},

            {next_state, NewState, Data, {reply, From, {ok, LatestTerm}}};
        Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

handle_wait_for_leader(Timeout, From, State, Data) ->
    case state_leader(State) of
        no_leader ->
            NewData = add_leader_waiter(Timeout, From, Data),
            {keep_state, NewData};
        LeaderInfo ->
            {keep_state_and_data, {reply, From, {ok, LeaderInfo}}}
    end.

handle_leader_wait_timeout(TRef, State,
                           #data{leader_waiters = Waiters} = Data) ->
    no_leader = state_leader(State),
    {From, NewWaiters} = maps:take(TRef, Waiters),
    gen_statem:reply(From, {error, no_leader}),
    {keep_state, Data#data{leader_waiters = NewWaiters}}.

add_leader_waiter(Timeout, From, #data{leader_waiters = Waiters} = Data) ->
    TRef = erlang:start_timer(Timeout, self(), leader_wait),
    NewWaiters = Waiters#{TRef => From},
    Data#data{leader_waiters = NewWaiters}.

reply_to_leader_waiters(State, #data{leader_waiters = Waiters} = Data) ->
    Reply =
        case state_leader(State) of
            no_leader ->
                {error, no_leader};
            LeaderInfo ->
                {ok, LeaderInfo}
        end,

    maps:fold(
      fun (TRef, From, _) ->
              gen_statem:reply(From, Reply),
              erlang:cancel_timer(TRef),
              ?FLUSH({timeout, TRef, _})
      end, unused, Waiters),

    Data#data{leader_waiters = #{}}.

start_election_worker(Data) ->
    Pid = proc_lib:spawn_link(
            fun () ->
                    Result = election_worker(Data),
                    exit({shutdown, {election_result, Result}})
            end),
    Data#data{election_worker = Pid}.

election_worker(#data{metadata = Metadata, peers = Peers} = Data) ->
    LatestTerm = get_established_term(Data),
    HistoryId = get_history_id(Data),
    Position = get_position(Metadata),
    Quorum = get_quorum(Data),

    ?INFO("Starting election.~n"
          "History ID: ~p~n"
          "Log position: ~p~n"
          "Peers: ~p~n"
          "Required quorum: ~p",
          [HistoryId, Position, Peers, Quorum]),

    Leader = ?PEER(),
    CallFun =
        fun (Peer) ->
                request_vote(Peer, Leader, HistoryId, Position)
        end,
    HandleResponse =
        fun (Peer, Resp, Acc) ->
                case Resp of
                    {ok, PeerTerm} ->
                        {no_quorum, Votes, Term} = Acc,
                        NewVotes = [Peer | Votes],
                        NewTerm = max(Term, PeerTerm),

                        case chronicle_proposer:have_quorum(NewVotes, Quorum) of
                            true ->
                                %% TODO: wait a little more to receive more
                                %% responses
                                {stop, {ok, NewTerm}};
                            false ->
                                NewAcc = {no_quorum, NewVotes, NewTerm},
                                {continue, NewAcc}
                        end;
                    {error, _} = Error ->
                        ?DEBUG("Failed to get leader vote from ~p: ~p",
                               [Peer, Error]),
                        {continue, Acc}
                end
        end,

    case Peers =:= [] of
        true ->
            ?INFO("I'm the only peer, so I'm the leader."),
            {ok, HistoryId, LatestTerm};
        false ->
            case parallel_mapfold(CallFun, HandleResponse,
                                  {no_quorum, [Leader], LatestTerm},
                                  Peers) of
                {no_quorum, FinalVotes, _} ->
                    {error, {no_quorum, FinalVotes, Quorum}};
                {ok, FinalTerm} ->
                    {ok, HistoryId, FinalTerm}
            end
    end.

handle_send_heartbeat(State, Data) ->
    send_heartbeat(State, Data),
    {keep_state, schedule_send_heartbeat(Data)}.

send_heartbeat(#leader{history_id = HistoryId,
                       term = Term},
               #data{peers = Peers}) ->
    Heartbeat = {heartbeat, ?PEER(), HistoryId, Term},
    lists:foreach(
      fun (Peer) ->
              send_msg(Peer, Heartbeat)
      end, Peers).

send_msg(Peer, Msg) ->
    ?SEND(?SERVER(Peer), Msg, [nosuspend, noconnect]).

handle_announce_leader(State, _Data) ->
    do_announce_leader(state_leader(State)),
    keep_state_and_data.

maybe_publish_leader(OldState, State, Data) ->
    OldLeaderInfo = state_leader(OldState),
    NewLeaderInfo = state_leader(State),

    case OldLeaderInfo =:= NewLeaderInfo of
        true ->
            Data;
        false ->
            publish_leader(NewLeaderInfo),
            case NewLeaderInfo of
                no_leader ->
                    Data;
                _ ->
                    reply_to_leader_waiters(State, Data)
            end
    end.

check_history_id(HistoryId, Data) ->
    OurHistoryId = get_history_id(Data),
    case HistoryId =:= OurHistoryId of
        true ->
            ok;
        false ->
            {error, {history_mismatch, OurHistoryId}}
    end.

check_is_leader(HistoryId, Term,
                #leader{history_id = OurHistoryId, term = OurTerm}) ->
    case HistoryId =:= OurHistoryId andalso Term =:= OurTerm of
        true ->
            ok;
        false ->
            {error, {wrong_term, Term, OurTerm}}
    end;
check_is_leader(_HistoryId, _Term, State) ->
    {error, {not_a_leader, state_name(State)}}.

check_accept_heartbeat(NewTerm, State, Data) ->
    Term = get_last_known_leader_term(State, Data),
    case Term =:= NewTerm orelse term_number(NewTerm) > term_number(Term) of
        true ->
            ok;
        false ->
            {error, {stale_term, NewTerm, Term}}
    end.

check_grant_vote(PeerPosition, State, #data{metadata = Metadata}) ->
    case State of
        #observer{} ->
            OurPosition = get_position(Metadata),
            case compare_positions(PeerPosition, OurPosition) of
                lt ->
                    {error, {behind, OurPosition}};
                _ ->
                    ok
            end;
        #candidate{} ->
            {error, in_election};
        _ ->
            {error, {have_leader, state_leader(State)}}
    end.

state_name(State) ->
    element(1, State).

start_state_timer(Name, Timeout, Data) ->
    NewData = cancel_state_timer(Name, Data),
    #data{state_timers = StateTimers} = NewData,
    TRef = erlang:send_after(Timeout, self(), {state_timer, Name}),
    NewData#data{state_timers = StateTimers#{Name => TRef}}.

take_state_timer(Name, #data{state_timers = Timers} = Data) ->
    case maps:take(Name, Timers) of
        {TRef, NewTimers} ->
            {ok, TRef, Data#data{state_timers = NewTimers}};
        error ->
            not_found
    end.

cancel_state_timer(Name, Data) ->
    case take_state_timer(Name, Data) of
        {ok, TRef, NewData} ->
            cancel_state_timer_tref(TRef, Name),
            NewData;
        not_found ->
            Data
    end.

cancel_state_timer_tref(TRef, Name) ->
    erlang:cancel_timer(TRef),
    receive
        {state_timer, Name} ->
            ok
    after
        0 ->
            ok
    end.

cancel_all_state_timers(#data{state_timers = StateTimers} = Data) ->
    maps:fold(
      fun (Name, TRef, _) ->
              cancel_state_timer_tref(TRef, Name)
      end, unused, StateTimers),
    Data#data{state_timers = #{}}.

state_leader(State) ->
    case State of
        #leader{history_id = HistoryId, term = Term} ->
            {?PEER(), HistoryId, Term};
        #follower{leader = Leader, history_id = HistoryId, term = Term} ->
            case Term of
                tentative ->
                    no_leader;
                _ ->
                    {Leader, HistoryId, Term}
            end;
        _ ->
            no_leader
    end.

publish_leader(LeaderInfo) ->
    ets:insert(?TABLE, {leader, LeaderInfo}),
    do_announce_leader(LeaderInfo).

do_announce_leader(LeaderInfo) ->
    chronicle_events:notify({leader, LeaderInfo}).

get_history_id(#data{metadata = Metadata}) ->
    chronicle_agent:get_history_id(Metadata).

get_quorum(#data{metadata = Metadata}) ->
    chronicle_proposer:get_establish_quorum(Metadata).

get_established_term(#data{metadata = Metadata}) ->
    Metadata#metadata.term.

get_active_leader_term(State) ->
    case state_leader(State) of
        {_Leader, _HistoryId, LeaderTerm} ->
            {ok, LeaderTerm};
        no_leader ->
            no_leader
    end.

get_last_known_leader_term(State, Data) ->
    %% For a short period of time, the leader term that we've received via a
    %% heartbeat may be ahead of the established term.
    case get_active_leader_term(State) of
        {ok, LeaderTerm} ->
            LeaderTerm;
        no_leader ->
            get_established_term(Data)
    end.

get_published_leader() ->
    case ets:lookup(?TABLE, leader) of
        [] ->
            {error, no_leader};
        [{leader, no_leader}] ->
            {error, no_leader};
        [{leader, LeaderInfo}] ->
            {ok, LeaderInfo}
    end.
