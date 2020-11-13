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

-import(chronicle_utils, [compare_positions/2,
                          get_establish_peers/1,
                          get_establish_quorum/1,
                          get_position/1,
                          get_quorum_peers/1,
                          have_quorum/2,
                          parallel_mapfold/4,
                          read_timeout/1,
                          term_number/1]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).

-define(TABLE, ?ETS_TABLE(?MODULE)).
-define(MAX_BACKOFF, 16).

-record(leader, { peer, history_id, term, status }).
-record(follower, { leader, history_id, term, status }).
-record(observer, { electable }).
-record(voted_for, { peer, ts }).
-record(candidate, {}).

-record(data, { %% Since heartbeats are sent frequently, keep a precomputed
                %% list of our peers.
                peers = [],

                history_id = ?NO_HISTORY,
                established_term = ?NO_TERM,

                electable = false,

                %% used only when the state is #candidate{}
                election_worker,

                %% used to track timers that get auto-canceled when the state
                %% changes
                state_timers = #{},

                leader_waiters = #{},

                backoff_factor = 1}).

start_link() ->
    gen_statem:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

get_leader() ->
    leader_info_to_leader(get_leader_info()).

get_leader_info() ->
    case chronicle_ets:get(leader_info) of
        not_found ->
            no_leader;
        {ok, LeaderInfo} ->
            LeaderInfo
    end.

wait_for_leader() ->
    wait_for_leader(5000).

wait_for_leader(Timeout) ->
    wait_for_leader(any, Timeout).

wait_for_leader(Incarnation, Timeout) ->
    case wait_for_leader_fast_path(Incarnation) of
        {_Leader, _LeaderIncarnation} = Result ->
            Result;
        no_leader ->
            wait_for_leader_slow_path(Incarnation, Timeout)
    end.

wait_for_leader_fast_path(Incarnation) ->
    check_leader_incarnation(Incarnation, get_leader()).

wait_for_leader_slow_path(Incarnation, Timeout) ->
    Result = gen_statem:call(
               ?SERVER,
               {wait_for_leader, Incarnation, read_timeout(Timeout)},
               infinity),
    case Result of
        {_Leader, LeaderIncarnation} ->
            true = (LeaderIncarnation =/= Incarnation),
            Result;
        no_leader ->
            exit(no_leader)
    end.

announce_leader_status() ->
    gen_statem:cast(?SERVER, announce_leader_status).

request_vote(Peer, Candidate, HistoryId, Position) ->
    gen_statem:call(?SERVER(Peer),
                    {request_vote, Candidate, HistoryId, Position}, infinity).

note_term_finished(HistoryId, Term) ->
    gen_statem:cast(?SERVER, {note_term_status, HistoryId, Term, finished}).

note_term_established(HistoryId, Term) ->
    gen_statem:cast(?SERVER, {note_term_status, HistoryId, Term, established}).

sync() ->
    gen_statem:call(?SERVER, sync, 10000).

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

    ok = chronicle_ets:register_writer([leader_info]),

    Data =
        case chronicle_agent:get_metadata() of
            {ok, Metadata} ->
                metadata2data(Metadata);
            {error, not_provisioned} ->
                #data{}
        end,

    {ok, make_observer(Data), Data}.

handle_event(enter, OldState, State, Data) ->
    handle_leader_transition(OldState, State, Data),
    NewData0 = maybe_publish_leader(OldState, State, Data),
    NewData1 = handle_state_leave(OldState, NewData0),
    handle_state_enter(State, NewData1);
handle_event(info, {chronicle_event, Event}, State, Data) ->
    handle_chronicle_event(Event, State, Data);
handle_event(info, {heartbeat, LeaderInfo}, State, Data) ->
    handle_heartbeat(LeaderInfo, State, Data);
handle_event(info, {stepping_down, LeaderInfo}, State, Data) ->
    handle_stepping_down(LeaderInfo, State, Data);
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
handle_event(cast, announce_leader_status, State, Data) ->
    handle_announce_leader_status(State, Data);
handle_event(cast, {note_term_status, HistoryId, Term, Status}, State, Data) ->
    handle_note_term_status(HistoryId, Term, Status, State, Data);
handle_event({call, From},
             {request_vote, Candidate, HistoryId, Position}, State, Data) ->
    handle_request_vote(Candidate, HistoryId, Position, From, State, Data);
handle_event({call, From},
             {wait_for_leader, Incarnation, Timeout}, State, Data) ->
    handle_wait_for_leader(Incarnation, Timeout, From, State, Data);
handle_event({call, From}, sync, _State, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
handle_event({call, From}, _Call, _State, _Data) ->
    {keep_state_and_data, [{reply, From, nack}]};
handle_event(Type, Event, _State, _Data) ->
    ?WARNING("Unexpected event ~p", [{Type, Event}]),
    keep_state_and_data.

terminate(_Reason, State, Data) ->
    _ = handle_state_leave(State, Data),

    case State of
        #leader{} ->
            on_leader_stepping_down(State, Data);
        _ ->
            ok
    end,

    _ = reply_to_leader_waiters(no_leader, Data),
    publish_leader(no_leader),
    announce_leader_status(not_leader).

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

handle_leader_transition(OldState, NewState, Data) ->
    WasLeader = is_record(OldState, leader),
    IsLeader = is_record(NewState, leader),

    case {WasLeader, IsLeader} of
        {true, false} ->
            on_leader_stepping_down(OldState, Data);
        {false, true} ->
            on_leader_starting(NewState, Data);
        _ ->
            ok
    end.

on_leader_starting(State, _Data) ->
    announce_leader_status(state_leader_info(State)).

on_leader_stepping_down(OldState, Data) ->
    announce_leader_status(not_leader),
    send_stepping_down(OldState, Data).

send_stepping_down(#leader{} = OldState, Data) ->
    LeaderInfo = state_leader_info(OldState),
    send_msg_to_peers({stepping_down, LeaderInfo}, Data).

handle_state_enter(State, Data) ->
    NewData0 = start_state_timers(State, Data),
    NewData1 = maybe_reset_backoff(State, NewData0),

    case State of
        #candidate{} ->
            {keep_state, start_election_worker(NewData1)};
        _ ->
            {keep_state, NewData1}
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
state_timers(#observer{electable = false}) ->
    [];
state_timers(_) ->
    [election].

start_election_timer(State, Data) ->
    start_state_timer(election, get_election_timeout(State, Data), Data).

get_election_timeout(State, Data) ->
    HeartbeatInterval = get_heartbeat_interval(),

    case State of
        #observer{} ->
            %% This is the timeout that needs to expire before an observer
            %% will decide to attempt to elect itself a leader. The timeout is
            %% randomized to avoid clashes with other nodes.
            BackoffFactor = Data#data.backoff_factor,
            HeartbeatInterval +
                rand:uniform(5 * BackoffFactor * HeartbeatInterval);
        #candidate{} ->
            %% This is used by the candidate when it starts election. This
            %% value is larger than 'long' timeout, which means that
            %% eventually other nodes will start trying to elect
            %% themselves. But this is probably ok.
            50 * HeartbeatInterval;
        _ ->
            %% This is the amount of time that it will take followers or nodes
            %% that graned their vote to decide that the leader is missing and
            %% move to the observer state.
            20 * HeartbeatInterval
    end.

schedule_send_heartbeat(Data) ->
    schedule_send_heartbeat(get_heartbeat_interval(), Data).

schedule_send_heartbeat(Timeout, Data) ->
    start_state_timer(send_heartbeat, Timeout, Data).

get_heartbeat_interval() ->
    %% TODO
    100.

is_interesting_event({system_state, _, _}) ->
    true;
is_interesting_event({system_event, _, _}) ->
    true;
is_interesting_event({new_history, _, _}) ->
    true;
is_interesting_event({term_established, _}) ->
    true;
is_interesting_event({new_config, _, _}) ->
    true;
is_interesting_event(_) ->
    false.

handle_chronicle_event({system_state, unprovisioned, _}, State, Data) ->
    handle_unprovisioned(State, Data);
handle_chronicle_event({system_state, provisioned, Metadata}, State, Data) ->
    handle_provisioned(Metadata, State, Data);
handle_chronicle_event({system_event, reprovisioned, Metadata}, State, Data) ->
    handle_reprovisioned(Metadata, State, Data);
handle_chronicle_event({new_config, Config, Metadata}, State, Data) ->
    handle_new_config(Config, Metadata, State, Data);
handle_chronicle_event({new_history, HistoryId, Metadata}, State, Data) ->
    handle_new_history(HistoryId, Metadata, State, Data);
handle_chronicle_event({term_established, Term}, State, Data) ->
    handle_new_term(Term, State, Data).

handle_unprovisioned(_State, Data) ->
    ?INFO("System became unprovisioned."),
    NewData = Data#data{peers = [],
                        history_id = ?NO_HISTORY,
                        established_term = ?NO_TERM,
                        electable = false},

    {next_state, make_observer(NewData), NewData}.

handle_provisioned(Metadata, State, Data) ->
    ?INFO("System became provisioned."),
    NewData = metadata2data(Metadata, Data),
    NewState =
        case State of
            #observer{electable = false} ->
                make_observer(NewData);
            _ ->
                State
        end,
    {next_state, NewState, NewData}.

handle_reprovisioned(Metadata, _State, Data) ->
    ?INFO("System reprovisioned."),
    NewData = metadata2data(Metadata, Data),

    %% This ultimately terminates the current term and starts a new one. We're
    %% transitioning straight to the candidate state to avoid extra election
    %% timeout that would have to expire if we moved to the observer state as
    %% is down elsewhere. Moving straight to the candidate state should be
    %% fine since reprovisioning can happen only when we are the only node in
    %% the cluster.
    {next_state, #candidate{}, NewData}.

handle_new_config(_Config, Metadata, State, Data) ->
    NewData = metadata2data(Metadata, Data),

    case Data#data.electable =:= NewData#data.electable of
        true ->
            {keep_state, NewData};
        false ->
            case State of
                #leader{} ->
                    %% When we are a leader, we may end up changing our node's
                    %% status making it unelectable. But chronicle_proposer
                    %% will step down in such situation on its own.
                    {keep_state, NewData};
                _ ->
                    ?INFO("Our electability (the new value is ~p) changed. "
                          "Becoming an observer.",
                          [NewData#data.electable]),
                    {next_state, make_observer(NewData), NewData}
            end
    end.

handle_new_history(HistoryId, Metadata, _State, Data) ->
    ?INFO("History changed to ~p. Becoming an observer.", [HistoryId]),
    NewData = metadata2data(Metadata, Data),
    {next_state, make_observer(NewData), NewData}.

handle_new_term(Term, State, Data) ->
    NewData = Data#data{established_term = Term},
    LeaderAndTerm = get_active_leader_and_term(State),
    Invalidate =
        case LeaderAndTerm of
            {_Leader, LeaderTerm} ->
                LeaderTerm =/= Term;
            no_leader ->
                true
        end,

    case Invalidate of
        true ->
            %% Some node established a new term when we either don't know who
            %% the leader is or our leader's term is different from the newly
            %% established one. Reset the state to prevent election timeout
            %% from expiring and interfering with that node. Hopefully we'll
            %% receive a heartbeat from it soon.
            ?INFO("Becoming an observer due to new term being established.~n"
                  "Established term: ~p~n"
                  "Our leader and term: ~p",
                  [Term, LeaderAndTerm]),
            {next_state, make_observer(NewData), NewData};
        false ->
            {keep_state, NewData}
    end.

metadata2data(Metadata) ->
    metadata2data(Metadata, #data{}).

metadata2data(Metadata, Data) ->
    Self = Metadata#metadata.peer,
    Peers = get_establish_peers(Metadata),
    Electable = lists:member(Self, Peers),

    Data#data{history_id = chronicle_agent:get_history_id(Metadata),
              established_term = Metadata#metadata.term,
              peers = Peers -- [Self],
              electable = Electable}.

handle_note_term_status(HistoryId, Term, Status, State, Data) ->
    case check_is_leader(HistoryId, Term, State) of
        ok ->
            case Status of
                finished ->
                    ?INFO("Term ~p has finished. Stepping down.", [Term]),
                    {next_state, make_observer(Data), Data};
                established ->
                    ?INFO("Term ~p established.", [Term]),
                    tentative = State#leader.status,
                    NewState = State#leader{status = Status},
                    {next_state, NewState, Data}
            end;
        {error, _} = Error ->
            ?DEBUG("Ignoring stale term status ~p: ~p",
                   [{HistoryId, Term, Status}, Error]),
            keep_state_and_data
    end.

handle_election_timeout(State, Data) ->
    ?DEBUG("Election timeout when state is: ~p", [State]),

    NewState =
        case State of
            #observer{} ->
                #candidate{};
            _ ->
                make_observer(Data)
        end,

    {next_state, NewState, Data}.

handle_heartbeat(LeaderInfo, State, Data) ->
    #{leader := Peer,
      history_id := HistoryId,
      term := Term,
      status := Status} = LeaderInfo,

    case ?CHECK(check_history_id(HistoryId, Data),
                check_accept_heartbeat(Term, Status, State, Data)) of
        ok ->
            NewState = #follower{leader = Peer,
                                 history_id = HistoryId,
                                 term = Term,
                                 status = Status},

            {next_state, NewState,
             %% We've received a heartbeat, so start the election timer anew.
             start_election_timer(NewState, Data)};
        Error ->
            %% TODO: this may be too much to log
            ?DEBUG("Rejecting heartbeat ~p: ~p",
                   [{Peer, HistoryId, Term}, Error]),
            keep_state_and_data
    end.

handle_stepping_down(LeaderInfo, State, Data) ->
    #{leader := Peer} = LeaderInfo,

    case State of
        #follower{leader = OurLeader}
          when Peer =:= OurLeader ->
            %% We don't check the history and term numbers. That's because
            %% heartbeats and stepping_down messages originate on the same
            %% node and there shouldn't be any reordering happening in
            %% transition.
            ?INFO("Leader ~p told us it's stepping down.~n"
                  "Full leader info: ~p",
                  [Peer, LeaderInfo]),
            {next_state, make_observer(Data), Data};
        _ ->
            ?INFO("Ignoring stepping_down message.~n"
                  "State: ~p~n"
                  "Leader info: ~p",
                  [State, LeaderInfo]),
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
        {ok, Peer, HistoryId, Term} ->
            NewTerm = chronicle_utils:next_term(Term, Peer),
            ?INFO("Going to become a leader in term ~p (history id ~p)",
                  [NewTerm, HistoryId]),
            NewState = #leader{peer = Peer,
                               history_id = HistoryId,
                               term = NewTerm,
                               status = tentative},
            {next_state, NewState, NewData};
        {error, _} = Error ->
            ?INFO("Election failed: ~p", [Error]),
            {next_state, make_observer(NewData), backoff(NewData)}
    end.

handle_request_vote(Candidate, HistoryId, Position, From, State, Data) ->
    case check_grant_vote(HistoryId, Position, State) of
        {ok, LatestTerm} ->
            {next_state,
             #voted_for{peer = Candidate,
                        ts = erlang:system_time()}, Data,
             {reply, From, {ok, LatestTerm}}};
        {error, _} = Error ->
            {keep_state_and_data, {reply, From, Error}}
    end.

check_consider_granting_vote(State) ->
    case State of
        #observer{} ->
            ok;
        #voted_for{} ->
            ok;
        #candidate{} ->
            {error, in_election};
        _ ->
            {error, {have_leader, state_leader_info(State)}}
    end.

check_grant_vote(HistoryId, PeerPosition, State) ->
    case check_consider_granting_vote(State) of
        ok ->
            case chronicle_agent:get_metadata() of
                {ok, Metadata} ->
                    case ?CHECK(check_history_id(HistoryId, Metadata),
                                check_peer_position(PeerPosition, Metadata)) of
                        ok ->
                            {ok, Metadata#metadata.term};
                        {error, _} = Error ->
                            Error
                    end;
                {error, not_provisioned} ->
                    %% When the node is unprovisioned, it'll vote for any
                    %% leader. This is important when the node is in the
                    %% process of being added to the cluster. If no votes are
                    %% granted while the node is unprovisioned, it's possible
                    %% to wound up in the state where no leader can be
                    %% elected.
                    %%
                    %% Consider the following case.
                    %%
                    %% 1. Node A is the only node in a cluster. So node A is a
                    %% leader.
                    %% 2. Node B is being added to the cluster. Node B is
                    %% unprovisioned.
                    %% 3. Node A writes a transitional configuration
                    %% locally. From this point on, in order for a new leader
                    %% to be elected, it needs to get votes from both A and B.
                    %% 4. So if node A restarts and needs to reelect itself a
                    %% leader, it'll fail to do so, because B won't grant it a
                    %% vote.
                    {ok, ?NO_TERM}
            end;
        {error, _} = Error ->
            Error
    end.

check_peer_position(PeerPosition, Metadata) ->
    OurPosition = get_position(Metadata),
    case compare_positions(PeerPosition, OurPosition) of
        lt ->
            {error, {behind, OurPosition}};
        _ ->
            ok
    end.

handle_wait_for_leader(Incarnation, Timeout, From, State, Data) ->
    case check_leader_incarnation(Incarnation, state_leader(State)) of
        {_Leader, _LeaderIncarnation} = Reply ->
            {keep_state_and_data, {reply, From, Reply}};
        no_leader ->
            NewData = add_leader_waiter(Timeout, From, Data),
            {keep_state, NewData}
    end.

handle_leader_wait_timeout(TRef, State,
                           #data{leader_waiters = Waiters} = Data) ->
    no_leader = state_leader(State),
    {From, NewWaiters} = maps:take(TRef, Waiters),
    gen_statem:reply(From, no_leader),
    {keep_state, Data#data{leader_waiters = NewWaiters}}.

add_leader_waiter(Timeout, From, #data{leader_waiters = Waiters} = Data) ->
    TRef = erlang:start_timer(Timeout, self(), leader_wait),
    NewWaiters = Waiters#{TRef => From},
    Data#data{leader_waiters = NewWaiters}.

maybe_reply_to_leader_waiters(LeaderInfo, Data) ->
    case leader_info_to_leader(LeaderInfo) of
        no_leader ->
            Data;
        {_Leader, _LeaderIncarnation} = Reply ->
            reply_to_leader_waiters(Reply, Data)
    end.

reply_to_leader_waiters(Reply, #data{leader_waiters = Waiters} = Data) ->
    chronicle_utils:maps_foreach(
      fun (TRef, From) ->
              gen_statem:reply(From, Reply),
              _ = erlang:cancel_timer(TRef),
              ?FLUSH({timeout, TRef, _})
      end, Waiters),

    Data#data{leader_waiters = #{}}.

start_election_worker(Data) ->
    Pid = proc_lib:spawn_link(fun election_worker/0),
    Data#data{election_worker = Pid}.

-spec election_worker() -> no_return().
election_worker() ->
    Result = do_election_worker(),
    exit({shutdown, {election_result, Result}}).

do_election_worker() ->
    {ok, Metadata} = chronicle_agent:get_metadata(),

    LatestTerm = Metadata#metadata.term,
    HistoryId = chronicle_agent:get_history_id(Metadata),
    Position = get_position(Metadata),
    Quorum = get_establish_quorum(Metadata),
    Peers = get_quorum_peers(Quorum),

    ?INFO("Starting election.~n"
          "History ID: ~p~n"
          "Log position: ~p~n"
          "Peers: ~p~n"
          "Required quorum: ~p",
          [HistoryId, Position, Peers, Quorum]),

    Leader = Metadata#metadata.peer,
    OtherPeers = Peers -- [Leader],

    case lists:member(Leader, Peers) of
        true ->
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

                                case have_quorum(NewVotes, Quorum) of
                                    true ->
                                        %% TODO: wait a little more to receive
                                        %% more responses
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

            case OtherPeers =:= [] of
                true ->
                    ?INFO("I'm the only peer, so I'm the leader."),
                    {ok, Leader, HistoryId, LatestTerm};
                false ->
                    case parallel_mapfold(CallFun, HandleResponse,
                                          {no_quorum, [Leader], LatestTerm},
                                          OtherPeers) of
                        {no_quorum, FinalVotes, _} ->
                            {error, {no_quorum, FinalVotes, Quorum}};
                        {ok, FinalTerm} ->
                            {ok, Leader, HistoryId, FinalTerm}
                    end
            end;
        false ->
            {error, {not_voter, Leader, Peers}}
    end.

handle_send_heartbeat(State, Data) ->
    send_heartbeat(State, Data),
    {keep_state, schedule_send_heartbeat(Data)}.

send_heartbeat(#leader{} = State, Data) ->
    LeaderInfo = state_leader_info(State),
    Heartbeat = {heartbeat, LeaderInfo},
    send_msg_to_peers(Heartbeat, Data).

send_msg_to_peers(Msg, #data{peers = Peers}) ->
    lists:foreach(
      fun (Peer) ->
              send_msg(Peer, Msg)
      end, Peers).

send_msg(Peer, Msg) ->
    ?SEND(?SERVER(Peer), Msg, [nosuspend, noconnect]).

handle_announce_leader_status(State, _Data) ->
    Status =
        case State of
            #leader{} ->
                state_leader_info(State);
            _ ->
                not_leader
        end,
    announce_leader_status(Status),
    keep_state_and_data.

maybe_publish_leader(OldState, State, Data) ->
    OldLeaderInfo = state_leader_info(OldState),
    NewLeaderInfo = state_leader_info(State),

    case OldLeaderInfo =:= NewLeaderInfo of
        true ->
            Data;
        false ->
            publish_leader(NewLeaderInfo),
            maybe_reply_to_leader_waiters(NewLeaderInfo, Data)
    end.

check_history_id(HistoryId, #data{history_id = OurHistoryId}) ->
    do_check_history_id(HistoryId, OurHistoryId);
check_history_id(HistoryId, #metadata{} = Metadata) ->
    do_check_history_id(HistoryId, chronicle_agent:get_history_id(Metadata)).

do_check_history_id(TheirHistoryId, OurHistoryId) ->
    case OurHistoryId =:= ?NO_HISTORY orelse TheirHistoryId =:= OurHistoryId of
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

check_accept_heartbeat(NewTerm, NewStatus, State, Data) ->
    {OurTerm, OurStatus} = get_last_known_leader_term(State, Data),

    case NewTerm =:= OurTerm of
        true ->
            %% This should be the most common case, so accept the hearbeat
            %% quickly.
            ok;
        false ->
            NewTermNumber = term_number(NewTerm),
            OurTermNumber = term_number(OurTerm),

            if
                NewTermNumber > OurTermNumber ->
                    ok;
                NewTermNumber =:= OurTermNumber ->
                    %% Two nodes are competing to become a leader in the same
                    %% term.
                    case {NewStatus, OurStatus} of
                        {established, _} ->
                            %% The node we got the heartbeat from successfully
                            %% established the term on a quorum of nodes. So
                            %% we accept it. Our term status then must not be
                            %% established.
                            true = (OurStatus =/= established),
                            ok;
                        {tentative, inactive} ->
                            %% Accept a tentative heartbeat only if we haven't
                            %% heard from any other leader before.
                            ok;
                        _ ->
                            {error, {have_leader,
                                     NewTerm, NewStatus,
                                     OurTerm, OurStatus}}
                    end;
                true ->
                    {error, {stale_term, NewTerm, OurTerm}}
            end
    end.

get_last_known_leader_term(State, Data) ->
    %% For a short period of time, the leader term that we've received via a
    %% heartbeat may be ahead of the established term.
    case state_leader_info(State) of
        #{term := Term, status := Status} ->
            {Term, Status};
        no_leader ->
            {Data#data.established_term, inactive}
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
    _ = erlang:cancel_timer(TRef),
    receive
        {state_timer, Name} ->
            ok
    after
        0 ->
            ok
    end.

cancel_all_state_timers(#data{state_timers = StateTimers} = Data) ->
    chronicle_utils:maps_foreach(
      fun (Name, TRef) ->
              cancel_state_timer_tref(TRef, Name)
      end, StateTimers),
    Data#data{state_timers = #{}}.

state_leader(State) ->
    leader_info_to_leader(state_leader_info(State)).

state_leader_info(State) ->
    case State of
        #leader{peer = Peer,
                history_id = HistoryId, term = Term, status = Status} ->
            make_leader_info(Peer, HistoryId, Term, Status);
        #follower{leader = Leader,
                  history_id = HistoryId,
                  term = Term,
                  status = Status} ->
            make_leader_info(Leader, HistoryId, Term, Status);
        _ ->
            no_leader
    end.

leader_info_to_leader(no_leader) ->
    no_leader;
leader_info_to_leader(#{leader := Leader,
                        history_id := HistoryId,
                        term := Term,
                        status := Status}) ->
    case Status of
        established ->
            %% Expose only established leaders to clients
            {Leader, {HistoryId, Term}};
        _ ->
            no_leader
    end.

check_leader_incarnation(_, no_leader) ->
    no_leader;
check_leader_incarnation(any, {_Leader, _LeaderIncarnation} = Result) ->
    Result;
check_leader_incarnation(Incarnation, {_Leader, LeaderIncarnation} = Result) ->
    case Incarnation =/= LeaderIncarnation of
        true ->
            Result;
        false ->
            no_leader
    end.

make_leader_info(Leader, HistoryId, Term, Status) ->
    #{leader => Leader,
      history_id => HistoryId,
      term => Term,
      status => Status}.

publish_leader(LeaderInfo) ->
    chronicle_ets:put(leader_info, LeaderInfo).

announce_leader_status(Status) ->
    chronicle_events:sync_notify({leader_status, Status}).

get_active_leader_and_term(State) ->
    case state_leader_info(State) of
        #{leader := Leader, term := LeaderTerm} ->
            true = (LeaderTerm =/= undefined),
            {Leader, LeaderTerm};
        _ ->
            no_leader
    end.

make_observer(#data{electable = Electable}) ->
    #observer{electable = Electable}.

backoff(#data{backoff_factor = Factor} = Data) ->
    case Factor >= ?MAX_BACKOFF of
        true ->
            Data;
        false ->
            Data#data{backoff_factor = Factor * 2}
    end.

reset_backoff(Data) ->
    Data#data{backoff_factor = 1}.

maybe_reset_backoff(State, Data) ->
    Reset =
        case State of
            #voted_for{} ->
                true;
            #follower{} ->
                true;
            #leader{} ->
                true;
            _ ->
                false
        end,

    case Reset of
        true ->
            reset_backoff(Data);
        false ->
            Data
    end.
