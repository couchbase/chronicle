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
-module(chronicle_catchup).

-behavior(gen_server).

-export([start_link/3]).
-export([catchup_peer/4, cancel_catchup/2, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("chronicle.hrl").

-import(chronicle_utils, [sanitize_stacktrace/1]).

-define(MAX_PARALLEL_CATCHUPS,
        chronicle_settings:get({catchup, max_parallel_catchups}, 4)).

-record(state, { parent,
                 peer,
                 history_id,
                 term,
                 pids,
                 pending }).

start_link(Self, HistoryId, Term) ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE,
                          [self(), Self, HistoryId, Term], []).

catchup_peer(Pid, Opaque, Peer, PeerSeqno) ->
    gen_server:cast(Pid, {catchup_peer, Opaque, Peer, PeerSeqno}).

cancel_catchup(Pid, Peer) ->
    gen_server:cast(Pid, {cancel_catchup, Peer}).

stop(Pid) ->
    ok = gen_server:call(Pid, stop, 10000),
    ok = chronicle_utils:wait_for_process(Pid, 1000).

%% callbacks
init([Parent, Self, HistoryId, Term]) ->
    {ok, #state{peer = Self,
                parent = Parent,
                history_id = HistoryId,
                term = Term,
                pids = #{},
                pending = queue:new()}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast({catchup_peer, Opaque, Peer, PeerSeqno}, State) ->
    handle_catchup_peer(Peer, PeerSeqno, Opaque, State);
handle_cast({cancel_catchup, Peer}, State) ->
    handle_cancel_catchup(Peer, State);
handle_cast(Cast, State) ->
    {stop, {unexpected_cast, Cast}, State}.

handle_info({catchup_result, Pid, Result}, State) ->
    handle_catchup_result(Pid, Result, State);
handle_info(Msg, State) ->
    ?WARNING("Unexpected message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, State) ->
    terminate_children(State).

%% internal
handle_catchup_peer(Peer, PeerSeqno, Opaque,
                    #state{peer = Self, pending = Pending} = State) ->
    %% We should never catchup ourselves
    true = (Peer =/= Self),

    NewPending = queue:in({Peer, PeerSeqno, Opaque}, Pending),
    NewState0 = State#state{pending = NewPending},
    {Spawned, NewState} = maybe_spawn_pending(NewState0),

    case Spawned of
        true ->
            ok;
        false ->
            ?INFO("Catchup for peer ~w at seqno ~b "
                  "delayed due to other catchups still in progress.",
                  [Peer, PeerSeqno])
    end,

    {noreply, NewState}.

handle_cancel_catchup(Peer, State) ->
    NewState0 = cancel_pending(Peer, State),
    NewState1 = cancel_active(Peer, NewState0),
    {_, NewState} = maybe_spawn_pending(NewState1),
    {noreply, NewState}.

handle_catchup_result(Pid, Result, #state{pids = Pids} = State) ->
    {{_, Opaque}, NewPids} = maps:take(Pid, Pids),
    reply_to_parent(Opaque, Result, State),
    {_, NewState} = maybe_spawn_pending(State#state{pids = NewPids}),
    {noreply, NewState}.

reply_to_parent(Opaque, Reply, #state{parent = Parent}) ->
    Parent ! {Opaque, Reply},
    ok.

terminate_children(#state{pids = Pids}) ->
    lists:foreach(
      fun (Pid) ->
              terminate_child(Pid)
      end, maps:keys(Pids)).

terminate_child(Pid) ->
    chronicle_utils:terminate_linked_process(Pid, kill),
    ?FLUSH({catchup_result, Pid, _}).

maybe_spawn_pending(State) ->
    maybe_spawn_pending(false, State).

maybe_spawn_pending(Spawned, #state{pids = Pids,
                                    pending = Pending} = State) ->
    case maps:size(Pids) < ?MAX_PARALLEL_CATCHUPS of
        true ->
            case queue:out(Pending) of
                {empty, _} ->
                    {Spawned, State};
                {{value, {Peer, PeerSeqno, Opaque}}, NewPending} ->
                    NewState = State#state{pending = NewPending},
                    maybe_spawn_pending(
                      true,
                      spawn_catchup(Peer, PeerSeqno, Opaque, NewState))
            end;
        false ->
            {Spawned, State}
    end.

spawn_catchup(Peer, PeerSeqno, Opaque, #state{pids = Pids} = State) ->
    ?DEBUG("Starting catchup for peer ~w at seqno ~b", [Peer, PeerSeqno]),

    true = (maps:size(Pids) < ?MAX_PARALLEL_CATCHUPS),
    Parent = self(),
    Pid = proc_lib:spawn_link(
            fun () ->
                    Result =
                        try do_catchup(Peer, PeerSeqno, State) of
                            R ->
                                R
                        catch
                            T:E:Stacktrace ->
                                ?ERROR("Catchup to peer ~p failed: ~p~n"
                                       "Stacktrace:~n~p",
                                       [Peer, {T, E},
                                        sanitize_stacktrace(Stacktrace)]),
                                {error, {catchup_failed, {T, E}}}
                        end,

                    Parent ! {catchup_result, self(), Result}
            end),
    State#state{pids = Pids#{Pid => {Peer, Opaque}}}.

do_catchup(Peer, PeerSeqno, State) ->
    case get_full_snapshot(PeerSeqno) of
        no_snapshot ->
            case chronicle_agent:get_term_for_seqno(PeerSeqno) of
                {ok, AtTerm} ->
                    send_entries(Peer, AtTerm, PeerSeqno, State);
                {error, compacted} ->
                    %% Have the proposer retry.
                    {compacted, PeerSeqno}
            end;
        Snapshot ->
            case install_snapshot(Peer, Snapshot, State) of
                {ok, _} ->
                    {SnapshotSeqno, _, SnapshotTerm, _, _} = Snapshot,
                    send_entries(Peer, SnapshotTerm, SnapshotSeqno, State);
                {error, _} = Error ->
                    Error
            end
    end.

get_full_snapshot(PeerSeqno) ->
    case chronicle_agent:get_full_snapshot(PeerSeqno) of
        {ok, Seqno, HistoryId, Term, Config, RSMSnapshots} ->
            {Seqno, HistoryId, Term, Config, RSMSnapshots};
        {error, no_snapshot} ->
            no_snapshot
    end.

install_snapshot(Peer,
                 {SnapshotSeqno,
                  SnapshotHistoryId, SnapshotTerm,
                  SnapshotConfig, RSMSnapshots},
                 #state{peer = Self, history_id = HistoryId, term = Term}) ->
    ServerRef = chronicle_agent:server_ref(Peer, Self),
    chronicle_agent:install_snapshot(ServerRef, HistoryId, Term,
                                     SnapshotSeqno,
                                     SnapshotHistoryId, SnapshotTerm,
                                     SnapshotConfig, RSMSnapshots).

send_entries(Peer, AtTerm, AtSeqno, State) ->
    case chronicle_agent:get_log_committed(AtSeqno + 1) of
        {ok, _, []} ->
            {ok, AtSeqno};
        {ok, CommittedSeqno, Entries} ->
            append(Peer, CommittedSeqno, AtTerm, AtSeqno, Entries, State);
        {error, compacted} ->
            %% Have the proposer retry.
            {compacted, AtSeqno}
    end.

append(Peer, CommittedSeqno, AtTerm, AtSeqno, Entries,
       #state{peer = Self, history_id = HistoryId, term = Term}) ->
    ServerRef = chronicle_agent:server_ref(Peer, Self),
    case chronicle_agent:append(ServerRef, HistoryId, Term,
                                CommittedSeqno, AtTerm, AtSeqno, Entries) of
        ok ->
            {ok, CommittedSeqno};
        {error, _} = Error ->
            Error
    end.

cancel_active(Peer, #state{pids = Pids} = State) ->
    NewPids =
        maps:filter(
          fun (Pid, {OtherPeer, _Opaque}) ->
                  case Peer =:= OtherPeer of
                      true ->
                          terminate_child(Pid),
                          false;
                      false ->
                          true
                  end
          end, Pids),

    State#state{pids = NewPids}.

cancel_pending(Peer, #state{pending = Pending} = State) ->
    NewPending =
        queue:filter(
          fun ({OtherPeer, _, _}) ->
                  OtherPeer =/= Peer
          end, Pending),
    State#state{pending = NewPending}.
