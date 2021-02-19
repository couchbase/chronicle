%% @author Couchbase <info@couchbase.com>
%% @copyright 2021 Couchbase, Inc.
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
-module(chronicle_snapshot_mgr).

-include("chronicle.hrl").

-export([start_link/0]).
-export([get_latest_snapshot/0, release_snapshot/1, store_snapshot/1]).
-export([pending_snapshot/2, cancel_pending_snapshot/1, save_snapshot/3]).
-export([need_snapshot/1, wipe/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-import(chronicle_utils, [sanitize_stacktrace/1]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(TIMEOUT, chronicle_settings:get({snapshot_mgr, timeout}, 15000)).

-record(pending_snapshot, { seqno,
                            savers,
                            remaining_rsms }).

-record(state, { snapshot,

                 monitors,
                 snapshot_readers,

                 pending_snapshot
               }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

get_latest_snapshot() ->
    gen_server:call(?SERVER, {get_latest_snapshot, self()}, ?TIMEOUT).

release_snapshot(Ref) ->
    gen_server:cast(?SERVER, {release_snapshot, Ref}).

store_snapshot(Snapshot) ->
    gen_server:cast(?SERVER, {store_snapshot, Snapshot}).

pending_snapshot(Seqno, RSMs) ->
    gen_server:cast(?SERVER, {pending_snapshot, Seqno, RSMs}).

cancel_pending_snapshot(Seqno) ->
    case gen_server:call(?SERVER, {cancel_pending_snapshot, Seqno}, ?TIMEOUT) of
        ok ->
            ok;
        {error, Error} ->
            exit(Error)
    end.

save_snapshot(Name, Seqno, Snapshot) ->
    Pid = self(),
    case gen_server:call(?SERVER,
                         {get_snapshot_saver, Name, Pid, Seqno},
                         ?TIMEOUT) of
        {ok, SaverPid} ->
            SaverPid ! {snapshot, Snapshot};
        {error, rejected} ->
            ?INFO("Snapshot for RSM ~p at "
                  "sequence number ~p got rejected.", [Name, Seqno])
    end.

need_snapshot(Name) ->
    gen_server:call(?SERVER, {need_snapshot, Name}, ?TIMEOUT).

wipe() ->
    ok = gen_server:call(?SERVER, wipe, ?TIMEOUT).

%% callbacks
init([]) ->
    {ok, #state{snapshot = undefined,
                monitors = #{},
                snapshot_readers = #{}}}.

handle_call({get_latest_snapshot, Pid}, _From, State) ->
    handle_get_latest_snapshot(Pid, State);
handle_call({get_snapshot_saver, RSM, RSMPid, Seqno}, _From, State) ->
    handle_get_snapshot_saver(RSM, RSMPid, Seqno, State);
handle_call({cancel_pending_snapshot, Seqno}, _From, State) ->
    handle_cancel_pending_snapshot(Seqno, State);
handle_call({need_snapshot, RSM}, _From, State) ->
    {reply, need_snapshot(RSM, State), State};
handle_call(wipe, _From, State) ->
    handle_wipe(State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast({store_snapshot, Snapshot}, State) ->
    handle_store_snapshot(Snapshot, State);
handle_cast({release_snapshot, Ref}, State) ->
    handle_release_snapshot(Ref, State);
handle_cast({pending_snapshot, Seqno, RSMs}, State) ->
    handle_pending_snapshot(Seqno, RSMs, State);
handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast: ~w", [Cast]),
    {noreply, State}.

handle_info({snapshot_result, RSM, Pid, Result}, State) ->
    handle_snapshot_result(RSM, Pid, Result, State);
handle_info({'DOWN', MRef, process, _Pid, _Reason}, State) ->
    handle_down(MRef, State);
handle_info(Msg, State) ->
    ?WARNING("Unexpected message: ~w", [Msg]),
    {noreply, State}.

%% internal
handle_get_latest_snapshot(Pid, #state{snapshot = Snapshot} = State) ->
    case Snapshot of
        undefined ->
            {reply, {error, no_snapshot}, State};
        {Seqno, HistoryId, Term, Config} ->
            MRef = erlang:monitor(process, Pid),
            Reply = {ok, MRef, Seqno, HistoryId, Term, Config},

            {reply, Reply, add_reader(MRef, Seqno, State)}
    end.

handle_wipe(#state{monitors = Monitors,
                   pending_snapshot = Snapshot} = State) ->
    %% chronicle_agent should cancel snapshots before calling wipe()
    undefined = Snapshot,

    %% All readers should be stopped by now, but it's possible that DOWN
    %% messages haven't gotten delivered yet.
    lists:foreach(
      fun (MRef) ->
              erlang:demonitor(MRef, [flush])
      end, maps:keys(Monitors)),

    {reply, ok, State#state{snapshot = undefined,
                            monitors = #{},
                            snapshot_readers = #{}}}.

handle_store_snapshot(Snapshot, #state{snapshot = OldSnapshot} = State) ->
    {_Seqno, _HistoryId, _Term, _Config} = Snapshot,
    true = (Snapshot =/= OldSnapshot),

    NewState = State#state{snapshot = Snapshot},
    case OldSnapshot of
        undefined ->
            ok;
        {OldSeqno, _, _, _} ->
            maybe_release_snapshot(OldSeqno, NewState)
    end,

    {noreply, NewState}.

handle_release_snapshot(MRef, State) ->
    erlang:demonitor(MRef, [flush]),
    {noreply, remove_reader(MRef, State)}.

handle_pending_snapshot(Seqno, RSMs, State) ->
    undefined = State#state.pending_snapshot,

    Snapshot = #pending_snapshot{
                  seqno = Seqno,
                  savers = #{},
                  remaining_rsms = sets:from_list(RSMs)},

    {noreply, State#state{pending_snapshot = Snapshot}}.

handle_cancel_pending_snapshot(Seqno,
                               #state{pending_snapshot = Snapshot} = State) ->
    case Snapshot of
        undefined ->
            {reply, ok, State};
        #pending_snapshot{seqno = SnapshotSeqno, savers = Savers}
          when SnapshotSeqno =:= Seqno ->
            ?DEBUG("Canceling snapshot at seqno ~p", [Seqno]),
            cancel_savers(Savers),

            {reply, ok, State#state{pending_snapshot = undefined}};
        #pending_snapshot{seqno = SnapshotSeqno} ->
            {reply, {error, {bad_snapshot, Seqno, SnapshotSeqno}}, State}
    end.

handle_get_snapshot_saver(RSM, RSMPid, Seqno, State) ->
    case need_snapshot(RSM, State) of
        {true, NeededSeqno} ->
            %% It's a bug if an RSM comes to us with a snapshot we're not yet
            %% aware of
            true = (NeededSeqno >= Seqno),

            case NeededSeqno =:= Seqno of
                true ->
                    Snapshot = State#state.pending_snapshot,
                    {Pid, NewSnapshot} =
                        spawn_snapshot_saver(RSM, RSMPid, Snapshot),
                    {reply, {ok, Pid},
                     State#state{pending_snapshot = NewSnapshot}};
                false ->
                    {reply, {error, rejected}, State}
            end;
        false ->
            {reply, {error, rejected}, State}
    end.

spawn_snapshot_saver(RSM, RSMPid,
                     #pending_snapshot{seqno = Seqno,
                                       savers = Savers,
                                       remaining_rsms = RSMs} = Snapshot) ->
    Parent = self(),
    Pid = proc_lib:spawn_link(
            fun () ->
                    Result =
                        try snapshot_saver(RSM, RSMPid, Seqno) of
                            R ->
                                R
                        catch
                            T:E:Stacktrace ->
                                ?ERROR("Exception while taking "
                                       "snapshot for RSM ~p~p at seqno ~p: ~p~n"
                                       "Stacktrace:~n~p",
                                       [RSM, RSMPid, Seqno, {T, E},
                                        sanitize_stacktrace(Stacktrace)]),
                                failed
                        end,

                    %% Make sure to change flush_snapshot_results() if the
                    %% format of the message is modified.
                    Parent ! {snapshot_result, RSM, self(), Result}
            end),

    {Pid, Snapshot#pending_snapshot{savers = Savers#{Pid => RSM},
                                    remaining_rsms =
                                        sets:del_element(RSM, RSMs)}}.

flush_snapshot_results() ->
    ?FLUSH({snapshot_result, _, _, _}),
    ok.

snapshot_saver(RSM, RSMPid, Seqno) ->
    MRef = erlang:monitor(process, RSMPid),

    receive
        {snapshot, Snapshot} ->
            chronicle_storage:save_rsm_snapshot(Seqno, RSM, Snapshot);
        {'DOWN', MRef, process, RSMPid, _Reason} ->
            ?ERROR("RSM ~p~p died "
                   "before passing a snapshot for seqno ~p.",
                   [RSM, RSMPid, Seqno]),
            failed
    end.

need_snapshot(RSM, #state{pending_snapshot = Snapshot}) ->
    case Snapshot of
        undefined ->
            false;
        #pending_snapshot{seqno = SnapshotSeqno,
                          remaining_rsms = RSMs} ->
            case sets:is_element(RSM, RSMs) of
                true ->
                    {true, SnapshotSeqno};
                false ->
                    false
            end
    end.

handle_snapshot_result(RSM, Pid, Result, State) ->
    case Result of
        ok ->
            handle_snapshot_ok(RSM, Pid, State);
        failed ->
            handle_snapshot_failed(RSM, State)
    end.

handle_snapshot_ok(RSM, Pid, #state{pending_snapshot = Snapshot} = State) ->
    #pending_snapshot{seqno = Seqno, savers = Savers} = Snapshot,

    ?DEBUG("Saved snapshot for RSM ~p at seqno ~p", [RSM, Seqno]),

    NewSavers = maps:remove(Pid, Savers),
    case maps:size(NewSavers) =:= 0 of
        true ->
            ?DEBUG("All RSM snapshots at seqno ~p saved.", [Seqno]),

            chronicle_agent:snapshot_ok(Seqno),
            {noreply, State#state{pending_snapshot = undefined}};
        false ->
            NewSnapshot = Snapshot#pending_snapshot{savers = NewSavers},
            {noreply, State#state{pending_snapshot = NewSnapshot}}
    end.

handle_snapshot_failed(RSM, #state{pending_snapshot = Snapshot} = State) ->
    #pending_snapshot{seqno = Seqno, savers = Savers} = Snapshot,

    ?ERROR("Aborting snapshot at seqno ~b "
           "because RSM ~p failed to take its snapshot",
           [Seqno, RSM]),

    cancel_savers(Savers),
    chronicle_agent:snapshot_failed(Seqno),
    {noreply, State#state{pending_snapshot = undefined}}.

cancel_savers(Savers) ->
    chronicle_utils:maps_foreach(
      fun (Pid, _RSM) ->
              chronicle_utils:terminate_linked_process(Pid, kill)
      end, Savers),
    flush_snapshot_results().

handle_down(MRef, State) ->
    {noreply, remove_reader(MRef, State)}.

add_reader(MRef, Seqno, #state{monitors = Monitors,
                               snapshot_readers = SnapshotReaders} = State) ->
    NewMonitors = maps:put(MRef, Seqno, Monitors),
    NewSnapshotReaders = maps:update_with(Seqno,
                                          fun (V) -> V + 1 end, 1,
                                          SnapshotReaders),
    State#state{monitors = NewMonitors, snapshot_readers = NewSnapshotReaders}.

remove_reader(MRef, #state{monitors = Monitors,
                           snapshot_readers = SnapshotReaders} = State) ->
    case maps:take(MRef, Monitors) of
        {Seqno, NewMonitors} ->
            NewNumReaders = maps:get(Seqno, SnapshotReaders) - 1,
            NewSnapshotReaders =
                case NewNumReaders > 0 of
                    true ->
                        maps:put(Seqno, NewNumReaders, SnapshotReaders);
                    false ->
                        maps:remove(Seqno, SnapshotReaders)
                end,

            NewState = State#state{monitors = NewMonitors,
                                   snapshot_readers = NewSnapshotReaders},
            maybe_release_snapshot(Seqno, NewState),
            NewState;
        error ->
            State
    end.

maybe_release_snapshot(Seqno, State) ->
    case can_release_snapshot(Seqno, State) of
        true ->
            chronicle_agent:release_snapshot(Seqno);
        false ->
            ok
    end.

can_release_snapshot(Seqno, State) ->
    is_stale_snapshot(Seqno, State) andalso no_readers(Seqno, State).

is_stale_snapshot(Seqno, #state{snapshot = {SnapshotSeqno, _, _, _}}) ->
    Seqno =/= SnapshotSeqno.

no_readers(Seqno, #state{snapshot_readers = SnapshotReaders}) ->
    not maps:is_key(Seqno, SnapshotReaders).
