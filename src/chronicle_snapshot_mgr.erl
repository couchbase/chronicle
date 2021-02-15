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
-export([get_latest_snapshot/0, release_snapshot/1, store_snapshot/1, wipe/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(TIMEOUT, chronicle_settings:get({snapshot_mgr, timeout}, 10000)).

-record(state, { snapshot,

                 monitors,
                 snapshot_readers }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

get_latest_snapshot() ->
    gen_server:call(?SERVER, {get_latest_snapshot, self()}, ?TIMEOUT).

release_snapshot(Ref) ->
    gen_server:cast(?SERVER, {release_snapshot, Ref}).

store_snapshot(Snapshot) ->
    gen_server:cast(?SERVER, {store_snapshot, Snapshot}).

wipe() ->
    gen_server:call(?SERVER, wipe, ?TIMEOUT).

%% callbacks
init([]) ->
    {ok, #state{snapshot = undefined,
                monitors = #{},
                snapshot_readers = #{}}}.

handle_call({get_latest_snapshot, Pid}, _From, State) ->
    handle_get_latest_snapshot(Pid, State);
handle_call(wipe, _From, State) ->
    handle_wipe(State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast({store_snapshot, Snapshot}, State) ->
    handle_store_snapshot(Snapshot, State);
handle_cast({release_snapshot, Ref}, State) ->
    handle_release_snapshot(Ref, State);
handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast: ~w", [Cast]),
    {noreply, State}.

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

handle_wipe(#state{monitors = Monitors} = State) ->
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
