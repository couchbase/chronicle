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
-compile(export_all).

-include("chronicle.hrl").

-import(chronicle_utils, [call_async/3]).

-define(MAX_PARALLEL_CATCHUPS, 4).

-record(state, { history_id,
                 term,
                 pids,
                 pending }).

start_link(HistoryId, Term) ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [HistoryId, Term], []).

catchup_peer(Pid, Opaque, Peer, PeerMetadata) ->
    call_async(Pid, Opaque, {catchup_peer, Peer, PeerMetadata}).

stop(Pid) ->
    ok = gen_server:call(Pid, stop, 10000),
    ok = chronicle_utils:wait_for_process(Pid, 1000).

%% callbacks
init([HistoryId, Term]) ->
    {ok, #state{history_id = HistoryId,
                term = Term,
                pids = #{},
                pending = queue:new()}}.

handle_call({catchup_peer, Peer, PeerMetadata}, From, State) ->
    handle_catchup_peer(Peer, PeerMetadata, From, State);
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, _State) ->
    {stop, {unexpected_cast, Cast}}.

handle_info({'DOWN', _MRef, process, Pid, Reason}, State) ->
    handle_down(Pid, Reason, State);
handle_info(Msg, State) ->
    ?WARNING("Unexpected message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, State) ->
    terminate_children(State).

%% internal
handle_catchup_peer(Peer, PeerMetadata, From,
                    #state{pids = Pids, pending = Pending} = State) ->
    case maps:size(Pids) < ?MAX_PARALLEL_CATCHUPS of
        true ->
            spawn_catchup(Peer, PeerMetadata, From, State);
        false ->
            NewPending = queue:in({Peer, PeerMetadata, From}, Pending),
            State#state{pending = NewPending}
    end.

handle_down(Pid, Reason, #state{pids = Pids} = State) ->
    {From, NewPids} = maps:take(Pid, Pids),
    Reply = case Reason of
                {result, Result} ->
                    Result;
                _ ->
                    {error, {catchup_failed, Reason}}
            end,
    gen_server:reply(From, Reply),
    {noreply, maybe_spawn_pending(State#state{pids = NewPids})}.

terminate_children(#state{pids = Pids}) ->
    lists:foreach(
      fun (Pid) ->
              chronicle_utils:terminate_and_wait(Pid, kill)
      end, maps:keys(Pids)).

maybe_spawn_pending(#state{pending = Pending} = State) ->
    case queue:out(Pending) of
        {empty, _} ->
            State;
        {{value, {Peer, PeerMetadata, From}}, NewPending} ->
            NewState = State#state{pending = NewPending},
            spawn_catchup(Peer, PeerMetadata, From, NewState)
    end.

spawn_catchup(Peer, PeerMetadata, From, #state{pids = Pids} = State) ->
    true = (maps:size(Pids) < ?MAX_PARALLEL_CATCHUPS),
    {Pid, _MRef} = spawn_monitor(
                     fun () ->
                             do_catchup(Peer, PeerMetadata, State)
                     end),
    State#state{pids = Pids#{Pid => From}}.

do_catchup(_Peer, _PeerMetadata, _State) ->
    exit({result, {error, unknown}}).