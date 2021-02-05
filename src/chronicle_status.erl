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
-module(chronicle_status).

-include("chronicle.hrl").

-behavior(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([get_global_status/0]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).

-define(PING_INTERVAL, 3000).

-record(state, { local_status,

                 last_heard,
                 statuses }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

get_global_status() ->
    gen_server:call(?SERVER, get_global_status).

%% callbacks
init([]) ->
    chronicle_peers:monitor(),
    request_status_all(),
    self() ! send_ping,

    Self = self(),
    chronicle_events:subscribe(
      fun (Event) ->
              case is_interesting_event(Event) of
                  true ->
                      Self ! refresh_status;
                  false ->
                      ok
              end
      end),

    State = #state{local_status = local_status(),
                   last_heard = #{},
                   statuses = #{}},

    send_status_all(State),

    {ok, State}.

handle_call(get_global_status, _From, State) ->
    {reply, global_status(State), State};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast:~n~p", [Cast]),
    {noreply, State}.

handle_info(refresh_status = Msg, State) ->
    ?FLUSH(Msg),
    handle_refresh_status(State);
handle_info({request_status, Peer} = Msg, State) ->
    ?FLUSH(Msg),
    handle_request_status(Peer, State);
handle_info(send_ping, State) ->
    handle_send_ping(State);
handle_info({ping, Peer} = Msg, State) ->
    ?FLUSH(Msg),
    handle_ping(Peer, State);
handle_info({status, Peer, Status}, State) ->
    ?FLUSH({status, Peer, _}),
    handle_status(Peer, Status, State);
handle_info({nodeup, Node, []}, State) ->
    handle_nodeup(Node, State);
handle_info({nodedown, Node, _}, State) ->
    handle_nodedown(Node, State);
handle_info(Msg, State) ->
    ?WARNING("Unexpected message:~n~p", [Msg]),
    {noreply, State}.

%% internal
is_interesting_event(Event) ->
    case Event of
        {new_history, _, _} ->
            true;
        {new_config, _, _} ->
            true;
        _ ->
            false
    end.

handle_refresh_status(#state{local_status = OldStatus} = State) ->
    NewStatus = local_status(),
    case NewStatus =:= OldStatus of
        true ->
            {noreply, State};
        false ->
            NewState = State#state{local_status = NewStatus},
            send_status_all(NewState),
            {noreply, NewState}
    end.

handle_request_status(Peer, State) ->
    send_status(Peer, State),
    {noreply, State}.

handle_send_ping(State) ->
    erlang:send_after(?PING_INTERVAL, self(), send_ping),
    send_ping(),
    {noreply, State}.

handle_ping(Peer, #state{last_heard = LastHeard} = State) ->
    Now = get_timestamp(),
    NewLastHeard = maps:put(Peer, Now, LastHeard),
    {noreply, State#state{last_heard = NewLastHeard}}.

handle_status(Peer, Status, #state{last_heard = LastHeard,
                                   statuses = Statuses} = State) ->
    Now = get_timestamp(),
    NewLastHeard = maps:put(Peer, Now, LastHeard),
    NewStatuses = maps:put(Peer, {Now, Status}, Statuses),
    {noreply, State#state{last_heard = NewLastHeard, statuses = NewStatuses}}.

handle_nodeup(Peer, State) ->
    %% Try not to request a status from ourselves. This is not 100%
    %% bullet-proof if there's a burst of renames. But everything should
    %% converge to a stable state anyway.
    case Peer =/= ?PEER() of
        true ->
            request_status(Peer);
        false ->
            ok
    end,
    {noreply, State}.

handle_nodedown(Peer, #state{last_heard = LastHeard,
                             statuses = Statuses} = State) ->
    NewLastHeard = maps:remove(Peer, LastHeard),
    NewStatuses = maps:remove(Peer, Statuses),
    {noreply, State#state{last_heard = NewLastHeard, statuses = NewStatuses}}.

get_timestamp() ->
    erlang:monotonic_time().

local_status() ->
    Metadata = chronicle_agent:get_metadata(),
    #metadata{pending_branch = Branch, history_id = HistoryId} = Metadata,

    #{history_id => HistoryId, branch => branch_status(Branch)}.

branch_status(undefined) ->
    no_branch;
branch_status(#branch{history_id = NewHistoryId,
                      old_history_id = OldHistoryId,
                      peers = Peers}) ->
    #{old_history_id => OldHistoryId,
      new_history_id => NewHistoryId,
      peers => Peers}.

request_status_all() ->
    request_status(live_peers()).

request_status(Peers) ->
    send_to(Peers, {request_status, ?PEER()}).

send_ping() ->
    send_all({ping, ?PEER()}).

send_status_all(State) ->
    send_status(live_peers(), State).

send_status(Peers, #state{local_status = Status}) ->
    send_to(Peers, {status, ?PEER(), Status}).

send_all(Msg) ->
    send_to(live_peers(), Msg).

send_to(Peer, Msg) when is_atom(Peer) ->
    send_to([Peer], Msg);
send_to(Peers, Msg) when is_list(Peers) ->
    lists:foreach(
      fun (Peer) ->
              chronicle_utils:send(?SERVER(Peer), Msg,
                                   [nosuspend, noconnect])
      end, Peers).

live_peers() ->
    chronicle_peers:get_live_peers_other().

global_status(#state{local_status = LocalStatus,
                     statuses = PeerStatuses}) ->
    AllStatuses = maps:put(?PEER(), {unused, LocalStatus}, PeerStatuses),

    Failovers = aggregate_failovers(AllStatuses),
    Histories = aggregate_histories(AllStatuses),

    #{failovers => Failovers, histories => Histories}.

aggregate_failovers(Statuses) ->
    maps:fold(
      fun (_Peer, {_, PeerStatus}, Acc) ->
              case maps:find(branch, PeerStatus) of
                  {ok, #{old_history_id := OldHistoryId,
                         new_history_id := NewHistoryId,
                         peers := Peers} = Branch} ->

                      case maps:is_key(NewHistoryId, Acc) of
                          true ->
                              Acc;
                          false ->
                              FailoverStatus =
                                  failover_status(Peers, OldHistoryId,
                                                  NewHistoryId, Statuses),
                              Acc#{NewHistoryId =>
                                       Branch#{status => FailoverStatus}}
                      end;
                  _ ->
                      Acc
              end
      end, #{}, Statuses).

failover_status(Peers, OldHistoryId, NewHistoryId, PeerStatuses) ->
    lists:foldl(
      fun (Peer, Acc) ->
              Status = failover_peer_status(Peer, OldHistoryId,
                                            NewHistoryId, PeerStatuses),
              Acc#{Peer => Status}
      end, #{}, lists:sort(Peers)).

failover_peer_status(Peer, OldHistoryId, NewHistoryId, PeerStatuses) ->
    case maps:find(Peer, PeerStatuses) of
        {ok, {_, #{history_id := PeerHistoryId,
                   branch := PeerBranch}}} ->
            case PeerBranch of
                no_branch ->
                    if
                        PeerHistoryId =:= NewHistoryId ->
                            done;
                        PeerHistoryId =:= OldHistoryId ->
                            pending;
                        true ->
                            diverged
                    end;
                _ ->
                    case PeerBranch of
                        #{new_history_id := BranchHistoryId} ->
                            case BranchHistoryId =:= NewHistoryId of
                                true ->
                                    started;
                                false ->
                                    conflict
                            end;
                        _ ->
                            unknown
                    end
            end;
        _ ->
            unknown
    end.

aggregate_histories(Statuses) ->
    Histories = maps:fold(
                  fun (Peer, {_TS, PeerStatus}, Acc) ->
                          update_histories(Peer, PeerStatus, Acc)
                  end, #{}, Statuses),
    maps:map(
      fun (_HistoryId, Peers) ->
              lists:sort(Peers)
      end, Histories).

update_histories(Peer, PeerStatus, Acc) ->
    case maps:find(history_id, PeerStatus) of
        {ok, HistoryId} ->
            maps:update_with(
              HistoryId,
              fun (Peers) ->
                      [Peer | Peers]
              end, [Peer], Acc);
        error ->
            Acc
    end.
