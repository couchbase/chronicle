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
-module(chronicle_peers_vnet).

-include("chronicle.hrl").

-behavior(gen_server).

-export([start_link/0]).
-export([get_live_peers/0, monitor/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, { clients, vnodes }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

get_live_peers() ->
    gen_server:call(?SERVER_NAME(?MODULE), get_live_peers).

monitor() ->
    gen_server:call(?SERVER_NAME(?MODULE), {monitor, self()}).

%% gen_server callbacks
init([]) ->
    {ok, _} = timer:send_interval(1000, refresh),
    {ok, #state{clients = [],
                vnodes = get_vnodes()}}.

handle_call({monitor, Pid}, _From, #state{clients = Clients} = State) ->
    erlang:monitor(process, Pid),
    {reply, ok, State#state{clients = [Pid | Clients]}};
handle_call(get_live_peers, _From, State) ->
    NewState = refresh(State),
    {reply, NewState#state.vnodes, NewState}.

handle_cast(Cast, _State) ->
    {stop, {unexpected_cast, Cast}}.

handle_info(refresh, State) ->
    {noreply, refresh(State)};
handle_info({'DOWN', _MRef, process, Pid, _},
            #state{clients = Clients} = State) ->
    {noreply, State#state{clients = lists:delete(Pid, Clients)}}.

%% internal
refresh(#state{clients = Clients,
               vnodes = OldVNodes} = State) ->
    NewVNodes = get_vnodes(),

    Up = NewVNodes -- OldVNodes,
    Down = OldVNodes -- NewVNodes,

    lists:foreach(
      fun (Pid) ->
              lists:foreach(
                fun (VNode) ->
                        Reason =
                            case VNode =:= ?PEER() of
                                true ->
                                    net_kernel_terminated;
                                false ->
                                    net_tick_timeout
                            end,

                        Pid ! {nodedown, VNode, [{nodedown_reason, Reason}]}
                end, Down),

              lists:foreach(
                fun (VNode) ->
                        Pid ! {nodeup, VNode, []}
                end, Up)
      end, Clients),

    State#state{vnodes = NewVNodes}.

get_vnodes() ->
    Children = supervisor:which_children(vnet),
    VNodes = lists:filtermap(
               fun ({Id, _Pid, worker, [Type]}) ->
                       case Type of
                           vnet_conn ->
                               false;
                           vnet_node ->
                               case is_vnode_started(Id) of
                                   true ->
                                       {true, Id};
                                   false ->
                                       false
                               end
                       end
               end, Children),
    lists:sort(VNodes).

is_vnode_started(VNode) ->
    case sys:get_state(vnet:node_proc(VNode)) of
        {started, _} ->
            true;
        {stopped, _} ->
            false
    end.
