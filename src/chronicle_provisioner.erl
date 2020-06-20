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
-module(chronicle_provisioner).

-behavior(gen_server).

-include("chronicle.hrl").

-export([start_link/0]).
-export([sync/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, { system_state,
                 supervisor }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [],
                          [{hibernate_after, 10000}]).

sync() ->
    ok = gen_server:call(?SERVER_NAME(?MODULE), sync, 10000).

note_system_state(Pid, NewSystemState) ->
    gen_server:cast(Pid, {note_system_state, NewSystemState}).

%% callbacks
init([]) ->
    process_flag(trap_exit, true),

    Self = self(),
    chronicle_events:subscribe(
      fun (Event) ->
              case Event of
                  {system_state, NewState} ->
                      note_system_state(Self, NewState);
                  _ ->
                      ok
              end
      end),

    State = #state{system_state = unprovisioned,
                   supervisor = undefined},
    SystemState = chronicle_agent:get_system_state(),

    case handle_new_system_state(SystemState, State) of
        {ok, FinalState} ->
            {ok, FinalState};
        {error, _} = Error ->
            {stop, Error}
    end.

handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast({note_system_state, NewSystemState}, State) ->
    case handle_new_system_state(NewSystemState, State) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Error} ->
            {stop, {start_error, Error}, State}
    end;
handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast ~p", [Cast]),
    {noreply, State}.

handle_info({'EXIT', Pid, Reason} = Exit, #state{supervisor = Sup} = State) ->
    case Pid =:= Sup of
        true ->
            %% propagate exit to the parent supervisor
            {stop, Reason, State#state{supervisor = undefined}};
        false ->
            {stop, {unexpected_exit, Exit}, State}
    end;
handle_info(Msg, State) ->
    ?WARNING("Unexpected message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, State) ->
    {ok, _} = stop_supervisor(State).

%% internal
handle_new_system_state(NewSystemState,
                        #state{system_state = OldSystemState} = State) ->
    case NewSystemState =:= OldSystemState of
        true ->
            {ok, State};
        false ->
            NewState = State#state{system_state = NewSystemState},
            case NewSystemState of
                provisioned ->
                    start_supervisor(NewState);
                unprovisioned ->
                    stop_supervisor(NewState)
            end
    end.

stop_supervisor(#state{supervisor = Sup} = State) ->
    case Sup of
        undefined ->
            {ok, State};
        _ when is_pid(Sup) ->
            chronicle_utils:terminate_linked_process(Sup, shutdown),
            {ok, State#state{supervisor = undefined}}
    end.

start_supervisor(#state{supervisor = undefined} = State) ->
    case chronicle_provisioned_sup:start_link() of
        {ok, Sup} ->
            {ok, State#state{supervisor = Sup}};
        {error, _} = Error ->
            Error
    end.
