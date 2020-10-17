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
-module(dynamic_supervisor).

-include("chronicle.hrl").

%% This module implements both supervisor and gen_server behaviors. But if I
%% uncomment the following line, this causes a "callback conflict" warning to
%% be emitted.
%%
%% -behavior(supervisor).
-behavior(gen_server).

-export([start_link/2, start_link/3, sync/2, send_event/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {supervisor :: undefined | pid(),
                mod :: module(),
                mod_state :: term(),
                child_specs :: list()}).

-callback init(Args :: term()) ->
    {ok, SupFlags :: supervisor:sup_flags(), State :: term()} |
    {stop, Reason :: term()} |
    ignore.
-callback child_specs(State :: term()) ->
    [ChildSpec :: supervisor:child_spec()].
-callback handle_event(Event :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {stop, Reason :: term()}.

start_link(Module, Args) ->
    gen_server:start_link(?MODULE, {gen_server, Module, Args}, []).

start_link(Name, Module, Args) ->
    gen_server:start_link(Name, ?MODULE, {gen_server, Module, Args}, []).

sync(ServerRef, Timeout) ->
    gen_server:call(ServerRef, sync, Timeout).

send_event(ServerRef, Event) ->
    gen_server:cast(ServerRef, {event, Event}).

%% callbacks
init({supervisor, Flags}) ->
    {ok, {Flags, []}};
init({gen_server, Module, Args}) ->
    process_flag(trap_exit, true),
    case Module:init(Args) of
        {ok, Flags, ModState} ->
            %% There's no simple way to enforce the order of children that are
            %% started dynamically. So only one_for_all and one_for_one
            %% restart strategies are supported, where the order doesn't
            %% matter.
            Strategy = maps:get(strategy, Flags),
            true = lists:member(Strategy, [one_for_all, one_for_one]),

            case supervisor:start_link(?MODULE, {supervisor, Flags}) of
                {ok, Pid} ->
                    State = #state{supervisor = Pid,
                                   mod = Module,
                                   mod_state = ModState,
                                   child_specs = []},
                    {ok, manage_children(State)};
                Other ->
                    {stop, {failed_to_start_sup, Other}}
            end;
        ignore ->
            ignore;
        {stop, _} = Stop ->
            Stop
    end.

handle_call(sync, _From, State) ->
    {reply, ok, State}.

handle_cast({event, Event}, #state{mod = Module,
                                   mod_state = ModState} = State) ->
    case Module:handle_event(Event, ModState) of
        {noreply, NewModState} ->
            {noreply, handle_mod_state(NewModState, State)};
        {stop, Reason} ->
            {stop, Reason, State}
    end.

handle_info({'EXIT', Pid, Reason}, #state{supervisor = Pid} = State) ->
    {stop, Reason, State#state{supervisor = undefined}};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info(Msg, State) ->
    ?WARNING("Ignored an unexpected message: ~p", [Msg]),
    {noreply, State}.

terminate(Reason, #state{supervisor = Supervisor}) ->
    case is_pid(Supervisor) of
        true ->
            chronicle_utils:terminate_linked_process(Supervisor, Reason);
        false ->
            ok
    end.

%% internal
handle_mod_state(NewModState, #state{mod_state = OldModState} = State) ->
    case NewModState =:= OldModState of
        true ->
            State;
        false ->
            manage_children(State#state{mod_state = NewModState})
    end.

manage_children(#state{supervisor = Pid,
                       mod = Module,
                       mod_state = ModState,
                       child_specs = OldSpecs} = State) ->
    NewSpecs = Module:child_specs(ModState),

    Removed = OldSpecs -- NewSpecs,
    Added = NewSpecs -- OldSpecs,

    stop_children(Pid, Removed),
    start_children(Pid, Added),

    State#state{child_specs = NewSpecs}.

stop_children(Pid, Specs) ->
    lists:foreach(
      fun (Spec) ->
              stop_child(Pid, Spec)
      end, Specs).

stop_child(Pid, #{id := Id}) ->
    case stop_child_loop(Pid, Id, 5) of
        ok ->
            ok;
        {error, Error} ->
            exit({failed_to_stop_child, Id, Error})
    end.

stop_child_loop(Pid, Id, Retries) ->
    case supervisor:terminate_child(Pid, Id) of
        ok ->
            case supervisor:delete_child(Pid, Id) of
                ok ->
                    ok;
                {error, Error}
                  when Error =:= running;
                       Error =:= restarting ->
                    %% There's no way to terminate and delete a child
                    %% atomically. Depending on the restart strategy, the
                    %% child that we just terminated above might be running
                    %% again by the time we're trying to delete it. To work
                    %% around this, we retry a couple of times.
                    case Retries of
                        0 ->
                            {error, exceeded_retries};
                        _ ->
                            stop_child_loop(Pid, Id, Retries - 1)
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

start_children(Pid, Specs) ->
    lists:foreach(
      fun (Spec) ->
              case supervisor:start_child(Pid, Spec) of
                  {ok, _} ->
                      ok;
                  {error, Error} ->
                      exit({failed_to_start_child, Spec, Error})
              end
      end, Specs).
