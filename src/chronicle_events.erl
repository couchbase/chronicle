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
-module(chronicle_events).

-behavior(gen_server).

-include("chronicle.hrl").

-define(SERVER, ?SERVER_NAME(?MODULE)).

-export([start_link/0, start_link/1]).
-export([notify/1, notify/2,
         sync_notify/1, sync_notify/2,
         subscribe/1, subscribe/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() ->
    start_link(?MODULE).

start_link(Name) ->
    gen_server:start_link(?START_NAME(Name), ?MODULE, [], []).

notify(Event) ->
    notify(?MODULE, Event).

notify(Name, Event) ->
    gen_server:cast(?SERVER_NAME(Name), {notify, Event}).

sync_notify(Event) ->
    sync_notify(?MODULE, Event).

sync_notify(Name, Event) ->
    gen_server:call(?SERVER_NAME(Name), {sync_notify, Event}, infinity).

subscribe(Handler) ->
    subscribe(?MODULE, Handler).

subscribe(Name, Handler) ->
    gen_server:call(?SERVER_NAME(Name), {subscribe, self(), Handler}, infinity).

%% callbacks
init([]) ->
    process_flag(trap_exit, true),
    {ok, #{}}.

handle_call({sync_notify, Event}, _From, Watchers) ->
    {reply, ok, notify_watchers(Event, Watchers)};
handle_call({subscribe, Pid, Handler}, _From, Watchers) ->
    {reply, self(), add_watcher(Pid, Handler, Watchers)};
handle_call(_Call, _From, Watchers) ->
    {reply, nack, Watchers}.

handle_cast({notify, Event}, Watchers) ->
    {noreply, notify_watchers(Event, Watchers)};
handle_cast(Cast, Watchers) ->
    ?WARNING("Unexpected cast ~p", [Cast]),
    {noreply, Watchers}.

handle_info({'EXIT', Pid, _Reason} = Exit, Watchers) ->
    case remove_watcher(Pid, Watchers) of
        {ok, NewWatchers} ->
            {noreply, NewWatchers};
        error ->
            {stop, {unknown_process_died, Exit}, Watchers}
    end;
handle_info(Msg, Watchers) ->
    ?WARNING("Received unexpected message ~p", [Msg]),
    {noreply, Watchers}.

terminate(Reason, Watchers) ->
    lists:foreach(
      fun (Pid) ->
              terminate_watcher(Pid, {shutdown, {?MODULE, Reason}})
      end, maps:keys(Watchers)).

%% internal
add_watcher(Pid, Handler, Watchers) ->
    link(Pid),
    maps:update_with(Pid,
                     fun (Handlers) ->
                             [Handler | Handlers]
                     end, [Handler], Watchers).

notify_watchers(Event, Watchers) ->
    Failed =
        maps:fold(
          fun (Pid, Handlers, Acc) ->
                  try
                      lists:foreach(
                        fun (Handler) ->
                                Handler(Event)
                        end, Handlers),
                      Acc
                  catch
                      T:E:Stack ->
                          Reason = {handler_crashed, {T, E, Stack}},
                          terminate_watcher(Pid, Reason),
                          [Pid | Acc]
                  end
          end, [], Watchers),

    maps:without(Failed, Watchers).

remove_watcher(Pid, Watchers) ->
    case maps:take(Pid, Watchers) of
        {_, NewWatchers} ->
            {ok, NewWatchers};
        error ->
            error
    end.

terminate_watcher(Pid, Reason) ->
    true = (Reason =/= normal),
    exit(Pid, Reason),
    unlink(Pid),
    receive
        {'EXIT', Pid, _} ->
            ok
    after
        0 ->
            ok
    end.
