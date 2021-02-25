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
-module(chronicle_ets).

-behavior(gen_server).

-include("chronicle.hrl").

-export([start_link/0]).
-export([register_writer/1, put/2, get/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(TABLE, ?ETS_TABLE(?MODULE)).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

register_writer(Keys) ->
    gen_server:call(?SERVER, {register_writer, self(), sets:from_list(Keys)}).

put(Key, Value) ->
    %% TODO: I could actually check that whoever's writing is the owner of the
    %% key.
    ets:insert(?TABLE, {Key, Value}).

get(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            not_found
    end.

%% gen_server callbacks
init([]) ->
    _ = ets:new(?TABLE, [public, named_table,
                         {read_concurrency, true},
                         {write_concurrency, true}]),
    {ok, #{}}.

handle_call({register_writer, Pid, Keys}, _From, State) ->
    handle_register_writer(Pid, Keys, State).

handle_cast(Cast, State) ->
    {stop, {unexpected_cast, Cast}, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    handle_down(Pid, State).

%% internal
handle_register_writer(Pid, Keys, Writers) ->
    case ?CHECK(check_not_registered(Pid, Writers),
                check_key_conflicts(Keys, Writers)) of
        {ok, NewWriters0} ->
            MRef = erlang:monitor(process, Pid),
            NewWriters = maps:put(Pid, {MRef, Keys}, NewWriters0),
            {reply, ok, NewWriters};
        {error, _} = Error ->
            {reply, Error, Writers}
    end.

check_not_registered(Pid, Writers) ->
    case maps:is_key(Pid, Writers) of
        true ->
            {error, already_registered};
        false ->
            ok
    end.

check_key_conflicts(Keys, Writers) ->
    try
        NewWriters =
            maps:filter(
              fun (Pid, {MRef, PidKeys}) ->
                  Intersection = sets:intersection(Keys, PidKeys),
                  case sets:size(Intersection) > 0 of
                      true ->
                          IntersectionList = sets:to_list(Intersection),

                          case is_process_alive(Pid) of
                              true ->
                                  throw({key_conflict, Pid, IntersectionList});
                              false ->
                                  %% The process is dead by we haven't
                                  %% received/processed the monitor
                                  %% notification yet. Other processes might
                                  %% have done so already. So don't error out
                                  %% unnecessarily.
                                  erlang:demonitor(MRef, [flush]),
                                  delete_keys(PidKeys),
                                  false
                          end;
                      false ->
                          true
                  end
          end, Writers),

        {ok, NewWriters}
    catch
        throw:{key_conflict, _Pid, _ConflictKeys} = Error ->
            {error, Error}
    end.

handle_down(Pid, Writers) ->
    {{_, Keys}, NewWriters} = maps:take(Pid, Writers),
    delete_keys(Keys),
    {noreply, NewWriters}.

delete_keys(Keys) ->
    lists:foreach(
      fun (Key) ->
              ets:delete(?TABLE, Key)
      end, sets:to_list(Keys)).
