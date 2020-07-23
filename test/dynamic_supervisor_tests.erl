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
-module(dynamic_supervisor_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

start_link() ->
    start_link(stopped).

start_link(State) ->
    dynamic_supervisor:start_link(?MODULE, State).

init(State) ->
    Flags = #{strategy => one_for_one,
              intensity => 1,
              period => 10},
    {ok, Flags, State}.

handle_event({state, NewState}, _) ->
    {noreply, NewState}.

child_specs(stopped) ->
    [];
child_specs(started) ->
    [child_spec(a), child_spec(b)].

child_spec(Name) ->
    #{id => Name,
      start => {?MODULE, start_child, [Name]}}.

start_child(Name) ->
    proc_lib:start_link(
      erlang, apply,
      [fun () ->
               register(Name, self()),
               proc_lib:init_ack({ok, self()}),
               receive
                   _ -> ok
               end
       end,
       []]).

basic_test_() ->
    {spawn,
     fun () ->
             process_flag(trap_exit, true),

             {ok, Pid} = start_link(),
             undefined = whereis(a),
             undefined = whereis(b),

             dynamic_supervisor:send_event(Pid, {state, started}),
             dynamic_supervisor:sync(Pid, 10000),
             true = is_pid(whereis(a)),
             true = is_pid(whereis(b)),

             dynamic_supervisor:send_event(Pid, {state, stopped}),
             dynamic_supervisor:sync(Pid, 10000),
             undefined = whereis(a),
             undefined = whereis(b),

             dynamic_supervisor:send_event(Pid, {state, started}),
             dynamic_supervisor:sync(Pid, 10000),
             chronicle_utils:terminate_linked_process(Pid, shutdown),
             undefined = whereis(a),
             undefined = whereis(b)
     end}.

sync_start_test_() ->
    {spawn,
     fun () ->
             process_flag(trap_exit, true),
             {ok, Pid} = start_link(started),
             true = is_pid(whereis(a)),
             true = is_pid(whereis(b)),

             chronicle_utils:terminate_linked_process(Pid, shutdown)
     end}.

restart_test_() ->
    {spawn,
     fun () ->
             process_flag(trap_exit, true),
             {ok, Pid} = start_link(started),
             exit(whereis(a), shutdown),
             receive
                 {'EXIT', Pid, _} ->
                     exit(bad)
             after
                 100 ->
                     ok
             end,

             exit(whereis(b), shutdown),
             receive
                 {'EXIT', Pid, _} ->
                     ok
             after
                 100 ->
                     exit(bad2)
             end
     end}.
