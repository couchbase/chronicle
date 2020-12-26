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
-module(chronicle_logger_filter).

-export([filter/2]).

filter(Event, Modules) ->
    case maps:find(msg, Event) of
        {ok, {report, Report}} when is_map(Report) ->
            case maps:find(label, Report) of
                {ok, Label} ->
                    Action =
                        case Label of
                            {gen_statem, terminate} ->
                                gen_statem_filter(Report, Modules);
                            {proc_lib, crash} ->
                                proc_lib_filter(Report, Modules);
                            {supervisor, Error}
                              when Error =:= child_terminated;
                                   Error =:= start_error;
                                   Error =:= shutdown_error;
                                   Error =:= shutdown ->
                                supervisor_filter(Report, Modules);
                            _ ->
                                ignore
                        end,

                    case Action of
                        ignore ->
                            ignore;
                        NewReport ->
                            Event#{msg => {report, NewReport}}
                    end;
                error ->
                    ignore
            end;
        _ ->
            ignore
    end.

gen_statem_filter(Report, Modules) ->
    case maps:find(modules, Report) of
        {ok, [Module|_]} when is_map_key(Module, Modules) ->
            case Report of
                #{queue := Queue,
                  postponed := Postponed,
                  reason := {Class, Reason, Stack}} ->
                    NewQueue = sanitize_events(Module, Queue),
                    NewPostponed = sanitize_events(Module, Postponed),
                    NewReason = chronicle_utils:sanitize_reason(Reason),
                    NewStack = chronicle_utils:sanitize_stacktrace(Stack),

                    Report#{queue => NewQueue,
                            postponed => NewPostponed,
                            reason => {Class, NewReason, NewStack}};
                _ ->
                    ignored
            end;
        _ ->
            ignore
    end.

sanitize_events(Module, Events) ->
    case erlang:function_exported(Module, sanitize_event, 2) of
        true ->
            [try Module:sanitize_event(Type, Event) of
                 Sanitized -> Sanitized
             catch
                 _:_ ->
                     {crashed, {Module, sanitize_event, 2}}
             end || {Type, Event} <- Events];
        false ->
            Events
    end.

proc_lib_filter(Report, Modules) ->
    case maps:find(report, Report) of
        {ok, [Info | Rest]} when is_list(Info) ->
            case lists:keyfind(initial_call, 1, Info) of
                {_, {Module, init, _}} when is_map_key(Module, Modules) ->
                    %% Messages can be large and may contain sensitive
                    %% information.
                    NewInfo0 = lists:keyreplace(messages, 1, Info,
                                                {messages, omitted}),
                    NewInfo1 =
                        case lists:keyfind(error_info, 1, NewInfo0) of
                            {_, {Class, Reason, Stack}} ->
                                NewReason =
                                    chronicle_utils:sanitize_reason(Reason),
                                NewStack =
                                    chronicle_utils:sanitize_stacktrace(Stack),

                                lists:keyreplace(error_info, 1, NewInfo0,
                                                 {error_info,
                                                  {Class,
                                                   NewReason, NewStack}});
                            _ ->
                                NewInfo0
                        end,

                    Report#{report => [NewInfo1 | Rest]};
                _ ->
                    ignore
            end;
        _ ->
            ignore
    end.

supervisor_filter(Report, Modules) ->
    case maps:find(report, Report) of
        {ok, Info} when is_list(Info) ->
            case lists:keyfind(supervisor, 1, Info) of
                {_, {_, Module}} when is_map_key(Module, Modules) ->
                    case lists:keyfind(reason, 1, Info) of
                        {_, Reason} ->
                            NewReason = chronicle_utils:sanitize_reason(Reason),
                            NewInfo = lists:keyreplace(reason, 1, Info,
                                                       {reason, NewReason}),
                            Report#{report => NewInfo};
                        false ->
                            ignore
                    end;
                _ ->
                    ignore
            end;
        _ ->
            ignore
    end.
