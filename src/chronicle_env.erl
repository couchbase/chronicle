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
-module(chronicle_env).

-include("chronicle.hrl").

-export([data_dir/0]).
-export([setup/0]).

-ifdef(TEST).
-export([set_env/2]).
-endif.

data_dir() ->
    case get_env(data_dir) of
        {ok, Dir} ->
            Dir;
        undefined ->
            exit(no_data_dir)
    end.

setup() ->
    case check_data_dir() of
        ok ->
            setup_logger();
        {error, _} = Error ->
            Error
    end.

check_data_dir() ->
    try data_dir() of
        _Dir ->
            ok
    catch
        exit:no_data_dir ->
            {error, {missing_parameter, data_dir}}
    end.

get_logger_function() ->
    case get_env(logger_function) of
        {ok, ModFun} ->
            case validate_logger_function(ModFun) of
                {true, LoggerFun} ->
                    {ok, LoggerFun};
                false ->
                    {error, {badarg, logger_function, ModFun}}
            end;
        undefined ->
            {ok, fun logger:log/4}
    end.

validate_logger_function({Mod, Fun}) ->
    case erlang:function_exported(Mod, Fun, 4) of
        true ->
            {true, fun Mod:Fun/4};
        false ->
            false
    end;
validate_logger_function(_) ->
    false.

setup_logger() ->
    case get_logger_function() of
        {ok, Fun} ->
            persistent_term:put(?CHRONICLE_LOGGER, Fun);
        {error, _} = Error ->
            Error
    end.

-ifndef(TEST).

get_env(Parameter) ->
    application:get_env(chronicle, Parameter).

-else.

get_env(Parameter) ->
    application:get_env(chronicle, {?PEER(), Parameter}).

set_env(Parameter, Value) ->
    application:set_env(chronicle, {?PEER(), Parameter}, Value).

-endif.
