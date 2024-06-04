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

%% For use by chronicle_dump.
-export([setup_logger/0]).
-export([setup_decrypt_function/1]).

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
    ?CHECK(check_data_dir(),
           setup_logger_filter(),
           setup_logger(),
           setup_stats(),
           setup_encryption()
          ).

check_data_dir() ->
    try data_dir() of
        _Dir ->
            ok
    catch
        exit:no_data_dir ->
            {error, {missing_parameter, data_dir}}
    end.

get_function(Name, Arity, Default) ->
    case get_env(Name) of
        {ok, ModFun} ->
            case validate_function(ModFun, Arity) of
                {true, Fun} ->
                    {ok, Fun};
                false ->
                    {error, {badarg, Name, ModFun}}
            end;
        undefined ->
            {ok, Default}
    end.

validate_function({Mod, Fun}, Arity) ->
    case chronicle_utils:is_function_exported(Mod, Fun, Arity) of
        true ->
            {true, fun Mod:Fun/Arity};
        false ->
            false
    end;
validate_function(_, _) ->
    false.

setup_function(Name, Arity, Default, Key) ->
    case get_function(Name, Arity, Default) of
        {ok, Fun} ->
            persistent_term:put(Key, Fun);
        {error, _} = Error ->
            Error
    end.

setup_logger() ->
    setup_function(logger_function, 4, fun logger:log/4, ?CHRONICLE_LOGGER).

setup_logger_filter() ->
    case get_env(setup_logger_filter, true) of
        true ->
            {ok, Modules} = application:get_key(chronicle, modules),
            ModulesMap = maps:from_list([{Mod, true} || Mod <- Modules]),
            Filter = {fun chronicle_logger_filter:filter/2, ModulesMap},
            case logger:add_primary_filter(chronicle_filter, Filter) of
                ok ->
                    ok;
                {error, {already_exist, _}} ->
                    ok;
                {error, _} = Error ->
                    Error
            end;
        false ->
            ok
    end.

setup_stats() ->
    setup_function(stats_function, 1,
                   fun chronicle_stats:ignore_stats/1, ?CHRONICLE_STATS).

setup_encryption() ->
    setup_function(encrypt_function, 1, fun (Data) -> Data end,
                   ?CHRONICLE_ENCRYPT),
    setup_function(decrypt_function, 1, fun (Data) -> {ok, Data} end,
                   ?CHRONICLE_DECRYPT).

setup_decrypt_function(Fun) ->
    persistent_term:put(?CHRONICLE_DECRYPT, Fun).

-ifndef(TEST).

get_env(Parameter) ->
    application:get_env(chronicle, Parameter).

-else.

peer_param(Parameter) ->
    case whereis(vnet) of
        undefined ->
            Parameter;
        Pid when is_pid(Pid) ->
            list_to_atom(atom_to_list(?PEER()) ++ "-" ++
                             atom_to_list(Parameter))
    end.

get_env(Parameter) ->
    application:get_env(chronicle, peer_param(Parameter)).

set_env(Parameter, Value) ->
    application:set_env(chronicle, peer_param(Parameter), Value).

-endif.

get_env(Parameter, Default) ->
    case get_env(Parameter) of
        {ok, Value} ->
            Value;
        undefined ->
            Default
    end.
