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
-module(chronicle).

-include("chronicle.hrl").

-import(chronicle_utils, [with_leader/2]).

-export([provision/1]).
-export([get_voters/0, get_voters/1]).
-export([add_voters/1, add_voters/2, remove_voters/1, remove_voters/2]).

-export_type([uuid/0, peer/0, history_id/0,
              leader_term/0, seqno/0, peer_position/0, revision/0]).

-define(DEFAULT_TIMEOUT, 15000).

-type uuid() :: binary().
-type peer() :: atom().
-type history_id() :: binary().

-type leader_term() :: {pos_integer(), peer()}.
-type seqno() :: pos_integer().
-type peer_position() :: {TermVoted :: leader_term(), HighSeqno :: seqno()}.
-type revision() :: {history_id(), seqno()}.

-spec provision([Machine]) -> chronicle_agent:provision_result() when
      Machine :: {Name :: atom(), Mod :: module(), Args :: [any()]}.
provision(Machines) ->
    chronicle_agent:provision(Machines).

add_voters(Voters) ->
    add_voters(Voters, ?DEFAULT_TIMEOUT).

add_voters(Voters, Timeout) ->
    update_voters(
      fun (OldVoters) ->
              Voters ++ OldVoters
      end, Timeout).

remove_voters(Voters) ->
    remove_voters(Voters, ?DEFAULT_TIMEOUT).

remove_voters(Voters, Timeout) ->
    update_voters(
      fun (OldVoters) ->
              OldVoters -- Voters
      end, Timeout).

get_voters() ->
    get_voters(?DEFAULT_TIMEOUT).

get_voters(Timeout) ->
    case get_config(Timeout) of
        {ok, Config, _} ->
            {ok, Config#config.voters};
        Error ->
            Error
    end.

get_config(Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader) ->
                        get_config(Leader, TRef)
                end).

get_config(Leader, TRef) ->
    chronicle_server:get_config(Leader, TRef).

update_voters(Fun, Timeout) ->
    update_config(
      fun (#config{voters = Voters} = Config) ->
              NewVoters = Fun(Voters),
              Config#config{voters = lists:usort(NewVoters)}
      end, Timeout).

update_config(Fun, Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader) ->
                        update_config_loop(Fun, Leader, TRef)
                end).

update_config_loop(Fun, Leader, TRef) ->
    case get_config(Leader, TRef) of
        {ok, Config, ConfigRevision} ->
            NewConfig = Fun(Config),
            case cas_config(Leader, NewConfig, ConfigRevision, TRef) of
                {ok, _} ->
                    ok;
                {error, {cas_failed, _}} ->
                    update_config_loop(Fun, Leader, TRef);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

cas_config(Leader, NewConfig, CasRevision, TRef) ->
    chronicle_server:cas_config(Leader, NewConfig, CasRevision, TRef).
