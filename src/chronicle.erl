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

-export([provision/1, reprovision/0]).
-export([get_cluster_info/0, get_cluster_info/1]).
-export([get_peers/0, get_peers/1,
         get_voters/0, get_voters/1, get_replicas/0, get_replicas/1]).
-export([add_voter/1, add_voter/2, add_voters/1, add_voters/2,
         add_replica/1, add_replica/2, add_replicas/1, add_replicas/2,
         remove_peer/1, remove_peer/2, remove_peers/1, remove_peers/2]).

-export_type([uuid/0, peer/0, history_id/0,
              leader_term/0, seqno/0, peer_position/0, revision/0]).

-define(DEFAULT_TIMEOUT, 15000).

-type uuid() :: binary().
-type peer() :: atom().
-type history_id() :: binary().

-type leader_term() :: {non_neg_integer(), peer()}.
-type seqno() :: non_neg_integer().
-type peer_position() :: {TermVoted :: leader_term(), HighSeqno :: seqno()}.
-type revision() :: {history_id(), seqno()}.

-spec provision([Machine]) -> chronicle_agent:provision_result() when
      Machine :: {Name :: atom(), Mod :: module(), Args :: [any()]}.
provision(Machines) ->
    chronicle_agent:provision(Machines).

-spec reprovision() -> chronicle_agent:provision_result().
reprovision() ->
    chronicle_agent:reprovision().

remove_peer(Peer) ->
    remove_peer(Peer, ?DEFAULT_TIMEOUT).

remove_peer(Peer, Timeout) ->
    remove_peers([Peer], Timeout).

remove_peers(Peers) ->
    remove_peers(Peers, ?DEFAULT_TIMEOUT).

remove_peers(Peers, Timeout) ->
    validate_peers(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              NewVoters = Voters -- Peers,
              NewReplicas = Replicas -- Peers,

              {NewVoters, NewReplicas}
      end, Timeout).

add_voter(Peer) ->
    add_voter(Peer, ?DEFAULT_TIMEOUT).

add_voter(Peer, Timeout) ->
    add_voters([Peer], Timeout).

add_voters(Peers) ->
    add_voters(Peers, ?DEFAULT_TIMEOUT).

add_voters(Peers, Timeout) ->
    validate_peers(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              NewVoters = lists:usort(Voters ++ Peers),
              NewReplicas = Replicas -- Peers,
              {NewVoters, NewReplicas}
      end, Timeout).

add_replica(Peer) ->
    add_replica(Peer, ?DEFAULT_TIMEOUT).

add_replica(Peer, Timeout) ->
    add_replicas([Peer], Timeout).

add_replicas(Peers) ->
    add_replicas(Peers, ?DEFAULT_TIMEOUT).

add_replicas(Peers, Timeout) ->
    validate_peers(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              NewVoters = Voters -- Peers,
              NewReplicas = lists:usort(Replicas ++ Peers),
              {NewVoters, NewReplicas}
      end, Timeout).

get_peers() ->
    get_peers(?DEFAULT_TIMEOUT).

get_peers(Timeout) ->
    get_config(Timeout,
               fun (Config, _ConfigRevision) ->
                       {ok, #{voters => Config#config.voters,
                              replicas => Config#config.replicas}}
               end).

get_voters() ->
    get_voters(?DEFAULT_TIMEOUT).

get_voters(Timeout) ->
    get_config(Timeout,
               fun (Config, _ConfigRevision) ->
                       {ok, Config#config.voters}
               end).

get_replicas() ->
    get_replicas(?DEFAULT_TIMEOUT).

get_replicas(Timeout) ->
    get_config(Timeout,
               fun (Config, _ConfigRevision) ->
                       {ok, Config#config.replicas}
               end).

get_config(Timeout, Fun) ->
    with_leader(Timeout,
                fun (TRef, Leader, _LeaderInfo) ->
                        get_config(Leader, TRef, Fun)
                end).

get_config(Leader, TRef, Fun) ->
    case chronicle_server:get_config(Leader, TRef) of
        {ok, Config, ConfigRevision} ->
            Fun(Config, ConfigRevision);
        {error, _} = Error ->
            Error
    end.

update_peers(Fun, Timeout) ->
    update_config(
      fun (#config{voters = Voters, replicas = Replicas} = Config) ->
              {NewVoters, NewReplicas} = Fun(Voters, Replicas),

              NewVoters = (NewVoters -- NewReplicas),
              NewReplicas = (NewReplicas -- NewVoters),

              case NewVoters of
                  [] ->
                      {stop, {error, no_voters_left}};
                  _ ->
                      NewConfig = Config#config{voters = NewVoters,
                                                replicas = NewReplicas},
                      {ok, NewConfig}
              end
      end, Timeout).

update_config(Fun, Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader, _LeaderInfo) ->
                        update_config_loop(Fun, Leader, TRef)
                end).

update_config_loop(Fun, Leader, TRef) ->
    get_config(
      Leader, TRef,
      fun (Config, ConfigRevision) ->
              case Fun(Config) of
                  {ok, NewConfig} when Config =:= NewConfig ->
                      ok;
                  {ok, NewConfig} ->
                      case cas_config(Leader, NewConfig,
                                      ConfigRevision, TRef) of
                          {ok, _} ->
                              ok;
                          {error, {cas_failed, _}} ->
                              update_config_loop(Fun, Leader, TRef);
                          {error, _} = Error ->
                              Error
                      end;
                  {stop, Return} ->
                      Return
              end
      end).

cas_config(Leader, NewConfig, CasRevision, TRef) ->
    chronicle_server:cas_config(Leader, NewConfig, CasRevision, TRef).

validate_peers(Peers) ->
    lists:foreach(
      fun (Peer) ->
              case is_atom(Peer) of
                  true ->
                      ok;
                  false ->
                      error(badarg)
              end
      end, Peers).

get_cluster_info() ->
    get_cluster_info(?DEFAULT_TIMEOUT).

get_cluster_info(Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader, _LeaderInfo) ->
                        chronicle_server:get_cluster_info(Leader, TRef)
                end).
