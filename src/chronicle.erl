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

-export([provision/1, reprovision/0, wipe/0]).
-export([get_cluster_info/0, get_cluster_info/1]).
-export([prepare_join/1, join_cluster/1]).
-export([acquire_lock/0, acquire_lock/1]).
-export([get_peers/0, get_peers/1,
         get_voters/0, get_voters/1, get_replicas/0, get_replicas/1]).
-export([add_voter/1, add_voter/2, add_voters/1, add_voters/2,
         add_replica/1, add_replica/2, add_replicas/1, add_replicas/2,
         remove_peer/1, remove_peer/2, remove_peers/1, remove_peers/2]).

-export_type([uuid/0, peer/0, history_id/0,
              leader_term/0, seqno/0, peer_position/0,
              revision/0, cluster_info/0]).

-define(DEFAULT_TIMEOUT, 15000).

-type uuid() :: binary().
-type peer() :: atom().
-type history_id() :: binary().

-type leader_term() :: {non_neg_integer(), peer()}.
-type seqno() :: non_neg_integer().
-type peer_position() :: {TermVoted :: leader_term(), HighSeqno :: seqno()}.
-type revision() :: {history_id(), seqno()}.

-type cluster_info() :: #{history_id := history_id(),
                          committed_seqno := seqno(),
                          peers := [peer()]}.

-spec provision([Machine]) -> chronicle_agent:provision_result() when
      Machine :: {Name :: atom(), Mod :: module(), Args :: [any()]}.
provision(Machines) ->
    chronicle_agent:provision(Machines).

-spec reprovision() -> chronicle_agent:reprovision_result().
reprovision() ->
    chronicle_agent:reprovision().

-spec wipe() -> chronicle_agent:wipe_result().
wipe() ->
    chronicle_agent:wipe().

acquire_lock() ->
    acquire_lock(?DEFAULT_TIMEOUT).

acquire_lock(Timeout) ->
    Lock = chronicle_utils:random_uuid(),
    Result = update_config(
               fun (Config) ->
                       {ok, Config#config{lock = Lock}}
               end, Timeout),

    case Result of
        ok ->
            {ok, Lock};
        _ ->
            Result
    end.

remove_peer(Peer) ->
    remove_peer(undefined, Peer).

remove_peer(Lock, Peer) ->
    remove_peer(Lock, Peer, ?DEFAULT_TIMEOUT).

remove_peer(Lock, Peer, Timeout) ->
    remove_peers(Lock, [Peer], Timeout).

remove_peers(Peers) ->
    remove_peers(undefined, Peers).

remove_peers(Lock, Peers) ->
    remove_peers(Lock, Peers, ?DEFAULT_TIMEOUT).

remove_peers(Lock, Peers, Timeout) ->
    validate_peers(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              NewVoters = Voters -- Peers,
              NewReplicas = Replicas -- Peers,

              {NewVoters, NewReplicas}
      end, Lock, Timeout).

add_voter(Peer) ->
    add_voter(undefined, Peer).

add_voter(Lock, Peer) ->
    add_voter(Lock, Peer, ?DEFAULT_TIMEOUT).

add_voter(Lock, Peer, Timeout) ->
    add_voters(Lock, [Peer], Timeout).

add_voters(Peers) ->
    add_voters(undefined, Peers).

add_voters(Lock, Peers) ->
    add_voters(Lock, Peers, ?DEFAULT_TIMEOUT).

add_voters(Lock, Peers, Timeout) ->
    validate_peers(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              NewVoters = lists:usort(Voters ++ Peers),
              NewReplicas = Replicas -- Peers,
              {NewVoters, NewReplicas}
      end, Lock, Timeout).

add_replica(Peer) ->
    add_replica(undefined, Peer).

add_replica(Lock, Peer) ->
    add_replica(Lock, Peer, ?DEFAULT_TIMEOUT).

add_replica(Lock, Peer, Timeout) ->
    add_replicas(Lock, [Peer], Timeout).

add_replicas(Peers) ->
    add_replicas(undefined, Peers).

add_replicas(Lock, Peers) ->
    add_replicas(Lock, Peers, ?DEFAULT_TIMEOUT).

add_replicas(Lock, Peers, Timeout) ->
    validate_peers(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              NewVoters = Voters -- Peers,
              NewReplicas = lists:usort(Replicas ++ Peers),
              {NewVoters, NewReplicas}
      end, Lock, Timeout).

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

update_peers(Fun, Lock, Timeout) ->
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
      end, Lock, Timeout).

update_config(Fun, Timeout) ->
    update_config(Fun, undefined, Timeout).

update_config(Fun, Lock, Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader, _LeaderInfo) ->
                        update_config_loop(Fun, Lock, Leader, TRef)
                end).

update_config_loop(Fun, Lock, Leader, TRef) ->
    get_config(
      Leader, TRef,
      fun (Config, ConfigRevision) ->
              ConfigLock = Config#config.lock,

              case Lock =:= undefined orelse ConfigLock =:= Lock of
                  true ->
                      case Fun(Config) of
                          {ok, NewConfig}
                            when Config =:= NewConfig ->
                              ok;
                          {ok, NewConfig} ->
                              case cas_config(Leader, NewConfig,
                                              ConfigRevision, TRef) of
                                  {ok, _} ->
                                      ok;
                                  {error, {cas_failed, _}} ->
                                      update_config_loop(Fun,
                                                         Lock, Leader, TRef);
                                  {error, _} = Error ->
                                      Error
                              end;
                          {stop, Return} ->
                              Return
                      end;
                  false ->
                      {error, {lock_revoked, Lock, ConfigLock}}
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

-spec get_cluster_info() -> cluster_info().
get_cluster_info() ->
    get_cluster_info(?DEFAULT_TIMEOUT).

-spec get_cluster_info(Timeout::non_neg_integer()) -> cluster_info().
get_cluster_info(Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader, _LeaderInfo) ->
                        chronicle_server:get_cluster_info(Leader, TRef)
                end).

-spec prepare_join(cluster_info()) -> chronicle_agent:prepare_join_result().
prepare_join(ClusterInfo) ->
    chronicle_agent:prepare_join(ClusterInfo).

-spec join_cluster(cluster_info()) -> chronicle_agent:join_cluster_result().
join_cluster(ClusterInfo) ->
    chronicle_agent:join_cluster(ClusterInfo).
