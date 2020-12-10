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
         add_peer/2, add_peer/3, add_peer/4,
         add_peers/1, add_peers/2, add_peers/3,
         remove_peer/1, remove_peer/2, remove_peers/1, remove_peers/2]).

-export_type([uuid/0, peer/0, history_id/0,
              leader_term/0, seqno/0, peer_position/0,
              revision/0, cluster_info/0]).

-define(DEFAULT_TIMEOUT, 15000).

-type uuid() :: binary().
-type peer() :: atom().
-type peers() :: [peer()].
-type peers_and_roles() :: [{peer(), role()}].
-type history_id() :: binary().

-type leader_term() :: {non_neg_integer(), peer()}.
-type seqno() :: non_neg_integer().
-type peer_position() :: {TermVoted :: leader_term(), HighSeqno :: seqno()}.
-type revision() :: {history_id(), seqno()}.

-type cluster_info() :: #{history_id := history_id(),
                          committed_seqno := seqno(),
                          peers := peers()}.

-type lock() :: binary().
-type lockreq() :: lock() | unlocked.
-type role() :: voter | replica.

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

-type acquire_lock_result() :: {ok, lock()}.
-spec acquire_lock() -> acquire_lock_result().
acquire_lock() ->
    acquire_lock(?DEFAULT_TIMEOUT).

-spec acquire_lock(timeout()) -> acquire_lock_result().
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

-type no_voters_left_error() :: no_voters_left.
-type lock_revoked_error() ::
        {lock_revoked, ExpectedLock::lock(), ActualLock::lock()}.

-type remove_peers_result() :: ok | {error, remove_peers_error()}.
-type remove_peers_error() :: no_voters_left_error()
                            | lock_revoked_error().

-spec remove_peer(peer()) -> remove_peers_result().
remove_peer(Peer) ->
    remove_peer(unlocked, Peer).

-spec remove_peer(lockreq(), peer()) -> remove_peers_result().
remove_peer(Lock, Peer) ->
    remove_peer(Lock, Peer, ?DEFAULT_TIMEOUT).

-spec remove_peer(lockreq(), peer(), timeout()) -> remove_peers_result().
remove_peer(Lock, Peer, Timeout) ->
    remove_peers(Lock, [Peer], Timeout).

-spec remove_peers(peers()) -> remove_peers_result().
remove_peers(Peers) ->
    remove_peers(unlocked, Peers).

-spec remove_peers(lockreq(), peers()) -> remove_peers_result().
remove_peers(Lock, Peers) ->
    remove_peers(Lock, Peers, ?DEFAULT_TIMEOUT).

-spec remove_peers(lockreq(), peers(), timeout()) -> remove_peers_result().
remove_peers(Lock, Peers, Timeout) ->
    validate_peers(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              NewVoters = Voters -- Peers,
              NewReplicas = Replicas -- Peers,

              {ok, NewVoters, NewReplicas}
      end, Lock, Timeout).

-spec add_voter(peer()) -> add_peers_result().
add_voter(Peer) ->
    add_voter(unlocked, Peer).

-spec add_voter(lockreq(), peer()) -> add_peers_result().
add_voter(Lock, Peer) ->
    add_voter(Lock, Peer, ?DEFAULT_TIMEOUT).

-spec add_voter(lockreq(), peer(), timeout()) -> add_peers_result().
add_voter(Lock, Peer, Timeout) ->
    add_voters(Lock, [Peer], Timeout).

-spec add_voters(peers()) -> add_peers_result().
add_voters(Peers) ->
    add_voters(unlocked, Peers).

-spec add_voters(lockreq(), peers()) -> add_peers_result().
add_voters(Lock, Peers) ->
    add_voters(Lock, Peers, ?DEFAULT_TIMEOUT).

-spec add_voters(lockreq(), peers(), timeout()) -> add_peers_result().
add_voters(Lock, Peers, Timeout) ->
    add_peers(Lock, [{Peer, voter} || Peer <- Peers], Timeout).

-spec add_replica(peer()) -> add_peers_result().
add_replica(Peer) ->
    add_replica(unlocked, Peer).

-spec add_replica(lockreq(), peer()) -> add_peers_result().
add_replica(Lock, Peer) ->
    add_replica(Lock, Peer, ?DEFAULT_TIMEOUT).

-spec add_replica(lockreq(), peer(), timeout()) -> add_peers_result().
add_replica(Lock, Peer, Timeout) ->
    add_replicas(Lock, [Peer], Timeout).

-spec add_replicas(peers()) -> add_peers_result().
add_replicas(Peers) ->
    add_replicas(unlocked, Peers).

-spec add_replicas(lockreq(), peers()) -> add_peers_result().
add_replicas(Lock, Peers) ->
    add_replicas(Lock, Peers, ?DEFAULT_TIMEOUT).

-spec add_replicas(lockreq(), peers(), timeout()) -> add_peers_result().
add_replicas(Lock, Peers, Timeout) ->
    add_peers(Lock, [{Peer, replica} || Peer <- Peers], Timeout).

-type add_peers_result() :: ok | {error, add_peers_error()}.
-type add_peers_error() :: lock_revoked_error()
                         | {already_member, peer(), role()}.

-spec add_peer(peer(), role()) -> add_peers_result().
add_peer(Peer, Role) ->
    add_peer(unlocked, Peer, Role).

-spec add_peer(lockreq(), peer(), role()) -> add_peers_result().
add_peer(Lock, Peer, Role) ->
    add_peer(Lock, Peer, Role, ?DEFAULT_TIMEOUT).

-spec add_peer(lockreq(), peer(), role(), timeout()) -> add_peers_result().
add_peer(Lock, Peer, Role, Timeout) ->
    add_peers(Lock, [{Peer, Role}], Timeout).

-spec add_peers(peers_and_roles()) -> add_peers_result().
add_peers(Peers) ->
    add_peers(unlocked, Peers).

-spec add_peers(lockreq(), peers_and_roles()) -> add_peers_result().
add_peers(Lock, Peers) ->
    add_peers(Lock, Peers, ?DEFAULT_TIMEOUT).

-spec add_peers(lockreq(), peers_and_roles(), timeout()) -> add_peers_result().
add_peers(Lock, Peers, Timeout) ->
    validate_peers_and_roles(Peers),
    update_peers(
      fun (Voters, Replicas) ->
              CurrentRoles =
                  [{V, voter} || V <- Voters] ++
                  [{R, replica} || R <- Replicas],
              add_peers_loop(Peers, Voters, Replicas,
                             maps:from_list(CurrentRoles))
      end, Lock, Timeout).

add_peers_loop([], AccVoters, AccReplicas, _) ->
    {ok, AccVoters, AccReplicas};
add_peers_loop([{Peer, Role} | Rest], AccVoters, AccReplicas, CurrentRoles) ->
    case maps:find(Peer, CurrentRoles) of
        {ok, CurrentRole} ->
            {error, {already_member, Peer, CurrentRole}};
        error ->
            {NewAccVoters, NewAccReplicas} =
                case Role of
                    voter ->
                        {[Peer | AccVoters], AccReplicas};
                    replica ->
                        {AccVoters, [Peer | AccReplicas]}
                end,

            add_peers_loop(Rest, NewAccVoters, NewAccReplicas, CurrentRoles)
    end.

-type get_peers_result() :: {ok, #{voters := peers(), replicas := peers()}}.

-spec get_peers() -> get_peers_result().
get_peers() ->
    get_peers(?DEFAULT_TIMEOUT).

-spec get_peers(timeout()) -> get_peers_result().
get_peers(Timeout) ->
    get_config(Timeout,
               fun (Config, _ConfigRevision) ->
                       {ok, #{voters => Config#config.voters,
                              replicas => Config#config.replicas}}
               end).

-spec get_voters() -> {ok, peers()}.
get_voters() ->
    get_voters(?DEFAULT_TIMEOUT).

-spec get_voters(timeout()) -> {ok, peers()}.
get_voters(Timeout) ->
    get_config(Timeout,
               fun (Config, _ConfigRevision) ->
                       {ok, Config#config.voters}
               end).

-spec get_replicas() -> {ok, peers()}.
get_replicas() ->
    get_replicas(?DEFAULT_TIMEOUT).

-spec get_replicas(timeout()) -> {ok, peers()}.
get_replicas(Timeout) ->
    get_config(Timeout,
               fun (Config, _ConfigRevision) ->
                       {ok, Config#config.replicas}
               end).

-spec get_cluster_info() -> cluster_info().
get_cluster_info() ->
    get_cluster_info(?DEFAULT_TIMEOUT).

-spec get_cluster_info(timeout()) -> cluster_info().
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

%% internal
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
              case Fun(Voters, Replicas) of
                  {ok, NewVoters, NewReplicas} ->
                      NewVoters = (NewVoters -- NewReplicas),
                      NewReplicas = (NewReplicas -- NewVoters),

                      case NewVoters of
                          [] ->
                              {stop, {error, no_voters_left}};
                          _ ->
                              NewConfig = Config#config{voters = NewVoters,
                                                        replicas = NewReplicas},
                              {ok, NewConfig}
                      end;
                  {error, _} = Error ->
                      {stop, Error}
              end
      end, Lock, Timeout).

update_config(Fun, Timeout) ->
    update_config(Fun, unlocked, Timeout).

update_config(Fun, Lock, Timeout) ->
    with_leader(Timeout,
                fun (TRef, Leader, _LeaderInfo) ->
                        update_config_loop(Fun, Lock, Leader, TRef)
                end).

update_config_loop(Fun, Lock, Leader, TRef) ->
    validate_lock(Lock),
    get_config(
      Leader, TRef,
      fun (Config, ConfigRevision) ->
              ConfigLock = Config#config.lock,

              case Lock =:= unlocked orelse ConfigLock =:= Lock of
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

validate_peers_and_roles(PeerRoles) ->
    Peers = [Peer || {Peer, _} <- PeerRoles],
    validate_peers(Peers),
    case lists:usort(Peers) =:= lists:sort(Peers) of
        true ->
            ok;
        false ->
            error(badarg)
    end,

    lists:foreach(
      fun ({_Peer, Role}) ->
              case lists:member(Role, [replica, voter]) of
                  true ->
                      ok;
                  false ->
                      error(badarg)
              end
      end, PeerRoles).

validate_lock(Lock) ->
    case Lock of
        unlocked ->
            ok;
        _ when is_binary(Lock) ->
            ok;
        _ ->
            error(badarg)
    end.
