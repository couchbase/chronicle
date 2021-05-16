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

-export([get_system_state/0]).
-export([force_snapshot/0, export_snapshot/1]).
-export([check_quorum/0, check_quorum/1]).
-export([get_peer_statuses/0, get_cluster_status/0]).
-export([provision/1, reprovision/0, wipe/0]).
-export([get_cluster_info/0, get_cluster_info/1]).
-export([prepare_join/1, join_cluster/1]).
-export([failover/1, failover/2, try_cancel_failover/2]).
-export([acquire_lock/0, acquire_lock/1]).
-export([get_peers/0, get_peers/1,
         get_voters/0, get_voters/1, get_replicas/0, get_replicas/1]).
-export([add_voter/1, add_voter/2, add_voters/1, add_voters/2,
         add_replica/1, add_replica/2, add_replicas/1, add_replicas/2,
         add_peer/2, add_peer/3, add_peer/4,
         add_peers/1, add_peers/2, add_peers/3,
         remove_peer/1, remove_peer/2, remove_peers/1, remove_peers/2]).
-export([set_peer_role/2, set_peer_role/3, set_peer_role/4,
         set_peer_roles/1, set_peer_roles/2, set_peer_roles/3]).
-export([switch_compat_version/0, switch_compat_version/1]).

%% For internal use only currently. Changing these may render chronicle
%% unusable.
-export([set_setting/2, set_setting/3,
         unset_setting/1, unset_setting/2,
         replace_settings/1, replace_settings/2]).

-export_type([uuid/0, peer/0, peer_id/0,
              history_id/0, history_log/0,
              leader_term/0, seqno/0, peer_position/0,
              revision/0,
              serial/0, incarnation/0,
              cluster_info/0]).

-define(DEFAULT_TIMEOUT, 15000).

-type uuid() :: binary().
-type peer() :: atom().
-type peer_id() :: uuid().
-type peers() :: [peer()].
-type peers_and_roles() :: [{peer(), role()}].
-type history_id() :: binary().
-type history_log() :: [{history_id(), seqno()}].
-type compat_version() :: non_neg_integer().

-type leader_term() :: {non_neg_integer(), peer()}.
-type seqno() :: non_neg_integer().
-type peer_position() :: {TermVoted :: leader_term(), HighSeqno :: seqno()}.
-type revision() :: {history_id(), seqno()}.

-type serial() :: non_neg_integer().
-type incarnation() :: non_neg_integer().

-type cluster_info() :: #{history_id := history_id(),
                          committed_seqno := seqno(),
                          peers := peers()}.

-type lock() :: binary().
-type lockreq() :: lock() | unlocked.
-type role() :: voter | replica.

-spec get_system_state() ->
          not_provisioned |
          joining_cluster |
          provisioned |
          removed.
get_system_state() ->
    case chronicle_agent:get_system_state() of
        not_provisioned ->
            not_provisioned;
        {State, _Extra}
          when State =:= joining_cluster;
               State =:= provisioned;
               State =:= removed ->
            State
    end.

-spec force_snapshot() -> chronicle_agent:force_snapshot_result().
force_snapshot() ->
    chronicle_agent:force_snapshot().

-spec export_snapshot(Dir::file:name()) ->
          chronicle_agent:export_snapshot_result().
export_snapshot(Path) ->
    chronicle_agent:export_snapshot(Path).

-type check_quorum_result() :: true
                             | {false, timeout | no_leader}.

-spec check_quorum() -> check_quorum_result().
check_quorum() ->
    check_quorum(?DEFAULT_TIMEOUT).

-spec check_quorum(timeout()) -> check_quorum_result().
check_quorum(Timeout) ->
    chronicle_config_rsm:check_quorum(Timeout).

-spec get_peer_statuses() -> chronicle_status:peer_statuses().
get_peer_statuses() ->
    chronicle_status:get_peers().

-spec get_cluster_status() -> chronicle_status:cluster_status().
get_cluster_status() ->
    chronicle_status:get_cluster_status().

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
    chronicle_config_rsm:acquire_lock(Timeout).

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
    chronicle_config_rsm:remove_peers(Lock, Peers, Timeout).

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
    chronicle_config_rsm:add_peers(Lock, Peers, Timeout).

-type set_peer_roles_result() :: ok | {error, set_peer_roles_error()}.
-type set_peer_roles_error() :: lock_revoked_error()
                              | no_voters_left_error()
                              | {not_member, peer()}.

-spec set_peer_role(peer(), role()) -> set_peer_roles_result().
set_peer_role(Peer, Role) ->
    set_peer_role(unlocked, Peer, Role).

-spec set_peer_role(lockreq(), peer(), role()) -> set_peer_roles_result().
set_peer_role(Lock, Peer, Role) ->
    set_peer_role(Lock, Peer, Role, ?DEFAULT_TIMEOUT).

-spec set_peer_role(lockreq(), peer(), role(), timeout()) ->
          set_peer_roles_result().
set_peer_role(Lock, Peer, Role, Timeout) ->
    set_peer_roles(Lock, [{Peer, Role}], Timeout).

-spec set_peer_roles(peers_and_roles()) -> set_peer_roles_result().
set_peer_roles(Peers) ->
    set_peer_roles(unlocked, Peers).

-spec set_peer_roles(lockreq(), peers_and_roles()) -> set_peer_roles_result().
set_peer_roles(Lock, Peers) ->
    set_peer_roles(Lock, Peers, ?DEFAULT_TIMEOUT).

-spec set_peer_roles(lockreq(), peers_and_roles(), timeout()) ->
          set_peer_roles_result().
set_peer_roles(Lock, Peers, Timeout) ->
    chronicle_config_rsm:set_peer_roles(Lock, Peers, Timeout).

-type get_peers_result() :: #{voters := peers(), replicas := peers()}.

-spec get_peers() -> get_peers_result().
get_peers() ->
    get_peers(?DEFAULT_TIMEOUT).

-spec get_peers(timeout()) -> get_peers_result().
get_peers(Timeout) ->
    chronicle_config_rsm:get_peers(Timeout).

-spec get_voters() -> peers().
get_voters() ->
    get_voters(?DEFAULT_TIMEOUT).

-spec get_voters(timeout()) -> peers().
get_voters(Timeout) ->
    #{voters := Voters} = get_peers(Timeout),
    Voters.

-spec get_replicas() -> peers().
get_replicas() ->
    get_replicas(?DEFAULT_TIMEOUT).

-spec get_replicas(timeout()) -> peers().
get_replicas(Timeout) ->
    #{replicas := Replicas} = get_peers(Timeout),
    Replicas.

-spec get_cluster_info() -> cluster_info().
get_cluster_info() ->
    get_cluster_info(?DEFAULT_TIMEOUT).

-spec get_cluster_info(timeout()) -> cluster_info().
get_cluster_info(Timeout) ->
    chronicle_config_rsm:get_cluster_info(Timeout).

-spec prepare_join(cluster_info()) -> chronicle_agent:prepare_join_result().
prepare_join(ClusterInfo) ->
    chronicle_agent:prepare_join(ClusterInfo).

-spec join_cluster(cluster_info()) -> chronicle_agent:join_cluster_result().
join_cluster(ClusterInfo) ->
    chronicle_agent:join_cluster(ClusterInfo).

-spec failover(peers()) -> chronicle_failover:failover_result().
failover(KeepPeers) ->
    chronicle_failover:failover(KeepPeers).

-spec failover(peers(), Opaque::any()) -> chronicle_failover:failover_result().
failover(KeepPeers, Opaque) ->
    chronicle_failover:failover(KeepPeers, Opaque).

-spec try_cancel_failover(history_id(), peers()) ->
          chronicle_failover:try_cancel_result().
try_cancel_failover(Id, Peers) ->
    chronicle_failover:try_cancel(Id, Peers).

-spec set_setting(term(), term()) -> ok.
set_setting(Name, Value) ->
    set_setting(Name, Value, ?DEFAULT_TIMEOUT).

-spec set_setting(term(), term(), timeout()) -> ok.
set_setting(Name, Value, Timeout) ->
    chronicle_config_rsm:set_settings(#{Name => Value}, Timeout).

-spec unset_setting(term()) -> ok.
unset_setting(Name) ->
    unset_setting(Name, ?DEFAULT_TIMEOUT).

-spec unset_setting(term(), timeout()) -> ok.
unset_setting(Name, Timeout) ->
    chronicle_config_rsm:unset_settings([Name], Timeout).

-spec replace_settings(map()) -> ok.
replace_settings(Settings) ->
    replace_settings(Settings, ?DEFAULT_TIMEOUT).

-spec replace_settings(map(), timeout()) -> ok.
replace_settings(Settings, Timeout) ->
    chronicle_config_rsm:replace_settings(Settings, Timeout).

-type switch_compat_version_result() ::
        {ok,
         OldVersion::chronicle:compat_version(),
         NewVersion::chronicle:compat_version()} |
        {error, switch_compat_version_error()}.
-type switch_compat_version_error() ::
        lock_revoked_error() |
        {get_peer_infos_failed,
         #{chronicle:peer() => Error::any()}}.

-spec switch_compat_version() -> switch_compat_version_result().
switch_compat_version() ->
    switch_compat_version(unlocked).

-spec switch_compat_version(lockreq()) -> switch_compat_version_result().
switch_compat_version(Lock) ->
    #{voters := Voters, replicas := Replicas} = get_peers(),
    Peers = Voters ++ Replicas,
    case chronicle_agent:get_peer_infos(Peers) of
        {ok, Infos0} ->
            Infos = maps:values(Infos0),
            Versions = [maps:get(supported_compat_version, Info) ||
                           Info <- Infos],

            SupportedVersion = lists:min(Versions),
            chronicle_config_rsm:set_compat_version(Lock, SupportedVersion,
                                                    ?DEFAULT_TIMEOUT);
        {error, Failed} ->
            {error, {get_peer_infos_failed, Failed}}
    end.
