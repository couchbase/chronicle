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
-module(chronicle_config).

-include("chronicle.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([is_config/1, is_stable/1]).
-export([transition/2, next_config/1]).
-export([init/2, reinit/3, branch/2]).
-export([set_lock/2, check_lock/2]).
-export([get_rsms/1, get_quorum/1]).
-export([get_peers/1, get_replicas/1, get_voters/1]).
-export([add_peers/2, remove_peers/2, set_peer_roles/2]).
-export([get_peer_id/2]).

-export_type([peers/0]).

-type peers() :: #{chronicle:peer() =>
                       #{id := chronicle:peer_id(), role := chronicle:role()}}.

is_config(Value) ->
    case Value of
        #config{} ->
            true;
        _ ->
            false
    end.

is_stable(#config{old_peers = OldPeers}) ->
    OldPeers =:= undefined.

next_config(#config{old_peers = OldPeers} = Config) ->
    case OldPeers =:= undefined of
        true ->
            Config;
        false ->
            Config#config{old_peers = undefined}
    end.

transition(NewConfig, OldConfig) ->
    case needs_transition(NewConfig, OldConfig) of
        true ->
            NewConfig#config{old_peers = OldConfig#config.peers};
        false ->
            NewConfig
    end.

init(Peer, Machines) ->
    MachinesMap =
        maps:from_list(
          [{Name, #rsm_config{module = Module, args = Args}} ||
              {Name, Module, Args} <- Machines]),
    Peers = #{Peer => peer_info(voter)},
    #config{peers = Peers, state_machines = MachinesMap}.

reinit(NewPeer, OldPeer, #config{old_peers = undefined} = Config) ->
    PeerId = get_peer_id(OldPeer, Config),
    Config#config{peers = #{NewPeer => peer_info(PeerId, voter)}}.

branch(Peers, Config) ->
    %% TODO: figure out what to do with replicas
    PeerInfos = [{Peer, peer_info(get_peer_id(Peer, Config), voter)} ||
                    Peer <- Peers],
    Config#config{peers = maps:from_list(PeerInfos),
                  old_peers = undefined}.

set_lock(Lock, #config{} = Config) ->
    Config#config{lock = Lock}.

check_lock(LockReq, #config{lock = Lock}) ->
    case LockReq =:= unlocked orelse LockReq =:= Lock of
        true ->
            ok;
        false ->
            {error, {lock_revoked, LockReq, Lock}}
    end.

get_rsms(#config{state_machines = RSMs}) ->
    RSMs.

get_quorum(#config{peers = Peers, old_peers = OldPeers}) ->
    case OldPeers =:= undefined of
        true ->
            peers_quorum(Peers);
        false ->
            {joint, peers_quorum(Peers), peers_quorum(OldPeers)}
    end.

get_peers(#config{peers = Peers, old_peers = OldPeers}) ->
    case OldPeers =:= undefined of
        true ->
            lists:sort(maps:keys(Peers));
        false ->
            lists:usort(maps:keys(Peers) ++ maps:keys(OldPeers))
    end.

get_replicas(#config{peers = Peers}) ->
    lists:sort(peers_replicas(Peers)).

get_voters(#config{peers = Peers}) ->
    lists:sort(peers_voters(Peers)).

add_peers(Peers, Config) ->
    update_peers(
      fun (CurrentPeers) ->
              add_peers_loop(Peers, CurrentPeers)
      end, Config).

add_peers_loop([], AccPeers) ->
    {ok, AccPeers};
add_peers_loop([{Peer, Role} | Rest], AccPeers) ->
    case maps:find(Peer, AccPeers) of
        {ok, CurrentRole} ->
            {error, {already_member, Peer, CurrentRole}};
        error ->
            add_peers_loop(Rest, AccPeers#{Peer => peer_info(Role)})
    end.

remove_peers(Peers, Config) ->
    update_peers(
      fun (CurrentPeers) ->
              {ok, maps:without(Peers, CurrentPeers)}
      end, Config).

set_peer_roles(Peers, Config) ->
    update_peers(
      fun (CurrentPeers) ->
              set_peer_roles_loop(Peers, CurrentPeers)
      end, Config).

set_peer_roles_loop([], AccPeers) ->
    {ok, AccPeers};
set_peer_roles_loop([{Peer, Role} | Rest], AccPeers) ->
    case maps:find(Peer, AccPeers) of
        {ok, Info} ->
            NewInfo = Info#{role => Role},
            set_peer_roles_loop(Rest, AccPeers#{Peer => NewInfo});
        error ->
            {error, {not_member, Peer}}
    end.

update_peers(Fun, #config{peers = OldPeers} = Config) ->
    case Fun(OldPeers) of
        {ok, NewPeers} ->
            NewVoters = peers_voters(NewPeers),
            case NewVoters of
                [] ->
                    {error, no_voters_left};
                _ ->
                    {ok, Config#config{peers = NewPeers}}
            end;
        {error, _} = Error ->
            Error
    end.

peers_of_role(Role, Peers) ->
    maps:keys(maps:filter(
                fun (_, #{role := PeerRole}) ->
                        PeerRole =:= Role
                end, Peers)).

peers_voters(Peers) ->
    peers_of_role(voter, Peers).

peers_replicas(Peers) ->
    peers_of_role(replica, Peers).

peers_quorum(Peers) ->
    Voters = peers_voters(Peers),
    {majority, sets:from_list(Voters)}.

needs_transition(NewConfig, OldConfig) ->
    NewVoters = get_voters(NewConfig),
    OldVoters = get_voters(OldConfig),
    do_needs_transition(NewVoters, OldVoters).

do_needs_transition(NewVoters, OldVoters) ->
    Added = NewVoters -- OldVoters,
    Removed = OldVoters -- NewVoters,
    NumChanges = length(Added) + length(Removed),

    %% If there's no more than one change, then all quorums in the new config
    %% interesect all quorums in the old config. So we don't need to go
    %% through a transitional configuration.
    NumChanges > 1.

-ifdef(TEST).
needs_transition_test() ->
    ?assertEqual(false, do_needs_transition([a, b, c], [a, b, c, d])),
    ?assertEqual(false, do_needs_transition([a, b, c], [a, b])),
    ?assertEqual(false, do_needs_transition([a, b, c], [c, a, d, b])),
    ?assertEqual(true, do_needs_transition([a, b, c], [a, b, c, d, e])),
    ?assertEqual(true, do_needs_transition([a, b, c], [a, b, d])),
    ?assertEqual(true, do_needs_transition([a, b, c], [c, a, e, d, b])).
-endif.

peer_info(Role) ->
    peer_info(chronicle_utils:random_uuid(), Role).

peer_info(Id, Role) ->
    #{id => Id, role => Role}.

get_peer_id(Peer, #config{peers = Peers, old_peers = OldPeers} = Config) ->
    MaybePeerInfo =
        case maps:find(Peer, Peers) of
            {ok, _} = Ok ->
                Ok;
            error ->
                case OldPeers =:= undefined of
                    true ->
                        error;
                    false ->
                        maps:find(Peer, OldPeers)
                end
        end,

    case MaybePeerInfo of
        error ->
            error({unknown_peer, Peer, Config});
        {ok, #{id := PeerId}} ->
            PeerId
    end.
