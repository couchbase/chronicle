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

-define(MAX_HISTORY_LOG_SIZE, 10).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([format_config/1]).
-export([is_config/1, is_stable/1]).
-export([transition/2, next_config/1]).
-export([init/3, reinit/3, branch/3]).
-export([get_request_id/1, set_request_id/2]).
-export([get_compat_version/1, set_compat_version/2]).
-export([set_lock/2, check_lock/2]).
-export([get_rsms/1, get_quorum/1]).
-export([get_peers/1, get_replicas/1, get_voters/1]).
-export([add_peers/2, remove_peers/2, set_peer_roles/2]).
-export([get_peer_ids/1]).
-export([get_peer_id/2, is_peer/3]).
-export([get_branch_opaque/1]).
-export([get_settings/1, set_settings/2]).
-export([is_compatible_revision/3]).

-export_type([peers/0]).

-type peers() :: #{chronicle:peer() =>
                       #{id := chronicle:peer_id(), role := chronicle:role()}}.

format_config(#config{request_id = Id,
                      lock = Lock,
                      peers = Peers,
                      old_peers = OldPeers,
                      state_machines = StateMachines,
                      settings = Settings,
                      branch = Branch,
                      history_log = HistoryLog}) ->
    [{"Request id", Id},
     {"Lock", Lock},
     {"Peers", format_peers(Peers)},
     {"Old peers", format_peers(OldPeers)},
     {"State machines", format_state_machines(StateMachines)},
     {"Branch", format_branch(Branch)},
     {"History log", HistoryLog},
     {"Settings", Settings}].

format_peers(undefined) ->
    undefined;
format_peers(Peers) ->
    lists:map(
      fun ({Peer, #{id := PeerId, role := Role}}) ->
              {Peer, [{"Id", PeerId}, {"Role", Role}]}
      end, maps:to_list(Peers)).

format_state_machines(StateMachines) ->
    lists:map(
      fun ({Name, #rsm_config{module = Module, args = Args}}) ->
              {Name, [{"Module", Module},
                      {"Args", chronicle_dump:raw(Args)}]}
      end, maps:to_list(StateMachines)).

format_branch(undefined) ->
    undefined;
format_branch(#branch{history_id = HistoryId,
                      old_history_id = OldHistoryId,
                      coordinator = Coordinator,
                      peers = Peers,
                      opaque = Opaque}) ->
    [{"History id", HistoryId},
     {"Previous history id", OldHistoryId},
     {"Coordinator", Coordinator},
     {"Peers", Peers},
     {"Opaque", Opaque}].

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

init(HistoryId, Peer, Machines0) ->
    Machines = [{chronicle_config_rsm, chronicle_config_rsm, []} | Machines0],
    MachinesMap =
        maps:from_list(
          [{Name, #rsm_config{module = Module, args = Args}} ||
              {Name, Module, Args} <- Machines]),
    Peers = #{Peer => peer_info(voter)},
    #config{compat_version = ?COMPAT_VERSION,
            peers = Peers,
            state_machines = MachinesMap,
            history_log = add_history(HistoryId, ?NO_SEQNO, [])}.

reinit(NewPeer, OldPeer, #config{old_peers = undefined} = Config) ->
    {ok, PeerId} = get_peer_id(OldPeer, Config),
    reset(Config#config{peers = #{NewPeer => peer_info(PeerId, voter)}}).

branch(Seqno, #branch{peers = Peers,
                      history_id = HistoryId} = Branch, Config0) ->
    Config = reset(Config0),
    #config{history_log = HistoryLog} = Config,
    NewHistoryLog = add_history(HistoryId, Seqno, HistoryLog),
    %% TODO: figure out what to do with replicas
    PeerInfos = [{Peer, peer_info(Peer, Config, voter)} || Peer <- Peers],
    Config#config{peers = maps:from_list(PeerInfos),
                  old_peers = undefined,
                  branch = Branch,
                  history_log = NewHistoryLog}.

add_history(HistoryId, Seqno, []) ->
    [{HistoryId, Seqno}];
add_history(HistoryId, Seqno, [{_, PrevSeqno} | _] = HistoryLog) ->
    true = Seqno > PrevSeqno,
    NewHistoryLog = [{HistoryId, Seqno} | HistoryLog],
    lists:sublist(NewHistoryLog, ?MAX_HISTORY_LOG_SIZE).

get_compat_version(#config{compat_version = Version}) ->
    Version.

set_compat_version(Version, Config) ->
    reset(Config#config{compat_version = Version}).

set_lock(Lock, #config{} = Config) ->
    reset(Config#config{lock = Lock}).

check_lock(LockReq, #config{lock = Lock}) ->
    case LockReq =:= unlocked orelse LockReq =:= Lock of
        true ->
            ok;
        false ->
            {error, {lock_revoked, LockReq, Lock}}
    end.

get_request_id(#config{request_id = RequestId})->
    RequestId.

set_request_id(RequestId, #config{} = Config) ->
    Config#config{request_id = RequestId}.

reset_request_id(Config) ->
    set_request_id(undefined, Config).

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

get_peer_ids(#config{peers = Peers, old_peers = OldPeers}) ->
    case OldPeers =:= undefined of
        true ->
            get_peer_ids(#{}, Peers);
        false ->
            get_peer_ids(OldPeers, Peers)
    end.

get_peer_ids(OldPeers, Peers) ->
    Ids = maps:fold(
            fun (_, #{id := Id}, Acc) ->
                    [Id | Acc]
            end, [], maps:merge(OldPeers, Peers)),
    lists:usort(Ids).

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
        {ok, #{role := CurrentRole}} ->
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
                    {ok, reset(Config#config{peers = NewPeers})}
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

peer_info(Peer, Config, Role) ->
    case get_peer_id(Peer, Config) of
        {ok, PeerId} ->
            peer_info(PeerId, Role);
        not_peer ->
            exit({unknown_peer, Peer, Config})
    end.

peer_info(Id, Role) ->
    #{id => Id, role => Role}.

get_peer_id(Peer, #config{peers = Peers, old_peers = OldPeers}) ->
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
            not_peer;
        {ok, #{id := PeerId}} ->
            {ok, PeerId}
    end.

is_peer(Peer, PeerId, Config) ->
    case get_peer_id(Peer, Config) of
        {ok, FoundPeerId} ->
            FoundPeerId =:= PeerId;
        not_peer ->
            false
    end.

get_branch_opaque(#config{branch = Branch}) ->
    case Branch of
        undefined ->
            no_branch;
        #branch{opaque = Opaque}->
            {ok, Opaque}
    end.

get_settings(#config{settings = Settings}) ->
    Settings.

set_settings(NewSettings, Config) ->
    reset(Config#config{settings = NewSettings}).

reset_branch(#config{branch = Branch} = Config) ->
    case Branch of
        undefined ->
            Config;
        _ ->
            Config#config{branch = undefined}
    end.

reset(Config) ->
    reset_request_id(reset_branch(Config)).

is_compatible_revision({RevHistoryId, RevSeqno} = Revision, HighSeqno,
                       #config{history_log = HistoryLog}) ->
    case is_compatible_revision(RevHistoryId,
                                RevSeqno, HighSeqno, HistoryLog) of
        true ->
            true;
        false ->
            {false, {Revision, HighSeqno, HistoryLog}}
    end.

is_compatible_revision(RevHistoryId, RevSeqno, HighSeqno, HistoryLog) ->
    case RevSeqno > HighSeqno of
        true ->
            [{CurrentHistoryId, _} | RestHistoryLog] = HistoryLog,
            case CurrentHistoryId =:= RevHistoryId of
                true ->
                    %% Most common case.
                    true;
                false ->
                    %% If RevHistoryId is one of "sealed" history ids, then
                    %% the revision is incompatible.
                    not lists:keymember(RevHistoryId, 1, RestHistoryLog)
            end;
        false ->
            MatchingLog =
                lists:dropwhile(
                  fun ({_, LogSeqno}) ->
                          LogSeqno > RevSeqno
                  end, HistoryLog),

            case MatchingLog of
                [] ->
                    false;
                [{LogHistoryId, _}|_] ->
                    LogHistoryId =:= RevHistoryId
            end
    end.

-ifdef(TEST).
is_compatible_revision_test() ->
    HistoryLog = [{b, 15}, {a, 10}],
    HighSeqno = 20,

    true = is_compatible_revision(b, 25, HighSeqno, HistoryLog),
    true = is_compatible_revision(c, 25, HighSeqno, HistoryLog),
    false = is_compatible_revision(a, 25, HighSeqno, HistoryLog),
    true = is_compatible_revision(b, 17, HighSeqno, HistoryLog),
    true = is_compatible_revision(b, 15, HighSeqno, HistoryLog),
    false = is_compatible_revision(b, 14, HighSeqno, HistoryLog),
    false = is_compatible_revision(a, 15, HighSeqno, HistoryLog),
    true = is_compatible_revision(a, 13, HighSeqno, HistoryLog),
    true = is_compatible_revision(a, 10, HighSeqno, HistoryLog),
    false = is_compatible_revision(a, 9, HighSeqno, HistoryLog),
    false = is_compatible_revision(b, 9, HighSeqno, HistoryLog),
    false = is_compatible_revision(c, 9, HighSeqno, HistoryLog).
-endif.
