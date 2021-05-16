%% @author Couchbase <info@couchbase.com>
%% @copyright 2021 Couchbase, Inc.
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
-module(chronicle_config_rsm).

-include("chronicle.hrl").

-export([get_config/1, get_cluster_info/1, get_peers/1, check_quorum/1]).
-export([acquire_lock/1, remove_peers/3, add_peers/3, set_peer_roles/3,
         set_settings/2, replace_settings/2, unset_settings/2]).

-export([format_state/1]).
-export([specs/2, init/2, post_init/3, state_enter/4,
         handle_config/5, apply_snapshot/5, handle_query/4,
         apply_command/6, handle_info/4, terminate/4]).

-define(NAME, ?MODULE).

-record(state, { current_request,
                 pending_requests,

                 config }).
-record(data, { is_leader = false }).

get_config(Timeout) ->
    query(get_config, Timeout).

get_cluster_info(Timeout) ->
    query(get_cluster_info, Timeout).

get_peers(Timeout) ->
    query(get_peers, Timeout).

query(Query, Timeout) ->
    TRef = chronicle_utils:start_timeout(Timeout),
    ok = chronicle_rsm:sync(?NAME, TRef),
    chronicle_rsm:query(?NAME, Query, TRef).

check_quorum(Timeout) ->
    try chronicle_rsm:sync(?NAME, Timeout) of
        ok ->
            true
    catch
        exit:timeout ->
            {false, timeout}
    end.

acquire_lock(Timeout) ->
    command(acquire_lock, Timeout).

remove_peers(Lock, Peers, Timeout) ->
    command({remove_peers, Lock, Peers}, Timeout).

add_peers(Lock, Peers, Timeout) ->
    command({add_peers, Lock, Peers}, Timeout).

set_peer_roles(Lock, Peers, Timeout) ->
    command({set_peer_roles, Lock, Peers}, Timeout).

set_settings(Settings, Timeout) ->
    command({set_settings, Settings}, Timeout).

replace_settings(Settings, Timeout) ->
    command({replace_settings, Settings}, Timeout).

unset_settings(Names, Timeout) ->
    command({unset_settings, Names}, Timeout).

%% callbacks
format_state(#state{current_request = CurrentRequest,
                    pending_requests = PendingRequests,
                    config = ConfigEntry}) ->
    #log_entry{history_id = HistoryId,
               term = Term,
               seqno = Seqno,
               value = Config} = ConfigEntry,

    [{"Current request", CurrentRequest},
     {"Pending requests", PendingRequests},
     {"Config",
      [{"History id", HistoryId},
       {"Term", Term},
       {"Seqno", Seqno},
       {"Value", chronicle_config:format_config(Config)}]}].

specs(_Name, _Args) ->
    [].

init(?NAME, []) ->
    {ok, #state{current_request = undefined, pending_requests = []}, #data{}}.

post_init(_, _, Data) ->
    {ok, Data}.

state_enter(leader, _Revision, State, Data) ->
    NewData = Data#data{is_leader = true},
    maybe_submit_current_request(State, NewData),
    {ok, NewData};
state_enter(_, _Revision, _State, Data) ->
    {ok, Data#data{is_leader = false}}.

handle_config(ConfigEntry, _Revision, _StateRevision,
              #state{current_request = CurrentRequest,
                     pending_requests = PendingRequests} = State, Data) ->
    #log_entry{value = Config} = ConfigEntry,
    ConfigRequestId = chronicle_config:get_request_id(Config),
    NewState0 = State#state{current_request = undefined,
                            config = ConfigEntry},

    case CurrentRequest of
        undefined ->
            {ok, NewState0, Data, []};
        {Id, _, Reply, _} when Id =:= ConfigRequestId ->
            loop_pending_requests([{Id, Reply}], NewState0, Data);
        {Id, Request, _, _} ->
            NewState1 = NewState0#state{
                          pending_requests = [{Id, Request} | PendingRequests]},
            loop_pending_requests([], NewState1, Data)
    end.

loop_pending_requests(Replies,
                      #state{pending_requests = Requests,
                             config = ConfigEntry} = State, Data) ->
    #log_entry{value = Config} = ConfigEntry,
    {SubmitRequest, FinalReplies, NewPendingRequests} =
        do_loop_pending_requests(Replies, Requests, Config),

    NewState = State#state{pending_requests = NewPendingRequests},
    FinalState =
        case SubmitRequest of
            no_request ->
                NewState;
            {Id, _Request, _Reply, NewConfig} ->
                maybe_submit_config(Id, NewConfig, NewState, Data),
                NewState#state{current_request = SubmitRequest}
        end,

    {ok, FinalState, Data, FinalReplies}.

do_loop_pending_requests(Replies, [], _) ->
    {no_request, Replies, []};
do_loop_pending_requests(Replies, [{Id, Request} | Requests], Config) ->
    case do_apply_command(Request, Config) of
        {accept, Reply, NewConfig} ->
            {{Id, Request, Reply, NewConfig}, Replies, Requests};
        {reject, Reply} ->
            NewReplies = [{Id, Reply} | Replies],
            do_loop_pending_requests(NewReplies, Requests, Config)
    end.

apply_snapshot(_Revision, State, _OldRevision, _OldState, Data) ->
    maybe_submit_current_request(State, Data),
    {ok, Data}.

handle_query(get_config, _, #state{config = ConfigEntry}, Data) ->
    Config = ConfigEntry#log_entry.value,
    Revision = chronicle_utils:log_entry_revision(ConfigEntry),
    {reply, {ok, Config, Revision}, Data};
handle_query(get_peers, _, #state{config = ConfigEntry}, Data) ->
    Config = ConfigEntry#log_entry.value,
    Voters = chronicle_config:get_voters(Config),
    Replicas = chronicle_config:get_replicas(Config),
    {reply, #{voters => Voters, replicas => Replicas}, Data};
handle_query(get_cluster_info, {HistoryId, Seqno}, State, Data) ->
    #state{config = ConfigEntry} = State,
    Info = #{history_id => HistoryId,
             committed_seqno => Seqno,
             config => ConfigEntry},
    {reply, Info, Data};
handle_query(_, _, _, Data) ->
    {reply, {error, unknown_query}, Data}.

apply_command(Id, Request, _, _,
              #state{current_request = CurrentRequest,
                     pending_requests = PendingRequests,
                     config = ConfigEntry} = State, Data) ->
    case CurrentRequest =:= undefined of
        true ->
            case do_apply_command(Request, ConfigEntry#log_entry.value) of
                {accept, Reply, NewConfig} ->
                    NewState =
                        State#state{
                          current_request = {Id, Request, Reply, NewConfig}},
                    maybe_submit_config(Id, NewConfig, State, Data),
                    {noreply, NewState, Data};
                {reject, Reply} ->
                    {reply, Reply, State, Data}
            end;
        false ->
            NewPendingRequests = [{Id, Request} | PendingRequests],
            NewState = State#state{pending_requests = NewPendingRequests},
            {noreply, NewState, Data}
    end.

do_apply_command(acquire_lock, Config) ->
    Lock = chronicle_utils:random_uuid(),
    NewConfig = chronicle_config:set_lock(Lock, Config),
    {accept, {ok, Lock}, NewConfig};
do_apply_command({remove_peers, Lock, Peers}, Config) ->
    update(fun handle_remove_peers/3, [Lock, Peers, Config]);
do_apply_command({add_peers, Lock, Peers}, Config) ->
    update(fun handle_add_peers/3, [Lock, Peers, Config]);
do_apply_command({set_peer_roles, Lock, Peers}, Config) ->
    update(fun handle_set_peer_roles/3, [Lock, Peers, Config]);
do_apply_command({set_settings, Settings}, Config) ->
    update(fun handle_set_settings/2, [Settings, Config]);
do_apply_command({replace_settings, Settings}, Config) ->
    update(fun handle_replace_settings/2, [Settings, Config]);
do_apply_command({unset_settings, Names}, Config) ->
    update(fun handle_unset_settings/2, [Names, Config]);
do_apply_command(_, _Config) ->
    {reject, {error, unknown_command}}.

handle_info(Msg, _StateRevision, _State, Data) ->
    ?WARNING("Unexpected message: ~p", [Msg]),
    {noreply, Data}.

terminate(_Reason, _StateRevision, _State, _Data) ->
    ok.

%% internal
command(Command, Timeout) ->
    case chronicle_rsm:command(?NAME, Command, Timeout) of
        {raise, Reason} ->
            error(Reason);
        Other ->
            Other
    end.

maybe_submit_config(Id, NewConfig, State, Data) ->
    case Data#data.is_leader of
        true ->
            submit_config(Id, NewConfig, State);
        false ->
            ok
    end.

submit_config(Id, NewConfig, #state{config = ConfigEntry}) ->
    Revision = chronicle_utils:log_entry_revision(ConfigEntry),
    FinalConfig = chronicle_config:set_request_id(Id, NewConfig),
    chronicle_server:cas_config(FinalConfig, Revision).

maybe_submit_current_request(State, Data) ->
    case State#state.current_request of
        undefined ->
            ok;
        {Id, _Request, _Reply, NewConfig} ->
            maybe_submit_config(Id, NewConfig, State, Data)
    end.

handle_remove_peers(Lock, Peers, Config) ->
    check_lock(Lock, Config),
    check_peers(Peers),
    chronicle_config:remove_peers(Peers, Config).

handle_add_peers(Lock, Peers, Config) ->
    check_lock(Lock, Config),
    check_peers_and_roles(Peers),
    chronicle_config:add_peers(Peers, Config).

handle_set_peer_roles(Lock, Peers, Config) ->
    check_lock(Lock, Config),
    check_peers_and_roles(Peers),
    chronicle_config:set_peer_roles(Peers, Config).

handle_set_settings(Settings, Config) ->
    check_settings(Settings),
    update_settings(
      fun (OldSettings) ->
              maps:merge(OldSettings, Settings)
      end, Config).

handle_replace_settings(Settings, Config) ->
    check_settings(Settings),
    update_settings(
      fun (_) ->
              Settings
      end, Config).

handle_unset_settings(Names, Config) ->
    check_settings_names(Names),
    update_settings(
      fun (Settings) ->
              maps:without(Names, Settings)
      end, Config).

check_peers(Peers) ->
    is_list(Peers) orelse raise(bad_peers),
    lists:foreach(
      fun (Peer) ->
              case is_atom(Peer) of
                  true ->
                      ok;
                  false ->
                      raise(bad_peers)
              end
      end, Peers).

check_peers_and_roles(PeerRoles) ->
    is_list(PeerRoles) orelse raise(bad_peers),

    Peers = [Peer || {Peer, _} <- PeerRoles],
    check_peers(Peers),
    case lists:usort(Peers) =:= lists:sort(Peers)
        andalso length(Peers) =:= length(PeerRoles) of
        true ->
            ok;
        false ->
            raise(bad_peers)
    end,

    lists:foreach(
      fun ({_Peer, Role}) ->
              case lists:member(Role, [replica, voter]) of
                  true ->
                      ok;
                  false ->
                      raise(bad_peers)
              end
      end, PeerRoles).

check_lock(Lock, Config) ->
    case Lock of
        unlocked ->
            ok;
        _ when is_binary(Lock) ->
            case chronicle_config:check_lock(Lock, Config) of
                ok ->
                    ok;
                {error, Error} ->
                    throw(Error)
            end;
        _ ->
            raise(bad_lock)
    end.

check_settings(Settings) ->
    is_map(Settings) orelse raise(bad_settings).

check_settings_names(Names) ->
    is_list(Names) orelse raise(bad_settings_names).

update(Fun, Args) ->
    try erlang:apply(Fun, Args) of
        {ok, NewConfig} ->
            {accept, ok, NewConfig};
        {error, _} = Error ->
            {reject, Error}
    catch
        throw:Error ->
            {reject,
             case Error of
                 {raise, _} ->
                     Error;
                 _ ->
                     {error, Error}
             end}
    end.

-spec raise(any()) -> no_return().
raise(Reason) ->
    throw({raise, Reason}).

update_settings(Fun, Config) ->
    NewSettings = Fun(chronicle_config:get_settings(Config)),
    {ok, chronicle_config:set_settings(NewSettings, Config)}.
