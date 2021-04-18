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

-export([get_config/1, get_cluster_info/1, query/2, check_quorum/1]).
-export([cas_config/3]).
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

cas_config(Config, CasRevision, Timeout) ->
    chronicle_rsm:command(?NAME, {cas_config, Config, CasRevision}, Timeout).

%% callbacks
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

handle_config(ConfigEntry, Revision, _StateRevision,
              #state{current_request = CurrentRequest,
                     pending_requests = PendingRequests} = State, Data) ->
    #log_entry{value = Config} = ConfigEntry,
    NewState = State#state{current_request = undefined,
                           pending_requests = [],
                           config = ConfigEntry},

    ConfigRequestId = chronicle_config:get_request_id(Config),
    Replies0 =
        case CurrentRequest of
            undefined ->
                [];
            {Id, _} ->
                Reply =
                    case Id =:= ConfigRequestId of
                        true ->
                            {ok, Revision};
                        false ->
                            {error, {cas_failed, Revision}}
                    end,
                [{Id, Reply}]
        end,

    Replies =
        Replies0 ++ [{Id, {error, {cas_failed, Revision}}} ||
                        {Id, _} <- PendingRequests],

    {ok, NewState, Data, Replies}.

apply_snapshot(_Revision, State, _OldRevision, _OldState, Data) ->
    maybe_submit_current_request(State, Data),
    {ok, Data}.

handle_query(get_config, _, #state{config = ConfigEntry}, Data) ->
    Config = ConfigEntry#log_entry.value,
    Revision = chronicle_utils:log_entry_revision(ConfigEntry),
    {reply, {ok, Config, Revision}, Data};
handle_query(get_cluster_info, {HistoryId, Seqno}, State, Data) ->
    #state{config = ConfigEntry} = State,
    Info = #{history_id => HistoryId,
             committed_seqno => Seqno,
             config => ConfigEntry},
    {reply, Info, Data}.

apply_command(Id, {cas_config, _Config, CasRevision} = Request,
              _, _,
              #state{current_request = CurrentRequest,
                     pending_requests = PendingRequests,
                     config = ConfigEntry} = State,
              Data) ->
    HaveRevision = chronicle_utils:log_entry_revision(ConfigEntry),
    case CasRevision =:= HaveRevision of
        true ->
            NewState =
                case CurrentRequest of
                    undefined ->
                        maybe_submit_request(Id, Request, Data),
                        State#state{current_request = {Id, Request}};
                    _ ->
                        NewPendingRequests = [{Id, Request} | PendingRequests],
                        State#state{pending_requests = NewPendingRequests}
                end,

            {noreply, NewState, Data};
        false ->
            {reply, {error, {cas_failed, HaveRevision}}, State, Data}
    end.

handle_info(Msg, _StateRevision, _State, Data) ->
    ?WARNING("Unexpected message: ~p", [Msg]),
    {noreply, Data}.

terminate(_Reason, _StateRevision, _State, _Data) ->
    ok.

%% internal
maybe_submit_current_request(State, Data) ->
    case State#state.current_request of
        undefined ->
            ok;
        {Id, Request} ->
            maybe_submit_request(Id, Request, Data)
    end.

maybe_submit_request(Id, Request, Data) ->
    case Data#data.is_leader of
        true ->
            submit_request(Id, Request);
        false ->
            ok
    end.

submit_request(Id, {cas_config, Config, CasRevision}) ->
    NewConfig = chronicle_config:set_request_id(Id, Config),
    chronicle_server:cas_config(NewConfig, CasRevision).
