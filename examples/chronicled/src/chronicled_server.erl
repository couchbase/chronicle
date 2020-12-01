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
-module(chronicled_server).

-include_lib("kernel/include/logger.hrl").

-export([start/0, stop/0]).
-export([init/2, content_types_provided/2, json_api/2,
         content_types_accepted/2, allowed_methods/2, resource_exists/2,
         allow_missing_post/2, delete_resource/2]).

-record(state, {domain, op}).

start() ->
    Port = get_port(),
    Opts = get_options(),

    ?LOG_DEBUG("starting cowboy rest server: ~p", [Port]),
    ?LOG_DEBUG("cookie: ~p", [erlang:get_cookie()]),
    ?LOG_DEBUG("node: ~p, nodes: ~p", [node(), nodes()]),

    {ok, _} = cowboy:start_clear(http, [{port, Port}], Opts),
    ignore.

stop() ->
    ok = cowboy:stop_listener(http).

get_port() ->
    8080 + application:get_env(chronicled, instance, 0).

get_options() ->
    Dispatch =
        cowboy_router:compile(
          [
           {'_', [
                  {"/config/info", ?MODULE, #state{domain=config,
                                                   op=info}},
                  {"/config/addnode", ?MODULE, #state{domain=config,
                                                      op={addnode, voter}}},
                  {"/config/addvoter", ?MODULE, #state{domain=config,
                                                       op={addnode, voter}}},
                  {"/config/addreplica", ?MODULE,
                   #state{domain=config, op={addnode, replica}}},
                  {"/config/removenode", ?MODULE, #state{domain=config,
                                                         op=removenode}},
                  {"/config/provision", ?MODULE, #state{domain=config,
                                                        op=provision}},
                  {"/node/wipe", ?MODULE, #state{domain=node, op=wipe}},
                  {"/node/status", ?MODULE, #state{domain=node, op=status}},
                  {"/kv/:key", ?MODULE, #state{domain=kv}}
                 ]}
          ]),
    #{env => #{dispatch => Dispatch}}.

init(Req, State) ->
    case State of
        #state{domain=kv} ->
            Method = method_to_atom(cowboy_req:method(Req)),
            ?LOG_DEBUG("Method: ~p", [Method]),
            {cowboy_rest, Req, State#state{op = Method}};
        #state{domain=config} ->
            {ok, config_api(Req, State), State};
        #state{domain=node} ->
            {ok, node_api(Req, State), State}
    end.

allowed_methods(Req, State) ->
    Methods = [<<"GET">>, <<"PUT">>, <<"POST">>, <<"DELETE">>],
    {Methods, Req, State}.

content_types_provided(Req, State) ->
    {[
      {<<"application/json">>, json_api}
     ], Req, State}.

content_types_accepted(Req, State) ->
    {[
      {<<"application/json">>, json_api}
     ], Req, State}.

resource_exists(Req, State) ->
    case get_value(Req) of
        {ok, Value} ->
            ?LOG_DEBUG("Resource exists: ~p", [Value]),
            {true, Req, State};
        _ ->
            {false, Req, State}
    end.

delete_resource(Req, State) ->
    Key = cowboy_req:binding(key, Req),
    ?LOG_DEBUG("delete_resource called for key: ~p", [Key]),
    Result = delete_value(Req, any),
    {Result, Req, State}.

allow_missing_post(Req, State) ->
    {false, Req, State}.

json_api(Req, #state{domain=kv, op=get}=State) ->
    case get_value(Req) of
        {ok, {Val, {HistoryId, Seqno} = Rev}} ->
            ?LOG_DEBUG("Rev: ~p", [Rev]),
            R = {[{<<"rev">>,
                   {[{<<"history_id">>, HistoryId},
                     {<<"seqno">>, Seqno}]}
                  },
                  {<<"value">>, jiffy:decode(Val)}]},
            {jiffy:encode(R), Req, State};
        _ ->
            {<<"">>, Req, State}
    end;
json_api(Req, #state{domain=kv, op=post}=State) ->
    {Result, Req1} = set_value(Req, any),
    {Result, Req1, State};
json_api(Req, #state{domain=kv, op=put}=State) ->
    {Result, Req1} = set_value(Req, any),
    {Result, Req1, State}.

config_api(Req, #state{domain=config, op=info}) ->
    {ok, Peers} = chronicle:get_peers(),
    reply_json(200, Peers, Req);
config_api(Req, #state{domain=config, op={addnode, Type}}) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    ?LOG_DEBUG("read content: ~p", [Body]),
    {Result, Message} = case parse_nodes(Body) of
                            Nodes when is_list(Nodes) ->
                                add_nodes(Nodes, Type);
                            {error, Msg} ->
                                {false, Msg}
                        end,
    Status = case Result of true -> 200; _ -> 400 end,
    reply_json(Status, Message, Req1);
config_api(Req, #state{domain=config, op=removenode}) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    ?LOG_DEBUG("read content: ~p", [Body]),
    {Result, Message} = case parse_nodes(Body) of
                            Nodes when is_list(Nodes) ->
                                remove_nodes(Nodes);
                            {error, Msg} ->
                                {false, Msg}
                        end,
    Status = case Result of true -> 200; _ -> 400 end,
    reply_json(Status, Message, Req1);
config_api(Req, #state{domain=config, op=provision}) ->
    Machines = [{kv, chronicle_kv, []}],
    case chronicle:provision(Machines) of
        ok ->
            ok;
        {error, provisioned} ->
            ok
    end,
    reply_json(200, <<"provisioned">>, Req).

node_api(Req, #state{domain=node, op=wipe}) ->
    ok = chronicle:wipe(),
    reply_json(200, <<"ok">>, Req);
node_api(Req, #state{domain=node, op=status}) ->
    Status = #{leader => get_leader_info()},
    reply_json(200, Status, Req).

%% internal module functions
reply_json(Status, Response, Req) ->
    cowboy_req:reply(Status,
                     #{<<"content-type">> => <<"application/json">>},
                     jiffy:encode(Response), Req).

method_to_atom(<<"GET">>) ->
    get;
method_to_atom(<<"PUT">>) ->
    put;
method_to_atom(<<"POST">>) ->
    post;
method_to_atom(<<"DELETE">>) ->
    delete.

get_value(Req) ->
    case cowboy_req:binding(key, Req) of
        undefined ->
            not_found;
        BinKey ->
            Key = binary_to_list(BinKey),
            Consistency = get_consistency(Req),
            do_get_value(Key, Consistency)
    end.

get_consistency(Req) ->
    #{consistency := Consistency} =
        cowboy_req:match_qs(
          [{consistency, [fun consistency_constraint/2], local}],
          Req),
    Consistency.

consistency_constraint(forward, Value) ->
    case Value of
        <<"local">> ->
            {ok, local};
        <<"leader">> ->
            {ok, leader};
        <<"quorum">> ->
            {ok, quorum};
        _ ->
            {error, bad_consistency}
    end;
consistency_constraint(format_error, _Error) ->
    "consistency must be one of 'local', 'leader' or 'quorum'".

do_get_value(Key, Consistency) ->
    ?LOG_DEBUG("key: ~p", [Key]),
    Result = chronicle_kv:get(kv, Key, #{read_consistency => Consistency}),
    case Result of
        {ok, Value} ->
            {ok, Value};
        _ ->
            not_found
    end.

set_value(Req, ExpectedRevision) ->
    BinKey = cowboy_req:binding(key, Req),
    Key = binary_to_list(BinKey),
    ?LOG_DEBUG("key: ~p", [Key]),
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    ?LOG_DEBUG("read content: ~p", [Body]),
    try jiffy:decode(Body) of
        _ ->
            chronicle_kv:set(kv, Key, Body, ExpectedRevision),
            {true, Req1}
    catch _:_ ->
            ?LOG_DEBUG("body not json: ~p", [Body]),
            {false, Req}
    end.

delete_value(Req, ExpectedRevision) ->
    BinKey = cowboy_req:binding(key, Req),
    Key = binary_to_list(BinKey),
    ?LOG_DEBUG("deleting key: ~p", [Key]),
    chronicle_kv:delete(kv, Key, ExpectedRevision),
    true.

parse_nodes(Body) ->
    try jiffy:decode(Body) of
        Nodes when is_list(Nodes) ->
            [binary_to_atom(N, utf8) || N <- Nodes];
        Node when is_binary(Node) ->
            [binary_to_atom(Node, utf8)];
        _ ->
            {error, "invalid JSON list of nodes"}
    catch _:_ ->
            {error, "body not json"}
    end.

add_nodes(Nodes, Type) ->
    CurrentNodes = get_nodes_of_type(Type),
    ToAdd = [N || N <- Nodes, not(lists:member(N, CurrentNodes))],
    case ToAdd of
        [] ->
            {false, <<"nodes already added">>};
        _ ->
            case call_nodes(ToAdd, prepare_join) of
                true ->
                    ?LOG_DEBUG("nodes ~p", [nodes()]),
                    ok = do_add_nodes(ToAdd, Type),
                    case call_nodes(ToAdd, join_cluster) of
                        true ->
                            {true, <<"nodes added">>};
                        false ->
                            {false, <<"join_cluster failed">>}
                    end;
                false ->
                    {false, <<"prepare_join failed">>}
            end
    end.

call_nodes(Nodes, Call) ->
    ClusterInfo = chronicle:get_cluster_info(),
    {Results, BadNodes} = rpc:multicall(Nodes,
                                        chronicle, Call, [ClusterInfo]),
    BadResults =
        [{N, R} || {N, R} <- Results, R =/= ok] ++
        [{N, down} || N <- BadNodes],

    case BadResults of
        [] ->
            true;
        _ ->
            ?LOG_DEBUG("~p failed on some nodes:~n~p", [Call, BadResults]),
            false
    end.

remove_nodes(Nodes) ->
    PresentNodes = get_present_nodes(),
    ToRemove = [N || N <- Nodes, lists:member(N, PresentNodes)],
    case ToRemove of
        Nodes ->
            Result = chronicle:remove_peers(Nodes),
            ?LOG_DEBUG("Result of voter addition: ~p", [Result]),
            case Result of
                ok ->
                    {true, <<"nodes removed">>};
                _ ->
                    {false, <<"nodes could not be removed">>}
            end;
        _ ->
            {false, <<"some nodes not part of cluster">>}
    end.

get_present_nodes() ->
    {ok, #{voters := Voters, replicas := Replicas}} = chronicle:get_peers(),
    Voters ++ Replicas.

get_nodes_of_type(Type) ->
    {ok, Nodes} =
        case Type of
            voter ->
                chronicle:get_voters();
            replica ->
                chronicle:get_replicas()
        end,

    Nodes.

do_add_nodes(Nodes, Type) ->
    case Type of
        voter ->
            chronicle:add_voters(Nodes);
        replica ->
            chronicle:add_replicas(Nodes)
    end.

get_leader_info() ->
    case chronicle_leader:get_leader() of
        {Leader, {HistoryId, {Term, _}}} ->
            #{node => Leader,
              history_id => HistoryId,
              term => Term};
        no_leader ->
            null
    end.
