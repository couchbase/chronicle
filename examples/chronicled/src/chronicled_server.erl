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
                                                      op=addnode}},
                  {"/config/removenode", ?MODULE, #state{domain=config,
                                                         op=removenode}},
                  {"/config/provision", ?MODULE, #state{domain=config,
                                                        op=provision}},
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
        _ ->
            {ok, config_api(Req, State), State}
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
    {ok, Vs} = chronicle:get_voters(),
    Voters = [atom_to_binary(V, utf8) || V <- Vs],
    R = {[{<<"voters">>, Voters}]},
    cowboy_req:reply(200, #{
                       <<"content-type">> => <<"application/json">>
                      }, jiffy:encode(R), Req);
config_api(Req, #state{domain=config, op=addnode}) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    ?LOG_DEBUG("read content: ~p", [Body]),
    {Result, Message} = case parse_nodes(Body) of
                            Nodes when is_list(Nodes) ->
                                add_nodes(Nodes);
                            {error, Msg} ->
                                {false, Msg}
                        end,
    Status = case Result of true -> 200; _ -> 400 end,
    cowboy_req:reply(Status, #{
                       <<"content-type">> => <<"application/json">>
                      }, Message, Req1);
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
    cowboy_req:reply(Status, #{
                       <<"content-type">> => <<"application/json">>
                      }, Message, Req1);
config_api(Req, #state{domain=config, op=provision}) ->
    Machines = [{kv, chronicle_kv, []}],
    case chronicle:provision(Machines) of
        ok ->
            ok;
        {error, already_provisioned} ->
            ok
    end,
    cowboy_req:reply(200, #{
                       <<"content-type">> => <<"application/json">>
                      }, <<"provisioned">>, Req).

%% internal module functions

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
            do_get_value(Key)
    end.

do_get_value(Key) ->
    ?LOG_DEBUG("key: ~p", [Key]),
    Result = chronicle_kv:get(kv, Key),
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

add_nodes(Nodes) ->
    {ok, Vs} = chronicle:get_voters(),
    ToAdd = [N || N <- Nodes, not(lists:member(N, Vs))],
    case ToAdd of
        [] ->
            {false, <<"nodes already added">>};
        _ ->
            case [N || N <- ToAdd, net_kernel:connect_node(N) =:= false] of
                [] ->
                    ?LOG_DEBUG("nodes ~p", [nodes()]),
                    Result = chronicle:add_voters(ToAdd),
                    ?LOG_DEBUG("Result of voter addition: ~p", [Result]),
                    case Result of
                        ok ->
                            {true, <<"nodes added">>};
                        _ ->
                            {false, <<"nodes could not be added">>}
                    end;
                CantConnect ->
                    ?LOG_DEBUG("Can't connect to nodes: ~p", [CantConnect]),
                    {false, <<"can't connect to some nodes to be added">>}
            end
    end.

remove_nodes(Nodes) ->
    {ok, Vs} = chronicle:get_voters(),
    ToRemove = [N || N <- Nodes, lists:member(N, Vs)],
    case ToRemove of
        Nodes ->
            Result = chronicle:remove_peers(Nodes),
            ?LOG_DEBUG("Result of voter addition: ~p", [Result]),
            Message = case Result of
                          ok ->
                              <<"nodes removed">>;
                          _ ->
                              <<"nodes could not be removed">>
                      end,
            {Result, Message};
        _ ->
            {false, <<"some nodes not part of cluster">>}
    end.
