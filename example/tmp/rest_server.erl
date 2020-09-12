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

-module(rest_server).

-include("chronicle.hrl").

-export([init/2, content_types_provided/2, json_api/2, get_options/0,
         content_types_accepted/2, allowed_methods/2, resource_exists/2]).

-record(state, {domain, op}).

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
                  {"/kv/get/:key", ?MODULE, #state{domain=kv, op=get}},
                  {"/kv/set/:key", ?MODULE, #state{domain=kv, op=set}}
                 ]}
          ]),
    #{env => #{dispatch => Dispatch}}.

init(Req, State) ->
    case State of
        #state{domain=kv} ->
            {cowboy_rest, Req, State};
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
        {ok, _} ->
            {true, Req, State};
        _ ->
            {false, Req, State}
    end.

json_api(Req, #state{domain=kv, op=get}=State) ->
    case cowboy_req:method(Req) of
        <<"GET">> ->
            case get_value(Req) of
                {ok, {Val, {HistoryId, Seqno} = Rev}} ->
                    ?DEBUG("Rev: ~p", [Rev]),
                    R = {[{<<"rev">>,
                           {[{<<"history_id">>, HistoryId},
                             {<<"seqno">>, Seqno}]}
                          },
                          {<<"value">>, jiffy:decode(Val)}]},
                    {jiffy:encode(R), Req, State};
                _ ->
                    {<<"">>, Req, State}
            end;
        _ ->
            ?DEBUG("get must be a GET"),
            {<<"get must be a GET">>, Req, State}
    end;
json_api(Req, #state{domain=kv, op=set}=State) ->
    case cowboy_req:method(Req) of
        <<"POST">> ->
            {Result, Req1} = set_value(Req),
            {Result, Req1, State};
        <<"PUT">> ->
            {Result, Req1} = set_value(Req),
            {Result, Req1, State};
        _ ->
            ?DEBUG("set must be a put or a post"),
            {"set must be a put or a post", Req, State}
    end.

config_api(Req, #state{domain=config, op=info}) ->
    {ok, Vs} = chronicle:get_voters(),
    Voters = [atom_to_binary(V, utf8) || V <- Vs],
    R = {[{<<"voters">>, Voters}]},
    cowboy_req:reply(200, #{
                       <<"content-type">> => <<"application/json">>
                      }, jiffy:encode(R), Req);
config_api(Req, #state{domain=config, op=addnode}) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    ?DEBUG("read content: ~p", [Body]),
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
    ?DEBUG("read content: ~p", [Body]),
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

get_value(Req) ->
    case cowboy_req:binding(key, Req) of
        undefined ->
            not_found;
        BinKey ->
            Key = binary_to_list(BinKey),
            do_get_value(Key)
    end.

do_get_value(Key) ->
    ?DEBUG("key: ~p", [Key]),
    Result = chronicle_kv:get(kv, Key),
    case Result of
        {ok, Value} ->
            {ok, Value};
        _ ->
            not_found
    end.

set_value(Req) ->
    BinKey = cowboy_req:binding(key, Req),
    Key = binary_to_list(BinKey),
    ?DEBUG("key: ~p", [Key]),
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    ?DEBUG("read content: ~p", [Body]),
    try jiffy:decode(Body) of
        _ ->
            chronicle_kv:set(kv, Key, Body),
            {true, Req1}
    catch _:_ ->
            ?DEBUG("body not json: ~p", [Body]),
            {false, Req}
    end.

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
                    ?DEBUG("nodes ~p", [nodes()]),
                    Result = chronicle:add_voters(ToAdd),
                    ?DEBUG("Result of voter addition: ~p", [Result]),
                    Message = case Result of
                                  ok ->
                                      <<"nodes added">>;
                                  _ ->
                                      <<"nodes could not be added">>
                              end,
                    {Result, Message};
                CantConnect ->
                    ?DEBUG("Can't connect to nodes: ~p", [CantConnect]),
                    {false, <<"can't connect to some nodes to be added">>}
            end
    end.

remove_nodes(Nodes) ->
    {ok, Vs} = chronicle:get_voters(),
    ToRemove = [N || N <- Nodes, lists:member(N, Vs)],
    case ToRemove of
        Nodes ->
            Result = chronicle:remove_voters(Nodes),
            ?DEBUG("Result of voter addition: ~p", [Result]),
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
