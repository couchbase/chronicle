-module(chronicle_dump).

-include("chronicle.hrl").

-export([main/1]).

usage() ->
    erlang:halt(1).

main(_Args) ->
    usage().
