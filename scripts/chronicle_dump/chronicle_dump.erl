-module(chronicle_dump).

-include("chronicle.hrl").

-export([main/1]).
-export([raw/1]).

-define(fmt(Msg), ?fmt(Msg, [])).
-define(fmt(Fmt, Args), io:format(Fmt "~n", Args)).

-define(RAW_TAG, '$chronicle_dump_raw').

raw(Term) ->
    {?RAW_TAG, Term}.

dump_snapshots([]) ->
    ok;
dump_snapshots([Path]) ->
    dump_snapshot(Path);
dump_snapshots([Path|Rest]) ->
    dump_snapshot(Path),
    ?fmt("~n"),
    dump_snapshots(Rest).

dump_snapshot(Path) ->
    case chronicle_storage:read_rsm_snapshot(Path) of
        {ok, Snapshot} ->
            Props = chronicle_rsm:format_snapshot(Snapshot),
            ?fmt("Dumping '~s'~n", [Path]),
            dump_props(Props);
        {error, Error} ->
            ?fmt("Couldn't read snapshot '~s': ~w", [Path, Error])
    end.

dump_props(Props) ->
    dump_props("", Props).

dump_props(Indent, Props) when is_list(Props) ->
    lists:foreach(
      fun (Elem) ->
              dump_elem(Indent, Elem)
      end, Props).

dump_elem(Indent, Elem) ->
    case Elem of
        {_, _} = Pair ->
            dump_pair(Indent, Pair);
        _ ->
            case type(Elem) of
                {string, String} ->
                    dump_string(Indent, String);
                {_, Term} ->
                    dump_term(Indent, Term)
            end
    end.

dump_string(Indent, String) ->
    ?fmt("~s~s", [Indent, String]).

dump_term(Indent, Term) ->
    ?fmt("~s~250p", [Indent, Term]).

dump_pair(Indent, {Name0, Value0} = Pair) ->
    case type(Name0) of
        {string, Name} ->
            case type(Value0) of
                {string, Value} ->
                    ?fmt("~s~s: ~s", [Indent, Name, Value]);
                {term, Value} ->
                    case large(Value) of
                        true ->
                            ?fmt("~s~s:", [Indent, Name]),
                            dump_term(indent(Indent), Value);
                        false ->
                            ?fmt("~s~s: ~250p", [Indent, Name, Value])
                    end;
                {list, Value} ->
                    ?fmt("~s~s:", [Indent, Name]),
                    dump_props(indent(Indent), Value)
            end;
        _ ->
            dump_term(Indent, Pair)
    end.

indent(Indent) ->
    "    " ++ Indent.

large(Term) ->
    erts_debug:flat_size(Term) > 100.

stringlike(Term) ->
    io_lib:printable_list(Term) orelse is_binary(Term) orelse is_atom(Term).

type({?RAW_TAG, Term}) ->
    {term, Term};
type(Term) ->
    case stringlike(Term) of
        true ->
            {string, Term};
        false ->
            case is_list(Term) of
                true ->
                    {list, Term};
                false ->
                    {term, Term}
            end
    end.

-spec usage() -> no_return().
usage() ->
    erlang:halt(1).

main(Args) ->
    persistent_term:put(?CHRONICLE_LOAD_NIFS, false),

    case Args of
        ["snapshot" | Paths] ->
            dump_snapshots(Paths);
        _ ->
            usage()
    end.
