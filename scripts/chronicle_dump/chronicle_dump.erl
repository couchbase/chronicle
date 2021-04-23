-module(chronicle_dump).

-include("chronicle.hrl").

-export([main/1]).
-export([raw/1]).

-define(fmt(Msg), ?fmt(Msg, [])).
-define(fmt(Fmt, Args),
        begin
            io:format(Fmt, Args),
            io:nl()
        end).

-define(RAW_TAG, '$chronicle_dump_raw').

raw(Term) ->
    {?RAW_TAG, Term}.

parse_args(Args, Spec0) ->
    Spec = maps:from_list(
             lists:map(
               fun ({Opt, Type}) ->
                       {atom_to_list(Opt), Type}
               end, maps:to_list(Spec0))),
    parse_args_loop(Args, [], #{}, Spec).

parse_args_loop([], AccArgs, AccOptions, _Spec) ->
    {lists:reverse(AccArgs), AccOptions};
parse_args_loop([Arg|Args], AccArgs, AccOptions, Spec) ->
    case Arg of
        "--" ->
            {lists:reverse(AccArgs, Args), AccOptions};
        "--" ++ Option ->
            case maps:find(Option, Spec) of
                {ok, flag} ->
                    Opt = list_to_atom(Option),
                    parse_args_loop(Args, AccArgs,
                                    AccOptions#{Opt => true},
                                    Spec);
                error ->
                    usage("unknown option '~s'", [Arg])
            end;
        _ ->
            parse_args_loop(Args, [Arg | AccArgs], AccOptions, Spec)
    end.

dump_snapshots(Args) ->
    {Paths, Options} = parse_args(Args, #{raw => flag}),
    dump_snapshots_loop(Paths, Options).

dump_snapshots_loop([], _) ->
    ok;
dump_snapshots_loop([Path], Options) ->
    dump_snapshot(Path, Options);
dump_snapshots_loop([Path|Rest], Options) ->
    dump_snapshot(Path, Options),
    ?fmt("~n"),
    dump_snapshots_loop(Rest, Options).

dump_snapshot(Path, Options) ->
    case chronicle_storage:read_rsm_snapshot(Path) of
        {ok, Snapshot} ->
            ?fmt("Dumping '~s'~n", [Path]),

            case maps:get(raw, Options, false) of
                true ->
                    dump_term(Snapshot);
                false ->
                    Props = chronicle_rsm:format_snapshot(Snapshot),
                    dump_props(Props)
            end;
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

dump_term(Term) ->
    dump_term("", Term).

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

-spec usage(Fmt::io:format(), Args::[any()]) -> no_return().
usage(Fmt, Args) ->
    ?fmt(Fmt, Args),
    usage().

main(Args) ->
    persistent_term:put(?CHRONICLE_LOAD_NIFS, false),

    case Args of
        ["snapshot" | RestArgs] ->
            dump_snapshots(RestArgs);
        _ ->
            usage()
    end.
