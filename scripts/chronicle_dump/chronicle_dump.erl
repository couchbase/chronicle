-module(chronicle_dump).

-include("chronicle.hrl").

-export([main/1]).
-export([raw/1]).

-define(fmt(Msg), ?fmt(Msg, [])).
-define(fmt(Fmt, Args), ?fmt(group_leader(), Fmt, Args)).
-define(fmt(IoDevice, Fmt, Args), io:format(IoDevice, Fmt ++ "~n", Args)).

-define(error(Msg), ?error(Msg, [])).
-define(error(Fmt, Args), ?fmt(standard_error, Fmt, Args)).

-define(RAW_TAG, '$chronicle_dump_raw').

-define(STATUS_OK, 0).
-define(STATUS_FATAL, 1).
-define(STATUS_ERROR, 2).

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
                {ok, {option, Fun}} ->
                    parse_args_option(Arg, Option, Fun,
                                      Args, AccArgs, AccOptions, Spec);
                error ->
                    usage("unknown option '~s'", [Arg])
            end;
        _ ->
            parse_args_loop(Args, [Arg | AccArgs], AccOptions, Spec)
    end.

parse_args_option(Option, _Name, _Fun, [], _AccArgs, _AccOption, _Spec) ->
    usage("argument required for '~s'", [Option]);
parse_args_option(Option, Name, Fun, [Arg|Args], AccArgs, AccOptions, Spec) ->
    case Fun(Arg) of
        {ok, Result} ->
            NewAccOptions = AccOptions#{list_to_atom(Name) => Result},
            parse_args_loop(Args, AccArgs, NewAccOptions, Spec);
        {error, Error} ->
            usage("invalid argument '~s' for '~s': ~w",
                  [Arg, Option, Error])
    end.

dump_logs(Args) ->
    {Paths, Options} = parse_args(Args,
                                  #{sanitize => {option, fun sanitize_opt/1}}),
    dump_many(Paths,
              fun (Path) ->
                      dump_log(Path, Options)
              end).

dump_log(Path, Options) ->
    ?fmt("Dumping '~s'~n", [Path]),
    SanitizeFun =
        case maps:find(sanitize, Options) of
            {ok, Fun} ->
                Fun;
            error ->
                fun (_, V) -> V end
        end,

    try chronicle_log:read_log(Path,
                               fun dump_log_header/2,
                               fun (Entry, State) ->
                                       dump_log_entry(SanitizeFun, Entry, State)
                               end,
                               header) of
        {ok, _} ->
            ok;
        {error, Error} ->
            set_error(),
            ?error("Error while dumping '~s': ~w", [Path, Error])
    catch
        T:E:Stacktrace ->
            set_error(),
            ?error("Unexpected exception: ~p:~p. Stacktrace:~n"
                   "~p",
                   [T, E,
                    chronicle_utils:sanitize_stacktrace(Stacktrace)])
    end.

dump_log_header(Header, header) ->
    ?fmt("Header:"),
    dump_term(indent(), Header),
    first.

dump_log_entry(SanitizeFun, Entry, State) ->
    case State of
        first ->
            ?fmt("~nEntries:");
        rest ->
            ok
    end,

    dump_term(indent(), unpack_entry(SanitizeFun, Entry)),
    rest.

unpack_entry(SanitizeFun, Entry) ->
    chronicle_storage:map_append(
      fun (#log_entry{value = Value} = LogEntry) ->
              case Value of
                  #rsm_command{rsm_name = Name} ->
                      Sanitized = chronicle_rsm:unpack_payload(
                                    fun (Payload) ->
                                            SanitizeFun(Name, Payload)
                                    end, Value),

                      LogEntry#log_entry{value = Sanitized};
                  _ ->
                      LogEntry
              end
      end, Entry).

sanitize_opt(Value) ->
    case string:split(Value, ":", all) of
        [Module, Function] ->
            M = list_to_atom(Module),
            F = list_to_atom(Function),
            A = 2,
            case chronicle_utils:is_function_exported(M, F, A) of
                true ->
                    {ok, fun M:F/A};
                false ->
                    {error, not_exported}
            end;
        _ ->
            {error, invalid_function}
    end.

output_item(Binary) when is_binary(Binary) -> Binary;
output_item(Atom) when is_atom(Atom) -> atom_to_binary(Atom, latin1);
output_item(Int) when is_integer(Int) -> integer_to_list(Int);
output_item(String) when is_list(String) -> String.

dump_guts(Args) ->
    {Path, _Options} = parse_args(Args, #{}),
    Guts = dump_guts_inner(Path),
    Items = [E || {K, V} <- Guts, E <- [K, V]],
    ?fmt("~s", [[[output_item(Item) | <<0:8>>] || Item <- Items]]).

dump_guts_inner(Path) ->
    case chronicle_storage:read_rsm_snapshot(Path) of
        {ok, Snapshot} ->
            try
                Props0 = chronicle_rsm:format_snapshot(Snapshot),
                Props1 = proplists:get_value("RSM state", Props0),
                {?RAW_TAG, Props} = Props1,

                CompatVer = get_value(cluster_compat_version, Props),
                BucketNames = get_value(bucket_names, Props),

                [{cluster_compat_version,
                  iolist_to_binary(io_lib:format("~p", [CompatVer]))},
                 {bucket_names, string:join(BucketNames, ",")},
                 {rebalance_type, get_value(rebalance_type, Props)}]
            catch
                T:E:Stacktrace ->
                    ?error("Unexpected exception: ~p:~p. Stacktrace:~n"
                           "~p",
                           [T, E,
                            chronicle_utils:sanitize_stacktrace(Stacktrace)])
            end;
        {error, Error} ->
            ?error("Couldn't read snapshot '~s': ~w", [Path, Error])
    end.

get_value(Key, Props) ->
    case proplists:get_value(Key, Props) of
        {Value, _ChronicleMeta} ->
            Value;
        undefined ->
            undefined
    end.

dump_snapshots(Args) ->
    {Paths, Options} = parse_args(Args,
                                  #{raw => flag,
                                    sanitize => {option, fun sanitize_opt/1}}),
    dump_many(Paths,
              fun (Path) ->
                      dump_snapshot(Path, Options)
              end).

dump_many([], _) ->
    ok;
dump_many([Path], Fun) ->
    Fun(Path);
dump_many([Path|Rest], Fun) ->
    Fun(Path),
    ?fmt("~n"),
    dump_many(Rest, Fun).

sanitize_snapshot(Snapshot, Options) ->
    case maps:find(sanitize, Options) of
        {ok, Fun} ->
            chronicle_rsm:map_snapshot(Fun, Snapshot);
        error ->
            Snapshot
    end.

dump_snapshot(Path, Options) ->
    case chronicle_storage:read_rsm_snapshot(Path) of
        {ok, RawSnapshot} ->
            ?fmt("Dumping '~s'~n", [Path]),

            try
                Snapshot = sanitize_snapshot(RawSnapshot, Options),

                case maps:get(raw, Options, false) of
                    true ->
                        dump_term(Snapshot);
                    false ->
                        Props = chronicle_rsm:format_snapshot(Snapshot),
                        dump_props(Props)
                end
            catch
                T:E:Stacktrace ->
                    set_error(),
                    ?error("Unexpected exception: ~p:~p. Stacktrace:~n"
                           "~p",
                           [T, E,
                            chronicle_utils:sanitize_stacktrace(Stacktrace)])
            end;
        {error, Error} ->
            set_error(),
            ?error("Couldn't read snapshot '~s': ~w", [Path, Error])
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

indent() ->
    indent("").

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
    ?error("Usage: ~s [COMMAND]", [escript:script_name()]),
    ?error("Commands:"),
    ?error("    snapshot [--raw] [--sanitize <Mod:Fun>] [FILE]..."),
    ?error("    dumpguts [FILE]"),
    ?error("         log [--sanitize <Mod:Fun>] [FILE]..."),
    stop(?STATUS_FATAL).

-spec usage(Fmt::io:format(), Args::[any()]) -> no_return().
usage(Fmt, Args) ->
    ?error("~s: " ++ Fmt, [escript:script_name() | Args]),
    usage().

-spec stop(non_neg_integer()) -> no_return().
stop(Status) ->
    catch logger_std_h:filesync(default),
    erlang:halt(Status).

status() ->
    case erlang:get(status) of
        undefined ->
            ?STATUS_OK;
        Status ->
            Status
    end.

set_status(Status) ->
    case status() of
        ?STATUS_OK ->
            erlang:put(status, Status);
        _ ->
            ok
    end.

set_error() ->
    set_status(?STATUS_ERROR).

setup_logger() ->
    ok = chronicle_env:setup_logger(),
    ok = logger:remove_handler(default),

    FormatterConfig = #{single_line => true,
                        template => [level, ": ", msg, "\n"]},
    Formatter = {logger_formatter, FormatterConfig},
    ok = logger:add_handler(default, logger_std_h,
                            #{config => #{type => standard_error},
                              formatter => Formatter}).

-spec main(list()) -> no_return().
main(Args) ->
    persistent_term:put(?CHRONICLE_LOAD_NIFS, false),
    setup_logger(),

    case Args of
        [Command | RestArgs] ->
            case Command of
                "snapshot" ->
                    dump_snapshots(RestArgs);
                "log" ->
                    dump_logs(RestArgs);
                "dumpguts" ->
                    dump_guts(RestArgs);
                _ ->
                    usage("unknown command '~s'", [Command])
            end;
        _ ->
            usage()
    end,

    stop(status()).
