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
-module(chronicle_log).

-compile(export_all).

-include("chronicle.hrl").

-define(MAGIC, <<"chronicle">>).
-define(MAGIC_BYTES, 9).
-define(VERSION, 1).
-define(HEADER_BYTES, ?MAGIC_BYTES + 1).

-define(READ_CHUNK_SIZE, 1024 * 1024).
-define(WRITE_CHUNK_SIZE, 1024 * 1024).

-define(TERM_SIZE_BITS, 32).
-define(TERM_SIZE_BYTES, (?TERM_SIZE_BITS bsr 3)).
-define(TERM_SIZE_MAX, (1 bsl ?TERM_SIZE_BITS) - 1).

-record(log, { fd, mode }).

open(Path, Fun, State) ->
    case open_int(Path, write) of
        {ok, Log} ->
            try scan(Log, Fun, State, #{repair => true}) of
                {ok, NewState} ->
                    {ok, Log, NewState};
                {error, _} = Error ->
                    ok = close(Log),
                    Error
            catch
                T:E:Stack ->
                    ok = close(Log),
                    erlang:raise(T, E, Stack)
            end;
        {error, _} = Error ->
            Error
    end.

open_int(Path, Mode) ->
    case file:open(Path, open_flags(Mode)) of
        {ok, Fd} ->
            case check_header(Fd) of
                ok ->
                    {ok, make_log(Fd, Mode)};
                {error, no_header} = Error ->
                    ok = file:close(Fd),
                    maybe_create(Path, Mode, Error);
                {error, _} = Error ->
                    ok = file:close(Fd),
                    Error
            end;
        {error, enoent} = Error ->
            maybe_create(Path, Mode, Error);
        {error, _} = Error ->
            Error
    end.

read_log(Path, Fun, State) ->
    case open_int(Path, read) of
        {ok, Log} ->
            try
                scan(Log, Fun, State)
            after
                close(Log)
            end;
        {error, _} = Error ->
            Error
    end.

sync(#log{fd = Fd}) ->
    %% TODO: preallocate log when possible and use fdatasync instead of sync.
    file:sync(Fd).

close(#log{fd = Fd})->
    file:close(Fd).

maybe_create(Path, Mode, Error) ->
    case Mode of
        write ->
            create(Path);
        read ->
            Error
    end.

create(Path) ->
    Mode = write,
    case file:open(Path, open_flags(Mode)) of
        {ok, Fd} ->
            %% Truncate the file if we're recreating it.
            ok = truncate(Fd, 0),
            ok = write_header(Fd),
            ok = file:sync(Fd),
            ok = chronicle_utils:sync_dir(filename:dirname(Path)),
            {ok, make_log(Fd, Mode)};
        {error, _} = Error ->
            Error
    end.

check_header(Fd) ->
    case chronicle_utils:read_full(Fd, ?HEADER_BYTES) of
        {ok, Data} ->
            check_header_data(Data);
        eof ->
            {error, no_header};
        {error, _} = Error ->
            Error
    end.

check_header_data(Data) ->
    case Data of
        <<MaybeMagic:?MAGIC_BYTES/binary, Version:8>> ->
            case MaybeMagic =:= ?MAGIC of
                true ->
                    case Version =:= ?VERSION of
                        true ->
                            ok;
                        false ->
                            {error, {unsupported_version, Version}}
                    end;
                false ->
                    {error, {corrupt_log, bad_header}}
            end
    end.

write_header(Fd) ->
    Header = <<?MAGIC/binary, ?VERSION:8>>,
    file:write(Fd, Header).

append(#log{mode = write, fd = Fd}, Terms) ->
    encode_terms(Terms,
                 fun (Data) ->
                         file:write(Fd, Data)
                 end).

encode_terms(Terms, Fun) ->
    encode_terms(Terms, <<>>, Fun).

encode_terms([], AccData, Fun) ->
    Fun(AccData);
encode_terms([Term|Terms], AccData, Fun) ->
    NewAccData = encode_term(Term, AccData),
    case byte_size(NewAccData) >= ?WRITE_CHUNK_SIZE of
        true ->
            case Fun(NewAccData) of
                ok ->
                    encode_terms(Terms, <<>>, Fun);
                Other ->
                    Other
            end;
        false ->
            encode_terms(Terms, NewAccData, Fun)
    end.

encode_term(Term, AccData) ->
    TermBinary = term_to_binary(Term),
    Size = byte_size(TermBinary),
    true = (Size =< ?TERM_SIZE_MAX),

    SizeEncoded = <<Size:?TERM_SIZE_BITS>>,
    SizeCrc = erlang:crc32(SizeEncoded),
    TermCrc = erlang:crc32(TermBinary),

    <<AccData/binary,
      SizeCrc:?CRC_BITS,
      SizeEncoded/binary,
      TermCrc:?CRC_BITS,
      TermBinary/binary>>.

make_log(Fd, Mode) ->
    #log{fd = Fd, mode = Mode}.

truncate(Fd, Pos) ->
    case file:position(Fd, Pos) of
        {ok, _} ->
            file:truncate(Fd);
        {error, _} = Error ->
            Error
    end.

scan(Log, Fun, State) ->
    scan(Log, Fun, State, #{}).

scan(#log{fd = Fd}, Fun, State, Opts) ->
    Pos = ?HEADER_BYTES,
    case file:position(Fd, Pos) of
        {ok, ActualPos} ->
            true = (Pos =:= ActualPos),
            scan_loop(Fd, Pos, <<>>, Fun, State, Opts);
        {error, _} = Error ->
            Error
    end.

scan_loop(Fd, Pos, AccData, Fun, State, Opts) ->
    case file:read(Fd, ?READ_CHUNK_SIZE) of
        {ok, ReadData} ->
            Data = <<AccData/binary, ReadData/binary>>,
            case scan_chunk(Pos, Data, Fun, State) of
                {ok, NewPos, DataLeft, NewState} ->
                    scan_loop(Fd, NewPos, DataLeft, Fun, NewState, Opts);
                {error, _} = Error ->
                    Error
            end;
        eof ->
            Repair = maps:get(repair, Opts, false),
            case AccData of
                <<>> ->
                    {ok, State};
                _ when Repair ->
                    case truncate(Fd, Pos) of
                        ok ->
                            {ok, State};
                        {error, Error} ->
                            {error, {truncate_failed, Error}}
                    end;
                _ ->
                    {error, {unexpected_eof, Pos}}
            end;
        {error, _} = Error ->
            Error
    end.

scan_chunk(Pos, Data, Fun, State) ->
    case decode_entry(Data) of
        {ok, Term, BytesConsumed, NewData} ->
            NewState = Fun(Term, State),
            NewPos = Pos + BytesConsumed,
            scan_chunk(NewPos, NewData, Fun, NewState);
        need_more_data ->
            {ok, Pos, Data, State};
        corrupt ->
            {error, {corrupt_log, {bad_entry, Pos}}}
    end.

decode_entry(Data) ->
    case decode_entry_size(Data) of
        {ok, Size, Consumed0, NewData0} ->
            case get_crc_data(NewData0, Size) of
                {ok, TermBinary, Consumed1, NewData} ->
                    Term = binary_to_term(TermBinary),
                    BytesConsumed = Consumed0 + Consumed1,
                    {ok, Term, BytesConsumed, NewData};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

decode_entry_size(Data) ->
    case get_crc_data(Data, ?TERM_SIZE_BYTES) of
        {ok, <<Size:?TERM_SIZE_BITS>>, Consumed, NewData} ->
            {ok, Size, Consumed, NewData};
        Error ->
            Error
    end.

open_flags(Mode) ->
    Flags = [raw, binary, read],
    case Mode of
        read ->
            Flags;
        write ->
            [append | Flags]
    end.

get_crc_data(Data, Size) ->
    NeedSize = ?CRC_BYTES + Size,
    case byte_size(Data) < NeedSize of
        true ->
            need_more_data;
        false ->
            <<Crc:?CRC_BITS,
              Payload:Size/binary,
              RestData/binary>> = Data,
            case erlang:crc32(Payload) =:= Crc of
                true ->
                    {ok, Payload, NeedSize, RestData};
                false ->
                    corrupt
            end
    end.
