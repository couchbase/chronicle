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

-include("chronicle.hrl").

-export([open/4, read_log/4, sync/1, close/1, create/2, append/2, data_size/1]).

-define(MAGIC, <<"chronicle">>).
-define(MAGIC_BYTES, 9).
-define(LOG_VERSION, 1).
-define(HEADER_BYTES, (?MAGIC_BYTES + 1)).

-define(READ_CHUNK_SIZE, 1024 * 1024).
-define(WRITE_CHUNK_SIZE, 1024 * 1024).

-define(TERM_SIZE_BITS, 32).
-define(TERM_SIZE_BYTES, (?TERM_SIZE_BITS bsr 3)).
-define(TERM_SIZE_MAX, (1 bsl ?TERM_SIZE_BITS) - 1).
-define(TERM_HEADER_BYTES, (?CRC_BYTES + ?TERM_SIZE_BYTES)).

-record(log, { fd,
               mode,
               start_pos }).

open(Path, UserDataFun, LogEntryFun, State) ->
    case open_int(Path, write) of
        {ok, Log, UserData} ->
            try scan(Log, LogEntryFun,
                     UserDataFun(UserData, State), #{repair => true}) of
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
            case read_header(Fd) of
                {ok, UserData} ->
                    {ok, make_log(Fd, Mode), UserData};
                {error, _} = Error ->
                    ok = file:close(Fd),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

read_log(Path, UserDataFun, LogEntryFun, State) ->
    case open_int(Path, read) of
        {ok, Log, UserData} ->
            try
                scan(Log, LogEntryFun, UserDataFun(UserData, State))
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

create(Path, UserData) ->
    Mode = write,
    case file:open(Path, open_flags(Mode)) of
        {ok, Fd} ->
            %% Truncate the file if we're recreating it.
            ok = truncate(Fd, 0),
            ok = write_header(Fd, UserData),
            ok = file:sync(Fd),
            ok = chronicle_utils:sync_dir(filename:dirname(Path)),
            {ok, make_log(Fd, Mode)};
        {error, _} = Error ->
            Error
    end.

read_header(Fd) ->
    Size = ?HEADER_BYTES + ?TERM_HEADER_BYTES,
    case chronicle_utils:read_full(Fd, Size) of
        {ok, <<HeaderData:?HEADER_BYTES/binary,
               TermHeader:?TERM_HEADER_BYTES/binary>>} ->
            case check_header_data(HeaderData) of
                ok ->
                    read_header_user_data(Fd, TermHeader);
                {error, _} = Error ->
                    Error
            end;
        eof ->
            {error, no_header};
        {error, _} = Error ->
            Error
    end.

read_header_user_data(Fd, TermHeader) ->
    case decode_entry_size(TermHeader) of
        {ok, Size, _, <<>>} ->
            FullSize = ?CRC_BYTES + Size,
            case chronicle_utils:read_full(Fd, FullSize) of
                {ok, TermData} ->
                    case decode_entry_term(TermData, Size) of
                        {ok, UserData, _, <<>>} ->
                            {ok, UserData};
                        corrupt ->
                            {error, {corrupt_log, bad_header_user_data}}
                    end;
                eof ->
                    {error, no_header};
                {error, _} = Error ->
                    Error
            end;
        corrupt ->
            {error, {corrupt_log, bad_header_user_data}}
    end.

check_header_data(Data) ->
    case Data of
        <<MaybeMagic:?MAGIC_BYTES/binary, Version:8>> ->
            case MaybeMagic =:= ?MAGIC of
                true ->
                    case Version =:= ?LOG_VERSION of
                        true ->
                            ok;
                        false ->
                            {error, {unsupported_version, Version}}
                    end;
                false ->
                    {error, {corrupt_log, bad_header}}
            end
    end.

write_header(Fd, UserData) ->
    Header = <<?MAGIC/binary, ?LOG_VERSION:8>>,
    file:write(Fd, encode_term(UserData, Header)).

append(#log{mode = write, fd = Fd}, Terms) ->
    encode_terms(Terms,
                 fun (Data) ->
                         case file:write(Fd, Data) of
                             ok ->
                                 {ok, byte_size(Data)};
                             {error, _} = Error ->
                                 Error
                         end
                 end).

data_size(#log{fd = Fd, start_pos = HeaderSize}) ->
    case file:position(Fd, cur) of
        {ok, Size} ->
            DataSize = Size - HeaderSize,
            true = (DataSize >= 0),
            {ok, DataSize};
        {error, _} = Error ->
            Error
    end.

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
    {ok, Pos} = file:position(Fd, cur),
    #log{fd = Fd, mode = Mode, start_pos = Pos}.

truncate(Fd, Pos) ->
    case file:position(Fd, Pos) of
        {ok, _} ->
            file:truncate(Fd);
        {error, _} = Error ->
            Error
    end.

scan(Log, Fun, State) ->
    scan(Log, Fun, State, #{}).

scan(#log{fd = Fd, start_pos = Pos}, Fun, State, Opts) ->
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
            case decode_entry_term(NewData0, Size) of
                {ok, Term, Consumed1, NewData} ->
                    {ok, Term, Consumed0 + Consumed1, NewData};
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

decode_entry_term(Data, Size) ->
    case get_crc_data(Data, Size) of
        {ok, TermBinary, Consumed, NewData} ->
            Term = binary_to_term(TermBinary),
            {ok, Term, Consumed, NewData};
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
