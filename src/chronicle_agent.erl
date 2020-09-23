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
%% TODO: pull is needed to detect situations where a node gets shunned
%% TODO: check more state invariants
-module(chronicle_agent).

-compile(export_all).

-export_type([provision_result/0]).

-behavior(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-include("chronicle.hrl").

-import(chronicle_utils, [call_async/3,
                          config_peers/1,
                          next_term/2,
                          term_number/1,
                          compare_positions/2]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer),
        case Peer of
            ?SELF_PEER ->
                ?SERVER;
            _ ->
                ?SERVER_NAME(Peer, ?MODULE)
        end).

-define(PROVISION_TIMEOUT, 10000).
-define(ESTABLISH_LOCAL_TERM_TIMEOUT, 10000).
-define(LOCAL_MARK_COMMITTED_TIMEOUT, 5000).
-define(STORE_BRANCH_TIMEOUT, 15000).

%% Used to indicate that a function will send a message with the provided Tag
%% back to the caller when the result is ready. And the result type is
%% _ReplyType. This is entirely useless for dializer, but is usefull for
%% documentation purposes.
-type replies(Tag, _ReplyType) :: Tag.
-type peer() :: ?SELF_PEER | chronicle:peer().

%% TODO: get rid of the duplication between #state{} and #metadata{}.
-record(state, { storage }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

-spec monitor(peer()) -> reference().
monitor(Peer) ->
    chronicle_utils:monitor_process(?SERVER(Peer)).

-spec get_system_state() -> provisioned | unprovisioned.
get_system_state() ->
    case get_metadata() of
        {ok, _} ->
            provisioned;
        {error, not_provisioned} ->
            unprovisioned;
        {error, _} = Error ->
            exit(Error)
    end.

-type get_metadata_result() :: {ok, #metadata{}} |
                               {error, not_provisioned}.

-spec get_metadata() -> get_metadata_result().
get_metadata() ->
    gen_server:call(?SERVER, get_metadata).

get_log() ->
    gen_server:call(?SERVER, get_log).

-spec get_log(chronicle:history_id(),
              chronicle:leader_term(),
              chronicle:seqno(),
              chronicle:seqno()) ->
          {ok, [#log_entry{}]} |
          {error, Error} when
      Error :: {history_mismatch, chronicle:history_id()} |
               {conflicting_term, chronicle:leader_term()} |
               bad_range.
get_log(HistoryId, Term, StartSeqno, EndSeqno) ->
    gen_server:call(?SERVER, {get_log, HistoryId, Term, StartSeqno, EndSeqno}).

-spec get_history_id(#metadata{}) -> chronicle:history_id().
get_history_id(#metadata{history_id = CommittedHistoryId,
                         pending_branch = undefined}) ->
    CommittedHistoryId;
get_history_id(#metadata{pending_branch =
                             #branch{history_id = PendingHistoryId}}) ->
    PendingHistoryId.

-type provision_result() :: ok | {error, already_provisioned}.
-spec provision([Machine]) -> provision_result() when
      Machine :: {Name :: atom(), Mod :: module(), Args :: [any()]}.
provision(Machines) ->
    case gen_server:call(?SERVER, {provision, Machines}, ?PROVISION_TIMEOUT) of
        ok ->
            ok = chronicle_secondary_sup:sync_system_state_change();
        Other ->
            Other
    end.

-spec reprovision() -> provision_result().
reprovision() ->
    case gen_server:call(?SERVER, reprovision, ?PROVISION_TIMEOUT) of
        ok ->
            ok = chronicle_secondary_sup:sync_system_state_change();
        Other ->
            Other
    end.

-spec wipe() -> ok.
wipe() ->
    case gen_server:call(?SERVER, wipe) of
        ok ->
            ok = chronicle_secondary_sup:sync_system_state_change();
        Other ->
            Other
    end.

-type establish_term_result() ::
        {ok, #metadata{}} |
        {error, establish_term_error()}.

-type establish_term_error() ::
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {behind, chronicle:peer_position()}.

-spec establish_local_term(chronicle:history_id(),
                           chronicle:leader_term()) ->
          establish_term_result().
establish_local_term(HistoryId, Term) ->
    gen_server:call(?SERVER, {establish_term, HistoryId, Term},
                    ?ESTABLISH_LOCAL_TERM_TIMEOUT).

-spec establish_term(peer(),
                     Opaque,
                     chronicle:history_id(),
                     chronicle:leader_term(),
                     chronicle:peer_position()) ->
          replies(Opaque, establish_term_result()).
establish_term(Peer, Opaque, HistoryId, Term, Position) ->
    %% TODO: don't abuse gen_server calls here and everywhere else
    call_async(?SERVER(Peer), Opaque,
               {establish_term, HistoryId, Term, Position}).

-type ensure_term_result() ::
        {ok, #metadata{}} |
        {error, ensure_term_error()}.

-type ensure_term_error() ::
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()}.

-spec ensure_term(peer(),
                  Opaque,
                  chronicle:history_id(),
                  chronicle:leader_term()) ->
          replies(Opaque, ensure_term_result()).
ensure_term(Peer, Opaque, HistoryId, Term) ->
    call_async(?SERVER(Peer), Opaque, {ensure_term, HistoryId, Term}).

-type append_result() :: ok | {error, append_error()}.
-type append_error() ::
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chorincle:leader_term()} |
        {missing_entries, #metadata{}} |
        {protocol_error, any()}.

-spec append(peer(),
             Opaque,
             chronicle:history_id(),
             chronicle:leader_term(),
             chronicle:seqno(),
             chronicle:seqno(),
             [#log_entry{}]) ->
          replies(Opaque, append_result()).
append(Peer, Opaque, HistoryId, Term, CommittedSeqno, AtSeqno, Entries) ->
    call_async(?SERVER(Peer), Opaque,
               {append, HistoryId, Term, CommittedSeqno, AtSeqno, Entries}).

-type local_mark_committed_result() ::
        ok | {error, local_mark_committed_error()}.
-type local_mark_committed_error() ::
        {history_mismatch, chronicle:history_id()} |
        {conflicting_term, chronicle:leader_term()} |
        {protocol_error, any()}.

-spec local_mark_committed(chronicle:history_id(),
                           chronicle:leader_term(),
                           chronicle:seqno()) ->
          local_mark_committed_result().
local_mark_committed(HistoryId, Term, CommittedSeqno) ->
    gen_server:call(?SERVER,
                    {local_mark_committed, HistoryId, Term, CommittedSeqno},
                    ?LOCAL_MARK_COMMITTED_TIMEOUT).

-type store_branch_result() ::
        {ok, #metadata{}} |
        {error, {concurrent_branch, OurBranch::#branch{}}}.

-spec store_branch(peer(), #branch{}) -> store_branch_result().
store_branch(Peer, Branch) ->
    gen_server:call(?SERVER(Peer),
                    {store_branch, Branch}, ?STORE_BRANCH_TIMEOUT).

-spec undo_branch(peer(), chronicle:history_id()) -> ok | {error, Error} when
      Error :: no_branch |
               {bad_branch, OurBranch::#branch{}}.
undo_branch(Peer, BranchId) ->
    gen_server:call(?SERVER(Peer), {undo_branch, BranchId}).

%% gen_server callbacks
init([]) ->
    {ok, restore_state()}.

handle_call(get_metadata, _From, State) ->
    handle_get_metadata(State);
handle_call(get_log, _From, State) ->
    %% TODO: get rid of this
    {reply, {ok, chronicle_storage:get_log()}, State};
handle_call({get_log, HistoryId, Term, StartSeqno, EndSeqno}, _From, State) ->
    handle_get_log(HistoryId, Term, StartSeqno, EndSeqno, State);
handle_call({provision, Machines}, _From, State) ->
    handle_provision(Machines, State);
handle_call(reprovision, _From, State) ->
    handle_reprovision(State);
handle_call(wipe, _From, State) ->
    handle_wipe(State);
handle_call({establish_term, HistoryId, Term}, _From, State) ->
    %% TODO: consider simply skipping the position check for this case
    Position = {get_meta(term_voted, State), get_high_seqno(State)},
    handle_establish_term(HistoryId, Term, Position, State);
handle_call({establish_term, HistoryId, Term, Position}, _From, State) ->
    handle_establish_term(HistoryId, Term, Position, State);
handle_call({ensure_term, HistoryId, Term}, _From, State) ->
    handle_ensure_term(HistoryId, Term, State);
handle_call({append, HistoryId, Term, CommittedSeqno, AtSeqno, Entries},
            _From, State) ->
    handle_append(HistoryId, Term, CommittedSeqno, AtSeqno, Entries, State);
handle_call({local_mark_committed, HistoryId, Term, CommittedSeqno},
            _From, State) ->
    handle_local_mark_committed(HistoryId, Term, CommittedSeqno, State);
handle_call({store_branch, Branch}, _From, State) ->
    handle_store_branch(Branch, State);
handle_call({undo_branch, BranchId}, _From, State) ->
    handle_undo_branch(BranchId, State);
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast ~p.~nState:~n~p",
             [Cast, State]),
    {noreply, State}.

%% internal
handle_get_metadata(State) ->
    case check_provisioned(State) of
        ok ->
            {reply, {ok, state2metadata(State)}, State};
        {error, _} = Error ->
            {reply, Error, State}
    end.

state2metadata(State) ->
    #{peer := Peer,
      history_id := HistoryId,
      term := Term,
      term_voted := TermVoted,
      committed_seqno := CommittedSeqno,
      pending_branch := PendingBranch} = get_meta(State),

    ConfigEntry = get_config(State),
    {Config, ConfigRevision} =
        case ConfigEntry of
            undefined ->
                {undefined, undefined};
            #log_entry{value = Value} ->
                {Value, chronicle_proposer:log_entry_revision(ConfigEntry)}
        end,

    #metadata{peer = Peer,
              history_id = HistoryId,
              term = Term,
              term_voted = TermVoted,
              high_seqno = get_high_seqno(State),
              committed_seqno  = CommittedSeqno,
              config = Config,
              config_revision = ConfigRevision,
              pending_branch = PendingBranch}.

handle_get_log(HistoryId, Term, StartSeqno, EndSeqno, State) ->
    case check_get_log(HistoryId, Term, StartSeqno, EndSeqno, State) of
        ok ->
            Entries = chronicle_storage:get_log(StartSeqno, EndSeqno),
            {reply, {ok, Entries}, State};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_get_log(HistoryId, Term, StartSeqno, EndSeqno, State) ->
    ?CHECK(check_history_id(HistoryId, State),
           check_same_term(Term, State),
           check_log_range(StartSeqno, EndSeqno, State)).

handle_reprovision(State) ->
    case check_reprovision(State) of
        {ok, Config} ->
            #{history_id := HistoryId, term := Term} = get_meta(State),
            HighSeqno = get_high_seqno(State),
            Peer = get_peer_name(),
            NewTerm = next_term(Term, Peer),
            NewConfig = Config#config{voters = [Peer]},
            Seqno = HighSeqno + 1,

            ConfigEntry = #log_entry{history_id = HistoryId,
                                     term = NewTerm,
                                     seqno = Seqno,
                                     value = NewConfig},

            ?DEBUG("Reprovisioning peer with config:~n~p", [ConfigEntry]),

            NewStorage = append_entry(ConfigEntry,
                                      #{peer => Peer,
                                        term => NewTerm,
                                        term_voted => NewTerm,
                                        committed_seqno => Seqno},
                                      State),

            NewState = State#state{storage = NewStorage},

            announce_system_reprovisioned(NewState),
            announce_new_config(NewState),
            announce_committed_seqno(Seqno),

            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_reprovision(State) ->
    case is_provisioned(State) of
        true ->
            Peer = get_meta(peer, State),
            ConfigEntry = get_config(State),
            Config = ConfigEntry#log_entry.value,
            case Config of
                #config{voters = Voters} ->
                    case Voters of
                        [Peer] ->
                            {ok, Config};
                        _ ->
                            {error, {bad_config, Peer, Voters}}
                    end;
                #transition{} ->
                    {error, {unstable_config, Config}}
            end;
        false ->
            {error, not_provisioned}
    end.

handle_provision(Machines0, State) ->
    case check_not_provisioned(State) of
        ok ->
            Peer = get_peer_name(),
            HistoryId = chronicle_utils:random_uuid(),
            Term = next_term(?NO_TERM, Peer),
            Seqno = 1,

            Machines = maps:from_list(
                         [{Name, #rsm_config{module = Module, args = Args}} ||
                             {Name, Module, Args} <- Machines0]),

            Config = #config{voters = [Peer], state_machines = Machines},
            ConfigEntry = #log_entry{history_id = HistoryId,
                                     term = Term,
                                     seqno = Seqno,
                                     value = Config},

            ?DEBUG("Provisioning with history ~p. Config:~n~p",
                   [HistoryId, Config]),

            NewStorage = append_entry(ConfigEntry,
                                      #{peer => Peer,
                                        history_id => HistoryId,
                                        term => Term,
                                        term_voted => Term,
                                        committed_seqno => Seqno},
                                      State),

            NewState = State#state{storage = NewStorage},

            announce_system_provisioned(NewState),
            announce_new_config(NewState),
            announce_committed_seqno(Seqno),

            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

is_provisioned(State) ->
    get_config(State) =/= undefined.

check_not_provisioned(State) ->
    case is_provisioned(State) of
        true ->
            {error, already_provisioned};
        false ->
            ok
    end.

check_provisioned(State) ->
    case is_provisioned(State) of
        true ->
            ok;
        false ->
            {error, not_provisioned}
    end.

handle_wipe(#state{storage = Storage}) ->
    chronicle_storage:close(Storage),
    chronicle_storage:wipe(),
    announce_system_state(unprovisioned),
    ?DEBUG("Wiped successfully", []),
    {reply, ok, restore_state()}.

handle_establish_term(HistoryId, Term, Position, State) ->
    assert_valid_history_id(HistoryId),
    assert_valid_term(Term),

    case check_establish_term(HistoryId, Term, Position, State) of
        ok ->
            NewStorage =
                store_meta(#{history_id => HistoryId, term => Term}, State),
            NewState = State#state{storage = NewStorage},
            announce_term_established(Term),
            ?DEBUG("Accepted term ~p in history ~p", [Term, HistoryId]),
            {reply, {ok, state2metadata(State)}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_establish_term(HistoryId, Term, Position, State) ->
    ?CHECK(check_history_id(HistoryId, State),
           check_later_term(Term, State),
           check_peer_current(Position, State)).

check_later_term(Term, State) ->
    CurrentTerm = get_meta(term, State),
    case term_number(Term) > term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

check_peer_current(Position, State) ->
    OurTermVoted = get_meta(term_voted, State),
    OurHighSeqno = get_high_seqno(State),
    OurPosition = {OurTermVoted, OurHighSeqno},
    case compare_positions(Position, OurPosition) of
        lt ->
            {error, {behind, OurPosition}};
        _ ->
            ok
    end.

handle_ensure_term(HistoryId, Term, State) ->
    case ?CHECK(check_history_id(HistoryId, State),
                check_not_earlier_term(Term, State)) of
        ok ->
            {reply, {ok, state2metadata(State)}, State};
        {error, _} = Error ->
            {reply, Error, State}
    end.

handle_append(HistoryId, Term, CommittedSeqno, AtSeqno, Entries, State) ->
    assert_valid_history_id(HistoryId),
    assert_valid_term(Term),

    case check_append(HistoryId, Term,
                      CommittedSeqno, AtSeqno, Entries, State) of
        {ok, Info} ->
            complete_append(HistoryId, Term, Info, State);
        {error, _} = Error ->
            {reply, Error, State}
    end.

extract_latest_config(Entries) ->
    lists:foldl(
      fun (Entry, Acc) ->
              case Entry#log_entry.value of
                  #config{} ->
                      Entry;
                  #transition{} ->
                      Entry;
                  #rsm_command{} ->
                      Acc
              end
      end, false, Entries).

complete_append(HistoryId, Term, Info, State) ->
    #{entries := Entries,
      start_seqno := StartSeqno,
      end_seqno := EndSeqno,
      committed_seqno := NewCommittedSeqno,
      truncate := Truncate} = Info,

    WasProvisioned = is_provisioned(State),
    Peer =
        case WasProvisioned of
            true ->
                get_meta(peer, State);
            false ->
                PeerName = get_peer_name(),

                %% TODO: This assumes that there's going to be a config among
                %% entries when node is auto-provisioned. This is a bit
                %% questionable. But should also be resolved once there's an
                %% install_snapshot step.
                Config = extract_latest_config(Entries),
                Peers = config_peers(Config#log_entry.value),
                true = lists:member(PeerName, Peers),

                PeerName
        end,

    PreMetadata =
        #{history_id => HistoryId,
          term => Term,
          term_voted => Term,
          pending_branch => undefined,
          peer => Peer},
    PostMetadata = #{committed_seqno => NewCommittedSeqno},
    NewStorage = append_entries(StartSeqno, EndSeqno, Entries,
                                PreMetadata, PostMetadata,
                                Truncate, State),

    NewState = State#state{storage = NewStorage},

    case WasProvisioned of
        true ->
            ok;
        false ->
            announce_system_state(provisioned, state2metadata(NewState))
    end,

    maybe_announce_term_established(Term, State),
    maybe_announce_new_config(State, NewState),
    maybe_announce_committed_seqno(State, NewState),

    ?DEBUG("Appended entries.~n"
           "History id: ~p~n"
           "Term: ~p~n"
           "High Seqno: ~p~n"
           "Committed Seqno: ~p~n"
           "Entries: ~p~n"
           "Config: ~p",
           [HistoryId, Term, EndSeqno,
            NewCommittedSeqno, Entries, get_config(NewState)]),

    {reply, ok, NewState}.

check_append(HistoryId, Term, CommittedSeqno, AtSeqno, Entries, State) ->
    ?CHECK(check_history_id(HistoryId, State),
           check_not_earlier_term(Term, State),
           check_append_obsessive(Term, CommittedSeqno,
                                  AtSeqno, Entries, State)).

check_append_obsessive(Term, CommittedSeqno, AtSeqno, Entries, State) ->
    case get_entries_seqnos(AtSeqno, Entries) of
        {ok, StartSeqno, EndSeqno} ->
            #{term_voted := OurTermVoted,
              committed_seqno := OurCommittedSeqno} = get_meta(State),
            OurHighSeqno = get_high_seqno(State),

            {SafeHighSeqno, NewTerm} =
                case Term =:= OurTermVoted of
                    true ->
                        {OurHighSeqno, false};
                    false ->
                        %% Last we received any entries was in a different
                        %% term. So any uncommitted entries might actually be
                        %% from alternative histories and they need to be
                        %% truncated.
                        {OurCommittedSeqno, true}
                end,

            case StartSeqno > SafeHighSeqno + 1 of
                true ->
                    %% TODO: add more information here?

                    %% There's a gap between what entries we've got and what
                    %% we were given. So the leader needs to send us more.
                    {error, {missing_entries, state2metadata(State)}};
                false ->
                    case EndSeqno < SafeHighSeqno of
                        true ->
                            %% Currently, this should never happen, because
                            %% proposer always sends all history it has. But
                            %% conceptually it doesn't have to be this way. If
                            %% proposer starts chunking appends into
                            %% sub-appends in the future, in combination with
                            %% message loss, this case will be normal. But
                            %% consider this a protocol error for now.
                            {error,
                             {protocol_error,
                              {stale_proposer, EndSeqno, SafeHighSeqno}}};
                        false ->
                            case check_committed_seqno(Term, CommittedSeqno,
                                                       EndSeqno, State) of
                                {ok, FinalCommittedSeqno} ->
                                    case drop_known_entries(
                                           Entries,
                                           SafeHighSeqno, OurHighSeqno,
                                           NewTerm, State) of
                                        {ok,
                                         FinalStartSeqno,
                                         Truncate,
                                         FinalEntries} ->
                                            {ok,
                                             #{entries => FinalEntries,
                                               start_seqno => FinalStartSeqno,
                                               end_seqno => EndSeqno,
                                               committed_seqno =>
                                                   FinalCommittedSeqno,
                                               truncate => Truncate}};
                                        {error, _} = Error ->
                                            Error
                                    end
                            end
                    end
            end;
        {error, {malformed, Entry}} ->
            %% TODO: remove logging of the entries
            ?ERROR("Received an ill-formed append request in term ~p.~n"
                   "Stumbled upon this entry: ~p~n"
                   "All entries:~n~p",
                   [Term, Entry, Entries]),
            {error, {protocol_error,
                     {malformed_append, Entry, Entries}}}

    end.

drop_known_entries(Entries, SafeHighSeqno, HighSeqno, NewTerm, State) ->
    {SafeEntries, UnsafeEntries} = split_entries(SafeHighSeqno, Entries),

    %% All safe entries must match.
    case check_entries_match(SafeEntries, State) of
        ok ->
            {PreHighSeqnoEntries, PostHighSeqnoEntries} =
                split_entries(HighSeqno, UnsafeEntries),

            case check_entries_match(PreHighSeqnoEntries, State) of
                ok ->
                    {ok, HighSeqno + 1, false, PostHighSeqnoEntries};
                {mismatch, MismatchSeqno, _OurEntry, Remaining} ->
                    true = NewTerm,

                    ?DEBUG("Mismatch at seqno ~p. "
                           "Going to drop the log tail.", [MismatchSeqno]),
                    {ok, MismatchSeqno, true, Remaining ++ PostHighSeqnoEntries}
            end;
        {mismatch, Seqno, OurEntry, Remaining} ->
            %% TODO: don't log entries
            ?ERROR("Unexpected mismatch in entries sent by the proposer.~n"
                   "Seqno: ~p~n"
                   "Our entry:~n~p~n"
                   "Remaining entries:~n~p",
                   [Seqno, OurEntry, Remaining]),
            {error, {protocol_error,
                     {mismatched_entry, Remaining, OurEntry}}}
    end.

split_entries(Seqno, Entries) ->
    lists:splitwith(
      fun (#log_entry{seqno = EntrySeqno}) ->
              EntrySeqno =< Seqno
      end, Entries).

check_entries_match([], _State) ->
    ok;
check_entries_match([Entry | Rest] = Entries, State) ->
    EntrySeqno = Entry#log_entry.seqno,
    {ok, OurEntry} = get_log_entry(EntrySeqno, State),

    %% TODO: it should be enough to compare histories and terms here. But for
    %% now let's compare complete entries to be doubly confident.
    case Entry =:= OurEntry of
        true ->
            check_entries_match(Rest, State);
        false ->
            {mismatch, EntrySeqno, OurEntry, Entries}
    end.

get_entries_seqnos(AtSeqno, Entries) ->
    get_entries_seqnos_loop(Entries, AtSeqno + 1, AtSeqno).

get_entries_seqnos_loop([], StartSeqno, EndSeqno) ->
    {ok, StartSeqno, EndSeqno};
get_entries_seqnos_loop([Entry|Rest], StartSeqno, EndSeqno) ->
    Seqno = Entry#log_entry.seqno,

    case Seqno =:= EndSeqno + 1 of
        true ->
            get_entries_seqnos_loop(Rest, StartSeqno, Seqno);
        false ->
            {error, {malformed, Entry}}
    end.

check_committed_seqno(Term, CommittedSeqno, HighSeqno, State) ->
    ?CHECK(check_committed_seqno_known(CommittedSeqno, HighSeqno, State),
           check_committed_seqno_rollback(Term, CommittedSeqno, State)).

check_committed_seqno_rollback(Term, CommittedSeqno, State) ->
    #{term_voted := OurTermVoted,
      committed_seqno := OurCommittedSeqno} = get_meta(State),
    case CommittedSeqno < OurCommittedSeqno of
        true ->
            case Term =:= OurTermVoted of
                true ->
                    ?ERROR("Refusing to lower our committed "
                           "seqno ~p to ~p in term ~p. "
                           "This should never happen.",
                           [OurCommittedSeqno, CommittedSeqno, Term]),
                    {error, {protocol_error,
                             {committed_seqno_rollback,
                              CommittedSeqno, OurCommittedSeqno}}};
                false ->
                    %% If this append establishes a new term, it's possible
                    %% that the leader doesn't know the latest committed
                    %% seqno. This is normal, in a sense that the leader will
                    %% re-commit the same value again. The receiving peer will
                    %% keep its committed seqno unchanged.
                    ?INFO("Leader in term ~p believes seqno ~p "
                          "to be committed, "
                          "while we know that ~p was committed in term ~p. "
                          "Keeping our committed seqno intact.",
                          [Term, CommittedSeqno,
                           OurCommittedSeqno, OurTermVoted]),
                    {ok, OurCommittedSeqno}
            end;
        false ->
            {ok, CommittedSeqno}
    end.

check_committed_seqno_known(CommittedSeqno, HighSeqno, State) ->
    case CommittedSeqno > HighSeqno of
        true ->
            %% TODO: add more information here?
            {error, {missing_entries, state2metadata(State)}};
        false ->
            ok
    end.

check_not_earlier_term(Term, State) ->
    CurrentTerm = get_meta(term, State),
    case term_number(Term) >= term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

handle_local_mark_committed(HistoryId, Term, CommittedSeqno, State) ->
    case check_local_mark_committed(HistoryId, Term, CommittedSeqno, State) of
        ok ->
            HighSeqno = get_high_seqno(State),

            complete_append(HistoryId, Term,
                            #{entries => [],
                              start_seqno => HighSeqno + 1,
                              end_seqno => HighSeqno,
                              committed_seqno => CommittedSeqno,

                              %% This call is only performed by the local
                              %% leader, so there should never be a need to
                              %% truncate the uncommitted tail.
                              truncate => false},
                            State);
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_local_mark_committed(HistoryId, Term, CommittedSeqno, State) ->
    ?CHECK(check_history_id(HistoryId, State),
           check_same_term(Term, State),
           case check_committed_seqno_rollback(Term, CommittedSeqno, State) of
               {ok, FinalCommittedSeqno} ->
                   %% This is only ever called by the local leader, so there
                   %% never should be a possibility of rollback.
                   true = (FinalCommittedSeqno =:= CommittedSeqno),
                   ok;
               {error, _} = Error ->
                   Error
           end).

handle_store_branch(Branch, State) ->
    assert_valid_branch(Branch),

    case ?CHECK(check_provisioned(State),
                check_branch_compatible(Branch, State),
                check_branch_coordinator(Branch, State)) of
        {ok, FinalBranch} ->
            NewStorage = store_meta(#{pending_branch => FinalBranch}, State),
            NewState = State#state{storage = NewStorage},

            case get_meta(pending_branch, State) of
                undefined ->
                    %% New branch, announce history change.
                    announce_new_history(NewState);
                _ ->
                    ok
            end,

            ?DEBUG("Stored a branch record:~n~p", [FinalBranch]),
            {reply, {ok, state2metadata(NewState)}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_branch_compatible(NewBranch, State) ->
    PendingBranch = get_meta(pending_branch, State),
    case PendingBranch =:= undefined of
        true ->
            ok;
        false ->
            PendingId = PendingBranch#branch.history_id,
            NewId = NewBranch#branch.history_id,

            case PendingId =:= NewId of
                true ->
                    ok;
                false ->
                    {error, {concurrent_branch, PendingBranch}}
            end
    end.

check_branch_coordinator(Branch, State) ->
    Peer = get_meta(peer, State),
    Coordinator =
        case Branch#branch.coordinator of
            self ->
                Peer;
            Other ->
                Other
        end,

    FinalBranch = Branch#branch{coordinator = Coordinator},
    Peers = Branch#branch.peers,

    case lists:member(Coordinator, Peers) of
        true ->
            {ok, FinalBranch};
        false ->
            {error, {coordinator_not_in_peers, Coordinator, Peers}}
    end.

handle_undo_branch(BranchId, State) ->
    assert_valid_history_id(BranchId),
    case check_branch_id(BranchId, State) of
        ok ->
            NewStorage = store_meta(#{pending_branch => undefined}, State),
            NewState = State#state{storage = NewStorage},
            announce_new_history(NewState),

            ?DEBUG("Undid branch ~p", [BranchId]),
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_branch_id(BranchId, State) ->
    OurBranch = get_meta(pending_branch, State),
    case OurBranch of
        undefined ->
            {error, no_branch};
        #branch{history_id = OurBranchId} ->
            case OurBranchId =:= BranchId of
                true ->
                    ok;
                false ->
                    {error, {bad_branch, OurBranch}}
            end
    end.

check_history_id(HistoryId, State) ->
    OurHistoryId = get_history_id_int(State),
    case OurHistoryId =:= ?NO_HISTORY orelse HistoryId =:= OurHistoryId of
        true ->
            ok;
        false ->
            {error, {history_mismatch, OurHistoryId}}
    end.

%% TODO: get rid of this once #state{} doesn't duplicate #metadata{}.
get_history_id_int(State) ->
    #{history_id := CommittedHistoryId,
      pending_branch := PendingBranch} = get_meta(State),

    case PendingBranch of
        undefined ->
            CommittedHistoryId;
        #branch{history_id = PendingHistoryId} ->
            PendingHistoryId
    end.

check_same_term(Term, State) ->
    OurTerm = get_meta(term, State),
    case Term =:= OurTerm of
        true ->
            ok;
        false ->
            {error, {conflicting_term, OurTerm}}
    end.

check_log_range(StartSeqno, EndSeqno, State) ->
    HighSeqno = get_high_seqno(State),
    case StartSeqno > HighSeqno
        orelse EndSeqno > HighSeqno
        orelse StartSeqno > EndSeqno of
        true ->
            {error, bad_range};
        false ->
            ok
    end.

restore_state() ->
    #state{storage = storage_open()}.

get_state_path() ->
    case application:get_env(chronicle, data_dir) of
        {ok, Dir} ->
            {ok, filename:join(Dir, "agent_state")};
        undefined ->
            undefined
    end.

assert_valid_history_id(HistoryId) ->
    true = is_binary(HistoryId).

assert_valid_term(Term) ->
    {TermNumber, _TermLeader} = Term,
    true = is_integer(TermNumber).

assert_valid_branch(#branch{history_id = HistoryId,
                            coordinator = Coordinator}) ->
    assert_valid_history_id(HistoryId),
    assert_valid_peer(Coordinator).

assert_valid_peer(_Coordinator) ->
    %% TODO
    ok.

announce_new_history(State) ->
    HistoryId = get_history_id_int(State),
    Metadata = state2metadata(State),
    chronicle_events:sync_notify({new_history, HistoryId, Metadata}).

maybe_announce_term_established(Term, State) ->
    OldTerm = get_meta(term, State),
    case Term =:= OldTerm of
        true ->
            ok;
        false ->
            announce_term_established(Term)
    end.

announce_term_established(Term) ->
    chronicle_events:sync_notify({term_established, Term}).

maybe_announce_new_config(OldState, NewState) ->
    case get_config(OldState) =:= get_config(NewState) of
        true ->
            ok;
        false ->
            announce_new_config(NewState)
    end.

announce_new_config(State) ->
    Metadata = state2metadata(State),
    ConfigEntry = get_config(State),
    Config = ConfigEntry#log_entry.value,
    chronicle_events:sync_notify({new_config, Config, Metadata}).

maybe_announce_committed_seqno(OldState, NewState) ->
    OldCommittedSeqno = get_meta(committed_seqno, OldState),
    NewCommittedSeqno = get_meta(committed_seqno, NewState),
    case OldCommittedSeqno =:= NewCommittedSeqno of
        true ->
            ok;
        false ->
            announce_committed_seqno(NewCommittedSeqno)
    end.

announce_committed_seqno(CommittedSeqno) ->
    chronicle_events:notify({committed_seqno, CommittedSeqno}).

announce_system_state(SystemState) ->
    announce_system_state(SystemState, no_extra).

announce_system_state(SystemState, Extra) ->
    chronicle_events:sync_notify({system_state, SystemState, Extra}).

announce_system_provisioned(State) ->
    announce_system_state(provisioned, state2metadata(State)).

announce_system_reprovisioned(State) ->
    chronicle_events:sync_notify({system_event,
                                  reprovisioned, state2metadata(State)}).

storage_open() ->
    Storage0 = chronicle_storage:open(),
    Meta = chronicle_storage:get_meta(Storage0),
    Storage1 =
        case maps:size(Meta) > 0 of
            true ->
                Storage0;
            false ->
                SeedMeta = #{peer => ?NO_PEER,
                             history_id => ?NO_HISTORY,
                             term => ?NO_TERM,
                             term_voted => ?NO_TERM,
                             committed_seqno => ?NO_SEQNO,
                             pending_branch => undefined},
                ?INFO("Found empty storage. "
                      "Seeding it with default metadata:~n~p", [SeedMeta]),
                chronicle_storage:store_meta(Storage0, SeedMeta)
        end,

    %% Sync storage to make sure that whatever state is exposed to the outside
    %% world is durable: theoretically it's possible for agent to crash after
    %% writing an update out to storage but before making it durable. This is
    %% meant to deal with such possibility.
    chronicle_storage:sync(Storage1),
    chronicle_storage:publish(propagate_committed_seqno(Storage1)).

propagate_committed_seqno(Storage) ->
    #{committed_seqno := CommittedSeqno} = chronicle_storage:get_meta(Storage),
    chronicle_storage:set_committed_seqno(CommittedSeqno, Storage).

append_entry(Entry, Meta, #state{storage = Storage}) ->
    Seqno = Entry#log_entry.seqno,
    NewStorage = chronicle_storage:append(Storage, Seqno, Seqno,
                                          [Entry], #{meta => Meta}),
    chronicle_storage:sync(NewStorage),
    chronicle_storage:publish(propagate_committed_seqno(NewStorage)).

store_meta(Meta, #state{storage = Storage}) ->
    NewStorage = chronicle_storage:store_meta(Storage, Meta),
    chronicle_storage:sync(NewStorage),
    chronicle_storage:publish(propagate_committed_seqno(NewStorage)).

append_entries(StartSeqno, EndSeqno, Entries,
               PreMetadata, PostMetadata, Truncate,
               #state{storage = Storage}) ->
    NewStorage0 =
        case Truncate of
            true ->
                %% It's important to truncate diverged entries before logging
                %% metadata. We only truncate on the first append in a new
                %% term. So the metadata will include the new term_voted
                %% value. But if truncate happens after this metadata gets
                %% logged, if the process crashes, upon recovery we might end
                %% up with an untruncated log and the new term_voted. Which
                %% would be in violation of the following invariant: all nodes
                %% with the same term_voted agree on the longest common prefix
                %% of their histories.
                chronicle_storage:truncate(StartSeqno - 1, Storage);
            false ->
                Storage
        end,

    NewStorage1 = chronicle_storage:store_meta(NewStorage0, PreMetadata),
    NewStorage2 = chronicle_storage:append(NewStorage1, StartSeqno,
                                           EndSeqno, Entries, #{}),
    NewStorage3 = chronicle_storage:store_meta(NewStorage2, PostMetadata),
    chronicle_storage:sync(NewStorage3),
    chronicle_storage:publish(propagate_committed_seqno(NewStorage3)).

get_peer_name() ->
    Peer = ?PEER(),
    case Peer =:= ?NO_PEER of
        true ->
            exit(nodistribution);
        false ->
            Peer
    end.

get_meta(#state{storage = Storage}) ->
    chronicle_storage:get_meta(Storage).

get_meta(Key, State) ->
    maps:get(Key, get_meta(State)).

get_high_seqno(#state{storage = Storage}) ->
    chronicle_storage:get_high_seqno(Storage).

get_config(#state{storage = Storage}) ->
    chronicle_storage:get_config(Storage).

get_log_entry(Seqno, #state{storage = Storage}) ->
    chronicle_storage:get_log_entry(Seqno, Storage).
