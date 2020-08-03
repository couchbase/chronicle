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
%% TODO: timeouts
%% TODO: pull is needed to detect situations where a node gets shunned
%% TODO: check more state invariants
-module(chronicle_agent).

-compile(export_all).

-export_type([provision_result/0]).

-behavior(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-include("chronicle.hrl").

-import(chronicle_utils, [call_async/3,
                          term_number/1,
                          compare_positions/2]).

-define(SERVER, ?SERVER_NAME(?MODULE)).
-define(SERVER(Peer), ?SERVER_NAME(Peer, ?MODULE)).

-define(ESTABLISH_LOCAL_TERM_TIMEOUT, 10000).
-define(LOCAL_MARK_COMMITTED_TIMEOUT, 5000).

%% Used to indicate that a function will send a message with the provided Tag
%% back to the caller when the result is ready. And the result type is
%% _ReplyType. This is entirely useless for dializer, but is usefull for
%% documentation purposes.
-type replies(Tag, _ReplyType) :: Tag.

%% TODO: get rid of the duplication between #state{} and #metadata{}.
-record(state, { history_id,
                 term,
                 term_voted,
                 high_seqno,
                 committed_seqno,
                 config_entry,
                 committed_config_entry,
                 pending_branch,

                 log_tab }).

start_link() ->
    gen_server:start_link(?START_NAME(?MODULE), ?MODULE, [], []).

-spec monitor(chronicle:peer()) -> reference().
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
    get_metadata(?PEER()).

-spec get_metadata(chronicle:peer()) -> get_metadata_result().
get_metadata(Peer) ->
    gen_server:call(?SERVER(Peer), get_metadata).

get_log(Peer) ->
    gen_server:call(?SERVER(Peer), get_log).

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
    case gen_server:call(?SERVER, {provision, Machines}) of
        ok ->
            ok = chronicle_secondary_sup:sync();
        Other ->
            Other
    end.

-spec wipe() -> ok.
wipe() ->
    case gen_server:call(?SERVER, wipe) of
        ok ->
            ok = chronicle_secondary_sup:sync();
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

-spec establish_term(chronicle:peer(),
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

-spec ensure_term(chronicle:peer(),
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

-spec append(chronicle:peer(),
             Opaque,
             chronicle:history_id(),
             chronicle:leader_term(),
             chronicle:seqno(),
             [#log_entry{}]) ->
          replies(Opaque, append_result()).
append(Peer, Opaque, HistoryId, Term, CommittedSeqno, Entries) ->
    call_async(?SERVER(Peer), Opaque,
               {append, HistoryId, Term, CommittedSeqno, Entries}).

-spec local_mark_committed(chronicle:history_id(),
                           chronicle:leader_term(),
                           chronicle:seqno()) ->
          append_result().
local_mark_committed(HistoryId, Term, CommittedSeqno) ->
    gen_server:call(?SERVER,
                    {append, HistoryId, Term, CommittedSeqno, []},
                    ?LOCAL_MARK_COMMITTED_TIMEOUT).

-spec store_branch(chronicle:peer(), #branch{}) ->
          {ok, #metadata{}} |
          {error, {concurrent_branch, OurBranch::#branch{}}}.
store_branch(Peer, Branch) ->
    gen_server:call(?SERVER(Peer), {store_branch, Branch}).


-spec undo_branch(chronicle:peer(), chronicle:history_id()) ->
          ok | {error, Error} when
      Error :: no_branch |
               {bad_branch, OurBranch::#branch{}}.
undo_branch(Peer, BranchId) ->
    gen_server:call(?SERVER(Peer), {undo_branch, BranchId}).

%% gen_server callbacks
init([]) ->
    {ok, restore_state()}.

handle_call(get_metadata, _From, State) ->
    handle_get_metadata(State);
handle_call(get_log, _From, #state{log_tab = Tab} = State) ->
    {reply, {ok, ets:tab2list(Tab)}, State};
handle_call({get_log, HistoryId, Term, StartSeqno, EndSeqno}, _From, State) ->
    handle_get_log(HistoryId, Term, StartSeqno, EndSeqno, State);
handle_call({provision, Machines}, _From, State) ->
    handle_provision(Machines, State);
handle_call(wipe, _From, State) ->
    handle_wipe(State);
handle_call({establish_term, HistoryId, Term}, _From,
            #state{term_voted = TermVoted, high_seqno = HighSeqno} = State) ->

    %% TODO: consider simply skipping the position check for this case
    Position = {TermVoted, HighSeqno},
    handle_establish_term(HistoryId, Term, Position, State);
handle_call({establish_term, HistoryId, Term, Position}, _From, State) ->
    handle_establish_term(HistoryId, Term, Position, State);
handle_call({ensure_term, HistoryId, Term}, _From, State) ->
    handle_ensure_term(HistoryId, Term, State);
handle_call({append, HistoryId, Term, CommittedSeqno, Entries}, _From, State) ->
    handle_append(HistoryId, Term, CommittedSeqno, Entries, State);
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
    Reply =
        case is_provisioned(State) of
            true ->
                {ok, state2metadata(State)};
            false ->
                {error, not_provisioned}
        end,

    {reply, Reply, State}.

state2metadata(#state{history_id = HistoryId,
                      term = Term,
                      term_voted = TermVoted,
                      high_seqno = HighSeqno,
                      committed_seqno = CommittedSeqno,
                      config_entry = ConfigEntry,
                      pending_branch = PendingBranch}) ->
    {Config, ConfigRevision} =
        case ConfigEntry of
            undefined ->
                {undefined, undefined};
            #log_entry{value = Value} ->
                {Value, chronicle_proposer:log_entry_revision(ConfigEntry)}
        end,

    #metadata{history_id = HistoryId,
              term = Term,
              term_voted = TermVoted,
              high_seqno = HighSeqno,
              committed_seqno  = CommittedSeqno,
              config = Config,
              config_revision = ConfigRevision,
              pending_branch = PendingBranch}.

handle_get_log(HistoryId, Term, StartSeqno, EndSeqno, State) ->
    case check_get_log(HistoryId, Term, StartSeqno, EndSeqno, State) of
        ok ->
            {reply, {ok, log_range(StartSeqno, EndSeqno, State)}, State};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_get_log(HistoryId, Term, StartSeqno, EndSeqno, State) ->
    ?CHECK(check_history_id(HistoryId, State),
           check_same_term(Term, State),
           check_log_range(StartSeqno, EndSeqno, State)).


handle_provision(Machines0, State) ->
    case check_not_provisioned(State) of
        ok ->
            HistoryId = chronicle_utils:random_uuid(),
            Term = ?NO_TERM,
            Seqno = 1,

            Machines = maps:from_list(
                         [{Name, #rsm_config{module = Module, args = Args}} ||
                             {Name, Module, Args} <- Machines0]),

            Config = #config{voters = [?PEER()], state_machines = Machines},

            LogEntry = #log_entry{history_id = HistoryId,
                                  term = Term,
                                  seqno = Seqno,
                                  value = Config},

            ?DEBUG("Provisioning with history ~p. Config:~n~p",
                   [HistoryId, Config]),

            handle_append(HistoryId, Term, Seqno, [LogEntry], State);
        {error, _} = Error ->
            {reply, Error, State}
    end.

is_provisioned(#state{history_id = HistoryId}) ->
    HistoryId =/= ?NO_HISTORY.

check_not_provisioned(State) ->
    case is_provisioned(State) of
        true ->
            {error, already_provisioned};
        false ->
            ok
    end.

handle_wipe(State) ->
    NewState = State#state{history_id = ?NO_HISTORY,
                           term = ?NO_TERM,
                           term_voted = ?NO_TERM,
                           high_seqno = ?NO_SEQNO,
                           committed_seqno = ?NO_SEQNO,
                           config_entry = undefined,
                           committed_config_entry = undefined,
                           pending_branch = undefined},
    log_wipe(State),
    persist_state(NewState),
    announce_system_state(unprovisioned),
    ?DEBUG("Wiped successfully", []),
    {reply, ok, NewState}.

handle_establish_term(HistoryId, Term, Position, State) ->
    assert_valid_history_id(HistoryId),
    assert_valid_term(Term),

    case check_establish_term(HistoryId, Term, Position, State) of
        ok ->
            NewState = State#state{term = Term},
            persist_state(NewState),
            announce_term_established(Term),
            announce_metadata(NewState),
            ?DEBUG("Accepted term ~p in history ~p", [Term, HistoryId]),
            {reply, {ok, state2metadata(State)}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_establish_term(HistoryId, Term, Position, State) ->
    ?CHECK(check_history_id(HistoryId, State),
           check_later_term(Term, State),
           check_peer_current(Position, State)).

check_later_term(Term, #state{term = CurrentTerm}) ->
    case term_number(Term) > term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

check_peer_current(Position, State) ->
    #state{term_voted = OurTermVoted,
           high_seqno = OurHighSeqno} = State,
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

handle_append(HistoryId, Term, CommittedSeqno, Entries, State) ->
    assert_valid_history_id(HistoryId),
    assert_valid_term(Term),

    case check_append(HistoryId, Term, CommittedSeqno, Entries, State) of
        {ok, Info} ->
            complete_append(HistoryId, Term, Info, State);
        {error, _} = Error ->
            {reply, Error, State}
    end.

maybe_promote_config(#state{committed_seqno = CommittedSeqno,
                            config_entry = ConfigEntry} = State) ->
    case ConfigEntry =:= undefined orelse
        ConfigEntry#log_entry.seqno > CommittedSeqno of
        true ->
            State;
        false ->
            State#state{committed_config_entry = ConfigEntry}
    end.

maybe_demote_config(#state{committed_seqno = CommittedSeqno,
                           config_entry = ConfigEntry} = State) ->
    case ConfigEntry =:= undefined
        orelse ConfigEntry#log_entry.seqno =< CommittedSeqno of
        true ->
            State;
        false ->
            #state{committed_config_entry = CommittedConfigEntry} = State,
            State#state{config_entry = CommittedConfigEntry}
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

maybe_update_config(Entries,
                    #state{committed_seqno = CommittedSeqno} = State) ->
    %% Promote current pending config to committed state if necessary.
    NewState0 = maybe_promote_config(State),

    {CommittedEntries, PendingEntries} =
        lists:splitwith(fun (#log_entry{seqno = Seqno}) ->
                                Seqno =< CommittedSeqno
                        end, Entries),

    NewState1 =
        case extract_latest_config(CommittedEntries) of
            false ->
                NewState0;
            CommittedConfig ->
                NewState0#state{config_entry = CommittedConfig,
                                committed_config_entry = CommittedConfig}
        end,

    case extract_latest_config(PendingEntries) of
        false ->
            NewState1;
        PendingConfig ->
            NewState1#state{config_entry = PendingConfig}
    end.

complete_append(HistoryId, Term, Info,
                #state{committed_seqno = OurCommittedSeqno} = State) ->
    #{entries := Entries,
      high_seqno := NewHighSeqno,
      committed_seqno := NewCommittedSeqno,
      truncate_uncommitted := TruncateUncommitted} = Info,

    NewState0 =
        case TruncateUncommitted of
            true ->
                %% TODO: this MUST happen atomically with the following append
                log_truncate(OurCommittedSeqno + 1, State),
                maybe_demote_config(State);
            false ->
                State
        end,

    log_append(Entries, NewState0),

    NewState1 = NewState0#state{history_id = HistoryId,
                                term = Term,
                                term_voted = Term,
                                committed_seqno = NewCommittedSeqno,
                                high_seqno = NewHighSeqno,
                                pending_branch = undefined},
    NewState = maybe_update_config(Entries, NewState1),
    persist_state(NewState),

    %% Announce if we got provisioned.
    case is_provisioned(State) of
        true ->
            ok;
        false ->
            announce_system_state(provisioned, state2metadata(NewState))
    end,

    maybe_announce_term_established(Term, State),
    maybe_announce_new_config(State, NewState),
    announce_metadata(NewState),

    ?DEBUG("Appended entries.~n"
           "History id: ~p~n"
           "Term: ~p~n"
           "High Seqno: ~p~n"
           "Committed Seqno: ~p~n"
           "Entries: ~p~n"
           "Config: ~p",
           [HistoryId, Term, NewHighSeqno,
            NewCommittedSeqno, Entries, NewState#state.config_entry]),

    {reply, ok, NewState}.

check_append(HistoryId, Term, CommittedSeqno, Entries, State) ->
    ?CHECK(check_history_id(HistoryId, State),
           check_not_earlier_term(Term, State),
           check_append_obsessive(Term, CommittedSeqno, Entries, State)).

check_append_obsessive(Term, CommittedSeqno, Entries, State) ->
    case get_entries_seqnos(Entries, State) of
        {ok, StartSeqno, EndSeqno} ->
            #state{term_voted = OurTermVoted,
                   high_seqno = OurHighSeqno,
                   committed_seqno = OurCommittedSeqno} = State,

            {SafeHighSeqno, Truncate} =
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
                                    %% TODO: validate that the entries we're
                                    %% dropping are the same that we've got?
                                    FinalEntries =
                                        lists:dropwhile(
                                          fun (#log_entry{seqno = Seqno}) ->
                                                  Seqno =< SafeHighSeqno
                                          end, Entries),

                                    {ok,
                                     #{entries => FinalEntries,
                                       high_seqno => EndSeqno,
                                       committed_seqno => FinalCommittedSeqno,
                                       truncate_uncommitted => Truncate}};
                                {error, _} = Error ->
                                    Error
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

get_entries_seqnos([], #state{high_seqno = HighSeqno}) ->
    {ok, HighSeqno + 1, HighSeqno};
get_entries_seqnos([_|_] = Entries, _State) ->
    get_entries_seqnos(Entries, undefined, undefined).

get_entries_seqnos([], StartSeqno, EndSeqno) ->
    {ok, StartSeqno, EndSeqno};
get_entries_seqnos([Entry|Rest], StartSeqno, EndSeqno) ->
    Seqno = Entry#log_entry.seqno,

    if
        EndSeqno =:= undefined ->
            get_entries_seqnos(Rest, Seqno, Seqno);
        Seqno =:= EndSeqno + 1 ->
            get_entries_seqnos(Rest, StartSeqno, Seqno);
        true ->
            {error, {malformed, Entry}}
    end.

check_committed_seqno(Term, CommittedSeqno, HighSeqno, State) ->
    ?CHECK(check_committed_seqno_known(CommittedSeqno, HighSeqno, State),
           check_committed_seqno_rollback(Term, CommittedSeqno, State)).

check_committed_seqno_rollback(Term, CommittedSeqno,
                               #state{term_voted = OurTermVoted,
                                      committed_seqno = OurCommittedSeqno}) ->
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

check_not_earlier_term(Term, #state{term = CurrentTerm}) ->
    case term_number(Term) >= term_number(CurrentTerm) of
        true ->
            ok;
        false ->
            {error, {conflicting_term, CurrentTerm}}
    end.

handle_store_branch(Branch, State) ->
    assert_valid_branch(Branch),

    case check_compatible_branch(Branch, State) of
        ok ->
            %% NOTE: not updating history id here, so that if the node
            %% is unprovisioned, it gets back to that state upon
            %% rollback
            %%
            %% TODO: think more if we need to support
            %% branching that involves unprovisioned nodes
            NewState = State#state{pending_branch = Branch},
            persist_state(NewState),

            case State#state.pending_branch of
                undefined ->
                    %% New branch, announce history change.
                    announce_new_history(NewState);
                _ ->
                    ok
            end,
            announce_metadata(NewState),

            ?DEBUG("Stored a branch record:~n~p", [Branch]),

            %% TODO: returing all metadata here for now, but that might be an
            %% overkill.
            {reply, {ok, state2metadata(State)}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_compatible_branch(NewBranch, #state{pending_branch = PendingBranch}) ->
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

handle_undo_branch(BranchId, State) ->
    assert_valid_history_id(BranchId),
    case check_branch_id(BranchId, State) of
        ok ->
            NewState = State#state{pending_branch = undefined},
            persist_state(NewState),
            announce_new_history(NewState),
            announce_metadata(NewState),

            ?DEBUG("Undid branch ~p", [BranchId]),
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

check_branch_id(BranchId, #state{pending_branch = OurBranch}) ->
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
    %% TODO: I might need to explicitly prime the history instead of letting
    %% the ?NO_HISTORY be overwritten by any history. That prevents situations
    %% where a node is removed and reinitializes itself with no history. But
    %% some stale and rogue coordinator still in the cluster establishes a
    %% term with the remove node and essentially screws with its state.
    case OurHistoryId =:= ?NO_HISTORY orelse HistoryId =:= OurHistoryId of
        true ->
            ok;
        false ->
            {error, {history_mismatch, OurHistoryId}}
    end.

%% TODO: get rid of this once #state{} doesn't duplicate #metadata{}.
get_history_id_int(#state{history_id = CommittedHistoryId,
                          pending_branch = undefined}) ->
    CommittedHistoryId;
get_history_id_int(#state{pending_branch =
                              #branch{history_id = PendingHistoryId}}) ->
    PendingHistoryId.

check_same_term(Term, #state{term = OurTerm}) ->
    case Term =:= OurTerm of
        true ->
            ok;
        false ->
            {error, {conflicting_term, OurTerm}}
    end.

check_log_range(StartSeqno, EndSeqno, #state{high_seqno = HighSeqno}) ->
    case StartSeqno > HighSeqno
        orelse EndSeqno > HighSeqno
        orelse StartSeqno > EndSeqno of
        true ->
            {error, bad_range};
        false ->
            ok
    end.

persist_state(#state{log_tab = Tab} = State) ->
    case get_state_path() of
        undefined ->
            ok;
        {ok, StatePath} ->
            ok = filelib:ensure_dir(StatePath),
            ok = write_file(StatePath,
                            fun (File) ->
                                    %% Get rid of references in the state and
                                    %% log entries, because those can't be
                                    %% read using file:consult(). But we don't
                                    %% need them.
                                    CleanState = State#state{log_tab = undefined},
                                    Entries = [term_to_binary(Entry) || Entry <- ets:tab2list(Tab)],

                                    io:format(File,
                                              "~w.~n"
                                              "~w.~n",
                                              [CleanState, Entries])
                            end)
    end.

write_file(Path, Body) ->
    TmpPath = Path ++ ".tmp",
    case file:open(TmpPath, [write]) of
        {ok, F} ->
            try Body(F) of
                ok ->
                    file:close(F),
                    file:rename(TmpPath, Path);
                Other ->
                    Other
            after
                (catch file:close(F))
            end;
        Error ->
            Error
    end.

restore_state() ->
    Log = log_create(),
    State = #state{history_id = ?NO_HISTORY,
                   term = ?NO_TERM,
                   term_voted = ?NO_TERM,
                   high_seqno = ?NO_SEQNO,
                   committed_seqno = ?NO_SEQNO,
                   config_entry = undefined,
                   committed_config_entry = undefined,
                   pending_branch = undefined,
                   log_tab = Log},

    case get_state_path() of
        undefined ->
            State;
        {ok, StatePath} ->
            case file:consult(StatePath) of
                {ok, [RestoredState0, EntriesBinaries]} ->
                    Entries = [binary_to_term(Entry) || Entry <- EntriesBinaries],
                    RestoredState = RestoredState0#state{log_tab = Log},
                    true = is_list(Entries),
                    log_append(Entries, RestoredState),
                    RestoredState;
                _ ->
                    State
            end
    end.

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

maybe_announce_term_established(Term, #state{term = OldTerm}) ->
    case Term =:= OldTerm of
        true ->
            ok;
        false ->
            announce_term_established(Term)
    end.

announce_term_established(Term) ->
    chronicle_events:sync_notify({term_established, Term}).

maybe_announce_new_config(#state{config_entry = OldConfigEntry},
                          #state{config_entry = NewConfigEntry} = NewState) ->
    case OldConfigEntry =:= NewConfigEntry of
        true ->
            ok;
        false ->
            Metadata = state2metadata(NewState),
            Config = NewConfigEntry#log_entry.value,
            chronicle_events:sync_notify({new_config, Config, Metadata})
    end.

announce_metadata(State) ->
    chronicle_events:sync_notify({metadata, state2metadata(State)}).

announce_system_state(SystemState) ->
    announce_system_state(SystemState, no_extra).

announce_system_state(SystemState, Extra) ->
    chronicle_events:sync_notify({system_state, SystemState, Extra}).

log_create() ->
    ets:new(log, [protected, ordered_set, {keypos, #log_entry.seqno}]).

log_append(Entries, #state{log_tab = Tab}) ->
    true = ets:insert_new(Tab, Entries).

log_range(StartSeqno, EndSeqno, #state{log_tab = Tab}) ->
    MatchSpec = ets:fun2ms(fun (#log_entry{seqno = EntrySeqno} = Entry)
                                 when EntrySeqno >= StartSeqno,
                                      EntrySeqno =< EndSeqno ->
                                   Entry
                           end),
    ets:select(Tab, MatchSpec).

log_truncate(Seqno, #state{committed_seqno = CommittedSeqno, log_tab = Tab}) ->
    %% Assert we're not truncating anything committed.
    true = (Seqno > CommittedSeqno),

    MatchSpec = ets:fun2ms(fun (#log_entry{seqno = EntrySeqno})
                                 when EntrySeqno >= Seqno ->
                                   true
                           end),
    ets:select_delete(Tab, MatchSpec).

log_wipe(#state{log_tab = Tab}) ->
    ets:delete_all_objects(Tab).
