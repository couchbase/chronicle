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

-ifdef(TEST).
-define(PEER(), vnet:vnode()).
-define(START_NAME(Name), {via, vnet, Name}).
-define(SERVER_NAME(Name), {via, vnet, Name}).
-define(SERVER_NAME(Peer, Name), {via, vnet, {Peer, Name}}).
-define(ETS_TABLE(Name), list_to_atom("ets-"
                                      ++ atom_to_list(vnet:vnode())
                                      ++ "-"
                                      ++ atom_to_list(Name))).
-else.
-define(PEER(), node()).
-define(START_NAME(Name), {local, Name}).
-define(SERVER_NAME(Name), Name).
-define(SERVER_NAME(Peer, Name), {Name, Peer}).
-define(ETS_TABLE(Name), Name).
-endif.

-define(NO_PEER, 'nonode@nohost').
-define(NO_PEER_ID, <<>>).
-define(NO_HISTORY, <<"no-history">>).
-define(NO_TERM, {0, ?NO_PEER}).
-define(NO_SEQNO, 0).

-define(COMPAT_VERSION, 0).

-record(rsm_config, { module :: module(),
                      args = [] :: list() }).

-record(rsm_command,
        { rsm_name :: atom(),
          peer_id :: chronicle:peer_id(),
          peer_incarnation :: chronicle:incarnation(),
          serial :: undefined | chronicle:serial(),
          seen_serial :: chronicle:serial(),
          payload :: noop | {command, any()} }).

-record(branch, {history_id,
                 old_history_id,
                 coordinator,
                 peers,

                 opaque :: any()}).

-record(config, { request_id :: any(),

                  compat_version :: chronicle:compat_version(),
                  lock :: undefined | binary(),
                  peers :: chronicle_config:peers(),
                  old_peers :: undefined | chronicle_config:peers(),
                  state_machines :: #{atom() => #rsm_config{}},

                  settings = #{} :: map(),

                  branch :: undefined | #branch{},
                  history_log :: chronicle:history_log() }).

-record(log_entry,
        { history_id :: chronicle:history_id(),
          term :: chronicle:leader_term(),
          seqno :: chronicle:seqno(),
          value :: noop | #config{} | #rsm_command{}}).

-record(metadata, { peer,
                    peer_id,
                    history_id,
                    term,
                    high_term,
                    high_seqno,
                    committed_seqno,
                    config,
                    committed_config,
                    pending_branch }).

-define(DEBUG(Fmt, Args), ?LOG(debug, Fmt, Args)).
-define(INFO(Fmt, Args), ?LOG(info, Fmt, Args)).
-define(WARNING(Fmt, Args), ?LOG(warning, Fmt, Args)).
-define(ERROR(Fmt, Args), ?LOG(error, Fmt, Args)).

-define(DEBUG(Msg), ?DEBUG(Msg, [])).
-define(INFO(Msg), ?INFO(Msg, [])).
-define(WARNING(Msg), ?WARNING(Msg, [])).
-define(ERROR(Msg), ?ERROR(Msg, [])).

-define(CHRONICLE_LOAD_NIFS, '$chronicle_load_nifs').
-define(WHEN_LOAD_NIFS(Body),
        %% It's impossible to load *.so files from an escript archive without
        %% lots of extra hoops. Currently I don't need this nif for
        %% chronicle_dump, so escripts will simply not even attempt to load
        %% it.
        case persistent_term:get(?CHRONICLE_LOAD_NIFS, true) of
            true ->
                Body;
            false ->
                ok
        end).

-define(CHRONICLE_LOGGER, '$chronicle_logger').
-define(CHRONICLE_STATS, '$chronicle_stats').
-define(CHRONICLE_ENCRYPT, '$chronicle_encrypt').
-define(CHRONICLE_DECRYPT, '$chronicle_decrypt').

-define(LOG(Level, Fmt, Args),
        (persistent_term:get(?CHRONICLE_LOGGER))(
          Level, Fmt, Args,
          #{file => ?FILE,
            line => ?LINE,
            module => ?MODULE,
            function => ?FUNCTION_NAME,
            arity => ?FUNCTION_ARITY})).

-define(ENCRYPT(Data), (persistent_term:get(?CHRONICLE_ENCRYPT))(Data)).
-define(DECRYPT(Data), (persistent_term:get(?CHRONICLE_DECRYPT))(Data)).

-define(CHECK(Cond1, Cond2),
        case Cond1 of
            ok ->
                Cond2;
            __Error ->
                __Error
        end).
-define(CHECK(Cond1, Cond2, Cond3),
        ?CHECK(Cond1, ?CHECK(Cond2, Cond3))).
-define(CHECK(Cond1, Cond2, Cond3, Cond4),
        ?CHECK(Cond1, ?CHECK(Cond2, Cond3, Cond4))).
-define(CHECK(Cond1, Cond2, Cond3, Cond4, Cond5),
        ?CHECK(Cond1, ?CHECK(Cond2, Cond3, Cond4, Cond5))).

-define(FLUSH(Pattern),
        (fun __Loop(__N) ->
                 receive
                     Pattern ->
                         __Loop(__N + 1)
                 after
                     0 ->
                         __N
                 end
         end)(0)).

-define(CRC_BITS, 32).
-define(CRC_BYTES, (?CRC_BITS bsr 3)).

-define(META_STATE_PROVISIONED, provisioned).
-define(META_STATE_NOT_PROVISIONED, not_provisioned).
-define(META_STATE_PREPARE_JOIN, prepare_join).
-define(META_STATE_JOIN_CLUSTER, join_cluster).
-define(META_STATE_REMOVED, removed).

-define(META_STATE, state).
-define(META_PEER, peer).
-define(META_PEER_ID, peer_id).
-define(META_HISTORY_ID, history_id).
-define(META_TERM, term).
-define(META_COMMITTED_SEQNO, committed_seqno).
-define(META_PENDING_BRANCH, pending_branch).

-define(RSM_EVENTS, chronicle_rsm_events).

-define(EXTERNAL_EVENTS, chronicle_external_events).
-define(EXTERNAL_EVENTS_SERVER, ?SERVER_NAME(?EXTERNAL_EVENTS)).

-define(HISTO_METRIC(Op), {?MODULE:get_histo_name(), [{op, Op}]}).

-define(TIME_OK(Op, StartTS),
        begin
            __EndTS = erlang:monotonic_time(?MODULE:get_histo_unit()),
            __Diff = __EndTS - StartTS,
            chronicle_stats:report_histo(
              ?HISTO_METRIC(Op), ?MODULE:get_histo_max(),
              ?MODULE:get_histo_unit(), __Diff)
        end).

-define(TIME(Op, Body), ?TIME(Op, ok, Body)).
-define(TIME(Op, OkPattern, Body),
        begin
            __StartTS = erlang:monotonic_time(?MODULE:get_histo_unit()),
            __Result = Body,
            case __Result of
                OkPattern ->
                    ?TIME_OK(Op, __StartTS);
                _ ->
                    ok
            end,

            __Result
        end).
