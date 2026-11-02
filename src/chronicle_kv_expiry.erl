%% @author Couchbase <info@couchbase.com>
%% @copyright 2026 Couchbase, Inc.
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

%% Expires chronicle_kv keys with a ttl. Only active on the leader, where
%% chronicle_kv feeds it the keys to track. Deadlines are local monotonic
%% times that are reset to a full ttl on activation, so a key is never
%% expired before its ttl has elapsed. Due keys are expired in a transaction
%% built by the RSM's kv_expire_handlers entry, see chronicle_kv:expire/3
-module(chronicle_kv_expiry).

-behavior(gen_server).

-include("chronicle.hrl").

-export([start_link/1]).
-export([activate/2, deactivate/1, track/4, untrack/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(BATCH_SIZE, chronicle_settings:get({kv_expiry, batch_size}, 1000)).
-define(RETRY_AFTER, chronicle_settings:get({kv_expiry, retry_after}, 1000)).
-define(MAX_BACKOFF, chronicle_settings:get({kv_expiry, max_backoff}, 64)).
-define(EXPIRE_TIMEOUT,
        chronicle_settings:get({kv_expiry, expire_timeout}, 15000)).

-type key() :: chronicle_kv:key().
-type ttl() :: pos_integer().
-type deadline() :: integer().
-type entry_id() :: {deadline(), integer()}.

-record(state, {
                %% RSM name to look up the correct expiry code
                name :: atom(),
                %% Track keys when active, active on leader.
                active = false :: boolean(),
                keys = #{} :: #{key() => {entry_id(),
                                          chronicle:revision(),
                                          ttl()}},
                deadlines = gb_trees:empty() :: gb_trees:tree(entry_id(),
                                                              key()),
                %% Set after a failed expiry, so writes don't skip the backoff
                retry_at :: undefined | integer(),
                %% Multiplies retry_after, doubled on each failed expiry
                backoff = 1 :: pos_integer(),
                timer :: undefined | reference(),
                expirer :: undefined | pid()}).

start_link(Name) ->
    gen_server:start_link(?START_NAME(server_name(Name)), ?MODULE, Name, []).

-spec activate(atom(), #{key() => {chronicle:revision(), ttl()}}) -> ok.
activate(Name, Keys) ->
    cast(Name, {activate, Keys}).

-spec deactivate(atom()) -> ok.
deactivate(Name) ->
    cast(Name, deactivate).

-spec track(atom(), key(), chronicle:revision(), ttl()) -> ok.
track(Name, Key, Revision, TTL) ->
    cast(Name, {track, Key, Revision, TTL}).

-spec untrack(atom(), key()) -> ok.
untrack(Name, Key) ->
    cast(Name, {untrack, Key}).

%% gen_server callbacks
init(Name) ->
    {ok, #state{name = Name}}.


handle_call(Call, _From, State) ->
    %% todo ?
    ?WARNING("Unexpected call: ~p", [Call]),
    {reply, nack, State}.


handle_cast({activate, Keys}, State) ->
    Now = now_ms(),
    NewState =
        maps:fold(
          fun (Key, {Revision, TTL}, Acc) ->
                  add_key(Key, Revision, TTL, Now, Acc)
          end, (reset(State))#state{active = true}, Keys),
    {noreply, schedule(NewState)};

handle_cast(deactivate, State) ->
    {noreply, reset(State)};

handle_cast({track, _, _, _}, #state{active = false} = State) ->
    {noreply, State};
handle_cast({track, Key, Revision, TTL}, #state{active = true} = State) ->
    NewState = add_key(Key, Revision, TTL, now_ms(), State),
    {noreply, schedule(NewState)};

handle_cast({untrack, _}, #state{active = false} = State) ->
    {noreply, State};
handle_cast({untrack, Key}, #state{active = true} = State) ->
    {noreply, schedule(remove_key(Key, State))};

handle_cast(Cast, State) ->
    ?WARNING("Unexpected cast: ~p", [Cast]),
    {noreply, State}.


handle_info({timeout, TRef, expire}, #state{timer = TRef} = State) ->
    {noreply, maybe_expire(State#state{timer = undefined})};
handle_info({timeout, _, expire}, State) ->
    %% Stale timer
    {noreply, State};

handle_info({expire_result, Pid, Batch, Result},
            #state{expirer = Pid} = State) ->
    {noreply, handle_expire_result(Batch, Result,
                                   State#state{expirer = undefined})};

handle_info({'EXIT', Pid, normal}, State) when is_pid(Pid) ->
    {noreply, State};
handle_info({'EXIT', Pid, Reason}, #state{expirer = Pid} = State) ->
    ?WARNING("Expirer ~p died: ~p", [Pid, Reason]),
    {noreply, schedule_retry(State#state{expirer = undefined})};

handle_info(Msg, State) ->
    ?WARNING("Unexpected message: ~p", [Msg]),
    {noreply, State}.


%% internal
server_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "-expiry").

cast(Name, Msg) ->
    gen_server:cast(?SERVER_NAME(server_name(Name)), Msg).

now_ms() ->
    erlang:monotonic_time(millisecond).

reset(State) ->
    NewState = cancel_timer(stop_expirer(State)),
    NewState#state{active = false,
                   keys = #{},
                   deadlines = gb_trees:empty(),
                   retry_at = undefined,
                   backoff = 1}.

add_key(Key, Revision, TTL, Now, State) ->
    #state{keys = Keys, deadlines = Deadlines} = remove_key(Key, State),
    %% Tree keys must be unique, add the integer wrapper
    Id = {Now + TTL, erlang:unique_integer()},
    State#state{keys = Keys#{Key => {Id, Revision, TTL}},
                deadlines = gb_trees:insert(Id, Key, Deadlines)}.

remove_key(Key, #state{keys = Keys, deadlines = Deadlines} = State) ->
    case maps:take(Key, Keys) of
        {{Id, _, _}, NewKeys} ->
            State#state{keys = NewKeys,
                        deadlines = gb_trees:delete(Id, Deadlines)};
        error ->
            State
    end.

next_deadline(#state{deadlines = Deadlines}) ->
    case gb_trees:is_empty(Deadlines) of
        true ->
            none;
        false ->
            {{Deadline, _}, _} = gb_trees:smallest(Deadlines),
            Deadline
    end.

next_expiry(#state{retry_at = RetryAt} = State) ->
    case {next_deadline(State), RetryAt} of
        {none, _} ->
            none;
        {Deadline, undefined} ->
            Deadline;
        {Deadline, _} ->
            max(Deadline, RetryAt)
    end.

schedule(#state{active = false} = State) ->
    State;
schedule(#state{expirer = Pid} = State) when is_pid(Pid) ->
    %% Rescheduled once the in-flight expiry completes
    State;
schedule(State) ->
    schedule_at(next_expiry(State), State).

schedule_retry(#state{backoff = Backoff} = State) ->
    schedule(State#state{retry_at = now_ms() + ?RETRY_AFTER * Backoff,
                         backoff = chronicle_utils:next_backoff(
                                     Backoff, ?MAX_BACKOFF)}).

schedule_at(none, State) ->
    cancel_timer(State);
schedule_at(Time, State) ->
    NewState = cancel_timer(State),
    TRef = erlang:start_timer(Time, self(), expire, [{abs, true}]),
    NewState#state{timer = TRef}.

cancel_timer(#state{timer = undefined} = State) ->
    State;
cancel_timer(#state{timer = TRef} = State) ->
    _ = erlang:cancel_timer(TRef),
    State#state{timer = undefined}.

maybe_expire(State) ->
    case due_keys(State) of
        [] ->
            schedule(State);
        Batch ->
            State#state{expirer = spawn_expirer(Batch, State)}
    end.

due_keys(#state{keys = Keys, deadlines = Deadlines}) ->
    Iter = gb_trees:iterator(Deadlines),
    due_keys_loop(Iter, now_ms(), ?BATCH_SIZE, Keys, []).

due_keys_loop(_Iter, _Now, 0, _Keys, Acc) ->
    Acc;
due_keys_loop(Iter, Now, Remaining, Keys, Acc) ->
    case gb_trees:next(Iter) of
        {{Deadline, _}, Key, NewIter} when Deadline =< Now ->
            {_, Revision, _} = maps:get(Key, Keys),
            due_keys_loop(NewIter, Now, Remaining - 1, Keys,
                          [{Key, Revision} | Acc]);
        _ ->
            Acc
    end.

spawn_expirer(Batch, #state{name = Name}) ->
    Self = self(),
    proc_lib:spawn_link(
      fun () ->
              Result =
                  try chronicle_kv:expire(Name, Batch, ?EXPIRE_TIMEOUT)
                  catch
                      T:E ->
                          {error, {T, E}}
                  end,
              Self ! {expire_result, self(), Batch, Result}
      end).

stop_expirer(#state{expirer = undefined} = State) ->
    State;
stop_expirer(#state{expirer = Pid} = State) ->
    chronicle_utils:terminate_linked_process(Pid, shutdown),
    ?FLUSH({expire_result, Pid, _, _}),
    State#state{expirer = undefined}.

handle_expire_result(Batch, {ok, _Revision}, State) ->
    %% Keys that were rewritten in the meantime carry a new revision and
    %% stay tracked
    NewState =
        lists:foldl(
          fun ({Key, Revision}, Acc) ->
                  case maps:find(Key, Acc#state.keys) of
                      {ok, {_, Revision, _}} ->
                          remove_key(Key, Acc);
                      _ ->
                          Acc
                  end
          end, State, Batch),
    maybe_expire(NewState#state{retry_at = undefined, backoff = 1});
handle_expire_result(Batch, Error, State) ->
    log_expire_failure(Batch, Error, State),
    %% The batch keeps its deadlines, so is retried before any later keys
    schedule_retry(State).

%% Only repeated failures push the backoff to its max
log_expire_failure(Batch, Error, #state{backoff = Backoff}) ->
    case Backoff >= ?MAX_BACKOFF of
        true ->
            ?ERROR("Failed to expire ~b keys at max backoff: ~p",
                   [length(Batch), Error]);
        false ->
            ?WARNING("Failed to expire ~b keys: ~p", [length(Batch), Error])
    end.
