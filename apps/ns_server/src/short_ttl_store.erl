%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Single-node, ETS-backed short-lived key-value store.
%%      Keys and values are arbitrary terms. Each entry has a TTL.
%%      `get` is non-destructive; `take` returns and removes the value.
%%      Expired entries are removed both lazily on access and via a periodic
%%      sweep.

-module(short_ttl_store).

-behaviour(gen_server).

-include("ns_common.hrl").

-define(TIME_UNIT, millisecond).

%% API
-export([start_link/3,
         put/3,
         put/4,
         get/2,
         take/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(entry, {
                key :: term(),
                value :: term(),
                %% In practice, this will always be a non-neg integer, but we
                %% specify atom here because the record construction '$1' is
                %% treated by dialyzer as an atom.
                exp_ts :: atom() | non_neg_integer()
               }).

-record(state, {
                table :: ets:table(),
                sweep_interval_ms :: non_neg_integer(),
                default_ttl_seconds :: non_neg_integer(),
                sweep_timer_ref :: reference() | undefined
               }).

-spec start_link(atom(), non_neg_integer(), non_neg_integer()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, SweepIntervalMS, DefaultTTLSeconds) ->
    gen_server:start_link({local, Name}, ?MODULE,
                          [Name, SweepIntervalMS, DefaultTTLSeconds], []).

-spec put(atom(), term(), term()) ->
          ok | {error, exists}.
put(Name, Key, Value) ->
    gen_server:call(Name, {put, Key, Value}, infinity).

-spec put(atom(), term(), term(), pos_integer()) ->
          ok | {error, exists}.
put(Name, Key, Value, TTLSeconds) ->
    gen_server:call(Name, {put, Key, Value, TTLSeconds}, infinity).

-spec get(atom(), term()) -> {ok, term()} | not_found.
%% @doc Non-destructive lookup.
get(Name, Key) ->
    gen_server:call(Name, {get, Key}, infinity).

-spec take(atom(), term()) -> {ok, term()} | not_found.
%% @doc Lookup and remove the value under Key.
take(Name, Key) ->
    gen_server:call(Name, {take, Key}, infinity).

init([Name, SweepIntervalMS, DefaultTTLSeconds]) ->
    NameStr = atom_to_list(Name),
    Table = ets:new(list_to_atom(NameStr),
                    [protected, named_table, set,
                     {keypos, #entry.key}]),
    State0 = #state{table = Table,
                    sweep_interval_ms = SweepIntervalMS,
                    default_ttl_seconds = DefaultTTLSeconds,
                    sweep_timer_ref = undefined},
    {ok, restart_sweep_timer(State0)}.

handle_call({put, Key, Value}, _From,
            State = #state{default_ttl_seconds = DefaultTTLSeconds}) ->
    handle_call({put, Key, Value, DefaultTTLSeconds}, _From, State);
handle_call({put, Key, Value, TTLSeconds}, _From,
            #state{table = Table} = State) ->
    Now = erlang:monotonic_time(?TIME_UNIT),
    ExpTS = Now + (TTLSeconds * 1000),
    case ets:lookup(Table, Key) of
        [#entry{exp_ts = OldExp}] when OldExp >= Now ->
            {reply, {error, exists}, State};
        _ ->
            ets:insert(Table, #entry{key = Key, value = Value,
                                     exp_ts = ExpTS}),
            {reply, ok, State}
    end;
%% Non-destructive get
handle_call({get, Key}, _From,
            #state{table = Table} = State) ->
    {reply, lookup(Key, Table), State};
%% A take is a get and remove
handle_call({take, Key}, _From,
            #state{table = Table} = State) ->
    Reply = lookup(Key, Table),
    case Reply of
        {ok, Value} ->
            ets:delete(Table, Key),
            {reply, {ok, Value}, State};
        not_found ->
            {reply, not_found, State}
    end;
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, #state{table = Table} = State) ->
    misc:flush(sweep),
    Now = erlang:monotonic_time(?TIME_UNIT),
    sweep_expired(Now, Table),
    {noreply, restart_sweep_timer(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

restart_sweep_timer(#state{sweep_timer_ref = Ref} = State) when
      is_reference(Ref) ->
    erlang:cancel_timer(Ref),
    restart_sweep_timer(State#state{sweep_timer_ref = undefined});
restart_sweep_timer(#state{sweep_timer_ref = undefined,
                           sweep_interval_ms = SweepIntervalMS} = State) ->
    State#state{sweep_timer_ref = erlang:send_after(SweepIntervalMS, self(),
                                                    sweep)}.

sweep_expired(Now, Table) ->
  ets:select_delete(
    Table,
    [{#entry{exp_ts = '$1', _ = '_'},
      [{'<', '$1', Now}],
      [true]}]).

lookup(Key, Table) ->
    Now = erlang:monotonic_time(?TIME_UNIT),
    case ets:lookup(Table, Key) of
        [#entry{exp_ts = ExpTS, value = Value}] when ExpTS >= Now ->
            {ok, Value};
        [#entry{}] ->
            ets:delete(Table, Key),
            not_found;
        [] ->
            not_found
    end.
