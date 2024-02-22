%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(mru_cache).

-ifdef(TEST).
-include("triq.hrl").
-endif.

-export([new/2, dispose/1,
         lookup/2, add/3, update/3, delete/2,
         flush/1]).

new(Name, Size) ->
    ets:new(Name, [named_table, set, public]),
    Recent = ets:new(ok, [set, public]),
    Stale = ets:new(ok, [set, public]),

    true = ets:insert_new(Name, {max_size, Size}),
    true = ets:insert_new(Name, {tables, {Recent, Stale}}).

dispose(Name) ->
    {Recent, Stale} = get_tables(Name),
    ets:delete(Recent),
    ets:delete(Stale),
    ets:delete(Name).

lookup(Name, Key) ->
    time_call({Name, lookup},
              fun () ->
                      with_item_lock(
                        Name, Key,
                        fun () ->
                                {Recent, Stale} = get_tables(Name),
                                do_lookup(Name, Recent, Stale, Key)
                        end)
              end).

do_lookup(Name, Recent, Stale, Key) ->
    case get_one(Recent, Key) of
        false ->
            case get_one(Stale, Key) of
                false ->
                    ns_server_stats:notify_counter({<<"mru_cache_lookup">>,
                                                    [{name, Name},
                                                     {type, miss}]}),
                    false;
                {ok, Value} ->
                    %% the key might have been evicted (or entire cache might
                    %% have been flushed) while we waited on the lock; so we
                    %% need to be prepared
                    case migrate(Name, Recent, Stale, {Key, Value}) of
                        ok ->
                            ns_server_stats:notify_counter(
                              {<<"mru_cache_lookup">>,
                               [{name, Name}, {type, stale}]}),
                            {ok, Value};
                        not_found ->
                            ns_server_stats:notify_counter(
                              {<<"mru_cache_lookup">>,
                               [{name, Name}, {type, evicted}]}),
                            false
                    end
            end;
        {ok, Value} ->
            ns_server_stats:notify_counter({<<"mru_cache_lookup">>,
                                            [{name, Name}, {type, recent}]}),
            {ok, Value}
    end.

update(Name, Key, Value) ->
    time_call({Name, update},
              fun () ->
                      with_item_lock(
                        Name, Key,
                        fun () ->
                                {Recent, Stale} = get_tables(Name),
                                do_update(Recent, Stale, Key, Value)
                        end)
              end).

do_update(Recent, Stale, Key, Value) ->
    case update_item(Recent, Key, Value) of
        true ->
            ok;
        false ->
            case update_item(Stale, Key, Value) of
                true ->
                    ok;
                false ->
                    not_found
            end
    end.

add(Name, Key, Value) ->
    time_call({Name, add},
              fun () ->
                      with_item_lock(
                        Name, Key,
                        fun () ->
                                {Recent, Stale} = get_tables(Name),
                                do_add(Name, Recent, Stale, Key, Value)
                        end)
              end).

do_add_try_update(Name, Recent, Stale, Key, Value) ->
    case update_item(Recent, Key, Value) of
        true ->
            ok;
        false ->
            migrate(Name, Recent, Stale, {Key, Value})
    end.

do_add(Name, Recent, Stale, Key, Value) ->
    case do_add_try_update(Name, Recent, Stale, Key, Value) of
        ok ->
            false;
        not_found ->
            add_recent(Name, {Key, Value}),
            true
    end.

delete(Name, Key) ->
    time_call(
      {Name, delete},
      fun () ->
              with_item_lock(
                Name, Key,
                fun () ->
                        {Recent, Stale} = get_tables(Name),
                        case ets:member(Recent, Key) orelse ets:member(Stale, Key) of
                            true ->
                                do_delete(Name, Recent, Stale, Key),
                                ok;
                            false ->
                                not_found
                        end
                end)
      end).

do_delete(Name, Recent, Stale, Key) ->
    with_lock(
      Name, housekeeping,
      fun () ->
              ets:delete(Recent, Key),
              ets:delete(Stale, Key)
      end).

flush(Name) ->
    time_call(
      {Name, flush},
      fun () ->
              with_lock(
                Name, housekeeping,
                fun () ->
                        %% it's safe to just delete all objects from both
                        %% recent and stale tables; the concurrent operations
                        %% might end up looking as if they happened before or
                        %% after the flush; so we just need to ensure that
                        %% migrate, add_recent and do_delete do not violate
                        %% any invariants; migrate is prepared that item of
                        %% interest might have been evicted/flushed, do_delete
                        %% just blindly deletes the object from both tables,
                        %% add_recent will just insert a new item into the
                        %% recent table
                        {Recent, Stale} = get_tables(Name),
                        ets:delete_all_objects(Recent),
                        ets:delete_all_objects(Stale),
                        ok
                end)
      end).

%% internal
get_tables(Name) ->
    {ok, Tables} = get_one(Name, tables),
    Tables.

migrate(Name, Recent, Stale, {Key, _} = Item) ->
    with_lock(
      Name, housekeeping,
      fun () ->
              %% the "recent" and "stale" tables might have been swapped; but
              %% since this only happens when stale table is empty and we hold
              %% the lock for our own key, ets:member will always return false
              %% in such case, so we are safe here
              case ets:member(Stale, Key) of
                  true ->
                      %% just asserting that tables weren't swapped
                      {Recent, Stale} = get_tables(Name),

                      ets:delete(Stale, Key),
                      true = ets:insert_new(Recent, Item),
                      maybe_swap_tables(Name, Recent, Stale, Item),
                      ok;
                  false ->
                      not_found
              end
      end).

add_recent(Name, Item) ->
    with_lock(
      Name, housekeeping,
      fun () ->
              %% the "recent" and "swapped" tables might have been swapped
              %% while we were waiting for the housekeeping lock, so we cannot
              %% just use the values passed from our caller; but given we have
              %% proper tables, the logic doesn't change otherwise: since
              %% we're adding a new element, it must not have existed in any
              %% of the tables
              {Recent, Stale} = get_tables(Name),
              true = ets:insert_new(Recent, Item),

              maybe_evict(Name, Recent, Stale),
              maybe_swap_tables(Name, Recent, Stale, Item)
      end).

maybe_evict(Name, Recent, Stale) ->
    {ok, MaxSize} = get_one(Name, max_size),
    RecentSize = ets:info(Recent, size),
    StaleSize = ets:info(Stale, size),
    case RecentSize + StaleSize > MaxSize of
        true ->
            %% one of our invariants is that when we need to evict, there's at
            %% least one item in the stale table
            true = (StaleSize >= 1),
            evict(Stale);
        false ->
            ok
    end.

evict(Stale) ->
    Victim = ets:first(Stale),
    %% we are holding the housekeeping lock, so the item couldn't have been
    %% deleted or migrated
    ets:delete(Stale, Victim).

maybe_swap_tables(Name, Recent, Stale, LastItem) ->
    {ok, MaxSize} = get_one(Name, max_size),
    RecentSize = ets:info(Recent, size),

    case RecentSize =:= MaxSize of
        true ->
            true = (ets:info(Stale, size) =:= 0),
            swap_tables(Name, Recent, Stale, LastItem);
        false ->
            ok
    end.

swap_tables(Name, Recent, Stale, {Key, _} = LastItem) ->
    NewRecent = Stale,
    NewStale = Recent,

    %% we leave just the most recent item in the new recent table; in addition
    %% we need to delete it from what used to be recent
    ets:delete(NewStale, Key),
    true = ets:insert_new(NewRecent, LastItem),

    %% now actual swapping
    update_item(Name, tables, {NewRecent, NewStale}).

time_call({CacheName, CallType}, Body) ->
    {CallType2, ExtraLabels} =
        case CallType of
            {lock, LT} -> {lock, [{type, LT}]};
            CT -> {CT, []}
        end,
    BinCallType = atom_to_binary(CallType2, latin1),
    Metric = {<<"mru_cache_", BinCallType/binary, "_time">>,
              [{name, CacheName} | ExtraLabels]},
    {T, R} = timer:tc(Body),
    ns_server_stats:notify_histogram(Metric, 1000000, microsecond, T),
    R.

%% locking stuff
-define(LOCK_ITERS_FAST, 10).
-define(LOCK_INVALIDATE_ITERS, 100).

take_lock(Name, LockName) ->
    Lock = {LockName, {make_ref(), self()}},
    case take_lock_fast(Name, Lock, ?LOCK_ITERS_FAST) of
        ok ->
            ns_server_stats:notify_counter({<<"mru_cache_take_lock">>,
                                            [{name, Name}, {type, fast}]}),
            ok;
        failed ->
            ns_server_stats:notify_counter({<<"mru_cache_take_lock">>,
                                            [{name, Name}, {type, slow}]}),
            take_lock_slow(Name, LockName, Lock)
    end.

take_lock_fast(_Name, _Lock, 0) ->
    failed;
take_lock_fast(Name, Lock, Iters) ->
    case ets:insert_new(Name, Lock) of
        true ->
            ok;
        false ->
            take_lock_fast(Name, Lock, Iters - 1)
    end.


take_lock_slow(Name, LockName, Lock) ->
    Iters = ?LOCK_INVALIDATE_ITERS + rand:uniform(?LOCK_INVALIDATE_ITERS),
    MaybeCurrentLock = get_one(Name, LockName),
    case take_lock_slow_loop(Name, Lock, Iters) of
        ok ->
            ok;
        failed ->
            try_invalidate_lock(Name, LockName, MaybeCurrentLock),
            take_lock_slow(Name, LockName, Lock)
    end.

try_invalidate_lock(_, _, false) ->
    ok;
try_invalidate_lock(Name, LockName, MaybeLockValue) ->
    case get_one(Name, LockName) =:= MaybeLockValue of
        true ->
            {ok, {_, Pid} = LockValue} = MaybeLockValue,

            case is_process_alive(Pid) of
                true ->
                    ok;
                false ->
                    ns_server_stats:notify_counter(
                      {<<"mru_cache_take_lock_invalidate">>, [{name, Name}]}),
                    %% this only succeeds if the object stays the same
                    ets:delete_object(Name, {LockName, LockValue})
            end;
        false ->
            ok
    end.

take_lock_slow_loop(_, _, 0) ->
    failed;
take_lock_slow_loop(Name, Lock, Iters) ->
    case ets:insert_new(Name, Lock) of
        true ->
            ok;
        false ->
            erlang:yield(),
            take_lock_slow_loop(Name, Lock, Iters - 1)
    end.

put_lock(Name, LockName) ->
    ets:delete(Name, LockName).

with_item_lock(Name, Key, Body) ->
    with_lock(Name, item, {lock, Key}, Body).

with_lock(Name, Lock, Body) ->
    with_lock(Name, Lock, Lock, Body).

with_lock(Name, LockType, Lock, Body) ->
    time_call({Name, {lock, LockType}},
              fun () -> take_lock(Name, Lock) end),
    try
        Body()
    after
        put_lock(Name, Lock)
    end.

%% ets helpers
get_one(Table, Key) ->
    case ets:lookup(Table, Key) of
        [] ->
            false;
        [{Key, Value}] ->
            {ok, Value}
    end.

update_item(Table, Key, Value) ->
    ets:update_element(Table, Key, {2, Value}).


-ifdef(TEST).

-define(CACHE, test_cache).
-define(CACHE_SIZE, 8).
-define(KEYS, [list_to_atom([C]) || C <- lists:seq($a, $a + 2 * ?CACHE_SIZE)]).

%% generators for the cache operation
key() ->
    oneof(?KEYS).

value() ->
    int().

ops() ->
    list(op()).

ops_conc() ->
    ?LET(Ops, ops(), chunk(Ops, 5)).

chunk([], _) ->
    [];
chunk(Ops, MaxSize) ->
    ?LET(ChunkSize, int(1, MaxSize),
         begin
             {Chunk, Rest} =
                 try
                     lists:split(ChunkSize, Ops)
                 catch
                     error:badarg ->
                         {Ops, []}
                 end,
             [{concurrent, Chunk} | chunk(Rest, MaxSize)]
         end).

op() ->
    frequency([{1, op_flush()},
               {20, ?LET(Key, key(),
                         frequency([{3, op_lookup(Key)},
                                    {2, op_add(Key)},
                                    {2, op_update(Key)},
                                    {2, op_delete(Key)}]))}]).

op_lookup(Key) ->
    {lookup, Key}.

op_add(Key) ->
    {add, Key, value()}.

op_update(Key) ->
    {update, Key, value()}.

op_delete(Key) ->
    {delete, Key}.

op_flush() ->
    flush.

%% routines to run and validate operations on cache
-record(cache_state, {meta, items, stale_count, recent_count}).

get_cache_state() ->
    {Recent, Stale} = get_tables(?CACHE),
    Items = lists:sort([{K, {recent, V}} || {K, V} <- ets:tab2list(Recent)] ++
                           [{K, {stale, V}} || {K, V} <- ets:tab2list(Stale)]),
    RecentCount = ets:info(Recent, size),
    StaleCount = ets:info(Stale, size),
    #cache_state{meta = ets:tab2list(?CACHE),
                 items = Items,
                 recent_count = RecentCount,
                 stale_count = StaleCount}.

dispatch_op({concurrent, Ops}) ->
    Results =
        async:with_many(
          fun (Op) ->
                  receive race -> ok end,
                  {Op, eval_mfa(op_to_mfa(Op))}
          end, Ops,
          fun (Asyncs) ->
                  lists:foreach(
                    fun (A) ->
                            async:send(A, race)
                    end, Asyncs),
                    Results = async:wait_many(Asyncs),
                    [R || {_, R} <- Results]
          end),
    {lists:sort(Results), get_cache_state()};
dispatch_op(Op) ->
    R = eval_mfa(op_to_mfa(Op)),
    {R, get_cache_state()}.

eval_mfa({M, F, A}) ->
    erlang:apply(M, F, A).

op_to_mfa(Op) when is_atom(Op) ->
    op_to_mfa({Op});
op_to_mfa(Op) when is_tuple(Op) ->
    [F | Args] = tuple_to_list(Op),
    {mru_cache, F, [?CACHE | Args]}.

run_check_cache(Operations, Check) ->
    run_check_cache(Operations,
                    fun (Op, Result, State) ->
                            case Check(Op, Result) of
                                ok ->
                                    {ok, State};
                                Other ->
                                    Other
                            end
                    end, ignored).

run_check_cache(Operations, Check, InitState) ->
    mru_cache:new(?CACHE, ?CACHE_SIZE),
    try
        check_cache_loop(Operations, [], Check, InitState)
    after
        mru_cache:dispose(?CACHE)
    end.

check_cache_loop([], _, _, _) ->
    true;
check_cache_loop([Op | Ops], Trace, Check, State) ->
    NewTrace = [Op | Trace],

    try dispatch_op(Op) of
        Result ->
            case Check(Op, Result, State) of
                {ok, NewState} ->
                    check_cache_loop(Ops, [Op | Trace], Check, NewState);
                {failed, Diag} ->
                    {failed, [{ops, lists:reverse(NewTrace)} | Diag]}
            end
    catch
        T:E:Stack ->
            {raised, [{what, {T, E}},
                      {stack, Stack},
                      {ops, lists:reverse(NewTrace)},
                      {last_state, State},
                      {cache_state, get_cache_state()}]}
    end.

check_invariants(Operations) ->
    run_check_cache(
      Operations,
      fun (_, {_, State}) ->
              #cache_state{meta = Meta,
                           recent_count = Recent,
                           stale_count = Stale,
                           items = Items} = State,
              Invariants =
                  [{meta, (proplists:get_keys(Meta) --
                               [max_size, tables]) =:= []},
                   {max_size, Recent + Stale =< ?CACHE_SIZE},
                   {recent, Recent < ?CACHE_SIZE},
                   {no_dups, lists:sort(proplists:get_keys(Items)) =:=
                        lists:usort(proplists:get_keys(Items))}],

              Violated = [P || {_, false} = P <- Invariants],
              case Violated of
                  [] ->
                      ok;
                  _ ->
                      {failed, [{cache_state, State},
                                {violated_invariants, Violated}]}
              end
      end).


-record(model_state, {items = [], recent_count = 0, stale_count = 0}).

check_model(Operations) ->
    run_check_cache(
      Operations,
      fun (Op, Result, ModelState) ->
              ModelResult = model_op(Op, ModelState),
              model_check_results(Result, ModelResult, ModelState)
      end, #model_state{}).

model_check_results(CacheResult, ModelResult, LastState)
  when is_tuple(ModelResult) ->
    model_check_results(CacheResult, [ModelResult], LastState);
model_check_results({CacheResult, CacheState}, ModelResults, LastState) ->
    R = case lists:keyfind(CacheResult, 1, ModelResults) of
            {_, ModelStates} ->
                CacheStateAsModel = cache_state2model_state(CacheState),
                case lists:member(CacheStateAsModel, ModelStates) of
                    true ->
                        {ok, CacheStateAsModel};
                    false ->
                        false
                end;
            false ->
                false
        end,

    case R of
        {ok, _} ->
            R;
        false ->
            {failed, [{result, CacheResult},
                      {last_state, LastState},
                      {state, CacheState},
                      {model_results, ModelResults}]}
    end.

cache_state2model_state(#cache_state{items = Items,
                                     recent_count = Recent,
                                     stale_count = Stale}) ->
    #model_state{items = Items,
                 recent_count = Recent,
                 stale_count = Stale}.

model_ops([], States) ->
    [{[], States}];
model_ops([Op | Ops], States) ->
    Results = [model_simple_op(Op, S) || S <- States],

    Groupped =
        lists:foldl(
          fun ({OpR, OpStates}, [{OpR, PrevStates} | Rest]) ->
                  [{OpR, lists:umerge(OpStates, PrevStates)} | Rest];
              (P, Acc) ->
                  [P | Acc]
          end, [], lists:keysort(1, Results)),

    lists:flatmap(
      fun ({OpR, OpStates}) ->
              [{[{Op, OpR} | RestR], RestStates} ||
                  {RestR, RestStates} <- model_ops(Ops, OpStates)]
      end, Groupped).

model_op({concurrent, Ops}, State) ->
    Results =
        lists:foldl(
          fun (Perm, Acc) ->
                  PermResults0 = model_ops(Perm, [State]),
                  PermResults = [{lists:sort(Rs), Ss} || {Rs, Ss} <- PermResults0],
                  dict:merge(
                    fun (_, X, Y) ->
                            lists:umerge(X, Y)
                    end, dict:from_list(PermResults), Acc)
          end, dict:new(), misc:upermutations(Ops)),

    dict:to_list(Results);
model_op(Op, State) ->
    model_simple_op(Op, State).

model_simple_op(flush, _) ->
    model_result(ok, #model_state{});
model_simple_op({lookup, Key}, State) ->
    case model_take(Key, State) of
        false ->
            model_result(false, State);
        {ok, recent, Value, _} ->
            model_result({ok, Value}, State);
        {ok, stale, Value, NewState} ->
            NewStates = model_put(Key, Value, recent, NewState),
            model_result({ok, Value}, NewStates)
    end;
model_simple_op({update, Key, Value}, State) ->
    case model_take(Key, State) of
        false ->
            model_result(not_found, State);
        {ok, Recency, _, NewState} ->
            NewStates = model_put(Key, Value, Recency, NewState),
            model_result(ok, NewStates)
    end;
model_simple_op({add, Key, Value}, State) ->
    {R, NewState} =
        case model_take(Key, State) of
            false ->
                {true, State};
            {ok, _, _, NewState0} ->
                {false, NewState0}
        end,
    NewStates = model_put(Key, Value, recent, NewState),
    model_result(R, NewStates);
model_simple_op({delete, Key}, State) ->
    case model_take(Key, State) of
        false ->
            model_result(not_found, State);
        {ok, _, _, NewState} ->
            model_result(ok, NewState)
    end.

model_result(Result, #model_state{} = State) ->
    model_result(Result, [State]);
model_result(Result, States) when is_list(States) ->
    {Result, States}.

model_take(Key, #model_state{items = Items} = State) ->
    case lists:keytake(Key, 1, Items) of
        false ->
            false;
        {value, {_, {Recency, Value}}, NewItems} ->
            NewState = State#model_state{items = NewItems},
            {ok, Recency, Value, model_dec_count(Recency, NewState)}
    end.

model_put(Key, Value, Recency, #model_state{items = Items} = State) ->
    NewItems = lists:merge([{Key, {Recency, Value}}], Items),
    NewState = model_inc_count(Recency, State#model_state{items = NewItems}),
    EvictStates = model_maybe_evict(NewState),
    lists:usort([model_maybe_swap(S, Key) || S <- EvictStates]).

model_maybe_swap(#model_state{recent_count = ?CACHE_SIZE,
                              stale_count = 0,
                              items = Items}, LastKey) ->
    {value, {_, {recent, Value}}, RestItems} = lists:keytake(LastKey, 1, Items),
    NewItems0 =
        [{LastKey, {recent, Value}}] ++
        [{K, {stale, V}} || {K, {_, V}} <- RestItems],
    NewItems = lists:sort(NewItems0),

    NewRecent = 1,
    NewStale = ?CACHE_SIZE - 1,

    #model_state{items = NewItems,
                 recent_count = NewRecent,
                 stale_count = NewStale};
model_maybe_swap(State, _) ->
    State.

model_maybe_evict(#model_state{recent_count = Recent,
                               stale_count = Stale,
                               items = Items})
  when Recent + Stale > ?CACHE_SIZE ->
    StaleKeys = [K || {K, {stale, _}} <- Items],
    lists:map(
      fun (Key) ->
              #model_state{recent_count = Recent,
                           stale_count = Stale - 1,
                           items = lists:keydelete(Key, 1, Items)}
      end, StaleKeys);
model_maybe_evict(State) ->
    [State].

model_dec_count(Recency, State) ->
    model_update_count(Recency, -1, State).

model_inc_count(Recency, State) ->
    model_update_count(Recency, +1, State).

model_update_count(recent, D, State) ->
    do_model_update_count(#model_state.recent_count, D, State);
model_update_count(stale, D, State) ->
    do_model_update_count(#model_state.stale_count, D, State).

do_model_update_count(N, D, State) ->
    V = element(N, State),
    setelement(N, State, V + D).

%% triq properties
prop_invariants_() ->
    {?FORALL(Ops, ops(), check_invariants(Ops) =:= true),
     [{iters, 1000},
      {diag, fun check_invariants/1}]}.

prop_sequential_() ->
    {?FORALL(Ops, ops(), check_model(Ops) =:= true),
     [{iters, 1000},
      {diag, fun check_model/1}]}.

prop_invariants_concurrent_() ->
    {?FORALL(Ops, ops_conc(), check_invariants(Ops)),
     [{iters, 1000}]}.

prop_concurrent_() ->
    {?FORALL(Ops, ops_conc(), check_model(Ops)),
     [{iters, 1000}]}.
-endif.
