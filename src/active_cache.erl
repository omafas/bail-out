%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(active_cache).

-behaviour(gen_server2).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-callback init([term()]) -> ok | {stop, Reason :: term()} | ignore.
-callback translate_options(term()) -> [{atom(), term()}].

%% API
-export([start_link/4,
         update_and_get_value/3,
         get_value_and_touch/3,
         get_value/3,
         reload_opts/2,
         renew_cache/1,
         flush/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ns_common.hrl").

-define(MIN_CLEANUP_INTERVAL, 1000).
-define(MAX_CLEANUP_INTERVAL, 600000).
-define(REQ_TIMEOUT, 30000).

-record(s, {
    table_name,
    max_size,
    max_parallel_procs,
    cleanup_timer_ref,
    renew_timer_ref,
    value_lifetime,
    renew_interval,
    module,
    generation_ref,
    cache_exceptions
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name, Module, Args, Opts) ->
    gen_server2:start_link({local, Name}, ?MODULE,
                          [Name, Module, Args, Opts], []).

update_and_get_value(Name, Key, GetValue) ->
    Res = gen_server2:call(Name, {force_update, Key, GetValue}, ?REQ_TIMEOUT),
    case Res of
        {ok, Value} -> Value;
        {exception, {C, E, ST}} -> erlang:raise(C, E, ST)
    end.

get_value_and_touch(Name, Key, GetValue) ->
    Res = get_value(Name, Key, GetValue),
    %% That value might be deleted or rewritten by that moment
    %% but we should be ok with that
    ets:update_element(Name, Key, {3, timestamp()}),
    Res.

get_value(Name, Key, GetValue) ->
    Res =
        case ets:lookup(Name, Key) of
            [{_, V, _, _}] -> V;
            [] -> gen_server2:call(Name, {cache, Key, GetValue}, ?REQ_TIMEOUT)
        end,
    case Res of
        {ok, Value} -> Value;
        {exception, {C, E, ST}} -> erlang:raise(C, E, ST)
    end.

reload_opts(Name, Opts) ->
    gen_server2:cast(Name, {reload_opts, Opts}).

flush(Name) ->
    gen_server2:call(Name, flush).

renew_cache(Name) ->
    Name ! renew.

%%%===================================================================
%%% gen_server2 callbacks
%%%===================================================================

init([Name, Module, Args, Opts]) ->
    case Module:init(Args) of
        ok ->
            ets:new(Name, [set, named_table, public]),
            MaxSize = proplists:get_value(max_size, Opts, 100),
            MaxParallel = proplists:get_value(max_parallel_procs, Opts, 100),
            ValueLifetime = proplists:get_value(value_lifetime, Opts, 1000),
            RenewInterval = proplists:get_value(renew_interval, Opts, infinity),
            CacheExceptions = proplists:get_value(cache_exceptions, Opts, true),
            S = #s{table_name = Name,
                   max_size = MaxSize,
                   max_parallel_procs = MaxParallel,
                   value_lifetime = ValueLifetime,
                   renew_interval = RenewInterval,
                   module = Module,
                   generation_ref = erlang:make_ref(),
                   cache_exceptions = CacheExceptions},
            {ok, restart_renew_timer(restart_cleanup_timer(S))};
        {stop, Reason} -> {stop, Reason};
        ignore -> ignore
    end.

handle_call({cache, Key, GetValue}, From, #s{table_name = Name} = State) ->
    case ets:lookup(Name, Key) of
        [] -> {noreply, handle_req(Key, GetValue, From, State)};
        [{_, V, _, _}] -> {reply, V, State}
    end;

handle_call({force_update, Key, GetValue}, From, #s{} = State) ->
    {noreply, handle_req(Key, GetValue, From, State)};

handle_call(flush, _From, State) ->
    {reply, ok, clean(State)};

handle_call(Request, _From, State) ->
    {reply, {unhandled, Request}, State}.

handle_cast({reload_opts, Opts}, #s{module = Module} = State) ->
    Opts2 = Module:translate_options(Opts),
    {noreply, reconfigure(Opts2, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(renew, State) ->
    misc:flush(renew),
    {noreply, restart_renew_timer(renew(State))};

handle_info(cleanup, #s{table_name = Name,
                        value_lifetime = Lifetime} = State) ->
    misc:flush(cleanup),
    cleanup(Name, Lifetime),
    {noreply, restart_cleanup_timer(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

reconfigure(Opts, State) ->
    NewState =
        lists:foldl(fun ({max_size, V}, S) ->
                            S#s{max_size = V};
                        ({value_lifetime, V}, S) ->
                            restart_cleanup_timer(S#s{value_lifetime = V});
                        ({renew_interval, V}, S) ->
                            restart_renew_timer(S#s{renew_interval = V});
                        ({max_parallel_procs, V}, S) ->
                            S#s{max_parallel_procs = V}
                    end, State, Opts),
    clean(NewState).

clean(#s{table_name = Name} = State) ->
    ets:delete_all_objects(Name),
    ?log_debug("Clearing the ~p cache", [Name]),
    State#s{generation_ref = erlang:make_ref()}.

timestamp() -> erlang:monotonic_time(millisecond).

handle_req(Key, GetValue, From, #s{max_parallel_procs = MaxParallel,
                                   generation_ref = Ref} = State) ->
    Job = fun () -> worker(Ref, Key, GetValue) end,
    gen_server2:async_job(erlang:phash2(Key, MaxParallel), Key,
                          Job, fun (Res, S) -> handle_res(Res, From, S) end),
    State.

worker(Ref, Key, GetValue) ->
    Res = try
              {ok, GetValue()}
          catch
              C:E:S -> {exception, {C, E, S}}
          end,
    {Ref, Key, GetValue, Res}.

handle_res({Ref, Key, _GetValue, {exception, Exception} = Res}, From,
           #s{generation_ref = Ref, cache_exceptions = false} = State) ->
    case From of
        undefined ->
            ?log_error("Cache renew exception for key ~p:~n~p",
                       [Key, Exception]);
        _ ->
            gen_server2:reply(From, Res)
    end,
    {noreply, State};

handle_res({Ref, Key, GetValue, Res}, From, #s{table_name = Name,
                                               generation_ref = Ref} = State) ->
    maybe_evict(State),
    case ets:update_element(Name, Key, {2, Res}) of
        false when From =/= undefined ->
            ets:insert(Name, {Key, Res, timestamp(), GetValue});
        _ -> ok
    end,

    case Res of
        {exception, Exception} when From =:= undefined ->
            ?log_error("Cache renew exception for key ~p:~n~p",
                       [Key, Exception]);
        _ -> ok
    end,

    From == undefined orelse gen_server2:reply(From, Res),
    {noreply, State};

handle_res({_, Key, GetValue, _}, From, State) ->
    %% Received result with wrong generation_ref
    %% It means the request was spawned before the opts change. In order to
    %% get proper result we spawn new job for that request
    {noreply, handle_req(Key, GetValue, From, State)}.

renew(#s{table_name = Name} = State) ->
    ?log_debug("Starting ~p cache renewal", [Name]),
    {N, StateRes} =
        ets:foldl(
          fun ({Key, _, _, GetValue}, {K, StateAcc}) ->
                  {K + 1, handle_req(Key, GetValue, undefined, StateAcc)}
          end, {0, State}, Name),
    ?log_debug("~p cache renewal process originated ~p queries", [Name, N]),
    StateRes.

cleanup(Name, Lifetime) ->
    TS = timestamp(),
    Size = ets:info(Name, size),
    Deleted1 = ets:select_delete(Name,
                                 [{{'_', '_', '$1', '_'},
                                  [{'=<', '$1', TS - Lifetime}],
                                  [true]}]),
    Deleted2 = ets:select_delete(Name,
                                 [{{'_', {exception, '_'}, '$1', '_'},
                                  [{'=<', '$1', TS - ?MIN_CLEANUP_INTERVAL}],
                                  [true]}]),
    ?log_debug("Cache ~p cleanup: ~p/~p records deleted",
               [Name, Deleted1 + Deleted2, Size]),
    ok.

maybe_evict(#s{max_size = MaxSize, table_name = Name}) ->
    case ets:info(Name, size) >= MaxSize of
        true ->
            ?log_debug("Cache ~p is full. Evicting one record", [Name]),
            ets:delete(Name, ets:first(Name));
        false -> ok
    end.

restart_renew_timer(#s{renew_timer_ref = Ref,
                       renew_interval = Timeout} = State) ->
    Ref =/= undefined andalso erlang:cancel_timer(Ref),
    case Timeout of
        infinity -> State;
        _ ->
            NewRef = erlang:send_after(Timeout, self(), renew),
            State#s{renew_timer_ref = NewRef}
    end.

restart_cleanup_timer(#s{cleanup_timer_ref = Ref,
                         value_lifetime = ValLifetime} = State) ->
    Ref =/= undefined andalso erlang:cancel_timer(Ref),
    Timeout = min(max(?MIN_CLEANUP_INTERVAL, ValLifetime div 4),
                  ?MAX_CLEANUP_INTERVAL),
    NewRef = erlang:send_after(Timeout, self(), cleanup),
    State#s{cleanup_timer_ref = NewRef}.


-ifdef(TEST).
basic_cache_test() ->
    with_cache_settings(
      test_cache, [],
      fun () ->
          ?assertEqual(1, get_value(test_cache, key1, fun () -> 1 end)),
          ?assertEqual(2, get_value(test_cache, key2, fun () -> 2 end)),
          ?assertEqual(1, get_value(test_cache, key1, fun () -> 2 end)),
          ?assertError(test, get_value(test_cache, key3,
                                       fun () -> erlang:error(test) end))
      end).

cache_queues_test() ->
    with_cache_settings(
      test_cache, [{max_parallel_procs, 2}],
      fun () ->
          Self = self(),
          Ref = erlang:make_ref(),
          ?assertEqual(0, erlang:phash2(key4, 2)),
          ?assertEqual(1, erlang:phash2(key5, 2)),
          ?assertEqual(0, erlang:phash2(key6, 2)),
          ?assertEqual(0, erlang:phash2(key8, 2)),
          P1 = spawn(
                 fun () -> get_value(test_cache, key4,
                                     fun () ->
                                             Self ! {Ref, 0},
                                             timer:sleep(3000),
                                             Self ! {Ref, 1}
                                     end)
                 end),
          receive {Ref, N0} -> ?assertEqual(0, N0) end,
          P2 = spawn(
                 fun () -> get_value(test_cache, key6,
                                     fun () -> Self ! {Ref, 2} end) end),
          P3 = spawn(
                 fun () -> get_value(test_cache, key5,
                                     fun () -> Self ! {Ref, 3} end) end),
          receive {Ref, N1} -> ?assertEqual(3, N1) end,
          receive {Ref, N2} -> ?assertEqual(1, N2) end,
          receive {Ref, N3} -> ?assertEqual(2, N3) end,
          misc:wait_for_process(P1, 10000),
          misc:wait_for_process(P2, 10000),
          misc:wait_for_process(P3, 10000)
      end).

cache_max_size_test() ->
    with_cache_settings(
      test_cache, [{max_size, 3}],
      fun () ->
          ?assertEqual(1, get_value(test_cache, key1, fun () -> 1 end)),
          ?assertEqual(2, get_value(test_cache, key2, fun () -> 2 end)),
          ?assertEqual(3, get_value(test_cache, key3, fun () -> 3 end)),
          ?assertEqual(4, get_value(test_cache, key4, fun () -> 4 end)),
          ?assertEqual(3, ets:info(test_cache, size))
      end).

chaos_test_() ->
    {timeout, 50, fun chaos_testing/0}.

chaos_testing() ->
    NumReq = 10000,
    NumProcs = 50,
    with_cache_settings(
      test_cache, [{max_parallel_procs, 40}, {max_size, 499},
                   {renew_interval, 1000}, {value_lifetime, 1000}],
      fun () ->
          List = [rand:uniform(500) || _ <- lists:seq(1, NumReq)],
          Self = self(),
          Ref = make_ref(),
          F = fun () ->
                      Res = [get_value_and_touch(test_cache, K, fun () -> K end)
                                || K <- List],
                      Self ! {Ref, self(), List == Res}
              end,
          Procs = [spawn(F) || _ <- lists:seq(1, NumProcs)],
          [misc:wait_for_process(P, 60000) || P <- Procs],
          lists:foreach(fun (P) ->
                                receive
                                    {Ref, P, Res} -> ?assertEqual(true, Res)
                                after 0 -> erlang:error(no_message)
                                end
                        end, Procs),
          ok
      end).

active_cache_test() ->
    with_cache_settings(
      test_cache, [{renew_interval, 100}, {value_lifetime, 10000}],
      fun () ->
          Self = self(),
          Ref = make_ref(),
          get_value(test_cache, key1, fun () -> Self ! Ref end),
          receive Ref -> ok end,
          timer:sleep(1000),
          receive Ref -> ok end,
          misc:flush(Ref)
      end).

cleanup_test() ->
    with_cache_settings(
      test_cache, [{renew_interval, infinity}, {value_lifetime, 100}],
      fun () ->
          get_value(test_cache, key1, fun () -> 1 end),
          ?assertEqual(1, ets:info(test_cache, size)),
          cleanup(test_cache, 0),
          ?assertEqual(0, ets:info(test_cache, size))
      end).

change_opts_test() ->
    with_cache_settings(
      test_cache, [{renew_interval, infinity}, {max_parallel_procs, 2}],
      fun () ->
          Self = self(),
          Ref = make_ref(),
          P = spawn(
                fun () ->
                       get_value(test_cache, key1,
                                 fun () ->
                                        Self ! Ref,
                                        timer:sleep(1000),
                                        Self ! Ref
                                 end)
                end),
          receive Ref -> ok end,
          reload_opts(test_cache, [{max_parallel_procs, 3}]),
          ok = misc:wait_for_process(P, 3000),
          receive Ref -> ok end,
          %% One more Ref means worker was restarted
          receive Ref -> ok end,
          misc:flush(Ref)
      end).

cache_exceptions_test() ->
    with_cache_settings(
      test_cache, [{cache_exceptions, true}],
      fun () ->
          R1 = rand:uniform(10000),
          R2 = rand:uniform(10000),
          try
              false = get_value(test_cache, key1,
                                fun () -> exit({test_exception, R1}) end)
          catch
              exit:{test_exception, N1} -> ?assertEqual(N1, R1)
          end,
          try
              false = get_value(test_cache, key1,
                                fun () -> exit({test_exception, R2}) end)
          catch
              exit:{test_exception, N2} -> ?assertEqual(N2, R1)
          end
      end).

dont_cache_exceptions_test() ->
    with_cache_settings(
      test_cache, [{cache_exceptions, false}],
      fun () ->
          R1 = rand:uniform(10000),
          R2 = rand:uniform(10000),
          try
              false = get_value(test_cache, key1,
                                fun () -> exit({test_exception, R1}) end)
          catch
              exit:{test_exception, N1} -> ?assertEqual(N1, R1)
          end,
          try
              false = get_value(test_cache, key1,
                                fun () -> exit({test_exception, R2}) end)
          catch
              exit:{test_exception, N2} -> ?assertEqual(N2, R2)
          end
      end).

with_cache_settings(Name, Settings, Fun) ->
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, read_key_fast,
                fun (Key, Default) ->
                    proplists:get_value(Key, Settings, Default)
                end),
    meck:new(ns_pubsub, [passthrough]),
    meck:expect(ns_pubsub, subscribe_link, fun (_, _) -> ok end),
    meck:new(test_mod, [non_strict]),
    meck:expect(test_mod, init, fun (_) -> ok end),
    meck:expect(test_mod, translate_options, fun (Opts) -> Opts end),
    {ok, Pid} = start_link(Name, test_mod, [], Settings),
    try
        Fun()
    after
        erlang:unlink(Pid),
        misc:terminate_and_wait(Pid, shutdown),
        true = meck:validate(ns_config),
        meck:unload(test_mod),
        meck:unload(ns_pubsub),
        meck:unload(ns_config)
    end.
-endif.
