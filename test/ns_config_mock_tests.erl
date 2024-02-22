%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ns_config_mock_tests).
-include_lib("eunit/include/eunit.hrl").
-include("ns_config.hrl").
-compile(nowarn_export_all).
-compile(export_all).

%% This module contains test that mock ns_config, using meck.
%% Mocking a module involves purging the mocked module from time to time.
%% See http://erlang.org/doc/man/code.html#purge-1.
%%
%% Purging a module will kill any processes which "executes" the old code in the
%% old module. Read http://erlang.org/doc/man/erlang.html#check_process_code-3
%% for a slightly more precise definition of what execute means in this context.
%%
%% In the context of running eunit tests, it means that tests which mock a
%% module named `mod` must not be written in `mod.erl` or `mod_tests.erl`.
%%
%% If the tests are written in `mod`, the test process will be farily obviously
%% be running the old code when things are purged at the end of the test -- causing
%% the test to die miserably and somewhat inscrutably. If the mock tests are
%% written in mod_tests - the tests when run alone will be fine. But when run as
%% part of a larger eunit suite including the tests in `mod`, eunit will run
%% the tests bundled with the tests in `mod` with a similar, unpleasant outcome.
%%
%% So, that's why the mock tests for ns_config are written in this module.

all_test_() ->
    {foreach, fun do_setup/0, fun do_teardown/1,
     [{"test_basic", fun test_basic/0},
      {"test_set", fun test_set/0},
      {"test_cas_config", fun test_cas_config/0},
      {"test_update", fun test_update/0}]}.

do_setup() ->
    ok = meck:new(ns_config, [passthrough]),
    ok = meck:expect(ns_config, init, fun([]) -> {ok, {}} end),
    ns_config:mock_tombstone_agent(),
    {ok, _} = gen_server:start_link({local, ns_config}, ns_config, [], []),
    ok.

shutdown_process(Name) ->
    OldWaitFlag = erlang:process_flag(trap_exit, true),
    try
        Pid = whereis(Name),
        exit(Pid, shutdown),
        receive
            {'EXIT', Pid, _} -> ok
        end
    catch Kind:What ->
            io:format("Ignoring ~p:~p while shutting down ~p~n", [Kind, What, Name])
    end,
    erlang:process_flag(trap_exit, OldWaitFlag).

do_teardown(_V) ->
    shutdown_process(ns_config),
    ns_config:unmock_tombstone_agent(),
    meck:unload().

test_basic() ->
    F = fun () -> ok end,
    ok = meck:expect(ns_config, handle_call,
                     fun({update_with_changes, Fun}, _From, _State) ->
                             {reply, Fun, {}}
                     end),
    ?assertEqual(F, gen_server:call(ns_config, {update_with_changes, F})).

-define(assertConfigEquals(A, B),
        ?assertEqual(lists:sort([{K, ns_config:strip_metadata(V)} || {K,V} <- A]),
                     lists:sort([{K, ns_config:strip_metadata(V)} || {K,V} <- B]))).

test_set() ->
    Self = self(),
    meck:expect(ns_config, handle_call,
                fun({update_with_changes, _} = Msg, _From, _State) ->
                        Self ! Msg,
                        {reply, ok, {}}
                end),
    ns_config:set(test, 1),
    Updater0 = (fun () -> receive {update_with_changes, F} -> F end end)(),

    ?assertConfigEquals([{test, 1}], element(2, Updater0([], <<"uuid">>))),
    {[{test, [{'_vclock', _} | 1]}], Val2} = Updater0([{foo, 2}], <<"uuid">>),
    ?assertConfigEquals([{test, 1}, {foo, 2}], Val2),

    %% and here we're changing value, so expecting vclock
    {[{test, [{'_vclock', [_]} | 1]}], Val3} =
        Updater0([{foo, [{k, 1}, {v, 2}]},
                  {xar, true},
                  {test, [{a, b}, {c, d}]}], <<"uuid">>),

    ?assertConfigEquals([{foo, [{k, 1}, {v, 2}]},
                         {xar, true},
                         {test, 1}], Val3),

    SetVal1 = [{suba, true}, {subb, false}],
    ns_config:set(test, SetVal1),
    Updater1 = (fun () -> receive {update_with_changes, F} -> F end end)(),

    {[{test, SetVal1Actual1}], Val4} = Updater1([{test, [{suba, false}, {subb, true}]}], <<"uuid2">>),
    ?assertMatch([{'_vclock', [{<<"uuid2">>, _}]} | SetVal1], SetVal1Actual1),
    ?assertEqual(SetVal1, ns_config:strip_metadata(SetVal1Actual1)),
    ?assertMatch([{test, SetVal1Actual1}], Val4).

test_cas_config() ->
    Self = self(),
    {ok, _FakeConfigEvents} = gen_event:start_link({local, ns_config_events}),
    try
        do_test_cas_config(Self)
    after
        (catch shutdown_process(ns_config_events)),
        (catch erlang:unregister(ns_config_events))
    end.

do_test_cas_config(Self) ->
    meck:expect(ns_config, handle_call,
                fun({cas_config, _, _, _, _} = Msg, _From, _State) ->
                        Self ! Msg,
                        {reply, ok, {}}
                end),

    ets:new(ns_config_announces_counter, [set, named_table]),
    ets:insert_new(ns_config_announces_counter, {changes_counter, 0}),

    (catch ets:new(ns_config_ets_dup, [public, set, named_table])),

    ns_config:cas_remote_config(new, [], old),
    receive
        {cas_config, new, [], old, _} ->
            ok
    after 0 ->
            exit(missing_cas_config_msg)
    end,

    Config = #config{dynamic=[[{a,1},{b,1}]],
                     saver_mfa = {?MODULE, send_config, [Self]},
                     saver_pid = Self,
                     pending_more_save = true},
    DynamicConfig = ns_config:get_kv_list_with_config(Config),

    ?assertEqual([{a,1},{b,1}], DynamicConfig),

    meck:delete(ns_config, handle_call, 3),
    {reply, true, NewConfig} = ns_config:handle_call({cas_config, [{a,2}], [],
                                                      DynamicConfig, remote}, [], Config),
    NewDynamicConfig = ns_config:get_kv_list_with_config(NewConfig),
    ?assertEqual(NewConfig, Config#config{dynamic=[NewDynamicConfig]}),
    ?assertEqual([{a,2}], NewDynamicConfig),
    {reply, false, NewConfig} = ns_config:handle_call({cas_config, [{a,3}], [],
                                                       DynamicConfig, remote}, [], NewConfig).

test_update() ->
    Self = self(),
    meck:expect(ns_config, handle_call,
                fun({update_with_changes, _Fun} = Msg, _From, _State) ->
                        Self ! Msg,
                        {reply, {ok, unused}, {}}
                end),
    RecvUpdater = fun () ->
                          receive
                              {update_with_changes, F} -> F
                          end
                  end,

    OldConfig = [{dont_change, 1},
                 {erase, 2},
                 {list_value, [{'_vclock', [{'n@never-really-possible-hostname', {1, 12345}}]},
                               {a, b}, {c, d}]},
                 {a, 3},
                 {b, 4},
                 {delete, 5}],
    ns_config:update(fun ({dont_change, _}) ->
                             skip;
                         ({erase, _}) ->
                             erase;
                         ({list_value, V}) ->
                             {update, {list_value, [V | V]}};
                         ({delete, _}) ->
                             delete;
                         ({K, V}) ->
                             {update, {K, -V}}
                     end),
    Updater = RecvUpdater(),
    {Changes, Erased, NewConfig, _} = Updater(OldConfig, <<"uuid">>),

    ?assertEqual(Erased, [erase]),
    ?assertConfigEquals(Changes ++ [{dont_change, 1}],
                        NewConfig),
    ?assertEqual(lists:keyfind(dont_change, 1, Changes), false),

    ?assertEqual(lists:sort([dont_change, list_value, a, b, delete]), lists:sort(proplists:get_keys(NewConfig))),

    {list_value, [{'_vclock', Clocks} | ListValues]} = lists:keyfind(list_value, 1, NewConfig),

    ?assertEqual({'n@never-really-possible-hostname', {1, 12345}},
                 lists:keyfind('n@never-really-possible-hostname', 1, Clocks)),
    ?assertMatch([{<<"uuid">>, _}], lists:keydelete('n@never-really-possible-hostname', 1, Clocks)),

    ?assertEqual([[{a, b}, {c, d}], {a, b}, {c, d}], ListValues),

    ?assertEqual(-3, ns_config:strip_metadata(proplists:get_value(a, NewConfig))),
    ?assertEqual(-4, ns_config:strip_metadata(proplists:get_value(b, NewConfig))),

    ?assertMatch({0, [{<<"uuid">>, _}]},
                 ns_config:extract_vclock(proplists:get_value(a, NewConfig))),
    ?assertMatch({0, [{<<"uuid">>, _}]},
                 ns_config:extract_vclock(proplists:get_value(b, NewConfig))),
    ?assertMatch({0, [{<<"uuid">>, _}]},
                 ns_config:extract_vclock(proplists:get_value(delete,
                                                              NewConfig))),

    ?assertEqual(false, ns_config:search([NewConfig], delete)),

    ns_config:update_key(a, fun (3) -> 10 end),
    Updater2 = RecvUpdater(),
    {[{a, [{'_vclock', [_]} | 10]}], NewConfig2} = Updater2(OldConfig, <<"uuid">>),

    ?assertConfigEquals([{a, 10} | lists:keydelete(a, 1, OldConfig)], NewConfig2),
    ok.

