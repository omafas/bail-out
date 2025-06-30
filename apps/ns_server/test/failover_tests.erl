%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc
%% A set of tests for failover/auto-failover behaviours.
-module(failover_tests).

-include("ns_test.hrl").
-include("ns_common.hrl").

-include_lib("eunit/include/eunit.hrl").

manual_failover_test_() ->
    Nodes = #{
              'a' => {active, [kv]},
              'b' => {active, [kv]},
              'c' => {active, [kv]}
             },
    SetupArgs =
        #{nodes => Nodes,
          buckets => ["default"]},

    Tests = [
             {"Manual failover",
              fun manual_failover_t/2},
             {"Manual failover data loss due to stale config",
              fun manual_failover_post_network_partition_stale_config/2},
             {"Manual failover incorrect expected topology",
              fun manual_failover_incorrect_expected_topology/2}],

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun manual_failover_test_setup/1,
     fun failover_test_teardown/2,
     [{SetupArgs, fun(T, R) ->
                          {Name, ?_test(TestFun(T, R))}
                  end} || {Name, TestFun} <- Tests]}.

manual_failover_test_setup(SetupConfig) ->
    config_profile:load_default_profile_for_test(),
    fake_ns_config:setup(),
    fake_chronicle_kv:setup(),

    fake_ns_config:setup_cluster_compat_version(?LATEST_VERSION_NUM),
    fake_chronicle_kv:setup_cluster_compat_version(?LATEST_VERSION_NUM),

    fake_config_helpers:setup_node_config(maps:get(nodes, SetupConfig)),
    fake_config_helpers:setup_bucket_config(maps:get(buckets, SetupConfig)),

    meck:new(chronicle),
    meck:expect(chronicle, check_quorum, fun() -> true end),

    meck:new(chronicle_master, [passthrough]),
    meck:expect(chronicle_master, deactivate_nodes,
                fun(Nodes) ->
                        ns_cluster_membership:deactivate(
                          Nodes,
                          fun(Fun) ->
                                  chronicle_kv:transaction(kv, [], Fun),
                                  ok
                          end)
                end),

    %% Some tests will establish an expectation on chronicle_compat, but we set
    %% it up and tear it down via the usual means in case the test fails.
    meck:new(chronicle_compat, [passthrough]),

    %% Test setups return a map of pids for later shutdown in the teardown
    mock_helpers:setup_mocks([testconditions, leader_activities,
                              auto_reprovision,
                              janitor_agent]).

failover_test_teardown(_Config, PidMap) ->
    mock_helpers:shutdown_processes(PidMap),

    meck:unload(),

    fake_chronicle_kv:teardown(),
    fake_ns_config:teardown(),
    config_profile:unload_profile_for_test().

manual_failover_t(_SetupConfig, _R) ->
    ?assertEqual({error,not_found},
                 chronicle_compat:get(counters, #{})),

    %% We are failing over all but node 'c' here.
    NodesToFailOver = ['a', 'b'],

    meck:expect(chronicle_compat, pull, 1, ok),

    %% Need to trap the exit of the failover process to avoid nuking the test
    %% process when it exits.
    erlang:process_flag(trap_exit, true),

    Options = #{
                allow_unsafe => false,
                %% auto failover
                auto => false,
                failover_reasons => "ok"
               },

    ?debugMsg("Starting failover test"),

    {ok, FailoverPid} = failover:start(NodesToFailOver, Options),

    %% Failover runs in a different process, wait for it to stop
    misc:wait_for_process(FailoverPid, 1000),

    erlang:process_flag(trap_exit, false),

    %% We should have gathered a quorum for the failover.
    ?assert(meck:called(leader_activities, run_activity,
                        [failover, majority, '_', '_'])),

    %% We should have completed the failover.
    Counters = chronicle_compat:get(counters, #{required => true}),
    ?assertNotEqual(undefined,
                    proplists:get_value(failover_complete, Counters)).


%% Test post-network partition that we do not allow failover of nodes due to a
%% stale config.
%%
%% Consider a scenario in which we have a network partition with as follows:
%%
%% Partition A - [a, b]
%% Partition B - [c]
%%
%% In which [a, b, c] are KV nodes.
%%
%% N.B. there can be multiple orchestrators, particularly during network
%% partitions. In this case we expect each side of a network partition to have
%% an orchestrator node provided the network is partitioned for long enough.
%%
%% In such a scenario, partition A may try to fail over all of the nodes in
%% partition B, and partition B may try to fail over all of the nodes in
%% partition A. It is allowed and acceptable to fail over either partition,
%% provided that we have 2 or more replicas. As partition A has a quorum, and
%% partition B does not, the nodes in partition B will be failed over by the
%% orchestrator of partition A. When the network partition heals, one of the
%% orchestrator nodes will yield to another. In this case the orchestrator of
%% partition A will yield to the orchestrator of partition B.
%%
%% The orchestrator of partition B will have a config prior to the network
%% partition (as no material change is possible without an orchestrator) config
%% for a brief period of time. This, combined with a lengthy (in machine time)
%% quorum gathering timeout of 15 seconds lead to a scenario in which the
%% orchestrator from partition B would pass the failover safety check
%% (preventing the removal of the final KV node) with the stale config, before
%% gathering a quorum and proceeding to fail over all of the KV nodes in the
%% cluster.
%%
%% This test tests that in such a scenario we check that the failover is
%% possible after gathering the quorum and syncing the config.
manual_failover_post_network_partition_stale_config(SetupConfig, _R) ->
    %% We are failing over all but node 'c' here.
    NodesToFailOver = ['a', 'b'],

    %% On config sync we find our updates config
    meck:expect(chronicle_compat, pull,
                fun(_) ->
                        %% Now sync the config and we realise that 'c' has
                        %% actually been failed over
                        OldNodes = maps:get(nodes, SetupConfig),
                        NewNodes = maps:put('c', {inactiveFailed, [kv]},
                                            OldNodes),

                        fake_config_helpers:setup_node_config(NewNodes),
                        fake_config_helpers:setup_bucket_config(
                          maps:get(buckets, SetupConfig)),
                        ok
                end),

    %% Need to trap the exit of the failover process to avoid nuking the test
    %% process when it exits.
    erlang:process_flag(trap_exit, true),

    Options = #{
                allow_unsafe => false,
                %% auto failover
                auto => false,
                failover_reasons => "ok"
               },

    ?debugMsg("Starting failover test"),

    {ok, FailoverPid} = failover:start(NodesToFailOver, Options),

    %% Failover runs in a different process, wait for it to stop
    misc:wait_for_process(FailoverPid, 1000),

    erlang:process_flag(trap_exit, false),

    %% This is the test, we must receive an 'EXIT' from FailoverPid with reason
    %% last_node (which means that we did not perform a failover).
    receive
        {'EXIT', FailoverPid, last_node} ->
            ok
    after 1000 ->
            exit(timeout)
    end,

    %% We should have gathered a quorum for the failover.
    ?assert(meck:called(leader_activities, run_activity,
                        [failover, majority, '_', '_'])).

get_counter(Name) ->
    Counters = chronicle_compat:get(counters, #{required => true}),
    case proplists:get_value(Name, Counters) of
        undefined ->
            0;
        {_, Value} ->
            Value
    end.

get_failover_complete_count() ->
    get_counter(failover_complete).

manual_failover_incorrect_expected_topology(_SetupConfig, _R) ->
    ?log_info("Starting manual failover incorrect expected topology test"),
    %% Need to trap the exit of the failover processes to avoid nuking the test
    %% process when it exits.
    erlang:process_flag(trap_exit, true),

    %% Cannot failover with incorrect topologies, missing some nodes
    ?assertEqual(
       expected_topology_mismatch,
       failover:start(['a'], #{expected_topology => #{active => ['a']}})),

    %% And now we have the correct active nodes, but did not specify the other
    %% fields. The REST API will default these to an empty list if any one
    %% parameter is set, but we should still test this behaviour.
    ?assertEqual(
       expected_topology_mismatch,
       failover:start(['a'],
                      #{expected_topology => #{active => ['a', 'b', 'c']}})),

    %% Invalid node in addition to the others
    ?assertEqual(
       expected_topology_mismatch,
       failover:start(['a'],
                      #{expected_topology =>
                            #{active => ['a', 'b', 'c', 'not_valid'],
                              inactiveFailed => [],
                              inactiveAdded => []}})),

    %% And now we can fail over.
    {ok, FailoverPid} =
        failover:start(['a'],
                       #{expected_topology => #{active => ['a', 'b', 'c'],
                                                inactiveFailed => [],
                                                inactiveAdded => []}}),

    %% Failover runs in a different process, wait for it to stop
    misc:wait_for_process(FailoverPid, 1000),

    ?assertEqual(1, get_failover_complete_count()),

    %% Now, this is where it matters. Can we prevent the failover of b when we
    %% think that a is still active?
    ?assertEqual(
       expected_topology_mismatch,
       failover:start(['b'], #{expected_topology => #{active => ['a', 'b', 'c'],
                                                      inactiveFailed => [],
                                                      inactiveAdded => []}})),

    %% Sanity check for inactiveAdded too
    ?assertEqual(
       expected_topology_mismatch,
       failover:start(['b'],
                      #{expected_topology => #{active => ['b', 'c'],
                                               inactiveFailed => ['a'],
                                               inactiveAdded => ['d']}})),

    %% And we can continue to fail over another node.
    {ok, FailoverPid2} =
        failover:start(['b'], #{expected_topology => #{active => ['b', 'c'],
                                                       inactiveFailed => ['a'],
                                                       inactiveAdded => []}}),

    %% Failover runs in a different process, wait for it to stop
    misc:wait_for_process(FailoverPid2, 1000),

    ?assertEqual(2, get_failover_complete_count()),

    erlang:process_flag(trap_exit, false).

add_nodes_to_setup_config(
    #{healthy_nodes := HealthyNodes,
      unhealthy_nodes := UnhealthyNodes} = SetupConfig) ->

    Nodes = lists:foldl(
              fun({Node, Services}, Acc) ->
                      Acc#{ Node => {active, Services}}
              end, #{}, HealthyNodes ++ UnhealthyNodes),

    SetupConfig#{nodes => Nodes}.

build_setup_config(SetupArgs) ->
    add_nodes_to_setup_config(SetupArgs).

basic_auto_failover_test_config() ->
    #{buckets => ["default"],
      healthy_nodes => [{'a', [kv]}, {'b', [kv]}],
      unhealthy_nodes => [{'c', [kv]}]}.

index_auto_failover_test_config() ->
    #{buckets => ["default"],
      healthy_nodes => [{'a', [kv]}, {'b', [index]}],
      unhealthy_nodes => [{'c', [index]}]}.

auto_failover_test_() ->
    Tests = [
             {"Auto failover",
              fun auto_failover_t/2,
              basic_auto_failover_test_config()},
             {"Auto failover async",
              fun auto_failover_async_t/2,
              basic_auto_failover_test_config()},
             {"Enable auto failover test",
              fun enable_auto_failover_test/2,
              basic_auto_failover_test_config()},
             {"Index safety check failure test",
              fun auto_failover_index_safety_check_failure_t/2,
              index_auto_failover_test_config()}
            ],

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun auto_failover_test_setup/1,
     fun failover_test_teardown/2,
     [{build_setup_config(SetupArgs),
       fun(T, R) ->
               {Name, ?_test(TestFun(T, R))}
       end} || {Name, TestFun, SetupArgs} <- Tests]}.

auto_failover_with_partition_test_() ->
    PartitionA = [{'a', [kv]}, {'b', [kv]}, {'q', [query]}],
    PartitionB = [{'c', [kv]}, {'d', [kv]}],

    Nodes = lists:foldl(
              fun({Node, Services}, Acc) ->
                      Acc#{ Node => {active, Services}}
              end, #{}, PartitionA ++ PartitionB),

    Buckets = ["default"],
    SetupArgs =
        #{nodes => Nodes,
          buckets => Buckets,
          %% The test will see the partition that had the quorum as down and
          %% attempt to fail it over, not realising that the partition without
          %% quorum had already been failed over.
          unhealthy_nodes => PartitionA,
          partition_without_quorum => PartitionB},

    Tests = [
             {"Auto failover post network partition stale config test",
              fun auto_failover_post_network_partition_stale_config/2}
            ],

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun auto_failover_test_setup/1,
     fun failover_test_teardown/2,
     [{SetupArgs, fun(T, R) ->
                          {Name, ?_test(TestFun(T, R))}
                  end} || {Name, TestFun} <- Tests]}.

auto_failover_test_setup(SetupConfig) ->
    Pids = manual_failover_test_setup(SetupConfig),

    %% Config for auto_failover, disabled by default. We will manually enable it
    %% or work around that in our tests. We disable it by default to avoid ticks
    %% that we may not want to handle.
    fake_ns_config:update_snapshot(
      [{auto_failover_cfg,
        [{enabled, false},
         {timeout, 1},
         {count, 0},
         {max_count, 5},
         {failover_preserve_durability_majority, true}]}]),

    meck:new(node_status_analyzer),
    meck:expect(node_status_analyzer, get_statuses,
                fun() ->
                        lists:foldl(
                          fun({Node, _Services}, Acc) ->
                                  dict:store(Node, {unhealthy, foo}, Acc)
                          end,
                          dict:new(),
                          maps:get(unhealthy_nodes, SetupConfig))
                end),

    %% May be required if the test tries to send an email alert (and wants to
    %% see that this has happened).
    meck:new(ns_email_alert, [passthrough]),

    mock_helpers:setup_mocks([compat_mode_manager,
                              ns_orchestrator,
                              auto_failover], Pids).

get_auto_failover_reported_errors(AutoFailoverPid) ->
    sets:to_list(
      auto_failover:get_errors_from_state(sys:get_state(AutoFailoverPid))).

get_auto_failover_tick_period(AutoFailoverPid) ->
    auto_failover:get_tick_period_from_state(sys:get_state(AutoFailoverPid)).

perform_auto_failover(AutoFailoverPid) ->
    %% Override tick period. This lets us tick auto_failover as few times as
    %% possible in the test as we essentially don't have to wait for nodes to
    %% be in a down state for n ticks at any point.
    fake_ns_config:update_snapshot(auto_failover_tick_period, 100000),
    AutoFailoverPid ! tick_period_updated,

    Config = auto_failover:get_cfg(),
    MaxCount = proplists:get_value(max_count, Config, 5),
    auto_failover:enable(1, MaxCount, []),

    %% Part of our test, we should not have any reported errors yet.
    ?assertEqual([],
                 get_auto_failover_reported_errors(AutoFailoverPid)),

    %% Tick auto-failover 4 times. We could wait long enough to do the auto
    %% failover but we can speed this test up a bit by manually ticking. This
    %% amount of ticks should be the minimum to process the auto-failover.
    lists:foreach(
      fun(_) ->
              AutoFailoverPid ! tick
      end,
      lists:seq(0, 3)).

perform_auto_failover_and_poll_counter(AutoFailoverPid, Counter, Value) ->
    perform_auto_failover(AutoFailoverPid),
    ?assert(mock_helpers:poll_for_counter_value(Counter, Value)).

auto_failover_t(_SetupConfig, PidMap) ->
    #{auto_failover := AutoFailoverPid} = PidMap,

    perform_auto_failover_and_poll_counter(AutoFailoverPid, failover_complete,
                                           1),

    %% Should not see any auto-failover errors
    ?assertEqual([],
                 get_auto_failover_reported_errors(AutoFailoverPid)).

auto_failover_async_t(_SetupConfig, PidMap) ->
    #{auto_failover := AutoFailoverPid} = PidMap,

    %% Janitor is called during auto_failover to "cleanup" buckets, one step of
    %% which is marking the buckets as warmed. We're already mocking the
    %% janitor_agent here so we will add an extra mock to this function to
    %% test that the auto_failover module isn't locked up mid-failover.
    meck:expect(janitor_agent, mark_bucket_warmed,
                fun(_,_) ->
                        Ticks = meck:num_calls(ns_doctor, get_nodes, '_'),

                        %% Send a bunch more ticks, we should be able to process
                        %% these, check against the calls to ns_doctor that we
                        %% are processing each tick as we send it.
                        lists:foreach(
                          fun(Count) ->
                                  AutoFailoverPid ! tick,

                                  misc:poll_for_condition(
                                    fun() ->
                                            Count + Ticks + 1 =:=
                                                meck:num_calls(ns_doctor,
                                                               get_nodes, '_')
                                    end, 5000, 100)
                          end,
                          lists:seq(0, 3)),

                        %% We shoud also be able to perform a call (to disable
                        %% auto_failover in this instance)
                        gen_server:call(AutoFailoverPid,
                                        {disable_auto_failover, []}),

                        %% And auto_failover should now be disabled
                        ?assertNot(proplists:get_value(enabled,
                                                       auto_failover:get_cfg()))
                end),

    perform_auto_failover_and_poll_counter(AutoFailoverPid, failover_complete,
                                           1),

    %% Without any auto-failover errors
    ?assertEqual([],
                 get_auto_failover_reported_errors(AutoFailoverPid)),

    %% And auto_failover should still be disabled
    Cfg = auto_failover:get_cfg(),
    ?assertNot(proplists:get_value(enabled, Cfg)).

%% Test post-network partition that we do not auto-failover nodes due to a stale
%% config.
%%
%% Consider a scenario in which we have a network partition with as follows:
%%
%% Partition A - [a, b, q]
%% Partition B - [c, d]
%%
%% In which [a, b, c, d] are KV nodes, and q runs any other service to provide
%% one side of the partition with a viable quorum.
%%
%% N.B. there can be multiple orchestrators, particularly during network
%% partitions. In this case we expect each side of a network partition to have
%% an orchestrator node provided the network is partitioned for long enough.
%%
%% In such a scenario, partition A may try to fail over all of the nodes in
%% partition B, and partition B may try to fail over all of the nodes in
%% partition A. It is allowed and acceptable to fail over either partition,
%% provided that we have 2 or more replicas. As partition A has a quorum, and
%% partition B does not, the nodes in partition B will be failed over by the
%% orchestrator of partition A. When the network partition heals, one of the
%% orchestrator nodes will yield to another. In this case the orchestrator of
%% partition A will yield to the orchestrator of partition B.
%%
%% The orchestrator of partition B will have been attempting to fail over the
%% nodes in partition A up til this point. Prior to this change, the safety
%% checks done by auto_failover, such as the preservation of durable mutations,
%% would be performed before gathering any quorum of nodes. This, combined with
%% a lengthy (in machine time) quorum gathering timeout of 15 seconds lead to a
%% scenario in which the orchestrator from partition B would pass auto_failover
%% checks with:
%%
%% 1) a view of the nodes in a partitioned state
%% 2) a config prior to the network partition (as no material change is possible
%%    without an orchestrator)
%%
%% In simpler terms, the orchestrator of partition B performed safety checks
%% without knowledge that the nodes of partition B had already been failed over.
%% This could then lead to the failover of all of the KV nodes in the cluster,
%% and 100% data loss.
%%
%% This test tests that in such a scenario we take the quorum and sync the
%% config before performing auto_failover checks.
auto_failover_post_network_partition_stale_config(SetupConfig, PidMap) ->
    %% On config sync we find our updated config
    meck:expect(chronicle_compat, pull,
                fun(_) ->
                        %% Now sync the config and we realise that the partition
                        %% without quorum has all been failed over...
                        OldNodes = maps:get(nodes, SetupConfig),
                        NewNodes = lists:foldl(
                                     fun({Node, Services}, Acc) ->
                                             Acc#{
                                                  Node => {inactiveFailed,
                                                           Services}
                                                 }
                                     end,
                                     OldNodes,
                                     maps:get(partition_without_quorum,
                                              SetupConfig)),

                        fake_config_helpers:setup_node_config(NewNodes),

                        %% Set our new map ('c' has failed over)
                        ok = ns_bucket:set_map_and_uploaders("default",
                                                             [['a', undefined]],
                                                             undefined)
                end),

    %% For this test we will force the map such that we don't trip over any
    %% auto-failover checks when attempting to fail over partition A, we want
    %% the code to make it into the failover module and for the checks there to
    %% fail.
    ok = ns_bucket:set_map_and_uploaders("default", [['a', 'c']], undefined),

    #{auto_failover := AutoFailoverPid} = PidMap,
    perform_auto_failover_and_poll_counter(AutoFailoverPid, failover_fail, 1),

    %% We should have failed to fail over, and, we should now have the reported
    %% error (autofailover_unsafe) stored in the auto_failover state.
    ?assertEqual([autofailover_unsafe],
                 get_auto_failover_reported_errors(AutoFailoverPid)).

enable_auto_failover_test(_SetupConfig, PidMap) ->
    #{auto_failover := AutoFailoverPid} = PidMap,

    %% With timeout 1 we should have set the tick period correctly to 100(ms).
    ?assertEqual(100, get_auto_failover_tick_period(AutoFailoverPid)),

    auto_failover:enable(1, 1, []),
    ?assertEqual(100, get_auto_failover_tick_period(AutoFailoverPid)),

    auto_failover:enable(2, 1, []),
    ?assertEqual(200, get_auto_failover_tick_period(AutoFailoverPid)),

    auto_failover:enable(3, 1, []),
    ?assertEqual(300, get_auto_failover_tick_period(AutoFailoverPid)),

    auto_failover:enable(4, 1, []),
    ?assertEqual(400, get_auto_failover_tick_period(AutoFailoverPid)),

    auto_failover:enable(5, 1, []),
    ?assertEqual(1000, get_auto_failover_tick_period(AutoFailoverPid)),

    auto_failover:enable(10, 1, []),
    ?assertEqual(1000, get_auto_failover_tick_period(AutoFailoverPid)),

    auto_failover:enable(120, 1, []),
    ?assertEqual(1000, get_auto_failover_tick_period(AutoFailoverPid)).

auto_failover_index_safety_check_failure_t(_SetupConfig, PidMap) ->
    #{auto_failover := AutoFailoverPid} = PidMap,
    perform_auto_failover(AutoFailoverPid),

    ?assert(misc:poll_for_condition(
              fun() ->
                      [{c, index, "Safety check failed."}] =:=
                          get_auto_failover_reported_errors(AutoFailoverPid)
              end, 5000, 100)),

    %% We should have sent an email alert (i.e. called log_unsafe_node).
    ?assert(meck:called(ns_email_alert, alert, [auto_failover_node, '_', '_'])).

graceful_failover_test_setup(SetupConfig) ->
    Pids = auto_failover_test_setup(SetupConfig),

    fake_chronicle_kv:update_snapshot(
      server_groups,
      [[{uuid, <<"0">>},
        {name, <<"Group 1">>},
        {nodes, ['a', 'b', 'c']}]]),

    fake_ns_config:update_snapshot(rebalance_out_delay_seconds, 0),

    meck:expect(chronicle_compat, push, fun(_) -> ok end),

    mock_helpers:setup_mocks([rebalance_quirks, ns_node_disco_events], Pids).

graceful_failover_test_() ->
    Nodes = #{
              'a' => {active, [kv]},
              'b' => {active, [kv]},
              'c' => {active, [kv]}
             },
    SetupArgs =
        #{nodes => Nodes,
          buckets => ["default"]},

    Tests = [
             {"Graceful failover", fun graceful_failover_t/2},
             {"Graceful failover incorrect expected topology",
              fun graceful_failover_incorrect_expected_topology/2},
             {"Graceful failover post network partition stale config",
              fun graceful_failover_post_network_partition_stale_config/2}
            ],

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun graceful_failover_test_setup/1,
     fun failover_test_teardown/2,
     [{SetupArgs, fun(T, R) ->
                          {Name, ?_test(TestFun(T, R))}
                  end} || {Name, TestFun} <- Tests]}.

graceful_failover_t(_SetupConfig, _PidMap) ->
    ok = ns_orchestrator:start_graceful_failover(['a']),

    ?assert(mock_helpers:poll_for_counter_value(graceful_failover_success, 1)),

    {ok, BucketConfig} = ns_bucket:get_bucket("default"),
    Servers = ns_bucket:get_servers(BucketConfig),
    ?assertNot(lists:member('a', Servers)).

graceful_failover_incorrect_expected_topology(_SetupConfig, _R) ->
    ?log_info("Starting graceful failover incorrect expected topology test"),

    %% For this test we will force the map such that we can gracefully fail over
    %% two nodes, 'a' and 'c', without issue.
    ok = ns_bucket:set_map_and_uploaders("default", [['a', 'b']], undefined),

    %% Need to trap the exit of the graceful failover processes to avoid nuking
    %% the test process when it exits.
    erlang:process_flag(trap_exit, true),

    %% We are running this manually in the failure cases so that we can catch
    %% the failure cases and test their types.
    HandleGracefulFailoverError =
        fun(Nodes, Opts) ->
                {ok, Pid} =
                    ns_rebalancer:start_link_graceful_failover(Nodes, Opts),
                receive
                    {'EXIT', Pid, Reason} ->
                        Reason
                after 5000 ->
                        exit(timeout)
                end
        end,

    %% Cannot failover with incorrect topologies, missing some nodes
    ?assertEqual(
       expected_topology_mismatch,
       HandleGracefulFailoverError(['a'],
                                   #{expected_topology => #{active => ['a']}})),

    %% And now we have the correct active nodes, but did not specify the other
    %% fields. The REST API will default these to an empty list if any one
    %% parameter is set, but we should still test this behaviour.
    ?assertEqual(
       expected_topology_mismatch,
       HandleGracefulFailoverError(
         ['a'],
         #{expected_topology => #{active => ['a', 'b', 'c']}})),

    %% Invalid node in addition to the others
    ?assertEqual(
       expected_topology_mismatch,
       HandleGracefulFailoverError(
         ['a'],
         #{expected_topology =>
               #{active => ['a', 'b', 'c', 'not_valid'],
                 inactiveFailed => [],
                 inactiveAdded => []}})),

    %% And now we can fail over.
    ok = ns_orchestrator:start_graceful_failover(
           ['a'],
           #{expected_topology => #{active => ['a', 'b', 'c'],
                                    inactiveFailed => [],
                                    inactiveAdded => []}}),
    ?assert(mock_helpers:poll_for_counter_value(graceful_failover_success, 1)),

    %% Now, this is where it matters. Can we prevent the failover of c when we
    %% think that a is still active?
    ?assertEqual(
       expected_topology_mismatch,
       HandleGracefulFailoverError(
         ['c'],
         #{expected_topology => #{active => ['a', 'b', 'c'],
                                  inactiveFailed => [],
                                  inactiveAdded => []}})),

    %% Sanity check for inactiveAdded too
    ?assertEqual(
       expected_topology_mismatch,
       HandleGracefulFailoverError(
         ['c'],
         #{expected_topology => #{active => ['b', 'c'],
                                  inactiveFailed => ['a'],
                                  inactiveAdded => ['d']}})),

    %% And we can continue to fail over another node.
    ok = ns_orchestrator:start_graceful_failover(
           ['c'],
           #{expected_topology => #{active => ['b', 'c'],
                                    inactiveFailed => ['a'],
                                    inactiveAdded => []}}),

    ?assert(mock_helpers:poll_for_counter_value(graceful_failover_success, 2)),

    erlang:process_flag(trap_exit, false).

graceful_failover_post_network_partition_stale_config(SetupConfig, _R) ->
    ?log_info("Starting graceful failover post network partition stale config "
              "test"),

    meck:expect(
      chronicle_compat, pull, 0,
      meck:seq([ok,
                fun() ->
                        %% Now sync the config and we realise that 'c' has
                        %% actually been failed over
                        OldNodes = maps:get(nodes, SetupConfig),
                        NewNodes = maps:put('c', {inactiveFailed, [kv]},
                                            OldNodes),

                        fake_config_helpers:setup_node_config(NewNodes),

                        %% Reflect the change in server config in our new bucket
                        %% map too.
                        ns_bucket:set_map_and_uploaders(
                          "default", [['a', undefined]], undefined),
                        ok
                end])),

    %% For this test we will force the map such that we cannot fail over 'a'
    %% and 'c' as we would lose a vBucket.
    ok = ns_bucket:set_map_and_uploaders("default", [['a', 'c']], undefined),

    ok = ns_orchestrator:start_graceful_failover(['a']),
    ?assert(mock_helpers:poll_for_counter_value(graceful_failover_fail, 1)).

multi_node_maxcount_test_config() ->
    #{buckets => ["default"],
      healthy_nodes =>
          [{'a', [kv]}, {'b', [index]}, {'c', [fts]}],
      unhealthy_nodes => [{'d', [index]}, {'e', [fts]}]}.

kv_maxcount_test_config() ->
    #{buckets => ["default"],
      healthy_nodes => [{'a', [kv]}, {'d', [kv]}, {'e', [index]}],
      unhealthy_nodes => [{'b', [index]}, {'c', [kv, index]}]}.

multi_node_failover_maxcount_test_() ->
    SetupArgs = multi_node_maxcount_test_config(),
    SetupConfig = build_setup_config(SetupArgs),

    Tests = [
             {"Multi-node failover maxCount test",
              fun multi_node_failover_maxcount_test/2}
            ],

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun auto_failover_multi_node_maxcount_setup/1,
     fun failover_test_teardown/2,
     [{SetupConfig, fun(T, R) ->
                            {Name, ?_test(TestFun(T, R))}
                    end} || {Name, TestFun} <- Tests]}.

kv_maxcount_failover_test_() ->
    SetupArgs = kv_maxcount_test_config(),
    SetupConfig = build_setup_config(SetupArgs),

    Tests = [
             {"KV node failover with max count test",
              fun kv_maxcount_failover_test/2}
            ],

    {foreachx,
     fun auto_failover_multi_node_maxcount_setup/1,
     fun failover_test_teardown/2,
     [{SetupConfig, fun(T, R) ->
                            {Name, ?_test(TestFun(T, R))}
                    end} || {Name, TestFun} <- Tests]}.

multi_node_failover_maxcount_test(_SetupConfig, Pids) ->
    #{auto_failover := AutoFailoverPid} = Pids,

    perform_auto_failover(AutoFailoverPid),

    ?assert(mock_helpers:poll_for_counter_value(failover_complete, 1)),

    ?assert(meck:called(service_api, is_safe, [index, '_']),
            "service_api:is_safe should be called for index service"),

    %% FTS node must have been failed over but not index (unsafe)
    ?assert(
       lists:member(
         'e',
         ns_cluster_membership:get_nodes_with_status(inactiveFailed)),
       "FTS node 'e' should have been failed over"),
    ?assert(lists:member('d',
                         ns_cluster_membership:get_nodes_with_status(active)),
            "Index node 'd' should NOT have been failed over"),

    ?assert(meck:called(service_janitor, complete_service_failover, [fts]),
            "Expected service_janitor:complete_service_failover to be called "
            "for FTS service"),

    ?assertNot(meck:called(service_janitor, complete_service_failover, [index]),
               "service_janitor:complete_service_failover should not be called "
               "for index service").

kv_maxcount_failover_test(_SetupConfig, Pids) ->
    #{auto_failover := AutoFailoverPid} = Pids,

    perform_auto_failover(AutoFailoverPid),

    %% Wait for failover to complete
    ?assert(mock_helpers:poll_for_counter_value(failover_complete, 1)),

    ?assert(meck:called(service_api, is_safe, [index, '_']),
            "service_api:is_safe should be called for index service"),

    FailedOverNodes =
        ns_cluster_membership:get_nodes_with_status(inactiveFailed),
    ?assertEqual(['c'], FailedOverNodes,
                 "Only node 'c' should be failed over"),

    %% Verify that node 'b' remained active
    ?assert(lists:member('b',
                         ns_cluster_membership:get_nodes_with_status(active)),
            "Node 'b' should NOT be failed over because it's unsafe").

auto_failover_multi_node_maxcount_setup(SetupConfig) ->
    Pids = auto_failover_test_setup(SetupConfig),

    fake_ns_config:update_snapshot(
      [{auto_failover_cfg,
        [{enabled, false},
         {timeout, 1},
         {count, 0},
         {max_count, 1},
         {failover_preserve_durability_majority, true}]}]),

    %% Mock pick_service_node to always return the local node so RPC calls work
    meck:new(ns_cluster_membership, [passthrough]),
    meck:expect(ns_cluster_membership, pick_service_node,
                fun(_Snapshot, _Service, _DownNodes) ->
                        node()
                end),

    meck:new(service_api, [passthrough]),
    meck:expect(service_api, is_safe,
                fun(index, _) ->
                        {error, "Index service unsafe for testing"};
                   (_, _) -> ok
                end),

    meck:new(service_manager, [passthrough]),
    meck:expect(service_manager, failover,
                fun(_Service, _Nodes, _Opts) -> ok end),

    meck:new(service_janitor, [passthrough]),
    meck:expect(service_janitor, complete_service_failover,
                fun(_) -> ok end),

    Pids.

auto_failover_service_safety_check_stale_config_test_() ->
    Nodes = #{
              'a' => {active, [kv]},
              'b' => {active, [index]},
              'c' => {active, [index]},
              'd' => {active, [index]}
             },
    SetupArgs =
        #{nodes => Nodes,
          buckets => ["default"],
          healthy_nodes => [{'a', [kv]}, {'d', [fts, index]}],
          unhealthy_nodes => [{'b', [index]}, {'c', [fts]}]},

    Tests = [
             {"Auto failover service safety check with stale config",
              fun auto_failover_service_safety_check_stale_config/1}],

    {foreachx,
     fun auto_failover_multi_node_maxcount_setup/1,
     fun failover_test_teardown/2,
     [{SetupArgs, fun(_T, R) ->
                          {Name, ?_test(TestFun(R))}
                  end} || {Name, TestFun} <- Tests]}.

%% Test that service safety checks are performed on the updated config after
%% getting quorum during auto-failover. This verifies that we don't proceed with
%% failover if the updated config shows it would make a service unsafe.
auto_failover_service_safety_check_stale_config(PidMap) ->
    #{auto_failover := AutoFailoverPid} = PidMap,

    meck:expect(chronicle_compat, pull, fun(_) -> ok end),

    %% Override the service_api mock to implement the MB-66630 scenario
    meck:expect(service_api, is_safe,
                [index, '_'],
                meck:seq([ok,
                          {error ,"Index service unsafe"}])),

    perform_auto_failover_and_poll_counter(AutoFailoverPid, failover_complete,
                                           1),

    %% Verify that no nodes were actually failed over due to service safety
    %% check failure
    FailedOverNodes =
        ns_cluster_membership:get_nodes_with_status(inactiveFailed),
    ?assertEqual([], FailedOverNodes,
                 "No nodes should have been failed over due to service safety "
                 "check failure"),
    ?assert(meck:called(leader_activities, run_activity,
                        [failover, majority, '_', '_']),
            "Should have gathered quorum for failover"),

    ?assert(meck:called(chronicle_compat, pull, '_'),
            "Should have performed config sync after gathering quorum"),

    %% Verify that service_api:is_safe was called at least twice:
    %% 1. First time during auto-failover (should return ok)
    %% 2. Second time during failover after config sync (should return error)
    History = meck:history(service_api),
    IndexCalls = [{Pid, Args, Result} ||
                     {Pid, {service_api, is_safe, [index, _] = Args}, Result}
                         <- History],

    ?assert(length(IndexCalls) =:= 2,
            io_lib:format("Must be 2 calls to service_api:is_safe, got ~p: ~p",
                          [length(IndexCalls), IndexCalls])),

    [{_, _, FirstResult}, {_, _, SecondResult} | _] = IndexCalls,
    ?assertEqual(ok, FirstResult,
                 "First service_api:is_safe call for index should return ok"),
    ?assertMatch({error, "Index service unsafe"}, SecondResult,
                 "Second service_api:is_safe call for index should return an "
                 "error").
