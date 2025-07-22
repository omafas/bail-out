%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc
%% A set of tests for rebalance behaviours.
-module(rebalance_tests).

-include("ns_test.hrl").
-include("ns_common.hrl").

-include_lib("eunit/include/eunit.hrl").

rebalance_test_() ->
    Nodes = #{
              node() => {active, [kv]},
              'b' => {inactiveAdded, [kv]}
             },
    SetupArgs =
        #{nodes => Nodes,
          buckets => ["default"]},

    Tests = [
             {"Add node",
              fun add_node_t/2},
             {"Expected topology test",
              fun expected_topology_t/2},
             {"Expected topology stale config",
              fun expected_topology_stale_config_t/2},
             {"Add node stale config",
              fun add_node_stale_config_t/2}
            ],

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun rebalance_test_setup/1,
     fun rebalance_test_teardown/2,
     [{SetupArgs, fun(T, R) ->
                          {Name, ?_test(TestFun(T, R))}
                  end} || {Name, TestFun} <- Tests]}.

rebalance_test_setup(SetupConfig) ->
    config_profile:load_default_profile_for_test(),
    fake_ns_config:setup(),
    fake_chronicle_kv:setup(),

    fake_ns_config:setup_cluster_compat_version(?LATEST_VERSION_NUM),
    fake_chronicle_kv:setup_cluster_compat_version(?LATEST_VERSION_NUM),

    fake_config_helpers:setup_cluster_config(maps:get(nodes, SetupConfig)),
    fake_config_helpers:setup_bucket_config(maps:get(buckets, SetupConfig)),

    fake_ns_config:update_snapshot(
      [{auto_failover_cfg,
        [{enabled, false},
         {timeout, 120},
         {count, 0},
         {max_count, 0},
         {failover_preserve_durability_majority, true}]}]),

    fake_chronicle_kv:update_snapshot(
      server_groups,
      [[{uuid, <<"0">>},
        {name, <<"Group 1">>},
        {nodes, [node(), 'b']}]]),

    %% We make a few misc mocks because we don't have other nodes to call. As of
    %% writing, we don't really care about the results.
    meck:new(misc, [passthrough]),
    meck:expect(misc, rpc_multicall_with_plist_result,
                fun(_,_,_,_) ->
                        {[], [], []}
                end),
    meck:expect(misc, multi_call_request,
                fun(_,_,_) ->
                        {[], []}
                end),

    mock_helpers:setup_mocks([leader_activities,
                              rebalance_agent,
                              compat_mode_manager,
                              auto_reprovision,
                              auto_failover,
                              janitor_agent,
                              chronicle_master,
                              ns_storage_conf,
                              ns_node_disco_events,
                              rebalance_quirks,
                              ns_orchestrator], #{}).

rebalance_test_teardown(_, PidMap) ->
    mock_helpers:shutdown_processes(PidMap),

    meck:unload(),

    fake_chronicle_kv:teardown(),
    fake_ns_config:teardown(),

    config_profile:unload_profile_for_test().

expect_rebalance_success(Params) ->
    perform_rebalance(Params, rebalance_success).

expect_rebalance_failure(Params) ->
    perform_rebalance(Params, rebalance_fail).

perform_rebalance(Params, Type) ->
    CurrentCounter =
        case mock_helpers:get_counter_value(Type) of
            V when is_integer(V) -> V;
            _ -> 0
        end,
    erlang:spawn_link(fun() ->
                              {ok, _} = rebalance:start(Params)
                      end),
    ?assert(mock_helpers:poll_for_counter_value(Type, CurrentCounter + 1)).

add_node_t(_SetupConfig, _) ->
    Params = #{known_nodes => ns_node_disco:nodes_wanted(),
               eject_nodes => [],
               services => all,
               desired_services_nodes => #{},
               delta_recovery_buckets => []
              },

    expect_rebalance_success(Params).

expected_topology_t(_SetupConfig, _) ->
    Params = #{known_nodes => ns_node_disco:nodes_wanted(),
               eject_nodes => [],
               services => all,
               desired_services_nodes => #{},
               delta_recovery_buckets => []
              },

    expect_rebalance_failure(
      Params#{expected_topology => #{active => [node()]}}),

    expect_rebalance_failure(
      Params#{expected_topology => #{active => [node(), 'b']}}),

    %% And now we have the correct active nodes, but did not specify the other
    %% fields. The REST API will default these to an empty list if any one
    %% parameter is set, but we should still test this behaviour.
    expect_rebalance_failure(
      Params#{expected_topology => #{active => [node()],
                                     inactiveAdded => ['b']}}),

    %% And now we have the full topology and this should succeed
    expect_rebalance_success(
      Params#{expected_topology => #{active => [node()],
                                     inactiveAdded => ['b'],
                                     inactiveFailed => []}}).

expected_topology_stale_config_t(_SetupConfig, _) ->
    Params = #{known_nodes => ns_node_disco:nodes_wanted(),
               eject_nodes => [],
               services => all,
               delta_recovery_buckets => [],
               desired_services_nodes => #{}
              },

    meck:expect(chronicle_compat, pull,
                fun() ->
                        fake_chronicle_kv:update_snapshot(
                          {node, 'b', membership}, inactiveFailed)
                end),

    expect_rebalance_failure(
      Params#{expected_topology => #{active => [node()],
                                     inactiveAdded => ['b'],
                                     inactiveFailed => []}}),

    meck:delete(chronicle_compat, pull, 0),
    fake_chronicle_kv:update_snapshot(
      {node, 'b', membership}, inactiveAdded),

    expect_rebalance_success(
      Params#{expected_topology => #{active => [node()],
                                     inactiveAdded => ['b'],
                                     inactiveFailed => []}}).

add_node_stale_config_t(_SetupConfig, _) ->
    Params = #{known_nodes => ns_node_disco:nodes_wanted(),
               eject_nodes => [],
               services => all,
               delta_recovery_buckets => [],
               desired_services_nodes => #{kv => [node()]}},

    meck:expect(chronicle_compat, pull,
                fun() ->
                        fake_chronicle_kv:update_snapshot(
                            {node, 'b', membership}, inactiveFailed)
                end),

    expect_rebalance_failure(Params).
