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
              fun add_node_t/2}
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

    fake_config_helpers:setup_node_config(maps:get(nodes, SetupConfig)),
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

add_node_t(_SetupConfig, _) ->
    Params = #{known_nodes => ns_node_disco:nodes_wanted(),
               eject_nodes => [],
               services => all,
               desired_services_nodes => #{},
               delta_recovery_buckets => []
              },

    _Pid = erlang:spawn_link(fun() ->
                                     {ok, _} = rebalance:start(Params)
                             end),

    ?assert(mock_helpers:poll_for_counter_value(rebalance_success, 1)).
