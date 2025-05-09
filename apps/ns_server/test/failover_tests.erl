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

-spec add_service_map_to_snapshot(atom(), list(), map()) -> map().
add_service_map_to_snapshot(Node, Services, Snapshot) ->
    lists:foldl(
        fun(kv, AccSnapshot) ->
                %% KV is handled in a special way
                AccSnapshot;
            (Service, S) ->
                case maps:find({service_map, Service}, S) of
                    error -> S#{{service_map, Service} => [Node]};
                    {ok, Nodes} -> S#{{service_map, Service} => [Node | Nodes]}
                end
        end, Snapshot, Services).

%% Map should be of the form Key => {State, Services (list)}.
-spec setup_node_config(map()) -> true.
setup_node_config(NodesMap) ->
    ClusterSnapshot =
        maps:fold(
            fun(Node, {State, Services}, Snapshot) ->
                    S = add_service_map_to_snapshot(Node, Services, Snapshot),
                    S#{{node, Node, membership} => State,
                        {node, Node, services} => Services,
                        {node, Node, failover_vbuckets} => []}
            end, #{}, NodesMap),
    fake_chronicle_kv:update_snapshot(ClusterSnapshot),

    Nodes = maps:keys(NodesMap),
    fake_chronicle_kv:update_snapshot(nodes_wanted, Nodes).

%% Takes a list of bucket names (strings).
%% Requires that node config is setup (i.e. we must be able to read from the
%% config which nodes have the data service).
-spec setup_bucket_config(list()) -> true.
setup_bucket_config(Buckets) ->
    KVNodes = ns_cluster_membership:service_active_nodes(kv),

    %% Asserting length of KV nodes, we create a simple vBucket map and that
    %% may not be valid with more nodes. That can be improved later if
    %% necessary.
    ?assert(length(KVNodes) =< 4),

    fake_chronicle_kv:update_snapshot(bucket_names, Buckets),

    Val = [
           {type, membase},
           {servers, KVNodes},
           %% map is 1 vBucket, all nodes in a single chain
           {map, [KVNodes]}
          ],

    fake_chronicle_kv:update_snapshot(
      maps:from_list([{{bucket, B, props}, Val} || B <- Buckets])).

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
              fun manual_failover_post_network_partition_stale_config/2}],

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun manual_failover_test_setup/1,
     fun manual_failover_test_teardown/2,
     [{SetupArgs, fun(T, R) ->
                          {Name, ?_test(TestFun(T, R))}
                  end} || {Name, TestFun} <- Tests]}.

manual_failover_test_setup(SetupConfig) ->
    fake_ns_config:setup(),
    fake_chronicle_kv:new(),

    fake_ns_config:setup_cluster_compat_version(?LATEST_VERSION_NUM),
    fake_chronicle_kv:setup_cluster_compat_version(?LATEST_VERSION_NUM),

    setup_node_config(maps:get(nodes, SetupConfig)),
    setup_bucket_config(maps:get(buckets, SetupConfig)),

    meck:new(leader_activities, [passthrough]),
    meck:expect(leader_activities, run_activity,
        fun(_Name, _Quorum, Body, _Opts) ->
            Body()
        end),
    meck:expect(leader_activities, deactivate_quorum_nodes,
        fun(_) -> ok end),


    meck:new(chronicle),
    meck:expect(chronicle, check_quorum, fun() -> true end),

    meck:new(testconditions, [passthrough]),
    meck:expect(testconditions, get, fun(_) -> ok end),

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

    %% We will spawn auto-reprovision for this test. To spawn it we must spawn
    %% the leader_registry. Needed for leader_registry
    meck:new(fake_ns_pubsub, [non_strict]),
    meck:new(ns_pubsub),
    meck:expect(ns_pubsub, subscribe_link,
                fun(_, Handler) ->
                        %% Stash the handler in some function, notify_key
                        meck:expect(fake_ns_pubsub, notify_key,
                                    fun(Key) ->
                                            Handler(Key)
                                    end),
                        ok
                end),

    {ok, LeaderRegistryPid} = leader_registry:start_link(),
    gen_server:cast(LeaderRegistryPid, {new_leader, node()}),

    %% Janitor_agent mecks required to perform a full failover (with map).
    meck:new(janitor_agent),
    meck:expect(janitor_agent, query_vbuckets,
                fun(_,_,_,_) ->
                        %% We don't need to return anything useful for this
                        %% failover, we are failing over all but one node so
                        %% we don't have to choose between any.
                        {dict:from_list([{1, []}]), []}
                end),

    meck:expect(janitor_agent, fetch_vbucket_states,
                fun(0, _) ->
                        %% We need to return some semi-valid vBucket stat map
                        %% from this. We might use a couple of different maps
                        %% for this test, so here we will generate it from
                        %% the map (assuming only 1 vBucket).
                        {ok, BucketConfig} = ns_bucket:get_bucket("default"),
                        [[Active | Replicas]] =
                            proplists:get_value(map, BucketConfig),
                        Seqnos = [{high_prepared_seqno, 1},
                                  {high_seqno, 1}],
                        A = [{Active, active, Seqnos}],
                        R = [{Replica, replica, Seqnos} || Replica <- Replicas],
                        A ++ R
                end),

    meck:expect(janitor_agent, apply_new_bucket_config,
                fun(_,_,_,_) ->
                        %% Just sets stuff in memcached, uninteresting here
                        ok
                end),

    meck:expect(janitor_agent, mark_bucket_warmed,
                fun(_,_) ->
                        %% Just sets stuff in memcached, uninteresting here
                        ok
                end),

    %% We need to check auto_reprovision settings via a gen_server call so we
    %% must start up auto_reprovision. We can disable it though, because we
    %% don't really care which options it has set.
    fake_chronicle_kv:update_snapshot(auto_reprovision_cfg, [{enabled, false}]),
    {ok, AutoReprovisionPid} = auto_reprovision:start_link(),

    %% Return a map of pids here, we will need to shut them all down in the
    %% teardown. Name them in case the test needs to lookup specific pids.
    #{leader_registry => LeaderRegistryPid,
      auto_reprovision => AutoReprovisionPid}.

manual_failover_test_teardown(_Config, PidMap) ->
    maps:foreach(
      fun(_Process, Pid) ->
              erlang:unlink(Pid),
              misc:terminate_and_wait(Pid, shutdown)
      end, PidMap),

    meck:unload(janitor_agent),
    meck:unload(fake_ns_pubsub),
    meck:unload(ns_pubsub),
    meck:unload(chronicle_compat),
    meck:unload(chronicle_master),
    meck:unload(testconditions),
    meck:unload(chronicle),
    meck:unload(leader_activities),

    fake_chronicle_kv:unload(),
    fake_ns_config:teardown().

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

                        setup_node_config(NewNodes),
                        setup_bucket_config(maps:get(buckets, SetupConfig)),
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
     fun auto_failover_test_teardown/2,
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
     fun auto_failover_test_teardown/2,
     [{SetupArgs, fun(T, R) ->
                          {Name, ?_test(TestFun(T, R))}
                  end} || {Name, TestFun} <- Tests]}.

auto_failover_test_setup(SetupConfig) ->
    Pids = manual_failover_test_setup(SetupConfig),

    %% Needed to complete a failover(/subsequent rebalance)
    fake_ns_config:update_snapshot(rest_creds, null),
    {ok, RebalanceReportManagerPid} = ns_rebalance_report_manager:start_link(),
    {ok, CompatModeManagerPid} = compat_mode_manager:start_link(),

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

    %% We need this to not throw an error in auto_failover tick, but we won't
    %% use the status so an empty list is fine.
    meck:new(ns_doctor),
    meck:expect(ns_doctor, get_nodes, fun() -> [] end),

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

    config_profile:mock_default_profile(),

    %% Needed to start the orchestrator. We don't really need the janitor to run
    %% for this test, so we will mock it instead of run it because we'd need to
    %% do some extra stuff to get it running.
    meck:new(ns_janitor_server),
    meck:expect(ns_janitor_server, start_cleanup,
                fun(_) -> {ok, self()} end),
    meck:expect(ns_janitor_server, terminate_cleanup,
                fun(_) ->
                        CallerPid = self(),
                        CallerPid ! {cleanup_done, foo, bar},
                        ok
                end),

    %% May be required if the test tries to send an email alert (and wants to
    %% see that this has happened).
    meck:new(ns_email_alert, [passthrough]),

    meck:new(cb_atomic_persistent_term, [passthrough]),
    meck:expect(cb_atomic_persistent_term, get_or_set_if_undefined,
                fun(_, _, F) ->
                        F()
                end),

    %% Need to start the orchestrator so that auto_failover can follow the full
    %% code path.
    {ok, OrchestratorPid} = ns_orchestrator:start_link(),

    %% And we must start auto_failover itself to tick it and test it as it would
    %% normally run.
    {ok, AutoFailoverPid} = auto_failover:start_link(),

    Pids#{orchestrator => OrchestratorPid,
          ns_rebalance_report_manager => RebalanceReportManagerPid,
          compat_mode_manager => CompatModeManagerPid,
          auto_failover => AutoFailoverPid}.

auto_failover_test_teardown(Config, PidMap) ->
    meck:unload(ns_janitor_server),
    meck:unload(node_status_analyzer),
    meck:unload(ns_doctor),
    meck:unload(ns_email_alert),
    meck:unload(cb_atomic_persistent_term),
    config_profile:unmock_default_profile(ok),

    manual_failover_test_teardown(Config, PidMap).

get_auto_failover_reported_errors(AutoFailoverPid) ->
    sets:to_list(
      auto_failover:get_errors_from_state(sys:get_state(AutoFailoverPid))).

get_auto_failover_tick_period(AutoFailoverPid) ->
    auto_failover:get_tick_period_from_state(sys:get_state(AutoFailoverPid)).

poll_for_auto_failover_completion() ->
    %% Failover is async to the auto_failover module, poll til it is completed
    misc:poll_for_condition(
        fun() ->
                case chronicle_compat:get(counters, #{}) of
                    {error, not_found} -> false;
                    {ok, Value} ->
                        proplists:is_defined(failover_complete, Value)
                end
        end, 5000, 100).

perform_auto_failover(AutoFailoverPid) ->
    %% Override tick period. This lets us tick auto_failover as few times as
    %% possible in the test as we essentially don't have to wait for nodes to
    %% be in a down state for n ticks at any point.
    fake_ns_config:update_snapshot(auto_failover_tick_period, 100000),
    AutoFailoverPid ! tick_period_updated,
    auto_failover:enable(1, 5, []),

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
      lists:seq(0, 3)),

    poll_for_auto_failover_completion().

auto_failover_t(_SetupConfig, PidMap) ->
    #{auto_failover := AutoFailoverPid} = PidMap,

    perform_auto_failover(AutoFailoverPid),

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

    perform_auto_failover(AutoFailoverPid),

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

                        setup_node_config(NewNodes),
                        setup_bucket_config(
                          maps:get(buckets, SetupConfig)),
                        %% TODO, remove this when we work out why we sometimes
                        %% fail over in this test.
                        ?log_debug("Continuing test with config ~p",
                                   [fake_chronicle_kv:get_ets_snapshot()]),
                        ok
                end),

    #{auto_failover := AutoFailoverPid} = PidMap,
    perform_auto_failover(AutoFailoverPid),

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

    %% Auto failover should not be possible
    ?assertEqual([{c, index, "Safety check failed."}],
        get_auto_failover_reported_errors(AutoFailoverPid)),

    %% We should have sent an email alert (i.e. called log_unsafe_node).
    ?assert(meck:called(ns_email_alert, alert, [auto_failover_node, '_', '_'])).
