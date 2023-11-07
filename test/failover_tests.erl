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

%% Map should be of the form Key => {State, Services (list)}.
-spec setup_node_config(map()) -> true.
setup_node_config(NodesMap) ->
    ClusterSnapshot = maps:fold(
        fun(Node, {State, Services}, Snapshot) ->
            Snapshot#{
                {node, Node, membership} => State,
                {node, Node, services} => Services,
                {node, Node, failover_vbuckets} => []
            }
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
    fake_ns_config:new(),
    fake_chronicle_kv:new(),

    fake_ns_config:setup_cluster_compat_version(?LATEST_VERSION_NUM),
    fake_chronicle_kv:setup_cluster_compat_version(?LATEST_VERSION_NUM),

    setup_node_config(maps:get(nodes, SetupConfig)),
    setup_bucket_config(maps:get(buckets, SetupConfig)),


    meck:new(leader_activities, [passthrough]),
    meck:expect(leader_activities, run_activity,
        fun(_, _, Fun, _) ->
            Fun()
        end),
    meck:expect(leader_activities, deactivate_quorum_nodes,
        fun(_) -> ok end),

    meck:new(chronicle),
    meck:expect(chronicle, check_quorum, fun() -> true end),

    meck:new(testconditions),
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
            %% We don't need to return anything useful for this failover, we are
            %% failing over all but one node so we don't have to choose between
            %% any.
            {dict:from_list([{1, []}]), []}
        end),

    meck:expect(janitor_agent, fetch_vbucket_states,
        fun(0, _) ->
            %% We need to return some semi-valid vBucket stat map from this. We
            %% might use a couple of different maps for this test, so here we
            %% will generate it from the map (assuming only 1 vBucket).
            {ok, BucketConfig} = ns_bucket:get_bucket("default"),
            [[Active | Replicas]] = proplists:get_value(map, BucketConfig),
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
    meck:unload(chronicle_compat),
    meck:unload(chronicle_master),
    meck:unload(testconditions),
    meck:unload(chronicle),
    meck:unload(leader_activities),

    fake_chronicle_kv:unload(),
    fake_ns_config:unload().

manual_failover_t(_SetupConfig, _R) ->
    ?assertEqual({error,not_found},
        chronicle_compat:get(counters, #{})),

    %% We are failing over all but node 'c' here.
    NodesToFailOver = ['a', 'b'],

    meck:expect(chronicle_compat, config_sync, fun(_,_,_) -> ok end),

    %% Need to trap the exit of the failover process to avoid nuking the test
    %% process when it exits.
    erlang:process_flag(trap_exit, true),

    Options = #{
        allow_unsafe => false,
        %% auto failover
        auto => false,
        failover_reasons => "ok",
        down_nodes => NodesToFailOver
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
    meck:expect(chronicle_compat, config_sync,
        fun(_,_,_) ->
            %% Now sync the config and we realise that 'c' has actually been
            %% failed over
            OldNodes = maps:get(nodes, SetupConfig),
            NewNodes = maps:put('c', {inactiveFailed, kv}, OldNodes),

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
        failover_reasons => "ok",
        down_nodes => NodesToFailOver
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

auto_failover_test_() ->
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
            partition_with_quorum => PartitionA,
            partition_without_quorum => PartitionB},

    Tests = [
        {"Auto failover",
            fun auto_failover_t/2}
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
    {ok, RebalanceReportManagerPid} = ns_rebalance_report_manager:start_link(),
    {ok, CompatModeManagerPid} = compat_mode_manager:start_link(),

    %% Config for auto_failover
    fake_ns_config:update_snapshot([{auto_failover_cfg,
        [{enabled, true},
            {timeout, 1},
            {count, 0},
            {max_count, 5},
            {failover_preserve_durability_majority, true}]}]),

    %% We need this to not throw an error in auto_failover tick, but we won't
    %% use the status so an empty list is fine.
    meck:new(ns_doctor),
    meck:expect(ns_doctor, get_nodes, fun() -> [] end),

    %% The test will see the partition that had the quorum as down and attempt
    %% to fail it over, not realising that the partition without quorum had
    %% already been failed over.
    meck:new(node_status_analyzer),
    meck:expect(node_status_analyzer, get_nodes,
        fun() ->
            lists:foldl(
                fun({Node, _Services}, Acc) ->
                    dict:store(Node, {unhealthy, foo}, Acc)
                end,
                dict:new(),
                maps:get(partition_with_quorum, SetupConfig))
        end),

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

    manual_failover_test_teardown(Config, PidMap).

get_auto_failover_reported_errors(AutoFailoverPid) ->
    %% Little bit of a hack, we are relying on the format of the
    %% auto_failover:state record, but this is exactly the information that we
    %% want to check.
    {state, _, _, _, _, _, _, _, Errors} = sys:get_state(AutoFailoverPid),
    sets:to_list(Errors).

auto_failover_t(_SetupConfig, PidMap) ->
    #{auto_failover := AutoFailoverPid} = PidMap,

    %% Part of our test, we should not have any reported errors yet.
    ?assertEqual([],
        get_auto_failover_reported_errors(AutoFailoverPid)),

    meck:expect(chronicle_compat, config_sync, fun(_,_,_) -> ok end),

    %% Tick auto-failover 4 times. We could wait long enough to do the auto
    %% failover but we can speed this test up a bit by manually ticking. This
    %% amount of ticks should be the minimum to process the auto-failover.
    lists:foreach(
        fun(_) ->
            AutoFailoverPid ! tick
        end,
        lists:seq(0, 3)),

    %% Disable auto-failover, this gen_server call will let us finish processing
    %% the ticks that we have queued above (which will run the auto-failover to
    %% completion), before returning which will mean that we've attempted an
    %% auto-failover provided we've ticked enough.
    gen_server:call(AutoFailoverPid, {disable_auto_failover, []}),

    %% We should have completed the failover.
    Counters = chronicle_compat:get(counters, #{required => true}),
    ?assertNotEqual(undefined,
        proplists:get_value(failover_complete, Counters)),

    %% Without any auto-failover errors
    ?assertEqual([],
        get_auto_failover_reported_errors(AutoFailoverPid)).
