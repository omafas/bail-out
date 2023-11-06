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
        fun manual_failover_t/2}
    ],

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
