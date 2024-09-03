%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(health_monitor_tests).

-include("ns_common.hrl").
-include("cut.hrl").

-include_lib("eunit/include/eunit.hrl").

%% Not all monitors are testable yet, just the ones that we run on all nodes.
-define (TESTABLE_MONITORS,
         [
          ns_server_monitor,
          node_status_analyzer,
          node_monitor,
          dcp_traffic_monitor,
          kv_monitor,
          kv_stats_monitor,
          index_monitor
         ]).

setup_service_monitors(Monitors) when is_list(Monitors) ->
    %% We will only spawn monitors for supported services, but this function
    %% by default will only create one or the other, as we should not fail
    %% over a KV node if indexing has issues. We will just pretend here that
    %% we have both.
    meck:expect(health_monitor, supported_services,
                fun(_) -> Monitors end),

    %% Need to refresh children to spawn the kv and index monitors
    health_monitor_sup:refresh_children(),

    %% When we refresh node_monitors local_monitors we need to override that
    %% function too or we will end up with the default behaviour of KV OR index.
    meck:expect(health_monitor, local_monitors,
                fun() -> [ns_server | Monitors] end),

    %% Need to tell the node_monitor that there are other monitors to look at
    node_monitor ! node_changed,

    %% And similar for the node_status_analyzer
    meck:expect(health_monitor, node_monitors,
                fun(_Node) -> [ns_server | Monitors] end),
    node_status_analyzer ! node_changed.

test_setup() ->
    %% Health monitor meck is used to "trick" the system into running some
    %% services and for some history checks
    meck:new(health_monitor, [passthrough]),

    health_monitor:common_test_setup(),

    %% Each monitor may have it's own required setup
    lists:foreach(
      fun(Module) ->
              Module:common_test_setup()
      end, ?TESTABLE_MONITORS),

    %% service_monitor_worker (one of the child processes that
    %% health_monitor_sup spawns) is a chronicle compat refresh worker. To
    %% start up health_monitor_sup correctly we need to return a valid Pid.
    %% We also need to be able to shut down that Pid later. Create a process
    %% that we can effectively throw away to appease this code.
    PidToMonitor =
        erlang:spawn(
          fun() ->
                  %% Block in receive to ensure that this process remains
                  %% alive til we want to kill it
                  receive _ ->
                          ok
                  end
          end),

    meck:expect(chronicle_compat_events,
                start_refresh_worker,
                fun(_,_) ->
                        {ok, PidToMonitor}
                end),

    {ok, SupervisorPid} = health_monitor_sup:start_link(),
    SupervisorPid.

test_teardown(SupervisorPid) ->
    true = erlang:exit(SupervisorPid, normal),

    lists:foreach(
      fun(Module) ->
              Module:common_test_teardown()
      end, ?TESTABLE_MONITORS),

    health_monitor:common_test_teardown(),
    meck:unload(health_monitor).

-spec is_node_status(node(), healthy | unhealthy) -> boolean().
is_node_status(Node, Status) ->
    NodeStatuses = node_status_analyzer:get_statuses(),
    case dict:find(Node, NodeStatuses) of
        error ->
            false;
        {ok, {Status, _RecvTs}} ->
            true;
        {ok, _} ->
            false
    end.

-spec is_node_healthy(node()) -> boolean().
is_node_healthy(Node) ->
    is_node_status(Node, healthy).

-spec is_node_unhealthy(node()) -> boolean().
is_node_unhealthy(Node) ->
    not is_node_status(Node, healthy).

%% Test that the ns_server_monitor detects failures that cause the node to
%% become unhealthy, and that the system can recover once the node becomes
%% healthy again.
ns_server_monitor_failure_detection_t() ->
    %% First, make sure that we are healthy so that we can test a transition
    %% to unhealthy
    ?assert(misc:poll_for_condition(
              fun() ->
                      is_node_healthy(node())
              end, 30000, 100)),

    %% Block all refresh messages in ns_server_monitor to simulate node
    %% unresponsive-ness
    meck:new(ns_server_monitor, [passthrough]),
    meck:expect(ns_server_monitor, handle_info,
                fun(refresh, _State) ->
                        noreply;
                   (_Msg, _State) ->
                        meck:passthrough()
                end),

    %% Should turn the node unhealthy
    ?assert(misc:poll_for_condition(
              fun() ->
                      is_node_unhealthy(node())
              end, 30000, 100)),

    %% Delete our expectation to unblock the refresh messages in
    %% ns_server_monitor
    meck:delete(ns_server_monitor, handle_info, 2),

    %% And we should transition back to healthy
    ?assert(misc:poll_for_condition(
              fun() ->
                      is_node_healthy(node())
              end, 30000, 100)).

index_monitor_failure_detection_t() ->
    setup_service_monitors([index]),

    gen_server:cast(index_monitor, {got_connection, self()}),


    %% First, make sure that we are healthy so that we can test a transition
    %% to unhealthy
    ?assert(misc:poll_for_condition(
              fun() ->
                      is_node_healthy(node())
              end, 30000, 100)),

    meck:new(index_monitor, [passthrough]),
    meck:expect(index_monitor, handle_info,
                fun
                    ({tick, _Result}, _State) ->
                       noreply;
                    (Msg, State) ->
                       meck:passthrough([Msg, State])
               end),

    %% Should turn the node unhealthy
    ?assert(misc:poll_for_condition(
              fun() ->
                      is_node_unhealthy(node())
              end, 30000, 100)),

    %% To send a tick message we must have tick = {tick, TS} in the state, so
    %% wait for that.
    meck:wait(index_monitor, handle_info, [refresh, #{tick => {tick, '_'}}],
              1000),

    %% Drop our message filter now so that we can transition back to healthy
    meck:delete(index_monitor, handle_info, 2),
    index_monitor ! {tick, {ok, 0}},

    %% And we should transition back to healthy
    ?assert(misc:poll_for_condition(
              fun() ->
                      is_node_healthy(node())
              end, 30000, 100)).

gen_test_() ->
    Tests = [
             fun ns_server_monitor_failure_detection_t/0,
             fun index_monitor_failure_detection_t/0
            ],

    {foreach,
     fun test_setup/0,
     fun test_teardown/1,
     Tests}.

%% Testing failure in the kv_stats_monitor, and propagation to
%% node_status_analyzer. kv_stats_monitor passes statuses up to kv_monitor,
%% which also pulls statuses from dcp_traffic_monitor, so we need half of the
%% health monitoring running for this test...
kv_stats_monitor_io_failure_detection_t(Stat) ->
    meck:expect(
        auto_failover, get_cfg,
        fun() ->
            [{enabled, true},
                %% timeout is the time (in seconds) a node needs to be down
                %% before it is automatically fail-overed
                {timeout, 1},
                {failover_on_data_disk_issues, [{enabled, true},
                    {timePeriod, 1}]}]
        end),

    meck:expect(ns_bucket, node_bucket_names_of_type,
        fun(_, persistent) ->
            ["default"]
        end),

    setup_service_monitors([kv]),

    dcp_traffic_monitor:node_alive(node(), {"default", 1, self()}),

    %% First, make sure that we are healthy so that we can test a transition
    %% to unhealthy
    ?assert(misc:poll_for_condition(
        fun() ->
            is_node_healthy(node())
        end, 30000, 100)),

    meck:expect(ns_memcached, stats,
        fun(_Bucket, <<"disk-failures">>) ->
            StatCount = integer_to_binary(
                meck:num_calls(ns_memcached, stats, '_')),
            {ok, [{Stat, StatCount}]}
        end),

    %% Should turn the node unhealthy
    ?assert(misc:poll_for_condition(
        fun() ->
            is_node_unhealthy(node())
        end, 30000, 100)).

kv_stats_monitor_io_failure_detection_test_() ->
    {foreach,
        fun test_setup/0,
        fun test_teardown/1,
        [?cut(kv_stats_monitor_io_failure_detection_t(atom_to_binary(Stat))) ||
            {Stat, _} <- kv_stats_monitor:failure_stats()]}.

kv_stats_monitor_io_slow_test_setup() ->
    SupPid = test_setup(),

    meck:expect(
        auto_failover, get_cfg,
        fun() ->
            [{enabled, true},
                %% timeout is the time (in seconds) a node needs to be down
                %% before it is automatically fail-overed
                {timeout, 1},
                {failover_on_data_disk_non_responsiveness, [{enabled, true},
                    {timePeriod, 1}]}]
        end),

    meck:expect(ns_bucket, node_bucket_names_of_type,
        fun(_, persistent) ->
            ["default"]
        end),

    setup_service_monitors([kv]),
    dcp_traffic_monitor:node_alive(node(), {"default", 1, self()}),

    %% First, make sure that we are healthy so that we can test a transition
    %% to unhealthy
    ?assert(misc:poll_for_condition(
        fun() ->
            is_node_healthy(node())
        end, 30000, 100)),

    SupPid.

kv_stats_monitor_io_slow_test_teardown(SupPid) ->
    meck:unload(ns_memcached),
    test_teardown(SupPid).

kv_stats_mon_io_slow_zero({StatNum, StatSlow}) ->
    %% Not all readers/writers stuck, we should remain healthy. We have to
    %% special case this though, because equality of any other value would drive
    %% a failover.
    meck:expect(ns_memcached, stats,
        fun(_Bucket, <<"disk-slowness 1">>) ->
            {ok, [{atom_to_binary(StatNum), integer_to_binary(0)},
                  {atom_to_binary(StatSlow), integer_to_binary(0)}]}
        end),

    %% Timeout as we will remain healthy
    ?assertEqual(timeout, misc:poll_for_condition(
        fun() ->
            is_node_unhealthy(node())
        end, 2000, 100)).

kv_stats_monitor_io_slow_detection_t({StatNum, StatSlow}) ->
    %% Another healthy case, Slow < Count.
    meck:expect(ns_memcached, stats,
        fun(_Bucket, <<"disk-slowness 1">>) ->
            {ok, [{atom_to_binary(StatNum), integer_to_binary(1)},
                  {atom_to_binary(StatSlow), integer_to_binary(0)}]}
        end),

    %% Timeout as we will remain healthy
    ?assertEqual(timeout, misc:poll_for_condition(
        fun() ->
            is_node_unhealthy(node())
        end, 2000, 100)).

kv_stats_mon_io_slow_equal_count({StatNum, StatSlow}) ->
    meck:expect(ns_memcached, stats,
        fun(_Bucket, <<"disk-slowness 1">>) ->
            {ok, [{atom_to_binary(StatNum), integer_to_binary(1)},
                  {atom_to_binary(StatSlow), integer_to_binary(1)}]}
        end),

    %% Should turn the node unhealthy
    ?assert(misc:poll_for_condition(
        fun() ->
            is_node_unhealthy(node())
        end, 30000, 100)).

%% We don't expect to see Slow > Count, but it should probably fail over anyways
kv_stats_mon_io_slow_greater_than_count({StatNum, StatSlow}) ->
    meck:expect(ns_memcached, stats,
        fun(_Bucket, <<"disk-slowness 1">>) ->
            {ok, [{atom_to_binary(StatNum), integer_to_binary(1)},
                  {atom_to_binary(StatSlow), integer_to_binary(2)}]}
        end),

    %% Should turn the node unhealthy
    ?assert(misc:poll_for_condition(
        fun() ->
            is_node_unhealthy(node())
        end, 30000, 100)).

kv_stats_mon_io_slow_and_disk_failure({StatNum, StatSlow}) ->
    meck:expect(
        auto_failover, get_cfg,
        fun() ->
            [{enabled, true},
                %% timeout is the time (in seconds) a node needs to be down
                %% before it is automatically fail-overed
                {timeout, 1},
                {failover_on_data_disk_issues, [{enabled, true},
                                                {timePeriod, 1}]},
                {failover_on_data_disk_non_responsiveness, [{enabled, true},
                                                            {timePeriod, 1}]}]
        end),

    %% Force a config refresh in the monitor for the disk issues config
    kv_stats_monitor ! {event, auto_failover_cfg},

    meck:expect(ns_memcached, stats,
        fun(_Bucket, <<"disk-slowness 1">>) ->
                {ok, [{atom_to_binary(StatNum), integer_to_binary(1)},
                      {atom_to_binary(StatSlow), integer_to_binary(1)}]};
            (_Bucket, <<"disk-failures">>) ->
                [{FirstStat, _FirstFail}|_] = kv_stats_monitor:failure_stats(),
                {ok, [{atom_to_binary(FirstStat),
                      integer_to_binary(
                          meck:num_calls(ns_memcached, stats, '_'))}]}
        end),

    %% Should turn the node unhealthy
    ?assert(misc:poll_for_condition(
        fun() ->
            [{_Bucket, Status}] = kv_stats_monitor:get_statuses(),
            Status =:= io_failed
        end, 30000, 100)).

kv_stats_monitor_io_slow_detection_test_() ->
    Tests = [fun kv_stats_monitor_io_slow_detection_t/1,
             fun kv_stats_mon_io_slow_zero/1,
             fun kv_stats_mon_io_slow_equal_count/1,
             fun kv_stats_mon_io_slow_greater_than_count/1,
             fun kv_stats_mon_io_slow_and_disk_failure/1],

    {foreach,
        fun kv_stats_monitor_io_slow_test_setup/0,
        fun kv_stats_monitor_io_slow_test_teardown/1,
        [?cut(T(Stats)) ||
            {Stats, _} <- kv_stats_monitor:slow_stats(),
            T <- Tests]}.

%% Get a list of callbacks in the behaviour spec that have not been called
%% yet. Returns a list of the form [{Module, [{Function, Arity}]}].
callbacks_not_made() ->
    lists:filtermap(
        fun(Module) ->
            Callbacks = health_monitor:behaviour_info(callbacks),
            MissingCallbacks = lists:filter(
                fun({Fun, _Arity}) ->
                    %% Most functions should be called in the child (monitor
                    %% itself)
                    CalledInChild = meck:called(Module, Fun, '_'),

                    %% Some functions will be intercepted by the parent
                    %% (health_monitor) code and won't make it to the
                    %% child, we check that they were actually called
                    ChildPid = whereis(Module),
                    CalledInParent = meck:called(health_monitor,
                        Fun, '_', ChildPid),

                    not (CalledInChild orelse CalledInParent)
                end,
                Callbacks),
            case MissingCallbacks of
                [] -> false;
                _ -> {true, {Module, MissingCallbacks}}
            end
        end, ?TESTABLE_MONITORS).

behaviour_cover_test_setup() ->
    %% Mock each individual monitor that we are testing here. Passthrough as
    %% above as we don't want to test the functionality, but we do need meck
    %% running to check the history later.
    lists:foreach(
      fun(Module) ->
              meck:new(Module, [passthrough])
      end, ?TESTABLE_MONITORS),

    test_setup().

behaviour_cover_test_teardown(SupPid) ->
    lists:foreach(
        fun(Module) ->
            meck:unload(Module)
        end, ?TESTABLE_MONITORS),

    test_teardown(SupPid).

%% Test that we are hitting every function in the behaviour API. This lets us
%% test that the monitors interact with one another as expected (and without
%% crashing).
behaviour_cover_t() ->
    setup_service_monitors([kv, index]),

    %% Called by auto_failover which we're not testing here.
    node_status_analyzer:get_statuses(),

    %% Never actually called, but we still want to test the rest of
    %% the modules.
    gen_server:cast(node_status_analyzer, foo),
    gen_server:cast(kv_monitor, foo),
    gen_server:cast(kv_stats_monitor, foo),

    %% For dcp_traffic_monitor we need to pretend that there is a bucket.
    PidToMonitor =
        erlang:spawn(
          fun() ->
                  %% Block in receive to ensure that this process remains
                  %% alive til we've called get_statuses() at least once
                  receive _ ->
                          ok
                  end
          end),

    Now = erlang:monotonic_time(),
    dcp_traffic_monitor:node_alive(node(), {"default", Now, PidToMonitor}),
    misc:terminate_and_wait(PidToMonitor, "reason"),

    gen_server:cast(index_monitor, {got_connection, self()}),

    ?assert(misc:poll_for_condition(
              fun() ->
                      %% Whilst it would be simpler to just check if we had
                      %% called every function we want to know which
                      %% functions were not called and we can re-use that
                      %% code here.
                      callbacks_not_made() =:= []
              end, 10000, 100), callbacks_not_made()).

behaviour_cover_test_() ->
    {setup,
     fun behaviour_cover_test_setup/0,
     fun behaviour_cover_test_teardown/1,
     [fun behaviour_cover_t/0]}.
