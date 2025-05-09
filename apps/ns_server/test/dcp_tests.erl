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
%% A set of tests for dcp behaviours.
-module(dcp_tests).

-include("ns_config.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").
-include("ns_test.hrl").

-include("ns_common.hrl").

-include_lib("eunit/include/eunit.hrl").

set_conn_count_for_buckets(Buckets, Conns) ->
    lists:foreach(
      fun(Bucket) ->
              fake_chronicle_kv:update_snapshot(
                  {bucket, Bucket, props},
                  [{dcp_connections_between_nodes, Conns}])
      end, Buckets).

dcp_test_setup(#{nodes := Nodes,
                 buckets := Buckets,
                 connections := Conns} = _SetupCfg) ->
    fake_ns_config:setup(),
    fake_chronicle_kv:new(),

    set_conn_count_for_buckets(Buckets, Conns),

    %% We call some stats in memcached to wait for the connections to run some
    %% final connection shutdown. We don't really need to test that here, so
    %% we'll meck it away and test that the connection has gone by this point.
    meck:new(ns_memcached, [passthrough]),
    meck:expect(ns_memcached,
                raw_stats,
                fun(_,_,_,_,_,_) ->
                        %% It should have gone away
                        assert_connections_for_nodes(Nodes, 0),
                        %% Return an empty list to skip the final socket closing
                        {ok, []}
                end),

    %% Now, start up a fake DCP server on all of the "nodes"
    DebugLogging = false,
    FakeDCPServerPids =
        lists:foldl(
          fun(Node, Acc) ->
                  {ok, Pid} = fake_dcp_server:start_link(Node, DebugLogging),
                  [Pid | Acc]
          end, [], Nodes),

    %% And set up connections for all of our buckets
    DCPBucketPids =
        lists:foldl(
          fun(Bucket, Acc) ->
                  {ok, DcpSupPid} = dcp_sup:start_link(Bucket),
                  {ok, DCPReplicationManagerPid} =
                      dcp_replication_manager:start_link(Bucket),
                  {ok, ReplicationManagerPid} =
                      replication_manager:start_link(Bucket),

                  replication_manager:update_replication_count(Bucket, Conns),

                  [DcpSupPid, DCPReplicationManagerPid, ReplicationManagerPid |
                   Acc]
          end, [], Buckets),

    #{fake_dcp_server_pids => FakeDCPServerPids,
      dcp_bucket_pids => DCPBucketPids}.

dcp_test_teardown(_SetupCfg, PidMap) ->
    maps:foreach(
      fun(_Process, Pids) when is_list(Pids) ->
              lists:foreach(
                fun(Pid) ->
                        erlang:unlink(Pid),
                        misc:terminate_and_wait(Pid, normal)
                end,
                Pids)
      end, PidMap),

    meck:unload(ns_memcached),

    fake_chronicle_kv:unload(),
    fake_ns_config:teardown().

set_replication_for_buckets(Buckets, Replications) ->
    lists:foreach(
      fun(Bucket) ->
              replication_manager:set_incoming_replication_map(Bucket,
                                                               Replications)
      end, Buckets).

%% @doc Add vBucket replication for given vBucket from node for given Buckets
-spec change_vbucket_replications_for_buckets([bucket_name()], vbucket_id(),
                                              node()) -> ok.
change_vbucket_replications_for_buckets(Buckets, VBucket, Node) ->
    lists:foreach(
      fun(Bucket) ->
              replication_manager:change_vbucket_replication(Bucket, VBucket,
                                                             Node)
      end, Buckets).

remove_undesired_reps_for_buckets(Buckets, Replications) ->
    lists:foreach(
      fun(Bucket) ->
              replication_manager:remove_undesired_replications(Bucket,
                                                                Replications)
      end, Buckets).

assert_connections(Node, Connections) ->
    %% We don't process the socket close synchronously to the proxy end of the
    %% connection so we have to poll for the connection count to drop to the
    %% expected value. If we see any sporadic failures here then perhaps the
    %% connection test is set to the "before" value and needs to be adjusted.
    %% We'll log a more human readable error at least.
    R = misc:poll_for_condition(
          fun() ->
                  Connections =:=
                      length(fake_dcp_server:get_connections(Node))
          end, 30000, 100),
    case R of
        timeout ->
            Current = length(fake_dcp_server:get_connections(Node)),
            Comment = list_to_binary(io_lib:format("Test failed for node ~p",
                                                   [Node])),
            ?assertEqual(Connections, Current, Comment);
        _ -> ok
    end.

assert_connections_for_nodes(Nodes, Connections) ->
    lists:foreach(
      fun(Node) ->
              assert_connections(Node, Connections)
      end, Nodes).

assert_replication_map_for_nodes_and_buckets(Buckets, Map) ->
    lists:foreach(
      fun(Bucket) ->
              ?assertEqual(Map,
                           replication_manager:get_incoming_replication_map(
                             Bucket))
      end, Buckets).

two_node_conn_and_map_t(#{nodes := Nodes,
                          buckets := Buckets,
                          connections := Connections} = _SetupCfg, _PidMap) ->
    %% We always run DCP replication "stuff" on the consumer side, so we will
    %% always set up for, and pass in node() as Node1.
    ThisNode = node(),
    [ThisNode, OtherNode] = Nodes,

    %% Sanity check
    assert_connections_for_nodes(Nodes, 0),

    ConnectionsMultiple = length(Buckets) * Connections,

    %% Lets replicate vBucket 0
    Map0 = [{OtherNode, [0]}],
    set_replication_for_buckets(Buckets, Map0),
    assert_connections_for_nodes(Nodes, 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map0),

    %% We can remove it too, the connections go away
    set_replication_for_buckets(Buckets, []),
    assert_connections_for_nodes(Nodes, 0 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, []),

    %% Add vBucket 0 and 1
    Map1 = [{OtherNode, [0, 1]}],
    set_replication_for_buckets(Buckets, Map1),
    assert_connections_for_nodes(Nodes, 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map1),

    %% And remove 1
    Map2 = [{OtherNode, [0]}],
    set_replication_for_buckets(Buckets, Map2),
    assert_connections_for_nodes(Nodes, 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map2),

    %% And remove 0
    set_replication_for_buckets(Buckets, []),
    assert_connections_for_nodes(Nodes, 0 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, []),

    %% Now some tests that build up and take down replications over time
    change_vbucket_replications_for_buckets(Buckets, 0, OtherNode),
    assert_connections_for_nodes(Nodes, 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map0),

    change_vbucket_replications_for_buckets(Buckets, 1, OtherNode),
    assert_connections_for_nodes(Nodes, 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map1),

    remove_undesired_reps_for_buckets(Buckets, [{OtherNode, [0]}]),
    assert_connections_for_nodes(Nodes, 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map0).

multi_node_conn_and_map_t(#{nodes := Nodes,
                            buckets := Buckets,
                            connections := Connections}, _PidMap) ->
    ThisNode = node(),
    [ThisNode, OtherNode1, OtherNode2] = Nodes,

    %% Sanity check
    assert_connections_for_nodes(Nodes, 0),
    ConnectionsMultiple = length(Buckets) * Connections,

    Map0 = [{OtherNode1, [0]}],
    set_replication_for_buckets(Buckets, Map0),
    assert_connections_for_nodes([ThisNode, OtherNode1],
                                 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map0),

    %% Setting the map does at it says, so our old connection to Node2 goes
    Map1 = [{OtherNode2, [1]}],
    set_replication_for_buckets(Buckets, Map1),
    assert_connections_for_nodes([ThisNode, OtherNode2],
                                 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map1),

    %% But we can establish connections to both, and we'll create connections
    %% to both.
    Map2 = [{OtherNode1, [0]}, {OtherNode2, [1]}],
    set_replication_for_buckets(Buckets, Map2),
    assert_connections_for_nodes([OtherNode1, OtherNode2],
                                 1 * ConnectionsMultiple),
    assert_connections_for_nodes([ThisNode], 2 * 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(
      Buckets, Map2),

    set_replication_for_buckets(Buckets, []),
    assert_connections_for_nodes(Nodes, 0 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, []),

    %% Now some tests that build up and take down replications over time
    change_vbucket_replications_for_buckets(Buckets, 0, OtherNode1),
    assert_connections_for_nodes([OtherNode1], 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map0),

    change_vbucket_replications_for_buckets(Buckets, 1, OtherNode2),
    assert_connections_for_nodes([OtherNode1, OtherNode2],
                                 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map2),

    Map3 = [{OtherNode1, [0, 2]}, {OtherNode2, [1]}],
    change_vbucket_replications_for_buckets(Buckets, 2, OtherNode1),
    assert_connections_for_nodes([OtherNode1, OtherNode2],
                                 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map3),

    Map4 = [{OtherNode1, [0, 2]}, {OtherNode2, [1, 3]}],
    change_vbucket_replications_for_buckets(Buckets, 3, OtherNode2),
    assert_connections_for_nodes([OtherNode1, OtherNode2],
                                 1 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map4),

    remove_undesired_reps_for_buckets(Buckets, [{OtherNode1, [0]}]),
    assert_connections_for_nodes([OtherNode1], 1 * ConnectionsMultiple),
    assert_connections_for_nodes([OtherNode2], 0 * ConnectionsMultiple),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map0).

change_connection_count_t(#{nodes := Nodes,
                            buckets := Buckets}, _PidMap) ->
    %% First we need a connection to test with
    [_, OtherNode] = Nodes,
    Map0 = [{OtherNode, [0, 1]}],

    set_replication_for_buckets(Buckets, Map0),

    %% Sanity check
    assert_connections_for_nodes(Nodes, 1),

    %% Now lets update our key
    set_conn_count_for_buckets(Buckets, 2),

    %% Nothing happens until we prod the replication_manager, which the janitor
    %% does.
    assert_connections_for_nodes(Nodes, 1),

    %% Before we do that, we must record our existing replications, usually the
    %% janitor works this out for us, but we aren't running it in these tests
    BucketReps =
        lists:map(
          fun(Bucket) ->
                  {Bucket, replication_manager:get_incoming_replication_map(Bucket)}
                                                % replication_manager:update_replication_count(Bucket, 2)
          end, Buckets),

    %% First the janitor nukes the existing replications
    lists:foreach(
      fun(Bucket) ->
              replication_manager:update_replication_count(Bucket, 2)
      end, Buckets),

    assert_connections_for_nodes(Nodes, 0),

    %% Then the call is made to create the new ones
    lists:foreach(
      fun({Bucket, Replications}) ->
              replication_manager:set_incoming_replication_map(Bucket, Replications)
      end, BucketReps),

    %% And we should have more connections now.
    assert_connections_for_nodes(Nodes, 2),
    assert_replication_map_for_nodes_and_buckets(Buckets, Map0).

make_nodes(Count) ->
    [node()] ++
        lists:map(
          fun(Node) ->
                  %% Node names have an '@' in them. We need that followed by a
                  %% resolvable host, so localhost it is.
                  Name = "otherNode" ++ integer_to_list(Node) ++
                      "@" ++ misc:localhost(),
                  list_to_atom(Name)
          end,
          %% Count - 2 because we will include our node in this list too
          lists:seq(0, Count - 2)).

dcp_test_() ->
    BucketCombinations = [["Bucket1"], ["Bucket1", "Bucket2"]],
    ConnectionCombinations = [1, 2, 4],

    TwoNodeTest =
        [{list_to_binary(
            io_lib:format("Two node conn and map test buckets ~p "
                          "connections ~p",
                          [Buckets, Conns])),
          fun two_node_conn_and_map_t/2,
          2,
          Buckets,
          Conns} || Buckets <- BucketCombinations,
                    Conns <- ConnectionCombinations],

    MultiNodeTest =
        [{list_to_binary(
            io_lib:format("Multi node conn and map test buckets ~p "
                          "connections ~p",
                          [Buckets, Conns])),
          fun multi_node_conn_and_map_t/2,
          3,
          Buckets,
          Conns} || Buckets <- BucketCombinations,
                    Conns <- ConnectionCombinations],

    ConnectionCountChangeTest =
        [{list_to_binary(
            io_lib:format("Conn count change test buckets ~p "
                          "connections ~p",
                          [Buckets, Conns])),
            fun change_connection_count_t/2,
            2,
            Buckets,
            Conns} || Buckets <- [["Bucket1"]],
            Conns <- [1]],

    TestCombinations = TwoNodeTest ++ MultiNodeTest ++ ConnectionCountChangeTest,

    %% foreachx here to let us pass parameters to setup.
    {foreachx,
     fun dcp_test_setup/1,
     fun dcp_test_teardown/2,
     [{#{buckets => Buckets,
         nodes => make_nodes(Nodes),
         connections => Conns},
       fun(T, R) ->
               {Name, ?_test(TestFun(T, R))}
       end} || {Name, TestFun, Nodes, Buckets, Conns} <- TestCombinations]}.
