%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Monitor and maintain the vbucket layout of each bucket.
%% There is one of these per bucket.
%%
%% @doc Failover implementation.
%%

-module(failover).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([fix_vbucket_map_test_wrapper/1, meck_query_vbuckets/2]).
-endif.

-export([start/2, is_possible/2, orchestrate/2,
         get_failover_vbuckets/2, promote_max_replicas/4,
         clear_failover_vbuckets_sets/1,
         nodes_needed_for_durability_failover/2,
         can_preserve_durability_majority/2]).

-define(DATA_LOST, 1).
-define(FAILOVER_OPS_TIMEOUT, ?get_timeout(failover_ops_timeout, 10000)).

start(Nodes, Options) ->
    Parent = self(),
    ?log_debug("Starting failover with Nodes = ~p, Options = ~p",
               [Nodes, Options]),

    %% Check if failover is possible. This is inherently unsafe. If our config
    %% is stale (which may be the case if we have just been partitioned off from
    %% the rest of the cluster) then this check may not have the most up to date
    %% information and we may pass it for a failover that should not be possible
    %% (i.e. if we are going to fail over the last node of some service). This
    %% check will be repeated after we gather the quorum and sync the config for
    %% the sake of safety. We still perform it here to avoid trying to take the
    %% quorum and blocking the orchestrator for impossible failovers.
    case is_possible(Nodes, Options) of
        ok ->
            Pid = proc_lib:spawn_link(
                    fun () ->
                            case run(Nodes, Options, Parent) of
                                {ok, UnsafeNodes} ->
                                    erlang:exit({shutdown, {ok, UnsafeNodes}});
                                Error ->
                                    erlang:exit(Error)
                            end
                    end),
            receive
                started ->
                    ?log_debug("Failover started. Pid = ~p", [Pid]),
                    {ok, Pid};
                {'EXIT', Pid, Reason} ->
                    ?log_debug("Failover ~p exited with ~p", [Pid, Reason]),
                    Reason
            end;
        Error ->
            ?log_debug("Failover is not possible due to ~p", [Error]),
            Error
    end.

allow_unsafe(Options) ->
    maps:get(allow_unsafe, Options, false).

run(Nodes, Options, Parent) when Nodes =/= [] ->
    Result = leader_activities:run_activity(
               failover, majority,
               ?cut(activity_body(Nodes, Options, Parent)),
               [{unsafe, allow_unsafe(Options)}]),

    case Result of
        {leader_activities_error, _, {quorum_lost, _}} ->
            quorum_lost;
        {leader_activities_error, _, {no_quorum, _}} ->
            orchestration_unsafe;
        _ ->
            Result
    end.

activity_body(Nodes, Options, Parent) ->
    case check_safeness(allow_unsafe(Options)) of
        true ->
            case maybe_restore_chronicle_quorum(Nodes, Options) of
                {ok, NewOptions} ->
                    Parent ! started,
                    orchestrate(Nodes,
                                maps:put(durability_aware, true, NewOptions));
                Error ->
                    Error
            end;
        false ->
            orchestration_unsafe
    end.

maybe_restore_chronicle_quorum(FailedNodes, Options) ->
    case allow_unsafe(Options) andalso chronicle_compat:enabled() of
        true ->
            restore_chronicle_quorum(FailedNodes, Options);
        false ->
            {ok, Options}
    end.

restore_chronicle_quorum(FailedNodes, Options) ->
    ?log_info("Attempting quorum loss failover of = ~p", [FailedNodes]),
    Ref = erlang:make_ref(),
    case chronicle_master:start_failover(FailedNodes, Ref) of
        ok ->
            case check_chronicle_quorum() of
                true ->
                    ns_cluster:counter_inc(quorum_failover_success),
                    {ok, maps:put(quorum_failover, Ref, Options)};
                false ->
                    orchestration_unsafe
            end;
        {error, Error} ->
            Error;
        {incompatible_with_previous, _} = Error ->
            Error
    end.

check_safeness(true) ->
    true;
check_safeness(false) ->
    case chronicle_compat:enabled() of
        true ->
            check_chronicle_quorum() andalso
                check_for_unfinished_failover();
        false ->
            true
    end.

check_for_unfinished_failover() ->
    case chronicle_master:get_prev_failover_nodes(direct) of
        [] ->
            true;
        Nodes ->
            ?log_info("Unfinished failover of nodes ~p was found.", [Nodes]),
            false
    end.

check_chronicle_quorum() ->
    case chronicle:check_quorum() of
        true ->
            true;
        {false, What} ->
            ?log_info("Cannot establish quorum due to: ~p", [What]),
            false
    end.

orchestrate(Nodes, Options) when Nodes =/= [] ->
    ale:info(?USER_LOGGER, "Starting failing over ~p", [Nodes]),
    master_activity_events:note_failover(Nodes),

    Res =
        case config_sync_and_orchestrate(Nodes, Options) of
            {done, ErrorNodes, UnsafeNodes} ->
                FailedOver = Nodes -- [N || {N, _} <- UnsafeNodes],
                case ErrorNodes of
                    [] ->
                        ns_cluster:counter_inc(failover_complete),
                        ale:info(?USER_LOGGER, "Failed over ~p: ok",
                                 [FailedOver]);
                    _ ->
                        ns_cluster:counter_inc(failover_incomplete),
                        ale:error(?USER_LOGGER,
                                  "Failed over ~p. Failover couldn't complete "
                                  "on some nodes:~n~p",
                                  [FailedOver, ErrorNodes])
                end,
                deactivate_nodes(FailedOver, Options),
                {ok, UnsafeNodes};
            Error ->
                ns_cluster:counter_inc(failover_failed),
                ale:error(?USER_LOGGER, "Failover failed with ~p", [Error]),
                Error
        end,
    ns_cluster:counter_inc(failover),
    master_activity_events:note_failover_ended(),
    Res.

config_sync_and_orchestrate(Nodes, Options) ->
    case pre_failover_config_sync(Nodes, Options) of
        ok ->
            try failover(Nodes, Options) of
                {ErrorNodes, UnsafeNodes} ->
                    {done, ErrorNodes, UnsafeNodes}
            catch throw:{failed, Bucket, Msg} ->
                    {failover_failed, Bucket, Msg}
            end;
        Error ->
            Error
    end.

pre_failover_config_sync(FailedNodes, Options) ->
    case durability_aware(Options) of
        true ->
            Timeout = ?get_timeout(failover_config_pull, 10000),
            SyncNodes = config_sync_nodes(FailedNodes),

            ?log_info("Going to pull config "
                      "from ~p before failover", [SyncNodes]),

            case chronicle_compat:config_sync(pull, SyncNodes, Timeout) of
                ok ->
                    ok;
                Error ->
                    ?log_error("Config pull from ~p failed: ~p",
                               [SyncNodes, Error]),
                    config_sync_failed
            end;
        false ->
            ok
    end.

config_sync_nodes(FailedNodes) ->
    Nodes = ns_cluster_membership:get_nodes_with_status(_ =/= inactiveFailed),
    Nodes -- FailedNodes.

deactivate_nodes(Nodes, Options) ->
    ok = leader_activities:deactivate_quorum_nodes(Nodes),
    case maps:get(quorum_failover, Options, undefined) of
        undefined ->
            ale:info(?USER_LOGGER, "Deactivating failed over nodes ~p",
                     [Nodes]),
            ok = chronicle_master:deactivate_nodes(Nodes);
        Ref ->
            ale:info(?USER_LOGGER, "Removing failed over nodes ~p",
                     [Nodes]),
            ok = chronicle_master:complete_failover(Nodes, Ref)
    end.

%% @doc Fail one or more nodes. Doesn't eject the node from the cluster. Takes
%% effect immediately.
failover(Nodes, Options) ->
    %% Note that we are checking for fail over possibility whilst holding the
    %% quorum and after syncing the config to ensure that we have the most up
    %% to date config when checking if it is possible to fail over.
    case is_possible(Nodes, Options) of
        ok -> continue;
        Error ->
          ?log_error("Failover is not possible due to ~p", [Error]),
          erlang:exit(Error)
    end,

    not maps:is_key(quorum_failover, Options) orelse
        failover_collections(),
    KVNodes = ns_cluster_membership:service_nodes(Nodes, kv),
    KVErrorNodes = failover_buckets(KVNodes, Options),
    {ServicesErrorNodes, UnsafeNodes} =
        failover_services(Nodes, KVNodes, Options),
    {lists:umerge([KVErrorNodes, ServicesErrorNodes]), UnsafeNodes}.

failover_collections() ->
    [collections:bump_epoch(BucketName) ||
        {BucketName, BucketConfig} <- ns_bucket:get_buckets(),
        collections:enabled(BucketConfig)].

set_failover_config(PrepResults, Nodes) ->
    BucketsMapUpdate =
        [{Bucket, NewMap} ||
            {Bucket, {membase, _, NewMap, _}} <- PrepResults, NewMap =/= []],

    BucketsServersUpdate =
        [Bucket ||
            {Bucket, {Type, _, NewMap, _}} <- PrepResults,
            (Type =:= memcached) or (NewMap =:= [])],

    %% we still need to make sure to remove ourselves from the bucket server
    %% list in cases of memcached buckets or buckets with no initial map
    ns_bucket:remove_servers_from_buckets(BucketsServersUpdate, Nodes),

    ns_bucket:set_buckets_config_failover(BucketsMapUpdate, Nodes),
    ok.

failover_buckets_group([], _Nodes) ->
    [];
failover_buckets_group(PrepResults, Nodes) ->
    ok = set_failover_config(PrepResults, Nodes),

    lists:flatmap(
      fun ({Bucket, {membase, BucketCfg, NewMap, Options1}}) ->
              Res = finalize_bucket_failover(Bucket, BucketCfg,
                                             NewMap, Nodes, Options1),
              master_activity_events:note_bucket_failover_ended(
                Bucket, Nodes),
              Res;
          ({Bucket, {memcached, _BucketCfg, _NewMap, _Options1}}) ->
              master_activity_events:note_bucket_failover_ended(
                Bucket, Nodes),
              []
      end, PrepResults).

failover_buckets([], _Options) ->
    [];
failover_buckets(Nodes, Options) ->
    PrepResults = failover_buckets_prep(ns_bucket:get_buckets(),
                                        Nodes, Options),

    BulkFactor = ns_config:read_key_fast(failover_bulk_buckets_config_factor,
                                         ?MAX_BUCKETS_SUPPORTED),
    Results = lists:flatmap(failover_buckets_group(_, Nodes),
                            misc:split(BulkFactor, PrepResults)),

    update_failover_vbuckets(Results),
    failover_handle_results(Results).

clear_failover_vbuckets_sets(Nodes) ->
    [{{node, N, failover_vbuckets}, []} || N <- Nodes].

update_failover_vbuckets(Results) ->
    GroupedByNode =
        misc:groupby_map(fun (L) ->
                                 Node   = proplists:get_value(node, L),
                                 Bucket = proplists:get_value(bucket, L),
                                 VBs    = proplists:get_value(vbuckets, L),

                                 {Node, {Bucket, VBs}}
                         end, Results),
    {ok, _} =
        chronicle_compat:transaction(
          [{node, N, failover_vbuckets} || {N, _} <- GroupedByNode],
          fun (Snapshot) ->
                  {commit,
                   lists:filtermap(update_failover_vbuckets_sets(Snapshot, _),
                                   GroupedByNode)}
          end).

update_failover_vbuckets_sets(Snapshot, {Node, BucketResults}) ->
    ExistingBucketResults = get_failover_vbuckets(Snapshot, Node),
    Merged = merge_failover_vbuckets(ExistingBucketResults, BucketResults),

    ?log_debug("Updating failover_vbuckets for ~p with ~p~n"
               "Existing vbuckets: ~p~nNew vbuckets: ~p",
               [Node, Merged, ExistingBucketResults, BucketResults]),

    case Merged of
        ExistingBucketResults ->
            false;
        _ ->
            {true, {set, {node, Node, failover_vbuckets}, Merged}}
    end.

merge_failover_vbuckets(ExistingBucketResults, BucketResults) ->
    Grouped =
        misc:groupby_map(fun functools:id/1,
                         ExistingBucketResults ++ BucketResults),
    lists:map(fun ({B, [VBs1, VBs2]}) when is_list(VBs1), is_list(VBs2) ->
                      {B, lists:usort(VBs1 ++ VBs2)};
                  ({B, [VBs]}) when is_list(VBs) ->
                      {B, lists:sort(VBs)}
              end, Grouped).

-ifdef(TEST).
merge_failover_vbuckets_test() ->
    ?assertEqual(
       [{"test", [0, 1, 2, 3]}, {"test1", [0, 1, 2, 3]}],
       merge_failover_vbuckets(
         [], [{"test", [0, 1, 2, 3]}, {"test1", [0, 1, 2, 3]}])),
    ?assertEqual(
       [{"test", [0, 1, 2, 3]}, {"test1", [0, 1, 2, 3]}],
       merge_failover_vbuckets(
         [{"test", [0, 1, 2, 3]}], [{"test1", [0, 1, 2, 3]}])),
    ?assertEqual(
       [{"test", [0, 1, 2, 3]}],
       merge_failover_vbuckets([{"test", [0, 3]}], [{"test", [1, 2]}])),
    ?assertEqual(
       [{"test", [0, 1, 2, 3]}],
       merge_failover_vbuckets([{"test", [0, 2, 3]}], [{"test", [1, 2]}])).
-endif.

failover_handle_results(Results) ->
    NodeStatuses =
        misc:groupby_map(fun (Result) ->
                                 Node   = proplists:get_value(node, Result),
                                 Status = proplists:get_value(status, Result),

                                 {Node, Status}
                         end, Results),

    lists:filtermap(fun ({Node, Statuses}) ->
                            NonOKs = [S || S <- Statuses, S =/= ok],

                            case NonOKs of
                                [] ->
                                    false;
                                _ ->
                                    {true, Node}
                            end
                    end, NodeStatuses).

failover_buckets_prep(BucketsConfig, Nodes, Options) ->
    lists:map(
      fun({Bucket, BucketConfig}) ->
              try
                  %% Verify that the server list is consistent with cluster
                  %% membership states.
                  ok = ns_janitor:check_server_list(Bucket, BucketConfig),

                  master_activity_events:note_bucket_failover_started(Bucket,
                                                                      Nodes),

                  Type  = ns_bucket:bucket_type(BucketConfig),
                  Map = proplists:get_value(map, BucketConfig, []),
                  {Bucket, failover_bucket_prep(Type, Nodes, Bucket,
                                                BucketConfig, Map, Options)}
              catch throw:{failed, Msg} ->
                      ?log_error("Caught failover exception: ~p", [Msg]),
                      throw({failed, Bucket, Msg})
              end
      end, BucketsConfig).

failover_bucket_prep(memcached, _Nodes, _Bucket, BucketConfig, Map,
                     Options) ->
    {memcached, BucketConfig, Map, Options};
failover_bucket_prep(membase, _Nodes, Bucket, BucketConfig, [], Options) ->
    ?log_debug("Skipping failover of bucket ~p because it has no vbuckets.",
               [Bucket]),
    {membase, BucketConfig, [], Options};
failover_bucket_prep(membase, Nodes, Bucket, BucketConfig, Map, Options) ->
    NewMap = fix_vbucket_map(Nodes, Bucket, Map, Options),
    true = (NewMap =/= undefined),

    ?log_debug("Original vbucket map: ~p~n"
               "VBucket map with failover applied: ~p", [Map, NewMap],
                                                        [{chars_limit, -1}]),

    case [I || {I, [undefined|_]} <- misc:enumerate(NewMap, 0)] of
        [] -> ok; % Phew!
        MissingVBuckets ->
            ?rebalance_error("Lost data in ~p for ~w",
                             [Bucket, MissingVBuckets]),
            ?user_log(?DATA_LOST,
                      "Data has been lost for ~B% of vbuckets in bucket ~p.",
                      [length(MissingVBuckets) * 100 div length(Map), Bucket])
    end,

    %% These values will be used to complete the failover in the finalize phase
    {membase, BucketConfig, NewMap, Options}.

finalize_bucket_failover(Bucket, BucketConfig, NewMap, Nodes, Options) ->
    R = janitor_bucket(Nodes, Bucket, NewMap, Options),
    OldMap = proplists:get_value(map, BucketConfig, []),

    [[{bucket, Bucket},
      {node, N},
      {status, R},
      {vbuckets, node_vbuckets(OldMap, N)}] || N <- Nodes].

failover_services(Nodes, _, #{skip_safety_check := true}) ->
    {failover_services(Nodes), []};
failover_services(Nodes, KVNodes, #{auto := true,
                                    down_nodes := DownNodes}) ->
    {ValidNodes, UnsafeNodes} =
        auto_failover:validate_services_safety(Nodes, DownNodes, KVNodes),
    {case ValidNodes of
         [] ->
             [];
         ValidNodes ->
             failover_services(ValidNodes)
     end, UnsafeNodes};
failover_services(Nodes, _, _) ->
    {failover_services(Nodes), []}.

failover_services(Nodes) ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    Services0 = lists:flatmap(
                  ns_cluster_membership:node_services(Snapshot, _), Nodes),
    Services  = lists:usort(Services0) -- [kv],

    Results = lists:flatmap(failover_service(Snapshot, _, Nodes), Services),
    failover_handle_results(Results).

failover_service(Snapshot, Service, Nodes) ->
    ok = ns_cluster_membership:failover_service_nodes(Snapshot, Service, Nodes),

    %% We're refetching the config since failover_service_nodes updated the
    %% one that we had.
    Result = service_janitor:complete_service_failover(Service, Nodes),
    case Result of
        ok ->
            ?log_debug("Failed over service ~p on nodes ~p successfully",
                       [Service, Nodes]);
        _ ->
            ?log_error("Failed to failover service ~p on nodes ~p: ~p",
                       [Service, Nodes, Result])
    end,

    [[{node, Node},
      {status, Result},
      {service, Service}] || Node <- Nodes].

janitor_bucket(_, _, [], _) ->
    ok;
janitor_bucket(Nodes, Bucket, Map, Options) ->
    true = (Map =/= []),
    CleanupOptions = janitor_cleanup_options(Nodes, Options),
    case (catch ns_janitor:cleanup(Bucket, CleanupOptions)) of
        ok ->
            ok;
        {error, _, BadNodes} ->
            ?rebalance_error("Skipped vbucket activations and "
                             "replication topology changes because not "
                             "all remaining nodes were found to have "
                             "healthy bucket ~p: ~p", [Bucket, BadNodes]),
            janitor_failed;
        Error ->
            ?rebalance_error("Janitor cleanup of ~p "
                             "failed after failover of ~p: ~p",
                             [Bucket, Nodes, Error]),
            janitor_failed
    end.

janitor_cleanup_options(FailedNodes, FailoverOptions) ->
    [{sync_nodes, config_sync_nodes(FailedNodes)},
     {failover_nodes, FailedNodes},
     {pull_config, false},
     {push_config, durability_aware(FailoverOptions)}].

durability_aware(Options) ->
    cluster_compat_mode:preserve_durable_mutations() andalso
        maps:get(durability_aware, Options, false).

fix_vbucket_map(FailoverNodes, Bucket, Map, Options) ->
    case durability_aware(Options) of
        true ->
            promote_max_replicas(FailoverNodes, Bucket, Map,
                                 mb_map:promote_replica(_, FailoverNodes));
        false ->
            mb_map:promote_replicas(Map, FailoverNodes)
    end.

%% Get the number of nodes required to prevent us from losing durable writes
get_required_node_count_to_preserve_majority(Chain) when is_list(Chain) ->
    %% Note that the node count required to preserve durable writes is not, in
    %% fact, a majority.
    %%
    %% Consider the following chain [a,b]. In such a case a majority is both
    %% nodes. That means that any durable writes need to be written to 2 nodes,
    %% and keeping 1 is what we need to preserve durable writes.
    %%
    %% It's useful to enumerate other the example scenarios here so lets also
    %% consider what happens when we have various replica counts:
    %%
    %% 0 replicas - [a]:
    %%
    %% We do not prevent durable writes from being used with 0 replicas,
    %% regardless of how un-durable that is. Failing over any node with 0
    %% replicas should trigger a safety check and either warn the user if they
    %% are failing over via the UI or prevent auto fail over.
    %%
    %% 1 replica - [a,b]:
    %%
    %% A majority is 2 nodes (all of them) so we can lose either node without
    %% losing durable writes. We cannot lose both, so we can only fail over 1
    %% node.
    %%
    %% 2 replicas - [a, b, c]:
    %%
    %% A majority is 2 nodes. The active must be part of the majority, and we
    %% must replicate a write to at least one of the replicas, b, or c. The
    %% other replica could be arbitrarily behind. Let b be the replica that is
    %% up to date, and c the replica that is arbitrarily behind. In such a case
    %% we could fail over c regardless as it is behind the other nodes. We could
    %% fail over a OR b without losing durable writes; were we to fail over
    %% both nodes then we could lose durable writes as c is arbitrarily behind.
    %% As we have a multi-node system which aims to evenly distribute
    %% active/replica vBuckets, we should not take into consideration whether or
    %% not a node is active or replica. As such, we can only fail over 1 node
    %% without running the risk of losing durable writes when we have 2
    %% replicas.
    %%
    %% 3 replicas - [a, b, c, d]:
    %%
    %% We don't currently support 3 replicas, but a more generic algorithm to
    %% calculate the number of nodes that we require is a good thing, so lets
    %% consider that here too.
    %%
    %% A majority is 3 nodes. The active, again, must be part of the majority,
    %% so as with the 2 replica scenario the number of nodes that we can fail
    %% over must be such that we can fail over any arbitrary node. As one node
    %% could be arbitrarily behind, similarly to the case for 2 replicas, we
    %% cannot fail over all nodes that make up our majority. As such, we can
    %% only fail over 2 nodes to ensure that we do not lose durable writes.
    ceil(length(Chain) / 2).

check_for_majority(Chain, FailoverNodes) ->
    %% Note here the check that N =/= undefined. This deals with the case in
    %% which have previously failed over a node and have undefined replicas in
    %% the chain. We filter them out of the list of nodes that we consider to be
    %% capable of being part of the majority here as they cannot be part of a
    %% majority if they do not exist.
    Majority = get_required_node_count_to_preserve_majority(Chain),
    length([N || N <- Chain,
            not lists:member(N, FailoverNodes),
            N =/= undefined]) >= Majority.

%% Returns whether or not a majority (for durable writes) can be maintained if
%% the given nodes are failed over
-spec can_preserve_durability_majority([[node()]], [node()]) -> boolean().
can_preserve_durability_majority(Map, FailoverNodes) ->
    lists:all(fun (Chain) ->
                  check_for_majority(Chain, FailoverNodes)
              end, Map).

should_promote_max_replica([Master | _] = Chain, FailoverNodes) ->
    lists:member(Master, FailoverNodes) andalso
        length([N || N <- Chain, not lists:member(N, FailoverNodes)]) > 1.

map_and_nodes_to_query(FailoverNodes, Map, PromoteReplicaFun) ->
    MarkedMap = [{should_promote_max_replica(Chain, FailoverNodes),
                  PromoteReplicaFun(Chain)} || Chain <- Map],

    EnumeratedMap = misc:enumerate(MarkedMap, 0),

    {EnumeratedMap, nodes_to_query(EnumeratedMap, FailoverNodes)}.

nodes_needed_for_durability_failover(Map, FailoverNodes) ->
    case cluster_compat_mode:preserve_durable_mutations() of
        true ->
            {_, NodesToQuery} =
                map_and_nodes_to_query(
                  FailoverNodes, Map, mb_map:promote_replica(_, FailoverNodes)),
            [N || {N, _} <- NodesToQuery];
        false ->
            []
    end.

promote_max_replicas(FailoverNodes, Bucket, Map, PromoteReplicaFun) ->
    {EnumeratedMap, NodesToQuery} =
        map_and_nodes_to_query(FailoverNodes, Map, PromoteReplicaFun),

    %% failover_nodes option causes replications from failed over nodes to be
    %%                shut down on 6.6.3 nodes
    %% stop_replications - backward compatibility with 6.6.2 on which
    %%                     failover_nodes option is ignored
    {Info, BadNodes} =
        janitor_agent:query_vbuckets(
          Bucket, NodesToQuery,
          [high_seqno, high_prepared_seqno],
          [stop_replications, {timeout, ?FAILOVER_OPS_TIMEOUT},
           {failover_nodes, FailoverNodes}]),

    BadNodes =:= [] orelse
        throw_failover_error("Failed to get failover info for bucket ~p: ~p",
                             [Bucket, BadNodes]),

    NodesToQuery =:= [] orelse
        ?log_debug("Retrieved the following vbuckets information: ~p",
                   [dict:to_list(Info)]),

    [fix_chain(MarkedChain, Info) || MarkedChain <- EnumeratedMap].

fix_chain({_VBucket, {false, Chain}}, _Info) ->
    Chain;
fix_chain({VBucket, {true, Chain}}, Info) ->
    NodeStates = janitor_agent:fetch_vbucket_states(VBucket, Info),
    case find_max_replica(Chain, NodeStates) of
        not_found ->
            Chain;
        MaxReplica ->
            [MaxReplica | lists:delete(MaxReplica, Chain)]
    end.

nodes_to_query(MarkedMap, FailoverNodes) ->
    NodeVBs =
        lists:flatmap(
          fun ({_VB, {false, _Chain}}) ->
                  [];
              ({VB, {true, Chain}}) ->
                  [{Node, VB} || Node <- Chain, Node =/= undefined,
                                 not lists:member(Node, FailoverNodes)]
          end, MarkedMap),
    [{N, lists:usort(VBs)} ||
        {N, VBs} <- misc:groupby_map(fun functools:id/1, NodeVBs),
        VBs =/= []].

throw_failover_error(Msg, Params) ->
    throw({failed, lists:flatten(io_lib:format(Msg, Params))}).

%% A replica is considered ahead of another replica if its last snapshot seqno
%% is greater, if they are the same, the replica with greater high_seqno is then
%% considered ahead.
find_max_replica(Chain, NodeStates) ->
    Get = fun (K, P) ->
                  V = proplists:get_value(K, P), true = V =/= undefined, V
          end,
    ChainStates =
        lists:filtermap(
          fun (undefined) ->
                  false;
              (Node) ->
                  case lists:keyfind(Node, 1, NodeStates) of
                      {Node, _, Props} ->
                          {true,
                           {Node,
                            {Get(high_prepared_seqno, Props),
                             Get(high_seqno, Props)}}};
                      false ->
                          false
                  end
          end, Chain),
    case ChainStates of
        [] ->
            not_found;
        _ ->
            {MaxReplica, _} = misc:min_by(fun ({_, SeqNos}, {_, MaxSeqNos}) ->
                                                  SeqNos > MaxSeqNos
                                          end, ChainStates),
            MaxReplica
    end.

node_vbuckets(Map, Node) ->
    [V || {V, Chain} <- misc:enumerate(Map, 0),
          lists:member(Node, Chain)].

is_possible(FailoverNodes, Options) ->
    ActiveNodes = ns_cluster_membership:active_nodes(),
    KVActiveNodes = ns_cluster_membership:service_nodes(ActiveNodes, kv),
    NodesWanted = ns_node_disco:nodes_wanted(),
    try
        case KVActiveNodes -- FailoverNodes of
            [] ->
                ?log_error("Attempt to fail over last KV node. "
                           "Failover nodes: ~p, KV nodes ~p",
                           [FailoverNodes, ActiveNodes]),
                throw(last_node);
            _ ->
                ok
        end,
        case FailoverNodes -- NodesWanted of
            [] ->
                ok;
            _ ->
                ?log_error("Failover of unknown nodes ~p is requested. "
                           "Known nodes: ~p", [FailoverNodes, NodesWanted]),
                throw(unknown_node)
        end,
        case FailoverNodes -- ActiveNodes of
            [] ->
                ok;
            _ ->
                case allow_unsafe(Options) of
                    true ->
                        %% inactiveFailed and inactiveAdded nodes participate in
                        %% chronicle quorum, therefore, in case of unsafe quorum
                        %% failover we allow failover of these nodes.
                        ok;
                    false ->
                        ?log_error(
                           "Failover of inactive nodes ~p is requested. "
                           "Active nodes: ~p", [FailoverNodes, ActiveNodes]),
                        throw(inactive_node)
                end
        end
    catch
        throw:Error ->
            Error
    end.

get_failover_vbuckets(Config, Node) ->
    chronicle_compat:get(Config, {node, Node, failover_vbuckets},
                         #{default => []}).


-ifdef(TEST).

fix_vbucket_map_test_wrapper(Funs) ->
    {foreach,
     fun() ->
             meck:new(cluster_compat_mode, [passthrough]),
             meck:new(janitor_agent, [passthrough]),
             meck:new(ns_config, [passthrough]),
             meck:expect(cluster_compat_mode, preserve_durable_mutations,
                         fun () -> true end),
             meck:expect(ns_config, get_timeout, fun (_, _) -> 1234 end)
     end,
     fun(_) ->
             meck:unload(ns_config),
             meck:unload(janitor_agent),
             meck:unload(cluster_compat_mode)
     end,
     Funs}.

meck_query_vbuckets(Input, Output) ->
    meck:expect(
      janitor_agent, query_vbuckets,
      fun ("test", Nodes, [high_seqno, high_prepared_seqno],
           [stop_replications, {timeout, 1234}, {failover_nodes, [a ,b]}]) ->
              ?assertEqual(Input, lists:sort(Nodes)),
              {dict:from_list(
                 [{VB, [{N, replica,
                         [{high_prepared_seqno, HPS},
                          {high_seqno, HS}]} || {N, HPS, HS} <- Stats]} ||
                     {VB, Stats} <- Output]), []}
      end).

load_group_failover_test_common_modules() ->
    meck:new([ns_config, ns_janitor, master_activity_events,
              cluster_compat_mode, chronicle_compat, ns_bucket], [passthrough]),

    meck:expect(ns_janitor, check_server_list,
                fun (_,_) ->
                        ok
                end),

    meck:expect(master_activity_events, note_bucket_failover_started,
                fun (_,_) ->
                        ok
                end),

    meck:expect(master_activity_events, note_bucket_failover_ended,
                fun (_,_) ->
                        ok
                end),

    meck:expect(cluster_compat_mode, preserve_durable_mutations,
                fun () -> true end),

    meck:expect(chronicle_compat, transaction,
                fun (_,_) ->
                        {ok, ok}
                end),

    meck:expect(chronicle_compat, get,
                fun (_,_,_) ->
                        []
                end),

    meck:expect(ns_config, get_timeout, fun (_, _) -> 1234 end).

get_test_bucket_config() ->
    B1Cfg = [{servers, [a,b,c]}, {type, membase}, {map, [[a, b], [c, a]]}],
    B2Cfg = [{servers, [a,b,c]}, {type, membase}, {map, []}],
    B3Cfg = [{servers, [a,b,c]}, {type, memcached}, {map, [[a, b], [c, a]]}],
    B4Cfg = [{servers, [a,b,c]}, {type, memcached}, {map, []}],
    B5Cfg = [{servers, [a,b,c,d]}, {type, membase}, {map, [[b, a], [a, b]]}],
    B6Cfg = [{servers, [a,b,c,d]}, {type, membase}, {map, [[a, b], [d, c]]}],
    [{"B1", B1Cfg}, {"B2", B2Cfg}, {"B3", B3Cfg}, {"B4", B4Cfg},
     {"B5", B5Cfg}, {"B6", B6Cfg}].

failover_bucket_groups_test_() ->
    {foreach,
     fun load_group_failover_test_common_modules/0,
     fun (_) ->
             meck:unload()
     end,
     [{"Prep Buckets",
       fun failover_buckets_prep_test_body/0},
      {"Failover Buckets Group Success",
       fun failover_buckets_group_test_body/0},
      {"Failover Buckets Group Fail",
       fun failover_buckets_group_failure_result_test_body/0}]
    }.

failover_buckets_prep_test_body() ->
    BConfig = get_test_bucket_config(),
    PrepResults = failover_buckets_prep(BConfig, [a, d], #{stubOpt => testOpt}),

    ?assertEqual(length(PrepResults), 6),

    {B1Type, _, B1NewMap, B1Opt} = proplists:get_value("B1", PrepResults),
    ?assertEqual(B1Type, membase),
    ?assertEqual(B1NewMap,
                 [[b, undefined],[c, undefined]]),
    ?assertEqual(maps:get(stubOpt, B1Opt), testOpt),

    {B2Type, _, B2NewMap, B2Opt} = proplists:get_value("B2", PrepResults),
    ?assertEqual(B2Type, membase),
    ?assertEqual(B2NewMap, []),
    ?assertEqual(maps:get(stubOpt, B2Opt), testOpt),

    {B3Type, _, _, _} = proplists:get_value("B3", PrepResults),
    ?assertEqual(B3Type, memcached),

    {B4Type, _, _, _} = proplists:get_value("B4", PrepResults),
    ?assertEqual(B4Type, memcached),

    {B5Type, _, B5NewMap, B5Opt} = proplists:get_value("B5", PrepResults),
    ?assertEqual(B5Type, membase),
    ?assertEqual(B5NewMap, [[b, undefined], [b, undefined]]),
    ?assertEqual(maps:get(stubOpt, B5Opt), testOpt),

    {B6Type, _, B6NewMap, B6Opt} = proplists:get_value("B6", PrepResults),
    ?assertEqual(B6Type, membase),
    ?assertEqual(B6NewMap, [[b, undefined], [c, undefined]]),
    ?assertEqual(maps:get(stubOpt, B6Opt), testOpt),
    ok.

failover_buckets_group_test_body() ->
    FailedNodes = [a,d],
    meck:expect(ns_bucket, remove_servers_from_buckets,
                fun (BucketsServersUpdate, Nodes) ->
                        ?assertEqual(BucketsServersUpdate, ["B2", "B3", "B4"]),
                        ?assertEqual(Nodes, [a, d]),
                        ok
                end),

    meck:expect(ns_bucket, set_buckets_config_failover,
                fun (BucketsMapUpdate, Nodes) ->
                        ?assertEqual(BucketsMapUpdate,
                                     [{"B1", [[b, undefined], [c, undefined]]},
                                      {"B5", [[b, undefined], [b, undefined]]},
                                      {"B6", [[b, undefined],
                                              [c, undefined]]}]),
                        ?assertEqual(Nodes, [a, d]),
                        ok
                end),

    meck:expect(ns_janitor, cleanup,
                fun (_,_) ->
                        ok
                end),

    BConfig = get_test_bucket_config(),
    PrepResults = failover_buckets_prep(BConfig, FailedNodes,
                                        #{stubOpt => testOpt}),

    Results1 = lists:flatmap(failover_buckets_group(_, FailedNodes),
                             misc:split(?MAX_BUCKETS_SUPPORTED,
                                        PrepResults)),

    ?assertEqual(Results1,
                 [[{bucket,"B1"},{node,a},{status,ok},{vbuckets,[0,1]}],
                  [{bucket,"B1"},{node,d},{status,ok},{vbuckets,[]}],
                  [{bucket,"B2"},{node,a},{status,ok},{vbuckets,[]}],
                  [{bucket,"B2"},{node,d},{status,ok},{vbuckets,[]}],
                  [{bucket,"B5"},{node,a},{status,ok},{vbuckets,[0,1]}],
                  [{bucket,"B5"},{node,d},{status,ok},{vbuckets,[]}],
                  [{bucket,"B6"},{node,a},{status,ok},{vbuckets,[0]}],
                  [{bucket,"B6"},{node,d},{status,ok},{vbuckets,[1]}]]

                ),

    %% Now failover buckets in groups of 2 at a time. The end results must be
    %% the same as the first failover run with failing entire bucket group
    meck:expect(ns_bucket, remove_servers_from_buckets,
                fun (_, _) ->
                        ok
                end),
    meck:expect(ns_bucket, set_buckets_config_failover,
                fun (_, _) ->
                        ok
                end),
    Results2 = lists:flatmap(failover_buckets_group(_, FailedNodes),
                             misc:split(2, PrepResults)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results2)),

    %% Lastly failing single bucket at a time, results must match
    Results3 = lists:flatmap(failover_buckets_group(_, FailedNodes),
                             misc:split(1, PrepResults)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results3)),
    ok.

failover_buckets_group_failure_result_test_body() ->
    FailedNodes = [a,d],
    meck:expect(ns_bucket, remove_servers_from_buckets,
                fun (_, _) ->
                        ok
                end),
    meck:expect(ns_bucket, set_buckets_config_failover,
                fun (_, _) ->
                        ok
                end),
    meck:expect(ns_janitor, cleanup,
                fun (Bucket,_) ->
                        case lists:member(Bucket, ["B1", "B6"]) of
                            false ->
                                ok;
                            _ ->
                                throw(failure_in_test)
                        end
                end),

    BConfig = get_test_bucket_config(),
    PrepResults = failover_buckets_prep(BConfig, FailedNodes,
                                        #{stubOpt => testOpt}),
    Results1 = lists:flatmap(failover_buckets_group(_, FailedNodes),
                             misc:split(?MAX_BUCKETS_SUPPORTED,
                                        PrepResults)),

    ?assertEqual(Results1,
                 [[{bucket,"B1"}, {node,a},
                   {status,janitor_failed}, {vbuckets,[0,1]}],
                  [{bucket,"B1"},{node,d},
                   {status,janitor_failed},{vbuckets,[]}],
                  [{bucket,"B2"},{node,a},
                   {status,ok},{vbuckets,[]}],
                  [{bucket,"B2"},{node,d},
                   {status,ok},{vbuckets,[]}],
                  [{bucket,"B5"},{node,a},
                   {status,ok},{vbuckets,[0,1]}],
                  [{bucket,"B5"},{node,d},
                   {status,ok},{vbuckets,[]}],
                  [{bucket,"B6"}, {node,a},
                   {status,janitor_failed}, {vbuckets,[0]}],
                  [{bucket,"B6"}, {node,d},
                   {status,janitor_failed}, {vbuckets,[1]}]]
                ),

    Results2 = lists:flatmap(failover_buckets_group(_, FailedNodes),
                             misc:split(3, PrepResults)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results2)),

    Results3 = lists:flatmap(failover_buckets_group(_, FailedNodes),
                             misc:split(1, PrepResults)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results3)),
    ok.

fix_vbucket_map_test_() ->
    fix_vbucket_map_test_wrapper(
      [{"not durability aware",
        fun () ->
                meck:delete(janitor_agent, query_vbuckets, 4, true),

                Map = [[a, b, c],
                       [b, d, c],
                       [c, d, e]],

                ?assertEqual(
                   [[c, undefined, undefined],
                    [d, c, undefined],
                    [c, d, e]],
                   fix_vbucket_map([a, b], "test", Map, #{})),
                ?assert(meck:validate(janitor_agent))
        end},
       {"durability aware",
        fun () ->
                meck_query_vbuckets([{c, [1, 2, 3]}, {d, [1, 2, 3]}],
                                    [{1, [{c, 2, 8}, {d, 3, 1}]},
                                     {2, [{c, 3, 1}, {d, 3, 2}]},
                                     {3, [{c, 1, 0}, {d, 0, 0}]}]),

                Map = [[a, b, c],
                       [b, c, d],
                       [a, c, d],
                       [a, c, d],
                       [c, d, a],
                       [c, d, e]],

                ?assertEqual(
                   [[c, undefined, undefined],
                    [d, c, undefined],
                    [d, c, undefined],
                    [c, d, undefined],
                    [c, d, undefined],
                    [c, d, e]],
                   fix_vbucket_map([a, b], "test", Map,
                                   #{durability_aware => true})),
                ?assert(meck:validate(janitor_agent))
        end}]).

can_preserve_durable_writes_test() ->
    %% Not checking every combination here, there is no point

    %% 0 replica. Cannot failover but kv safety check should kick in first.
    ?assertNot(can_preserve_durability_majority([[a]], [a])),

    %% 1 replica. Active vs Replica should have no bearing.
    ?assert(can_preserve_durability_majority([[a,b]], [a])),
    ?assert(can_preserve_durability_majority([[a,b]], [b])),

    ?assert(can_preserve_durability_majority([[a,b], [b,a]], [b])),
    ?assertNot(can_preserve_durability_majority([[a,b], [b,a]], [a,b])),

    ?assertNot(can_preserve_durability_majority([[a,b]], [a,b])),
    ?assertNot(can_preserve_durability_majority([[a,b], [b,c]], [a,b])),

    ?assertNot(can_preserve_durability_majority([[a,undefined]], [a])),
    ?assert(can_preserve_durability_majority([[a,undefined]], [b])),

    %% 2 replicas. Active vs Replica should have no bearing. We should be able to
    %% fail over any one node.
    ?assert(can_preserve_durability_majority([[a,b,c]], [a])),
    ?assertNot(can_preserve_durability_majority([[a,b,c]], [a,b])),
    ?assertNot(can_preserve_durability_majority([[a,b,c]], [b,c])),

    ?assertNot(can_preserve_durability_majority([[a,b,c]], [a,b,c])),

    ?assert(can_preserve_durability_majority([[a,b,c], [b,c,d]], [b])),
    ?assertNot(can_preserve_durability_majority([[a,b,c], [b,c,d]], [a,b])),

    ?assertNot(can_preserve_durability_majority([[a,b,undefined]], [a])),
    ?assert(can_preserve_durability_majority([[a,b,undefined]], [c])),

    %% 3 replicas. Not currently supported but we implemented the check
    %% generically such that it does not need to be changed when we do support 3
    %% replicas. Active vs replica should have no bearing. We should be able to
    %% fail over any 2 nodes.
    ?assert(can_preserve_durability_majority([[a,b,c,d]], [a])),
    ?assert(can_preserve_durability_majority([[a,b,c,d]], [a,b])),
    ?assert(can_preserve_durability_majority([[a,b,c,d]], [a,c])),
    ?assert(can_preserve_durability_majority([[a,b,c,d]], [b,c])),
    ?assert(can_preserve_durability_majority([[a,b,c,d]], [b,d])),
    ?assert(can_preserve_durability_majority([[a,b,c,d]], [c,d])),

    ?assertNot(can_preserve_durability_majority([[a,b,c,d]], [a,b,c])),
    ?assertNot(can_preserve_durability_majority([[a,b,c,d]], [a,c,d])),
    ?assertNot(can_preserve_durability_majority([[a,b,c,d]], [a,b,d])),
    ?assertNot(can_preserve_durability_majority([[a,b,c,d]], [b,c,d])).
-endif.
