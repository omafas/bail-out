%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc If time required to replicate backlog of un-replicated yet items
%% (drain time) is less than 2 seconds the failover is considered safe (green).
%% Otherwise, if the drain time is greater than 2 second the level is yellow.
%%
%% We also consider the level to be yellow if the drain time is higher than 1s
%% and has spiked higher than 2s at least once for the most recent minute
%% (in order to avoid too frequent changing of levels).
%%
%% If our information is too stale (> 2 stats collection intervals), then we
%% respond with 'stale' level.

-module(failover_safeness_level).

-include("ns_stats.hrl").

-export([build_local_safeness_info/1,
         extract_replication_uptodateness/4]).

-spec get_value(bucket_name()) ->
                       stale | unknown | green | yellow.
get_value(BucketName) ->
    case stats_interface:failover_safeness_level(BucketName) of
        {ok, {LastUpdateTimestamp, UpdateInterval, Value}} ->
            Now = erlang:system_time(second),
            case Now - LastUpdateTimestamp < 2 * UpdateInterval of
                true when Value == 1 -> green;
                true when Value == 0 -> yellow;
                false -> stale;
                true ->
                    ?log_error("Unexpected failover_safeness_level(~p): ~p",
                               [BucketName, Value]),
                    unknown
            end;
        {error, not_available} -> stale;
        {error, _} -> unknown
    end.

%% Builds local replication safeness information. ns_heart normally
%% broadcasts it with heartbeats. This information from all nodes can
%% then be used to to estimate failover safeness level of particular
%% node.
build_local_safeness_info(BucketNames) ->
    ReplicationsSafeness =
        [{Name, get_value(Name)} || Name <- BucketNames],

    %% [{BucketName, [{SrcNode0, HashOfVBucketsReplicated0}, ..other nodes..]}, ..other buckets..]
    IncomingReplicationConfs =
        [{BucketName,
          [{SrcNode, erlang:phash2(VBuckets)} ||
              {SrcNode, _DstNode, VBuckets} <-
                  janitor_agent:this_node_replicator_triples(BucketName)]
         }
         || BucketName <- BucketNames],
    [{outgoing_replications_safeness_level, ReplicationsSafeness},
     {incoming_replications_conf_hashes, IncomingReplicationConfs}].

%% Returns indication of whether it's safe to fail over given node
%% w.r.t. given bucket. Implementation uses information from
%% build_local_safeness_info/1 from all replica nodes.
%%
%% We check that all needed outgoing replications are there (with
%% right vbuckets) and that dcp producer stats of given node indicate
%% that all outgoing replications from given node are reasonably up to
%% date (see discussion of green/yellow levels at top of this
%% file). So we actually use node statuses of all nodes (well, only
%% replicas of given node in fact).
extract_replication_uptodateness(BucketName, BucketConfig, Node, NodeStatuses) ->
    Map = proplists:get_value(map, BucketConfig, []),
    case outgoing_replications_started(BucketName, Map, Node, NodeStatuses) of
        false ->
            0.0;
        true ->
            NodeInfo = ns_doctor:get_node(Node, NodeStatuses),
            SafenessLevelAll = proplists:get_value(outgoing_replications_safeness_level, NodeInfo, []),
            SafenessLevel = proplists:get_value(BucketName, SafenessLevelAll, unknown),
            case SafenessLevel of
                unknown -> 0.0;
                stale -> 0.0;
                yellow -> 0.5;
                green -> 1.0
            end
    end.

outgoing_replications_started(BucketName, Map, Node, NodeStatuses) ->
    %% NOTE: we only care about first replicas. I.e. when Node is
    %% master, bacause that actually defines failover safeness
    ReplicaNodes = lists:foldl(fun (Chain, Set) ->
                                       case Chain of
                                           [Node, DstNode | _] -> % NOTE: Node is bound higher
                                               sets:add_element(DstNode, Set);
                                           _ ->
                                               Set
                                       end
                               end, sets:new(), Map),
    ReplicaOkP =
        fun (ReplicaNode) ->
                %% NOTE: this includes all replicated vbuckets not just active vbuckets
                ExpectedVBuckets = ns_bucket:replicated_vbuckets(Map, Node, ReplicaNode),
                ReplicaInfo = ns_doctor:get_node(ReplicaNode, NodeStatuses),
                AllIncomingConfs = proplists:get_value(incoming_replications_conf_hashes, ReplicaInfo, []),
                IncomingConfsAllNodes = proplists:get_value(BucketName, AllIncomingConfs, []),
                ActualVBucketsHash = proplists:get_value(Node, IncomingConfsAllNodes),
                erlang:phash2(ExpectedVBuckets) =:= ActualVBucketsHash
        end,
    sets:fold(fun (ReplicaNode, Ok) ->
                      Ok andalso ReplicaOkP(ReplicaNode)
              end, true, ReplicaNodes).
