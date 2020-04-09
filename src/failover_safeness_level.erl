%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2018 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc This gen_event handler holds failover safeness level for some
%% particular bucket. We estimate failover safeness as approximation
%% of time required to replicate backlog of un-replicated yet items.
%%
%% The approach is to subscribe to stats and keep moving averages of
%% replication streams backlog and replication streams drain
%% rate. After each new sample we update averages and compute
%% estimated time to drain all replication backlog. If that estimate
%% is higher then 2000 ms we switch to 'yellow' level. Switching back
%% to green has much lower bound to avoid too frequent changing of
%% levels (i.e. hysteresis).
%%
%% We also keep track of last time we've seen any stats sample. And we
%% use it when asked about current safeness level. If our information
%% is too stale (5 or more seconds old), then we respond with 'stale'
%% level.

-module(failover_safeness_level).

-behaviour(gen_event).

-include("ns_stats.hrl").

-define(TO_YELLOW_SECONDS, 2.0).
-define(FROM_YELLOW_SECONDS, 1.0).
-define(STALENESS_THRESHOLD, 5000).
-define(TOO_SMALL_BACKLOG, 0.5).

%% window in milliseconds of averaging of stats used in computation of
%% failover safeness
-define(AVERAGING_WINDOW, 7000).

-export([start_link/1, get_value/1,
         build_local_safeness_info/1,
         extract_replication_uptodateness/4]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(safeness_info,
        {backlog_size :: moving_average:moving_average(),
         drain_rate :: moving_average:moving_average(),
         safeness_level = unknown :: yellow | green | unknown
        }).

-record(state,
        {bucket_name :: bucket_name(),
         dcp_info :: #safeness_info{},
         last_update_local_clock :: non_neg_integer()
        }).

start_link(BucketName) ->
    misc:start_event_link(
      fun() ->
              ok = gen_event:add_sup_handler(ns_stats_event,
                                             {?MODULE, {?MODULE, BucketName}},
                                             [BucketName])
      end).

-spec get_value(bucket_name()) ->
                       stale | unknown | green | yellow.
get_value(BucketName) ->
    case gen_event:call(ns_stats_event, {?MODULE, {?MODULE, BucketName}}, get_level, 2000) of
        {ok, Value} -> Value;
        _ -> unknown
    end.

init([BucketName]) ->
    MovingAvg = moving_average:new(?AVERAGING_WINDOW),
    DcpInfo   = #safeness_info{backlog_size = MovingAvg,
                               drain_rate   = MovingAvg},

    {ok, #state{bucket_name = BucketName,
                dcp_info = DcpInfo,
                last_update_local_clock = erlang:monotonic_time()}}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_event({stats, StatsBucket, #stat_entry{timestamp = TS, values = Values}},
             #state{bucket_name = StatsBucket,
                    dcp_info = DcpInfo} = State) ->
    NewDcpInfo =
         new_safeness_info(orddict:find(ep_dcp_replica_items_remaining, Values),
                           orddict:find(ep_dcp_replica_items_sent, Values),
                           TS,
                           DcpInfo),

    {ok, State#state{dcp_info = NewDcpInfo,
                     last_update_local_clock = erlang:monotonic_time()}};

handle_event(_, State) ->
    {ok, State}.


new_safeness_level(Level, BacklogSize, DrainRate) when Level =/= green ->
    try BacklogSize < ?TOO_SMALL_BACKLOG orelse BacklogSize / DrainRate =< ?FROM_YELLOW_SECONDS of
        true -> green;
        _ -> yellow
    catch error:badarith ->
            %% division by zero or overflow/underflow (erlang doesn't have infinities)
            case BacklogSize > DrainRate of
                true ->
                    %% overflow
                    yellow;
                _ ->
                    %% underflow or 0/0
                    green
            end
    end;
new_safeness_level(green, BacklogSize, DrainRate) ->
    try BacklogSize >= ?TOO_SMALL_BACKLOG andalso BacklogSize / DrainRate > ?TO_YELLOW_SECONDS of
        true -> yellow;
        _ -> green
    catch error:badarith ->
            %% division by zero or overflow/underflow (erlang doesn't have infinities)
            case BacklogSize > DrainRate of
                true ->
                    %% overflow
                    yellow;
                _ ->
                    %% underflow or 0/0
                    green
            end
    end.

new_safeness_info({ok, BacklogSize}, {ok, DrainRate}, TS,
                  #safeness_info{backlog_size   = BacklogSizeAvg,
                                 drain_rate     = DrainRateAvg,
                                 safeness_level = SafenessLevel}) ->

    NewBacklogSizeAvg   = moving_average:feed(BacklogSize, TS, BacklogSizeAvg),
    NewBacklogSizeValue = moving_average:get_estimate(NewBacklogSizeAvg),

    NewDrainRateAvg   = moving_average:feed(DrainRate, TS, DrainRateAvg),
    NewDrainRateValue = moving_average:get_estimate(NewDrainRateAvg),

    NewSafenessLevel = new_safeness_level(SafenessLevel,
                                          NewBacklogSizeValue,
                                          NewDrainRateValue),

    #safeness_info{backlog_size   = NewBacklogSizeAvg,
                   drain_rate     = NewDrainRateAvg,
                   safeness_level = NewSafenessLevel};
new_safeness_info(_, _, _, Info) ->
    Info.

handle_call(get_level, #state{last_update_local_clock = UpdateTS,
                              dcp_info = #safeness_info{safeness_level = DcpLevel}} = State) ->
    Now        = erlang:monotonic_time(),
    TimePassed = erlang:convert_time_unit(Now - UpdateTS, native, millisecond),

    RV = case TimePassed > ?STALENESS_THRESHOLD of
             true ->
                 stale;
             _ ->
                 DcpLevel
         end,
    {ok, {ok, RV}, State};
handle_call(get_state, State) ->
    {ok, State, State}.


handle_info(_Info, State) ->
    {ok, State}.

%% Builds local replication safeness information. ns_heart normally
%% broadcasts it with heartbeats. This information from all nodes can
%% then be used to to estimate failover safeness level of particular
%% node.
build_local_safeness_info(BucketNames) ->
    ReplicationsSafeness =
        [{Name, failover_safeness_level:get_value(Name)} || Name <- BucketNames],

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
