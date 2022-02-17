%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% Monitor and maintain the vbucket layout of each bucket.
%% There is one of these per bucket.
%%
%% @doc Rebalancing functions.
%%

-module(ns_rebalancer).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([check_graceful_failover_possible/2,
         generate_initial_map/1,
         start_link_rebalance/5,
         move_vbuckets/2,
         unbalanced/2,
         bucket_needs_rebalance/2,
         eject_nodes/1,
         maybe_cleanup_old_buckets/1,
         start_link_graceful_failover/1,
         check_test_condition/2,
         rebalance_topology_aware_services/3]).

-export([wait_local_buckets_shutdown_complete/0]). % used via rpc:multicall


-define(BAD_REPLICATORS, 2).

-define(BUCKETS_SHUTDOWN_WAIT_TIMEOUT, ?get_timeout(buckets_shutdown, 60000)).

-define(REBALANCER_READINESS_WAIT_TIMEOUT, ?get_timeout(readiness, 60000)).
-define(REBALANCER_QUERY_STATES_TIMEOUT,   ?get_timeout(query_states, 10000)).
-define(REBALANCER_APPLY_CONFIG_TIMEOUT,   ?get_timeout(apply_config, 300000)).
%%
%% API
%%
generate_vbucket_map_options(KeepNodes, BucketConfig) ->
    %% The set of server groups is limited to those containing KeepNodes. This
    %% is because decide whether to generate a rack-aware map based on the
    %% number of server groups. Only if there's more than one server group do
    %% we want do generate a rack-aware map. But while there may be more than
    %% one server group in the current configuration, there may be only one
    %% left post-rebalance.
    ServerGroups = ns_cluster_membership:get_nodes_server_groups(KeepNodes),

    Tags = case ns_cluster_membership:rack_aware(ServerGroups) of
               true ->
                   Tags0 = [case proplists:get_value(uuid, G) of
                                T ->
                                    [{N, T} || N <- proplists:get_value(
                                                      nodes, G),
                                               lists:member(N, KeepNodes)]
                            end || G <- ServerGroups],

                   TagsRV = lists:append(Tags0),

                   case KeepNodes -- [N || {N, _T} <- TagsRV] of
                       [] -> ok;
                       _ ->
                           %% there's tiny race between start of rebalance and
                           %% somebody changing server_groups. We largely ignore
                           %% it, but in case where it can clearly cause problem
                           %% we raise exception
                           erlang:error(server_groups_race_detected)
                   end,
                   TagsRV;
               false ->
                   undefined
           end,

    Opts0 = ns_bucket:config_to_map_options(BucketConfig),

    %% Note that we don't need to have replication_topology here (in fact as
    %% of today it's still returned by ns_bucket:config_to_map_options/1), but
    %% these options are used to compute map_opts_hash which in turn is used
    %% to decide if rebalance is needed. So if we remove this, old nodes will
    %% wrongly believe that rebalance is needed even when the cluster is
    %% balanced. See MB-15543 for details.
    misc:update_proplist(Opts0, [{replication_topology, star},
                                 {tags, Tags}]).

generate_vbucket_map(CurrentMap, KeepNodes, BucketConfig) ->
    Opts = generate_vbucket_map_options(KeepNodes, BucketConfig),

    Map0 =
        case lists:keyfind(deltaRecoveryMap, 1, BucketConfig) of
            {deltaRecoveryMap, DRMapAndOpts} when DRMapAndOpts =/= undefined ->
                {DRMap, DROpts} = DRMapAndOpts,

                case mb_map:is_trivially_compatible_past_map(KeepNodes, CurrentMap,
                                                             Opts, DRMap, DROpts) of
                    true ->
                        DRMap;
                    false ->
                        undefined
                end;
            _ ->
                undefined
        end,

    Map = case Map0 of
              undefined ->
                  EffectiveOpts = [{maps_history, ns_bucket:past_vbucket_maps()} | Opts],
                  NumReplicas = ns_bucket:num_replicas(BucketConfig),
                  mb_map:generate_map(CurrentMap, NumReplicas, KeepNodes, EffectiveOpts);
              _ ->
                  Map0
          end,

    {Map, Opts}.

generate_initial_map(BucketConfig) ->
    Chain = lists:duplicate(proplists:get_value(num_replicas, BucketConfig) + 1,
                            undefined),
    Map1 = lists:duplicate(proplists:get_value(num_vbuckets, BucketConfig),
                           Chain),
    Servers = ns_bucket:get_servers(BucketConfig),
    generate_vbucket_map(Map1, Servers, BucketConfig).

local_buckets_shutdown_loop(Ref, CanWait) ->
    ExcessiveBuckets = ns_memcached:active_buckets() -- ns_bucket:node_bucket_names(node()),
    case ExcessiveBuckets of
        [] ->
            ok;
        _ ->
            case CanWait of
                false ->
                    exit({old_buckets_shutdown_wait_failed, ExcessiveBuckets});
                true ->
                    ?log_debug("Waiting until the following old bucket instances are gone: ~p", [ExcessiveBuckets]),
                    receive
                        {Ref, timeout} ->
                            local_buckets_shutdown_loop(Ref, false);
                        {Ref, _Msg} ->
                            local_buckets_shutdown_loop(Ref, true)
                    end
            end
    end.

%% note: this is rpc:multicall-ed
wait_local_buckets_shutdown_complete() ->
    ExcessiveBuckets =
        ns_memcached:active_buckets() -- ns_bucket:node_bucket_names(node()),
    do_wait_local_buckets_shutdown_complete(ExcessiveBuckets).

do_wait_local_buckets_shutdown_complete([]) ->
    ok;
do_wait_local_buckets_shutdown_complete(ExcessiveBuckets) ->
    Timeout = ?BUCKETS_SHUTDOWN_WAIT_TIMEOUT * length(ExcessiveBuckets),
    misc:executing_on_new_process(
      fun () ->
              Ref = erlang:make_ref(),
              Parent = self(),
              Subscription = ns_pubsub:subscribe_link(buckets_events,
                                                      fun ({stopped, _} = StoppedMsg) ->
                                                              Parent ! {Ref, StoppedMsg};
                                                          (_) ->
                                                              ok
                                                      end),
              erlang:send_after(Timeout, Parent, {Ref, timeout}),
              try
                  local_buckets_shutdown_loop(Ref, true)
              after
                  (catch ns_pubsub:unsubscribe(Subscription))
              end
      end).

do_wait_buckets_shutdown(KeepNodes) ->
    {Good, ReallyBad, FailedNodes} =
        misc:rpc_multicall_with_plist_result(
          KeepNodes, ns_rebalancer, wait_local_buckets_shutdown_complete, []),
    NonOk = [Pair || {_Node, Result} = Pair <- Good,
                     Result =/= ok],
    Failures = ReallyBad ++ NonOk ++ [{N, node_was_down} || N <- FailedNodes],
    case Failures of
        [] ->
            ok;
        _ ->
            ?rebalance_error("Failed to wait deletion of some buckets on some nodes: ~p~n", [Failures]),
            exit({buckets_shutdown_wait_failed, Failures})
    end.

sanitize(Config) ->
    misc:rewrite_key_value_tuple(sasl_password, "*****", Config).

config_sync(Type, Nodes) ->
    case chronicle_compat:config_sync(Type, Nodes) of
        ok ->
            ok;
        Error ->
            exit({config_sync_failed, Error})
    end.

push_ns_config(Nodes) ->
    case cluster_compat_mode:is_cluster_70() of
        true ->
            ok;
        false ->
            case ns_config_rep:ensure_config_seen_by_nodes(Nodes) of
                ok ->
                    ok;
                {error, SyncFailedNodes} ->
                    ?log_error("Failed to push config to nodes ~p",
                               [SyncFailedNodes]),
                    {config_sync_failed, SyncFailedNodes}
            end
    end.

start_link_rebalance(KeepNodes, EjectNodes,
                     FailedNodes, DeltaNodes, DeltaRecoveryBucketNames) ->
    proc_lib:start_link(
      erlang, apply,
      [fun () ->
               FailKvChk = check_test_condition(no_kv_nodes_left) =/= ok,

               KVKeep = ns_cluster_membership:service_nodes(KeepNodes, kv),
               case KVKeep =:= [] orelse FailKvChk of
                   true ->
                       proc_lib:init_ack({error, no_kv_nodes_left}),
                       exit(normal);
                   false ->
                       ok
               end,

               KVDeltaNodes = ns_cluster_membership:service_nodes(DeltaNodes,
                                                                  kv),
               BucketConfigs = ns_bucket:get_buckets(),
               %% Pre-emptive check to see if delta recovery is possible.
               case build_delta_recovery_buckets(KVKeep, KVDeltaNodes,
                                                 BucketConfigs, DeltaRecoveryBucketNames) of
                   {ok, _DeltaRecoveryBucketTuples} ->
                       proc_lib:init_ack({ok, self()}),

                       master_activity_events:note_rebalance_start(
                         self(), KeepNodes, EjectNodes, FailedNodes, DeltaNodes),

                       rebalance(KeepNodes, EjectNodes, FailedNodes,
                                 DeltaNodes, DeltaRecoveryBucketNames);
                   {error, not_possible} ->
                       proc_lib:init_ack({error, delta_recovery_not_possible})
               end
       end, []]).

move_vbuckets(Bucket, Moves) ->
    {ok, Config} = ns_bucket:get_bucket(Bucket),
    Map = proplists:get_value(map, Config),
    TMap = lists:foldl(fun ({VBucket, TargetChain}, Map0) ->
                               setelement(VBucket+1, Map0, TargetChain)
                       end, list_to_tuple(Map), Moves),
    NewMap = tuple_to_list(TMap),
    ProgressFun = make_progress_fun(0, 1),
    run_mover(Bucket, Config, ns_bucket:get_servers(Config),
              ProgressFun, Map, NewMap).

rebalance_services(KeepNodes, EjectNodes) ->
    Snapshot = ns_cluster_membership:get_snapshot(),

    AllServices = ns_cluster_membership:cluster_supported_services() -- [kv],
    TopologyAwareServices = ns_cluster_membership:topology_aware_services(),
    SimpleServices = AllServices -- TopologyAwareServices,

    SimpleTSs = rebalance_simple_services(Snapshot, SimpleServices, KeepNodes),
    TopologyAwareTSs = rebalance_topology_aware_services(
                         Snapshot, TopologyAwareServices,
                         KeepNodes, EjectNodes),

    maybe_delay_eject_nodes(SimpleTSs ++ TopologyAwareTSs, EjectNodes).

rebalance_simple_services(Snapshot, Services, KeepNodes) ->
    lists:filtermap(
      fun (Service) ->
              ServiceNodes =
                  ns_cluster_membership:service_nodes(KeepNodes, Service),
              master_activity_events:note_rebalance_stage_started(
                Service, ServiceNodes),
              Updated = update_service_map_with_snapshot(
                          Snapshot, Service, ServiceNodes),

              master_activity_events:note_rebalance_stage_completed(
                Service),
              case Updated of
                  false ->
                      false;
                  true ->
                      {true, {Service, os:timestamp()}}
              end
      end, Services).

update_service_map_with_snapshot(Snapshot, Service, ServiceNodes0) ->
    CurrentNodes0 = ns_cluster_membership:get_service_map(Snapshot, Service),
    update_service_map(Service, CurrentNodes0, ServiceNodes0).

update_service_map(Service, CurrentNodes0, ServiceNodes0) ->
    CurrentNodes = lists:sort(CurrentNodes0),
    ServiceNodes = lists:sort(ServiceNodes0),

    case CurrentNodes =:= ServiceNodes of
        true ->
            false;
        false ->
            ?rebalance_info("Updating service map for ~p:~n~p",
                            [Service, ServiceNodes]),
            ok = ns_cluster_membership:set_service_map(Service, ServiceNodes),
            true
    end.

rebalance_topology_aware_services(Services, KeepNodesAll, EjectNodesAll) ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    rebalance_topology_aware_services(Snapshot, Services, KeepNodesAll,
                                      EjectNodesAll).

rebalance_topology_aware_services(Snapshot, Services, KeepNodesAll,
                                  EjectNodesAll) ->
    %% TODO: support this one day
    DeltaNodesAll = [],

    lists:filtermap(
      fun (Service) ->
              ok = check_test_condition(service_rebalance_start, Service),
              KeepNodes = ns_cluster_membership:service_nodes(
                            Snapshot, KeepNodesAll, Service),
              DeltaNodes = ns_cluster_membership:service_nodes(
                             Snapshot, DeltaNodesAll, Service),

              %% if a node being ejected is not active, then it means that it
              %% was never rebalanced in in the first place; so we can
              %% postpone the heat death of the universe a little bit by
              %% ignoring such nodes
              ActiveNodes =
                  ns_cluster_membership:get_service_map(Snapshot, Service),
              EjectNodes = [N || N <- EjectNodesAll,
                                 lists:member(N, ActiveNodes)],

              AllNodes = EjectNodes ++ KeepNodes,

              case AllNodes of
                  [] ->
                      false;
                  _ ->
                      master_activity_events:note_rebalance_stage_started(
                        Service, AllNodes),
                      update_service_map_with_snapshot(
                        Snapshot, Service, AllNodes),
                      ok = rebalance_topology_aware_service(
                             Service, KeepNodes, EjectNodes, DeltaNodes),
                      update_service_map(Service, AllNodes, KeepNodes),
                      master_activity_events:note_rebalance_stage_completed(
                        Service),
                      {true, {Service, os:timestamp()}}
              end
      end, Services).

rebalance_topology_aware_service(Service, KeepNodes, EjectNodes, DeltaNodes) ->
    ProgressCallback =
        fun (Progress) ->
                ns_rebalance_observer:update_progress(Service, Progress)
        end,

    misc:with_trap_exit(
      fun () ->
              {Pid, MRef} = service_rebalancer:spawn_monitor_rebalance(
                              Service, KeepNodes,
                              EjectNodes, DeltaNodes, ProgressCallback),

              receive
                  {'EXIT', _Pid, Reason} = Exit ->
                      ?log_debug("Got an exit signal while waiting "
                                 "for the service rebalance to complete. "
                                 "Service: ~p. Exit message: ~p",
                                 [Service, Exit]),

                      misc:terminate_and_wait(Pid, Reason),
                      exit(Reason);
                  {'DOWN', MRef, _, _, Reason} ->
                      case Reason of
                          normal ->
                              ok;
                          _ ->
                              exit({service_rebalance_failed, Service, Reason})
                      end
              end
      end).

get_service_eject_delay(Service) ->
    Default =
        case Service of
            n1ql ->
                20000;
            fts ->
                10000;
            _ ->
                0
        end,

    ?get_param({eject_delay, Service}, Default).

maybe_delay_eject_nodes(Timestamps, EjectNodes) ->
    do_maybe_delay_eject_nodes(Timestamps, EjectNodes).

do_maybe_delay_eject_nodes(_Timestamps, []) ->
    ok;
do_maybe_delay_eject_nodes(Timestamps, EjectNodes) ->
    EjectedServices =
        ordsets:union([ordsets:from_list(ns_cluster_membership:node_services(N))
                       || N <- EjectNodes]),
    Now = os:timestamp(),

    Delays = [begin
                  ServiceDelay = get_service_eject_delay(Service),

                  case proplists:get_value(Service, Timestamps) of
                      undefined ->
                          %% it's possible that a node is ejected without ever
                          %% getting rebalanced in; there's no point in
                          %% delaying anything in such case
                          0;
                      RebalanceTS ->
                          SinceRebalance = max(0, timer:now_diff(Now, RebalanceTS) div 1000),
                          ServiceDelay - SinceRebalance
                  end
              end || Service <- EjectedServices],

    Delay = lists:max(Delays),

    case Delay > 0 of
        true ->
            ?log_info("Waiting ~pms before ejecting nodes:~n~p",
                      [Delay, EjectNodes]),
            timer:sleep(Delay);
        false ->
            ok
    end.

rebalance(KeepNodes, EjectNodesAll, FailedNodesAll,
          DeltaNodes, DeltaRecoveryBucketNames) ->
    ok = check_test_condition(rebalance_start),
    ok = leader_activities:run_activity(
           rebalance, majority,
           ?cut(rebalance_body(KeepNodes, EjectNodesAll,
                               FailedNodesAll,
                               DeltaNodes, DeltaRecoveryBucketNames))).

rebalance_body(KeepNodes,
               EjectNodesAll,
               FailedNodesAll,
               DeltaNodes, DeltaRecoveryBucketNames) ->
    LiveNodes = KeepNodes ++ EjectNodesAll,
    LiveKVNodes = ns_cluster_membership:service_nodes(LiveNodes, kv),
    KVDeltaNodes = ns_cluster_membership:service_nodes(DeltaNodes, kv),

    prepare_rebalance(LiveNodes),

    ok = drop_old_2i_indexes(KeepNodes),

    master_activity_events:note_rebalance_stage_started(kv, LiveKVNodes),
    %% wait till all bucket shutdowns are done on nodes we're
    %% adding (or maybe adding).
    do_wait_buckets_shutdown(KeepNodes),

    %% We run the janitor here to make sure that the vbucket map is in sync
    %% with the vbucket states.
    %% Unfortunately, we need to run it once more in rebalance_kv after
    %% the server list for the bucket is updated. So that the states of the
    %% vbucket on newly added nodes are applied.
    KVKeep = ns_cluster_membership:service_nodes(KeepNodes, kv),
    lists:foreach(
      fun (Bucket) ->
              deactivate_bucket_data_on_unknown_nodes(Bucket, KVKeep),
              run_janitor_pre_rebalance(Bucket)
      end, ns_bucket:get_bucket_names()),

    %% Fetch new BucketConfigs and re build DeltaRecoveryBuckets, as janitor run
    %% might have updated vbucket map.
    BucketConfigs = ns_bucket:get_buckets(),
    DeltaRecoveryBuckets = case build_delta_recovery_buckets(
                                  KVKeep, KVDeltaNodes,
                                  BucketConfigs, DeltaRecoveryBucketNames) of
                               {ok, DRB} ->
                                   DRB;
                               {error, not_possible} ->
                                   throw({error, delta_recovery_not_possible})
                           end,
    master_activity_events:note_rebalance_stage_started(
      [kv, kv_delta_recovery], KVDeltaNodes),
    ok = apply_delta_recovery_buckets(DeltaRecoveryBuckets,
                                      KVDeltaNodes, BucketConfigs),
    ok = check_test_condition(after_apply_delta_recovery),

    ok = chronicle_compat:set_multiple(
           ns_cluster_membership:clear_recovery_type_sets(KeepNodes) ++
               failover:clear_failover_vbuckets_sets(KeepNodes)),

    master_activity_events:note_rebalance_stage_completed(
      [kv, kv_delta_recovery]),
    ok = service_janitor:cleanup(),

    ok = chronicle_master:activate_nodes(KeepNodes),
    ok = leader_activities:activate_quorum_nodes(KeepNodes),

    config_sync(pull, LiveNodes),

    %% Eject failed nodes first so they don't cause trouble
    FailedNodes = FailedNodesAll -- [node()],
    eject_nodes(FailedNodes),

    ok = check_test_condition(rebalance_cluster_nodes_active),

    rebalance_kv(KeepNodes, EjectNodesAll, BucketConfigs, DeltaRecoveryBuckets),
    master_activity_events:note_rebalance_stage_completed(kv),
    rebalance_services(KeepNodes, EjectNodesAll),

    ok = leader_activities:deactivate_quorum_nodes(EjectNodesAll),

    %% Note that we "unprepare" rebalance only if it terminates normally. If
    %% it's interrupted or fails, that's likely because some of the nodes are
    %% unhealthy. Specifically, in the case of autofailover interrupting
    %% rebalance we don't want to get stuck trying to reach the node that
    %% needs to be auto failed over.
    unprepare_rebalance(LiveNodes),

    %% don't eject ourselves at all here; this will be handled by
    %% ns_orchestrator
    EjectNowNodes = EjectNodesAll -- [node()],
    eject_nodes(EjectNowNodes),

    ok.

make_progress_fun(BucketCompletion, NumBuckets) ->
    fun (P) ->
            Progress = dict:map(fun (_, N) ->
                                        N / NumBuckets + BucketCompletion
                                end, P),
            update_kv_progress(Progress)
    end.

update_kv_progress(Progress) ->
    ns_rebalance_observer:update_progress(kv, Progress).

update_kv_progress(Nodes, Progress) ->
    update_kv_progress(dict:from_list([{N, Progress} || N <- Nodes])).

rebalance_kv(KeepNodes, EjectNodes, BucketConfigs, DeltaRecoveryBuckets) ->
    NumBuckets = length(BucketConfigs),
    ?rebalance_debug("BucketConfigs = ~p", [sanitize(BucketConfigs)]),

    KeepKVNodes = ns_cluster_membership:service_nodes(KeepNodes, kv),
    LiveKVNodes =
        ns_cluster_membership:service_nodes(KeepNodes ++ EjectNodes, kv),

    case maybe_cleanup_old_buckets(KeepNodes) of
        ok ->
            ok;
        Error ->
            exit(Error)
    end,

    lists:foreach(
      fun ({I, {BucketName, BucketConfig}}) ->
              ok = check_test_condition({fail_bucket_rebalance_at_bucket,
                                         BucketName}),
              BucketCompletion = I / NumBuckets,
              update_kv_progress(LiveKVNodes, BucketCompletion),

              ProgressFun = make_progress_fun(BucketCompletion, NumBuckets),
              ForcedMap = get_target_map_and_opts(BucketName,
                                                  DeltaRecoveryBuckets),
              rebalance_bucket(BucketName, BucketConfig, ProgressFun,
                               KeepKVNodes, EjectNodes, ForcedMap)
      end, misc:enumerate(BucketConfigs, 0)),

    update_kv_progress(LiveKVNodes, 1.0).

rebalance_bucket(BucketName, BucketConfig, ProgressFun,
                 KeepKVNodes, EjectNodes, ForcedMap) ->
    ale:info(?USER_LOGGER, "Started rebalancing bucket ~s", [BucketName]),
    ?rebalance_info("Rebalancing bucket ~p with config ~p",
                    [BucketName, sanitize(BucketConfig)]),
    case proplists:get_value(type, BucketConfig) of
        memcached ->
            rebalance_memcached_bucket(BucketName, KeepKVNodes);
        membase ->
            rebalance_membase_bucket(BucketName, BucketConfig, ProgressFun,
                                     KeepKVNodes, EjectNodes, ForcedMap)
    end.

rebalance_memcached_bucket(BucketName, KeepKVNodes) ->
    master_activity_events:note_bucket_rebalance_started(BucketName),
    ns_bucket:set_servers(BucketName, KeepKVNodes),
    master_activity_events:note_bucket_rebalance_ended(BucketName).

rebalance_membase_bucket(BucketName, BucketConfig, ProgressFun,
                         KeepKVNodes, EjectNodes, ForcedMap) ->
    %% Only start one bucket at a time to avoid
    %% overloading things
    ThisEjected = ordsets:intersection(
                    lists:sort(ns_bucket:get_servers(BucketConfig)),
                    lists:sort(EjectNodes)),
    ThisLiveNodes = KeepKVNodes ++ ThisEjected,

    ns_bucket:set_servers(BucketName, ThisLiveNodes),
    ?rebalance_info("Waiting for bucket ~p to be ready on ~p", [BucketName, ThisLiveNodes]),
    case janitor_agent:check_bucket_ready(BucketName, ThisLiveNodes,
                                          ?REBALANCER_READINESS_WAIT_TIMEOUT) of
        ready ->
            ?rebalance_info("Bucket is ready on all nodes"),
            ok;
        {_, Zombies} ->
            exit({not_all_nodes_are_ready_yet, Zombies})
    end,

    run_janitor_pre_rebalance(BucketName),

    {ok, NewConf} =
        ns_bucket:get_bucket(BucketName),
    master_activity_events:note_bucket_rebalance_started(BucketName),
    {NewMap, MapOptions} =
        do_rebalance_membase_bucket(BucketName, NewConf,
                                    KeepKVNodes, ProgressFun, ForcedMap),
    ns_bucket:set_map_opts(BucketName, MapOptions),
    ns_bucket:update_bucket_props(BucketName,
                                  [{deltaRecoveryMap, undefined}]),
    master_activity_events:note_bucket_rebalance_ended(BucketName),
    verify_replication(BucketName, KeepKVNodes, NewMap).

run_janitor_pre_rebalance(BucketName) ->
    case ns_janitor:cleanup(BucketName,
                            [{query_states_timeout, ?REBALANCER_QUERY_STATES_TIMEOUT},
                             {apply_config_timeout, ?REBALANCER_APPLY_CONFIG_TIMEOUT}]) of
        ok ->
            ok;
        Error ->
            exit({pre_rebalance_janitor_run_failed, BucketName, Error})
    end.

%% @doc Rebalance the cluster. Operates on a single bucket. Will
%% either return ok or exit with reason 'stopped' or whatever reason
%% was given by whatever failed.
do_rebalance_membase_bucket(Bucket, Config,
                            KeepNodes, ProgressFun, ForcedMap) ->
    Map = proplists:get_value(map, Config),
    {FastForwardMap, MapOptions} =
        case ForcedMap of
            undefined ->
                NumReplicas = ns_bucket:num_replicas(Config),
                AdjustedMap = mb_map:align_replicas(Map, NumReplicas),
                generate_vbucket_map(AdjustedMap, KeepNodes, Config);
            _ ->
                ForcedMap
        end,

    ns_bucket:update_vbucket_map_history(FastForwardMap, MapOptions),
    ?rebalance_debug("Target map options: ~p (hash: ~p)",
                     [MapOptions, erlang:phash2(MapOptions)]),
    {run_mover(Bucket, Config, KeepNodes, ProgressFun, Map, FastForwardMap),
     MapOptions}.

sleep_for_sdk_clients(Type) ->
    SecondsToWait = ns_config:read_key_fast(rebalance_out_delay_seconds, 10),
    ?rebalance_info("Waiting ~w seconds before completing ~p. "
                    "So that clients receive graceful not my vbucket "
                    "instead of silent closed connection",
                    [SecondsToWait, Type]),
    timer:sleep(SecondsToWait * 1000).

run_mover(Bucket, Config, KeepNodes, ProgressFun, Map, FastForwardMap) ->
    Servers = ns_bucket:get_servers(Config),

    %% At this point the server list must have already been updated to include
    %% all future nodes in addition to the old ones (some of which might be
    %% being removed).
    true = ((KeepNodes -- Servers) =:= []),

    ?rebalance_info("Target map (distance: ~p):~n~p", [(catch mb_map:vbucket_movements(Map, FastForwardMap)), FastForwardMap]),
    ns_bucket:set_fast_forward_map(Bucket, FastForwardMap),
    misc:with_trap_exit(
      fun () ->
              {ok, Pid} = ns_vbucket_mover:start_link(Bucket, Servers,
                                                      Map, FastForwardMap,
                                                      ProgressFun),
              wait_for_mover(Pid)
      end),

    HadRebalanceOut = ((Servers -- KeepNodes) =/= []),
    case HadRebalanceOut of
        true ->
            sleep_for_sdk_clients("rebalance out");
        false ->
            ok
    end,
    ns_bucket:set_fast_forward_map(Bucket, undefined),
    ns_bucket:set_servers(Bucket, KeepNodes),
    FastForwardMap.

unbalanced(Map, BucketConfig) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    NumServers = length(Servers),

    R = lists:any(
          fun (Chain) ->
                  lists:member(
                    undefined,
                    %% Don't warn about missing replicas when you have
                    %% fewer servers than your copy count!
                    lists:sublist(Chain, NumServers))
          end, Map),

    R orelse do_unbalanced(Map, Servers).

do_unbalanced(Map, Servers) ->
    {Masters, Replicas} =
        lists:foldl(
          fun ([M | R], {AccM, AccR}) ->
                  {[M | AccM], R ++ AccR}
          end, {[], []}, Map),
    Masters1 = lists:sort([M || M <- Masters, lists:member(M, Servers)]),
    Replicas1 = lists:sort([R || R <- Replicas, lists:member(R, Servers)]),

    MastersCounts = misc:uniqc(Masters1),
    ReplicasCounts = misc:uniqc(Replicas1),

    NumServers = length(Servers),

    lists:any(
      fun (Counts0) ->
              Counts1 = [C || {_, C} <- Counts0],
              Len = length(Counts1),
              Counts = case Len < NumServers of
                           true ->
                               lists:duplicate(NumServers - Len, 0) ++ Counts1;
                           false ->
                               true = Len =:= NumServers,
                               Counts1
                       end,
              Counts =/= [] andalso lists:max(Counts) - lists:min(Counts) > 1
      end, [MastersCounts, ReplicasCounts]).

bucket_needs_rebalance(BucketConfig, Nodes) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    case proplists:get_value(type, BucketConfig) of
        membase ->
            case Servers of
                [] ->
                    false;
                _ ->
                    ns_bucket:num_replicas_changed(BucketConfig) orelse
                        lists:sort(Nodes) =/= lists:sort(Servers) orelse
                        map_needs_rebalance(Servers, BucketConfig)
            end;
        memcached ->
            lists:sort(Nodes) =/= lists:sort(Servers)
    end.

map_needs_rebalance(Servers, BucketConfig) ->
    case proplists:get_value(map, BucketConfig) of
        undefined ->
            true;
        Map ->
            case map_options_changed(Servers, BucketConfig) of
                true ->
                    true;
                {false, MapOpts} ->
                    unbalanced(Map, BucketConfig) andalso
                        incompatible_with_past_map(Servers, MapOpts, Map)
            end
    end.

map_options_changed(Servers, BucketConfig) ->
    case proplists:get_value(map_opts_hash, BucketConfig) of
        undefined ->
            true;
        OptsHash ->
            MapOpts = generate_vbucket_map_options(Servers,
                                                   BucketConfig),
            case erlang:phash2(MapOpts) of
                OptsHash ->
                    {false, MapOpts};
                _ ->
                    true
            end
    end.

incompatible_with_past_map(Nodes, MapOpts, Map) ->
    History = ns_bucket:past_vbucket_maps(),
    Matching =
        mb_map:find_matching_past_maps(Nodes, Map, MapOpts, History, [trivial]),
    not lists:member(Map, Matching).

%%
%% Internal functions
%%

%% @private


%% @doc Eject a list of nodes from the cluster, making sure this node is last.
eject_nodes(Nodes) ->
    %% Leave myself last
    LeaveNodes = case lists:member(node(), Nodes) of
                     true ->
                         (Nodes -- [node()]) ++ [node()];
                     false ->
                         Nodes
                 end,
    lists:foreach(fun ns_cluster:leave/1, LeaveNodes).

verify_replication(Bucket, Nodes, Map) ->
    ExpectedReplicators0 = ns_bucket:map_to_replicas(Map),
    ExpectedReplicators = lists:sort(ExpectedReplicators0),

    {ActualReplicators, BadNodes} = janitor_agent:get_src_dst_vbucket_replications(Bucket, Nodes),
    case BadNodes of
        [] -> ok;
        _ ->
            ale:error(?USER_LOGGER, "Rebalance is done, but failed to verify replications on following nodes:~p", [BadNodes]),
            exit(bad_replicas_due_to_bad_results)
    end,

    ok = check_test_condition(verify_replication, Bucket),

    case misc:comm(ExpectedReplicators, ActualReplicators) of
        {[], [], _} ->
            ok;
        {Missing, Extra, _} ->
            ?user_log(?BAD_REPLICATORS,
                      "Bad replicators after rebalance:~nMissing = ~p~nExtras = ~p",
                      [Missing, Extra]),
            exit(bad_replicas)
    end.

wait_for_mover(Pid) ->
    receive
        {'EXIT', Pid, Reason} ->
            case Reason of
                normal ->
                    ok;
                _ ->
                    exit({mover_crashed, Reason})
            end;
        {'EXIT', _Pid, {shutdown, stop} = Stop} ->
            ?log_debug("Got rebalance stop request"),
            TimeoutPid = diag_handler:arm_timeout(
                           5000,
                           fun (_) ->
                                   ?log_debug("Observing slow rebalance stop (mover pid: ~p)", [Pid]),
                                   timeout_diag_logger:log_diagnostics(slow_rebalance_stop)
                           end),
            try
                terminate_mover(Pid, Stop)
            after
                diag_handler:disarm_timeout(TimeoutPid)
            end;
        {'EXIT', _Pid, Reason} ->
            exit(Reason)
    end.

terminate_mover(Pid, StopReason) ->
    ?log_debug("Terminating mover ~p with reason ~p", [Pid, StopReason]),
    exit(Pid, StopReason),

    receive
        {'EXIT', Pid, MoverReason} ->
            ?log_debug("Mover ~p terminated with reason ~p",
                       [Pid, MoverReason]),
            %% No matter what the mover's termination reason was, we terminate
            %% with the reason that was asked of us. This is to deal with the
            %% cases when the mover just happens to terminate at around the
            %% time we request its termination.
            exit(StopReason);
        {'EXIT', _OtherPid, OtherReason} = Exit ->
            ?log_debug("Received an exit ~p while waiting for "
                       "mover ~p to terminate.", [Exit, Pid]),
            exit(OtherReason)
    end.

maybe_cleanup_old_buckets(KeepNodes) ->
    Requests = [{Node, ?cut(rpc:call(Node, ns_storage_conf,
                                     delete_unused_buckets_db_files, []))} ||
                   Node <- KeepNodes],

    case push_ns_config(KeepNodes) of
        ok ->
            case misc:multi_call_request(Requests, infinity, _ =:= ok) of
                {_, []} ->
                    ok;
                {_, BadNodes} ->
                    [?rebalance_error(
                        "Failed to cleanup old buckets on node ~p: ~p",
                        [Node, Error]) || {Node, Error} <- BadNodes],
                    {buckets_cleanup_failed, [N || {N, _} <- BadNodes]}
            end;
        Error ->
            Error
    end.

find_delta_recovery_map(Config, AllNodes, DeltaNodes, Bucket, BucketConfig) ->
    %% Ideally this should be caught by mb_map:find_matching_past_maps, but
    %% getting there requires a lot of changes.
    case ns_bucket:num_replicas_changed(BucketConfig) of
        true ->
            false;
        false ->
            {map, CurrentMap} = lists:keyfind(map, 1, BucketConfig),
            CurrentOptions = generate_vbucket_map_options(AllNodes,
                                                          BucketConfig),
            History = ns_bucket:past_vbucket_maps(Config),
            MatchingMaps = mb_map:find_matching_past_maps(AllNodes, CurrentMap,
                                                          CurrentOptions,
                                                          History),
            FailoverVBs = bucket_failover_vbuckets(Config, Bucket, DeltaNodes),

            case find_delta_recovery_map(CurrentMap,
                                         FailoverVBs, MatchingMaps) of
                not_found ->
                    false;
                {ok, Map} ->
                    {ok, #{target_map => Map,
                           target_map_opts => CurrentOptions,
                           failover_vbuckets => FailoverVBs}}
            end
    end.

find_delta_recovery_map(CurrentMap, FailoverVBs, MatchingMaps) ->
    CurrentVBs = map_to_vbuckets_dict(CurrentMap),
    MergeFun   = ?cut(lists:umerge(_2, _3)),
    DesiredVBs = dict:merge(MergeFun, FailoverVBs, CurrentVBs),

    Pred = ?cut(compare_vb_dict(map_to_vbuckets_dict(_), DesiredVBs)),
    misc:find_by(Pred, MatchingMaps).

compare_vb_dict(D1, D2) ->
    lists:sort(dict:to_list(D1)) =:= lists:sort(dict:to_list(D2)).


map_to_vbuckets_dict(Map) ->
    lists:foldr(
      fun ({V, Chain}, Acc) ->
              lists:foldl(fun (N, D) ->
                                  misc:dict_update(N, [V|_], [], D)
                          end,
                          Acc, lists:filter(_ =/= undefined, Chain))
      end, dict:new(), misc:enumerate(Map, 0)).

bucket_failover_vbuckets(Config, Bucket, DeltaNodes) ->
    dict:from_list(
      lists:map(
        fun (Node) ->
                VBs = proplists:get_value(
                        Bucket,
                        failover:get_failover_vbuckets(Config, Node),
                        []),
                {Node, lists:usort(VBs)}
        end, DeltaNodes)).

get_buckets_to_delta_recover(BucketConfigs, RequestedBuckets) ->
    [P || {Bucket, BucketConfig} = P <- BucketConfigs,
          ns_bucket:is_persistent(BucketConfig),
          RequestedBuckets =:= all orelse
              lists:member(Bucket, RequestedBuckets)].

build_delta_recovery_buckets(_AllNodes, [] = _DeltaNodes,
                             _AllBucketConfigs, _DeltaRecoveryBuckets) ->
    {ok, []};
build_delta_recovery_buckets(AllNodes, DeltaNodes,
                             AllBucketConfigs, DeltaRecoveryBuckets) ->
    Config = ns_config:get(),
    HandleBucketFun =
        handle_one_delta_recovery_bucket(Config, AllNodes, DeltaNodes, _),
    RequiredBuckets =
        get_buckets_to_delta_recover(AllBucketConfigs, DeltaRecoveryBuckets),

    case misc:partitionmap(HandleBucketFun, RequiredBuckets) of
        {FoundBuckets, []} ->
            {ok, FoundBuckets};
        _ ->
            {error, not_possible}
    end.

handle_one_delta_recovery_bucket(Config, AllNodes, DeltaNodes,
                                 {Bucket, BucketConfig}) ->
    case find_delta_recovery_map(Config, AllNodes,
                                 DeltaNodes, Bucket, BucketConfig) of
        false ->
            ?rebalance_debug("Couldn't delta recover bucket ~s because "
                             "suitable vbucket map is not found in the history",
                             [Bucket]),
            {right, Bucket};
        {ok, BucketInfo} ->
            ?rebalance_debug("Found delta recovery map for bucket ~s:~n~p",
                             [Bucket, BucketInfo]),
            {left, {Bucket, BucketInfo}}
    end.

apply_delta_recovery_buckets([], _DeltaNodes, _CurrentBuckets) ->
    ok;
apply_delta_recovery_buckets(DeltaRecoveryBuckets, DeltaNodes, CurrentBuckets) ->
    prepare_delta_recovery(DeltaNodes, DeltaRecoveryBuckets),
    TransitionalBuckets = prepare_delta_recovery_buckets(DeltaRecoveryBuckets,
                                                         DeltaNodes,
                                                         CurrentBuckets),

    ok = ns_bucket:update_buckets(
           TransitionalBuckets, CurrentBuckets,
           ns_cluster_membership:update_membership_sets(DeltaNodes, active) ++
               failover:clear_failover_vbuckets_sets(DeltaNodes)),

    config_sync(push, DeltaNodes),
    complete_delta_recovery(DeltaNodes),

    ok = check_test_condition(apply_delta_recovery),
    lists:foreach(
      fun ({Bucket, BucketConfig}) ->
              ok = wait_for_bucket(Bucket, DeltaNodes),
              ok = ns_janitor:cleanup_apply_config(
                     Bucket, DeltaNodes, BucketConfig,
                     [{apply_config_timeout, ?REBALANCER_APPLY_CONFIG_TIMEOUT}])
      end, TransitionalBuckets),

    ok.

wait_for_bucket(Bucket, Nodes) ->
    ?log_debug("Waiting until bucket ~p gets ready on nodes ~p", [Bucket, Nodes]),
    do_wait_for_bucket(Bucket, Nodes).

do_wait_for_bucket(Bucket, Nodes) ->
    case janitor_agent:check_bucket_ready(Bucket, Nodes, 60000) of
        ready ->
            ?log_debug("Bucket ~p became ready on nodes ~p", [Bucket, Nodes]),
            ok;
        {warming_up, Zombies} ->
            ?log_debug("Bucket ~p still not ready on nodes ~p",
                       [Bucket, Zombies]),
            do_wait_for_bucket(Bucket, Zombies);
        {failed, Zombies} ->
            ?log_error("Bucket ~p not available on nodes ~p",
                       [Bucket, Zombies]),
            fail
    end.

build_transitional_bucket_config(BucketConfig, TargetMap,
                                 Options, DeltaNodes, PresentVBuckets) ->
    {map, CurrentMap} = lists:keyfind(map, 1, BucketConfig),
    TransitionalMap = build_transitional_map(CurrentMap, PresentVBuckets),

    NewServers = DeltaNodes ++ ns_bucket:get_servers(BucketConfig),

    misc:update_proplist(BucketConfig,
                         [{map, TransitionalMap},
                          {servers, NewServers},
                          {deltaRecoveryMap, {TargetMap, Options}}]).

build_transitional_map(CurrentMap, PresentVBuckets) ->
    VBucketDeltaNodes =
        lists:foldl(
          fun ({Node, VBuckets}, Acc0) ->
                  lists:foldl(
                    fun (VBucket, Acc) ->
                            maps:update_with(VBucket, [Node|_], [Node], Acc)
                    end, Acc0, VBuckets)
          end, #{}, PresentVBuckets),

    lists:map(
      fun ({VBucket, CurrentChain}) ->
              DeltaNodes = maps:get(VBucket, VBucketDeltaNodes, []),
              build_transitional_chain(CurrentChain, DeltaNodes)
      end, misc:enumerate(CurrentMap, 0)).

build_transitional_chain([undefined | _] = CurrentChain, _DeltaNodes) ->
    CurrentChain;
build_transitional_chain(CurrentChain, DeltaNodes) ->
    PreservedNodes = [N || N <- CurrentChain, N =/= undefined],

    %% Previously the code here expected that some of the delta nodes might
    %% already be in the current chain. But that actually shouldn't
    %% happen. And elsewhere in the code we don't handle this situation
    %% gracefully. So we're going to assert instead.
    false = lists:any(lists:member(_, DeltaNodes), PreservedNodes),

    TransitionalChain = PreservedNodes ++ DeltaNodes,

    N = length(CurrentChain),
    true = length(TransitionalChain) =< N,
    misc:align_list(TransitionalChain, N, undefined).

-ifdef(TEST).
build_transitional_chain_test() ->
    ?assertEqual([undefined, undefined],
                 build_transitional_chain([undefined, undefined], [a, b])),
    ?assertEqual([a, b],
                 build_transitional_chain([a, undefined], [b])),
    ?assertEqual([a, b, c, undefined],
                 build_transitional_chain([a, undefined, undefined, undefined],
                                          [b, c])).
-endif.

start_link_graceful_failover(Nodes) ->
    proc_lib:start_link(erlang, apply, [fun run_graceful_failover/1, [Nodes]]).

run_graceful_failover(Nodes) ->
    NodesWanted = ns_node_disco:nodes_wanted(),
    config_sync(pull, NodesWanted),

    case failover:is_possible(Nodes, #{}) of
        ok ->
            ok;
        Error ->
            erlang:exit(Error)
    end,

    AllBucketConfigs = ns_bucket:get_buckets(),
    InterestingBuckets = [BC || BC = {_, Conf} <- AllBucketConfigs,
                                proplists:get_value(type, Conf) =:= membase,
                                %% when bucket doesn't have a vbucket map,
                                %% there's not much to do with respect to
                                %% graceful failover; so we skip these;
                                %%
                                %% note, that failover will still operate on
                                %% these buckets and, if needed, will remove
                                %% the node from server list
                                proplists:get_value(map, Conf, []) =/= []],
    NumBuckets = length(InterestingBuckets),

    case check_graceful_failover_possible(Nodes, InterestingBuckets) of
        true -> ok;
        {false, Type} ->
            erlang:exit(Type)
    end,

    config_sync(push, NodesWanted),

    proc_lib:init_ack({ok, self()}),

    ok = leader_activities:run_activity(
           graceful_failover, majority,
           fun () ->
                   ale:info(?USER_LOGGER,
                            "Starting vbucket moves for "
                            "graceful failover of ~p", [Nodes]),

                   ActiveNodes = ns_cluster_membership:active_nodes(),
                   InvolvedNodes = ns_cluster_membership:service_nodes(
                                     ActiveNodes, kv),
                   master_activity_events:note_rebalance_stage_started(
                     kv, InvolvedNodes),
                   lists:foldl(
                     fun ({BucketName, BucketConfig}, I) ->
                             do_run_graceful_failover_moves(Nodes,
                                                            BucketName,
                                                            BucketConfig,
                                                            I / NumBuckets,
                                                            NumBuckets),
                             I+1
                     end, 0, InterestingBuckets),
                   master_activity_events:note_rebalance_stage_completed(kv),
                   sleep_for_sdk_clients("graceful failover"),
                   {ok, []} = failover:orchestrate(Nodes, #{}),

                   ok
           end).

do_run_graceful_failover_moves(Nodes, BucketName, BucketConfig, I, N) ->
    master_activity_events:note_bucket_rebalance_started(BucketName),
    run_janitor_pre_rebalance(BucketName),

    Map = proplists:get_value(map, BucketConfig, []),
    Map1 = mb_map:promote_replicas_for_graceful_failover(Map, Nodes),

    ProgressFun = make_progress_fun(I, N),
    RV = run_mover(BucketName, BucketConfig,
                   ns_bucket:get_servers(BucketConfig),
                   ProgressFun, Map, Map1),
    master_activity_events:note_bucket_rebalance_ended(BucketName),
    RV.

check_graceful_failover_possible(Nodes, BucketsAll) ->
    %% No graceful failovers for non KV node
    case lists:all(?cut(lists:member(kv, ns_cluster_membership:node_services(_))),
                   Nodes) of
        true ->
            check_graceful_failover_possible_rec(Nodes, BucketsAll);
        false ->
            {false, non_kv_node}
    end.

check_graceful_failover_possible_rec(_Nodes, []) ->
    true;
check_graceful_failover_possible_rec(Nodes, [{_BucketName, BucketConfig} | RestBucketConfigs]) ->
    Map = proplists:get_value(map, BucketConfig, []),
    Servers = ns_bucket:get_servers(BucketConfig),
    case lists:any(lists:member(_, Servers), Nodes) of
        true ->
            Map1 = mb_map:promote_replicas_for_graceful_failover(Map, Nodes),
            %% Do not allow graceful failover if the returned map, Map1, has any
            %% of the nodes to be removed as the head of Chain in vbucket map.
            case lists:any(?cut(lists:member(hd(_), Nodes)), Map1) of
                true ->
                    {false, not_graceful};
                false ->
                    check_graceful_failover_possible_rec(Nodes, RestBucketConfigs)
            end;
        false ->
            check_graceful_failover_possible_rec(Nodes, RestBucketConfigs)
    end.

drop_old_2i_indexes(KeepNodes) ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    NewNodes = KeepNodes -- ns_cluster_membership:active_nodes(Snapshot),
    %% Only delta recovery is supported for index service.
    %% Note that if a node is running both KV and index service,
    %% and if user selects the full recovery option for such
    %% a node, then recovery_type will be set to full.
    %% But, we will treat delta and full recovery the same for
    %% the index data.
    %% Also, delta recovery for index service is different
    %% from that for the KV service. In case of index, it just
    %% means that we will not drop the indexes and their meta data.
    CleanupNodes =
        [N || N <- NewNodes,
              ns_cluster_membership:get_recovery_type(Snapshot, N) =:= none],
    ?rebalance_info("Going to drop possible old 2i indexes on nodes ~p",
                    [CleanupNodes]),
    {Oks, RPCErrors, Downs} = misc:rpc_multicall_with_plist_result(
                                CleanupNodes,
                                ns_storage_conf, delete_old_2i_indexes, []),
    RecoveryNodes = NewNodes -- CleanupNodes,
    ?rebalance_info("Going to keep possible 2i indexes on nodes ~p",
                    [RecoveryNodes]),
    %% Clear recovery type for non-KV nodes here.
    %% recovery_type for nodes running KV services gets cleared later.
    NonKV = [N || N <- RecoveryNodes,
                  not lists:member(
                        kv, ns_cluster_membership:node_services(Snapshot, N))],

    ok = chronicle_compat:set_multiple(
           ns_cluster_membership:update_membership_sets(NonKV, active) ++
               ns_cluster_membership:clear_recovery_type_sets(NonKV)),

    Errors = [{N, RV}
              || {N, RV} <- Oks,
                 RV =/= ok]
        ++ RPCErrors
        ++ [{N, node_down} || N <- Downs],
    case Errors of
        [] ->
            ?rebalance_debug("Cleanup succeeded: ~p", [Oks]),
            ok;
        _ ->
            ?rebalance_error("Failed to cleanup indexes: ~p", [Errors]),
            {old_indexes_cleanup_failed, Errors}
    end.

%%
%% Check whether user wants us to fail or delay the specified step
%% during rebalance.
%%
%% There are following 3 types of rebalance test conditions:
%%  1. Applicable to a bucket or service. E.g. the service_rebalance_start
%%      test condition can be used to fail or delay the start of rebalance
%%      of any topology aware service.
%%  2. Applicable to certain step during vBucket move for specified bucket.
%%  3. Applicable to entire rebalance. E.g. delay rebalance at the start.
%%
%% The delay can be used to inject other failures. E.g. Introduce a delay
%% of 60s during rebalance of a bucket. During those 60s, user can
%% SIGSTOP memcached on a node.
%%
%% 'Kind' can be a bucket or a service.
%%
check_test_condition(Step) ->
    check_test_condition(Step, []).

check_test_condition(Step, Kind) ->
    case testconditions:get(Step) of
        fail ->
            %% E.g. fail rebalance at the start.
            %% Triggered by: testconditions:set(rebalance_start, fail)
            trigger_failure(Step, []);
        {delay, Sleep} ->
            %% E.g. delay rebalance by 60s at the start.
            %% Triggered by:
            %%  testconditions:set(rebalance_start, {delay, 60000})
            trigger_delay(Step, [], Sleep);
        {fail, Kind} ->
            %% E.g. fail verify_replication for bucket "test".
            %% Triggered by:
            %%  testconditions:set(verify_replication, {fail, “test”})
            trigger_failure(Step, Kind);
        {delay, Kind, Sleep} ->
            %% E.g. delay service_rebalance_start by 1s for index service.
            %% Triggered by:
            %%  testconditions:set(service_rebalance_start,
            %%                     {delay, index, 1000})
            trigger_delay(Step, Kind, Sleep);
        {for_vb_move, Kind, N, Condition} ->
            %% Trigger the test condition for Nth vBucket move.
            %% Note it is NOT vBucket #N, but rather the Nth vBucket
            %% that is being moved. The actual vBucket # may be anything.
            %% This is done because generally rebalance does not move all
            %% vBuckets and normally users don’t know which vBuckets will
            %% move during a particular rebalance.
            %% E.g. during a rebalance, users may not know whether
            %% vBucket #678 will move. So, instead they can set the
            %% test condition to fail rebalance during say 10th vBucket move.
            %% The 10th vBucket to move may be any vBucket e.g. vBucket #348.
            %% E.g. fail rebalance after backfill for 5th vBucket,
            %% bucket "test".
            %% Triggered by:
            %%  testconditions:set(backfill_done,
            %%                     {for_vb_move, "test", 5, fail}).
            trigger_condition_for_Nth_move(Step, Kind, N, Condition);
        _ ->
            ok
    end.

trigger_failure(Step, Kind) ->
    Msg = case Kind of
              [] ->
                  io_lib:format("Failure triggered by test during ~p", [Step]);
              _ ->
                  io_lib:format("Failure triggered by test during ~p for ~p",
                                [Step, Kind])
          end,
    ?rebalance_error("~s", [lists:flatten(Msg)]),
    testconditions:delete(Step),
    fail_by_test_condition.

trigger_delay(Step, Kind, Sleep) ->
    Msg = case Kind of
              [] ->
                  io_lib:format("Delay triggered by test during ~p. "
                                "Sleeping for ~p ms", [Step, Sleep]);
              _ ->
                  io_lib:format("Delay triggered by test during ~p for ~p. "
                                "Sleeping for ~p ms", [Step, Kind, Sleep])
          end,
    ?rebalance_error("~s", [lists:flatten(Msg)]),
    testconditions:delete(Step),
    timer:sleep(Sleep).

trigger_condition_for_Nth_move(Step, Kind, 1, Condition) ->
    case Condition of
        fail ->
            trigger_failure(Step, Kind);
        {delay, Sleep} ->
            trigger_delay(Step, Kind, Sleep)
    end;
trigger_condition_for_Nth_move(Step, Kind, N, Condition) ->
    testconditions:set(Step, {for_vb_move, Kind, N - 1, Condition}).


-ifdef(TEST).
find_delta_recovery_map_test() ->
    Map = [[b, undefined],
           [b, undefined],
           [b, undefined],
           [b, c],
           [c, b],
           [c, b],
           [d, c],
           [d, c]],
    FailoverVBs = dict:from_list([{a, [0, 1, 2]}]),

    Matching = [[a, b],
                [a, b],
                [b, a],
                [b, c],
                [c, b],
                [c, b],
                [d, c],
                [d, c]],

    NonMatching1 = [[a, b],
                    [a, b],
                    [b, a],
                    [b, a],
                    [c, b],
                    [c, b],
                    [d, c],
                    [d, c]],

    NonMatching2 = [[a, b],
                    [a, b],
                    [b, a],
                    [b, c],
                    [c, b],
                    [c, b],
                    [d, b],
                    [d, b]],

    {ok, Matching} = find_delta_recovery_map(Map, FailoverVBs, [Matching]),

    not_found = find_delta_recovery_map(Map, FailoverVBs, [NonMatching1]),
    not_found = find_delta_recovery_map(Map, FailoverVBs, [NonMatching2]),
    not_found = find_delta_recovery_map(Map, FailoverVBs,
                                        [NonMatching1, NonMatching2]),

    {ok, Matching} = find_delta_recovery_map(Map, FailoverVBs,
                                             [NonMatching1,
                                              Matching, NonMatching2]),

    %% This test is essentially for compare_vb_dict, and this fun was
    %% introduced as dict's don't support proper comparison, i.e., D1 =:= D2
    %% is not an accurate comparison.
    Map2 = [['n_0@10.17.2.22', 'n_1@127.0.0.1'],
            ['n_0@10.17.2.22', 'n_2@127.0.0.1'],
            ['n_1@127.0.0.1', 'n_0@10.17.2.22'],
            ['n_1@127.0.0.1', undefined],
            ['n_2@127.0.0.1', 'n_0@10.17.2.22'],
            ['n_2@127.0.0.1', undefined],
            ['n_1@127.0.0.1', undefined],
            ['n_2@127.0.0.1', undefined]],
    MatchingMaps = [['n_0@10.17.2.22', 'n_1@127.0.0.1'],
                    ['n_0@10.17.2.22', 'n_2@127.0.0.1'],
                    ['n_1@127.0.0.1', 'n_0@10.17.2.22'],
                    ['n_1@127.0.0.1', 'n_3@127.0.0.1'],
                    ['n_2@127.0.0.1', 'n_0@10.17.2.22'],
                    ['n_2@127.0.0.1', 'n_3@127.0.0.1'],
                    ['n_3@127.0.0.1', 'n_1@127.0.0.1'],
                    ['n_3@127.0.0.1', 'n_2@127.0.0.1']],
    FailoverVBs2 = dict:from_list([{'n_3@127.0.0.1', [3, 5, 6, 7]}]),
    {ok, MatchingMaps} = find_delta_recovery_map(Map2, FailoverVBs2, [MatchingMaps]).

compare_vb_dict_test() ->
    List1 = [{aa2, [0, 1, 2]}, {c, [0, 1, 2]}, {aa1, [0, 1, 2]}],
    List2 = [{aa3, [0, 1, 2]}],

    D1 = dict:from_list(List1),
    D2 = dict:from_list(List2),
    DMerge = dict:merge(fun (_K, _V1, _V2) -> [] end, D1, D2),

    ListAll = lists:sort(List1 ++ List2),
    DAll = dict:from_list(ListAll),

    ?assertEqual(false, DAll =:= DMerge),
    ?assertEqual(true, compare_vb_dict(DAll,DMerge)).

map_to_vbuckets_dict_test() ->
    Map = [[a, b],
           [a, b],
           [b, a],
           [b, c],
           [c, b],
           [c, b]],
    ?assertEqual([{a, [0, 1, 2]},
                  {b, [0, 1, 2, 3, 4, 5]},
                  {c, [3, 4, 5]}],
                 lists:sort(dict:to_list(map_to_vbuckets_dict(Map)))).

get_buckets_to_delta_recovery_test() ->
    Buckets = [{"b1", [{type, membase}]},
               {"b2", [{type, memcached}]},
               {"b3", [{type, membase},
                       {storage_mode, couchstore}]},
               {"b4", [{type, membase},
                       {storage_mode, ephemeral}]},
               {"b5", [{type, membase},
                       {storage_mode, magma}]}],
    ?assertMatch([{"b1", _}, {"b3", _}, {"b5", _}],
                 get_buckets_to_delta_recover(Buckets,
                                              ["b1", "b2", "b3", "b4", "b5"])),
    ?assertMatch([{"b1", _}, {"b3", _}, {"b5", _}],
                 get_buckets_to_delta_recover(Buckets, all)),
    ?assertMatch([{"b3", _}],
                 get_buckets_to_delta_recover(Buckets, ["b3", "b4"])).
-endif.

prepare_rebalance(Nodes) ->
    case rebalance_agent:prepare_rebalance(Nodes, self()) of
        ok ->
            ok;
        Error ->
            exit({prepare_rebalance_failed, Error})
    end.

unprepare_rebalance(Nodes) ->
    case rebalance_agent:unprepare_rebalance(Nodes, self()) of
        ok ->
            ok;
        Error ->
            ?log_error("Failed to reach rebalance_agent on "
                       "some nodes to cleanup after reblance: ~p",
                       [Error])
    end.

prepare_delta_recovery(Nodes, BucketConfigs) ->
    Buckets = proplists:get_keys(BucketConfigs),
    case rebalance_agent:prepare_delta_recovery(Nodes, self(), Buckets) of
        ok ->
            ok;
        Errors ->
            ?log_error("Failed to prepare delta "
                       "recovery for bucket ~p on some nodes:~n~p",
                       [Buckets, Errors]),
            exit({prepare_delta_recovery_failed, Buckets, Errors})
    end.

prepare_delta_recovery_buckets(DeltaRecoveryBuckets,
                               DeltaNodes, CurrentBuckets) ->
    lists:map(
      fun ({Bucket, BucketInfo}) ->
              {_, BucketConfig} = lists:keyfind(Bucket, 1, CurrentBuckets),
              #{target_map := Map,
                target_map_opts := Opts,
                failover_vbuckets := FailoverVBuckets} = BucketInfo,

              PresentVBuckets =
                  prepare_one_delta_recovery_bucket(Bucket, BucketConfig,
                                                    FailoverVBuckets),
              TransitionalBucket =
                  build_transitional_bucket_config(BucketConfig, Map, Opts,
                                                   DeltaNodes, PresentVBuckets),
              {Bucket, TransitionalBucket}
      end, DeltaRecoveryBuckets).

prepare_one_delta_recovery_bucket(Bucket, BucketConfig, FailoverVBuckets) ->
    Map = proplists:get_value(map, BucketConfig, []),
    VBucketsToRecover =
        dict:fold(
          fun (_Node, VBuckets, Acc) ->
                  sets:union(sets:from_list(VBuckets), Acc)
          end, sets:new(), FailoverVBuckets),

    ?log_debug("Going to get failover logs for delta recovery.~n"
               "Bucket: ~p~n"
               "VBuckets: ~p",
               [Bucket, sets:to_list(VBucketsToRecover)]),
    FailoverLogs = get_active_failover_logs(Bucket, Map, VBucketsToRecover),
    ?log_debug("Got the following failover logs:~n~p", [FailoverLogs]),

    FailoverVBucketsList = dict:to_list(FailoverVBuckets),
    ?log_debug("Going to prepare bucket ~p on some nodes for delta recovery.~n"
               "Nodes: ~p",
               [Bucket, FailoverVBucketsList]),
    case rebalance_agent:prepare_delta_recovery_bucket(
           self(), Bucket, FailoverVBucketsList, FailoverLogs) of
        {ok, NodeVBuckets} ->
            Nodes = dict:fetch_keys(FailoverVBuckets),
            ?log_debug("Prepared bucket ~p for delta "
                       "recovery on ~p successfully. "
                       "VBuckets that are left intact are:~n~p",
                       [Bucket, Nodes, NodeVBuckets]),
            NodeVBuckets;
        Errors ->
            ?log_error("Failed to prepare bucket ~p for delta recovery "
                       "on some nodes:~n~p", [Bucket, Errors]),
            exit({prepare_delta_recovery_failed, Bucket, Errors})
    end.

get_target_map_and_opts(Bucket, DeltaRecoveryBuckets) ->
    case lists:keyfind(Bucket, 1, DeltaRecoveryBuckets) of
        false ->
            undefined;
        {_, #{target_map := TargetMap, target_map_opts := TargetMapOpts}} ->
            {TargetMap, TargetMapOpts}
    end.

get_active_failover_logs(Bucket, Map, VBucketsSet) ->
    NodeVBuckets0   = find_active_nodes_of_vbuckets(Map, VBucketsSet),
    MissingVBuckets = maps:get(undefined, NodeVBuckets0, []),

    NodeVBuckets = maps:to_list(maps:remove(undefined, NodeVBuckets0)),
    case janitor_agent:get_failover_logs(Bucket, NodeVBuckets) of
        {ok, FailoverLogs} ->
            Result = maps:from_list([{V, missing} || V <- MissingVBuckets]),
            lists:foldl(
              fun ({_Node, NodeFailoverLogs}, Acc) ->
                      maps:merge(Acc, maps:from_list(NodeFailoverLogs))
              end, Result, FailoverLogs);
        Errors ->
            ?log_error("Failed to get failover logs "
                       "from some nodes for delta recovery.~n"
                       "Bucket: ~p~n"
                       "Requests: ~p~n"
                       "Errors: ~p",
                       [Bucket, NodeVBuckets, Errors]),
            exit({get_failover_logs_failed, Bucket, NodeVBuckets, Errors})
    end.

find_active_nodes_of_vbuckets(Map, VBucketsSet) ->
    lists:foldl(
      fun ({VBucket, [Active|_]}, Acc) ->
              case sets:is_element(VBucket, VBucketsSet) of
                  true ->
                      maps:update_with(Active, [VBucket | _], [VBucket], Acc);
                  false ->
                      Acc
              end
      end, #{}, misc:enumerate(Map, 0)).

-ifdef(TEST).
find_active_nodes_of_vbuckets_test() ->
    Map = [[a, b],
           [b, a],
           [a, b],
           [undefined, undefined]],

    SortMap = ?cut(maps:map(?cut(lists:sort(_2)), _)),

    ?assertEqual(#{ a => [0, 2],
                    b => [1],
                    undefined => [3] },
                 SortMap(find_active_nodes_of_vbuckets(
                           Map, sets:from_list([0, 1, 2, 3])))),

    ?assertEqual(#{ a => [0],
                    b => [1],
                    undefined => [3] },
                 SortMap(find_active_nodes_of_vbuckets(
                           Map, sets:from_list([0, 1, 3])))),


    ?assertEqual(#{ a => [0, 2] },
                 SortMap(find_active_nodes_of_vbuckets(
                           Map, sets:from_list([0, 2])))).
-endif.

complete_delta_recovery(Nodes) ->
    ?log_debug("Going to complete delta "
               "recovery preparation on nodes ~p", [Nodes]),
    case rebalance_agent:complete_delta_recovery(Nodes, self()) of
        ok ->
            ?log_debug("Delta recovery preparation completed.");
        Errors ->
            ?log_error("Failed to complete delta recovery "
                       "preparation on some nodes:~n~p", [Errors]),
            exit({complete_delta_recovery_failed, Errors})
    end.

deactivate_bucket_data_on_unknown_nodes(BucketName, Nodes) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    Servers = ns_bucket:get_servers(BucketConfig),
    UnknownNodes = Nodes -- Servers,
    case rebalance_agent:deactivate_bucket_data(BucketName, UnknownNodes,
                                                self()) of
        ok ->
            ok;
        {error, Error} ->
            exit({error, deactivate_bucket_data_failed, Error})
    end.
