%% @author Couchbase <info@couchbase.com>
%% @copyright 2023-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(guardrail_monitor).

-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("ns_test.hrl").
-endif.

-behaviour(gen_server).

-export([is_enabled/0, get_config/0, get/1, get/2, start_link/0,
         validate_topology_change/2, validate_storage_migration/3,
         check_num_replicas_change/3, get_local_status/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).


%% Amount of time to wait between state checks (ms)
-define(CHECK_INTERVAL, ?get_param(check_interval, 20000)).

%% 60s should be sufficient time that we don't timeout when a node is busy
-define(RPC_TIMEOUT, ?get_timeout(rpc_timeout, 60000)).
%% 120s should be enough that we never hit it when the 60s timeout for an
%% individual rpc call is reached
-define(PARALLEL_RPC_TIMEOUT, ?get_timeout(parallel_rpc_timeout, 120000)).

-define(SERVER, ?MODULE).

-record(state, {
                statuses = [] :: [{resource(), status()}],
                timer_ref = undefined :: undefined | reference()
               }).

-type resource() :: {bucket, bucket_name()} | index.
-export_type([resource/0]).

-type disk_severity() :: serious | critical | maximum.
-type index_severity() :: warning | serious | critical.
-type status() :: ok | data_ingress_status() | disk_severity() |
                  index_severity().
-export_type([status/0]).

-type disk_stats_error() :: no_dbdir
                          | {dbdir_path_error, term()}
                          | no_disk_stats_found.

-type topology_change_error() :: data_size_will_be_too_high
                               | rr_will_be_too_low
                               | not_enough_cores_for_num_buckets
                               | disk_usage_too_high
                               | disk_usage_error.

-spec is_enabled() -> boolean().
is_enabled() ->
    cluster_compat_mode:is_cluster_76() andalso
        config_profile:get_bool({resource_management, enabled}).


-spec get_config() -> proplists:proplist().
get_config() ->
    ns_config:read_key_fast(resource_management, []).

-spec get(cores_per_bucket | collections_per_quota | disk_usage) ->
    undefined | number() | [{atom(), number()}].
get(cores_per_bucket) ->
    case proplists:get_value(cores_per_bucket, get_config()) of
        undefined -> undefined;
        Config ->
            case proplists:get_value(enabled, Config) of
                true -> proplists:get_value(minimum, Config);
                false -> undefined
            end
    end;
get(collections_per_quota) ->
    case proplists:get_value(collections_per_quota, get_config()) of
        undefined -> undefined;
        Config ->
            case proplists:get_value(enabled, Config) of
                true -> proplists:get_value(maximum, Config);
                false -> undefined
            end
    end;
get(disk_usage) ->
    case proplists:get_value(disk_usage, get_config()) of
        undefined ->
            undefined;
        DiskUsageConfig ->
            get_thresholds(DiskUsageConfig)
    end.

get(bucket, Key) ->
    case proplists:get_value(bucket, get_config()) of
        undefined ->
            undefined;
        BucketConfig ->
            case proplists:get_value(Key, BucketConfig) of
                undefined ->
                    undefined;
                ResourceConfig ->
                    case proplists:get_value(enabled, ResourceConfig) of
                        false ->
                            undefined;
                        true ->
                            ResourceConfig
                    end
            end
    end.

get_local_status(Resource, Config, Default) ->
    ns_config:search_node_prop(Config, local_resource_statuses, Resource,
                               Default).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


validate_topology_change(EjectedLiveNodes, KeepNodes) ->
    case is_enabled() of
        true ->
            KeepKVNodes = ns_cluster_membership:service_nodes(KeepNodes, kv),
            ActiveNodes = ns_cluster_membership:active_nodes(),
            ActiveKVNodes = ns_cluster_membership:service_nodes(ActiveNodes,
                                                                kv),
            NewNodes = KeepNodes -- ActiveNodes,
            NewKVNodes = KeepKVNodes -- ActiveKVNodes,
            functools:sequence_(
              [?cut(validate_topology_change_data_grs(ActiveKVNodes,
                                                      EjectedLiveNodes,
                                                      KeepKVNodes)),
               ?cut(validate_topology_change_disk_usage(NewNodes,
                                                        EjectedLiveNodes,
                                                        KeepNodes)),
               ?cut(validate_topology_change_cores_per_bucket(NewKVNodes))]);
        false ->
            ok
    end.

validate_topology_change_data_grs(ActiveKVNodes, EjectedNodes, KeepKVNodes) ->
    lists:foldl(
      fun (Resource, ok) ->
              validate_topology_change_data_gr(Resource, ActiveKVNodes,
                                               EjectedNodes, KeepKVNodes);
          (_, Err) ->
              Err
      end, ok, [data_size, resident_ratio]).

-spec topology_change_error(
        {data_size, [bucket_name()]}
        | {resident_ratio, [bucket_name()]}
        | {cores_per_bucket, [string()], [string()]}
        | {disk_usage_high, [string()]}
        | {disk_usage_error, [string()]}) ->
          {topology_change_error(), binary()}.
topology_change_error({data_size, Buckets}) ->
    {data_size_will_be_too_high,
     iolist_to_binary(
       io_lib:format(
         "The following buckets are expected to "
         "breach the maximum data size per node: ~s.",
         [lists:join(", ", Buckets)]))};
topology_change_error({resident_ratio, Buckets}) ->
    {rr_will_be_too_low,
     iolist_to_binary(
       io_lib:format(
         "The following buckets are expected to "
         "breach the resident ratio minimum: ~s.",
         [lists:join(", ", Buckets)]))};
topology_change_error({cores_per_bucket, LowCoreNodes, NoCoreNodes}) ->
    {not_enough_cores_for_num_buckets,
     iolist_to_binary(
       case LowCoreNodes of
           [] ->
               "";
           _ ->
               io_lib:format(
                 "The following node(s) being added have insufficient cpu "
                 "cores for the number of buckets already in the cluster: ~s. ",
                 [lists:join(", ", LowCoreNodes)])
       end ++
           case NoCoreNodes of
               [] ->
                   "";
               _ ->
                   io_lib:format(
                     "The following node(s) being added got error(s) fetching "
                     "the number of cpu cores available, so may not have "
                     "sufficient cores for the number of buckets already in "
                     "the cluster: ~s.",
                     [lists:join(", ", NoCoreNodes)])
           end)};
topology_change_error({disk_usage_high, HighDiskNodes}) ->
    {disk_usage_too_high,
     iolist_to_binary(
       io_lib:format(
         "The following node(s) have insufficient disk space to safely "
         "reduce the total disk capacity of nodes in the cluster: "
         "~s.", [lists:join(", ", HighDiskNodes)]))};
topology_change_error({disk_usage_error, ErrorDiskNodes}) ->
    {disk_usage_error,
     iolist_to_binary(
       io_lib:format(
         "The following node(s) got error(s) fetching the disk usage "
         "stats, so there may not be sufficient disk space to safely reduce "
         "the total disk capacity of nodes in the cluster: ~s.",
         [lists:join(", ", ErrorDiskNodes)]))}.

validate_topology_change_data_gr(Resource, ActiveKVNodes, EjectedNodes,
                                 KeepKVNodes) ->
    case get(bucket, Resource) of
        undefined ->
            ok;
        ResourceConfig ->
            BucketDataSizes =
                stats_interface:total_active_logical_data_size(
                  EjectedNodes ++ KeepKVNodes),
            BadBuckets =
                maps:keys(
                  maps:filter(
                    fun (_Name, 0) ->
                            false;
                        (Name, TotalDataSize) ->
                            maybe_validate_bucket_topology_change(
                              Resource, Name, ActiveKVNodes, KeepKVNodes,
                              TotalDataSize, ResourceConfig)
                    end, BucketDataSizes)),
            case BadBuckets of
                [] ->
                    %% No bucket is anticipated to violate this guardrail
                    ok;
                _ ->
                    %% Guardrail violation expected for each of BadBuckets
                    {error, topology_change_error({Resource, BadBuckets})}

            end
    end.

maybe_validate_bucket_topology_change(Resource, Name, ActiveKVNodes,
                                      KeepKVNodes, TotalDataSize,
                                      ResourceConfig) ->
    case ns_bucket:get_bucket(Name) of
        not_present ->
            false;
        {ok, BCfg} ->
            OldNumNodes = case ns_bucket:get_servers(BCfg) of
                              undefined -> length(ActiveKVNodes);
                              S -> length(S)
                          end,
            NumNodes = case ns_bucket:get_width(BCfg) of
                           undefined -> length(KeepKVNodes);
                           W -> W * ns_cluster_membership:server_groups()
                       end,
            case (OldNumNodes > NumNodes) orelse
                ns_bucket:storage_mode_migration_in_progress(BCfg) of
                false ->
                    %% We will have at least as many nodes after the rebalance
                    %% as before, and there is no storage migration in progress,
                    %% so this won't make RR% worse
                    false;
                true ->
                    %% The number of nodes will decrease, or there is a storage
                    %% migration in progress, so we must check if we expect the
                    %% rebalance of this bucket to cause a RR% / data size issue
                    validate_bucket_topology_change(
                      Resource, KeepKVNodes, TotalDataSize, ResourceConfig,
                      BCfg, NumNodes)
            end
    end.

validate_bucket_topology_change(data_size, KeepKVNodes, TotalDataSize,
                                ResourceConfig, BCfg, NumNodes) ->
    ExpDataSizePerNode = TotalDataSize / (NumNodes * math:pow(10, 12)),
    DataSizeLimit = get_data_size_maximum_on_nodes(
                      ResourceConfig, BCfg, KeepKVNodes),
    validate_resource_max(ExpDataSizePerNode, DataSizeLimit);
validate_bucket_topology_change(resident_ratio, KeepKVNodes, TotalDataSize,
                                ResourceConfig, BCfg, NumNodes) ->
    Quota = ns_bucket:raw_ram_quota(BCfg),
    ExpResidentRatio = 100 * NumNodes * Quota / TotalDataSize,
    ResidentRatioLimit = get_resident_ratio_minimum_on_nodes(
                           ResourceConfig, BCfg, KeepKVNodes),
    validate_resource_min(ExpResidentRatio, ResidentRatioLimit).

validate_topology_change_cores_per_bucket(NewKVNodes) ->
    case guardrail_monitor:get(cores_per_bucket) of
        undefined ->
            ok;
        MinCoresPerBucket ->
            NumBuckets = length(ns_bucket:get_bucket_names()),
            MinCores = MinCoresPerBucket * NumBuckets,
            NodeCores = get_cores_from_nodes(NewKVNodes),
            BadNodes =
                lists:filter(
                  fun ({Node, [{cpu_cores_available, 0}]}) ->
                          ?log_error(
                             "Got that node ~p has 0 cpu_cores_available. "
                             "This is likely an error, so we will not permit "
                             "the rebalance to go ahead.", [Node]),
                          true;
                      ({_Node, [{cpu_cores_available, C}]})
                        when is_number(C), C < MinCores ->
                          true;
                      ({_Node, [{cpu_cores_available, C}]})
                        when is_number(C), C >= MinCores ->
                          false;
                      ({Node, Other}) ->
                          ?log_error(
                             "Couldn't get cpu_cores_available for node ~p. "
                             "Instead got ~w. Will not permit rebalance to "
                             "go ahead.", [Node, Other]),
                          true
                  end, NodeCores),
            {LowCoreNodes, NoCoreNodes} =
                lists:partition(
                  fun({_Node, [{cpu_cores_available, C}]})
                        when is_number(C), C > 0 ->
                          true;
                     (_) ->
                          false
                  end, BadNodes),
            case LowCoreNodes ++ NoCoreNodes of
                [] ->
                    ok;
                _ ->
                    {error, topology_change_error(
                              {cores_per_bucket,
                               lists:map(?cut(atom_to_list(element(1, _))),
                                         LowCoreNodes),
                               lists:map(?cut(atom_to_list(element(1, _))),
                                         NoCoreNodes)})}
            end
    end.

get_cores_from_nodes(Nodes) ->
    misc:parallel_map(
      fun (Node) ->
              {Node, rpc:call(Node, sigar, get_gauges,
                              [[cpu_cores_available]], ?RPC_TIMEOUT)}
      end, Nodes, ?PARALLEL_RPC_TIMEOUT).

-spec validate_topology_change_disk_usage([node()], [node()], [node()]) ->
    ok | {error, topology_change_error()}.
validate_topology_change_disk_usage(AddedNodes, EjectedNodes, KeepNodes) ->
    case guardrail_monitor:get(disk_usage) of
        undefined ->
            ok;
        Thresholds ->
            %% Fetch disk stats for all nodes, so that we will be able to catch
            %% if an existing or added node has got high disk usage
            NodeDiskStats = get_disk_stats_from_nodes(
                              KeepNodes ++ EjectedNodes),
            PossiblyBadNodes = get_high_disk_usage_from_stats(
                         Thresholds, NodeDiskStats),
            %% Exclude nodes that only cross the serious/critical thresholds,
            %% as these thresholds are ignored for topology change
            BadNodes = lists:filter(
                fun ({_Node, critical}) ->  false;
                    ({_Node, serious}) ->  false;
                    (_) -> true
                end, PossiblyBadNodes),
            %% Split the bad nodes into those that have high disk usage and
            %% those that we failed to get disk usage stats for, in order to
            %% give appropriate error messages
            {HighDiskNodes, ErrorDiskNodes} =
                lists:partition(
                  fun ({_Node, maximum}) -> true;
                      (_) -> false
                  end, BadNodes),
            case {HighDiskNodes, ErrorDiskNodes} of
                {[], []} ->
                    %% Since no node is currently above the disk usage
                    %% limit, we assume that the rebalance is safe to perform.
                    %% This is not a good assumption to make as it is entirely
                    %% possible that the disk usage will increase to an unsafe
                    %% limit, as we are reducing the total disk size for the
                    %% cluster. However, it is not feasible to determine the
                    %% resultant disk usage after the rebalance, as it is
                    %% impacted by a number of factors, such as no longer
                    %% being compacted, and fragmentation.
                    ok;
                {_, []} ->
                    %% When there are no errors fetching disk stats and there
                    %% are nodes with high disk usage, we should check that the
                    %% rebalance won't decrease the total disk capacity, as that
                    %% would make the high disk usage worse
                    validate_disk_size_not_decreased(
                      NodeDiskStats, EjectedNodes, AddedNodes, HighDiskNodes);
                {_, _} ->
                    %% If there were errors fetching the disk usage for any
                    %% nodes, we will just report these as the reason for not
                    %% permitting the rebalance, even if there were also nodes
                    %% with high disk usage
                    {error,
                     topology_change_error(
                       {disk_usage_error,
                        lists:map(?cut(atom_to_list(element(1, _))),
                                  ErrorDiskNodes)})}
            end
    end.

-spec validate_disk_size_not_decreased(
        [{node(), {string(), number(), number()}}],
        [node()],
        [node()],
        [{atom(), term()}]) ->
          ok | {error, term()}.
validate_disk_size_not_decreased(NodeDiskStats, EjectedNodes, AddedNodes,
                                 HighDiskNodes) ->
    %% Compare the total disk sizes for the nodes being ejected with
    %% that of those being added
    case total_disk_size(NodeDiskStats, EjectedNodes) >
        total_disk_size(NodeDiskStats, AddedNodes) of
        false ->
            %% We expect to have no less total disk space available
            %% after the rebalance than before, so we assume that this
            %% rebalance is safe to perform
            ok;
        true ->
            %% The rebalance will reduce the total disk size
            {error,
             topology_change_error(
               {disk_usage_high,
                lists:map(?cut(atom_to_list(element(1, _))),
                          HighDiskNodes)})}
    end.

-spec total_disk_size([{node(), {string(), number(), number()}}], [node()]) ->
          number().
total_disk_size(DiskStats, Nodes) ->
    DataDiskSizes =
        lists:filtermap(
          fun ({Node, {ok, {_Disk, Size, _Used}}}) ->
                  case lists:member(Node, Nodes) of
                      true -> {true, Size};
                      false -> false
                  end
          end, DiskStats),
    lists:sum(DataDiskSizes).

-spec get_disk_stats_from_nodes([node()]) ->
          [{ok,
            {node(),
             {ok, ns_disksup:disk_stat()}
            | {badrpc, term()}
            | {error, disk_stats_error()}}}].
get_disk_stats_from_nodes(Nodes) ->
    misc:parallel_map(
      fun (Node) ->
              case rpc:call(Node, ns_disksup, get_disk_data, [],
                            ?RPC_TIMEOUT) of
                  {badrpc, _} = Error ->
                      {Node, Error};
                  Mounts ->
                      {Node, get_disk_data(Mounts)}
              end
      end, Nodes, ?PARALLEL_RPC_TIMEOUT).

-spec get_high_disk_usage_from_nodes([{atom(), number()}],
                                     [node()]) ->
          [{node(), disk_severity() | {error, any()} | any()}].
get_high_disk_usage_from_nodes(Thresholds, Nodes) ->
    NodeDiskStats = get_disk_stats_from_nodes(Nodes),
    get_high_disk_usage_from_stats(Thresholds, NodeDiskStats).

-spec get_high_disk_usage_from_stats(
        [{atom(), number()}],
        [{node(),
          {ok, ns_disksup:disk_stat()}
         | {badrpc, any()}
         | {error, disk_stats_error()}}]) ->
          [{node(), disk_severity() | {error, any()}}].
get_high_disk_usage_from_stats(Thresholds, NodeDiskStats) ->
    lists:filtermap(
      fun ({Node, {ok, DiskData}}) ->
              case check_disk_usage(Thresholds, DiskData) of
                  ok ->
                      false;
                  Severity ->
                      {true, {Node, Severity}}
              end;
          ({Node, {badrpc, Error} = E}) ->
              %% If there is a communication issue, or an error getting
              %% the disk stats, we want to bubble up a clear error,
              %% rather than letting it fail later in a less clear way
              ?log_error("Couldn't get disk stats for node ~p. Instead got ~w.",
                         [Node, Error]),
              {true, {Node, {error, E}}};
          ({Node, {error, _} = E}) ->
              {true, {Node, E}}
      end, NodeDiskStats).

-spec check_num_replicas_change(pos_integer(), pos_integer(), [node()]) ->
          ok | {error, binary()}.
check_num_replicas_change(OldNumReplicas, NewNumReplicas, Nodes) ->
    case {guardrail_monitor:get(disk_usage), OldNumReplicas < NewNumReplicas} of
        {undefined, _} ->
            ok;
        {_Thresholds, false} ->
            %% The number of replicas is not being increased so no guardrail
            %% needs to be checked, to ensure the change is safe to perform
            ok;
        {Thresholds, true} ->
            %% The number of replicas is being increased so we need to check
            %% the disk usage, to ensure the change is safe to perform
            PossiblyBadNodes = get_high_disk_usage_from_nodes(Thresholds,
                                                              Nodes),
            %% Exclude nodes that only cross the serious/critical thresholds,
            %% as these thresholds are ignored for num_replicas
            BadNodes = lists:filter(
                fun ({_Node, critical}) ->  false;
                    ({_Node, serious}) ->  false;
                    (_) -> true
                end, PossiblyBadNodes),
            %% Split the bad nodes into those with an error and those with
            %% critical disk usage
            {HighDiskNodes, ErrorDiskNodes} =
                lists:partition(
                  fun ({_Node, maximum}) -> true;
                      (_) -> false
                  end, BadNodes),
            case {HighDiskNodes, ErrorDiskNodes} of
                {[], []} ->
                    ok;
                {_, []} ->
                    {error,
                     list_to_binary(
                       io_lib:format(
                         "The following data node(s) have insufficient disk "
                         "space to safely increase the number of replicas: ~s",
                         [lists:join(
                            ", ",
                            lists:map(?cut(atom_to_list(element(1, _))),
                                      HighDiskNodes))]))};
                {[], _} ->
                    {error,
                     list_to_binary(
                       io_lib:format(
                         "Couldn't determine safety of increasing number of "
                         "replicas as there were errors getting disk usage on "
                         "the following nodes: ~s",
                         [lists:join(
                            ", ",
                            lists:map(?cut(atom_to_list(element(1, _))),
                                      ErrorDiskNodes))]))}
            end
    end.

-spec validate_storage_migration(bucket_name(), proplists:proplist(), atom()) ->
          ok | {error, data_size | resident_ratio, number()}.
validate_storage_migration(BucketName, BucketConfig, NewStorageMode) ->
    KvNodes = ns_bucket:get_servers(BucketConfig),
    Stats = stats_interface:for_storage_mode_migration(BucketName, KvNodes),
    case validate_storage_migration_data_size(Stats, NewStorageMode) of
        ok ->
            case validate_storage_migration_resident_ratio(Stats,
                                                           NewStorageMode) of
                ok ->
                    ok;
                {error, Limit} ->
                    {error, resident_ratio, Limit}
            end;
        {error, Limit} ->
            {error, data_size, Limit}
    end.

validate_storage_migration_data_size(Stats, StorageMode) ->
    case get(bucket, data_size) of
        undefined ->
            ok;
        ResourceConfig ->
            case maps:get(data_size, Stats, undefined) of
                undefined ->
                    ok;
                DataSize ->
                    Limit = get_data_size_maximum(ResourceConfig, StorageMode,
                                                  true),
                    case validate_resource_max(DataSize, Limit) of
                        false ->
                            ok;
                        true ->
                            {error, Limit}
                    end
            end
    end.

validate_storage_migration_resident_ratio(Stats, StorageMode) ->
    case get(bucket, resident_ratio) of
        undefined ->
            ok;
        ResourceConfig ->
            case maps:get(resident_ratio, Stats, undefined) of
                undefined ->
                    ok;
                ResidentRatio ->
                    Minimum = get_resident_ratio_minimum(ResourceConfig,
                                                         StorageMode, true),
                    case validate_resource_min(ResidentRatio, Minimum) of
                        false ->
                            ok;
                        true ->
                            {error, Minimum}
                    end
            end
    end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================


init([]) ->
    self() ! check,
    %% Initialise the statuses with whatever the enforcer currently sees, so
    %% that we correctly detect changes relative to that
    Statuses = ns_config:search_node_with_default(resource_statuses, []),
    {ok, #state{statuses = Statuses}}.

handle_call(_, _From, #state{} = State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(check, #state{statuses = OldStatuses} = State) ->
    %% Remind myself to check resource statuses again, after the check interval
    State1 = restart_timer(State),
    State2 =
        case is_enabled() of
            true ->
                NewStatuses = check_resources(),
                case OldStatuses == NewStatuses of
                    true ->
                        State1;
                    false ->
                        ?log_info("Resource statuses changed from ~p to ~p",
                                  [OldStatuses, NewStatuses]),
                        %% Report local and global statuses separately, to avoid
                        %% guardrail_enforcer trying to handle local statuses
                        {LocalStatuses, GlobalStatuses} =
                            lists:partition(
                              fun ({{index, _Resource}, _Status}) ->
                                      true;
                                  (_) ->
                                      false
                              end, NewStatuses),
                        ns_config:set({node, node(), resource_statuses},
                                      GlobalStatuses),
                        ns_config:set({node, node(), local_resource_statuses},
                                      LocalStatuses),
                        State1#state{statuses = NewStatuses}
                end;
            false ->
                State1
        end,
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% We need to make sure there is only one timer at any given moment, otherwise
%% the system would be fragile to future changes or diag/evals
restart_timer(#state{timer_ref = Ref} = State) when is_reference(Ref) ->
    erlang:cancel_timer(Ref),
    restart_timer(State#state{timer_ref = undefined});
restart_timer(#state{timer_ref = undefined} = State) ->
    State#state{timer_ref = erlang:send_after(?CHECK_INTERVAL, self(), check)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Checks all enabled resources and returns the status map
-spec check_resources() -> [{resource(), status()}].
check_resources() ->
    Config = get_config(),
    Stats = stats_interface:for_resource_management(),
    lists:flatmap(check(_, Stats), Config).

%% Checks if a resource threshold has been met, returning all the statuses for
%% that resource (for instance the status for each bucket)
check({bucket, Config}, Stats) ->
    lists:flatten(
      lists:map(
        fun ({BucketName, BucketConfig}) ->
                case proplists:get_value({bucket, BucketName}, Stats) of
                    undefined ->
                        [];
                    BucketStats ->
                        check_bucket(Config, BucketName, BucketConfig,
                                     BucketStats)
                end
        end, ns_bucket:get_buckets()));
check({disk_usage = Resource, Config}, _Stats) ->
    case get_thresholds(Config) of
        undefined ->
            [];
        Thresholds ->
            %% Get the live disk stats, which we do for consistency as we must
            %% get live stats for node addition, to avoid waiting for stats to
            %% be scraped on the new node
            Node = node(),
            {Severity, Statuses} =
                case get_high_disk_usage_from_nodes(Thresholds, [Node]) of
                    [{Node, {error, _Error}}] ->
                        %% If we fail to get disk stats then we assume the disk
                        %% usage is safe, rather than disabling data ingress.
                        %% We do this because disabling data ingress is an
                        %% extreme action that we do not want to take unless we
                        %% are sure that we have to.
                        {ok, []};
                    [{Node, maximum = S}] ->
                        %% For now if we see disk usage reach the limit we apply
                        %% the guard for all buckets. In future we should allow
                        %% this to apply on a per-service and per-bucket level,
                        %% when these are mapped to different disk partitions
                        {S,
                         [{{bucket, BucketName}, disk_usage}
                          || BucketName <- ns_bucket:get_bucket_names()] ++
                             [{{index, disk_usage}, S}]};
                    [{Node, S}] when S =/= ok ->
                        %% For now if we see disk usage reach the limit we apply
                        %% the guard for all buckets. In future we should allow
                        %% this to apply on a per-service and per-bucket level,
                        %% when these are mapped to different disk partitions
                        {S, [{{index, disk_usage}, S}]};
                    [] ->
                        {ok, []}
                end,
            lists:foreach(
              fun (SeverityToReport) ->
                      Gauge =
                          case Severity of
                              serious when SeverityToReport =:= serious ->
                                  1;
                              critical when SeverityToReport =/= maximum ->
                                  1;
                              maximum ->
                                  1;
                              _ ->
                                  0
                          end,
                      ns_server_stats:notify_gauge(
                        {<<"resource_limit_reached">>,
                         [{resource, Resource},
                          {severity, SeverityToReport}]},
                        Gauge)
              end,
              [maximum, critical, serious]),
            Statuses
    end;
check({index, IndexConfig}, Stats) ->
    case proplists:get_value(index, Stats) of
        undefined ->
            [];
        IndexStats ->
            lists:flatmap(
              fun ({index_growth_rr, ResourceConfig}) ->
                      check_index_resident_ratio(ResourceConfig, IndexStats);
                  (_) ->
                      []
              end, IndexConfig)
    end;
check({_Resource, _Config}, _Stats) ->
    %% Other resources do not need regular checks
    [].

check_bucket(Config, BucketName, BucketConfig, BucketStats) ->
    Results =
        lists:filtermap(
          fun (Resource) ->
                  ResourceConfig = proplists:get_value(Resource, Config),
                  Result =
                      case proplists:get_value(enabled, ResourceConfig) of
                          true ->
                              check_bucket_guard_rail(Resource, ResourceConfig,
                                                      BucketConfig,
                                                      BucketStats);
                          false ->
                              false
                      end,
                  NotifyStat = ns_server_stats:notify_gauge(
                                 {<<"resource_limit_reached">>,
                                  [{resource, Resource},
                                   {bucket, BucketName}]}, _),
                  case Result of
                      true ->
                          NotifyStat(1),
                          {true, Resource};
                      false ->
                          NotifyStat(0),
                          false
                  end
          end, proplists:get_keys(Config)),
    [{{bucket, BucketName}, Result} || Result <- Results].

check_bucket_guard_rail(Resource, ResourceConfig, BucketConfig, BucketStats) ->
    case proplists:get_value(Resource, BucketStats) of
        undefined ->
            false;
        Metric ->
            case Resource of
                resident_ratio ->
                    Limit = get_resident_ratio_minimum(ResourceConfig,
                                                       BucketConfig),
                    validate_resource_min(Metric, Limit);
                data_size ->
                    Limit = get_data_size_maximum(ResourceConfig, BucketConfig),
                    validate_resource_max(Metric, Limit)
            end
    end.

validate_resource_min(Metric, Limit) ->
    case Metric of
        %% Ignore infinity/neg_infinity as these are not meaningful here
        infinity ->
            false;
        neg_infinity ->
            false;
        Value ->
            Value < Limit
    end.

validate_resource_max(Metric, Limit) ->
    case Metric of
        %% Ignore infinity/neg_infinity as these are not meaningful here
        infinity ->
            false;
        neg_infinity ->
            false;
        Value ->
            %% Inclusive inequality so that when limit is 0 and value is 0, the
            %% guard rail still fires, which is useful for testing, and doesn't
            %% impact real world behaviour in a noticeable manner
            Value >= Limit
    end.

get_resident_ratio_minimum(ResourceConfig, BucketConfig) ->
    get_resident_ratio_minimum(
        ResourceConfig, ns_bucket:storage_mode(BucketConfig),
        ns_bucket:storage_mode_migration_in_progress(BucketConfig)).

get_resident_ratio_minimum(ResourceConfig, StorageMode, Migration) ->
    CouchstoreLimit = proplists:get_value(couchstore_minimum, ResourceConfig),
    MagmaLimit = proplists:get_value(magma_minimum, ResourceConfig),
    case {StorageMode, Migration} of
        {couchstore, false} ->
            CouchstoreLimit;
        {magma, false} ->
            MagmaLimit;
        {_, false} ->
            %% For memcached and ephemeral buckets, there is no limit
            -1;
        _ ->
            %% Always use the most restrictive storage mode if storage mode
            %% migration is in progress
            max(CouchstoreLimit, MagmaLimit)
    end.

get_resident_ratio_minimum_on_nodes(ResourceConfig, BucketConfig, Nodes) ->
    CouchstoreLimit = proplists:get_value(couchstore_minimum, ResourceConfig),
    MagmaLimit = proplists:get_value(magma_minimum, ResourceConfig),
    %% Use the most restrictive limit, only considering specific nodes
    case get_limits_from_node_storage_modes(BucketConfig, CouchstoreLimit,
                                            MagmaLimit, Nodes) of
        [] -> -1;
        Limits -> lists:max(Limits)
    end.

get_limits_from_node_storage_modes(BucketConfig, CouchstoreLimit, MagmaLimit,
                                   Nodes) ->
    lists:filtermap(
      fun (Node) ->
              case ns_bucket:node_storage_mode(Node, BucketConfig) of
                  couchstore -> {true, CouchstoreLimit};
                  magma -> {true, MagmaLimit};
                  %% memcached and ephemeral buckets are not considered
                  _ -> false
              end
      end, Nodes).

get_data_size_maximum(ResourceConfig, BucketConfig) ->
    get_data_size_maximum(
        ResourceConfig, ns_bucket:storage_mode(BucketConfig),
        ns_bucket:storage_mode_migration_in_progress(BucketConfig)).

get_data_size_maximum(ResourceConfig, StorageMode, Migration) ->
    CouchstoreLimit = proplists:get_value(couchstore_maximum, ResourceConfig),
    MagmaLimit = proplists:get_value(magma_maximum, ResourceConfig),
    case {StorageMode, Migration} of
        {couchstore, false} ->
            CouchstoreLimit;
        {magma, false} ->
            MagmaLimit;
        {_, false} ->
            %% For memcached and ephemeral buckets, there is no limit
            infinity;
        _ ->
            %% Always use the most restrictive storage mode if storage mode
            %% migration is in progress
            min(CouchstoreLimit, MagmaLimit)
    end.

get_data_size_maximum_on_nodes(ResourceConfig, BucketConfig, Nodes) ->
    CouchstoreLimit = proplists:get_value(couchstore_maximum, ResourceConfig),
    MagmaLimit = proplists:get_value(magma_maximum, ResourceConfig),
    %% Use the most restrictive limit, only considering specific nodes
    case get_limits_from_node_storage_modes(BucketConfig, CouchstoreLimit,
                                            MagmaLimit, Nodes) of
        [] -> infinity;
        Limits -> lists:min(Limits)
    end.

get_severity_for_thresholds(Resource, Thresholds, Metric, Order) ->
    %% Sort thresholds in ascending/descending order, such that we check the
    %% most severe threshold first
    Compare = case Order of
                  ascending -> fun(A, B) -> A < B end;
                  descending -> fun(A, B) -> A > B end
              end,
    ThresholdsSorted = lists:filtermap(
        fun (Severity) ->
            case proplists:get_value(Severity, Thresholds) of
                undefined -> false;
                Threshold -> {true, {Severity, Threshold}}
            end
        end, guardrail_enforcer:priority_order(Resource)),
    lists:foldl(
      fun ({Severity, Threshold}, ok) ->
              case Compare(Metric, Threshold) of
                  true -> Severity;
                  false -> ok
              end;
          (_, NotOk) ->
              NotOk
      end, ok, ThresholdsSorted).

-spec check_disk_usage([{atom(), number()}], ns_disksup:disk_stat()) ->
    ok | disk_severity().
check_disk_usage(Thresholds, {_Disk, _Cap, Used}) ->
    get_severity_for_thresholds(index, Thresholds, Used, descending).

-spec get_disk_data(ns_disksup:disk_stats()) ->
          {ok, ns_disksup:disk_stat()} | {error, disk_stats_error()}.
get_disk_data(Mounts) ->
    case ns_storage_conf:this_node_dbdir() of
        {ok, DbDir} ->
            case misc:realpath(DbDir, "/") of
                {ok, RealFile} ->
                    case ns_storage_conf:extract_disk_stats_for_path(
                           Mounts, RealFile) of
                        {ok, _} = DiskData ->
                            DiskData;
                        none ->
                            ?log_error("Couldn't check disk space as ~p "
                                       "wasn't found in ~p",
                                       [RealFile, Mounts]),
                            {error, no_disk_stats_found}
                    end;
                Error ->
                    ?log_error("Couldn't check disk space as ~p doesn't "
                               "appear to be a valid path. Error: ~p",
                               [DbDir, Error]),
                    {error, {dbdir_path_error, Error}}
            end;
        {error, not_found} ->
            ?log_error("Couldn't check disk usage as node db dir is missing"),
            {error, no_dbdir}
    end.

get_thresholds(Config) ->
    case proplists:get_value(enabled, Config) of
        true ->
            %% Just return the rest of the config
            lists:keydelete(enabled, 1, Config);
        false ->
            undefined
    end.

check_index_resident_ratio(Config, Stats) ->
    case get_thresholds(Config) of
        undefined ->
            [];
        Thresholds ->
            Severity = get_index_resident_ratio_severity(Thresholds, Stats),
            lists:foreach(
              fun (SeverityToReport) ->
                      Gauge =
                          case Severity of
                              warning when SeverityToReport =:= warning ->
                                  1;
                              serious when SeverityToReport =/= critical ->
                                  1;
                              critical ->
                                  1;
                              _ ->
                                  0
                          end,
                      ns_server_stats:notify_gauge(
                        {<<"resource_limit_reached">>,
                         [{resource, index_resident_ratio},
                          {severity, SeverityToReport}]},
                        Gauge)
              end,
              [critical, serious, warning]),
            case Severity of
                ok ->
                    [];
                _ ->
                    [{{index, resident_ratio}, Severity}]
            end
    end.

get_index_resident_ratio_severity(Thresholds, Stats) ->
    case proplists:get_value(resident_ratio, Stats) of
        undefined ->
            ok;
        Metric ->
            get_severity_for_thresholds(index, Thresholds, Metric, ascending)
    end.

-ifdef(TEST).

basic_test_setup() ->
    %% We need unstick to be able to meck rpc
    meck:new([rpc], [unstick]),
    meck:new([ns_server_stats], [passthrough]),

    meck:expect(cluster_compat_mode, is_cluster_76, ?cut(true)),
    meck:expect(config_profile, get_bool,
                fun ({resource_management, enabled}) -> true end),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) -> Default end).

basic_test_teardown() ->
    meck:unload().

check_bucket_t() ->
    %% Resource level check
    RRConfig = [{enabled, true},
                {couchstore_minimum, 10},
                {magma_minimum, 1}],
    CouchstoreBucket = [{type, membase},
                        {storage_mode, couchstore}],

    %% RR% above minimum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, CouchstoreBucket,
                   [{resident_ratio, 15}])),

    %% RR% at minimum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, CouchstoreBucket,
                   [{resident_ratio, 10}])),

    %% RR% below minimum
    ?assertEqual(true,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, CouchstoreBucket,
                   [{resident_ratio, 5}])),

    DataSizeConfig = [{enabled, true},
                      {couchstore_maximum, 1.6},
                      {magma_maximum, 16}],

    %% Data size below maximum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   data_size, DataSizeConfig, CouchstoreBucket,
                   [{data_size, 1}])),

    %% Data size above maximum
    ?assertEqual(true,
                 check_bucket_guard_rail(
                   data_size, DataSizeConfig, CouchstoreBucket,
                   [{data_size, 2}])),

    MagmaBucket = [{type, membase},
                   {storage_mode, magma}],

    %% Bucket level check
    Config = [{resident_ratio, RRConfig},
              {data_size, DataSizeConfig}],


    %% RR% above couchstore minimum
    ?assertEqual(
       [],
       check_bucket(Config, "couchstore", CouchstoreBucket,
                    [{resident_ratio, 15}])),
    %% RR% below couchstore minimum
    ?assertEqual(
       [{{bucket, "couchstore"}, resident_ratio}],
       check_bucket(Config, "couchstore", CouchstoreBucket,
                    [{resident_ratio, 5}])),

    %% RR% above magma minimum
    ?assertEqual(
       [],
       check_bucket(Config, "magma", MagmaBucket,
                    [{resident_ratio, 2}])),
    %% RR% below magma minimum
    ?assertEqual(
       [{{bucket, "magma"}, resident_ratio}],
       check_bucket(Config, "magma", MagmaBucket,
                    [{resident_ratio, 0.5}])),

    %% Data size below couchstore maximum
    ?assertEqual(
       [],
       check_bucket(Config, "couchstore", CouchstoreBucket,
                    [{data_size, 1}])),
    %% Data size above couchstore maximum
    ?assertEqual(
       [{{bucket, "couchstore"}, data_size}],
       check_bucket(Config, "couchstore", CouchstoreBucket,
                    [{data_size, 5}])),

    %% Data size below magma maximum
    ?assertEqual(
       [],
       check_bucket(Config, "magma", MagmaBucket,
                    [{data_size, 5}])),
    %% Data size above magma maximum
    ?assertEqual(
       [{{bucket, "magma"}, data_size}],
       check_bucket(Config, "magma", MagmaBucket,
                    [{data_size, 20}])),

    %% Service level check
    meck:expect(ns_bucket, get_buckets,
                fun () ->
                        [{"couchstore", CouchstoreBucket},
                         {"magma", MagmaBucket}]
                end),
    %% RR% above couchstore minimum
    ?assertListsEqual(
       [],
       check({bucket, Config},
             [{{bucket, "couchstore"}, [{resident_ratio, 15}]}])),
    %% RR% below couchstore minimum
    ?assertListsEqual(
       [{{bucket, "couchstore"}, resident_ratio}],
       check({bucket, Config},
             [{{bucket, "couchstore"}, [{resident_ratio, 5}]}])),

    %% RR% above magma minimum
    ?assertListsEqual(
       [],
       check({bucket, Config}, [{{bucket, "magma"}, [{resident_ratio, 2}]}])),
    %% RR% below magma minimum
    ?assertListsEqual(
       [{{bucket, "magma"}, resident_ratio}],
       check({bucket, Config}, [{{bucket, "magma"}, [{resident_ratio, 0.5}]}])),

    %% Data size below couchstore maximum
    ?assertListsEqual(
       [],
       check({bucket, Config}, [{{bucket, "couchstore"}, [{data_size, 1}]}])),
    %% Data size above couchstore maximum
    ?assertListsEqual(
       [{{bucket, "couchstore"}, data_size}],
       check({bucket, Config}, [{{bucket, "couchstore"}, [{data_size, 5}]}])),

    %% Data size below magma maximum
    ?assertListsEqual(
       [],
       check({bucket, Config}, [{{bucket, "magma"}, [{data_size, 5}]}])),
    %% Data size above magma maximum
    ?assertListsEqual(
       [{{bucket, "magma"}, data_size}],
       check({bucket, Config}, [{{bucket, "magma"}, [{data_size, 20}]}])),

    %% Have bucket stats but don't have bucket
    ?assertListsEqual(
       [],
       check({bucket, Config}, [{{bucket, "other"}, [{data_size, 20}]}])),
    ok.

check_bucket_during_storage_migration_t() ->
    RRConfig = [{enabled, true},
                {couchstore_minimum, 10},
                {magma_minimum, 1}],
    CouchstoreToMagmaBucket = [{type, membase},
                               {storage_mode, magma},
                               %% Migration from couchstore in progress
                               {{node, node1, storage_mode}, couchstore}],

    %% Bucket being migrated from couchstore to magma should continue to be
    %% treated as a couchstore bucket during migration

    %% RR% above minimum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, CouchstoreToMagmaBucket,
                   [{resident_ratio, 15}])),

    %% RR% at minimum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, CouchstoreToMagmaBucket,
                   [{resident_ratio, 10}])),

    %% RR% below minimum
    ?assertEqual(true,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, CouchstoreToMagmaBucket,
                   [{resident_ratio, 5}])),

    DataSizeConfig = [{enabled, true},
                      {couchstore_maximum, 1.6},
                      {magma_maximum, 16}],

    %% Data size below maximum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   data_size, DataSizeConfig, CouchstoreToMagmaBucket,
                   [{data_size, 1}])),

    %% Data size above maximum
    ?assertEqual(true,
                 check_bucket_guard_rail(
                   data_size, DataSizeConfig, CouchstoreToMagmaBucket,
                   [{data_size, 2}])),

    MagmaToCouchstoreBucket = [{type, membase},
                               {storage_mode, couchstore},
                               %% Migration from magma in progress
                               {{node, node1, storage_mode}, magma}],

    %% Bucket being migrated from magma to couchstore should immediately be
    %% treated the same as a couchstore bucket

    %% RR% above minimum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, MagmaToCouchstoreBucket,
                   [{resident_ratio, 15}])),

    %% RR% at minimum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, MagmaToCouchstoreBucket,
                   [{resident_ratio, 10}])),

    %% RR% below minimum
    ?assertEqual(true,
                 check_bucket_guard_rail(
                   resident_ratio, RRConfig, MagmaToCouchstoreBucket,
                   [{resident_ratio, 5}])),

    %% Data size below maximum
    ?assertEqual(false,
                 check_bucket_guard_rail(
                   data_size, DataSizeConfig, MagmaToCouchstoreBucket,
                   [{data_size, 1}])),

    %% Data size above maximum
    ?assertEqual(true,
                 check_bucket_guard_rail(
                   data_size, DataSizeConfig, MagmaToCouchstoreBucket,
                   [{data_size, 2}])),
                   ok.

dont_check_memcached_or_ephemeral_t() ->
    MemcachedBucket = [{type, memcached},
                       {storage_mode, undefined}],

    EphemeralBucket = [{type, membase},
                       {storage_mode, ephemeral}],

    %% Bucket level check

    meck:expect(ns_bucket, get_buckets,
                fun () ->
                        [{"memcached", MemcachedBucket},
                         {"ephemeral", EphemeralBucket}]
                end),
    RRConfig = [{enabled, true},
                {couchstore_minimum, 10},
                {magma_minimum, 1}],
    DataSizeConfig = [{enabled, true},
                      {couchstore_maximum, 1.6},
                      {magma_maximum, 16}],
    Config = [{resident_ratio, RRConfig},
              {data_size, DataSizeConfig}],


    %% Low memcached RR% (should be impossible, but make sure this isn't checked
    %% anyway
    ?assertEqual(
       [],
       check_bucket(Config, "memcached", MemcachedBucket,
                    [{resident_ratio, 0}])),

    %% Low ephemeral RR% (should be impossible, but make sure this isn't checked
    %% anyway
    ?assertEqual(
       [],
       check_bucket(Config, "ephemeral", MemcachedBucket,
                    [{resident_ratio, 0}])),

    %% High memcached data size
    ?assertEqual(
       [],
       check_bucket(Config, "memcached", MemcachedBucket,
                    [{data_size, 100}])),

    %% High ephemeral data size
    ?assertEqual(
       [],
       check_bucket(Config, "ephemeral", MemcachedBucket,
                    [{data_size, 100}])),

    %% Topology check setup

    Servers = [node1, node2],
    DesiredServers = [{"memcached", Servers},
                      {"ephemeral", Servers}],
    meck:expect(ns_bucket, get_bucket,
                fun ("memcached") ->
                        {ok, [{ram_quota, 10} | MemcachedBucket]};
                    ("ephemeral") ->
                        {ok, [{ram_quota, 10} | EphemeralBucket]};
                    (_) ->
                        not_present
                end),

    %% Check topology change doesn't care about memcached
    ?assertEqual(false,
                 maybe_validate_bucket_topology_change(
                     resident_ratio, "memcached", Servers, DesiredServers, 1000,
                     RRConfig)),

    %% Check topology change doesn't care about ephemeral
    ?assertEqual(false,
                 maybe_validate_bucket_topology_change(
                     resident_ratio, "ephemeral", Servers, DesiredServers, 1000,
                     RRConfig)),
    ok.

-define(assertResourceMetrics(ResourceMap),
        begin
            maps:foreach(
              fun (__Labels, __Gauge) ->
                      ?assertEqual(1,
                                   meck:num_calls(
                                     ns_server_stats, notify_gauge,
                                     [{<<"resource_limit_reached">>, __Labels},
                                      __Gauge]),
                                   [{labels, __Labels},
                                    {gauge, __Gauge}])
              end, ResourceMap),
            meck:reset(ns_server_stats)
        end).

check_resources_t() ->
    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{bucket,
                          [{resident_ratio,
                            [{enabled, false},
                             {couchstore_minimum, 10},
                             {magma_minimum, 1}]}]}]
                end),
    CouchstoreBucket = [{type, membase},
                        {storage_mode, couchstore}],
    MagmaBucket = [{type, membase},
                   {storage_mode, magma}],
    meck:expect(ns_bucket, get_buckets,
                fun () ->
                        [{"couchstore_bucket", CouchstoreBucket},
                         {"magma_bucket", MagmaBucket}]
                end),
    meck:expect(index_settings_manager, get,
                fun (guardrails) ->
                        [{index_creation_rr,
                          [{enabled, false},
                           {minimum, 10}]},
                         {topology_change_rr,
                          [{enabled, false},
                           {minimum, 10}]}]
                end),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        []
                end),
    ?assertEqual([],
                 check_resources()),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"}, []}]
                end),
    ?assertEqual([],
                 check_resources()),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 11}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 2}]}]
                end),
    %% Reset meck history for ns_server_stats so we can track calls from now on
    meck:reset(ns_server_stats),
    ?assertEqual([],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 0}),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 9},
                           {data_size, 2}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 0.5},
                           {data_size, 20}]}]
                end),
    ?assertEqual([],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 0}),

    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{bucket,
                          [{resident_ratio,
                            [{enabled, true},
                             {couchstore_minimum, 10},
                             {magma_minimum, 1}]},
                           {data_size,
                            [{enabled, true},
                             {couchstore_maximum, 1.6},
                             {magma_maximum, 16}]}]}]
                end),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 9}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 2}]}]
                end),
    ?assertEqual([{{bucket, "couchstore_bucket"}, resident_ratio}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 1,
         [{resource, data_size}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 0,
         [{resource, data_size}, {bucket, "magma_bucket"}] => 0}),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 11}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 0.5}]}]
                end),
    ?assertEqual([{{bucket, "magma_bucket"}, resident_ratio}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, data_size}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 1,
         [{resource, data_size}, {bucket, "magma_bucket"}] => 0}),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 9}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 0.5}]}]
                end),
    ?assertEqual([{{bucket, "couchstore_bucket"}, resident_ratio},
                  {{bucket, "magma_bucket"}, resident_ratio}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 1,
         [{resource, data_size}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 1,
         [{resource, data_size}, {bucket, "magma_bucket"}] => 0}),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{data_size, 2}]},
                         {{bucket, "magma_bucket"},
                          [{data_size, 20}]}]
                end),
    ?assertEqual([{{bucket, "couchstore_bucket"}, data_size},
                  {{bucket, "magma_bucket"}, data_size}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, data_size}, {bucket, "couchstore_bucket"}] => 1,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 0,
         [{resource, data_size}, {bucket, "magma_bucket"}] => 1}),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 11},
                           {data_size, 2}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 2},
                           {data_size, 20}]}]
                end),
    ?assertEqual([{{bucket, "couchstore_bucket"}, data_size},
                  {{bucket, "magma_bucket"}, data_size}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 0,
         [{resource, data_size}, {bucket, "couchstore_bucket"}] => 1,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 0,
         [{resource, data_size}, {bucket, "magma_bucket"}] => 1}),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 9},
                           {data_size, 2}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 0.5},
                           {data_size, 20}]}]
                end),
    ?assertListsEqual([{{bucket, "couchstore_bucket"}, resident_ratio},
                       {{bucket, "couchstore_bucket"}, data_size},
                       {{bucket, "magma_bucket"}, resident_ratio},
                       {{bucket, "magma_bucket"}, data_size}],
                      check_resources()),
    ?assertResourceMetrics(
       #{[{resource, resident_ratio}, {bucket, "couchstore_bucket"}] => 1,
         [{resource, data_size}, {bucket, "couchstore_bucket"}] => 1,
         [{resource, resident_ratio}, {bucket, "magma_bucket"}] => 1,
         [{resource, data_size}, {bucket, "magma_bucket"}] => 1}),

    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{disk_usage,
                          [{enabled, true},
                           {maximum, 96},
                           {critical, 85},
                           {serious, 80}]}]
                end),
    pretend_disk_data(#{node() => [{"/", 1, 50}]}),

    meck:expect(ns_storage_conf, this_node_dbdir,
                fun () -> {ok, "invalid_file"} end),

    meck:expect(ns_storage_conf, extract_disk_stats_for_path,
                fun (_, _) -> none end),

    meck:expect(ns_bucket, get_bucket_names,
                fun () -> ["couchstore_bucket", "magma_bucket"] end),

    ?assertEqual([], check_resources()),
    ?assertResourceMetrics(
       #{[{resource, disk_usage},
          {severity, serious}] => 0,
         [{resource, disk_usage},
          {severity, critical}] => 0,
         [{resource, disk_usage},
          {severity, maximum}] => 0}),

    meck:expect(ns_storage_conf, this_node_dbdir,
                fun () -> {ok, ""} end),

    ?assertEqual([], check_resources()),
    ?assertResourceMetrics(
       #{[{resource, disk_usage},
          {severity, serious}] => 0,
         [{resource, disk_usage},
          {severity, critical}] => 0,
         [{resource, disk_usage},
          {severity, maximum}] => 0}),

    meck:expect(ns_storage_conf, extract_disk_stats_for_path,
                fun ([Value], _) -> {ok, Value} end),

    ?assertEqual([], check_resources()),
    ?assertResourceMetrics(
       #{[{resource, disk_usage},
          {severity, serious}] => 0,
         [{resource, disk_usage},
          {severity, critical}] => 0,
         [{resource, disk_usage},
          {severity, maximum}] => 0}),

    pretend_disk_data(#{node() => [{"/", 1, 97}]}),

    ?assertEqual([{{bucket, "couchstore_bucket"}, disk_usage},
                  {{bucket, "magma_bucket"}, disk_usage},
                  {{index, disk_usage}, maximum}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, disk_usage},
          {severity, serious}] => 1,
         [{resource, disk_usage},
          {severity, critical}] => 1,
         [{resource, disk_usage},
          {severity, maximum}] => 1}),

    pretend_disk_data(#{node() => [{"/", 1, 86}]}),

    ?assertEqual([{{index, disk_usage}, critical}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, disk_usage},
          {severity, serious}] => 1,
         [{resource, disk_usage},
          {severity, critical}] => 1,
         [{resource, disk_usage},
          {severity, maximum}] => 0}),

    pretend_disk_data(#{node() => [{"/", 1, 81}]}),

    ?assertEqual([{{index, disk_usage}, serious}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, disk_usage},
          {severity, serious}] => 1,
         [{resource, disk_usage},
          {severity, critical}] => 0,
         [{resource, disk_usage},
          {severity, maximum}] => 0}),

    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{index,
                          [{index_growth_rr,
                            [{enabled, true},
                             {critical, 1},
                             {serious, 5},
                             {warning, 10}]}]
                         }]
                end),

    PretendIndexRR =
        fun (RR) ->
                meck:expect(stats_interface, for_resource_management,
                            fun () ->
                                    [{index,
                                      [{resident_ratio, RR}]}]
                            end)
        end,

    PretendIndexRR(10),

    ?assertEqual([], check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_resident_ratio},
          {severity, warning}] => 0,
         [{resource, index_resident_ratio},
          {severity, serious}] => 0,
         [{resource, index_resident_ratio},
          {severity, critical}] => 0}),

    PretendIndexRR(9),

    ?assertEqual([{{index, resident_ratio}, warning}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_resident_ratio},
          {severity, warning}] => 1,
         [{resource, index_resident_ratio},
          {severity, serious}] => 0,
         [{resource, index_resident_ratio},
          {severity, critical}] => 0}),

    PretendIndexRR(4),

    ?assertEqual([{{index, resident_ratio}, serious}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_resident_ratio},
          {severity, warning}] => 1,
         [{resource, index_resident_ratio},
          {severity, serious}] => 1,
         [{resource, index_resident_ratio},
          {severity, critical}] => 0}),

    PretendIndexRR(0.5),

    ?assertEqual([{{index, resident_ratio}, critical}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_resident_ratio},
          {severity, warning}] => 1,
         [{resource, index_resident_ratio},
          {severity, serious}] => 1,
         [{resource, index_resident_ratio},
          {severity, critical}] => 1}),
    ok.

validate_bucket_topology_change_t() ->

    Servers = [node1, node2],
    RRConfig = [{couchstore_minimum, 10},
                {magma_minimum, 1}],
    meck:expect(ns_bucket, get_bucket,
                fun ("couchstore_bucket") ->
                        {ok, [{ram_quota, 10},
                              {type, membase},
                              {storage_mode, couchstore}]};
                    ("magma_bucket") ->
                        {ok, [{ram_quota, 10},
                              {type, membase},
                              {storage_mode, magma}]};
                    (_) ->
                        not_present
                end),

    meck:expect(ns_cluster_membership, service_active_nodes,
                fun (kv) -> Servers end),

    %% Resident ratio will end up just below the couchstore minimum
    ?assertEqual(true,
                 maybe_validate_bucket_topology_change(
                   resident_ratio,
                   "couchstore_bucket",
                   [node3 | Servers],
                   Servers,
                   201,  %% Bytes
                   RRConfig)),

    %% Resident ratio will end up just below the magma minimum
    ?assertEqual(true,
                 maybe_validate_bucket_topology_change(
                   resident_ratio,
                   "magma_bucket",
                   [node3 | Servers],
                   Servers,
                   2001,  %% Bytes
                   RRConfig)),

    %% Resident ratio will end up exactly at the couchstore minimum
    ?assertEqual(false,
                 maybe_validate_bucket_topology_change(
                   resident_ratio,
                   "couchstore_bucket",
                   [node3 | Servers],
                   Servers,
                   200,  %% Bytes
                   RRConfig)),

    %% Resident ratio will end up exactly at the magma minimum
    ?assertEqual(false,
                 maybe_validate_bucket_topology_change(
                   resident_ratio,
                   "magma_bucket",
                   [node3 | Servers],
                   Servers,
                   2000,  %% Bytes
                   RRConfig)),

    DataSizeConfig = [{couchstore_maximum, 0.000000001},  %% 1,000 Bytes
                      {magma_maximum,      0.00000001}],  %% 10,000 Bytes
    ?assertEqual(true,
                 maybe_validate_bucket_topology_change(
                   data_size,
                   "couchstore_bucket",
                   [node3 | Servers],
                   Servers,
                   2000,  %% Bytes
                   DataSizeConfig)),

    ?assertEqual(true,
                 maybe_validate_bucket_topology_change(
                   data_size,
                   "magma_bucket",
                   [node3 | Servers],
                   Servers,
                   20000,  %% Bytes
                   DataSizeConfig)),

    ?assertEqual(true,
                 maybe_validate_bucket_topology_change(
                   data_size,
                   "couchstore_bucket",
                   [node3 | Servers],
                   Servers,
                   2000,  %% Bytes
                   DataSizeConfig)),

    ?assertEqual(false,
                 maybe_validate_bucket_topology_change(
                   data_size,
                   "couchstore_bucket",
                   [node3 | Servers],
                   Servers,
                   1999,  %% Bytes
                   DataSizeConfig)),

    ?assertEqual(false,
                 maybe_validate_bucket_topology_change(
                   data_size,
                   "magma_bucket",
                   [node3 | Servers],
                   Servers,
                   19999,  %% Bytes
                   DataSizeConfig)),

    ?assertEqual(false,
                 maybe_validate_bucket_topology_change(
                   data_size,
                   "couchstore_bucket",
                   [node3 | Servers],
                   Servers,
                   1999,  %% Bytes
                   DataSizeConfig)),

    meck:expect(ns_bucket, get_bucket,
                fun ("couchstore_bucket") ->
                        {ok, [{ram_quota, 10},
                              {type, membase},
                              {storage_mode, couchstore},
                              {{node, node1, storage_mode}, magma}]};
                    ("magma_bucket") ->
                        {ok, [{ram_quota, 10},
                              {type, membase},
                              {storage_mode, magma},
                              {{node, node1, storage_mode}, couchstore}]};
                    (_) ->
                        not_present
                end),
    %% Test each storage migration scenario for both resident_ratio and
    %% data_size guardrails
    lists:foreach(
      fun ({Resource, Bucket, DataSize, Config, Result} = TestCase) ->
              ?assertEqual(Result,
                           maybe_validate_bucket_topology_change(
                             Resource,
                             Bucket,
                             Servers,
                             Servers,
                             DataSize,  %% Bytes
                             Config), TestCase)
      end,
      [{resident_ratio, "couchstore_bucket", 200, RRConfig, false},
       {resident_ratio, "magma_bucket", 200, RRConfig, false},
       {resident_ratio, "couchstore_bucket", 201, RRConfig, true},
       {resident_ratio, "magma_bucket", 201, RRConfig, true},
       {data_size, "couchstore_bucket", 1999, DataSizeConfig, false},
       {data_size, "magma_bucket", 1999, DataSizeConfig, false},
       {data_size, "couchstore_bucket", 2000, DataSizeConfig, true},
       {data_size, "magma_bucket", 2000, DataSizeConfig, true}]).

test_validate_topology_change(#{active_nodes := ActiveNodes,
                                keep_nodes := KeepNodes,
                                kv_nodes := KVNodes}) ->
    meck:expect(ns_cluster_membership, service_nodes,
                fun (Servers, kv) ->
                        [Node || Node <- Servers, lists:member(Node, KVNodes)]
                end),
    meck:expect(ns_cluster_membership, active_nodes,
                fun () -> ActiveNodes end),
    validate_topology_change(ActiveNodes -- KeepNodes, KeepNodes).

validate_topology_change_data_grs_t() ->
    RRResourceConfig0 = [{couchstore_minimum, 10},
                         {magma_minimum, 1}],
    RRResourceConfig1 = [{enabled, false} | RRResourceConfig0],

    DataSizeResourceConfig0 =
        [{couchstore_maximum, 0.000000001},  %% 1,000 Bytes
         {magma_maximum,      0.00000001}],  %% 10,000 Bytes
    DataSizeResourceConfig1 = [{enabled, false} | DataSizeResourceConfig0],

    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{bucket,
                          [{resident_ratio, RRResourceConfig1},
                           {data_size, DataSizeResourceConfig1}]}]
                end),
    %% 3 buckets, each with a quota of 10 bytes
    meck:expect(ns_bucket, get_bucket,
                fun ("couchstore_bucket") ->
                        {ok, [{ram_quota, 10},
                              {type, membase},
                              {storage_mode, couchstore}]};
                    ("magma_bucket") ->
                        {ok, [{ram_quota, 10},
                              {type, membase},
                              {storage_mode, magma}]};
                    ("new") ->
                        {ok, [{ram_quota, 10},
                              {type, membase},
                              {storage_mode, couchstore}]};
                    (_) ->
                        not_present
                end),
    %% For 3 nodes, with a total of 200/2000 bytes of data for couchstore and
    %% magma respectively, giving a RR% of 15/1.5% respectively, then to remove
    %% a node would give a RR% of 10/1%. Taking data size slightly larger than
    %% that, 201/2001, ensures that a rebalance from 3 to 2 nodes should only
    %% just violate the guardrail
    meck:expect(stats_interface, total_active_logical_data_size,
                fun (_) -> #{"couchstore_bucket" => 201,
                             "magma_bucket" => 2001} end),

    %% Don't give an error ejecting a node when the guard rail is disabled
    ?assertEqual(ok, test_validate_topology_change(
                       #{active_nodes => [node1, node2, node3],
                         keep_nodes => [node1, node2],
                         kv_nodes => [node1, node2, node3]})),

    RRResourceConfig2 = [{enabled, true} | RRResourceConfig0],
    DataSizeResourceConfig2 = [{enabled, true} | DataSizeResourceConfig0],
    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{bucket,
                          [{resident_ratio, RRResourceConfig2},
                           {data_size, DataSizeResourceConfig2}]}]
                end),

    %% Error when just ejecting a live node
    ?assertMatch({error,
                  {rr_will_be_too_low,
                   <<"The following buckets are expected to breach the "
                     "resident ratio minimum: couchstore_bucket, "
                     "magma_bucket.">>}},
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2, node3],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2, node3]})),

    %% Don't give an error when no nodes are being ejected
    ?assertEqual(ok, test_validate_topology_change(
                       #{active_nodes => [node1, node2],
                         keep_nodes => [node1, node2],
                         kv_nodes => [node1, node2]})),

    %% Don't give an error when a node is being added
    ?assertEqual(ok, test_validate_topology_change(
                       #{active_nodes => [node1],
                         keep_nodes => [node1, node2],
                         kv_nodes => [node1, node2]})),

    %% Don't give an error when ejecting a node while adding another
    ?assertMatch(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2],
                     keep_nodes => [node1, node3],
                     kv_nodes => [node1, node2, node3]})),

    %% For 3 nodes, with a total of 200/2000 bytes of data for couchstore and
    %% magma respectively, giving a RR% of 15/1.5% respectively, so to remove a
    %% node would just remain within the limits (10/1%)
    meck:expect(stats_interface, total_active_logical_data_size,
                fun (_) -> #{"couchstore_bucket" => 200,
                             "magma_bucket" => 2000,
                             %% Ignored as size is 0
                             "new" => 0,
                             %% Ignored as the bucket name is not found with
                             %% ns_bucket:get_bucket/1
                             "deleted2" => 4000} end),

    %% Don't give an error when ejecting a node if the resident ratio is not too
    %% low
    ?assertMatch(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2, node3],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2, node3]})),

    meck:expect(stats_interface, total_active_logical_data_size,
                fun (_) -> #{"couchstore_bucket" => 2001,
                             "magma_bucket" => 20001} end),
    ?assertMatch({error,
                  {data_size_will_be_too_high,
                   <<"The following buckets are expected to breach the maximum "
                     "data size per node: couchstore_bucket, magma_bucket.">>}},
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2, node3],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2, node3]})),

    %% Ignore non-kv nodes
    ?assertMatch(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2, node3],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2]})),

    meck:expect(stats_interface, total_active_logical_data_size,
                fun (_) -> #{} end),

    %% If no data size can be found for the bucket, don't give an error
    ?assertMatch(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2, node3],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2]})),
    ok.

pretend_cpu_cores_available(Node, Value) ->
    meck:expect(rpc, call,
                [Node, sigar, get_gauges, [[cpu_cores_available]], 60000],
                Value).

validate_topology_change_cores_per_bucket_t() ->
    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{cores_per_bucket,
                          [{enabled, true},
                           {minimum, 0.5}]}]
                end),
    meck:expect(ns_bucket, get_bucket_names,
                fun () -> ["bucket1", "bucket2"] end),
    %% For 2 buckets and 0.5 cores per bucket, the minimum will be 1

    %% Core count at minimum
    pretend_cpu_cores_available(node1, [{cpu_cores_available, 1}]),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [],
                     kv_nodes => [node1, node2, node3]})),

    %% Core count below minimum
    pretend_cpu_cores_available(node2, [{cpu_cores_available, 0.9}]),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node2],
                     keep_nodes => [],
                     kv_nodes => [node1, node2, node3]})),
    ?assertMatch({error,
                  {not_enough_cores_for_num_buckets, _}},
                 test_validate_topology_change(
                   #{active_nodes => [],
                     keep_nodes => [node2],
                     kv_nodes => [node1, node2, node3]})),
    ?assertMatch({error,
                  {not_enough_cores_for_num_buckets, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})),

    %% Ignore non-kv nodes
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node3],
                     kv_nodes => [node1, node2]})),

    %% Get 0 cpu cores from sigar, which we treat as invalid
    pretend_cpu_cores_available(node1, [{cpu_cores_available, 0.0}]),
    ?assertMatch({error,
                  {not_enough_cores_for_num_buckets, _}},
                 test_validate_topology_change(
                   #{active_nodes => [],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})),

    %% Get unexpected cpu cores from sigar
    pretend_cpu_cores_available(node1, error),
    ?assertMatch({error,
                  {not_enough_cores_for_num_buckets, _}},
                 test_validate_topology_change(
                   #{active_nodes => [],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})).

pretend_disk_data(DiskDataMap) ->
    meck:expect(rpc, call,
                fun (Node, ns_disksup, get_disk_data, [], _Timeout) ->
                        maps:get(Node, DiskDataMap)
                end).

validate_topology_change_disk_usage_t() ->
    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{disk_usage,
                          [{enabled, true},
                           {maximum, 50},
                           {critical, 40},
                           {serious, 30}]}]
                end),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) -> Default end),

    pretend_disk_data(#{node1 => {badrpc, nodedown}}),

    %% Test case where we fail to get disk stats
    ?assertMatch({error, {disk_usage_error, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [],
                     kv_nodes => [node1, node2, node3]})),

    %% Set disk data over limit, to cover high disk usage case
    pretend_disk_data(#{node1 => [{"/", 1, 51}],
                        node2 => [{"/", 1, 51}]}),

    %% Test case where we fail to extract disk stats for the data directory
    meck:expect(ns_storage_conf, this_node_dbdir,
                fun () -> {ok, "invalid_file"} end),

    ?assertMatch({error, {disk_usage_error, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [],
                     kv_nodes => [node1, node2, node3]})),

    meck:expect(ns_storage_conf, this_node_dbdir,
                fun () -> {ok, ""} end),


    meck:expect(ns_storage_conf, extract_disk_stats_for_path,
                fun (_, _) -> none end),

    %% Test case where we fail to get disk stats
    ?assertMatch({error, {disk_usage_error, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [],
                     kv_nodes => [node1, node2, node3]})),

    meck:expect(ns_storage_conf, extract_disk_stats_for_path,
                fun ([Value], _) -> {ok, Value}
                end),

    %% Permit all rebalances when disk usage is at the limit
    pretend_disk_data(#{node1 => [{"/", 1, 50}],
                        node2 => [{"/", 1, 50}]}),

    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node2],
                     keep_nodes => [],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node2],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2, node3]})),

    %% Permit rebalance despite high disk usage when there is no reduction in
    %% the number of data nodes
    pretend_disk_data(#{node1 => [{"/", 1, 51}],
                        node2 => [{"/", 1, 51}]}),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node1, node2],
                     kv_nodes => [node1, node2, node3]})),
    ?assertEqual(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node2],
                     kv_nodes => [node1, node2, node3]})),

    %% Don't permit rebalance when there is a reduction in the number of data
    %% nodes and the disk usage is above the limit
    ?assertMatch({error, {disk_usage_too_high, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [],
                     kv_nodes => [node1, node2, node3]})),
    ?assertMatch({error, {disk_usage_too_high, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})),

    %% Don't permit rebalance when there is a reduction in total disk size
    pretend_disk_data(#{node1 => [{"/", 2, 51}],
                        node2 => [{"/", 1, 51}]}),
    ?assertMatch({error, {disk_usage_too_high, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node2],
                     kv_nodes => [node1, node2, node3]})),

    %% We should check non-kv nodes that are not ejected
    pretend_disk_data(#{node1 => [{"/", 1, 51}],
                        node2 => [{"/", 1, 50}]}),
    ?assertMatch({error, {disk_usage_too_high, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2],
                     keep_nodes => [node1],
                     kv_nodes => [node2]})),
    %% We should check non-kv nodes being ejected
    ?assertMatch({error, {disk_usage_too_high, _}},
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2],
                     keep_nodes => [node2],
                     kv_nodes => [node2]})),

    %% Permit rebalance when total disk size is reduced but GR not hit
    pretend_disk_data(#{node1 => [{"/", 2, 50}],
                        node2 => [{"/", 1, 50}]}),
    ?assertMatch(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1],
                     keep_nodes => [node2],
                     kv_nodes => [node1, node2, node3]})),

    %% Permit rebalance when only serious threshold is crossed
    pretend_disk_data(#{node1 => [{"/", 1, 40}],
                        node2 => [{"/", 1, 40}]}),
    ?assertMatch(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2]})),

    meck:expect(config_profile, get_bool,
                fun ({resource_management, enabled}) -> false end),
    %% Don't validate rebalance when guardrails are disabled
    ?assertMatch(ok,
                 test_validate_topology_change(
                   #{active_nodes => [node1, node2],
                     keep_nodes => [node1],
                     kv_nodes => [node1, node2, node3]})).

validate_storage_migration_t() ->
    DataSizeConfig0 = [{enabled, true},
                       {couchstore_maximum, 1.6},
                       {magma_maximum, 16}],
    RRConfig0 = [{enabled, true},
                 {couchstore_minimum, 10},
                 {magma_minimum, 1}],
    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{bucket,
                          [{data_size, DataSizeConfig0},
                           {resident_ratio, RRConfig0}]}]
                end),
    meck:expect(ns_cluster_membership, service_active_nodes,
                fun (kv) -> [] end),

    %% Test couchstore -> magma
    CouchstoreConfig = [{storage_mode, couchstore}],

    %% Data size and RR% below/above couchstore limit respectively
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{data_size => 1,
                          resident_ratio => 20} end),
    ?assertEqual(ok,
                 validate_storage_migration("", CouchstoreConfig, magma)),

    %% Data size above couchstore limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{data_size => 10} end),
    ?assertEqual({error, data_size, 1.6},
                 validate_storage_migration("", CouchstoreConfig, magma)),

    %% RR% below couchstore limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{resident_ratio => 5} end),
    ?assertEqual({error, resident_ratio, 10},
                 validate_storage_migration("", CouchstoreConfig, magma)),

    %% Data size above magma limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{data_size => 20} end),
    ?assertEqual({error, data_size, 1.6},
                 validate_storage_migration("", CouchstoreConfig, magma)),

    %% RR% below magma limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{resident_ratio => 0.5} end),
    ?assertEqual({error, resident_ratio, 10},
                 validate_storage_migration("", CouchstoreConfig, magma)),

    %% Data size and RR% above/below magma limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{data_size => 20,
                          resident_ratio => 0.5} end),
    ?assertEqual({error, data_size, 1.6},
                 validate_storage_migration("", CouchstoreConfig, magma)),

    %% Test magma -> couchstore
    MagmaConfig = [{storage_mode, magma}],

    %% Data size and RR% below/above couchstore limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{data_size => 1,
                          resident_ratio => 20} end),
    ?assertEqual(ok,
                 validate_storage_migration("", MagmaConfig, couchstore)),

    %% Data size above couchstore limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{data_size => 2} end),
    ?assertEqual({error, data_size, 1.6},
                 validate_storage_migration("", MagmaConfig, couchstore)),

    %% RR% below couchstore limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{resident_ratio => 5} end),
    ?assertEqual({error, resident_ratio, 10},
                 validate_storage_migration("", MagmaConfig, couchstore)),

    %% Data size and RR% above/below couchstore limit
    meck:expect(stats_interface, for_storage_mode_migration,
                fun ("", _Nodes) ->
                        #{data_size => 2,
                          resident_ratio => 5} end),
    ?assertEqual({error, data_size, 1.6},
                 validate_storage_migration("", MagmaConfig, couchstore)),

    ok.

basic_test_() ->
    %% We can re-use (setup) the test environment that we setup/teardown here
    %% for each test rather than create a new one (foreach) to save time.
    {setup,
     fun() ->
             basic_test_setup()
     end,
     fun(_) ->
             basic_test_teardown()
     end,
     [{"check bucket test", fun () -> check_bucket_t() end},
      {"check bucket during storage migration test",
       fun () -> check_bucket_during_storage_migration_t() end},
      {"dont check memcached or ephemeral test",
       fun () -> dont_check_memcached_or_ephemeral_t() end},
      {"check all resources test", fun () -> check_resources_t() end},
      {"validate bucket topology change test",
       fun validate_bucket_topology_change_t/0},
      {"validate topology change data guard rails test",
       fun () -> validate_topology_change_data_grs_t() end},
      {"validate topology change cores_per_bucket test",
       fun () -> validate_topology_change_cores_per_bucket_t() end},
      {"validate topology change disk usage test",
       fun () -> validate_topology_change_disk_usage_t() end},
      {"validate storage migration test",
       fun () -> validate_storage_migration_t() end}]}.

check_test_modules() ->
    [ns_config, cluster_compat_mode, menelaus_web_guardrails,stats_interface,
     config_profile, ns_bucket, rpc].

check_test_setup() ->
    %% We need unstick, so that we can meck rpc
    meck:new(check_test_modules(), [passthrough, unstick]).

regular_checks_t() ->
    meck:expect(ns_config, search_node_with_default,
                fun ({?MODULE, check_interval}, _Default) ->
                        %% Use tiny timeout to force a second check immediately
                        1;
                    (_, Default) ->
                        Default
                end),
    meck:expect(ns_config, set,
                fun ({node, _, _}, _) -> ok end),

    meck:expect(cluster_compat_mode, is_cluster_76, ?cut(true)),
    meck:expect(config_profile, get_bool,
                fun ({resource_management, enabled}) -> false end),

    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{bucket,
                          [{resident_ratio,
                            [{enabled, true},
                             {couchstore_minimum, 10},
                             {magma_minimum, 1}]},
                           {data_size,
                            [{enabled, true},
                             {couchstore_maximum, 1},
                             {magma_maximum, 10}]}]},
                         {disk_usage,
                          [{enabled, true},
                           {maximum, 96}]},
                         {collections_per_quota,
                          [{enabled, true},
                           {maximum, 1}]},
                         {cores_per_bucket,
                          [{enabled, true},
                           {minimum, 0.5}]}]
                end),
    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 10}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 1}]}]
                end),

    CouchstoreBucket = [{type, membase},
                        {storage_mode, couchstore}],
    MagmaBucket = [{type, membase},
                   {storage_mode, magma}],
    meck:expect(ns_bucket, get_buckets,
                fun () ->
                        [{"couchstore_bucket", CouchstoreBucket},
                         {"magma_bucket", MagmaBucket}]
                end),

    meck:expect(ns_config, search_node,
                fun (database_dir) ->
                        {value, "dir"}
                end),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) -> Default end),
    pretend_disk_data(#{node() => [{"/", 1, 50}]}),

    {ok, _Pid} = start_link(),

    meck:expect(config_profile, get_bool,
                fun ({resource_management, enabled}) -> true end),

    %% Wait to see second check after enable (implying the first one completed)
    meck:wait(2, ns_config, read_key_fast, [resource_management, '_'],
              ?MECK_WAIT_TIMEOUT),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 9}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 1}]}]
                end),
    meck:wait(1, ns_config, set,
              [{node, node(), resource_statuses},
               [{{bucket, "couchstore_bucket"}, resident_ratio}]],
              ?MECK_WAIT_TIMEOUT),

    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 10}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 0.5}]}]
                end),
    meck:wait(1, ns_config, set,
              [{node, node(), resource_statuses},
               [{{bucket, "magma_bucket"}, resident_ratio}]],
              ?MECK_WAIT_TIMEOUT),

    %% Test bucket missing and stat missing
    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"}, []}]
                end),
    meck:wait(1, ns_config, set, [{node, node(), resource_statuses}, []],
              ?MECK_WAIT_TIMEOUT),

    %% Test that we don't crash when we get no stats
    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        []
                end),
    Checks0 = meck_history:num_calls('_', ns_config, read_key_fast,
                                     [resource_management, '_']),
    %% Wait until two further checks have started, proving that at least one
    %% completed without crashing
    meck:wait(Checks0 + 2, ns_config, read_key_fast, [resource_management, '_'],
              ?MECK_WAIT_TIMEOUT),

    %% Test that we don't crash when dbdir can't be found (such as during node
    %% rename)
    meck:expect(ns_config, search_node,
                fun (database_dir) ->
                        false
                end),

    Checks1 = meck_history:num_calls('_', ns_config, read_key_fast,
                                     [resource_management, '_']),
    %% Wait until two further checks have started, proving that at least one
    %% completed without crashing
    meck:wait(Checks1 + 2, ns_config, read_key_fast, [resource_management, '_'],
              ?MECK_WAIT_TIMEOUT),

    %% Confirm that expected functions were called in the first check
    meck:validate(ns_config),
    meck:validate(cluster_compat_mode),
    meck:validate(stats_interface).


%% Test the scenario where the monitor starts up on a node where a limit was
%% reached previously, but became no longer reached between the node's last
%% check and the monitor starting back up
initial_check_t() ->
    meck:expect(ns_config, search_node_with_default,
                fun ({?MODULE, check_interval}, _Default) ->
                        %% Use tiny timeout to force a second check immediately
                        1;
                    (resource_statuses, _Default) ->
                        [{{bucket, "default"}, disk_usage}]
                end),
    meck:expect(ns_config, set,
                fun ({node, _, _}, _) -> ok end),

    meck:expect(cluster_compat_mode, is_cluster_76, ?cut(true)),
    meck:expect(config_profile, get_bool,
                fun ({resource_management, enabled}) -> false end),

    meck:expect(ns_config, read_key_fast,
                fun (resource_management, _) ->
                        [{bucket,
                          [{resident_ratio,
                            [{enabled, true},
                             {couchstore_minimum, 10},
                             {magma_minimum, 1}]},
                           {data_size,
                            [{enabled, true},
                             {couchstore_maximum, 1},
                             {magma_maximum, 10}]}]},
                         {disk_usage,
                          [{enabled, true},
                           {maximum, 96}]},
                         {collections_per_quota,
                          [{enabled, true},
                           {maximum, 1}]},
                         {cores_per_bucket,
                          [{enabled, true},
                           {minimum, 0.5}]}]
                end),
    meck:expect(stats_interface, for_resource_management,
                fun () ->
                        [{{bucket, "couchstore_bucket"},
                          [{resident_ratio, 10}]},
                         {{bucket, "magma_bucket"},
                          [{resident_ratio, 1}]}]
                end),

    CouchstoreBucket = [{type, membase},
                        {storage_mode, couchstore}],
    MagmaBucket = [{type, membase},
                   {storage_mode, magma}],
    meck:expect(ns_bucket, get_buckets,
                fun () ->
                        [{"couchstore_bucket", CouchstoreBucket},
                         {"magma_bucket", MagmaBucket}]
                end),

    meck:expect(ns_config, search_node,
                fun (database_dir) ->
                        {value, "dir"}
                end),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) -> Default end),

    %% No disk usage issue at startup
    pretend_disk_data(#{node() => [{"/", 1, 50}]}),

    {ok, _Pid} = start_link(),

    meck:expect(config_profile, get_bool,
                fun ({resource_management, enabled}) -> true end),

    %% Wait to see second check after enable (implying the first one completed)
    meck:wait(2, ns_config, read_key_fast, [resource_management, '_'],
              ?MECK_WAIT_TIMEOUT),

    %% Confirm that ns_config gets updated to match the real state
    meck:wait(1, ns_config, set,
              [{node, node(), resource_statuses},
               []],
              ?MECK_WAIT_TIMEOUT),

    %% Confirm that expected functions were called in the first check
    meck:validate(ns_config),
    meck:validate(cluster_compat_mode),
    meck:validate(stats_interface).

check_test_teardown() ->
    gen_server:stop(?SERVER),
    meck:unload(check_test_modules()).

check_test_() ->
    {foreach,
     fun () ->
             check_test_setup()
     end,
     fun(_) ->
             check_test_teardown()
     end,
     [{"regular checks test", fun () -> regular_checks_t() end},
      {"initial check test", fun () -> initial_check_t() end}]}.

-endif.
