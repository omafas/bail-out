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
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("ns_test.hrl").
-endif.

-behaviour(gen_server).

-export([is_enabled/0, get_config/0, get/1, get/2, start_link/0,
         check_num_replicas_change/3, get_local_status/3, check_alert/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% Exported for rpc
-export([get_disk_data_for_service/1]).


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

-type resource() :: {bucket, bucket_name()} | disk | index.
-export_type([resource/0]).

-type disk_severity() :: warning | serious | critical | maximum.
-type index_severity() :: warning | serious | critical.
-type status() :: ok | data_ingress_status() | disk_severity() |
                  index_severity().
-export_type([status/0]).

-type disk_stats_error() :: no_dbdir
                          | {dbdir_path_error, term()}
                          | no_disk_stats_found
                          | service_not_active_on_this_node.

-spec is_enabled() -> boolean().
is_enabled() ->
    cluster_compat_mode:is_cluster_phoenix()
        orelse (cluster_compat_mode:is_cluster_76()
                andalso config_profile:get_bool({resource_management,
                                                 enabled})).


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

-spec check_alert(Guardrail, Limits) -> ok | {Level, Disk, Value} when
      Guardrail :: disk,
      Level :: warning | maximum,
      Limits :: [{Level, integer()}],
      Disk :: string(),
      Value :: integer().
check_alert(disk, Thresholds) ->
    case get_disk_data_for_service(kv) of
        {ok, DiskEntry} ->
            {Disk, _, Value} = DiskEntry,
            case check_disk_usage(Thresholds, DiskEntry) of
                ok -> ok;
                Severity -> {Severity, Disk, Value}
            end;
        _Error ->
            %% No need to log here, since the error gets logged earlier
            ok
    end.


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec get_disk_stats_from_nodes([node()], service()) ->
          [{ok,
            {node(),
             {ok, ns_disksup:disk_stat()}
            | {badrpc, term()}
            | {error, disk_stats_error()}}}].
get_disk_stats_from_nodes(Nodes, Service) ->
    misc:parallel_map(
      fun (Node) ->
              case rpc:call(Node, guardrail_monitor,
                            get_disk_data_for_service, [Service],
                            ?RPC_TIMEOUT) of
                  {badrpc, _} = Error ->
                      {Node, Error};
                  DiskData ->
                      {Node, DiskData}
              end
      end, Nodes, ?PARALLEL_RPC_TIMEOUT).

-spec get_disk_data_for_service(service()) -> {ok, ns_disksup:disk_stat()}
              | {error, disk_stats_error()}.
get_disk_data_for_service(Service) ->
    MaybeDbDir = case Service of
                     kv -> ns_storage_conf:this_node_dbdir();
                     index -> ns_storage_conf:this_node_ixdir()
                 end,
    case MaybeDbDir of
        {ok, DbDir} ->
            Mounts = ns_disksup:get_disk_data(),
            get_disk_data_for_dir(Mounts, DbDir);
        {error, not_found} ->
            ?log_error("Couldn't check disk usage for ~s as node db dir is "
                       "missing", [Service]),
            {error, no_dbdir}
    end.

-spec get_high_disk_usage_from_nodes([{atom(), number()}],
                                     [node()], service()) ->
          [{node(), disk_severity() | {error, any()} | any()}].
get_high_disk_usage_from_nodes(Thresholds, Nodes, Service) ->
    NodeDiskStats = get_disk_stats_from_nodes(Nodes, Service),
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
            BadNodes = get_high_disk_usage_from_nodes(Thresholds,
                                                      Nodes,
                                                      kv),
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
    Keys = stats_keys_from_config(Config),
    Stats = stats_interface:for_resource_management(Keys),
    lists:flatmap(check(_, Stats), Config).

stats_keys_from_config(Config) ->
    KeyIfEnabled =
        fun (Key, GRConfig) ->
                case proplists:get_bool(enabled, GRConfig) of
                    false -> false;
                    true -> {true, Key}
                end
        end,
    lists:flatmap(
      fun ({bucket, BucketConfig}) ->
              lists:filtermap(
                fun ({resident_ratio, GRConfig}) ->
                        KeyIfEnabled(<<"kv_resident_ratio">>, GRConfig);
                    ({data_size, GRConfig}) ->
                        KeyIfEnabled(<<"kv_data_size">>, GRConfig);
                    (_) ->
                        false
                end, BucketConfig);
          ({index, IndexConfig}) ->
              lists:filtermap(
                fun ({index_growth_rr, GRConfig}) ->
                        KeyIfEnabled(<<"index_resident_ratio">>, GRConfig);
                    (_) ->
                        false
                end, IndexConfig);
          (_) ->
              []
      end, Config).

-ifdef(TEST).
stats_keys_from_config_test() ->
    ?assertEqual([],
                 stats_keys_from_config([{bucket,
                                          [{other,
                                            [{enabled, true}]}]}])),
    ?assertEqual([],
                 stats_keys_from_config([{other,
                                          [{resident_ratio,
                                            [{enabled, true}]}]}])),
    ?assertEqual([],
                 stats_keys_from_config([{bucket,
                                          [{resident_ratio, []}]}])),
    ?assertEqual([],
                 stats_keys_from_config([{index,
                                          [{other,
                                            [{enabled, true}]}]}])),
    ?assertEqual([<<"kv_resident_ratio">>],
                 stats_keys_from_config([{bucket,
                                          [{resident_ratio,
                                            [{enabled, true}]}]}])),
    ?assertEqual([<<"kv_data_size">>],
                 stats_keys_from_config([{bucket,
                                          [{data_size,
                                            [{enabled, true}]}]}])),
    ?assertEqual([<<"index_resident_ratio">>],
                 stats_keys_from_config([{index,
                                          [{index_growth_rr,
                                            [{enabled, true}]}]}])),
    ?assertEqual([<<"kv_resident_ratio">>,
                  <<"kv_data_size">>,
                  <<"index_resident_ratio">>],
                 stats_keys_from_config([{bucket,
                                          [{resident_ratio,
                                            [{enabled, true}]},
                                           {data_size,
                                            [{enabled, true}]}]},
                                         {index,
                                          [{index_growth_rr,
                                            [{enabled, true}]}]}])),
    ?assertEqual([<<"kv_resident_ratio">>],
                 stats_keys_from_config([{bucket,
                                          [{resident_ratio,
                                            [{enabled, true}]},
                                           {data_size,
                                            []}]},
                                         {index,
                                          [{index_growth_rr,
                                            [{enabled, false}]}]}])).
-endif.

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
check({disk_usage, Config}, _Stats) ->
    check_disk_usage_for_service(kv, Config);
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
    end ++
        check_disk_usage_for_service(
          index, proplists:get_value(disk_usage, IndexConfig, []));
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

check_disk_usage_for_service(Service, Config) ->
    case get_thresholds(Config) of
        undefined ->
            [];
        Thresholds ->
            Node = node(),
            Services = ns_cluster_membership:node_active_services(Node),
            case lists:member(Service, Services) of
                false ->
                    [];
                true ->
                    {Severity, Statuses} =
                        case get_high_disk_usage_from_nodes(Thresholds, [Node],
                                                            Service) of
                            [{Node, {error, _Error}}] ->
                                %% If we fail to get disk stats then we assume
                                %% the disk usage is safe, rather than disabling
                                %% data ingress.
                                %% We do this because disabling data ingress is
                                %% an extreme action that we do not want to take
                                %% unless we are sure that we have to.
                                {ok, []};
                            [{Node, Sev}] ->
                                {Sev,
                                 get_disk_usage_statuses_for_service(Service,
                                                                     Sev)};
                            [] ->
                                {ok, []}
                        end,
                    notify_disk_usage_severity_for_service(Service, Severity),
                    Statuses
            end
    end.

get_disk_usage_statuses_for_service(kv, maximum) ->
    %% For now if we see disk usage reach the limit on any
    %% node, we apply the guard for all buckets.
    %% It may seem odd that we don't restrict this check to
    %% only data-service nodes, since the impact (prior to
    %% the addition of other service guardrails) was
    %% restricted to only data-service nodes. However, by
    %% making the check on all nodes, we ensure that data
    %% doesn't indirectly get ingested into other nodes via
    %% KV, which would make a disk usage issue worse.
    %%
    %% In future we may wish to allow this to apply on a
    %% per-service and per-bucket level, when these are
    %% mapped to different disk partitions
    [{{bucket, BucketName}, disk_usage}
     || BucketName <- ns_bucket:get_bucket_names()];
get_disk_usage_statuses_for_service(index, Severity) ->
    [{{index, disk_usage}, Severity}].

notify_disk_usage_severity_for_service(kv, Severity) ->
    Gauge = case Severity of
                maximum -> 1;
                ok -> 0
            end,
    ns_server_stats:notify_gauge(
      {<<"resource_limit_reached">>,
       [{resource, kv_disk_usage},
        {severity, maximum}]},
      Gauge);
notify_disk_usage_severity_for_service(index, Severity) ->
    lists:foreach(
      fun (SeverityToReport) ->
              Gauge =
                  case Severity of
                      serious when SeverityToReport =:= serious ->
                          1;
                      critical ->
                          1;
                      _ ->
                          0
                  end,
              ns_server_stats:notify_gauge(
                {<<"resource_limit_reached">>,
                 [{resource, index_disk_usage},
                  {severity, SeverityToReport}]},
                Gauge)
      end,
      [critical, serious]).

-spec check_disk_usage([{disk_severity(), number()}], ns_disksup:disk_stat()) ->
          ok | disk_severity().
check_disk_usage(Thresholds, {_Disk, _Cap, Used}) ->
    get_severity_for_thresholds(disk, Thresholds, Used, descending).

-spec get_disk_data_for_dir(ns_disksup:disk_stats(), string()) ->
          {ok, ns_disksup:disk_stat()} | {error, disk_stats_error()}.
get_disk_data_for_dir(Mounts, DbDir) ->
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
    end.

get_thresholds(Config) ->
    case proplists:get_bool(enabled, Config) of
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
    meck:expect(cluster_compat_mode, is_cluster_phoenix, ?cut(true)),
    meck:expect(cluster_compat_mode, is_enterprise, ?cut(true)),
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
    pretend_disk_data(#{}),

    meck:expect(stats_interface, for_resource_management,
                fun ([]) ->
                        []
                end),
    ?assertEqual([],
                 check_resources()),

    meck:expect(stats_interface, for_resource_management,
                fun ([]) ->
                        [{{bucket, "couchstore_bucket"}, []}]
                end),
    ?assertEqual([],
                 check_resources()),

    meck:expect(stats_interface, for_resource_management,
                fun ([]) ->
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
                fun ([]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                           {maximum, 96}]},
                         {index,
                          [{disk_usage,
                            [{enabled, true},
                             {critical, 85},
                             {serious, 80}]}]}]
                end),
    pretend_disk_data(#{node() => #{kv => {"/", 1, 50}}}),

    meck:expect(ns_storage_conf, this_node_dbdir,
                fun () -> {ok, "invalid_file"} end),

    meck:expect(ns_storage_conf, extract_disk_stats_for_path,
                fun (_, _) -> none end),

    meck:expect(ns_bucket, get_bucket_names,
                fun () -> ["couchstore_bucket", "magma_bucket"] end),


    meck:expect(stats_interface, for_resource_management,
                fun ([]) -> [] end),
    ?assertEqual([], check_resources()),
    ?assertResourceMetrics(
       #{[{resource, kv_disk_usage},
          {severity, maximum}] => 0}),

    meck:expect(ns_storage_conf, this_node_dbdir,
                fun () -> {ok, ""} end),

    ?assertEqual([], check_resources()),
    ?assertResourceMetrics(
       #{[{resource, kv_disk_usage},
          {severity, maximum}] => 0}),

    meck:expect(ns_storage_conf, extract_disk_stats_for_path,
                fun ([Value], _) -> {ok, Value} end),

    ?assertEqual([], check_resources()),
    ?assertResourceMetrics(
       #{[{resource, kv_disk_usage},
          {severity, maximum}] => 0}),

    pretend_disk_data(#{node() => #{kv => {"/", 1, 97}}}),

    ?assertEqual([{{bucket, "couchstore_bucket"}, disk_usage},
                  {{bucket, "magma_bucket"}, disk_usage}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, kv_disk_usage},
          {severity, maximum}] => 1}),

    pretend_disk_data(#{node() => #{kv => {"/", 1, 86},
                                    index => {"/", 1, 86}}}),

    ?assertEqual([{{index, disk_usage}, critical}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_disk_usage},
          {severity, serious}] => 1,
         [{resource, index_disk_usage},
          {severity, critical}] => 1,
         [{resource, kv_disk_usage},
          {severity, maximum}] => 0}),

    pretend_disk_data(#{node() => #{kv => {"/", 1, 81},
                                    index => {"/", 1, 81}}}),

    ?assertEqual([{{index, disk_usage}, serious}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_disk_usage},
          {severity, serious}] => 1,
         [{resource, index_disk_usage},
          {severity, critical}] => 0,
         [{resource, kv_disk_usage},
          {severity, maximum}] => 0}),

    pretend_disk_data(#{node() => #{kv => {"/", 1, 97},
                                    index => {"/", 1, 81}}}),

    ?assertEqual([{{bucket, "couchstore_bucket"}, disk_usage},
                  {{bucket, "magma_bucket"}, disk_usage},
                  {{index, disk_usage}, serious}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_disk_usage},
          {severity, serious}] => 1,
         [{resource, index_disk_usage},
          {severity, critical}] => 0,
         [{resource, kv_disk_usage},
          {severity, maximum}] => 1}),

    pretend_disk_data(#{node() => #{kv => {"/", 1, 97},
                                    index => {"/", 1, 86}}}),

    ?assertEqual([{{bucket, "couchstore_bucket"}, disk_usage},
                  {{bucket, "magma_bucket"}, disk_usage},
                  {{index, disk_usage}, critical}],
                 check_resources()),
    ?assertResourceMetrics(
       #{[{resource, index_disk_usage},
          {severity, serious}] => 1,
         [{resource, index_disk_usage},
          {severity, critical}] => 1,
         [{resource, kv_disk_usage},
          {severity, maximum}] => 1}),

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
                            fun ([<<"index_resident_ratio">>]) ->
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

pretend_disk_data(DiskDataMap) ->
    meck:expect(ns_cluster_membership, node_active_services,
                fun (Node) ->
                    case maps:find(Node, DiskDataMap) of
                        {ok, NodeMap} ->
                            maps:keys(NodeMap);
                        error ->
                            {badrpc, noproc}
                    end
                end),
    meck:expect(rpc, call,
                fun (Node, guardrail_monitor, get_disk_data_for_service,
                     [Service], _Timeout) ->
                        case maps:find(Node, DiskDataMap) of
                            {ok, NodeMap} ->
                                case maps:find(Service, NodeMap) of
                                    error -> {error, no_dbdir};
                                    DiskData -> DiskData
                                end;
                            error ->
                                {badrpc, noproc}
                        end
                end).

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
      {"check all resources test", fun () -> check_resources_t() end}]}.

check_test_modules() ->
    [ns_config, cluster_compat_mode, menelaus_web_guardrails,stats_interface,
     config_profile, ns_bucket, rpc, ns_cluster_membership].

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
    meck:expect(cluster_compat_mode, is_cluster_phoenix, ?cut(true)),
    meck:expect(cluster_compat_mode, is_enterprise, ?cut(true)),
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
    pretend_disk_data(#{node() => #{kv => {"/", 1, 50}}}),

    {ok, _Pid} = start_link(),

    meck:expect(config_profile, get_bool,
                fun ({resource_management, enabled}) -> true end),

    %% Wait to see second check after enable (implying the first one completed)
    meck:wait(2, ns_config, read_key_fast, [resource_management, '_'],
              ?MECK_WAIT_TIMEOUT),

    meck:expect(stats_interface, for_resource_management,
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
                        [{{bucket, "couchstore_bucket"}, []}]
                end),
    meck:wait(1, ns_config, set, [{node, node(), resource_statuses}, []],
              ?MECK_WAIT_TIMEOUT),

    %% Test that we don't crash when we get no stats
    meck:expect(stats_interface, for_resource_management,
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
    meck:expect(cluster_compat_mode, is_cluster_phoenix, ?cut(true)),
    meck:expect(cluster_compat_mode, is_enterprise, ?cut(true)),
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
                fun ([<<"kv_resident_ratio">>, <<"kv_data_size">>]) ->
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
    pretend_disk_data(#{node() => #{kv => {"/", 1, 50}}}),

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
