%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_orchestrator).

-behaviour(gen_statem).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("bucket_hibernation.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type busy() :: rebalance_running |
                in_recovery |
                in_bucket_hibernation |
                in_buckets_shutdown.

-export_type([busy/0]).

%% Constants and definitions

-record(idle_state, {}).
-record(janitor_state, {cleanup_id :: undefined | pid()}).

-record(rebalancing_state, {rebalancer,
                            rebalance_observer,
                            keep_nodes,
                            eject_nodes,
                            failed_nodes,
                            delta_recov_bkts,
                            retry_check,
                            to_failover,
                            stop_timer,
                            type,
                            rebalance_id,
                            abort_reason,
                            reply_to,
                            opts}).

-record(bucket_hibernation_state,
        {hibernation_manager :: pid(),
         bucket :: bucket_name(),
         op  :: pause_bucket | resume_bucket,
         stop_tref = undefined :: undefined | reference(),
         stop_reason = undefined :: term()}).

-record(bucket_shutdown_ctx,
        {from :: {pid(), gen_statem:reply_tag()},
         pid :: pid(),
         bucket_name :: bucket_name()}).

-record(recovery_state, {pid :: pid()}).
-record(ejecting_state, {}).


%% API
-export([create_bucket/3,
         update_bucket/4,
         delete_bucket/1,
         flush_bucket/1,
         start_pause_bucket/1,
         stop_pause_bucket/1,
         start_resume_bucket/2,
         stop_resume_bucket/1,
         failover/2,
         start_failover/2,
         try_autofailover/2,
         needs_rebalance/0,
         needs_rebalance_with_detail/0,
         start_link/0,
         start_rebalance/1,
         retry_rebalance/4,
         stop_rebalance/0,
         start_recovery/1,
         stop_recovery/2,
         commit_vbucket/3,
         recovery_status/0,
         recovery_map/2,
         is_recovery_running/0,
         ensure_janitor_run/1,
         ensure_janitor_run/2,
         rebalance_type2text/1,
         start_graceful_failover/1,
         start_graceful_failover/2,
         request_janitor_run/1,
         get_state/1,
         prepare_fusion_rebalance/1,
         fusion_upload_mounted_volumes/2]).

-define(SERVER, {via, leader_registry, ?MODULE}).

-define(FLUSH_BUCKET_TIMEOUT,   ?get_timeout(flush_bucket, 60000)).
-define(CREATE_BUCKET_TIMEOUT,  ?get_timeout(create_bucket, 5000)).
-define(JANITOR_RUN_TIMEOUT,    ?get_timeout(ensure_janitor_run, 30000)).
-define(JANITOR_INTERVAL,       ?get_param(janitor_interval, 5000)).
-define(STOP_REBALANCE_TIMEOUT, ?get_timeout(stop_rebalance, 10000)).
-define(STOP_PAUSE_BUCKET_TIMEOUT,
        ?get_timeout(stop_pause_bucket, 10 * 1000)). %% 10 secs.
-define(STOP_RESUME_BUCKET_TIMEOUT,
        ?get_timeout(stop_pause_bucket, 10 * 1000)). %% 10 secs.
-define(DEFAULT_RETRIES_IF_LEAVING_CLUSTER,
        ?get_param(default_retries_if_ejecting, 2)).
-define(EJECTING_ORCHESTRATOR_WAIT_TIMEOUT,
        ?get_timeout(retry_if_ejecting, 5000)).

-define(FUSION_REBALANCE_PLAN, fusion_rebalance_plan).

%% gen_statem callbacks
-export([code_change/4,
         init/1,
         callback_mode/0,
         handle_event/4,
         terminate/3]).

%% States
-export([idle/2, idle/3,
         janitor_running/2, janitor_running/3,
         rebalancing/2, rebalancing/3,
         recovery/2, recovery/3,
         bucket_hibernation/3,
         buckets_shutdown/3,
         ejecting/2]).

%%
%% API
%%

start_link() ->
    misc:start_singleton(gen_statem, ?MODULE, [], []).

wait_for_orchestrator() ->
    misc:wait_for_global_name(?MODULE).

call(Msg) ->
    call(Msg, infinity).

call(Msg, Timeout) ->
    call(Msg, Timeout, ?DEFAULT_RETRIES_IF_LEAVING_CLUSTER).

%% There are two timeouts used in this function. 'Timeout' is the one used for
%% orchestrator calls. If the orchestrator is leaving the cluster and we make a
%% call to it, it will not be able to handle it and returns
%% 'orchestrator_ejecting'. In this case, we need to wait for some time until
%% the process goes down. The wait time is defined as
%% EJECTING_ORCHESTRATOR_WAIT_TIMEOUT and after that we will retry.
call(Msg, Timeout, RetriesLeftIfLeavingCluster) ->
    wait_for_orchestrator(),
    Res = gen_statem:call(?SERVER, Msg, Timeout),
    case {RetriesLeftIfLeavingCluster, Res} of
        {0, {orchestrator_ejecting, _EjectingOrchestratorPid}} ->
            exit(Res);
        {RetriesLeftIfLeavingCluster,
         {orchestrator_ejecting, EjectingOrchestratorPid}} ->
            misc:wait_for_process(EjectingOrchestratorPid,
                                  ?EJECTING_ORCHESTRATOR_WAIT_TIMEOUT),
            ?log_debug("Retrying ~p. Retries left: ~p",
                       [Msg, RetriesLeftIfLeavingCluster]),
            call(Msg, Timeout, RetriesLeftIfLeavingCluster - 1);
        _ ->
            Res
    end.

-spec get_state(integer()) -> atom().
get_state(Timeout) ->
    gen_statem:call(?SERVER, get_state, Timeout).

-spec create_bucket(memcached|membase, nonempty_string(), list()) ->
          ok | {error, {already_exists, nonempty_string()}} |
          {error, {still_exists, nonempty_string()}} |
          {error, {port_conflict, integer()}} |
          {error, {need_more_space, list()}} |
          {error, {incorrect_parameters, nonempty_string()}} |
          {error, {kek_not_found, nonempty_string()}} |
          {error, secret_not_found} |
          {error, secret_not_allowed} | busy().
create_bucket(BucketType, BucketName, NewConfig) ->
    call({create_bucket, BucketType, BucketName, NewConfig}, infinity).

-spec update_bucket(memcached|membase, undefined|couchstore|magma|ephemeral,
                    nonempty_string(), list()) ->
          ok | {exit, {not_found, nonempty_string()}, []} |
          {error, {need_more_space, list()}} |
          {error, {storage_mode_migration, in_progress}} |
          {error, cc_versioning_already_enabled} |
          {error, {storage_mode_migration, janitor_not_run}} |
          {error, {storage_mode_migration,
                   history_retention_enabled_on_bucket}} |
          {error, {storage_mode_migration,
                   history_retention_enabled_on_collections}} |
          {error, secret_not_found} |
          {error, secret_not_allowed} | busy().
update_bucket(BucketType, StorageMode, BucketName, UpdatedProps) ->
    call({update_bucket, BucketType, StorageMode, BucketName, UpdatedProps},
         infinity).

%% Deletes bucket. Makes sure that once it returns it's already dead.
%% In implementation we make sure config deletion is propagated to
%% child nodes. And that ns_memcached for bucket being deleted
%% dies. But we don't wait more than ?DELETE_BUCKET_TIMEOUT.
%%
%% Return values are ok if it went fine at least on local node
%% (failure to stop ns_memcached on any nodes is merely logged);
%% rebalance_running if delete bucket request came while rebalancing;
%% and {exit, ...} if bucket does not really exists
-spec delete_bucket(bucket_name()) ->
                           ok | rebalance_running | in_recovery |
                           in_bucket_hibernation |
                           {shutdown_failed, [node()]} |
                           shutdown_incomplete |
                           {exit, {not_found, bucket_name()}, _}.
delete_bucket(BucketName) ->
    call({delete_bucket, BucketName}, infinity).

-spec flush_bucket(bucket_name()) ->
          ok | busy() |
          bucket_not_found |
          flush_disabled |
          {prepare_flush_failed, _, _} |
          {flush_wait_failed, _, _} |
          {old_style_flush_failed, _, _}.
flush_bucket(BucketName) ->
    call({flush_bucket, BucketName}, infinity).

-spec start_pause_bucket(Args :: #bucket_hibernation_op_args{}) ->
          ok | busy() |
          bucket_not_found |
          not_supported |
          bucket_type_not_supported |
          no_width_parameter |
          requires_rebalance |
          full_servers_unavailable |
          failed_service_nodes |
          map_servers_mismatch.
start_pause_bucket(Args) ->
    call({{bucket_hibernation_op, {start, pause_bucket}},
          {Args, []}}).

-spec stop_pause_bucket(bucket_name()) ->
    ok |
    in_recovery |
    rebalance_running |
    bucket_not_found |
    not_running_pause_bucket |
    in_bucket_hibernation |
    [Errors :: bucket_not_found | not_running_pause_bucket].
stop_pause_bucket(Bucket) ->
    call({{bucket_hibernation_op, {stop, pause_bucket}}, [Bucket]}).

-spec start_resume_bucket(#bucket_hibernation_op_args{}, list()) ->
          ok | busy() |
          {need_more_space, term()} |
          bucket_exists.
start_resume_bucket(Args, Metadata) ->
    call({{bucket_hibernation_op, {start, resume_bucket}},
          {Args, [Metadata]}}).

-spec stop_resume_bucket(bucket_name()) ->
    ok |
    rebalance_running |
    in_recovery |
    bucket_not_found |
    not_running_resume_bucket |
    in_buckets_shutdown |
    [Errors :: bucket_not_found | not_running_resume_bucket].
stop_resume_bucket(Bucket) ->
    call({{bucket_hibernation_op, {stop, resume_bucket}}, [Bucket]}).

%% The second argument can be a boolean or a map. Pre-Morpheus compat this will
%% be a boolean. Post-Morpheus compat this will be a map. This can be update
%% once we move the minimum compat version beyond Morpheus.
-spec failover([node()], boolean() | map()) ->
          ok |
          rebalance_running |
          in_recovery |
          last_node |
          {last_node_for_bucket, list()} |
          unknown_node |
          inactive_node |
          orchestration_unsafe |
          config_sync_failed |
          quorum_lost |
          stopped_by_user |
          in_buckets_shutdown |
          {incompatible_with_previous, [atom()]} |
          expected_topology_mismatch.
failover(Nodes, AllowUnsafe) when is_boolean(AllowUnsafe) ->
    %% Pre-Morpheus compat function clause.
    call({failover, Nodes, AllowUnsafe}, infinity);
failover(Nodes, Options) when is_map(Options) ->
    call({failover, Nodes, Options}, infinity).

%% The second argument can be a boolean or a map. Pre-Morpheus compat this will
%% be a boolean. Post-Morpheus compat this will be a map. This can be update
%% once we move the minimum compat version beyond Morpheus.
-spec start_failover([node()], boolean() | map()) ->
          ok |
          rebalance_running |
          in_recovery |
          last_node |
          {last_node_for_bucket, list()} |
          unknown_node |
          inactive_node |
          in_buckets_shutdown |
          {incompatible_with_previous, [atom()]} |
          expected_topology_mismatch.
start_failover(Nodes, AllowUnsafe) when is_boolean(AllowUnsafe) ->
    %% Pre-Morpheus compat function clause.
    call({start_failover, Nodes, AllowUnsafe});
start_failover(Nodes, Options) when is_map(Options) ->
    call({start_failover, Nodes, Options}).

-spec try_autofailover(list(), map()) ->
                              {ok, list()} |
                              {operation_running, list()}|
                              retry_aborting_rebalance |
                              in_recovery |
                              orchestration_unsafe |
                              last_node |
                              config_sync_failed |
                              quorum_lost |
                              stopped_by_user |
                              {autofailover_unsafe, [bucket_name()]} |
                              {nodes_down, [node()], [bucket_name()]} |
                              {cannot_preserve_durability_majority,
                               [bucket_name()]}.
try_autofailover(Nodes, Options) ->
    case call({try_autofailover, Nodes, Options}, infinity) of
        ok ->
            {ok, []};
        Other ->
            Other
    end.

-spec prepare_fusion_rebalance([node()]) ->
          {ok, term()} | busy() |
          {unknown_nodes, [node()]} |
          {remote_call_failed, node()}.
prepare_fusion_rebalance(KeepNodes) ->
    call({prepare_fusion_rebalance, KeepNodes}, infinity).

-type rebalance_plan_uuid() :: string().
-spec fusion_upload_mounted_volumes(rebalance_plan_uuid(),
                                    list()) -> ok | busy() | not_found |
          id_mismatch | {need_nodes, [node()]} | {extra_nodes, [node()]}.
fusion_upload_mounted_volumes(PlanUUID, Volumes) ->
    call({fusion_upload_mounted_volumes, PlanUUID, Volumes}, infinity).

-spec needs_rebalance() -> boolean().
needs_rebalance() ->
    needs_rebalance(needs_rebalance_with_detail()).

-spec needs_rebalance(map()) -> boolean().
needs_rebalance(Input) ->
    #{services => [], buckets => []} =/= Input.

-spec needs_rebalance_with_detail() -> #{services := list(), buckets := list()}.
needs_rebalance_with_detail() ->
    NodesWanted = ns_node_disco:nodes_wanted(),
    ServiceNeedsRebalanceReasons =
        lists:filtermap(
            fun (S) ->
                case service_needs_rebalance(S, NodesWanted) of
                    false ->
                        false;
                    {true, Id} ->
                        {true, {Id, S}}
                end
            end,
            ns_cluster_membership:cluster_supported_services()),
    BucketNeedsRebalanceReasons =
        [X || X <- buckets_need_rebalance(NodesWanted), X =/= false],

    #{services => ServiceNeedsRebalanceReasons,
      buckets => BucketNeedsRebalanceReasons}.

service_needs_rebalance(Service, NodesWanted) ->
    ServiceNodes = ns_cluster_membership:service_nodes(NodesWanted, Service),
    ActiveServiceNodes = ns_cluster_membership:service_active_nodes(Service),
    NeedsRebalance =
        lists:sort(ServiceNodes) =/= lists:sort(ActiveServiceNodes) orelse
        topology_aware_service_needs_rebalance(Service, ActiveServiceNodes),
    ns_rebalancer:needs_rebalance_with_reason(NeedsRebalance,
                                              service_not_balanced, Service).

topology_aware_service_needs_rebalance(Service, ServiceNodes) ->
    case lists:member(Service,
                      ns_cluster_membership:topology_aware_services()) of
        true ->
            %% TODO: consider caching this
            Statuses = ns_doctor:get_nodes(),
            lists:any(
              fun (Node) ->
                      NodeStatus = misc:dict_get(Node, Statuses, []),
                      ServiceStatus =
                          proplists:get_value({service_status, Service},
                                              NodeStatus, []),
                      proplists:get_value(needs_rebalance, ServiceStatus, false)
              end, ServiceNodes);
        false ->
            false
    end.

-spec buckets_need_rebalance([node(), ...]) -> list().
buckets_need_rebalance(NodesWanted) ->
    KvNodes = ns_cluster_membership:service_nodes(NodesWanted, kv),
    lists:filtermap(fun ({Bucket, BucketConfig}) ->
                      case ns_rebalancer:bucket_needs_rebalance_with_details(
                        Bucket, BucketConfig, KvNodes) of
                          false ->
                              false;
                          {true, Value} ->
                              {true, {Value, Bucket}}
                      end
                    end,
                    ns_bucket:get_buckets_by_rank()).

-spec request_janitor_run(janitor_item()) -> ok.
request_janitor_run(Item) ->
    gen_statem:cast(?SERVER, {request_janitor_run, Item}).

-spec ensure_janitor_run(janitor_item()) ->
                                ok |
                                in_recovery |
                                in_bucket_hibernation |
                                rebalance_running |
                                janitor_failed |
                                bucket_deleted.
ensure_janitor_run(Item) ->
    ensure_janitor_run(Item, ?JANITOR_RUN_TIMEOUT).

ensure_janitor_run(Item, Timeout) ->
    wait_for_orchestrator(),
    misc:poll_for_condition(
      fun () ->
              case gen_statem:call(?SERVER, {ensure_janitor_run, Item},
                                   infinity) of
                  warming_up ->
                      false;
                  interrupted ->
                      false;
                  Ret ->
                      Ret
              end
      end, Timeout, 1000).

-spec start_rebalance(#{known_nodes := [node()],
                        eject_nodes := [node()],
                        delta_recovery_buckets := all | [bucket_name()],
                        defragment_zones := [list()],
                        services := all | [atom()],
                        desired_services_nodes := map() | undefined,
                        plan_uuid := rebalance_plan_uuid() | undefined}) ->
          {ok, binary()} | ok | in_progress |
          nodes_mismatch |
          no_active_nodes_left | in_recovery |
          in_bucket_hibernation |
          in_buckets_shutdown | {nodes_down, [atom()]} |
          delta_recovery_not_possible | no_kv_nodes_left |
          {need_more_space, list()} |
          {must_rebalance_services, list()} |
          {unhosted_services, list()} |
          {total_quota_too_high, list()} |
          {rebalance_not_allowed, list()} |
          {params_mismatch, list()} |
          {invalid_rebalance_plan, string()}.
start_rebalance(Params) ->
    #{desired_services_nodes := DesiredServicesNodes,
      known_nodes := KnownNodes} = Params,
    case get_services_nodes_memory_data(DesiredServicesNodes, KnownNodes) of
        {error, E} ->
            E;
        {ok, MemoryData} ->
            call({maybe_start_rebalance,
                  Params#{memory_data => MemoryData}})
    end.

retry_rebalance(rebalance, Params, Id, Chk) ->
    call({maybe_start_rebalance,
          maps:merge(maps:from_list(Params),
                     #{id => Id, chk => Chk, services => all})});

retry_rebalance(graceful_failover, Params, Id, Chk) ->
    call({maybe_retry_graceful_failover,
          proplists:get_value(nodes, Params),
          proplists:get_value(opts, Params), Id, Chk}).

%% Pre-Morpheus compat function. start_graceful_failover/2 is the new function.
-spec start_graceful_failover([node()]) ->
          ok |
          in_progress |
          in_recovery |
          non_kv_node |
          not_graceful |
          unknown_node |
          inactive_node |
          last_node |
          {last_node_for_bucket, list()} |
          {config_sync_failed, any()}.
start_graceful_failover(Nodes) ->
    call({start_graceful_failover, Nodes}).

-spec start_graceful_failover([node()], map()) ->
          ok |
          in_progress |
          in_recovery |
          non_kv_node |
          not_graceful |
          unknown_node |
          inactive_node |
          last_node |
          {last_node_for_bucket, list()} |
          {config_sync_failed, any()} |
          expected_topology_mismatch.
start_graceful_failover(Nodes, Opts) ->
    call({start_graceful_failover, Nodes, Opts}).

-spec stop_rebalance() -> ok | not_rebalancing.
stop_rebalance() ->
    call(stop_rebalance).

-spec start_recovery(bucket_name()) ->
                            {ok, UUID, RecoveryMap} |
                            unsupported |
                            rebalance_running |
                            in_bucket_hibernation |
                            not_present |
                            not_needed |
                            {error, {failed_nodes, [node()]}} |
                            {error, {janitor_error, any()}}
                                when UUID :: binary(),
                                     RecoveryMap :: dict:dict().
start_recovery(Bucket) ->
    call({start_recovery, Bucket}).

-spec recovery_status() -> not_in_recovery | {ok, Status}
                               when Status :: [{bucket, bucket_name()} |
                                               {uuid, binary()} |
                                               {recovery_map, RecoveryMap}],
                                    RecoveryMap :: dict:dict().
recovery_status() ->
    case is_recovery_running() of
        false ->
            not_in_recovery;
        _ ->
            call(recovery_status)
    end.

-spec recovery_map(bucket_name(), UUID) -> bad_recovery | {ok, RecoveryMap}
                                               when RecoveryMap :: dict:dict(),
                                                    UUID :: binary().
recovery_map(Bucket, UUID) ->
    call({recovery_map, Bucket, UUID}).

-spec commit_vbucket(bucket_name(), UUID, vbucket_id()) ->
                            ok | recovery_completed |
                            vbucket_not_found | bad_recovery |
                            {error, {failed_nodes, [node()]}}
                                when UUID :: binary().
commit_vbucket(Bucket, UUID, VBucket) ->
    call({commit_vbucket, Bucket, UUID, VBucket}).

-spec stop_recovery(bucket_name(), UUID) -> ok | bad_recovery |
                                            in_bucket_hibernation
                                              when UUID :: binary().
stop_recovery(Bucket, UUID) ->
    call({stop_recovery, Bucket, UUID}).

-spec is_recovery_running() -> boolean().
is_recovery_running() ->
    recovery_server:is_recovery_running().

%%
%% gen_statem callbacks
%%

callback_mode() ->
    handle_event_function.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

init([]) ->
    process_flag(trap_exit, true),

    {ok, idle, #idle_state{}, {{timeout, janitor}, 0, run_janitor}}.

handle_event({call, From}, get_state, StateName, _State) ->
    {keep_state_and_data, [{reply, From, StateName}]};

handle_event({call, From}, recovery_status, StateName, State) ->
    case StateName of
        recovery ->
            ?MODULE:recovery(recovery_status, From, State);
        _ ->
            {keep_state_and_data, [{reply, From, not_in_recovery}]}
    end;

handle_event({call, From}, Msg, StateName, State)
    when element(1, Msg) =:= recovery_map;
         element(1, Msg) =:= commit_vbucket;
         element(1, Msg) =:= stop_recovery ->
    case StateName of
        recovery ->
            ?MODULE:recovery(Msg, From, State);
        _ ->
            {keep_state_and_data, [{reply, From, bad_recovery}]}
    end;

handle_event({call, From}, EventData, ejecting, State) ->
    ejecting(From, EventData, State);

%% called remotely from pre-7.6 nodes
handle_event({call, From},
             {maybe_start_rebalance, KnownNodes, EjectedNodes,
              DeltaRecoveryBuckets}, _StateName, _State) ->
    {keep_state_and_data,
     [{next_event, {call, From},
       {maybe_start_rebalance,
        #{known_nodes => KnownNodes,
          eject_nodes => EjectedNodes,
          delta_recovery_buckets => DeltaRecoveryBuckets,
          services => all}}}]};

handle_event({call, From}, {maybe_start_rebalance,
                            Params = #{known_nodes := KnownNodes,
                                       eject_nodes := EjectedNodes,
                                       services := Services}},
             _StateName, _State) ->
    NewParams =
        case maps:is_key(id, Params) of
            false ->
                auto_rebalance:cancel_any_pending_retry_async(
                  "manual rebalance"),
                Params#{id => couch_uuids:random()};
            true ->
                Params
        end,

    Snapshot = chronicle_compat:get_snapshot(
                 [ns_bucket:fetch_snapshot(all, _, [uuid, props]),
                  ns_cluster_membership:fetch_snapshot(_),
                  chronicle_master:fetch_snapshot(_)],
                 #{read_consistency => quorum}),

    try
        case {EjectedNodes -- KnownNodes,
              lists:sort(ns_cluster_membership:nodes_wanted(Snapshot)),
              lists:sort(KnownNodes)} of
            {[], X, X} ->
                ok;
            _ ->
                throw(nodes_mismatch)
        end,
        MaybeKeepNodes = KnownNodes -- EjectedNodes,
        FailedNodes = get_failed_nodes(Snapshot, KnownNodes),
        KeepNodes = MaybeKeepNodes -- FailedNodes,
        DeltaNodes = get_delta_recovery_nodes(Snapshot, KeepNodes),

        KeepNodes =/= [] orelse throw(no_active_nodes_left),
        case rebalance_allowed(Snapshot) of
            ok -> ok;
            {error, Msg} ->
                set_rebalance_status(rebalance, {none, Msg}, undefined),
                throw({rebalance_not_allowed, Msg})
        end,
        NewChk = case retry_ok(Snapshot, FailedNodes, NewParams) of
                     false ->
                         throw(retry_check_failed);
                     Other ->
                         Other
                 end,
        EjectedLiveNodes = EjectedNodes -- FailedNodes,

        ServiceNodesMap = ns_rebalancer:get_desired_services_nodes(Params),
        validate_services_nodes(ServiceNodesMap, KeepNodes, DeltaNodes,
                                FailedNodes),

        validate_services(Services, EjectedLiveNodes, DeltaNodes, Snapshot,
                          ServiceNodesMap),

        validate_quotas(ServiceNodesMap, Params, Snapshot),

        NewParams1 = validate_rebalance_plan(NewParams, KeepNodes, Snapshot),

        %% with both possible outcomes (success or failure)
        %% the plan becomes invalid so we just delete it here after it is
        %% validated and stored in parameters
        case erlang:get(?FUSION_REBALANCE_PLAN) of
            undefined ->
                ok;
            _ ->
                ?rebalance_info("Delete stored rebalance plan"),
                erlang:erase(?FUSION_REBALANCE_PLAN)
        end,

        NewParams2 = NewParams1#{keep_nodes => KeepNodes,
                                 eject_nodes => EjectedLiveNodes,
                                 failed_nodes => FailedNodes,
                                 delta_nodes => DeltaNodes,
                                 chk => NewChk},
        {keep_state_and_data,
         [{next_event, {call, From}, {start_rebalance, NewParams2}}]}
    catch
        throw:Error -> {keep_state_and_data, [{reply, From, Error}]}
    end;

handle_event({call, From},
             {maybe_retry_graceful_failover, Nodes, Opts, Id, Chk},
             _StateName, _State) ->
    case graceful_failover_retry_ok(Chk) of
        false ->
            {keep_state_and_data, [{reply, From, retry_check_failed}]};
        Chk ->
            StartEvent = {start_graceful_failover, Nodes, Opts, Id, Chk},
            {keep_state_and_data, [{next_event, {call, From}, StartEvent}]}
    end;

handle_event(info, Event, StateName, StateData)->
    handle_info(Event, StateName, StateData);
handle_event(cast, Event, StateName, StateData) ->
    ?MODULE:StateName(Event, StateData);
handle_event({call, From}, Event, StateName, StateData) ->
    ?MODULE:StateName(Event, From, StateData);

handle_event({timeout, janitor}, run_janitor, idle, _State) ->
    {ok, ID} = ns_janitor_server:start_cleanup(
                 fun(Pid, UnsafeNodes, CleanupID) ->
                         Pid ! {cleanup_done, UnsafeNodes, CleanupID},
                         ok
                 end),
    {next_state, janitor_running, #janitor_state{cleanup_id = ID},
     {{timeout, janitor}, ?JANITOR_INTERVAL, run_janitor}};

handle_event({timeout, janitor}, run_janitor, StateName, _StateData) ->
    ?log_info("Skipping janitor in state ~p", [StateName]),
    {keep_state_and_data,
     {{timeout, janitor}, ?JANITOR_INTERVAL, run_janitor}}.

handle_info({'EXIT', Pid, Reason}, rebalancing,
            #rebalancing_state{rebalancer = Pid} = State) ->
    handle_rebalance_completion(Reason, State);

handle_info({'EXIT', ObserverPid, Reason}, rebalancing,
            #rebalancing_state{rebalance_observer = ObserverPid} = State) ->
    {keep_state, stop_rebalance(State, {rebalance_observer_terminated, Reason})};

handle_info({'EXIT', Pid, Reason}, recovery, #recovery_state{pid = Pid}) ->
    ale:error(?USER_LOGGER,
              "Recovery process ~p terminated unexpectedly: ~p", [Pid, Reason]),
    {next_state, idle, #idle_state{}};

handle_info({cleanup_done, UnsafeNodes, ID}, janitor_running,
            #janitor_state{cleanup_id = CleanupID}) ->
    %% If we get here we don't expect the IDs to be different.
    ID = CleanupID,

    %% If any 'unsafe nodes' were found then trigger an auto_reprovision
    %% operation via the orchestrator.
    MaybeNewTimeout = case UnsafeNodes =/= [] of
                          true ->
                              %% The unsafe nodes only affect the ephemeral
                              %% buckets.
                              Buckets = ns_bucket:get_bucket_names_of_type(
                                          {membase, ephemeral}),
                              RV = auto_reprovision:reprovision_buckets(
                                     Buckets, UnsafeNodes),
                              ?log_info("auto_reprovision status = ~p "
                                        "(Buckets = ~p, UnsafeNodes = ~p)",
                                        [RV, Buckets, UnsafeNodes]),

                              %% Trigger the janitor cleanup immediately as
                              %% the buckets need to be brought online.
                              [{{timeout, janitor}, 0, run_janitor}];
                          false ->
                              []
                      end,
    {next_state, idle, #idle_state{}, MaybeNewTimeout};

handle_info({timeout, _TRef, stop_timeout} = Msg, rebalancing, StateData) ->
    ?MODULE:rebalancing(Msg, StateData);

handle_info(Msg, bucket_hibernation, StateData) ->
    handle_info_in_bucket_hibernation(Msg, StateData);

handle_info(Msg, buckets_shutdown, StateData) ->
    handle_info_in_buckets_shutdown(Msg, StateData);

handle_info(Msg, StateName, StateData) ->
    ?log_warning("Got unexpected message ~p in state ~p with data ~p",
                 [Msg, StateName, StateData]),
    keep_state_and_data.

terminate(_Reason, _StateName, _StateData) ->
    ok.

%%
%% States
%%

%% Asynchronous idle events
idle({request_janitor_run, Item}, State) ->
    do_request_janitor_run(Item, idle, State);
idle(_Event, _State) ->
    %% This will catch stray progress messages
    keep_state_and_data.

janitor_running({request_janitor_run, Item}, State) ->
    do_request_janitor_run(Item, janitor_running, State);
janitor_running(_Event, _State) ->
    keep_state_and_data.

%% Synchronous idle events
idle({create_bucket, BucketType, BucketName, BucketConfig}, From, _State) ->
    maybe
        {ok, NewBucketConfig} ?=
            validate_create_bucket(BucketName, BucketType, BucketConfig),
        {ok, UUID, ActualBucketConfig} ?=
            ns_bucket:create_bucket(BucketType, BucketName, NewBucketConfig),
        ConfigJSON = ns_bucket:build_bucket_props_json(
                       ns_bucket:extract_bucket_props(ActualBucketConfig)),
        master_activity_events:note_bucket_creation(BucketName, BucketType,
                                                    ConfigJSON),
        event_log:add_log(
          bucket_created,
          [{bucket, list_to_binary(BucketName)},
           {bucket_uuid, UUID},
           {bucket_type, ns_bucket:display_type(ActualBucketConfig)},
           {bucket_props, {ConfigJSON}}]),
        request_janitor_run({bucket, BucketName}),
        {keep_state_and_data, [{reply, From, ok}]}
    else
        {error, _} = Error ->
            {keep_state_and_data, [{reply, From, Error}]}
    end;
idle({flush_bucket, BucketName}, From, _State) ->
    RV = perform_bucket_flushing(BucketName),
    case RV of
        ok -> ok;
        {flush_wait_failed, _FailedNodes, _FailedCalls} ->
            ale:info(?USER_LOGGER,
                     "Flushing ~p failed or timed out with error: ~n~p",
                     [BucketName, RV]);
        _ ->
            ale:info(?USER_LOGGER, "Flushing ~p failed with error: ~n~p",
                     [BucketName, RV])
    end,
    {keep_state_and_data, [{reply, From, RV}]};
idle({delete_bucket, BucketName}, From, _State) ->
    handle_delete_bucket(BucketName, From, idle, []);
%% In the mixed mode, depending upon the node from which the update bucket
%% request is being sent, the length of the message could vary. In order to
%% be backward compatible we need to field both types of messages.
idle({update_bucket, memcached, BucketName, UpdatedProps}, From, _State) ->
    {keep_state_and_data,
     [{next_event, {call, From},
       {update_bucket, memcached, undefined, BucketName, UpdatedProps}}]};
idle({update_bucket, membase, BucketName, UpdatedProps}, From, _State) ->
    {keep_state_and_data,
     [{next_event, {call, From},
       {update_bucket, membase, couchstore, BucketName, UpdatedProps}}]};
idle({update_bucket,
      BucketType, StorageMode, BucketName, UpdatedProps}, From, _State) ->
    Reply =
        case bucket_placer:place_bucket(BucketName, UpdatedProps) of
            {ok, NewUpdatedProps} ->
                ns_bucket:update_bucket_props(
                  BucketType, StorageMode, BucketName, NewUpdatedProps);
            {error, BadZones} ->
                {error, {need_more_space, BadZones}}
        end,
    case Reply of
        ok ->
            %% request janitor run to fix map if the replica # has
            %% changed
            request_janitor_run({bucket, BucketName});
        _ ->
            ok
    end,

    {keep_state_and_data, [{reply, From, Reply}]};
idle({failover, Node}, From, _State) ->
    %% calls from pre-5.5 nodes
    {keep_state_and_data,
     [{next_event, {call, From}, {failover, [Node], false}}]};
idle({failover, Nodes, AllowUnsafe}, From, _State)
  when is_boolean(AllowUnsafe) ->
    %% calls from pre-morpheus nodes
    idle({failover, Nodes, #{allow_unsafe => AllowUnsafe}}, From, _State);
idle({failover, Nodes, Options}, From, _State) when is_map(Options) ->
    handle_start_failover(Nodes, From, true, hard_failover, Options);
idle({start_failover, Nodes, Options}, From, _State) ->
    handle_start_failover(Nodes, From, false, hard_failover, Options);
idle({try_autofailover, Nodes, #{down_nodes := DownNodes} = Options},
     From, _State) ->
    Snapshot = failover:get_snapshot(),
    case auto_failover:validate_kv(Snapshot, Nodes, DownNodes) of
        {unsafe, UnsafeBuckets} ->
            {keep_state_and_data,
             [{reply, From, {autofailover_unsafe, UnsafeBuckets}}]};
        {nodes_down, NodesNeeded, Buckets} ->
            {keep_state_and_data,
             [{reply, From, {nodes_down, NodesNeeded, Buckets}}]};
        {cannot_preserve_durability_majority, Buckets} ->
            {keep_state_and_data,
             [{reply, From, {cannot_preserve_durability_majority, Buckets}}]};
        ok ->
            %% Auto-failover should never be unsafe. We add the option here
            %% rather than in the auto_failover module so that we don't have to
            %% worry about which node auto-failover is running on. It /should/
            %% almost always be this node, the node running the orchestrator,
            %% but the requests are routed via the leader registry so we would
            %% need to take particular care around process creation and deletion
            %% to ensure that this is /always/ the case. Better to be safe than
            %% sorry, and to keep setting the option here.
            handle_start_failover(Nodes, From, true, auto_failover,
                                  Options#{allow_unsafe => false})
    end;
idle({start_graceful_failover, Nodes}, From, _State) ->
    %% calls from pre-morpheus nodes
    idle({start_graceful_failover, Nodes, #{}}, From, _State);
idle({start_graceful_failover, Nodes, Opts}, From, _State) ->
    auto_rebalance:cancel_any_pending_retry_async("graceful failover"),
    {keep_state_and_data,
     [{next_event, {call, From},
       {start_graceful_failover, Nodes, Opts, couch_uuids:random(),
        get_graceful_fo_chk()}}]};
idle({start_graceful_failover, Nodes, Opts, Id, RetryChk}, From, _State) ->
    ActiveNodes = ns_cluster_membership:active_nodes(),
    NodesInfo = [{active_nodes, ActiveNodes},
                 {failover_nodes, Nodes},
                 {master_node, node()}],
    Services = [kv],
    Type = graceful_failover,
    {ok, ObserverPid} = ns_rebalance_observer:start_link(
                          Services, NodesInfo, Type, Id),

    case ns_rebalancer:start_link_graceful_failover(Nodes, Opts) of
        {ok, Pid} ->
            ale:info(?USER_LOGGER,
                     "Starting graceful failover of nodes ~p. "
                     "Operation Id = ~s", [Nodes, Id]),
            Type = graceful_failover,
            event_log:add_log(graceful_failover_initiated,
                              [{nodes_info, {NodesInfo}},
                               {operation_id, Id}]),
            ns_cluster:counter_inc(Type, start),
            set_rebalance_status(Type, running, Pid),

            {next_state, rebalancing,
             #rebalancing_state{rebalancer = Pid,
                                rebalance_observer = ObserverPid,
                                eject_nodes = [],
                                keep_nodes = [],
                                failed_nodes = [],
                                delta_recov_bkts = [],
                                retry_check = RetryChk,
                                to_failover = Nodes,
                                abort_reason = undefined,
                                type = Type,
                                rebalance_id = Id,
                                opts = Opts},
             [{reply, From, ok}]};
        {error, RV} ->
            misc:unlink_terminate_and_wait(ObserverPid, kill),
            {keep_state_and_data, [{reply, From, RV}]}
    end;
%% NOTE: this is not remotely called but is used by maybe_start_rebalance
idle({start_rebalance, Params = #{keep_nodes := KeepNodes,
                                  eject_nodes := EjectNodes,
                                  failed_nodes := FailedNodes,
                                  delta_nodes := DeltaNodes,
                                  delta_recovery_buckets :=
                                      DeltaRecoveryBuckets,
                                  services := Services,
                                  id := RebalanceId}}, From, _State) ->

    NodesInfo = [{active_nodes, KeepNodes ++ EjectNodes},
                 {keep_nodes, KeepNodes},
                 {eject_nodes, EjectNodes},
                 {delta_nodes, DeltaNodes},
                 {failed_nodes, FailedNodes}],

    DesiredServicesNodes = ns_rebalancer:get_desired_services_nodes(Params),

    Type = rebalance,

    {ServicesToObserve, ServicesMsg} =
        case Services of
            all ->
                {ns_cluster_membership:cluster_supported_services(), []};
            Services ->
                {Services,
                 lists:flatten(io_lib:format(" Services = ~p;", [Services]))}
        end,
    {ok, ObserverPid} = ns_rebalance_observer:start_link(
                          ServicesToObserve, NodesInfo, Type, RebalanceId),
    DeltaRecoveryMsg =
        case DeltaNodes of
            [] ->
                "no delta recovery nodes";
            _ ->
                lists:flatten(
                  io_lib:format(
                    "Delta recovery nodes = ~p, Delta recovery buckets = ~p;",
                    [DeltaNodes, DeltaRecoveryBuckets]))
        end,

    MsgServicesTopology =
        case DesiredServicesNodes of
            undefined ->
                "";
            Topology ->
                lists:flatten(io_lib:format(";DesiredServiceNodes = ~p",
                                            [Topology]))
        end,

    Msg = lists:flatten(
            io_lib:format(
              "Starting rebalance, KeepNodes = ~p, EjectNodes = ~p, "
              "Failed over and being ejected nodes = ~p; ~s;~s "
              "Operation Id = ~s",
              [KeepNodes, EjectNodes, FailedNodes, DeltaRecoveryMsg,
               ServicesMsg, RebalanceId])) ++ MsgServicesTopology,

    ?log_info(Msg),
    case ns_rebalancer:start_link_rebalance(Params) of
        {ok, Pid} ->
            ale:info(?USER_LOGGER, Msg),
            TopologyParams =
                case DesiredServicesNodes of
                    undefined ->
                        [];
                    _ ->
                        [{set_services_topology,
                          {maps:to_list(DesiredServicesNodes)}}]
                end,
            event_log:add_log(rebalance_initiated,
                              [{operation_id, RebalanceId},
                               {nodes_info, {NodesInfo}},
                               {services, Services}] ++ TopologyParams),
            ns_cluster:counter_inc(Type, start),
            set_rebalance_status(Type, running, Pid),
            ReturnValue =
                case cluster_compat_mode:is_cluster_76() of
                    true ->
                        {ok, RebalanceId};
                    false ->
                        ok
                end,

            {next_state, rebalancing,
             #rebalancing_state{rebalancer = Pid,
                                rebalance_observer = ObserverPid,
                                keep_nodes = KeepNodes,
                                eject_nodes = EjectNodes,
                                failed_nodes = FailedNodes,
                                delta_recov_bkts = DeltaRecoveryBuckets,
                                retry_check = maps:get(chk, Params, undefined),
                                to_failover = [],
                                abort_reason = undefined,
                                type = Type,
                                rebalance_id = RebalanceId,
                                opts = Params},
             [{reply, From, ReturnValue}]};
        {error, Error} ->
            ?log_info("Rebalance ~p was not started due to error: ~p",
                      [RebalanceId, Error]),
            misc:unlink_terminate_and_wait(ObserverPid, kill),
            {keep_state_and_data, [{reply, From, Error}]}
    end;
idle({move_vbuckets, Bucket, Moves}, From, _State) ->
    Id = couch_uuids:random(),
    KeepNodes = ns_node_disco:nodes_wanted(),
    Type = move_vbuckets,
    NodesInfo = [{active_nodes, ns_cluster_membership:active_nodes()},
                 {keep_nodes, KeepNodes}],
    Services = [kv],
    {ok, ObserverPid} = ns_rebalance_observer:start_link(
                          Services, NodesInfo, Type, Id),
    Pid = spawn_link(
            fun () ->
                    ns_rebalancer:move_vbuckets(Bucket, Moves)
            end),

    ?log_debug("Moving vBuckets in bucket ~p. Moves ~p. "
               "Operation Id = ~s", [Bucket, Moves, Id]),
    ns_cluster:counter_inc(Type, start),
    set_rebalance_status(Type, running, Pid),

    {next_state, rebalancing,
     #rebalancing_state{rebalancer = Pid,
                        rebalance_observer = ObserverPid,
                        keep_nodes = ns_node_disco:nodes_wanted(),
                        eject_nodes = [],
                        failed_nodes = [],
                        delta_recov_bkts = [],
                        retry_check = undefined,
                        to_failover = [],
                        abort_reason = undefined,
                        type = Type,
                        rebalance_id = Id},
     [{reply, From, ok}]};
idle(stop_rebalance, From, _State) ->
    rebalance:reset_status(
      fun () ->
              ale:info(?USER_LOGGER,
                       "Resetting rebalance status since rebalance stop "
                       "was requested but rebalance isn't orchestrated on "
                       "our node"),
              none
      end),
    {keep_state_and_data, [{reply, From, not_rebalancing}]};
idle({start_recovery, Bucket}, {FromPid, _} = From, _State) ->
    case recovery_server:start_recovery(Bucket, FromPid) of
        {ok, Pid, UUID, Map} ->
            {next_state, recovery, #recovery_state{pid = Pid},
             [{reply, From, {ok, UUID, Map}}]};
        Error ->
            {keep_state_and_data, [{reply, From, Error}]}
    end;
idle({ensure_janitor_run, Item}, From, State) ->
    do_request_janitor_run(
      Item,
      fun (Reason) ->
              gen_statem:reply(From, Reason)
      end, idle, State);

idle({prepare_fusion_rebalance, KeepNodes}, From, _State) ->
    RV =
        case KeepNodes -- ns_cluster_membership:nodes_wanted() of
            [] ->
                case ns_rebalancer:prepare_fusion_rebalance(KeepNodes) of
                    {ok, {RebalancePlan, AccelerationPlan}} ->
                        erlang:put(?FUSION_REBALANCE_PLAN, RebalancePlan),
                        {ok, AccelerationPlan};
                    {error, Error} ->
                        Error
                end;
            UnknownNodes ->
                {unknown_nodes, UnknownNodes}
        end,
    {keep_state_and_data, [{reply, From, RV}]};

idle({fusion_upload_mounted_volumes, PlanUUID, Volumes}, From, _State) ->
    PlanUUIDBin = list_to_binary(PlanUUID),
    RV =
        try
            RebalancePlan =
                case erlang:get(?FUSION_REBALANCE_PLAN) of
                    undefined ->
                        throw(not_found);
                    RP ->
                        RP
                end,
            Nodes = proplists:get_value(nodes, RebalancePlan),
            PlanNodeNames = [atom_to_list(N) || N <- Nodes],
            PassedNodeNames = [N || {N, _} <- Volumes],
            case PlanNodeNames -- PassedNodeNames of
                Missing when Missing =/= [] ->
                    throw({need_nodes, Missing});
                [] -> ok
            end,
            case PassedNodeNames -- PlanNodeNames of
                Extra when Extra =/= [] ->
                    throw({extra_nodes, Extra});
                [] -> ok
            end,
            proplists:get_value(planUUID, RebalancePlan) =:= PlanUUIDBin
                orelse throw(id_mismatch),
            PreparedVolumes = [{list_to_atom(N), V} || {N, V} <- Volumes],
            ?rebalance_info(
               "Uploading mounted volumes ~p to rebalance plan ~p",
               [PreparedVolumes, PlanUUID]),
            NewPlan = lists:keystore(mountedVolumes, 1, RebalancePlan,
                                     {mountedVolumes, PreparedVolumes}),
            erlang:put(?FUSION_REBALANCE_PLAN, NewPlan),
            ok
        catch throw:E -> E
        end,
    {keep_state_and_data, [{reply, From, RV}]};

%% Start Pause/Resume bucket operations.
idle({{bucket_hibernation_op, {start, Op}},
      {#bucket_hibernation_op_args{bucket = Bucket} = Args,
       ExtraArgs}}, From, _State) ->
    Result =
        case Op of
            pause_bucket ->
                hibernation_utils:check_allow_pause_op(Bucket);
            resume_bucket ->
                [Metadata] = ExtraArgs,
                hibernation_utils:check_allow_resume_op(Bucket, Metadata)
        end,

    case Result of
        {ok, RunOpExtraArgs} ->
            run_hibernation_op(
              Op, Args, ExtraArgs ++ RunOpExtraArgs, From);
        {error, Error} ->
            {keep_state_and_data, [{reply, From, Error}]}
    end;
idle({{bucket_hibernation_op, {stop, Op}}, [_Bucket]}, From, _State) ->
    {keep_state_and_data, {reply, From, not_running(Op)}}.

%% Synchronous janitor_running events
janitor_running({ensure_janitor_run, Item}, From, State) ->
    do_request_janitor_run(
      Item,
      fun (Reason) ->
              gen_statem:reply(From, Reason)
      end, janitor_running, State);

janitor_running(Msg, From, #janitor_state{cleanup_id = ID})
  when ID =/= undefined ->
    %% When handling some call while janitor is running we kill janitor
    %% and then handle original call in idle state
    ok = ns_janitor_server:terminate_cleanup(ID),

    %% Eat up the cleanup_done message that gets sent by ns_janitor_server when
    %% the cleanup process ends.
    receive
        {cleanup_done, _, _} ->
            ok
    end,
    {next_state, idle, #idle_state{}, [{next_event, {call, From}, Msg}]}.

%% Asynchronous rebalancing events
rebalancing({timeout, _Tref, stop_timeout},
            #rebalancing_state{rebalancer = Pid} = State) ->
    ?log_debug("Stop rebalance timeout, brutal kill pid = ~p", [Pid]),
    exit(Pid, kill),
    Reason =
        receive
            {'EXIT', Pid, killed} ->
                %% still treat this as user-stopped rebalance
                {shutdown, stop};
            {'EXIT', Pid, R} ->
                R
        end,
    handle_rebalance_completion(Reason, State);
rebalancing({request_janitor_run, _Item} = Msg, _State) ->
    ?log_debug("Message ~p ignored", [Msg]),
    keep_state_and_data.

%% Synchronous rebalancing events
rebalancing({try_autofailover, Nodes, Options}, From,
            #rebalancing_state{type = Type} = State) ->
    case menelaus_web_auto_failover:config_check_can_abort_rebalance() andalso
         Type =/= failover of
        false ->
            TypeStr = binary_to_list(rebalance_type2text(Type)),
            {keep_state_and_data,
             [{reply, From, {operation_running, TypeStr}}]};
        true ->
            case stop_rebalance(State,
                                {try_autofailover, From, Nodes, Options}) of
                State ->
                    %% Unlikely event, that a user has stopped rebalance and
                    %% before rebalance has terminated we get an autofailover
                    %% request.
                    {keep_state_and_data,
                     [{reply, From, retry_aborting_rebalance}]};
                NewState ->
                    {keep_state, NewState}
            end
    end;
rebalancing({start_rebalance, _Params}, From, _State) ->
    ale:info(?USER_LOGGER,
             "Not rebalancing because rebalance is already in progress.~n"),
    {keep_state_and_data, [{reply, From, in_progress}]};
rebalancing({start_graceful_failover, _}, From, _State) ->
    {keep_state_and_data, [{reply, From, in_progress}]};
rebalancing({start_graceful_failover, _Nodes, _Options}, From, _State) ->
    {keep_state_and_data, [{reply, From, in_progress}]};
rebalancing({start_graceful_failover, _, _, _}, From, _State) ->
    {keep_state_and_data, [{reply, From, in_progress}]};
rebalancing({start_failover, _, _}, From, _State) ->
    {keep_state_and_data, [{reply, From, in_progress}]};
rebalancing(stop_rebalance, From,
            #rebalancing_state{rebalancer = Pid} = State) ->
    ?log_debug("Sending stop to rebalancer: ~p", [Pid]),
    {keep_state, stop_rebalance(State, user_stop), [{reply, From, ok}]};
rebalancing(Event, From, _State) ->
    ?log_warning("Got event ~p while rebalancing.", [Event]),
    {keep_state_and_data, [{reply, From, rebalance_running}]}.

%% Asynchronous recovery events
recovery(Event, _State) ->
    ?log_warning("Got unexpected event: ~p", [Event]),
    keep_state_and_data.

%% Synchronous recovery events
recovery({start_recovery, _Bucket}, From, _State) ->
    {keep_state_and_data, [{reply, From, recovery_running}]};
recovery({commit_vbucket, Bucket, UUID, VBucket}, From, State) ->
    Result = call_recovery_server(State,
                                  commit_vbucket, [Bucket, UUID, VBucket]),
    case Result of
        recovery_completed ->
            {next_state, idle, #idle_state{}, [{reply, From, Result}]};
        _ ->
            {keep_state_and_data, [{reply, From, Result}]}
    end;
recovery({stop_recovery, Bucket, UUID}, From, State) ->
    case call_recovery_server(State, stop_recovery, [Bucket, UUID]) of
        ok ->
            {next_state, idle, #idle_state{}, [{reply, From, ok}]};
        Error ->
            {keep_state_and_data, [{reply, From, Error}]}
    end;
recovery(recovery_status, From, State) ->
    {keep_state_and_data,
     [{reply, From, call_recovery_server(State, recovery_status)}]};
recovery({recovery_map, Bucket, RecoveryUUID}, From, State) ->
    {keep_state_and_data,
     [{reply, From,
       call_recovery_server(State, recovery_map, [Bucket, RecoveryUUID])}]};

recovery(stop_rebalance, From, _State) ->
    {keep_state_and_data, [{reply, From, not_rebalancing}]};
recovery(_Event, From, _State) ->
    {keep_state_and_data, [{reply, From, in_recovery}]}.

bucket_hibernation({try_autofailover, Nodes, Options}, From, State) ->
    {keep_state, stop_bucket_hibernation_op(
                   State, {try_autofailover, From, Nodes, Options})};

bucket_hibernation({{bucket_hibernation_op, {stop, Op}} = Msg, [Bucket]}, From,
                   #bucket_hibernation_state{
                      op = Op,
                      bucket = Bucket} = State) ->
    {keep_state, stop_bucket_hibernation_op(State, Msg),
     [{reply, From, ok}]};

%% Handle the cases when {stop, Op} doesn't match the current running Op, i.e:
%% 1. {stop, pause_bucket} while resume_bucket is running.
%% 2. {stop, resume_bucket} while pause_bucket is running.
%% 3. {stop, pause_bucket}/{stop, resume_bucket} for a bucket that isn't
%%    currently being paused/resumed.

bucket_hibernation({{bucket_hibernation_op, {stop, Op}}, [Bucket]}, From,
                   #bucket_hibernation_state{bucket = HibernatingBucket,
                                             op = RunningOp}) ->
    Reply =
        if
            Op =:= RunningOp ->
                bucket_not_found;
            Bucket =:= HibernatingBucket ->
                not_running(Op);
            true ->
                [bucket_not_found, not_running(Op)]
        end,

    {keep_state_and_data, [{reply, From, Reply}]};

bucket_hibernation(
  {{bucket_hibernation_op, {start, _Op}}, _Args},
  From, State) ->
    {keep_state_and_data, [{reply, From, build_error(State)}]};

%% Handle other msgs that come while ns_orchestrator is in the
%% bucket_hibernation_state.

bucket_hibernation(stop_rebalance, From, _State) ->
    {keep_state_and_data, [{reply, From, not_rebalancing}]};
bucket_hibernation(_Msg, From, _State) ->
    {keep_state_and_data, [{reply, From, in_bucket_hibernation}]}.

ejecting(Event, _State) ->
    ?log_info("Ignoring event ~p while leaving the cluster.", [Event]),
    keep_state_and_data.

ejecting({{bucket_hibernation_op, {stop, Op}}, [_Bucket]}, From, _State) ->
    {keep_state_and_data, {reply, From, not_running(Op)}};
ejecting(stop_rebalance, From, _State) ->
    {keep_state_and_data, [{reply, From, not_rebalancing}]};
ejecting(_, From, _State) ->
    {keep_state_and_data, [{reply, From, {orchestrator_ejecting, self()}}]}.

build_error(#bucket_hibernation_state{
              bucket = Bucket,
              op = Op}) ->
    {[{<<"message">>,
       <<"Cannot pause/resume bucket, while another bucket is "
         "being paused/resumed.">>},
      {<<"bucket">>, list_to_binary(Bucket)},
      {<<"op">>, Op}]}.

buckets_shutdown({try_autofailover, Nodes, Options}, From, State) ->
    lists:foreach(
      fun (#bucket_shutdown_ctx{
              from = DeleteBucketFrom,
              pid = Pid,
              bucket_name = BucketName}) ->
              ?log_warning("Bucket shutdown interrupted by auto-failover. "
                           "Bucket - ~p", [BucketName]),
              misc:unlink_terminate_and_wait(Pid, kill),
              gen_statem:reply(DeleteBucketFrom, shutdown_incomplete)
      end, State),
    maybe_try_autofailover_in_idle_state(
      {try_autofailover, From, Nodes, Options});
buckets_shutdown({delete_bucket, BucketName}, From, State) ->
    handle_delete_bucket(BucketName, From, buckets_shutdown, State);
buckets_shutdown(Msg, From, State) ->
    ?log_debug("Ignore Msg: ~p in State: ~p", [Msg, State]),
    {keep_state_and_data, [{reply, From, in_buckets_shutdown}]}.
%%
%% Internal functions
%%
stop_rebalance(#rebalancing_state{rebalancer = Pid,
                                  abort_reason = undefined} = State, Reason) ->
    exit(Pid, {shutdown, stop}),
    TRef = erlang:start_timer(?STOP_REBALANCE_TIMEOUT, self(), stop_timeout),
    State#rebalancing_state{stop_timer = TRef, abort_reason = Reason};
stop_rebalance(State, _Reason) ->
    %% Do nothing someone has already tried to stop rebalance.
    State.

do_request_janitor_run(Item, FsmState, State) ->
    do_request_janitor_run(Item, fun(_Reason) -> ok end,
                           FsmState, State).

do_request_janitor_run(Item, Fun, FsmState, State) ->
    RV = ns_janitor_server:request_janitor_run({Item, [Fun]}),
    MaybeNewTimeout = case FsmState =:= idle andalso RV =:= added of
                          true ->
                              [{{timeout, janitor}, 0, run_janitor}];
                          false ->
                              []
                      end,
    {next_state, FsmState, State, MaybeNewTimeout}.

run_hibernation_op(Op,
                   #bucket_hibernation_op_args{
                      bucket = Bucket,
                      remote_path = RemotePath,
                      blob_storage_region = BlobStorageRegion,
                      rate_limit = RateLimit} = Args,
                   ExtraArgs, From) ->
    hibernation_utils:log_hibernation_event(initiated, Op, Bucket),

    Manager = hibernation_manager:run_op(Op, Args, ExtraArgs),

    ale:info(?USER_LOGGER, "Starting hibernation operation (~p) for bucket: "
             "~p, RemotePath - ~p, BlobStorageRegion - ~p, "
             "RateLimit - ~.2f MiB/s.",
             [Op, Bucket, RemotePath, BlobStorageRegion, RateLimit / ?MIB]),

    hibernation_utils:set_hibernation_status(Bucket, {Op, running}),
    {next_state, bucket_hibernation,
     #bucket_hibernation_state{hibernation_manager = Manager,
                               bucket = Bucket,
                               op = Op},
     [{reply, From, ok}]}.

perform_bucket_flushing(BucketName) ->
    case ns_bucket:get_bucket(BucketName) of
        not_present ->
            bucket_not_found;
        {ok, BucketConfig} ->
            case proplists:get_value(flush_enabled, BucketConfig, false) of
                true ->
                    RV = perform_bucket_flushing_with_config(BucketName,
                                                             BucketConfig),
                    case RV of
                        ok ->
                            UUID = ns_bucket:uuid(BucketName, direct),
                            event_log:add_log(
                              bucket_flushed,
                              [{bucket, list_to_binary(BucketName)},
                               {bucket_uuid, UUID}]),
                            ok;

                        _ ->
                            RV
                    end;
                false ->
                    flush_disabled
            end
    end.


perform_bucket_flushing_with_config(BucketName, BucketConfig) ->
    ale:info(?MENELAUS_LOGGER, "Flushing bucket ~p from node ~p",
             [BucketName, erlang:node()]),
    case ns_bucket:bucket_type(BucketConfig) =:= memcached of
        true ->
            do_flush_old_style(BucketName, BucketConfig);
        _ ->
            RV = do_flush_bucket(BucketName, BucketConfig),
            case RV of
                ok ->
                    ?log_info("Requesting janitor run to actually "
                              "revive bucket ~p after flush", [BucketName]),
                    JanitorRV = ns_janitor:cleanup(
                                  BucketName, [{query_states_timeout, 1000}]),
                    case JanitorRV of
                        ok -> ok;
                        _ ->
                            ?log_error("Flusher's janitor run failed: ~p",
                                       [JanitorRV])
                    end,
                    RV;
                {error, Reason} ->
                    Reason
            end
    end.

do_flush_bucket(BucketName, BucketConfig) ->
    Nodes = ns_bucket:get_servers(BucketConfig),
    case janitor_agent:mass_prepare_flush(BucketName, Nodes) of
        {_, [], []} ->
            continue_flush_bucket(BucketName, BucketConfig, Nodes);
        {_, BadResults, BadNodes} ->
            %% NOTE: I'd like to undo prepared flush on good
            %% nodes, but given we've lost information whether
            %% janitor ever marked them as warmed up I
            %% cannot. We'll do it after some partial
            %% janitoring support is achieved. And for now
            %% we'll rely on janitor cleaning things up.
            {error, {prepare_flush_failed, BadNodes, BadResults}}
    end.

continue_flush_bucket(BucketName, BucketConfig, Nodes) ->
    OldFlushCount = proplists:get_value(flushseq, BucketConfig, 0),
    NewConfig = lists:keystore(flushseq, 1, BucketConfig,
                               {flushseq, OldFlushCount + 1}),
    ns_bucket:set_bucket_config(BucketName, NewConfig),
    {_GoodNodes, FailedCalls, FailedNodes} =
        janitor_agent:complete_flush(BucketName, Nodes, ?FLUSH_BUCKET_TIMEOUT),
    case FailedCalls =:= [] andalso FailedNodes =:= [] of
        true ->
            ok;
        _ ->
            {error, {flush_wait_failed, FailedNodes, FailedCalls}}
    end.

do_flush_old_style(BucketName, BucketConfig) ->
    Nodes = ns_bucket:get_servers(BucketConfig),
    {Results, BadNodes} =
        rpc:multicall(Nodes, ns_memcached, flush, [BucketName],
                      ?MULTICALL_DEFAULT_TIMEOUT),
    case BadNodes =:= [] andalso lists:all(fun(A) -> A =:= ok end, Results) of
        true ->
            ok;
        false ->
            {old_style_flush_failed, Results, BadNodes}
    end.

set_rebalance_status(move_vbuckets, Status, Pid) ->
    set_rebalance_status(rebalance, Status, Pid);
set_rebalance_status(service_upgrade, Status, Pid) ->
    set_rebalance_status(rebalance, Status, Pid);
set_rebalance_status(Type, Status, Pid) ->
    rebalance:set_status(Type, Status, Pid).

cancel_stop_timer(TRef) ->
    do_cancel_stop_timer(TRef).

do_cancel_stop_timer(undefined) ->
    ok;
do_cancel_stop_timer(TRef) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    receive {timeout, TRef, _} -> 0
    after 0 -> ok
    end.

maybe_try_autofailover(StopReason, EjectionInProgress) ->
    {NextState, NextStateData} =
        case EjectionInProgress andalso
            cluster_compat_mode:is_cluster_76() of
            true ->
                {ejecting, #ejecting_state{}};
            false ->
                {idle, #idle_state{}}
        end,
    maybe_try_autofailover_in_idle_state(StopReason, NextState, NextStateData).

maybe_try_autofailover_in_idle_state(StopReason) ->
    maybe_try_autofailover_in_idle_state(StopReason, idle, #idle_state{}).

maybe_try_autofailover_in_idle_state({try_autofailover, From, Nodes, Options},
                                     idle , NextStateData) ->
    {next_state, idle, NextStateData,
     [{next_event, {call, From}, {try_autofailover, Nodes, Options}}]};
maybe_try_autofailover_in_idle_state({try_autofailover, From, _Nodes, _Options},
                                     ejecting, NextStateData) ->
    {next_state, ejecting, NextStateData,
     [{reply, From, {orchestrator_ejecting, self()}}]};
maybe_try_autofailover_in_idle_state(_, NextState, NextStateData) ->
    {next_state, NextState, NextStateData}.

terminate_observer(#rebalancing_state{rebalance_observer = undefined}) ->
    ok;
terminate_observer(#rebalancing_state{rebalance_observer = ObserverPid}) ->
    misc:unlink_terminate_and_wait(ObserverPid, kill).

handle_rebalance_completion({shutdown, {ok, _}} = ExitReason, State) ->
    handle_rebalance_completion(normal, ExitReason, State);
handle_rebalance_completion(ExitReason, State) ->
    handle_rebalance_completion(ExitReason, ExitReason, State).

handle_rebalance_completion(ExitReason, ToReply, State) ->
    cancel_stop_timer(State#rebalancing_state.stop_timer),
    maybe_reset_autofailover_count(ExitReason, State),
    maybe_reset_reprovision_count(ExitReason, State),
    {ResultType, Msg} = log_rebalance_completion(ExitReason, State),
    maybe_retry_rebalance(ExitReason, State),
    update_rebalance_counters(ExitReason, State),
    ns_rebalance_observer:record_rebalance_report(
      {ResultType, list_to_binary(Msg)}),
    update_rebalance_status(ExitReason, State),
    terminate_observer(State),
    maybe_reply_to(ToReply, State),
    maybe_request_janitor_run(ExitReason, State),

    R = compat_mode_manager:consider_switching_compat_mode(),
    ns_bucket:maybe_remove_vbucket_map_history(),
    case maybe_start_service_upgrader(ExitReason, R, State) of
        {started, NewState} ->
            {next_state, rebalancing, NewState};
        not_needed ->
            EjectionInProgress = maybe_eject_myself(ExitReason, State),
            %% Use the reason for aborting rebalance here, and not the reason
            %% for exit, we should base our next state and following activities
            %% based on the reason for aborting rebalance.
            maybe_try_autofailover(
              State#rebalancing_state.abort_reason, EjectionInProgress)
    end.

maybe_request_janitor_run({failover_failed, Bucket, _},
                          #rebalancing_state{type = failover}) ->
    ?log_debug("Requesting janitor run for bucket ~p after unsuccessful "
               "failover", [Bucket]),
    request_janitor_run({bucket, Bucket});
maybe_request_janitor_run(_, _) ->
    ok.

maybe_retry_rebalance(ExitReason,
                      #rebalancing_state{type = Type,
                                         rebalance_id = ID} = State) ->
    case retry_rebalance(ExitReason, State) of
        true ->
            ok;
        false ->
            %% Cancel retry if there is one pending from previous failure.
            By = binary_to_list(rebalance_type2text(Type)) ++ " completion",
            auto_rebalance:cancel_pending_retry_async(ID, By)
    end.

retry_rebalance(normal, _State) ->
    false;
retry_rebalance({shutdown, stop}, _State) ->
    false;
retry_rebalance(_, #rebalancing_state{type = rebalance,
                                      keep_nodes = KNs,
                                      eject_nodes = ENs,
                                      failed_nodes = FNs,
                                      delta_recov_bkts = DRBkts,
                                      retry_check = Chk,
                                      rebalance_id = Id,
                                      opts = Opts}) ->
    case lists:member(node(), FNs) of
        true ->
            ?log_debug("Orchestrator is one of the failed nodes "
                       "and may be ejected. "
                       "Failed rebalance with Id = ~s will not be retried.",
                       [Id]),
            false;
        false ->
            %% Restore the KnownNodes & EjectedNodes to the way they were
            %% at the start of this rebalance.
            EjectedNodes0 = FNs ++ ENs,
            KnownNodes0 = EjectedNodes0 ++ KNs,

            %% Rebalance may have ejected some nodes before failing.
            EjectedByReb = KnownNodes0 -- ns_node_disco:nodes_wanted(),

            %% KnownNodes0 was equal to ns_node_disco:nodes_wanted()
            %% at the start of this rebalance. So, EjectedByReb
            %% will be the nodes that have been ejected by this rebalance.
            %% These will be the nodes in either the failed nodes or eject
            %% nodes list.
            %% As an extra sanity check verify that there are no
            %% additional nodes in EjectedByReb.
            case EjectedByReb -- EjectedNodes0 of
                [] ->
                    KnownNodes = KnownNodes0 -- EjectedByReb,
                    EjectedNodes = EjectedNodes0 -- EjectedByReb,

                    NewChk = update_retry_check(EjectedByReb, Chk),
                    Params0 = [{known_nodes,  KnownNodes},
                               {eject_nodes, EjectedNodes},
                               {delta_recovery_buckets, DRBkts}],

                    %% TODO: Ideally we should move to put all of the params in
                    %% the opts map such that we can just pass that in, but that
                    %% requires a bit more refactoring.
                    Params =
                        case maps:get(expected_topology, Opts, []) of
                            undefined ->
                                Params0;
                            TopologyArgs ->
                                [{expected_topology, TopologyArgs}] ++
                                    Params0
                        end,

                    auto_rebalance:retry_rebalance(rebalance, Params, Id,
                                                   NewChk);

                Extras ->
                    ale:info(?USER_LOGGER,
                             "~p nodes have been removed from the "
                             "nodes_wanted() list. This is not expected. "
                             "Rebalance with Id ~s will not be retried.",
                             [Extras, Id]),
                    false
            end
    end;

retry_rebalance(_, #rebalancing_state{type = graceful_failover,
                                      to_failover = Nodes,
                                      retry_check = Chk,
                                      rebalance_id = Id,
                                      opts = Opts}) ->
    auto_rebalance:retry_rebalance(graceful_failover,
                                   [{nodes, Nodes}, {opts, Opts}],
                                   Id, Chk);

retry_rebalance(_, _) ->
    false.

%% Fail the retry if there are newly failed over nodes,
%% server group configuration has changed or buckets have been added
%% or deleted or their replica count changed.
retry_ok(Snapshot, FailedNodes, #{chk := RetryChk}) ->
    retry_ok(RetryChk, get_retry_check(Snapshot, FailedNodes));
retry_ok(Snapshot, FailedNodes, _) ->
    get_retry_check(Snapshot, FailedNodes).

retry_ok(Chk, Chk) ->
    Chk;
retry_ok(RetryChk, NewChk) ->
    ?log_debug("Retry check failed. (RetryChk -- NewChk): ~p~n"
               "(NewChk -- RetryChk): ~p",
               [RetryChk -- NewChk, NewChk -- RetryChk]),
    false.

get_retry_check(Snapshot, FailedNodes) ->
    SGs = ns_cluster_membership:server_groups(Snapshot),
    [{failed_nodes, lists:sort(FailedNodes)},
     {server_groups, groups_chk(SGs, fun (Nodes) -> Nodes end)},
     {buckets, buckets_chk(Snapshot)}].

buckets_chk(Snapshot) ->
    Bkts = lists:map(fun({B, BC}) ->
                             {B, proplists:get_value(num_replicas, BC),
                              ns_bucket:uuid(B, Snapshot)}
                     end, ns_bucket:get_buckets(Snapshot)),
    erlang:phash2(lists:sort(Bkts)).

groups_chk(SGs, UpdateFn) ->
    lists:map(
      fun (SG) ->
              Nodes = lists:sort(proplists:get_value(nodes, SG, [])),
              lists:keyreplace(nodes, 1, SG, {nodes, UpdateFn(Nodes)})
      end, SGs).

update_retry_check([], Chk0) ->
    Chk0;
update_retry_check(EjectedByReb, Chk0) ->
    ENs = lists:sort(EjectedByReb),
    FNs = proplists:get_value(failed_nodes, Chk0) -- ENs,
    Chk1 = lists:keyreplace(failed_nodes, 1, Chk0, {failed_nodes, FNs}),

    %% User may have changed server group configuration during rebalance.
    %% In that case, we want to fail the retry.
    %% So, we save the server group configuration at the start of rebalance
    %% However, we need to account for nodes ejected by rebalance itself.
    SGs0 = proplists:get_value(server_groups, Chk1),
    UpdateFn = fun (Nodes) -> Nodes -- ENs end,
    lists:keyreplace(server_groups, 1, Chk1,
                     {server_groups, groups_chk(SGs0, UpdateFn)}).

get_failed_nodes(Snapshot, KnownNodes) ->
    [N || N <- KnownNodes,
          ns_cluster_membership:get_cluster_membership(N, Snapshot)
              =:= inactiveFailed].

graceful_failover_retry_ok(Chk) ->
    retry_ok(Chk, get_graceful_fo_chk()).

get_graceful_fo_chk() ->
    Cfg = ns_config:get(),
    Snapshot = chronicle_compat:get_snapshot(
                 [ns_bucket:fetch_snapshot(all, _, [uuid, props]),
                  ns_cluster_membership:fetch_snapshot(_)],
                 #{ns_config => Cfg}),
    KnownNodes0 = ns_cluster_membership:nodes_wanted(Snapshot),
    UUIDDict = ns_config:get_node_uuid_map(Cfg),
    KnownNodes = ns_cluster_membership:attach_node_uuids(KnownNodes0, UUIDDict),
    FailedNodes = get_failed_nodes(Snapshot, KnownNodes0),
    [{known_nodes, KnownNodes}] ++ get_retry_check(Snapshot, FailedNodes).

maybe_eject_myself(Reason, State) ->
    case need_eject_myself(Reason, State) of
        true ->
            eject_myself(State),
            true;
        false ->
            false
    end.

need_eject_myself(normal, #rebalancing_state{eject_nodes = EjectNodes,
                                             failed_nodes = FailedNodes}) ->
    lists:member(node(), EjectNodes) orelse lists:member(node(), FailedNodes);
need_eject_myself(_Reason, #rebalancing_state{failed_nodes = FailedNodes}) ->
    lists:member(node(), FailedNodes).

eject_myself(#rebalancing_state{keep_nodes = KeepNodes}) ->
    ok = ns_config_rep:ensure_config_seen_by_nodes(KeepNodes),
    ns_rebalancer:eject_nodes([node()]).

maybe_reset_autofailover_count(normal, #rebalancing_state{type = rebalance}) ->
    auto_failover:reset_count_async();
maybe_reset_autofailover_count(_, _) ->
    ok.

maybe_reset_reprovision_count(normal, #rebalancing_state{type = rebalance}) ->
    auto_reprovision:reset_count();
maybe_reset_reprovision_count(_, _) ->
    ok.

log_rebalance_completion(
  ExitReason, #rebalancing_state{type = Type, abort_reason = AbortReason,
                                 rebalance_id = RebalanceId}) ->
    {ResultType, Severity, Fmt, Args} = get_log_msg(ExitReason,
                                                    Type,
                                                    AbortReason),
    ale:log(?USER_LOGGER, Severity, Fmt ++ "~nRebalance Operation Id = ~s",
            Args ++ [RebalanceId]),
    {ResultType, lists:flatten(io_lib:format(Fmt, Args))}.

% ResultType() is used to add an event log with the appropriate event-id
% via rebalance_observer.
-spec get_log_msg(any(), any(), any()) -> {ResultType :: success | failure |
                                           interrupted,
                                           LogLevel :: info | error,
                                           Fmt :: io:format(),
                                           Args :: [term()]}.

get_log_msg(normal, Type, _) ->
    {success, info, "~s completed successfully.",
     [rebalance_type2text(Type)]};
get_log_msg({shutdown, stop}, Type, AbortReason) ->
    get_log_msg(AbortReason, Type);
get_log_msg(Error, Type, undefined) ->
    {failure, error, "~s exited with reason ~p.",
     [rebalance_type2text(Type), Error]};
get_log_msg(_Error, Type, AbortReason) ->
    get_log_msg(AbortReason, Type).

get_log_msg({try_autofailover, _, Nodes, _}, Type) ->
    {interrupted, info, "~s interrupted due to auto-failover of nodes ~p.",
     [rebalance_type2text(Type), Nodes]};
get_log_msg({rebalance_observer_terminated, Reason}, Type) ->
    {failure, error, "~s interrupted as observer exited with reason ~p.",
     [rebalance_type2text(Type), Reason]};
get_log_msg(user_stop, Type) ->
    {interrupted, info, "~s stopped by user.",
     [rebalance_type2text(Type)]}.

rebalance_type2text(rebalance) ->
    <<"Rebalance">>;
rebalance_type2text(move_vbuckets) ->
    rebalance_type2text(rebalance);
rebalance_type2text(failover) ->
    <<"Failover">>;
rebalance_type2text(graceful_failover) ->
    <<"Graceful failover">>;
rebalance_type2text(service_upgrade) ->
    <<"Service upgrade">>.

update_rebalance_counters(Reason, #rebalancing_state{type = Type}) ->
    %% If any new counter is added a corresponding convert_to_reported_event
    %% must be added to ns_server_stats.erl.
    Counter =
        case Reason of
            normal ->
                success;
            {shutdown, stop} ->
                stop;
            _Error ->
                fail
        end,

    ns_cluster:counter_inc(Type, Counter).

update_rebalance_status(Reason, #rebalancing_state{type = Type}) ->
    set_rebalance_status(Type, reason2status(Reason, Type), undefined).

reason2status(normal, _Type) ->
    none;
reason2status({shutdown, stop}, _Type) ->
    none;
reason2status(_Error, Type) ->
    Msg = io_lib:format(
            "~s failed. See logs for detailed reason. "
            "You can try again.",
            [rebalance_type2text(Type)]),
    {none, iolist_to_binary(Msg)}.

maybe_start_service_upgrader(normal, unchanged, _State) ->
    not_needed;
maybe_start_service_upgrader(normal, {changed, OldVersion, NewVersion},
                             #rebalancing_state{keep_nodes = KeepNodes,
                                                rebalance_id = Id} = State) ->
    Old = ns_cluster_membership:topology_aware_services_for_version(OldVersion),
    New = ns_cluster_membership:topology_aware_services_for_version(NewVersion),

    Services = [S || S <- New -- Old,
                     ns_cluster_membership:service_nodes(KeepNodes, S) =/= []],
    case Services of
        [] ->
            not_needed;
        _ ->
            ale:info(?USER_LOGGER,
                     "Starting upgrade for the following services: ~p",
                     [Services]),
            Type = service_upgrade,
            NodesInfo = [{active_nodes, KeepNodes},
                         {keep_nodes, KeepNodes}],
            {ok, ObserverPid} = ns_rebalance_observer:start_link(
                                  Services, NodesInfo, Type, Id),
            Pid = start_service_upgrader(KeepNodes, Services),

            set_rebalance_status(Type, running, Pid),
            ns_cluster:counter_inc(Type, start),
            NewState = State#rebalancing_state{type = Type,
                                               rebalance_observer = ObserverPid,
                                               rebalancer = Pid},

            {started, NewState}
    end;
maybe_start_service_upgrader(_Reason, _SwitchCompatResult, _State) ->
    %% rebalance failed, so we'll just let the user start rebalance again
    not_needed.

start_service_upgrader(KeepNodes, Services) ->
    proc_lib:spawn_link(
      fun () ->
              ok = leader_activities:run_activity(
                     service_upgrader, majority,
                     fun () ->
                             service_upgrader_body(Services, KeepNodes)
                     end)
      end).

service_upgrader_body(Services, KeepNodes) ->
    ok = service_janitor:cleanup(),

    %% since we are not actually ejecting anything here, we can ignore the
    %% return value
    EjectNodes = [],
    _ = ns_rebalancer:rebalance_topology_aware_services(
          Services, KeepNodes, EjectNodes),
    ok.

call_recovery_server(State, Call) ->
    call_recovery_server(State, Call, []).

call_recovery_server(#recovery_state{pid = Pid}, Call, Args) ->
    erlang:apply(recovery_server, Call, [Pid | Args]).

get_delta_recovery_nodes(Snapshot, Nodes) ->
    [N || N <- Nodes,
          ns_cluster_membership:get_cluster_membership(N, Snapshot)
              =:= inactiveAdded
              andalso ns_cluster_membership:get_recovery_type(Snapshot, N)
              =:= delta].

validate_services_nodes(undefined, _, _, _) ->
    ok;
validate_services_nodes(_, _, DeltaNodes, _)
  when DeltaNodes =/= [] ->
    throw({params_mismatch,
           "Service topology change is incompatible with delta recovery"});
validate_services_nodes(_, _, _, FailedNodes)
  when FailedNodes =/= [] ->
    throw({params_mismatch,
           "Service topology change is not possible if some nodes are failed "
           "over"});
validate_services_nodes(ServiceNodesMap, KeepNodes, _, _) ->
    maps:foreach(
      fun (_, Nodes) ->
              Nodes -- KeepNodes =:= [] orelse throw(nodes_mismatch)
      end, ServiceNodesMap).

validate_services(all, _, _, _, _) ->
    ok;
validate_services(_, _, DeltaNodes, _, _) when DeltaNodes =/= [] ->
    throw({must_rebalance_services, all});
validate_services(Services, NodesToEject, [], Snapshot, ServiceNodesMap) ->
    ServicesWithNodesToChange =
        case ServiceNodesMap of
            undefined ->
                [];
            _ ->
                maps:keys(ServiceNodesMap)
        end,
    case Services -- ns_cluster_membership:hosted_services(Snapshot) of
        [] ->
            ok;
        ExtraServices ->
            case ExtraServices -- ServicesWithNodesToChange of
                [] ->
                    ok;
                ExtraServices1 ->
                    throw({unhosted_services, ExtraServices1})
            end
    end,
    case get_uninitialized_services(Services, Snapshot) ++
        get_unejected_services(Services, NodesToEject, Snapshot) ++
        (ServicesWithNodesToChange -- Services) of
        [] ->
            ok;
        NeededServices ->
            throw({must_rebalance_services, lists:usort(NeededServices)})
    end.

validate_rebalance_plan(Params, KeepNodes, Snapshot) ->
    RebalancePlan = erlang:get(?FUSION_REBALANCE_PLAN),
    Err =
        fun (Message) ->
                ?rebalance_info(
                   "Rebalance plan validation failed. ~s.~nStored plan: ~p",
                   [Message, RebalancePlan]),
                throw({invalid_rebalance_plan, Message})
        end,
    case maps:get(plan_uuid, Params, undefined) of
        undefined ->
            RebalancePlan =:= undefined orelse
                ?rebalance_info(
                   "Rebalance was called with no planUUID provided"
                   ", though the stored rebalance plan is found: ~p",
                   [RebalancePlan]),
            Params;
        PlanUUID ->
            RebalancePlan =/= undefined orelse Err("No rebalance plan stored"),

            list_to_binary(PlanUUID) =:=
                proplists:get_value(planUUID, RebalancePlan) orelse
                Err("Plan UUID's don't match"),

            Nodes = proplists:get_value(nodes, RebalancePlan),
            case Nodes -- KeepNodes of
                [] -> ok;
                Extra ->
                    Err(lists:flatten(
                          io_lib:format("Unknown nodes in rebalance plan: ~p",
                                        [Extra])))
            end,

            proplists:get_value(mountedVolumes, RebalancePlan) =/= undefined
                orelse Err("Volumes not uploaded"),

            Buckets = proplists:get_value(buckets, RebalancePlan),
            lists:foreach(
              fun ({Bucket, Props}) ->
                      case ns_bucket:get_bucket_with_revision(Bucket,
                                                              Snapshot) of
                          {ok, {_, Rev}} ->
                              proplists:get_value(revision, Props) =:= Rev
                                  orelse Err(Bucket ++ " has changed");
                          not_present ->
                              Err(Bucket ++ " is not found")
                      end
              end, Buckets),
            Params#{rebalance_plan => RebalancePlan}
    end.

get_nodes_to_change(ServiceNodesMap, KnownNodes, Snapshot) ->
    lists:usort(
      maps:fold(
        fun (Service, DesiredNodes, Acc) ->
                ServiceNodes = ns_cluster_membership:service_nodes(
                                 Snapshot, KnownNodes, Service),
                NewServiceNodes = DesiredNodes -- ServiceNodes,
                Acc ++ NewServiceNodes
        end, [], ServiceNodesMap)).

get_services_nodes_memory_data(undefined, _) ->
    {ok, undefined};
get_services_nodes_memory_data(ServiceNodesMap, KnownNodes) ->
    get_services_nodes_memory_data(ServiceNodesMap, KnownNodes,
                                   ns_cluster_membership:get_snapshot()).

get_services_nodes_memory_data(ServiceNodesMap, KnownNodes, Snapshot) ->
    InterestingNodes = get_nodes_to_change(ServiceNodesMap, KnownNodes,
                                           Snapshot),
    case ns_doctor:get_memory_data(InterestingNodes) of
        {ok, MemoryData} ->
            {ok, MemoryData};
        {error, {timeout, Missing}} ->
            ?log_error("Cannot fetch memory data from nodes ~p", [Missing]),
            {error, {nodes_down, Missing}}
    end.

validate_quotas(undefined, _, _) ->
    ok;
validate_quotas(_, #{memory_data := []}, _) ->
    ok;
validate_quotas(ServiceNodesMap, #{memory_data := MemoryData}, Snapshot)  ->
    validate_quotas(ServiceNodesMap, MemoryData, Snapshot,
                    memory_quota:get_quotas(ns_config:get())).

validate_quotas(ServiceNodesMap, MemoryData, Snapshot, Quotas) ->
    %% verify that nothing significant was changed since the memory
    %% data was fetched
    KnownNodes = lists:usort(lists:flatmap(fun ({_, Nodes}) -> Nodes end,
                                           maps:to_list(ServiceNodesMap))),
    [N || {N, _} <- MemoryData] =:=
        get_nodes_to_change(ServiceNodesMap, KnownNodes, Snapshot)
        orelse throw(nodes_mismatch),

    NodeInfos =
        lists:map(
          fun ({Node, MD}) ->
                  CurrentServices =
                      ns_cluster_membership:node_services(Snapshot, Node),
                  DesiredServices =
                      lists:filtermap(
                        fun ({Service, ServiceNodes}) ->
                                case lists:member(Node, ServiceNodes) of
                                    true ->
                                        {true, Service};
                                    false ->
                                        false
                                end
                        end, maps:to_list(ServiceNodesMap)),
                  {Node, lists:usort(CurrentServices ++ DesiredServices), MD}
          end, MemoryData),

    case memory_quota:check_nodes_total_quota(NodeInfos, Quotas) of
        ok ->
            ok;
        {error, {total_quota_too_high, Node, TotalQuota, Max}} ->
            throw({total_quota_too_high,
                   ns_error_messages:bad_memory_size_error(
                     maps:keys(ServiceNodesMap), TotalQuota, Max, Node)})
    end.

get_uninitialized_services(Services, Snapshot) ->
    ns_cluster_membership:nodes_services(
      Snapshot, ns_cluster_membership:inactive_added_nodes(Snapshot)) --
        Services.

get_unejected_services(Services, NodesToEject, Snapshot) ->
    ns_cluster_membership:nodes_services(Snapshot, NodesToEject) -- Services.

rebalance_allowed(Snapshot) ->
    case chronicle_master:get_prev_failover_nodes(Snapshot) of
        [] ->
            ok;
        Nodes ->
            Msg = io_lib:format("Unfinished failover of nodes ~p was found.",
                                [Nodes]),
            {error, lists:flatten(Msg)}
    end.

handle_start_failover(Nodes, From, Wait, FailoverType, Options) ->
    #{allow_unsafe := AllowUnsafe} = Options,
    auto_rebalance:cancel_any_pending_retry_async("failover"),

    ActiveNodes = ns_cluster_membership:active_nodes(),
    NodesInfo = [{active_nodes, ActiveNodes},
                 {failover_nodes, Nodes},
                 {master_node, node()}],
    Id = couch_uuids:random(),
    {ok, ObserverPid} =
        ns_rebalance_observer:start_link([], NodesInfo, FailoverType, Id),
    case failover:start(Nodes,
                        maps:merge(#{auto => FailoverType =:= auto_failover},
                                   Options)) of
        {ok, Pid} ->
            ale:info(?USER_LOGGER, "Starting failover of nodes ~p AllowUnsafe = ~p "
                     "Operation Id = ~s", [Nodes, AllowUnsafe, Id]),

            Event = list_to_atom(atom_to_list(FailoverType) ++ "_initiated"),

            FailoverReasons = maps:get(failover_reasons, Options, []),
            JSONFun = fun (V) when is_list(V) ->
                              list_to_binary(V);
                          (V) ->
                              V
                      end,
            FOReasonsJSON = case FailoverReasons of
                                 [] ->
                                     [];
                                 _ ->
                                     [{failover_reason,
                                       {[{Node, JSONFun(Reason)} ||
                                         {Node, Reason} <- FailoverReasons]}}]
                             end,
            event_log:add_log(Event, [{operation_id, Id},
                                      {nodes_info, {NodesInfo}},
                                      {allow_unsafe, AllowUnsafe}] ++
                                      FOReasonsJSON),

            Type = failover,
            ns_cluster:counter_inc(Type, start),
            set_rebalance_status(Type, running, Pid),
            NewState = #rebalancing_state{rebalancer = Pid,
                                          rebalance_observer = ObserverPid,
                                          eject_nodes = [],
                                          keep_nodes = [],
                                          failed_nodes = [],
                                          delta_recov_bkts = [],
                                          retry_check = undefined,
                                          to_failover = Nodes,
                                          abort_reason = undefined,
                                          type = Type,
                                          rebalance_id = Id},
            case Wait of
                false ->
                    {next_state, rebalancing, NewState, [{reply, From, ok}]};
                true ->
                    {next_state, rebalancing,
                     NewState#rebalancing_state{reply_to = From}}
            end;
        Error ->
            misc:unlink_terminate_and_wait(ObserverPid, kill),
            {keep_state_and_data, [{reply, From, Error}]}
    end.

maybe_reply_to(_, #rebalancing_state{reply_to = undefined}) ->
    ok;
maybe_reply_to(normal, State) ->
    maybe_reply_to(ok, State);
maybe_reply_to({shutdown, {ok, []}}, State) ->
    maybe_reply_to(ok, State);
maybe_reply_to({shutdown, {ok, UnsafeNodes}}, State) ->
    maybe_reply_to({ok, UnsafeNodes}, State);
maybe_reply_to({shutdown, stop}, State) ->
    maybe_reply_to(stopped_by_user, State);
maybe_reply_to(Reason, #rebalancing_state{reply_to = ReplyTo}) ->
    gen_statem:reply(ReplyTo, Reason).

%% Handler for messages that come to the gen_statem in bucket_hibernation
%% state.
handle_info_in_bucket_hibernation(
  {timeout, TRef, {Op, stop}},
  #bucket_hibernation_state{
     hibernation_manager = Manager,
     stop_tref = TRef,
     stop_reason = StopReason,
     bucket = Bucket,
     op = Op}) ->
    %% The hibernation_manager couldn't be gracefully killed - brutally kill it
    %% at the end of the graceful kill timeout.
    misc:unlink_terminate_and_wait(Manager, kill),
    handle_hibernation_manager_shutdown(StopReason, Bucket, Op),
    maybe_try_autofailover_in_idle_state(StopReason);

handle_info_in_bucket_hibernation(
  {'EXIT', Manager, Reason},
  #bucket_hibernation_state{
     hibernation_manager = Manager,
     bucket = Bucket,
     op = Op,
     stop_tref = TRef,
     stop_reason = StopReason}) ->
    cancel_stop_timer(TRef),
    handle_hibernation_manager_exit(Reason, Bucket, Op),
    maybe_try_autofailover_in_idle_state(StopReason);

handle_info_in_bucket_hibernation(Msg, State) ->
    ?log_debug("Message ~p ignored in State: ~p", [Msg, State]),
    keep_state_and_data.

-spec handle_hibernation_manager_exit(Reason, Bucket, Op) -> ok
    when Reason :: normal | shutdown | {shutdown, stop} | any(),
         Bucket :: bucket_name(),
         Op :: pause_bucket | resume_bucket.

handle_hibernation_manager_exit(normal, Bucket, Op) ->
    ale:debug(?USER_LOGGER, "~p done for Bucket ~p.",
              [Op, Bucket]),

    ok = testconditions:check_test_condition(
           exit_ns_orchestrator_after_hibernation_op_done),

    hibernation_utils:update_hibernation_status(completed),
    hibernation_utils:log_hibernation_event(completed, Op, Bucket),

    case Op of
        resume_bucket ->
            %% Run janitor right after, so topology information can be
            %% refreshed quickly
            request_janitor_run({bucket, Bucket});
        _ ->
            ok
    end;
handle_hibernation_manager_exit(shutdown, Bucket, Op) ->
    handle_hibernation_manager_shutdown(shutdown, Bucket, Op);
handle_hibernation_manager_exit({shutdown, _} = Reason, Bucket, Op) ->
    handle_hibernation_manager_shutdown(Reason, Bucket, Op);
handle_hibernation_manager_exit(Reason, Bucket, Op) ->
    ale:error(?USER_LOGGER, "~p for Bucket ~p failed. Reason: ~p",
              [Op, Bucket, Reason]),
    hibernation_utils:log_hibernation_event(failed, Op, Bucket),
    hibernation_utils:update_hibernation_status(failed).

handle_hibernation_manager_shutdown(Reason, Bucket, Op) ->
    ale:debug(?USER_LOGGER, "~p for Bucket ~p stopped. Reason: ~p.",
              [Op, Bucket, Reason]),
    hibernation_utils:log_hibernation_event(stopped, Op, Bucket),
    hibernation_utils:update_hibernation_status(stopped).

-spec not_running(Op :: pause_bucket | resume_bucket) -> atom().
not_running(Op) ->
    list_to_atom("not_running_" ++ atom_to_list(Op)).

get_hibernation_op_stop_timeout(pause_bucket) ->
    ?STOP_PAUSE_BUCKET_TIMEOUT;
get_hibernation_op_stop_timeout(resume_bucket) ->
    ?STOP_RESUME_BUCKET_TIMEOUT.

stop_bucket_hibernation_op(#bucket_hibernation_state{
                             hibernation_manager = Manager,
                             op = Op,
                             stop_tref = undefined} = State, Reason) ->
    exit(Manager, {shutdown, stop}),
    TRef = erlang:start_timer(
             get_hibernation_op_stop_timeout(Op), self(), {stop, Op}),
    State#bucket_hibernation_state{stop_tref = TRef,
                                   stop_reason = Reason};
%% stop_tref is not 'undefined' and therefore a previously initated stop is
%% current running; do a simple pass-through and update the stop_reason
%% if necessary.
stop_bucket_hibernation_op(State, Reason) ->
    %% If we receive a try_autofailover while we are stopping a bucket
    %% hibernation op - we simply update the stop_reason with the
    %% try_autofailover one, to process the autofailover message after the
    %% bucket_hibernation op has been stopped.
    case Reason of
        {try_autofailover, _, _, _} ->
            State#bucket_hibernation_state{stop_reason = Reason};
        _ ->
            State
    end.

handle_info_in_buckets_shutdown({'EXIT', Pid, Reason}, State) ->
    {[Ctx], NewState} =
        lists:partition(
          fun (#bucket_shutdown_ctx{pid = P}) ->
                  P =:= Pid
          end, State),

   #bucket_shutdown_ctx{from = From, bucket_name = BucketName} = Ctx,

    Reply =
        case Reason of
            normal ->
                ns_bucket:del_marked_for_shutdown(BucketName),
                ok;
            Error ->
                Error
        end,

    case NewState of
        [] ->
            {next_state, idle, #idle_state{}, [{reply, From, Reply}]};
        _ ->
            {keep_state, NewState, [{reply, From, Reply}]}
    end.

validate_create_bucket(BucketName, BucketType, BucketConfig) ->
    try
        not ns_bucket:name_conflict(BucketName) orelse
            throw({already_exists, BucketName}),

        BucketType =/= memcached orelse
            throw({incorrect_parameters,
                 "memcached buckets are no longer supported"}),

        ShutdownBuckets =
            case cluster_compat_mode:is_cluster_76() of
                true ->
                    ns_bucket:get_bucket_names_marked_for_shutdown();
                false ->
                    {Results, FailedNodes} =
                        rpc:multicall(ns_node_disco:nodes_wanted(),
                                      ns_memcached, active_buckets, [],
                                      ?CREATE_BUCKET_TIMEOUT),
                    case FailedNodes of
                        [] -> ok;
                        _ ->
                            ?log_warning(
                                "Best-effort check for presense of bucket "
                                "failed to be made on following nodes: ~p",
                                [FailedNodes])
                    end,
                    lists:usort(lists:append(Results))
            end,

        not ns_bucket:name_conflict(BucketName, ShutdownBuckets) orelse
            throw({still_exists, BucketName}),

        case ns_bucket:get_width(BucketConfig) of
            undefined ->
                bucket_placer:allow_regular_buckets() orelse
                    throw({incorrect_parameters,
                           "Cannot create regular bucket because placed buckets"
                           " are present in the cluster"});
            _ ->
                bucket_placer:can_place_bucket() orelse
                    throw({incorrect_parameters,
                           "Cannot place bucket because regular buckets"
                           " are present in the cluster"})
        end,

        PlacedBucketConfig =
            case bucket_placer:place_bucket(BucketName, BucketConfig) of
                {ok, NewConfig} ->
                    NewConfig;
                {error, BadZones} ->
                    throw({need_more_space, BadZones})
            end,
        {ok, PlacedBucketConfig}
    catch
        throw:Error ->
            {error, Error}
    end.

handle_delete_bucket(BucketName, From, CurrentState, StateData) ->
    Result = ns_bucket:remove_bucket(BucketName),
    case Result of
        {ok, BucketConfig} ->
            master_activity_events:note_bucket_deletion(BucketName),
            BucketUUID = proplists:get_value(uuid, BucketConfig),
            event_log:add_log(bucket_deleted,
                              [{bucket,
                                list_to_binary(BucketName)},
                               {bucket_uuid, BucketUUID}]),

            Servers = ns_bucket:get_servers(BucketConfig),
            Timeout = ns_bucket:get_shutdown_timeout(BucketConfig),

            case cluster_compat_mode:is_cluster_76() of
                true ->
                    Pid = erlang:spawn_link(
                            fun () ->
                                    RV = ns_bucket:wait_for_bucket_shutdown(
                                           BucketName, Servers, Timeout),
                                    case RV of
                                        ok -> ok;
                                        Error -> exit(Error)
                                    end
                            end),
                    NewStateData =
                        [#bucket_shutdown_ctx{
                            from = From, pid = Pid, bucket_name = BucketName}
                         | StateData],

                    case CurrentState of
                        idle ->
                            {next_state, buckets_shutdown, NewStateData};
                        buckets_shutdown ->
                            {keep_state, NewStateData}
                    end;
                false ->
                    Res = ns_bucket:wait_for_bucket_shutdown(
                            BucketName, Servers, Timeout),
                    {keep_state_and_data, [{reply, From, Res}]}
            end;
        Error ->
            {keep_state_and_data, [{reply, From, Error}]}
    end.

-ifdef(TEST).
needs_rebalance_api_changed_test() ->
    Resp = needs_rebalance(#{services => [], buckets => []}),
    ?assertEqual(false, Resp),
    Resp2 = needs_rebalance(#{services => [dummyKey], buckets => []}),
    ?assertEqual(true, Resp2),
    Resp3 = needs_rebalance(#{services => [], buckets => [dummyKey]}),
    ?assertEqual(true, Resp3),
    Resp4 = needs_rebalance(#{services => [dummyKey], buckets => [dummyKey]}),
    ?assertEqual(true, Resp4),
    Resp5 = needs_rebalance(#{services => [dummyKey, key2],
                              buckets => [dummyKey]}),
    ?assertEqual(true, Resp5).

get_uninitialized_services_test() ->
    Snapshot =
        #{nodes_wanted => {[n1, n2], rev},
          {node, n1 ,services} => {[index, kv], rev},
          {node, n2 ,services} => {[index, kv, n1ql], rev},
          {node, n1, membership} => {active, rev},
          {node, n2, membership} => {inactiveAdded, rev}},
    ?assertEqual([index, n1ql], get_uninitialized_services([kv], Snapshot)).

get_unejected_services_test() ->
    Snapshot =
        #{{node, n1 ,services} => {[index, kv], rev},
          {node, n2 ,services} => {[index, kv, n1ql], rev}},
    ?assertEqual([index, n1ql],
                 lists:sort(get_unejected_services([kv], [n2], Snapshot))),
    ?assertEqual([], get_unejected_services([kv], [], Snapshot)),
    ?assertEqual([kv], get_unejected_services(
                         [index, n1ql], [n1, n2], Snapshot)).

validate_quotas_test_() ->
    Snapshot =
        #{{node, n1 ,services} => {[index], rev},
          {node, n2 ,services} => {[n1ql], rev}},
    Test =
        fun (ServiceNodesMap, Quotas) ->
                {ok, MemoryData} = get_services_nodes_memory_data(
                                     ServiceNodesMap, [n1, n2], Snapshot),
                try validate_quotas(ServiceNodesMap, MemoryData, Snapshot,
                                    Quotas)
                catch throw:Err -> Err end
        end,
    MinRam = ?MIN_FREE_RAM * ?MIB,
    {foreach,
     fun () ->
             ok = meck:new(ns_doctor),
             ok =
                 meck:expect(
                   ns_doctor, get_memory_data,
                   fun (Nodes) ->
                           {ok, [{N, {MinRam * 10, 0, 0}} || N <- Nodes]}
                   end)
     end,
     fun (_) ->
             ok = meck:unload(ns_doctor)
     end,
     [{"Enough quota to change services topology",
       fun () ->
               ?assertEqual(ok, Test(#{index => [n1, n2], n1ql => [n1, n2]},
                                     [{index, ?MIN_FREE_RAM * 3},
                                      {n1ql, ?MIN_FREE_RAM * 3}]))
       end},
      {"Not enough quota to change services topology",
       fun () ->
               ?assertMatch({total_quota_too_high, _},
                            Test(#{index => [n1, n2], n1ql => [n1, n2]},
                                 [{index, ?MIN_FREE_RAM * 6},
                                  {n1ql, ?MIN_FREE_RAM * 6}]))
       end},
      {"Swapping 2 services with large quotas",
       fun () ->
               ?assertMatch({total_quota_too_high, _},
                            Test(#{index => [n2], n1ql => [n1]},
                                 [{index, ?MIN_FREE_RAM * 6},
                                  {n1ql, ?MIN_FREE_RAM * 6}]))
       end}]}.

-endif.
