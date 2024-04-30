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
         get_failover_vbuckets/1, promote_max_replicas/4,
         clear_failover_vbuckets_sets/1,
         nodes_needed_for_durability_failover/2,
         can_preserve_durability_majority/2,
         get_snapshot/0]).

-define(DATA_LOST, 1).
-define(FAILOVER_OPS_TIMEOUT, ?get_timeout(failover_ops_timeout, 10000)).
-define(DEFAULT_JANITOR_BULK_FACTOR, 8).
-define(MAX_FAILOVER_TRANSACTION_RETRIES, 10).

-record(failover_params,
        {bucket_type :: bucket_type(),
         bucket_config :: list(),
         bucket_map :: vbucket_map() | nil(),
         bucket_options :: map()}).

-record(failover_transaction_result,
        {services :: [service()],
         service_nodes :: [node()] | nil(),
         unsafe_nodes :: list() | nil(),
         kv_nodes :: [node()] | nil(),
         transaction_snapshot :: map(),
         buckets_prep_res :: list() | nil()}).

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
    case allow_unsafe(Options) of
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
    check_chronicle_quorum() andalso check_for_unfinished_failover().

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
    case pre_failover_config_sync(Options) of
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

pre_failover_config_sync(Options) ->
    case durability_aware(Options) of
        true ->
            Timeout = ?get_timeout(failover_config_pull, 10000),
            ?log_info("Going to sync with chronicle quorum"),
            try chronicle_compat:pull(Timeout) of
                ok ->
                    ok
            catch
                T:E:Stack ->
                    ?log_error("Chronicle sync failed with: ~p",
                               [{T, E, Stack}]),
                    config_sync_failed
            end;
        false ->
            ok
    end.

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

get_snapshot() ->
    chronicle_compat:get_snapshot(
      [ns_bucket:fetch_snapshot(all, _, [props]),
       ns_cluster_membership:fetch_snapshot(_)]).

buckets_failover_commits(BucketsPrep, FailedNodes, Snapshot) ->
    BucketsUpdates =
        lists:map(
          fun({Bucket, #failover_params{bucket_map = NewMap,
                                        bucket_type = membase}}) ->
                  %% Note even if the map is empty, we still write it back
                  %% to force rev update which will invalidate any spurious
                  %% updates from the past that may show up in the future
                  NewMap =:= [] orelse ns_bucket:validate_map(NewMap),
                  {Bucket,
                   ns_bucket:update_servers_and_map(Bucket, _, FailedNodes,
                                                    NewMap)};
             ({Bucket, #failover_params{bucket_map = [],
                                        bucket_type = memcached}}) ->
                  {Bucket,
                   ns_bucket:remove_servers_from_bucket(_, FailedNodes)}
          end, BucketsPrep),


    ns_bucket:get_commits_from_snapshot(BucketsUpdates, Snapshot).

validate_snapshot_consistent(TransSnapshot, Snapshot) ->
    A = chronicle_compat:get_keys_and_rev(TransSnapshot),
    B = chronicle_compat:get_keys_and_rev(Snapshot),

    case (A == B) of
        true ->
            ok;
        false ->
            %% Very unlikely but still worth the safety check
            snapshot_rev_conflict
    end.

check_transaction_safety(
    Nodes, TransSnapshot, Snapshot, Options) ->
    functools:sequence_(
      [?cut(validate_snapshot_consistent(TransSnapshot, Snapshot)),
       ?cut(is_possible(Nodes, TransSnapshot, Options)),
       ?cut(maybe_check_kv_safety(Nodes, TransSnapshot, Options))]).

transaction_logic(ok, Nodes, BktPrepResults, SvcNodes, UnsafeNodes, Snapshot) ->
    KVNodes =
        ns_cluster_membership:service_nodes(Snapshot, Nodes, kv),

    %% Update service maps. Sets service_failover_pending for each
    %% service, which is cleared after a rebalance in
    %% complete_services_failover.
    {Services, ServicesCommits} =
        ns_cluster_membership:failover_service_nodes_commits(SvcNodes,
                                                             Snapshot),

    BucketsCommits =
        buckets_failover_commits(BktPrepResults, Nodes, Snapshot),

    case lists:keyfind(abort, 1, BucketsCommits) of
        false ->
            {commit,
             BucketsCommits ++ ServicesCommits,
             #failover_transaction_result{
                services = Services,
                service_nodes = SvcNodes,
                unsafe_nodes = UnsafeNodes,
                kv_nodes = KVNodes,
                transaction_snapshot = Snapshot,
                buckets_prep_res = BktPrepResults}};
        Res ->
            Res
    end;
transaction_logic(Error, _Nodes, _BktPrepResults, _SvcNodes,
                  _UnsafeNodes, _Snapshot) ->
    {abort, {error, Error}}.

failover_transaction(Nodes, BktPrepResults, SvcNodes, UnsafeNodes,
                     Snapshot, Options) ->
    Keys = maps:keys(Snapshot),
    chronicle_kv:transaction(
      kv, Keys,
      fun(TransSnapshot) ->
              IsSafe = check_transaction_safety(Nodes, TransSnapshot,
                                                Snapshot, Options),
              transaction_logic(IsSafe, Nodes, BktPrepResults, SvcNodes,
                                UnsafeNodes, TransSnapshot)
      end, #{retries => 0}).

failover_transaction_with_retries(_Nodes, _Options, Rv, 0) ->
    Rv;
failover_transaction_with_retries(Nodes, Options, _Rv, Retries) ->
    Snapshot = get_snapshot(),
    assert_failover_possible(Nodes, Snapshot, Options),

    BucketsConfig = ns_bucket:get_buckets_by_rank(Snapshot),
    KVNodes = ns_cluster_membership:service_nodes(Snapshot, Nodes, kv),
    BktPrepResults = failover_buckets_prep(KVNodes, BucketsConfig, Options),

    {SvcNodes, UnsafeNodes} =
        validate_failover_services_safety(Snapshot, Nodes, KVNodes, Options),

    case failover_transaction(Nodes, BktPrepResults, SvcNodes, UnsafeNodes,
                              Snapshot, Options) of
        {ok, _, _}  = Rv ->
            Rv;
        {error, snapshot_rev_conflict} = Rv ->
            ?log_warning("Retrying transaction due to: ~p", [Rv]),
            failover_transaction_with_retries(Nodes, Options, Rv, Retries - 1);
        {error, exceeded_retries} = Rv ->
            %% We run the failover chronicle transaction with internal retries
            %% set to 0, since we intend to retry the transaction externally,
            %% so all safety can be rechecked. Therefore we handle the
            %% failure here accordingly by issuing the external retry
            ?log_warning("Retrying transaction due to internal Rev conflict"),
            failover_transaction_with_retries(Nodes, Options, Rv, Retries - 1);
        Rv ->
            Rv
    end.

maybe_failover_collections(Snapshot, Options) ->
    not maps:is_key(quorum_failover, Options) orelse
        failover_collections(Snapshot).

%% @doc Fail one or more nodes. Doesn't eject the node from the cluster. Takes
%% effect immediately.
failover(Nodes, Options) ->
    {ok, _, Results} =
        failover_transaction_with_retries(Nodes, Options, undefined,
                                          ?MAX_FAILOVER_TRANSACTION_RETRIES),

    #failover_transaction_result{services = Services,
                                 service_nodes = SvcNodes,
                                 unsafe_nodes = UnsafeNodes,
                                 kv_nodes = KVNodes,
                                 transaction_snapshot = Snapshot,
                                 buckets_prep_res = BktPrepResults} = Results,

    maybe_failover_collections(Snapshot, Options),
    KVErrorNodes = complete_buckets_failover(KVNodes, BktPrepResults),
    ServicesErrorNodes = complete_services_failover(SvcNodes, Services),
    {lists:umerge([KVErrorNodes, ServicesErrorNodes]), UnsafeNodes}.

failover_collections(Snapshot) ->
    [collections:bump_epoch(BucketName) ||
        {BucketName,
         BucketConfig} <- ns_bucket:get_buckets_by_rank(Snapshot),
        collections:enabled(BucketConfig)].

janitor_membase_buckets_group([], _Nodes) ->
    [];
janitor_membase_buckets_group(Params, Nodes) ->
    {ToJanitorParams, CompletedRes} =
        misc:partitionmap(
          fun({Bucket, #failover_params{bucket_type = membase,
                                        bucket_map = []}}) ->
                  {right, {Bucket, ok}};
             ({_Bucket, #failover_params{bucket_type = membase}} = Param) ->
                  {left, Param}
          end, Params),

    Results = janitor_buckets(ToJanitorParams, Nodes) ++ CompletedRes,

    lists:flatmap(
      fun({Bucket, Result}) ->
              #failover_params{
                 bucket_type = membase,
                 bucket_config = BucketCfg} = proplists:get_value(Bucket,
                                                                  Params),
              master_activity_events:note_bucket_failover_ended(Bucket, Nodes),
              OldMap = proplists:get_value(map, BucketCfg, []),
              [[{bucket, Bucket},
                {node, N},
                {status, Result},
                {vbuckets, node_vbuckets(OldMap, N)}] || N <- Nodes]
      end, Results).

get_janitor_bulk_factor() ->
    SchedulersOnline = erlang:system_info(schedulers_online),
    BulkFactor = ns_config:read_key_fast(failover_bulk_buckets_janitor_factor,
                                         ?DEFAULT_JANITOR_BULK_FACTOR),
    case SchedulersOnline < BulkFactor of
        true ->
            ?log_debug("Throttling down parallelization BulkFactor from "
                       "~p to ~p due to less schedulers online",
                       [BulkFactor, SchedulersOnline]),
            SchedulersOnline;
        false ->
            BulkFactor
    end.

handle_buckets_failover(Nodes, PrepResults) ->
    MembaseParams =
        lists:filter(
          fun({_,  #failover_params{bucket_type = membase}}) ->
                  true;
             ({Bucket, _}) ->
                  %% Only membase buckets are janitored for failover, so
                  %% report failover end for other bucket types here
                  master_activity_events:note_bucket_failover_ended(Bucket,
                                                                    Nodes),
                  false
          end, PrepResults),

    Results = lists:flatmap(
                janitor_membase_buckets_group(_, Nodes),
                misc:split(get_janitor_bulk_factor(), MembaseParams)),

    lists:map(
      fun ({Bucket, _}) ->
              ok = testconditions:check_test_condition(
                  {fail_finalize_failover_at_bucket, Bucket})
      end, PrepResults),

    Results.

complete_buckets_failover([], _PrepResults) ->
    [];
complete_buckets_failover(_Nodes, []) ->
    [];
complete_buckets_failover(Nodes, PrepResults) ->
    Results = handle_buckets_failover(Nodes, PrepResults),

    update_failover_vbuckets(Results),
    failover_handle_results(Results).

clear_failover_vbuckets_sets(Nodes) ->
    [{{node, N, failover_vbuckets}, []} || N <- Nodes].

update_failover_vbuckets(Results) ->
    GroupedByNode = maps:to_list(
                      maps:groups_from_list(
                        proplists:get_value(node, _),
                        fun(L) ->
                                Bucket = proplists:get_value(bucket, L),
                                VBs = proplists:get_value(vbuckets, L),
                                {Bucket, VBs}
                        end,
                        Results)),
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
    Grouped = maps:to_list(maps:groups_from_list(
                             element(1, _), element(2, _),
                             ExistingBucketResults ++ BucketResults)),
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
    NodeStatuses = maps:to_list(maps:groups_from_list(
                                  proplists:get_value(node, _),
                                  proplists:get_value(status, _),
                                  Results)),

    lists:filtermap(fun ({Node, Statuses}) ->
                            NonOKs = [S || S <- Statuses, S =/= ok],

                            case NonOKs of
                                [] ->
                                    false;
                                _ ->
                                    {true, Node}
                            end
                    end, NodeStatuses).

failover_buckets_prep([], _BucketsConfig, _Options) ->
    [];
failover_buckets_prep(Nodes, BucketsConfig, Options) ->
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
                     _JanitorOptions) ->
    #failover_params{
       bucket_type = memcached,
       bucket_config = BucketConfig,
       bucket_map = Map,
       bucket_options = #{}};
failover_bucket_prep(membase, _Nodes, Bucket, BucketConfig, [],
                     _JanitorOptions) ->
    ?log_debug("Skipping failover of bucket ~p because it has no vbuckets.",
               [Bucket]),
    #failover_params{
       bucket_type = membase,
       bucket_config = BucketConfig,
       bucket_map = [],
       bucket_options = #{}};
failover_bucket_prep(membase, Nodes, Bucket, BucketConfig, Map,
                     JanitorOptions) ->
    Servers = ns_bucket:get_servers(BucketConfig) -- Nodes,
    case ns_bucket:get_hibernation_state(BucketConfig) of
        pausing ->
            ok = hibernation_utils:unpause_bucket(Bucket, Servers);
        _ ->
            ok
    end,

    %% Bread crumb to inform the janitor that bucket has already been checked
    %% for a failed hibernation pause status, and it doesn't require for
    %% unpause to be re-issued during the janitor run
    BucketOptions = #{unpause_checked_hint => true},

    NewMap = fix_vbucket_map(Nodes, Bucket, Map, JanitorOptions),
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

    %% These params will be used to complete the failover in the finalize phase
    #failover_params{
       bucket_type = membase,
       bucket_config = BucketConfig,
       bucket_map = NewMap,
       bucket_options = BucketOptions}.

validate_failover_services_safety(_Snapshot, Nodes, _,
                                  #{skip_safety_check := true}) ->
    {Nodes, []};
validate_failover_services_safety(Snapshot, Nodes, KVNodes,
                                  #{auto := true, down_nodes := DownNodes}) ->
    auto_failover:validate_services_safety(Snapshot, Nodes, DownNodes, KVNodes);
validate_failover_services_safety(_Snapshot, Nodes, _, _) ->
    {Nodes, []}.

complete_services_failover(_Nodes, []) ->
    [];
complete_services_failover(Nodes, Services) ->
    ?log_debug("Failover nodes ~p from services ~p", [Nodes, Services]),
    Results = lists:flatmap(complete_failover_service(Nodes, _), Services),
    failover_handle_results(Results).

complete_failover_service(Nodes, Service) ->
    %% We're refetching the config since failover_service_nodes updated the
    %% one that we had.
    Result = service_janitor:complete_service_failover(Service),
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

janitor_buckets([], _Nodes) ->
    [];
janitor_buckets(BucketsParams, Nodes) ->
    CleanupOptions = janitor_cleanup_options(Nodes),
    JanitorParams =
        lists:map(
          fun({Bucket, #failover_params{bucket_options = BucketOptions,
                                        bucket_map = NewMap}}) ->
                  true = (NewMap =/= []),
                  {Bucket, maybe_add_hibernation_hint(BucketOptions)}
          end, BucketsParams),

    try
        Results = ns_janitor:cleanup_buckets(JanitorParams, CleanupOptions),
        lists:map(
          fun({Bucket, ok}) ->
                  {Bucket, ok};
             ({Bucket, {error, _, BadNodes}}) ->
                  ?rebalance_error("Skipped vbucket activations and "
                                   "replication topology changes because not "
                                   "all remaining nodes were found to have "
                                   "healthy bucket ~p: ~p", [Bucket, BadNodes]),
                  {Bucket, janitor_failed};
             ({Bucket, Error}) ->
                  ?rebalance_error("Janitor cleanup of ~p "
                                   "failed after failover of ~p: ~p",
                                   [Bucket, Nodes, Error]),
                  {Bucket, janitor_failed}
          end, Results)
    catch T:E:S ->
            Buckets = [Bucket || {Bucket, _} <- BucketsParams],
            ?rebalance_error("Janitor cleanup of ~p "
                             "failed after failover of ~p: ~p",
                             [Buckets, Nodes, {T,E,S}]),
            [{Bucket, janitor_failed} || Bucket <- Buckets]
    end.

maybe_add_hibernation_hint(FailoverOptions) ->
    case maps:get(unpause_checked_hint, FailoverOptions, false) of
        true ->
            [{unpause_checked_hint, true}];
        _ ->
            []
    end.

janitor_cleanup_options(FailedNodes) ->
    [{failover_nodes, FailedNodes}].

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
        {N, VBs} <- maps:to_list(maps:groups_from_list(
                                   element(1, _), element(2, _), NodeVBs)),
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

check_is_possible(FailoverNodes, Snapshot, Options) ->
    try
        ActiveNodes = ns_cluster_membership:active_nodes(Snapshot),
        KVActiveNodes =
            ns_cluster_membership:service_nodes(Snapshot, ActiveNodes, kv),
        NodesWanted = ns_node_disco:nodes_wanted(Snapshot),
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
        end,
        BucketsConfig = ns_bucket:get_buckets_by_rank(Snapshot),
        check_last_server(BucketsConfig, FailoverNodes)
    catch
        throw:Error ->
            Error
    end.

is_possible(FailoverNodes, Snapshot, Options) ->
    check_is_possible(FailoverNodes, Snapshot, Options).

is_possible(FailoverNodes, Options) ->
    Snapshot = get_snapshot(),
    is_possible(FailoverNodes, Snapshot, Options).

maybe_check_kv_safety(Nodes, Snapshot, Options) ->
    %% Whilst we also check this in ns_orchestrator in the auto_failover path,
    %% it could be the case that the config has materially changed before we
    %% acquired our leader activity. We re-check KV safety here (we check
    %% services safety again later) against the snapshot that we will perform
    %% this failover on. If the snapshot proves to be stale when we attempt to
    %% commit the transaction then we should abort the failover. It could be the
    %% case that KV failover is no longer safe at this point too, in which case
    %% we can abort early...
    case maps:find(auto, Options) of
        {ok, true} ->
            #{down_nodes := DownNodes} = Options,
            case auto_failover:validate_kv(Snapshot, Nodes, DownNodes) of
                ok -> ok;
                {unsafe, _Buckets} = UnsafeBuckets ->
                    UnsafeBuckets
            end;
        _ -> ok
    end.

assert_failover_possible(Nodes, Snapshot, Options) ->
    case maybe_check_kv_safety(Nodes, Snapshot, Options) of
        ok ->
            ok;
        {unsafe, Buckets} ->
            ?log_error("Failover is not possible, KV failover is "
                       "unsafe for buckets ~p", [Buckets]),
            erlang:exit({autofailover_unsafe, Buckets})
    end,

    case is_possible(Nodes, Snapshot, Options) of
        ok ->
            ok;
        Error ->
            ?log_error("Failover is not possible due to ~p", [Error]),
            erlang:exit(Error)
    end.

check_last_server([], _FailoverNodes) ->
    ok;
check_last_server([{BucketName, BucketConfig} | Rest], FailoverNodes) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    case Servers -- FailoverNodes of
        [] ->
            ?log_error("Attempt to fail over the last server for bucket ~p. "
                       "Failover nodes: ~p, KV nodes ~p",
                       [BucketName, FailoverNodes, Servers]),
            throw({last_node_for_bucket, BucketName});
        _ ->
            check_last_server(Rest, FailoverNodes)
    end.

get_failover_vbuckets(Node) ->
    get_failover_vbuckets(direct, Node).

get_failover_vbuckets(Snapshot, Node) ->
    chronicle_compat:get(Snapshot, {node, Node, failover_vbuckets},
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
              cluster_compat_mode, chronicle_compat, chronicle_kv, ns_bucket,
              testconditions], [passthrough]),

    meck:expect(ns_janitor, check_server_list,
                fun (_,_) ->
                        ok
                end),

    meck:expect(ns_config, read_key_fast,
                fun (_, Default) ->
                        Default
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

    meck:expect(cluster_compat_mode, get_compat_version,
                fun () -> [?MIN_SUPPORTED_VERSION] end),

    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> true end),

    meck:expect(chronicle_compat, transaction,
                fun (_,_) ->
                        {ok, ok}
                end),

    meck:expect(chronicle_compat, get,
                fun (_,_,_) ->
                        []
                end),

    meck:expect(testconditions, get,
                fun (_) ->
                        ok
                end),

    meck:expect(ns_config, get_timeout, fun (_, _) -> 1234 end).

get_test_bucket_config() ->
    B1Cfg = [{servers, [a,b,c]}, {type, membase}, {map, [[a, b], [c, a]]}],
    B2Cfg = [{servers, [a,b,c]}, {type, membase}, {map, []}],
    B3Cfg = [{servers, [a,b,c,d]}, {type, memcached}, {map, []}],
    B4Cfg = [{servers, [a,b,c,d]}, {type, membase}, {map, [[b, a], [a, b]]}],
    B5Cfg = [{servers, [a,b,c,d]}, {type, membase}, {map, [[a, b], [d, c]]}],
    [{"B1", B1Cfg}, {"B2", B2Cfg}, {"B3", B3Cfg}, {"B4", B4Cfg},
     {"B5", B5Cfg}].

get_test_snapshot() ->
    Version = 1,
    {Snapshot, BucketNames} =
        lists:foldl(
          fun({Bucket, Cfg}, {Acc, Buckets}) ->
                  {Acc#{{bucket, Bucket, props} => {Cfg, Version}},
                   [Bucket | Buckets]}
          end, {#{}, []}, get_test_bucket_config()),
    Snapshot#{bucket_names => {BucketNames, Version},
              cluster_compat_version => {?VERSION_76, Version},
              nodes_wanted => {[a,b,c,d], Version},
              {service_map, index} => {[a,c], Version},
              {service_map, fts} => {[b,c,d], Version},
              {node, a, services} => {[kv, index], Version},
              {node, b, services} => {[kv, fts], Version},
              {node, c, services} => {[kv, index, fts], Version},
              {node, d, services} => {[kv, fts], Version},
              {node, a, membership} => {active, Version},
              {node, b, membership} => {active, Version},
              {node, c, membership} => {active, Version},
              {node, d, membership} => {active, Version}
             }.

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
       fun failover_buckets_group_failure_result_test_body/0},
      {"Failover Transaction Test",
       fun failover_transaction_test_body/0},
      {"Failover Transaction Failure Test",
       fun failover_transaction_failure_test_body/0},
      {"Failover Transaction invoke Retries",
       fun failover_transaction_invoke_retries/0}]
    }.

failover_buckets_prep_test_body() ->
    BConfig = get_test_bucket_config(),
    PrepResults = failover_buckets_prep([a, d], BConfig, #{}),

    ?assertEqual(length(PrepResults), 5),

    #failover_params{bucket_type = B1Type,
                     bucket_map = B1NewMap,
                     bucket_options = B1Opt} = proplists:get_value(
                                                 "B1", PrepResults),
    ?assertEqual(B1Type, membase),
    ?assertEqual(B1NewMap,
                 [[b, undefined],[c, undefined]]),
    ?assertEqual(maps:get(unpause_checked_hint, B1Opt), true),

    #failover_params{bucket_type = B2Type,
                     bucket_map = B2NewMap} = proplists:get_value(
                                                "B2", PrepResults),
    ?assertEqual(B2Type, membase),
    ?assertEqual(B2NewMap, []),

    #failover_params{bucket_type = B3Type} = proplists:get_value(
                                               "B3", PrepResults),
    ?assertEqual(B3Type, memcached),

    #failover_params{bucket_type = B5Type,
                     bucket_map = B5NewMap} = proplists:get_value(
                                                "B4", PrepResults),
    ?assertEqual(B5Type, membase),
    ?assertEqual(B5NewMap, [[b, undefined], [b, undefined]]),

    #failover_params{bucket_type = B6Type,
                     bucket_map = B6NewMap} = proplists:get_value(
                                                "B5", PrepResults),
    ?assertEqual(B6Type, membase),
    ?assertEqual(B6NewMap, [[b, undefined], [c, undefined]]),
    ok.

failover_buckets_group_test_body() ->
    FailedNodes = [a,d],
    BConfig = get_test_bucket_config(),

    meck:expect(ns_janitor, cleanup_buckets,
                fun (JanitorParams, _CleanupOpts) ->
                        Expected = ["B1", "B4", "B5"],
                        lists:map(
                          fun({Bucket, Opts}) ->
                                  ?assertEqual(true,
                                               lists:member(Bucket, Expected)),
                                  Exists =
                                      proplists:get_value(unpause_checked_hint,
                                                          Opts),
                                  ?assertEqual(true, Exists),
                                  {Bucket, ok}
                          end, JanitorParams)
                end),

    JOpts = #{durability_aware => true},
    PrepResults = failover_buckets_prep(FailedNodes, BConfig, JOpts),

    Results1 = lists:flatmap(handle_buckets_failover(FailedNodes, _),
                             misc:split(?MAX_BUCKETS_SUPPORTED,
                                        PrepResults)),

    lists:foreach(
      fun({Bucket, _BucketConfig}) ->
              %% Ensure start and end failover events are reported for all
              %% buckets
              ?assertEqual(
                 1, meck:num_calls(master_activity_events,
                                   note_bucket_failover_started, [Bucket,
                                                                  '_'])),
              ?assertEqual(
                 1, meck:num_calls(master_activity_events,
                                   note_bucket_failover_ended, [Bucket, '_']))
      end, BConfig),

    ?assertEqual(lists:sort(Results1),
                 lists:sort(
                   [[{bucket,"B1"},{node,a},{status,ok},{vbuckets,[0, 1]}],
                    [{bucket,"B1"},{node,d},{status,ok},{vbuckets,[]}],
                    [{bucket,"B2"},{node,a},{status,ok},{vbuckets,[]}],
                    [{bucket,"B2"},{node,d},{status,ok},{vbuckets,[]}],
                    [{bucket,"B4"},{node,a},{status,ok},{vbuckets,[0,1]}],
                    [{bucket,"B4"},{node,d},{status,ok},{vbuckets,[]}],
                    [{bucket,"B5"},{node,a},{status,ok},{vbuckets,[0]}],
                    [{bucket,"B5"},{node,d},{status,ok},{vbuckets,[1]}]])),

    PrepResultsUpdt =
        lists:filter(
          fun({_, #failover_params{bucket_type = membase}}) ->
                  true;
             (_) ->
                  false
          end, PrepResults),

    %% Now test janitor_membase_buckets groups of 2 buckets at a time, the
    %% results must match the main results
    Results2 = lists:flatmap(
                 janitor_membase_buckets_group(_, FailedNodes),
                 misc:split(2, PrepResultsUpdt)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results2)),

    %% Previous invocation code path should have only affected membase buckets
    %% so ensure failover end event counts match accordingly
    lists:foreach(
      fun({Bucket, Config}) ->
              case ns_bucket:bucket_type(Config) of
                  membase ->
                      ?assertEqual(
                         2,
                         meck:num_calls(master_activity_events,
                                        note_bucket_failover_ended,
                                        [Bucket, '_']));
                  _ ->
                      ?assertEqual(
                         1,
                         meck:num_calls(master_activity_events,
                                        note_bucket_failover_ended,
                                        [Bucket, '_']))
              end
      end, BConfig),

    %% Lastly single bucket at a time, results must match
    Results3 = lists:flatmap(
                 janitor_membase_buckets_group(_, FailedNodes),
                 misc:split(1, PrepResultsUpdt)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results3)),
    ok.

failover_buckets_group_failure_result_test_body() ->
    FailedNodes = [a,d],
    meck:expect(ns_bucket, remove_servers_from_bucket,
                fun (_, _) ->
                        ok
                end),
    meck:expect(ns_bucket, update_buckets_config,
                fun (_) ->
                        ok
                end),
    meck:expect(ns_janitor, cleanup_buckets,
                fun (JParams, _CleanupOpts) ->
                        OutRes = [{"B1", failure_in_test},
                                  {"B5", failure_in_test},
                                  {"B4", ok}],
                        [{Bucket,
                          proplists:get_value(
                            Bucket, OutRes)} || {Bucket, _} <- JParams]
                end),

    BConfig = get_test_bucket_config(),
    JOpts = #{},
    PrepResults = failover_buckets_prep(FailedNodes, BConfig, JOpts),
    Results1 = lists:flatmap(handle_buckets_failover(FailedNodes, _),
                             misc:split(?MAX_BUCKETS_SUPPORTED,
                                        PrepResults)),

    ?assertEqual(lists:sort(Results1),
                 lists:sort(
                   [[{bucket,"B1"}, {node,a},
                     {status,janitor_failed}, {vbuckets,[0,1]}],
                    [{bucket,"B1"},{node,d},
                     {status,janitor_failed},{vbuckets,[]}],
                    [{bucket,"B2"},{node,a},
                     {status,ok},{vbuckets,[]}],
                    [{bucket,"B2"},{node,d},
                     {status,ok},{vbuckets,[]}],
                    [{bucket,"B4"},{node,a},
                     {status,ok},{vbuckets,[0,1]}],
                    [{bucket,"B4"},{node,d},
                     {status,ok},{vbuckets,[]}],
                    [{bucket,"B5"}, {node,a},
                     {status,janitor_failed}, {vbuckets,[0]}],
                    [{bucket,"B5"}, {node,d},
                     {status,janitor_failed}, {vbuckets,[1]}]])),

    PrepResultsUpdt =
        lists:filter(
          fun({_, #failover_params{bucket_type = membase}}) ->
                  true;
             (_) ->
                  false
          end, PrepResults),

    Results2 = lists:flatmap(
                 janitor_membase_buckets_group(_, FailedNodes),
                 misc:split(3, PrepResultsUpdt)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results2)),

    Results3 = lists:flatmap(
                 janitor_membase_buckets_group(_, FailedNodes),
                 misc:split(1, PrepResultsUpdt)),
    ?assertEqual(lists:sort(Results1), lists:sort(Results3)),
    ok.

transaction_test_setup(FailedNodes) ->
    Snapshot = get_test_snapshot(),
    BConfig = ns_bucket:get_buckets(Snapshot),

    meck:expect(ns_bucket, remove_servers_from_bucket,
                fun (_, Nodes) ->
                        ?assertEqual(FailedNodes, Nodes)
                end),
    meck:expect(ns_bucket, update_buckets_config,
                fun (BucketsUpdates) ->
                        lists:foreach(
                          fun({Bucket, Fun}) ->
                                  Cfg = proplists:get_value(Bucket, BConfig),
                                  Fun(Cfg)
                          end, BucketsUpdates)
                end),

    meck:expect(chronicle_compat, get_snapshot,
                fun (_) ->
                        Snapshot
                end),
    meck:delete(chronicle_compat, get, 3),

    meck:expect(chronicle_kv, transaction,
                fun (_, _, TransLogic, Opts) ->
                        ?assertEqual(0, maps:get(retries, Opts)),
                        case TransLogic(Snapshot) of
                            {abort, Error} ->
                                Error;
                            Rv ->
                                Rv
                        end
                end),

    meck:expect(auto_failover, validate_services_safety,
                fun (_, _, _, _) ->
                        {FailedNodes, []}
                end),

    {Snapshot, BConfig}.

failover_transaction_test_body() ->
    FailedNodes = [a,d],
    {_, BConfig} = transaction_test_setup(FailedNodes),
    MembaseExpectedMaps =
        [{"B1", [[b, undefined], [c, undefined]]},
         {"B2", []},
         {"B4", [[b, undefined], [b, undefined]]},
         {"B5", [[b, undefined], [c, undefined]]}],

    lists:foreach(
      fun({Bucket, BucketConfig}) ->
              %% Ensure correct update function being called for bucket types
              case proplists:get_value(type, BucketConfig) of
                  membase ->
                      ExpectedMap =
                          proplists:get_value(Bucket, MembaseExpectedMaps),
                      ?assertEqual(
                         1,
                         meck:num_calls(ns_bucket, update_servers_and_map,
                                        [Bucket, '_', FailedNodes,
                                         ExpectedMap]));
                  memcached ->
                      ?assertEqual(
                         1,
                         meck:num_calls(ns_bucket, remove_servers_from_bucket,
                                        ['_', FailedNodes]))
              end
      end, BConfig),

    {commit, Commits, Results} =
        failover_transaction_with_retries(FailedNodes, #{}, undefined,
                                          ?MAX_FAILOVER_TRANSACTION_RETRIES),

    #failover_transaction_result{services = Services,
                                 service_nodes = SvcNodes,
                                 unsafe_nodes = UnsafeNodes,
                                 kv_nodes = KVNodes,
                                 buckets_prep_res = _BktPrepResults} = Results,
    ExpectedCommits =
        [{set,
          {bucket,"B1",props},
          [{servers,[b,c]}, {fastForwardMap,undefined},
           {map,[[b,undefined],[c,undefined]]}, {type,membase}]},
         {set,
          {bucket,"B2",props},
          [{servers,[b,c]}, {fastForwardMap,undefined},
           {map,[]}, {type,membase}]},
         {set,{bucket,"B3",props},ok},
         {set,
          {bucket,"B4",props},
          [{servers,[b,c]}, {fastForwardMap,undefined},
           {map,[[b,undefined],[b,undefined]]}, {type,membase}]},
         {set,
          {bucket,"B5",props},
          [{servers,[b,c]}, {fastForwardMap,undefined},
           {map,[[b,undefined],[c,undefined]]}, {type,membase}]},
         {set,{service_map,fts},[b,c]},
         {set,{service_failover_pending,fts},true},
         {set,{service_map,index},[c]},
         {set,{service_failover_pending,index},true}],

    ?assertEqual(ExpectedCommits, Commits),
    ?assertEqual(Services, [fts, index]),
    ?assertEqual(UnsafeNodes, []),
    ?assertEqual(SvcNodes, [a,d]),
    ?assertEqual(KVNodes, [a,d]),
    ok.

failover_transaction_failure_test_body() ->
    FailedNodes1 = [a,b,c,d],
    {Snapshot, _} =
        transaction_test_setup(FailedNodes1),

    BucketsConfig = ns_bucket:get_buckets_by_rank(Snapshot),
    KVNodes = ns_cluster_membership:service_nodes(Snapshot, FailedNodes1, kv),
    BktPrepResults = failover_buckets_prep(KVNodes, BucketsConfig, #{}),

    %% Test for unsafe failover failure
    Failure1 =
        try
            failover_transaction_with_retries(
              FailedNodes1, #{}, undefined,
              ?MAX_FAILOVER_TRANSACTION_RETRIES)
        catch exit:Code ->
                Code
        end,
    ?assertEqual(Failure1, last_node),
    ?assertEqual({error, last_node},
                 failover_transaction(FailedNodes1, BktPrepResults, [], [],
                                      Snapshot, #{})),

    %% Now force a mismatch between the keys of the external Snapshot
    %% and the Transaction one and check for appropriate error
    FailedNodes2 = [c,d],
    meck:expect(chronicle_kv, transaction,
                fun (_, _, TransLogic, _) ->
                        case TransLogic(maps:remove({node, a, membership}, Snapshot)) of
                            {abort, Error} ->
                                Error;
                            Rv ->
                                Rv
                        end
                end),
    ?assertEqual({error, snapshot_rev_conflict},
                 failover_transaction_with_retries(
                   FailedNodes2, #{}, undefined,
                   ?MAX_FAILOVER_TRANSACTION_RETRIES)),

    %% Modify the version of a buckets prop key and ensure proper error
    %% after retries
    meck:expect(chronicle_kv, transaction,
                fun (_, _, TransLogic, _) ->
                        {Val, Rev} = maps:get({bucket, "B4", props}, Snapshot),
                        NewSnapShot = Snapshot#{{bucket, "B4", props} => {Val, Rev + 1}},
                        case TransLogic(NewSnapShot) of
                            {abort, Error} ->
                                Error;
                            Rv ->
                                Rv
                        end
                end),
    ?assertEqual({error, snapshot_rev_conflict},
                 failover_transaction_with_retries(
                   FailedNodes2, #{}, undefined,
                   ?MAX_FAILOVER_TRANSACTION_RETRIES)),

    ok.

failover_transaction_invoke_retries() ->
    FailedNodes = [c,d],
    {Snapshot, _} = transaction_test_setup(FailedNodes),

    %% Force a mismatch of the Snapshot on first try, and fix it on the second
    %% try, therefore the transaction should succeed after retry
    meck:expect(chronicle_kv, transaction,
                fun (_, _, TransLogic, _) ->
                        {Val, Rev} = maps:get({bucket, "B4", props}, Snapshot),
                        NewSnapshot =
                            Snapshot#{{bucket, "B4", props} => {Val, Rev + 1}},
                        case TransLogic(NewSnapshot) of
                            {abort, Error} ->
                                meck:expect(chronicle_compat, get_snapshot,
                                            fun (_) ->
                                                    NewSnapshot
                                            end),
                                Error;
                            Rv ->
                                Rv
                        end
                end),


    %% With just 1 retries, it should fail because we invoke mismatch on
    %% first try
    ?assertEqual({error, snapshot_rev_conflict},
                 failover_transaction_with_retries(FailedNodes, #{},
                                                   undefined, 1)),

    %% Should work with the second try as test updates the Snapshot to match
    {commit, _, _} =
        failover_transaction_with_retries(FailedNodes, #{},
                                          undefined, 2),
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
