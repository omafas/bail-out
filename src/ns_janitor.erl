%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Monitor and maintain the vbucket layout of each bucket.
%%
-module(ns_janitor).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([cleanup/2,
         cleanup_buckets/2,
         cleanup_apply_config/4,
         check_server_list/2]).

-record(janitor_params,
        {bucket_config :: list(),
         bucket_servers :: [node()],
         vbucket_states :: dict:dict() | undefined}).

-spec cleanup(Bucket::bucket_name(), Options::list()) ->
          ok |
          {error, wait_for_memcached_failed, [node()]} |
          {error, marking_as_warmed_failed, [node()]} |
          {error, set_data_ingress_failed, [node()]} |
          {error, unsafe_nodes, [node()]} |
          {error, {config_pull_failed, Details :: any()}} |
          {error, {bad_vbuckets, [vbucket_id()]}} |
          {error, {corrupted_server_list, [node()], [node()]}}.
cleanup(Bucket, Options) ->
    [{Bucket, Res}] = cleanup_buckets([{Bucket, []}], Options),
    Res.

maybe_get_membase_config(not_present) ->
    ok;
maybe_get_membase_config({ok, BucketConfig}) ->
    case ns_bucket:bucket_type(BucketConfig) of
        membase ->
            {ok, BucketConfig};
        _ ->
            ok
    end.

cleanup_buckets(BucketsAndParams, Options) ->
    %% We always want to check for unsafe nodes, as we want to honor the
    %% auto-reprovisioning settings for ephemeral buckets. That is, we do not
    %% want to simply activate any bucket on a restarted node and lose the data
    %% instead of promoting the replicas.
    JanitorOptions = Options ++ auto_reprovision:get_cleanup_options(),
    Buckets = [Bucket || {Bucket, _} <- BucketsAndParams],
    BucketsFetchers =
        [ns_bucket:fetch_snapshot(Bucket, _, [props]) || Bucket <- Buckets],
    SnapShot =
        chronicle_compat:get_snapshot(
          [ns_cluster_membership:fetch_snapshot(_) | BucketsFetchers]),
    {Completed, BucketsAndCfg} =
        misc:partitionmap(
          fun ({Bucket, BucketOpts}) ->
                  CfgRes = ns_bucket:get_bucket(Bucket, SnapShot),
                  case maybe_get_membase_config(CfgRes) of
                      ok ->
                          {left, {Bucket, ok}};
                      {ok, BucketConfig} ->
                          {right, {Bucket, {BucketConfig, BucketOpts}}}
                  end
          end, BucketsAndParams),

    run_buckets_cleanup_activity(
      BucketsAndCfg, SnapShot, JanitorOptions) ++ Completed.

run_buckets_cleanup_activity([], _Snapshot, _Options) ->
    [];
run_buckets_cleanup_activity(BucketsAndCfg, SnapShot, Options) ->
    Buckets = [Bucket || {Bucket, _} <- BucketsAndCfg],
    {ok, Rv} =
        leader_activities:run_activity(
          {ns_janitor, Buckets, cleanup}, majority,
          fun () ->
                  ConfigPhaseRes =
                      [{Bucket,
                        cleanup_with_membase_bucket_check_hibernation(
                          Bucket, Options ++ BktOpts, BktConfig, SnapShot)} ||
                          {Bucket, {BktConfig, BktOpts}} <- BucketsAndCfg],

                  {Completed, Remaining} =
                      misc:partitionmap(
                        fun({Bucket, {ok, BktConfig}}) ->
                                case ns_bucket:get_servers(BktConfig) of
                                    [] ->
                                        {left, {Bucket, {error, no_servers}}};
                                    Servers ->
                                        {right, {Bucket,
                                                 #janitor_params{
                                                    bucket_servers = Servers,
                                                    bucket_config = BktConfig
                                                   }}}
                                end;
                           ({Bucket, Response}) ->
                                {left, {Bucket, Response}}
                        end, ConfigPhaseRes),

                  {ok, cleanup_with_membase_buckets_vbucket_map(
                         Remaining, Options) ++ Completed}
          end,
          [quiet]),

    Rv.

repeat_bucket_config_cleanup(Bucket, Options) ->
    SnapShot =
        chronicle_compat:get_snapshot(
          [ns_bucket:fetch_snapshot(Bucket, _, [props]),
           ns_cluster_membership:fetch_snapshot(_)]),
    CfgRes = ns_bucket:get_bucket(Bucket, SnapShot),
    case maybe_get_membase_config(CfgRes) of
        ok ->
            ok;
        {ok, BucketConfig} ->
            cleanup_with_membase_bucket_check_hibernation(
              Bucket, Options, BucketConfig, SnapShot)
    end.

cleanup_with_membase_bucket_check_servers(Bucket, Options, BucketConfig,
                                          Snapshot) ->
    case check_server_list(Bucket, BucketConfig, Snapshot, Options) of
        ok ->
            cleanup_with_membase_bucket_check_map(Bucket,
                                                  Options, BucketConfig);
        {update_servers, NewServers} ->
            update_servers(Bucket, NewServers),
            repeat_bucket_config_cleanup(Bucket, Options);
        {error, _} = Error ->
            Error
    end.

update_servers(Bucket, Servers) ->
    ?log_debug("janitor decided to update "
               "servers list for bucket ~p to ~p", [Bucket, Servers]),

    ns_bucket:set_servers(Bucket, Servers).

unpause_bucket(Bucket, Nodes, Options) ->
    case proplists:get_value(unpause_checked_hint, Options, false) of
        true ->
            ok;
        false ->
            hibernation_utils:unpause_bucket(Bucket, Nodes)
    end.

handle_hibernation_cleanup(Bucket, Options, BucketConfig, State = pausing) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    case unpause_bucket(Bucket, Servers, Options) of
        ok ->
            ns_bucket:clear_hibernation_state(Bucket),
            ?log_debug("Cleared hibernation state"),
            repeat_bucket_config_cleanup(Bucket, Options);
        _ ->
            {error, hibernation_cleanup_failed, State}
    end;
handle_hibernation_cleanup(Bucket, _Options, _BucketConfig, State = resuming) ->
    %% A bucket in "resuming" hibernation state during janitor cleanup is an
    %% inactive bucket with no server list or map. It does not exist in
    %% memcached so cleanup of it mostly involves a delete from the config.

    case ns_bucket:delete_bucket(Bucket) of
        {ok, _} ->
            ns_janitor_server:delete_bucket_request(Bucket);
        _ ->
            {error, hibernation_cleanup_failed, State}
    end.

cleanup_with_membase_bucket_check_hibernation(Bucket, Options, BucketConfig,
                                              Snapshot) ->
    case ns_bucket:get_hibernation_state(BucketConfig) of
        undefined ->
            cleanup_with_membase_bucket_check_servers(Bucket, Options,
                                                      BucketConfig, Snapshot);
        State ->
            handle_hibernation_cleanup(Bucket, Options, BucketConfig, State)
    end.

cleanup_with_membase_bucket_check_map(Bucket, Options, BucketConfig) ->
    case proplists:get_value(map, BucketConfig, []) of
        [] ->
            Servers = ns_bucket:get_servers(BucketConfig),
            true = (Servers =/= []),

            ?log_info("janitor decided to generate initial vbucket map"),
            {Map, MapOpts} =
                ns_rebalancer:generate_initial_map(Bucket, BucketConfig),
            set_initial_map(Map, Servers, MapOpts, Bucket),

            repeat_bucket_config_cleanup(Bucket, Options);
        _ ->
            {ok, BucketConfig}
    end.

set_initial_map(Map, Servers, MapOpts, Bucket) ->
    ns_bucket:store_last_balanced_vbmap(Bucket, Map, MapOpts),
    ok = ns_bucket:set_initial_map(Bucket, Map, Servers, MapOpts).

partition_param_results(Res) ->
    lists:partition(
      fun({_Bucket, #janitor_params{}}) ->
              true;
         (_) ->
              false
      end, Res).

cleanup_with_membase_buckets_vbucket_map([], _Options) ->
    [];
cleanup_with_membase_buckets_vbucket_map(ConfigPhaseRes, Options) ->
    Timeout = proplists:get_value(query_states_timeout, Options),
    Opts = [{timeout, Timeout} || Timeout =/= undefined],
    QueryPhaseFun =
        fun({Bucket, #janitor_params{bucket_servers = Servers} = JParams}) ->
                case janitor_agent:query_vbuckets(Bucket, Servers, [], Opts) of
                    {States, []} ->
                        {Bucket,
                         JParams#janitor_params{vbucket_states = States}};
                    {_States, Zombies} ->
                        ?log_info("Bucket ~p not yet ready on ~p",
                                  [Bucket, Zombies]),
                        {Bucket, {error, wait_for_memcached_failed, Zombies}}
                end
        end,

    QueryRes = misc:parallel_map(QueryPhaseFun, ConfigPhaseRes, infinity),
    {Remaining, CurrErrors} = partition_param_results(QueryRes),
    CurrErrors ++ cleanup_buckets_with_states(Remaining, Options).

cleanup_buckets_with_states([], _Options) ->
    [];
cleanup_buckets_with_states(Params, Options) ->
    {Remaining, Errors} =
        partition_param_results(apply_config_prep(Params, Options)),

    %% Note that the nominal case is that all remaining params get grouped
    %% into a single group as the server list will be the same for them in
    %% the nominal case. We still handle the corner cases when that is not
    %% true, so that we don't fail janitoring for buckets for which we have a
    %% quorum on all nodes in the server list. In these cases, we further
    %% group bucket params based on server list, and buckets with same server
    %% list are handled together for apply config phase
    ParamGroups =
        maps:groups_from_list(
          fun({_Bucket, #janitor_params{bucket_servers = Servers}}) ->
                  lists:sort(Servers)
          end, Remaining),
    maps:fold(
      fun(QuorumServers, ParamsGroup, Acc) ->
              Acc ++ cleanup_apply_config_on_buckets(
                       ParamsGroup, QuorumServers, Options)
      end, [], ParamGroups) ++ Errors.


check_unsafe_nodes(BucketConfig, States, Options) ->
    %% Find all the unsafe nodes (nodes on which memcached restarted within
    %% the auto-failover timeout) using the vbucket states. If atleast one
    %% unsafe node is found then we won't bring the bucket online until we
    %% we reprovision it. Reprovisioning is initiated by the orchestrator at
    %% the end of every janitor run.
    UnsafeNodes = find_unsafe_nodes_with_vbucket_states(
                    BucketConfig, States,
                    should_check_for_unsafe_nodes(BucketConfig, Options)),

    case UnsafeNodes =/= [] of
        true ->
            {error, unsafe_nodes, UnsafeNodes};
        false ->
            ok
    end.

maybe_fixup_vbucket_map(Bucket, BucketConfig, States) ->
    case do_maybe_fixup_vbucket_map(Bucket, BucketConfig, States) of
        not_needed ->
            {ok, BucketConfig};
        {ok, FixedBucketConfig} ->
            {ok, FixedBucketConfig};
        FixupError ->
            FixupError
    end.

check_prep_param({Bucket, #janitor_params{bucket_config = BucketConfig,
                                          vbucket_states = States}} = Param,
                 Options) ->
    case check_unsafe_nodes(BucketConfig, States, Options) of
        ok ->
            Param;
        Error ->
            {Bucket, Error}
    end.

apply_config_prep(Params, Options) ->
    try
        maybe_pull_config(Params, Options),

        lists:map(
          fun({Bucket,
               #janitor_params{vbucket_states = States} = JParam}) ->
                  {ok, CurrBucketConfig} = ns_bucket:get_bucket(Bucket),
                  case maybe_fixup_vbucket_map(Bucket, CurrBucketConfig,
                                               States) of
                      {ok, NewConfig} ->
                          Param = {Bucket, JParam#janitor_params{
                                             bucket_config = NewConfig}},
                          check_prep_param(Param, Options);
                      Error ->
                          {Bucket, Error}
                  end
          end, Params)
    catch
        throw:Error ->
            [{Bucket, Error} || {Bucket, _} <- Params]
    end.

do_maybe_fixup_vbucket_map(Bucket, BucketConfig, States) ->
    {NewBucketConfig, IgnoredVBuckets} = compute_vbucket_map_fixup(Bucket,
                                                                   BucketConfig,
                                                                   States),
    case IgnoredVBuckets of
        [] ->
            case NewBucketConfig =:= BucketConfig of
                true ->
                    not_needed;
                false ->
                    fixup_vbucket_map(Bucket, BucketConfig,
                                      NewBucketConfig, States),
                    {ok, NewBucketConfig}
            end;
        _ when is_list(IgnoredVBuckets) ->
            {error, {bad_vbuckets, IgnoredVBuckets}}
    end.

fixup_vbucket_map(Bucket, BucketConfig, NewBucketConfig, States) ->
    ?log_info("Janitor is going to change "
              "bucket config for bucket ~p", [Bucket]),
    ?log_info("VBucket states:~n~p", [dict:to_list(States)]),
    ?log_info("Old bucket config:~n~p", [BucketConfig]),

    ok = ns_bucket:set_bucket_config(Bucket, NewBucketConfig).

cleanup_apply_config(Bucket, Servers, BucketConfig, Options) ->
    Results =
        cleanup_apply_config_on_buckets(
          [{Bucket, #janitor_params{bucket_config = BucketConfig,
                                    bucket_servers = Servers}}],
          Servers, Options),

    [{Bucket, Result}] = Results,
    Result.

cleanup_apply_config_on_buckets(Params, QuorumServers, Options) ->
    Buckets = [Bucket || {Bucket, _} <- Params],
    {ok, Result} =
        leader_activities:run_activity(
          {ns_janitor, Buckets, apply_config}, {all, QuorumServers},
          fun () ->
                  SortedQServers = lists:sort(QuorumServers),
                  Results =
                      misc:parallel_map(
                        fun({Bucket,
                             #janitor_params{bucket_config = BucketConfig,
                                             bucket_servers = Servers}}) ->
                                SortedQServers = lists:sort(Servers),
                                {Bucket,
                                 cleanup_apply_config_body(Bucket, Servers,
                                                           BucketConfig,
                                                           Options)}
                        end, Params, infinity),
                  {ok, Results}
          end,
          [quiet]),

    Result.

check_states_match(Bucket, BucketConfig, States) ->
    {_, Map} = lists:keyfind(map, 1, BucketConfig),
    case map_matches_states_exactly(Map, States) of
        true ->
            false;
        {false, Mismatch} ->
            ?log_debug("Found states mismatch in bucket ~p:~n~p",
                       [Bucket, Mismatch]),
            true
    end.

maybe_pull_config(Params, Options) when is_list(Params) ->
    SyncRequired =
        proplists:get_value(pull_config, Options, true) andalso
        cluster_compat_mode:preserve_durable_mutations() andalso
        lists:any(
          fun({Bucket, #janitor_params{bucket_config = BucketConfig,
                                       vbucket_states = States}}) ->
                  check_states_match(Bucket, BucketConfig, States)
          end, Params),
    not SyncRequired orelse pull_config().

pull_config() ->
    Timeout = ?get_timeout({config_sync, pull}, 10000),

    ?log_debug("Going to pull config"),
    try chronicle_compat:pull(Timeout) of
        ok ->
            ok
    catch
        T:E:Stack ->
            throw({error, {config_pull_failed, {T, E, Stack}}})
    end.

cleanup_apply_config_body(Bucket, Servers, BucketConfig, Options) ->
    ok = janitor_agent:apply_new_bucket_config(
           Bucket, Servers, BucketConfig,
           proplists:get_value(apply_config_timeout, Options,
                               undefined_timeout)),

    maybe_reset_rebalance_status(Options),

    cleanup_mark_bucket_warmed(Bucket, Servers).

cleanup_mark_bucket_warmed(Bucket, Servers) ->
    case janitor_agent:mark_bucket_warmed(Bucket, Servers) of
        ok ->
            ok;
        {errors, BadReplies} ->
            ?log_error("Failed to mark bucket `~p` as warmed up."
                       "~nBadReplies:~n~p", [Bucket, BadReplies]),
            {error, marking_as_warmed_failed, [N || {N, _} <- BadReplies]}
    end.

should_check_for_unsafe_nodes(BCfg, Options) ->
    proplists:get_bool(check_for_unsafe_nodes, Options) andalso
        ns_bucket:storage_mode(BCfg) =:= ephemeral.

find_unsafe_nodes_with_vbucket_states(_BucketConfig, _States, false) ->
    [];
find_unsafe_nodes_with_vbucket_states(BucketConfig, States, true) ->
    Map = proplists:get_value(map, BucketConfig, []),
    true = (Map =/= []),
    EnumeratedChains = misc:enumerate(Map, 0),

    lists:foldl(
      fun ({VB, [Master | _ ] = Chain}, UnsafeNodesAcc) ->
              case lists:member(Master, UnsafeNodesAcc) of
                  true ->
                      UnsafeNodesAcc;
                  false ->
                      case data_loss_possible(VB, Chain, States) of
                          {true, Node} ->
                              [Node | UnsafeNodesAcc];
                          false ->
                              UnsafeNodesAcc
                      end
              end
      end, [], EnumeratedChains).

%% Condition that indicates possibility of data loss:
%% A vBucket is "missing" on a node where it is supposed to be active as per the
%% vBucket map, it is not active elsewhere in the cluster, and the vBucket is in
%% replica state on some other node[s]. If such a vBucket is brought online on
%% the node supposed to be its current master, then it will come up empty and
%% when the replication streams are establised the replicas will also lose their
%% data.
data_loss_possible(VBucket, Chain, States) ->
    NodeStates = janitor_agent:fetch_vbucket_states(VBucket, States),
    [Master | Replicas] = Chain,
    case janitor_agent:find_vbucket_state(Master, NodeStates) of
        missing ->
            %% Replicas might be in wrong states due to interrupted rebalance
            %% (since this code is executed with a fixed up vbucket map, but
            %% before the state changes are actually applied to the system),
            %% so we check for any existing vbuckets among expected replicas.
            ExistingReplicas =
                [N || N <- Replicas,
                      N =/= undefined,
                      janitor_agent:find_vbucket_state(N, NodeStates) =/=
                          missing],

            case ExistingReplicas of
                [] ->
                    false;
                _ ->
                    ?log_info("vBucket ~p missing on master ~p while "
                              "replicas ~p are active. Can lead to "
                              "dataloss.",
                              [VBucket, Master, ExistingReplicas]),
                    {true, Master}
            end;
        _ ->
            false
    end.

maybe_reset_rebalance_status(Options) ->
    case proplists:get_bool(consider_resetting_rebalance_status, Options) of
        true ->
            %% We can't run janitor when rebalance is running. This usually
            %% means previous rebalance was stopped/terminated but we haven't
            %% recorded the status as such.
            Running = case rebalance:status() of
                          running ->
                              true;
                          _ ->
                              false
                      end,
            Msg = <<"Rebalance stopped by janitor.">>,
            rebalance:reset_status(
              fun () ->
                      ale:info(?USER_LOGGER,
                               "Resetting rebalance status "
                               "since it's not really running"),
                      {none, Msg}
              end),

            %% We do not wish to call record_rebalance_report inside the
            %% transaction above, as this involves writing to file and hence can
            %% stall the transaction.
            %% Since this is mainly for the UI, we are ok with the report not
            %% being strongly consistent with the status.
            Running andalso
                ns_rebalance_report_manager:record_rebalance_report(
                  ejson:encode({[{completionMessage, Msg}]}),
                  [node()]);
        false ->
            ok
    end.

%% !!! only purely functional code below (with notable exception of logging) !!!
%% lets try to keep as much as possible logic below this line
check_server_list(Bucket, BucketConfig) ->
    check_server_list(Bucket, BucketConfig, direct, []).

check_server_list(Bucket, BucketConfig, Snapshot, Options) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    ActiveKVNodes = ns_cluster_membership:service_active_nodes(Snapshot, kv) --
                        proplists:get_value(failover_nodes, Options, []),
    do_check_server_list(Bucket, BucketConfig, Servers, ActiveKVNodes).

ephemeral_bucket_fixup_needed(BucketConfig) ->
    %% If we allow failover even when there are no remaining replicas for
    %% ephemeral buckets, we may need to provision a new active in some chains.
    case ns_bucket:kv_bucket_type(BucketConfig) of
        persistent -> false;
        ephemeral ->
            Map = proplists:get_value(map, BucketConfig, []),
            lists:any(fun ([undefined|_]) ->
                              true;
                          (_) -> false
                      end,  Map)
    end.

do_check_server_list(_Bucket, BucketConfig, [], ActiveKVNodes) ->
    DesiredServers = case ns_bucket:get_desired_servers(BucketConfig) of
                         undefined ->
                             ActiveKVNodes;
                         Servers ->
                             Servers
                     end,
    {update_servers, DesiredServers};
do_check_server_list(Bucket, BucketConfig, Servers, ActiveKVNodes) when
      is_list(Servers) ->
    case ephemeral_bucket_fixup_needed(BucketConfig) of
        true ->
            case lists:sort(Servers) =/= lists:sort(ActiveKVNodes) of
                true -> {update_servers, ActiveKVNodes};
                false -> ok
            end;
        _ ->
            %% We don't expect for buckets to refer to servers that are not
            %% active. We can't guarantee this though due to weaknesses of
            %% ns_config. The best we can do if we detect a mismatch is to
            %% complain and have a human intervene.
            UnexpectedServers = Servers -- ActiveKVNodes,
            case UnexpectedServers of
                [] ->
                    ok;
                _ ->
                    ?log_error("Found a corrupt server list in bucket ~p.~n"
                               "Server list: ~p~n"
                               "Active KV nodes: ~p~n"
                               "Unexpected servers: ~p",
                               [Bucket, Servers, ActiveKVNodes,
                                UnexpectedServers]),
                    {error, {corrupted_server_list, Servers, ActiveKVNodes}}
            end
    end.

compute_vbucket_map_fixup(Bucket, BucketConfig, States) ->
    Map = proplists:get_value(map, BucketConfig, []),
    true = ([] =/= Map),
    FFMap = proplists:get_value(fastForwardMap, BucketConfig),

    EnumeratedChains = mb_map:enumerate_chains(Map, FFMap),

    NewMasterCandidates =
        case ephemeral_bucket_fixup_needed(BucketConfig) of
            true -> ns_bucket:get_servers(BucketConfig);
            false -> []
        end,
    MapUpdates =
        [sanify_chain(Bucket, States, Chain, FutureChain, VBucket,
                      NewMasterCandidates)
         || {VBucket, Chain, FutureChain} <- EnumeratedChains],

    MapLen = length(Map),
    IgnoredVBuckets = [VBucket || {VBucket, ignore} <-
                                      lists:zip(lists:seq(0, MapLen - 1),
                                                MapUpdates)],
    NewMap = [case NewChain of
                  ignore -> OldChain;
                  _ -> NewChain
              end || {NewChain, OldChain} <- lists:zip(MapUpdates, Map)],
    NewBucketConfig = case NewMap =:= Map of
                          true ->
                              BucketConfig;
                          false ->
                              ?log_debug("Janitor decided to update vbucket map"),
                              lists:keyreplace(map, 1, BucketConfig,
                                               {map, NewMap})
                      end,
    {NewBucketConfig, IgnoredVBuckets}.

%% this will decide what vbucket map chain is right for this vbucket
sanify_chain(_Bucket, _States,
             [undefined | _] = CurrentChain,
             _FutureChain, _VBucket,
             []) ->
    %% We can get here on a hard-failover case.
    CurrentChain;
%% This case is specific to ephemeral buckets after failover. Assign a new
%% active if there is at least one active KV Node.
sanify_chain(Bucket, _States,
             [undefined | Rest],
             _FutureChain, VBucket,
             NewMasterCandidates) ->
    NewMaster = lists:nth(rand:uniform(length(NewMasterCandidates)),
                          NewMasterCandidates),
    true = NewMaster =/= undefined,
    ?log_info("Ephemeral bucket provisioning new active.~n"
              "Setting vbucket ~p in ~p on ~p to active.",
              [VBucket, Bucket, NewMaster]),
    [NewMaster | Rest];
sanify_chain(Bucket, States,
             [CurrentMaster | _] = CurrentChain,
             FutureChain, VBucket,
             _NewMasterCandidates) ->
    NodeStates = janitor_agent:fetch_vbucket_states(VBucket, States),
    Actives = [N || {N, active, _} <- NodeStates],

    case Actives of
        %% No Actives.
        [] ->
            CurrentMasterState =
                janitor_agent:find_vbucket_state(CurrentMaster, NodeStates),
            ?log_info("Setting vbucket ~p in ~p on ~p from ~p to active.",
                      [VBucket, Bucket, CurrentMaster, CurrentMasterState], [{chars_limit, -1}]),
            %% Let's activate according to vbucket map.
            CurrentChain;

        %% One Active.
        [ActiveNode] ->
            sanify_chain_one_active(Bucket, VBucket, ActiveNode,
                                    NodeStates, CurrentChain, FutureChain);

        %% Multiple Actives.
        _ ->
            ?log_error("Extra active nodes ~p for vbucket ~p in ~p. "
                       "This should never happen!", [Actives, Bucket, VBucket]),
            case lists:member(CurrentMaster, Actives) of
                false ->
                    ignore;
                true ->
                    %% Pick CurrentChain if CurrentMaster is active.
                    CurrentChain
            end
    end.

fill_missing_replicas(Chain, ExpectedLength) when ExpectedLength > length(Chain) ->
    Chain ++ lists:duplicate(ExpectedLength - length(Chain), undefined);
fill_missing_replicas(Chain, _) ->
    Chain.

derive_chain(Bucket, VBucket, ActiveNode, Chain) ->
    DerivedChain = case misc:position(ActiveNode, Chain) of
                       false ->
                           %% It's an extra node
                           ?log_error(
                              "Master for vbucket ~p in ~p is not "
                              "active, but ~p is, so making that the "
                              "master.",
                              [VBucket, Bucket, ActiveNode]),
                           [ActiveNode];
                       Pos ->
                           ?log_error(
                              "Master for vbucket ~p in ~p "
                              "is not active, but ~p is (one of "
                              "replicas). So making that master.",
                              [VBucket, Bucket, ActiveNode]),
                           [ActiveNode | lists:nthtail(Pos, Chain)]
                   end,
    %% Fill missing replicas, so we don't lose durability constraints.
    fill_missing_replicas(DerivedChain, length(Chain)).

sanify_chain_one_active(_Bucket, _VBucket, ActiveNode, _States,
                        [CurrentMaster | _CurrentReplicas] = CurrentChain,
                        _FutureChain)
  when ActiveNode =:= CurrentMaster ->
    CurrentChain;
sanify_chain_one_active(Bucket, VBucket, ActiveNode, States,
                        [CurrentMaster | _CurrentReplicas] = CurrentChain,
                        [FutureMaster | FutureReplicas] = FutureChain)
  when ActiveNode =:= FutureMaster ->
    %% we check expected replicas to be replicas. One other allowed
    %% possibility is if old master is replica in ff chain. In which
    %% case depending on where rebalance was stopped it may be dead (if
    %% stopped right after takeover) or replica (if stopped after
    %% post-move vbucket states are set).
    PickFutureChain = lists:all(
                        fun (undefined) ->
                                true;
                            (N) ->
                                case janitor_agent:find_vbucket_state(N,
                                                                      States) of
                                    replica ->
                                        true;
                                    dead when N =:= CurrentMaster ->
                                        %% old master might be dead or
                                        %% replica. Replica is tested
                                        %% above
                                        true;
                                    _ ->
                                        false
                                end
                        end, FutureReplicas),
    case PickFutureChain of
        true ->
            FutureChain;
        false ->
            derive_chain(Bucket, VBucket, ActiveNode, CurrentChain)
    end;
sanify_chain_one_active(Bucket, VBucket, ActiveNode, _States,
                        CurrentChain, _FutureChain) ->
    %% One active node, but it's not the master and it's not fast-forward map
    %% master, so we'll just update vbucket map. Note behavior below with losing
    %% replicas makes little sense as of now. Especially with star replication.
    %% But we can adjust it later.
    derive_chain(Bucket, VBucket, ActiveNode, CurrentChain).

map_matches_states_exactly(Map, States) ->
    Mismatch =
        lists:filtermap(
          fun ({VBucket, Chain}) ->
                  NodeStates =
                      janitor_agent:fetch_vbucket_states(VBucket, States),

                  case chain_matches_states_exactly(Chain, NodeStates) of
                      true ->
                          false;
                      false ->
                          {true, {VBucket, Chain, NodeStates}}
                  end
          end, misc:enumerate(Map, 0)),

    case Mismatch of
        [] ->
            true;
        _ ->
            {false, Mismatch}
    end.

chain_matches_states_exactly(Chain0, NodeStates) ->
    Chain = [N || N <- Chain0, N =/= undefined],

    case length(Chain) =:= length(NodeStates) of
        true ->
            lists:all(
              fun ({Pos, Node}) ->
                      ExpectedState =
                          case Pos of
                              1 ->
                                  active;
                              _ ->
                                  replica
                          end,

                      ActualState =
                          janitor_agent:find_vbucket_state(Node, NodeStates),

                      ActualState =:= ExpectedState
              end, misc:enumerate(Chain));
        false ->
            %% Some extra nodes have the vbucket.
            false
    end.

-ifdef(TEST).
sanify_chain_t(States, CurrentChain, FutureChain, NewMasterCandidates) ->
    sanify_chain("B",
                 dict:from_list(
                   [{0, [{N, S, []} || {N, S} <- States]}]),
                 CurrentChain, FutureChain, 0, NewMasterCandidates).

sanify_basic_test() ->
    meck:new([ns_config], [passthrough]),
    meck:expect(ns_config, read_key_fast,
                fun (failover_ephemeral_no_replicas, false) ->
                        false
                end),

    %% normal case when everything matches vb map
    [a, b] = sanify_chain_t([{a, active}, {b, replica}], [a, b], [], []),

    %% yes, the code will keep both masters as long as expected master
    %% is there. Possibly something to fix in future
    [a, b] = sanify_chain_t([{a, active}, {b, active}], [a, b], [], []),

    %% main chain doesn't match but fast-forward chain does
    [b, c] = sanify_chain_t([{a, dead}, {b, active}, {c, replica}],
                            [a, b], [b, c], []),

    %% main chain doesn't match but ff chain does. And old master is already
    %% deleted
    [b, c] = sanify_chain_t([{b, active}, {c, replica}], [a, b], [b, c], []),

    %% lets make sure we touch all paths just in case
    %% this runs "there are >1 unexpected master" case
    ignore = sanify_chain_t([{a, active}, {b, active}], [c, a, b], [], []),

    %% this runs "master is one of replicas" case
    [b, undefined] = sanify_chain_t([{b, active}, {c, replica}], [a, b], [],
                                    []),

    %% and this runs "master is some non-chain member node" case
    [c, undefined] = sanify_chain_t([{c, active}], [a, b], [], []),

    %% lets also test rebalance stopped prior to complete takeover
    [a, b] = sanify_chain_t([{a, dead}, {b, replica}, {c, pending},
                             {d, replica}], [a, b], [c, d], []),

    [undefined] = sanify_chain_t([], [undefined], [a, b, c], []),
    [undefined, undefined] = sanify_chain_t([], [undefined, undefined],
                                            [a, b, c], []),

    meck:expect(ns_config, read_key_fast,
                fun (failover_ephemeral_no_replicas, false) ->
                        true
                end),
    [X] = sanify_chain_t([], [undefined], [], [a, b, c]),
    ?assertEqual(lists:member(X, [a, b, c]), true),

    [a, undefined] = sanify_chain_t([], [undefined, undefined], [], [a]),
    [undefined, undefined] = sanify_chain_t([], [undefined, undefined], [], []),

    meck:unload([ns_config]),
    ok.

sanify_doesnt_lose_replicas_on_stopped_rebalance_test() ->
    meck:new([ns_config], [passthrough]),
    meck:expect(ns_config, read_key_fast,
                fun (failover_ephemeral_no_replicas, false) ->
                        false
                end),

    %% simulates the following: We've completed move that switches
    %% replica and active but rebalance was stopped before we updated
    %% vbmap. We have code in sanify to detect this condition using
    %% fast-forward map and is supposed to recover perfectly from this
    %% condition.
    [a, b] = sanify_chain_t([{a, active}, {b, dead}], [b, a], [a, b], []),

    %% rebalance can be stopped after updating vbucket states but
    %% before vbucket map update
    [a, b] = sanify_chain_t([{a, active}, {b, replica}], [b, a], [a, b], []),
    %% same stuff but prior to takeover
    [a, b] = sanify_chain_t([{a, dead}, {b, pending}], [a, b], [b, a], []),

    %% lets test more usual case too
    [c, d] = sanify_chain_t([{a, dead}, {b, replica}, {c, active},
                             {d, replica}], [a, b], [c, d], []),

    %% but without FF map we're (too) conservative (should be fixable
    %% someday)
    [c, undefined] = sanify_chain_t([{a, dead}, {b, replica}, {c, active},
                                     {d, replica}], [a, b], [], []),
    meck:unload([ns_config]).

sanify_addition_of_replicas_test() ->
    meck:new([ns_config], [passthrough]),
    meck:expect(ns_config, read_key_fast,
                fun (failover_ephemeral_no_replicas, false) ->
                        false
                end),

    [a, b] = sanify_chain_t([{a, active}, {b, replica}], [a, b], [a, b, c], []),
    [a, b] = sanify_chain_t([{a, active}, {b, replica}, {c, replica}],
                            [a, b], [a, b, c], []),

    %% replica addition with possible move.
    [a, b] = sanify_chain_t([{a, dead}, {b, replica}, {c, pending}],
                            [a, b], [c, a, b], []),
    [c, d, a] = sanify_chain_t([{a, dead}, {b, replica}, {c, active},
                                {d, replica}], [a, b], [c, d, a], []),
    [c, d, a] = sanify_chain_t([{a, replica}, {b, replica}, {c, active},
                                {d, replica}], [a, b], [c, d, a], []),
    meck:unload([ns_config]).

chain_matches_states_exactly_test() ->
    ?assert(chain_matches_states_exactly([a, b],
                                         [{a, active, []},
                                          {b, replica, []}])),

    ?assertNot(chain_matches_states_exactly([a, b],
                                            [{a, active, []},
                                             {b, pending, []}])),

    ?assertNot(chain_matches_states_exactly([a, undefined],
                                            [{a, active, []},
                                             {b, replica, []}])),

    ?assertNot(chain_matches_states_exactly([b, a],
                                            [{a, active, []},
                                             {b, replica, []}])),

    ?assertNot(chain_matches_states_exactly([undefined, undefined],
                                            [{a, active, []},
                                             {b, replica, []}])),

    ?assert(chain_matches_states_exactly([undefined, undefined], [])).

map_matches_states_exactly_test() ->
    Map = [[a, b],
           [a, b],
           [c, undefined],
           [undefined, undefined]],
    GoodStates = dict:from_list(
                   [{0, [{a, active, []}, {b, replica, []}]},
                    {1, [{a, active, []}, {b, replica, []}]},
                    {2, [{c, active, []}]},
                    {3, []}]),

    ?assert(map_matches_states_exactly(Map, GoodStates)),

    BadStates1 = dict:from_list(
                   [{0, [{a, active, []}, {b, replica, []}]},
                    {1, [{a, replica, []}, {b, replica, []}]},
                    {2, [{c, active, []}]},
                    {3, []}]),
    BadStates2 = dict:from_list(
                   [{0, [{a, active, []}, {b, replica, []}, {c, active, []}]},
                    {1, [{a, active, []}, {b, replica, []}]},
                    {2, [{c, active, []}]},
                    {3, []}]),
    BadStates3 = dict:from_list(
                   [{0, [{a, active, []}, {b, replica, []}]},
                    {1, [{a, active, []}, {b, replica, []}]},
                    {2, [{c, active, []}]},
                    {3, [{c, replica}]}]),
    BadStates4 = dict:from_list(
                   [{0, [{a, active, []}, {b, replica, []}]},
                    {1, [{a, active, []}, {b, replica, []}]},
                    {2, []},
                    {3, []}]),
    BadStates5 = dict:from_list(
                   [{0, [{a, active, []}, {b, replica, []}]},
                    {1, [{a, active, []}]},
                    {2, [{c, active, []}]},
                    {3, []}]),


    lists:foreach(
      fun (States) ->
              ?assertMatch({false, _}, map_matches_states_exactly(Map, States))
      end, [BadStates1, BadStates2, BadStates3, BadStates4, BadStates5]).

janitor_buckets_group_test_() ->
    {foreach,
     fun load_apply_config_prep_common_modules/0,
     fun (_) ->
             meck:unload()
     end,
     [{"Apply Config Prep Test",
       fun apply_config_prep_test_body/0},
      {"Apply Config Prep Errors Test",
       fun apply_config_prep_test_errors_body/0},
      {"Cleanup Buckets With Map Test",
       fun cleanup_buckets_with_map_test_body/0},
      {"Cleanup Buckets With States Test",
       fun cleanup_buckets_with_states_test_body/0},
      {"Cleanup Mark Bucket Warmed Data Ingress Test",
       fun cleanup_mark_bucket_warmed_data_ingress_test_body/0}]
    }.

load_apply_config_prep_common_modules() ->
    meck:new([ns_config, chronicle_compat, cluster_compat_mode, ns_bucket,
              leader_activities], [passthrough]),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) ->
                        Default
                end),
    meck:expect(ns_config, read_key_fast,
                fun (failover_ephemeral_no_replicas, false) ->
                        false
                end),
    meck:expect(cluster_compat_mode, preserve_durable_mutations,
                fun () ->
                        true
                end),
    meck:expect(chronicle_compat, pull,
                fun (_) ->
                        ok
                end).

get_apply_config_prep_params() ->
    Map1 = [[a,b], [a,b], [b,a], [b,c]],
    Map2 = [[b,a], [a,b], [b,a], [b,c]],
    BucketConfig1 = [{map, Map1}, {servers, [a,b,c]}],
    BucketConfig2 = [{map, Map2}, {servers, [a,b,c]}],
    States = [{0,[{a,active,[]},{b,replica,[]}]},
              {3,[{b,active,[]},{c,replica,[]}]},
              {2,[{b,active,[]},{a,replica,[]}]},
              {1,[{a,active,[]},{b,replica,[]}]}],

    Param1 = {"B1", #janitor_params{bucket_servers = [a,b,c],
                                    bucket_config = BucketConfig1,
                                    vbucket_states = dict:from_list(States)}},
    Param2 = {"B2", #janitor_params{bucket_servers = [a,b,c],
                                    bucket_config = BucketConfig2,
                                    vbucket_states = dict:from_list(States)}},

    [Param1, Param2].

apply_config_prep_test_body() ->
    [Param1, Param2] = get_apply_config_prep_params(),
    {_, #janitor_params{bucket_config = BucketConfig1}} = Param1,
    {_, JParams2} = Param2,
    Param2Expected = {"B2", JParams2#janitor_params{bucket_config =
                                                        BucketConfig1}},

    Options = [{pull_config, true}],

    meck:expect(chronicle_compat, pull,
                fun (_) ->
                        self() ! chronicle_pull_called,
                        ok
                end
               ),
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, BucketConfig1}
                end),

    %% Param1 has map that matches states, Param2 has map that doesn't map
    %% states. Any call to ns_bucket:get_bucket will provide map for both
    %% params where states match. The expectation are that Param2 gets updated
    %% with the new config in that case.
    ?assertEqual([Param1, Param2Expected], apply_config_prep([Param1, Param2],
                                                             Options)),

    %% Also we verify that chronicle pull got called because param2 had a
    %% states mismatch with config
    receive
        chronicle_pull_called ->
            ok
    after
        1000 ->
            ?assert(false)
    end,

    0 = ?flush(_),

    %% Expectation is no chronicle pull gets called if states always match
    %% config
    meck:expect(chronicle_compat, pull,
                fun (_) ->
                        ?assert(false),
                        ok
                end
               ),
    ?assertEqual([Param1, Param1], apply_config_prep([Param1, Param1],
                                                     Options)),
    0 = ?flush(_),
    ok.

apply_config_prep_test_errors_body() ->
    [Param1, Param2] = get_apply_config_prep_params(),
    {_, #janitor_params{bucket_config = BucketConfig1}} = Param1,

    Options = [{pull_config, true},
               {check_for_unsafe_nodes, true}],

    meck:expect(chronicle_compat, pull,
                fun (_) ->
                        throw({config_pull_faled})
                end
               ),
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, BucketConfig1}
                end),

    [{"B1", {error, {config_pull_failed, _}}},
     {"B2", {error, {config_pull_failed, _}}}] =
        apply_config_prep([Param1, Param2], Options),

    meck:expect(chronicle_compat, pull,
                fun (_) ->
                        ok
                end
               ),
    meck:expect(ns_bucket, storage_mode,
                fun (_) ->
                        ephemeral
                end
               ),

    States = [{0,[{a,missing,[]},{b,replica,[]}]},
              {3,[{b,active,[]},{c,replica,[]}]},
              {2,[{b,active,[]},{a,replica,[]}]},
              {1,[{a,active,[]},{b,replica,[]}]}],
    {Bkt, JParam2} = Param2,
    Param2Updt =
        {Bkt,
         JParam2#janitor_params{vbucket_states = dict:from_list(States)}},

    ?assertEqual([Param1, {"B2", {error,unsafe_nodes,[a]}}],
                 apply_config_prep([Param1, Param2Updt], Options)),

    ok.

cleanup_buckets_with_map_test_body() ->
    [Param1, Param2] = get_apply_config_prep_params(),
    {B1, #janitor_params{vbucket_states = States,
                         bucket_config = BucketConfig1} = JParam1} = Param1,
    {B2, JParam2} = Param2,
    InputParam1 = {B1, JParam1#janitor_params{vbucket_states = undefined}},
    InputParam2 = {B2, JParam2#janitor_params{vbucket_states = undefined}},

    Options = [{pull_config, true}],

    meck:expect(leader_activities, run_activity,
                fun ({ns_janitor, Buckets, apply_config}, _, _, _) ->
                        {ok, [{Bucket, ok} || Bucket <- Buckets]}
                end
               ),
    meck:expect(janitor_agent, query_vbuckets,
                fun (_, _, _, _) ->
                        {States, []}
                end
               ),
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, BucketConfig1}
                end),

    ?assertEqual([{B1, ok}, {B2, ok}],
                 cleanup_with_membase_buckets_vbucket_map(
                   [InputParam1, InputParam2], Options)),
    ?assertEqual([],
                 cleanup_with_membase_buckets_vbucket_map(
                   [], Options)),
    ?assertEqual([{B2, ok}],
                 cleanup_with_membase_buckets_vbucket_map(
                   [InputParam2], Options)),

    %% Test single error in caller, and successes in called
    meck:expect(janitor_agent, query_vbuckets,
                fun ("B2", _, _, _) ->
                        {States, {error, zombie_error_stub}};
                    (_, _, _, _) ->
                        {States, []}
                end
               ),
    Res = cleanup_with_membase_buckets_vbucket_map(
            [InputParam1, InputParam2, {"B3", JParam2}], Options),
    ?assertEqual(
       [{"B2",{error,wait_for_memcached_failed,{error,zombie_error_stub}}},
        {"B1",ok}, {"B3", ok}], Res),

    %% All errors in caller, no calls will be made further from caller
    meck:expect(janitor_agent, query_vbuckets,
                fun (_, _, _, _) ->
                        {States, {error, zombie_error_stub}}
                end
               ),
    Res3 = cleanup_with_membase_buckets_vbucket_map(
             [InputParam1, InputParam2, {"B3", JParam2}], Options),
    ?assertEqual(
       [{"B1",{error,wait_for_memcached_failed,{error,zombie_error_stub}}},
        {"B2",{error,wait_for_memcached_failed,{error,zombie_error_stub}}},
        {"B3", {error,wait_for_memcached_failed,{error,zombie_error_stub}}}],
       Res3),

    ok.

cleanup_buckets_with_states_test_body() ->
    [Param1, Param2] = get_apply_config_prep_params(),
    {_, #janitor_params{bucket_config = BucketConfig1} = JParam} = Param1,

    Options = [{pull_config, true}],

    meck:expect(leader_activities, run_activity,
                fun ({ns_janitor, Buckets, apply_config}, _, _, _) ->
                        ?assertEqual(["B1", "B2"], Buckets),
                        {ok, [{Bucket, ok} || Bucket <- Buckets]}
                end
               ),
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, BucketConfig1}
                end),

    Res1 = cleanup_buckets_with_states([Param1, Param2], Options),
    ?assertEqual([{"B1", ok}, {"B2", ok}], Res1),

    Param3 = {"B3", JParam#janitor_params{bucket_servers = [c,b,a]}},
    Param4 = {"B4", JParam#janitor_params{bucket_servers = [c,b]}},
    Param5 = {"B5", JParam#janitor_params{bucket_servers = [b,c]}},
    Param6 = {"B6", JParam#janitor_params{bucket_servers = [c]}},
    Param7 = {"B7", JParam#janitor_params{bucket_servers = [c, d, e]}},
    Param8 = {"B8", JParam#janitor_params{bucket_servers = [e, d, c]}},
    Param9 = {"B9", JParam#janitor_params{bucket_servers = [d, e, c]}},

    %% We are creating params with different type of server groups in the
    %% set of buckets, and in this case we will verify the apply config activity
    %% is called with the appropriate groups and buckets
    meck:expect(
      leader_activities, run_activity,
      fun ({ns_janitor, Buckets, apply_config}, {all, Servers}, _, _)
            when (length(Buckets) =:= 3) and (Servers =:= [a, b, c]) ->
              ?assertEqual(["B1", "B2", "B3"], Buckets),
              {ok, [{Bucket, ok} || Bucket <- Buckets]};
          ({ns_janitor, Buckets, apply_config}, {all, Servers} ,_ ,_)
            when (length(Buckets) =:= 3) and (Servers =:= [c, d, e]) ->
              ?assertEqual(["B7", "B8", "B9"], Buckets),
              {ok, [{Bucket, ok} || Bucket <- Buckets]};
          ({ns_janitor, Buckets, apply_config}, {all, Servers} ,_ ,_)
            when (length(Buckets) =:= 2) ->
              ?assertEqual(["B4", "B5"], Buckets),
              ?assertEqual([b, c], Servers),
              {ok, [{Bucket, ok} || Bucket <- Buckets]};
          ({ns_janitor, Buckets, apply_config}, {all, Servers} ,_ ,_)
            when (length(Buckets) =:= 1) ->
              ?assertEqual(["B6"], Buckets),
              ?assertEqual([c], Servers),
              {ok, [{Bucket, ok} || Bucket <- Buckets]}
      end
     ),
    Res2 = cleanup_buckets_with_states(
             [Param1, Param2, Param3, Param4, Param5, Param6, Param7, Param8,
              Param9], Options),

    ?assertEqual([{"B1", ok}, {"B2", ok}, {"B3", ok}, {"B4", ok}, {"B5", ok},
                  {"B6", ok}, {"B7", ok}, {"B8", ok}, {"B9", ok}], Res2),
    ok.

test_mark_bucket_warmed(Status) ->
    meck:expect(guardrail_enforcer, get_status, [{bucket, "B1"}], Status),
    ok = cleanup_mark_bucket_warmed("B1", [node()]),
    %% Return the status that mark_warmed received
    meck:capture(last, ns_memcached, mark_warmed, ["B1", '_'], 2).

cleanup_mark_bucket_warmed_data_ingress_test_body() ->
    Node = node(),
    meck:expect(cluster_compat_mode, is_cluster_76, ?cut(false)),

    meck:expect(janitor_agent_sup, get_registry_pid,
                fun (_) -> self() end),
    meck:expect(ns_bucket, get_bucket,
                fun ("B1") ->
                        {ok, [{type, membase},
                              {storage_mode, magma},
                              {servers, [Node]}]}
                end),
    meck:expect(ns_config, get_timeout,
                fun (_, Default) -> Default end),
    meck:expect(dcp_sup, nuke,
                fun (_) -> ok end),
    meck:expect(ns_storage_conf, this_node_bucket_dbdir,
                fun (_) -> {ok, ""} end),
    meck:expect(ns_bucket, activate_bucket_data_on_this_node,
                fun (_) -> ok end),
    {ok, _} = janitor_agent:start_link("B1"),

    meck:expect(ns_memcached, mark_warmed,
                fun ("B1", _Status) -> ok end),

    %% Pre-7.6, we can't set a status, so ns_memcached:mark_warmed should
    %% get status undefined
    ?assertEqual(undefined, test_mark_bucket_warmed(ok)),

    meck:expect(cluster_compat_mode, is_cluster_76, ?cut(true)),

    %% Post-7.6, ns_memcached:mark_warmed should receive whatever status we
    %% get from guardrail_enforcer
    ?assertEqual(undefined, test_mark_bucket_warmed(undefined)),
    ?assertEqual(ok, test_mark_bucket_warmed(ok)),
    ?assertEqual(resident_ratio, test_mark_bucket_warmed(resident_ratio)).

data_loss_possible_t(Chain, States) ->
    data_loss_possible(0, Chain,
                       dict:from_list([{0, [{N, S, []} || {N, S} <- States]}])).

data_loss_possible_test() ->
    ?assertEqual({true, a}, data_loss_possible_t([a, b], [{b, replica}])),

    %% No copies left, so no data loss.
    ?assertNot(data_loss_possible_t([a, b], [])),

    %% Normal case, all copies are where we expect them to be.
    ?assertNot(data_loss_possible_t([a, b], [{a, active}, {b, replica}])),

    %% For some reason our vbucket is a bad state, but the data is there, so
    %% data loss is possible.
    ?assertEqual({true, a}, data_loss_possible_t([a, b], [{b, dead}])),

    %% Vbuckets that exists on nodes not in the vbucket chain don't matter.
    ?assertNot(data_loss_possible_t([a, b], [{c, replica}])).

check_server_list_test() ->
    meck:new([ns_config], [passthrough]),
    meck:expect(ns_config, read_key_fast,
                fun (failover_ephemeral_no_replicas, false) ->
                        false
                end),

    ?assertEqual({update_servers, [a, b, c]},
                 do_check_server_list("bucket", [], [], [a, b, c])),
    ?assertEqual(ok, do_check_server_list("bucket", [], [a, b], [a, b, c])),
    ?assertEqual(ok, do_check_server_list("bucket", [], [a, b], [a, c, b])),
    ?assertMatch({error, _}, do_check_server_list("bucket", [], [a, b, c],
                                                  [a, b])),
    meck:unload([ns_config]).
-endif.
