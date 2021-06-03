%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
         cleanup_apply_config/4,
         check_server_list/2]).

-spec cleanup(Bucket::bucket_name(), Options::list()) ->
                     ok |
                     {error, wait_for_memcached_failed, [node()]} |
                     {error, marking_as_warmed_failed, [node()]} |
                     {error, unsafe_nodes, [node()]} |
                     {error, {config_sync_failed,
                              pull | push, Details :: any()}} |
                     {error, {bad_vbuckets, [vbucket_id()]}} |
                     {error, {corrupted_server_list, [node()], [node()]}}.
cleanup(Bucket, Options) ->
    Snapshot =
        chronicle_compat:get_snapshot(
          [ns_bucket:fetch_snapshot(Bucket, _),
           ns_cluster_membership:fetch_snapshot(_)]),
    case ns_bucket:get_bucket(Bucket, Snapshot) of
        not_present ->
            ok;
        {ok, BucketConfig} ->
            case ns_bucket:bucket_type(BucketConfig) of
                membase ->
                    cleanup_membase_bucket(Bucket,
                                           Options, BucketConfig, Snapshot);
                _ -> ok
            end
    end.

cleanup_membase_bucket(Bucket, Options, BucketConfig, Snapshot) ->
    %% We always want to check for unsafe nodes, as we want to honor the
    %% auto-reprovisioning settings for ephemeral buckets. That is, we do not
    %% want to simply activate any bucket on a restarted node and lose the data
    %% instead of promoting the replicas.
    AllOptions = Options ++ auto_reprovision:get_cleanup_options(),
    {ok, RV} =
        leader_activities:run_activity(
          {ns_janitor, Bucket, cleanup}, majority,
          fun () ->
                  {ok, cleanup_with_membase_bucket_check_servers(Bucket,
                                                                 AllOptions,
                                                                 BucketConfig,
                                                                 Snapshot)}
          end,
          [quiet]),

    RV.

cleanup_with_membase_bucket_check_servers(Bucket, Options, BucketConfig,
                                          Snapshot) ->
    case check_server_list(Bucket, BucketConfig, Snapshot) of
        ok ->
            cleanup_with_membase_bucket_check_map(Bucket, Options, BucketConfig);
        {update_servers, NewServers} ->
            update_servers(Bucket, NewServers, Options),
            cleanup(Bucket, Options);
        {error, _} = Error ->
            Error
    end.

update_servers(Bucket, Servers, Options) ->
    ?log_debug("janitor decided to update "
               "servers list for bucket ~p to ~p", [Bucket, Servers]),

    ns_bucket:set_servers(Bucket, Servers),
    push_config(Options).

cleanup_with_membase_bucket_check_map(Bucket, Options, BucketConfig) ->
    case proplists:get_value(map, BucketConfig, []) of
        [] ->
            Servers = ns_bucket:get_servers(BucketConfig),
            true = (Servers =/= []),

            ?log_info("janitor decided to generate initial vbucket map"),
            {Map, MapOpts} = ns_rebalancer:generate_initial_map(BucketConfig),
            set_initial_map(Map, MapOpts, Bucket, BucketConfig, Options),

            cleanup(Bucket, Options);
        _ ->
            cleanup_with_membase_bucket_vbucket_map(Bucket, Options, BucketConfig)
    end.

set_initial_map(Map, MapOpts, Bucket, BucketConfig, Options) ->
    case ns_rebalancer:unbalanced(Map, BucketConfig) of
        false ->
            ns_bucket:update_vbucket_map_history(Map, MapOpts);
        true ->
            ok
    end,

    ns_bucket:set_map(Bucket, Map),
    ns_bucket:set_map_opts(Bucket, MapOpts),

    push_config(Options).

cleanup_with_membase_bucket_vbucket_map(Bucket, Options, BucketConfig) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    true = (Servers =/= []),
    Timeout = proplists:get_value(query_states_timeout, Options),
    Opts = [{timeout, Timeout} || Timeout =/= undefined],
    case janitor_agent:query_vbuckets(Bucket, Servers, [], Opts) of
        {States, []} ->
            cleanup_with_states(Bucket, Options, BucketConfig, Servers, States);
        {_States, Zombies} ->
            ?log_info("Bucket ~p not yet ready on ~p", [Bucket, Zombies]),
            {error, wait_for_memcached_failed, Zombies}
    end.

cleanup_with_states(Bucket, Options, BucketConfig, Servers, States) ->
    case maybe_fixup_vbucket_map(Bucket, BucketConfig, States, Options) of
        {ok, NewBucketConfig} ->
            cleanup_check_unsafe_nodes(Bucket, Options,
                                       NewBucketConfig, Servers, States);
        Error ->
            Error
    end.

cleanup_check_unsafe_nodes(Bucket, Options, BucketConfig, Servers, States) ->
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
            cleanup_apply_config(Bucket, Servers, BucketConfig, Options)
    end.

maybe_fixup_vbucket_map(Bucket, BucketConfig, States, Options) ->
    try
        NewBucketConfig = maybe_pull_config(Bucket,
                                            BucketConfig, States, Options),

        case do_maybe_fixup_vbucket_map(Bucket, NewBucketConfig, States) of
            not_needed ->
                %% We decided not to update the bucket config. It still may be
                %% the case that some nodes have extra vbuckets. Before
                %% deleting those, we need to push the config, so all nodes
                %% are on the same page.
                maybe_push_config(Bucket, NewBucketConfig, States, Options),
                {ok, NewBucketConfig};
            {ok, FixedBucketConfig} ->
                %% We decided to fix the bucket config. In this case we push
                %% the config no matter what, i.e. even if durability
                %% awareness is disabled.
                push_config(Options),
                {ok, FixedBucketConfig};
            FixupError ->
                FixupError
        end
    catch
        throw:Error ->
            Error
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
    {ok, Result} =
        leader_activities:run_activity(
          {ns_janitor, Bucket, apply_config}, {all, Servers},
          fun () ->
                  {ok, cleanup_apply_config_body(Bucket, Servers,
                                                 BucketConfig, Options)}
          end,
          [quiet]),

    Result.

config_sync_nodes(Options) ->
    case proplists:get_value(sync_nodes, Options) of
        undefined ->
            ns_cluster_membership:get_nodes_with_status(_ =/= inactiveFailed);
        Nodes when is_list(Nodes) ->
            Nodes
    end.

maybe_config_sync(Type, Bucket, BucketConfig, States, Options) ->
    Flag = config_sync_type_to_flag(Type),
    case proplists:get_value(Flag, Options, true)
        andalso cluster_compat_mode:preserve_durable_mutations() of
        true ->
            {_, Map} = lists:keyfind(map, 1, BucketConfig),
            case map_matches_states_exactly(Map, States) of
                true ->
                    ok;
                {false, Mismatch} ->
                    ?log_debug("Found states mismatch in bucket ~p:~n~p",
                               [Bucket, Mismatch]),
                    config_sync(Type, Options)
            end;
        false ->
            ok
    end.

config_sync_type_to_flag(pull) ->
    pull_config;
config_sync_type_to_flag(push) ->
    push_config.

config_sync(Type, Options) ->
    Nodes = config_sync_nodes(Options),
    Timeout = ?get_timeout({config_sync, Type}, 10000),

    ?log_debug("Going to ~s config to/from nodes:~n~p", [Type, Nodes]),
    try do_config_sync(chronicle_compat:backend(), Type, Nodes, Timeout) of
        ok ->
            ok;
        Error ->
            throw({error, {config_sync_failed, Type, Error}})
    catch
        T:E:Stack ->
            throw({error, {config_sync_failed, Type, {T, E, Stack}}})
    end.

push_config(Options) ->
    config_sync(push, Options).

do_config_sync(chronicle, pull, _Nodes, Timeout) ->
    chronicle_compat:pull(Timeout);
do_config_sync(chronicle, push, _Nodes, _Timeout) ->
    ok; %% don't need to push buckets since we do quorum write
do_config_sync(ns_config, pull, Nodes, Timeout) ->
    ns_config_rep:pull_remotes(Nodes, Timeout);
do_config_sync(ns_config, push, Nodes, Timeout) ->
    %% Explicitly push buckets to other nodes even if didn't modify them. This
    %% is needed because ensure_conig_seen_by_nodes() only makes sure that any
    %% outstanding local mutations are pushed out. But it's possible that we
    %% didn't have any local modifications to buckets, we still want to make
    %% sure that all nodes have received all updates.
    ns_config_rep:push_keys([buckets]),
    ns_config_rep:ensure_config_seen_by_nodes(Nodes, Timeout).

maybe_pull_config(Bucket, BucketConfig, States, Options) ->
    maybe_config_sync(pull, Bucket, BucketConfig, States, Options),
    {ok, NewBucketConfig} = ns_bucket:get_bucket(Bucket),
    NewBucketConfig.

maybe_push_config(Bucket, BucketConfig, States, Options) ->
    maybe_config_sync(push, Bucket, BucketConfig, States, Options).

cleanup_apply_config_body(Bucket, Servers, BucketConfig, Options) ->
    ok = janitor_agent:apply_new_bucket_config(
           Bucket, Servers, BucketConfig,
           proplists:get_value(apply_config_timeout, Options,
                               undefined_timeout)),

    maybe_reset_rebalance_status(Options),

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
            rebalance:reset_status(
              fun () ->
                      ale:info(?USER_LOGGER,
                               "Resetting rebalance status "
                               "since it's not really running"),
                      {none, <<"Rebalance stopped by janitor.">>}
              end);
        false ->
            ok
    end.

%% !!! only purely functional code below (with notable exception of logging) !!!
%% lets try to keep as much as possible logic below this line
check_server_list(Bucket, BucketConfig) ->
    check_server_list(Bucket, BucketConfig, ns_config:latest()).

check_server_list(Bucket, BucketConfig, Snapshot) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    ActiveKVNodes = ns_cluster_membership:service_active_nodes(Snapshot, kv),
    do_check_server_list(Bucket, Servers, ActiveKVNodes).

do_check_server_list(_Bucket, [], ActiveKVNodes) ->
    {update_servers, ActiveKVNodes};
do_check_server_list(Bucket, Servers, ActiveKVNodes) when is_list(Servers) ->
    %% We don't expect for buckets to refer to servers that are not active. We
    %% can't guarantee this though due to weaknesses of ns_config. The best we
    %% can do if we detect a mismatch is to complain and have a human
    %% intervene.
    UnexpectedServers = Servers -- ActiveKVNodes,
    case UnexpectedServers of
        [] ->
            ok;
        _ ->
            ?log_error("Found a corrupt server list in bucket ~p.~n"
                       "Server list: ~p~n"
                       "Active KV nodes: ~p~n"
                       "Unexpected servers: ~p",
                       [Bucket, Servers, ActiveKVNodes, UnexpectedServers]),
            {error, {corrupted_server_list, Servers, ActiveKVNodes}}
    end.

compute_vbucket_map_fixup(Bucket, BucketConfig, States) ->
    Map = proplists:get_value(map, BucketConfig, []),
    true = ([] =/= Map),
    FFMap = proplists:get_value(fastForwardMap, BucketConfig),

    EnumeratedChains = mb_map:enumerate_chains(Map, FFMap),
    MapUpdates = [sanify_chain(Bucket, States, Chain, FutureChain, VBucket)
                  || {VBucket, Chain, FutureChain} <- EnumeratedChains],

    MapLen = length(Map),
    IgnoredVBuckets = [VBucket || {VBucket, ignore} <-
                                      lists:zip(lists:seq(0, MapLen - 1),
                                                MapUpdates)],
    NewMap = [case NewChain of
                  ignore -> OldChain;
                  _ -> NewChain
              end || {NewChain, OldChain} <- lists:zip(MapUpdates, Map)],
    NewAdjustedMap = case cluster_compat_mode:is_cluster_65() of
                         true ->
                             %% Defer adjusting chain length to rebalance, at
                             %% the time of writing this code the logic is in,
                             %% ns_rebalancer:do_rebalance_membase_bucket.
                             NewMap;
                         false ->
                             NumReplicas = ns_bucket:num_replicas(BucketConfig),
                             mb_map:align_replicas(Map, NumReplicas)
                     end,
    NewBucketConfig = case NewAdjustedMap =:= Map of
                          true ->
                              BucketConfig;
                          false ->
                              ?log_debug("Janitor decided to update vbucket map"),
                              lists:keyreplace(map, 1, BucketConfig,
                                               {map, NewAdjustedMap})
                      end,
    {NewBucketConfig, IgnoredVBuckets}.

%% this will decide what vbucket map chain is right for this vbucket
sanify_chain(_Bucket, _States,
             [CurrentMaster | _] = CurrentChain,
             _FutureChain, _VBucket) when CurrentMaster =:= undefined ->
    %% We can get here on a hard-failover case.
    CurrentChain;
sanify_chain(Bucket, States,
             [CurrentMaster | _] = CurrentChain,
             FutureChain, VBucket) ->
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
sanify_chain_t(States, CurrentChain, FutureChain) ->
    sanify_chain("B",
                 dict:from_list(
                   [{0, [{N, S, []} || {N, S} <- States]}]),
                 CurrentChain, FutureChain, 0).

sanify_basic_test() ->
    %% normal case when everything matches vb map
    [a, b] = sanify_chain_t([{a, active}, {b, replica}], [a, b], []),

    %% yes, the code will keep both masters as long as expected master
    %% is there. Possibly something to fix in future
    [a, b] = sanify_chain_t([{a, active}, {b, active}], [a, b], []),

    %% main chain doesn't match but fast-forward chain does
    [b, c] = sanify_chain_t([{a, dead}, {b, active}, {c, replica}],
                            [a, b], [b, c]),

    %% main chain doesn't match but ff chain does. And old master is already
    %% deleted
    [b, c] = sanify_chain_t([{b, active}, {c, replica}], [a, b], [b, c]),

    %% lets make sure we touch all paths just in case
    %% this runs "there are >1 unexpected master" case
    ignore = sanify_chain_t([{a, active}, {b, active}], [c, a, b], []),

    %% this runs "master is one of replicas" case
    [b, undefined] = sanify_chain_t([{b, active}, {c, replica}], [a, b], []),

    %% and this runs "master is some non-chain member node" case
    [c, undefined] = sanify_chain_t([{c, active}], [a, b], []),

    %% lets also test rebalance stopped prior to complete takeover
    [a, b] = sanify_chain_t([{a, dead}, {b, replica}, {c, pending},
                             {d, replica}], [a, b], [c, d]),
    ok.

sanify_doesnt_lose_replicas_on_stopped_rebalance_test() ->
    %% simulates the following: We've completed move that switches
    %% replica and active but rebalance was stopped before we updated
    %% vbmap. We have code in sanify to detect this condition using
    %% fast-forward map and is supposed to recover perfectly from this
    %% condition.
    [a, b] = sanify_chain_t([{a, active}, {b, dead}], [b, a], [a, b]),

    %% rebalance can be stopped after updating vbucket states but
    %% before vbucket map update
    [a, b] = sanify_chain_t([{a, active}, {b, replica}], [b, a], [a, b]),
    %% same stuff but prior to takeover
    [a, b] = sanify_chain_t([{a, dead}, {b, pending}], [a, b], [b, a]),

    %% lets test more usual case too
    [c, d] = sanify_chain_t([{a, dead}, {b, replica}, {c, active},
                             {d, replica}], [a, b], [c, d]),

    %% but without FF map we're (too) conservative (should be fixable
    %% someday)
    [c, undefined] = sanify_chain_t([{a, dead}, {b, replica}, {c, active},
                                     {d, replica}], [a, b], []).

sanify_addition_of_replicas_test() ->
    [a, b] = sanify_chain_t([{a, active}, {b, replica}], [a, b], [a, b, c]),
    [a, b] = sanify_chain_t([{a, active}, {b, replica}, {c, replica}],
                            [a, b], [a, b, c]),

    %% replica addition with possible move.
    [a, b] = sanify_chain_t([{a, dead}, {b, replica}, {c, pending}],
                            [a, b], [c, a, b]),
    [c, d, a] = sanify_chain_t([{a, dead}, {b, replica}, {c, active},
                                {d, replica}], [a, b], [c, d, a]),
    [c, d, a] = sanify_chain_t([{a, replica}, {b, replica}, {c, active},
                                {d, replica}], [a, b], [c, d, a]).

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
    ?assertEqual({update_servers, [a, b, c]},
                 do_check_server_list("bucket", [], [a, b, c])),
    ?assertEqual(ok, do_check_server_list("bucket", [a, b], [a, b, c])),
    ?assertEqual(ok, do_check_server_list("bucket", [a, b], [a, c, b])),
    ?assertMatch({error, _}, do_check_server_list("bucket", [a, b, c], [a, b])).
-endif.
