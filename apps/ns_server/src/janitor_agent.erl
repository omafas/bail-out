%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(janitor_agent).

-behavior(gen_server).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(WAIT_FOR_MEMCACHED_TIMEOUT, ?get_timeout(wait_for_memcached, 5000)).
-define(APPLY_NEW_CONFIG_TIMEOUT,   ?get_timeout(apply_config, 60000)).
%% NOTE: there's also ns_memcached timeout anyways
-define(DELETE_VBUCKET_TIMEOUT,     ?get_timeout(delete_vbucket, 120000)).
-define(PREPARE_REBALANCE_TIMEOUT,  ?get_timeout(prepare_rebalance, 30000)).
-define(PREPARE_FLUSH_TIMEOUT,      ?get_timeout(prepare_flush, 30000)).
-define(SET_VBUCKET_STATE_TIMEOUT,  ?get_timeout(set_vbucket_state, infinity)).
-define(GET_SRC_DST_REPLICATIONS_TIMEOUT,
        ?get_timeout(get_src_dst_replications, 30000)).
-define(QUERY_VBUCKETS_SLEEP, ?get_param(query_vbuckets_sleep, 1000)).
-define(MOUNT_VOLUMES_TIMEOUT,  ?get_timeout(mount_volumes, 30000)).

-record(state, {bucket_name :: bucket_name(),
                rebalance_pid :: undefined | pid(),
                rebalance_mref :: undefined | reference(),
                rebalance_subprocesses = [] :: [{From :: term(),
                                                 Worker :: pid()}],
                last_applied_vbucket_states :: undefined | list(),
                rebalance_only_vbucket_states :: undefined | list(),
                flushseq,
                rebalance_status = finished :: in_process | finished,

                apply_vbucket_states_queue :: undefined | queue:queue(),
                apply_vbucket_states_worker :: undefined | pid(),
                rebalance_subprocesses_registry :: pid()}).

-export([query_vbuckets/4,
         fetch_vbucket_states/2,
         find_vbucket_state/2,
         check_bucket_ready/3,
         apply_new_bucket_config/4,
         mark_bucket_warmed/2,
         maybe_set_data_ingress/3,
         delete_vbucket_copies/4,
         prepare_nodes_for_rebalance/3,
         finish_rebalance/3,
         this_node_replicator_triples/1,
         bulk_set_vbucket_state/4,
         set_vbucket_state/8,
         get_src_dst_vbucket_replications/2,
         get_src_dst_vbucket_replications/3,
         initiate_indexing/5,
         wait_index_updated/5,
         mass_prepare_flush/2,
         complete_flush/3,
         get_dcp_docs_estimate/4,
         get_mass_dcp_docs_estimate/3,
         get_all_vb_seqnos/2,
         wait_dcp_data_move/5,
         wait_seqno_persisted/5,
         get_vbucket_high_seqno/4,
         dcp_takeover/5,
         inhibit_view_compaction/3,
         uninhibit_view_compaction/4,
         get_failover_logs/2,
         mount_volumes/4,
         maybe_start_fusion_uploaders/3,
         maybe_stop_fusion_uploaders/3,
         get_active_guest_volumes/2,
         sync_fusion_log_store/1]).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

call(Bucket, Node, Call, Timeout) ->
    gen_server:call({server_name(Bucket), Node}, Call, Timeout).

multi_call(Bucket, Nodes, Call, Timeout) ->
    gen_server:multi_call(Nodes, server_name(Bucket), Call, Timeout).

rebalance_call(undefined, Bucket, Node, Call, Timeout) ->
    call(Bucket, Node, Call, Timeout);
rebalance_call(Rebalancer, Bucket, Node, Call, Timeout) ->
    call(Bucket, Node, {if_rebalance, Rebalancer, Call}, Timeout).

rebalance_multi_call(Rebalancer, Bucket, Nodes, Call, Timeout) ->
    multi_call(Bucket, Nodes, {if_rebalance, Rebalancer, Call}, Timeout).

query_vbuckets(Node, Bucket, Call, Parent, Timeout) ->
    async:run_with_timeout(
        fun() ->
                query_vbuckets_loop(Node, Bucket, Call, Parent)
        end, Timeout).

query_vbuckets_loop(Node, Bucket, Call, Parent) ->
    query_vbuckets_loop_inner(Node, Bucket, Call, Parent, not_sent_warming).

query_vbuckets_loop_inner(Node, Bucket, Call, Parent, Warming) ->
    case (catch call(Bucket, Node, Call, infinity)) of
        {ok, _} = Msg ->
            Msg;
        warming_up ->
            maybe_send_warming(Parent, Warming),
            timer:sleep(?QUERY_VBUCKETS_SLEEP),
            query_vbuckets_loop_inner(Node, Bucket, Call, Parent, sent_warming);
        {'EXIT', {noproc, _}} = Exc ->
            ?log_debug("Exception from ~p of ~p:~p~n~p",
                [Call, Bucket, Node, Exc]),
            timer:sleep(?QUERY_VBUCKETS_SLEEP),
            query_vbuckets_loop_inner(Node, Bucket, Call, Parent, Warming);
        Exc ->
            ?log_debug("Exception from ~p of ~p:~p~n~p",
                        [Call, Bucket, Node, Exc]),
            Exc
    end.

maybe_send_warming(Pid, not_sent_warming) ->
    Pid ! warming_up;
maybe_send_warming(_Pid, sent_warming) ->
    ok.

-type query_vbuckets_call() :: {query_vbuckets, [vbucket_id()], list(), list()}.
-spec wait_for_memcached([{node(), query_vbuckets_call()}], bucket_name(),
                         non_neg_integer()) -> [{node(),
                                                 {ok, list()} |
                                                 {ok, dict:dict()} |
                                                 warming_up | timeout | any()}].
wait_for_memcached(NodeCalls, Bucket, WaitTimeout) ->
    misc:parallel_map(
        fun({Node, Call}) ->
            Parent = self(),
            case query_vbuckets(Node, Bucket, Call, Parent, WaitTimeout) of
                {error, timeout} ->
                    %% query_vbuckets will send us a warming_up message if it
                    %% sees a node warming up before checking the status again.
                    %% It will only return if it is successful (otherwise it
                    %% will get timed out). We want to return warming_up up the
                    %% stack though to try again later.
                    receive
                        warming_up -> {Node, warming_up}
                        after 0 -> {Node, timeout}
                    end;
                {ok, Result} ->
                    %% This (async) process is about to get destroyed and we
                    %% may have some 'warming_up' message in the mailbox here.
                    %% As we're about to nuke the process, clearing the
                    %% mailbox is unnecessary, but if this code is modified
                    %% in the future then this message may need to be flushed
                    %% before continuing processing.
                    {Node, Result}
            end
        end, NodeCalls,
        %% Infinite timeout here so that:
        %%     1) the inner function can handle a timeout in a controlled
        %%        manner and return a meaningful status instead of just
        %%        exiting the process.
        %%     2) We can give each node the same exact timeout,
        %%        misc:parallel_map would give nodes later in the list
        %%        slightly less time.
        infinity).

complete_flush(Bucket, Nodes, Timeout) ->
    {Replies, BadNodes} = multi_call(Bucket, Nodes, complete_flush, Timeout),
    {GoodReplies, BadReplies} = lists:partition(fun ({_N, R}) -> R =:= ok end,
                                                Replies),
    GoodNodes = [N || {N, _R} <- GoodReplies],
    {GoodNodes, BadReplies, BadNodes}.

failures_to_zombies(Failures) ->
    [N || {N, _} <- Failures].

check_bucket_ready(Bucket, Nodes, Timeout) ->
    case do_query_vbuckets(Bucket, Nodes, [], [{timeout, Timeout}]) of
        {_States, []} ->
            ready;
        {_States, Failures} ->
            Zombies = failures_to_zombies(Failures),
            ?log_debug("Querying bucket ~p states failed. Failures: ~p",
                       [Bucket, Failures]),
            case lists:all(fun ({_Node, warming_up}) ->
                                   true;
                               ({_Node, _}) ->
                                   false
                           end, Failures) of
                true ->
                    {warming_up, Zombies};
                false ->
                    {failed, Zombies}
            end
    end.

convert_call_result(Node, {VBucket, State}) ->
    {VBucket, {Node, State, []}};
convert_call_result(Node, {VBucket, State, ExtraValues}) ->
    {VBucket, {Node, State, ExtraValues}}.

-type nodes_states() :: [{node(), vbucket_state(), list()}].

-spec fetch_vbucket_states(vbucket_id(), dict:dict()) -> nodes_states().
fetch_vbucket_states(VBucket, States) ->
    case dict:find(VBucket, States) of
        {ok, Infos} ->
            Infos;
        error ->
            []
    end.

-spec find_vbucket_state(node(), nodes_states()) -> vbucket_state() | missing.
find_vbucket_state(Node, NodeStates) ->
    case lists:keyfind(Node, 1, NodeStates) of
        {Node, State, _} ->
            State;
        false ->
            missing
    end.

-spec query_vbuckets(bucket_name(), [node()] | [{node(), [vbucket_id()]}],
                     list(), list()) -> {dict:dict(), [node()]}.
query_vbuckets(Bucket, Nodes, ExtraKeys, Options) ->
    {NodeRVs, Failures} = do_query_vbuckets(Bucket, Nodes, ExtraKeys, Options),
    ConvertedResults =
        [convert_call_result(Node, ResultTuple)
         || {Node, {ok, Tuples}} <- NodeRVs, ResultTuple <- Tuples],

    case Failures =/= [] of
        true ->
            ?log_debug("Query vbuckets for bucket ~p failed on some nodes:"
                       "~n~p", [Bucket, Failures]);
        false ->
            ok
    end,

    {dict:from_list(maps:to_list(maps:groups_from_list(
                                   element(1, _), element(2, _),
                                   ConvertedResults))),
     failures_to_zombies(Failures)}.

%% TODO: consider supporting partial janitoring
do_query_vbuckets(Bucket, Nodes, ExtraKeys, Options) ->
    Timeout = proplists:get_value(timeout, Options,
                                  ?WAIT_FOR_MEMCACHED_TIMEOUT),

    CreateCall = {query_vbuckets, _, ExtraKeys, Options},

    NodeCalls = lists:map(fun ({N, VBs}) ->
                                  {N, CreateCall(VBs)};
                              (N) ->
                                  {N, CreateCall(all)}
                          end, Nodes),

    RVs = wait_for_memcached(NodeCalls, Bucket, Timeout),
    lists:partition(fun ({_, {ok, _}}) ->
                            true;
                        ({_, _}) ->
                            false
                    end, RVs).

-spec mark_bucket_warmed(Bucket::bucket_name(),
                         [node()]) -> ok | {errors, [{node(), term()}]}.
mark_bucket_warmed(Bucket, Nodes) ->
    DataIngress = guardrail_enforcer:get_status({bucket, Bucket}),
    Call =
        case cluster_compat_mode:is_cluster_76() of
            false ->
                %% Ensure that we send the correct call to down-version nodes
                %% during upgrade
                mark_warmed;
            true ->
                {mark_warmed, DataIngress}
        end,
    process_multicall_rv(
      multi_call(Bucket, Nodes, Call, warmed_timeout())).

%% This timeout accounts for the timeouts of underlying operations plus
%% an additional second to ensure we catch any occurrences of the worst
%% case.
warmed_timeout() ->
    Default =
        ns_memcached:get_mark_warmed_timeout() +
        chronicle_kv:get_txn_default_timeout() + 1000,
    %% Allow overriding
    ?get_timeout(warmed, Default).

apply_new_bucket_config(Bucket, Servers, NewBucketConfig, undefined_timeout) ->
    apply_new_bucket_config(Bucket, Servers, NewBucketConfig,
                            ?APPLY_NEW_CONFIG_TIMEOUT);
apply_new_bucket_config(Bucket, Servers, NewBucketConfig, Timeout) ->
    functools:sequence_(
      [?cut(call_on_servers(Bucket, Servers, NewBucketConfig,
                            apply_new_config, Timeout)),
       ?cut(call_on_servers(Bucket, Servers, NewBucketConfig,
                            apply_new_config_replicas_phase, Timeout))]).

-spec maybe_set_data_ingress(bucket_name(), Status, [node()]) ->
          ok | {errors, [{node(), mc_error()}]}
              when Status :: undefined | data_ingress_status().
maybe_set_data_ingress(_Bucket, undefined, _Servers) ->
    %% Guard rail not enabled
    ok;
maybe_set_data_ingress(Bucket, Status, Servers) ->
    RVs = lists:map(
            fun (Node) ->
                    {Node,
                     catch servant_call(Bucket, Node,
                                        {set_data_ingress, Status})}
            end, Servers),
    RV = lists:filter(fun ({_Node, ok}) -> false;
                          (_) -> true
                      end, RVs),
    case RV of
        [] ->
            ok;
        BadReplies ->
            {errors, BadReplies}
    end.

-spec mount_volumes(bucket_name(), [{node(), list()}], map(), pid()) ->
          ok | {errors, [{node(), term()}]}.
mount_volumes(Bucket, VolumesToMount, NodesMap, RebalancerPid) ->
    NodesCalls =
        lists:map(
          fun ({N, VBuckets}) ->
                  Volumes = proplists:get_value(N, VolumesToMount),
                  {N, {mount_volumes, VBuckets, Volumes}}
          end, maps:to_list(NodesMap)),
    call_on_nodes(Bucket, NodesCalls,
                  rebalance_call(RebalancerPid, _, _, _,
                                 ?MOUNT_VOLUMES_TIMEOUT)).

call_on_nodes(Bucket, NodesCalls, Caller) ->
    case do_call_on_nodes(Bucket, NodesCalls, Caller) of
        {error, Error} ->
            {error, Error};
        _ ->
            ok
    end.

call_on_nodes_with_returns(Bucket, NodesCalls, Caller) ->
    case do_call_on_nodes(Bucket, NodesCalls, Caller) of
        {error, Error} ->
            {error, Error};
        Returns ->
            {ok, [{N, R} || {N, _, {ok, R}} <- Returns]}
    end.

do_call_on_nodes(Bucket, NodesCalls, Caller) ->
    Replies =
        misc:parallel_map(
          fun ({Node, Call}) ->
                  {Node, Call, catch Caller(Bucket, Node, Call)}
          end, NodesCalls, infinity),
    {Returns, BadReplies} = lists:partition(fun ({_, _, {ok, _}}) ->
                                                    true;
                                                ({_, _, ok}) ->
                                                    true;
                                                (_) ->
                                                    false
                                            end, Replies),
    case BadReplies of
        [] ->
            Returns;
        _ ->
            ?log_info("~s:Some janitor requests have failed"
                      ":~n~p", [Bucket, BadReplies]),
            {error, {failed_nodes, [N || {N, _, _} <- BadReplies]}}
    end.

call_on_servers(Bucket, Servers, BucketConfig, Call, Timeout) ->
    NodesCalls = [{N, {Call, BucketConfig}} || N <- Servers],
    call_on_nodes(Bucket, NodesCalls, call(_, _, _, Timeout)).

process_multicall_rv({Replies, BadNodes}) ->
    BadReplies = [R || {_, RV} = R <- Replies, RV =/= ok],
    process_multicall_rv(BadReplies, BadNodes).

process_multicall_rv([], []) ->
    ok;
process_multicall_rv(BadReplies, BadNodes) ->
    {errors, [{N, bad_node} || N <- BadNodes] ++ BadReplies}.

-spec maybe_start_fusion_uploaders(
        node(), bucket_name(), [{vbucket_id(), integer()}]) -> ok.
maybe_start_fusion_uploaders(_Node, _Bucket, []) ->
    ok;
maybe_start_fusion_uploaders(Node, Bucket, Uploaders) ->
    gen_server:cast({server_name(Bucket), Node},
                    {maybe_start_fusion_uploaders, Uploaders}).

-spec maybe_stop_fusion_uploaders(node(), bucket_name(), [vbucket_id()]) -> ok.
maybe_stop_fusion_uploaders(_Node, _Bucket, []) ->
    ok;
maybe_stop_fusion_uploaders(Node, Bucket, VBuckets) ->
    gen_server:cast({server_name(Bucket), Node},
                    {maybe_stop_fusion_uploaders, VBuckets}).

-spec get_active_guest_volumes(bucket_name(), proplists:proplist()) ->
          {error, {failed_nodes, [node()]}} | {ok, [{node(), [binary()]}]}.
get_active_guest_volumes(Bucket, BucketConfig) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    NodesCalls = [{Node, get_active_guest_volumes} || Node <- Servers],
    call_on_nodes_with_returns(Bucket, NodesCalls, fun servant_call/3).

-spec sync_fusion_log_store([bucket_name()]) -> ok | {failed_nodes, [node()]}.
sync_fusion_log_store(BucketNames) ->
    Replies =
        misc:parallel_map(
          fun (Bucket) ->
                  Uploaders = ns_bucket:get_fusion_uploaders(Bucket),
                  UploadersMap =
                      lists:foldl(
                        fun ({VB, {Node, _}}, Acc) ->
                                maps:update_with(Node, [VB | _], [VB], Acc)
                        end, #{}, misc:enumerate(Uploaders, 0)),
                  NodesCalls =
                      [{Node, {sync_fusion_log_store, VBuckets}} ||
                          {Node, VBuckets} <- maps:to_list(UploadersMap)],
                  {Bucket,
                   call_on_nodes(Bucket, NodesCalls, fun servant_call/3)}
          end, BucketNames, infinity),

    case [R || {_, RV} = R <- Replies, RV =/= ok] of
        [] ->
            ok;
        BadReplies ->
            ?log_info("Errors synchronizing fusion log store:~n~p",
                      [BadReplies]),
            BadNodes =
                lists:usort(lists:flatten(
                              [Nodes || {_, {error, {failed_nodes, Nodes}}}
                                            <- BadReplies])),
            {failed_nodes, BadNodes}
    end.

-spec delete_vbucket_copies(bucket_name(), pid(), [node()], vbucket_id()) ->
                                   ok | {errors, [{node(), term()}]}.
delete_vbucket_copies(Bucket, RebalancerPid, Nodes, VBucket) ->
    process_multicall_rv(
      rebalance_multi_call(RebalancerPid, Bucket, Nodes,
                           {delete_vbucket, VBucket}, ?DELETE_VBUCKET_TIMEOUT)).

-spec prepare_nodes_for_rebalance(bucket_name(), [node()], pid()) ->
                                         ok | {errors, [{node(), term()}]}.
prepare_nodes_for_rebalance(Bucket, Nodes, RebalancerPid) ->
    {Replies, BadNodes} =
        multi_call(Bucket, Nodes, {prepare_rebalance, RebalancerPid},
                   ?PREPARE_REBALANCE_TIMEOUT),
    BadReplies = lists:filter(fun ({_, ok}) ->
                                      false;
                                  (_) ->
                                      true
                              end, Replies),
    process_multicall_rv(BadReplies, BadNodes).

finish_rebalance(Bucket, Nodes, RebalancerPid) ->
    process_multicall_rv(
      rebalance_multi_call(RebalancerPid, Bucket, Nodes, finish_rebalance,
                           ?PREPARE_REBALANCE_TIMEOUT)).

%% this is only called by
%% failover_safeness_level:build_local_safeness_info_new.
%%
%% It's also ok to do 'dirty' reads, i.e. outside of janitor agent,
%% because stale data is ok.
this_node_replicator_triples(Bucket) ->
    case replication_manager:get_incoming_replication_map(Bucket) of
        not_running ->
            [];
        List ->
            [{SrcNode, node(), VBs} || {SrcNode, VBs} <- List]
    end.

-spec bulk_set_vbucket_state(
        bucket_name(),
        pid(),
        vbucket_id(),
        [{Node::node(), vbucket_state(), rebalance_vbucket_state(),
          Src::(node()|undefined), [ns_memcached:set_vbucket_option()]}])
                            -> ok.
bulk_set_vbucket_state(Bucket, RebalancerPid, VBucket, StateMutation) ->
    ?rebalance_info("Doing bulk vbucket ~p state change~n~p",
                    [VBucket, StateMutation]),
    RVs = misc:parallel_map(
            fun ({Node, active, _, _, _}) ->
                    {Node, unexpected_state_active};
                ({Node, VBucketState, VBucketRebalanceState, ReplicateFrom,
                  Options}) ->
                    {Node, (catch set_vbucket_state(
                                    Bucket, Node, RebalancerPid, VBucket,
                                    VBucketState, VBucketRebalanceState,
                                    ReplicateFrom, Options))}
            end, StateMutation, infinity),
    NonOks = [Pair || {_Node, R} = Pair <- RVs,
                      R =/= ok],
    case NonOks of
        [] -> ok;
        _ ->
            ?rebalance_debug("bulk vbucket state change failed for:~n~p",
                             [NonOks]),
            erlang:error({bulk_set_vbucket_state_failed, NonOks})
    end.

set_vbucket_state(Bucket, Node, RebalancerPid, VBucket, VBucketState,
                  VBucketRebalanceState, ReplicateFrom, Options) ->
    SubCall = case cluster_compat_mode:is_cluster_morpheus() of
                  true ->
                      {update_vbucket_state, VBucket, VBucketState,
                       VBucketRebalanceState, ReplicateFrom, Options};
                  false ->
                      {update_vbucket_state, VBucket, VBucketState,
                       VBucketRebalanceState, ReplicateFrom,
                       proplists:get_value(topology, Options)}
              end,
    set_vbucket_state_inner(Bucket, Node, RebalancerPid, VBucket, SubCall).

set_vbucket_state_inner(Bucket, Node, RebalancerPid, VBucket, SubCall) ->
    ?rebalance_info("Doing vbucket ~p state change: ~p",
                    [VBucket, {Node, SubCall}]),
    ok = rebalance_call(RebalancerPid, Bucket, Node, SubCall,
                        ?SET_VBUCKET_STATE_TIMEOUT).

get_src_dst_vbucket_replications(Bucket, Nodes) ->
    get_src_dst_vbucket_replications(Bucket, Nodes,
                                     ?GET_SRC_DST_REPLICATIONS_TIMEOUT).

get_src_dst_vbucket_replications(Bucket, Nodes, Timeout) ->
    {OkResults, FailedNodes} =
        multi_call(Bucket, Nodes, get_incoming_replication_map, Timeout),
    Replications = [{Src, Dst, VB}
                    || {Dst, Pairs} <- OkResults,
                       {Src, VBs} <- Pairs,
                       VB <- VBs],
    {lists:sort(Replications), FailedNodes}.

initiate_indexing(_Bucket, _Rebalancer, [] = _MaybeMaster, _ReplicaNodes,
                  _VBucket) ->
    ok;
initiate_indexing(Bucket, Rebalancer, [NewMasterNode], _ReplicaNodes,
                  _VBucket) ->
    ?rebalance_info("~s: Doing initiate_indexing call for ~s",
                    [Bucket, NewMasterNode]),
    ok = rebalance_call(Rebalancer, Bucket, NewMasterNode, initiate_indexing,
                        infinity).

wait_index_updated(Bucket, Rebalancer, NewMasterNode, _ReplicaNodes, VBucket) ->
    ?rebalance_info("~s: Doing wait_index_updated call for ~s (vbucket ~p)",
                    [Bucket, NewMasterNode, VBucket]),
    ok = rebalance_call(Rebalancer, Bucket, NewMasterNode,
                        {wait_index_updated, VBucket}, infinity).

wait_dcp_data_move(Bucket, Rebalancer, MasterNode, ReplicaNodes, VBucket) ->
    rebalance_call(Rebalancer, Bucket, MasterNode,
                   {wait_dcp_data_move, ReplicaNodes, VBucket}, infinity).

dcp_takeover(Bucket, Rebalancer, OldMasterNode, NewMasterNode, VBucket) ->
    rebalance_call(Rebalancer, Bucket, NewMasterNode,
                   {dcp_takeover, OldMasterNode, VBucket}, infinity).

get_vbucket_high_seqno(Bucket, Rebalancer, MasterNode, VBucket) ->
    ?rebalance_info(
       "~s: Doing get_vbucket_high_seqno call for vbucket ~p on ~s",
       [Bucket, VBucket, MasterNode]),
    RV = rebalance_call(Rebalancer, Bucket, MasterNode,
                        {get_vbucket_high_seqno, VBucket}, infinity),
    true = is_integer(RV),
    RV.

-spec wait_seqno_persisted(bucket_name(), pid(), node(), vbucket_id(),
                           seq_no()) -> ok.
wait_seqno_persisted(Bucket, Rebalancer, Node, VBucket, SeqNo) ->
    ok = rebalance_call(Rebalancer, Bucket, Node,
                        {wait_seqno_persisted, VBucket, SeqNo}, infinity).

-spec inhibit_view_compaction(bucket_name(), pid(), node()) ->
                                     {ok, reference()} | nack.
inhibit_view_compaction(Bucket, Rebalancer, Node) ->
    rebalance_call(Rebalancer, Bucket, Node,
                   {inhibit_view_compaction, Rebalancer}, infinity).

-spec uninhibit_view_compaction(bucket_name(), pid(), node(), reference()) ->
                                       ok | nack.
uninhibit_view_compaction(Bucket, Rebalancer, Node, Ref) ->
    rebalance_call(Rebalancer, Bucket, Node,
                   {uninhibit_view_compaction, Ref}, infinity).

initiate_servant_call(Bucket, Node, Request) ->
    {ServantPid, Tag} = call(Bucket, Node, Request, infinity),
    MRef = erlang:monitor(process, ServantPid),
    {MRef, Tag}.

get_servant_call_reply({MRef, Tag}) ->
    receive
        {'DOWN', MRef, _, _, Reason} ->
            receive
                {Tag, Reply} ->
                    Reply
            after 0 ->
                    erlang:error({janitor_agent_servant_died, Reason})
            end
    end.

servant_call(Bucket, Node, Request) ->
    get_servant_call_reply(initiate_servant_call(Bucket, Node, Request)).

-spec get_dcp_docs_estimate(bucket_name(), node(), vbucket_id(), [node()]) ->
                                   [{ok, {non_neg_integer(), non_neg_integer(),
                                          binary()}}].
get_dcp_docs_estimate(Bucket, SrcNode, VBucket, ReplicaNodes) ->
    servant_call(Bucket, SrcNode,
                 {get_dcp_docs_estimate, VBucket, ReplicaNodes}).

-spec get_mass_dcp_docs_estimate(bucket_name(), node(), [vbucket_id()]) ->
                                        {ok, [{non_neg_integer(),
                                               non_neg_integer(), binary()}]}.
get_mass_dcp_docs_estimate(_Bucket, _Node, []) ->
    {ok, []};
get_mass_dcp_docs_estimate(Bucket, Node, VBuckets) ->
    RV = servant_call(Bucket, Node, {get_mass_dcp_docs_estimate, VBuckets}),
    {ok, _} = RV,
    RV.

-spec get_all_vb_seqnos(bucket_name(), node()) ->
          {ok, [{vbucket_id(), seq_no()}]} | mc_error().
get_all_vb_seqnos(Bucket, Node) ->
    servant_call(Bucket, Node, get_all_vb_seqnos).

mass_prepare_flush(Bucket, Nodes) ->
    {Replies, BadNodes} =
        multi_call(Bucket, Nodes, prepare_flush, ?PREPARE_FLUSH_TIMEOUT),
    {GoodReplies, BadReplies} =
        lists:partition(fun ({_N, R}) -> R =:= ok end, Replies),
    GoodNodes = [N || {N, _R} <- GoodReplies],
    {GoodNodes, BadReplies, BadNodes}.

get_failover_logs(Bucket, NodeVBuckets) ->
    Timeout = ?get_timeout(get_failover_logs, 60000),
    Results =
        misc:parallel_map_partial(
          fun ({Node, VBuckets}) ->
                  servant_call(Bucket, Node, {get_failover_logs, VBuckets})
          end, NodeVBuckets, Timeout),
    process_get_failover_logs_results(NodeVBuckets, Results).

process_get_failover_logs_results(NodeVBuckets, Results) ->
    {Good, Bad} =
        lists:foldl(
          fun ({{Node, _VBuckets}, Result}, {AccGood, AccBad}) ->
                  case Result of
                      {ok, {ok, FailoverLogs}} ->
                          NewGood = [{Node, FailoverLogs} | AccGood],
                          {NewGood, AccBad};
                      {ok, Error} ->
                          {AccGood, [{Node, Error} | AccBad]};
                      _Error ->
                          {AccGood, [{Node, Result} | AccBad]}
                  end
          end, {[], []}, lists:zip(NodeVBuckets, Results)),

    case Bad of
        [] ->
            {ok, Good};
        _ ->
            {error, {failed_nodes, Bad}}
    end.

%% ----------- implementation -----------

start_link(Bucket) ->
    proc_lib:start_link(?MODULE, init, [Bucket]).

init(BucketName) ->
    ServerName = server_name(BucketName),
    register(ServerName, self()),

    proc_lib:init_ack({ok, self()}),

    RegistryPid = janitor_agent_sup:get_registry_pid(BucketName),
    true = is_pid(RegistryPid),

    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    case ns_bucket:bucket_type(BucketConfig) of
        memcached ->
            ok;
        _ ->
            %% Drop all replications if we crashed, so that:
            %%  - Our state is consistent with the state of replications.
            %%  - All outstanding state changes due to inflight takeovers have
            %%    completed, and therefore query_vbuckets returns consistent
            %%    results.
            dcp_sup:nuke(BucketName)
    end,

    State = #state{bucket_name = BucketName,
                   flushseq = read_flush_counter(BucketName),
                   rebalance_subprocesses_registry = RegistryPid},
    gen_server:enter_loop(?MODULE, [], State, {local, ServerName}).

handle_call({if_rebalance, RebalancerPid, Subcall},
            From,
            #state{rebalance_pid = RealRebalancerPid} = State) ->
    case RealRebalancerPid =:= RebalancerPid of
        true ->
            do_handle_call(Subcall, From, State);
        false ->
            ?log_error(
               "Rebalance call failed due to the wrong rebalancer pid ~p. "
               "Should be ~p.", [RebalancerPid, RealRebalancerPid]),
            {reply, wrong_rebalancer_pid, State}
    end;
handle_call({get_dcp_docs_estimate, _VBucketId, _ReplicaNodes} = Req, From,
            State) ->
    handle_call_via_servant(
      From, State, Req,
      fun ({_, VBucketId, ReplicaNodes}, #state{bucket_name = Bucket}) ->
              [dcp_replicator:get_docs_estimate(Bucket, VBucketId, Node)
               || Node <- ReplicaNodes]
      end);
handle_call({get_mass_dcp_docs_estimate, VBucketsR}, From, State) ->
    handle_call_via_servant(
      From, State, VBucketsR,
      fun (VBuckets, #state{bucket_name = Bucket}) ->
              ns_memcached:get_mass_dcp_docs_estimate(Bucket, VBuckets)
      end);
handle_call(get_all_vb_seqnos, From, State) ->
    handle_call_via_servant(
      From, State, undefined,
      fun (undefined, #state{bucket_name = Bucket}) ->
              ns_memcached:get_all_vb_seqnos(Bucket)
      end);
handle_call({set_data_ingress, _Status} = Req, From, State) ->
    handle_call_via_servant(
      From, State, Req,
      fun ({set_data_ingress, Status}, #state{bucket_name = Bucket}) ->
              ns_memcached:set_data_ingress(Bucket, Status)
      end);
handle_call(get_active_guest_volumes, From, State) ->
    handle_call_via_servant(
      From, State, undefined,
      fun (undefined, #state{bucket_name = Bucket}) ->
              ns_memcached:get_active_guest_volumes(Bucket)
      end);
handle_call(Call, From, State) ->
    do_handle_call(Call, From, cleanup_rebalance_artifacts(Call, State)).

do_handle_call(prepare_flush, _From, #state{bucket_name = BucketName}
               = State) ->
    ?log_info("Preparing flush by disabling bucket traffic"),
    %% make sure that node received bucket configuration updates made
    %% by orchestrator
    chronicle_compat:pull(),
    ns_bucket:deactivate_bucket_data_on_this_node(BucketName),
    {reply, ns_memcached:disable_traffic(BucketName, infinity), State};
do_handle_call(complete_flush, _From, State) ->
    %% make sure that node received bucket configuration updates made
    %% by orchestrator
    chronicle_compat:pull(),
    {reply, ok, consider_doing_flush(State)};
do_handle_call({query_vbuckets, _, _, _} = Call, _From, State) ->
    {RV, NewState} =
        handle_query_vbuckets(Call, State),
    {reply, RV, NewState};
do_handle_call(get_incoming_replication_map, _From,
               #state{bucket_name = BucketName} = State) ->
    %% NOTE: has infinite timeouts but uses only local communication
    RV = replication_manager:get_incoming_replication_map(BucketName),
    {reply, RV, State};
do_handle_call({prepare_rebalance, _Pid}, _From,
               #state{last_applied_vbucket_states = undefined} = State) ->
    {reply, no_vbucket_states_set, State};
do_handle_call({prepare_rebalance, Pid} = Call, _From,
               State) ->
    State1 =
        State#state{
          rebalance_only_vbucket_states =
              [undefined || _ <- State#state.rebalance_only_vbucket_states]},
    {reply, ok, set_rebalance_mref(Call, Pid, State1)};

do_handle_call(finish_rebalance, _From, State) ->
    {reply, ok, State#state{rebalance_status = finished}};

do_handle_call({update_vbucket_state, VBucket, NormalState, RebalanceState,
                _ReplicateFrom, _Options} = Call, From, State) ->
    NewState = apply_new_vbucket_state(VBucket, NormalState, RebalanceState,
                                       State),
    delegate_apply_vbucket_state(Call, From, NewState);
do_handle_call({delete_vbucket, VBucket} = Call, From, State) ->
    NewState = apply_new_vbucket_state(VBucket, missing, undefined, State),
    delegate_apply_vbucket_state(Call, From, NewState);
do_handle_call({apply_new_config, NewBucketConfig}, _From, State) ->
    handle_apply_new_config(NewBucketConfig, State);
do_handle_call({apply_new_config_replicas_phase, NewBucketConfig},
               _From, State) ->
    handle_apply_new_config_replicas_phase(NewBucketConfig, State);
do_handle_call({wait_index_updated, VBucket}, From,
               #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From, ?cut(ns_couchdb_api:wait_index_updated(Bucket, VBucket)));
do_handle_call({wait_dcp_data_move, ReplicaNodes, VBucket}, From,
               #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From, ?cut(dcp_replicator:wait_for_data_move(ReplicaNodes, Bucket,
                                                          VBucket)));
do_handle_call({dcp_takeover, OldMasterNode, VBucket}, From,
               #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From, ?cut(replication_manager:dcp_takeover(Bucket, OldMasterNode,
                                                         VBucket)));
do_handle_call(initiate_indexing, From, #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From, ?cut(ok = ns_couchdb_api:initiate_indexing(Bucket)));
do_handle_call({wait_seqno_persisted, VBucket, SeqNo},
               From,
               #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From,
      fun () ->
              ?rebalance_debug(
                 "Going to wait for persistence of seqno ~B in vbucket ~B",
                 [SeqNo, VBucket]),
              Replicator =
                  dcp_replication_manager:get_replicator_pid(Bucket,
                                                             VBucket),
              erlang:link(Replicator),
              ok = do_wait_seqno_persisted(Bucket, VBucket, SeqNo),
              erlang:unlink(Replicator),
              ?rebalance_debug(
                 "Done waiting for persistence of seqno ~B in vbucket ~B",
                 [SeqNo, VBucket]),
              ok
      end);
do_handle_call({inhibit_view_compaction, Pid},
               From,
               #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From,
      ?cut(compaction_daemon:inhibit_view_compaction(Bucket, Pid)));
do_handle_call({uninhibit_view_compaction, Ref},
               From,
               #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From,
      ?cut(compaction_daemon:uninhibit_view_compaction(Bucket, Ref)));
do_handle_call({get_vbucket_high_seqno, VBucket},
               _From,
               #state{bucket_name = Bucket} = State) ->
    %% NOTE: this happens on current master of vbucket thus undefined
    %% persisted seq no should not be possible here
    {ok, SeqNo} = ns_memcached:get_vbucket_high_seqno(Bucket, VBucket),
    {reply, SeqNo, State};
do_handle_call({get_failover_logs, VBucketsR}, From, State) ->
    handle_call_via_servant(
      From, State, VBucketsR,
      fun (VBuckets, #state{bucket_name = Bucket}) ->
              case ns_memcached:get_failover_logs(Bucket, VBuckets) of
                  {ok, FailoverLogs} ->
                      {ok, lists:zip(VBuckets, FailoverLogs)};
                  Error ->
                      Error
              end
      end);
do_handle_call(mark_warmed, From, State) ->
    do_handle_call({mark_warmed, undefined}, From, State);
do_handle_call({mark_warmed, DataIngress}, _From,
               #state{bucket_name = Bucket} = State) ->
    RV = ns_memcached:mark_warmed(Bucket, DataIngress),
    ok = ns_bucket:activate_bucket_data_on_this_node(Bucket),
    {reply, RV, State};
do_handle_call({mount_volumes, VBuckets, Volumes}, From,
               #state{bucket_name = Bucket} = State) ->
    spawn_rebalance_subprocess(
      State, From,
      fun () ->
              RV =
                  [{VB,
                    ns_memcached:mount_fusion_vbucket(Bucket, VB, Volumes)} ||
                      VB <- VBuckets],
              Bad = lists:filter(
                      fun ({_, {ok, _}}) ->
                              false;
                          ({VB, Error}) ->
                              ?log_error("Mounting volumes ~p for vbucket ~p, "
                                         "bucket ~p failed with ~p",
                                         [Volumes, VB, Bucket, Error]),
                              true
                      end, RV),
              case Bad of
                  [] ->
                      ok;
                  _ ->
                      {error, mount_volumes_failed}
              end
      end);
do_handle_call({sync_fusion_log_store, VBuckets}, From, State) ->
    handle_call_via_servant(
      From, State, sync_fusion_log_store,
      fun (sync_fusion_log_store, #state{bucket_name = Bucket}) ->
              ns_memcached:sync_fusion_log_store(Bucket, VBuckets)
      end).

-dialyzer({no_opaque, [handle_call_via_servant/4]}).

handle_call_via_servant({FromPid, _Tag}, State, Req, Body) ->
    Tag = erlang:make_ref(),
    From = {FromPid, Tag},
    Pid = proc_lib:spawn(fun () ->
                                 gen_server:reply(From, Body(Req, State))
                         end),
    {reply, {Pid, Tag}, State}.

handle_cast({apply_vbucket_state_reply, ReplyPid, Call, Reply},
            #state{apply_vbucket_states_queue = Q,
                   apply_vbucket_states_worker = WorkerPid} = State) ->
    case ReplyPid =:= WorkerPid of
        true ->
            ?log_debug(
               "Got reply to call ~p from apply_vbucket_states_worker: ~p",
               [Call, Reply]),
            {{value, From}, NewQ} = queue:out(Q),
            gen_server:reply(From, Reply),
            {noreply, State#state{apply_vbucket_states_queue = NewQ}};
        false ->
            ?log_debug("Got reply from old "
                       "apply_vbucket_states_worker ~p (current worker ~p): ~p. "
                       "Dropping on the floor",
                       [ReplyPid, WorkerPid, Reply]),
            {noreply, State}
    end;
handle_cast({maybe_start_fusion_uploaders, Uploaders},
            #state{bucket_name = Bucket} = State) ->
    case ns_memcached:maybe_start_fusion_uploaders(Bucket, Uploaders) of
        ok ->
            ok;
        Error ->
            ?log_error("Error starting fusion uploaders: ~p", [Error])
    end,
    {noreply, State};
handle_cast({maybe_stop_fusion_uploaders, VBuckets},
            #state{bucket_name = Bucket} = State) ->
    case ns_memcached:maybe_stop_fusion_uploaders(Bucket, VBuckets) of
        ok ->
            ok;
        Error ->
            ?log_error("Error stopping fusion uploaders: ~p", [Error])
    end,
    {noreply, State};

handle_cast(_, _State) ->
    erlang:error(cannot_do).

handle_info({'DOWN', MRef, _, Pid, Reason} = Call,
            #state{rebalance_mref = RMRef,
                   last_applied_vbucket_states = WantedVBuckets} = State)
  when MRef =:= RMRef ->
    ?log_info("Rebalancer ~p died with reason ~p. Undoing temporary vbucket "
              "states caused by rebalance", [Pid, Reason]),
    State2 = State#state{rebalance_only_vbucket_states =
                             [undefined || _ <- WantedVBuckets]},
    State3 = cleanup_rebalance_artifacts(Call, State2),
    {noreply, pass_vbucket_states_to_set_view_manager(State3)};
handle_info({subprocess_done, Pid, RV},
            #state{rebalance_subprocesses = Subprocesses} = State) ->
    ?log_debug("Got done message from subprocess: ~p (~p)", [Pid, RV]),
    case lists:keyfind(Pid, 2, Subprocesses) of
        false ->
            {noreply, State};
        {From, _} = Pair ->
            gen_server:reply(From, RV),
            {noreply,
             State#state{rebalance_subprocesses = Subprocesses -- [Pair]}}
    end;
handle_info(Info, State) ->
    ?log_debug("Ignoring unexpected message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

server_name(Bucket) ->
    list_to_atom("janitor_agent-" ++ Bucket).

pass_vbucket_states_to_set_view_manager(
  #state{bucket_name = BucketName,
         last_applied_vbucket_states = WantedVBuckets,
         rebalance_only_vbucket_states = RebalanceVBuckets} = State) ->
    ok = ns_couchdb_api:set_vbucket_states(BucketName,
                                           WantedVBuckets,
                                           RebalanceVBuckets),
    State.

set_rebalance_mref(Call, Pid, State0) ->
    case State0#state.rebalance_pid of
        Pid ->
            ok;
        _ ->
            ?log_debug("Changing rebalance pid from ~p to ~p for ~p",
                       [State0#state.rebalance_pid, Pid, Call])
    end,
    [begin
         ?log_debug("Killing rebalance-related subprocess: ~p", [P]),
         misc:unlink_terminate_and_wait(P, shutdown),
         gen_server:reply(From, rebalance_aborted)
     end || {From, P} <- State0#state.rebalance_subprocesses],

    case State0#state.apply_vbucket_states_worker of
        undefined ->
            ok;
        P ->
            ?log_debug("Killing apply_vbucket_states_worker: ~p", [P]),
            misc:unlink_terminate_and_wait(P, shutdown),
            [gen_server:reply(From, rebalance_aborted) ||
                From <- queue:to_list(State0#state.apply_vbucket_states_queue)]
    end,

    case State0#state.rebalance_mref of
        undefined ->
            ok;
        OldMRef ->
            case State0#state.rebalance_status =:= in_process of
                true ->
                    %% something went wrong. nuke replicator just in case
                    dcp_sup:nuke(State0#state.bucket_name);
                false ->
                    ok
            end,
            erlang:demonitor(OldMRef, [flush])
    end,

    State = State0#state{rebalance_pid = Pid,
                         rebalance_subprocesses = [],
                         apply_vbucket_states_queue = queue:new(),
                         apply_vbucket_states_worker = undefined},
    case Pid of
        undefined ->
            State#state{rebalance_mref = undefined,
                        rebalance_status = finished};
        _ ->
            WorkerPid = proc_lib:spawn_link(
                          fun () ->
                                  ns_process_registry:register_pid(
                                    State#state.rebalance_subprocesses_registry,
                                    erlang:make_ref(), self()),
                                  apply_vbucket_states_worker_loop()
                          end),

            State#state{rebalance_mref = erlang:monitor(process, Pid),
                        rebalance_status = in_process,
                        apply_vbucket_states_worker = WorkerPid}
    end.

cleanup_rebalance_artifacts(Call, State) ->
    set_rebalance_mref(Call, undefined, State).

spawn_rebalance_subprocess(
  #state{rebalance_subprocesses = Subprocesses,
         rebalance_subprocesses_registry = RegistryPid} = State, From, Fun) ->
    Parent = self(),
    Pid = proc_lib:spawn_link(
            fun () ->
                    ns_process_registry:register_pid(RegistryPid,
                                                     erlang:make_ref(), self()),
                    RV = Fun(),
                    Parent ! {subprocess_done, self(), RV}
            end),
    {noreply,
     State#state{rebalance_subprocesses = [{From, Pid} | Subprocesses]}}.

flushseq_file_path(BucketName) ->
    {ok, DBSubDir} = ns_storage_conf:this_node_bucket_dbdir(BucketName),
    filename:join(DBSubDir, "flushseq").

read_flush_counter(BucketName) ->
    FlushSeqFile = flushseq_file_path(BucketName),
    case file:read_file(FlushSeqFile) of
        {ok, Contents} ->
            try list_to_integer(binary_to_list(Contents)) of
                FlushSeq ->
                    ?log_info("Got flushseq from local file: ~p", [FlushSeq]),
                    FlushSeq
            catch T:E:S ->
                    ?log_error("Parsing flushseq failed: ~p",
                               [{T, E, S}]),
                    read_flush_counter_from_config(BucketName)
            end;
        Error ->
            ?log_info("Loading flushseq failed: ~p. "
                      "Assuming it's equal to global config.", [Error]),
            read_flush_counter_from_config(BucketName)
    end.

read_flush_counter_from_config(BucketName) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    RV = proplists:get_value(flushseq, BucketConfig, 0),
    ?log_info("Initialized flushseq ~p from bucket config", [RV]),
    RV.

consider_doing_flush(State) ->
    BucketName = State#state.bucket_name,
    case ns_bucket:get_bucket(BucketName) of
        {ok, BucketConfig} ->
            ConfigFlushSeq = proplists:get_value(flushseq, BucketConfig, 0),
            MyFlushSeq = State#state.flushseq,
            case ConfigFlushSeq > MyFlushSeq of
                true ->
                    ?log_info("Config flushseq ~p is greater than local "
                              "flushseq ~p. Going to flush",
                              [ConfigFlushSeq, MyFlushSeq]),
                    perform_flush(State, BucketConfig, ConfigFlushSeq);
                false ->
                    case ConfigFlushSeq =/= MyFlushSeq of
                        true ->
                            ?log_error("That's weird. Config flushseq is lower "
                                       "than ours: ~p vs. ~p. Ignoring",
                                       [ConfigFlushSeq, MyFlushSeq]),
                            State#state{flushseq = ConfigFlushSeq};
                        _ ->
                            State
                    end
            end;
        not_present ->
            ?log_info("Detected that our bucket is actually dead"),
            State
    end.

perform_flush(#state{bucket_name = BucketName} = State, BucketConfig,
              ConfigFlushSeq) ->
    ?log_info("Doing local bucket flush"),
    {ok, VBStates} = ns_memcached:local_connected_and_list_vbuckets(BucketName),
    NewVBStates =
        lists:duplicate(
          proplists:get_value(num_vbuckets, BucketConfig), missing),
    RebalanceVBStates =
        lists:duplicate(
          proplists:get_value(num_vbuckets, BucketConfig), undefined),
    NewState = State#state{last_applied_vbucket_states = NewVBStates,
                           rebalance_only_vbucket_states = RebalanceVBStates,
                           flushseq = ConfigFlushSeq},
    ?log_info("Removing all vbuckets from indexes"),
    pass_vbucket_states_to_set_view_manager(NewState),
    ok = ns_couchdb_api:reset_master_vbucket(BucketName),
    ?log_info("Shutting down incoming replications"),
    ok = stop_all_replications(BucketName),
    %% kill all vbuckets
    [ok = ns_memcached:sync_delete_vbucket(BucketName, VB)
     || {VB, _} <- VBStates],
    ?log_info("Local flush is done"),
    save_flushseq(BucketName, ConfigFlushSeq),
    NewState.

save_flushseq(BucketName, ConfigFlushSeq) ->
    ?log_info("Saving new flushseq: ~p", [ConfigFlushSeq]),
    Cont = list_to_binary(integer_to_list(ConfigFlushSeq)),
    misc:atomic_write_file(flushseq_file_path(BucketName), Cont).

do_wait_seqno_persisted(Bucket, VBucket, SeqNo) ->
    case ns_memcached:wait_for_seqno_persistence(Bucket, VBucket, SeqNo) of
        ok ->
            ok;
        {memcached_error, etmpfail, _} ->
            ?rebalance_debug("Got etmpfail while waiting for sequence number "
                             "~p to persist for vBucket:~p. Will retry.",
                             [SeqNo, VBucket]),
            do_wait_seqno_persisted(Bucket, VBucket, SeqNo)
    end.

apply_new_vbucket_state(VBucket, NormalState, RebalanceState, State) ->
    #state{last_applied_vbucket_states = WantedVBuckets,
           rebalance_only_vbucket_states = RebalanceVBuckets} = State,

    NewWantedVBuckets =
        misc:nthreplace(VBucket + 1, NormalState, WantedVBuckets),
    NewRebalanceVBuckets =
        misc:nthreplace(VBucket + 1, RebalanceState, RebalanceVBuckets),
    State#state{last_applied_vbucket_states = NewWantedVBuckets,
                rebalance_only_vbucket_states = NewRebalanceVBuckets}.

delegate_apply_vbucket_state(
  Call, From,
  #state{apply_vbucket_states_queue = Q,
         apply_vbucket_states_worker = Pid} = State) ->
    Pid ! {self(), Call, State},
    NewState = State#state{apply_vbucket_states_queue = queue:in(From, Q)},
    {noreply, NewState}.

apply_vbucket_states_worker_loop() ->
    receive
        {Parent, Call, State} ->
            Reply = handle_apply_vbucket_state(Call, State),
            gen_server:cast(Parent, {apply_vbucket_state_reply, self(), Call,
                                     Reply}),
            apply_vbucket_states_worker_loop()
    end.

handle_apply_vbucket_state({update_vbucket_state, VBucket, NormalState,
                            _RebalanceState, ReplicateFrom, OptionsOrTopology},
                           #state{bucket_name = BucketName} = AgentState) ->
    Options =
        case OptionsOrTopology of
            [] ->
                OptionsOrTopology;
            [El | _] when is_tuple(El) ->
                OptionsOrTopology;
            undefined ->
                %% pre-Morpheus cluster
                [];
            _ ->
                %% pre-Morpheus cluster
                [{topology, OptionsOrTopology}]
        end,
    %% TODO: consider infinite timeout. It's local memcached after all
    ok = ns_memcached:set_vbucket(BucketName, VBucket, NormalState, Options),
    ok = replication_manager:change_vbucket_replication(BucketName,
                                                        VBucket, ReplicateFrom),
    pass_vbucket_states_to_set_view_manager(AgentState),
    ok;
handle_apply_vbucket_state({delete_vbucket, VBucket},
                           #state{bucket_name = BucketName} = AgentState) ->
    pass_vbucket_states_to_set_view_manager(AgentState),
    ok = ns_memcached:delete_vbucket(BucketName, VBucket).

decode_topology("null") ->
    undefined;
decode_topology(Topology) ->
    [lists:map(fun (null) ->
                       undefined;
                   (Node) ->
                       %% In general, we expect the topology node to be
                       %% an existing atom - but in the new serverless world,
                       %% a bucket could be paused and resumed on an entirely
                       %% different cluster and therefore accept a reference to
                       %% a node from the old cluster.
                       binary_to_atom(Node, latin1)
               end, Chain) || Chain <- ejson:decode(Topology)].

is_topology_same(active, Chain, MemcachedTopology) ->
    [Chain] =:= MemcachedTopology;
is_topology_same(_, _, _) ->
    true.

decode_value(state, V) ->
    erlang:list_to_existing_atom(V);
decode_value(high_seqno, V) ->
    list_to_integer(V);
decode_value(high_prepared_seqno, V) ->
    list_to_integer(V);
decode_value(topology, V) ->
    decode_topology(V);
decode_value(_, V) ->
    V.

get_state_and_topology(BucketName) ->
    Keys = ["state", "topology"],
    case ns_memcached:get_vbucket_details_stats(BucketName, Keys) of
        {ok, VBDetails} ->
            {ok, decode_vbucket_details(VBDetails)};
        Error ->
            Error
    end.

decode_vbucket_details(VBDetails) ->
    dict:map(
      fun (_VB, List) ->
              lists:map(
                fun ({Key, Value}) ->
                        KeyAtom = list_to_existing_atom(Key),
                        {KeyAtom, decode_value(KeyAtom, Value)}
                end, List)
      end, VBDetails).

handle_query_vbuckets(Call, #state{bucket_name = BucketName,
                                   rebalance_status = finished} = State) ->
    %% NOTE: uses 'outer' memcached timeout of 60 seconds
    {Fun, Options} =
        case Call of
            {query_vbuckets, VBs, Keys, Opts} ->
                {?cut((catch perform_query_vbuckets(Keys, VBs, BucketName))),
                 Opts}
        end,
    NewState = consider_doing_flush(State),
    case proplists:get_value(failover_nodes, Options, []) of
        [] ->
            ok;
        FailoverNodes ->
            ?log_info("Stopping replications for nodes ~p", [FailoverNodes]),
            ok = replication_manager:stop_nodes(BucketName, FailoverNodes)
    end,
    {Fun(), NewState}.

filter_vbucket_dict(all, Dict) ->
    Dict;
filter_vbucket_dict(VBs, Dict) ->
    lists:foldl(
      fun (VB, D) ->
              case dict:find(VB, Dict) of
                  {ok, Value} ->
                      dict:store(VB, Value, D);
                  error ->
                      D
              end
      end, dict:new(), VBs).

perform_query_vbuckets(Keys, VBs, BucketName) ->
    KeyStrings = ["state" | [atom_to_list(K) || K <- Keys, is_atom(K)]],
    case ns_memcached:local_connected_and_list_vbucket_details(
           BucketName, KeyStrings) of
        {ok, Dict} ->
            Filtered = filter_vbucket_dict(VBs, Dict),
            Decoded = decode_vbucket_details(Filtered),
            {ok,
             [{VB,
               proplists:get_value(state, Details),
               lists:keydelete(state, 1, Details)}
              || {VB, Details} <- dict:to_list(Decoded)]};
        Err ->
            Err
    end.

stop_all_replications(Bucket) ->
    replication_manager:set_incoming_replication_map(Bucket, []).

handle_apply_new_config(NewBucketConfig, State) ->
    check_for_node_rename(apply_new_config,
                          NewBucketConfig, State,
                          fun handle_apply_new_config/3).

handle_apply_new_config(Node, NewBucketConfig,
                        #state{bucket_name = BucketName} = State) ->
    {ok, VBDetails} = get_state_and_topology(BucketName),
    Map = proplists:get_value(map, NewBucketConfig),
    true = (Map =/= undefined),
    {_, ToSet, ToDelete, NewWantedRev}
        = lists:foldl(
            fun (Chain, {VBucket, ToSet, ToDelete, PrevWanted}) ->
                    WantedState =
                        case [Pos || {Pos, N} <- misc:enumerate(Chain, 0),
                                     N =:= Node] of
                            [0] ->
                                active;
                            [_] ->
                                replica;
                            [] ->
                                missing
                        end,
                    {ActualState, ActualTopology} =
                        case dict:find(VBucket, VBDetails) of
                            {ok, Val} ->
                                StateVal = proplists:get_value(state, Val),
                                %% Always expect "state" to be present.
                                false = StateVal =:= undefined,
                                {StateVal, proplists:get_value(topology, Val)};
                            _ ->
                                {missing, undefined}
                        end,
                    NewWanted = [WantedState | PrevWanted],
                    case WantedState =:= ActualState andalso
                         is_topology_same(WantedState, Chain, ActualTopology) of
                        true ->
                            {VBucket + 1, ToSet, ToDelete, NewWanted};
                        false ->
                            case WantedState of
                                missing ->
                                    {VBucket + 1, ToSet, [VBucket | ToDelete],
                                     NewWanted};
                                active ->
                                    {VBucket + 1,
                                     [{VBucket, WantedState,
                                       [{topology, [Chain]}]} | ToSet],
                                     ToDelete, NewWanted};
                                _ ->
                                    {VBucket + 1,
                                     [{VBucket, WantedState, []} | ToSet],
                                     ToDelete, NewWanted}
                            end
                    end
            end, {0, [], [], []}, Map),

    NewWanted = lists:reverse(NewWantedRev),
    NewRebalance = [undefined || _ <- NewWantedRev],
    State2 = State#state{last_applied_vbucket_states = NewWanted,
                         rebalance_only_vbucket_states = NewRebalance},

    %% We are going to tell the replication manager to update the connection
    %% count. This is a no-op change if it is the same, and we don't expect it
    %% to change very often. We are doing this in the janitor because we want to
    %% make sure that the number of connections is the expected amount during a
    %% rebalance, and consistent between all nodes. Doing this in the janitor
    %% means that we are doing this with the orchestrator, a single node as up
    %% to date as possible, and we are using the orchestrators view of the
    %% BucketConfig ensuring that we set this to the same value on all nodes.
    %% If we had different numbers of connections on different nodes then we
    %% would see rebalance failures as we would fail to find connections or
    %% streams belonging to connections where we expect.
    DesiredConnections = ns_bucket:get_num_dcp_connections(NewBucketConfig),
    replication_manager:update_replication_count(BucketName,
                                                 DesiredConnections),

    %% before changing vbucket states (i.e. activating or killing
    %% vbuckets) we must stop replications into those vbuckets
    WantedReplicas = [{Src, VBucket}
                      || {Src, Dst, VBucket} <- ns_bucket:map_to_replicas(Map),
                         Dst =:= Node],
    WantedReplications =
        [{Src, [VB || {_, VB} <- Pairs]}
         || {Src, Pairs} <- misc:keygroup(1, lists:sort(WantedReplicas))],
    ok = replication_manager:remove_undesired_replications(
           BucketName, WantedReplications),

    %% then we're ok to change vbucket states
    ok = ns_memcached:set_vbuckets(BucketName, ToSet),

    %% and ok to delete vbuckets we want to delete
    ok = ns_memcached:delete_vbuckets(BucketName, ToDelete),

    {reply, ok, pass_vbucket_states_to_set_view_manager(State2)}.

handle_apply_new_config_replicas_phase(NewBucketConfig, State) ->
    check_for_node_rename(apply_new_config_replicas_phase,
                          NewBucketConfig, State,
                          fun handle_apply_new_config_replicas_phase/3).

handle_apply_new_config_replicas_phase(Node, NewBucketConfig,
                                       #state{bucket_name =
                                                  BucketName} = State) ->
    Map = proplists:get_value(map, NewBucketConfig),
    true = (Map =/= undefined),
    WantedReplicas = [{Src, VBucket}
                      || {Src, Dst, VBucket} <- ns_bucket:map_to_replicas(Map),
                         Dst =:= Node],
    WantedReplications =
        [{Src, [VB || {_, VB} <- Pairs]}
         || {Src, Pairs} <- misc:keygroup(1, lists:sort(WantedReplicas))],
    ok = replication_manager:set_incoming_replication_map(
           BucketName, WantedReplications),
    {reply, ok, State}.

%% It's possible that we got renamed in between when ns_janitor grabbed the
%% bucket config and when the call made it to janitor_agent. Properly
%% preventing this from happening proved hard: we'd want to stop almost all
%% processes, but that has nasty side-effects visible to end user. So instead
%% we're detecting the node rename by looking into the incoming bucket
%% config. If we don't find our node in the server list, that must be due to a
%% rename. So we'll refuse to handle such a call.
%%
%% The rename might also happen after our check succeeded. But the handler is
%% given a node name that is consistent with the bucket config. So as long as
%% the handlers use the passed node name, everything should be fine.
%%
%% See https://issues.couchbase.com/browse/MB-34598 for more details.
check_for_node_rename(Call, BucketConfig, State, Body) ->
    Node = node(),
    Servers = ns_bucket:get_servers(BucketConfig),
    case lists:member(Node, Servers) of
        true ->
            RV = Body(Node, BucketConfig, State),

            NewNode = node(),
            case NewNode =:= Node of
                true ->
                    ok;
                false ->
                    ?log_info("Node name changed while handling ~p.~n"
                              "Old name: ~p.~n"
                              "New name: ~p.~n"
                              "Bucket config:~n~p~n"
                              "Result:~n~p",
                              [Call, Node, NewNode, BucketConfig, RV])
            end,

            RV;
        false ->
            ?log_info("Detected node rename when handling ~p. "
                      "Our name: ~p. Bucket server list: ~p",
                      [Call, Node, Servers]),
            {reply, {node_rename_detected, Node, Servers}, State}
    end.

-ifdef(TEST).

%% This test tests that we can interrupt a call to wait_for_memcached
%% (performed by the janitor) as we must be able to interrupt the janitor to
%% perform more important actions in the orchestrator such as failover. In
%% the case in which memcached is unresponsive, it was observed that we could
%% not interrupt this function call, and had to wait for the configured
%% timeout which delayed a failover of the node on which memcached was
%% unresponsive. This test should hit the eunit timeout (or just get stuck if
%% one does not exist) if we cannot interrupt this function.
wait_for_memcached_interruptible_t() ->
    VeryBigTimeout = 1000000,

    meck:expect(ns_config, get_timeout,
                fun(_,_) ->
                        VeryBigTimeout
                end),

    %% We want to hang when we make the call to memcached, then have the test
    %% attempt to "cancel" the janitor run by killing the process that
    %% started the check_bucket_ready(...) call. Set up a meck to notify the
    %% test process when it should continue (using send and receive as a
    %% baton), before we hang here with a very long sleep to emulate
    %% memcached being slow.
    TestProc = self(),
    meck:expect(ns_memcached,
                local_connected_and_list_vbucket_details,
                fun(_,_) ->
                        TestProc ! ready_to_terminate,
                        timer:sleep(VeryBigTimeout)
                end),

    %% We need to kill the process emulating the janitor for this test so run
    %% the check_bucket_ready() in a new process.
    JanitorPid = erlang:spawn_link(
                   fun() ->
                           check_bucket_ready(wait_for_memcached_test_bucket(),
                                              [node()], VeryBigTimeout)
                   end),

    receive ready_to_terminate ->
            %% If we can't interrupt the process then we should get stuck here.
            misc:unlink_terminate_and_wait(JanitorPid, shutdown)
    after 1000 ->
            erlang:exit(timeout)
    end.

wait_for_memcached_success_t() ->
    meck:expect(ns_memcached,
                local_connected_and_list_vbucket_details,
                fun(_, _) ->
                        %% Enumerating the types that vbucket state supports
                        %% here to test each one
                        {ok, dict:from_list([{0, [{"state", "active"}]},
                                             {1, [{"state", "dead"}]},
                                             {2, [{"state", "replica"}]},
                                             {3, [{"state", "pending"}]}])}
                end),

    ?assertEqual(ready, check_bucket_ready(wait_for_memcached_test_bucket(),
                                           [node()], 1000000)).

wait_for_memcached_timeout_t() ->
    BigTimeout = 1000000,
    SmallTimeout = 1,
    meck:expect(ns_memcached,
                local_connected_and_list_vbucket_details,
                fun(_,_) ->
                        %% Big sleep, testing that wait_for_memcached() can
                        %% timeout  correctly so we want to get it stuck.
                        timer:sleep(BigTimeout)
                end),

    ?assertEqual(
       {failed, [node()]},
       check_bucket_ready(wait_for_memcached_test_bucket(),
                          [node()], SmallTimeout)).

wait_for_memcached_warmup_t() ->
    BigTimeout = 1000000,
    %% This needs to give us a little bit of time to get the first "response"
    %% from memcached, but not too much that the test takes ages. 500ms is
    %% probably fine, but it can be bumped if necessary.
    SmallTimeout = 500,

    meck:expect(ns_memcached,
                local_connected_and_list_vbucket_details,
                2,
                %% Setup a sequence of expectations, the first returns
                %% warming_up, the second sleeps for a long time. The sleep
                %% must be wrapped in a fun or it will be evaluated
                %% immediately, rather than when this function is called, and
                %% the test will time out.
                meck:seq([warming_up,
                          fun (_, _) -> timer:sleep(BigTimeout) end])),

    ?assertEqual(
       {warming_up, [node()]},
       check_bucket_ready(wait_for_memcached_test_bucket(),
                          [node()], SmallTimeout)).

wait_for_memcached_exception_t() ->
    %% (Most) exceptions should not retry, just return, hence a big timeout
    BigTimeout = 1000000,

    meck:expect(ns_memcached,
                local_connected_and_list_vbucket_details,
                fun(_, _) ->
                        erlang:throw(exception)
                end),

    ?assertEqual({failed, [node()]},
                 check_bucket_ready(wait_for_memcached_test_bucket(), [node()],
                                    BigTimeout)).

wait_for_memcached_noproc_exception_t() ->
    %% noproc exceptions should retry, we will hit this timeout so it should be
    %% small
    SmallTimeout = 1000,

    meck:expect(ns_memcached,
                local_connected_and_list_vbucket_details,
                fun(_, _) ->
                        erlang:exit({noproc, a})
                end),

    ?assertEqual({failed, [node()]},
                 check_bucket_ready(wait_for_memcached_test_bucket(), [node()],
                                    SmallTimeout)),

    ?assert(2 =< meck:num_calls(ns_memcached,
                                local_connected_and_list_vbucket_details, '_')).

wait_for_memcached_test_bucket() ->
    "default".

wait_for_memcached_test_setup() ->
    %% Setup required for the janitor_agent gen_server.
    %% Whilst we could just call the wait_for_memcached function manually, it
    %% dispatches a gen_server:call() to janitor_agent-<bucket> which throws
    %% an exception and won't let us easily test what happens when we get
    %% stuck talking to memcached. As such, we will startup the gen_server
    %% properly to process that call and hang it in ns_memcached at the
    %% appropriate time.
    meck:new(janitor_agent_sup),
    meck:expect(janitor_agent_sup, get_registry_pid,
                fun(_) ->
                        self()
                end),

    meck:new(ns_bucket, [passthrough]),
    meck:expect(ns_bucket, get_bucket,
                fun(_) ->
                        {ok, [{type, memcached}]}
                end),

    meck:new(ns_storage_conf),
    meck:expect(ns_storage_conf, this_node_bucket_dbdir,
                fun(_) ->
                        {ok, "/"}
                end),

    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, get_timeout,
                fun(_, Default) ->
                       Default
                end),

    meck:expect(ns_config, search_node_with_default,
                fun({?MODULE, query_vbuckets_sleep}, _Default) ->
                        %% Smaller than default value to speed up tests
                        100
                end),

    %% Usage of this is test specific, so the expect does not accompany the new
    %% meck.
    meck:new(ns_memcached),

    {ok, ServerPid} = start_link(wait_for_memcached_test_bucket()),
    ServerPid.

wait_for_memcached_test_teardown(ServerPid) ->
    %% We need to terminate abnormally (i.e. not with reason normal) as some
    %% tests may leave the server in an unresponsive state.
    misc:unlink_terminate_and_wait(ServerPid, shutdown),

    meck:unload(ns_memcached),
    meck:unload(ns_config),
    meck:unload(ns_storage_conf),
    meck:unload(ns_bucket),
    meck:unload(janitor_agent_sup).

wait_for_memcached_test_() ->
    Tests = [fun wait_for_memcached_interruptible_t/0,
             fun wait_for_memcached_success_t/0,
             fun wait_for_memcached_timeout_t/0,
             fun wait_for_memcached_warmup_t/0,
             fun wait_for_memcached_exception_t/0,
             fun wait_for_memcached_noproc_exception_t/0],

    {foreach,
     fun wait_for_memcached_test_setup/0,
     fun wait_for_memcached_test_teardown/1,
     Tests}.

-endif.
