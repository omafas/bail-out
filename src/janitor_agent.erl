%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2018 Couchbase, Inc.
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
-module(janitor_agent).

-behavior(gen_server).

-include("cut.hrl").
-include("ns_common.hrl").

-define(WAIT_FOR_MEMCACHED_TIMEOUT, ?get_timeout(wait_for_memcached, 5000)).
-define(APPLY_NEW_CONFIG_TIMEOUT,   ?get_timeout(apply_config, 60000)).
%% NOTE: there's also ns_memcached timeout anyways
-define(DELETE_VBUCKET_TIMEOUT,     ?get_timeout(delete_vbucket, 120000)).
-define(PREPARE_REBALANCE_TIMEOUT,  ?get_timeout(prepare_rebalance, 30000)).
-define(PREPARE_FLUSH_TIMEOUT,      ?get_timeout(prepare_flush, 30000)).
-define(SET_VBUCKET_STATE_TIMEOUT,  ?get_timeout(set_vbucket_state, infinity)).
-define(WARMED_TIMEOUT,             ?get_timeout(warmed, 6000)).
-define(GET_SRC_DST_REPLICATIONS_TIMEOUT,
        ?get_timeout(get_src_dst_replications, 30000)).

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
         delete_vbucket_copies/4,
         prepare_nodes_for_rebalance/3,
         finish_rebalance/3,
         this_node_replicator_triples/1,
         bulk_set_vbucket_state/4,
         set_vbucket_state/7,
         set_vbucket_state/8,
         get_src_dst_vbucket_replications/2,
         get_src_dst_vbucket_replications/3,
         initiate_indexing/5,
         wait_index_updated/5,
         mass_prepare_flush/2,
         complete_flush/3,
         get_dcp_docs_estimate/4,
         get_mass_dcp_docs_estimate/3,
         wait_dcp_data_move/5,
         wait_seqno_persisted/5,
         get_vbucket_high_seqno/4,
         dcp_takeover/5,
         inhibit_view_compaction/3,
         uninhibit_view_compaction/4,
         get_failover_logs/2]).

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

query_vbuckets_loop(Node, Bucket, Call, Parent) ->
    query_vbuckets_loop(Node, Bucket, Call, Parent, undefined).
query_vbuckets_loop(Node, Bucket, Call, Parent, Warming) ->
    case (catch call(Bucket, Node, Call, infinity)) of
        {ok, _} = Msg ->
            Msg;
        warming_up ->
            query_vbuckets_loop_next_step(Node, Bucket, Call, Parent,
                                          Warming, warming_up);
        {'EXIT', {noproc, _}} = Exc ->
            ?log_debug("Exception from ~p of ~p:~p~n~p",
                       [Call, Bucket, Node, Exc]),
            query_vbuckets_loop_next_step(Node, Bucket, Call, Parent,
                                          Warming, noproc);
        Exc ->
            ?log_debug("Exception from ~p of ~p:~p~n~p",
                       [Call, Bucket, Node, Exc]),
            Exc
    end.

query_vbuckets_loop_next_step(Node, Bucket, Call, Parent, Warming, Reason) ->
    ?log_debug("Waiting for ~p on ~p", [Bucket, Node]),
    NewWarming = maybe_send_warming(Parent, Warming, Reason),
    timer:sleep(1000),
    query_vbuckets_loop(Node, Bucket, Call, Parent, NewWarming).

maybe_send_warming(Parent, undefined, warming_up) ->
    Parent ! warming_up;
maybe_send_warming(_Parent, Warming, _Reason) ->
    Warming.

-type query_vbuckets_call() :: query_vbucket_states |
                               {query_vbuckets, [vbucket_id()], list(), list()}.
-spec wait_for_memcached([{node(), query_vbuckets_call()}], bucket_name(),
                         non_neg_integer()) -> [{node(),
                                                 {ok, list()} |
                                                 {ok, dict:dict()} |
                                                 warming_up | timeout | any()}].
wait_for_memcached(NodeCalls, Bucket, WaitTimeout) ->
    Parent = self(),
    misc:executing_on_new_process(
      fun () ->
              erlang:process_flag(trap_exit, true),
              Ref = make_ref(),
              Me = self(),
              NodePids =
                  [{Node, proc_lib:spawn_link(
                            fun () ->
                                    {ok, TRef} = timer2:kill_after(WaitTimeout),
                                    RV = query_vbuckets_loop(Node, Bucket,
                                                             Call, Me),
                                    Me ! {'EXIT', self(), {Ref, RV}},
                                    %% doing cancel is quite
                                    %% important. kill_after is
                                    %% not automagically
                                    %% canceled
                                    timer2:cancel(TRef),
                                    %% Nodes list can be reasonably
                                    %% big. Let's not slow down
                                    %% receive loop below due to
                                    %% extra garbage. It's O(N²)
                                    %% already
                                    erlang:unlink(Me)
                            end)}
                   || {Node, Call} <- NodeCalls],
              [recv_result(Bucket, Parent, Ref, NodePid) || NodePid <- NodePids]
      end).

recv_result(Bucket, Parent, Ref, {Node, Pid}) ->
    receive
        {'EXIT', Parent, Reason} ->
            ?log_debug("Parent died ~p", [Reason]),
            exit(Reason);
        {'EXIT', Pid, {Ref, RV}} ->
            {Node, RV};
        {'EXIT', Pid, killed} ->
            receive
                warming_up ->
                    {Node, warming_up}
            after 0 ->
                    {Node, timeout}
            end;
        {'EXIT', Pid, Reason} = ExitMsg ->
            ?log_info("Got exception trying to query vbuckets of ~p "
                      "bucket ~p~n~p", [Node, Bucket, Reason]),
            {Node, ExitMsg}
    end.

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
    {dict:from_list(misc:groupby_map(fun functools:id/1, ConvertedResults)),
     failures_to_zombies(Failures)}.

%% TODO: consider supporting partial janitoring
do_query_vbuckets(Bucket, Nodes, ExtraKeys, Options) ->
    Timeout = proplists:get_value(timeout, Options,
                                  ?WAIT_FOR_MEMCACHED_TIMEOUT),

    CreateCall = case cluster_compat_mode:is_cluster_65() of
                     false ->
                         fun (_) -> query_vbucket_states end;
                     true ->
                         {query_vbuckets, _, ExtraKeys, Options}
                 end,

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
    case cluster_compat_mode:is_cluster_65() of
        true -> mark_bucket_warmed_65(Bucket, Nodes);
        false -> mark_bucket_warmed_pre_65(Bucket, Nodes)
    end.

mark_bucket_warmed_65(Bucket, Nodes) ->
    process_multicall_rv(
      multi_call(Bucket, Nodes, mark_warmed, ?WARMED_TIMEOUT)).

mark_bucket_warmed_pre_65(Bucket, Nodes) ->
    process_multicall_rv(ns_memcached:mark_warmed(Nodes, Bucket)).

apply_new_bucket_config(Bucket, Servers, NewBucketConfig, undefined_timeout) ->
    apply_new_bucket_config(Bucket, Servers, NewBucketConfig,
                            ?APPLY_NEW_CONFIG_TIMEOUT);
apply_new_bucket_config(Bucket, Servers, NewBucketConfig, Timeout) ->
    functools:sequence_(
      [?cut(call_on_servers(Bucket, Servers, NewBucketConfig,
                            apply_new_config, Timeout)),
       ?cut(call_on_servers(Bucket, Servers, NewBucketConfig,
                            apply_new_config_replicas_phase, Timeout))]).

format_apply_new_config_call(Call, BucketConfig) ->
    case cluster_compat_mode:is_cluster_65() of
        true -> {Call, BucketConfig};
        false -> {Call, BucketConfig, []}
    end.

call_on_servers(Bucket, Servers, BucketConfig, Call, Timeout) ->
    CompleteCall = format_apply_new_config_call(Call, BucketConfig),
    Replies = misc:parallel_map(
                ?cut({_1, catch call(Bucket, _1, CompleteCall, Timeout)}),
                Servers, infinity),
    BadReplies = [R || {_, RV} = R <- Replies, RV =/= ok],
    case BadReplies of
        [] ->
            ok;
        _ ->
            ?log_info("~s:Some janitor state change requests (~p) have failed"
                      ":~n~p", [Bucket, Call, BadReplies]),
            {error, {failed_nodes, [N || {N, _} <- BadReplies]}}
    end.

process_multicall_rv({Replies, BadNodes}) ->
    BadReplies = [R || {_, RV} = R <- Replies, RV =/= ok],
    process_multicall_rv(BadReplies, BadNodes).

process_multicall_rv([], []) ->
    ok;
process_multicall_rv(BadReplies, BadNodes) ->
    {errors, [{N, bad_node} || N <- BadNodes] ++ BadReplies}.

-spec delete_vbucket_copies(bucket_name(), pid(), [node()], vbucket_id()) ->
                                   ok | {errors, [{node(), term()}]}.
delete_vbucket_copies(Bucket, RebalancerPid, Nodes, VBucket) ->
    process_multicall_rv(
      rebalance_multi_call(RebalancerPid, Bucket, Nodes,
                           {delete_vbucket, VBucket}, ?DELETE_VBUCKET_TIMEOUT)).

-spec prepare_nodes_for_rebalance(bucket_name(), [node()], pid()) ->
                                         {ok, [{node(), [integer()]}]} |
                                         {errors, [{node(), term()}]}.
prepare_nodes_for_rebalance(Bucket, Nodes, RebalancerPid) ->
    {Replies, BadNodes} =
        multi_call(Bucket, Nodes, {prepare_rebalance, RebalancerPid},
                   ?PREPARE_REBALANCE_TIMEOUT),
    {BadReplies, Versions} =
        lists:foldl(fun ({_, ok}, {BRAcc, VAcc}) ->
                            {BRAcc, VAcc};
                        ({Node, {ok, [{version, Version}]}}, {BRAcc, VAcc}) ->
                            {BRAcc, [{Node, Version} | VAcc]};
                        (R, {BRAcc, VAcc}) ->
                            {[R | BRAcc], VAcc}
                    end, {[], []}, Replies),
    case process_multicall_rv(BadReplies, BadNodes) of
        ok ->
            {ok, Versions};
        Errors ->
            Errors
    end.

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
          Src::(node()|undefined)}])
                            -> ok.
bulk_set_vbucket_state(Bucket, RebalancerPid, VBucket,
                       NodeVBucketStateRebalanceStateReplicateFromS) ->
    ?rebalance_info("Doing bulk vbucket ~p state change~n~p",
                    [VBucket, NodeVBucketStateRebalanceStateReplicateFromS]),
    RVs = misc:parallel_map(
            fun ({Node, active, _, _}) ->
                    {Node, unexpected_state_active};
                ({Node, VBucketState, VBucketRebalanceState, ReplicateFrom}) ->
                    {Node, (catch set_vbucket_state(
                                    Bucket, Node, RebalancerPid, VBucket,
                                    VBucketState, VBucketRebalanceState,
                                    ReplicateFrom))}
            end, NodeVBucketStateRebalanceStateReplicateFromS, infinity),
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
                  VBucketRebalanceState, ReplicateFrom) ->
    SubCall = {update_vbucket_state, VBucket, VBucketState,
               VBucketRebalanceState, ReplicateFrom},
    set_vbucket_state_inner(Bucket, Node, RebalancerPid, VBucket, SubCall).

set_vbucket_state(Bucket, Node, RebalancerPid, VBucket, VBucketState,
                  VBucketRebalanceState, ReplicateFrom, Topology) ->
    SubCall = {update_vbucket_state, VBucket, VBucketState,
               VBucketRebalanceState, ReplicateFrom, Topology},
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
handle_call(Call, From, State) ->
    do_handle_call(Call, From, cleanup_rebalance_artifacts(Call, State)).

do_handle_call(prepare_flush, _From, #state{bucket_name = BucketName} = State) ->
    ?log_info("Preparing flush by disabling bucket traffic"),
    ns_bucket:deactivate_bucket_data_on_this_node(BucketName),
    {reply, ns_memcached:disable_traffic(BucketName, infinity), State};
do_handle_call(complete_flush, _From, State) ->
    {reply, ok, consider_doing_flush(State)};
do_handle_call(query_vbucket_states, _From, State) ->
    {RV, NewState} = handle_query_vbuckets(query_vbucket_states, State),
    {reply, RV, NewState};
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
    {reply, {ok, [{version,
                   cluster_compat_mode:mb_master_advertised_version()}]},
     set_rebalance_mref(Call, Pid, State1)};

do_handle_call(finish_rebalance, _From, State) ->
    {reply, ok, State#state{rebalance_status = finished}};

do_handle_call({update_vbucket_state, VBucket, NormalState, RebalanceState,
                ReplicateFrom}, From, State) ->
    do_handle_call({update_vbucket_state, VBucket, NormalState, RebalanceState,
                    ReplicateFrom, undefined}, From, State);
do_handle_call({update_vbucket_state, VBucket, NormalState, RebalanceState,
                _ReplicateFrom, _Topology} = Call, From, State) ->
    NewState = apply_new_vbucket_state(VBucket, NormalState, RebalanceState,
                                       State),
    delegate_apply_vbucket_state(Call, From, NewState);
do_handle_call({delete_vbucket, VBucket} = Call, From, State) ->
    NewState = apply_new_vbucket_state(VBucket, missing, undefined, State),
    delegate_apply_vbucket_state(Call, From, NewState);
do_handle_call({apply_new_config,
                NewBucketConfig, IgnoredVBuckets}, From, State) ->
    %% called on pre 6.5 clusters only
    [] = IgnoredVBuckets,
    do_handle_call({apply_new_config, NewBucketConfig}, From, State);
do_handle_call({apply_new_config,
                _Caller, NewBucketConfig, IgnoredVBuckets}, From, State) ->
    %% called on pre 6.5 clusters only
    [] = IgnoredVBuckets,
    do_handle_call({apply_new_config, NewBucketConfig}, From, State);
do_handle_call({apply_new_config, NewBucketConfig}, _From, State) ->
    handle_apply_new_config(NewBucketConfig, State);
do_handle_call({apply_new_config_replicas_phase,
                NewBucketConfig, IgnoredVBuckets}, From, State) ->
    %% called on pre 6.5 clusters only
    [] = IgnoredVBuckets,
    do_handle_call({apply_new_config_replicas_phase, NewBucketConfig},
                   From, State);
do_handle_call({apply_new_config_replicas_phase, NewBucketConfig},
               _From, State) ->
    handle_apply_new_config_replicas_phase(NewBucketConfig, State);
do_handle_call({wait_index_updated, VBucket}, From,
               #state{bucket_name = Bucket} = State) ->
    State2 = spawn_rebalance_subprocess(
               State,
               From,
               fun () ->
                       ns_couchdb_api:wait_index_updated(Bucket, VBucket)
               end),
    {noreply, State2};
do_handle_call({wait_dcp_data_move, ReplicaNodes, VBucket}, From,
               #state{bucket_name = Bucket} = State) ->
    State2 = spawn_rebalance_subprocess(
               State,
               From,
               fun () ->
                       dcp_replicator:wait_for_data_move(ReplicaNodes, Bucket,
                                                         VBucket)
               end),
    {noreply, State2};
do_handle_call({dcp_takeover, OldMasterNode, VBucket}, From,
               #state{bucket_name = Bucket} = State) ->
    State2 = spawn_rebalance_subprocess(
               State,
               From,
               fun () ->
                       replication_manager:dcp_takeover(Bucket, OldMasterNode,
                                                        VBucket)
               end),
    {noreply, State2};
do_handle_call(initiate_indexing, From, #state{bucket_name = Bucket} = State) ->
    State2 = spawn_rebalance_subprocess(
               State,
               From,
               fun () ->
                       ok = ns_couchdb_api:initiate_indexing(Bucket)
               end),
    {noreply, State2};
do_handle_call({wait_seqno_persisted, VBucket, SeqNo},
               From,
               #state{bucket_name = Bucket} = State) ->
    State2 =
        spawn_rebalance_subprocess(
          State,
          From,
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
          end),
    {noreply, State2};
do_handle_call({inhibit_view_compaction, Pid},
               From,
               #state{bucket_name = Bucket} = State) ->
    State2 = spawn_rebalance_subprocess(
               State,
               From,
               fun () ->
                       compaction_daemon:inhibit_view_compaction(Bucket, Pid)
               end),
    {noreply, State2};
do_handle_call({uninhibit_view_compaction, Ref},
               From,
               #state{bucket_name = Bucket} = State) ->
    State2 = spawn_rebalance_subprocess(
               State,
               From,
               fun () ->
                       compaction_daemon:uninhibit_view_compaction(Bucket, Ref)
               end),
    {noreply, State2};
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
do_handle_call(mark_warmed, _From, #state{bucket_name = Bucket} = State) ->
    RV = ns_memcached:mark_warmed(Bucket),
    ok = ns_bucket:activate_bucket_data_on_this_node(Bucket),
    {reply, RV, State}.

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
    State#state{rebalance_subprocesses = [{From, Pid} | Subprocesses]}.

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
            catch T:E ->
                    ?log_error("Parsing flushseq failed: ~p",
                               [{T, E, erlang:get_stacktrace()}]),
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
                            _RebalanceState, ReplicateFrom, Topology},
                           #state{bucket_name = BucketName} = AgentState) ->
    %% TODO: consider infinite timeout. It's local memcached after all
    ok = ns_memcached:set_vbucket(BucketName, VBucket, NormalState, Topology),
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
                       binary_to_existing_atom(Node, latin1)
               end, Chain) || Chain <- ejson:decode(Topology)].

is_topology_same(active, Chain, MemcachedTopology) ->
    cluster_compat_mode:is_cluster_65() =:= false orelse
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
            query_vbucket_states ->
                {?cut((catch ns_memcached:local_connected_and_list_vbuckets(
                               BucketName))), []};
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
                                     [{VBucket, WantedState, [Chain]} | ToSet],
                                     ToDelete, NewWanted};
                                _ ->
                                    {VBucket + 1,
                                     [{VBucket, WantedState,
                                       undefined} | ToSet],
                                     ToDelete, NewWanted}
                            end
                    end
            end, {0, [], [], []}, Map),

    NewWanted = lists:reverse(NewWantedRev),
    NewRebalance = [undefined || _ <- NewWantedRev],
    State2 = State#state{last_applied_vbucket_states = NewWanted,
                         rebalance_only_vbucket_states = NewRebalance},

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
    case cluster_compat_mode:is_cluster_65() of
        true ->
            ok = ns_memcached:set_vbuckets(BucketName, ToSet);
        false ->
            %% Do not set the Topology, only state of vbucket.
            CompatToSet = [{VB, S, undefined} || {VB, S, _} <- ToSet],
            ok = ns_memcached:set_vbuckets(BucketName, CompatToSet)
    end,

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
