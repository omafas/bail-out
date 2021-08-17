%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

-module(ns_single_vbucket_mover).

-export([spawn_mover/5, mover/6]).

-include("ns_common.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

spawn_mover(Bucket, VBucket, OldChain, NewChain, Quirks) ->
    Parent = self(),
    Pid = proc_lib:spawn_link(ns_single_vbucket_mover, mover,
                              [Parent, Bucket,
                               VBucket, OldChain, NewChain, Quirks]),
    ?rebalance_debug("Spawned single vbucket mover: ~p (~p)",
                     [[Parent, Bucket, VBucket,
                       OldChain, NewChain, Quirks], Pid]),
    Pid.

get_cleanup_list() ->
    case erlang:get(cleanup_list) of
        undefined -> [];
        X -> X
    end.

cleanup_list_add(Pid) ->
    List = get_cleanup_list(),
    List2 = ordsets:add_element(Pid, List),
    erlang:put(cleanup_list, List2).

cleanup_list_del(Pid) ->
    List = get_cleanup_list(),
    List2 = ordsets:del_element(Pid, List),
    erlang:put(cleanup_list, List2).

mover(Parent, Bucket, VBucket, OldChain, NewChain, Quirks) ->
    master_activity_events:note_vbucket_mover(self(), Bucket, hd(OldChain),
                                              VBucket, OldChain, NewChain),
    misc:try_with_maybe_ignorant_after(
      fun () ->
              process_flag(trap_exit, true),
              mover_inner(Parent, Bucket, VBucket, OldChain, NewChain, Quirks),
              on_move_done(Parent, Bucket, VBucket, OldChain, NewChain)
      end,
      fun () ->
              misc:sync_shutdown_many_i_am_trapping_exits(get_cleanup_list())
      end),

    ns_vbucket_mover:note_move_done(Parent, self()).

spawn_and_wait(Body) ->
    WorkerPid = proc_lib:spawn_link(Body),
    cleanup_list_add(WorkerPid),
    receive
        {'EXIT', From, Reason} = ExitMsg ->
            case From =:= WorkerPid andalso Reason =:= normal of
                true ->
                    cleanup_list_del(WorkerPid),
                    ok;
                false ->
                    self() ! ExitMsg,

                    case misc:is_shutdown(Reason) of
                        true ->
                            ?log_debug("Got shutdown exit signal ~p. "
                                       "Assuming it's from our parent", [ExitMsg]),
                            exit(Reason);
                        false ->
                            ?log_error("Got unexpected exit signal ~p", [ExitMsg]),
                            exit({unexpected_exit, ExitMsg})
                    end
            end
    end.

wait_index_updated(Bucket, Parent, NewNode, ReplicaNodes, VBucket) ->
    case ns_config:read_key_fast(rebalance_index_waiting_disabled, false) of
        false ->
            master_activity_events:note_wait_index_updated_started(Bucket, NewNode, VBucket),
            spawn_and_wait(
              fun () ->
                      ok = janitor_agent:wait_index_updated(Bucket, Parent, NewNode, ReplicaNodes, VBucket)
              end),
            master_activity_events:note_wait_index_updated_ended(Bucket, NewNode, VBucket);
        _ ->
            ok
    end.

maybe_inhibit_view_compaction(_Parent, _Node, _Bucket, _NewNode, false) ->
    ok;
maybe_inhibit_view_compaction(Parent, Node, Bucket, NewNode, true) ->
    inhibit_view_compaction(Parent, Node, Bucket, NewNode).

inhibit_view_compaction(Parent, Node, Bucket, NewNode) ->
    case cluster_compat_mode:rebalance_ignore_view_compactions() of
        false ->
            spawn_and_wait(
              fun () ->
                      InhibitedNodes = lists:usort([Node, NewNode]),
                      ns_vbucket_mover:inhibit_view_compaction(Bucket, Parent,
                                                               InhibitedNodes)
              end);
        _ ->
            ok
    end.

maybe_initiate_indexing(_Bucket, _Parent, _JustBackfillNodes, _ReplicaNodes, _VBucket, false) ->
    ok;
maybe_initiate_indexing(Bucket, Parent, JustBackfillNodes, ReplicaNodes, VBucket, true) ->
    ok = janitor_agent:initiate_indexing(Bucket, Parent, JustBackfillNodes, ReplicaNodes, VBucket),
    master_activity_events:note_indexing_initiated(Bucket, JustBackfillNodes, VBucket).

mover_inner(Parent, Bucket, VBucket,
            [undefined|_] = _OldChain,
            [NewMaster|_] = _NewChain, _Quirks) ->
    set_vbucket_state(Bucket, NewMaster, Parent, VBucket,
                      active, undefined, undefined, undefined);
mover_inner(Parent, Bucket, VBucket,
            [OldMaster|OldReplicas] = OldChain,
            [NewMaster|_] = NewChain, Quirks) ->
    IndexAware = cluster_compat_mode:is_index_aware_rebalance_on(),

    maybe_inhibit_view_compaction(Parent, OldMaster, Bucket, NewMaster, IndexAware),

    %% build new chain as replicas of existing master
    {ReplicaNodes, JustBackfillNodes} =
        get_replica_and_backfill_nodes(OldMaster, NewChain),

    maybe_reset_replicas(Bucket, Parent, VBucket,
                         ReplicaNodes ++ JustBackfillNodes, Quirks),

    %% setup replication streams to replicas from the existing master
    set_initial_vbucket_state(Bucket, Parent, VBucket, OldMaster, ReplicaNodes, JustBackfillNodes),

    %% initiate indexing on new master (replicas are ignored for now)
    %% at this moment since the stream to new master is created (if there is a new master)
    %% ep-engine guarantees that it can support indexing
    maybe_initiate_indexing(Bucket, Parent, JustBackfillNodes, ReplicaNodes, VBucket, IndexAware),

    master_activity_events:note_backfill_phase_started(Bucket, VBucket),
    %% wait for backfill on all the opened streams
    AllBuiltNodes = JustBackfillNodes ++ ReplicaNodes,
    wait_dcp_data_move(Bucket, Parent, OldMaster, AllBuiltNodes, VBucket),

    %% grab the seqno from the old master and wait till this seqno is
    %% persisted on all the replicas
    wait_master_seqno_persisted_on_replicas(Bucket, VBucket, Parent,
                                            OldMaster, AllBuiltNodes),

    ?rebalance_debug("Backfill of vBucket ~p completed after waiting for "
                     "persistence of high sequence number.", [VBucket]),

    master_activity_events:note_backfill_phase_ended(Bucket, VBucket),

    %% notify parent that the backfill is done, so it can start rebalancing
    %% next vbucket
    ns_vbucket_mover:note_backfill_done(Parent, self()),

    ok = ns_rebalancer:check_test_condition(backfill_done, Bucket),

    case OldMaster =:= NewMaster of
        true ->
            %% if there's nothing to move, we're done
            %% we're safe if old and new masters are the same; basically our
            %% replication streams are already established
            set_dual_topology(Bucket, OldMaster, Parent, VBucket,
                              undefined, OldChain, NewChain, AllBuiltNodes);
        false ->
            case IndexAware of
                true ->
                    %% pause index on old master node
                    case cluster_compat_mode:is_index_pausing_on() of
                        true ->
                            ns_server_stats:notify_counter(<<"index_pausing_runs">>),
                            set_vbucket_state(Bucket, OldMaster, Parent, VBucket,
                                              active, paused, undefined,
                                              [OldChain]),
                            wait_master_seqno_persisted_on_replicas(Bucket, VBucket, Parent, OldMaster,
                                                                    AllBuiltNodes);
                        false ->
                            ok
                    end,

                    wait_index_updated(Bucket, Parent, NewMaster, ReplicaNodes, VBucket),

                    ?rebalance_debug("Index is updated on new master. Bucket ~p, partition ~p",
                                     [Bucket, VBucket]);
                false ->
                    ok
            end,

            set_dual_topology(Bucket, OldMaster, Parent, VBucket,
                              paused, OldChain, NewChain, AllBuiltNodes),

            master_activity_events:note_takeover_started(Bucket, VBucket, OldMaster,
                                                         NewMaster),

            AllReplicaNodes = lists:usort(ReplicaNodes ++ OldReplicas) -- [undefined],
            dcp_takeover(Bucket, Parent,
                         OldMaster, NewMaster,
                         AllReplicaNodes, VBucket, Quirks),

            master_activity_events:note_takeover_ended(Bucket, VBucket, OldMaster, NewMaster),

            set_vbucket_state(Bucket, NewMaster, Parent, VBucket,
                              active, undefined, undefined, undefined),

            %% Vbucket on the old master is dead.
            %% Cleanup replication streams from the old master to the
            %% new and old replica nodes.
            %% We need to cleanup streams to the old replicas as well
            %% to prevent race condition like the one described below.
            %%
            %% At the end of the vbucket move,
            %% update_replication_post_move performs bulk vbucket state
            %% update. Based on how the old and new chains are, one possible
            %% set of state transitions are as follows:
            %%  - change state of vbucket, say vb1, on old master to replica
            %%  - change state of vb1 on old replicas. This results in closing
            %%    of streams from old master to the old replicas.
            %% Ideally, we want all replication streams from the old master
            %% to close before we change the state of the vbucket on the
            %% old master. But, bulk vbucket state change is racy which causes
            %% other races. Consider this sequence which can occur
            %% if state of vb on old master changes before the replication
            %% streams from it are closed.
            %%  1. State of vb1 on old master changes to replica. Replication
            %%     stream from old master to the old replicas are still active.
            %%  2. Because of the state change, EP engine sends dcp stream
            %%     end to old replicas.
            %%  3. Old replica is in middle of processing the dcp stream end.
            %%     There is a few milisecond window when
            %%     ns-server has processed dcp stream end but EP engine has not.
            %%  4. Setup replication stream for some other vbucket comes in
            %%     during the above window. It tries to add back the
            %%     replication stream from old master to the old replicas.
            %%     Since EP engine has not processed the dcp stream end
            %%     yet, the stream add fails with eexist causing rebalance to
            %%     fail.
            %% Since state of the vb on old master is no longer active, we
            %% should not be trying to add a stream from it.
            %% If all replication streams from old master are closed
            %% here before the vbucket state changes on the old master,
            %% then we will not end up in race conditions like these.

            OldReplicaNodes = [N || N <- OldReplicas,
                                    N =/= undefined,
                                    N =/= NewMaster],
            CleanupNodes = lists:subtract(OldReplicaNodes, ReplicaNodes) ++
                ReplicaNodes,
            cleanup_old_streams(Bucket, CleanupNodes, Parent, VBucket)
    end.

maybe_reset_replicas(Bucket, RebalancerPid, VBucket, Nodes, Quirks) ->
    case rebalance_quirks:is_enabled(reset_replicas, Quirks) of
        true ->
            ?log_info("Resetting replicas for "
                      "bucket ~p, vbucket ~p on nodes ~p",
                      [Bucket, VBucket, Nodes]),

            cleanup_old_streams(Bucket, Nodes, RebalancerPid, VBucket),
            spawn_and_wait(
              fun () ->
                      ok = janitor_agent:delete_vbucket_copies(
                             Bucket, RebalancerPid, Nodes, VBucket)
              end);
        false ->
            ok
    end.

set_dual_topology(Bucket, ActiveNode,
                  RebalancerPid, VBucket, VBucketRebalanceState,
                  OldTopology, NewTopology, AllBuiltNodes) ->
    case cluster_compat_mode:is_cluster_65() of
        true ->
            DualTopology = [OldTopology, NewTopology],
            set_vbucket_state_65(Bucket, ActiveNode, RebalancerPid,
                                        VBucket, active, VBucketRebalanceState,
                                        undefined, DualTopology),
            %% We wait for seqno because we may not have sync write on NewChain
            %% but have been committed on the OldChain.
            wait_master_seqno_persisted_on_replicas(Bucket,
                                                    VBucket, RebalancerPid,
                                                    ActiveNode, AllBuiltNodes);
        false ->
            ok
    end.

set_vbucket_state(Bucket, Node, RebalancerPid, VBucket,
                  VBucketState, VBucketRebalanceState, ReplicateFrom,
                  Topology) ->
    case cluster_compat_mode:is_cluster_65() of
        true ->
            set_vbucket_state_65(Bucket, Node,
                                        RebalancerPid, VBucket, VBucketState,
                                        VBucketRebalanceState,
                                        ReplicateFrom, Topology);
        false ->
            set_vbucket_state_pre_65(Bucket, Node,
                                            RebalancerPid, VBucket,
                                            VBucketState, VBucketRebalanceState,
                                            ReplicateFrom)
    end.

set_vbucket_state_pre_65(Bucket, Node,
                                RebalancerPid, VBucket, VBucketState,
                                VBucketRebalanceState, ReplicateFrom) ->
    spawn_and_wait(
      fun () ->
              ok = janitor_agent:set_vbucket_state(
                     Bucket, Node, RebalancerPid, VBucket,
                     VBucketState, VBucketRebalanceState, ReplicateFrom)
      end).

set_vbucket_state_65(Bucket, Node, RebalancerPid, VBucket,
                            VBucketState, VBucketRebalanceState, ReplicateFrom,
                            Topology) ->
    spawn_and_wait(
      fun () ->
              ok = janitor_agent:set_vbucket_state(
                     Bucket, Node, RebalancerPid, VBucket,
                     VBucketState, VBucketRebalanceState, ReplicateFrom,
                     Topology)
      end).

%% This ensures that all streams into new set of replicas (either replica
%% building streams or old replications) are closed. It's needed because
%% ep-engine doesn't like it when there are two consumer connections for the
%% same vbucket on a node.
%%
%% Note that some of the same streams appear to be cleaned up in
%% update_replication_post_move, but this is done in unpredictable order
%% there, so it's still possible to add a stream before the old one is
%% closed. In addition to that, it's also not enough to just clean up old
%% replications, because we also create rebalance-specific streams that can
%% lead to the same problems.
cleanup_old_streams(Bucket, Nodes, RebalancerPid, VBucket) ->
    Changes = [{Node, replica, undefined, undefined} || Node <- Nodes],
    spawn_and_wait(
      fun () ->
              ok = janitor_agent:bulk_set_vbucket_state(Bucket, RebalancerPid, VBucket, Changes)
      end).

dcp_takeover(Bucket, Parent,
             OldMaster, NewMaster, ReplicaNodes, VBucket, Quirks) ->
    case rebalance_quirks:is_enabled(takeover_via_orchestrator, Quirks) of
        true ->
            dcp_takeover_via_orchestrator(Bucket, Parent,
                                          OldMaster, NewMaster,
                                          ReplicaNodes, VBucket, Quirks);
        false ->
            dcp_takeover_regular(Bucket, Parent, OldMaster, NewMaster, VBucket)
    end.

dcp_takeover_via_orchestrator(Bucket, Parent,
                              OldMaster, NewMaster,
                              ReplicaNodes, VBucket, Quirks) ->
    ?log_info("Performing special takeover~n"
              "Bucket = ~p, VBucket = ~p~n"
              "OldMaster = ~p, NewMaster = ~p",
              [Bucket, VBucket, OldMaster, NewMaster]),

    ?log_info("Tearing down replication for "
              "vbucket ~p (bucket ~p) from ~p to ~p",
              [VBucket, Bucket, OldMaster, NewMaster]),
    set_vbucket_state(Bucket, NewMaster, Parent,
                      VBucket, pending, passive, undefined, undefined),

    DisableOldMaster = rebalance_quirks:is_enabled(disable_old_master, Quirks),
    case DisableOldMaster of
        true ->
            %% The subsequent vbucket state change on the old master will
            %% result in these replications being terminated anyway. But that
            %% will happen asynchronously. So once we create post-rebalance
            %% replications, we won't be able to reliably know that
            %% replications don't exist anymore (or rather, that ep-engine
            %% processed the stream_end messages sent by the old
            %% master). That's why we terminate these replications in advance
            %% synchronously.
            ?log_info("Terminating replications for "
                      "vbucket ~p (bucket ~p) from ~p to nodes ~p",
                      [VBucket, Bucket, OldMaster, ReplicaNodes]),
            cleanup_old_streams(Bucket, ReplicaNodes, Parent, VBucket),

            ?log_info("Disabling vbucket ~p (bucket ~p) on ~p",
                      [VBucket, Bucket, OldMaster]),
            set_vbucket_state(Bucket, OldMaster, Parent,
                              VBucket, replica, passive, undefined, undefined);
        false ->
            ok
    end,

    ?log_info("Spawning takeover replicator for vbucket ~p (bucket ~p) from ~p to ~p",
              [VBucket, Bucket, OldMaster, NewMaster]),
    Pid = start_takeover_replicator(NewMaster, OldMaster, Bucket, VBucket),
    cleanup_list_add(Pid),
    spawn_and_wait(
      fun () ->
              ?log_info("Starting takeover for vbucket ~p (bucket ~p) from ~p to ~p",
                        [VBucket, Bucket, OldMaster, NewMaster]),
              do_takeover(DisableOldMaster, Pid, Bucket, VBucket),
              ?log_info("Takeover for vbucket ~p (bucket ~p) from ~p to ~p finished",
                        [VBucket, Bucket, OldMaster, NewMaster])
      end).

start_takeover_replicator(NewMaster, OldMaster, Bucket, VBucket) ->
    ConnName = get_takeover_connection_name(NewMaster, OldMaster, Bucket,
                                            VBucket),

    RepFeatures = dcp_sup:get_replication_features(),
    {ok, Pid} = dcp_replicator:start_link(undefined, NewMaster, OldMaster,
                                          Bucket, ConnName, RepFeatures),
    Pid.

get_takeover_connection_name(NewMaster, OldMaster, Bucket, VBucket) ->
    ConnName0 = lists:concat(["replication:takeover:",
                              binary_to_list(couch_uuids:random()), ":",
                              atom_to_list(OldMaster), "->",
                              atom_to_list(NewMaster), ":",
                              Bucket, ":",
                              integer_to_list(VBucket)]),

    case length(ConnName0) =< ?MAX_DCP_CONNECTION_NAME of
        true ->
            ConnName0;
        false ->
            {OldM, NewM} = dcp_replicator:trim_common_prefix(
                             OldMaster, NewMaster),
            ConnName1 =
                lists:concat(["replication:takeover:",
                              binary_to_list(couch_uuids:random()), ":",
                              OldM, "->",
                              NewM, ":",
                              string:slice(Bucket, 0, 60), ":",
                              integer_to_list(VBucket)]),
                true = length(ConnName1) =< ?MAX_DCP_CONNECTION_NAME,
                ConnName1
    end.

do_takeover(false, Pid, _Bucket, VBucket) ->
    do_takeover(Pid, VBucket);
do_takeover(true, Pid, Bucket, VBucket) ->
    Timeout = ns_config:get_timeout(takeover_via_orchestrator, 10000),
    {ok, TRef} = timer:exit_after(Timeout,
                                  {takeover_timeout, Bucket, VBucket}),
    try
        do_takeover(Pid, VBucket)
    after
        timer:cancel(TRef)
    end.

do_takeover(Pid, VBucket) ->
    ok = dcp_replicator:takeover(Pid, VBucket).

dcp_takeover_regular(Bucket, Parent, OldMaster, NewMaster, VBucket) ->
    spawn_and_wait(
      fun () ->
              ok = janitor_agent:dcp_takeover(Bucket, Parent, OldMaster, NewMaster, VBucket)
      end).

wait_dcp_data_move(Bucket, Parent, SrcNode, DstNodes, VBucket) ->
    spawn_and_wait(
      fun () ->
              ?rebalance_debug(
                 "Will wait for backfill on all opened streams for bucket = ~p partition ~p src node = ~p dest nodes = ~p",
                 [Bucket, VBucket, SrcNode, DstNodes]),
              case janitor_agent:wait_dcp_data_move(Bucket, Parent, SrcNode, DstNodes, VBucket) of
                  ok ->
                      ?rebalance_debug(
                         "DCP data is up to date for bucket = ~p partition ~p src node = ~p dest nodes = ~p",
                         [Bucket, VBucket, SrcNode, DstNodes]),
                      ok;
                  Error ->
                      erlang:error({dcp_wait_for_data_move_failed,
                                    Bucket, VBucket, SrcNode, DstNodes, Error})
              end
      end).

wait_master_seqno_persisted_on_replicas(Bucket, VBucket, Parent, MasterNode, ReplicaNodes) ->
    SeqNo = janitor_agent:get_vbucket_high_seqno(Bucket, Parent, MasterNode, VBucket),
    master_activity_events:note_seqno_waiting_started(Bucket, VBucket, SeqNo, ReplicaNodes),
    wait_seqno_persisted_many(Bucket, Parent, ReplicaNodes, VBucket, SeqNo),
    master_activity_events:note_seqno_waiting_ended(Bucket, VBucket, SeqNo, ReplicaNodes).

wait_seqno_persisted_many(Bucket, Parent, Nodes, VBucket, SeqNo) ->
    spawn_and_wait(
      fun () ->
              RVs = misc:parallel_map(
                      fun (Node) ->
                              {Node, (catch janitor_agent:wait_seqno_persisted(Bucket, Parent, Node, VBucket, SeqNo))}
                      end, Nodes, infinity),
              NonOks = [P || {_N, V} = P <- RVs,
                             V =/= ok],
              case NonOks =:= [] of
                  true -> ok;
                  false ->
                      erlang:error({wait_seqno_persisted_failed, Bucket, VBucket, SeqNo, NonOks})
              end
      end).

get_replica_and_backfill_nodes(MasterNode, [NewMasterNode|_] = NewChain) ->
    ReplicaNodes = [N || N <- NewChain,
                         N =/= MasterNode,
                         N =/= undefined,
                         N =/= NewMasterNode],
    JustBackfillNodes = [N || N <- [NewMasterNode],
                              N =/= MasterNode],
    true = (JustBackfillNodes =/= [undefined]),
    {ReplicaNodes, JustBackfillNodes}.

set_initial_vbucket_state(Bucket, Parent, VBucket, SrcNode, ReplicaNodes, JustBackfillNodes) ->
    Changes = [{Replica, replica, undefined, SrcNode}
               || Replica <- ReplicaNodes]
        ++ [{FutureMaster, replica, passive, SrcNode}
            || FutureMaster <- JustBackfillNodes],
    spawn_and_wait(
      fun () ->
              janitor_agent:bulk_set_vbucket_state(Bucket, Parent, VBucket, Changes)
      end).

%% @private
%% @doc {Src, Dst} pairs from a chain with unmapped nodes filtered out.
pairs([undefined | _]) ->
    [];
pairs([Master | Replicas]) ->
    [{Master, R} || R <- Replicas, R =/= undefined].

%% @private
%% @doc Perform post-move replication fixup.
update_replication_post_move(RebalancerPid, BucketName, VBucket, OldChain, NewChain) ->
    ChangeReplica = fun (Dst, Src) ->
                            {Dst, replica, undefined, Src}
                    end,
    %% destroy remnants of old replication chain
    DelChanges = [ChangeReplica(D, undefined) || {_, D} <- pairs(OldChain),
                                                 not lists:member(D, NewChain)],
    %% just start new chain of replications. Old chain is dead now
    AddChanges = [ChangeReplica(D, S) || {S, D} <- pairs(NewChain)],
    ok = janitor_agent:bulk_set_vbucket_state(BucketName, RebalancerPid,
                                              VBucket, AddChanges ++ DelChanges).

on_move_done(RebalancerPid, Bucket, VBucket, OldChain, NewChain) ->
    WorkerPid = self(),

    spawn_and_wait(
      fun () ->
              on_move_done_body(RebalancerPid, WorkerPid,
                                Bucket, VBucket, OldChain, NewChain)
      end).

on_move_done_body(RebalancerPid, WorkerPid, Bucket, VBucket, OldChain,
                  [NewMaster | _] = NewChain) ->
    update_vbucket_map(RebalancerPid, WorkerPid, Bucket, VBucket),

    case cluster_compat_mode:is_cluster_65() of
        true ->
            %% Set topology on the NewMaster.
            janitor_agent:set_vbucket_state(
              Bucket, NewMaster, RebalancerPid, VBucket,
              active, undefined, undefined, [NewChain]);
        false ->
            ok
    end,

    update_replication_post_move(RebalancerPid, Bucket, VBucket, OldChain, NewChain),

    OldCopies0 = OldChain -- NewChain,
    OldCopies = [OldCopyNode || OldCopyNode <- OldCopies0,
                                OldCopyNode =/= undefined],
    ?rebalance_info("Moving vbucket ~p done. Will delete it on: ~p", [VBucket, OldCopies]),
    case janitor_agent:delete_vbucket_copies(Bucket, RebalancerPid, OldCopies, VBucket) of
        ok ->
            ok;
        {errors, BadDeletes} ->
            ?log_error("Deleting some old copies of vbucket failed: ~p", [BadDeletes])
    end.

update_vbucket_map(RebalancerPid, WorkerPid, Bucket, VBucket) ->
    ?log_debug("Updating vbucket map "
               "for bucket ~p, vbucket ~p", [Bucket, VBucket]),

    Start = erlang:monotonic_time(microsecond),
    case ns_vbucket_mover:update_vbucket_map(RebalancerPid, WorkerPid) of
        ok ->
            End = erlang:monotonic_time(microsecond),
            ?log_debug("Updated vbucket map for bucket ~p, vbucket ~p in ~b us",
                       [Bucket, VBucket, End - Start]);
        Error ->
            ?log_error("Failed to update vbucket "
                       "map for bucket ~p, vbucket ~p:~n~p",
                       [Bucket, VBucket, Error]),
            exit({failed_to_update_vbucket_map, Bucket, VBucket, Error})
    end.

-ifdef(TEST).
get_takeover_connection_name_test() ->
    meck:new(couch_uuids, [passthrough]),
    meck:expect(couch_uuids, random,
                fun () -> <<"a5292f34ef9062cae8dc4a86e82ac3c8">> end),

    %% Connection name fits into the maximum allowed

    NodeA = 'nodeA.eng.couchbase.com',
    NodeB = 'nodeB.eng.couchbase.com',
    BucketAB = "bucket1",
    ConnAB = get_takeover_connection_name(NodeA, NodeB, BucketAB, 0),
    ?assertEqual("replication:takeover:a5292f34ef9062cae8dc4a86e82ac3c8:"
                 "nodeB.eng.couchbase.com->nodeA.eng.couchbase.com:bucket1:0",
                 ConnAB),
    ?assertEqual(true, length(ConnAB) =< ?MAX_DCP_CONNECTION_NAME),

    %% Test where the connection name, using the pre-NEO method, won't
    %% fit into the maximum allowed.

    Node1 = "ns_1@platform-couchbase-cluster-0000.platform-couchbase-cluster."
            "couchbase-new-pxxxxxxx.svc",
    Node2 = "ns_1@platform-couchbase-cluster-0001.platform-couchbase-cluster."
            "couchbase-new-pxxxxxxx.svc",
    Bucket12 = "com.yyyyyy.digital.ms.shoppingcart.shoppingcart.1234567890"
               "12345678901234567890",
    Conn12 = get_takeover_connection_name(list_to_atom(Node1),
                                          list_to_atom(Node2),
                                          Bucket12, 1023),
    ?assertEqual("replication:takeover:a5292f34ef9062cae8dc4a86e82ac3c8:1."
                 "platform-couchbase-cluster.couchb->0.platform-couchbase-"
                 "cluster.couchb:com.yyyyyy.digital.ms.shoppingcart."
                 "shoppingcart.123456789012:1023", Conn12),

    %% Test that the node names aren't shortened too much (note the only
    %% difference is the last character).

    Node3 = "ManyManyManyManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-0000",
    Node4 = "ManyManyManyManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-0001",
    LongBucket = "travel-sample-with-a-very-very-very-very-long-bucket-name",
    Conn34 = get_takeover_connection_name(list_to_atom(Node3),
                                          list_to_atom(Node4),
                                          LongBucket, 777),
    ?assertEqual("replication:takeover:a5292f34ef9062cae8dc4a86e82ac3c8:"
                 "s_1@platform-couchbase-cluster-0001->s_1@platform-couchbase-"
                 "cluster-0000:travel-sample-with-a-very-very-very-very-"
                 "long-bucket-name:777", Conn34),

    %% Test with unique node names but one is much longer than the other.

    Node5 = "AShortNodeName",
    Node6 = "ManyManyManyManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-AndEvenMoreCharactersToMakeThisNodeNameLongEnoughToRequireIt"
            "ToBeShortened",
    Conn56 = get_takeover_connection_name(list_to_atom(Node5),
                                          list_to_atom(Node6),
                                          LongBucket, 789),
    ?assertEqual("replication:takeover:a5292f34ef9062cae8dc4a86e82ac3c8:"
                 "ManyManyManyManyCommonCharacters_ns->AShortNodeName:"
                 "travel-sample-with-a-very-very-very-very-long-bucket-name:"
                 "789", Conn56),

    %% Long node names with no common prefix.

    Node7 = "ManyManyManyManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-AndEvenMoreCharactersToMakeThisNodeNameLongEnoughToRequireIt"
            "ToBeShortened",
    Node8 = "NoCommonPrefixManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-AndEvenMoreCharactersToMakeThisNodeNameLongEnoughToRequireIt"
            "ToBeShortened",
    Conn78 = get_takeover_connection_name(list_to_atom(Node7),
                                          list_to_atom(Node8),
                                          LongBucket, 222),
    ?assertEqual("replication:takeover:a5292f34ef9062cae8dc4a86e82ac3c8:"
                 "NoCommonPrefixManyCommonCharacters_->ManyManyManyMany"
                 "CommonCharacters_ns:travel-sample-with-a-very-very-very-"
                 "very-long-bucket-name:222", Conn78),

    meck:unload(couch_uuids).

-endif.
