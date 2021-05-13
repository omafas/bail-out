%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(chronicle_master).

-behaviour(gen_server2).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("cut.hrl").

-define(SERVER, {via, leader_registry, ?MODULE}).

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_info/2,
         add_replica/3,
         remove_peer/1,
         activate_nodes/1,
         deactivate_nodes/1,
         start_failover/2,
         complete_failover/2,
         upgrade_cluster/1,
         get_prev_failover_nodes/1,
         fetch_snapshot/1]).

-define(CALL_TIMEOUT, ?get_timeout(call, 60000)).
-define(JANITOR_TIMEOUT, ?get_timeout(janitor, 60000)).
-define(UPGRADE_TIMEOUT, ?get_timeout(upgrade, 240000)).
-define(AFTER_UPGRADE_CLEANUP_TIMEOUT,
        ?get_timeout(after_upgrade_cleanup, 60000)).

-record(state, {self_ref, janitor_timer_ref, events}).

start_link() ->
    misc:start_singleton(gen_server2, start_link, [?SERVER, ?MODULE, [], []]).

wait_for_server_start() ->
    misc:wait_for_global_name(?MODULE).

add_replica(Node, GroupUUID, Services) ->
    call({add_replica, Node, GroupUUID, Services}).

activate_nodes(Nodes) ->
    call({activate_nodes, Nodes}).

deactivate_nodes(Nodes) ->
    call({deactivate_nodes, Nodes}).

remove_peer(Node) ->
    call({remove_peer, Node}).

start_failover(Nodes, Ref) when Nodes =/= [] ->
    chronicle = chronicle_compat:backend(),
    wait_for_server_start(),
    gen_server2:call(?SERVER, {start_failover, Nodes, Ref}, ?CALL_TIMEOUT).

complete_failover(Nodes, Ref) when Nodes =/= [] ->
    chronicle = chronicle_compat:backend(),
    wait_for_server_start(),
    gen_server2:call(?SERVER, {complete_failover, Nodes, Ref}, ?CALL_TIMEOUT).

upgrade_cluster([]) ->
    ok;
upgrade_cluster(OtherNodes) ->
    wait_for_server_start(),
    gen_server2:call(?SERVER, {upgrade_cluster, OtherNodes}, ?UPGRADE_TIMEOUT).

fetch_snapshot({ns_config, _}) ->
    #{};
fetch_snapshot(Txn) ->
    chronicle_compat:txn_get_many([failover_opaque_key()], Txn).

call(Oper) ->
    case chronicle_compat:backend() of
        chronicle ->
            ?log_debug("Calling chronicle_master with ~p", [Oper]),
            call(Oper, 3);
        ns_config ->
            ?log_debug("Performing operation ~p on ns_config", [Oper]),
            case handle_kv_oper(Oper,
                                chronicle_compat:txn(ns_config, _, #{})) of
                {ok, _} ->
                    ok;
                Error ->
                    Error
            end
    end.

call(_Oper, 0) ->
    exit(chronicle_master_call_failed);
call(Oper, Tries) ->
    wait_for_server_start(),
    try gen_server2:call(?SERVER, Oper, ?CALL_TIMEOUT) of
        RV -> RV
    catch
        exit:Error ->
            ?log_debug("Retry due to error: ~p", [Error]),
            timer:sleep(200),
            call(Oper, Tries - 1)
    end.

init([]) ->
    erlang:process_flag(trap_exit, true),
    Self = self(),
    SelfRef = erlang:make_ref(),
    ?log_debug("Starting with SelfRef = ~p", [SelfRef]),
    Events = subscribe_to_chronicle_events(SelfRef),
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({after_upgrade_cleanup, _}) ->
              schedule_after_upgrade_cleanup(Self);
          (_) ->
              ok
      end),

    case ns_config:search(after_upgrade_cleanup) of
        {value, _} ->
            schedule_after_upgrade_cleanup(Self);
        false ->
            ok
    end,

    {ok, arm_janitor_timer(#state{self_ref = SelfRef,
                                  events = Events})}.

handle_call({upgrade_cluster, NodesToAdd}, _From,
            State = #state{self_ref = SelfRef, events = Events}) ->
    ?log_debug("Reinitialize chronicle before the upgrade"),
    chronicle_compat_events:hush_chronicle(),
    ns_pubsub:unsubscribe(Events),
    %% this will wipe chronicle on this node and reprovision it
    chronicle_local:leave_cluster(),
    NewEvents = subscribe_to_chronicle_events(SelfRef),
    chronicle_compat_events:resume_chronicle(),

    ?log_debug("Starting upgrade to chronicle cluster"),

    {ok, Lock} = chronicle:acquire_lock(),
    ?log_debug("Adding nodes ~p to chronicle cluster. Lock: ~p",
               [NodesToAdd, Lock]),
    ClusterInfo = chronicle:get_cluster_info(),

    ?log_debug("Preparing nodes ~p to join chronicle cluster with info ~p",
               [NodesToAdd, ClusterInfo]),
    ok = ns_cluster:prep_chronicle(NodesToAdd, ClusterInfo),

    ?log_debug("Adding nodes ~p as replicas to chronicle cluster",
               [NodesToAdd]),
    ok = chronicle:add_replicas(Lock, NodesToAdd),

    ClusterInfo1 = chronicle:get_cluster_info(),

    ?log_debug("Asking nodes ~p to join chronicle cluster with info ~p",
               [NodesToAdd, ClusterInfo1]),
    ok = ns_cluster:join_chronicle(NodesToAdd, ClusterInfo1),

    ?log_debug("Promoting nodes ~p to voters, Lock: ~p", [NodesToAdd, Lock]),
    set_peer_roles(Lock, NodesToAdd, voter),
    ?log_info("Cluster successfully upgraded to chronicle"),
    {reply, ok, State#state{events = NewEvents}};

handle_call({start_failover, Nodes, Ref}, _From, State) ->
    PreviousFailoverNodes = get_prev_failover_nodes(direct),

    case PreviousFailoverNodes -- Nodes of
        [] ->
            Opaque = {Ref, Nodes},
            NodesToKeep = ns_cluster_membership:nodes_wanted() -- Nodes,

            ?log_info("Starting quorum failover with opaque ~p, "
                      "keeping nodes ~p", [Opaque, NodesToKeep]),
            RV = case chronicle:failover(NodesToKeep, Opaque) of
                     ok ->
                         ok;
                     {error, Error} = E ->
                         ?log_error("Unsuccesfull quorum loss failover. (~p).",
                                    [Error]),
                         E
                 end,
            {reply, RV, State};
        _ ->
            ?log_warning("Requested failover of ~p is incompatibe with "
                         "unfinished failover of ~p",
                         [Nodes, PreviousFailoverNodes]),
            {reply, {incompatible_with_previous, PreviousFailoverNodes}, State}
    end;

handle_call({complete_failover, Nodes, Ref}, _From, State) ->
    ?log_info("Completing quorum loss failover with ref = ~p "
              "removing nodes ~p", [Ref, Nodes]),
    {ok, _} =
        ns_cluster_membership:remove_nodes(
          Nodes,
          transaction_with_key_remove(
            failover_opaque_key(), fun ({ORef, _}) -> ORef =:= Ref end, _)),
    NewState = cancel_janitor_timer(State),
    self() ! janitor,
    {reply, ok, NewState};

handle_call(Oper, _From, #state{self_ref = SelfRef} = State) ->
    NewState = cancel_janitor_timer(State),
    RV = case acquire_lock() of
             {ok, Lock} ->
                 handle_oper(Oper, Lock, SelfRef);
             cannot_acquire_lock ->
                 cannot_acquire_lock
         end,
    {reply, RV, NewState}.

handle_info(arm_janitor_timer, State) ->
    {noreply, arm_janitor_timer(State)};

handle_info(janitor, #state{self_ref = SelfRef} = State) ->
    CleanState = cancel_janitor_timer(State),
    NewState =
        case acquire_lock() of
            {ok, Lock} ->
                case transaction(undefined, Lock, SelfRef,
                                 fun (_) -> {abort, clean} end) of
                    {ok, _, {need_recovery, RecoveryOper}} ->
                        ?log_debug("Janitor found that recovery is needed for "
                                   "operation ~p", [RecoveryOper]),
                        ok = handle_oper(RecoveryOper, Lock, SelfRef);
                    unfinished_failover ->
                        ?log_debug("Skip janitor due to unfinished failover.");
                    clean ->
                        ok
                end,
                CleanState;
            cannot_acquire_lock ->
                ?log_debug("Cannot acquire lock. Try janitor later."),
                arm_janitor_timer(CleanState)
        end,
    {noreply, NewState};

handle_info(after_upgrade_cleanup, State) ->
    RV =
        ns_config:run_txn(
          fun (Config, SetFn) ->
                  case ns_config:search(Config, after_upgrade_cleanup) of
                      false ->
                          {abort, nothing_to_do};
                      {value, KVs} ->
                          {commit,
                           lists:foldl(
                             fun ({K, V}, Acc) -> SetFn(K, V, Acc) end,
                             Config, [{after_upgrade_cleanup,
                                       ?DELETED_MARKER} | KVs])}
                  end
          end),
    case RV of
        {commit, _} ->
            ?log_debug("Successful after upgrade cleanup"),
            ok;
        {abort, nothing_to_do} ->
            ok
    end,
    {noreply, State};

handle_info({'EXIT', From, Reason}, State) ->
    ?log_debug("Received exit from ~p with reason ~p. Exiting.",
               [From, Reason]),
    {stop, Reason, State}.

acquire_lock() ->
    try chronicle:acquire_lock() of
        {ok, Lock} ->
            {ok, Lock}
    catch Type:What ->
            ?log_debug("Cannot acquire lock due to ~p:~p.", [Type, What]),
            cannot_acquire_lock
    end.

set_peer_roles(_Lock, [], _Role) ->
    ok;
set_peer_roles(Lock, Nodes, Role) ->
    ok = chronicle:set_peer_roles(Lock, [{N, Role} || N <- Nodes]).

failover_opaque_key() ->
    '$failover_opaque'.

operation_key() ->
    unfinished_topology_operation.

operation_key_set(Oper, Lock, SelfRef) ->
    {set, operation_key(), {Oper, Lock, SelfRef}}.

get_prev_failover_nodes(Snapshot) ->
    case chronicle_compat:get(Snapshot, failover_opaque_key(), #{}) of
        {ok, {_, Nodes}} ->
            Nodes;
        {error, not_found} ->
            []
    end.

transaction(Oper, Lock, SelfRef, Fun) ->
    chronicle_compat:txn(
      fun (Txn) ->
              Snapshot = chronicle_compat:txn_get_many(
                           [operation_key(), failover_opaque_key()], Txn),
              case maps:find(failover_opaque_key(), Snapshot) of
                  {ok, {V, _Rev}} ->
                      ?log_info("Unfinished failover ~p is detected.", [V]),
                      {abort, unfinished_failover};
                  _ ->
                      case maps:find(operation_key(), Snapshot) of
                          {ok, {{AnotherOper, _Lock, _SelfRef}, _Rev}}
                            when AnotherOper =/= Oper ->
                              RecoveryOper = recovery_oper(AnotherOper),
                              {commit, [operation_key_set(
                                          RecoveryOper, Lock, SelfRef)],
                               {need_recovery, RecoveryOper}};
                          _ ->
                              case Fun(Txn) of
                                  {commit, Sets} ->
                                      {commit,
                                       [operation_key_set(Oper, Lock, SelfRef)
                                        | Sets]};
                                  {abort, Error} ->
                                      {abort, Error}
                              end
                      end
              end
      end, #{read_consistency => quorum}).

transaction_with_key_remove(Key, Verify, Do) ->
    chronicle_compat:txn(
      fun (Txn) ->
              Value = case chronicle_compat:txn_get(Key, Txn) of
                          {ok, {V, _}} ->
                              V;
                          {error, not_found} ->
                              undefined
                      end,
              case Verify(Value) of
                  true ->
                      case Do(Txn) of
                          {commit, Sets} ->
                              {commit, [{delete, Key} | Sets]};
                          {abort, Error} ->
                              {abort, Error}
                      end;
                  false ->
                      ?log_warning("Incompatible value for key ~p found: ~p",
                                   [Key, Value]),
                      {abort, {incompatible_key_value, Key, Value}}
              end
      end).

remove_oper_key(Lock) ->
    ?log_debug("Removing operation key with lock ~p", [Lock]),
    {ok, _} =
        transaction_with_key_remove(
          operation_key(), fun({_, L, _}) -> L =:= Lock end,
          fun (_) -> {commit, []} end).

handle_kv_oper({add_replica, Node, GroupUUID, Services}, Transaction) ->
    ns_cluster_membership:add_node(Node, GroupUUID, Services, Transaction);
handle_kv_oper({remove_peer, Node}, Transaction) ->
    ns_cluster_membership:remove_nodes([Node], Transaction);
handle_kv_oper({activate_nodes, Nodes}, Transaction) ->
    ns_cluster_membership:activate(Nodes, Transaction);
handle_kv_oper({deactivate_nodes, Nodes}, Transaction) ->
    ns_cluster_membership:deactivate(Nodes, Transaction).

handle_topology_oper({add_replica, Node, _, _}, Lock) ->
    case chronicle:add_replica(Lock, Node) of
        {error, {already_member, Node, replica}} ->
            ?log_debug("Node ~p is already a member.", [Node]);
        ok ->
            ok
    end,
    ClusterInfo = chronicle:get_cluster_info(),
    ?log_debug("Cluster info: ~p", [ClusterInfo]),
    {ok, ClusterInfo};
handle_topology_oper({remove_peer, Node}, Lock) ->
    case chronicle:remove_peer(Lock, Node) of
        ok ->
            ok;
        {not_member, _} ->
            ?log_debug("Node ~p is not a member", [Node]),
            ok
    end;
handle_topology_oper({activate_nodes, Nodes}, Lock) ->
    set_peer_roles(Lock, Nodes, voter);
handle_topology_oper({deactivate_nodes, Nodes}, Lock) ->
    set_peer_roles(Lock, Nodes, replica).

handle_oper(Oper, Lock, SelfRef) ->
    ?log_debug("Starting kv operation ~p with lock ~p", [Oper, Lock]),
    case handle_kv_oper(Oper, transaction(Oper, Lock, SelfRef, _)) of
        {ok, _} ->
            ?log_debug("Starting topology operation ~p with lock ~p",
                       [Oper, Lock]),
            RV = handle_topology_oper(Oper, Lock),
            remove_oper_key(Lock),
            RV;
        {ok, _, {need_recovery, RecoveryOper}} ->
            ?log_debug("Recovery is needed for operation ~p", [RecoveryOper]),
            ok = handle_oper(RecoveryOper, Lock, SelfRef),
            handle_oper(Oper, Lock, SelfRef);
        Error ->
            Error
    end.

recovery_oper({add_replica, Node, _, _}) ->
    {remove_peer, Node};
recovery_oper(Oper) ->
    Oper.

cancel_janitor_timer(#state{janitor_timer_ref = Ref} = State) ->
    Ref =/= undefined andalso erlang:cancel_timer(Ref),
    misc:flush(janitor),
    misc:flush(arm_janitor_timer),
    State#state{janitor_timer_ref = undefined}.

arm_janitor_timer(State) ->
    NewState = cancel_janitor_timer(State),
    NewRef = erlang:send_after(?JANITOR_TIMEOUT, self(), janitor),
    NewState#state{janitor_timer_ref = NewRef}.

schedule_after_upgrade_cleanup(Self) ->
    erlang:send_after(?AFTER_UPGRADE_CLEANUP_TIMEOUT, Self,
                      after_upgrade_cleanup).

subscribe_to_chronicle_events(SelfRef) ->
    Self = self(),
    OperationKey = operation_key(),
    ns_pubsub:subscribe_link(
      chronicle_kv:event_manager(kv),
      fun ({{key, Key}, _, {updated, {_, _, Ref}}} = Evt)
            when Key =:= OperationKey,
                 Ref =/= SelfRef ->
              ?log_debug("Detected update on operation key: ~p. "
                         "Scheduling janitor.", [Evt]),
              Self ! arm_janitor_timer;
          (_) ->
              ok
      end).
