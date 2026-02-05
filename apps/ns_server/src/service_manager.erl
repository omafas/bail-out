%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(service_manager).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").
-include("bucket_hibernation.hrl").

-export([rebalance/6,
         failover/3,
         pause_bucket/6,
         resume_bucket/7]).

-record(rebalance_args, {keep_nodes = [] :: [node()],
                         eject_nodes = [] :: [node()],
                         delta_nodes = [] :: [node()]}).

%% TODO: These default timeouts are a function of the blobStorage
%% upload/download speeds and the size of the data - therefore needs
%% re-evaluation.

-define(PAUSE_BUCKET_TIMEOUT, infinity).
-define(RESUME_BUCKET_TIMEOUT, infinity).

-record(state, { parent :: pid(),
                 service_manager :: pid(),
                 service :: service(),
                 all_nodes :: [node()],
                 op_type :: failover | rebalance | pause_bucket | resume_bucket,
                 op_body :: function(),
                 op_args :: #rebalance_args{} |
                            {#bucket_hibernation_op_args{}, tuple()},
                 dynamic_topology = false :: boolean(),
                 progress_callback :: fun ((dict:dict()) -> any())}).

rebalance(Service, KeepNodes, EjectNodes, DeltaNodes, ProgressCallback, Opts) ->
    with_trap_exit_spawn_monitor(Service, rebalance, KeepNodes ++ EjectNodes,
                                 fun rebalance_op/5,
                                 #rebalance_args{
                                    keep_nodes = KeepNodes,
                                    eject_nodes = EjectNodes,
                                    delta_nodes = DeltaNodes},
                                 ProgressCallback, Opts).

failover(Service, KeepNodes, Opts) ->
    with_trap_exit_spawn_monitor(Service, failover, KeepNodes,
                                 fun rebalance_op/5,
                                 #rebalance_args{keep_nodes = KeepNodes},
                                 fun (_) -> ok end, Opts).

pause_bucket(Service, Args, Snapshot, Nodes, ProgressCallback, Opts) ->
    OpBody =
        case Service of
            kv ->
                fun kv_pause_bucket_op/2;
            _ ->
                fun pause_bucket_op/5
        end,

    with_trap_exit_spawn_monitor(Service, pause_bucket, Nodes,
                                 OpBody,
                                 {Args, {Snapshot}},
                                 ProgressCallback, Opts).

resume_bucket(Service, Args, DryRun, ServerMapping, Nodes, ProgressCallback,
              Opts) ->
    OpBody =
        case Service of
            kv ->
                fun kv_resume_bucket_op/2;
            _ ->
                fun resume_bucket_op/5
        end,

    with_trap_exit_spawn_monitor(Service, resume_bucket, Nodes,
                                 OpBody,
                                 {Args, {DryRun, ServerMapping}},
                                 ProgressCallback, Opts).

with_trap_exit_spawn_monitor(
  Service, Op, AllNodes, OpBody, OpArgs, ProgressCallback, Opts) ->
    Parent = self(),

    Timeout = maps:get(timeout, Opts, infinity),

    misc:with_trap_exit(
      fun () ->
              {Pid, MRef} =
                  misc:spawn_monitor(
                    fun () ->
                            run_op(
                              #state{parent = Parent,
                                     service_manager = self(),
                                     service = Service,
                                     op_type = Op,
                                     all_nodes = AllNodes,
                                     op_body = OpBody,
                                     op_args = OpArgs,
                                     progress_callback = ProgressCallback})
                    end),
              wait_for_op(Pid, MRef, Service, Op, Timeout, ok)
      end).

wait_for_op(Pid, MRef, Service, Op, Timeout, Result) ->
    receive
        {op_result, NewResult} ->
            wait_for_op(Pid, MRef, Service, Op, Timeout, NewResult);
        {'EXIT', _Pid, Reason} = Exit ->
            ?log_debug("Got an exit signal while running op: ~p "
                       "for service: ~p. Exit message: ~p",
                       [Op, Service, Exit]),
            misc:terminate_and_wait(Pid, Reason),
            exit(Reason);
        {'DOWN', MRef, _, _, normal} ->
            Result;
        {'DOWN', MRef, _, _, Reason} ->
            FailedAtom =
                list_to_atom("service_" ++
                             atom_to_list(Op) ++ "_failed"),
            exit({FailedAtom, Service, Reason})
    after
        Timeout ->
            misc:terminate_and_wait(Pid, shutdown),
            TimeoutAtom = list_to_atom("service_" ++
                                       atom_to_list(Op) ++
                                       "_timeout"),
            {error, {TimeoutAtom, Service}}
    end.

run_op(#state{parent = Parent} = State) ->
    erlang:register(name(State), self()),
    erlang:monitor(process, Parent),

    Agents = wait_for_agents(State),
    lists:foreach(
      fun ({_Node, Agent}) ->
              erlang:monitor(process, Agent)
      end, Agents),

    set_service_manager(State),
    case run_op_worker(State) of
        ok ->
            %% Only unset the service_manager when everything went
            %% smoothly. Otherwise, the cleanup will happen
            %% asynchronously.
            unset_service_manager(State);
        {error, Reason} ->
            exit(Reason)
    end.

wait_for_agents(#state{service = kv,
                       all_nodes = Nodes}) ->
    {ok, Agents} = kv_hibernation_agent:get_agents(Nodes),
    Agents;
wait_for_agents(#state{op_type = Type,
                       service = Service,
                       all_nodes = AllNodes}) ->
    Timeout = wait_for_agents_timeout(Type),
    {ok, Agents} = service_agent:wait_for_agents(Service, AllNodes, Timeout),
    Agents.

wait_for_agents_timeout(Type) ->
    Default = wait_for_agents_default_timeout(Type),
    ?get_timeout({wait_for_agent, Type}, Default).

wait_for_agents_default_timeout(rebalance) ->
    60000;
wait_for_agents_default_timeout(failover) ->
    10000;
wait_for_agents_default_timeout(pause_bucket) ->
    10000;
wait_for_agents_default_timeout(resume_bucket) ->
    10000.

set_service_manager(#state{service = kv,
                           all_nodes = Nodes,
                           service_manager = Manager}) ->
    ok = kv_hibernation_agent:set_service_manager(Nodes, Manager);
set_service_manager(#state{service = Service,
                           all_nodes = AllNodes,
                           service_manager = Manager}) ->
    ok = service_agent:set_service_manager(Service, AllNodes, Manager).

unset_service_manager_inner(#state{service = kv,
                                   all_nodes = Nodes,
                                   service_manager = Manager}) ->
    kv_hibernation_agent:unset_service_manager(Nodes, Manager);
unset_service_manager_inner(#state{service = Service,
                                   all_nodes = Nodes,
                                   service_manager = Manager}) ->
    service_agent:unset_service_manager(Service, Nodes, Manager).

unset_service_manager(#state{service = Service} = State) ->
    case unset_service_manager_inner(State) of
        ok ->
            ok;
        Other ->
            ?log_warning("Failed to unset service_manager (~p) "
                         "on some nodes:~n~p", [Service, Other])
    end.

run_op_worker(#state{parent = Parent, op_type = Type} = State) ->
    {_, false} = process_info(self(), trap_exit),

    misc:with_trap_exit(
      fun () ->
              Worker = proc_lib:spawn_link(?cut(do_run_op(State))),
              receive
                  {'EXIT', Worker, normal} ->
                      ?log_debug("Worker terminated normally"),
                      ok;
                  {'EXIT', Worker, _Reason} = Exit ->
                      ?log_error("Worker terminated abnormally: ~p", [Exit]),
                      {error, {worker_died, Exit}};
                  {'EXIT', Parent, Reason} = Exit ->
                      ?log_error("Got exit message from parent: ~p", [Exit]),
                      misc:unlink_terminate_and_wait(Worker, shutdown),
                      {error, Reason};
                  {'DOWN', _, _, Parent, Reason} = Down ->
                      ?log_error("Parent died unexpectedly: ~p", [Down]),
                      misc:unlink_terminate_and_wait(Worker, shutdown),
                      {error, {parent_died, Parent, Reason}};
                  {'DOWN', _, _, Agent, Reason} = Down ->
                      ?log_error("Agent terminated during op: ~p, ~p",
                                 [Type, Down]),
                      misc:unlink_terminate_and_wait(Worker, shutdown),
                      {error, {agent_died, Agent, Reason}}
              end
      end).

do_run_op(#state{service = kv} = State) ->
    do_run_kv_op(State);
do_run_op(#state{op_body = OpBody,
                 op_args = OpArgs,
                 service = Service,
                 all_nodes = AllNodes,
                 service_manager = Manager} = State) ->
    erlang:register(worker_name(State), self()),

    Id = couch_uuids:random(),

    {ok, NodeInfos} = service_agent:get_node_infos(Service,
                                                   AllNodes, Manager),
    ?log_debug("Got node infos:~n~p", [NodeInfos]),

    LeaderCandidates = leader_candidates(State),
    Leader = pick_leader(NodeInfos, LeaderCandidates),

    ?log_debug("Using node ~p as a leader", [Leader]),

    NewState = OpBody(State, OpArgs, Id, Leader, NodeInfos),
    wait_for_task_completion(NewState).

wait_for_task_completion(#state{service = Service, op_type = Type} = State) ->
    Timeout = ?get_timeout({Type, Service}, 10 * 60 * 1000),
    wait_for_task_completion_loop(Timeout, State).

wait_for_task_completion_loop(Timeout, #state{op_type = Type} = State) ->
    receive
        {new_topology, Nodes} ->
            maybe_update_service_map(Nodes, State),
            wait_for_task_completion_loop(Timeout, State);
        {task_progress, Progress} ->
            report_progress(Progress, State),
            wait_for_task_completion_loop(Timeout, State);
        {task_failed, Error} ->
            exit({task_failed, Type, {service_error, Error}});
        task_done ->
            ok;
        %% We would receive the following messages from the service-agent
        %% running on the leader, if a node with version less than
        %% ?VERSION_76 is picked as the leader for the service rebalance.
        {rebalance_progress, Progress} ->
            report_progress(Progress, State),
            wait_for_task_completion_loop(Timeout, State);
        {rebalance_failed, Error} ->
            exit({task_failed, Type, {service_error, Error}});
        rebalance_done ->
            ok
    after
        Timeout ->
            exit({task_failed, Type, inactivity_timeout})
    end.

maybe_update_service_map(Nodes, #state{service = Service,
                                       all_nodes = AllNodes,
                                       dynamic_topology = true}) ->
    case Nodes -- AllNodes of
        [] ->
            ns_cluster_membership:update_service_map(Service, Nodes);
        ExtraNodes ->
            ?log_error("Service ~p is trying to include unknown nodes ~p "
                       "into the service map", [Service, ExtraNodes])
    end;
maybe_update_service_map(_, _) ->
    ok.

report_progress(Progress, #state{all_nodes = AllNodes,
                                 progress_callback = Callback}) ->
    D = dict:from_list([{N, Progress} || N <- AllNodes]),
    Callback(D).

leader_candidates(#state{op_type = Op,
                         op_args = OpArgs}) when Op =:= rebalance;
                                                 Op =:= failover ->
    #rebalance_args{keep_nodes = Nodes} = OpArgs,
    Nodes;
leader_candidates(#state{op_type = Op,
                         all_nodes = Nodes}) when Op =:= pause_bucket;
                                                  Op =:= resume_bucket ->
    Nodes.

rebalance_op(#state{
                op_type = Type,
                service = Service,
                all_nodes = AllNodes,
                service_manager = Manager} = State,
             #rebalance_args{
                keep_nodes = KeepNodes,
                eject_nodes = EjectNodes,
                delta_nodes = DeltaNodes}, Id, Leader, NodeInfos) ->

    DynamicTopology = case service_agent:get_params(Service, Leader) of
                          {ok, Params} ->
                              proplists:get_value(<<"dynamicTopology">>, Params,
                                                  false);
                          _ ->
                              false
                      end,

    ?rebalance_info("Rebalancing service ~p with id ~p."
                    "~nKeepNodes: ~p~nEjectNodes: ~p~nDeltaNodes: ~p"
                    "~nDynamicTopology: ~p",
                    [Service, Id, KeepNodes, EjectNodes, DeltaNodes,
                     DynamicTopology]),

    case DynamicTopology of
        true ->
            ok;
        false ->
            {ok, _} =
                ns_cluster_membership:update_service_map(Service, AllNodes)
    end,

    {KeepNodesArg, EjectNodesArg} = build_rebalance_args(KeepNodes, EjectNodes,
                                                         DeltaNodes, NodeInfos),

    ok = service_agent:prepare_rebalance(Service, AllNodes, Manager,
                                         Id, Type, KeepNodesArg, EjectNodesArg),

    ok = service_agent:start_rebalance(Service, Leader, Manager,
                                       Id, Type, KeepNodesArg, EjectNodesArg),
    State#state{dynamic_topology = DynamicTopology}.

pause_bucket_op(#state{service = Service,
                       all_nodes = Nodes,
                       service_manager = Manager} = State,
                {#bucket_hibernation_op_args{} = Args, _ExtraArgs},
                Id, Leader, _NodesInfo) ->
    ok = testconditions:check_test_condition({pause_bucket, Service}),
    ok = service_agent:prepare_pause_bucket(Service, Nodes, Id, Args, Manager),
    ok = service_agent:pause_bucket(Service, Leader, Id, Args, Manager),
    State.

resume_bucket_op(#state{service = Service,
                        all_nodes = Nodes,
                        service_manager = Manager} = State,
                 {#bucket_hibernation_op_args{} = Args,
                  {DryRun, _ServerMapping}},
                 Id, Leader, _NodesInfo) ->
    ok = testconditions:check_test_condition({{resume_bucket, DryRun},
                                                 Service}),
    ok = service_agent:prepare_resume_bucket(
           Service, Nodes, Id, Args, DryRun, Manager),
    ok = service_agent:resume_bucket(
           Service, Leader, Id, Args, DryRun, Manager),
    State.

build_rebalance_args(KeepNodes, EjectNodes, DeltaNodes0, NodeInfos0) ->
    NodeInfos = dict:from_list(NodeInfos0),
    DeltaNodes = sets:from_list(DeltaNodes0),

    KeepNodesArg =
        lists:map(
          fun (Node) ->
                  NodeInfo = dict:fetch(Node, NodeInfos),
                  RecoveryType =
                      case sets:is_element(Node, DeltaNodes) of
                          true ->
                              delta;
                          false ->
                              full
                      end,
                  {NodeInfo, RecoveryType}
          end, KeepNodes),

    EjectNodesArg = [dict:fetch(Node, NodeInfos) || Node <- EjectNodes],

    {KeepNodesArg, EjectNodesArg}.

worker_name(#state{service = Service}) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ atom_to_list(Service) ++ "-worker").

name(#state{service = Service}) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ atom_to_list(Service)).

pick_leader(NodeInfos, KeepNodes) ->
    Master = node(),
    {Leader, _} =
        misc:min_by(
          fun ({NodeLeft, InfoLeft}, {NodeRight, InfoRight}) ->
                  {_, PrioLeft} = lists:keyfind(priority, 1, InfoLeft),
                  {_, PrioRight} = lists:keyfind(priority, 1, InfoRight),
                  KeepLeft = lists:member(NodeLeft, KeepNodes),
                  KeepRight = lists:member(NodeRight, KeepNodes),

                  {PrioLeft, KeepLeft, NodeLeft =:= Master} >
                      {PrioRight, KeepRight, NodeRight =:= Master}
          end, NodeInfos),

    Leader.

do_run_kv_op(#state{
                all_nodes = Nodes,
                op_type = Op,
                op_body = OpBody,
                op_args = OpArgs} = State) ->
    ?log_debug("Starting kv hibernation operation: ~p, on Nodes: ~p",
               [Op, Nodes]),

    OpBody(State, OpArgs).

kv_pause_bucket_op(#state{all_nodes = Nodes,
                          service_manager = Manager} = State,
                   {Args, {Snapshot}}) ->
    ok = hibernation_utils:upload_metadata_to_s3(Args, Snapshot),
    hibernation_utils:run_hibernation_op(
      fun (Node) ->
              ok = kv_hibernation_agent:pause_bucket(Args, Node, Manager)
      end, Nodes, ?PAUSE_BUCKET_TIMEOUT),
    State.

kv_resume_bucket_op(#state{all_nodes = Nodes,
                           service_manager = Manager} = State,
                    {#bucket_hibernation_op_args{
                        remote_path = RemotePath} = Args,
                     {_DryRun, ServerMapping}}) ->
    hibernation_utils:run_hibernation_op(
      fun (Node) ->
              {ok, OldNodeName} = maps:find(Node, ServerMapping),
              SourceRemotePath = hibernation_utils:get_node_data_remote_path(
                                   RemotePath, OldNodeName),
              ok = kv_hibernation_agent:resume_bucket(
                     Args#bucket_hibernation_op_args{
                       remote_path = SourceRemotePath}, Node, Manager)
      end, Nodes, ?RESUME_BUCKET_TIMEOUT),
    State.
