%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(service_rebalancer).

-include("cut.hrl").
-include("ns_common.hrl").

-export([spawn_monitor_rebalance/5, spawn_monitor_failover/2]).

-record(state, { parent :: pid(),
                 rebalancer :: pid(),
                 service :: service(),
                 type :: failover | rebalance,
                 keep_nodes :: [node()],
                 eject_nodes :: [node()],
                 delta_nodes :: [node()],
                 all_nodes :: [node()],
                 progress_callback :: fun ((dict:dict()) -> any())}).

spawn_monitor_rebalance(Service, KeepNodes,
                        EjectNodes, DeltaNodes, ProgressCallback) ->
    spawn_monitor(Service, rebalance, KeepNodes,
                  EjectNodes, DeltaNodes, ProgressCallback).

spawn_monitor_failover(Service, KeepNodes) ->
    ProgressCallback = fun (_) -> ok end,

    spawn_monitor(Service, failover, KeepNodes, [], [], ProgressCallback).

spawn_monitor(Service, Type, KeepNodes,
              EjectNodes, DeltaNodes, ProgressCallback) ->
    Parent = self(),

    misc:spawn_monitor(
      fun () ->
              State = #state{parent = Parent,
                             rebalancer = self(),
                             service = Service,
                             type = Type,
                             keep_nodes = KeepNodes,
                             eject_nodes = EjectNodes,
                             delta_nodes = DeltaNodes,
                             all_nodes = KeepNodes ++ EjectNodes,
                             progress_callback = ProgressCallback},
              run_rebalance(State)
      end).

run_rebalance(#state{parent = Parent} = State) ->
    erlang:register(name(State), self()),
    erlang:monitor(process, Parent),

    Agents = wait_for_agents(State),
    lists:foreach(
      fun ({_Node, Agent}) ->
              erlang:monitor(process, Agent)
      end, Agents),

    set_rebalancer(State),
    case run_rebalance_worker(State) of
        ok ->
            %% Only unset the rebalancer when everything went
            %% smoothly. Otherwise, the cleanup will happen
            %% asynchronously.
            unset_rebalancer(State);
        {error, Reason} ->
            exit(Reason)
    end.

wait_for_agents(#state{type = Type,
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
    10000.

set_rebalancer(#state{service = Service,
                      all_nodes = AllNodes,
                      rebalancer = Rebalancer}) ->
    ok = service_agent:set_rebalancer(Service, AllNodes, Rebalancer).

unset_rebalancer(#state{service = Service,
                        all_nodes = AllNodes,
                        rebalancer = Rebalancer}) ->
    case service_agent:unset_rebalancer(Service, AllNodes, Rebalancer) of
        ok ->
            ok;
        Other ->
            ?log_warning("Failed to unset "
                         "rebalancer on some nodes:~n~p", [Other])
    end.

run_rebalance_worker(#state{parent = Parent} = State) ->
    {_, false} = process_info(self(), trap_exit),

    misc:with_trap_exit(
      fun () ->
              Worker = proc_lib:spawn_link(?cut(rebalance_worker(State))),
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
                      ?log_error("Agent terminated during the rebalance: ~p",
                                 [Down]),
                      misc:unlink_terminate_and_wait(Worker, shutdown),
                      {error, {agent_died, Agent, Reason}}
              end
      end).

rebalance_worker(#state{type = Type,
                        service = Service,
                        all_nodes = AllNodes,
                        keep_nodes = KeepNodes,
                        eject_nodes = EjectNodes,
                        delta_nodes = DeltaNodes,
                        rebalancer = Rebalancer} = State) ->
    erlang:register(worker_name(State), self()),

    Id = couch_uuids:random(),
    ?rebalance_info("Rebalancing service ~p with id ~p."
                    "~nKeepNodes: ~p~nEjectNodes: ~p~nDeltaNodes: ~p",
                    [Service, Id, KeepNodes, EjectNodes, DeltaNodes]),

    {ok, NodeInfos} = service_agent:get_node_infos(Service,
                                                   AllNodes, Rebalancer),
    ?log_debug("Got node infos:~n~p", [NodeInfos]),

    {KeepNodesArg, EjectNodesArg} = build_rebalance_args(KeepNodes, EjectNodes,
                                                         DeltaNodes, NodeInfos),

    ok = service_agent:prepare_rebalance(Service, AllNodes, Rebalancer,
                                         Id, Type, KeepNodesArg, EjectNodesArg),

    Leader = pick_leader(NodeInfos, KeepNodes),
    ?log_debug("Using node ~p as a leader", [Leader]),

    ok = service_agent:start_rebalance(Service, Leader, Rebalancer,
                                       Id, Type, KeepNodesArg, EjectNodesArg),
    wait_for_rebalance_completion(State).

wait_for_rebalance_completion(#state{service = Service} = State) ->
    Timeout = ?get_timeout({rebalance, Service}, 10 * 60 * 1000),
    wait_for_rebalance_completion_loop(Timeout, State).

wait_for_rebalance_completion_loop(Timeout, State) ->
    receive
        {rebalance_progress, Progress} ->
            report_progress(Progress, State),
            wait_for_rebalance_completion_loop(Timeout, State);
        {rebalance_failed, Error} ->
            exit({rebalance_failed, {service_error, Error}});
        rebalance_done ->
            ok
    after
        Timeout ->
            exit({rebalance_failed, inactivity_timeout})
    end.

report_progress(Progress, #state{all_nodes = AllNodes,
                                 progress_callback = Callback}) ->
    D = dict:from_list([{N, Progress} || N <- AllNodes]),
    Callback(D).

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
