%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-2019 Couchbase, Inc.
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
%%
-module(auto_failover_logic).

-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([process_frame/5,
         init_state/1,
         service_failover_min_node_count/1]).

%% number of frames where node that we think is down needs to be down
%% _alone_ in order to trigger autofailover
-define(DOWN_GRACE_PERIOD, 2).

%% Auto-failover is possible for a service only if the number of nodes
%% in the cluster running that service is greater than the count specified
%% below.
%% E.g. to auto-failover kv (data) service, cluster needs atleast 3 data nodes.
-define(AUTO_FAILOVER_KV_NODE_COUNT, 2).
-define(AUTO_FAILOVER_INDEX_NODE_COUNT, 1).
-define(AUTO_FAILOVER_N1QL_NODE_COUNT, 1).
-define(AUTO_FAILOVER_FTS_NODE_COUNT, 1).
-define(AUTO_FAILOVER_EVENTING_NODE_COUNT, 1).
-define(AUTO_FAILOVER_CBAS_NODE_COUNT, 1).

-record(node_state, {
          name :: term(),
          down_counter = 0 :: non_neg_integer(),
          state :: removed|new|half_down|nearly_down|failover|up,
          %% Whether are down_warning for this node was already
          %% mailed or not
          mailed_down_warning = false :: boolean()
         }).

-record(service_state, {
          name :: term(),
          %% List of nodes running this service when the "too small cluster"
          %% event was generated.
          mailed_too_small_cluster = nil :: nil | list(),
          %% Have we already logged the auto_failover_disabled message
          %% for this service?
          logged_auto_failover_disabled = false :: boolean()
         }).

-record(down_group_state, {
          name :: term(),
          down_counter = 0 :: non_neg_integer(),
          state :: nil|nearly_down|failover
         }).

-record(state, {
          nodes_states :: [#node_state{}],
          services_state :: [#service_state{}],
          down_server_group_state :: #down_group_state{},
          down_threshold :: pos_integer()
         }).

init_state(DownThreshold) ->
    init_state(DownThreshold, cluster_compat_mode:get_compat_version()).

init_state(DownThreshold, CompatVersion) ->
    %% When the auto-failover timeout value is small the skew among the
    %% various monitors checking state is a significant portion of the
    %% overall timeout value.  Because of this we allow an extra second
    %% to mitigate the skew.
    AdjustedDownThreshold = DownThreshold - ?DOWN_GRACE_PERIOD,
    #state{nodes_states = [],
           services_state = init_services_state(CompatVersion),
           down_server_group_state = init_down_group_state(),
           down_threshold = AdjustedDownThreshold}.

init_services_state(CompatVersion) ->
    lists:map(
      fun (Service) ->
              #service_state{name = Service,
                             mailed_too_small_cluster = nil,
                             logged_auto_failover_disabled = false}
      end, ns_cluster_membership:supported_services_for_version(CompatVersion)).

init_down_group_state() ->
    #down_group_state{name = nil, down_counter = 0, state = nil}.

fold_matching_nodes([], NodeStates, Fun, Acc) ->
    lists:foldl(fun (S, A) ->
                        Fun(S#node_state{state = removed}, A)
                end, Acc, NodeStates);
fold_matching_nodes([Node | RestNodes], [], Fun, Acc) ->
    NewAcc = Fun(#node_state{name = Node,
                             state = new},
                 Acc),
    fold_matching_nodes(RestNodes, [], Fun, NewAcc);
fold_matching_nodes([Node | RestNodes] = AllNodes,
                    [#node_state{name = Name} = NodeState | RestStates] = States,
                    Fun, Acc) ->
    case Node < Name of
        true ->
            NewAcc = Fun(#node_state{name = Node,
                                     state = new},
                         Acc),
            fold_matching_nodes(RestNodes, States, Fun, NewAcc);
        false ->
            case Node =:= Name of
                false ->
                    NewAcc = Fun(NodeState#node_state{state = removed}, Acc),
                    fold_matching_nodes(AllNodes, RestStates, Fun, NewAcc);
                _ ->
                    NewAcc = Fun(NodeState, Acc),
                    fold_matching_nodes(RestNodes, RestStates, Fun, NewAcc)
            end
    end.

process_down_state(NodeState, Threshold, NewState, ResetCounter) ->
    CurrCounter = NodeState#node_state.down_counter,
    NewCounter = CurrCounter + 1,
    case NewCounter >= Threshold of
        true ->
            Counter = case ResetCounter of
                          true ->
                              0;
                          false ->
                              CurrCounter
                      end,
            NodeState#node_state{down_counter = Counter, state = NewState};
        _ ->
            NodeState#node_state{down_counter = NewCounter}
    end.

increment_down_state(NodeState, DownNodes, BigState, NodesChanged) ->
    case {NodeState#node_state.state, NodesChanged} of
        {new, _} ->
            NodeState#node_state{state = half_down};
        {up, _} ->
            NodeState#node_state{state = half_down};
        {_, true} ->
            NodeState#node_state{state = half_down, down_counter = 0};
        {half_down, _} ->
            process_down_state(NodeState, BigState#state.down_threshold,
                               nearly_down, true);
        {nearly_down, _} ->
            case DownNodes of
                [_,_|_] ->
                    NodeState#node_state{down_counter = 0};
                [_] ->
                    process_down_state(NodeState, ?DOWN_GRACE_PERIOD,
                                       failover, false)
            end;
        {failover, _} ->
            NodeState
    end.

log_master_activity(#node_state{state = _Same, down_counter = _SameCounter},
                    #node_state{state = _Same, down_counter = _SameCounter}) ->
    ok;
log_master_activity(#node_state{state = Prev, name = {Node, _} = Name} = NodeState,
                    #node_state{state = New, down_counter = NewCounter} = NewState) ->
    case New of
        up ->
            false = Prev =:= up,
            ?log_debug("Transitioned node ~p state ~p -> up", [Name, Prev]);
        _ ->
            ?log_debug("Incremented down state:~n~p~n->~p", [NodeState,
                                                             NewState])
    end,
    master_activity_events:note_autofailover_node_state_change(Node, Prev,
                                                               New, NewCounter).
get_up_states(UpNodes, NodeStates) ->
    UpFun =
        fun (#node_state{state = removed}, Acc) -> Acc;
            (NodeState, Acc) ->
                NewUpState = NodeState#node_state{state = up,
                                                  down_counter = 0,
                                                  mailed_down_warning = false},
                log_master_activity(NodeState, NewUpState),
                [NewUpState | Acc]
        end,
    UpStates0 = fold_matching_nodes(UpNodes, NodeStates, UpFun, []),
    lists:reverse(UpStates0).

get_down_states(DownNodes, State, NodesChanged) ->
    DownFun =
        fun (#node_state{state = removed}, Acc) -> Acc;
            (NodeState, Acc) ->
                NewState = increment_down_state(NodeState, DownNodes,
                                                State, NodesChanged),
                log_master_activity(NodeState, NewState),
                [NewState | Acc]
        end,
    DownStates0 = fold_matching_nodes(DownNodes, State#state.nodes_states,
                                      DownFun, []),
    lists:reverse(DownStates0).

log_down_sg_state_change(OldState, Newstate) ->
    case Newstate of
        OldState ->
            ok;
        _ ->
            log_down_sg_master_activity(OldState, Newstate),
            ?log_debug("Transitioned down server group state from ~p to ~p",
                       [OldState, Newstate])
    end.

log_down_sg_master_activity(OldState, NewState) ->
    SG = case NewState#down_group_state.name of
             nil ->
                 OldState#down_group_state.name;
             Other ->
                 Other
         end,
    Prev = OldState#down_group_state.state,
    New = NewState#down_group_state.state,
    Ctr = NewState#down_group_state.down_counter,
    master_activity_events:note_autofailover_server_group_state_change(SG,
                                                                       Prev,
                                                                       New,
                                                                       Ctr).

get_down_sg_state(DownStates, DownSG, DownSgState) ->
    NewDownSgState = get_down_sg_state_inner(DownStates, DownSG, DownSgState),
    log_down_sg_state_change(DownSgState, NewDownSgState),
    NewDownSgState.

get_down_sg_state_inner(_, [], _) ->
    init_down_group_state();
get_down_sg_state_inner(DownStates, DownSG, DownSgState) ->
    Pred =
        fun (#node_state{state = nearly_down}) -> true;
            (#node_state{state = failover}) -> true;
            (_) -> false
        end,
    case lists:all(Pred, DownStates) of
        true ->
            process_group_down_state(DownSG, DownSgState);
        false ->
            init_down_group_state()
    end.

process_group_down_state(DownSG,
                         #down_group_state{name = PrevSG, down_counter = Ctr,
                                           state = State} = DownSGState) ->
    case DownSG of
        PrevSG ->
            case State of
                nearly_down ->
                    NewCtr = Ctr + 1,
                    case NewCtr >= ?DOWN_GRACE_PERIOD of
                        true ->
                            DownSGState#down_group_state{state = failover};
                        false ->
                            DownSGState#down_group_state{down_counter = NewCtr}
                    end;
                failover ->
                    DownSGState
            end;
        _ ->
            #down_group_state{name = DownSG, down_counter = 0,
                              state = nearly_down}
    end.

process_frame(Nodes, DownNodes, State, SvcConfig, DownSG) ->
    SortedNodes = ordsets:from_list(Nodes),
    SortedDownNodes = ordsets:from_list(DownNodes),

    PrevNodes = [NS#node_state.name || NS <- State#state.nodes_states],
    NodesChanged = (SortedNodes =/= ordsets:from_list(PrevNodes)),

    UpStates = get_up_states(ordsets:subtract(SortedNodes, SortedDownNodes),
                             State#state.nodes_states),
    DownStates = get_down_states(SortedDownNodes, State, NodesChanged),
    DownSGState = get_down_sg_state(DownStates, DownSG,
                                    State#state.down_server_group_state),

    {Actions, NewDownStates} = process_downs(DownStates, State, SvcConfig,
                                             DownSGState),

    NodeStates = lists:umerge(UpStates, NewDownStates),
    SvcS = update_multi_services_state(Actions, State#state.services_state),

    case Actions of
        [] ->
            ok;
        _ ->
            ?log_debug("Decided on following actions: ~p", [Actions])
    end,
    {Actions, State#state{nodes_states = NodeStates, services_state = SvcS,
                          down_server_group_state = DownSGState}}.

process_downs(DownStates, State, SvcConfig, #down_group_state{name = nil}) ->
    process_node_down(DownStates, State, SvcConfig);
process_downs(DownStates, _, _, #down_group_state{state = nearly_down}) ->
    {[], DownStates};
process_downs(DownStates, State, SvcConfig,
              #down_group_state{name = DownSG, state = failover}) ->
    {process_group_down(DownSG, DownStates, State, SvcConfig), DownStates}.

get_down_node_names(DownStates) ->
    ordsets:from_list([N || #node_state{name = {N, _UUID}} <- DownStates]).

process_group_down(SG, DownStates, State, SvcConfig) ->
    DownNodes = get_down_node_names(DownStates),
    lists:foldl(
      fun (#node_state{name = Node}, Actions) ->
              case should_failover_node(State, Node, SvcConfig, DownNodes) of
                  [{failover, Node}] ->
                      case lists:keyfind(failover_group, 1, Actions) of
                          false ->
                              [{failover_group, SG, [Node]} | Actions];
                          {failover_group, SG, Ns} ->
                              lists:keystore(failover_group, 1, Actions,
                                             {failover_group, SG, [Node | Ns]})
                      end;
                  [Action] ->
                      [Action | Actions];
                  [] ->
                      Actions
              end
      end, [], DownStates).

process_node_down([#node_state{state = failover, name = Node}] = DownStates,
                  State, SvcConfig) ->
    DownNodes = get_down_node_names(DownStates),
    {should_failover_node(State, Node, SvcConfig, DownNodes), DownStates};
process_node_down([#node_state{state = nearly_down}] = DownStates, _, _) ->
    {[], DownStates};
process_node_down(DownStates, _, _) ->
    Fun = fun (#node_state{state = nearly_down}) -> true; (_) -> false end,
    case lists:any(Fun, DownStates) of
        true ->
            process_multiple_nodes_down(DownStates);
        _ ->
            {[], DownStates}
    end.

%% Return separate events for all nodes that are down.
process_multiple_nodes_down(DownStates) ->
    {Actions, NewDownStates} =
        lists:foldl(
          fun (#node_state{state = nearly_down, name = Node,
                           mailed_down_warning = false} = S, {Warnings, DS}) ->
                  {[{mail_down_warning, Node} | Warnings],
                   [S#node_state{mailed_down_warning = true} | DS]};
              %% Warning was already sent
              (S, {Warnings, DS}) ->
                  {Warnings, [S | DS]}
          end, {[], []}, DownStates),
    {lists:reverse(Actions), lists:reverse(NewDownStates)}.

update_multi_services_state([], ServicesState) ->
    ServicesState;
update_multi_services_state([Action | Rest], ServicesState) ->
    NewServicesState = update_services_state(Action, ServicesState),
    update_multi_services_state(Rest, NewServicesState).

%% Update mailed_too_small_cluster state
%% At any time, only one node can have mail_too_small Action.
update_services_state({mail_too_small, Svc, SvcNodes, _}, ServicesState) ->
    MTSFun =
        fun (S) ->
                S#service_state{mailed_too_small_cluster = SvcNodes}
        end,
    update_services_state_inner(ServicesState, Svc, MTSFun);

%% Update mail_auto_failover_disabled state
%% At any time, only one node can have mail_auto_failover_disabled Action.
update_services_state({mail_auto_failover_disabled, Svc, _}, ServicesState) ->
    LogAFOFun =
        fun (S) ->
                S#service_state{logged_auto_failover_disabled = true}
        end,
    update_services_state_inner(ServicesState, Svc, LogAFOFun);

%% Do not update services state for other Actions
update_services_state(_, ServicesState) ->
    ServicesState.

update_services_state_inner(ServicesState, Svc, Fun) ->
    case lists:keyfind(Svc, #service_state.name, ServicesState) of
        false ->
            exit(node_running_unknown_service);
        S ->
            lists:keyreplace(Svc, #service_state.name, ServicesState, Fun(S))
    end.

%% Decide whether to failover the node based on the services running
%% on the node.
should_failover_node(State, Node, SvcConfig, DownNodes) ->
    %% Find what services are running on the node
    {NodeName, _ID} = Node,
    NodeSvc = get_node_services(NodeName, SvcConfig, []),
    %% Is this a dedicated node running only one service or collocated
    %% node running multiple services?
    case NodeSvc of
        [Service] ->
            %% Only one service running on this node, so follow its
            %% auto-failover policy.
            should_failover_service(State, SvcConfig, Service, Node,
                                    DownNodes);
        _ ->
            %% Node is running multiple services.
            should_failover_colocated_node(State, SvcConfig, NodeSvc, Node,
                                           DownNodes)
    end.

get_node_services(_, [], Acc) ->
    Acc;
get_node_services(NodeName, [ServiceInfo | Rest], Acc) ->
    {Service, {_, {nodes, NodesList}}} = ServiceInfo,
    case lists:member(NodeName, NodesList) of
        true ->
            get_node_services(NodeName, Rest, [Service | Acc]);
        false ->
            get_node_services(NodeName, Rest,  Acc)
    end.


should_failover_colocated_node(State, SvcConfig, NodeSvc, Node, DownNodes) ->
    %% Is data one of the services running on the node?
    %% If yes, then we give preference to its auto-failover policy
    %% otherwise we treat all other servcies equally.
    case lists:member(kv, NodeSvc) of
        true ->
            should_failover_service(State, SvcConfig, kv, Node, DownNodes);
        false ->
            should_failover_colocated_service(State, SvcConfig, NodeSvc, Node,
                                              DownNodes)
    end.

%% Iterate through all services running on this node and check if
%% each of those services can be failed over.
%% Auto-failover the node only if ok to auto-failover all the services running
%% on the node.
should_failover_colocated_service(_, _, [], Node, _) ->
    [{failover, Node}];
should_failover_colocated_service(State, SvcConfig, [Service | Rest], Node,
                                  DownNodes) ->
    %% OK to auto-failover this service? If yes, then go to the next one.
    case should_failover_service(State, SvcConfig, Service, Node, DownNodes) of
        [{failover, Node}] ->
            should_failover_colocated_service(State, SvcConfig, Rest, Node,
                                              DownNodes);
        Else ->
            Else
    end.

should_failover_service(State, SvcConfig, Service, Node, DownNodes) ->
    %% Check whether auto-failover is disabled for the service.
    case is_failover_disabled_for_service(SvcConfig, Service) of
        false ->
            should_failover_service_policy(State, SvcConfig, Service, Node,
                                           DownNodes);
        true ->
            ?log_debug("Auto-failover for ~p service is disabled.~n",
                       [Service]),
            LogFun =
                fun (S) ->
                        S#service_state.logged_auto_failover_disabled =:= false
                end,
            case check_if_action_needed(State#state.services_state,
                                        Service, LogFun) of
                true ->
                    [{mail_auto_failover_disabled, Service, Node}];
                false ->
                    []
            end
    end.

is_failover_disabled_for_service(SvcConfig, Service) ->
    {{disable_auto_failover, V}, _} = proplists:get_value(Service, SvcConfig),
    V.

%% Determine whether to failover the service based on
%% how many nodes in the cluster are running the same service and
%% whether that count is above the the minimum required by the service.
should_failover_service_policy(State, SvcConfig, Service, Node, DownNodes) ->
    {_, {nodes, SvcNodes0}} = proplists:get_value(Service, SvcConfig),
    SvcNodes = ordsets:subtract(ordsets:from_list(SvcNodes0), DownNodes),
    SvcNodeCount = length(SvcNodes),
    case SvcNodeCount >= service_failover_min_node_count(Service) of
        true ->
            %% doing failover
            [{failover, Node}];
        false ->
            %% Send mail_too_small only if the new set of nodes
            %% running the service do not match the list of nodes
            %% when the last time the event was generated for this
            %% service.
            MTSFun =
                fun (S) ->
                        S#service_state.mailed_too_small_cluster =/= SvcNodes
                end,
            case check_if_action_needed(State#state.services_state,
                                        Service, MTSFun) of
                false ->
                    [];
                true ->
                    [{mail_too_small, Service, SvcNodes, Node}]
            end
    end.

%% Check the existing state of services to decide if need to
%% take any action.
check_if_action_needed(ServicesState, Service, ActFun) ->
    case lists:keyfind(Service, #service_state.name, ServicesState) of
        false ->
            exit(node_running_unknown_service);
        S ->
            ActFun(S)
    end.

%% Helper to get the minimum node count.
service_failover_min_node_count(kv) ->
    ?AUTO_FAILOVER_KV_NODE_COUNT;
service_failover_min_node_count(index) ->
    ?AUTO_FAILOVER_INDEX_NODE_COUNT;
service_failover_min_node_count(n1ql) ->
    ?AUTO_FAILOVER_N1QL_NODE_COUNT;
service_failover_min_node_count(fts) ->
    ?AUTO_FAILOVER_FTS_NODE_COUNT;
service_failover_min_node_count(eventing) ->
    ?AUTO_FAILOVER_EVENTING_NODE_COUNT;
service_failover_min_node_count(cbas) ->
    ?AUTO_FAILOVER_CBAS_NODE_COUNT.


-ifdef(TEST).
service_failover_min_node_count_test() ->
    Services = ns_cluster_membership:supported_services(),
    lists:foreach(
      fun (Service) ->
              true = is_integer(service_failover_min_node_count(Service))
      end, Services).

test_init(DownThreshold) ->
    init_state(DownThreshold + ?DOWN_GRACE_PERIOD, ?LATEST_VERSION_NUM).

attach_test_uuid(Node) ->
    {Node, list_to_binary(atom_to_list(Node))}.

attach_test_uuids(Nodes) ->
    [attach_test_uuid(N) || N <- Nodes].

test_frame(Tries, Nodes, DownNodes, State) ->
    NodesWithIDs = attach_test_uuids(Nodes),
    SvcConfig = [{kv, {{disable_auto_failover, false}, {nodes, Nodes}}}],
    test_frame(Tries, [], NodesWithIDs,
               attach_test_uuids(DownNodes), State, SvcConfig).

test_frame(0, Actions, _Nodes, _DownNodes, State, _SvcConfig) ->
    {Actions, State};
test_frame(Times, Actions, Nodes, DownNodes, State, SvcConfig) ->
    ?assertEqual([], Actions),
    {NewActions, NewState} = process_frame(
                               Nodes, DownNodes, State, SvcConfig, []),
    test_frame(Times - 1, NewActions, Nodes, DownNodes, NewState, SvcConfig).

expect_no_actions({Actions, State}) ->
    ?assertEqual([], Actions),
    State.

expect_failover(Node, {Actions, State}) ->
    ?assertEqual([{failover, attach_test_uuid(Node)}], Actions),
    State.

expect_mail_down_warnings(Nodes, {Actions, State}) ->
    ?assertEqual([{mail_down_warning, N} || N <- attach_test_uuids(Nodes)],
                 Actions),
    State.

basic_kv_1_test() ->
    functools:chain(
      test_init(3),
      [?cut(expect_no_actions(test_frame(1, [a, b, c], [], _))),
       ?cut(expect_failover(b, test_frame(6, [a, b, c], [b], _)))]).

basic_kv_2_test() ->
    expect_failover(b, test_frame(7, [a, b, c], [b], test_init(4))).

min_size_test_body(Threshold) ->
    {Actions, State} = test_frame(Threshold + 3, [a, b], [b],
                                  test_init(Threshold)),
    ?assertMatch([{mail_too_small, _, _, _}], Actions),
    test_frame(30, [a, b], [b], State).

min_size_test() ->
    min_size_test_body(2),
    min_size_test_body(3),
    min_size_test_body(4).

min_size_and_increasing_test() ->
    S = expect_no_actions(min_size_test_body(2)),
    expect_failover(b, test_frame(5, [a, b, c], [b], S)).

other_down_test() ->
    Nodes = [a, b, c],
    functools:chain(
      test_init(3),
      [?cut(expect_no_actions(test_frame(5, Nodes, [b], _))),
       ?cut(expect_mail_down_warnings([b], test_frame(1, Nodes, [b, c], _))),
       ?cut(expect_failover(b, test_frame(2, Nodes, [b], _))),
       ?cut(expect_no_actions(test_frame(1, Nodes, [b, c], _))),
       ?cut(expect_failover(b, test_frame(1, Nodes, [b], _)))]).

two_down_at_same_time_test() ->
    Nodes = [a, b, c, d],
    functools:chain(
      test_init(3),
      [?cut(expect_no_actions(test_frame(3, Nodes, [b, c], _))),
       ?cut(expect_mail_down_warnings([b, c],
                                      test_frame(1, Nodes, [b, c], _)))]).

multiple_mail_down_warning_test() ->
    Nodes = [a, b, c],
    functools:chain(
      test_init(3),
      [?cut(expect_no_actions(test_frame(4, Nodes, [b], _))),
       ?cut(expect_mail_down_warnings([b], test_frame(1, Nodes, [b, c], _))),
       %% Make sure not every tick sends out a message
       ?cut(expect_no_actions(test_frame(2, Nodes, [b, c], _))),
       ?cut(expect_mail_down_warnings([c], test_frame(1, Nodes, [b, c], _)))]).

%% Test if mail_down_warning is sent again if node was up in between
mail_down_warning_down_up_down_test() ->
    Nodes = [a, b, c],
    functools:chain(
      test_init(3),
      [?cut(expect_no_actions(test_frame(4, Nodes, [b], _))),
       ?cut(expect_mail_down_warnings([b], test_frame(1, Nodes, [b, c], _))),
       %% Node is up again
       ?cut(expect_no_actions(test_frame(1, Nodes, [], _))),
       ?cut(expect_no_actions(test_frame(3, Nodes, [b], _))),
       ?cut(expect_mail_down_warnings([b], test_frame(1, Nodes, [b, c], _)))]).
-endif.
