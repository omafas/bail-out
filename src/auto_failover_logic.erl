%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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

-record(node_state, {
          name :: term(),
          down_counter = 0 :: non_neg_integer(),
          state :: new|half_down|nearly_down|failover|up,
          issued_warning = undefined :: atom()
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
    init_state(DownThreshold, cluster_compat_mode:get_compat_version(),
               cluster_compat_mode:is_enterprise()).

init_state(DownThreshold, CompatVersion, IsEnterprise) ->
    %% When the auto-failover timeout value is small the skew among the
    %% various monitors checking state is a significant portion of the
    %% overall timeout value.  Because of this we allow an extra second
    %% to mitigate the skew.
    AdjustedDownThreshold = DownThreshold - ?DOWN_GRACE_PERIOD,
    #state{nodes_states = [],
           services_state = init_services_state(CompatVersion, IsEnterprise),
           down_server_group_state = init_down_group_state(),
           down_threshold = AdjustedDownThreshold}.

init_services_state(CompatVersion, IsEnterprise) ->
    lists:map(
      fun (Service) ->
              #service_state{name = Service,
                             mailed_too_small_cluster = nil,
                             logged_auto_failover_disabled = false}
      end, ns_cluster_membership:supported_services_for_version(
             CompatVersion, IsEnterprise)).

init_down_group_state() ->
    #down_group_state{name = nil, down_counter = 0, state = nil}.

filter_node_states(Nodes, NodeStates) ->
    filter_node_states(Nodes, NodeStates, []).

filter_node_states([], _NodeStates, Acc) ->
    lists:reverse(Acc);
filter_node_states([Node | RestNodes], [], Acc) ->
    filter_node_states(RestNodes, [],
                       [#node_state{name = Node, state = new} | Acc]);
filter_node_states([Node | RestNodes] = AllNodes,
                   [#node_state{name = Name} = NodeState | RestStates] = States,
                   Acc) ->
    case Node < Name of
        true ->
            filter_node_states(RestNodes, States,
                               [#node_state{name = Node, state = new} | Acc]);
        false ->
            case Node =:= Name of
                false ->
                    filter_node_states(AllNodes, RestStates, Acc);
                _ ->
                    filter_node_states(RestNodes, RestStates, [NodeState | Acc])
            end
    end.

increment_down_state(NodeState, Threshold, NodesChanged) ->
    case {NodeState#node_state.state, NodesChanged} of
        {new, _} ->
            NodeState#node_state{state = half_down};
        {up, _} ->
            NodeState#node_state{state = half_down};
        {_, true} ->
            NodeState#node_state{state = half_down, down_counter = 0};
        {half_down, _} ->
            CurrCounter = NodeState#node_state.down_counter,
            NewCounter = CurrCounter + 1,
            case NewCounter >= Threshold of
                true ->
                    NodeState#node_state{down_counter = 0, state = nearly_down};
                false ->
                    NodeState#node_state{down_counter = NewCounter}
            end;
        _ ->
            NodeState
    end.

log_state_changes(PrevStates, NewStates) ->
    [log_master_activity(Prev, New) || {Prev, New} <- lists:zip(PrevStates,
                                                                NewStates)].

log_master_activity(#node_state{state = Same, down_counter = SameCounter},
                    #node_state{state = Same, down_counter = SameCounter}) ->
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

get_candidates(NodeStates) ->
    lists:filtermap(
      fun (#node_state{name = Name, state = State}) when State =:= nearly_down;
                                                         State =:= failover->
              {true, Name};
          (_) ->
              false
      end, NodeStates).

reset_candidates(NodeStates) ->
    lists:map(
      fun (NodeState = #node_state{state = nearly_down}) ->
              NodeState#node_state{down_counter = 0};
          (NodeState = #node_state{state = failover}) ->
              NodeState#node_state{state = nearly_down, down_counter = 0};
          (NodeState) ->
              NodeState
      end, NodeStates).

advance_candidates(NodeStates) ->
    lists:map(
      fun (NodeState = #node_state{state = nearly_down,
                                   down_counter = Counter}) ->
              case Counter + 1 of
                  ?DOWN_GRACE_PERIOD ->
                      NodeState#node_state{state = failover};
                  NewCounter ->
                      NodeState#node_state{down_counter = NewCounter}
              end;
          (NodeState) ->
              NodeState
      end, NodeStates).

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

process_frame(Nodes, DownNodes, State = #state{nodes_states = NodeStates,
                                               down_threshold = Threshold},
              SvcConfig, DownSG) ->
    SortedNodes = ordsets:from_list(Nodes),
    SortedDownNodes = ordsets:from_list(DownNodes),

    PrevNodes = [NS#node_state.name || NS <- NodeStates],
    NodesChanged = (SortedNodes =/= ordsets:from_list(PrevNodes)),

    SortedUpNodes = ordsets:subtract(SortedNodes, SortedDownNodes),
    UpStates = filter_node_states(SortedUpNodes, NodeStates),
    NewUpStates =
        [NS#node_state{state = up, down_counter = 0,
                       issued_warning = undefined} || NS <- UpStates],

    DownStates = filter_node_states(SortedDownNodes, NodeStates),

    NewDownStates = [increment_down_state(NS, Threshold, NodesChanged) ||
                        NS <- DownStates],

    %% we can promote nodes status to failover only if the group of down nodes
    %% remains stable during ?DOWN_GRACE_PERIOD and all down nodes are promoted
    %% to nearly_down status
    NewDownStates1 =
        case get_candidates(NewDownStates) of
            [] ->
                NewDownStates;
            Candidates ->
                case get_node_names_and_uuids(DownStates) of
                    Candidates ->
                        case get_candidates(NodeStates) of
                            Candidates ->
                                advance_candidates(NewDownStates);
                            OldCandidates ->
                                ?log_debug(
                                   "List of candidates changed from ~p to ~p. "
                                   "Resetting counter", [OldCandidates,
                                                         Candidates]),
                                reset_candidates(NewDownStates)
                        end;
                    Other ->
                        ?log_debug("List of auto failover candidates ~p "
                                   "doesn't match the nodes being down ~p. "
                                   "Resetting counter",
                                   [Candidates, Other]),
                        reset_candidates(NewDownStates)
                end
        end,

    log_state_changes(UpStates, NewUpStates),
    log_state_changes(DownStates, NewDownStates1),

    {{Actions, NewDownStates2}, NewState} =
        case DownSG of
            undefined ->
                {decide_on_actions(DownStates, NewDownStates1, State,
                                   SvcConfig), State};
            _ ->
                DownSGState = get_down_sg_state(
                                NewDownStates1, DownSG,
                                State#state.down_server_group_state),
                {process_with_group_failover(NewDownStates1, State, SvcConfig,
                                             DownSGState),
                 State#state{down_server_group_state = DownSGState}}
        end,

    NewNodeStates = lists:umerge(NewUpStates, NewDownStates2),
    SvcS = update_multi_services_state(Actions, NewState#state.services_state),

    case Actions of
        [] ->
            ok;
        _ ->
            ?log_debug("Decided on following actions: ~p", [Actions])
    end,
    {Actions, State#state{nodes_states = NewNodeStates, services_state = SvcS}}.

process_with_group_failover(DownStates, State, SvcConfig,
                            #down_group_state{name = nil}) ->
    decide_on_actions_pre_71(DownStates, State, SvcConfig, 1);
process_with_group_failover(DownStates, _, _,
                            #down_group_state{state = nearly_down}) ->
    {[], DownStates};
process_with_group_failover(DownStates, State, SvcConfig,
                            #down_group_state{name = DownSG,
                                              state = failover}) ->
    {process_group_down(DownSG, DownStates, State, SvcConfig), DownStates}.

get_node_names_and_uuids(States) ->
    [NameWithUUID || #node_state{name = NameWithUUID} <- States].

get_node_names(States) ->
    [N || {N, _UUID} <- get_node_names_and_uuids(States)].

process_group_down(SG, DownStates, State, SvcConfig) ->
    DownNodes = ordsets:from_list(get_node_names(DownStates)),
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

ready_for_failover(DownStates) ->
    lists:all(
      fun (#node_state{state = failover}) ->
              true;
          (_) ->
              false
      end, DownStates).

decide_on_failover(DownStates, State, SvcConfig) ->
    case ready_for_failover(DownStates) of
        true ->
            DownNodes = get_node_names_and_uuids(DownStates),
            DownNodesOrdset = ordsets:from_list(
                                get_node_names(DownStates)),
            Actions = lists:flatmap(should_failover_node(
                                      State, _, SvcConfig,
                                      DownNodesOrdset), DownNodes),
            decide_on_services_failovers(Actions, SvcConfig, DownNodes,
                                         DownStates);
        false ->
            {[], DownStates}
    end.

is_kv_node({NodeName, _UID}, SvcConfig) ->
    lists:member(kv, get_node_services(NodeName, SvcConfig)).

maybe_issue_mail_kv_not_fully_failed_over(NodeTuple, Actions, NodeStates) ->
    NodeState = lists:keyfind(NodeTuple, #node_state.name, NodeStates),
    case maybe_issue_warning(mail_kv_not_fully_failed_over, NodeState) of
        false ->
            {Actions, NodeStates};
        {Action, NewNodeState} ->
            {[Action | Actions],
             lists:keyreplace(NodeTuple, #node_state.name, NodeStates,
                              NewNodeState)}
    end.

is_non_kv_failover({failover, NodeTuple}, SvcConfig) ->
    {not is_kv_node(NodeTuple, SvcConfig), NodeTuple};
is_non_kv_failover(_, _) ->
    false.

decide_on_services_failovers(Actions, SvcConfig, DownNodes, DownStates) ->
    FailOverNodes = [N || {failover, N} <- Actions],
    NotFailedOver = DownNodes -- FailOverNodes,

    case lists:any(is_kv_node(_, SvcConfig), NotFailedOver) of
        true ->
            %% if any single KV node is not failover-able then
            %% (1) we expect all KV nodes to be non failover-able and
            %% (2) we filter out all failover actions on non-KV nodes and
            %%     replace with alert emails, if needed.
            {ReversedActions, NewDownStates} =
                lists:foldl(
                  fun (Action, {ActionsAcc, DownStatesAcc}) ->
                          case is_non_kv_failover(Action, SvcConfig) of
                              {true, NodeTuple} ->
                                  maybe_issue_mail_kv_not_fully_failed_over(
                                    NodeTuple, ActionsAcc, DownStatesAcc);
                              _ ->
                                  {[Action | ActionsAcc], DownStatesAcc}
                          end
                  end, {[], DownStates}, Actions),
            {lists:reverse(ReversedActions), NewDownStates};
        false ->
            {Actions, DownStates}
    end.

decide_on_actions_pre_71(DownStates, State, SvcConfig, MaxGroupSize) ->
    case length(DownStates) > MaxGroupSize of
        true ->
            issue_mail_down_warnings_pre_71(DownStates);
        false ->
            decide_on_failover(DownStates, State, SvcConfig)
    end.

decide_on_actions(PrevDownStates, DownStates, State, SvcConfig) ->
    case decide_on_failover(DownStates, State, SvcConfig) of
        {[], NewDownStates} ->
            issue_mail_down_warnings(PrevDownStates, NewDownStates);
        {Actions, NewDownStates} ->
            {Failovers, Other} =
                lists:partition(fun ({failover, _}) ->
                                        true;
                                    (_) ->
                                        false
                                end, Actions),
            {combine_failovers(Failovers, Other, SvcConfig),
             NewDownStates}
    end.

combine_failovers([], Other, _SvcConfig) ->
    Other;
combine_failovers(Failovers, Other, SvcConfig) ->
    FailoverNodes = [N || {failover, N} <- Failovers],
    {KV, OtherServiceNodes} =
        lists:partition(is_kv_node(_, SvcConfig), FailoverNodes),
    [{failover, KV ++ OtherServiceNodes} | Other].

maybe_issue_warning(Warning, #node_state{issued_warning = Warning}) ->
    false;
maybe_issue_warning(Warning, S = #node_state{name = Node}) ->
    {{Warning, Node}, S#node_state{issued_warning = Warning}}.

maybe_issue_warning(Warning, S, true, {Warnings, DS}) ->
    case maybe_issue_warning(Warning, S) of
        false ->
            {Warnings, [S | DS]};
        {W, NewS} ->
            {[W | Warnings], [NewS | DS]}
    end;
maybe_issue_warning(_Warning, S, false, {Warnings, DS}) ->
    {Warnings, [S | DS]}.

fold_states(Fun, List) ->
    {Actions, NewDownStates} = lists:foldl(Fun, {[], []}, List),
    {lists:reverse(Actions), lists:reverse(NewDownStates)}.

%% Return separate events for all nodes that are down.
issue_mail_down_warnings_pre_71(DownStates) ->
    fold_states(
      fun (#node_state{state = DownState} = S, Acc) ->
              maybe_issue_warning(
                mail_down_warning, S, DownState =:= nearly_down, Acc)
      end, DownStates).

should_issue_mail_down_warning(
  #node_state{state = failover}, #node_state{state = nearly_down}) ->
    true;
should_issue_mail_down_warning(
  #node_state{state = nearly_down},
  #node_state{state = nearly_down, down_counter = 0}) ->
    true;
should_issue_mail_down_warning(_, _) ->
    false.

issue_mail_down_warnings(PrevDownStates, DownStates) ->
    fold_states(
      fun ({PrevDownState, DownState}, Acc) ->
              maybe_issue_warning(
                mail_down_warning_multi_node,
                DownState,
                should_issue_mail_down_warning(PrevDownState, DownState), Acc)
      end, lists:zip(PrevDownStates, DownStates)).

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
    NodeSvc = get_node_services(NodeName, SvcConfig),
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

get_node_services(NodeName, SvcConfig) ->
    get_node_services(NodeName, SvcConfig, []).

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

service_failover_min_node_count(kv) ->
    2;
service_failover_min_node_count(_) ->
    1.

-ifdef(TEST).
service_failover_min_node_count_test() ->
    Services = ns_cluster_membership:supported_services_for_version(
                 ?LATEST_VERSION_NUM, true),
    lists:foreach(
      fun (Service) ->
              true = is_integer(service_failover_min_node_count(Service))
      end, Services).

test_init(DownThreshold) ->
    init_state(DownThreshold + ?DOWN_GRACE_PERIOD, ?LATEST_VERSION_NUM, true).

attach_test_uuid(Node) ->
    {Node, list_to_binary(atom_to_list(Node))}.

attach_test_uuids(Nodes) ->
    [attach_test_uuid(N) || N <- Nodes].

test_frame(Tries, RawSvcConfig, DownNodes, DownSG, State) ->
    SvcConfig = [{Service, {{disable_auto_failover, false},
                            {nodes, ServiceNodes}}} ||
                    {Service, ServiceNodes} <- RawSvcConfig],
    Nodes = lists:flatten([N || {_, N} <- RawSvcConfig]),
    NodesWithIDs = attach_test_uuids(Nodes),
    test_frame(Tries, [], NodesWithIDs,
               attach_test_uuids(DownNodes), State, SvcConfig, DownSG,
               [{Tries, [], State}]).

test_frame(0, Actions, _Nodes, _DownNodes, State, _SvcConfig, _DownSG,
           Report) ->
    ?log_debug("~n---------------~n~s~n----------------~n",
               [generate_report(Report, [])]),
    {Actions, State};
test_frame(Times, Actions, Nodes, DownNodes, State, SvcConfig, DownSG,
           Report) ->
    ?assertEqual([], Actions),
    {NewActions, NewState} =
        process_frame(Nodes, DownNodes, State, SvcConfig, DownSG),
    StateDiff = flatten_state(NewState) -- flatten_state(State),
    test_frame(Times - 1, NewActions, Nodes, DownNodes, NewState, SvcConfig,
               DownSG, [{Times - 1, NewActions, StateDiff} | Report]).

generate_report([], Iolist) ->
    lists:flatten(Iolist);
generate_report([{Times, Actions, State} | Rest], Iolist) ->
    generate_report(
      Rest,
      [io_lib:format("~p: ~p~n~p~n", [Times, Actions, State]),
       Iolist]).

flatten_state(#state{nodes_states = NS, services_state = SS,
                     down_server_group_state = DSGS}) ->
    NS ++ SS ++ [DSGS].

test_body(Threshold, DownSG, Steps, RawSvcConfig) ->
    lists:foldl(
      fun ({Expected, NFrames, DownNodes}, State) ->
              {Actions, NewState} = test_frame(NFrames, RawSvcConfig, DownNodes,
                                               DownSG, State),
              PreparedActions =
                  lists:map(fun ({failover, Nodes}) when is_list(Nodes) ->
                                    {failover, lists:sort(Nodes)};
                                (Other) ->
                                    Other
                            end, Actions),
              ?assertEqual(lists:sort(Expected), lists:sort(PreparedActions)),
              NewState
      end, test_init(Threshold), Steps).

get_sg(?VERSION_71) ->
    undefined;
get_sg(?VERSION_70) ->
    [].

ver_suffix(Ver) ->
    lists:flatten(io_lib:format(" Ver = ~p", [Ver])).

compare_with(_Ver, no_actions) ->
    [];
compare_with(?VERSION_71, List) when is_list(List) ->
    lists:flatten([compare_with(?VERSION_71, X) || X <- List]);
compare_with(?VERSION_71, {failover, Nodes}) ->
    [{failover, [N || N <- attach_test_uuids(Nodes)]}];
compare_with(?VERSION_70, {failover, Nodes}) ->
    [{failover, N} || N <- attach_test_uuids(Nodes)];
compare_with(?VERSION_71, {mail_too_small, Svc, SvcNodes, Node}) ->
    [{mail_too_small, Svc, SvcNodes, attach_test_uuid(Node)}];
compare_with(?VERSION_71, {mail_down_warnings, Nodes}) ->
    [{mail_down_warning_multi_node, N} || N <- attach_test_uuids(Nodes)];
compare_with(?VERSION_71, {mail_kv_not_fully_failed_over, Node}) ->
    {mail_kv_not_fully_failed_over, attach_test_uuid(Node)};
compare_with(?VERSION_70, {mail_down_warnings, Nodes}) ->
    [{mail_down_warning, N} || N <- attach_test_uuids(Nodes)].

generate(Versions, Tests) ->
    generate(Versions, [{kv, [a, b, c, d]}], Tests).

generate(Versions, RawSvcConfig, Tests) ->
    T = fun (Ver, Threshold, Steps) ->
                ?cut(test_body(Threshold, get_sg(Ver),
                               Steps, RawSvcConfig))
        end,
    [{Title ++ ver_suffix(Ver),
      T(Ver, Threshold,
        [{compare_with(Ver, CompareWith), Frames, DownNodes} ||
            {CompareWith, Frames, DownNodes} <- Steps])} ||
        {Title, Threshold, Steps} <- Tests,
        Ver <- Versions].

pre_71_process_frame_test_() ->
    generate(
      [?VERSION_70],
      [{"Two nodes down at the same time", 3,
        [{no_actions, 3, [b, c]},
         {{mail_down_warnings, [b, c]}, 1, [b, c]}]},
       {"Two nodes down at the same time with shift", 3,
        [{no_actions, 1, [b]},
         {{mail_down_warnings, [b]}, 3, [b, c]},
         {{mail_down_warnings, [c]}, 1, [b, c]}]},
       {"Multiple mail down warnings", 3,
        [{no_actions, 4, [b]},
         {{mail_down_warnings, [b]}, 1, [b, c]},
         %% Make sure not every tick sends out a message
         {no_actions, 2, [b, c]},
         {{mail_down_warnings, [c]}, 1, [b, c]}]},
       {"Test if mail_down_warning is sent again if node was up in between", 3,
        [{no_actions, 4, [b]},
         {{mail_down_warnings, [b]}, 1, [b, c]},
         {no_actions, 1, []},
         {no_actions, 3, [b]},
         {{mail_down_warnings, [b]}, 1, [b, c]}]}]).

common_process_frame_test_() ->
    generate(
      [?VERSION_70, ?VERSION_71],
      [{"Basic one node failover", 3,
        [{no_actions, 1, []},
         {{failover, [b]}, 6, [b]}]},
       {"Basic one node failover 2", 4,
        [{{failover, [b]}, 7, [b]}]},
       {"Other node down.", 3,
        [{no_actions, 5, [b]},
         {{mail_down_warnings, [b]}, 1, [b, c]},
         {{failover, [b]}, 2, [b]},
         {no_actions, 1, [b, c]},
         {{failover, [b]}, 2, [b]}]}]).

process_frame_test_() ->
    generate(
      [?VERSION_71],
      [{"Basic 2 nodes down.", 3,
        [{no_actions, 5, [b, c]},
         {{failover, [b, c]}, 1, [b, c]}]},
       {"2 nodes down, 3rd node down and then up", 3,
        [{no_actions, 5, [b, c]},
         {{mail_down_warnings, [b, c]}, 1, [b, c, d]},
         {no_actions, 1, [a, b, c]},
         {{failover, [b, c]}, 2, [b, c]}]},
       {"Two nodes down at the same time", 3,
        [{no_actions, 5, [b, c]},
         {{failover, [b, c]}, 1, [b, c]}]},
       {"Two nodes down at the same time with shift", 3,
        [{no_actions, 1, [b]},
         {{mail_down_warnings, [b]}, 4, [b, c]},
         {{failover, [b, c]}, 2, [b, c]}]},
       {"Test if mail_down_warning is sent again if node was up in between", 3,
        [{no_actions, 5, [b]},
         {{mail_down_warnings, [b]}, 1, [b, c]},
         {no_actions, 1, []},
         {no_actions, 4, [b]},
         {{mail_down_warnings, [b]}, 1, [b, c]}]}]).

multiple_services_test_() ->
    generate(
      [?VERSION_71],
      [{fts, [a1, a2]}, {n1ql, [b1, b2]}, {kv, [c1, c2, c3]},
       {index, [d1, d2]}],
      [{"3 nodes of different services are down.", 3,
        [{no_actions, 5, [a1, b1, c1]},
         {{failover, [a1, b1, c1]}, 1, [a1, b1, c1]}]},
       {"4 nodes are down, but one service is not safe to fail over", 3,
        [{no_actions, 5, [a1, b1, b2, d1]},
         {[{failover, [a1, d1]},
           {mail_too_small, n1ql, [], b1},
           {mail_too_small, n1ql, [], b2}], 1, [a1, b1, b2, d1]}]},
       {"Do not fail over services if kv cannot be failed over", 3,
        [{no_actions, 5, [a1, b1, c1, c2, d1]},
         {[{mail_kv_not_fully_failed_over, a1},
           {mail_kv_not_fully_failed_over, b1},
           {mail_kv_not_fully_failed_over, d1},
           {mail_too_small, kv, [c3], c1},
           {mail_too_small, kv, [c3], c2}], 1, [a1, b1, c1, c2, d1]},
         {no_actions, 1, [a1, b1, c1, c2, d1]},
         {{mail_down_warnings, [a1, b1, c1, d1]}, 1, [a1, b1, c1, d1]},
         {{failover, [a1, b1, c1, d1]}, 2, [a1, b1, c1, d1]}]}]).

min_size_test_() ->
    MinSizeTest =
        fun (Threshold, Ver) ->
                SvcConfig = [{kv, [a, b]}],
                {Actions, State} =
                    test_frame(Threshold + 3, SvcConfig, [b],
                               get_sg(Ver), test_init(Threshold)),
                ?assertMatch([{mail_too_small, _, _, _}], Actions),
                test_frame(30, SvcConfig, [b], get_sg(Ver), State)
        end,
    MinSizeAndIncreasing =
        fun (Ver) ->
                SvcConfig = [{kv, [a, b, c]}],
                {Actions, State} = MinSizeTest(2, Ver),
                ?assertEqual(compare_with(Ver, no_actions), Actions),
                {Actions1, _} = test_frame(5, SvcConfig, [b], get_sg(Ver),
                                           State),
                ?assertEqual(compare_with(Ver, {failover, [b]}), Actions1,
                             SvcConfig)
        end,
    [{lists:flatten(
        io_lib:format("Min size test. Threshold = ~p, Ver = ~p", [T, V])),
      ?cut(MinSizeTest(T, V))} || T <- [2, 3, 4],
                                  V <- [?VERSION_70, ?VERSION_71]] ++
        [{lists:flatten(
            io_lib:format("Min size and increasing. Ver = ~p", [V])),
          ?cut(MinSizeAndIncreasing(V))} || V <- [?VERSION_70, ?VERSION_71]].

filter_node_states_test() ->
    Test = fun (Nodes, NodesForStates) ->
                   NodeStates = [#node_state{name = N, state = up} ||
                                    N <- NodesForStates],
                   Res = filter_node_states(Nodes, NodeStates),
                   [{N, S} || #node_state{name = N, state = S} <- Res]
           end,
    ?assertEqual([{a, new}, {b, up}, {d, up}], Test([a, b, d], [b, c, d, e])),
    ?assertEqual([{b, up}, {d, up}], Test([b, d], [a, b, d, e])).

-endif.
