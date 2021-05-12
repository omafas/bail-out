%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

%%
%% @doc Auto-failover logic at a very high level:
%%  - User sets the auto-failover timeout. This is the time period for which
%%  a node or server group must be down before it is automatically failed over.
%%  - User specifies the maximum number of auto-failover events that are
%%  allowed before requiring manual intervention/reset of quota.
%%  - Whenever a node or a server group is automatically failed over, a
%%  counter is incremented by one.
%%  - Auto-failover of maximum one server group is allowed before requiring
%%  manual intervention/reset of quota. This is irrespective of the max count
%%  set by the user.
%%  - When the maximum number of nodes or server groups that can be failed over
%%  has been reached and there is another failure, user will receive
%%  appropriate notification.
%%  - The max auto-failover count applies for cascading failures only.
%%      - Cascading failures are where one node fails, it is automatically
%%      failed over then another node fails, it is automatically failed over
%%      and so on. This will continue up to the max count.
%%      - If two or more nodes fail concurrently and it is not a server group
%%      failure, then none of the nodes will be automatically failed over.
%%      This is one of the restrictions that prevents a network partition
%%      from causing two or more halves of a cluster from failing each other
%%      over.
%%  - If one ore more buckets have insufficient replicas (unsafe buckets), then
%%  the node will not be failed over. This is irresepctive of the value of
%%  max count.
%%  E.g. cluster has a bucket with one replica. User has set max count to 2.
%%  KVNode1 fails and is automatically failed over. KVNode2 fails. It's
%%  auto-failover will be attempted but validate_autofailover() will prevent
%%  the failover because of unsafe bucket.
%%  - Auto-failover of server groups is disabled by default.
%%

-module(auto_failover).

-behaviour(gen_server).

-include("cut.hrl").
-include("ns_common.hrl").
-include("ns_config.hrl").
-include("ns_heart.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0,
         get_cfg/0,
         get_cfg/1,
         upgrade_cfg/2,
         enable/3,
         disable/1,
         reset_count/0,
         reset_count_async/0,
         is_enabled/0]).

%% For email alert notificatons
-export([alert_keys/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, {via, leader_registry, ?MODULE}).

%% @doc The time a stats request to a bucket may take (in milliseconds)
-define(STATS_TIMEOUT, 2000).

%% @doc Frequency (in milliseconds) at which to check for down nodes.
-define(TICK_PERIOD, 1000).

%% @doc Minimum number of active server groups needed for server group failover
-define(MIN_ACTIVE_SERVER_GROUPS, 3).

-record(state,
        { auto_failover_logic_state,
          %% Reference to the tick timer.
          tick_ref = nil :: nil | reference(),
          %% Time a node needs to be down until it is automatically failovered
          timeout = nil :: nil | integer(),
          %% Maximum number of auto-failover events
          max_count = 0  :: non_neg_integer(),
          %% Counts the number of auto-failover events
          count = 0 :: non_neg_integer(),
          %% List of server groups that have already been automatically failed
          %% over. Currently, auto-failover of only one server group is allowed
          %% before requiring reset of the auto-failover count.
          %% This list is reset when auto-failover count is reset.
          failed_over_server_groups = [] :: list(),
          %% Whether we reported why the node is considered down
          reported_down_nodes_reason = [] :: list(),
          %% Keeps all errors that have been reported.
          reported_errors = sets:new() :: sets:set()
        }).

%%
%% API
%%

start_link() ->
    misc:start_singleton(gen_server, ?MODULE, [], []).

%% @doc Enable auto-failover. Failover after a certain time (in seconds),
%% Returns an error (and reason) if it couldn't be enabled, e.g. because
%% not all nodes in the cluster were healthy.
%% `Timeout` is the number of seconds a node must be down before it will be
%% automatically failovered
%% `Max` is the maximum number of auto-failover events that are allowed.
%% `Extras` are optional settings.
-spec enable(Timeout::integer(), Max::integer(), Extras::list()) -> ok.
enable(Timeout, Max, Extras) ->
    %% Request will be sent to the master for processing.
    %% In a mixed version cluster, node running highest version is
    %% usually selected as the master.
    call({enable_auto_failover, Timeout, Max, Extras}).

%% @doc Disable auto-failover
-spec disable(Extras::list()) -> ok.
disable(Extras) ->
    call({disable_auto_failover, Extras}).

%% @doc Reset the number of nodes that were auto-failovered to zero
-spec reset_count() -> ok.
reset_count() ->
    call(reset_auto_failover_count).

-spec reset_count_async() -> ok.
reset_count_async() ->
    cast(reset_auto_failover_count).

-spec get_cfg() -> list().
get_cfg() ->
    get_cfg(ns_config:latest()).

-spec get_cfg(ns_config()) -> list().
get_cfg(Config) ->
    {value, Cfg} = ns_config:search(Config, auto_failover_cfg),
    Cfg.

upgrade_cfg(Config, Fun) ->
    [{set, auto_failover_cfg, Fun(get_cfg(Config))}].

-spec is_enabled() -> true | false.
is_enabled() ->
    proplists:get_value(enabled, get_cfg(), false).

call(Call) ->
    misc:wait_for_global_name(?MODULE),
    gen_server:call(?SERVER, Call).

cast(Call) ->
    misc:wait_for_global_name(?MODULE),
    gen_server:cast(?SERVER, Call).

-define(log_info_and_email(Alert, Fmt, Args),
        ale:info(?USER_LOGGER, Fmt, Args),
        ns_email_alert:alert(Alert, Fmt, Args)).

%% @doc Returns a list of all alerts that might send out an email notification.
-spec alert_keys() -> [atom()].
alert_keys() ->
    [auto_failover_node,
     auto_failover_maximum_reached,
     auto_failover_other_nodes_down,
     auto_failover_cluster_too_small,
     auto_failover_disabled].

%%
%% gen_server callbacks
%%

init([]) ->
    restart_on_compat_mode_change(),

    Config = get_cfg(),
    ?log_debug("init auto_failover.", []),
    Timeout = proplists:get_value(timeout, Config),
    Count = proplists:get_value(count, Config),
    MaxCount = proplists:get_value(max_count, Config, 1),
    FailedOverSGs = proplists:get_value(failed_over_server_groups, Config, []),
    State0 = #state{timeout = Timeout,
                    max_count = MaxCount,
                    count = Count,
                    failed_over_server_groups = FailedOverSGs,
                    auto_failover_logic_state = undefined},
    State1 = init_reported(State0),
    case proplists:get_value(enabled, Config) of
        true ->
            {reply, ok, State2} = handle_call(
                                    {enable_auto_failover, Timeout, MaxCount},
                                    self(), State1),
            {ok, State2};
        false ->
            {ok, State1}
    end.

init_logic_state(Timeout) ->
    TickPeriod = get_tick_period(),
    DownThreshold = (Timeout * 1000 + TickPeriod - 1) div TickPeriod,
    auto_failover_logic:init_state(DownThreshold).

handle_call({enable_auto_failover, Timeout, Max}, From, State) ->
    handle_call({enable_auto_failover, Timeout, Max, []}, From, State);
%% @doc Auto-failover isn't enabled yet (tick_ref isn't set).
handle_call({enable_auto_failover, Timeout, Max, Extras}, _From,
            #state{tick_ref = nil} = State) ->
    ale:info(?USER_LOGGER,
             "Enabled auto-failover with timeout ~p and max count ~p",
             [Timeout, Max]),
    Ref = send_tick_msg(),
    State1 = State#state{tick_ref = Ref, timeout = Timeout, max_count = Max,
                         auto_failover_logic_state = init_logic_state(Timeout)},
    make_state_persistent(State1, Extras),
    {reply, ok, State1};
%% @doc Auto-failover is already enabled, just update the settings.
handle_call({enable_auto_failover, Timeout, Max, Extras}, _From, State) ->
    ?log_debug("updating auto-failover settings: ~p", [State]),
    State1 = update_state_timeout(Timeout, State),
    State2 = update_state_max_count(Max, State1),
    make_state_persistent(State2, Extras),
    {reply, ok, State2};

%% @doc Auto-failover is already disabled, so we don't do anything
handle_call({disable_auto_failover, _}, _From,
            #state{tick_ref = nil} = State) ->
    {reply, ok, State};
%% @doc Auto-failover is enabled, disable it
handle_call({disable_auto_failover, Extras}, _From,
            #state{tick_ref = Ref} = State) ->
    ?log_debug("disable_auto_failover: ~p", [State]),
    erlang:cancel_timer(Ref),
    misc:flush(tick),
    State2 = State#state{tick_ref = nil, auto_failover_logic_state = undefined},
    make_state_persistent(State2, Extras),
    ale:info(?USER_LOGGER, "Disabled auto-failover"),
    {reply, ok, State2};
handle_call(reset_auto_failover_count, _From, State) ->
    {noreply, NewState} = handle_cast(reset_auto_failover_count, State),
    {reply, ok, NewState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(reset_auto_failover_count, #state{count = 0} = State) ->
    {noreply, State};
handle_cast(reset_auto_failover_count, State) ->
    ?log_debug("reset auto_failover count: ~p", [State]),
    State1 = State#state{failed_over_server_groups = []},
    LogicState = init_logic_state(State1#state.timeout),
    State2 = State1#state{count = 0, auto_failover_logic_state = LogicState},
    State3 = init_reported(State2),
    make_state_persistent(State3),
    ale:info(?USER_LOGGER, "Reset auto-failover count"),
    {noreply, State3};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Check if nodes should/could be auto-failovered on every tick
handle_info(tick, State0) ->
    Ref = send_tick_msg(),
    Config = ns_config:get(),
    Snapshot = ns_cluster_membership:get_snapshot(#{ns_config => Config}),

    %% Reread autofailover count from config just in case. This value can be
    %% different, for instance, if due to network issues we get disconnected
    %% from the part of the cluster. This part of the cluster will elect new
    %% master node. Now say this new master node autofailovers some other
    %% node. Then if network issues disappear, we will connect back to the
    %% rest of the cluster. And say we win the battle over mastership
    %% again. In this case our failover count will still be zero which is
    %% incorrect.
    AFOConfig = get_cfg(Config),
    FOSGs = proplists:get_value(failed_over_server_groups, AFOConfig, []),
    State1 = State0#state{count = proplists:get_value(count, AFOConfig),
                          failed_over_server_groups = FOSGs},

    NonPendingNodes = lists:sort(ns_cluster_membership:active_nodes(Snapshot)),

    NodeStatuses = ns_doctor:get_nodes(),
    DownNodes = fastfo_down_nodes(NonPendingNodes),
    DownSG = get_down_server_group(DownNodes, Config, Snapshot,
                                   NonPendingNodes),

    State = log_down_nodes_reason(DownNodes, State1),
    CurrentlyDown = [N || {N, _, _} <- DownNodes],
    NodeUUIDs = ns_config:get_node_uuid_map(Config),

    %% Extract service specfic information from the Config
    ServicesConfig = all_services_config(Config, Snapshot),

    {Actions, LogicState} =
        auto_failover_logic:process_frame(attach_node_uuids(NonPendingNodes, NodeUUIDs),
                                          attach_node_uuids(CurrentlyDown, NodeUUIDs),
                                          State#state.auto_failover_logic_state,
                                          ServicesConfig, DownSG),
    NewState = lists:foldl(
                 fun(Action, S) ->
                         process_action(Action, S, DownNodes, NodeStatuses,
                                        Snapshot)
                 end, State#state{auto_failover_logic_state = LogicState},
                 Actions),

    NewState1 = update_reported_flags_by_actions(Actions, NewState),

    if
        NewState1#state.count =/= State#state.count ->
            make_state_persistent(NewState1);
        true -> ok
    end,

    {noreply, NewState1#state{tick_ref = Ref}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%
%% Internal functions
%%

%% The auto-fail over message reports the reason for failover.
%% But, not all events lead to auto-failover.
%% Sometimes auto-failover is not possible or the down node recovers before
%% auto-failover can be triggered. In either case, it will be good to log
%% the reason when a node is reported as down the first time. This may help
%% in triage of issues.
log_down_nodes_reason(DownNodes,
                      #state{reported_down_nodes_reason = Curr} = State) ->
    New = lists:filtermap(
            fun ({_Node, unknown, _}) ->
                    false;
                ({Node, {Reason, _}, _}) ->
                    case lists:keyfind(Node, 1, Curr) of
                        {Node, Reason} ->
                            ok;
                        _ ->
                            %% Either this is the first time the node is down
                            %% or the reason has changed.
                            ?log_debug("Node ~p is considered down. Reason:~p",
                                       [Node, Reason])
                    end,

                    {true, {Node, Reason}}
            end, DownNodes),
    State#state{reported_down_nodes_reason = New}.

update_state_timeout(Timeout, #state{timeout = CurrTimeout} = State) ->
    case Timeout =/= CurrTimeout of
        true ->
            ale:info(?USER_LOGGER, "Updating auto-failover timeout to ~p",
                     [Timeout]),
            State#state{timeout = Timeout,
                        auto_failover_logic_state = init_logic_state(Timeout)};
        false ->
            ?log_debug("No change in timeout ~p", [Timeout]),
            State
    end.

update_state_max_count(Max, #state{max_count = CurrMax} = State) ->
    case Max =/= CurrMax of
        true ->
            ale:info(?USER_LOGGER, "Updating auto-failover max count to ~p",
                     [Max]),
            State#state{max_count = Max};
        false ->
            ?log_debug("No change in max count ~p", [Max]),
            State
    end.

process_action({mail_too_small, Service, SvcNodes, {Node, _UUID}},
               S, _, _, _) ->
    ?log_info_and_email(
       auto_failover_cluster_too_small,
       "Could not auto-failover node (~p). "
       "Number of remaining nodes that are running ~s service is ~p. "
       "You need at least ~p nodes.",
       [Node,
        ns_cluster_membership:user_friendly_service_name(Service),
        length(SvcNodes),
        auto_failover_logic:service_failover_min_node_count(Service)]),
    S;
process_action({mail_down_warning, {Node, _UUID}}, S, _, _, _) ->
    ?log_info_and_email(
       auto_failover_other_nodes_down,
       "Could not auto-failover node (~p). "
       "There was at least another node down.",
       [Node]),
    S;
process_action({mail_auto_failover_disabled, Service, {Node, _}}, S, _, _, _) ->
    ?log_info_and_email(
       auto_failover_disabled,
       "Could not auto-failover node (~p). "
       "Auto-failover for ~s service is disabled.",
       [Node, ns_cluster_membership:user_friendly_service_name(Service)]),
    S;
process_action({failover, {Node, _UUID}}, S, DownNodes, NodeStatuses,
               Snapshot) ->
    SG = ns_cluster_membership:get_node_server_group(Node, Snapshot),
    case allow_failover(SG, S, failover) of
        {false, ErrMsg} ->
            case should_report(max_node_reached, S) of
                true ->
                    ?log_info_and_email(
                       auto_failover_maximum_reached,
                       "Could not auto-failover more nodes (~p). ~s",
                       [Node, ErrMsg]),
                    note_reported(max_node_reached, S);
                false ->
                    S
            end;
        {true, UpdateCount} ->
            failover_nodes([Node], S, DownNodes, NodeStatuses, UpdateCount)
    end;
process_action({failover_group, SG, Nodes0}, S, DownNodes, NodeStatuses, _) ->
    Nodes = [N || {N, _} <- Nodes0],
    case allow_failover(SG, S, failover_group) of
        {false, ErrMsg} ->
            case should_report(max_group_reached, S) of
                true ->
                    ?log_info_and_email(
                       auto_failover_maximum_reached,
                       "Could not auto-failover server group (~p) "
                       "with nodes (~p). ~s",
                       [binary_to_list(SG), Nodes, ErrMsg]),
                    note_reported(max_group_reached, S);
                false ->
                    S
            end;
        {true, UpdateCount} ->
            failover_group(Nodes, S, DownNodes, NodeStatuses, UpdateCount, SG)
    end.

failover_group(Nodes, S0, DownNodes, NodeStatuses, UpdateCount, SG) ->
    S = log_group_failover_attempt(SG, Nodes, S0),
    NewState = failover_nodes(Nodes, S, DownNodes, NodeStatuses, UpdateCount),
    case NewState#state.count =:= S#state.count of
        true ->
            NewState;
        false ->
            NewState#state{failed_over_server_groups = [SG]}
    end.

log_group_failover_attempt(SG, Nodes, State) ->
    case should_report(group_failover_attempt, State) of
        true ->
            ale:info(?USER_LOGGER,
                     "Attempting auto-failover of server group (~p) with "
                     "nodes (~p).", [binary_to_list(SG), Nodes]),
            note_reported(group_failover_attempt, State);
        false ->
            State
    end.

allow_failover(SG, #state{count = Count, max_count = Max,
                          failed_over_server_groups = FOSGs}, Action) ->
    case lists:member(SG, FOSGs) of
        true ->
            %% SG was partially failed over in the past; allow failover of
            %% rest of it's nodes even though we may have reached the quota.
            ?log_debug("Server group ~p was partially auto-failed over in "
                       "the past. Allow auto-failover of rest of it's nodes. "
                       "Auto-failover count: ~p, max_count:~p, "
                       "failed_over_server_groups:~p",
                       [binary_to_list(SG), Count, Max, FOSGs]),
            %% Continuation of an earlier server group auto-failover event.
            %% Do not update auto-failover count.
            {true, false};
        false ->
            %% Count can be greater than Max if user reduces the Max
            %% before resetting the count.
            case Count >= Max of
                true ->
                    M = io_lib:format("Maximum number of auto-failover events "
                                      "(~p) has been reached.", [Max]),
                    {false, lists:flatten(M)};
                false ->
                    case Action of
                        failover ->
                            {true, true};
                        failover_group ->
                            allow_failover_group(FOSGs)
                    end
            end
    end.

allow_failover_group([]) ->
    {true, true};
allow_failover_group(FOSGs) ->
    M = io_lib:format("Auto-failover of maximum one server group is allowed "
                      "before requiring reset of auto-failover quota. "
                      "Server group ~p was failed over in the past. Reset "
                      "quota to allow auto-failover of additional "
                      "server groups.", [FOSGs]),
    {false, lists:flatten(M)}.

failover_nodes(Nodes, S, DownNodes, NodeStatuses, UpdateCount) ->
    case ns_orchestrator:try_autofailover(Nodes) of
        ok ->
            lists:foreach(
              fun (Node) ->
                      log_failover_success(Node, DownNodes, NodeStatuses)
              end, Nodes),
            NewCount = case UpdateCount of
                           false ->
                               S#state.count;
                           true ->
                               S#state.count + 1
                       end,
            init_reported(S#state{count = NewCount});
        Error ->
            process_failover_error(Error, Nodes, S)
    end.

log_failover_success(Node, DownNodes, NodeStatuses) ->
    {_, DownInfo, _} = lists:keyfind(Node, 1, DownNodes),
    case DownInfo of
        unknown ->
            ?log_info_and_email(
               auto_failover_node,
               "Node (~p) was automatically failed over.~n~p",
               [Node, ns_doctor:get_node(Node, NodeStatuses)]);
        {Reason, MARes} ->
            MA = [atom_to_list(M) || M <- MARes],
            master_activity_events:note_autofailover_done(Node,
                                                          string:join(MA, ",")),
            ?log_info_and_email(
               auto_failover_node,
               "Node (~p) was automatically failed over. Reason: ~s",
               [Node, Reason])
    end.

process_failover_error({autofailover_unsafe, UnsafeBuckets}, Nodes, S) ->
    ErrMsg = lists:flatten(io_lib:format("Would lose vbuckets in the"
                                         " following buckets: ~p",
                                         [UnsafeBuckets])),
    report_failover_error(autofailover_unsafe, ErrMsg, Nodes, S);
process_failover_error(retry_aborting_rebalance, Nodes, S) ->
     ?log_debug("Rebalance is being stopped by user, will retry auto-failover "
                "of nodes, ~p", [Nodes]),
     S;
process_failover_error({operation_running, Type} = Flag, Nodes, S) ->
    report_failover_error(Flag, Type ++ " is running.", Nodes, S);
process_failover_error(in_recovery, Nodes, S) ->
    report_failover_error(in_recovery,
                          "Cluster is in recovery mode.", Nodes, S);
process_failover_error(quorum_lost, Nodes, S) ->
    process_failover_error(orchestration_unsafe, Nodes, S);
process_failover_error(orchestration_unsafe, Nodes, S) ->
    report_failover_error(orchestration_unsafe,
                          "Could not contact majority of servers. "
                          "Orchestration may be compromised.", Nodes, S);
process_failover_error(config_sync_failed, Nodes, S) ->
    report_failover_error(config_sync_failed,
                          "Could not synchronize metadata with some nodes.",
                          Nodes, S);
process_failover_error(stopped_by_user, Nodes, S) ->
    report_failover_error(stopped_by_user, "Stopped by user.", Nodes, S).

report_failover_error(Flag, ErrMsg, Nodes, State) ->
    case should_report(Flag, State) of
        true ->
            ?log_info_and_email(
               auto_failover_node,
               "Could not automatically fail over nodes (~p). ~s",
               [Nodes, ErrMsg]),
            note_reported(Flag, State);
        false ->
            State
    end.

get_tick_period() ->
    ?TICK_PERIOD.

%% Returns list of nodes that are down/unhealthy along with the reason
%% why the node is considered unhealthy.
fastfo_down_nodes(NonPendingNodes) ->
    NodeStatuses = node_status_analyzer:get_nodes(),
    lists:foldl(
      fun (Node, Acc) ->
              case dict:find(Node, NodeStatuses) of
                  error ->
                      Acc;
                  {ok, NodeStatus} ->
                      case is_node_down(Node, NodeStatus) of
                          false ->
                              Acc;
                          {true, DownInfo} ->
                              [{Node, DownInfo, false} | Acc];
                          {true, DownInfo, AllMonitorsDown} ->
                              [{Node, DownInfo, AllMonitorsDown} | Acc]
                      end
              end
      end, [], NonPendingNodes).

is_node_down(Node, {unhealthy, _}) ->
    %% When ns_server is the only monitor running on a node,
    %% then we cannot distinguish between ns_server down and node down.
    %% This is currently true for all non-KV nodes.
    %% For such nodes, display ns-server down message as it is
    %% applicable during both conditions.
    %%
    %% The node is down because all monitors on the node report it as
    %% unhealthy. This is one of the requirements for server group
    %% auto-failover.
    case health_monitor:node_monitors(Node) of
        [ns_server] = Monitor ->
            {true, Res} = is_node_down(Monitor),
            {true, Res, true};
        _ ->
            {true, {"All monitors report node is unhealthy.",
                    [unhealthy_node]}, true}
    end;
is_node_down(_, {{needs_attention, MonitorStatuses}, _}) ->
    %% Different monitors are reporting different status for the node.
    is_node_down(MonitorStatuses);
is_node_down(_, _) ->
    false.

is_node_down(MonitorStatuses) ->
    Down = lists:foldl(
             fun (MonitorStatus, {RAcc, MAcc}) ->
                     {Monitor, Status} = case MonitorStatus of
                                             {M, S} ->
                                                 {M, S};
                                             M ->
                                                 {M, needs_attention}
                                         end,
                     Module = health_monitor:get_module(Monitor),
                     case Module:is_node_down(Status) of
                         false ->
                             {RAcc, MAcc};
                         {true, {Reason, MAinfo}} ->
                             {Reason ++ " " ++  RAcc, [MAinfo | MAcc]}
                     end
             end, {[], []}, MonitorStatuses),
    case Down of
        {[], []} ->
            false;
        _ ->
            {true, Down}
    end.

%%
%% Requirements for server group auto-failover:
%%  - User has enabled auto-failover of server groups.
%%  - There are 3 or more active server groups in the cluster
%%    at the time of the failure.
%%      - If there are only two server groups and there is a network
%%      partition between them, then both groups may try to fail each other
%%      over. This requirement prevents such a scenario.
%%  - All nodes in the server group are down.
%%  - All down nodes belong to the same server group.
%%      - This prevents a network partition from causing two or more
%%      partitions of a cluster from failing each other over.
%%  - All monitors report the down nodes are unhealthy.
%%      - When all monitors report all nodes in the server group are
%%        down then it is taken as an indication of a correlated failure.
%%      - Scenario #1: Say a server group has two nodes, node1 and node2.
%%        node1 is reported as down by KV monitor due to say memcached issues
%%        and node2 is reported as down by ns_server monitor.
%%        This scenario does not meet the requirement for server group
%%        failover.
%%      - Scenario #2: Both ns-server & KV monitor on node1 and node2
%%        report those nodes are down. This meets the requirement for
%%        server group failover.
%%
get_down_server_group([], _, _, _) ->
    [];
get_down_server_group([_], _, _, _) ->
    %% Only one node is down. Skip the checks for server group failover.
    [];
get_down_server_group(DownNodesInfo, Config, Snapshot, NonPendingNodes) ->
    %% TODO: Temporary. Save the failover_server_group setting in
    %% the auto_failover gen_server state to avoid this lookup.
    AFOConfig = get_cfg(Config),
    case proplists:get_value(failover_server_group, AFOConfig, false) of
        true ->
            case length(DownNodesInfo) > (length(NonPendingNodes)/2) of
                true ->
                    DownNodes = [N || {N, _, _} <- DownNodesInfo],
                    ?log_debug("Skipping checks for server group autofailover "
                               "because majority of nodes are down. "
                               "Down nodes:~p", [DownNodes]),
                    [];
                false ->
                    get_down_server_group_inner(DownNodesInfo, Snapshot)
            end;
        false ->
            []
    end.

get_down_server_group_inner(DownNodesInfo, Snapshot) ->
    %% Do all monitors on all down nodes report them as unhealthy?
    Pred = fun ({_, _, true}) -> true; (_) -> false end,
    case lists:all(Pred, DownNodesInfo) of
        true ->
            case enough_active_server_groups(Snapshot) of
                false ->
                    [];
                SGs ->
                    DownNodes = lists:sort([N || {N, _, _} <- DownNodesInfo]),
                    case all_nodes_down_in_same_group(SGs, DownNodes) of
                        false ->
                            [];
                        DownSG ->
                            DownSG
                    end
            end;
        false ->
            []
    end.

enough_active_server_groups(Snapshot) ->
    ActiveSGs = active_server_groups(Snapshot),
    case length(ActiveSGs) < ?MIN_ACTIVE_SERVER_GROUPS of
        true ->
            false;
        false ->
            ActiveSGs
    end.

active_server_groups(Snapshot) ->
    Groups = ns_cluster_membership:server_groups(Snapshot),
    AllSGs = [{proplists:get_value(name, SG),
               proplists:get_value(nodes, SG)} || SG <- Groups],
    lists:filtermap(
      fun ({SG, Ns}) ->
              case ns_cluster_membership:get_nodes_with_status(
                     Snapshot, Ns, active) of
                  [] ->
                      false;
                  ActiveNodes ->
                      {true, {SG, ActiveNodes}}
              end
      end, AllSGs).

%% All nodes in the server group are down.
%% All down nodes belong to the same server group.
all_nodes_down_in_same_group([], _) ->
    false;
all_nodes_down_in_same_group([{SG, Nodes} | Rest], DownNodes) ->
    case lists:sort(Nodes) =:= DownNodes of
        true ->
            SG;
        false ->
            all_nodes_down_in_same_group(Rest, DownNodes)
    end.

%% @doc Save the current state in ns_config
-spec make_state_persistent(State::#state{}) -> ok.
make_state_persistent(State) ->
    make_state_persistent(State, []).
make_state_persistent(State, Extras) ->
    Enabled = case State#state.tick_ref of
                  nil ->
                      false;
                  _ ->
                      true
              end,
    ok = ns_config:update_key(
           auto_failover_cfg,
           fun (Cfg) ->
                   misc:update_proplist(
                     Cfg,
                     [{enabled, Enabled},
                      {timeout, State#state.timeout},
                      {count, State#state.count},
                      {max_count, State#state.max_count},
                      {failed_over_server_groups,
                       State#state.failed_over_server_groups}] ++ Extras)
           end).

note_reported(Flag, State) ->
    true = should_report(Flag, State),
    misc:update_field(#state.reported_errors, State, sets:add_element(Flag, _)).

should_report(Flag, #state{reported_errors = Reported}) ->
    not sets:is_element(Flag, Reported).

init_reported(State) ->
    State#state{reported_errors = sets:new()}.

update_reported_flags_by_actions(Actions, State) ->
    case lists:keymember(failover, 1, Actions) orelse
        lists:keymember(failover_group, 1, Actions) of
        false ->
            init_reported(State);
        true ->
            State
    end.

%% Create a list of all services with following info for each service:
%% - is auto-failover for the service disabled
%% - list of nodes that are currently running the service.
all_services_config(Config, Snapshot) ->
    %% Get list of all supported services
    AllServices = ns_cluster_membership:cluster_supported_services(),
    lists:map(
      fun (Service) ->
              %% Get list of all nodes running the service.
              SvcNodes = ns_cluster_membership:service_active_nodes(
                           Snapshot, Service),
              %% Is auto-failover for the service disabled?
              ServiceKey = {auto_failover_disabled, Service},
              DV = case Service of
                       index ->
                           %% Auto-failover disabled by default for index service.
                           true;
                       _ ->
                           false
                   end,
              AutoFailoverDisabled = ns_config:search(Config, ServiceKey, DV),
              {Service, {{disable_auto_failover, AutoFailoverDisabled},
                         {nodes, SvcNodes}}}
      end, AllServices).

attach_node_uuids(Nodes, UUIDDict) ->
    lists:map(
      fun (Node) ->
              case dict:find(Node, UUIDDict) of
                  {ok, UUID} ->
                      {Node, UUID};
                  error ->
                      {Node, undefined}
              end
      end, Nodes).

restart_on_compat_mode_change() ->
    Self = self(),
    ns_pubsub:subscribe_link(compat_mode_events,
                             case _ of
                                 {compat_mode_changed, _, _} = Event ->
                                     exit(Self, {shutdown, Event});
                                 _ ->
                                     ok
                             end).

send_tick_msg() ->
    erlang:send_after(get_tick_period(), self(), tick).

-ifdef(TEST).
-define(FLAG, autofailover_unsafe).
reported_test() ->
    %% nothing reported initially
    State = init_reported(#state{}),

    %% we correctly instructed to report the condition for the first time
    ?assertEqual(should_report(?FLAG, State), true),
    State1 = note_reported(?FLAG, State),
    State2 = update_reported_flags_by_actions([{failover, some_node}], State1),

    %% we don't report it second time
    ?assertEqual(should_report(?FLAG, State2), false),

    %% we report the condition again for the other "instance" of failover
    %% (i.e. failover is not needed for some time, but the it's needed again)
    State3 = update_reported_flags_by_actions([], State2),
    ?assertEqual(should_report(?FLAG, State3), true),

    %% we report the condition after we explicitly drop it (note that we use
    %% State2)
    State4 = init_reported(State2),
    ?assertEqual(should_report(?FLAG, State4), true),

    ok.
-endif.
