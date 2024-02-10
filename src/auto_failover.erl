%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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
%%  auto-failover will be attempted but validate_kv_safety/1 will prevent
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
         enable/3,
         disable/1,
         reset_count/0,
         reset_count_async/0,
         is_enabled/0,
         is_enabled/1,
         validate_kv/3,
         validate_services_safety/4]).

%% For email alert notificatons
-export([alert_keys/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([get_tick_period_from_state/1,
         get_errors_from_state/1]).
-endif.

-define(SERVER, {via, leader_registry, ?MODULE}).

%% @doc The time a stats request to a bucket may take (in milliseconds)
-define(STATS_TIMEOUT, 2000).

%% @doc Default frequency (in milliseconds) at which to check for down nodes.
-define(DEFAULT_TICK_PERIOD, 1000).

-define(SAFETY_CHECK_TIMEOUT, ?get_timeout(safety_check, 2000)).

%% The auto_failover gen_server is expected to be unresponsive during an
%% auto-failover attempt. An auto-failover attempt may take a while,
%% particularly if there are issues such as gathering a quorum majority
%% (which has a 15s timeout by default). Supply a sufficiently large timeout
%% to the gen_server calls such that we're unlikely to time out waiting for
%% auto_failover to respond.
-define(CALL_TIMEOUT, ?get_timeout(call, 60000)).

-record(state,
        { auto_failover_logic_state = undefined,
          %% Reference to the tick timer.
          tick_ref = nil :: nil | reference(),
          %% Time a node needs to be down until it is automatically failovered
          timeout = nil :: nil | integer(),
          %% Optionally disable failover based on number of failover events
          disable_max_count = false :: boolean(),
          %% Maximum number of auto-failover events
          max_count = 0  :: non_neg_integer(),
          %% Counts the number of auto-failover events
          count = 0 :: non_neg_integer(),
          %% Whether we reported why the node is considered down
          reported_down_nodes_reason = [] :: list(),
          %% Keeps all errors that have been reported.
          reported_errors = sets:new() :: sets:set(),
          %% Frequency (in ms) at which to check for down nodes.
          tick_period = ?DEFAULT_TICK_PERIOD :: integer()
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

-spec is_enabled(list()) -> true | false.
is_enabled(Cfg) ->
    proplists:get_value(enabled, Cfg, false).

-spec is_enabled() -> true | false.
is_enabled() ->
    is_enabled(get_cfg()).

should_preserve_durability_majority() ->
    should_preserve_durability_majority(get_cfg()).

should_preserve_durability_majority(Config) ->
    proplists:get_bool(failover_preserve_durability_majority, Config).

%% Call has a large timeout by default. Calls are handled to reconfigure
%% auto-failover and the auto_failover gen_server is expected to be unresponsive
%% during an auto-failover attempt. An auto-failover attempt may take a while,
%% particularly if there are issues such as gathering a quorum majority
%% (which has a 15s timeout by default). Supply a sufficiently large timeout
%% to the gen_server calls such that we're unlikely to time out waiting for
%% auto_failover to respond. Care should be taken when using this function for
%% calls, or when adding new call handlers as this code works under the
%% expectation that calls are generally quick and have a well defined
%% upperbound of duration. Long calls could delay an auto-failover.
call(Call) ->
    misc:wait_for_global_name(?MODULE),
    gen_server:call(?SERVER, Call, ?CALL_TIMEOUT).

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

    chronicle_compat_events:notify_if_key_changes(
        [auto_failover_tick_period], tick_period_updated),

    Config = get_cfg(),
    ?log_debug("init auto_failover.", []),
    Timeout = proplists:get_value(timeout, Config),
    Count = proplists:get_value(count, Config),
    MaxCount = proplists:get_value(max_count, Config, 1),
    DisableMaxCount = proplists:get_value(disable_max_count, Config, false),
    State0 = #state{timeout = Timeout,
                    disable_max_count = DisableMaxCount,
                    max_count = MaxCount,
                    count = Count,
                    auto_failover_logic_state = undefined,
                    tick_period = get_tick_period(Timeout)},
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

init_logic_state(#state{timeout = Timeout,
                        tick_period = TickPeriod}) ->
    DownThreshold = (Timeout * 1000 + TickPeriod - 1) div TickPeriod,
    State = auto_failover_logic:init_state(DownThreshold),
    ?log_debug("Using auto-failover logic state ~p", [State]),
    State.

%% Care should be taken when adding new call handlers as this code works under
%% the expectation that calls are generally quick and have a well defined
%% upperbound of duration. Long calls could delay an auto-failover as the
%% gen_server cannot process two things at once!
handle_call({enable_auto_failover, Timeout, Max}, From, State) ->
    handle_call({enable_auto_failover, Timeout, Max, []}, From, State);
%% @doc Auto-failover isn't enabled yet (tick_ref isn't set).
handle_call({enable_auto_failover, Timeout, Max, Extras}, _From,
            #state{tick_ref = nil} = State) ->
    Ref = send_tick_msg(State),
    NewState = enable_auto_failover(
        Timeout, Max, Extras, State#state{tick_ref = Ref}),
    {reply, ok, NewState};
%% @doc Auto-failover is already enabled, just update the settings.
handle_call({enable_auto_failover, Timeout, Max, Extras}, _From, State) ->
    ?log_debug("updating auto-failover settings: ~p", [State]),
    NewState = enable_auto_failover(Timeout, Max, Extras, State),
    {reply, ok, NewState};

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
%% Care should be taken when adding new call handlers as this code works under
%% the expectation that calls are generally quick and have a well defined
%% upperbound of duration. Long calls could delay an auto-failover as the
%% gen_server cannot process two things at once!
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(reset_auto_failover_count, #state{count = 0} = State) ->
    {noreply, State};
handle_cast(reset_auto_failover_count, State) ->
    ?log_debug("reset auto_failover count: ~p", [State]),
    LogicState = init_logic_state(State),
    State1 = State#state{count = 0, auto_failover_logic_state = LogicState},
    State2 = init_reported(State1),
    make_state_persistent(State2),
    ale:info(?USER_LOGGER, "Reset auto-failover count"),
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick_period_updated, #state{timeout = Timeout} = State) ->
    NewState = State#state{tick_period = get_tick_period(Timeout)},
    {noreply,
     NewState#state{auto_failover_logic_state = init_logic_state(NewState)}};

%% @doc Check if nodes should/could be auto-failovered on every tick
handle_info(tick, State0) ->
    Ref = send_tick_msg(State0),
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
    State1 = State0#state{count = proplists:get_value(count, AFOConfig)},

    NonPendingNodes = lists:sort(ns_cluster_membership:active_nodes(Snapshot)),

    NodeStatuses = ns_doctor:get_nodes(),
    DownNodes = fastfo_down_nodes(NonPendingNodes),

    State = log_down_nodes_reason(DownNodes, State1),
    CurrentlyDown = [N || {N, _, _} <- DownNodes],
    NodeUUIDs = ns_config:get_node_uuid_map(Config),

    %% Extract service specfic information from the Config
    ServicesConfig = all_services_config(Config, Snapshot),

    {Actions, LogicState} =
        auto_failover_logic:process_frame(
          ns_cluster_membership:attach_node_uuids(NonPendingNodes, NodeUUIDs),
          ns_cluster_membership:attach_node_uuids(CurrentlyDown, NodeUUIDs),
          State#state.auto_failover_logic_state,
          ServicesConfig),
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

enable_auto_failover(Timeout, Max, Extras, BaseState) ->
    DisableMaxCount = proplists:get_value(disable_max_count, Extras,
                                          BaseState#state.disable_max_count),
    case DisableMaxCount of
        false ->
            ale:info(?USER_LOGGER,
                     "Enabled auto-failover with timeout ~p and max count ~p",
                     [Timeout, Max]);
        true ->
            ale:info(?USER_LOGGER,
                     "Enabled auto-failover with timeout ~p", [Timeout])
    end,
    update_and_save_auto_failover_state(
      DisableMaxCount, Timeout, Max, Extras, BaseState).

update_and_save_auto_failover_state(DisableMaxCount, NewTimeout, NewMax, Extras,
                                    #state{timeout = OldTimeout,
                                           max_count = OldMax} =
                                        OldState) ->
    case NewTimeout =/= OldTimeout of
        true ->
            ale:info(?USER_LOGGER, "Updating auto-failover timeout to ~p",
                     [NewTimeout]);
        false ->
            ?log_debug("No change in timeout ~p", [NewTimeout])
    end,

    case NewMax =/= OldMax of
        true ->
            ale:info(?USER_LOGGER, "Updating auto-failover max count to ~p",
                     [NewMax]);
        false ->
            ?log_debug("No change in max count ~p", [NewMax])
    end,

    NewTickPeriod = get_tick_period(NewTimeout),

    NewState =
        maybe_update_auto_failover_logic_state(
          OldTimeout, NewTimeout,
          OldState#state{timeout = NewTimeout, max_count = NewMax,
                         tick_period = NewTickPeriod,
                         disable_max_count = DisableMaxCount}),

    make_state_persistent(NewState, Extras),
    NewState.

maybe_update_auto_failover_logic_state(
  OldTimeout, NewTimeout,
  #state{auto_failover_logic_state = LogicState} = State)
  when OldTimeout =:= NewTimeout
       andalso LogicState =/= undefined ->
    State;
maybe_update_auto_failover_logic_state(_OldTimeout, _NewTimeout,
                                       State) ->
    State#state{auto_failover_logic_state = init_logic_state(State)}.

get_tick_period(Timeout) ->
    case ns_config:read_key_fast(auto_failover_tick_period, undefined) of
        undefined ->
            health_monitor:maybe_scale_refresh_interval(Timeout,
                                                        ?DEFAULT_TICK_PERIOD);
        Value -> Value
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
        auto_failover_logic:service_failover_min_node_count()]),
    S;
process_action({mail_down_warning, {Node, _UUID}}, S, _, _, _) ->
    ?log_info_and_email(
       auto_failover_other_nodes_down,
       "Could not auto-failover node (~p). "
       "There was at least another node down.",
       [Node]),
    S;
process_action({mail_down_warning_multi_node, {Node, _UUID}}, S, _, _, _) ->
    ?log_info_and_email(
       auto_failover_other_nodes_down,
       "Could not auto-failover node (~p). "
       "The list of nodes being down has changed.",
       [Node]),
    S;
process_action({mail_auto_failover_disabled, Service, {Node, _}}, S, _, _, _) ->
    ?log_info_and_email(
       auto_failover_disabled,
       "Could not auto-failover node (~p). "
       "Auto-failover for ~s service is disabled.",
       [Node, ns_cluster_membership:user_friendly_service_name(Service)]),
    S;
process_action({mail_kv_not_fully_failed_over, {Node, _}}, S, _, _, _) ->
    ?log_info_and_email(
       auto_failover_other_nodes_down,
       "Could not auto-failover service node (~p). "
       "One of the data service nodes cannot be automatically failed over.",
       [Node]),
    S;
process_action({failover, NodesWithUUIDs}, S, DownNodes, NodeStatuses,
               _Snapshot)
  when is_list(NodesWithUUIDs) ->
    Nodes = [N || {N, _} <- NodesWithUUIDs],
    {Nodes1, S1} =
        case S#state.disable_max_count of
            true -> {Nodes, S};
            false ->
                TrimmedNodes = trim_nodes(Nodes, S),
                case Nodes -- TrimmedNodes of
                    [] ->
                        {TrimmedNodes, S};
                    NotFailedOver ->
                        {TrimmedNodes,
                         maybe_report_max_node_reached(
                           Nodes, NotFailedOver, max_nodes_error_msg(S), S)}
                end
        end,
    failover_nodes(Nodes1, S1, DownNodes, NodeStatuses).

trim_nodes(Nodes, #state{count = Count, max_count = Max}) ->
    lists:sublist(Nodes, Max - Count).

max_nodes_error_msg(#state{max_count = Max}) ->
    M = io_lib:format("Maximum number of auto-failover nodes "
                      "(~p) has been reached.", [Max]),
    lists:flatten(M).

maybe_report_max_node_reached(AllNodes, NotFailedOver, ErrMsg, S) ->
    case S#state.disable_max_count =:= false andalso
        should_report(max_node_reached, S) of
        true ->
            case AllNodes -- NotFailedOver of
                [] ->
                    ?log_info_and_email(
                       auto_failover_maximum_reached,
                       "Could not auto-failover more nodes (~p). ~s",
                       [NotFailedOver, ErrMsg]);
                RemainingNodes ->
                    ?log_info_and_email(
                       auto_failover_maximum_reached,
                       "Could not auto-failover nodes (~p). ~s Continuing to "
                       "auto-failover nodes ~p",
                       [NotFailedOver, ErrMsg, RemainingNodes])
            end,

            event_log:add_log(auto_failover_warning,
                              [{reason, list_to_binary(ErrMsg)},
                               {nodes, NotFailedOver}]),
            note_reported(max_node_reached, S);
        false ->
            S
    end.

failover_nodes([], S, _DownNodes, _NodeStatuses) ->
    S;
failover_nodes(Nodes, S, DownNodes, NodeStatuses) ->
    FailoverReasons = failover_reasons(Nodes, DownNodes, NodeStatuses),
    DownNodeNames = [N || {N, _, _} <- DownNodes],
    case try_autofailover(Nodes, DownNodeNames, FailoverReasons) of
        {ok, UnsafeNodes} ->
            FailedOver = Nodes -- [N || {N, _} <- UnsafeNodes],
            [log_failover_success(N, DownNodes, NodeStatuses) ||
                N <- FailedOver],
            NewState = lists:foldl(fun log_unsafe_node/2, S, UnsafeNodes),
            NewCount = NewState#state.count + length(FailedOver),
            NewState1 = NewState#state{count = NewCount},
            case FailedOver of
                [] ->
                    NewState1;
                _ ->
                    init_reported(NewState1)
            end;
        Error ->
            process_failover_error(Error, Nodes, S)
    end.


try_autofailover(Nodes, DownNodes, FailoverReasons) ->
    case ns_cluster_membership:service_nodes(Nodes, kv) of
        [] ->
            Snapshot = failover:get_snapshot(),
            {ValidNodes, UnsafeNodes} =
                validate_services_safety(Snapshot, Nodes, DownNodes, []),
            case ValidNodes of
                [] ->
                    {ok, UnsafeNodes};
                _ ->
                    case ns_orchestrator:try_autofailover(
                           ValidNodes,
                           #{skip_safety_check => true,
                             failover_reasons => FailoverReasons,
                             down_nodes => DownNodes}) of
                        {ok, UN} ->
                            UN = [],
                            {ok, UnsafeNodes};
                        Error ->
                            Error
                    end
            end;
        _ ->
            ns_orchestrator:try_autofailover(
              Nodes, #{failover_reasons => FailoverReasons,
                       down_nodes => DownNodes})
    end.

log_failover_success(Node, DownNodes, NodeStatuses) ->
    case failover_reason(Node, DownNodes, NodeStatuses) of
        {Reason, MA} ->
            master_activity_events:note_autofailover_done(Node, MA),
            ?log_info_and_email(
               auto_failover_node,
               "Node (~p) was automatically failed over. Reason: ~s",
               [Node, Reason]);
        Reason ->
            ?log_info_and_email(
               auto_failover_node,
               "Node (~p) was automatically failed over.~n~p", [Node, Reason])

    end.

failover_reasons(FailedOverNodes, DownNodes, NodeStatuses) ->
    Fun = fun (N) ->
                  {Reason, _} = failover_reason(N, DownNodes, NodeStatuses),
                  {N, Reason}
          end,
    [Fun(Node) || Node <- FailedOverNodes].

failover_reason(Node, DownNodes, NodeStatuses) ->
    {_, DownInfo, _} = lists:keyfind(Node, 1, DownNodes),
    case DownInfo of
        unknown ->
            ns_doctor:get_node(Node, NodeStatuses);
        {Reason, MARes} ->
            MA = [atom_to_list(M) || M <- MARes],
            {Reason, string:join(MA, ",")}
    end.

log_unsafe_node({Node, {Service, Error}}, State) ->
    Flag = {Node, Service, Error},
    case should_report(Flag, State) of
        true ->
            ?log_info_and_email(
               auto_failover_node,
               "Could not automatically fail over node (~p) due to operation "
               "being unsafe for service ~p. ~s",
               [Node, Service, Error]),
            note_reported(Flag, State);
        false ->
            State
    end.

process_failover_error({autofailover_unsafe, UnsafeBuckets}, Nodes, S) ->
    ErrMsg = lists:flatten(io_lib:format("Would lose vbuckets in the"
                                         " following buckets: ~p",
                                         [UnsafeBuckets])),
    report_failover_error(autofailover_unsafe, ErrMsg, Nodes, S);
process_failover_error({nodes_down, NodesNeeded, Buckets}, Nodes, S) ->
    ErrMsg =
        lists:flatten(
          io_lib:format(
            "Nodes ~p needed to preserve durability writes on buckets ~p "
            "are down", [NodesNeeded, Buckets])),
    report_failover_error(nodes_down, ErrMsg, Nodes, S);
process_failover_error({cannot_preserve_durability_majority, Buckets},
                       Nodes, S) ->
    ErrMsg =
        lists:flatten(
            io_lib:format(
                "Cannot preserve the durability majority, and hence cannot "
                "guarantee the preservation of durable writes on buckets ~p",
                [Buckets])),
    report_failover_error(cannot_preserve_durability_majority, ErrMsg, Nodes,
                          S);
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
    report_failover_error(stopped_by_user, "Stopped by user.", Nodes, S);
process_failover_error(last_node, Nodes, S) ->
    report_failover_error(last_node, "Could not fail over the final active "
                                     "node running a service.", Nodes, S).

report_failover_error(Flag, ErrMsg, Nodes, State) ->
    case should_report(Flag, State) of
        true ->
            ?log_info_and_email(
               auto_failover_node,
               "Could not automatically fail over nodes (~p). ~s",
               [Nodes, ErrMsg]),
            event_log:add_log(auto_failover_warning,
                              [{reason, list_to_binary(ErrMsg)},
                               {nodes, Nodes}]),
            note_reported(Flag, State);
        false ->
            State
    end.

%% Returns list of nodes that are down/unhealthy along with the reason
%% why the node is considered unhealthy.
fastfo_down_nodes(NonPendingNodes) ->
    NodeStatuses = node_status_analyzer:get_statuses(),
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
        [ns_server] ->
            {true, Res} = is_node_down([{ns_server, unhealthy}]),
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
             fun ({Monitor, Status}, {RAcc, MAcc}) ->
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
                      {max_count, State#state.max_count}] ++ Extras)
           end).

note_reported(Flag, State) ->
    true = should_report(Flag, State),
    misc:update_field(#state.reported_errors, State, sets:add_element(Flag, _)).

should_report(Flag, #state{reported_errors = Reported}) ->
    not sets:is_element(Flag, Reported).

init_reported(State) ->
    State#state{reported_errors = sets:new()}.

update_reported_flags_by_actions(Actions, State) ->
    case lists:keymember(failover, 1, Actions) of
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
              AutoFailoverDisabled = ns_config:search(Config, ServiceKey, false),
              {Service, {{disable_auto_failover, AutoFailoverDisabled},
                         {nodes, SvcNodes}}}
      end, AllServices).

restart_on_compat_mode_change() ->
    Self = self(),
    ns_pubsub:subscribe_link(compat_mode_events,
                             case _ of
                                 {compat_mode_changed, _, _} = Event ->
                                     exit(Self, {shutdown, Event});
                                 _ ->
                                     ok
                             end).

send_tick_msg(#state{tick_period = TickPeriod}) ->
    erlang:send_after(TickPeriod, self(), tick).

validate_kv(Snapshot, FailoverNodes, DownNodes) ->
    case ns_cluster_membership:service_nodes(Snapshot, FailoverNodes, kv) of
        [] ->
            ok;
        FailoverKVNodes ->
            case validate_kv_safety(Snapshot, FailoverKVNodes) of
                ok ->
                    validate_durability_failover(Snapshot, FailoverKVNodes,
                                                 DownNodes);
                Error ->
                    Error
            end
    end.

validate_kv_safety(Snapshot, Nodes) ->
    case validate_membase_buckets(Snapshot,
                                  validate_bucket_safety(_, _, Nodes)) of
        [] ->
            ok;
        UnsafeBuckets ->
            {unsafe, [B || {B, _} <- UnsafeBuckets]}
    end.

validate_bucket_safety(_BucketName, Map, Nodes) ->
    lists:any(fun ([undefined|_]) ->
                      true;
                  (_) ->
                      false
              end, mb_map:promote_replicas(Map, Nodes)).

validate_membase_buckets(Snapshot, ValidateFun) ->
    lists:filtermap(
      fun ({BucketName, BucketConfig}) ->
              case ns_bucket:bucket_type(BucketConfig) of
                  membase ->
                      case proplists:get_value(map, BucketConfig) of
                          undefined ->
                              false;
                          Map ->
                              ValidateFun(BucketName, Map)
                      end;
                  memcached ->
                      false
              end
      end, ns_bucket:get_buckets(Snapshot)).

validate_durability_failover(Snapshot, FailoverNodes, DownNodes) ->
    ShouldPreserveDurabilityMajority = should_preserve_durability_majority(),
    case validate_membase_buckets(
           Snapshot,
           validate_nodes_up_for_durability_failover_for_bucket(_, _,
                                                                FailoverNodes,
                                                                DownNodes)) of
        [] when ShouldPreserveDurabilityMajority ->
            case validate_membase_buckets(
                   Snapshot,
                   validate_durability_majority_preserved_for_bucket(
                     _, _, FailoverNodes)) of
                [] ->
                    ok;
                UnsafeBuckets ->
                    {cannot_preserve_durability_majority, UnsafeBuckets}
            end;
        [] ->
            ok;
        Errors ->
            Buckets = [B || {B, _} <- Errors],
            Nodes = lists:usort(lists:flatten([N || {_, N} <- Errors])),
            {nodes_down, Nodes, Buckets}
    end.

validate_nodes_up_for_durability_failover_for_bucket(BucketName, Map,
                                                     FailoverNodes,
                                                     DownNodes) ->
    %% Check that we can get stats for the nodes that we might want to promote,
    %% i.e. are the nodes that we may want to promote down?
    NodesNeeded =
        failover:nodes_needed_for_durability_failover(Map, FailoverNodes),
    case NodesNeeded -- DownNodes of
        NodesNeeded ->
            false;
        AliveNodes ->
            {true, {BucketName, NodesNeeded -- AliveNodes}}
    end.

validate_durability_majority_preserved_for_bucket(BucketName, Map,
                                                  FailoverNodes) ->
    case failover:can_preserve_durability_majority(Map,
                                                   FailoverNodes) of
        true ->
            false;
        false ->
            {true, BucketName}
    end.

has_safe_check(index) ->
    true;
has_safe_check(_) ->
    false.

service_safety_check(Snapshot, Service, DownNodes, UUIDDict) ->
    case ns_cluster_membership:pick_service_node(
           Snapshot, Service, DownNodes) of
        undefined ->
            {error, mail_too_small};
        NodeToCall ->
            ActiveNodes =
                ns_cluster_membership:service_active_nodes(Snapshot, Service),
            ServiceDownNodes =
                lists:filter(lists:member(_, ActiveNodes), DownNodes),
            NodeIds = ns_cluster_membership:get_node_uuids(ServiceDownNodes,
                                                           UUIDDict),
            case rpc:call(NodeToCall, service_api, is_safe, [Service, NodeIds],
                          ?SAFETY_CHECK_TIMEOUT) of
                {badrpc, Error} ->
                    ?log_warning("Failed to execute safety check for service ~p"
                                 " on node ~p. Error = ~p",
                                 [Service, NodeToCall, Error]),
                    {error, "Safety check failed."};
                Other ->
                    Other
            end
    end.

get_service_safety(Snapshot, Service, DownNodes, UUIDDict, Cache) ->
    case has_safe_check(Service) of
        true ->
            case maps:find(Service, Cache) of
                {ok, Res} ->
                    {Res, Cache};
                error ->
                    Res = service_safety_check(Snapshot, Service, DownNodes,
                                               UUIDDict),
                    {Res, maps:put(Service, Res, Cache)}
            end;
        false ->
            {ok, Cache}
    end.

validate_services_safety(_Snapshot, [], _DownNodes, _UUIDDict, Cache) ->
    {ok, Cache};
validate_services_safety(Snapshot, [Service | Rest], DownNodes, UUIDDict,
                         Cache) ->
    case get_service_safety(Snapshot, Service, DownNodes, UUIDDict, Cache) of
        {ok, NewCache} ->
            validate_services_safety(Snapshot, Rest, DownNodes, UUIDDict,
                                     NewCache);
        {{error, Error}, NewCache} ->
            {{error, Error}, Service, NewCache}
    end.

%% Returns the list of nodes that are OK to failover and those
%% that are not taking into account the service safety check.
%% Note: the service safety check may involve an RPC to
%%       the service on a remote node.
%% Note: NodesToFailover should be a subset of DownNodes.
-spec validate_services_safety(map(), [node()], [node()], [node()]) ->
          {[node()], [{node(), {atom(), list()}}]}.
validate_services_safety(Snapshot, NodesToFailover, DownNodes, KVNodes) ->
    NonKVNodes = NodesToFailover -- KVNodes,
    UUIDDict = ns_config:get_node_uuid_map(ns_config:latest()),

    {ValidNodes, UnsafeNodes, _} =
        lists:foldl(
          fun (Node, {Nodes, Errors, Cache}) ->
                  Services = ns_cluster_membership:node_services(Snapshot,
                                                                 Node),
                  case validate_services_safety(Snapshot, Services, DownNodes,
                                                UUIDDict, Cache) of
                      {ok, NewCache} ->
                          {[Node | Nodes], Errors, NewCache};
                      {{error, Error}, Service, NewCache} ->
                          {Nodes, [{Node, {Service, Error}} | Errors], NewCache}
                  end
          end, {[], [], #{}}, NonKVNodes),
    {KVNodes ++ ValidNodes, UnsafeNodes}.

-ifdef(TEST).
%% Test function, gets the tick period from a provided state. Used outside of
%% this module where we don't have access to the state record.
-spec get_tick_period_from_state(#state{}) -> pos_integer().
get_tick_period_from_state(#state{tick_period = TickPeriod}) ->
    TickPeriod.

%% Test function, gets the reported errors from a provided state. Used outside
%% of this module where we don't have access to the state record.
-spec get_errors_from_state(#state{}) -> sets:set().
get_errors_from_state(#state{reported_errors = Errors}) ->
    Errors.

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
