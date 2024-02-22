%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(mb_master).

-behaviour(gen_statem).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Constants and definitions
-define(HEARTBEAT_INTERVAL, ?get_param(heartbeat_interval, 2000)).
-define(TIMEOUT_INTERVAL_COUNT, ?get_param(timeout_interval_count, 5)).
-define(TIMEOUT, ?HEARTBEAT_INTERVAL * ?TIMEOUT_INTERVAL_COUNT).

-type services() :: [service()].
-type node_info() :: {version(), node(), services()}.
-type priority() :: lower | equal | higher.
-type service_weights() :: [{service(), integer()}].

-record(state, {child :: undefined | pid(),
                master :: node(),
                peers :: [node()],
                last_heard :: integer(),
                higher_priority_nodes = [] :: [{node(), integer()}],
                service_weights = [] :: service_weights()}).


%% API
-export([start_link/0,
         master_node/0,
         config_upgrade_to_76/1]).


%% gen_statem callbacks
-export([code_change/4,
         init/1,
         callback_mode/0,
         terminate/3]).

%% States
-export([candidate/3,
         master/3]).

%%
%% API
%%

start_link() ->
    maybe_invalidate_current_master(),
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Returns the master node for the cluster, or undefined if it's
%% not known yet.
master_node() ->
    gen_statem:call(?MODULE, master_node).

%% Returns the master node according to Node. For mb_master's internal use
%% only.
master_node(Node, Timeout) ->
    gen_statem:call({?MODULE, Node}, master_node, Timeout).

%%
%% gen_statem handlers
%%

callback_mode() ->
    state_functions.

init([]) ->
    chronicle_compat_events:notify_if_key_changes([nodes_wanted,
                                                   service_orchestrator_weight],
                                                  config_changed),
    erlang:process_flag(trap_exit, true),
    CurHBInterval = ?HEARTBEAT_INTERVAL,
    ?log_debug("Heartbeat interval is ~p", [CurHBInterval]),
    send_heartbeat_msg(CurHBInterval),
    Now = erlang:monotonic_time(),
    case ns_node_disco:nodes_wanted() of
        [N] = P when N == node() ->
            ale:info(?USER_LOGGER, "I'm the only node, so I'm the master.", []),
            %% @state_change
            %% @from init
            %% @to master
            %% @reason Only node in cluster
            {ok, master,
                 start_master(#state{last_heard=Now,
                                     peers=P,
                                     service_weights = get_service_weights()})};
        Peers when is_list(Peers) ->
            %% We're a candidate
            ?log_debug("Starting as candidate. Peers: ~p", [Peers]),
            %% @state_change
            %% @from init
            %% @to candidate
            %% @reason Other nodes in cluster
            {ok, candidate, #state{last_heard = Now,
                                   %% Prevent new node from becoming master by
                                   %% accident, and wait for TIMEOUT amount of
                                   %% time before making a decision.
                                   higher_priority_nodes = [{node(), Now}],
                                   peers = Peers,
                                   service_weights = get_service_weights()}}
    end.

maybe_invalidate_current_master() ->
    do_maybe_invalidate_current_master(3, true).

do_maybe_invalidate_current_master(0, _FirstTime) ->
    ale:error(?USER_LOGGER,
              "We're out of luck taking mastership over older node", []),
    ok;
do_maybe_invalidate_current_master(TriesLeft, FirstTime) ->
    NodesWantedActual = ns_node_disco:nodes_actual(),
    case check_master_takeover_needed(NodesWantedActual -- [node()]) of
        false ->
            case FirstTime of
                true -> ok;
                false ->
                    ale:warn(?USER_LOGGER,
                             "Decided not to forcefully take over mastership",
                             [])
            end,
            ok;
        MasterToShutdown ->
            case do_invalidate_master(MasterToShutdown) of
                ok ->
                    ok;
                retry ->
                    do_maybe_invalidate_current_master(TriesLeft-1, false);
                {error, Error} ->
                    ale:error(?USER_LOGGER,
                              "Failed to forcefully take mastership "
                              "over old node (~p): ~p",
                              [MasterToShutdown, Error])
            end
    end.

do_invalidate_master(MasterToShutdown) ->
    %% send our config to this master it doesn't make sure
    %% mb_master will see us in peers because of a couple of
    %% races, but at least we'll delay a bit on some work and
    %% increase chance of it. We'll retry if it's not the case
    ok = chronicle_compat:push([MasterToShutdown]),
    %% ask master to give up
    send_heartbeat_with_peers([MasterToShutdown],
                              master, [node(), MasterToShutdown]),
    %% sync that "surrender" event
    case sync_surrender(MasterToShutdown, 5000) of
        {ok, NewMaster} ->
            if NewMaster =:= node() ->
                    ok;
               NewMaster =:= MasterToShutdown ->
                    retry;
               true ->
                    {error, {unexpected_master, NewMaster}}
            end;
        Error ->
            Error
    end.

sync_surrender(MasterToShutdown, Timeout) ->
    try master_node(MasterToShutdown, Timeout) of
        Node ->
            {ok, Node}
    catch
        T:E ->
            {error, {T, E}}
    end.

check_master_takeover_needed(Peers) ->
    TenNodesToAsk = lists:sublist(misc:shuffle(Peers), 10),
    ?log_debug("Sending master node question to the following nodes: ~p",
               [TenNodesToAsk]),
    {MasterReplies, _} = rpc:multicall(TenNodesToAsk, mb_master, master_node,
                                       [], 5000),
    ?log_debug("Got replies: ~p", [MasterReplies]),
    GoodMasterReplies = [M || M <- MasterReplies,
                              M =/= undefined,
                              is_atom(M)],
    case GoodMasterReplies of
        [] ->
            ?log_debug("Was unable to discover master, not going to force "
                       "mastership takeover"),
            false;
        [Master|_] when Master =:= node() ->
            %% assuming it happens only second round
            ale:warn(?USER_LOGGER,
                     "Somebody thinks we're master. Not forcing mastership "
                     "takover over ourselves"),
            false;
        [Master|_] ->
            ?log_debug("Checking version of current master: ~p", [Master]),
            case rpc:call(Master, cluster_compat_mode,
                          mb_master_advertised_version, [], 5000) of
                {badrpc, _} = Error ->
                    ale:warn(?USER_LOGGER,
                             "Failed to grab master's version. "
                             "Assuming force mastership "
                             "takeover is not needed. Reason: ~p", [Error]),
                    false;
                CompatVersion ->
                    MasterNodeInfo =
                        build_node_info(build_node_version(CompatVersion),
                                        Master),
                    {_MasterVersion, _MasterName, MasterServices} =
                        MasterNodeInfo,
                    {Version, _Name, Services} = node_info(),
                    ?log_debug("Current master's compat version: ~p "
                               "services ~p. This node's compat version: ~p "
                               "services ~p",
                               [CompatVersion, MasterServices, Version,
                                Services]),
                    ServiceWeights = get_service_weights(),
                    case strongly_lower_priority_node(MasterNodeInfo,
                                                      ServiceWeights) of
                        true ->
                            ale:warn(?USER_LOGGER,
                                     "Current master is strongly lower "
                                     "priority and I'll try to takeover", []),
                            Master;
                        false ->
                            ?log_debug("Current master is strongly higher "
                                       "priority, not taking over"),
                            false
                    end
            end
    end.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.


terminate(_Reason, _StateName, StateData) ->
    case StateData of
        #state{child=Child} when is_pid(Child) ->
            ?log_info("Synchronously shutting down child mb_master_sup"),
            shutdown_master_sup(StateData);
        _ ->
            ok
    end.


%%
%% States
%%

candidate(info, config_changed, StateData) ->
    Peers = ns_node_disco:nodes_wanted(),
    S0 = update_service_weights(StateData),
    S1 = refresh_high_priority_nodes(update_peers(S0, Peers)),
    case Peers of
        [N] when N == node() ->
            ale:info(?USER_LOGGER,
                     "I'm now the only node, so I'm the master.",
                     []),
            %% @state_change
            %% @from candidate
            %% @to master
            %% @reason Only node remaining
            {next_state, master, start_master(S1)};
        _ ->
            case can_be_master(S1) of
                true ->
                    ale:info(?USER_LOGGER,
                             "Master has been removed from cluster. "
                             "I'm taking over as the master.", []),
                    %% @state_change
                    %% @from candidate
                    %% @to master
                    %% @reason Master removed from the cluster
                    {next_state, master, start_master(S1)};
                false ->
                    {keep_state, S1}
            end
    end;
candidate(info, {send_heartbeat, LastHBInterval},
          #state{peers=Peers} = StateData) ->
    StartTS = erlang:monotonic_time(),

    MostOfTimeout = ?TIMEOUT * 4 div 5,

    Armed = diag_handler:arm_timeout(MostOfTimeout),
    send_heartbeat_msg(LastHBInterval),
    send_heartbeat_with_peers(Peers, candidate, Peers),
    diag_handler:disarm_timeout(Armed),

    SpentOnSending = erlang:convert_time_unit(erlang:monotonic_time() - StartTS,
                                              native, millisecond),

    SinceHeard  = erlang:convert_time_unit(StartTS - StateData#state.last_heard,
                                           native, millisecond),

    case SinceHeard >= ?TIMEOUT andalso SpentOnSending < MostOfTimeout of
        true ->
            %% Take over
            ale:info(?USER_LOGGER,
                     "Haven't heard from a higher priority node or a master, "
                     "so I'm taking over.", []),
            %% @state_change
            %% @from candidate
            %% @to master
            %% @reason Have not heard from higher priority node for 10s
            {next_state, master, start_master(StateData)};
        false ->
            keep_state_and_data
    end;
candidate(info, {heartbeat, {Version, Node}, master, _H},
          #state{peers=Peers, service_weights = ServiceWeights} = State) ->
    NodeInfo = build_node_info(Version, Node),

    case lists:member(Node, Peers) of
        false ->
            ?log_warning("Candidate got master heartbeat from node ~p "
                         "which is not in peers ~p", [Node, Peers]),
            keep_state_and_data;
        true ->
            %% If master is of strongly lower priority than we are, then we send
            %% fake mastership heartbeat to force previous master to
            %% surrender. Thus there will be some time when cluster won't
            %% have any master node. But after timeout mastership will be
            %% taken over by the node with highest priority.
            NewState =
                case strongly_lower_priority_node(NodeInfo, ServiceWeights) of
                    false ->
                        Now = erlang:monotonic_time(),
                        update_high_priority_nodes(
                          {Node, Now}, State#state{last_heard = Now,
                                                   master = Node});
                    true ->
                        case rebalance:status() of
                            running ->
                                ale:info(?USER_LOGGER,
                                         "Candidate got master heartbeat from "
                                         "node ~p which has lower priority. "
                                         "But I won't try to take over since "
                                         "rebalance seems to be running",
                                         [Node]),
                                State#state{last_heard=erlang:monotonic_time(),
                                            master=Node};
                            _ ->
                                ale:info(?USER_LOGGER,
                                         "Candidate got master heartbeat from "
                                         "node ~p which has lower priority. "
                                         "Will try to take over. This node ~p",
                                         [NodeInfo, node_info()]),

                                send_heartbeat_with_peers([Node], master,
                                                          State#state.peers),
                                State#state{master=undefined}
                        end
                end,

            OldMaster = State#state.master,
            NewMaster = NewState#state.master,
            case OldMaster =:= NewMaster of
                true ->
                    ok;
                false ->
                    ?log_info("Changing master from ~p to ~p",
                              [OldMaster, NewMaster]),
                    announce_leader(NewMaster)
            end,
            {keep_state, NewState}
    end;

candidate(info, {heartbeat, {Version, Node}, candidate, _H},
          #state{peers=Peers, service_weights = ServiceWeights} = State) ->
    NodeInfo = build_node_info(Version, Node),

    case lists:member(Node, Peers) of
        true ->
            case higher_priority_node(NodeInfo, ServiceWeights) of
                true ->
                    Now = erlang:monotonic_time(),
                    %% Higher priority node
                    {keep_state, update_high_priority_nodes(
                                   {Node, Now},
                                   State#state{last_heard = Now})};
                false ->
                    %% Lower priority, so ignore it
                    keep_state_and_data
            end;
        false ->
            ?log_warning("Candidate got candidate heartbeat from node ~p which "
                         "is not in peers ~p", [Node, Peers]),
            keep_state_and_data

    end;

candidate(Type, Msg, State) ->
    handle_event(Type, Msg, candidate, State).

master(info, config_changed, StateData) ->
    Peers = ns_node_disco:nodes_wanted(),
    S0 = update_service_weights(StateData),
    S1 = refresh_high_priority_nodes(update_peers(S0, Peers)),
    case lists:member(node(), Peers) of
        true ->
            {keep_state, S1};
        false ->
            ?log_info("Master has been demoted. Peers = ~p", [Peers]),
            NewState = shutdown_master_sup(S1),
            %% @state_change
            %% @from master
            %% @to candidate
            %% @reason Master removed from cluster
            {next_state, candidate, NewState}
    end;
master(info, {send_heartbeat, LastHBInterval}, StateData) ->
    send_heartbeat_msg(LastHBInterval),
    send_heartbeat_with_peers(ns_node_disco:nodes_wanted(), master,
                              StateData#state.peers),
    keep_state_and_data;
master(info,
       {heartbeat, {Version, Node}, master, _H},
       #state{peers = Peers, service_weights = ServiceWeights} = State) ->
    NodeInfo = build_node_info(Version, Node),

    case lists:member(Node, Peers) of
        true ->
            Now = erlang:monotonic_time(),

            case higher_priority_node(NodeInfo, ServiceWeights) of
                true ->
                    ?log_info("Surrendering mastership to ~p", [Node]),
                    NewState = shutdown_master_sup(State),
                    announce_leader(Node),
                    %% @state_change
                    %% @from master
                    %% @to candidate
                    %% @reason Surrendering mastership, newer node in cluster
                    {next_state, candidate,
                     update_high_priority_nodes(
                       {Node, Now},
                       NewState#state{last_heard = Now,
                                      master = Node})};
                false ->
                    ?log_info("Got master heartbeat from ~p when I'm master",
                              [Node]),
                    {keep_state, State#state{last_heard=Now}}
            end;
        false ->
            ?log_warning("Master got master heartbeat from node ~p which is "
                         "not in peers ~p", [Node, Peers]),
            keep_state_and_data
    end;

master(info,
       {heartbeat, {_Version, Node}, candidate, _H},
       #state{peers=Peers} = State) ->
    case lists:member(Node, Peers) of
        true ->
            ok;
        false ->
            ?log_warning("Master got candidate heartbeat from node ~p which is "
                         "not in peers ~p", [Node, Peers])
    end,
    {keep_state, State#state{last_heard=erlang:monotonic_time()}};

master(Type, Msg, State) ->
    handle_event(Type, Msg, master, State).

handle_event(info, {'EXIT', _From, Reason} = Msg, _, _) ->
    ?log_info("Dying because of linked process exit: ~p~n", [Msg]),
    exit(Reason);
handle_event({call, From}, master_node, _State, StateData) ->
    {keep_state_and_data, [{reply, From, StateData#state.master}]};
handle_event(Type, Msg, State, StateData) ->
    ?log_warning("Got unexpected event ~p of type ~p in state ~p with data ~p",
                 [Msg, Type, State, StateData]),
    keep_state_and_data.

%%
%% Internal functions
%%

%% @private
%% @doc Send an heartbeat to a list of nodes, except this one.
send_heartbeat_with_peers(Nodes, StateName, Peers) ->
    %% We send...
    NodeInfo = communicated_node_info(),

    Args = {heartbeat, NodeInfo, StateName,
            [{peers, Peers},
             {versioning, true}]},
    try
        %% we try to avoid sending event to nodes that are
        %% down. Because send call inside gen_fsm will try to
        %% establish connection each time we try to send.
        %% + also exclude local node here
        AliveNodes = lists:filter(lists:member(_, nodes()), Nodes),
        misc:parallel_map(
          fun (Node)  ->
                  catch erlang:send({?MODULE, Node}, Args, [noconnect])
          end, AliveNodes, 2000),
        ok
    catch exit:timeout ->
            ?log_warning("send heartbeat timed out")
    end.


%% @private
%% @doc Go into master state. Returns new state data.
start_master(StateData) ->
    announce_leader(node()),
    {ok, Pid} = mb_master_sup:start_link(),
    OldMaster = StateData#state.master,
    OldMasterJSON = case OldMaster of
                        undefined ->
                            [];
                        _ ->
                            [{old_master, OldMaster}]
                    end,
    event_log:add_log(master_selected,
                      [{new_master, node()}] ++ OldMasterJSON),
    StateData#state{child = Pid,
                    master = node(),
                    higher_priority_nodes = []}.


%% @private
%% @doc Update the list of peers in the state. Also logs when it
%% changes.
update_peers(StateData, Peers) ->
    O = lists:sort(StateData#state.peers),
    P = lists:sort(Peers),
    case O == P of
        true ->
            %% No change
            StateData;
        false ->
            ?log_debug("List of peers has changed from ~p to ~p", [O, P]),
            StateData#state{peers=P}
    end.

shutdown_master_sup(State) ->
    Pid = State#state.child,
    misc:unlink_terminate_and_wait(Pid, shutdown),
    announce_leader(undefined),
    State#state{child = undefined,
                master = undefined}.


%% Auxiliary functions

build_node_version(CompatVersion) ->
    {CompatVersion, release, 0}.

build_node_info(Version, Node) ->
    Services = ns_cluster_membership:node_services(Node),
    {Version, Node, Services}.

%% Return node information for ourselves.
-spec node_info() -> node_info().
node_info() ->
    Version =
        build_node_version(cluster_compat_mode:mb_master_advertised_version()),
    build_node_info(Version, node()).

%% Return the node information for ourselves that we communicate to other
%% nodes. For compat reasons we don't communicate services (older versions
%% wouldn't be able to handle that) so it's simpler to avoid communicating
%% services to any node and to just check the chronicle config locally.
-spec communicated_node_info() -> {version(), node()}.
communicated_node_info() ->
    {{cluster_compat_mode:mb_master_advertised_version(), release, 0}, node()}.

-spec compare_version(version(), version()) -> priority().
compare_version(VersionA, VersionB) ->
    if VersionA =:= VersionB ->
           equal;
       VersionA < VersionB ->
           lower;
       VersionA > VersionB ->
           higher
    end.

-spec compare_name(node(), node()) -> priority().
compare_name(NameA, NameB) ->
    if NameA =:= NameB ->
          equal;
       NameA < NameB ->
           higher;
       NameB < NameA ->
           lower
    end.

-spec compare_services(services(), services(), service_weights()) -> priority().
compare_services(ServicesA, ServicesB, ServiceWeights) ->
    ServicesAPrio = get_services_total_priority(ServicesA, ServiceWeights),
    ServicesBPrio = get_services_total_priority(ServicesB, ServiceWeights),
    if ServicesAPrio =:= ServicesBPrio ->
           equal;
       ServicesAPrio < ServicesBPrio ->
           higher;
       ServicesAPrio > ServicesBPrio ->
           lower
    end.

get_service_priority(Service, ServiceWeights) ->
    {Service, Priority} = lists:keyfind(Service, 1, ServiceWeights),
    Priority.

get_services_total_priority(Services, ServiceWeights) ->
    lists:foldl(
        fun(Service, Acc) ->
            Acc + get_service_priority(Service, ServiceWeights)
        end,
        0, Services).

%% Determine whether some node is of higher priority than ourselves.
-spec higher_priority_node(node_info(), service_weights()) -> boolean().
higher_priority_node(NodeInfo, ServiceWeights) ->
    Self = node_info(),
    higher_priority_node(Self, NodeInfo, ServiceWeights).

-spec higher_priority_node(
          node_info(), node_info(), service_weights()) -> boolean().
higher_priority_node({SelfVersion, SelfNode, SelfServices},
                     {OtherVersion, OtherNode, OtherServices},
                     ServiceWeights) ->
    higher_priority_compare_node([SelfVersion, SelfServices, SelfNode],
                                 [OtherVersion, OtherServices, OtherNode],
                                 [fun compare_version/2,
                                  compare_services(_, _, ServiceWeights),
                                  fun compare_name/2]).

%% Compare node info terms against one another with the given comparators.
%% The list of terms should be of the form [version(), services(), node()]
-spec higher_priority_compare_node(
          [term()], [term()], [function()]) -> boolean().
higher_priority_compare_node(Self, Other, Comparators) ->
    case compare_node(Self, Other, Comparators) of
        lower -> false;
        higher -> true;
        equal -> false
    end.

%% true iff we need to take over mastership of given node
-spec strongly_lower_priority_node(node_info(), service_weights()) -> boolean().
strongly_lower_priority_node(NodeInfo, ServiceWeights) ->
    Self = node_info(),
    strongly_lower_priority_node(Self, NodeInfo, ServiceWeights).

-spec strongly_lower_priority_node(
          node_info(), node_info(), service_weights()) -> boolean().
strongly_lower_priority_node({SelfVersion, _SelfNode, SelfServices},
                             {OtherVersion, _OtherNode, OtherServices},
                             ServiceWeights) ->
    strongly_lower_priority_compare_node([SelfVersion, SelfServices],
                                         [OtherVersion, OtherServices],
                                         [fun compare_version/2,
                                          compare_services(_, _,
                                                           ServiceWeights)]).

%% Compare node info terms against one another with the given comparators.
%% The list of terms should be of the form [version(), services()]. We don't
%% compare node name here like we do in higher_priority_node as a node with
%% lower name does not need to take over from one with the same priority
%% otherwise.
-spec strongly_lower_priority_compare_node(
          [term()], [term()], [function()]) -> boolean().
strongly_lower_priority_compare_node(Self, Other, Comparators) ->
    case compare_node(Self, Other, Comparators) of
        lower -> true;
        higher -> false;
        equal -> false
    end.

-spec compare_node([term()], [term()], [function()]) -> priority().
compare_node([],[],[]) ->
    %% Exhausted all comparators, and criterion
    equal;
compare_node([SelfValue | Self], [OtherValue | Other], [Comparator |
    Comparators]) ->
    case Comparator(OtherValue, SelfValue) of
        equal -> compare_node(Self, Other, Comparators);
        Priority -> Priority
    end.

announce_leader(Node) ->
    gen_event:sync_notify(leader_events, {new_leader, Node}).

send_heartbeat_msg(LastHBInterval) ->
    CurHBInterval = ?HEARTBEAT_INTERVAL,
    case LastHBInterval =/= CurHBInterval of
        true ->
            ?log_debug("Heartbeat interval changed from ~p to ~p",
                       [LastHBInterval, CurHBInterval]);
        false ->
            ok
    end,

    erlang:send_after(CurHBInterval, self(), {send_heartbeat, CurHBInterval}).

can_be_master(#state{master = Master,
                     peers = Peers,
                     higher_priority_nodes = HigherPriorityNodes}) ->
    not lists:member(Master, Peers) andalso
        HigherPriorityNodes =:= [] andalso
        lists:member(node(), Peers).

update_high_priority_nodes({Node, Now},
                           #state{higher_priority_nodes = Nodes} = State) ->
    NewNodes = lists:keystore(Node, 1, Nodes, {Node, Now}),
    State#state{higher_priority_nodes = NewNodes}.

refresh_high_priority_nodes(#state{higher_priority_nodes = Nodes,
                                   peers = Peers} = State) ->
    Now = erlang:monotonic_time(),
    NewNodes =
        lists:filter(
          fun ({N, LastSeen}) ->
                  SinceHeard  = erlang:convert_time_unit(Now - LastSeen,
                                                         native, millisecond),
                  SinceHeard < ?TIMEOUT andalso (lists:member(N, Peers) orelse
                                                 %% This forces a newly
                                                 %% initialized node to wait for
                                                 %% ?TIMEOUT before it can
                                                 %% become the master. Don't
                                                 %% clear it we are not part of
                                                 %% the cluster yet.
                                                 N =:= node())

          end, Nodes),
    State#state{higher_priority_nodes = NewNodes}.

config_upgrade_to_76(_Config) ->
    [{delete, mb33750_workaround_enabled}].

update_service_weights(State) ->
    State#state{service_weights = get_service_weights()}.

get_service_weights() ->
    ns_config:read_key_fast(service_orchestrator_weight,
                            ?DEFAULT_SERVICE_WEIGHTS).

-ifdef(TEST).
-define(TEST_NO_SERVICES, []).

%% Test for the pre-7.6.0 priority algorithm.
%% Note, the priority algorithm does not have a cluster compat mode check as
%% it is designed to be backwards compatible.
higher_priority_node_t() ->
    %% VersionA < VersionB => NodeB higher priority (NameA < NameB)
    ?assertEqual(true,
                 higher_priority_node({misc:parse_version("1.7.1"),
                                       'ns_1@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      {misc:parse_version("2.0"),
                                       'ns_2@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA < VersionB => NodeB higher priority (NameA > NameB)
    ?assertEqual(true,
                 higher_priority_node({misc:parse_version("1.7.1"),
                                       'ns_2@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      {misc:parse_version("2.0"),
                                       'ns_1@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA > VersionB => NodeA higher priority (NameA < NameB)
    ?assertEqual(false,
                 higher_priority_node({misc:parse_version("2.0"),
                                       'ns_0@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      {misc:parse_version("1.7.2"),
                                       'ns_1@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA > VersionB => NodeA higher priority (NameA > NameB)
    ?assertEqual(false,
                 higher_priority_node({misc:parse_version("2.0"),
                                       'ns_1@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      {misc:parse_version("1.7.2"),
                                       'ns_0@192.168.1.1',
                                       ?TEST_NO_SERVICES},
                                      ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA = VersionB and NameA < NameB => NodeA higher priority
    ?assertEqual(false, higher_priority_node({misc:parse_version("2.0"),
                                              'ns_1@192.168.1.1',
                                              ?TEST_NO_SERVICES},
                                             {misc:parse_version("2.0"),
                                              'ns_2@192.168.1.1',
                                              ?TEST_NO_SERVICES},
                                             ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA = VersionB and NameA > NameB => NodeB higher priority
    ?assertEqual(true, higher_priority_node({misc:parse_version("2.0"),
                                             'ns_2@192.168.1.1',
                                             ?TEST_NO_SERVICES},
                                            {misc:parse_version("2.0"),
                                             'ns_1@192.168.1.1',
                                             ?TEST_NO_SERVICES},
                                            ?DEFAULT_SERVICE_WEIGHTS)).

%% Test for the post-7.6.0 priority algorithm.
%% Note, the priority algorithm does not have a cluster compat mode check as
%% it is designed to be backwards compatible.
higher_priority_services_t() ->
    %% We won't re-test all of the variants above, given that the algorithm
    %% is the same, we will just test the new part (services) and the edge
    %% cases.
    Version72 = misc:parse_version("7.2.0"),
    Version76 = misc:parse_version("7.6.0"),
    NameA = 'ns_1',
    NameB = 'ns_2',

    HPNVersionALower =
        fun(SA, SB) ->
            higher_priority_node({Version72, NameA, SA},
                                 {Version76, NameB, SB},
                                 ?DEFAULT_SERVICE_WEIGHTS)
        end,

    %% Version always takes precedence, regardless of services.
    %% VersionA < VersionB => NodeA higher priority (ServicesA < ServicesB)
    ?assertEqual(true, HPNVersionALower(?TEST_NO_SERVICES, [kv])),

    %% VersionA < VersionB => NodeA higher priority (ServicesA > ServicesB)
    ?assertEqual(true, HPNVersionALower([kv], ?TEST_NO_SERVICES)),

    HPNNameALower =
        fun(SA, SB) ->
            higher_priority_node({Version76, NameA, SA},
                                 {Version76, NameB, SB},
                                 ?DEFAULT_SERVICE_WEIGHTS)
        end,

    %% The next priority criteria is the service list so we will use the same
    %% version for these tests.
    %% VersionA = VersionB and ServicesA < ServicesB => NodeA higher priority
    %% (service-less)
    ?assertEqual(false, HPNNameALower(?TEST_NO_SERVICES, [backup])),

    %% VersionA = VersionB and ServicesA < ServicesB => NodeB higher priority
    %% (service-less)
    ?assertEqual(true, HPNNameALower([backup], ?TEST_NO_SERVICES)),

    %% VersionA = VersionB and ServicesA < ServicesB => NodeA higher priority
    ?assertEqual(false, HPNNameALower([index], [kv])),

    %% VersionA = VersionB and ServicesA < ServicesB => NodeB higher priority
    ?assertEqual(true, HPNNameALower([kv], [index])),

    %% VersionA = VersionB and ServicesA = ServicesB and NodeA < NodeB =>
    %% NodeA higher priority
    ?assertEqual(false, HPNNameALower([kv], [kv])),

    %% VersionA = VersionB and ServicesA = ServicesB and NodeA > NodeB =>
    %% NodeA higher priority
    ?assertEqual(true, higher_priority_node({Version76, NameB, [kv]},
                                            {Version76, NameA, [kv]},
                                            ?DEFAULT_SERVICE_WEIGHTS)).

%% Test for the pre-7.6.0 priority algorithm.
%% Note, the priority algorithm does not have a cluster compat mode check as
%% it is designed to be backwards compatible.
strongly_lower_priority_node_t() ->
    %% VersionA < VersionB => NodeB higher priority (NameA < NameB)
    ?assertEqual(false,
                 strongly_lower_priority_node({misc:parse_version("7.2.0"),
                                               'ns_0@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              {misc:parse_version("7.6.0"),
                                               'ns_1@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA < VersionB => NodeB higher priority (NameA > NameB)
    ?assertEqual(false,
                 strongly_lower_priority_node({misc:parse_version("7.2.0"),
                                               'ns_1@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              {misc:parse_version("7.6.0"),
                                               'ns_0@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA > VersionB => NodeA higher priority (NameA < NameB)
    ?assertEqual(true,
                 strongly_lower_priority_node({misc:parse_version("7.6.0"),
                                               'ns_0@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              {misc:parse_version("7.2.0"),
                                               'ns_1@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA > VersionB => NodeA higher priority (NameA > NameB)
    ?assertEqual(true,
                 strongly_lower_priority_node({misc:parse_version("7.6.0"),
                                               'ns_1@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              {misc:parse_version("7.2.0"),
                                               'ns_0@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA = VersionB => NodeB higher priority (NameA < NameB)
    ?assertEqual(false,
                 strongly_lower_priority_node({misc:parse_version("7.6.0"),
                                               'ns_0@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              {misc:parse_version("7.6.0"),
                                               'ns_1@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              ?DEFAULT_SERVICE_WEIGHTS)),

    %% VersionA = VersionB => NodeB higher priority (NameA > NameB)
    ?assertEqual(false,
                 strongly_lower_priority_node({misc:parse_version("7.6.0"),
                                               'ns_1@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              {misc:parse_version("7.6.0"),
                                               'ns_0@192.168.1.1',
                                               ?TEST_NO_SERVICES},
                                              ?DEFAULT_SERVICE_WEIGHTS)).

%% Test for the post-7.6.0 priority algorithm.
%% Note, the priority algorithm does not have a cluster compat mode check as
%% it is designed to be backwards compatible.
strongly_lower_priority_services_t() ->
    %% We won't re-test all of the variants above, given that the algorithm
    %% is the same, we will just test the new part (services) and the edge
    %% cases.
    Version72 = misc:parse_version("7.2.0"),
    Version76 = misc:parse_version("7.6.0"),
    NameA = 'ns_1',
    NameB = 'ns_2',

    SLPNVersionALower =
        fun(SA, SB) ->
            strongly_lower_priority_node({Version72, NameA, SA},
                                         {Version76, NameB, SB},
                                         ?DEFAULT_SERVICE_WEIGHTS)
        end,

    %% Version always takes precedence, regardless of services.
    %% VersionA < VersionB => NodeA higher priority (ServicesA < ServicesB)
    ?assertEqual(false, SLPNVersionALower(?TEST_NO_SERVICES, [kv])),

    %% VersionA < VersionB => NodeA higher priority (ServicesA > ServicesB)
    ?assertEqual(false, SLPNVersionALower([kv], ?TEST_NO_SERVICES)),

    SLPNNameALower =
        fun(SA, SB) ->
            strongly_lower_priority_node({Version76, NameA, SA},
                                         {Version76, NameB, SB},
                                         ?DEFAULT_SERVICE_WEIGHTS)
        end,

    %% The next priority criteria is the service list so we will use the same
    %% version for these tests.
    %% VersionA = VersionB and ServicesA < ServicesB => NodeA higher priority
    %% (service-less)
    ?assertEqual(true, SLPNNameALower(?TEST_NO_SERVICES, [backup])),

    %% VersionA = VersionB and ServicesA < ServicesB => NodeB higher priority
    %% (service-less)
    ?assertEqual(false, SLPNNameALower([backup], ?TEST_NO_SERVICES)),

    %% VersionA = VersionB and ServicesA < ServicesB => NodeA higher priority
    ?assertEqual(true, SLPNNameALower([index], [kv])),

    %% VersionA = VersionB and ServicesA < ServicesB => NodeB higher priority
    ?assertEqual(false, SLPNNameALower([kv], [index])),

    %% VersionA = VersionB and ServicesA = ServicesB and NodeA < NodeB =>
    %% NodeA higher priority
    ?assertEqual(false, SLPNNameALower([kv], [kv])),

    %% VersionA = VersionB and ServicesA = ServicesB and NodeA > NodeB =>
    %% NodeA higher priority
    ?assertEqual(false, strongly_lower_priority_node({Version76, NameB, [kv]},
                                                     {Version76, NameA, [kv]},
                                                     ?DEFAULT_SERVICE_WEIGHTS)).

node_info_t() ->
    {Version, Node, Services} =
        build_node_info(build_node_version([7,6,0]), 'ns_1@192.168.1.1'),
    ?assertEqual(misc:parse_version("7.6.0"), Version),
    ?assertEqual('ns_1@192.168.1.1', Node),
    ?assertEqual([kv], Services).

priority_test_setup() ->
    meck:new(ns_cluster_membership),
    meck:expect(ns_cluster_membership, node_services,
        fun(_) ->
            [kv]
        end).

priority_test_teardown(_R) ->
    meck:unload(ns_cluster_membership).

priority_test_() ->
    {setup,
        fun priority_test_setup/0,
        fun priority_test_teardown/1,
        [{"higher priority test", fun higher_priority_node_t/0},
         {"strongly lower priority test", fun strongly_lower_priority_node_t/0},
         {"higher priority service test", fun higher_priority_services_t/0},
         {"strongly lower priority test", fun strongly_lower_priority_node_t/0},
         {"strongly lower priority services test",
          fun strongly_lower_priority_services_t/0},
         {"node info test", fun node_info_t/0}]
    }.

-endif.
