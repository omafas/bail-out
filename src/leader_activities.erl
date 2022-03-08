%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(leader_activities).

-behaviour(gen_server2).

-include("cut.hrl").
-include("ns_common.hrl").

%% API
-export([start_link/0]).

-export([start_activity/3, start_activity/4]).
-export([start_activity/5, start_activity/6]).

-export([run_activity/3, run_activity/4, run_activity/5, run_activity/6]).

-export([register_process/2, register_process/3]).
-export([register_process/4, register_process/5]).

-export([switch_quorum/1, switch_quorum/2]).

-export([activate_quorum_nodes/1, activate_quorum_nodes/2]).
-export([deactivate_quorum_nodes/1, deactivate_quorum_nodes/2]).
-export([update_quorum_nodes/1, update_quorum_nodes/2]).

%% used only by leader_* modules only
-export([register_acquirer/1, register_agent/1]).
-export([lease_acquired/2, lease_lost/2]).
-export([local_lease_granted/2, local_lease_expired/2]).

-export([register_quorum_nodes_manager/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(PRECONDITIONS_TIMEOUT,
        ?get_timeout(preconditions_timeout, 15000)).

-type lease_holder() :: {node(), binary()}.

-type user_quorum() :: quorum(sets:set(node()) | [node()]).

-type quorum() :: quorum(sets:set(node())).
-type quorum(Nodes) :: leader_quorum(Nodes) | follower_quorum().

-type follower_quorum() :: follower.
-type leader_quorum(Nodes) :: all
                            | majority
                            | {all, Nodes}
                            | {majority, Nodes}
                            | [leader_quorum(Nodes)].

-type activity_option() :: {quorum_timeout, non_neg_integer()}
                         | {timeout, non_neg_integer()}
                         | quiet
                         | unsafe.
-type activity_options() :: [activity_option()].

-record(activity, { pid          :: pid(),
                    mref         :: reference(),
                    domain       :: term(),
                    domain_token :: binary() | any(),
                    name         :: [term()],
                    quorum       :: quorum(),
                    options      :: activity_options() }).

-record(activity_token, { lease        :: leader | {node(), binary()},
                          domain_token :: binary(),
                          domain       :: term(),
                          name         :: [term()],
                          options      :: activity_options() }).

-record(state, { agent                :: undefined | {pid(), reference()},
                 acquirer             :: undefined | {pid(), reference()},
                 quorum_nodes_manager :: undefined | {pid(), reference()},

                 quorum_nodes       :: undefined | sets:set(node()),
                 leases             :: sets:set(node()),
                 local_lease_holder :: undefined | lease_holder(),

                 activities :: [#activity{}]
               }).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

register_acquirer(Pid) ->
    call_register_internal_process(acquirer, Pid).

register_agent(Pid) ->
    call_register_internal_process(agent, Pid).

register_quorum_nodes_manager(Pid, QuorumNodes) ->
    case call_register_internal_process(quorum_nodes_manager, Pid) of
        {ok, _ServerPid} = Ok ->
            case call_if_internal_process(quorum_nodes_manager, Pid,
                                          {note_quorum_nodes, QuorumNodes}) of
                ok ->
                    Ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

lease_acquired(Pid, Node) ->
    call_if_internal_process(acquirer, Pid, {lease_acquired, Node}).

lease_lost(Pid, Node) ->
    call_if_internal_process(acquirer, Pid, {lease_lost, Node}).

local_lease_granted(Pid, LocalLease) ->
    call_if_internal_process(agent, Pid, {local_lease_granted, LocalLease}).

local_lease_expired(Pid, LocalLease) ->
    call_if_internal_process(agent, Pid, {local_lease_expired, LocalLease}).

run_activity(Name, Quorum, Body) ->
    run_activity(Name, Quorum, Body, []).

run_activity(Name, Quorum, Body, Opts) ->
    run_activity(default, Name, Quorum, Body, Opts).

run_activity(Domain, Name, Quorum, Body, Opts) ->
    run_activity(node(), Domain, Name, Quorum, Body, Opts).

run_activity(Node, Domain, Name, Quorum, Body, Opts) ->
    case async:get_identity() of
        {ok, _Async} ->
            case start_activity(Node, Domain, Name, Quorum, Body, Opts) of
                {ok, Activity} ->
                    try
                        async:wait(Activity)
                    catch
                        exit:{shutdown, local_lease_expired = Reason} ->
                            report_error(Domain, Name, Reason);
                        exit:{shutdown, {quorum_lost, _} = Reason} ->
                            report_error(Domain, Name, Reason);
                        exit:{shutdown, {leader_process_died, _} = Reason} ->
                            report_error(Domain, Name, Reason)
                    end;
                {error, Error} ->
                    report_error(Domain, Name, Error)
            end;
        not_async ->
            MaybeToken = get_activity_token(),
            misc:executing_on_new_process(
              fun () ->
                      {ok, _} = async:get_identity(),

                      case MaybeToken of
                          {ok, Token} ->
                              set_activity_token(Token);
                          not_activity ->
                              ok
                      end,

                      run_activity(Domain, Name, Quorum, Body, Opts)
              end)
    end.

start_activity(Name, Quorum, Body) ->
    start_activity(Name, Quorum, Body, []).

start_activity(Name, Quorum, Body, Opts) ->
    start_activity(default, Name, Quorum, Body, Opts).

start_activity(Domain, Name, Quorum, Body, Opts) ->
    start_activity(node(), Domain, Name, Quorum, Body, Opts).

start_activity(Node, Domain, Name, Quorum, Body, Opts) ->
    ActivityToken = case get_activity_token() of
                        {ok, Token} ->
                            true = (Token#activity_token.domain =:= Domain),
                            Token;
                        not_activity ->
                            make_fresh_activity_token(Domain)
                    end,

    start_activity_with_token(Node, ActivityToken, Name, Quorum, Body, Opts).

start_activity_with_token(Node, ActivityToken, Name, Quorum, Body, Opts) ->
    {ok, Async} = async:get_identity(),

    FinalOpts = merge_options(Opts, ActivityToken#activity_token.options),
    check_activity_body(Node, Body),
    call_wait_for_preconditions(Node, ActivityToken, Quorum, FinalOpts,
                                start_activity, [Async, Name, Body]).

register_process(Name, Quorum) ->
    register_process(Name, Quorum, []).

register_process(Name, Quorum, Opts) ->
    register_process(default, undefined, Name, Quorum, Opts).

register_process(DomainToken, Name, Quorum, Opts) ->
    register_process(default, DomainToken, Name, Quorum, Opts).

register_process(Domain, DomainToken, Name, Quorum, Opts) ->
    {ok, ActivityToken} = call_wait_for_preconditions(
                            node(),
                            make_fresh_activity_token(Domain, DomainToken),
                            Quorum, Opts, register_process, [Name, self()]),
    set_activity_token(ActivityToken).

switch_quorum(NewQuorum) ->
    switch_quorum(NewQuorum, []).

switch_quorum(NewQuorum, Opts) ->
    Activity            = get_activity_pid(),
    {ok, ActivityToken} = get_activity_token(),

    EffectiveOpts = Opts ++ ActivityToken#activity_token.options,
    call_wait_for_preconditions(node(),
                                ActivityToken,
                                NewQuorum, EffectiveOpts,
                                switch_quorum, [Activity]).

activate_quorum_nodes(Nodes) ->
    activate_quorum_nodes(Nodes, []).

activate_quorum_nodes(Nodes, Opts) ->
    update_quorum_nodes(?cut(lists:usort(_ ++ Nodes)), Opts).

deactivate_quorum_nodes(Nodes) ->
    deactivate_quorum_nodes(Nodes, []).

deactivate_quorum_nodes(Nodes, Opts) ->
    update_quorum_nodes(_ -- Nodes, Opts).

update_quorum_nodes(Fun) ->
    update_quorum_nodes(Fun, []).

update_quorum_nodes(Fun, Opts) ->
    run_activity(update_quorum_nodes, majority,
                 fun () ->
                         {ok, Nodes} = call(get_quorum_nodes),
                         NewNodes    = Fun(Nodes),
                         case switch_quorum([majority, {majority, NewNodes}]) of
                             ok ->
                                 call({set_quorum_nodes, Nodes, NewNodes});
                             Error ->
                                 Error
                         end
                 end, Opts).

%% gen_server callbacks
init([]) ->
    process_flag(priority, high),
    process_flag(trap_exit, true),

    {ok, #state{quorum_nodes       = undefined,
                leases             = sets:new(),
                local_lease_holder = undefined,

                activities = []}}.

handle_call({if_internal_process, Type, Pid, SubCall}, From, State) ->
    {noreply, handle_if_internal_process(Type, Pid, SubCall, From, State)};
handle_call({wait_for_preconditions,
             Domain, DomainToken,
             Lease, Quorum, Unsafe, SubCall, Timeout}, From, State) ->
    handle_wait_for_preconditions(Domain, DomainToken,
                                  Lease, Quorum, Unsafe, SubCall,
                                  Timeout, From, State);
handle_call(get_quorum_nodes, From, State) ->
    {noreply, handle_get_quorum_nodes(From, State)};
handle_call({set_quorum_nodes, OldNodes, NewNodes}, From, State) ->
    {noreply, handle_set_quorum_nodes(OldNodes, NewNodes, From, State)};
handle_call(Request, From, State) ->
    ?log_error("Received unexpected call ~p from ~p when state is~n~p",
               [Request, From, State]),
    {reply, nack, State}.

handle_cast(Msg, State) ->
    ?log_error("Received unexpected cast ~p when state is~n~p", [Msg, State]),
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, Reason}, State) ->
    {noreply, handle_down(MRef, Pid, Reason, State)};
handle_info({'EXIT', _Pid, _Reason} = Exit, State) ->
    ?log_error("Received unexpected exit message ~p. Exiting"),
    {stop, {unexpected_exit, Exit}, State};
handle_info(Info, State) ->
    ?log_error("Received unexpected message ~p when state is~n~p",
               [Info, State]),
    {noreply, State}.

terminate(Reason, State) ->
    terminate_all_activities(State, Reason).

%% internal functions
call_register_internal_process(Type, Pid) ->
    call_if_internal_process(Type, undefined,
                             {register_internal_process, Type, Pid}).

call_if_internal_process(Type, Pid, SubCall) ->
    call({if_internal_process, Type, Pid, SubCall}, infinity).

call_wait_for_preconditions(Node, Token, UserQuorum, Opts, Call, Args) ->
    Unsafe               = proplists:get_bool(unsafe, Opts),
    PreconditionsTimeout = proplists:get_value(preconditions_timeout, Opts,
                                               ?PRECONDITIONS_TIMEOUT),
    OuterTimeout         =
        proplists:get_value(timeout, Opts, PreconditionsTimeout + 5000),

    Lease       = Token#activity_token.lease,
    Domain      = Token#activity_token.domain,
    DomainToken = Token#activity_token.domain_token,
    TokenName   = Token#activity_token.name,

    Quorum  = convert_quorum(UserQuorum),
    SubCall = list_to_tuple([Call,
                             Domain,
                             DomainToken, TokenName, Quorum, Opts | Args]),

    call(Node, {wait_for_preconditions,
                Domain, DomainToken, Lease, Quorum, Unsafe,
                SubCall, PreconditionsTimeout}, OuterTimeout).

call(Call) ->
    call(Call, infinity).

call(Call, Timeout) ->
    call(node(), Call, Timeout).

call(Node, Call, Timeout) ->
    gen_server2:call({?SERVER, Node}, Call, Timeout).

handle_start_activity(Async,
                      Domain,
                      DomainToken, Name, Quorum, Opts, Body, From, State) ->
    ActivityToken = make_activity_token(Domain, DomainToken, Name, Opts, State),

    {Pid, MRef} = async:start(fun () ->
                                      set_activity_token(ActivityToken),
                                      run_body(Body)
                              end,
                              [monitor, {adopters, [Async]}]),
    gen_server2:reply(From, {ok, Pid}),
    add_activity(ActivityToken, Quorum, Opts, Pid, MRef, State).

handle_register_process(Domain,
                        DomainToken, Name, Quorum, Opts, Pid, From, State) ->
    ActivityToken = make_activity_token(Domain, DomainToken, Name, Opts, State),

    MRef = erlang:monitor(process, Pid),
    gen_server2:reply(From, {ok, ActivityToken}),
    add_activity(ActivityToken, Quorum, Opts, Pid, MRef, State).

handle_if_internal_process(Type, Pid, SubCall, From, State) ->
    ExpectedPid = extract_internal_process_pid(Type, State),

    case Pid =:= ExpectedPid of
        true ->
            handle_if_internal_process_subcall(Type, SubCall, From, State);
        false ->
            Reply = {error, {wrong_pid, Type, Pid, ExpectedPid}},
            gen_server2:reply(From, Reply),
            State
    end.

handle_if_internal_process_subcall(Type,
                                   {register_internal_process, Type, Pid},
                                   From, State) ->
    handle_register_internal_process(Type, Pid, From, State);
handle_if_internal_process_subcall(acquirer,
                                   {lease_acquired, Node}, From, State) ->
    handle_lease_acquired(Node, From, State);
handle_if_internal_process_subcall(acquirer,
                                   {lease_lost, Node}, From, State) ->
    handle_lease_lost(Node, From, State);
handle_if_internal_process_subcall(agent,
                                   {local_lease_granted, LocalLease},
                                   From, State) ->
    handle_local_lease_granted(LocalLease, From, State);
handle_if_internal_process_subcall(agent,
                                   {local_lease_expired, LocalLease},
                                   From, State) ->
    handle_local_lease_expired(LocalLease, From, State);
handle_if_internal_process_subcall(quorum_nodes_manager,
                                   {note_quorum_nodes, Nodes},
                                   From, State) ->
    handle_note_quorum_nodes(Nodes, From, State);
handle_if_internal_process_subcall(Type, SubCall, From, State) ->
    ?log_error("Received unexpected internal process ~p "
               "subcall ~p from ~p, state =~n~p",
               [Type, SubCall, From, State]),
    gen_server2:reply(From, nack),
    State.

handle_register_internal_process(Type, Pid, From, State) ->
    undefined = extract_internal_process_pid(Type, State),

    gen_server2:reply(From, {ok, self()}),

    MRef = erlang:monitor(process, Pid),
    set_internal_process(Type, {Pid, MRef}, State).

handle_down(MRef, Pid, Reason, State) ->
    R = functools:alternative(
          State, [handle_internal_process_down(MRef, Pid, Reason, _),
                  handle_activity_down(MRef, Pid, Reason, _)]),

    case R of
        {ok, NewState} ->
            NewState;
        false ->
            ?log_error("Received unexpected DOWN message ~p",
                       [{MRef, Pid, Reason}]),
            exit({unexpected_down, MRef, Pid, Reason})
    end.

handle_internal_process_down(MRef, Pid, Reason, State) ->
    functools:alternative(
      State,
      [handle_internal_process_down(Type, MRef, Pid, Reason, _) ||
          {Type, _} <- internal_processes()]).

handle_internal_process_down(Type, MRef, Pid, Reason, State) ->
    case extract_internal_process(Type, State) =:= {Pid, MRef} of
        true ->
            ?log_debug("Process ~p terminated with reason ~p",
                       [{Type, Pid}, Reason]),
            {ok, cleanup_after_internal_process(Type, State)};
        false ->
            false
    end.

handle_activity_down(MRef, Pid, Reason, State) ->
    case take_activity(MRef, #activity.mref, State) of
        not_found ->
            false;
        {ok, Activity, NewState} ->
            true = (Activity#activity.pid =:= Pid),

            case is_verbose(Activity)
                orelse not misc:is_normal_termination(Reason) of
                true ->
                    ?log_debug("Activity "
                               "terminated with reason ~p. Activity:~n~p",
                               [Reason, Activity]);
                false ->
                    ok
            end,

            {ok, NewState}
    end.

handle_lease_acquired(Node, From, State) ->
    gen_server2:reply(From, ok),
    misc:update_field(#state.leases, State, sets:add_element(Node, _)).

handle_lease_lost(Node, From, State) ->
    NewState0 = misc:update_field(#state.leases, State,
                                  sets:del_element(Node, _)),
    NewState  = check_quorums(NewState0, {lease_lost, Node}),

    gen_server2:reply(From, ok),
    NewState.

handle_local_lease_granted(LocalLease, From, State) ->
    undefined = State#state.local_lease_holder,

    gen_server2:reply(From, ok),
    State#state{local_lease_holder = LocalLease}.

handle_local_lease_expired(LocalLease, From, State) ->
    true = (State#state.local_lease_holder =:= LocalLease),

    NewState = expire_local_lease(State),
    gen_server2:reply(From, ok),
    NewState.

expire_local_lease(State) ->
    NewState = State#state{local_lease_holder = undefined},

    %% Terminate all activities that require local lease, that is, activities
    %% that requested a 'follower' quorum.
    %%
    %% Note that we don't clean up activities that simply requested this node
    %% as part of their quorum here, i.e., something like {all, [node()]}, nor
    %% do we remove the lease for node() from State#state.leases. This is
    %% handled when we receive a notification from leader_lease_acquirer that
    %% this lease was lost. leader_lease_acquirer is our source of truth with
    %% respect to which leases we have. Normally, we expect to receive the
    %% notification from leader_lease_acquirer before local lease expiriation
    %% notification from leader_lease_agent. But in pathological situations
    %% they can get reordered and touching State#state.leases would break
    %% internal invariants.
    Pred = ?cut(is_follower_quorum(_#activity.quorum)),
    terminate_activities(Pred, NewState, {shutdown, local_lease_expired}).

handle_note_quorum_nodes(QuorumNodes, From, State) ->
    %% The quorum nodes manager will only notify us of the quorum nodes once,
    %% right after registering. So our current quorum_nodes must be undefined.
    undefined = State#state.quorum_nodes,

    gen_server2:reply(From, ok),
    State#state{quorum_nodes = QuorumNodes}.

set_internal_process(Type, Value, State) ->
    setelement(internal_process_type_to_field(Type), State, Value).

internal_processes() ->
    [{agent, #state.agent},
     {acquirer, #state.acquirer},
     {quorum_nodes_manager, #state.quorum_nodes_manager}].

internal_process_type_to_field(Type) ->
    {Type, Field} = lists:keyfind(Type, 1, internal_processes()),
    Field.

extract_internal_process(Type, State) ->
    element(internal_process_type_to_field(Type), State).

extract_internal_process_pid(Type, State) ->
    case extract_internal_process(Type, State) of
        undefined ->
            undefined;
        {Pid, _MRef} ->
            Pid
    end.

cleanup_after_internal_process(Type, State) ->
    functools:chain(State,
                    [set_internal_process(Type, undefined, _),
                     cleanup_activities_after_internal_process(Type, _)]).

cleanup_activities_after_internal_process(agent, State) ->
    expire_local_lease(State);
cleanup_activities_after_internal_process(acquirer, State) ->
    NewState = State#state{leases = sets:new()},
    cleanup_after_leader_internal_process(acquirer, NewState);
cleanup_activities_after_internal_process(quorum_nodes_manager, State) ->
    NewState = State#state{quorum_nodes = undefined},
    cleanup_after_leader_internal_process(quorum_nodes_manager, NewState).

cleanup_after_leader_internal_process(Type, State) ->
    Pred = ?cut(quorum_requires_leader(_#activity.quorum)),
    terminate_activities(Pred, State,
                         {shutdown, {leader_process_died, Type}}).

terminate_activities([], _Reason) ->
    ok;
terminate_activities(Activities, Reason) ->
    ?log_debug("Terminating activities (reason is ~p):~n~p",
               [Reason, Activities]),

    lists:foreach(?cut(erlang:demonitor(_#activity.mref, [flush])), Activities),
    misc:terminate_and_wait([A#activity.pid || A <- Activities], Reason).

terminate_activities(Pred, State, Reason) ->
    {Matching, Rest} = lists:partition(Pred, State#state.activities),
    terminate_activities(Matching, Reason),

    State#state{activities = Rest}.

terminate_all_activities(State, Reason) ->
    terminate_activities(functools:const(true), State, Reason).

add_activity(Token, Quorum, Opts, Pid, MRef, State) ->
    Activity = #activity{pid          = Pid,
                         mref         = MRef,
                         domain       = Token#activity_token.domain,
                         domain_token = Token#activity_token.domain_token,
                         name         = Token#activity_token.name,
                         quorum       = Quorum,
                         options      = Opts},

    case is_verbose(Activity) of
        true ->
            ?log_debug("Added activity:~n~p", [Activity]);
        false ->
            ok
    end,

    add_activity(Activity, State).

add_activity(Activity, State)
  when is_record(Activity, activity) ->
    misc:update_field(#state.activities, State, [Activity | _]).

find_activity(Key, N, #state{activities = Activities}) ->
    case lists:keyfind(Key, N, Activities) of
        false ->
            not_found;
        A when is_record(A, activity) ->
            {ok, A}
    end.

take_activity(Key, N, #state{activities = Activities} = State) ->
    case lists:keytake(Key, N, Activities) of
        false ->
            not_found;
        {value, A, Rest} ->
            {ok, A, State#state{activities = Rest}}
    end.

is_leader(#state{local_lease_holder   = {Node, _},
                 acquirer             = Acquirer,
                 quorum_nodes_manager = QuorumNodesManager,
                 quorum_nodes         = QuorumNodes}) ->
    Node =:= node() andalso
        not lists:member(undefined,
                         [Acquirer, QuorumNodesManager, QuorumNodes]);
is_leader(_) ->
    false.

check_is_leader(State) ->
    case is_leader(State) of
        true ->
            ok;
        false ->
            {error, not_leader}
    end.

check_lease(leader, State) ->
    check_is_leader(State);
check_lease(ExpectedLease, #state{local_lease_holder = ActualLease}) ->
    case ExpectedLease =:= ActualLease of
        true ->
            ok;
        false ->
            {error, {no_lease,
                     [{required_lease, ExpectedLease},
                      {actual_lease, ActualLease}]}}
    end.

have_quorum(follower, State) ->
    true = (State#state.local_lease_holder =/= undefined),
    true;
have_quorum(Tag, State)
  when Tag =:= all;
       Tag =:= majority ->
    true = sets:is_set(State#state.quorum_nodes),
    have_quorum({Tag, State#state.quorum_nodes}, State);
have_quorum({all, Nodes}, #state{leases = Leases}) ->
    sets:is_subset(Nodes, Leases);
have_quorum({majority, Nodes}, #state{leases = Leases}) ->
    Required = sets:size(Nodes) div 2,
    sets:size(sets:intersection(Nodes, Leases)) > Required;
have_quorum(Quorums, State)
  when is_list(Quorums) ->
    lists:all(have_quorum(_, State), Quorums).

quorum_requires_leader(Quorum) ->
    not is_follower_quorum(Quorum).

is_follower_quorum(follower) ->
    true;
is_follower_quorum(_) ->
    false.

check_quorum_requires_leader(Quorum, State) ->
    case quorum_requires_leader(Quorum) of
        true ->
            check_is_leader(State);
        false ->
            ok
    end.

check_quorum(Unsafe, Quorum, State) ->
    case Unsafe of
        true ->
            ok;
        false ->
            case have_quorum(Quorum, State) of
                true ->
                    ok;
                false ->
                    Leases = sets:to_list(State#state.leases),

                    {error, {no_quorum,
                             [{required_quorum, Quorum},
                              {leases, Leases}]}}
            end
    end.

activity_lost_quorum_pred(Activity, State) ->
    Unsafe = proplists:get_bool(unsafe, get_options(Activity)),
    Quorum = Activity#activity.quorum,
    check_quorum(Unsafe, Quorum, State) =/= ok.

check_quorums(State, Reason) ->
    terminate_activities(activity_lost_quorum_pred(_, State),
                         State, {shutdown, {quorum_lost, Reason}}).

check_no_domain_conflicts(Domain, DomainToken, State) ->
    case find_activity(Domain, #activity.domain, State) of
        {ok, FoundActivity} ->
            case FoundActivity#activity.domain_token =:= DomainToken of
                true ->
                    ok;
                false ->
                    {error, {domain_conflict,
                             [{domain, Domain},
                              {domain_token, Domain},
                              {conflicting_activity, FoundActivity}]}}
            end;
        not_found ->
            ok
    end.

handle_wait_for_preconditions(Domain, DomainToken, Lease, Quorum,
                              false = _Unsafe, SubCall, Timeout, From, State) ->
    gen_server2:conditional(
      wait_for_preconditions_pred(Domain, DomainToken, Lease, Quorum, _),
      ?cut(handle_wait_for_preconditions_success(SubCall, From, _2)),
      Timeout,
      %% Capture the last check_preconditions error on timeout.
      handle_wait_for_preconditions_timeout(SubCall, From,
                                            Domain, DomainToken,
                                            Lease, Quorum, false, _)),

    {noreply, State};
handle_wait_for_preconditions(Domain, DomainToken, Lease, Quorum,
                              true = _Unsafe, SubCall, _Timeout, From, State) ->
    %% When unsafe is true exit early if check_conditions fails.
    handle_wait_for_preconditions_timeout(SubCall, From, Domain, DomainToken,
                                          Lease, Quorum, true, State).

wait_for_preconditions_pred(Domain, DomainToken, Lease, Quorum, State) ->
    check_preconditions(Domain, DomainToken,
                        Lease, Quorum, false, State) =:= ok.

check_preconditions(Domain, DomainToken, Lease, Quorum, Unsafe, State) ->
    functools:sequence_(
      [?cut(check_no_domain_conflicts(Domain, DomainToken, State)),
       ?cut(check_lease(Lease, State)),
       ?cut(check_quorum_requires_leader(Quorum, State)),
       ?cut(check_quorum(Unsafe, Quorum, State))]).

handle_wait_for_preconditions_success(SubCall, From, State) ->
    {noreply, handle_activity_subcall(SubCall, From, State)}.

handle_wait_for_preconditions_timeout(SubCall, From,
                                      Domain, DomainToken,
                                      RequiredLease, RequiredQuorum, Unsafe,
                                      #state{leases = RemoteLeases} = State) ->
    case check_preconditions(Domain, DomainToken,
                             RequiredLease, RequiredQuorum, Unsafe, State) of
        ok ->
            %% This should only be possible if Unsafe is true
            true = Unsafe,

            ?log_debug("Performing call ~p when 'unsafe' option set.~n"
                       "Required quorum: ~p~n"
                       "Leases: ~p",
                       [SubCall, RequiredQuorum, RemoteLeases]),
            handle_wait_for_preconditions_success(SubCall, From, State);
        Error ->
            gen_server2:reply(From, Error),
            {noreply, State}
    end.

handle_activity_subcall({start_activity,
                         Domain,
                         DomainToken,
                         ParentName, Quorum, Opts, Async, Name, Body},
                        From, State) ->
    FullName = ParentName ++ [Name],
    handle_start_activity(Async,
                          Domain,
                          DomainToken,
                          FullName, Quorum, Opts, Body, From, State);
handle_activity_subcall({register_process,
                         Domain,
                         DomainToken, ParentName, Quorum, Opts, Name, Pid},
                        From, State) ->
    FullName = ParentName ++ [Name],
    handle_register_process(Domain,
                            DomainToken,
                            FullName, Quorum, Opts, Pid, From, State);
handle_activity_subcall({switch_quorum,
                         Domain,
                         _DomainToken,
                         Name, Quorum, _Opts, Activity}, From, State) ->
    handle_switch_quorum(Domain, Name, Quorum, Activity, From, State);
handle_activity_subcall(Request, From, State) ->
    ?log_error("Received unexpected activity call ~p from ~p when state is~n~p",
               [Request, From, State]),
    gen_server2:reply(From, nack),
    State.

-spec convert_quorum(user_quorum()) -> quorum().
convert_quorum(FollowerQuorum)
  when FollowerQuorum =:= follower ->
    FollowerQuorum;
convert_quorum(LeaderQuorum) ->
    convert_leader_quorum(LeaderQuorum).

convert_leader_quorum(Tag)
  when Tag =:= all;
       Tag =:= majority ->
    Tag;
convert_leader_quorum({Tag, Nodes} = UserQuorum)
  when Tag =:= all;
       Tag =:= majority ->

    case sets:is_set(Nodes) of
        true ->
            UserQuorum;
        false ->
            true = is_list(Nodes),
            {Tag, sets:from_list(Nodes)}
    end;
convert_leader_quorum(Quorums)
  when is_list(Quorums) ->
    lists:map(fun convert_leader_quorum/1, Quorums).

check_activity_body(_Node, {_M, _F, _A}) ->
    ok;
check_activity_body(Node, Fun)
  when is_function(Fun, 0) ->
    case Node =:= node() of
        true ->
            ok;
        false ->
            %% Anonymous functions don't work well when they are called on
            %% other nodes. The other node would have to have the exact same
            %% version of the module (afair), or at least the exact same
            %% version of the anonymous function. All of this obviously makes
            %% backward compatibility hard. So we explicitly MFA's for remote
            %% calls.
            error(non_local_function_disallowed)
    end.

run_body({M, F, A}) ->
    erlang:apply(M, F, A);
run_body(Body)
  when is_function(Body, 0) ->
    Body().

make_activity_token(Domain, DomainToken, Name, Opts, State) ->
    {LeaseNode, _LeaseUUID} = Lease = State#state.local_lease_holder,
    true = (LeaseNode =:= node()),

    #activity_token{lease        = Lease,
                    domain_token = DomainToken,
                    domain       = Domain,
                    name         = Name,
                    options      = Opts}.

make_fresh_activity_token(Domain) ->
    make_fresh_activity_token(Domain, undefined).

make_fresh_activity_token(Domain, undefined) ->
    make_fresh_activity_token(Domain, couch_uuids:random());
make_fresh_activity_token(Domain, DomainToken) ->
    #activity_token{domain       = Domain,
                    domain_token = DomainToken,
                    name         = [],
                    lease        = leader,
                    options      = []}.

set_activity_token(Token) ->
    erlang:put('$leader_activities_token', Token).

get_activity_token() ->
    case erlang:get('$leader_activities_token') of
        undefined ->
            not_activity;
        Token when is_record(Token, activity_token) ->
            {ok, Token}
    end.

get_activity_pid() ->
    {ok, _} = get_activity_token(),

    case async:get_identity() of
        {ok, Async} ->
            Async;
        not_async ->
            self()
    end.

handle_switch_quorum(Domain, Name, NewQuorum, Pid, From, State) ->
    case take_activity(Pid, #activity.pid, State) of
        {ok, Activity, NewState} ->
            ?log_debug("Updating quorum for activity ~p to ~p",
                       [{Domain, Name, Pid}, NewQuorum]),

            gen_server2:reply(From, ok),
            add_activity(Activity#activity{quorum = NewQuorum}, NewState);
        not_found ->
            ?log_debug("Attempt to switch "
                       "quorum by an unknown/stale activity: ~p ",
                       [{Domain, Name, NewQuorum, Pid}]),

            gen_server2:reply(From, nack),
            State
    end.

handle_get_quorum_nodes(From, #state{quorum_nodes = QuorumNodes} = State) ->
    Reply =
        case QuorumNodes of
            undefined ->
                {error, no_quorum_nodes};
            _ ->
                {ok, sets:to_list(QuorumNodes)}
        end,

    gen_server2:reply(From, Reply),
    State.

handle_set_quorum_nodes(OldNodesList, NewNodesList, From,
                        #state{quorum_nodes         = QuorumNodes,
                               quorum_nodes_manager = {Mgr, _}} = State) ->
    OldNodes = sets:from_list(OldNodesList),
    NewNodes = sets:from_list(NewNodesList),

    case OldNodes =:= QuorumNodes of
        true ->
            ok = leader_quorum_nodes_manager:set_quorum_nodes(Mgr, NewNodes),
            gen_server2:reply(From, ok),
            State#state{quorum_nodes = NewNodes};
        false ->
            gen_server2:reply(From, {error, conflict}),
            State
    end;
handle_set_quorum_nodes(_OldNodesList, _NewNodesList, From, State) ->
    gen_server2:reply(From, {error, no_quorum_manager}),
    State.

report_error(Domain, Name, Error) ->
    ?log_error("Activity ~p failed with error ~p", [{Domain, Name}, Error]),
    {leader_activities_error, {Domain, Name}, Error}.

is_verbose(Activity) ->
    not proplists:get_bool(quiet, get_options(Activity)).

get_options(#activity{options = Options}) ->
    Options.

inheritable_options() ->
    [{unsafe, true}].

merge_option({Key, Value}, Options, ParentOptions) ->
    merge_option(Key, _ =:= Value, Options, ParentOptions);
merge_option(Key, Options, ParentOptions)
  when is_atom(Key) ->
    merge_option(Key, functools:const(true), Options, ParentOptions).

merge_option(Key, ValuePred, Options, ParentOptions) ->
    case proplists:is_defined(Key, Options) of
        true ->
            %% Explicitly set in by the caller, so ignoring whatever value the
            %% parent has.
            Options;
        false ->
            NotSet = make_ref(),
            case proplists:get_value(Key, ParentOptions, NotSet) of
                NotSet ->
                    %% Not set by the parent, so ignoring too.
                    Options;
                Value ->
                    case ValuePred(Value) of
                        true ->
                            [{Key, Value} | Options];
                        false ->
                            Options
                    end
            end
    end.

merge_options(Options, ParentOptions) ->
    lists:foldl(merge_option(_, _, ParentOptions),
                Options, inheritable_options()).
