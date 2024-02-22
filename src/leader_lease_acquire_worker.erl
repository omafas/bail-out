%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(leader_lease_acquire_worker).

-include("cut.hrl").
-include("ns_common.hrl").

-export([spawn_link/2]).

-define(LEASE_TIME,        ?get_param(lease_time, 15000)).
-define(LEASE_RENEW_AFTER, ?get_param(lease_renew_after, 2000)).
-define(LEASE_GRACE_TIME,  ?get_param(lease_grace_time, 5000)).

-record(state, { parent :: pid(),
                 target :: node(),
                 uuid   :: binary(),

                 have_lease       :: boolean(),
                 lease_acquire_ts :: undefined | integer(),
                 lease_expire_ts  :: undefined | integer(),
                 retry_backoff    :: backoff:backoff(),
                 acquire_timer    :: misc:timer(acquire) }).

spawn_link(TargetNode, UUID) ->
    Parent = self(),
    proc_lib:spawn_link(?cut(init(Parent, TargetNode, UUID))).

init(Parent, TargetNode, UUID) ->
    process_flag(priority, high),

    RetryBackoff =
        backoff:new([{initial,    ?get_param(retry_initial, 500)},
                     {threshold,  ?get_param(retry_threshold, ?LEASE_TIME)},
                     {multiplier, ?get_param(retry_backoff, 2)}]),

    State = #state{parent = Parent,
                   target = TargetNode,
                   uuid   = UUID,

                   retry_backoff = RetryBackoff,

                   have_lease    = false,
                   acquire_timer = misc:create_timer(acquire)},

    loop(acquire_now(State)).

loop(State) ->
    receive
        acquire ->
            loop(handle_acquire_lease(State));
        {Ref, _} when is_reference(Ref) ->
            %% this is late gen_server call response, so just skipping
            loop(State);
        Msg ->
            ?log_warning("Received unexpected message ~p when state is~n~p",
                         [Msg, State]),
            loop(State)
    end.

%% To simplify the code elsewhere, we may try to "revoke" the lease when we
%% actually are not holding it. In such a case, we simply schedule another
%% attempt to acquire the lease.
revoke_lease(#state{have_lease = false} = State) ->
    retry_acquire(State);
revoke_lease(State) ->
    ok = leader_activities:lease_lost(parent(State), target_node(State)),
    retry_acquire(State#state{have_lease = false}).

handle_acquire_lease(State) ->
    Start   = erlang:monotonic_time(millisecond),
    Options = get_acquire_lease_options(Start, State),

    try call_acquire_lease(Options, State) of
        {ok, LeaseProps} ->
            handle_lease_acquired(Start, LeaseProps, State);
        {error, timeout} ->
            handle_acquire_timeout(Options, State);
        {error, lease_lost} ->
            handle_lease_lost(State);
        {error, {already_acquired, LeaseProps}} ->
            handle_lease_already_acquired(LeaseProps, State);
        Other ->
            handle_unexpected_response(Other, State)
    catch
        T:E ->
            handle_exception({T, E}, State)
    end.

get_acquire_lease_options(Start, State) ->
    Timeout = get_acquire_timeout(Start, State),

    [{timeout, Timeout},
     {period, ?LEASE_TIME}].

call_acquire_lease(Options, #state{uuid = UUID, target = TargetNode}) ->
    leader_lease_agent:acquire_lease(TargetNode, node(), UUID, Options).

handle_acquire_timeout(AcquireOptions, State) ->
    ?log_warning("Timeout while trying to acquire lease from ~p.~n"
                 "Acquire options were ~p",
                 [target_node(State), AcquireOptions]),

    functools:chain(State,
                    [%% Since we timed out, it most likely doesn't make sense
                     %% to wait much before retrying, so we just reset the
                     %% timeout to the lowest value.
                     reset_retry(_),

                     %% The way we pick the timeout pretty much ensures that
                     %% we don't have time to retry. So we backoff to try to
                     %% avoid this in future and also revoking the lease.
                     revoke_lease(_)]).

handle_lease_acquired(StartTime, LeaseProps, State) ->
    NewState =
        case State#state.have_lease of
            true ->
                State;
            false ->
                handle_fresh_lease_acquired(State)
        end,

    Now = erlang:monotonic_time(millisecond),
    update_inflight_histo(StartTime, Now, State),

    functools:chain(NewState,
                    [update_lease_expire_ts(StartTime,
                                            Now, LeaseProps, State, _),
                     reset_retry(_),
                     schedule_next_acquire(Now, _)]).

schedule_next_acquire(Now, #state{lease_acquire_ts = AcquireTS} = State) ->
    true = is_integer(AcquireTS),

    RenewTS  = AcquireTS + ?LEASE_RENEW_AFTER,
    TimeLeft = max(0, RenewTS - Now),

    acquire_after(TimeLeft, State).

update_lease_expire_ts(Start, Now, LeaseProps, PrevState, State) ->
    {_, TimeLeft} = lists:keyfind(time_left, 1, LeaseProps),
    AcquireTS     = compute_new_acquire_time(Start, Now, LeaseProps, PrevState),
    ExpireTS      = AcquireTS + TimeLeft - ?LEASE_GRACE_TIME,

    State#state{lease_expire_ts  = ExpireTS,
                lease_acquire_ts = AcquireTS}.

compute_new_acquire_time(Start, Now, LeaseProps, State) ->
    PrevAcquireEstimate = get_prev_acquire_estimate(Now, LeaseProps, State),
    pick_acquire_time_estimate(Start, PrevAcquireEstimate, State).

pick_acquire_time_estimate(Start, undefined, _State) ->
    Start;
pick_acquire_time_estimate(Start, PrevAcquireEstimate, State) ->
    true = is_integer(Start),

    if
        Start > PrevAcquireEstimate ->
            inc_counter(<<"used_start_estimate">>, State),
            add_histo(<<"start_time_minus_prev_acquire_estimate">>,
                      Start - PrevAcquireEstimate, State),
            Start;
        PrevAcquireEstimate > Start ->
            inc_counter(<<"used_prev_acquire_estimate">>, State),
            add_histo(<<"prev_acquire_estimate_minus_start_time">>,
                      PrevAcquireEstimate - Start, State),
            PrevAcquireEstimate;
        true ->
            Start
    end.

update_inflight_histo(Start, Now, State) ->
    TimeInFlight = Now - Start,
    add_histo(<<"time_inflight">>, TimeInFlight, State).

get_prev_acquire_estimate(_Now, _LeaseProps, #state{have_lease = false}) ->
    undefined;
get_prev_acquire_estimate(Now, LeaseProps,
                          #state{lease_acquire_ts = PrevAcquireTS} = State) ->
    SincePrevAcquire = get_time_since_prev_acquire(LeaseProps),
    get_prev_acquire_estimate(SincePrevAcquire,
                              Now, PrevAcquireTS, LeaseProps, State).

get_prev_acquire_estimate(undefined,
                          _Now, _PrevAcquireTS, _LeaseProps, _State) ->
    undefined;
get_prev_acquire_estimate(SincePrevAcquire,
                          Now, PrevAcquireTS, LeaseProps, State) ->
    Estimate = PrevAcquireTS + SincePrevAcquire,

    case Estimate =< Now of
        true ->
            Estimate;
        false ->
            ?log_warning("Lease period start time estimate is in the future. "
                         "The time on the agent node ~p must be "
                         "flowing at a faster pace.~n"
                         "Now: ~p, PrevAcquireTS: ~p, "
                         "SincePrevAcquire: ~p, LeaseProps: ~p",
                         [target_node(State), Now,
                          PrevAcquireTS, SincePrevAcquire, LeaseProps]),
            inc_counter(<<"prev_acquire_estimate_in_future">>, State),
            undefined
    end.

get_time_since_prev_acquire(LeaseProps) ->
    case proplists:get_value(time_since_prev_acquire, LeaseProps) of
        Value when is_integer(Value), Value >= 0 ->
            Value;
        _ ->
            undefined
    end.

handle_lease_already_acquired(LeaseProps, State) ->
    {node, Node}          = lists:keyfind(node, 1, LeaseProps),
    {uuid, UUID}          = lists:keyfind(uuid, 1, LeaseProps),
    {time_left, TimeLeft} = lists:keyfind(time_left, 1, LeaseProps),

    ?log_warning("Failed to acquire lease from ~p "
                 "because its already taken by ~p (valid for ~bms)",
                 [target_node(State), {Node, UUID}, TimeLeft]),

    functools:chain(State,
                    [reset_retry(_),

                     %% Being pessimistic here and assuming that our
                     %% communication to the remote node was instantaneous.
                     acquire_after(TimeLeft, _)]).

handle_unexpected_response(Resp, State) ->
    ?log_warning("Received unexpected "
                 "response ~p from node ~p", [Resp, target_node(State)]),

    exit({unexpected_response, Resp}).

handle_exception(Exception, State) ->
    ?log_warning("Failed to acquire lease from ~p: ~p",
                 [target_node(State), Exception]),

    revoke_lease(backoff_retry(State)).

handle_lease_lost(State) ->
    ?log_warning("Node ~p told us that we lost its lease", [target_node(State)]),

    %% this is supposed to be a very short condition, so we don't backoff here
    %% and even update the round trip time
    revoke_lease(State).

%% Since acquire_lease internally is just a gen_server call, we can be sure
%% that our message either reaches the worker process on target node or we get
%% an exception as a result of the distribution connection between the nodes
%% being closed. So we want to pick the timeout as long as possible, but so
%% that we can process the revocation in a timely way.
get_acquire_timeout(_Now, #state{have_lease = false}) ->
    %% Well, if that's not enough, we probably won't be able to keep the lease
    %% anyway.
    ?LEASE_TIME;
get_acquire_timeout(Now, #state{have_lease      = true,
                                lease_expire_ts = ExpireTS}) ->
    max(ExpireTS - Now, 0).

get_retry_timeout(State) ->
    backoff:get_timeout(State#state.retry_backoff).

backoff_retry(State) ->
    misc:update_field(#state.retry_backoff, State, backoff:next(_)).

reset_retry(State) ->
    misc:update_field(#state.retry_backoff, State, backoff:reset(_)).

acquire_now(State) ->
    acquire_after(0, State).

acquire_after(Timeout, State) ->
    misc:update_field(#state.acquire_timer, State, misc:arm_timer(Timeout, _)).

target_node(#state{target = Node}) ->
    Node.

parent(#state{parent = Parent}) ->
    Parent.

retry_acquire(State) ->
    acquire_after(get_retry_timeout(State), State).

handle_fresh_lease_acquired(#state{uuid   = LeaseUUID,
                                   parent = Parent,
                                   target = TargetNode} = State) ->
    ?log_info("Acquired lease from node ~p (lease uuid: ~p)",
              [TargetNode, LeaseUUID]),
    ok = leader_activities:lease_acquired(Parent, TargetNode),

    State#state{have_lease = true}.

add_histo(Name, Value, State) ->
    ns_server_stats:notify_histogram(build_stat_name(Name, State), Value).

inc_counter(Name, State) ->
    ns_server_stats:notify_counter(build_stat_name(Name, State)).

build_stat_name(Name, State) ->
    {<<"lease_acquirer_", Name/binary>>, [{node, target_node(State)}]}.
