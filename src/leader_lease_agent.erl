%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(leader_lease_agent).

-behaviour(gen_server2).

-export([start_link/0]).

-export([get_current_lease/0, get_current_lease/1,
         acquire_lease/4, abolish_leases/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("cut.hrl").
-include("ns_common.hrl").

-define(SERVER, ?MODULE).

-type lease_ts() :: integer().
-type lease_state() :: active | expiring.

-record(lease_holder, { uuid :: binary(),
                        node :: node() }).

-record(lease, { holder      :: #lease_holder{},
                 %% acquired_at can be undefined when the lease is recovered
                 %% from disk and exact acquisition time is unknown
                 acquired_at :: undefined | lease_ts(),
                 expires_at  :: lease_ts(),
                 timer       :: misc:timer(),
                 state       :: lease_state() }).

-record(state, { lease           :: undefined | #lease{},
                 persisted_lease :: undefined | list() }).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

get_current_lease() ->
    get_current_lease(node()).

get_current_lease(Node) ->
    gen_server2:call({?SERVER, Node}, get_current_lease).

acquire_lease(WorkerNode, Node, UUID, Options) ->
    Timeout = proplists:get_value(timeout, Options),
    true = (Timeout =/= undefined),

    call_acquire_lease(WorkerNode, Node, UUID, Options, Timeout).

abolish_leases(WorkerNodes, Node, UUID) ->
    gen_server2:abcast(WorkerNodes, ?SERVER, {abolish_lease, Node, UUID}).

%% gen_server callbacks
init([]) ->
    process_flag(priority, high),
    process_flag(trap_exit, true),

    {ok, _} = leader_activities:register_agent(self()),
    {ok, maybe_recover_persisted_lease(#state{})}.

handle_call({acquire_lease, Node, UUID, Options}, From, State) ->
    Caller = #lease_holder{node = Node, uuid = UUID},
    {noreply, handle_acquire_lease(Caller, Options, From, State)};
handle_call(get_current_lease, From, State) ->
    {noreply, handle_get_current_lease(From, State)};
handle_call(Request, From, State) ->
    ?log_warning("Unexpected call ~p from ~p when the state is:~n~p",
                 [Request, From, State]),
    {reply, nack, State}.

handle_cast({abolish_lease, Node, UUID}, State) ->
    Caller = #lease_holder{node = Node, uuid = UUID},
    {noreply, handle_abolish_lease(Caller, State)};
handle_cast(Msg, State) ->
    ?log_warning("Unexpected cast ~p when the state is:~n~p",
                 [Msg, State]),
    {noreply, State}.

handle_info({lease_expired, Holder}, State) ->
    {noreply, handle_lease_expired(Holder, State)};
handle_info(Info, State) ->
    ?log_warning("Unexpected message ~p when the state is:~n~p",
                 [Info, State]),
    {noreply, State}.

terminate(_Reason, #state{lease = undefined}) ->
    ok;
terminate(Reason, #state{lease = Lease} = State) ->
    handle_terminate(Reason, Lease#lease.state, State).

%% internal functions
call_acquire_lease(_WorkerNode, _Node, _UUID, _Options, Timeout)
  when Timeout =< 0 ->
    {error, timeout};
call_acquire_lease(WorkerNode, Node, UUID, Options, Timeout) ->
    try
        misc:executing_on_new_process(
          fun () ->
                  gen_server2:call({?SERVER, WorkerNode},
                                   {acquire_lease, Node, UUID, Options},
                                   infinity)
          end, [{abort_after, Timeout}])
    catch
        exit:timeout ->
            {error, timeout}
    end.

handle_acquire_lease(Caller, Options, From, State) ->
    case validate_acquire_lease_period(Options) of
        {ok, Period} ->
            do_handle_acquire_lease(Caller, Period, From, State);
        Error ->
            gen_server2:reply(From, Error),
            State
    end.

validate_acquire_lease_period(Options) ->
    validate_option(period, Options, fun is_integer/1).

validate_option(Key, Options, Pred) ->
    Value = proplists:get_value(Key, Options),
    case Pred(Value) of
        true ->
            {ok, Value};
        false ->
            {error, {bad_option, Key, Value}}
    end.

do_handle_acquire_lease(Caller, Period, From,
                        #state{lease = undefined} = State) ->
    ?log_debug("Granting lease to ~p for ~bms", [Caller, Period]),
    grant_lease(Caller, Period, From, State);
do_handle_acquire_lease(Caller, Period, From, #state{lease = Lease} = State) ->
    case Lease#lease.holder =:= Caller of
        true ->
            case Lease#lease.state of
                active ->
                    extend_lease(Period, From, State);
                expiring ->
                    gen_server2:reply(From, {error, lease_lost}),
                    State
            end;
        false ->
            gen_server2:reply(From, {error, {already_acquired,
                                             build_lease_props(Lease)}}),
            State
    end.

grant_lease(Caller, Period, From, #state{lease = Lease} = State) ->
    true  = (Lease =:= undefined),

    %% this is not supposed to take long, so doing this in main process
    notify_local_lease_granted(self(), Caller),
    grant_lease_dont_notify(Caller, Period,
                            grant_lease_reply(From, _),
                            State).

grant_lease_reply(From, Lease) ->
    LeaseProps = build_lease_props(Lease#lease.acquired_at, Lease),
    gen_server2:reply(From, {ok, LeaseProps}).

grant_lease_dont_notify(Caller, Period, HandleResult, State)
  when is_function(HandleResult, 1) ->
    NewState = functools:chain(State,
                               [grant_lease_update_state(Caller, Period, _),
                                persist_fresh_lease(_)]),
    HandleResult(NewState#state.lease),

    NewState.

grant_lease_update_state(Caller, Period, State) ->
    Now = get_now(),
    grant_lease_update_state(Now, Now, Caller, Period, State).

grant_lease_update_state(Now, AcquiredAt, Caller, PeriodMs, State) ->
    Period    = erlang:convert_time_unit(PeriodMs, millisecond, native),
    ExpiresAt = Now + Period,
    Timer     = misc:create_timer(PeriodMs, {lease_expired, Caller}),

    NewLease = #lease{holder      = Caller,
                      acquired_at = AcquiredAt,
                      expires_at  = ExpiresAt,
                      timer       = Timer,
                      state       = active},

    State#state{lease = NewLease}.

extend_lease(Period, From, #state{lease = Lease} = State)
  when Lease =/= undefined ->
    cancel_timer(Lease),
    grant_lease_dont_notify(Lease#lease.holder,
                            Period,
                            extend_lease_handle_result(From, State, _),
                            State).

cancel_timer(Lease) ->
    misc:update_field(#lease.timer, Lease, misc:cancel_timer(_)).

extend_lease_handle_result(From, State, Lease) ->
    AcquiredAt  = Lease#lease.acquired_at,
    LeaseProps0 = build_lease_props(AcquiredAt, Lease),
    LeaseProps  = maybe_add_time_since_prev_acquire(AcquiredAt,
                                                    State, LeaseProps0),

    gen_server2:reply(From, {ok, LeaseProps}).

maybe_add_time_since_prev_acquire(AcquiredAt, State, LeaseProps) ->
    PrevLease      = State#state.lease,
    PrevAcquiredAt = PrevLease#lease.acquired_at,

    case PrevAcquiredAt of
        undefined ->
            LeaseProps;
        _ when is_integer(PrevAcquiredAt) ->
            true = (AcquiredAt >= PrevAcquiredAt),

            SincePrevAcquire   = AcquiredAt - PrevAcquiredAt,
            SincePrevAcquireMs =
                misc:convert_time_unit(SincePrevAcquire, millisecond),

            [{time_since_prev_acquire, SincePrevAcquireMs} | LeaseProps]
    end.

handle_get_current_lease(From, #state{lease = Lease} = State) ->
    Reply = case Lease of
                undefined ->
                    {error, no_lease};
                _ ->
                    {ok, build_lease_props(Lease)}
            end,

    gen_server2:reply(From, Reply),

    State.

handle_abolish_lease(Caller, #state{lease = Lease} = State) ->
    ?log_debug("Received abolish lease request from ~p when lease is ~p",
               [Caller, Lease]),

    case can_abolish_lease(Caller, Lease) of
        true ->
            ?log_debug("Expiring abolished lease"),

            %% Passing lease holder instead of Caller here due to possible
            %% node rename. See can_abolish_lease for details.
            start_expire_lease(Lease#lease.holder,
                               State#state{lease = cancel_timer(Lease)});
        false ->
            ?log_debug("Ignoring stale abolish request"),
            State
    end.

can_abolish_lease(_Caller, undefined) ->
    false;
can_abolish_lease(Caller, #lease{state  = State,
                                 holder = Holder}) ->
    %% This is not exactly clean, but we only compare the UUIDs here to deal
    %% with node renames. We restart leader related processes on rename, but
    %% only after node name has changed. So an attempt to abolish the lease
    %% will fail.
    %%
    %% We could of course use node UUIDs instead of node names, but that would
    %% complicate debugging quite significantly.
    State =:= active andalso
        Holder#lease_holder.uuid =:= Caller#lease_holder.uuid.

handle_lease_expired(Holder, State) ->
    ?log_debug("Lease held by ~p expired. Starting expirer.", [Holder]),
    start_expire_lease(Holder, State).

start_expire_lease(Holder, #state{lease = Lease} = State) ->
    true = (Lease#lease.holder =:= Holder),
    true = (Lease#lease.state =:= active),

    Self = self(),
    gen_server2:async_job(?cut(notify_local_lease_expired(Self, Holder)),
                          handle_expire_done(Holder, _, _)),

    NewLease = Lease#lease{state = expiring},
    State#state{lease = NewLease}.

handle_expire_done(Holder, Reply, #state{lease = Lease} = State) ->
    ok       = Reply,
    true     = (Lease#lease.holder =:= Holder),
    expiring = Lease#lease.state,

    remove_persisted_lease(),

    {noreply, State#state{lease           = undefined,
                          persisted_lease = undefined}}.

handle_terminate(Reason, active, State) ->
    ?log_warning("Terminating with reason ~p "
                 "when we own an active lease:~n~p~n"
                 "Persisting updated lease.",
                 [Reason, State#state.lease]),
    persist_lease(State);
handle_terminate(Reason, expiring, State) ->
    ?log_warning("Terminating with reason ~p when lease is expiring:~n~p~n"
                 "Removing the persisted lease.",
                 [Reason, State#state.lease]),

    %% Even though we haven't finished expiring the lease, it's safe to remove
    %% the persisted lease: the leader_activites process will cleanup after
    %% us. If we get restarted, we'll first have to register with
    %% leader_activities again, so we won't be able to grant a lease before
    %% all old activities are terminated.
    remove_persisted_lease().

build_lease_props(Lease) ->
    build_lease_props(undefined, Lease).

build_lease_props(undefined, Lease) ->
    build_lease_props(get_now(), Lease);
build_lease_props(Now, #lease{holder = Holder} = Lease) ->
    [{node,      Holder#lease_holder.node},
     {uuid,      Holder#lease_holder.uuid},
     {time_left, time_left_ms(Now, Lease)},
     {status,    Lease#lease.state}].

time_left_ms(Now, #lease{expires_at = ExpiresAt}) ->
    TimeLeft = misc:convert_time_unit(ExpiresAt - Now, millisecond),

    %% Sometimes the expiration message may be a bit late, or maybe we're busy
    %% doing other things. Return zero in those cases. It essentially means
    %% that the lease is about to expire.
    max(0, TimeLeft).

parse_lease_props(Dump) ->
    misc:parse_term(Dump).

lease_path() ->
    path_config:component_path(data, "leader_lease").

persist_lease(State) ->
    persist_lease(undefined, State).

persist_lease(Now, #state{lease           = Lease,
                          persisted_lease = PersistedProps} = State) ->
    true = (Lease =/= undefined),

    LeaseProps = build_lease_props(Now, Lease),
    case LeaseProps =:= PersistedProps of
        true ->
            State;
        false ->
            misc:create_marker(lease_path(),
                               [misc:dump_term(LeaseProps), $\n]),
            State#state{persisted_lease = LeaseProps}
    end.

persist_fresh_lease(#state{lease = Lease} = State) ->
    AcquiredAt = Lease#lease.acquired_at,
    persist_lease(AcquiredAt, State).

remove_persisted_lease() ->
    misc:remove_marker(lease_path()).

load_lease_props() ->
    try
        do_load_lease_props()
    catch
        T:E ->
            ?log_error("Can't read the lease because "
                       "of ~p. Going to ignore.", [{T, E}]),
            catch remove_persisted_lease(),
            not_found
    end.

do_load_lease_props() ->
    case misc:read_marker(lease_path()) of
        {ok, Data} ->
            {ok, parse_lease_props(Data)};
        false ->
            not_found
    end.

maybe_recover_persisted_lease(State) ->
    case load_lease_props() of
        {ok, Props} ->
            ?log_warning("Found persisted lease ~p", [Props]),
            recover_lease_from_props(Props, State);
        not_found ->
            State
    end.

recover_lease_from_props(Props, State) ->
    Node     = misc:expect_prop_value(node, Props),
    UUID     = misc:expect_prop_value(uuid, Props),
    TimeLeft = misc:expect_prop_value(time_left, Props),

    Holder = #lease_holder{node = Node,
                           uuid = UUID},

    notify_local_lease_granted(self(), Holder),
    grant_lease_update_state(get_now(), undefined, Holder, TimeLeft, State).

unpack_lease_holder(Holder) ->
    {Holder#lease_holder.node,
     Holder#lease_holder.uuid}.

notify_local_lease_granted(Pid, Holder) ->
    ok = leader_activities:local_lease_granted(Pid,
                                               unpack_lease_holder(Holder)).

notify_local_lease_expired(Pid, Holder) ->
    ok = leader_activities:local_lease_expired(Pid,
                                               unpack_lease_holder(Holder)).

get_now() ->
    erlang:monotonic_time().
