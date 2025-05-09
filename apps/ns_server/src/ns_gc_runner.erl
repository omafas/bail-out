%% @author Couchbase <info@couchbase.com>
%% @copyright 2022-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Periodic Garbage Collection: This module will do periodic garbage collection
%% of the live processes on the node. The collection is spread out per process
%% over the configured interval.

-module(ns_gc_runner).

-behaviour(gen_server).

-include("ns_common.hrl").

-define(SERVER, {local, ?MODULE}).

-export([start_link/0]).

-export([handle_call/3, handle_cast/2, handle_info/2, init/1,
         terminate/2]).

-record(state, {is_gc_enabled :: boolean(),
                pid_list :: [pid()],
                max_gc_duration:: integer(),
                timer_ref:: reference() | undefined}).

%%
%% API
%%
start_link() ->
    gen_server:start_link(?SERVER, ?MODULE, [], []).

%%
%% Helper Functions
%%
garbage_collect_process(Pid, MaxDuration) ->
    Start = os:timestamp(),
    Rv = erlang:garbage_collect(Pid),
    CurrDuration = timer:now_diff(os:timestamp(), Start),
    ns_server_stats:notify_histogram(<<"gc_duration">>, 1000000,
                                     microsecond, CurrDuration),
    {Rv, erlang:max(MaxDuration, CurrDuration)}.

send_gc_msg() ->
    Interval = ?get_param(gc_pass_interval, 1000),
    send_gc_msg(Interval).

send_gc_msg(Interval) ->
    erlang:send_after(Interval, self(), collect_garbage).

process_gc_tick(Pid, MaxDuration) ->
    {RV, NewMaxDuration} = garbage_collect_process(Pid, MaxDuration),
    TimerRef = case RV of
                true ->
                    send_gc_msg();
                _ ->
                    Interval = ?get_param(gc_fail_interval, 100),
                    send_gc_msg(Interval)
    end,
    {NewMaxDuration, TimerRef}.

handle_config_event({{ns_gc_runner, gc_state}, Value}, Pid) ->
    Pid ! {gc_state_change_event, Value},
    Pid;
handle_config_event(_, Pid) ->
    Pid.

%%
%% gen_server callbacks
%%
init([]) ->
    {TimerRef, GcEnabled} = case ?get_param(gc_state, enabled) of
                                enabled ->
                                    {send_gc_msg(), true};
                                _ ->
                                    {undefined, false}
                            end,
    ns_pubsub:subscribe_link(ns_config_events, fun handle_config_event/2,
                             self()),
    {ok, #state{is_gc_enabled = GcEnabled,
                pid_list = [],
                max_gc_duration = 0,
                timer_ref = TimerRef}}.

handle_call(Request, _From, State) ->
    ?log_warning("Unexpected handle_call(~p, ~p)", [Request, State]),
    {reply, {unhandled, ?MODULE, Request}, State}.

handle_cast(Msg, State) ->
    ?log_warning("Unexpected handle_cast(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_info({gc_state_change_event, _NewState = disabled},
            #state{is_gc_enabled = true, timer_ref = TimerRef} = State) ->
    ?log_debug("GC is now disabled"),
    erlang:cancel_timer(TimerRef),
    misc:flush(collect_garbage),
    {noreply, State#state{is_gc_enabled=false,
                          pid_list=[], timer_ref=undefined}};
handle_info({gc_state_change_event, _NewState = disabled},
            #state{is_gc_enabled = false} = State) ->
    ?log_debug("GC is already disabled"),
    {noreply, State};
handle_info({gc_state_change_event, _NewState = enabled},
            #state{is_gc_enabled = false} = State) ->
    ?log_debug("GC is now enabled"),
    TimerRef = send_gc_msg(),
    {noreply, State#state{is_gc_enabled=true, timer_ref=TimerRef}};
handle_info({gc_state_change_event, _NewState = enabled},
            #state{is_gc_enabled = true} = State) ->
    ?log_debug("GC is already enabled"),
    {noreply, State};
handle_info(collect_garbage, #state{is_gc_enabled = false} = State) ->
    ?log_warning("Unexpected handle_info call ~p", [State]),
    misc:flush(collect_garbage),
    {noreply, State};
handle_info(collect_garbage, #state{pid_list = [],
                                    max_gc_duration = MaxGcDuration} = State) ->
    NewProcessList = erlang:processes(),
    ?log_debug("GC populating new pid list of size=~p, prevMaxGcDuration=~p us",
               [length(NewProcessList), MaxGcDuration]),
    handle_info(collect_garbage, State#state{pid_list=NewProcessList,
                                             max_gc_duration=0});
handle_info(collect_garbage, #state{pid_list = PidList,
                                    max_gc_duration = MaxDuration,
                                    timer_ref = PrevTimerRef} = State) ->
    erlang:cancel_timer(PrevTimerRef),
    misc:flush(collect_garbage),
    [CurrPid | RestPids] = PidList,
    {NewMaxDuration, TimerRef} = process_gc_tick(CurrPid, MaxDuration),
    {noreply, State#state{pid_list=RestPids,
                          max_gc_duration=NewMaxDuration,
                          timer_ref=TimerRef}};
handle_info(Msg, State) ->
    ?log_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
