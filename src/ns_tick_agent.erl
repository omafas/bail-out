%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_tick_agent).

-behavior(gen_server2).

-include("cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0, send_tick/2, time_offset_status/0]).

%% gen_server2 callbacks
-export([init/1, handle_info/2, handle_call/3, handle_cast/2]).

%% Maximum amount of time we will add to the first time offset interval in
%% order to prevent all the nodes from querying the Master in lock-step.
-define(INITIAL_INTERVAL_MAX_SLOP_MSEC, 500).

-record(state, {master, time_out_of_sync = false :: boolean()}).

-record(time_offset_request, {from :: node(), send_time_sys :: integer(),
                              send_time_mono :: integer()}).
-record(time_offset_reply, {from :: node(), request = #time_offset_request{},
                            reply_time_sys :: integer()}).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

send_tick(Nodes, TS) ->
    case cluster_compat_mode:is_cluster_65() of
        true ->
            gen_server2:abcast(Nodes, ?MODULE, {tick, node(), TS});
        false ->
            lists:foreach(notify(_, TS), Nodes)
    end.

time_offset_status() ->
    gen_server:call(?MODULE, get_time_offset_status).

%% callbacks
init([]) ->
    Master = mb_master:master_node(),

    Self = self(),
    ns_pubsub:subscribe_link(leader_events,
                             case _ of
                                 {new_leader, _} = Event ->
                                     Self ! Event;
                                 _ ->
                                     ok
                             end),

    Interval = time_offset_interval(),
    InitialIntervalSlop = rand:uniform(?INITIAL_INTERVAL_MAX_SLOP_MSEC),
    erlang:send_after(Interval + InitialIntervalSlop, self(), offset_timer),
    {ok, #state{master = Master}}.

handle_info({new_leader, NewMaster}, State) ->
    {noreply, State#state{master=NewMaster}};

handle_info(offset_timer, State) ->
    %% Re-arm the timer.
    erlang:send_after(time_offset_interval(), self(), offset_timer),

    maybe_send_time_offset_request(State, node()),
    {noreply, State};

handle_info(Info, State) ->
    ?log_warning("Received an unexpected message ~p", [Info]),
    {noreply, State}.

handle_call(get_time_offset_status, _From,  State) ->
    TimeOffsetStatus = {time_offset_status, State#state.time_out_of_sync},
    {reply, TimeOffsetStatus, State}.

handle_cast({tick, FromNode, TS}, State) ->
    {noreply, handle_tick(FromNode, TS, State)};

%% Reply to a time_offset_request with our time.
handle_cast(#time_offset_request{from = From} = Req, State) ->
    ReplyTime = os:system_time(millisecond),
    gen_server:cast({?MODULE, From},
                    #time_offset_reply{from = node(), request = Req,
                                       reply_time_sys = ReplyTime}),
    {noreply, State};
handle_cast(#time_offset_reply{from = From} = Reply,
            #state{master = From} = State) ->
    %% handle a reply from Master
    NowMono = erlang:monotonic_time(millisecond),
    {noreply, handle_time_offset_reply(Reply, NowMono, State)};
handle_cast(#time_offset_reply{from = From, request = Req}, State) ->
    %% handle a reply from a non-Master
    ?log_warning("Received a reply to a request ~p from non-master ~p",
                 [Req, From]),
    {noreply, State};

handle_cast(Cast, State) ->
    ?log_warning("Received an unexpected cast ~p", [Cast]),
    {noreply, State}.

%% internal
handle_tick(FromNode, TS, #state{master = Master} = State)
  when FromNode =:= Master ->
    notify(TS),
    State;
handle_tick(FromNode, _TS, #state{master = Master} = State) ->
    ?log_warning("Ignoring tick from a non-master node ~p. Master: ~p",
                 [FromNode, Master]),
    State.

maybe_send_time_offset_request(#state{master = undefined}, _Node) ->
    ok;
maybe_send_time_offset_request(#state{master = Node}, Node) ->
    %% We're the master; don't send request.
   ok;
maybe_send_time_offset_request(#state{master = Master}, _Node) ->
    case is_time_offset_enabled() of
        true ->
            send_time_offset_request(Master);
        false ->
            ok
    end.

send_time_offset_request(Master) ->
    NowSys = os:system_time(millisecond),
    NowMono = erlang:monotonic_time(millisecond),
    Req = #time_offset_request{from = node(), send_time_sys = NowSys,
                               send_time_mono = NowMono},
    gen_server:cast({?MODULE, Master}, Req).

handle_time_offset_reply(Reply, NodeReceiveTimeMono,
                         #state{time_out_of_sync = true} = State) ->
    %% We are currently out of sync.
    TimeOffsetThreshold = time_offset_go_in_sync_threshold(),
    RttThreshold = time_offset_rtt_threshold(),

    case is_time_in_sync(Reply, NodeReceiveTimeMono, RttThreshold,
                         TimeOffsetThreshold) of
        true ->
            %% We were out of sync and are now in sync.
            ?log_debug("now in sync"),
            State#state{time_out_of_sync = false};
        false ->
            %% Still out of sync.
            State
    end;
handle_time_offset_reply(Reply, NodeReceiveTimeMono,
                         #state{time_out_of_sync = false} = State) ->
    %% We are currently in sync.
    TimeOffsetThreshold = time_offset_go_out_of_sync_threshold(),
    RttThreshold = time_offset_rtt_threshold(),

    case is_time_in_sync(Reply, NodeReceiveTimeMono, RttThreshold,
                         TimeOffsetThreshold) of
        true ->
            %% Still in sync.
            State;
        false ->
            %% We were in sync and are now out of sync.
            ?log_debug("now out of sync"),
            State#state{time_out_of_sync = true}
    end.

notify(TS) ->
    notify(node(), TS).

notify(Node, TS) ->
    gen_event:notify({ns_tick_event, Node}, {tick, TS}).

is_time_in_sync(
  #time_offset_reply{request = Req, reply_time_sys = ReplyTimeSys},
  ReceiveTimeMono, RttThreshold, OffsetThreshold) ->
    #time_offset_request{send_time_sys = SendTimeSys,
                         send_time_mono = SendTimeMono } = Req,
    Rtt = ReceiveTimeMono - SendTimeMono,

    if Rtt > RttThreshold ->
            %% Ignore this response since the round trip time is too high.
            ?log_debug("Round trip time ~p is greater than "
                       "threshold ~p; ignoring time offset reply~n",
                       [Rtt, RttThreshold]),
            true;
       SendTimeSys - ReplyTimeSys > OffsetThreshold ->
            false;
       ReplyTimeSys - (SendTimeSys + Rtt) > OffsetThreshold ->
            false;
       true ->
            true
    end.

%% config internal

time_offset_default_values() ->
     [{enabled, true},
      %% Period in msec for querying the master for time offset.
      {interval, 5000},
      %% Maximum time offset request round trip time, in msec. If the RTT
      %% is greater than this, we don't check whether time is in sync.
      {rtt_threshold, 500},
      %% Maximum difference between node and Master system clock we will
      %% tolerate while still considering a node in sync, in msec.
      {threshold, 1000},
      %% The time difference above which we raise a "time out of sync"
      %% alert, represented as a percentage of "threshold".  Must be
      %% greater than 100.
      {out_of_sync_percentage, 120}].

time_offset_config_value(Key) ->
    Proplist = ns_config:read_key_fast(time_offset_cfg,
                                       time_offset_default_values()),
    proplists:get_value(Key, Proplist).

is_time_offset_enabled() ->
    time_offset_config_value(enabled).

time_offset_interval() ->
    time_offset_config_value(interval).

time_offset_rtt_threshold() ->
    time_offset_config_value(rtt_threshold).

time_offset_threshold() ->
    time_offset_config_value(threshold).

time_offset_out_of_sync_percentage() ->
    time_offset_config_value(out_of_sync_percentage).

%% In order to prevent oscillation between being in sync and out of sync,
%% the time offset which causes us to go "in sync" must be smaller than the
%% offset that causes us to go "out of sync".

time_offset_go_in_sync_threshold() ->
    time_offset_threshold().

time_offset_go_out_of_sync_threshold() ->
    trunc(time_offset_threshold()) *
              (time_offset_out_of_sync_percentage() / 100).

-ifdef(TEST).

%% Maximum difference between node and Master system clock we will
%% tolerate before raising a "time out of sync" alert, in msec.
-define(TIME_OFFSET_THRESHOLD_MSEC, 1000).

%% Maximum time offset request round trip time, in msec.
%% If the RTT is greater than this, we don't check whether time is in sync.
-define(RTT_THRESHOLD_MSEC, 500).

is_time_in_sync_tester(TimeDelta, Rtt) ->
    SendTimeSys = os:system_time(millisecond),
    SendTimeMono = erlang:monotonic_time(millisecond),
    ReplyTimeSys = SendTimeSys + TimeDelta,
    Req = #time_offset_request{from = node(), send_time_sys = SendTimeSys,
                               send_time_mono = SendTimeMono},
    Reply = #time_offset_reply{from = "master_node", request = Req,
                               reply_time_sys = ReplyTimeSys},
    ReceiveTimeMono = SendTimeMono + Rtt,

    is_time_in_sync(Reply, ReceiveTimeMono, ?RTT_THRESHOLD_MSEC,
                        ?TIME_OFFSET_THRESHOLD_MSEC).

%% With a small time delta and a small rtt, we should be in sync.
small_positive_delta_small_rtt_test() ->
    ?assert(is_time_in_sync_tester(200, 100)).

%% With a small time delta and a small rtt, we should be in sync.
small_negative_delta_small_rtt_test() ->
    ?assert(is_time_in_sync_tester(-200, 100)).

%% With a big time delta and a small rtt, we should be out of sync.
big_positive_delta_small_rtt_test() ->
    ?assertNot(is_time_in_sync_tester(1200, 100)).

%% With a big time delta and a small rtt, we should be out of sync.
big_negative_delta_small_rtt_test() ->
    ?assertNot(is_time_in_sync_tester(-1200, 100)).

%% With a small delta and a big rtt, we should be in sync.
small_positive_delta_big_rtt_test() ->
    ?assert(is_time_in_sync_tester(200, 600)).

%% With a small delta and a big rtt, we should be in sync.
small_negative_delta_big_rtt_test() ->
    ?assert(is_time_in_sync_tester(-200, 600)).

%% With a big delta and a big rtt, we should be in sync.
big_positive_delta_big_rtt_test() ->
    ?assert(is_time_in_sync_tester(1200, 600)).

%% With a big delta and a big rtt, we should be in sync.
big_negative_delta_big_rtt_test() ->
    ?assert(is_time_in_sync_tester(-1200, 600)).

%% Returns the new time_out_of_sync state.
time_offset_reply_tester(CurrentlyOutOfSync, TimeDelta) ->
    SendTimeSys = os:system_time(millisecond),
    SendTimeMono = erlang:monotonic_time(millisecond),
    ReplyTimeSys = SendTimeSys + TimeDelta,
    Req = #time_offset_request{from = node(), send_time_sys = SendTimeSys,
                               send_time_mono = SendTimeMono},
    Reply = #time_offset_reply{from = "master_node", request = Req,
                               reply_time_sys = ReplyTimeSys},
    %% For testing, we assume the RTT is zero.
    ReceiveTimeMono = SendTimeMono,
    #state{time_out_of_sync = NewOutOfSync} =
        handle_time_offset_reply(Reply, ReceiveTimeMono,
                                 #state{time_out_of_sync = CurrentlyOutOfSync}),
    NewOutOfSync.

time_offset_reply_test_() ->
    {foreach,
     %% per-test setup
     fun() ->
             meck:new(ns_config, [passthrough]),
             meck:expect(ns_config, read_key_fast,
                         fun (_, _) -> time_offset_default_values() end)
     end,
     %% per-test cleanup
     fun(_) ->
             meck:unload(ns_config)
     end,
     %% tests
     [
      {"In sync: if offset > threshold we should become out of sync",
       fun() ->
               OutOfSync = false,
               Delta = time_offset_go_out_of_sync_threshold() + 1,
               NowOutOfSync = time_offset_reply_tester(OutOfSync, Delta),
               ?assert(NowOutOfSync)
       end},
      {"In sync: if offset == threshold we should stay in sync",
       fun() ->
               OutOfSync = false,
               Delta = time_offset_go_out_of_sync_threshold(),
               NowOutOfSync = time_offset_reply_tester(OutOfSync, Delta),
               ?assertNot(NowOutOfSync)
       end},
      {"Out of sync: if offset == threshold we should become in sync",
       fun() ->
               OutOfSync = true,
               Delta = time_offset_go_in_sync_threshold(),
               NowOutOfSync = time_offset_reply_tester(OutOfSync, Delta),
               ?assertNot(NowOutOfSync)
       end},
      {"Out of sync: if offset > threshold we should stay out of sync",
       fun() ->
               OutOfSync = true,
               Delta = time_offset_go_in_sync_threshold() + 1,
               NowOutOfSync = time_offset_reply_tester(OutOfSync, Delta),
               ?assert(NowOutOfSync)
       end}
     ]}.
-endif.
