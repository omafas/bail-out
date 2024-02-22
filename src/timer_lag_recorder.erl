%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% This process sends itself a message using send_after and then measures
%% the lag of when the message is received vs when it is expected.

-module(timer_lag_recorder).

-include_lib("kernel/include/logger.hrl").
-include("ns_common.hrl").

%% How often to send ourself a message in milliseconds.
-define(TIMER_INTERVAL, 1000).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    send_check_time_msg(),
    {ok, #state{}}.

handle_call(Msg, _From, _State) ->
    erlang:error({unknown_msg, Msg}).

handle_cast(Msg, _State) ->
    erlang:error({unknown_cast, Msg}).

report_missed_msgs(Skipped, Lag) when Skipped > 10 ->
    ?LOG_ERROR("Detected time forward jump (or too large "
               "erlang scheduling latency).  Skipping ~w "
               "samples (or ~w milliseconds)",
               [Skipped, Lag]);
report_missed_msgs(Skipped, _Lag) when Skipped > 0 ->
    ?log_warning("Skipped ~p 'check_time' messages", [Skipped]);
report_missed_msgs(_Skipped, _Lag) ->
    ok.

handle_info({check_time, ExpectedTime}, State) ->
    TimeNow = erlang:monotonic_time(millisecond),
    Lag = TimeNow - ExpectedTime,
    ns_server_stats:notify_histogram(<<"timer_lag">>, Lag),

    Skipped = trunc(Lag / ?TIMER_INTERVAL),
    report_missed_msgs(Skipped, Lag),

    send_check_time_msg(),

    {noreply, State}.

send_check_time_msg() ->
    ExpectedTime = erlang:monotonic_time(millisecond) + ?TIMER_INTERVAL,
    erlang:send_after(ExpectedTime, self(), {check_time, ExpectedTime},
                      [{abs, true}]).
