%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc scheduler for compaction daemon
%%

-module(compaction_scheduler).

-include("ns_common.hrl").

-record(state, {start_ts,
                timer,
                interval,
                message}).

-export([init/2, set_interval/2, schedule_next/1, start_now/1, cancel/1]).

-spec init(integer(), term()) -> #state{}.
init(Interval, Message) ->
    #state{start_ts = undefined,
           timer = misc:create_timer(Message),
           interval = Interval,
           message = Message}.

-spec set_interval(integer(), #state{}) -> #state{}.
set_interval(Interval, State) ->
    State#state{interval=Interval}.

-spec schedule_next(#state{}) -> #state{}.
schedule_next(#state{start_ts = StartTs,
                     interval = CheckInterval,
                     timer = Timer,
                     message = Message} = State) ->
    Diff = case StartTs of
               undefined ->
                   0;
               _ ->
                   now_utc_seconds() - StartTs
           end,

    Timeout =
        case Diff < CheckInterval of
            true ->
                RepeatIn = (CheckInterval - Diff),
                ?log_debug("Finished compaction for ~p too soon. "
                           "Next run will be in ~ps", [Message, RepeatIn]),
                RepeatIn * 1000;
            false ->
                0
        end,

    State#state{start_ts = undefined,
                timer = misc:arm_timer(Timeout, Timer)}.

-spec start_now(#state{}) -> #state{}.
start_now(State) ->
    State#state{start_ts = now_utc_seconds()}.

-spec cancel(#state{}) -> #state{}.
cancel(#state{timer = Timer} = State) ->
    State#state{timer = misc:cancel_timer(Timer)}.

now_utc_seconds() ->
    calendar:datetime_to_gregorian_seconds(erlang:universaltime()).
