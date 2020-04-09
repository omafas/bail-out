%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2015 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc scheduler for compaction daemon
%%

-module(compaction_scheduler).

-include("ns_common.hrl").

-record(state, {start_ts,
                timer_ref,
                interval,
                message}).

-export([init/2, set_interval/2, schedule_next/1, start_now/1, cancel/1]).

-spec init(integer(), term()) -> #state{}.
init(Interval, Message) ->
    #state{start_ts=undefined,
           timer_ref=undefined,
           interval=Interval,
           message=Message}.

-spec set_interval(integer(), #state{}) -> #state{}.
set_interval(Interval, State) ->
    State#state{interval=Interval}.

-spec schedule_next(#state{}) -> #state{}.
schedule_next(#state{start_ts=StartTs0,
                     interval=CheckInterval,
                     message=Message} = State) ->
    Now = now_utc_seconds(),

    StartTs = case StartTs0 of
                  undefined ->
                      Now;
                  _ ->
                      StartTs0
              end,

    Diff = Now - StartTs,

    NewState0 =
        case Diff < CheckInterval of
            true ->
                RepeatIn = (CheckInterval - Diff),
                ?log_debug("Finished compaction for ~p too soon. Next run will be in ~ps",
                           [Message, RepeatIn]),
                {ok, NewTRef} = timer2:send_after(RepeatIn * 1000, Message),
                State#state{timer_ref=NewTRef};
            false ->
                self() ! Message,
                State#state{timer_ref=undefined}
        end,

    NewState0#state{start_ts=undefined}.

-spec start_now(#state{}) -> #state{}.
start_now(State) ->
    State#state{start_ts=now_utc_seconds(),
                timer_ref=undefined}.

-spec cancel(#state{}) -> #state{}.
cancel(#state{timer_ref=undefined} = State) ->
    State;
cancel(#state{timer_ref=TRef,
              message=Message} = State) ->
    {ok, cancel} = timer2:cancel(TRef),

    receive
        Message ->
            ok
    after 0 ->
            ok
    end,

    State#state{timer_ref=undefined}.

now_utc_seconds() ->
    calendar:datetime_to_gregorian_seconds(erlang:universaltime()).
