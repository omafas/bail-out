%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015 Couchbase, Inc.
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
-module(base_stats_collector).

-behaviour(gen_server).

-include("ns_common.hrl").

-include("ns_stats.hrl").

%% API
-export([start_link/2, start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% helpers
-export([calculate_counters/5]).

-record(state, {module,
                impl_state,
                count = 0,
                prev_counters,
                prev_ts = 0}).

start_link(Module, InitParams) ->
    gen_server:start_link(?MODULE, [Module, InitParams], []).

start_link(Name, Module, InitParams) ->
    gen_server:start_link(Name, ?MODULE, [Module, InitParams], []).

init([Module, InitParams]) ->
    {ok, State} = Module:init(InitParams),
    ns_pubsub:subscribe_link(ns_tick_event),
    {ok, #state{module = Module,
                impl_state = State}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

latest_tick(TS, Module, NumDropped) ->
    receive
        {tick, TS1} ->
            latest_tick(TS1, Module, NumDropped + 1)
    after 0 ->
            if NumDropped > 0 ->
                    ?stats_warning("(Collector: ~p) Dropped ~b ticks", [Module, NumDropped]);
               true ->
                    ok
            end,
            TS
    end.

handle_info({tick, TS0}, #state{module = Module,
                                count = Count,
                                impl_state = ImplState,
                                prev_counters = PrevCounters,
                                prev_ts = PrevTS} = State) ->
    NewCount = case Count >= misc:get_env_default(grab_stats_every_n_ticks, 1) - 1 of
                   true ->
                       0;
                   false ->
                       Count + 1
               end,
    case Count of
        0 ->
            try Module:grab_stats(ImplState) of
                empty_stats ->
                    {noreply, State#state{count = NewCount}};
                GrabbedStats ->
                    TS = latest_tick(TS0, Module, 0),
                    {Stats, NewCounters, NewImplState} =
                        Module:process_stats(TS, GrabbedStats, PrevCounters, PrevTS, ImplState),

                    lists:foreach(
                      fun ({EventName, Values}) ->
                              gen_event:notify(ns_stats_event,
                                               {stats, EventName,
                                                #stat_entry{timestamp = TS,
                                                            values = Values}})
                      end, Stats),
                    {noreply, State#state{count = NewCount,
                                          impl_state = NewImplState,
                                          prev_counters = NewCounters,
                                          prev_ts = TS}}
            catch T:E ->
                    ?stats_error("(Collector: ~p) Exception in stats collector: ~p~n",
                                 [Module, {T,E, erlang:get_stacktrace()}]),
                    {noreply, State#state{count = NewCount}}
            end;
        _ ->
            {noreply, State#state{count = NewCount}}
    end;
handle_info(Info, #state{module = Module,
                         impl_state = ImplState} = State) ->
    NewImplState =
        case erlang:function_exported(Module, handle_info, 2) of
            true ->
                {noreply, NewImplState1} = Module:handle_info(Info, ImplState),
                NewImplState1;
            false ->
                ImplState
        end,
    {noreply, State#state{impl_state = NewImplState}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

diff_counters(_InvTSDiff, [], _PrevCounters, Acc) ->
    lists:reverse(Acc);
diff_counters(InvTSDiff, [{K, V} | RestCounters] = Counters, PrevCounters, Acc) ->
    case PrevCounters of
        %% NOTE: K is bound
        [{K, OldV} | RestPrev] ->
            D = case (V - OldV) * InvTSDiff of
                    Res when Res < 0 ->
                        0;
                    Res ->
                        Res
                end,
            diff_counters(InvTSDiff, RestCounters, RestPrev, [{K, D} | Acc]);
        [{PrevK, _} | RestPrev] when PrevK < K ->
            diff_counters(InvTSDiff, Counters, RestPrev, Acc);
        _ ->
            diff_counters(InvTSDiff, RestCounters, PrevCounters, [{K, 0} | Acc])
    end.

calculate_counters(TS, Gauges, Counters, PrevCounters, PrevTS) ->
    TSDiff = TS - PrevTS,

    SortedCounters = lists:sort(Counters),
    NewCounters = diff_counters(1000.0 / TSDiff, SortedCounters, PrevCounters, []),
    {lists:merge(NewCounters, lists:sort(Gauges)), SortedCounters}.
