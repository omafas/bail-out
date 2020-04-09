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
-module(query_stats_collector).

-include("ns_common.hrl").

-include("ns_stats.hrl").

%% API
-export([start_link/0]).

%% callbacks
-export([init/1, grab_stats/1, process_stats/5]).

start_link() ->
     base_stats_collector:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ets:new(query_stats_collector_names, [private, named_table]),
    {ok, []}.

%% [{<<"active_requests.count">>,0},
%%  {<<"deletes.count">>,0},
%%  {<<"errors.count">>,0},
%%  {<<"inserts.count">>,0},
%%  {<<"invalid_requests.count">>,0},
%%  {<<"mutations.count">>,0},
%%  {<<"queued_requests.count">>,0},
%%  {<<"request_time.count">>,0},
%%  {<<"requests.count">>,0},
%%  {<<"requests_1000ms.count">>,0},
%%  {<<"requests_250ms.count">>,0},
%%  {<<"requests_5000ms.count">>,0},
%%  {<<"requests_500ms.count">>,0},
%%  {<<"result_count.count">>,0},
%%  {<<"result_size.count">>,0},
%%  {<<"selects.count">>,0},
%%  {<<"service_time.count">>,0},
%%  {<<"updates.count">>,0},
%%  {<<"warnings.count">>,0}]

%% Those are not part of any graphs yet, but otherwise dialyzer
%% doesn't like trying to deal with empty gauges below.
-define(Q_GAUGES, [active_requests, queued_requests]).
-define(Q_COUNTERS, [errors,
                     invalid_requests,
                     request_time,
                     requests,
                     requests_500ms,
                     requests_250ms,
                     requests_1000ms,
                     requests_5000ms,
                     result_count,
                     result_size,
                     selects,
                     service_time,
                     warnings]).

recognize_name(K) ->
    case ets:lookup(query_stats_collector_names, K) of
        [{K, Type, NewK}] ->
            {Type, NewK};
        [{K, undefined}] ->
            undefined;
        [] ->
            case do_recognize_name(K) of
                undefined ->
                    ets:insert(query_stats_collector_names, {K, undefined}),
                    undefined;
                {Type, NewK} ->
                    ets:insert(query_stats_collector_names, {K, Type, NewK}),
                    {Type, NewK}
            end
    end.

do_recognize_name(K) ->
    MaybeGauge = [NK || NK <- ?Q_GAUGES,
                        NKT <- [iolist_to_binary(io_lib:format("~s.count", [NK]))],
                        NKT =:= K],
    MaybeCounter = [NK || NK <- ?Q_COUNTERS,
                          NKT <- [iolist_to_binary(io_lib:format("~s.count", [NK]))],
                          NKT =:= K],
    case {MaybeGauge, MaybeCounter} of
        {[], []} -> undefined;
        {[NK], []} ->
            {gauge, list_to_atom("query_" ++ atom_to_list(NK))};
        {[], [NK]} ->
            {counter, list_to_atom("query_" ++ atom_to_list(NK))}
    end.

massage_stats([], AccGauges, AccCounters) ->
    {AccGauges, AccCounters};
massage_stats([{K, V} | Rest], AccGauges, AccCounters) ->
    case recognize_name(K) of
        undefined ->
            massage_stats(Rest, AccGauges, AccCounters);
        {counter, NewK} ->
            massage_stats(Rest, AccGauges, [{NewK, V} | AccCounters]);
        {gauge, NewK} ->
            massage_stats(Rest, [{NewK, V} | AccGauges], AccCounters)
    end.

grab_stats([]) ->
    case query_rest:get_stats() of
        {error, _} ->
            empty_stats;
        Stats ->
            Stats
    end.

process_stats(TS, GrabbedStats, PrevCounters, PrevTS, []) ->
    {Gauges, Counters} = massage_stats(GrabbedStats, [], []),
    {Stats, SortedCounters} =
        base_stats_collector:calculate_counters(TS, Gauges, Counters, PrevCounters, PrevTS),

    {[{"@query", Stats}], SortedCounters, []}.
