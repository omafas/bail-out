%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-2018 Couchbase, Inc.
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
-module(goxdcr_stats_collector).

-include("ns_common.hrl").

-include("ns_stats.hrl").

%% API
-export([start_link/1]).

%% callbacks
-export([init/1, grab_stats/1, process_stats/5]).

start_link(Bucket) ->
    base_stats_collector:start_link(?MODULE, Bucket).

init(Bucket) ->
    {ok, Bucket}.

grab_stats(Bucket) ->
    Stats = goxdcr_rest:stats(Bucket),
    case Stats of
        [] ->
            empty_stats;
        _ ->
            Stats
    end.

is_counter(<<"docs_filtered">>) ->
    true;
is_counter(_) ->
    false.

build_stat_key(RepID, StatK) ->
    iolist_to_binary(["replications/", RepID, $/, StatK]).

massage_rep_stats(_RepID, [], AccGauges, AccCounters, ChangesLeft, DocsRepQueue) ->
    {AccGauges, AccCounters, ChangesLeft, DocsRepQueue};
massage_rep_stats(RepID, [{K, V} | Rest], AccGauges, AccCounters, ChangesLeft, DocsRepQueue) ->
    {NewChangesLeft, NewDocsRepQueue} =
        case K of
            <<"changes_left">> ->
                {V, DocsRepQueue};
            <<"docs_rep_queue">> ->
                {ChangesLeft, V};
            _ ->
                {ChangesLeft, DocsRepQueue}
        end,
    NewPair = {build_stat_key(RepID, K), V},
    case is_counter(K) of
        true ->
            massage_rep_stats(RepID, Rest, AccGauges, [NewPair | AccCounters],
                              NewChangesLeft, NewDocsRepQueue);
        false ->
            massage_rep_stats(RepID, Rest, [NewPair | AccGauges], AccCounters,
                              NewChangesLeft, NewDocsRepQueue)
    end.

process_stats_loop([], Gauges, Counters, TotalChangesLeft, TotalDocsRepQueue) ->
    {Gauges, Counters, TotalChangesLeft, TotalDocsRepQueue};
process_stats_loop([{RepID, RepStats} | T], Gauges, Counters, TotalChangesLeft, TotalDocsRepQueue) ->
    {PerRepGauges, PerRepCounters, ChangesLeft, DocsRepQueue} =
        massage_rep_stats(RepID, RepStats, [], [], 0, 0),

    NewTotalChangesLeft = TotalChangesLeft + ChangesLeft,
    NewTotalDocsRepQueue = TotalDocsRepQueue + DocsRepQueue,
    process_stats_loop(T, PerRepGauges ++ Gauges, PerRepCounters ++ Counters,
                       NewTotalChangesLeft, NewTotalDocsRepQueue).

process_stats(TS, Stats, PrevCounters, PrevTS, Bucket) ->
    {Gauges, Counters, TotalChangesLeft, TotalDocsRepQueue} =
        process_stats_loop(Stats, [], [], 0, 0),

    {RepStats, SortedCounters} =
        base_stats_collector:calculate_counters(TS, Gauges, Counters, PrevCounters, PrevTS),

    GlobalList = [{<<"replication_changes_left">>, TotalChangesLeft},
                  {<<"replication_docs_rep_queue">>, TotalDocsRepQueue}],

    {[{"@xdcr-" ++ Bucket, lists:sort(GlobalList ++ RepStats)}],
     SortedCounters, Bucket}.
