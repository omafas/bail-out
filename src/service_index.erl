%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-2019 Couchbase, Inc.
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
-module(service_index).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_keeper/0, get_status/1, get_indexes/0, get_indexes_version/0]).

-export([get_type/0, get_remote_items/1, get_local_status/0, restart/0,
         get_gauges/0, get_counters/0, get_computed/0, grab_stats/0,
         compute_gauges/1, get_service_gauges/0, compute_service_gauges/1,
         get_service_counters/0, process_status/1, compute_version/2,
         split_stat_name/1, is_started/0]).

get_status(Timeout) ->
    service_status_keeper:get_status(?MODULE, Timeout).

get_indexes() ->
    service_status_keeper:get_items(?MODULE).

get_indexes_version() ->
    service_status_keeper:get_version(?MODULE).

get_type() ->
    index.

get_port() ->
    service_ports:get_port(indexer_http_port).

get_timeout() ->
    ?get_timeout(rest_request, 10000).

get_remote_items(Node) ->
    remote_api:get_indexes(Node).

get_local_status() ->
    rest_utils:get_json_local(indexer, "getIndexStatus",
                              get_port(), get_timeout()).

restart() ->
    ns_ports_setup:restart_port_by_name(indexer).

status_mapping() ->
    AddType0 = [{storageMode, <<"indexType">>}],
    AddType = case cluster_compat_mode:is_cluster_55() andalso
                  cluster_compat_mode:is_enterprise() of
                  true ->
                      [{instId, <<"instId">>},
                       {partitioned, <<"partitioned">>},
                       {numPartition, <<"numPartition">>},
                       {partitionMap, <<"partitionMap">>} | AddType0];
                  false ->
                      AddType0
              end,

    [{id, <<"defnId">>},
     {index, <<"name">>},
     {indexName, <<"indexName">>},
     {lastScanTime, <<"lastScanTime">>},
     {numReplica, <<"numReplica">>},
     {bucket, <<"bucket">>},
     {status, <<"status">>},
     {definition, <<"definition">>},
     {progress, <<"completion">>},
     {stale, <<"stale">>},
     {hosts, <<"hosts">>} | AddType].

process_status(Status) ->
    service_status_keeper:process_indexer_status(?MODULE, Status,
                                                 status_mapping()).

start_keeper() ->
    service_status_keeper:start_link(?MODULE).

get_gauges() ->
    [disk_size, data_size, memory_used, num_docs_pending, num_docs_queued,
     items_count, frag_percent, recs_in_mem, recs_on_disk, data_size_on_disk,
     log_space_on_disk, raw_data_size].

get_counters() ->
    [num_requests, num_rows_returned, num_docs_indexed,
     scan_bytes_read, total_scan_duration, cache_misses, cache_hits].

get_service_gauges() ->
    [memory_quota, memory_used].

get_service_counters() ->
    [].

get_computed() ->
    [disk_overhead_estimate].

is_started() ->
    misc:is_local_port_open(get_port(), 1000).

grab_stats() ->
    rest_utils:get_json_local(indexer, "stats?async=true",
                              get_port(), get_timeout()).

compute_service_gauges(Gauges) ->
    compute_index_ram_usage_stats(Gauges).

compute_index_ram_usage_stats(Stats) ->
    Quota = proplists:get_value(<<"index_memory_quota">>, Stats, undefined),
    Used = proplists:get_value(<<"index_memory_used">>, Stats, undefined),
    case Quota =/= undefined andalso Used =/= undefined of
        true ->
            [{<<"index_ram_percent">>, min(((Used / Quota) * 100), 100)},
             {<<"index_remaining_ram">>, max(Quota - Used, 0)}];
        false ->
            []
    end.

compute_gauges(Gauges) ->
    compute_disk_overhead_estimates(Gauges).

compute_version_ignored_items() ->
    [lastScanTime, progress].

compute_version(Items, IsStale) ->
    %% Don't include items that change frequently and are "unimportant"
    %% wrt determining whether or not a change has occurred.  This is done
    %% to avoid unnecessary change notifications.
    Items0 = lists:map(
               fun (ItemList) ->
                       lists:foldl(
                         fun (Key, Acc) ->
                                 lists:keydelete(Key, 1, Acc)
                         end, ItemList, compute_version_ignored_items())
               end, Items),
    erlang:phash2({Items0, IsStale}).

split_stat_name(Name) ->
    binary:split(Name, <<":">>, [global]).

compute_disk_overhead_estimates(Stats) ->
    Dict = lists:foldl(
             fun ({StatKey, Value}, D) ->
                     {Bucket, Index, Metric} = StatKey,
                     Key = {Bucket, Index},

                     case Metric of
                         <<"frag_percent">> ->
                             misc:dict_update(
                               Key,
                               fun ({_, DiskSize}) ->
                                       {Value, DiskSize}
                               end, {undefined, undefined}, D);
                         <<"disk_size">> ->
                             misc:dict_update(
                               Key,
                               fun ({Frag, _}) ->
                                       {Frag, Value}
                               end, {undefined, undefined}, D);
                         _ ->
                             D
                     end
             end, dict:new(), Stats),

    dict:fold(
      fun ({Bucket, Index}, {Frag, DiskSize}, Acc) ->
              if
                  Frag =/= undefined andalso DiskSize =/= undefined ->
                      Est = (DiskSize * Frag) div 100,
                      [{{Bucket, Index, <<"disk_overhead_estimate">>}, Est} | Acc];
                  true ->
                      Acc
              end
      end, [], Dict).


-ifdef(TEST).
compute_disk_overhead_estimates_test() ->
    In = [{{<<"a">>, <<"idx1">>, <<"disk_size">>}, 100},
          {{<<"a">>, <<"idx1">>, <<"frag_percent">>}, 0},
          {{<<"b">>, <<"idx2">>, <<"frag_percent">>}, 100},
          {{<<"b">>, <<"idx2">>, <<"disk_size">>}, 100},
          {{<<"b">>, <<"idx3">>, <<"disk_size">>}, 100},
          {{<<"b">>, <<"idx3">>, <<"frag_percent">>}, 50},
          {{<<"b">>, <<"idx3">>, <<"m">>}, 42}],
    Out = lists:keysort(1, compute_disk_overhead_estimates(In)),

    Expected0 = [{{<<"a">>, <<"idx1">>, <<"disk_overhead_estimate">>}, 0},
                 {{<<"b">>, <<"idx2">>, <<"disk_overhead_estimate">>}, 100},
                 {{<<"b">>, <<"idx3">>, <<"disk_overhead_estimate">>}, 50}],
    Expected = lists:keysort(1, Expected0),

    ?assertEqual(Expected, Out).

compute_version_test() ->
    ItemsWithLst = [[{storageMode,<<"plasma">>},
                     {progress,100},
                     {hosts,[<<"127.0.0.1:9000">>]},
                     {lastScanTime,<<"Mon Sep 23 11:07:30 PDT 2019">>},
                     {index,<<"beer_primary">>}],
                    [{storageMode,<<"plasma">>},
                     {progress,55},
                     {hosts,[<<"127.0.0.1:9001">>]},
                     {lastScanTime,<<"Mon Sep 23 12:11:22 PDT 2019">>},
                     {index,<<"def_airportname">>}]],
    ItemsRedactedLst = [[{storageMode,<<"plasma">>},
                         {hosts,[<<"127.0.0.1:9000">>]},
                         {index,<<"beer_primary">>}],
                        [{storageMode,<<"plasma">>},
                         {hosts,[<<"127.0.0.1:9001">>]},
                         {index,<<"def_airportname">>}]],
    LstVersion = compute_version(ItemsWithLst, false),
    NoLstVersion = compute_version(ItemsRedactedLst, false),
    Direct = erlang:phash2({ItemsRedactedLst, false}),

    ?assertEqual(LstVersion, NoLstVersion),
    ?assertEqual(LstVersion, Direct).

-endif.
