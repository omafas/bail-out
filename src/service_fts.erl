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
-module(service_fts).

-include("ns_common.hrl").

-export([start_keeper/0, get_indexes/0]).

-export([get_type/0, get_remote_items/1, get_local_status/0, restart/0,
         process_status/1,
         get_gauges/0, get_counters/0, get_computed/0, grab_stats/0,
         compute_gauges/1, get_service_gauges/0, compute_version/2,
         compute_service_gauges/1, get_service_counters/0, split_stat_name/1,
         is_started/0]).

get_indexes() ->
    service_status_keeper:get_items(?MODULE).

get_type() ->
    fts.

get_port() ->
    service_ports:get_port(fts_http_port).

get_timeout() ->
    ?get_timeout(rest_request, 10000).

get_remote_items(Node) ->
    remote_api:get_fts_indexes(Node).

get_local_status() ->
    rest_utils:get_json_local(fts, "api/nsstatus", get_port(), get_timeout()).

restart() ->
    ns_ports_setup:restart_port_by_name(fts).

status_mapping() ->
    [{[index, id], <<"name">>},
     {bucket, <<"bucket">>},
     {hosts, <<"hosts">>}].

process_status(Status) ->
    service_status_keeper:process_indexer_status(?MODULE, Status,
                                                 status_mapping()).

start_keeper() ->
    service_status_keeper:start_link(?MODULE).

get_gauges() ->
    [num_mutations_to_index, doc_count, num_recs_to_persist, num_bytes_used_disk,
    num_pindexes_actual, num_pindexes_target, num_files_on_disk, num_root_memorysegments,
    num_root_filesegments].

get_counters() ->
    [total_bytes_indexed, total_compaction_written_bytes, total_queries,
    total_queries_slow, total_queries_timeout, total_queries_error,
    total_bytes_query_results, total_term_searchers, total_request_time].

get_computed() ->
    [].

get_service_gauges() ->
    [num_bytes_used_ram, total_queries_rejected_by_herder, curr_batches_blocked_by_herder].

get_service_counters() ->
    [].

is_started() ->
    misc:is_local_port_open(get_port(), 1000).

grab_stats() ->
    rest_utils:get_json_local(fts, "api/nsstats", get_port(), get_timeout()).

compute_service_gauges(_Gauges) ->
    [].

compute_gauges(_Gauges) ->
    [].

compute_version(Items, IsStale) ->
    erlang:phash2({Items, IsStale}).

split_stat_name(Name) ->
    binary:split(Name, <<":">>, [global]).
