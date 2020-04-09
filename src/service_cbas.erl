%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
-module(service_cbas).

-include("ns_common.hrl").

-export([get_type/0, restart/0,
         get_gauges/0, get_counters/0, get_computed/0, grab_stats/0,
         compute_gauges/1, get_service_gauges/0,
         get_service_counters/0, compute_service_gauges/1, split_stat_name/1,
         is_started/0]).

get_type() ->
    cbas.

get_port() ->
    service_ports:get_port(cbas_admin_port).

restart() ->
    [].

get_gauges() ->
    ['incoming_records_count_total', 'failed_at_parser_records_count_total'].

get_counters() ->
    ['incoming_records_count', 'failed_at_parser_records_count'].

get_computed() ->
    [].

get_service_gauges() ->
    ['heap_used', 'system_load_average', 'thread_count', 'disk_used'].

get_service_counters() ->
    ['gc_count', 'gc_time', 'io_reads', 'io_writes'].

is_started() ->
    misc:is_local_port_open(get_port(), 1000).

grab_stats() ->
    Port = get_port(),
    Timeout = ?get_timeout(stats, 30000),
    Path = "analytics/node/stats",
    CDataEncrypt = misc:should_cluster_data_be_encrypted(),
    rest_utils:get_json_local(cbas, Path, Port, Timeout, CDataEncrypt).

compute_service_gauges(_Gauges) ->
    [].

compute_gauges(_Gauges) ->
    [].

split_stat_name(Name) ->
    binary:split(Name, <<":">>, [global]).
