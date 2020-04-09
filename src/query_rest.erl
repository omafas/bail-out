%% @author Couchbase <info@couchbase.com>
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
%% @doc this module implements access to cbq-engine via REST API
%%

-module(query_rest).

-include("ns_common.hrl").

-export([get_stats/0]).

get_stats() ->
    case ns_cluster_membership:should_run_service(ns_config:latest(), n1ql, node()) of
        true ->
            do_get_stats();
        false ->
            []
    end.

do_get_stats() ->
    Port = service_ports:get_port(query_port, ns_config:latest(), node()),
    Timeout = ?get_timeout(stats, 30000),
    case rest_utils:get_json_local(n1ql, "/admin/stats", Port, Timeout) of
        {ok, {Stats}} ->
            Stats;
        Error ->
            Error
    end.
