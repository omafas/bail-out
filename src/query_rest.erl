%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc this module implements access to cbq-engine via REST API
%%

-module(query_rest).

-include("ns_common.hrl").

-export([get_stats/0]).

get_stats() ->
    case ns_cluster_membership:should_run_service(n1ql, node()) of
        true ->
            do_get_stats();
        false ->
            []
    end.

do_get_stats() ->
    Port = service_ports:get_port(query_port, ns_config:latest(), node()),
    Timeout = ?get_timeout(stats, 30000),
    case rest_utils:get_json_local(n1ql, "/admin/stats", Port, Timeout) of
        {ok, _Headers, {Stats}} ->
            Stats;
        Error ->
            Error
    end.
