%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(service_fts).

-include("ns_common.hrl").

-export([start_keeper/0, get_indexes/0]).

-export([get_type/0,
         get_remote_items/1,
         get_local_status/1,
         process_status/1,
         compute_version/2]).

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

get_local_status(_Headers) ->
    rest_utils:get_json_local(fts, "api/nsstatus", get_port(), get_timeout()).

status_mapping() ->
    [{[index, id], <<"name">>},
     {bucket, <<"bucket">>},
     {hosts, <<"hosts">>}].

process_status(Status) ->
    service_status_keeper:process_indexer_status(?MODULE, Status,
                                                 status_mapping()).

start_keeper() ->
    service_status_keeper:start_link(?MODULE).

compute_version(Items, IsStale) ->
    erlang:phash2({Items, IsStale}).
