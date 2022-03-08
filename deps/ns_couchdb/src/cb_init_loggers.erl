%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(cb_init_loggers).

-export([start_link/0]).

start_link() ->
    set_couchdb_loglevel(),
    ignore.

set_couchdb_loglevel() ->
    LogLevel = ns_server:get_loglevel(couchdb),
    CouchLogLevel = ns_server_to_couchdb_loglevel(LogLevel),
    couch_config:set("log", "level", CouchLogLevel).

ns_server_to_couchdb_loglevel(debug) ->
    "debug";
ns_server_to_couchdb_loglevel(info) ->
    "info";
ns_server_to_couchdb_loglevel(warn) ->
    "error";
ns_server_to_couchdb_loglevel(error) ->
    "error";
ns_server_to_couchdb_loglevel(critical) ->
    "error".
