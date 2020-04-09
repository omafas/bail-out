%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2015 Couchbase, Inc.
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
