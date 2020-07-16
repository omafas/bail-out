%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2015 Couchbase, Inc.
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
%% @doc application for ns_couchdb node
%%

-module(ns_couchdb).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% child_erlang callbacks
-export([start/0, stop/0]).

-include("ns_common.hrl").
-include_lib("ale/include/ale.hrl").

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
    application:start(ns_couchdb, permanent).

start(_StartType, _StartArgs) ->
    Apps = [ale, asn1, crypto, public_key, ssl],
    lists:foreach(
      fun (App) ->
              ok = application:start(App, permanent)
      end, Apps),

    setup_env(),
    init_logging(),

    ok = dist_manager:configure_net_kernel(),

    ?log_info("CouchDB node ~p was initialized for ~p. Cookie: ~p",
              [node(), ns_node_disco:ns_server_node(),
              ns_cookie_manager:sanitize_cookie(erlang:get_cookie())]),
    ns_couchdb_sup:start_link().

stop(_State) ->
    ok.

%% called by child_erlang when we're asked to exit.
stop() ->
    ok.

setup_env() ->
    EnvArgsStr = os:getenv("NS_COUCHDB_ENV_ARGS"),
    true = is_list(EnvArgsStr),

    ok = application:load(ns_server),

    {ok, EnvArgs} = couch_util:parse_term(EnvArgsStr),
    lists:foreach(
      fun ({Key, Value}) ->
              application:set_env(ns_couchdb, Key, Value),
              application:set_env(ns_server, Key, Value)
      end, EnvArgs).

init_logging() ->
    StdLoggers = [?ALE_LOGGER, ?ERROR_LOGGER],
    Loggers = ?NS_COUCHDB_LOGGERS,
    AllLoggers = Loggers ++ StdLoggers,
    MainFileLoggers = AllLoggers -- [?COUCHDB_LOGGER, ?MAPREDUCE_ERRORS_LOGGER],

    lists:foreach(
      fun (Logger) ->
              ale:stop_logger(Logger)
      end, Loggers),

    ok = ns_server:start_disk_sink(ns_couchdb_sink, ?NS_COUCHDB_LOG_FILENAME),
    ok = ns_server:start_disk_sink(disk_couchdb, ?COUCHDB_LOG_FILENAME),
    ok = ns_server:start_disk_sink(disk_views, ?VIEWS_LOG_FILENAME),
    ok = ns_server:start_disk_sink(disk_mapreduce_errors, ?MAPREDUCE_ERRORS_LOG_FILENAME),

    ok = ale:set_loglevel(?ERROR_LOGGER, debug),

    lists:foreach(
      fun (Logger) ->
              ale:start_logger(Logger, debug)
      end, Loggers),

    lists:foreach(
      fun (Logger) ->
              ok = ale:set_loglevel(Logger, debug)
      end,
      StdLoggers),

    lists:foreach(
      fun (Logger) ->
              ok = ale:add_sink(Logger, ns_couchdb_sink, ns_server:get_loglevel(Logger))
      end, MainFileLoggers),

    ok = ale:add_sink(?COUCHDB_LOGGER, disk_couchdb, ns_server:get_loglevel(?COUCHDB_LOGGER)),
    ok = ale:add_sink(?VIEWS_LOGGER, disk_views),
    ok = ale:add_sink(?MAPREDUCE_ERRORS_LOGGER, disk_mapreduce_errors),

    case misc:get_env_default(ns_couchdb, dont_suppress_stderr_logger, false) of
        true ->
            ale:stop_sink(stderr),
            ok = ale:start_sink(stderr, ale_stderr_sink, []),

            lists:foreach(
              fun (Logger) ->
                      ok = ale:add_sink(Logger, stderr, debug)
              end, AllLoggers);
        false ->
            ok
    end,
    ale:info(?NS_SERVER_LOGGER, "Brought up ns_couchdb logging").
