%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2019 Couchbase, Inc.
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
%% @doc main supervisor for ns_couchdb node
%%

-module(ns_couchdb_sup).

-behaviour(supervisor2).

%% API
-export([start_link/0, restart_capi_ssl_service/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one,
            misc:get_env_default(max_r, 3),
            misc:get_env_default(max_t, 10)}, child_specs()} }.

child_specs() ->
    [
     {menelaus_users_auth_cache, {menelaus_users, start_auth_cache, []},
      {permanent, 5}, 1000, worker, [versioned_cache]},

     {cb_couch_sup, {cb_couch_sup, start_link, []},
      permanent, 5000, supervisor, [cb_couch_sup]},

     %% this must be placed after cb_couch_sup since couchdb starts
     %% sasl application
     {cb_init_loggers, {cb_init_loggers, start_link, []},
      transient, 1000, worker, [cb_init_loggers]},

     {timeout_diag_logger, {timeout_diag_logger, start_link, []},
      permanent, 1000, worker, [timeout_diag_logger]},

     {ns_memcached_sockets_pool, {ns_memcached_sockets_pool, start_link, []},
      permanent, 1000, worker, []},

     {ns_couchdb_stats_collector, {ns_couchdb_stats_collector, start_link, []},
      permanent, 1000, worker, [ns_couchdb_stats_collector]},

     {ns_couchdb_config_sup, {ns_couchdb_config_sup, start_link, []},
      permanent, infinity, supervisor,
      [ns_couchdb_config_sup]},

     {compiled_roles_cache, {menelaus_roles, start_compiled_roles_cache, []},
      {permanent, 5}, 1000, worker, [versioned_cache]},

     {roles_cache, {roles_cache, start_link, []},
      {permanent, 5}, 1000, worker, []},

     {request_throttler, {request_throttler, start_link, []},
      permanent, 1000, worker, [request_throttler]},

     {vbucket_map_mirror, {vbucket_map_mirror, start_link, []},
      permanent, brutal_kill, worker, []},

     {set_view_update_daemon, {set_view_update_daemon, start_link, []},
      permanent, 1000, worker, [set_view_update_daemon]},

     restartable:spec(
       {ns_capi_ssl_service,
        {ns_ssl_services_setup, start_link_capi_service, []},
        {permanent, 4}, 1000, worker, []}),

     {dir_size, {dir_size, start_link, []},
      permanent, 1000, worker, [dir_size]}
    ].

restart_capi_ssl_service() ->
    case restartable:restart(?MODULE, ns_capi_ssl_service) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.
