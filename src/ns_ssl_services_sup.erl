%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2017 Couchbase, Inc.
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

-module(ns_ssl_services_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([init/1, start_link/0, restart_ssl_service/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs()}}.

restart_ssl_service() ->
    case restartable:restart(?MODULE, ns_rest_ssl_service) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

child_specs() ->
    [{ssl_service_events,
      {gen_event, start_link, [{local, ssl_service_events}]},
      permanent, 1000, worker, []},

     {ns_ssl_services_setup,
      {ns_ssl_services_setup, start_link, []},
      permanent, 1000, worker, []},

     restartable:spec(
       {ns_rest_ssl_service,
        {ns_ssl_services_setup, start_link_rest_service, []},
        permanent, 1000, worker, []})
    ].
