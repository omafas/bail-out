%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

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
