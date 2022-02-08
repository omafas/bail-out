%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(service_status_keeper_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children =
        [{service_status_keeper_worker,
          {work_queue, start_link, [service_status_keeper_worker]},
          permanent, 1000, worker, []},
         {service_status_keeper_index, {service_index, start_keeper, []},
          permanent, 1000, worker, []},
         {service_status_keeper_fts, {service_fts, start_keeper, []},
          permanent, 1000, worker, []},
         {service_status_keeper_eventing, {service_eventing, start_keeper, []},
          permanent, 1000, worker, []}],
    {ok, {{one_for_all,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          Children}}.
