%% @author Couchbase <info@couchbase.com>
%% @copyright 2022-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(rest_lhttpc_pool_sup).

-behaviour(supervisor).

-export([init/1, start_link/0]).

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs()}}.

child_specs() ->
    [
     {rest_lhttpc_pool, {lhttpc_manager, start_link,
                         [[{name, rest_lhttpc_pool},
                           {connection_timeout, 120000},
                           {pool_size, 20}]]},
      {permanent, 1}, 1000, worker, [lhttpc_manager]}
    ].
