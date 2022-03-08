%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(ns_orchestrator_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one, 3, 10}, child_specs()}}.

child_specs() ->
    [{compat_mode_events,
      {gen_event, start_link, [{local, compat_mode_events}]},
      permanent, 1000, worker, dynamic},
     {compat_mode_manager,
      {compat_mode_manager, start_link, []},
      permanent, 1000, worker, [compat_mode_manager]},
     {ns_orchestrator_child_sup, {ns_orchestrator_child_sup, start_link, []},
      permanent, infinity, supervisor, [ns_orchestrator_child_sup]},
     {auto_failover, {auto_failover, start_link, []},
      permanent, 1000, worker, [auto_failover]}].
