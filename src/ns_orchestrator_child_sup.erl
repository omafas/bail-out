%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ns_orchestrator_child_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_all, 3, 10}, child_specs()}}.

child_specs() ->
    [{ns_janitor_server, {ns_janitor_server, start_link, []},
      permanent, 1000, worker, [ns_janitor_server]},
     {auto_reprovision, {auto_reprovision, start_link, []},
      permanent, 1000, worker, [auto_reprovision]},
     {auto_rebalance, {auto_rebalance, start_link, []},
      permanent, 1000, worker, [auto_rebalance]},
     {ns_orchestrator, {ns_orchestrator, start_link, []},
      permanent, 1000, worker, [ns_orchestrator]}].
