%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(ale_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_all, 5, 10},
           [{ale_dymamic_sup,
             {ale_dynamic_sup, start_link, []},
             permanent, 5000, supervisor, [ale_dynamic_sup]},
            {ale_stats_events,
             {gen_event, start_link, [{local, ale_stats_events}]},
             permanent, 1000, worker, []},
            {ale, {ale, start_link, []}, permanent, 5000, worker, [ale]}]}}.
