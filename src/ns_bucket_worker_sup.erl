%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ns_bucket_worker_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_all, 3, 10}, child_specs()}}.

child_specs() ->
    [{ns_bucket_sup, {ns_bucket_sup, start_link, []},
      permanent, infinity, supervisor, [ns_bucket_sup]},

     %% ns_bucket_worker calls into ns_bucket_sup, so it needs to start after
     %% it.
     {ns_bucket_worker, {ns_bucket_worker, start_link, []},
      permanent, 1000, worker, [ns_bucket_worker]}].
