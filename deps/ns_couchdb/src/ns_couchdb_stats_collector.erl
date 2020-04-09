%% @author Couchbase <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
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
%% @doc facility for collecting stats on ns_couchdb node
%%

-module(ns_couchdb_stats_collector).

-export([start_link/0, get_stats/0]).

start_link() ->
    proc_lib:start_link(erlang, apply, [fun start_loop/0, []]).

start_loop() ->
    ets:new(ns_server_system_stats, [public, named_table, set]),

    proc_lib:init_ack({ok, self()}),
    system_stats_collector:stale_histo_epoch_cleaner().

get_stats() ->
    lists:sort(ets:tab2list(ns_server_system_stats)).
