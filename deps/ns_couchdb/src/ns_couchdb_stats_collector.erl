%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc facility for collecting stats on ns_couchdb node
%%

-module(ns_couchdb_stats_collector).

-export([start_link/0, get_stats/0]).

start_link() ->
    proc_lib:start_link(erlang, apply, [fun start_loop/0, []]).

start_loop() ->
    ns_server_stats:init_stats(),

    proc_lib:init_ack({ok, self()}),
    ns_server_stats:stale_histo_epoch_cleaner().

get_stats() ->
    lists:sort(ets:tab2list(ns_server_system_stats)).
