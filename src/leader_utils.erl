%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(leader_utils).

-include("cut.hrl").
-include("ns_common.hrl").

-export([is_new_orchestration_disabled/0,
         ignore_if_new_orchestraction_disabled/1,
         live_nodes/0, live_nodes/1, live_nodes/2]).

is_new_orchestration_disabled() ->
    ns_config:read_key_fast(force_disable_new_orchestration, false).

ignore_if_new_orchestraction_disabled(Body) ->
    case is_new_orchestration_disabled() of
        true ->
            ignore;
        false ->
            Body()
    end.

live_nodes() ->
    live_nodes(ns_node_disco:nodes_wanted()).

live_nodes(WantedNodes) ->
    live_nodes(ns_config:latest(), WantedNodes).

live_nodes(Snapshot, WantedNodes) ->
    Nodes = ns_cluster_membership:get_nodes_with_status(Snapshot,
                                                        WantedNodes,
                                                        _ =/= inactiveFailed),
    ns_node_disco:only_live_nodes(Nodes).
