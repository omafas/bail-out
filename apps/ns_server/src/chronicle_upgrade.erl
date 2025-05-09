%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc chronicle upgrade
%%

-module(chronicle_upgrade).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").

-define(UPGRADE_PULL_TIMEOUT, ?get_timeout(upgrade_pull, 60000)).

-export([get_key/2, set_key/3, upgrade/2, maybe_initialize/0]).

get_key(Key, {Snapshot, Txn}) ->
    case maps:find(Key, Snapshot) of
        {ok, V} ->
            {ok, V};
        error ->
            case chronicle_kv:txn_get(Key, Txn) of
                {ok, {V, _}} ->
                    {ok, V};
                {error, not_found} ->
                    {error, not_found}
            end
    end.

set_key(Key, Value, {Snapshot, Txn}) ->
    {maps:put(Key, Value, Snapshot), Txn}.

maybe_initialize() ->
    case chronicle_kv:get(kv, nodes_wanted) of
        {ok, {_, _}} ->
            ok;
        {error, not_found} ->
            initialize()
    end.

initialize() ->
    Node = node(),
    Sets = [{set, counters, []},
            %% auto-reprovision (mostly applicable to ephemeral buckets) is
            %% the operation that is carried out when memcached process on
            %% a node restarts within the auto-failover timeout.
            {set, auto_reprovision_cfg,
             [{enabled, true},
              %% max_nodes is the maximum number of nodes
              %% that may be automatically reprovisioned
              {max_nodes, 1},
              %% count is the number of nodes that were auto-reprovisioned
              {count, 0}]},
            {set, bucket_names, []},
            {set, nodes_wanted, [Node]},
            {set, server_groups, [[{uuid, <<"0">>},
                                   {name, <<"Group 1">>},
                                   {nodes, [Node]}]]},
            {set, {node, Node, membership}, active},
            {set, autocompaction,
             [{database_fragmentation_threshold, {30, undefined}},
              {view_fragmentation_threshold, {30, undefined}},
              {magma_fragmentation_percentage, 50}]}],

    ?log_info("Setup initial chronicle content ~p", [Sets]),
    {ok, Rev} = chronicle_kv:multi(kv, Sets),
    ?log_info("Chronicle content was initialized. Rev = ~p.", [Rev]).

upgrade(Version, Nodes) ->
    RV = chronicle_kv:txn(
           kv,
           fun (Txn) ->
                   {Changes, Txn} = upgrade_loop({#{}, Txn}, Version),
                   {commit, maps:fold(
                              fun (K, V, Acc) ->
                                      [{set, K, V} | Acc]
                              end, [], Changes)}
           end),
    case RV of
        {ok, _} ->
            OtherNodes = Nodes -- [node()],
            case chronicle_compat:push(OtherNodes, ?UPGRADE_PULL_TIMEOUT) of
                ok ->
                    ok;
                Error ->
                    ale:error(
                      ?USER_LOGGER,
                      "Failed to push chronicle config to some nodes: ~p",
                      [Error]),
                    error
            end;
        Error ->
            ale:error(?USER_LOGGER, "Error upgrading chronicle: ~p", [Error]),
            error
    end.

upgrade_loop(UpgradeTxn, FinalVersion) ->
    CurrentVersion =
        case get_key(cluster_compat_version, UpgradeTxn) of
            {error, not_found} ->
                ?VERSION_71;
            {ok, V} ->
                V
        end,
    case CurrentVersion of
        FinalVersion ->
            UpgradeTxn;
        _ ->
            ?log_info("Upgrading chronicle from ~p. Final version = ~p",
                      [CurrentVersion, FinalVersion]),
            {NewVersion, NewTxn} = upgrade_to(CurrentVersion, UpgradeTxn),
            upgrade_loop(set_key(cluster_compat_version, NewVersion, NewTxn),
                         FinalVersion)
    end.

upgrade_to(?VERSION_71, UpgradeTxn) ->
    {?VERSION_72, ns_bucket:chronicle_upgrade_to_72(UpgradeTxn)};

upgrade_to(?VERSION_72, UpgradeTxn) ->
    {?VERSION_76,
     functools:chain(
       UpgradeTxn,
       [ns_bucket:chronicle_upgrade_to_76(_)])};

upgrade_to(?VERSION_76, UpgradeTxn) ->
    {?VERSION_MORPHEUS,
     functools:chain(
       UpgradeTxn,
       [ns_bucket:chronicle_upgrade_to_morpheus(_),
        jwt_issuer:chronicle_upgrade_to_morpheus(_),
        ns_server_cert:chronicle_upgrade_to_morpheus(_)])}.
