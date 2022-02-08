%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc chronicle upgrade
%%

-module(chronicle_upgrade).

-include("cut.hrl").
-include("ns_common.hrl").

-define(UPGRADE_PULL_TIMEOUT, ?get_timeout(upgrade_pull, 60000)).

-export([get_key/2, set_key/3, upgrade/2]).

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

upgrade(Version, Nodes) ->
    Config = ns_config:get(),
    RV = chronicle_kv:txn(
           kv,
           fun (Txn) ->
                   {Changes, Txn} = upgrade_loop({#{}, Txn}, Version, Config),
                   {commit, maps:fold(
                              fun (K, V, Acc) ->
                                      [{set, K, V} | Acc]
                              end, [], Changes)}
           end),
    case RV of
        {ok, _} ->
            OtherNodes = Nodes -- [node()],
            case chronicle_compat:remote_pull(OtherNodes,
                                              ?UPGRADE_PULL_TIMEOUT) of
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

upgrade_loop(UpgradeTxn, FinalVersion, Config) ->
    CurrentVersion =
        case get_key(cluster_compat_version, UpgradeTxn) of
            {error, not_found} ->
                ?VERSION_70;
            {ok, V} ->
                V
        end,
    case CurrentVersion of
        FinalVersion ->
            UpgradeTxn;
        _ ->
            ?log_debug("Upgading chronicle from ~p. Final version = ~p",
                       [CurrentVersion, FinalVersion]),
            {NewVersion, NewTxn} = upgrade_to(CurrentVersion, UpgradeTxn,
                                              Config),
            upgrade_loop(set_key(cluster_compat_version, NewVersion, NewTxn),
                         FinalVersion, Config)
    end.

upgrade_to(?VERSION_70, UpgradeTxn, Config) ->
    {?VERSION_NEO,
     functools:chain(
       UpgradeTxn,
       [ns_ssl_services_setup:chronicle_upgrade_to_NEO(_, Config),
        ns_bucket:chronicle_upgrade_to_NEO(_),
        compaction_daemon:chronicle_upgrade_to_NEO(_, Config)])}.
