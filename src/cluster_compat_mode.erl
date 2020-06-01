%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2019 Couchbase, Inc.
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

-module(cluster_compat_mode).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_compat_version/0,
         get_compat_version/1,
         is_enabled/1, is_enabled_at/2,
         consider_switching_compat_mode/0,
         is_index_aware_rebalance_on/0,
         is_index_pausing_on/0,
         rebalance_ignore_view_compactions/0,
         is_cluster_51/0,
         is_cluster_51/1,
         is_version_51/1,
         is_cluster_55/0,
         is_cluster_55/1,
         is_version_55/1,
         is_cluster_65/0,
         is_cluster_65/1,
         is_version_65/1,
         is_cluster_66/0,
         is_cluster_66/1,
         is_version_66/1,
         is_enterprise/0,
         is_saslauthd_enabled/0,
         is_cbas_enabled/0,
         supported_compat_version/0,
         min_supported_compat_version/0,
         effective_cluster_compat_version/0,
         effective_cluster_compat_version_for/1,
         is_developer_preview/0,
         is_developer_preview/1,
         get_cluster_capabilities/1,
         tls_supported/0,
         preserve_durable_mutations/0]).

%% NOTE: this is rpc:call-ed by mb_master
-export([mb_master_advertised_version/0]).

n1ql_cluster_capabilities(?VERSION_65, true) ->
    [costBasedOptimizer, indexAdvisor, javaScriptFunctions, inlineFunctions |
     n1ql_cluster_capabilities(?VERSION_65, false)];
n1ql_cluster_capabilities(?VERSION_65, false) ->
    [enhancedPreparedStatements].

cluster_capabilities(Version, IsDP) ->
    case is_version_65(Version) of
        true ->
            [{n1ql, n1ql_cluster_capabilities(?VERSION_65, IsDP)}];
        false ->
            []
    end.

get_cluster_capabilities(Config) ->
    cluster_capabilities(get_compat_version(Config),
                         is_developer_preview(Config)).

get_compat_version() ->
    get_compat_version(ns_config:latest()).

get_compat_version(Config) ->
    ns_config:search(Config, cluster_compat_version, undefined).

supported_compat_version() ->
    case get_pretend_version() of
        undefined ->
            ?LATEST_VERSION_NUM;
        Version ->
            Version
    end.

min_supported_compat_version() ->
    ?VERSION_50.

%% NOTE: this is rpc:call-ed by mb_master
%%
%% I.e. we want later version to be able to take over mastership even
%% without requiring compat mode upgrade
mb_master_advertised_version() ->
    case get_pretend_version() of
        undefined ->
            ?MASTER_ADVERTISED_VERSION;
        Version ->
            Version ++ [0]
    end.

is_enabled_at(undefined = _ClusterVersion, _FeatureVersion) ->
    false;
is_enabled_at(ClusterVersion, FeatureVersion) ->
    ClusterVersion >= FeatureVersion.

is_enabled(FeatureVersion) ->
    is_enabled(ns_config:latest(), FeatureVersion).

is_enabled(Config, FeatureVersion) ->
    is_enabled_at(get_compat_version(Config), FeatureVersion).

is_version_51(ClusterVersion) ->
    is_enabled_at(ClusterVersion, ?VERSION_51).

is_cluster_51() ->
    is_cluster_51(ns_config:latest()).

is_cluster_51(Config) ->
    is_enabled(Config, ?VERSION_51).

is_version_55(ClusterVersion) ->
    is_enabled_at(ClusterVersion, ?VERSION_55).

is_cluster_55() ->
    is_cluster_55(ns_config:latest()).

is_cluster_55(Config) ->
    is_enabled(Config, ?VERSION_55).

is_version_65(ClusterVersion) ->
    is_enabled_at(ClusterVersion, ?VERSION_65).

is_cluster_65() ->
    is_cluster_65(ns_config:latest()).

is_cluster_65(Config) ->
    is_enabled(Config, ?VERSION_65).

is_version_66(ClusterVersion) ->
    is_enabled_at(ClusterVersion, ?VERSION_66).

is_cluster_66() ->
    is_cluster_66(ns_config:latest()).

is_cluster_66(Config) ->
    is_enabled(Config, ?VERSION_66).

is_index_aware_rebalance_on() ->
    not ns_config:read_key_fast(index_aware_rebalance_disabled, false).

is_index_pausing_on() ->
    is_index_aware_rebalance_on() andalso
        (not ns_config:read_key_fast(index_pausing_disabled, false)).

is_enterprise() ->
    ns_config:read_key_fast({node, node(), is_enterprise}, false).

is_saslauthd_enabled() ->
    is_enterprise() andalso
        ns_config:search(ns_config:latest(),
                         {node, node(), saslauthd_enabled}, false).

is_cbas_enabled() ->
    is_enterprise().

rebalance_ignore_view_compactions() ->
    ns_config:read_key_fast(rebalance_ignore_view_compactions, false).

consider_switching_compat_mode() ->
    Config = ns_config:get(),
    CurrentVersion = ns_config:search(Config, cluster_compat_version, undefined),
    case CurrentVersion =:= supported_compat_version() of
        true ->
            ok;
        false ->
            do_consider_switching_compat_mode(Config, CurrentVersion)
    end.

upgrades() ->
    [{?VERSION_55, rbac, menelaus_users, upgrade}].

do_upgrades(undefined, _, _, _) ->
    %% this happens during the cluster initialization. no upgrade needed
    ok;
do_upgrades(CurrentVersion, NewVersion, Config, NodesWanted) ->
    do_upgrades(upgrades(), CurrentVersion, NewVersion, Config, NodesWanted).

do_upgrades([], _, _, _, _) ->
    ok;
do_upgrades([{Version, Name, Module, Fun} | Rest],
            CurrentVersion, NewVersion, Config, NodesWanted)
  when CurrentVersion < Version andalso NewVersion >= Version ->
    ?log_debug("Initiating ~p upgrade due to version change from ~p to ~p",
               [Name, CurrentVersion, NewVersion]),
    case Module:Fun(Version, Config, NodesWanted) of
        ok ->
            do_upgrades(Rest, CurrentVersion, NewVersion, Config, NodesWanted);
        _ ->
            Name
    end;
do_upgrades([_ | Rest], CurrentVersion, NewVersion, Config, NodesWanted) ->
    do_upgrades(Rest, CurrentVersion, NewVersion, Config, NodesWanted).

do_consider_switching_compat_mode(Config, CurrentVersion) ->
    NodesWanted = lists:sort(ns_config:search(Config, nodes_wanted, undefined)),
    NodesUp = lists:sort([node() | nodes()]),
    case ordsets:is_subset(NodesWanted, NodesUp) of
        true ->
            NodeInfos = ns_doctor:get_nodes(),
            case consider_switching_compat_mode_loop(NodeInfos, NodesWanted, supported_compat_version()) of
                CurrentVersion ->
                    ok;
                AnotherVersion ->
                    case is_enabled_at(AnotherVersion, CurrentVersion) of
                        true ->
                            case do_upgrades(CurrentVersion, AnotherVersion, Config, NodesWanted) of
                                ok ->
                                    do_switch_compat_mode(AnotherVersion, NodesWanted),
                                    changed;
                                Name ->
                                    ?log_error("Refusing to upgrade the compat "
                                               "version from ~p to ~p due to failure of ~p upgrade"
                                               "~nNodesWanted: ~p~nNodeInfos: ~p",
                                               [CurrentVersion, AnotherVersion, Name,
                                                NodesWanted, NodeInfos])
                            end;
                        false ->
                            ?log_error("Refusing to downgrade the compat "
                                       "version from ~p to ~p."
                                       "~nNodesWanted: ~p~nNodeInfos: ~p",
                                       [CurrentVersion, AnotherVersion, NodesWanted, NodeInfos]),
                            ok
                    end
            end;
        false ->
            ok
    end.

do_switch_compat_mode(NewVersion, NodesWanted) ->
    ns_online_config_upgrader:upgrade_config(NewVersion),
    try
        case ns_config_rep:ensure_config_seen_by_nodes(NodesWanted) of
            ok -> ok;
            {error, BadNodes} ->
                ale:error(?USER_LOGGER, "Was unable to sync cluster_compat_version update to some nodes: ~p", [BadNodes]),
                ok
        end
    catch T:E ->
            ale:error(?USER_LOGGER, "Got problems trying to replicate cluster_compat_version update~n~p", [{T,E,erlang:get_stacktrace()}])
    end.

consider_switching_compat_mode_loop(_NodeInfos, _NodesWanted, _Version = undefined) ->
    undefined;
consider_switching_compat_mode_loop(_NodeInfos, [], Version) ->
    Version;
consider_switching_compat_mode_loop(NodeInfos, [Node | RestNodesWanted], Version) ->
    case dict:find(Node, NodeInfos) of
        {ok, Info} ->
            NodeVersion = proplists:get_value(supported_compat_version, Info, undefined),
            AgreedVersion = case is_enabled_at(NodeVersion, Version) of
                                true ->
                                    Version;
                                false ->
                                    NodeVersion
                            end,
            consider_switching_compat_mode_loop(NodeInfos, RestNodesWanted, AgreedVersion);
        _ ->
            undefined
    end.

%% undefined is "used" shortly after node is initialized and when
%% there's no compat mode yet
effective_cluster_compat_version_for(undefined) ->
    1;
effective_cluster_compat_version_for([VersionMaj, VersionMin] = _CompatVersion) ->
    VersionMaj * 16#10000 + VersionMin.

effective_cluster_compat_version() ->
    effective_cluster_compat_version_for(get_compat_version()).

get_pretend_version() ->
    case application:get_env(ns_server, pretend_version) of
        undefined ->
            undefined;
        {ok, VersionString} ->
            {[A, B | _], _, _} = misc:parse_version(VersionString),
            [A, B]
    end.

is_developer_preview() -> is_developer_preview(ns_config:get()).
is_developer_preview(Config) ->
    ns_config:search(Config, developer_preview_enabled, false).

tls_supported() ->
    is_enterprise().

-ifdef(TEST).
mb_master_advertised_version_test() ->
    true = mb_master_advertised_version() >= ?LATEST_VERSION_NUM ++ [0].
-endif.

preserve_durable_mutations() ->
    cluster_compat_mode:is_cluster_65() andalso
        ns_config:read_key_fast({failover, preserve_durable_mutations}, true).
