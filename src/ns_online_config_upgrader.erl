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
%%

-module(ns_online_config_upgrader).

-include("cut.hrl").
-include("ns_common.hrl").

-export([upgrade_config/1]).

upgrade_config(NewVersion) ->
    true = (NewVersion =< ?LATEST_VERSION_NUM),

    ok = ns_config:upgrade_config_explicitly(
           do_upgrade_config(_, NewVersion)).

do_upgrade_config(Config, FinalVersion) ->
    case ns_config:search(Config, cluster_compat_version) of
        {value, FinalVersion} ->
            [];
        %% The following two cases don't actually correspond to upgrade from
        %% pre-2.0 clusters, we don't support those anymore. Instead, it's an
        %% upgrade from pristine ns_config_default:default(). I tried setting
        %% cluster_compat_version to the most up-to-date compat version in
        %% default config, but that uncovered issues that I'm too scared to
        %% touch at the moment.
        false ->
            upgrade_compat_version(?VERSION_50);
        {value, undefined} ->
            upgrade_compat_version(?VERSION_50);
        {value, Ver} ->
            {NewVersion, Upgrade} = upgrade(Ver, Config),
            ?log_info("Performing online config upgrade to ~p", [NewVersion]),
            upgrade_compat_version(NewVersion) ++
                maybe_final_upgrade(NewVersion) ++ Upgrade
    end.

upgrade_compat_version(NewVersion) ->
    [{set, cluster_compat_version, NewVersion}].

maybe_final_upgrade(?LATEST_VERSION_NUM) ->
    ns_audit_cfg:upgrade_descriptors();
maybe_final_upgrade(_) ->
    [].

upgrade(?VERSION_50, Config) ->
    {?VERSION_51,
     ns_ssl_services_setup:upgrade_client_cert_auth_to_51(Config) ++
         ns_bucket:config_upgrade_to_51(Config)};

upgrade(?VERSION_51, Config) ->
    {?VERSION_55,
     menelaus_web_auto_failover:config_upgrade_to_55(Config) ++
         query_settings_manager:config_upgrade_to_55() ++
         eventing_settings_manager:config_upgrade_to_55() ++
         ns_bucket:config_upgrade_to_55(Config) ++
         menelaus_users:config_upgrade(?VERSION_55) ++
         ns_audit_cfg:upgrade_to_55(Config) ++
         leader_quorum_nodes_manager:config_upgrade_to_55(Config) ++
         scram_sha:config_upgrade_to_55()};

upgrade(?VERSION_55, _Config) ->
    {?VERSION_60, []};

upgrade(?VERSION_60, Config) ->
    {?VERSION_65,
     menelaus_web_auto_failover:config_upgrade_to_65(Config) ++
         ns_bucket:config_upgrade_to_65(Config) ++
         auto_rebalance_settings:config_upgrade_to_65() ++
         query_settings_manager:config_upgrade_to_65(Config) ++
         menelaus_web_settings:config_upgrade_to_65(Config)};

upgrade(?VERSION_65, Config) ->
    {?VERSION_66,
     menelaus_users:config_upgrade(?VERSION_66) ++
         ns_bucket:config_upgrade_to_66(Config)}.
