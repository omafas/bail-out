%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(analytics_settings_manager).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behavior(json_settings_manager).

-export([start_link/0,
         get/1,
         get_from_config/3,
         update/2,
         config_default/0]).

-export([cfg_key/0,
         is_enabled/0,
         known_settings/0,
         on_update/2,
         config_upgrade_to_76/1]).

-import(json_settings_manager,
        [id_lens/1,
         config_upgrade_settings/5]).

-define(ANALYTICS_CONFIG_KEY, {metakv, <<"/analytics/settings/config">>}).

start_link() ->
    json_settings_manager:start_link(?MODULE).

get(Key) ->
    json_settings_manager:get(?MODULE, Key, []).

get_from_config(Config, Key, Default) ->
    json_settings_manager:get_from_config(?MODULE, Config, Key, Default).

cfg_key() ->
    ?ANALYTICS_CONFIG_KEY.

is_enabled() ->
    true.

on_update(_Key, _Value) ->
    ok.

update(Key, Value) ->
    json_settings_manager:update(?MODULE, [{Key, Value}]).

default_settings() ->
    [{generalSettings,
      general_settings_defaults(?MIN_SUPPORTED_VERSION)}].

%% settings manager populates settings per version. For each online upgrade,
%% it computes the delta between adjacent supported versions to update only the
%% settings that changed between the two.
%% Note that a node (running any version) is seeded with settings specified in
%% config_default(). If we specify settings(LATEST_VERSION) here, the node
%% contains settings as per LATEST_VERSION at start. A node with LATEST_VERSION
%% settings may be part of a cluster with compat_version v1 < latest_version. If
%% the version moves up from v1 to latest, config_upgrade_to_latest is called.
%% This will update settings that changed between v1 and latest (when the node
%% was already initialized with latest_version settings). So config_default()
%% must specify settings for the min supported version.
config_default() ->
    {?ANALYTICS_CONFIG_KEY, json_settings_manager:build_settings_json(
                              default_settings(), maps:new(),
                              known_settings(?MIN_SUPPORTED_VERSION))}.

known_settings() ->
    ClusterVersion = cluster_compat_mode:get_ns_config_compat_version(),
    known_settings(ClusterVersion).

known_settings(ClusterVersion) ->
    [{generalSettings, general_settings_lens(ClusterVersion)}].

general_settings_defaults(ClusterVersion) ->
    [{numReplicas, 0}] ++
        case cluster_compat_mode:is_version_76(ClusterVersion) of
            true ->
                [
                 {blobStorageScheme, <<"">>},
                 {blobStorageBucket, <<"">>},
                 {blobStoragePrefix, <<"">>},
                 {blobStorageRegion, <<"">>}] ++
                 [{Key, Default} || {Key, _, Default} <-
                     config_profile:search({cbas, extra_settings_version_76},
                                           [])];
            false ->
                []
        end.

general_settings_lens(ClusterVersion) ->
    json_settings_manager:props_lens(
      general_settings_lens_props(ClusterVersion)).

general_settings_lens_props(ClusterVersion) ->
    [{numReplicas, id_lens(<<"analytics.settings.num_replicas">>)}] ++
        case cluster_compat_mode:is_version_76(ClusterVersion) of
            true ->
                [{blobStorageScheme,
                  id_lens(<<"analytics.settings.blob_storage_scheme">>)},
                 {blobStorageBucket,
                  id_lens(<<"analytics.settings.blob_storage_bucket">>)},
                 {blobStoragePrefix,
                  id_lens(<<"analytics.settings.blob_storage_prefix">>)},
                 {blobStorageRegion,
                  id_lens(<<"analytics.settings.blob_storage_region">>)}] ++
                [{Key, id_lens(Option)} || {Key, Option, _} <-
                    config_profile:search({cbas, extra_settings_version_76},
                                          [])];
            false ->
                []
        end.

config_upgrade_to_76(Config) ->
    config_upgrade_settings(?MODULE, Config,
                            general_settings_defaults(?VERSION_72),
                            general_settings_defaults(?VERSION_76),
                            known_settings(?VERSION_76)).

-ifdef(TEST).
defaults_test() ->
    config_profile:mock_default_profile(),
    Versions = [?MIN_SUPPORTED_VERSION, ?VERSION_76],
    lists:foreach(fun(V) -> default_versioned(V) end, Versions),
    config_profile:unmock_default_profile(ok).

default_versioned(Version) ->
    Keys = fun (L) -> lists:sort([K || {K, _} <- L]) end,

    %% This only works across versions because the keys are the same
    %% i.e. [generalSettings] in default_settings() and in 7.6.
    ?assertEqual(Keys(known_settings(Version)), Keys(default_settings())),

    ?assertEqual(Keys(general_settings_lens_props(Version)),
                 Keys(general_settings_defaults(Version))).

config_upgrade_test() ->
    config_profile:mock_default_profile(),
    CmdList = config_upgrade_to_76([]),
    [{set, {metakv, Meta}, Data}] = CmdList,
    ?assertEqual(<<"/analytics/settings/config">>, Meta),
    ?assertEqual(<<"{\"analytics.settings.blob_storage_bucket\":\"\","
                   "\"analytics.settings.blob_storage_prefix\":\"\","
                   "\"analytics.settings.blob_storage_region\":\"\","
                   "\"analytics.settings.blob_storage_scheme\":\"\"}">>,
                 Data),
    config_profile:unmock_default_profile(ok).
    %% TODO: add upgrade test w/ columnar profile
-endif.
