%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(query_settings_manager).

-include("ns_config.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behavior(json_settings_manager).

-export([start_link/0,
         get/1,
         get_from_config/3,
         update/2,
         update_txn/1,
         config_default/0,
         config_upgrade_to_76/1,
         config_upgrade_to_morpheus/1]).

-export([cfg_key/0,
         is_enabled/0,
         known_settings/0,
         on_update/2]).

-import(json_settings_manager,
        [id_lens/1,
         config_upgrade_settings/5]).

-define(QUERY_CONFIG_KEY, {metakv, <<"/query/settings/config">>}).

start_link() ->
    json_settings_manager:start_link(?MODULE).

get(Key) ->
    json_settings_manager:get(?MODULE, Key, undefined).

get_from_config(Config, Key, Default) ->
    json_settings_manager:get_from_config(?MODULE, Config, Key, Default).

cfg_key() ->
    ?QUERY_CONFIG_KEY.

is_enabled() ->
    true.

on_update(_Key, _Value) ->
    ok.

update(Key, Value) ->
    json_settings_manager:update(?MODULE, [{Key, Value}]).

update_txn(Props) ->
    json_settings_manager:update_txn(?MODULE, Props).

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
    {?QUERY_CONFIG_KEY, json_settings_manager:build_settings_json(
                          default_settings(?MIN_SUPPORTED_VERSION),
                          maps:new(),
                          known_settings(?MIN_SUPPORTED_VERSION))}.


config_upgrade_to_morpheus(Config) ->
    config_upgrade_settings(?MODULE, Config,
                            general_settings_defaults(?VERSION_76),
                            general_settings_defaults(?VERSION_MORPHEUS),
                            known_settings(?VERSION_MORPHEUS)).

config_upgrade_to_76(Config) ->
    config_upgrade_settings(?MODULE, Config,
                            general_settings_defaults(?VERSION_72),
                            general_settings_defaults(?VERSION_76),
                            known_settings(?VERSION_76)).

known_settings() ->
    known_settings(cluster_compat_mode:get_ns_config_compat_version()).

known_settings(Ver) ->
    [{generalSettings, general_settings_lens(Ver)},
     {curlWhitelistSettings, curl_whitelist_settings_lens()}] ++
        case cluster_compat_mode:is_version_76(Ver) of
            true ->
                %% map the memoryQuota to node-quota parameter
                [{memoryQuota, id_lens(<<"node-quota">>)}];
            false ->
                []
        end.

default_settings(Ver) ->
    [{generalSettings, general_settings_defaults(Ver)},
     {curlWhitelistSettings, curl_whitelist_settings_defaults()}] ++
        case cluster_compat_mode:is_version_76(Ver) of
            true ->
                [{memoryQuota, ?QUERY_NODE_QUOTA_DEFAULT}];
            false ->
                []
        end.

n1ql_feature_ctrl_setting(Ver) ->
    %% From Kamini Jagtiani - we can turn sequential scans off by ”OR”ing the
    %% current n1ql_feat_ctrl with 0x0000004000 (Decimal: 16,384). I believe the
    %% current value for n1ql_feat_ctrl is 4c (Decimal:76).
    %%
    %% So the new value should be 0x404c(Decimal: 16460) to turn it off.
    Default = 16#4c,
    SequentialScanDisabled =
        cluster_compat_mode:is_version_76(Ver) andalso
            config_profile:get_bool({n1ql, sequential_scan_disabled}),

    Val =
        case SequentialScanDisabled of
            true -> Default bor 16#4000;
            false -> Default
        end,

    [{queryN1QLFeatCtrl, "n1ql-feat-ctrl", Val}].

general_settings(Ver) ->
    [{queryTmpSpaceDir, "query.settings.tmp_space_dir",
      list_to_binary(path_config:component_path(tmp))},
     {queryTmpSpaceSize, "query.settings.tmp_space_size",
      ?QUERY_TMP_SPACE_DEF_SIZE},
     {queryPipelineBatch,      "pipeline-batch",      16},
     {queryPipelineCap,        "pipeline-cap",        512},
     {queryScanCap,            "scan-cap",            512},
     {queryTimeout,            "timeout",             0},
     {queryPreparedLimit,      "prepared-limit",      16384},
     {queryCompletedLimit,     "completed-limit",     4000},
     {queryCompletedThreshold, "completed-threshold", 1000},
     {queryLogLevel,           "loglevel",            <<"info">>},
     {queryMaxParallelism,     "max-parallelism",     1},
     {queryTxTimeout,          "txtimeout",           <<"0ms">>},
     {queryMemoryQuota,        "memory-quota",        0},
     {queryUseCBO,             "use-cbo",             true},
     {queryCleanupClientAttempts, "cleanupclientattempts", true},
     {queryCleanupLostAttempts, "cleanuplostattempts", true},
     {queryCleanupWindow,      "cleanupwindow",       <<"60s">>},
     {queryNumAtrs,            "numatrs",             1024}]
        ++ case cluster_compat_mode:is_version_76(Ver) of
               true ->
                   [{queryNodeQuota, "node-quota", ?QUERY_NODE_QUOTA_DEFAULT},
                    {queryUseReplica, "use-replica", <<"unset">>},
                    {queryNodeQuotaValPercent,
                     "node-quota-val-percent", 67},
                    {queryNumCpus, "num-cpus", 0},
                    {queryCompletedMaxPlanSize,
                     "completed-max-plan-size", 262144}];
               false ->
                   []
           end
        ++ case cluster_compat_mode:is_version_morpheus(Ver) of
               true ->
                   [{queryCompletedStreamSize,
                     "completed-stream-size", 0}];
               false ->
                   []
           end
        ++ n1ql_feature_ctrl_setting(Ver).

curl_whitelist_settings_len_props() ->
    [{queryCurlWhitelist, id_lens(<<"query.settings.curl_whitelist">>)}].

general_settings_defaults(Ver) ->
    [{K, D} || {K, _, D} <- general_settings(Ver)].

curl_whitelist_settings_defaults() ->
    [{queryCurlWhitelist, {[{<<"all_access">>, false},
                            {<<"allowed_urls">>, []},
                            {<<"disallowed_urls">>, []}]}}].

general_settings_lens(Ver) ->
    json_settings_manager:props_lens(
      [{K, id_lens(list_to_binary(L))} || {K, L, _} <- general_settings(Ver)]).

curl_whitelist_settings_lens() ->
    json_settings_manager:props_lens(curl_whitelist_settings_len_props()).

-ifdef(TEST).
config_upgrade_test() ->
    meck:new(config_profile, [passthrough]),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),

    CmdList = config_upgrade_to_76([]),
    [{set, {metakv, Meta}, Data}] = CmdList,
    ?assertEqual(<<"/query/settings/config">>, Meta),
    ?assertEqual(<<"{\"completed-max-plan-size\":262144,"
                   "\"node-quota\":0,"
                   "\"node-quota-val-percent\":67,"
                   "\"num-cpus\":0,"
                   "\"use-replica\":\"unset\"}">>,
                 Data),

    %% Upgrade to 7.6 for provisioned profile should update n1ql-feat-ctrl
    %% to disable sequential scans.
    meck:expect(config_profile, get_bool,
                fun ({n1ql, sequential_scan_disabled}) ->
                        true
                end),

    CmdList1 = config_upgrade_to_76([]),
    [{set, {metakv, Meta1}, Data1}] = CmdList1,
    ?assertEqual(<<"/query/settings/config">>, Meta1),
    ?assertEqual(<<"{\"completed-max-plan-size\":262144,"
                   "\"n1ql-feat-ctrl\":16460,"
                   "\"node-quota\":0,"
                   "\"node-quota-val-percent\":67,"
                   "\"num-cpus\":0,"
                   "\"use-replica\":\"unset\"}">>,
                 Data1),

    meck:expect(config_profile, get_bool,
                fun ({n1ql, sequential_scan_disabled}) ->
                        false
                end),

    CmdList2 = config_upgrade_to_morpheus([]),
    [{set, {metakv, Meta2}, Data2}] = CmdList2,
    ?assertEqual(<<"/query/settings/config">>, Meta2),
    ?assertEqual(<<"{\"completed-stream-size\":0}">>,
                 Data2),
    meck:unload(config_profile).

create_test_config_n1ql_quotas(NodeQuotaValue) when is_number(NodeQuotaValue) ->
    WOutNodeQuota = proplists:delete(queryNodeQuota,
                                     general_settings(?VERSION_76)),
    Settings =
        [{K, V} ||
            {K, _, V} <-
                [{queryNodeQuota,
                  "node-quota", NodeQuotaValue} | WOutNodeQuota]],
    SettingsBlob = json_settings_manager:build_settings_json(
                     [{generalSettings, Settings}],
                     maps:new(),
                     known_settings(?VERSION_76)),
    #config{static = [[], []], dynamic = [[{cfg_key(), SettingsBlob}], []]}.

quota_test_fun(Number) when is_number(Number) ->
    Config = create_test_config_n1ql_quotas(Number),
    Quota = memory_quota:get_quota(Config, n1ql),
    ?assertEqual({ok, Number}, Quota).

%% These tests ensure 1 very necessary fact which is that modifying
%% queryNodeQuota will modify the 'memoryQuota' (or more accurately, what we
%% THINK the memoryQuota is) of n1ql on that node. This was done because there
%% already existed a 'memoryQuota' field but it means something completely
%% different.
n1ql_quota_test_() ->
    {setup,
     fun () ->
             config_profile:mock_default_profile(),
             meck:new(cluster_compat_mode, [passthrough]),
             meck:expect(cluster_compat_mode, is_cluster_76,
                         fun () -> true end),
             meck:expect(cluster_compat_mode,
                         get_ns_config_compat_version,
                         fun () -> ?VERSION_76 end)
     end,
     fun (_X) ->
             config_profile:unmock_default_profile(ok),
             meck:unload(cluster_compat_mode)
     end,
     %% Keep in mind there is no validation on this function, but it will be
     %% validated on it's way in normally through menelaus_web_queries and the
     %% associated validators. Instead we merely want to ensure the change is
     %% properly reflected, and uses a few values to test it.
     [{"n1ql quota: 0", fun () -> quota_test_fun(0) end},
      {"n1ql quota: 1024", fun () -> quota_test_fun(1024) end},
      {"n1ql quota: 10240", fun () -> quota_test_fun(10240) end}]}.
-endif.
