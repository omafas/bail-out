%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(query_settings_manager).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behavior(json_settings_manager).

-export([start_link/0,
         get/1,
         get_from_config/3,
         update/2,
         config_default/0,
         config_upgrade_to_70/1
        ]).

-export([cfg_key/0,
         is_enabled/0,
         known_settings/0,
         on_update/2]).

-import(json_settings_manager,
        [id_lens/1]).

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

config_default() ->
    {?QUERY_CONFIG_KEY, json_settings_manager:build_settings_json(
                          default_settings(?VERSION_65),
                          dict:new(), known_settings(?VERSION_65))}.

config_upgrade_to_70(Config) ->
    NewSettings = general_settings_defaults(?VERSION_70) --
        general_settings_defaults(?VERSION_65),
    UpdatePropsFun = fun (PropsDict) ->
                             dict:update(<<"n1ql-feat-ctrl">>,
                                         fun (OldValue) ->
                                                 OldValue bor 16#40
                                         end, 16#40, PropsDict)
                     end,
    json_settings_manager:upgrade_existing_key(
      ?MODULE, Config, [{generalSettings, NewSettings}],
      known_settings(?VERSION_70), UpdatePropsFun).

known_settings() ->
    known_settings(cluster_compat_mode:get_compat_version()).

known_settings(Ver) ->
    [{generalSettings, general_settings_lens(Ver)},
     {curlWhitelistSettings, curl_whitelist_settings_lens()}].

default_settings(Ver) ->
    [{generalSettings, general_settings_defaults(Ver)},
     {curlWhitelistSettings, curl_whitelist_settings_defaults()}].

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
     {queryN1QLFeatCtrl,       "n1ql-feat-ctrl",      12}] ++
        case cluster_compat_mode:is_version_70(Ver) of
            true ->
                [{queryTxTimeout,          "txtimeout",           <<"0ms">>},
                 {queryMemoryQuota,        "memory-quota",        0},
                 {queryUseCBO,             "use-cbo",             true},
                 {queryCleanupClientAttempts, "cleanupclientattempts", true},
                 {queryCleanupLostAttempts, "cleanuplostattempts", true},
                 {queryCleanupWindow,      "cleanupwindow",       <<"60s">>},
                 {queryNumAtrs,            "numatrs",             1024}];
            false ->
                []
        end.

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
