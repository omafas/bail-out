%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-2019 Couchbase, Inc.
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
         config_upgrade_to_55/0,
         config_upgrade_to_65/1
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
    cluster_compat_mode:is_cluster_55().

on_update(_Key, _Value) ->
    ok.

update(Key, Value) ->
    json_settings_manager:update(?MODULE, [{Key, Value}]).

config_upgrade_to_55() ->
    [{set, ?QUERY_CONFIG_KEY,
      json_settings_manager:build_settings_json(
        default_settings(?VERSION_55), dict:new(),
        known_settings(?VERSION_55))}].

config_upgrade_to_65(Config) ->
    NewSettings = general_settings_defaults(?VERSION_65) --
        general_settings_defaults(?VERSION_55),
    json_settings_manager:upgrade_existing_key(
      ?MODULE, Config, [{generalSettings, NewSettings}],
      known_settings(?VERSION_65)).

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
      ?QUERY_TMP_SPACE_DEF_SIZE}] ++
        case cluster_compat_mode:is_version_65(Ver) of
            true ->
                [{queryPipelineBatch,      "pipeline-batch",      16},
                 {queryPipelineCap,        "pipeline-cap",        512},
                 {queryScanCap,            "scan-cap",            512},
                 {queryTimeout,            "timeout",             0},
                 {queryPreparedLimit,      "prepared-limit",      16384},
                 {queryCompletedLimit,     "completed-limit",     4000},
                 {queryCompletedThreshold, "completed-threshold", 1000},
                 {queryLogLevel,           "loglevel",            <<"info">>},
                 {queryMaxParallelism,     "max-parallelism",     1},
                 {queryN1QLFeatCtrl,       "n1ql-feat-ctrl",      12}];
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
