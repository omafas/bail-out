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

-behavior(json_settings_manager).

-export([start_link/0,
         get/1,
         get_from_config/3,
         update/2,
         config_default/0]).

-export([cfg_key/0,
         is_enabled/0,
         known_settings/0,
         on_update/2]).

-import(json_settings_manager,
        [id_lens/1]).

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
    cluster_compat_mode:is_cluster_71().

on_update(_Key, _Value) ->
    ok.

update(Key, Value) ->
    json_settings_manager:update(?MODULE, [{Key, Value}]).

config_default() ->
    {?ANALYTICS_CONFIG_KEY, json_settings_manager:build_settings_json(
                 default_settings(), dict:new(), known_settings())}.

known_settings() ->
    [{generalSettings, general_settings_lens()}].

default_settings() ->
    [{generalSettings, general_settings_defaults()}].

general_settings() ->
    [{numReplicas, "analytics.settings.num_replicas", 0}].

general_settings_defaults() ->
    [{K, D} || {K, _, D} <- general_settings()].

general_settings_lens() ->
    json_settings_manager:props_lens(
      [{K, id_lens(list_to_binary(L))} || {K, L, _} <- general_settings()]).
