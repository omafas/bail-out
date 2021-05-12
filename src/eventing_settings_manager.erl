%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(eventing_settings_manager).

-include("ns_common.hrl").

-behavior(json_settings_manager).

-export([start_link/0,
         get_from_config/3,
         update_txn/1,
         config_default/0
        ]).

-export([cfg_key/0,
         is_enabled/0,
         known_settings/0,
         on_update/2]).

-import(json_settings_manager,
        [id_lens/1]).

start_link() ->
    json_settings_manager:start_link(?MODULE).

cfg_key() ->
    {metakv, <<"/eventing/settings/config">>}.

is_enabled() ->
    true.

on_update(_Key, _Value) ->
    ok.

known_settings() ->
    [{memoryQuota, id_lens(<<"ram_quota">>)}].

config_default() ->
    {cfg_key(), json_settings_manager:build_settings_json(
                  default_settings(), dict:new(),
                  known_settings())}.

default_settings() ->
    [{memoryQuota, 256}].

get_from_config(Config, Key, Default) ->
    json_settings_manager:get_from_config(?MODULE, Config, Key, Default).

update_txn(Props) ->
    json_settings_manager:update_txn(?MODULE, Props).
