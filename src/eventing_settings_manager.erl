%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-2019 Couchbase, Inc.
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
-module(eventing_settings_manager).

-include("ns_common.hrl").

-behavior(json_settings_manager).

-export([start_link/0,
         get_from_config/3,
         update_txn/1,
         config_upgrade_to_55/0
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
    cluster_compat_mode:is_cluster_55().

on_update(_Key, _Value) ->
    ok.

known_settings() ->
    [{memoryQuota, id_lens(<<"ram_quota">>)}].

default_settings() ->
    [{memoryQuota, 256}].

get_from_config(Config, Key, Default) ->
    json_settings_manager:get_from_config(?MODULE, Config, Key, Default).

update_txn(Props) ->
    json_settings_manager:update_txn(?MODULE, Props).

config_upgrade_to_55() ->
    [{set, cfg_key(),
      json_settings_manager:build_settings_json(default_settings(), dict:new(),
                                                known_settings())}].
