%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(menelaus_web_app_telemetry).

-include("ns_common.hrl").

%% API
-export([handle_get/1, handle_post/1, get_config/0, is_enabled/1,
         get_max_clients_per_node/1, get_scrape_interval/1,
         is_accepting_connections/0]).

-define(CONFIG_KEY, app_telemetry).

%%%===================================================================
%%% API
%%%===================================================================

handle_get(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),

    menelaus_web_settings2:handle_get([], params(), undefined, get_config(),
                                      Req).

handle_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),

    menelaus_web_settings2:handle_post(
      fun (Params, Req2) ->
              case Params of
                  [] -> ok;
                  _ ->
                      Props = lists:map(fun ({[K], V}) -> {K, V} end, Params),
                      Values = set_config(Props),
                      ns_audit:app_telemetry_settings(Req, Values)
              end,
              handle_get(Req2)
      end, [], params(), undefined, get_config(), [], Req).

set_config(Changes) ->
    OldConfig = get_config(),

    NewConfig = misc:update_proplist(OldConfig, Changes),
    ns_config:set(?CONFIG_KEY, NewConfig),
    NewConfig.

params() ->
    [{"enabled",
      #{type => bool,
        cfg_key => enabled,
        default => ?APP_TELEMETRY_ENABLED}},
     {"maxScrapeClientsPerNode",
      #{type => {num, 1, 1024},
        cfg_key => max_scrape_clients_per_node,
        default => ?APP_TELEMETRY_MAX_CLIENTS_PER_NODE}},
     {"scrapeIntervalSeconds",
      #{type => {num, ?APP_TELEMETRY_MIN_SCRAPE_INTERVAL_SECONDS,
                 ?APP_TELEMETRY_MAX_SCRAPE_INTERVAL_SECONDS},
        cfg_key => scrape_interval_seconds,
        default => ?APP_TELEMETRY_DEFAULT_SCRAPE_INTERVAL_SECONDS}}].

-spec get_config() -> proplists:proplist().
get_config() ->
    ns_config:read_key_fast(?CONFIG_KEY, []).

-spec is_enabled(proplists:proplist()) -> boolean().
is_enabled(Config) ->
    proplists:get_value(enabled, Config, ?APP_TELEMETRY_ENABLED).

-spec get_max_clients_per_node(proplists:proplist()) -> integer().
get_max_clients_per_node(Config) ->
    proplists:get_value(max_scrape_clients_per_node, Config,
                        ?APP_TELEMETRY_MAX_CLIENTS_PER_NODE).

-spec get_scrape_interval(proplists:proplist()) -> integer().
get_scrape_interval(Config) ->
    proplists:get_value(scrape_interval_seconds, Config,
                        ?APP_TELEMETRY_DEFAULT_SCRAPE_INTERVAL_SECONDS).

-spec is_accepting_connections() -> boolean().
is_accepting_connections() ->
    Config = get_config(),
    is_enabled(Config).
