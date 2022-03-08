%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(menelaus_web_analytics).

-include("ns_common.hrl").
-include("cut.hrl").
-export([handle_settings_get/1, handle_settings_post/1]).

handle_settings_get(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_71(),
    Settings = get_settings(),
    menelaus_util:reply_json(Req, {Settings}).

get_settings() ->
    analytics_settings_manager:get(generalSettings).

settings_post_validators() ->
    [validator:has_params(_),
     validator:integer(numReplicas, 0, 3, _),
     validator:unsupported(_)].

update_settings(Key, Value) ->
    case analytics_settings_manager:update(Key, Value) of
        {ok, _} ->
            ok;
        retry_needed ->
            erlang:error(exceeded_retries)
    end.

handle_settings_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_71(),
    validator:handle(
      fun (Values) ->
              case Values of
                  [] ->
                      ok;
                  _ ->
                      ok = update_settings(generalSettings, Values),
                      ns_audit:modify_analytics_settings(Req, Values)
              end,
              menelaus_util:reply_json(Req, {get_settings()})
      end, Req, form, settings_post_validators()).
