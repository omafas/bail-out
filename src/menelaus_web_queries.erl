%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-2018 Couchbase, Inc.
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
-module(menelaus_web_queries).
-include("ns_common.hrl").
-include("cut.hrl").
-export([handle_settings_get/1,
         handle_curl_whitelist_post/1,
         handle_curl_whitelist_get/1,
         handle_settings_post/1]).

handle_settings_get(Req) ->
    menelaus_util:assert_is_55(),

    Config = get_settings(),
    menelaus_util:reply_json(Req, {Config}).

get_settings() ->
    query_settings_manager:get(generalSettings) ++
    query_settings_manager:get(curlWhitelistSettings).

settings_post_validators() ->
    [validator:has_params(_),
     validator:integer(queryTmpSpaceSize, _),
     validate_tmp_space_size(queryTmpSpaceSize, _),
     validator:dir(queryTmpSpaceDir, _),
     validator:convert(queryTmpSpaceDir, fun list_to_binary/1, _)] ++
        settings_post_validators_65() ++
        [validator:unsupported(_)].

settings_post_validators_65() ->
    case cluster_compat_mode:is_cluster_65() of
        true ->
            [validator:integer(queryPipelineBatch, _),
             validator:integer(queryPipelineCap, _),
             validator:integer(queryScanCap, _),
             validator:integer(queryTimeout, _),
             validator:integer(queryPreparedLimit, 0, infinity, _),
             validator:integer(queryCompletedLimit, _),
             validator:integer(queryCompletedThreshold, _),
             validator:integer(queryMaxParallelism, _),
             validator:integer(queryN1QLFeatCtrl, _),
             validator:one_of(queryLogLevel, ["debug", "trace", "info", "warn",
                                              "error", "severe", "none"], _),
             validator:convert(queryLogLevel, fun list_to_binary/1, _)];
        false ->
            []
    end.

validate_tmp_space_size(Name, State) ->
    %% zero disables the feature, and -1 implies unlimited quota
    validator:range(Name, -1, infinity, State).

update_settings(Key, Value) ->
    case query_settings_manager:update(Key, Value) of
        {ok, _} ->
            ok;
        retry_needed ->
            erlang:error(exceeded_retries)
    end.

handle_settings_post(Req) ->
    menelaus_util:assert_is_55(),

    validator:handle(
      fun (Values) ->
              ok = update_settings(generalSettings, Values),
              ns_audit:modify_query_settings(Req, Values),
              menelaus_util:reply_json(Req, {get_settings()})
      end, Req, form, settings_post_validators()).

settings_curl_whitelist_validators() ->
    ConvertArray = [list_to_binary(L) || L <- _],
    [validator:required(all_access, _),
     validator:boolean(all_access, _),
     validator:string_array(allowed_urls, _),
     validator:convert(allowed_urls, ConvertArray, _),
     validator:string_array(disallowed_urls, _),
     validator:convert(disallowed_urls, ConvertArray, _),
     validator:unsupported(_)].

get_curl_whitelist_settings() ->
    Config = query_settings_manager:get(curlWhitelistSettings),
    %% queryCurlWhitelist should always be present.
    proplists:get_value(queryCurlWhitelist, Config).

handle_curl_whitelist_post(Req) ->
    menelaus_util:assert_is_55(),
    validator:handle(
      fun (Values) ->
              ok = update_settings(curlWhitelistSettings,
                                   [{queryCurlWhitelist, {Values}}]),
              ns_audit:modify_query_curl_whitelist_setting(Req, Values),
              menelaus_util:reply_json(Req, get_curl_whitelist_settings())
      end, Req, json, settings_curl_whitelist_validators()).

handle_curl_whitelist_get(Req) ->
    menelaus_util:assert_is_55(),
    menelaus_util:reply_json(Req, get_curl_whitelist_settings()).
