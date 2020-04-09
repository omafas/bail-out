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

%% @doc rest api's for license and on-demand pricing related settings

-module(menelaus_web_license).

-include("ns_common.hrl").
-include_lib("cut.hrl").

-export([handle_settings_get/1,
         handle_settings_post/1,
         handle_settings_validate_post/1]).

handle_settings_get(Req) ->
    menelaus_util:assert_is_enterprise(),
    Settings = license_reporting:build_settings(),
    menelaus_util:reply_json(Req, {prepare_settings(Settings)}).

handle_settings_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    validator:handle(
      fun (Props) ->
              set_settings(Props),
              ns_audit:license_settings(Req, prepare_settings(Props)),
              handle_settings_get(Req)
      end, Req, form, settings_validators()).

handle_settings_validate_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    validator:handle(
      fun (Props) ->
              Settings = misc:update_proplist(
                           license_reporting:build_settings(), Props),
              case license_reporting:validate_settings(Settings) of
                  {ok, Report} -> menelaus_util:reply_json(Req, Report);
                  {error, Reason} -> menelaus_util:reply_json(Req, Reason, 400)
              end
      end, Req, form, [validator:boolean(generation_only, _)|
                       settings_validators()]).

settings_validators() ->
    [
        validator:boolean(reporting_enabled, _),
        validator:integer(reporting_interval, 60000, infinity, _),
        validator:touch(contract_id, _),
        validator:touch(customer_token, _),
        validator:convert(customer_token, ?cut({password, _}), _),
        validator:has_params(_),
        validator:unsupported(_)
    ].

prepare_settings(Settings) ->
    Fun =
      fun (customer_token, {password, ""}) -> <<>>;
          (customer_token, {password, _}) -> <<"**********">>;
          (contract_id, B) -> iolist_to_binary(B);
          (reporting_endpoint, E) -> iolist_to_binary(E);
          (_, Value) -> Value
      end,
    [{K, Fun(K, V)} || {K, V} <- Settings].

set_settings(UpdatedProps) ->
    OldProps = ns_config:read_key_fast(license_settings, []),
    NewProps = misc:update_proplist(OldProps, UpdatedProps),
    ns_config:set(license_settings, NewProps).

