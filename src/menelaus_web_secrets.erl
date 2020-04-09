%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2018 Couchbase, Inc.
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

%% @doc rest api's for secrets

-module(menelaus_web_secrets).

-include("ns_common.hrl").
-include("cut.hrl").

-export([handle_change_master_password/1,
         handle_rotate_data_key/1]).

handle_change_master_password(Req) ->
    menelaus_util:assert_is_enterprise(),
    validator:handle(
      fun (Values) ->
              NewPassword = proplists:get_value(newPassword, Values),
              case encryption_service:change_password(NewPassword) of
                  ok ->
                      ns_audit:master_password_change(Req, undefined),
                      menelaus_util:reply(Req, 200);
                  {error, Error} ->
                      ns_audit:master_password_change(Req, Error),
                      menelaus_util:reply_global_error(Req, Error)
              end
      end, Req, form, change_master_password_validators()).

change_master_password_validators() ->
    [validator:required(newPassword, _),
     validator:unsupported(_)].

handle_rotate_data_key(Req) ->
    menelaus_util:assert_is_enterprise(),

    RV = encryption_service:rotate_data_key(),
    %% the reason that resave is called regardless of the return value of
    %% rotate_data_key is that in case of previous insuccessful attempt to
    %% rotate, the backup key is still might be set in encryption_service
    %% and we want to clean it up, so the next attempt to rotate will succeed
    ns_config:resave(),
    case RV of
        ok ->
            ns_audit:data_key_rotation(Req, undefined),
            menelaus_util:reply(Req, 200);
        {error, Error} ->
            ns_audit:data_key_rotation(Req, Error),
            menelaus_util:reply_global_error(Req, Error ++ ". You might try one more time.")
    end.
