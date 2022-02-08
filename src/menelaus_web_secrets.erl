%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
