%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc rest api's for node secrets manager

-module(menelaus_web_sm).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-export([handle_change_master_password/1,
         handle_rotate_data_key/1,
         handle_get_settings/2,
         handle_post_settings/2]).

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
    [validator:touch(newPassword, _),
     validator:unsupported(_)].

handle_rotate_data_key(Req) ->
    menelaus_util:assert_is_enterprise(),

    RV = encryption_service:rotate_data_key(),
    {ok, KeysRef} = encryption_service:get_keys_ref(),
    ok = encryption_service:maybe_clear_backup_key(KeysRef),
    case RV of
        ok ->
            ns_audit:data_key_rotation(Req, undefined),
            menelaus_util:reply(Req, 200);
        {error, Error} ->
            ns_audit:data_key_rotation(Req, Error),
            menelaus_util:reply_global_error(Req, Error ++ ". You might try one more time.")
    end.

handle_get_settings(Path, Req) ->
    menelaus_util:assert_is_enterprise(),
    {ok, Rv} = encryption_service:get_state(),

    Node = dist_manager:this_node(),
    Settings = ns_config:read_key_fast({node, Node, secret_mngmt_cfg}, []),
    Settings2 = misc:update_proplist(cb_gosecrets_runner:defaults(), Settings),
    ROSettings = [{es_password_state, binary_to_list(Rv)}],
    menelaus_web_settings2:handle_get(
      Path, params(), undefined, cleanup_settings(Settings2 ++ ROSettings),
      Req).

handle_post_settings(Path, Req) ->
    menelaus_util:assert_is_enterprise(),
    Node = dist_manager:this_node(),
    Current = ns_config:read_key_fast({node, Node, secret_mngmt_cfg}, []),
    menelaus_web_settings2:handle_post(
      fun (Proplist, NewReq) ->
          Proplist2 = lists:map(fun ({[K], V}) -> {K, V} end, Proplist),
          NewSettings = misc:update_proplist(Current, Proplist2),
          case encryption_service:reconfigure(cleanup_settings(NewSettings)) of
              ok ->
                    handle_get_settings(Path, NewReq);
              {error, ErrorIOList} ->
                    menelaus_util:reply_global_error(Req, ErrorIOList)
          end
      end, Path, params(), undefined,
      Current, cb_gosecrets_runner:defaults(), Req).

params() ->
    [{"encryptionService.keyStorageType",
      #{cfg_key => es_key_storage_type,
        type => {one_of, existing_atom, [file, script]}}},
     {"encryptionService.keyPath",
      #{cfg_key => es_key_path_type,
        type => {one_of, existing_atom, [auto, custom]}}},
     {"encryptionService.customKeyPath",
      #{cfg_key => es_custom_key_path,
        type => string,
        mandatory => fun (#{es_key_path_type := custom}) -> true;
                         (_) -> false
                      end}},
     {"encryptionService.keyEncrypted",
      #{cfg_key => es_encrypt_key,
        type => bool}},
     {"encryptionService.passwordSource",
      #{cfg_key => es_password_source,
        type => {one_of, existing_atom, [env, script]}}},
     {"encryptionService.passwordEnv",
      #{cfg_key => es_password_env,
        type => string}},
     {"encryptionService.passwordCmd",
      #{cfg_key => es_password_cmd,
        type => string,
        mandatory => fun (#{es_password_source := script}) -> true;
                         (_) -> false
                     end}},
     {"encryptionService.readCmd",
      #{cfg_key => es_read_cmd,
        type => string,
        mandatory => fun (#{es_key_storage_type := script}) -> true;
                         (_) -> false
                     end}},
     {"encryptionService.writeCmd",
      #{cfg_key => es_write_cmd,
        type => string,
        mandatory => fun (#{es_key_storage_type := script}) -> true;
                         (_) -> false
                     end}},
     {"encryptionService.deleteCmd",
      #{cfg_key => es_delete_cmd,
        type => string,
        mandatory => fun (#{es_key_storage_type := script}) -> true;
                         (_) -> false
                     end}},
     {"encryptionService.passwordState",
      #{cfg_key => es_password_state,
        type => {read_only, string}}}].

cleanup_settings(Settings) ->
    Defaults = cb_gosecrets_runner:defaults(),
    DefaultType = proplists:get_value(es_key_storage_type, Defaults),
    DefaultPass = proplists:get_value(es_password_source, Defaults),
    DefaultEncr = proplists:get_value(es_encrypt_key, Defaults),
    Fields =
        case proplists:get_value(es_key_storage_type, Settings, DefaultType) of
            file ->
                [es_key_path_type, es_custom_key_path, es_encrypt_key] ++
                case proplists:get_value(es_encrypt_key, Settings,
                                         DefaultEncr) of
                    true ->
                        [es_password_source] ++
                        case proplists:get_value(es_password_source, Settings,
                                                 DefaultPass) of
                            env -> [es_password_env];
                            script -> [es_password_cmd]
                        end;
                    false ->
                        []
                end;
            script ->
                [es_read_cmd, es_write_cmd, es_delete_cmd]
        end ++ [es_key_storage_type, es_password_state, bucket_dek_path],
    lists:filter(fun ({K, _}) -> lists:member(K, Fields) end, Settings).
