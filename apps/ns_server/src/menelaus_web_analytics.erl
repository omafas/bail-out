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
-include_lib("ns_common/include/cut.hrl").
-export([handle_settings_get/1, handle_settings_post/1]).

handle_settings_get(Req) ->
    menelaus_util:assert_is_enterprise(),
    Settings = get_settings(),
    menelaus_util:reply_json(Req, {Settings}).

blob_storage_settings() ->
    blob_storage_required_params() ++ blob_storage_optional_params().

maybe_filter_settings(Settings) ->
    FilterOutSettings =
        lists:flatten([blob_storage_settings() ||
                       not config_profile:search({cbas, enable_blob_storage},
                                                 false)]),
    maybe_filter_settings(Settings, FilterOutSettings).

maybe_filter_settings(Settings, []) ->
    Settings;
maybe_filter_settings(Settings, FilterOutSettings) ->
    lists:filter(
      fun( {Key, _Value}) ->
              not lists:member(Key, FilterOutSettings)
      end, Settings).

get_settings() ->
    Settings = analytics_settings_manager:get(generalSettings),
    maybe_filter_settings(Settings).

blob_storage_required_params() ->
    [blobStorageScheme, blobStorageBucket, blobStoragePrefix, blobStorageRegion].

blob_storage_optional_params() ->
    [blobStorageEndpoint, blobStorageAnonymousAuth, numStoragePartitions].

valid_blob_storage_param(blobStorageScheme, State) ->
    validator:one_of(blobStorageScheme, ["s3", "gs", "azblob"], State);
valid_blob_storage_param(_Param, State) ->
    State.

blob_storage_params_validator() ->
    Params = blob_storage_required_params(),
    MaxStoragePartitions =
        config_profile:get_value({cbas, max_storage_partitions}, infinity),

    % Validation should pass if:
    % 1. None of the blobStorage params are present.
    % 2. Or if all of the blobStorage Params are present and are all valid.
    [validator:validate_multiple(
        fun (_Values, State) ->
            NewState =
            functools:chain(
                State,
                lists:foldr(
                    fun (Param, Acc) ->
                        [validator:required(Param, _),
                            validator:string(Param, _),
                            valid_blob_storage_param(Param, _),
                            validator:convert(
                                Param, fun list_to_binary/1, _)
                            | Acc]
                    end, [], Params)),
            {ok, NewState}
        end, Params, _),
        validator:string(blobStorageEndpoint, _),
        validator:convert(blobStorageEndpoint, fun list_to_binary/1, _),
        validator:boolean(blobStorageAnonymousAuth, _),
        fun (State) ->
            %% blobStorageEndpoint is required when blobStorageScheme is azblob
            case validator:get_value(blobStorageScheme, State) of
                <<"azblob">> -> validator:required(blobStorageEndpoint, State);
                _ -> State
            end
        end,
        %% TODO(MB-61761): we don't currently support changing the number of
        %% storage partitions once Columnar has bootstrapped- ideally we can
        %% catch this here, but that requires context we don't have at present.
        validator:integer(numStoragePartitions, 1, MaxStoragePartitions, _)].

settings_post_validators() ->
    [validator:has_params(_),
     validator:integer(numReplicas, 0, 3, _)] ++
        case cluster_compat_mode:is_cluster_76() andalso
            config_profile:search({cbas, enable_blob_storage}, false) of
            true ->
                blob_storage_params_validator();
            false ->
                []
        end ++
        [validator:no_duplicates(_),
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
    validator:handle(
      fun (Values) ->
              case Values of
                  [] ->
                      ok;
                  _ ->
                      ok = update_settings(generalSettings, Values),
                      ns_audit:settings(Req, modify_analytics, Values)
              end,
              menelaus_util:reply_json(Req, {get_settings()})
      end, Req, form, settings_post_validators()).
