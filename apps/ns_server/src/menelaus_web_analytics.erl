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
-export([handle_settings_get/1, handle_settings_post/1, cluster_init_validators/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

handle_settings_get(Req) ->
    case cluster_compat_mode:prod_name() of
        ?ANALYTICS_PROD_NAME ->
            handle_settings_get_enterprise_analytics(Req);
        _ ->
            handle_settings_get_legacy(Req)
    end.

handle_settings_post(Req) ->
    case cluster_compat_mode:prod_name() of
        ?ANALYTICS_PROD_NAME ->
            handle_settings_post_enterprise_analytics(Req);
        _ ->
            handle_settings_post_legacy(Req)
    end.

handle_settings_get_legacy(Req) ->
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

handle_settings_post_legacy(Req) ->
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

handle_settings_get_enterprise_analytics(Req) ->
    Env = ns_ports_setup:build_cbauth_env_vars(
        ns_config:latest(), "cbas_settings"),
    case misc:run_external_tool(
        path_config:component_path(bin, "cbas"),
        ["columnarSettings", "--get"], Env, [{stderr_to_stdout, false}]) of
        {0, StdErr, StdOut} ->
            ?log_debug("handle_settings_get success stderr: ~s", [StdErr]),
            menelaus_util:reply_ok(Req, "application/json", StdOut);
        {Code, StdErr, StdOut} ->
            ?log_debug("handle_settings_get fail ~p stderr: ~p",
                [Code, StdErr]),
            menelaus_util:reply_global_error(Req, StdOut)
    end.

handle_settings_post_enterprise_analytics(Req) ->
    %% we convert the body into a json object, and pass it to the cbas binary
    %% along with the preparation for audit logging
    Settings = [{settings,
        (lists:map(fun({K, V}) ->
            {K, convert_for_json_encoding(V)} end,
            mochiweb_request:parse_post(Req)))}],
    JsonObject = mochijson2:encode(
        Settings ++ [{audit, ns_audit:prepare_log_body(Req, [])}]),
    Env = ns_ports_setup:build_cbauth_env_vars(ns_config:latest(),
        "cbas_settings"),
    MaxStoragePartitions = config_profile:search(
        {cbas, max_storage_partitions}, -1),
    SkipValidation = config_profile:search(
        {cbas, skip_blob_storage_validation}, false),

    Args = ["columnarSettings", "--set"]
        ++ ["-maxStoragePartitions=" ++ integer_to_list(MaxStoragePartitions)]
        ++ ["-skipValidation=" ++ atom_to_list(SkipValidation)],

    case misc:run_external_tool(
        path_config:component_path(bin, "cbas"), Args, Env,
        [{write_data, JsonObject}, {stderr_to_stdout, false}]) of
        {0, StdErr, StdOut} ->
            ?log_debug("handle_settings_post success stderr: ~s", [StdErr]),
            menelaus_util:reply_ok(Req, "application/json", StdOut);
        {Code, StdErr, StdOut} ->
            ?log_debug("handle_settings_post fail ~p stderr: ~s",
                       [Code, StdErr]),
            menelaus_util:reply(Req, StdOut, 400,
                                [{"Content-Type", "application/json"}])
    end.

convert_for_json_encoding(Value) ->
    case Value of
        "true" -> true;
        "false" -> false;
        _ ->
            case catch list_to_integer(Value) of
                {'EXIT', _} ->
                    case catch list_to_float(Value) of
                        {'EXIT', _} ->
                            case is_list(Value) of
                                true -> list_to_binary(Value);
                                false -> Value
                            end;
                        Float -> Float
                    end;
                Int -> Int
            end
    end.

cluster_init_validators() ->
    [validator:post_validate_all(
        fun (_Props) ->
            Env = ns_ports_setup:build_cbauth_env_vars(ns_config:latest(),
                "cbas_settings"),
            Args = ["columnarSettings", "--validate"],
            case misc:run_external_tool(
                path_config:component_path(bin, "cbas"), Args, Env,
                [{stderr_to_stdout, false}]) of
                {0, StdErr, _StdOut} ->
                    ?log_debug("validate settings success stderr: ~p",
                        [StdErr]),
                    ok;
                {_, StdErr, StdOut} ->
                    ?log_debug(
                        "validate settings fail stderr: ~p stdout: ~p",
                        [StdErr, StdOut]),
                    {error, StdOut}
            end
        end, _) || not config_profile:search(
        {cbas, skip_blob_storage_validation}, true)].

-ifdef(TEST).

-define(assertResponse(ExpectedBody, ExpectedCode, BC),
    (fun () ->
        {Body, Code} = BC,
        ?assertEqual(ExpectedCode, Code),
        ?assertEqual(ExpectedBody, Body)
     end)()).
-define(MISSING_SETTINGS, "Columnar settings not configured. Configure"
" Columnar settings and try again.").

meck_modules() ->
    [config_profile, mochiweb_request, menelaus_util, ns_ports_setup, misc].

cluster_init_validators_test() ->
    try
        meck:new(meck_modules(), [passthrough]),
        meck:expect(config_profile, get,
            fun () ->
                ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
            end),
        [] = cluster_init_validators(),
        meck:expect(config_profile, get,
            fun () ->
                [
                    {name, "columnar"},
                    {{cbas, skip_blob_storage_validation}, false}
                ]
            end),
        meck:expect(mochiweb_request, recv_body, fun (Req) -> Req end),
        meck:expect(mochiweb_request, parse_qs, fun (_Req) -> [] end),
        Respond = fun (Body, Code) ->
            erlang:put(json_test_response, {Body, Code})
                  end,
        GlobalError = ?cut({[{errors, {[{"_", list_to_binary(_)}]}}]}),
        JsonObject = <<"{\"key1\": \"v1\"}">>,
        meck:expect(menelaus_util, reply_json,
            fun (_Req, Body, Code) ->
                Respond(Body, Code)
            end),
        meck:expect(ns_ports_setup,build_cbauth_env_vars,
            fun (_Config, _Name) ->
                [{<<"CBAuth">>, <<"cbas_settings">>}]
            end),
        meck:expect(misc, run_external_tool,
            fun (_Path, _Args, _Env, _Options) ->
                {0, <<"<stderr>">>, <<"<stdout>">>}
            end),
        validator:handle(Respond(_, 200), JsonObject,
            json, cluster_init_validators()),
        meck:expect(misc, run_external_tool,
            fun (_Path, _Args, _Env, _Options) ->
                {1, <<"<stderr>">>, <<?MISSING_SETTINGS>>}
            end),
        validator:handle(Respond(_, 200), JsonObject,
            json, cluster_init_validators()),
        ?assertResponse(
            GlobalError(?MISSING_SETTINGS), 400, erlang:get(json_test_response))
    after
        meck:unload(meck_modules())
    end.
-endif.
