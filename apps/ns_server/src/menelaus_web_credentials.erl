%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc REST API for credential store settings
%% This module provides the REST API for managing credential store settings via
%% the /settings/credentialStore JSON-only endpoint.
%%
%% Sample REST request:
%% curl -X PUT -u Administrator:password -H "Content-Type: application/json" \
%%      -d '{
%%           "configEncryptionOverride": false,
%%           "n2nEncryptionOverride": false
%%          }'
%%      http://localhost:8091/settings/credentialStore

-module(menelaus_web_credentials).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_settings/2]).

%% @doc Parameters and their descriptions:
%% config_encryption_override - By default, config encryption is required.
%% n2n_encryption_override - By default, node-to-node encryption must be
%% enabled.
-define(PARAMS_WITH_FORMATTERS,
        [
         {config_encryption_override, undefined},
         {n2n_encryption_override, undefined}
        ]).

-define(REST_TO_STORAGE,
        maps:from_list([{snake_to_camel_atom(Key), Key} ||
                           {Key, _} <- ?PARAMS_WITH_FORMATTERS])).

-define(STORAGE_TO_REST,
        maps:from_list([{Key, {snake_to_camel_atom(Key), Format}} ||
                           {Key, Format} <- ?PARAMS_WITH_FORMATTERS])).

snake_to_camel_atom(Atom) when is_atom(Atom) ->
    Parts = string:split(atom_to_list(Atom), "_", all),
    [First | Rest] = Parts,
    Camel = [First | [string:titlecase(Part) || Part <- Rest]],
    list_to_atom(lists:concat(Camel)).

encode_response(Value) ->
    try
        json:encode(Value)
    catch T:E:Stack ->
            ?log_error("Error encoding response:~n~p", [Value]),
            erlang:raise(T, E, Stack)
    end.

handle_settings(Method, Req) ->
    try
        menelaus_util:assert_is_enterprise(),
        menelaus_util:assert_is_totoro(),
        case Method of
            'GET' -> handle_settings_get(Req);
            'PUT' -> handle_settings_put(Req);
            'DELETE' -> handle_settings_delete(Req)
        end
    catch
        throw:{web_exception, Status, Msg} ->
            menelaus_util:reply_json(Req, {[{error, iolist_to_binary(Msg)}]},
                                     Status)
    end.

defaults() ->
    #{config_encryption_override => false,
      n2n_encryption_override => false}.

get_settings() ->
    Stored = chronicle_compat:get(direct, credential_store_settings,
                                  #{default => #{}}),
    maps:merge(defaults(), Stored).

handle_settings_get(Req) ->
    Settings = get_settings(),
    RestFormat = storage_to_rest_format(Settings),
    JsonBin = encode_response(RestFormat),
    menelaus_util:reply(Req, JsonBin, 200,
                        [{"Content-Type", "application/json"}]).

handle_settings_put(Req) ->
    validator:handle(
      fun (Props) ->
              validate_and_store_settings(Props, Req)
      end,
      Req, json, validators()).

handle_settings_delete(Req) ->
    Fun = fun (_) -> {commit, [{delete, credential_store_settings}]} end,
    case chronicle_kv:transaction(kv, [], Fun, #{}) of
        {ok, _} ->
            ns_audit:settings(Req, modify_credential_store,
                              [{settings, deleted}]),
            menelaus_util:reply_json(Req, {[]}, 200);
        {error, Error} ->
            ?log_error("Failed to delete creds store settings: ~p", [Error]),
            menelaus_util:reply_json(Req,
                                     {[{error,
                                        <<"Failed to delete settings">>}]},
                                     500)
    end.

validators() ->
    [validator:required(configEncryptionOverride, _),
     validator:boolean(configEncryptionOverride, _),
     validator:required(n2nEncryptionOverride, _),
     validator:boolean(n2nEncryptionOverride, _),
     validator:unsupported(_)].

validate_and_store_settings(Props, Req) ->
    Settings = validated_to_storage_format(Props),
    Fun = fun (_) -> {commit, [{set, credential_store_settings, Settings}]} end,
    case chronicle_kv:transaction(kv, [], Fun, #{}) of
        {ok, _} ->
            RestFormat = storage_to_rest_format(Settings),
            EncodedSettings = encode_response(RestFormat),
            ns_audit:settings(Req, modify_credential_store,
                              [{settings,
                                {json,
                                 iolist_to_binary(EncodedSettings)}}]),
            menelaus_util:reply(Req, EncodedSettings, 200,
                                [{"Content-Type", "application/json"}]);
        {error, Error} ->
            ?log_error("Failed to store creds store settings: ~p", [Error]),
            menelaus_util:reply_json(Req,
                                     {[{error,
                                        <<"Failed to store settings">>}]},
                                     500)
    end.

storage_to_rest_format(Settings) ->
    maps:fold(
      fun(StorageKey, Value, Acc) ->
              storage_to_rest_format_key(StorageKey, Value, Acc,
                                         ?STORAGE_TO_REST)
      end, #{}, Settings).

storage_to_rest_format_key(StorageKey, Value, Acc, Table) ->
    case maps:find(StorageKey, Table) of
        {ok, {RestKey, undefined}} when Value =/= undefined ->
            Acc#{RestKey => Value};
        {ok, {RestKey, Formatter}} ->
            case Formatter(Value) of
                undefined -> Acc;
                FormattedValue -> Acc#{RestKey => FormattedValue}
            end;
        _ ->
            Acc
    end.

%% @doc Converts validated properties (from validator:handle/4) to storage
%% format. Input is a proplist with atom keys (from validator) and values in
%% their validated format. Output is a map with snake_case atom keys suitable
%% for storage.
validated_to_storage_format(Props) ->
    lists:foldl(
      fun({OtherKey, Value}, Acc) ->
              {ok, StorageKey} = maps:find(OtherKey, ?REST_TO_STORAGE),
              Acc#{StorageKey => Value}
      end, #{}, Props).

-ifdef(TEST).
roundtrip_test() ->
    Settings = #{config_encryption_override => false,
                 n2n_encryption_override => false},
    RestFormat = storage_to_rest_format(Settings),
    ?assertEqual(false, maps:get(configEncryptionOverride, RestFormat)),
    ?assertEqual(false, maps:get(n2nEncryptionOverride, RestFormat)).
-endif.
