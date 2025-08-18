%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc REST API for JWT support
%% This module provides the REST API for managing JWT settings and issuers via
%% the /settings/jwt JSON-only endpoint.
%%
%% Storage Format:
%% Settings are stored in chronicle_kv as an Erlang map with snake_case atom
%% keys. Issuers are stored as a map keyed by issuer name for efficient lookup.
%% Example:
%% #{
%%   enabled => true,
%%   jwks_uri_refresh_interval_s => 14400,
%%   issuers => #{
%%     "issuer1" => #{
%%       signing_algorithm => "RS256",
%%       aud_claim => "aud",
%%       audience_handling => "any",
%%       audiences => ["audience1", "audience2"],
%%       expiry_leeway_s => 15,
%%       sub_claim => "sub",
%%       public_key_source => "pem",
%%       public_key => "-----BEGIN PUBLIC KEY-----\nMIIB...AQAB\n-----END
%%                       PUBLIC KEY-----",
%%       jit_provisioning => false
%%     },
%%     "issuer2" => #{
%%       signing_algorithm => "ES256",
%%       ...
%%     }
%%   }
%% }
%%
%% Sample REST request:
%% curl -X PUT -u Administrator:password -H "Content-Type: application/json" \
%%      -d '{
%%           "enabled": true,
%%           "jwksUriRefreshIntervalS": 14400,
%%           "issuers": [{
%%             "name": "issuer1",
%%             "signingAlgorithm": "RS256",
%%             "audClaim": "aud",
%%             "audienceHandling": "any",
%%             "audiences": ["audience1", "audience2"],
%%             "expiryLeewayS": 15,
%%             "subClaim": "sub",
%%             "publicKeySource": "pem",
%%             "publicKey": "-----BEGIN PUBLIC KEY-----\nMIIB...AQAB\n-----END
%%                           PUBLIC KEY-----",
%%             "jitProvisioning": false
%%           }]
%%          }'
%%      http://Administrator:8091/settings/jwt

-module(menelaus_web_jwt).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-include_lib("jose/include/jose_jwk.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_settings/2,
         is_enabled/0,
         sanitize_chronicle_cfg/1]).

-define(JWKS_URI_MIN_TIMEOUT_MS, 5000). % 5 seconds
-define(JWKS_URI_DEFAULT_TIMEOUT_MS, 5000). % 5 seconds
-define(JWKS_URI_MAX_TIMEOUT_MS, 60000). % 1 minute
-define(JWKS_URI_REFRESH_MIN_S,
        ?get_param(jwks_uri_refresh_min_s, 5*60)). % 5 minutes
-define(JWKS_URI_REFRESH_DEFAULT_S, 60*60). % 1 hour
-define(JWKS_URI_REFRESH_MAX_S, 86400). % 1 day
-define(EXPIRY_LEEWAY_MIN_S, 0).
-define(EXPIRY_LEEWAY_DEFAULT_S, 15). % 15 seconds
-define(EXPIRY_LEEWAY_MAX_S, 5*60). % 5 minutes
-define(SYNC_TIMEOUT, 60000). % 1 minute

%% @doc Main parameters and their descriptions:
%% enabled - Indicates whether JWT support is enabled.
%% issuers - A list of JWT issuers with their respective settings.
%% jwks_uri_refresh_interval_s - The interval in seconds for refreshing the
%% JWKS URI.
-define(MAIN_PARAMS_WITH_FORMATTERS,
        [
         {enabled, undefined},
         {issuers, fun storage_to_rest_format_issuers/1},
         {jwks_uri_refresh_interval_s, undefined}
        ]).

-define(MAIN_REST_TO_STORAGE,
        maps:from_list([{snake_to_camel_atom(Key), Key} ||
                           {Key, _} <- ?MAIN_PARAMS_WITH_FORMATTERS])).

-define(MAIN_STORAGE_TO_REST,
        maps:from_list([{Key, {snake_to_camel_atom(Key), Format}} ||
                           {Key, Format} <- ?MAIN_PARAMS_WITH_FORMATTERS])).

%% @doc Issuer parameters and their descriptions:
%% aud_claim - Field path of the claim containing the audience value
%% audience_handling - How to handle audience validation (any/all)
%% audiences - List of valid audience values to match against
%% expiry_leeway_s - Number of seconds of leeway when validating token expiry
%% groups_claim - Field path of the claim containing group memberships
%% groups_maps - Rules for mapping groups from tokens to local groups
%% groups_maps_stop_first_match - Whether to stop after first matching rule
%% sub_maps - Rules for mapping sub claims to local identities
%% jit_provisioning - Whether to allow authentication without a pre-existing
%% user in Couchbase - by dynamically mapping permissions from groups/roles
%% jwks - JSON Web Key Set containing public keys
%% jwks_uri - URI for fetching JWKS
%% jwks_uri_address_family - IP address family preference for JWKS URI (inet/
%% inet6)
%% jwks_uri_http_timeout_ms - HTTP request timeout for JWKS URI in milliseconds
%% jwks_uri_tls_ca - CA certificate for validating JWKS URI TLS connection
%% jwks_uri_tls_extra_opts - Additional TLS options for JWKS URI
%% jwks_uri_tls_sni - Server name for TLS SNI extension
%% jwks_uri_tls_verify_peer - Whether to verify JWKS URI server certificate
%% name - Unique name identifying this issuer
%% public_key - Public key used to verify token signatures
%% public_key_source - Source of the public key (jwks/jwks_uri/pem)
%% roles_claim - Field path of the claim containing role assignments
%% roles_maps - Rules for mapping roles from tokens to local roles
%% roles_maps_stop_first_match - Whether to stop after first matching roles map
%% shared_secret - Secret key for HMAC algorithms
%% signing_algorithm - Algorithm used to sign tokens
%% sub_claim - Field path of the claim containing the subject identifier
-define(ISSUER_PARAMS_WITH_FORMATTERS,
        [
         {aud_claim, fun format_string/1},
         {audience_handling, undefined},
         {audiences, fun format_string_list/1},
         {expiry_leeway_s, undefined},
         {groups_claim, fun format_string/1},
         {groups_maps, fun auth_mapping:format_mapping_rules/1},
         {groups_maps_stop_first_match, undefined},
         {sub_maps, fun auth_mapping:format_mapping_rules/1},
         {jit_provisioning, undefined},
         {jwks, fun format_jwks/1},
         {jwks_uri, fun format_string/1},
         {jwks_uri_address_family, undefined},
         {jwks_uri_http_timeout_ms, undefined},
         {jwks_uri_tls_ca, fun format_tls_ca/1},
         {jwks_uri_tls_extra_opts, fun format_tls_extra_opts/1},
         {jwks_uri_tls_sni, fun format_string/1},
         {jwks_uri_tls_verify_peer, undefined},
         {name, fun format_string/1},
         {public_key, fun format_public_key/1},
         {public_key_source, undefined},
         {roles_claim, fun format_string/1},
         {roles_maps, fun auth_mapping:format_mapping_rules/1},
         {roles_maps_stop_first_match, undefined},
         {shared_secret, fun format_secret/1},
         {signing_algorithm, undefined},
         {sub_claim, fun format_string/1}
        ]).

%% REST to storage mapping (camelCase atom -> snake_case atom)
-define(ISSUER_REST_TO_STORAGE,
        maps:from_list([{snake_to_camel_atom(Key), Key} ||
                           {Key, _} <- ?ISSUER_PARAMS_WITH_FORMATTERS])).

%% Storage to REST mapping (snake_case atom -> {camelCase atom, formatter})
-define(ISSUER_STORAGE_TO_REST,
        maps:from_list([{Key, {snake_to_camel_atom(Key), Format}} ||
                           {Key, Format} <- ?ISSUER_PARAMS_WITH_FORMATTERS])).

snake_to_camel_atom(Atom) when is_atom(Atom) ->
    Parts = string:split(atom_to_list(Atom), "_", all),
    [First | Rest] = Parts,
    Camel = [First | [string:titlecase(Part) || Part <- Rest]],
    list_to_atom(lists:concat(Camel)).

%% Best effort local sync to ensure subsequent JWT validation requests use the
%% new settings.
sync_with_node() ->
    %% Make sure we have the latest chronicle data
    ok = chronicle_kv:sync(kv, ?SYNC_TIMEOUT),
    %% Make sure all notifications have been sent
    chronicle_compat_events:sync(),
    %% Ensures the cache has been invalidated. This sync is processed only after
    %% all prior messages including the settings update.
    gen_server:call(jwt_cache, sync, ?SYNC_TIMEOUT).

%% We store the JWT settings in Erlang map format in chronicle_kv. This is done
%% so we can query them efficiently. We use jiffy to encode directly from
%% Erlang maps. ejson does not support it.
encode_response(Value) ->
    try
        jiffy:encode(Value)
    catch T:E:Stack ->
            ?log_debug("Error encoding response:~n~p", [Value]),
            erlang:raise(T, E, Stack)
    end.

handle_settings(Method, Req) ->
    try
        menelaus_util:assert_is_dev_preview(),
        menelaus_util:assert_is_enterprise(),
        menelaus_util:assert_is_79(),
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

handle_settings_get(Req) ->
    case chronicle_kv:get(kv, jwt_settings) of
        {ok, {Settings, _Rev}} ->
            RestFormat = storage_to_rest_format(Settings),
            JsonBin = encode_response(RestFormat),
            menelaus_util:reply(Req, JsonBin, 200,
                                [{"Content-Type", "application/json"}]);
        {error, Error} ->
            ?log_error("Failed to get JWT settings: ~p", [Error]),
            menelaus_util:reply_json(Req,
                                     {[{error, <<"Failed to get settings">>}]},
                                     404)
    end.

handle_settings_put(Req) ->
    validator:handle(
      fun (Props) ->
              validate_and_store_settings(Props, Req)
      end,
      Req, json, main_validators()).

handle_settings_delete(Req) ->
    Fun = fun (_) -> {commit, [{delete, jwt_settings}]} end,
    case chronicle_kv:transaction(kv, [], Fun, #{}) of
        {ok, _} ->
            ns_audit:settings(Req, modify_jwt, [{jwt_settings, deleted}]),
            _ = sync_with_node(),
            menelaus_util:reply_json(Req, {[]}, 200);
        {error, Error} ->
            ?log_error("Failed to delete JWT settings: ~p", [Error]),
            menelaus_util:reply_json(Req,
                                     {[{error,
                                        <<"Failed to delete settings">>}]},
                                     500)
    end.

is_enabled() ->
    case chronicle_kv:get(kv, jwt_settings) of
        {ok, {#{enabled := true}, _Rev}} -> true;
        _ -> false
    end.

main_validators() ->
    [validator:required(enabled, _),
     validator:boolean(enabled, _),
     validator:integer(jwksUriRefreshIntervalS,
                       ?JWKS_URI_REFRESH_MIN_S,
                       ?JWKS_URI_REFRESH_MAX_S, _),
     validator:default(jwksUriRefreshIntervalS, ?JWKS_URI_REFRESH_DEFAULT_S, _),
     validator:required(issuers, _),
     validator:json_array(issuers, issuer_validators(), _),
     validator:validate(
       fun ([]) -> {error, "Must contain at least one issuer"};
           (_) -> ok
       end, issuers, _),
     validator:validate(
       fun(Issuers) ->
               Names = [proplists:get_value(name, I) || {I} <- Issuers],
               ValidNames = [N || N <- Names, N =/= undefined],
               case length(ValidNames) =:= length(lists:usort(ValidNames)) of
                   true -> ok;
                   false -> {error, "Duplicate issuer names not allowed"}
               end
       end, issuers, _),
     validator:unsupported(_)].

issuer_validators() ->
    basic_validators() ++
        key_validators() ++
        mapping_validators() ++
        [validator:unsupported(_)].

basic_validators() ->
    [validator:required(name, _),
     validator:non_empty_string(name, _),
     validator:required(signingAlgorithm, _),
     validator:one_of(signingAlgorithm,
                      menelaus_web_jwt_key:signing_algorithms(), _),
     validator:convert(signingAlgorithm, fun binary_to_existing_atom/1, _),
     validator:required(audClaim, _),
     validator:non_empty_string(audClaim, _),
     validator:validate_field_path(audClaim, _),
     validator:required(audienceHandling, _),
     validator:one_of(audienceHandling, [any, all], _),
     validator:convert(audienceHandling, fun binary_to_existing_atom/1, _),
     validator:required(audiences, _),
     validator:string_array(audiences, _),
     validator:integer(expiryLeewayS,
                       ?EXPIRY_LEEWAY_MIN_S,
                       ?EXPIRY_LEEWAY_MAX_S, _),
     validator:default(expiryLeewayS, ?EXPIRY_LEEWAY_DEFAULT_S, _),
     validator:boolean(jitProvisioning, _),
     validator:default(jitProvisioning, false, _),
     validator:required(subClaim, _),
     validator:non_empty_string(subClaim, _),
     validator:validate_field_path(subClaim, _)].

mapping_validators() ->
    [validator:string_array(subMaps, auth_mapping:validate_mapping_rule(_), _),
     validator:string(groupsClaim, _),
     validator:validate_field_path(groupsClaim, _),
     validator:string_array(groupsMaps,
                            auth_mapping:validate_mapping_rule(_), _),
     validator:boolean(groupsMapsStopFirstMatch, _),
     validator:default(groupsMapsStopFirstMatch, true, _),
     validator:string(rolesClaim, _),
     validator:validate_field_path(rolesClaim, _),
     validator:string_array(rolesMaps,
                            auth_mapping:validate_mapping_rule(_), _),
     validator:boolean(rolesMapsStopFirstMatch, _),
     validator:default(rolesMapsStopFirstMatch, true, _)].

key_validators() ->
    public_key_validators() ++
        shared_secret_validators().

public_key_validators() ->
    [validator:validate_multiple(
       fun([undefined, Algorithm]) ->
               case menelaus_web_jwt_key:is_symmetric_algorithm(Algorithm) of
                   true -> ok;
                   false -> {error, "publicKeySource required for algorithm"}
               end;
          (_) -> ok
       end, [publicKeySource, signingAlgorithm], _),
     validator:one_of(publicKeySource, [jwks, jwks_uri, pem], _),
     validator:convert(publicKeySource, fun binary_to_existing_atom/1, _)] ++
        pem_validators() ++
        jwks_validators() ++
        jwks_uri_validators().

shared_secret_validators() ->
    [validator:validate_multiple(
       fun([undefined, Algorithm]) ->
               case menelaus_web_jwt_key:is_symmetric_algorithm(Algorithm) of
                   true -> {error, "sharedSecret required for HMAC algorithm"};
                   false -> ok
               end;
          (_) -> ok
       end, [sharedSecret, signingAlgorithm], _),
     validator:validate_relative(
       fun(Secret, Algorithm) ->
               case menelaus_web_jwt_key:validate_shared_secret(
                      Secret, Algorithm) of
                   {ok, {value, SecretBin}} -> {value, SecretBin};
                   {error, Reason} -> {error, Reason}
               end
       end, sharedSecret, signingAlgorithm, _)].

pem_validators() ->
    [validator:validate_multiple(
       fun([undefined, pem]) -> {error, "publicKey is required"};
          (_) -> ok
       end, [publicKey, publicKeySource], _),
     validator:validate_relative(
       fun(Value, Algorithm) ->
               case menelaus_web_jwt_key:is_symmetric_algorithm(Algorithm) of
                   true -> ok;
                   false ->
                       case menelaus_web_jwt_key:get_key_from_pem_contents(
                              Value) of
                           {error, Reason} -> {error, Reason};
                           Key ->
                               case menelaus_web_jwt_key:validate_key_algorithm(
                                      Key, Algorithm) of
                                   ok ->
                                       try
                                           _ = jose_jwk:from_pem(Value),
                                           {value, Value}
                                       catch T:E:S ->
                                               ?log_error("jose_jwk parsing "
                                                          "error:~n~p",
                                                          [{T, E, S}]),
                                               {error, "Invalid PEM"}
                                       end;
                                   {error, Reason} -> {error, Reason}
                               end
                       end
               end
       end, publicKey, signingAlgorithm, _)].

jwks_validators() ->
    [validator:validate_multiple(
       fun([undefined, jwks]) -> {error, "jwks is required"};
          (_) -> ok
       end, [jwks, publicKeySource], _),
     validator:validate_relative(
       fun(Value, Algorithm) ->
               case menelaus_web_jwt_key:is_symmetric_algorithm(Algorithm) of
                   true -> ok;
                   false ->
                       try
                           Map = proplist_to_map(Value),
                           case menelaus_web_jwt_key:validate_jwks_algorithm(
                                  Map, Algorithm) of
                               {error, Reason} -> {error, Reason};
                               {ok, KidToJWKMap} ->
                                   {value,
                                    {iolist_to_binary(ejson:encode(Value)),
                                     KidToJWKMap}}
                           end
                       catch T:E:S ->
                               ?log_error("Error converting JWKS to map:~n~p",
                                          [{T, E, S}]),
                               {error, "Invalid JWKS"}
                       end
               end
       end, jwks, signingAlgorithm, _)].

jwks_uri_validators() ->
    [validator:validate_multiple(
       fun([undefined, jwks_uri]) -> {error, "jwksUri is required"};
          (_) -> ok
       end, [jwksUri, publicKeySource], _),
     validator:string(jwksUri, _),
     validator:url(jwksUri, [<<"http">>, <<"https">>], _),
     validator:one_of(jwksUriAddressFamily, [inet, inet6], _),
     validator:convert(jwksUriAddressFamily, fun binary_to_existing_atom/1, _),
     validator:integer(jwksUriHttpTimeoutMs,
                       ?JWKS_URI_MIN_TIMEOUT_MS,
                       ?JWKS_URI_MAX_TIMEOUT_MS, _),
     validator:default(jwksUriHttpTimeoutMs, ?JWKS_URI_DEFAULT_TIMEOUT_MS, _),
     validator:boolean(jwksUriTlsVerifyPeer, _),
     validator:default(jwksUriTlsVerifyPeer, true, _),
     validator:string(jwksUriTlsCa, _),
     validator:validate(
       fun (Cert) ->
               BinCert = iolist_to_binary(Cert),
               case ns_server_cert:decode_cert_chain(BinCert) of
                   {ok, Decoded} -> {value, {BinCert, Decoded}};
                   {error, _} -> {error, "invalid certificate"}
               end
       end, jwksUriTlsCa, _),
     validator:default(jwksUriTlsCa, {<<>>, []}, _),
     validator:string(jwksUriTlsSni, _),
     validator:validate(
       fun (_) -> {error, "modification not supported"} end,
       jwksUriTlsExtraOpts, _)].

validate_and_store_settings(Props, Req) ->
    Settings = validated_to_storage_format(Props),
    Fun = fun (_) -> {commit, [{set, jwt_settings, Settings}]} end,
    case chronicle_kv:transaction(kv, [], Fun, #{}) of
        {ok, _} ->
            RestFormat = storage_to_rest_format(Settings),
            EncodedSettings = encode_response(RestFormat),
            ns_audit:settings(Req, modify_jwt, [{jwt_settings,
                                                 {json, EncodedSettings}}]),
            _ = sync_with_node(),
            menelaus_util:reply(Req, EncodedSettings, 200,
                                [{"Content-Type", "application/json"}]);
        {error, Error} ->
            ?log_error("Failed to store JWT settings: ~p", [Error]),
            menelaus_util:reply_json(Req,
                                     {[{error,
                                        <<"Failed to store settings">>}]},
                                     500)
    end.

%% @doc Converts storage format (map with snake_case atom keys) to REST format
%% (map with camelCase binary keys). Special handling for issuers maps and
%% formatted values like jwks, certificates, and secrets.
%% Note that the parameter names are not binary, they were converted to strings
%% by the validator:string/2 calls.
storage_to_rest_format(Settings) ->
    maps:fold(
      fun(issuers, IssuersMap, Acc) ->
              Acc#{issuers => storage_to_rest_format_issuers(IssuersMap)};
         (OtherKey, Value, Acc) ->
              storage_to_rest_format_key(OtherKey, Value, Acc,
                                         ?MAIN_STORAGE_TO_REST)
      end, #{}, Settings).

%% @doc Converts map of issuers to list for REST API
storage_to_rest_format_issuers(IssuersMap) ->
    maps:fold(
      fun(Name, IssuerProps, AccList) ->
              [storage_to_rest_format_issuer(Name, IssuerProps) | AccList]
      end, [], IssuersMap).

%% @doc Formats a single issuer's properties for REST
storage_to_rest_format_issuer(Name, IssuerProps) ->
    PropsWithName = IssuerProps#{name => Name},
    maps:fold(
      fun(StorageKey, Value, Acc) ->
              storage_to_rest_format_key(StorageKey, Value, Acc,
                                         ?ISSUER_STORAGE_TO_REST)
      end, #{}, PropsWithName).

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

%% JSON decoding in validator returns a proplist which we convert to a map.
%% This is used only to map JWKS JSON to a map, for jose.
proplist_to_map(Value) when is_tuple(Value) ->
    proplist_to_map(element(1, Value));
proplist_to_map(Value) when is_list(Value) ->
    case lists:all(fun({Key,_}) when is_binary(Key) -> true;
                      (_) -> false
                   end, Value) of
        true ->
            maps:from_list([{Key, proplist_to_map(Val)} || {Key,Val} <- Value]);
        false ->
            [proplist_to_map(Item) || Item <- Value]
    end;
proplist_to_map(Value) -> Value.

%% @doc Converts validated properties (from validator:handle/4) to storage
%% format. Input is a proplist with atom keys (from validator) and values in
%% their validated format. Output is a map with snake_case atom keys suitable
%% for storage.
validated_to_storage_format(Props) ->
    lists:foldl(
      fun({issuers, IssuersList}, Acc) ->
              Acc#{issuers => validated_to_storage_format_issuers(IssuersList)};
         ({OtherKey, Value}, Acc) ->
              {ok, StorageKey} = maps:find(OtherKey, ?MAIN_REST_TO_STORAGE),
              Acc#{StorageKey => Value}
      end, #{}, Props).

%% @doc Converts list of issuer proplists to storage map
validated_to_storage_format_issuers(IssuersList) ->
    lists:foldl(
      fun({IssuerProps}, AccMap) ->
              Name = proplists:get_value(name, IssuerProps),
              AccMap#{Name => validated_to_storage_format_issuer(IssuerProps)}
      end, #{}, IssuersList).

%% @doc Formats a single issuer's properties for storage
validated_to_storage_format_issuer(IssuerProps) ->
    lists:foldl(
      fun({name, _}, Acc) ->
              Acc;
         ({PropK, PropV}, Acc) ->
              {ok, StorageKey} = maps:find(PropK, ?ISSUER_REST_TO_STORAGE),
              Acc#{StorageKey => PropV}
      end, #{}, IssuerProps).

format_jwks({EncodedValue, _JsonMap}) -> EncodedValue;
format_jwks(Value) -> Value.

format_public_key({PemBin, _Key}) when is_binary(PemBin) -> PemBin;
format_public_key(Value) -> Value.

format_secret({_Secret, _JWK}) -> <<"********">>;
format_secret(Value) -> Value.

format_string(undefined) -> undefined;
format_string(Value) ->
    list_to_binary(Value).

format_string_list(undefined) -> undefined;
format_string_list(Values) ->
    [list_to_binary(Value) || Value <- Values].

format_tls_ca(undefined) -> undefined;
format_tls_ca(<<"redacted">>) -> <<"redacted">>;
format_tls_ca({Cert, _DecodedCerts}) -> Cert.

format_tls_extra_opts(undefined) -> undefined;
format_tls_extra_opts(List) ->
    Sanitize = fun ({password, _}) -> <<"********">>;
                   (V) -> V
               end,
    Sanitized = [{K, Sanitize(V)} || {K, V} <- List],
    iolist_to_binary(io_lib:format("~p", [Sanitized])).

-spec sanitize_chronicle_cfg(map()) -> map().
sanitize_chronicle_cfg(#{issuers := Issuers} = Settings) ->
    SanitizedIssuers =
        maps:map(fun(_Name, Props) ->
                         case maps:get(shared_secret, Props, undefined) of
                             undefined -> Props;
                             _ -> Props#{shared_secret =>
                                             chronicle_kv_log:masked()}
                         end
                 end, Issuers),
    Settings#{issuers => SanitizedIssuers};
sanitize_chronicle_cfg(Settings) -> Settings.

-ifdef(TEST).

proplist_to_map_test_() ->
    [
     ?_assertEqual(
        #{<<"key">> => <<"value">>},
        proplist_to_map([{<<"key">>, <<"value">>}])),

     ?_assertEqual(
        #{<<"key">> => 123},
        proplist_to_map([{<<"key">>, 123}])),

     ?_assertEqual(
        #{<<"key">> => [1, 2, 3]},
        proplist_to_map([{<<"key">>, [1, 2, 3]}])),

     ?_assertEqual(
        #{<<"key">> => #{<<"nested">> => <<"value">>}},
        proplist_to_map([{<<"key">>, {[{<<"nested">>, <<"value">>}]}}])),

     ?_assertEqual(
        #{<<"keys">> => [
                         #{<<"kty">> => <<"RSA">>,
                           <<"n">> => <<"123">>,
                           <<"e">> => <<"AQAB">>}
                        ]},
        proplist_to_map([{<<"keys">>, [{[{<<"kty">>, <<"RSA">>},
                                         {<<"n">>, <<"123">>},
                                         {<<"e">>, <<"AQAB">>}]}]}])),

     ?_assertEqual(
        [1, 2, 3],
        proplist_to_map([1, 2, 3])),

     ?_assertEqual(
        <<"simple">>,
        proplist_to_map(<<"simple">>))
    ].

snake_to_camel_test() ->
    [
     ?_assertEqual('audClaim', snake_to_camel_atom(aud_claim)),
     ?_assertEqual('jwksUriHttpTimeoutMs',
                   snake_to_camel_atom(jwks_uri_http_timeout_ms)),
     ?_assertEqual('simple', snake_to_camel_atom(simple))
    ].

%% @doc Tests for format conversion functions.
%% This includes verifying that validated camel case atoms are converted to
%% snake case, and ensuring that in the response, the keys are converted back
%% to their original form. Additionally, it checks that values containing
%% lists are converted to binary format so that jiffy can encode them properly.
format_conversion_test() ->
    [
     ?_assertEqual(
        #{enabled => true,
          jwks_uri_refresh_interval_s => 14400,
          issuers => #{
                       "issuer1" => #{
                                      aud_claim => "aud",
                                      audiences => ["aud1", "aud2"],
                                      expiry_leeway_s => 15,
                                      jit_provisioning => false,
                                      jwks_uri_tls_extra_opts =>
                                          [{verify, verify_peer}]
                                     },
                       "issuer2" => #{
                                      signing_algorithm => "ES256",
                                      audiences => ["aud3"]
                                     }
                      }},
        validated_to_storage_format([
                                     {enabled, true},
                                     {jwksUriRefreshIntervalS, 14400},
                                     {issuers, [
                                                {[{name, "issuer1"},
                                                  {audClaim, "aud"},
                                                  {audiences, ["aud1", "aud2"]},
                                                  {expiryLeewayS, 15},
                                                  {jitProvisioning, false},
                                                  {jwksUriTlsExtraOpts,
                                                   [{verify, verify_peer}]}]},
                                                {[{name, "issuer2"},
                                                  {signingAlgorithm, "ES256"},
                                                  {audiences, ["aud3"]}]}
                                               ]}
                                    ]))
    ].
-endif.
