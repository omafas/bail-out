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
%% custom_claims - List of user-defined non-standard custom claims
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
%% jwks_uri_tls_sni - Server name for TLS SNI extension
%% jwks_uri_tls_verify_peer - Whether to verify JWKS URI server certificate
%% name - Unique name identifying this issuer
%% oidc_settings - Optional OIDC configuration for this issuer
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
         {custom_claims, fun format_custom_claims/1},
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
         {jwks_uri_tls_sni, fun format_string/1},
         {jwks_uri_tls_verify_peer, undefined},
         {name, fun format_string/1},
         {oidc_settings, fun storage_to_rest_format_oidc_provider/1},
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

%% Custom claims parameters and their descriptions:
%% name - Unique name identifying this custom claim
%% type - Type of the claim (string/number/boolean/array/object)
%% mandatory - Whether the claim is required
%% pattern - Regex pattern for string type
%% min - Minimum value for number type
%% max - Maximum value for number type
%% enum - Array of allowed numbers for number type
%% const - Expected value for boolean type
-define(CUSTOM_CLAIM_PARAMS_WITH_FORMATTERS,
        [
         {name, fun format_string/1},
         {type, undefined},
         {mandatory, undefined},
         {pattern, fun format_string/1},
         {min, undefined},
         {max, undefined},
         {enum, undefined},
         {const, undefined}
        ]).

%% REST to storage mapping (camelCase atom -> snake_case atom)
-define(CUSTOM_CLAIM_REST_TO_STORAGE,
        maps:from_list([{snake_to_camel_atom(Key), Key} ||
                           {Key, _} <- ?CUSTOM_CLAIM_PARAMS_WITH_FORMATTERS])).

%% Storage to REST mapping (snake_case atom -> {camelCase atom, formatter})
-define(CUSTOM_CLAIM_STORAGE_TO_REST,
        maps:from_list([{Key, {snake_to_camel_atom(Key), Format}} ||
                           {Key, Format} <-
                               ?CUSTOM_CLAIM_PARAMS_WITH_FORMATTERS])).

%% @doc OIDC provider parameters and their descriptions:
%% client_id - Unique id for the application registered with OIDC provider
%% client_secret - Secret key for confidential client authentication
%% base_redirect_uris - Allowed base URLs for redirect targets (array)
%% oidc_discovery_uri - OIDC discovery endpoint for automatic endpoint detection
%% authorization_endpoint - OIDC provider's authorization endpoint URL
%% (required if no discovery; must be absent when discovery is used)
%% token_endpoint - URL for exchanging authorization code for tokens URL
%% (required if no discovery; must be absent when discovery is used)
%% end_session_endpoint - RP-initiated logout endpoint URL
%% (optional, must be absent when discovery is used)
%% scopes - List of requested permissions (must include "openid")
%% nonce_validation - Whether to validate nonce parameter for replay protection
%% pkce_enabled - Whether PKCE is enabled (should be true for security)
%% post_logout_redirect_uris - Allowed logout redirect URIs (array, optional).
%% tls_ca - CA certificate for validating OIDC provider TLS connection
%% tls_verify_peer - Whether to verify OIDC provider server certificate
%% tls_sni - Optional SNI hostname for TLS
%% address_family - Optional address family for DNS/connect (inet | inet6)
%% http_timeout_ms - HTTP request timeout for OIDC provider calls
%% token_endpoint_auth_method - Client auth method for token endpoint
%% (client_secret_basic | client_secret_post; default client_secret_basic)
%% disable_pushed_authorization_requests - Disable PAR even if OP supports it
%% (To circumvent Keycloak IdP Issue #43034 (OIDCC #443); default false)
-define(OIDC_PROVIDER_PARAMS_WITH_FORMATTERS,
        [
         {client_id, fun format_string/1},
         {client_secret, fun format_secret/1},
         {base_redirect_uris, fun format_string_list/1},
         {oidc_discovery_uri, fun format_string/1},
         {authorization_endpoint, fun format_string/1},
         {token_endpoint, fun format_string/1},
         {end_session_endpoint, fun format_string/1},
         {scopes, fun format_string_list/1},
         {nonce_validation, undefined},
         {pkce_enabled, undefined},
         {post_logout_redirect_uris, fun format_string_list/1},
         {tls_ca, fun format_tls_ca/1},
         {tls_verify_peer, undefined},
         {tls_sni, fun format_string/1},
         {address_family, undefined},
         {http_timeout_ms, undefined},
         {token_endpoint_auth_method, undefined},
         {disable_pushed_authorization_requests, undefined}
        ]).

%% REST to storage mapping (camelCase atom -> snake_case atom)
-define(OIDC_PROVIDER_REST_TO_STORAGE,
        maps:from_list([{snake_to_camel_atom(Key), Key} ||
                           {Key, _} <- ?OIDC_PROVIDER_PARAMS_WITH_FORMATTERS])).

%% Storage to REST mapping (snake_case atom -> {camelCase atom, formatter})
-define(OIDC_PROVIDER_STORAGE_TO_REST,
        maps:from_list([{Key, {snake_to_camel_atom(Key), Format}} ||
                           {Key, Format} <-
                               ?OIDC_PROVIDER_PARAMS_WITH_FORMATTERS])).

%% OIDC provider validation constants
-define(OIDC_HTTP_TIMEOUT_MIN_MS, 1000).       % 1 second
-define(OIDC_HTTP_TIMEOUT_MAX_MS, 60000).      % 1 minute
-define(OIDC_HTTP_TIMEOUT_DEFAULT_MS, 10000).  % 10 seconds

tls_validators(VerifyPeerKey, CaKey, SniKey, AddressFamilyKey) ->
    [validator:one_of(AddressFamilyKey, [inet, inet6], _),
     validator:convert(AddressFamilyKey, fun binary_to_existing_atom/1, _),
     validator:boolean(VerifyPeerKey, _),
     validator:default(VerifyPeerKey, true, _),
     validator:string(CaKey, _),
     validator:validate(
       fun (Cert) ->
               BinCert = iolist_to_binary(Cert),
               case ns_server_cert:decode_cert_chain(BinCert) of
                   {ok, Decoded} -> {value, {BinCert, Decoded}};
                   {error, _} -> {error, "invalid certificate"}
               end
       end, CaKey, _),
     validator:default(CaKey, {<<>>, []}, _),
     validator:string(SniKey, _)
    ].

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
    %% Restart the OIDC provider workers to ensure they use the new settings
    oidc_provider_manager:restart_workers(),
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
        custom_claims_validators() ++
        [validator:decoded_json(oidcSettings, oidc_provider_validators(), _)] ++
        [validator:post_validate_all(
           fun(Props) ->
                   CustomClaims = proplists:get_value(customClaims, Props, []),
                   Names = [proplists:get_value(name, ClaimProps) ||
                               {ClaimProps} <- CustomClaims],

                   Standard = jwt_auth:get_standard_claim_names(),

                   Sub = proplists:get_value(subClaim, Props),
                   Aud = proplists:get_value(audClaim, Props),

                   Groups = case proplists:get_value(groupsClaim, Props) of
                                undefined -> "groups";
                                GroupsV -> GroupsV
                            end,
                   Roles = case proplists:get_value(rolesClaim, Props) of
                               undefined -> "roles";
                               RolesV -> RolesV
                           end,

                   Protected = Standard ++ [Sub, Aud, Groups, Roles],
                   Conflicts = [N || N <- Names, lists:member(N, Protected)],
                   case Conflicts of
                       [] -> ok;
                       _  -> {error, "Custom claim names cannot conflict with "
                              "JWT claims: " ++ string:join(Conflicts, ", ")}
                   end
           end, _),
         validator:post_validate_all(
           fun(Props) ->
                   PubKeySource = proplists:get_value(publicKeySource, Props),
                   JwksUri = proplists:get_value(jwksUri, Props),
                   OidcSettings = proplists:get_value(oidcSettings, Props),
                   case {PubKeySource, JwksUri, OidcSettings} of
                       {jwks_uri, undefined, undefined} ->
                           {error, "jwksUri is required"};
                       {jwks_uri, undefined, OidcSettings} ->
                           %% Check if OIDC discovery provides jwks_uri
                           case proplists:get_value(oidcDiscoveryUri,
                                                    OidcSettings) of
                               undefined ->
                                   {error, "jwksUri is required"};
                               _ ->
                                   ok  %% OIDC discovery will provide jwks_uri
                           end;
                       {jwks_uri, JwksUri, OidcSettings} when
                             JwksUri =/= undefined,
                             OidcSettings =/= undefined ->

                           case proplists:get_value(oidcDiscoveryUri,
                                                    OidcSettings) of
                               undefined ->
                                   ok;
                               _ ->
                                   {error, "Both jwksUri and OIDC discovery are"
                                    " configured. OIDC discovery will provide "
                                    "the jwks_uri."}
                           end;
                       _ -> ok
                   end
           end, _),
         validator:unsupported(_)].

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

custom_claims_validators() ->
    [validator:json_array(customClaims, custom_claim_validators(), _),
     validator:validate(
       fun ([]) -> {error, "Must contain at least one custom claim"};
           (_) -> ok
       end, customClaims, _),
     validator:validate(
       fun(CustomClaims) ->
               Names = [proplists:get_value(name, C) || {C} <- CustomClaims],
               ValidNames = [N || N <- Names, N =/= undefined],
               case length(ValidNames) =:= length(lists:usort(ValidNames)) of
                   true -> ok;
                   false -> {error, "Duplicate custom claim names not allowed"}
               end
       end, customClaims, _)].

custom_claim_validators() ->
    [validator:required(name, _),
     validator:non_empty_string(name, _),
     validator:required(type, _),
     validator:one_of(type, ["string", "number", "boolean", "array", "object"],
                      _),
     validator:convert(type, fun binary_to_existing_atom/1, _),
     validator:required(mandatory, _),
     validator:boolean(mandatory, _)] ++
        custom_string_validators() ++
        custom_number_validators() ++
        custom_boolean_validators().

custom_string_validators() ->
    [validator:validate_multiple(
       fun([undefined, string]) ->
               {error, "pattern is required for string type"};
          (_) -> ok
       end, [pattern, type], _),
     validator:string(pattern, _),
     validator:regex(pattern, _)].

custom_number_validators() ->
    [validator:number(min, _),
     validator:number(max, _),
     validator:validate_relative(
       fun(Min, Max) when Min =< Max -> ok;
          (_, _) -> {error, "min must be less than or equal to max"}
       end, min, max, _),
     validator:validate(
       fun ([]) ->
               {error, "Must contain at least one element"};
           (Array) ->
               case lists:all(?cut(is_number(_1)), Array) of
                   false ->
                       {error, "Must be an array of numbers"};
                   true ->
                       ok
               end
       end, enum, _)].

custom_boolean_validators() ->
    [validator:boolean(const, _)].

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
    [
     validator:string(jwksUri, _),
     validator:validate(fun validate_public_https_url/1, jwksUri, _),
     validator:integer(jwksUriHttpTimeoutMs,
                       ?JWKS_URI_MIN_TIMEOUT_MS,
                       ?JWKS_URI_MAX_TIMEOUT_MS, _),
     validator:default(jwksUriHttpTimeoutMs, ?JWKS_URI_DEFAULT_TIMEOUT_MS, _)
    ] ++ tls_validators(jwksUriTlsVerifyPeer,
                        jwksUriTlsCa,
                        jwksUriTlsSni,
                        jwksUriAddressFamily).

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

storage_to_rest_format_oidc_provider(ProviderProps) ->
    maps:fold(
      fun(StorageKey, Value, Acc) ->
              storage_to_rest_format_key(StorageKey, Value, Acc,
                                         ?OIDC_PROVIDER_STORAGE_TO_REST)
      end, #{}, ProviderProps).

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

validated_to_storage_format_issuer(IssuerProps) ->
    lists:foldl(
      fun({name, _}, Acc) ->
              Acc;
         ({customClaims, Claims}, Acc) ->
              Acc#{custom_claims =>
                       validated_to_storage_format_custom_claims(Claims)};
         ({oidcSettings, Settings}, Acc) ->
              Acc#{oidc_settings =>
                       validated_to_storage_format_oidc_provider(Settings)};
         ({PropK, PropV}, Acc) ->
              {ok, StorageKey} = maps:find(PropK, ?ISSUER_REST_TO_STORAGE),
              Acc#{StorageKey => PropV}
      end, #{}, IssuerProps).

validated_to_storage_format_oidc_provider(ProviderProps) ->
    lists:foldl(
      fun({name, _}, Acc) ->
              Acc;
         ({PropK, PropV}, Acc) ->
              {ok, StorageKey} = maps:find(PropK,
                                           ?OIDC_PROVIDER_REST_TO_STORAGE),
              Acc#{StorageKey => PropV}
      end, #{}, ProviderProps).

validated_to_storage_format_custom_claims(Claims) ->
    lists:foldl(
      fun({ClaimProps}, AccMap) ->
              Name = proplists:get_value(name, ClaimProps),
              AccMap#{Name =>
                          validated_to_storage_format_custom_claim(ClaimProps)}
      end, #{}, Claims).

validated_to_storage_format_custom_claim(ClaimProps) ->
    lists:foldl(
      fun({name, _}, Acc) ->
              Acc;
         ({PropK, PropV}, Acc) ->
              {ok, StorageKey} = maps:find(PropK,
                                           ?CUSTOM_CLAIM_REST_TO_STORAGE),
              Acc#{StorageKey => PropV}
      end, #{}, ClaimProps).

format_jwks({EncodedValue, _JsonMap}) -> EncodedValue;
format_jwks(Value) -> Value.

format_public_key({PemBin, _Key}) when is_binary(PemBin) -> PemBin;
format_public_key(Value) -> Value.

format_secret(undefined) -> undefined;
format_secret(_) -> <<"********">>.

format_string(undefined) -> undefined;
format_string(Value) ->
    list_to_binary(Value).

format_string_list(undefined) -> undefined;
format_string_list(Values) ->
    [list_to_binary(Value) || Value <- Values].

format_custom_claims(undefined) -> undefined;
format_custom_claims(ClaimsMap) ->
    maps:fold(
      fun(Name, ClaimProps, AccList) ->
              [format_custom_claim(Name, ClaimProps) | AccList]
      end, [], ClaimsMap).

format_custom_claim(Name, ClaimProps) ->
    PropsWithName = ClaimProps#{name => Name},
    maps:fold(
      fun(StorageKey, Value, Acc) ->
              storage_to_rest_format_key(StorageKey, Value, Acc,
                                         ?CUSTOM_CLAIM_STORAGE_TO_REST)
      end, #{}, PropsWithName).

format_tls_ca(undefined) -> undefined;
format_tls_ca(<<"redacted">>) -> <<"redacted">>;
format_tls_ca({Cert, _DecodedCerts}) -> Cert.

-spec validate_redirect_uri(string()) -> ok | {error, string()}.
validate_redirect_uri(Url) ->
    case uri_string:parse(Url) of
        {error, _, _} ->
            {error, "Invalid URL syntax"};

        #{scheme := Scheme, host := _Host} = Map ->
            case lists:member(Scheme, ["http", "https"]) of
                true -> validate_strict_base(Map);
                false -> {error, "Invalid scheme (must be http/https)"}
            end;
        #{scheme := _Scheme} ->
            {error, "Missing host"};

        _ ->
            {error, "Invalid URL format"}
    end.

validate_strict_base(Map) ->
    Path = maps:get(path, Map, ""),
    Query = maps:get(query, Map, ""),
    Fragment = maps:get(fragment, Map, ""),
    UserInfo = maps:get(userinfo, Map, undefined),

    case {Path, Query, Fragment, UserInfo} of
        {"", "", "", undefined} -> ok;
        {"/", "", "", undefined} -> ok;
        {_, _, _, DefinedUser} when DefinedUser /= undefined ->
            {error, "User info not allowed in redirect base"};
        _ ->
            {error, "Path (except /), Query, and Fragment must be empty"}
    end.

%% Validate OIDC endpoint URL: allow https anywhere; allow http only for
%% localhost/127.0.0.1. localhost is used for local testing.
validate_public_https_url(Url) ->
    case uri_string:parse(Url) of
        {error, _, _} -> {error, "Invalid URL"};
        Map when is_map(Map) ->
            Scheme = maps:get(scheme, Map, ""),
            Host = maps:get(host, Map, ""),
            case Scheme of
                "http" ->
                    case Host of
                        "127.0.0.1" -> ok;
                        "localhost" -> ok;
                        _ -> {error,
                              "HTTP allowed only for localhost (127.0.0.1)"}
                    end;
                "https" -> ok;
                _ -> {error, "Invalid scheme"}
            end
    end.

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

oidc_provider_validators() ->
    [validator:required(clientId, _),
     validator:non_empty_string(clientId, _),
     %% When private_key_jwt support is added, clientSecret will not be required
     %% Right now, client_secret_basic and client_secret_post are supported,
     %% both of which require a clientSecret.
     validator:required(clientSecret, _),
     validator:non_empty_string(clientSecret, _),
     validator:required(baseRedirectUris, _),
     validator:string_array(baseRedirectUris,
                            fun validate_redirect_uri/1, false, _),
     validator:non_empty_string(oidcDiscoveryUri, _),
     validator:url(oidcDiscoveryUri, [<<"http">>, <<"https">>], _),
     validator:validate(fun validate_public_https_url/1,
                        oidcDiscoveryUri, _),
     validator:non_empty_string(authorizationEndpoint, _),
     validator:url(authorizationEndpoint, [<<"http">>, <<"https">>], _),
     validator:validate(fun validate_public_https_url/1,
                        authorizationEndpoint, _),
     validator:non_empty_string(tokenEndpoint, _),
     validator:url(tokenEndpoint, [<<"http">>, <<"https">>], _),
     validator:validate(fun validate_public_https_url/1,
                        tokenEndpoint, _),
     validator:non_empty_string(endSessionEndpoint, _),
     validator:url(endSessionEndpoint, [<<"http">>, <<"https">>], _),
     validator:validate(fun validate_public_https_url/1,
                        endSessionEndpoint, _),
     validator:validate_multiple(
       fun([Discovery, Auth, Token, End]) ->
               case Discovery of
                   undefined ->
                       case {Auth =/= undefined, Token =/= undefined} of
                           {true, true} ->
                               ok;
                           _ ->
                               {error, "authorizationEndpoint and tokenEndpoint"
                                " are required when discovery is not in use"}
                       end;
                   _ ->
                       case {Auth, Token, End} of
                           {undefined, undefined, undefined} ->
                               ok;
                           _ ->
                               {error, "authorizationEndpoint, tokenEndpoint, "
                                "and endSessionEndpoint must not be provided "
                                "when discovery is in use"}
                       end
               end
       end, [oidcDiscoveryUri, authorizationEndpoint, tokenEndpoint,
             endSessionEndpoint], _),
     validator:required(scopes, _),
     validator:string_array(scopes, _),
     validator:validate(
       fun(Scopes) ->
               case lists:member("openid", Scopes) of
                   true -> ok;
                   false -> {error, "scopes must include 'openid'"}
               end
       end, scopes, _),
     validator:boolean(nonceValidation, _),
     validator:default(nonceValidation, true, _),
     validator:boolean(pkceEnabled, _),
     validator:default(pkceEnabled, true, _),
     validator:string_array(postLogoutRedirectUris,
                            fun validate_redirect_uri/1, false, _)
    ] ++ tls_validators(tlsVerifyPeer, tlsCa, tlsSni, addressFamily) ++
        [validator:integer(httpTimeoutMs,
                           ?OIDC_HTTP_TIMEOUT_MIN_MS,
                           ?OIDC_HTTP_TIMEOUT_MAX_MS, _),
         validator:default(httpTimeoutMs, ?OIDC_HTTP_TIMEOUT_DEFAULT_MS, _),
     validator:one_of(tokenEndpointAuthMethod,
                      [client_secret_basic, client_secret_post], _),
     validator:default(tokenEndpointAuthMethod, <<"client_secret_basic">>, _),
     validator:convert(tokenEndpointAuthMethod, fun binary_to_existing_atom/1,
                       _),
     validator:boolean(disablePushedAuthorizationRequests, _),
     validator:default(disablePushedAuthorizationRequests, false, _),
     validator:unsupported(_)].

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

snake_to_camel_test_() ->
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
format_conversion_test_() ->
    BaseCb = "https://couchbase.example.com",
    OktaAuth = "https://example.okta.com/oauth2/v1/authorize",
    OktaToken = "https://example.okta.com/oauth2/v1/token",
    OktaLogout = "https://example.okta.com/oauth2/v1/logout",
    AzureDisc =
        "https://login.microsoftonline.com/tenant/v2.0/.well-known/"
        "openid-configuration",
    ExampleDisc =
        "https://example.com/.well-known/openid-configuration",
    ExampleJwks = "https://example.com/.well-known/jwks.json",
    StorageFormat =
        #{enabled => true,
          jwks_uri_refresh_interval_s => 14400,
          issuers =>
              #{
                "issuer1" =>
                    #{
                      aud_claim => "aud",
                      audiences => ["aud1", "aud2"],
                      expiry_leeway_s => 15,
                      jit_provisioning => false,
                      custom_claims =>
                          #{
                            "email" =>
                                #{
                                  type => string,
                                  pattern => "^[a-z]+@[a-z]+\.[a-z]+$",
                                  mandatory => true
                                 },
                            "age" => #{
                                       type => number,
                                       min => 18,
                                       max => 65,
                                       mandatory => false
                                      },
                            "level" => #{
                                         type => number,
                                         enum => [1, 2, 3, 4, 5],
                                         mandatory => true
                                        },
                            "admin" => #{
                                         type => boolean,
                                         const => true,
                                         mandatory => true
                                        }
                           },
                      oidc_settings =>
                          #{
                            client_id => "okta_client_id",
                            client_secret =>
                                "encrypted_okta_secret",
                            authorization_endpoint => OktaAuth,
                            base_redirect_uris => [BaseCb],
                            token_endpoint => OktaToken,
                            end_session_endpoint => OktaLogout,
                            scopes => ["openid", "profile", "email",
                                       "groups"],
                            nonce_validation => true,
                            pkce_enabled => true,
                            tls_verify_peer => true,
                            http_timeout_ms => 10000
                           }
                     },
                "issuer2" =>
                    #{
                      signing_algorithm => "ES256",
                      audiences => ["aud3"],
                      custom_claims =>
                          #{
                            "role" =>
                                #{
                                  type => string,
                                  pattern => "^(admin|user)$",
                                  mandatory => true
                                 }
                           },
                      oidc_settings =>
                          #{
                            client_id => "azure_client_id",
                            client_secret =>
                                "encrypted_azure_secret",
                            base_redirect_uris => [BaseCb],
                            oidc_discovery_uri =>
                                AzureDisc,
                            scopes => ["openid", "profile",
                                       "email"],
                            nonce_validation => true,
                            pkce_enabled => true,
                            tls_verify_peer => true,
                            http_timeout_ms => 15000
                           }
                     },
                "issuer3" =>
                    #{
                      signing_algorithm => "RS256",
                      aud_claim => "aud",
                      audiences => ["aud4"],
                      sub_claim => "sub",
                      public_key_source => "jwks_uri",
                      jwks_uri => ExampleJwks,
                      oidc_settings =>
                          #{
                            client_id =>
                                "conflict_client_id",
                            client_secret =>
                                "conflict_secret",
                            base_redirect_uris => [BaseCb],
                            oidc_discovery_uri =>
                                ExampleDisc,
                            scopes => ["openid", "profile",
                                       "email"],
                            nonce_validation => true,
                            pkce_enabled => true,
                            tls_verify_peer => true,
                            http_timeout_ms => 10000
                           }
                     }
               }
         },
    RestFormat =
        #{enabled => true,
          jwksUriRefreshIntervalS => 14400,
          issuers => [
                      #{name => "issuer1",
                        audClaim => "aud",
                        audiences => ["aud1", "aud2"],
                        expiryLeewayS => 15,
                        jitProvisioning => false,
                        customClaims => [
                                         #{name => "email",
                                           type => string,
                                           pattern => "^[a-z]+@[a-z]+\.[a-z]+$",
                                           mandatory => true},
                                         #{name => "age",
                                           type => number,
                                           min => 18,
                                           max => 65,
                                           mandatory => false},
                                         #{name => "level",
                                           type => number,
                                           enum => [1, 2, 3, 4, 5],
                                           mandatory => true},
                                         #{name => "admin",
                                           type => boolean,
                                           const => true,
                                           mandatory => true}
                                        ],
                        %% Add OIDC settings to issuer1 (using manual endpoints)
                        oidcSettings => #{
                                          clientId => "okta_client_id",
                                          clientSecret => "********",
                                          baseRedirectUris => [BaseCb],
                                          authorizationEndpoint => OktaAuth,
                                          tokenEndpoint => OktaToken,
                                          endSessionEndpoint => OktaLogout,
                                          scopes => ["openid", "profile",
                                                     "email", "groups"],
                                          nonceValidation => true,
                                          pkceEnabled => true,
                                          tlsVerifyPeer => true,
                                          httpTimeoutMs => 10000
                                         }},
                      #{name => "issuer2",
                        signingAlgorithm => "ES256",
                        audiences => ["aud3"],
                        customClaims => [
                                         #{name => "role",
                                           type => string,
                                           pattern => "^(admin|user)$",
                                           mandatory => true}
                                        ],
                        %% Add OIDC settings to issuer2 (using OIDC discovery)
                        oidcSettings => #{
                                          clientId => "azure_client_id",
                                          clientSecret => "********",
                                          baseRedirectUris => [BaseCb],
                                          oidcDiscoveryUri => AzureDisc,
                                          scopes => ["openid", "profile",
                                                     "email"],
                                          nonceValidation => true,
                                          pkceEnabled => true,
                                          tlsVerifyPeer => true,
                                          httpTimeoutMs => 15000
                                         }
                       },
                      #{name => "issuer3",
                        signingAlgorithm => "RS256",
                        audClaim => "aud",
                        audiences => ["aud4"],
                        subClaim => "sub",
                        publicKeySource => "jwks_uri",
                        jwksUri => ExampleJwks,
                        oidcSettings => #{
                                          clientId => "conflict_client_id",
                                          clientSecret => "********",
                                          baseRedirectUris => [BaseCb],
                                          oidcDiscoveryUri => ExampleDisc,
                                          scopes => ["openid", "profile",
                                                     "email"],
                                          nonceValidation => true,
                                          pkceEnabled => true,
                                          tlsVerifyPeer => true,
                                          httpTimeoutMs => 10000
                                         }
                       }]
         },

    Props = [{enabled, true},
             {jwksUriRefreshIntervalS, 14400},
             {issuers, [
                        {[{name, "issuer1"},
                          {audClaim, "aud"},
                          {audiences, ["aud1", "aud2"]},
                          {expiryLeewayS, 15},
                          {jitProvisioning, false},
                          {customClaims, [
                                          {[{name, "email"},
                                            {type, string},
                                            {pattern,
                                             "^[a-z]+@[a-z]+\.[a-z]+$"},
                                            {mandatory, true}]},
                                          {[{name, "age"},
                                            {type, number},
                                            {min, 18},
                                            {max, 65},
                                            {mandatory, false}]},
                                          {[{name, "level"},
                                            {type, number},
                                            {enum, [1, 2, 3, 4, 5]},
                                            {mandatory, true}]},
                                          {[{name, "admin"},
                                            {type, boolean},
                                            {const, true},
                                            {mandatory, true}]}
                                         ]},
                          {oidcSettings,
                           [{clientId, "okta_client_id"},
                            {clientSecret, "encrypted_okta_secret"},
                            {baseRedirectUris, [BaseCb]},
                            {authorizationEndpoint, OktaAuth},
                            {tokenEndpoint, OktaToken},
                            {endSessionEndpoint, OktaLogout},
                            {scopes, ["openid", "profile", "email", "groups"]},
                            {nonceValidation, true},
                            {pkceEnabled, true},
                            {tlsVerifyPeer, true},
                            {httpTimeoutMs, 10000}]}]},
                        {[{name, "issuer2"},
                          {signingAlgorithm, "ES256"},
                          {audiences, ["aud3"]},
                          {customClaims, [
                                          {[{name, "role"},
                                            {type, string},
                                            {pattern,
                                             "^(admin|user)$"},
                                            {mandatory, true}]}
                                         ]},
                          %% Add OIDC settings to issuer2 (using OIDC discovery)
                          {oidcSettings,
                           [{clientId, "azure_client_id"},
                            {clientSecret, "encrypted_azure_secret"},
                            {baseRedirectUris, [BaseCb]},
                            {oidcDiscoveryUri, AzureDisc},
                            {scopes, ["openid", "profile", "email"]},
                            {nonceValidation, true},
                            {pkceEnabled, true},
                            {tlsVerifyPeer, true},
                            {httpTimeoutMs, 15000}]}]},
                        {[{name, "issuer3"},
                          {signingAlgorithm, "RS256"},
                          {audClaim, "aud"},
                          {audiences, ["aud4"]},
                          {subClaim, "sub"},
                          {publicKeySource, "jwks_uri"},
                          {jwksUri, ExampleJwks},
                          %% This issuer has both jwks_uri and OIDC discovery
                          {oidcSettings,
                           [{clientId, "conflict_client_id"},
                            {clientSecret, "conflict_secret"},
                            {baseRedirectUris, [BaseCb]},
                            {oidcDiscoveryUri, ExampleDisc},
                            {scopes, ["openid", "profile", "email"]},
                            {nonceValidation, true},
                            {pkceEnabled, true},
                            {tlsVerifyPeer, true},
                            {httpTimeoutMs, 10000}]}]}
                       ]}],

    GetNameStr =
        fun(Map) ->
                Name = maps:get(name, Map),
                case Name of
                    <<_/binary>> -> binary_to_list(Name);
                    _ -> Name
                end
        end,
    SortClaims =
        fun(L) ->
                lists:sort(fun(A, B) ->
                                   maps:get(name, A) =< maps:get(name, B)
                           end, L)
        end,
    SortIssuers =
        fun(L) ->
                lists:sort(fun(A, B) ->
                                   GetNameStr(A) =< GetNameStr(B)
                           end,
                           [case maps:get(customClaims, I, undefined) of
                                undefined -> I;
                                Cs -> I#{customClaims => SortClaims(Cs)}
                            end || I <- L])
        end,
    %% Convert all binaries to lists for comparison with expected
    DeepToList =
        fun F(V) when is_map(V) ->
                maps:from_list([{K, F(Val)} || {K, Val} <- maps:to_list(V)]);
            F(V) when is_list(V) ->
                [F(X) || X <- V];
            F(V) when is_binary(V) ->
                binary_to_list(V);
            F(V) -> V
        end,

    Actual0 = storage_to_rest_format(StorageFormat),
    Actual1 = Actual0#{issuers := SortIssuers(maps:get(issuers, Actual0))},
    Expected1 =
        RestFormat#{issuers := SortIssuers(maps:get(issuers, RestFormat))},

    [
     %% Test validated_to_storage_format
     ?_assertEqual(StorageFormat,
                   validated_to_storage_format(Props)),

     %% Test storage_to_rest_format with normalized ordering and string types
     ?_assertEqual(DeepToList(Expected1),
                   DeepToList(Actual1))
    ].
-endif.
