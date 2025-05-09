%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc jwt_issuer

-module(jwt_issuer).

-include("ns_common.hrl").
-include("rbac.hrl").
-include("jwt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(JWT_EL_CURVE, 'Ed25519').
-define(JWT_SIGNING_ALG, 'EdDSA').

-export([chronicle_upgrade_to_morpheus/1,
         name/0,
         issue/3,
         settings/0]).

-spec name() -> string().
name() ->
    "ns_server".

generate_keys() ->
    {_, Map} = jose_jwk:to_map(jose_jwk:generate_key({okp, ?JWT_EL_CURVE})),
    Map.

-spec chronicle_upgrade_to_morpheus(term()) -> term().
chronicle_upgrade_to_morpheus(ChronicleTxn) ->
    chronicle_upgrade:set_key(?JWT_SIGNING_KEYS_KEY, generate_keys(),
                              ChronicleTxn).

get_jwk() ->
    case chronicle_kv:get(kv, ?JWT_SIGNING_KEYS_KEY) of
        {ok, {Map, _}} ->
            {ok, Map};
        {error, not_found} ->
            {error, keys_are_not_setup}
    end.

-spec issue(rbac_user_id(), [rbac_role()], integer()) ->
          {ok, binary()} | {error, keys_are_not_setup}.
issue(User, Roles, LifetimeSec) ->
    case get_jwk() of
        {ok, JWKMap} ->
            Now = erlang:system_time(second),
            JWT = #{<<"iss">> => list_to_binary(name()),
                    <<"exp">> => Now + LifetimeSec,
                    <<"sub">> => list_to_binary(User),
                    <<"aud">> => <<"ns_server_internal">>,
                    <<"roles">> =>
                        [list_to_binary(menelaus_web_rbac:role_to_string(R)) ||
                            R <- Roles]
                   },
            JWK = jose_jwk:from_map(JWKMap),
            JWS = jose_jwt:sign(
                    JWK, #{<<"alg">> => atom_to_binary(?JWT_SIGNING_ALG)}, JWT),
            {_, CompactJWS} = jose_jws:compact(JWS),
            {ok, CompactJWS};
        Error ->
            Error
    end.

-spec settings() -> map().
settings() ->
    case get_jwk() of
        {ok, JWKMap} ->
            PEM = jose_jwk:to_pem(jose_jwk:to_public(JWKMap)),
            #{
              "ns_server" =>
                  #{signing_algorithm => ?JWT_SIGNING_ALG,
                    public_key_source => pem,
                    public_key => PEM,
                    expiry_leeway_s => 0,
                    audience_handling => any,
                    audiences => ["ns_server_internal"],
                    jit_provisioning => true
                   }
             };
        _Error ->
            #{}
    end.

-ifdef(TEST).
basic_test() ->
    fake_ns_config:setup(),
    fake_ns_config:update_snapshot({node, node() ,is_enterprise}, true),

    meck:new(chronicle_compat_events, [passthrough]),
    meck:expect(chronicle_compat_events, subscribe, fun(_) -> ok end),
    fake_chronicle_kv:new(),
    fake_chronicle_kv:update_snapshot(?JWT_SIGNING_KEYS_KEY, generate_keys()),

    {ok, CachePid} = jwt_cache:start_link(),
    CachePid ! internal_key_update,
    gen_server:call(jwt_cache, sync, 1000),

    RV = issue("@test", [admin, metakv2_access], 1000),
    ?assertMatch({ok, _}, RV),
    {ok, TokenBin} = RV,
    RV1 = jwt_auth:authenticate(binary_to_list(TokenBin)),
    ?assertMatch({ok, #authn_res{type = tmp,
                                 session_id = undefined,
                                 authenticated_identity = {"@test", external},
                                 identity = {"@test", external},
                                 extra_groups = [],
                                 extra_roles = [admin, metakv2_access],
                                 expiration_datetime_utc = _,
                                 password_expired = false}, _}, RV1),

    gen_server:stop(CachePid),
    fake_chronicle_kv:unload(),
    fake_ns_config:teardown(),
    meck:unload(chronicle_compat_events).

-endif.
