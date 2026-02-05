%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc JWT authentication module
%%
%% This module handles JWT authentication by validating tokens against
%% configured settings. Settings are stored in chronicle_kv and JWKS keys are
%% cached in an ETS table for performance.
%%
%% Authentication Flow:
%% 1. Settings are read directly from chronicle_kv for each auth request
%% 2. All keys are read from an ETS cache with read_concurrency
%% 3. For JWKS URIs, keys are periodically refreshed in the background
%%
%% Cache Consistency:
%% The JWT key cache is designed for performance and simplicity:
%% - Keys are always looked up in the cache first, regardless of settings age
%% - If a key is not found in the cache, a direct lookup is performed
%% - During settings changes, there is a brief window where:
%%   * The settings in chronicle_kv have been updated
%%   * But the cache update has not yet been processed
%%   * During this window, auth requests will use the old cached keys
%% - Applications are expected to retry failed authentication requests

-module(jwt_auth).

%% API
-export([authenticate/1, get_standard_claim_names/0]).


-include("ns_common.hrl").
-include("rbac.hrl").
-include("jwt.hrl").

-include_lib("ns_common/include/cut.hrl").
-include_lib("jose/include/jose_jwk.hrl").
-include_lib("jose/include/jose_jwt.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type jose_jwk() :: #jose_jwk{
                       keys   :: undefined | {module(), any()},
                       kty    :: undefined | {module(), any()},
                       fields :: map()
                      }.

-type claim_value_type() :: list | string | integer.

%%%===================================================================
%%% API
%%%===================================================================

-spec authenticate(Token :: string()) ->
          {ok, #authn_res{}, auth_audit_props()} | {error, auth_audit_props()}.
authenticate(Token) ->
    Persisted =
        case chronicle_kv:get(kv, jwt_settings) of
            {ok, {#{enabled := true, issuers := Issuers}, _Rev}} ->
                Issuers;
            _ ->
                #{}
        end,
    validate_token(Token, maps:merge(jwt_issuer:settings(), Persisted)).

%%%===================================================================
%%% JWT validation
%%%===================================================================

%% Issuer may specify mapped claim names for these claims.
-type mapped_claim() :: aud | sub | groups | roles.

%% Issuer must use standard claim names for these claims.
-type standard_claim() :: iss | jti | alg | kid | exp | nbf | iat.

-type custom_claim() :: string().
-type claims() :: mapped_claim() | standard_claim() | custom_claim().

-type claim_type() :: mapped | standard.

-spec get_claim_type(Name :: claims()) -> claim_type().
get_claim_type(aud) -> mapped;
get_claim_type(sub) -> mapped;
get_claim_type(groups) -> mapped;
get_claim_type(roles) -> mapped;
get_claim_type(Name) when is_atom(Name) -> standard.

-spec get_claim_value_type(Name :: claims()) -> claim_value_type().
get_claim_value_type(Name) ->
    case Name of
        aud -> list;
        sub -> string;
        groups -> list;
        roles -> list;
        iss -> string;
        jti -> string;
        alg -> string;
        kid -> string;
        exp -> integer;
        nbf -> integer;
        iat -> integer
    end.

-spec required_claims() -> [claims()].
required_claims() ->
    [aud, sub, iss, alg, exp].

-spec header_claims() -> [claims()].
header_claims() ->
    [alg, kid].

-spec payload_claims() -> [claims()].
payload_claims() ->
    [exp, nbf, iat, jti, iss, aud, sub, groups, roles].

-spec get_claim_name(claims(), IssuerProps :: map()) -> binary().
get_claim_name(Name, IssuerProps) ->
    case get_claim_type(Name) of
        standard ->
            atom_to_binary(Name);
        mapped ->
            Claim = list_to_atom(atom_to_list(Name) ++ "_claim"),
            case maps:get(Claim, IssuerProps, undefined) of
                undefined -> atom_to_binary(Name);
                Value -> list_to_binary(Value)
            end
    end.

-spec get_claim_value(claims(), RawTokens :: map(), IssProps :: map()) ->
          binary() | [binary()] | integer() | undefined.
get_claim_value(Claim, RawTokens, IssProps) ->
    NameBin = get_claim_name(Claim, IssProps),
    ValueBin = get_nested_value(NameBin, RawTokens),
    get_claim_value(get_claim_value_type(Claim), ValueBin).

-spec get_nested_value(binary(), map()) -> binary() | [binary()] | integer() |
          undefined.
get_nested_value(PathBin, Map) ->
    get_nested_value_path(binary:split(PathBin, <<".">>, [global]), Map).

-spec get_nested_value_path([binary()], map()) -> binary() | [binary()] |
          integer() | undefined.
get_nested_value_path([Key | Rest], Map) when is_map(Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> undefined;
        Value when Rest =:= [] -> Value;
        NestedMap when is_map(NestedMap) ->
            get_nested_value_path(Rest, NestedMap);
        _ -> undefined
    end.

get_claim_value(number, Value) when is_integer(Value) -> Value;
get_claim_value(number, Value) when is_float(Value) -> Value;
get_claim_value(number, Value) when is_binary(Value) ->
    try binary_to_integer(Value)
    catch _:_ ->
            try binary_to_float(Value)
            catch _:_ ->
                    undefined
            end
    end;
get_claim_value(integer, Value) when is_integer(Value) -> Value;
get_claim_value(integer, Value) when is_binary(Value) ->
    try binary_to_integer(Value)
    catch _:_ ->
            undefined
    end;
get_claim_value(string, Value) when is_binary(Value) ->
    try binary_to_list(Value)
    catch _:_ ->
            undefined
    end;
get_claim_value(list, Value) ->
    try
        case Value of
            Values when is_list(Values) ->
                [binary_to_list(Elem) || Elem <- Values];
            Value when is_binary(Value) ->
                [binary_to_list(Value)];
            _ -> undefined
        end
    catch _:_ ->
            undefined
    end;
get_claim_value(boolean, Value) when is_boolean(Value) -> Value;
get_claim_value(boolean, Value) when is_binary(Value) ->
    try string:lowercase(binary_to_list(Value)) of
        "true" -> true;
        "false" -> false;
        _ -> undefined
    catch _:_ ->
            undefined
    end;
get_claim_value(array, Value) ->
    Value;
get_claim_value(object, Value) ->
    Value;
get_claim_value(_, _) ->
    undefined.

format_number(Value) when is_integer(Value) ->
    integer_to_list(Value);
format_number(Value) when is_float(Value) ->
    float_to_list(Value).

-spec get_standard_claim_names() -> [string()].
get_standard_claim_names() ->
    [atom_to_list(Claim) || Claim <- header_claims() ++ payload_claims()].

%% Extracts claims from a JWT in map format for further processing.
%% Keys are atoms, claim values are converted to standard formats (integer, list
%% of strings, string). Type validation is done and only valid claims are
%% included in the parsed claims.
-spec extract_claims(TokenBin :: binary(),
                     Issuers :: map()) ->
          {ok,
           ParsedPlusCustomClaims :: #{claims() => string() | [string()] |
                                       integer() |
                                       binary() | number() |
                                       boolean() | map() | list()}} |
          {error, Msg :: binary()}.
extract_claims(TokenBin, Issuers) ->
    try
        {_, HeaderMap} = jose_jws:to_map(jose_jwt:peek_protected(TokenBin)),
        {_, PayloadMap} = jose_jwt:peek_payload(TokenBin),

        IssuerName = case get_claim_value(iss, PayloadMap, #{}) of
                         undefined ->
                             throw({error, <<"Missing/invalid iss claim">>});
                         Name -> Name
                     end,
        IssProps = case maps:find(IssuerName, Issuers) of
                       {ok, Props} -> Props;
                       error ->
                           IssuerBin = list_to_binary(IssuerName),
                           throw({error, <<"Unknown issuer: ",
                                           IssuerBin/binary>>})
                   end,

        Claims0 = lists:foldl(
                    fun(Claim, Acc) ->
                            Map = case lists:member(Claim, header_claims()) of
                                      true -> HeaderMap;
                                      false -> PayloadMap
                                  end,
                            case get_claim_value(Claim, Map, IssProps) of
                                undefined -> Acc;
                                Value -> Acc#{Claim => Value}
                            end
                    end, #{}, header_claims() ++ payload_claims()),

        Claims = case maps:get(custom_claims, IssProps, undefined) of
                     Custom when is_map(Custom) ->
                         maps:fold(
                           fun(ClaimNameStr, _Conf, Acc) ->
                                   NameBin = list_to_binary(ClaimNameStr),
                                   case get_nested_value(NameBin, PayloadMap) of
                                       undefined -> Acc;
                                       Value ->
                                           Acc#{ClaimNameStr => Value}
                                   end
                           end, Claims0, Custom);
                     undefined -> Claims0
                 end,

        lists:foreach(
          fun(Claim) ->
                  case maps:is_key(Claim, Claims) of
                      false ->
                          throw(
                            {error, <<"Missing/invalid claim: ",
                                      (atom_to_binary(Claim))/binary>>});
                      true -> ok
                  end
          end, required_claims()),
        {ok, Claims}
    catch
        throw:Error -> Error;
        _:_ -> {error, <<"Invalid token format">>}
    end.

%% These normalization functions are used to convert jiffy-encoded JSON values
%% to proplists for auditing.

%% Standard claims are atoms. Custom claims are strings.
-spec normalize_claim(Claim :: claims()) -> binary().
normalize_claim(Claim) when is_atom(Claim) -> atom_to_binary(Claim);
normalize_claim(Claim) when is_list(Claim) -> list_to_binary(Claim).

%% JSON keys must be strings (binaries).
normalize_key(K) when is_binary(K) -> K.

%% Normalize JSON values (as decoded by jiffy) for auditing (ejson encoding).
normalize_val(V) when is_number(V); is_boolean(V); is_binary(V) -> V;
normalize_val(null) -> null;
normalize_val(V) when is_list(V) ->
    case io_lib:printable_list(V) of
        true -> list_to_binary(V);
        false -> [normalize_val(E) || E <- V]
    end;
normalize_val(V) when is_map(V) ->
    {[{normalize_key(K), normalize_val(Val)} ||
         {K, Val} <- maps:to_list(V)]}.

%% Converts the claims map to a proplist containing only binaries for auditing.
-spec audit_map_to_proplist(AuditMap :: map()) -> auth_audit_props().
audit_map_to_proplist(AuditMap) ->
    lists:sort(maps:fold(
                 fun(Claim, Value, Acc) ->
                         [{normalize_claim(Claim), normalize_val(Value)} | Acc]
                 end, [], AuditMap)).

-spec audit_success(Claims :: map(), AuthnRes :: #authn_res{}) ->
          auth_audit_props().
audit_success(Claims, AuthnRes) ->
    audit_map_to_proplist(Claims#{type => <<"jwt">>}) ++
        menelaus_auth:get_authn_res_audit_props(AuthnRes).

-spec audit_failure(map(), binary()) -> auth_audit_props().
audit_failure(Claims, Reason) ->
    audit_map_to_proplist(Claims#{reason => Reason, type => <<"jwt">>}).

-spec validate_token(Token :: string(), Issuers :: map()) ->
          {ok, #authn_res{}, auth_audit_props()} | {error, auth_audit_props()}.
validate_token(Token, Issuers) ->
    TokenBin = list_to_binary(Token),
    case extract_claims(TokenBin, Issuers) of
        {ok, Claims} ->
            IssuerName = maps:get(iss, Claims),
            RawProps = maps:get(IssuerName, Issuers),
            IssuerProps = RawProps#{name => IssuerName},
            case validate_signature(TokenBin, Claims, IssuerProps) of
                ok ->
                    case validate_payload(Claims, IssuerProps) of
                        {ok, AuthnRes} ->
                            AuditProps = audit_success(Claims, AuthnRes),
                            {ok, AuthnRes, AuditProps};
                        {error, Reason} ->
                            AuditProps = audit_failure(Claims, Reason),
                            {error, AuditProps}
                    end;
                {error, Reason} ->
                    AuditProps = audit_failure(Claims, Reason),
                    {error, AuditProps}
            end;
        {error, Reason} ->
            AuditProps = audit_failure(#{}, Reason),
            {error, AuditProps}
    end.

-spec validate_signature(TokenBin :: binary(), Claims :: map(),
                         IssProps :: map()) ->
          ok | {error, binary()}.
validate_signature(TokenBin, Claims, IssProps) ->
    AlgoConfig = maps:get(signing_algorithm, IssProps),
    AlgoBin = atom_to_binary(AlgoConfig),
    AlgoToken = maps:get(alg, Claims),
    case AlgoBin =:= list_to_binary(AlgoToken) of
        false ->
            {error, <<"Mismatched signing algorithm in JWT">>};
        true ->
            case lookup_jwk(Claims, IssProps, AlgoConfig) of
                {ok, JWK} ->
                    case jose_jwt:verify_strict(JWK, [AlgoBin], TokenBin) of
                        {true, _, _} -> ok;
                        {false, _, _} -> {error, <<"Invalid signature">>}
                    end;
                Error -> Error
            end
    end.

-spec lookup_jwk(Claims :: map(), IssuerProps :: map(),
                 Algorithm :: jwt_algorithm()) ->
          {ok, jose_jwk()} | {error, binary()}.
lookup_jwk(Claims, IssuerProps, Algorithm) ->
    case menelaus_web_jwt_key:is_symmetric_algorithm(Algorithm) of
        true ->
            JWK = jose_jwk:from_oct(maps:get(shared_secret, IssuerProps)),
            {ok, JWK};
        false ->
            KidBin =
                case maps:get(public_key_source, IssuerProps) of
                    pem -> undefined;
                    _ ->
                        case maps:get(kid, Claims, undefined) of
                            undefined -> undefined;
                            Kid -> list_to_binary(Kid)
                        end
                end,
            jwt_cache:get_jwk(IssuerProps, KidBin)
    end.

-spec validate_custom_claims(Claims :: map(), IssProps :: map()) ->
          ok | {error, binary()}.
validate_custom_claims(Claims, IssProps) ->
    case maps:get(custom_claims, IssProps, undefined) of
        undefined -> ok;
        CustomClaims when is_map(CustomClaims) ->
            maps:fold(
              fun(ClaimName, ClaimConfig, ok) ->
                      validate_single_custom_claim(Claims, ClaimName,
                                                   ClaimConfig);
                 (_ClaimName, _ClaimConfig, Error) ->
                      Error
              end, ok, CustomClaims)
    end.

-spec validate_single_custom_claim(Claims :: map(), ClaimName :: string(),
                                   ClaimConfig :: map()) ->
          ok | {error, binary()}.
validate_single_custom_claim(Claims, ClaimName, ClaimConfig) ->
    Type = maps:get(type, ClaimConfig),
    Mandatory = maps:get(mandatory, ClaimConfig, false),
    CNameBin = list_to_binary(ClaimName),
    RawClaimValue = maps:get(ClaimName, Claims, undefined),
    ParsedValue =
        case RawClaimValue of
            undefined -> undefined;
            Value -> get_claim_value(Type, Value)
        end,

    case {RawClaimValue, ParsedValue} of
        {undefined, _} when Mandatory ->
            {error, <<"Missing mandatory custom claim: ", CNameBin/binary>>};
        {undefined, _} when not Mandatory ->
            ok;
        {_, undefined} ->
            {error, <<"Custom claim ", CNameBin/binary,
                      " cannot be parsed as ",
                      (atom_to_binary(Type, utf8))/binary>>};
        {_, _} ->
            case validate_custom_claim(ParsedValue, Type, ClaimConfig) of
                ok -> ok;
                {error, Reason} ->
                    {error, <<"Custom claim validation failed for ",
                              CNameBin/binary, ": ", Reason/binary>>}
            end
    end.

-spec validate_custom_claim(ClaimValue :: string() | number() | boolean() |
                                          binary(),
                            Type :: string | number | boolean | array | object,
                            Config :: map()) ->
          ok | {error, binary()}.
validate_custom_claim(ClaimValue, string, Config) ->
    validate_custom_string_claim(ClaimValue, Config);
validate_custom_claim(ClaimValue, number, Config) ->
    validate_custom_number_claim(ClaimValue, Config);
validate_custom_claim(ClaimValue, boolean, Config) ->
    validate_boolean_claim(ClaimValue, Config);
validate_custom_claim(ClaimValue, array, _Config) ->
    validate_custom_array_claim(ClaimValue);
validate_custom_claim(ClaimValue, object, _Config) ->
    validate_custom_object_claim(ClaimValue).

-spec validate_custom_string_claim(ClaimValue :: string(), Config :: map()) ->
          ok | {error, binary()}.
validate_custom_string_claim(ClaimValue, Config) ->
    try re:run(ClaimValue, maps:get(pattern, Config)) of
        {match, _} -> ok;
        nomatch -> {error, <<"Value does not match pattern">>}
    catch
        _:_ -> {error, <<"Invalid regex pattern">>}
    end.

-spec validate_custom_number_claim(ClaimValue :: number(), Config :: map()) ->
          ok | {error, binary()}.
validate_custom_number_claim(ClaimValue, Config) ->
    Min = maps:get(min, Config, undefined),
    Max = maps:get(max, Config, undefined),
    RangeResult = validate_number_range(ClaimValue, Min, Max),
    EnumResult = validate_custom_number_enum_check(ClaimValue, Config),

    case {RangeResult, EnumResult} of
        {ok, ok} -> ok;
        {{error, RangeError}, ok} ->
            {error, <<"Number claim validation failed: ", RangeError/binary>>};
        {ok, {error, EnumError}} ->
            {error, <<"Number claim validation failed: ", EnumError/binary>>};
        {{error, RangeError}, {error, EnumError}} ->
            {error, <<"Number claim validation failed: ", RangeError/binary,
                      "; ", EnumError/binary>>}
    end.

-spec validate_boolean_claim(ClaimValue :: boolean(), Config :: map()) ->
          ok | {error, binary()}.
validate_boolean_claim(ClaimValue, Config) ->
    case maps:get(const, Config, undefined) of
        undefined -> ok;
        Const when ClaimValue =:= Const -> ok;
        Const -> {error, <<"Value must be ",
                       (atom_to_binary(Const))/binary>>}
    end.

-spec validate_custom_number_enum_check(number(), map()) -> ok |
    {error, binary()}.
validate_custom_number_enum_check(Value, Config) ->
    case maps:get(enum, Config, undefined) of
        undefined -> ok;
        Enum ->
            case lists:member(Value, Enum) of
                true -> ok;
                false ->
                    EnumStr = lists:join(", ", [format_number(N) || N <- Enum]),
                    {error, <<"Value must be one of: ",
                              (list_to_binary(EnumStr))/binary>>}
            end
    end.

-spec validate_number_range(Number :: number(), Min :: number() | undefined,
                            Max :: number() | undefined) ->
          ok | {error, binary()}.
validate_number_range(_Number, undefined, undefined) -> ok;
validate_number_range(Number, Min, undefined) ->
    case Number >= Min of
        true -> ok;
        false -> {error, <<"Value must be greater than or equal to ",
                           (list_to_binary(format_number(Min)))/binary>>}
    end;
validate_number_range(Number, undefined, Max) ->
    case Number =< Max of
        true -> ok;
        false -> {error, <<"Value must be less than or equal to ",
                           (list_to_binary(format_number(Max)))/binary>>}
    end;
validate_number_range(Number, Min, Max) ->
    case Number >= Min andalso Number =< Max of
        true -> ok;
        false -> {error, <<"Value must be between ",
                           (list_to_binary(format_number(Min)))/binary, " and ",
                           (list_to_binary(format_number(Max)))/binary>>}
    end.

%% Array validation - just check presence and non-empty for now
-spec validate_custom_array_claim(ClaimValue :: term()) ->
          ok | {error, binary()}.
validate_custom_array_claim(ClaimValue) ->
    case ClaimValue of
        Value when is_list(Value) ->
            ok;
        _ -> {error, <<"Value must be an array">>}
    end.

%% Object validation - just check presence
-spec validate_custom_object_claim(ClaimValue :: term()) ->
          ok | {error, binary()}.
validate_custom_object_claim(ClaimValue) ->
    case ClaimValue of
        Value when is_map(Value) -> ok;
        {Props} when is_list(Props) -> ok;  % JSON object format
        _ -> {error, <<"Value must be an object">>}
    end.

-spec validate_payload(Claims :: map(), IssProps :: map()) ->
          {ok, #authn_res{}} | {error, binary()}.
validate_payload(Claims, IssProps) ->
    Valid = functools:sequence_(
              [fun() -> validate(Claim, Claims, IssProps) end ||
                  Claim <- [exp, nbf, aud]]),
    case Valid of
        ok ->
            case validate_custom_claims(Claims, IssProps) of
                ok ->
                    case validate_user(Claims, IssProps) of
                        {ok, Username} -> get_auth_info(Claims, IssProps,
                                                        Username);
                        Error -> Error
                    end;
                Error -> Error
            end;
        Error -> Error
    end.

-spec validate_user(Claims :: map(), IssProps :: map()) ->
          {ok, string()} | {error, binary()}.
validate_user(Claims, IssProps) ->
    case map_claim(sub, Claims, IssProps) of
        [] ->
            UserBin = list_to_binary(maps:get(sub, Claims)),
            {error, <<UserBin/binary, " isn't a valid user name">>};
        [Username] -> {ok, Username}
    end.

-spec validate(exp | nbf | aud, Claims :: map(), IssProps :: map()) ->
          ok | {error, binary()}.
validate(exp, Claims, IssProps) ->
    Now = erlang:system_time(second),
    ExpiryLeeway = maps:get(expiry_leeway_s, IssProps),
    case maps:get(exp, Claims) of
        Exp when Now =< Exp + ExpiryLeeway ->
            ok;
        _ ->
            {error, <<"Token has expired">>}
    end;
validate(nbf, Claims, IssProps) ->
    Now = erlang:system_time(second),
    ExpiryLeeway = maps:get(expiry_leeway_s, IssProps),
    case maps:get(nbf, Claims, undefined) of
        undefined -> ok;
        NBF when Now >= NBF - ExpiryLeeway -> ok;
        _ -> {error, <<"Token not yet valid">>}
    end;
validate(aud, Claims, #{audience_handling := Handling,
                        audiences := Expected}) ->
    TokenAuds = maps:get(aud, Claims),
    case Handling of
        all ->
            case ordsets:subtract(Expected, TokenAuds) of
                [] -> ok;
                _ -> {error, <<"Invalid audience">>}
            end;
        any ->
            case ordsets:is_disjoint(Expected, TokenAuds) of
                true -> {error, <<"Invalid audience">>};
                false -> ok
            end
    end.

validate_map_claim_values(Type, MappingType, IssProps, Values) ->
    Key = list_to_atom(atom_to_list(Type) ++ "_maps"),
    Rules = maps:get(Key, IssProps, []),
    StopFirstMatch = maps:get(list_to_atom(atom_to_list(Key) ++
                                               "_stop_first_match"),
                              IssProps, true),
    auth_mapping:map_identities(MappingType, Values, Rules, StopFirstMatch).

-spec map_claim(sub | groups | roles, Claims :: map(), IssProps :: map()) ->
          [string()] | {error, binary()}.
map_claim(sub, Claims, IssProps) ->
    Value = maps:get(sub, Claims),
    case maps:get(name, IssProps) =:= jwt_issuer:name() of
        true ->
            %% we can trust our own token to have the correct user name
            [Value];
        false ->
            validate_map_claim_values(sub, user, IssProps, [Value])
    end;
map_claim(Type, Claims, IssProps) when Type =:= groups; Type =:= roles ->
    Values = case maps:get(Type, Claims, undefined) of
                 undefined ->
                     [];
                 TokenValues ->
                     TokenValues
             end,
    MappingType =
        case Type of
            roles ->
                {roles, case maps:get(name, IssProps) =:= jwt_issuer:name() of
                            true ->
                                all;
                            false ->
                                public
                        end};
            _ ->
                Type
        end,
    validate_map_claim_values(Type, MappingType, IssProps, Values).

get_auth_info(Claims, IssProps, Username) ->
    AuthnRes0 = menelaus_auth:init_auth({Username, external}),
    {ExtraGroups, ExtraRoles} =
        case maps:get(jit_provisioning, IssProps, false) of
            false -> {[], []};
            true -> {
                     map_claim(groups, Claims, IssProps),
                     map_claim(roles, Claims, IssProps)}
        end,

    %% Expiration time is in seconds since epoch (RFC 7519 4.1.4)
    Expiry = maps:get(exp, Claims) + maps:get(expiry_leeway_s, IssProps),

    {ok, AuthnRes0#authn_res{
           extra_groups = ExtraGroups,
           extra_roles = ExtraRoles,
           expiration_datetime_utc =
               calendar:system_time_to_universal_time(Expiry, second)
          }}.

-ifdef(TEST).

get_claim_value_test() ->
    ?assertEqual(123, get_claim_value(integer, 123)),
    ?assertEqual(123, get_claim_value(integer, <<"123">>)),
    ?assertEqual(undefined, get_claim_value(integer, <<"not_a_number">>)),

    ?assertEqual("test", get_claim_value(string, <<"test">>)),
    ?assertEqual(undefined, get_claim_value(string, [<<"test1">>])),
    ?assertEqual(undefined, get_claim_value(string, 123)),

    ?assertEqual(["test1"], get_claim_value(list, [<<"test1">>])),
    ?assertEqual(["test1", "test2"],
                 get_claim_value(list, [<<"test1">>, <<"test2">>])),
    ?assertEqual(["single"], get_claim_value(list, <<"single">>)),
    ?assertEqual(undefined, get_claim_value(list, 123)),

    ?assertEqual(undefined, get_claim_value(string, undefined)),
    ?assertEqual(undefined, get_claim_value(integer, undefined)),
    ?assertEqual(undefined, get_claim_value(list, undefined)).

validate_sub_test_() ->
    {setup,
     fun() -> meck:new(auth_mapping) end,
     fun(_) -> meck:unload(auth_mapping) end,
     fun(_) ->
             IssProps = #{name => "test-issuer"},
             [
              {"mapped username",
               fun() ->
                       meck:expect(auth_mapping, map_identities,
                                   fun(user, ["test-user"], _, true) ->
                                           ["mapped-user"] end),
                       ?assertEqual({ok, "mapped-user"},
                                    validate_user(#{sub => "test-user"},
                                                  IssProps))
               end},

              {"unmapped username",
               fun() ->
                       meck:expect(auth_mapping, map_identities,
                                   fun(user, ["unmapped-user"], _, true) ->
                                           [] end),
                       ?assertEqual({error, <<"unmapped-user isn't a valid user"
                                              " name">>},
                                    validate_user(#{sub => "unmapped-user"},
                                                  IssProps))
               end}
             ]
     end}.

validate_claims_test() ->
    Now = erlang:system_time(second),
    IssProps = #{
                 name => "test-issuer",
                 expiry_leeway_s => 300,
                 audience_handling => any,
                 audiences => ["aud1", "aud2"]
                },

    %% Test exp validation
    ?assertEqual(ok, validate(exp, #{exp => Now + 600}, IssProps)),
    ?assertEqual({error, <<"Token has expired">>},
                 validate(exp, #{exp => Now - 600}, IssProps)),

    %% Test nbf validation
    ?assertEqual(ok, validate(nbf, #{}, IssProps)),
    ?assertEqual(ok, validate(nbf, #{nbf => Now - 600}, IssProps)),
    ?assertEqual({error, <<"Token not yet valid">>},
                 validate(nbf, #{nbf => Now + 600}, IssProps)),

    %% Test aud validation
    ?assertEqual(ok, validate(aud, #{aud => ["aud1", "other"]}, IssProps)),

    IssPropsAll = IssProps#{audience_handling => all},
    ?assertEqual(ok, validate(aud, #{aud => ["aud1", "aud2"]}, IssPropsAll)),
    ?assertEqual({error, <<"Invalid audience">>},
                 validate(aud, #{aud => ["aud1", "other"]}, IssPropsAll)).

get_nested_value_test() ->
    Map = #{
            <<"simple">> => <<"value1">>,
            <<"nested">> => #{
                              <<"key">> => <<"value2">>,
                              <<"deep">> => #{
                                              <<"key">> => <<"value3">>
            }
        }
    },
    ?assertEqual(<<"value1">>, get_nested_value(<<"simple">>, Map)),
    ?assertEqual(<<"value2">>, get_nested_value(<<"nested.key">>, Map)),
    ?assertEqual(<<"value3">>, get_nested_value(<<"nested.deep.key">>, Map)),
    ?assertEqual(undefined, get_nested_value(<<"missing">>, Map)),
    ?assertEqual(undefined, get_nested_value(<<"nested.missing">>, Map)),
    ?assertEqual(undefined, get_nested_value(<<"nested.deep.missing">>, Map)),
    ?assertEqual(undefined, get_nested_value(<<"nested.key.missing">>, Map)),
    ?assertEqual(undefined, get_nested_value(<<"nested.deep.key.missing">>,
        Map)).

extract_claims_test_() ->
    {setup,
     fun() -> meck:new(jose_jwt) end,
     fun(_) -> meck:unload(jose_jwt) end,
     fun(_) ->
             Issuers = #{
                         "test-issuer" =>
                             #{
                               signing_algorithm => hs256,
                               aud_claim => "aud",
                               sub_claim => "sub"
                              }
                        },
             [
              {"valid claims",
               fun() ->
                       HeaderMap = #{
                                     <<"alg">> => <<"HS256">>,
                                     <<"kid">> => <<"key-1">>
                                    },
                       PayloadMap = #{
                                      <<"iss">> => <<"test-issuer">>,
                                      <<"sub">> => <<"test-user">>,
                                      <<"aud">> => <<"test-aud">>,
                                      <<"exp">> => 1234567890
                                     },

                       meck:expect(jose_jwt, peek_protected,
                                   fun(_) -> {ok, HeaderMap} end),
                       meck:expect(jose_jws, to_map,
                                   fun({ok, Map}) -> {ok, Map} end),
                       meck:expect(jose_jwt, peek_payload,
                                   fun(_) -> {ok, PayloadMap} end),

                       {ok, Claims} = extract_claims(<<"token">>, Issuers),
                       ?assertEqual("test-issuer", maps:get(iss, Claims)),
                       ?assertEqual("test-user", maps:get(sub, Claims)),
                       ?assertEqual(["test-aud"], maps:get(aud, Claims)),
                       ?assertEqual(1234567890, maps:get(exp, Claims)),
                       ?assertEqual("HS256", maps:get(alg, Claims)),
                       ?assertEqual("key-1", maps:get(kid, Claims))
               end},

              {"mapped claim names",
               fun() ->
                       Issuers3 = #{
                                    "test-issuer" =>
                                        #{
                                          signing_algorithm => hs256,
                                          sub_claim => "user.preferred_user",
                                          aud_claim => "azp",
                                          roles_claim => "resource.test.roles"
                                         }
                                   },
                       HeaderMap = #{
                                     <<"alg">> => <<"HS256">>,
                                     <<"kid">> => <<"key-1">>
                                    },
                       PayloadMap = #{
                                      <<"iss">> => <<"test-issuer">>,
                                      <<"user">> => #{
                                                      <<"preferred_user">> =>
                                                          <<"nested-user">>
                                                     },
                                      <<"azp">> => <<"test-client">>,
                                      <<"resource">> =>
                                          #{
                                            <<"test">> =>
                                                #{
                                                  <<"roles">> =>
                                                      [<<"role1">>, <<"role2">>]
                                                 }
                                           },
                                      <<"exp">> => 1234567890
                                     },

                       meck:expect(jose_jwt, peek_protected,
                                   fun(_) -> {ok, HeaderMap} end),
                       meck:expect(jose_jws, to_map,
                                   fun({ok, Map}) -> {ok, Map} end),
                       meck:expect(jose_jwt, peek_payload,
                                   fun(_) -> {ok, PayloadMap} end),

                       {ok, Claims} = extract_claims(<<"token">>, Issuers3),
                       ?assertEqual("test-issuer", maps:get(iss, Claims)),
                       ?assertEqual("nested-user", maps:get(sub, Claims)),
                       ?assertEqual(["test-client"], maps:get(aud, Claims)),
                       ?assertEqual(["role1", "role2"],
                                    maps:get(roles, Claims)),
                       ?assertEqual(1234567890, maps:get(exp, Claims))
               end},

              {"missing issuer",
               fun() ->
                       PayloadMap = #{<<"sub">> => <<"test-user">>},
                       meck:expect(jose_jwt, peek_protected,
                                   fun(_) -> {ok, #{}} end),
                       meck:expect(jose_jws, to_map,
                                   fun({ok, Map}) -> {ok, Map} end),
                       meck:expect(jose_jwt, peek_payload,
                                   fun(_) -> {ok, PayloadMap} end),

                       ?assertEqual({error, <<"Missing/invalid iss claim">>},
                                    extract_claims(<<"token">>, Issuers))
               end},

              {"unknown issuer",
               fun() ->
                       HeaderMap = #{<<"alg">> => <<"HS256">>},
                       PayloadMap = #{<<"iss">> => <<"unknown">>},

                       meck:expect(jose_jwt, peek_protected,
                                   fun(_) -> {ok, HeaderMap} end),
                       meck:expect(jose_jws, to_map,
                                   fun({ok, Map}) -> {ok, Map} end),
                       meck:expect(jose_jwt, peek_payload,
                                   fun(_) -> {ok, PayloadMap} end),

                       ?assertEqual({error, <<"Unknown issuer: unknown">>},
                                    extract_claims(<<"token">>, Issuers))
               end},

              {"missing required claims",
               fun() ->
                       HeaderMap = #{<<"alg">> => <<"HS256">>},
                       PayloadMap = #{
                                      <<"iss">> => <<"test-issuer">>,
                                      <<"sub">> => <<"test-user">>
                                     },
                       meck:expect(jose_jwt, peek_protected,
                                   fun(_) -> {ok, HeaderMap} end),
                       meck:expect(jose_jws, to_map,
                                   fun({ok, Map}) -> {ok, Map} end),
                       meck:expect(jose_jwt, peek_payload,
                                   fun(_) -> {ok, PayloadMap} end),

                       ?assertMatch({error,
                                     <<"Missing/invalid claim: ",_/binary>>},
                                    extract_claims(<<"token">>, Issuers))
               end},

              {"invalid token format",
               fun() ->
                       meck:expect(jose_jwt, peek_protected,
                                   fun(_) -> {error, invalid} end),
                       meck:expect(jose_jws, to_map,
                                   fun(_) -> {error, invalid} end),
                       ?assertEqual({error, <<"Invalid token format">>},
                                    extract_claims(<<"token">>, Issuers))
               end}
             ]
     end}.

get_claim_name_test() ->
    Props = #{sub_claim => "username",
              aud_claim => "scope",
              roles_claim => "nested.roles",
              groups_claim => "custom.groups"},
    ?assertEqual(<<"username">>, get_claim_name(sub, Props)),
    ?assertEqual(<<"scope">>, get_claim_name(aud, Props)),
    ?assertEqual(<<"nested.roles">>, get_claim_name(roles, Props)),
    ?assertEqual(<<"custom.groups">>, get_claim_name(groups, Props)),
    ?assertEqual(<<"exp">>, get_claim_name(exp, Props)), % standard claim
    ?assertEqual(<<"groups">>, get_claim_name(groups, #{})), % fallback default
    ?assertEqual(<<"roles">>, get_claim_name(roles, #{})). % fallback default

audit_map_to_proplist_test() ->
    %% Test simple claims
    Simple = #{
               iss => "test-issuer",
               sub => "test-user",
               exp => 1234567890,
               nbf => 1234567800
              },
    ?assertEqual([
                  {<<"exp">>, 1234567890},
                  {<<"iss">>, <<"test-issuer">>},
                  {<<"nbf">>, 1234567800},
                  {<<"sub">>, <<"test-user">>}
                 ], lists:sort(audit_map_to_proplist(Simple))),

    %% Test array claims
    Arrays = #{
               aud => ["aud1", "aud2"],
               groups => ["group1", "group2"]
              },
    ?assertEqual([
                  {<<"aud">>, [<<"aud1">>, <<"aud2">>]},
                  {<<"groups">>, [<<"group1">>, <<"group2">>]}
                 ], lists:sort(audit_map_to_proplist(Arrays))),

    %% Test complex roles
    Roles = #{
              roles => [
                        "admin",
                        "bucket_admin[default]",
                        "data_writer[default:scope1]",
                        "query_select[default:scope1.collection1]"
                       ]
             },
    ?assertEqual([
                  {<<"roles">>, [
                                 <<"admin">>,
                                 <<"bucket_admin[default]">>,
                                 <<"data_writer[default:scope1]">>,
                                 <<"query_select[default:scope1.collection1]">>
                                ]}
                 ], lists:sort(audit_map_to_proplist(Roles))),

    %% Test error reason
    Error = #{
              reason => <<"Token has expired">>,
              exp => 1234567890
             },
    ?assertEqual([
                  {<<"exp">>, 1234567890},
                  {<<"reason">>, <<"Token has expired">>}
                 ], lists:sort(audit_map_to_proplist(Error))),

    %% Test mapped claims
    Mapped = #{
               mapped_groups => ["admins", "users"],
               mapped_roles => ["bucket_admin[*]", "data_reader[*:*.*]"]
              },
    ?assertEqual([
                  {<<"mapped_groups">>, [<<"admins">>, <<"users">>]},
                  {<<"mapped_roles">>, [<<"bucket_admin[*]">>,
                                        <<"data_reader[*:*.*]">>]}
                 ], lists:sort(audit_map_to_proplist(Mapped))),


    Nested = #{
               "user_metadata" =>
                   #{
                     <<"preferences">> => #{
                                            <<"theme">> => <<"dark">>,
                                            <<"notifications">> => true
                                           },
                     <<"profile">> => #{
                                        <<"age">> => 30,
                                        <<"active">> => true
                                       }
                                 }
              },
    Actual = audit_map_to_proplist(Nested),
    ?assertEqual(1, length(Actual)),
    [{<<"user_metadata">>, UserMetadata}] = Actual,
    {UserMetadataKVs} = UserMetadata,
    ?assertEqual(2, length(UserMetadataKVs)),

    {Preferences} = proplists:get_value(<<"preferences">>, UserMetadataKVs),
    ?assertEqual(2, length(Preferences)),
    ?assert(lists:keymember(<<"theme">>, 1, Preferences)),
    ?assert(lists:keymember(<<"notifications">>, 1, Preferences)),
    {<<"theme">>, <<"dark">>} = lists:keyfind(<<"theme">>, 1, Preferences),
    {<<"notifications">>, true} = lists:keyfind(<<"notifications">>, 1,
                                                Preferences),

    {Profile} = proplists:get_value(<<"profile">>, UserMetadataKVs),
    ?assertEqual(2, length(Profile)),
    ?assert(lists:keymember(<<"age">>, 1, Profile)),
    ?assert(lists:keymember(<<"active">>, 1, Profile)),
    {<<"age">>, 30} = lists:keyfind(<<"age">>, 1, Profile),
    {<<"active">>, true} = lists:keyfind(<<"active">>, 1, Profile).

custom_claims_validation_test() ->
    [
     %% Test string validation
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"email">> => <<"test@example.com">>},
                         <<"email">>,
                         #{type => string,
                           pattern => "^[a-z]+@[a-z]+\\.[a-z]+$",
                           mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"email">> => <<"invalid">>},
                                   <<"email">>,
                                   #{type => string,
                                     pattern => "^[a-z]+@[a-z]+\\.[a-z]+$",
                                     mandatory => true})),

     %% Test number validation with integers
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"age">> => 25},
                         <<"age">>,
                         #{type => number,
                           min => 18,
                           max => 65,
                           mandatory => true})),
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"age">> => 18},
                         <<"age">>,
                         #{type => number,
                           min => 18,
                           max => 65,
                           mandatory => true})),
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"age">> => 65},
                         <<"age">>,
                         #{type => number,
                           min => 18,
                           max => 65,
                           mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"age">> => 17},
                                   <<"age">>,
                                   #{type => number,
                                     min => 18,
                                     max => 65,
                                     mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"age">> => 66},
                                   <<"age">>,
                                   #{type => number,
                                     min => 18,
                                     max => 65,
                                     mandatory => true})),

     %% Test number validation with floats
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"score">> => 3.5},
                         <<"score">>,
                         #{type => number,
                           min => 1.0,
                           max => 5.0,
                           mandatory => true})),
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"score">> => 1.0},
                         <<"score">>,
                         #{type => number,
                           min => 1.0,
                           max => 5.0,
                           mandatory => true})),
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"score">> => 5.0},
                         <<"score">>,
                         #{type => number,
                           min => 1.0,
                           max => 5.0,
                           mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"score">> => 0.9},
                                   <<"score">>,
                                   #{type => number,
                                     min => 1.0,
                                     max => 5.0,
                                     mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"score">> => 5.1},
                                   <<"score">>,
                                   #{type => number,
                                     min => 1.0,
                                     max => 5.0,
                                     mandatory => true})),

     %% Test number validation with enum
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"level">> => 2},
                         <<"level">>,
                         #{type => number,
                           enum => [1, 2, 3, 4, 5],
                           mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"level">> => 6},
                                   <<"level">>,
                                   #{type => number,
                                     enum => [1, 2, 3, 4, 5],
                                     mandatory => true})),

     %% Test boolean validation
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"admin">> => true},
                         <<"admin">>,
                         #{type => boolean,
                           const => true,
                           mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"admin">> => false},
                                   <<"admin">>,
                                   #{type => boolean,
                                     const => true,
                                     mandatory => true})),

     %% Test array validation
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"roles">> => [<<"admin">>, <<"user">>]},
                         <<"roles">>,
                         #{type => array,
                           mandatory => true})),
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{<<"roles">> => []},
                                   <<"roles">>,
                                   #{type => array,
                                     mandatory => true})),

     %% Test object validation
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{<<"profile">> => #{<<"name">> => <<"John">>}},
                         <<"profile">>,
                         #{type => object,
                           mandatory => true})),

     %% Test optional claims
     ?_assertEqual(ok, validate_single_custom_claim(
                         #{},
                         <<"optional">>,
                         #{type => string,
                           pattern => ".*",
                           mandatory => false})),

     %% Test missing mandatory claims
     ?_assertMatch({error, _}, validate_single_custom_claim(
                                   #{},
                                   <<"required">>,
                                   #{type => string,
                                     pattern => ".*",
                                     mandatory => true}))
    ].

-endif.
