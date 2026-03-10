%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc Registry of credential types and their field specifications.
%%
%% This module is the single source of truth for all credential-type-specific
%% knowledge: which fields exist, which are sensitive (encrypted), what their
%% REST wire names are, and how to validate them.
%%
%% Adding a new credential type requires adding a new clause to
%% `field_specs/1', additional validators to `fields_validators/1' and adding
%% the new credential type to CREDENTIAL_TYPES, credential_type().
%%
%% Consumers:
%%   - cb_credentials_store — sensitive_fields/1
%%   - menelaus_web_credentials — validators, export_fields, ingest fields

-module(cb_credential_types).

-include_lib("ns_common/include/cut.hrl").
-include("credentials.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([field_specs/1,
         sensitive_fields/1,
         rest_key/1,
         export_fields/2,
         validated_fields_to_store/2,
         fields_validators/1]).

%% A field specification describes one field of a credential type.
%%
%% #{
%%   storage_key => atom(),        %% internal snake_case key
%%   required    => boolean(),
%%   sensitive   => boolean(),     %% true → value masked in public view
%%   type        => string | integer | boolean | cert_pem | pkey_pem
%%                | http_auth_scheme | couchbase_encryption_type | json_object
%% }
-type field_spec() :: #{storage_key := atom(),
                        required    := boolean(),
                        sensitive   := boolean(),
                        type        := string | integer | boolean | cert_pem
                       | pkey_pem | http_auth_scheme
                       | couchbase_encryption_type | json_object}.

%% @doc Field specifications for a credential type.
-spec field_specs(atom()) -> [field_spec()].
field_specs(aws) ->
    [#{storage_key => access_key_id,
       required    => true,
       sensitive   => false,
       type        => string},
     #{storage_key => secret_access_key,
       required    => true,
       sensitive   => true,
       type        => string},
     #{storage_key => region,
       required    => true,
       sensitive   => false,
       type        => string},
     #{storage_key => endpoint,
       required    => false,
       sensitive   => false,
       type        => string},
     #{storage_key => session_token,
       required    => false,
       sensitive   => true,
       type        => string}
    ];

field_specs(azure_shared) ->
    [#{storage_key => account_name,
       required    => true,
       sensitive   => false,
       type        => string},
     #{storage_key => account_key,
       required    => true,
       sensitive   => true,
       type        => string},
     #{storage_key => endpoint,
       required    => false,
       sensitive   => false,
       type        => string}
    ];

field_specs(azure_ad) ->
    [#{storage_key => client_id,
       required    => true,
       sensitive   => false,
       type        => string},
     #{storage_key => tenant_id,
       required    => true,
       sensitive   => false,
       type        => string},
     #{storage_key => client_secret,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => certificate,
       required    => false,
       sensitive   => false,
       type        => cert_pem},
     #{storage_key => cert_password,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => endpoint,
       required    => false,
       sensitive   => false,
       type        => string}
    ];

field_specs(azure_sas) ->
    [#{storage_key => account_name,
       required    => true,
       sensitive   => false,
       type        => string},
     #{storage_key => shared_access_signature,
       required    => true,
       sensitive   => true,
       type        => string},
     #{storage_key => endpoint,
       required    => false,
       sensitive   => false,
       type        => string}
    ];

field_specs(azure_managed) ->
    [#{storage_key => managed_identity_id,
       required    => false,
       sensitive   => false,
       type        => string},
     #{storage_key => endpoint,
       required    => false,
       sensitive   => false,
       type        => string}
    ];

field_specs(gcp) ->
    [#{storage_key => json_credentials,
       required    => false,
       sensitive   => true,
       type        => json_object},
     #{storage_key => access_key_id,
       required    => false,
       sensitive   => false,
       type        => string},
     #{storage_key => secret_access_key,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => region,
       required    => false,
       sensitive   => false,
       type        => string},
     #{storage_key => endpoint,
       required    => false,
       sensitive   => false,
       type        => string}
    ];

field_specs(http) ->
    [#{storage_key => auth_scheme,
       required    => true,
       sensitive   => false,
       type        => http_auth_scheme},
     #{storage_key => username,
       required    => false,
       sensitive   => false,
       type        => string},
     #{storage_key => password,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => header_name,
       required    => false,
       sensitive   => false,
       type        => string},
     #{storage_key => token,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => certificate,
       required    => false,
       sensitive   => false,
       type        => cert_pem},
     #{storage_key => private_key,
       required    => false,
       sensitive   => true,
       type        => pkey_pem},
     #{storage_key => passphrase,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => root_certificate,
       required    => false,
       sensitive   => false,
       type        => cert_pem},
     #{storage_key => skip_verify,
       required    => false,
       sensitive   => false,
       type        => boolean}
    ];

field_specs(couchbase) ->
    [#{storage_key => encryption_type,
       required    => true,
       sensitive   => false,
       type        => couchbase_encryption_type},
     #{storage_key => username,
       required    => false,
       sensitive   => false,
       type        => string},
     #{storage_key => password,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => certificate,
       required    => false,
       sensitive   => false,
       type        => cert_pem},
     #{storage_key => private_key,
       required    => false,
       sensitive   => true,
       type        => pkey_pem},
     #{storage_key => passphrase,
       required    => false,
       sensitive   => true,
       type        => string},
     #{storage_key => root_certificate,
       required    => false,
       sensitive   => false,
       type        => cert_pem}
    ].

%% @doc Return the REST (camelCase) key atom for a field spec,
%% derived via misc:snake_to_camel_atom/1.
-spec rest_key(field_spec()) -> atom().
rest_key(#{storage_key := SK}) -> misc:snake_to_camel_atom(SK).

%% @doc Return the list of sensitive (secret) storage keys for a type.
-spec sensitive_fields(atom()) -> [atom()].
sensitive_fields(Type) ->
    [maps:get(storage_key, S) || S <- field_specs(Type),
                                 maps:get(sensitive, S) =:= true].

%% @doc Convert a storage-format fields map to the REST wire format
%% (binary camelCase keys, binary values).  Sensitive fields that have been
%% masked to <<"********">> by redact_credential are passed through as-is.
-spec export_fields(atom(), map()) -> map().
export_fields(Type, Fields) ->
    %% Build storage_key -> rest_key atom map from specs.
    KeyMap = maps:from_list([{maps:get(storage_key, S), rest_key(S)}
                             || S <- field_specs(Type)]),
    maps:fold(
      fun (StorageKey, Value, Acc) ->
              {ok, RestKey} = maps:find(StorageKey, KeyMap),
              Acc#{atom_to_binary(RestKey) => export_value(Value)}
      end, #{}, Fields).

%% @doc Convert a validated proplist (from the REST validator, camelCase atom
%% keys) to the storage map (snake_case atom keys).
-spec validated_fields_to_store(atom(), [{atom(), term()}]) -> map().
validated_fields_to_store(Type, FieldsProplist) ->
    %% Build rest_key atom -> storage_key map from specs.
    KeyMap = maps:from_list([{rest_key(S), maps:get(storage_key, S)}
                             || S <- field_specs(Type)]),
    lists:foldl(
      fun ({RestKey, Value}, Acc) ->
              {ok, StorageKey} = maps:find(RestKey, KeyMap),
              Acc#{StorageKey => Value}
      end, #{}, FieldsProplist).

%% @doc Generate validator rules for the fields of a credential type.
-spec fields_validators(atom()) -> [term()].
fields_validators(Type) ->
    Specs = field_specs(Type),
    Required = [validator:required(rest_key(S), _)
                || S <- Specs, maps:get(required, S) =:= true],
    Typed = lists:flatmap(fun field_type_validator/1, Specs),
    CrossField = cross_field_validators(Type),
    Required ++ Typed ++ CrossField ++ [validator:unsupported(_)].

field_type_validator(#{type := string} = S) ->
    [validator:non_empty_string(rest_key(S), _)];
field_type_validator(#{type := integer} = S) ->
    [validator:integer(rest_key(S), _)];
field_type_validator(#{type := boolean} = S) ->
    [validator:boolean(rest_key(S), _)];
field_type_validator(#{type := cert_pem} = S) ->
    [validator:non_empty_string(rest_key(S), _),
     validator:validate(fun validate_cert_pem/1, rest_key(S), _)];
field_type_validator(#{type := pkey_pem} = S) ->
    [validator:non_empty_string(rest_key(S), _),
     validator:validate(fun validate_pkey_pem/1, rest_key(S), _)];
field_type_validator(#{type := http_auth_scheme} = S) ->
    [validator:non_empty_string(rest_key(S), _),
     validator:one_of(rest_key(S), ["basic", "bearer", "mtls"], _)];
field_type_validator(#{type := couchbase_encryption_type} = S) ->
    [validator:non_empty_string(rest_key(S), _),
     validator:one_of(rest_key(S), ["none", "half", "full"], _)];
field_type_validator(#{type := json_object} = S) ->
    [validator:validate(fun validate_json_object/1, rest_key(S), _)].

validate_cert_pem(Cert) ->
    case ns_server_cert:decode_cert_chain(iolist_to_binary(Cert)) of
        {ok, [_ | _]} -> {value, Cert};
        {ok, []} -> {error, "invalid certificate"};
        {error, _} -> {error, "invalid certificate"}
    end.

validate_pkey_pem(Key) ->
    case ns_server_cert:validate_pkey(iolist_to_binary(Key),
                                      fun () -> undefined end) of
        {ok, _} -> {value, Key};
        {error, could_not_decrypt} -> {value, Key};
        {error, _} -> {error, "invalid private key"}
    end.

validate_json_object(Value) when is_binary(Value) ->
    try json:decode(Value) of
        Map when is_map(Map) -> {value, Value};
        _ -> {error, "must be a JSON object"}
    catch
        _:_ -> {error, "invalid JSON"}
    end.

%% HTTP: require scheme-specific fields based on authScheme.
cross_field_validators(http) ->
    [validator:post_validate_all(fun validate_http_fields/1, _),
     validator:post_validate_all(fun validate_pkey_with_passphrase/1, _)];

%% Azure AD: exactly one of clientSecret or certificate must be provided.
cross_field_validators(azure_ad) ->
    [validator:post_validate_all(fun validate_azure_ad_fields/1, _)];

%% GCP: either jsonCredentials (service-account) or
%% accessKeyId + secretAccessKey (HMAC) must be provided, but not both.
cross_field_validators(gcp) ->
    [validator:post_validate_all(fun validate_gcp_fields/1, _)];

cross_field_validators(couchbase) ->
    [validator:post_validate_all(fun validate_pkey_with_passphrase/1, _)];

cross_field_validators(_) ->
    [].

validate_http_fields(Props) ->
    case proplists:get_value(authScheme, Props) of
        "basic" ->
            require_fields([username, password], Props);
        "bearer" ->
            require_fields([token], Props);
        "mtls" ->
            require_fields([certificate, privateKey], Props);
        _ ->
            %% one_of already rejected unknown schemes
            ok
    end.

validate_azure_ad_fields(Props) ->
    HasSecret = proplists:get_value(clientSecret, Props) =/= undefined,
    HasCert   = proplists:get_value(certificate, Props) =/= undefined,
    case {HasSecret, HasCert} of
        {false, false} ->
            {error, "Either clientSecret or certificate must be provided"};
        {true, true} ->
            {error, "Only one of clientSecret or certificate may be provided"};
        _ ->
            ok
    end.

validate_gcp_fields(Props) ->
    HasSA   = proplists:get_value(jsonCredentials, Props) =/= undefined,
    HasHMAC = proplists:get_value(accessKeyId, Props) =/= undefined orelse
        proplists:get_value(secretAccessKey, Props) =/= undefined,
    case {HasSA, HasHMAC} of
        {false, false} ->
            {error, "Either jsonCredentials (service-account) or "
             "accessKeyId and secretAccessKey (HMAC) must be provided"};
        {true, true} ->
            {error, "Only one of jsonCredentials or "
             "accessKeyId/secretAccessKey may be provided"};
        {false, true} ->
            %% HMAC mode: both accessKeyId and secretAccessKey are required.
            require_fields([accessKeyId, secretAccessKey], Props);
        {true, false} ->
            ok
    end.

require_fields(Keys, Props) ->
    Missing = [atom_to_list(K)
               || K <- Keys,
                  proplists:get_value(K, Props) =:= undefined],
    case Missing of
        [] ->
            ok;
        _ ->
            {error, io_lib:format("Missing required field(s): ~s",
                                  [lists:join(", ", Missing)])}
    end.

validate_pkey_with_passphrase(Props) ->
    PKey = proplists:get_value(privateKey, Props),
    Passphrase = proplists:get_value(passphrase, Props),
    case PKey of
        undefined ->
            ok;
        _ ->
            PassFun = fun () -> Passphrase end,
            case ns_server_cert:validate_pkey(iolist_to_binary(PKey),
                                              PassFun) of
                {ok, _} -> ok;
                {error, could_not_decrypt} ->
                    {error, "Could not decrypt private key with passphrase"};
                {error, _} -> ok
            end
    end.


export_value(V) when is_binary(V)  -> V;
export_value(V) when is_list(V)    -> list_to_binary(V);
export_value(V) when is_atom(V)    -> atom_to_binary(V);
export_value(V) when is_integer(V) -> V;
export_value(V) when is_boolean(V) -> V;
export_value(V)                    -> V.

-ifdef(TEST).

aws_sensitive_fields_test() ->
    ?assertEqual([secret_access_key, session_token], sensitive_fields(aws)).

gcp_sensitive_fields_test() ->
    ?assertEqual([json_credentials, secret_access_key], sensitive_fields(gcp)).

gcp_sa_cross_validator_test() ->
    %% Service-account mode: jsonCredentials alone is sufficient.
    ?assertEqual(ok, validate_gcp_fields([{jsonCredentials, "{}"}])).

gcp_hmac_cross_validator_test() ->
    %% HMAC mode: both accessKeyId and secretAccessKey are required.
    ?assertEqual(ok, validate_gcp_fields([{accessKeyId, "AK"},
                                          {secretAccessKey, "SK"}])).

http_bearer_valid_test() ->
    Props = [{authScheme, "bearer"}, {token, "mytoken"}],
    ?assertEqual(ok, validate_http_fields(Props)).

http_bearer_missing_token_test() ->
    Props = [{authScheme, "bearer"}],
    ?assertMatch({error, _}, validate_http_fields(Props)).

http_mtls_valid_test() ->
    Props = [{authScheme, "mtls"}, {certificate, "cert"}, {privateKey, "key"}],
    ?assertEqual(ok, validate_http_fields(Props)).

http_mtls_missing_cert_test() ->
    Props = [{authScheme, "mtls"}, {privateKey, "key"}],
    ?assertMatch({error, _}, validate_http_fields(Props)).

key_maps_consistent_test() ->
    %% For every type, rest_key -> storage_key -> rest_key must round-trip.
    %% This is to catch ambiguous names like rootCA.
    lists:foreach(
      fun (Type) ->
              lists:foreach(
                fun (Spec) ->
                        SK = maps:get(storage_key, Spec),
                        RK = rest_key(Spec),
                        %% All storage keys within a type must be unique.
                        Dups = [S || S <- field_specs(Type),
                                     maps:get(storage_key, S) =:= SK],
                        ?assertEqual(1, length(Dups)),
                        %% All rest keys within a type must be unique.
                        RDups = [S || S <- field_specs(Type),
                                      rest_key(S) =:= RK],
                        ?assertEqual(1, length(RDups))
                end, field_specs(Type))
      end, ?CREDENTIAL_TYPES).

-endif.
