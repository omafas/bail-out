%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc REST API for the credential store.
%%
%% Handles two groups of endpoints:
%%
%% 1. Store settings  –  /settings/credentialStore
%%      GET | PUT | DELETE
%% Sample REST request:
%% curl -X PUT -u Administrator:password -H "Content-Type: application/json" \
%%      -d '{
%%           "configEncryptionOverride": false,
%%           "n2nEncryptionOverride": false
%%          }'
%%      http://localhost:8091/settings/credentialStore
%%
%% 2. Credential CRUD  –  /settings/credentials[/:id]
%%      GET    /settings/credentials           -> list all (optional ?prefix=)
%%      GET    /settings/credentials/:id       -> get one
%%      POST   /settings/credentials/:id       -> create
%%      PUT    /settings/credentials/:id       -> update (full field replace)
%%      DELETE /settings/credentials/:id       -> delete
%%
%% JSON wire format for create/update body:
%%   {"type": "aws",
%%    "fields": {"accessKeyId": "...", "secretAccessKey": "...",
%%               "region": "us-east-1", "endpoint": "..."},
%%    "guardrails": {"allowedServices": ["n1ql"],
%%                   "urlWhitelist": {"allAccess": false,
%%                                    "allowedUrls": ["https://..."],
%%                                    "disallowedUrls": ["https://..."]},
%%                   "allowedResources": ["bucket/path"],
%%                   "allowedOperations": ["READ", "LIST"]},
%%    "description": "...",
%%    "expiresAt": 1740000000000}
%%

-module(menelaus_web_credentials).

-include("ns_common.hrl").
-include("credentials.hrl").
-include_lib("ns_common/include/cut.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_settings/2]).

%% Credential CRUD endpoints
-export([handle_list/1,
         handle_get/2,
         handle_post/2,
         handle_put/2,
         handle_delete/2]).

%% Settings endpoint – /settings/credentialStore
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
        maps:from_list([{misc:snake_to_camel_atom(Key), Key} ||
                           {Key, _} <- ?PARAMS_WITH_FORMATTERS])).

-define(STORAGE_TO_REST,
        maps:from_list([{Key, {misc:snake_to_camel_atom(Key), Format}} ||
                           {Key, Format} <- ?PARAMS_WITH_FORMATTERS])).


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
      Req, json, settings_validators()).

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

settings_validators() ->
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
    {ok, {RestKey, Format}} = maps:find(StorageKey, Table),
    case Format of
        undefined when Value =/= undefined ->
            Acc#{RestKey => Value};
        undefined ->
            Acc;
        Formatter ->
            case Formatter(Value) of
                undefined -> Acc;
                FormattedValue -> Acc#{RestKey => FormattedValue}
            end
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

-define(MAX_CRED_ID_LENGTH, 128).

%% @doc Validate a credential ID extracted from the URL path or query string.
-spec validate_credential_id(credential_id()) -> ok | {error, iolist()}.
validate_credential_id([]) ->
    {error, "Credential id must not be empty"};
validate_credential_id(Id) ->
    case length(Id) > ?MAX_CRED_ID_LENGTH of
        true ->
            {error,
             io_lib:format(
               "Credential id length (~p) must not exceed ~p characters",
               [length(Id), ?MAX_CRED_ID_LENGTH])};
        false ->
            AllAsciiPrintable =
                lists:all(fun (C) -> C >= 16#21 andalso C =< 16#7E end, Id),
            case AllAsciiPrintable of
                true  -> ok;
                false ->
                    {error,
                     "Credential id must contain only printable "
                     "ASCII characters (0x21-0x7E)"}
            end
    end.

%% @doc Validate the prefix query parameter from handle_list.
-spec validate_prefix(string()) -> ok | {error, iolist()}.
validate_prefix("") -> ok;
validate_prefix(Prefix) -> validate_credential_id(Prefix).

handle_list(Req) ->
    Prefix = proplists:get_value("prefix",
                                 mochiweb_request:parse_qs(Req), ""),
    case validate_prefix(Prefix) of
        {error, Reason} ->
            menelaus_util:reply_json(
              Req, {[{error, iolist_to_binary(Reason)}]}, 400);
        ok ->
            case cb_credentials_store:list(Prefix) of
                {ok, Creds} ->
                    JsonBin = encode_response(
                                [export_credential(C) || C <- Creds]),
                    reply_json_ok(Req, JsonBin, 200);
                {error, Reason2} ->
                    reply_store_error(Req, Reason2)
            end
    end.

handle_get(IdStr, Req) ->
    case cb_credentials_store:get(IdStr) of
        {ok, Cred} ->
            reply_json_ok(Req,
                          encode_response(export_credential(Cred)),
                          200);
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, Reason} ->
            reply_store_error(Req, Reason)
    end.

handle_post(IdStr, Req) ->
    case validate_credential_id(IdStr) of
        {error, Reason} ->
            menelaus_util:reply_json(
              Req, {[{error, iolist_to_binary(Reason)}]}, 400);
        ok ->
            Author = get_author(Req),
            validator:handle(
              fun (Props) ->
                      Type      = proplists:get_value(type, Props),
                      Fields    = validated_fields_to_store(
                                    Type, proplists:get_value(fields, Props)),
                      MetaExtra = validated_meta_extra(Props),
                      case cb_credentials_store:create(IdStr, Type, Fields,
                                                       MetaExtra, Author) of
                          {ok, Cred} ->
                              reply_json_ok(Req,
                                            encode_response(
                                              export_credential(Cred)), 201);
                          {error, already_exists} ->
                              reply_json_ok(Req,
                                            encode_response(
                                              #{error => <<"Credential already "
                                                           "exists">>}),
                                            409);
                          {error, Reason2} ->
                              reply_store_error(Req, Reason2)
                      end
              end,
              Req, json, cred_validators())
    end.

handle_put(IdStr, Req) ->
    Author = get_author(Req),
    validator:handle(
      fun (Props) ->
              Type      = proplists:get_value(type, Props),
              Fields    = validated_fields_to_store(
                            Type, proplists:get_value(fields, Props)),
              MetaExtra = validated_meta_extra(Props),
              case cb_credentials_store:update(IdStr, Type, Fields,
                                               MetaExtra, Author) of
                  {ok, Cred} ->
                      reply_json_ok(Req,
                                    encode_response(export_credential(Cred)),
                                    200);
                  {error, not_found} ->
                      menelaus_util:reply_not_found(Req);
                  {error, Reason} ->
                      reply_store_error(Req, Reason)
              end
      end,
      Req, json, cred_validators()).

handle_delete(IdStr, Req) ->
    case cb_credentials_store:delete(IdStr) of
        ok ->
            menelaus_util:reply(Req, 200);
        {error, not_found} ->
            menelaus_util:reply_not_found(Req);
        {error, Reason} ->
            reply_store_error(Req, Reason)
    end.

reply_json_ok(Req, JsonBin, Code) ->
    menelaus_util:reply(Req, JsonBin, Code,
                        [{"Content-Type", "application/json"}]).

%% Credential request validation

cred_validators() ->
    [validator:required(type, _),
     validator:one_of(type, ?CREDENTIAL_TYPES, _),
     validator:convert(type, fun binary_to_existing_atom/1, _),
     validator:required(fields, _),
     %% Field validators are determined by type; we use a 2-arity validate
     %% callback so we can read the already-validated `type` from the state
     %% and dispatch to the type-specific field validators.
     validator:validate(
       fun (Fields, State) ->
               Type = validator:get_value(type, State),
               FieldValidators = cb_credential_types:fields_validators(Type),
               case validator:validate_decoded_object(Fields,
                                                      FieldValidators) of
                   {value, Validated} -> {value, Validated, State};
                   {error, Err}      -> {error, Err, State}
               end
       end, fields, _),
     validator:non_empty_string(description, _),
     validator:integer(expiresAt, 0, max_uint64, _),
     validate_expiry_in_future(expiresAt, _),
     validator:decoded_json(guardrails, guardrails_validators(), _),
     validator:unsupported(_)].

validate_expiry_in_future(Name, State) ->
    validator:validate(
      fun (V) ->
              Now = os:system_time(millisecond),
              case V > Now of
                  true  -> ok;
                  false ->
                      {error,
                       "expiresAt must be a timestamp in "
                       "the future"}
              end
      end, Name, State).

validated_meta_extra(Props) ->
    lists:foldl(
      fun ({expiresAt, V}, Acc) -> Acc#{expires_at => V};
          ({description, V}, Acc) -> Acc#{description => V};
          ({guardrails, V}, Acc) ->
              Acc#{guardrails => validated_guardrails_to_store(V)};
          (_, Acc) -> Acc
      end, #{}, Props).

%% @doc Validators for the optional guardrails sub-object.
%%
%% Most guardrail fields are optional arrays of strings.  allowedServices
%% is additionally constrained to known service names (kv, n1ql, index, ...).
%%
%% urlWhitelist is an optional sub-object with the following fields:
%%   allAccess      – boolean (default false); when true, all URLs are allowed
%%   allowedUrls    – array of URL strings (validated as proper URLs)
%%   disallowedUrls – array of URL strings (validated as proper URLs)
guardrails_validators() ->
    ConvertArray = fun (L) -> [list_to_binary(S) || S <- L] end,
    ServiceNames = [atom_to_list(S)
                    || S <- ns_cluster_membership:supported_services()],
    [validator:string_array(allowedServices,
                            fun (S) ->
                                    case lists:member(S, ServiceNames) of
                                        true  -> ok;
                                        false ->
                                            {error,
                                             io_lib:format(
                                               "Unknown service: ~s. "
                                               "Valid services: ~s",
                                               [S, lists:join(", ",
                                                              ServiceNames)])}
                                    end
                            end, false, _),
     validator:convert(allowedServices, ConvertArray, _),
     validator:decoded_json(urlWhitelist,
                            url_whitelist_validators(), _),
     validator:string_array(allowedResources,
                            fun (_) -> ok end, false, _),
     validator:convert(allowedResources, ConvertArray, _),
     validator:string_array(allowedOperations,
                            fun (_) -> ok end, false, _),
     validator:convert(allowedOperations, ConvertArray, _),
     validator:unsupported(_)].

%% @doc Validators for the urlWhitelist sub-object inside guardrails.
%%
%% Fields (all optional):
%%   allAccess      – boolean; when true, all URLs are permitted
%%   allowedUrls    – array of http/https URL strings
%%   disallowedUrls – array of http/https URL strings
url_whitelist_validators() ->
    UrlValidatorFun =
        fun (S) ->
                Validation =
                    fun (Scheme) ->
                            case lists:member(Scheme,
                                              [<<"http">>, <<"https">>]) of
                                true -> valid;
                                false -> {error, invalid_scheme}
                            end
                    end,
                case misc:parse_url(S,
                                    [{scheme_validation_fun, Validation}]) of
                    {ok, _} -> ok;
                    {error, _} -> {error, "Invalid URL"}
                end
        end,
    ConvertUrlArray = fun (L) -> [list_to_binary(U) || U <- L] end,
    [validator:has_params(_),
     validator:boolean(allAccess, _),
     validator:string_array(allowedUrls,
                            UrlValidatorFun, false, _),
     validator:convert(allowedUrls, ConvertUrlArray, _),
     validator:string_array(disallowedUrls,
                            UrlValidatorFun, false, _),
     validator:convert(disallowedUrls, ConvertUrlArray, _),
     validator:unsupported(_)].

%% @doc Convert validated guardrails proplist (camelCase atom keys) to the
%% storage map (snake_case atom keys).  Empty arrays are rejected at
%% validation time.  urlWhitelist is converted to a nested map with
%% snake_case keys.
validated_guardrails_to_store(GuardrailProps) ->
    Mapping = [{allowedServices, allowed_services},
               {allowedResources, allowed_resources},
               {allowedOperations, allowed_operations}],
    Base = lists:foldl(
             fun ({RestKey, StorageKey}, Acc) ->
                     case proplists:get_value(RestKey, GuardrailProps) of
                         undefined -> Acc;
                         Value     -> Acc#{StorageKey => Value}
                     end
             end, #{}, Mapping),
    case proplists:get_value(urlWhitelist, GuardrailProps) of
        undefined -> Base;
        WhitelistProps ->
            Base#{url_whitelist =>
                      validated_url_whitelist_to_store(
                        WhitelistProps)}
    end.

%% @doc Convert validated urlWhitelist proplist to storage map.
validated_url_whitelist_to_store(Props) ->
    Mapping = [{allAccess, all_access},
               {allowedUrls, allowed_urls},
               {disallowedUrls, disallowed_urls}],
    lists:foldl(
      fun ({RestKey, StorageKey}, Acc) ->
              case proplists:get_value(RestKey, Props) of
                  undefined -> Acc;
                  Value     -> Acc#{StorageKey => Value}
              end
      end, #{}, Mapping).

%% Convert validated fields proplist from decoded_json (camelCase list keys,
%% list string values) to the store's internal map (snake_case atom keys).
%% Delegates to the central type registry.
validated_fields_to_store(Type, FieldsProplist) ->
    cb_credential_types:validated_fields_to_store(Type, FieldsProplist).

%% Store internal map -> wire JSON map (camelCase, secrets already stripped
%% by store).  Output uses maps with binary keys for json:encode.
export_credential(#{id := Id, schema_version := SV, type := Type,
                    meta := Meta, fields := Fields}) ->
    #{<<"id">>            => ensure_binary(Id),
      <<"type">>          => atom_to_binary(Type),
      <<"schemaVersion">> => SV,
      <<"meta">>          => export_meta(Meta),
      <<"fields">>        => export_fields(Type, Fields)}.

export_meta(#{created_at := CA, created_by := CB} = Meta) ->
    Base = #{<<"createdAt">> => CA, <<"createdBy">> => export_author(CB)},
    WithUpdated = case Meta of
                      #{updated_at := UA, updated_by := UB} ->
                          Base#{<<"updatedAt">> => UA,
                                <<"updatedBy">> => export_author(UB)};
                      _ ->
                          Base
                  end,
    WithExpiry = case maps:find(expires_at, Meta) of
                     {ok, EA} -> WithUpdated#{<<"expiresAt">> => EA};
                     error    -> WithUpdated
                 end,
    WithDesc = case maps:find(description, Meta) of
                   {ok, Desc} -> WithExpiry#{<<"description">> =>
                                                 ensure_binary(Desc)};
                   error      -> WithExpiry
               end,
    WithGuardrails = case maps:find(guardrails, Meta) of
                         {ok, Guardrails} ->
                             WithDesc#{<<"guardrails">> =>
                                           export_guardrails(Guardrails)};
                         error ->
                             WithDesc
                     end,
    {ok, Rev} = maps:find(payload_version, Meta),
    WithGuardrails#{<<"payloadVersion">> => rev_to_binary(Rev)}.

export_author(#{user := User, domain := Domain}) ->
    #{<<"user">> => ensure_binary(User),
      <<"domain">> => atom_to_binary(Domain)}.

%% @doc Convert stored guardrails map (snake_case atom keys) to wire format
%% (camelCase binary keys).  Iterates over stored data so unknown keys
%% crash immediately.  url_whitelist is exported as a nested JSON object.
export_guardrails(Guardrails) ->
    Mapping = #{allowed_services => <<"allowedServices">>,
                allowed_resources => <<"allowedResources">>,
                allowed_operations => <<"allowedOperations">>},
    maps:fold(
      fun (url_whitelist, WL, Acc) ->
              Acc#{<<"urlWhitelist">> =>
                       export_url_whitelist(WL)};
          (StorageKey, Value, Acc) ->
              {ok, RestKey} = maps:find(StorageKey, Mapping),
              Acc#{RestKey => Value}
      end, #{}, Guardrails).

%% @doc Convert stored url_whitelist map to wire format.
export_url_whitelist(WL) ->
    Mapping = #{all_access => <<"allAccess">>,
                allowed_urls => <<"allowedUrls">>,
                disallowed_urls => <<"disallowedUrls">>},
    maps:fold(
      fun (StorageKey, Value, Acc) ->
              {ok, RestKey} = maps:find(StorageKey, Mapping),
              Acc#{RestKey => Value}
      end, #{}, WL).

%% Delegates field export to the central type registry.
export_fields(Type, Fields) ->
    cb_credential_types:export_fields(Type, Fields).

ensure_binary(V) when is_binary(V) -> V;
ensure_binary(V) when is_list(V)   -> list_to_binary(V);
ensure_binary(V) when is_atom(V)   -> atom_to_binary(V).

%% @doc Serialise an opaque chronicle revision to a JSON-safe binary string.
%% The revision is an Erlang term; we use term_to_binary + base64 so it
%% round-trips safely and remains opaque to API clients.
rev_to_binary(Rev) ->
    base64:encode(term_to_binary(Rev)).

get_author(Req) ->
    case menelaus_auth:get_identity(Req) of
        {User, Domain} -> #{user => iolist_to_binary(User), domain => Domain};
        undefined      -> #{user => <<"unknown">>, domain => local}
    end.

reply_store_error(Req, config_encryption_required) ->
    reply_json_ok(Req,
                  encode_response(
                    #{error => <<"Credential store requires config encryption "
                                 "at rest to be enabled, or "
                                 "configEncryptionOverride to be set in "
                                 "/settings/credentialStore">>}),
                  400);
reply_store_error(Req, n2n_encryption_required) ->
    reply_json_ok(Req,
                  encode_response(
                    #{error => <<"Credential store requires node-to-node "
                                 "encryption to be enabled on all nodes, or "
                                 "n2nEncryptionOverride to be set in "
                                 "/settings/credentialStore">>}),
                  400);
reply_store_error(Req, already_expired) ->
    reply_json_ok(Req,
                  encode_response(
                    #{error => <<"expiresAt must be a timestamp "
                                 "in the future">>}),
                  400);
reply_store_error(Req, {txn_failed, Reason}) ->
    ?log_error("Credential store transaction failed: ~p", [Reason]),
    reply_json_ok(Req,
                  encode_response(#{error => <<"Internal store error">>}), 500);
reply_store_error(Req, Reason) ->
    ?log_error("Credential store unexpected error: ~p", [Reason]),
    reply_json_ok(Req,
                  encode_response(#{error => <<"Internal error">>}), 500).

-ifdef(TEST).
roundtrip_test() ->
    Settings = #{config_encryption_override => false,
                 n2n_encryption_override => false},
    RestFormat = storage_to_rest_format(Settings),
    ?assertEqual(false, maps:get(configEncryptionOverride, RestFormat)),
    ?assertEqual(false, maps:get(n2nEncryptionOverride, RestFormat)).

export_credential_test() ->
    Cred = #{id             => <<"backup/aws/prod">>,
             schema_version => 1,
             type           => aws,
             meta           => #{created_at => 1740000000000,
                                 created_by => #{user => <<"Administrator">>,
                                                 domain => local},
                                 updated_at => 1740000000000,
                                 updated_by => #{user => <<"Administrator">>,
                                                 domain => local},
                                 payload_version => <<"abddefsdf">>},
             fields         => #{access_key_id => <<"AKIA">>,
                                 region        => <<"us-east-1">>}},
    Got = export_credential(Cred),
    ?assertEqual(<<"backup/aws/prod">>, maps:get(<<"id">>, Got)),
    ?assertEqual(<<"aws">>,             maps:get(<<"type">>, Got)),
    ?assertEqual(1,                     maps:get(<<"schemaVersion">>, Got)),
    Fields = maps:get(<<"fields">>, Got),
    ?assertEqual(<<"AKIA">>,      maps:get(<<"accessKeyId">>, Fields)),
    ?assertEqual(<<"us-east-1">>, maps:get(<<"region">>, Fields)).

export_guardrails_test() ->
    Guardrails = #{allowed_services => [<<"n1ql">>, <<"fts">>],
                   url_whitelist => #{all_access => false,
                                      allowed_urls =>
                                          [<<"https://api.stripe.com/*">>],
                                      disallowed_urls =>
                                          [<<"https://evil.com">>]},
                   allowed_operations => [<<"READ">>]},
    Got = export_guardrails(Guardrails),
    ?assertEqual([<<"n1ql">>, <<"fts">>],
                 maps:get(<<"allowedServices">>, Got)),
    ?assert(maps:is_key(<<"urlWhitelist">>, Got)),
    WL = maps:get(<<"urlWhitelist">>, Got),
    ?assertEqual(false, maps:get(<<"allAccess">>, WL)),
    ?assertEqual([<<"https://api.stripe.com/*">>],
                 maps:get(<<"allowedUrls">>, WL)),
    ?assertEqual([<<"https://evil.com">>],
                 maps:get(<<"disallowedUrls">>, WL)),
    ?assertEqual([<<"READ">>],
                 maps:get(<<"allowedOperations">>, Got)),
    %% allowed_resources was not set, so it must be absent
    ?assertNot(maps:is_key(<<"allowedResources">>, Got)).

export_guardrails_empty_test() ->
    ?assertEqual(#{}, export_guardrails(#{})).

export_meta_with_guardrails_test() ->
    Meta = #{created_at => 1740000000000,
             created_by => #{user => <<"admin">>, domain => local},
             guardrails => #{allowed_services => [<<"n1ql">>]},
             payload_version => <<"23423">>},
    Got = export_meta(Meta),
    ?assert(maps:is_key(<<"guardrails">>, Got)),
    GR = maps:get(<<"guardrails">>, Got),
    ?assertEqual([<<"n1ql">>], maps:get(<<"allowedServices">>, GR)).

export_meta_without_guardrails_test() ->
    Meta = #{created_at => 1740000000000,
             created_by => #{user => <<"admin">>, domain => local},
             payload_version => <<"12312">>},
    Got = export_meta(Meta),
    ?assertNot(maps:is_key(<<"guardrails">>, Got)).

validated_guardrails_to_store_test() ->
    Props = [{allowedServices, [<<"index">>, <<"n1ql">>]},
             {urlWhitelist, [{allAccess, false},
                             {allowedUrls, [<<"https://example.com">>]},
                             {disallowedUrls, [<<"https://bad.com">>]}]},
             {allowedOperations, [<<"READ">>, <<"LIST">>]}],
    Got = validated_guardrails_to_store(Props),
    ?assertEqual([<<"index">>, <<"n1ql">>],
                 maps:get(allowed_services, Got)),
    ?assert(maps:is_key(url_whitelist, Got)),
    WL = maps:get(url_whitelist, Got),
    ?assertEqual(false, maps:get(all_access, WL)),
    ?assertEqual([<<"https://example.com">>],
                 maps:get(allowed_urls, WL)),
    ?assertEqual([<<"https://bad.com">>],
                 maps:get(disallowed_urls, WL)),
    ?assertEqual([<<"READ">>, <<"LIST">>],
                 maps:get(allowed_operations, Got)),
    %% allowedResources was not present in Props, so must be absent
    ?assertNot(maps:is_key(allowed_resources, Got)).

-endif.
