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
%%      PUT    /settings/credentials/:id       -> full replace (rotate material)
%%      PATCH  /settings/credentials/:id       -> partial metadata update
%%                                                (description, expiresAt,
%%                                                 guardrails)
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
-include("cb_cluster_secrets.hrl").
-include_lib("ns_common/include/cut.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type credential_audit_event() :: create_credential | list_credentials |
                                  read_credential | update_credential |
                                  delete_credential | consume_credential.

-type credential_audit_args() ::
        {credential_id(), credential_type(),
         credential_public_view() | undefined} | % create
        {string(), non_neg_integer()} |  % list
        {credential_id(), credential_type() | undefined} | % read
        {credential_id(), credential_public_view() | undefined} | % update
        credential_id() | % delete
        {credential_id(), atom(), string(), atom()}. % consume

-export([handle_settings/2]).

%% Credential CRUD endpoints
-export([handle_list/1,
         handle_get/2,
         handle_post/2,
         handle_put/2,
         handle_patch/2,
         handle_delete/2,
         handle_get_credential_for_cbauth/2]).

-export([sanitize_chronicle_cfg/1]).

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

get_settings(Snapshot) ->
    Stored =
        case maps:find(?CREDENTIAL_STORE_SETTINGS_KEY, Snapshot) of
            {ok, {Val, _Rev}} -> Val;
            error -> #{}
        end,
    maps:merge(defaults(), Stored).

handle_settings_get(Req) ->
    Keys = [?CREDENTIAL_STORE_SETTINGS_KEY, ?CREDENTIAL_IDS_KEY,
            ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY],
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, Keys),
    Settings = get_settings(Snapshot),
    RestFormat = storage_to_rest_format(Settings),
    Warnings = cb_credentials_store:get_credential_warnings(Snapshot),
    Result = case Warnings of
                 [] -> RestFormat;
                 _  -> RestFormat#{warnings => Warnings}
             end,
    JsonBin = encode_response(Result),
    menelaus_util:reply(Req, JsonBin, 200,
                        [{"Content-Type", "application/json"}]).

handle_settings_put(Req) ->
    validator:handle(
      fun (Props) ->
              validate_and_store_settings(Props, Req)
      end,
      Req, json, settings_validators()).

handle_settings_delete(Req) ->
    Fun = fun (_) -> {commit, [{delete, ?CREDENTIAL_STORE_SETTINGS_KEY}]} end,
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
    Fun = fun (_) ->
                  {commit, [{set, ?CREDENTIAL_STORE_SETTINGS_KEY, Settings}]}
          end,
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

%% Minimum time (ms) that expiresAt must be in the future.
%% Prevents credentials that expire before they can be meaningfully consumed.
-define(MIN_EXPIRY_PERIOD_MS, 300000). %% 5 minutes

%% Audit helpers

-spec audit_credential(term(), credential_audit_event(),
                       credential_audit_args(),
                       ok | credential_error_reason()) -> ok.
audit_credential(Req, EventName, Args, Error) ->
    Params = format_audit_params(EventName, Args, Error),
    ns_audit:credential_event(Req, EventName, Params).

%% @doc Format audit event parameters based on the event type.
%% Each clause handles a specific credential_audit_event().
-spec format_audit_params(credential_audit_event(),
                          credential_audit_args(),
                          ok | credential_error_reason()) ->
          [{atom(), term()}].

format_audit_params(create_credential, {Id, Type, Cred}, Error) ->
    Meta = case Cred of
               #{meta := M} -> M;
               undefined    -> #{}
           end,
    [{id,         list_to_binary(Id)},
     {type,       Type},
     {created_at, maps:get(created_at, Meta, undefined)},
     {created_by, format_identity(maps:get(created_by, Meta, undefined))}]
        ++ optional_meta(Meta)
        ++ format_error(Error);

format_audit_params(list_credentials, {Prefix, Count}, Error) ->
    prefix_param(Prefix)
        ++ case Error of
               ok -> [{count, Count}];
               _ -> []
           end
        ++ format_error(Error);

format_audit_params(read_credential, {Id, Type}, Error) ->
    [{id, list_to_binary(Id)}]
        ++ case Type of
               undefined -> [];
               _ -> [{type, Type}]
           end
        ++ format_error(Error);

format_audit_params(update_credential, {Id, Cred}, Error) ->
    MetaParams =
        case {Error, Cred} of
            {ok, #{meta := Meta, type := Type}} ->
                [{type,       Type},
                 {created_at, maps:get(created_at, Meta, undefined)},
                 {created_by, format_identity(
                                maps:get(created_by, Meta, undefined))},
                 {updated_at, maps:get(updated_at, Meta, undefined)},
                 {updated_by, format_identity(
                                maps:get(updated_by, Meta, undefined))}]
                    ++ optional_meta(Meta);
            _ ->
                []
        end,
    [{id, list_to_binary(Id)}] ++ MetaParams ++ format_error(Error);

format_audit_params(delete_credential, Id, Error) ->
    [{id, list_to_binary(Id)}] ++ format_error(Error);

format_audit_params(consume_credential, {Id, Service, User, Domain}, Error) ->
    [{id, list_to_binary(Id)},
     {service, atom_to_binary(Service)},
     {on_behalf_of, format_identity(
                      #{user => list_to_binary(User), domain => Domain})}]
        ++ format_error(Error).

%% Convert an author() map to the ejson identity shape used in audit events.
%% Delegates to ns_audit:get_identity/1 which handles domain conversion
%% (e.g. admin → builtin) and user binary formatting.
-spec format_identity(credential_author() | undefined) ->
          undefined | {[{atom(), term()}]}.
format_identity(undefined) ->
    undefined;
format_identity(#{user := User, domain := Domain}) ->
    ns_audit:get_identity({User, Domain}).

%% Optional metadata fields shared by create/update.
-spec optional_meta(map()) -> [{atom(), term()}].
optional_meta(Meta) ->
    [{K, V} || {K, V} <-
                   [{expires_at,  maps:get(expires_at,  Meta, undefined)},
                    {description, maps:get(description, Meta, undefined)}],
               V =/= undefined].

%% Prefix param for list.
-spec prefix_param(string()) -> [{prefix, binary()}].
prefix_param("") -> [];
prefix_param(P)  -> [{prefix, list_to_binary(P)}].

%% Error field — absent on success, human-readable binary on failure.
-spec format_error(ok | credential_error_reason()) ->
          [{error, binary()}].
format_error(ok) -> [];
format_error(Reason) when is_atom(Reason) ->
    [{error, atom_to_binary(Reason)}];
format_error({txn_failed, Detail}) ->
    [{error, iolist_to_binary(["txn_failed: ",
                               io_lib:format("~p", [Detail])])}].

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
                {ok, Creds, Warnings} ->
                    audit_credential(Req, list_credentials,
                                     {Prefix, length(Creds)}, ok),
                    Credentials =
                        #{credentials => [export_credential(C) || C <- Creds]},
                    Result = case Warnings of
                                 [] -> Credentials;
                                 _ -> Credentials#{warnings => Warnings}
                             end,
                    JsonBin = encode_response(Result),
                    reply_json_ok(Req, JsonBin, 200);
                {error, Reason2} ->
                    audit_credential(Req, list_credentials,
                                     {Prefix, 0}, Reason2),
                    reply_store_error(Req, Reason2)
            end
    end.

handle_get(IdStr, Req) ->
    case cb_credentials_store:get(IdStr) of
        {ok, #{type := Type} = Cred} ->
            audit_credential(Req, read_credential, {IdStr, Type}, ok),
            reply_json_ok(Req, encode_response(export_credential(Cred)), 200);
        {error, not_found} ->
            audit_credential(Req, read_credential, {IdStr, undefined},
                             not_found),
            menelaus_util:reply_not_found(Req);
        {error, Reason} ->
            audit_credential(Req, read_credential, {IdStr, undefined}, Reason),
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
                              audit_credential(Req, create_credential,
                                               {IdStr, Type, Cred}, ok),
                              reply_json_ok(Req,
                                            encode_response(
                                              export_credential(Cred)), 201);
                          {error, already_exists} ->
                              audit_credential(Req, create_credential,
                                               {IdStr, Type, undefined},
                                               already_exists),
                              reply_json_ok(Req,
                                            encode_response(
                                              #{error => <<"Credential already "
                                                           "exists">>}),
                                            409);
                          {error, Reason2} ->
                              audit_credential(Req, create_credential,
                                               {IdStr, Type, undefined},
                                               Reason2),
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
              reply_update_result(
                IdStr,
                cb_credentials_store:update(IdStr, Type, Fields,
                                            MetaExtra, Author),
                Req)
      end,
      Req, json, cred_validators()).

%% @doc Partial update of an existing credential's metadata.
%% Accepts only description, expiresAt, and guardrails — never type or fields.
%% Omitted keys are preserved; an empty body is rejected with 400.
%% To rotate credential material, use PUT.
handle_patch(IdStr, Req) ->
    Author = get_author(Req),
    validator:handle(
      fun (Props) ->
              MetaExtra = validated_meta_extra(Props),
              case maps:size(MetaExtra) of
                  0 ->
                      menelaus_util:reply_json(
                        Req,
                        {[{error,
                           <<"At least one of description, expiresAt, "
                             "guardrails must be provided">>}]}, 400);
                  _ ->
                      reply_update_result(
                        IdStr,
                        cb_credentials_store:update_meta(
                          IdStr, MetaExtra, Author),
                        Req)
              end
      end,
      Req, json, patch_validators()).

%% @doc Shared response handling for the update_credential audit event,
%% used by both PUT (full replace) and PATCH (partial metadata update).
reply_update_result(IdStr, Result, Req) ->
    case Result of
        {ok, Cred} ->
            audit_credential(Req, update_credential, {IdStr, Cred}, ok),
            reply_json_ok(Req, encode_response(export_credential(Cred)),
                          200);
        {error, not_found} ->
            audit_credential(Req, update_credential, {IdStr, undefined},
                             not_found),
            menelaus_util:reply_not_found(Req);
        {error, Reason} ->
            audit_credential(Req, update_credential, {IdStr, undefined},
                             Reason),
            reply_store_error(Req, Reason)
    end.

handle_delete(IdStr, Req) ->
    case cb_credentials_store:delete(IdStr) of
        ok ->
            menelaus_users:cleanup_credential_roles(IdStr),
            audit_credential(Req, delete_credential, IdStr, ok),
            menelaus_util:reply(Req, 200);
        {error, not_found} ->
            audit_credential(Req, delete_credential, IdStr, not_found),
            menelaus_util:reply_not_found(Req);
        {error, Reason} ->
            audit_credential(Req, delete_credential, IdStr, Reason),
            reply_store_error(Req, Reason)
    end.

%% @doc Internal cbauth endpoint: GET /_cbauth/getCredential/<id>
%%
%% Called by Go services via cbauth's Creds.GetCredential(id). The service
%% authenticates using its own identity (e.g. @cbq-engine, @fts, @backup) and
%% passes the end-user identity in query parameters:
%%   ?user=<user>&domain=<domain>&extras=<base64-encoded extras>
%%
%% Enforces:
%%   1. Service-identity binding — when the on-behalf-of user is a service
%%      identity (@-prefixed), it must match the authenticated caller.
%%   2. RBAC — the on-behalf-of user must have `consume` permission on
%%      `{credentials, Id}`.
%%   3. Expiry — credentials past their `expires_at` are rejected (enforced by
%%      cb_credentials_store:consume_credential/1).
%%   4. a. Service user — bypass guardrails.
%%      b. End user — the calling service must be explicitly listed in the
%%         credential's `allowed_services` guardrail.
handle_get_credential_for_cbauth(IdStr, Req) ->
    Params = mochiweb_request:parse_qs(Req),
    User = proplists:get_value("user", Params),
    Domain = list_to_existing_atom(proplists:get_value("domain", Params)),
    Extras = proplists:get_value("extras", Params, undefined),
    OnBehalf = {User, Domain},
    Service = service_from_identity(Req),
    case check_caller_identity(Req, Service, OnBehalf) of
        ok ->
            AuthnRes =
                menelaus_auth:get_authn_res_from_on_behalf_of(
                  User, Domain, Extras),
            case check_consume_permission(IdStr, AuthnRes) of
                true ->
                    handle_consume_credential(
                      IdStr, Service, User, Domain, Req);
                false ->
                    audit_consume(Req, IdStr, Service,
                                  User, Domain, access_denied),
                    reply_cbauth_error(
                      Req,
                      <<"INSUFFICIENT_PERMISSIONS">>,
                      <<"Access denied: insufficient permissions to consume "
                        "this credential">>, 403)
            end;
        {error, Reason} ->
            audit_consume(Req, IdStr, Service, User, Domain, access_denied),
            reply_cbauth_error(Req, <<"INSUFFICIENT_PERMISSIONS">>, Reason, 403)
    end.

%% @doc Validate the calling service identity.
%%
%% Two checks, both of which must pass:
%%   1. The caller must map to a known service (not `unknown`).
%%   2. When the on-behalf-of identity is a service identity (@-prefixed), it
%%      must match the authenticated caller.
-spec check_caller_identity(term(), atom(), {string(), atom()}) ->
          ok | {error, binary()}.
check_caller_identity(_Req, unknown, _OnBehalf) ->
    {error, <<"Access denied: unknown service">>};
check_caller_identity(Req, _Service, OnBehalf) ->
    case is_service_identity(OnBehalf) of
        false ->
            ok;
        true ->
            case menelaus_auth:get_identity(Req) =:= OnBehalf of
                true ->
                    ok;
                false ->
                    {error,
                     <<"Access denied: on-behalf-of service identity does not "
                       "match authenticated caller">>}
            end
    end.

%% @doc Second stage: call consume (which checks expiry), then bifurcate based
%% on whether the on-behalf-of identity is a service user or an end user.
%%
%% Service users: bypass guardrails.
%% End users: enforce the allowedServices guardrail.
handle_consume_credential(IdStr, Service, User, Domain, Req) ->
    case cb_credentials_store:consume_credential(IdStr) of
        {ok, Cred} ->
            OnBehalf = {User, Domain},
            case is_service_identity(OnBehalf) of
                true ->
                    audit_consume(Req, IdStr, Service, User, Domain, ok),
                    reply_json_ok(Req, encode_response(
                                         export_credential(Cred)), 200);
                false ->
                    handle_end_user_consume(IdStr, Service, User, Domain, Cred,
                                            Req)
            end;
        {error, not_found} ->
            audit_consume(Req, IdStr, Service, User, Domain, not_found),
            menelaus_util:reply_not_found(Req);
        {error, expired} ->
            audit_consume(Req, IdStr, Service, User, Domain, expired),
            reply_cbauth_error(
              Req, <<"CREDENTIAL_EXPIRED">>, <<"Credential has expired">>, 403);
        {error, unsupported_schema_version} ->
            audit_consume(Req, IdStr, Service, User, Domain,
                          unsupported_schema_version),
            reply_cbauth_error(
              Req, <<"UNSUPPORTED_SCHEMA_VERSION">>,
              <<"Credential uses an unsupported schema version">>, 503);
        {error, Reason} ->
            audit_consume(Req, IdStr, Service, User, Domain, Reason),
            reply_store_error(Req, Reason)
    end.

%% @doc Handle consume for an end user: enforce the allowedServices guardrail.
handle_end_user_consume(IdStr, Service, User, Domain, Cred, Req) ->
    case check_service_guardrail(Service, Cred) of
        ok ->
            audit_consume(Req, IdStr, Service, User, Domain, ok),
            reply_json_ok(Req, encode_response(export_credential(Cred)), 200);
        {error, service_not_allowed} ->
            audit_consume(Req, IdStr, Service, User, Domain,
                          service_not_allowed),
            reply_cbauth_error(
              Req,
              <<"SERVICE_GUARDRAIL_BLOCKED">>,
              <<"Access denied: service not listed in credential's "
                "allowedServices guardrail">>, 403)
    end.

%% @doc Derive the calling service from the authenticated identity on the
%% request.  Services authenticate as @-prefixed internal users (e.g.
%% @cbq-engine, @backup, @fts) to access /_cbauth endpoints.  We map
%% that identity to the canonical service atom used in guardrails.
%%
%% Returns `unknown` when the identity is missing, not an @-prefixed user,
%% or does not map to a known service.
-spec service_from_identity(term()) -> atom().
service_from_identity(Req) ->
    case menelaus_auth:get_identity(Req) of
        {[$@ | Name], _Domain} ->
            misc:identity_name_to_service(Name);
        _ ->
            unknown
    end.

%% @doc Check if identity is a service identity (@ user in admin domain).
%% Service identities like @backup, @cbq-engine bypass guardrails.
%% Human admins like "Administrator" in admin domain do NOT bypass guardrails.
%% We check the on-behalf-of identity (from query params), not the caller's
%% identity from the request.
-spec is_service_identity({string(), atom()}) -> boolean().
is_service_identity({[$@ | _], admin}) -> true;
is_service_identity(_) -> false.

%% @doc Check that the on-behalf-of user has RBAC `consume` permission on
%% the requested credential.
-spec check_consume_permission(credential_id(), term()) -> boolean().
check_consume_permission(IdStr, AuthnRes) ->
    menelaus_roles:is_allowed(
      {[{credentials, IdStr}], consume}, AuthnRes).


%% @doc Check the `allowed_services` guardrail for end-users.
%%
%% The calling service must be explicitly listed in `allowed_services`.
%% If `allowed_services` is not set or empty, access is denied.
-spec check_service_guardrail(atom(), map()) ->
          ok | {error, service_not_allowed}.
check_service_guardrail(Service, #{meta := Meta}) ->
    case maps:find(guardrails, Meta) of
        {ok, #{allowed_services := AllowedBins}}
          when AllowedBins =/= [] ->
            ServiceBin = atom_to_binary(Service),
            case lists:member(ServiceBin, AllowedBins) of
                true  -> ok;
                false -> {error, service_not_allowed}
            end;
        _ ->
            {error, service_not_allowed}
    end.

%% @doc Audit a consume_credential event.  Called for both successes and
%% failures so that denied access is visible in the audit trail.
%% Includes both the calling service and the on-behalf-of user identity.
-spec audit_consume(term(), credential_id(), atom(), string(), atom(),
                    ok | credential_error_reason()) -> ok.
audit_consume(Req, IdStr, Service, User, Domain, Error) ->
    audit_credential(Req, consume_credential,
                     {IdStr, Service, User, Domain}, Error).

reply_json_ok(Req, JsonBin, Code) ->
    menelaus_util:reply(Req, JsonBin, Code,
                        [{"Content-Type", "application/json"}]).

reply_cbauth_error(Req, Code, Reason, HttpStatus) ->
    reply_json_ok(Req,
                  encode_response(
                    #{error => #{code => Code, reason => Reason}}),
                  HttpStatus).

%% Credential request validation

cred_validators() ->
    WireToAtom = maps:from_list(
                   [{atom_to_binary(misc:snake_to_camel_atom(T)), T}
                    || T <- ?CREDENTIAL_TYPES]),
    [validator:required(type, _),
     validator:one_of(type, maps:keys(WireToAtom), _),
     validator:convert(type, fun (B) -> maps:get(B, WireToAtom) end, _),
     validator:required(fields, _),
     %% Field validators are determined by type; we use a 2-arity validate
     %% callback so we can read the already-validated `type` from the state
     %% and dispatch to the type-specific field validators.
     validator:validate(
       fun (Fields, State) ->
               case validator:get_value(type, State) of
                   undefined ->
                       %% `type' is missing or invalid; the request will already
                       %% fail on the type error. Skip field validation.
                       {ok, State};
                   Type ->
                       FieldValidators =
                           cb_credential_types:fields_validators(Type),
                       case validator:validate_decoded_object(
                              Fields, FieldValidators) of
                           {value, Validated} -> {value, Validated, State};
                           {error, Err} -> {error, Err, State}
                       end
               end
       end, fields, _)]
        ++ [V || {_, V} <- meta_field_validators()]
        ++ [validator:unsupported(_)].

%% @doc Per-field validators for the optional meta fields, shared by
%% cred_validators/0 (POST/PUT) and patch_validators/0 (PATCH). Adding a new
%% constraint here covers all three endpoints automatically.
meta_field_validators() ->
    [{description, validator:non_empty_string(description, _)},
     {expiresAt,   validator:integer(expiresAt, 0, max_uint64, _)},
     {expiresAt,   validate_expiry_in_future(expiresAt, _)},
     {guardrails,  validator:decoded_json(guardrails,
                                          guardrails_validators(), _)}].

%% @doc Validators for PATCH — meta-only.  Type and fields are not
%% accepted.  All keys are optional; the handler rejects empty bodies
%% separately.
%%
%% JSON null on description/expiresAt/guardrails means "clear this field":
%% the `clear` atom flows through to the store, which removes the key
%% from the stored meta map.  Non-null values are validated by the same
%% validators POST/PUT use via meta_field_validators/0.
patch_validators() ->
    NullableKeys = [description, expiresAt, guardrails],
    [accept_null_as_clear(NullableKeys, _)]
        ++ [skip_if_clear(K, V, _) || {K, V} <- meta_field_validators()]
        ++ [validator:unsupported(_)].

%% @doc For each named key, replace JSON null with the `clear` sentinel.
%% Non-null values pass through unchanged; missing keys remain missing.
accept_null_as_clear(Keys, State) ->
    lists:foldl(
      fun (K, S) ->
              validator:validate(
                fun (null) -> {value, clear};
                    (_)    -> ok
                end, K, S)
      end, State, Keys).

%% @doc Run the wrapped validator only if the named key is not the
%% `clear` sentinel.  Lets PATCH reuse the standard validators without
%% them choking on `clear`.
skip_if_clear(Name, Validator, State) ->
    case validator:get_value(Name, State) of
        clear -> State;
        _     -> Validator(State)
    end.

validate_expiry_in_future(Name, State) ->
    validator:validate(
      fun (V) ->
              MinExpiry = os:system_time(millisecond) + ?MIN_EXPIRY_PERIOD_MS,
              case V >= MinExpiry of
                  true  -> ok;
                  false ->
                      {error,
                       "expiresAt must be at least 5 minutes in the future"}
              end
      end, Name, State).

%% Shared by POST, PUT, and PATCH.  The `clear` atom only ever arrives on
%% the PATCH path (patch_validators/0 emits it for explicit JSON null);
%% cred_validators/0 rejects null, so POST and PUT never produce `clear`
%% here.  Downstream, only build_patched_meta/4 in the store knows how to
%% drop `clear` keys — create_impl and build_updated_meta/4 must therefore
%% never receive it.
validated_meta_extra(Props) ->
    lists:foldl(
      fun ({expiresAt, V}, Acc) -> Acc#{expires_at => V};
          ({description, V}, Acc) -> Acc#{description => V};
          ({guardrails, clear}, Acc) -> Acc#{guardrails => clear};
          ({guardrails, V}, Acc) ->
              Acc#{guardrails => validated_guardrails_to_store(V)};
          (_, Acc) -> Acc
      end, #{}, Props).

%% @doc Validators for the optional guardrails sub-object.
%%
%% Most guardrail fields are optional arrays of strings. allowedServices
%% is constrained to services that consume credentials via cbauth
%% (n1ql, backup, index, xdcr, fts, eventing, cbas).
%%
%% urlWhitelist is an optional sub-object with the following fields:
%%   allAccess      – boolean (default false); when true, all URLs are allowed
%%   allowedUrls    – array of URL strings (validated as proper URLs)
%%   disallowedUrls – array of URL strings (validated as proper URLs)
guardrails_validators() ->
    ConvertArray = fun (L) -> [list_to_binary(S) || S <- L] end,
    ServiceNames = [atom_to_list(S) || S <- ?CREDENTIAL_CONSUMER_SERVICES],
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
      <<"type">>          => atom_to_binary(misc:snake_to_camel_atom(Type)),
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
reply_store_error(Req, {txn_failed, Reason}) ->
    ?log_error("Credential store transaction failed: ~p", [Reason]),
    reply_json_ok(Req,
                  encode_response(#{error => <<"Internal store error">>}), 500);
reply_store_error(Req, Reason) ->
    ?log_error("Credential store unexpected error: ~p", [Reason]),
    reply_json_ok(Req,
                  encode_response(#{error => <<"Internal error">>}), 500).

-spec sanitize_chronicle_cfg(credential_full_view()) -> credential_public_view().
sanitize_chronicle_cfg(#{type := Type, fields := Fields} = Cred) ->
    Sensitive = cb_credential_types:sensitive_fields(Type),
    Masked = chronicle_kv_log:masked(),
    SanitizedFields =
        maps:map(
          fun (K, V) ->
                  case lists:member(K, Sensitive) of
                      true  -> Masked;
                      false -> V
                  end
          end, Fields),
    Cred#{fields => SanitizedFields};
sanitize_chronicle_cfg(Value) ->
    Value.

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

sanitize_chronicle_cfg_test() ->
    Cred = #{type => aws,
             fields => #{access_key_id => <<"AK">>,
                         secret_access_key => <<"SK">>,
                         region => <<"us-east-1">>},
             meta => #{created_at => 0}},
    Sanitized = sanitize_chronicle_cfg(Cred),
    Expected = Cred#{fields =>
                         #{access_key_id => <<"AK">>,
                           secret_access_key =>
                               chronicle_kv_log:masked(),
                           region => <<"us-east-1">>}},
    ?assertEqual(Expected, Sanitized).
-endif.
