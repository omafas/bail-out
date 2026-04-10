%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% is governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

%% @doc Credential Store - Persistent storage for external credentials
%%
%% Stores credentials in chronicle. Key: {credentials, Id}
%% Stored credential map structure (schema_version = 1):
%%   #{
%%     id             => string(),
%%     schema_version => 1,
%%     type           => aws,
%%     meta           => #{created_at, created_by, updated_at, updated_by,
%%                         expires_at  => integer() (ms since epoch, optional),
%%                         description => string() (optional)},
%%     fields         => #{...type-specific fields, all plaintext...}
%%   }
%%

-module(cb_credentials_store).

-include("ns_common.hrl").
-include("credentials.hrl").
-include("cb_cluster_secrets.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([create/5,
         get/1,
         list/1,
         update/5,
         delete/1,
         consume_credential/1,
         get_credential_warnings/1,
         credentials_requiring_config_encryption/1,
         credentials_requiring_n2n_encryption/0]).

%% Internal helpers (exported for testing)
-export([build_key/1,
         redact_credential/1,
         sensitive_fields/1,
         ensure_prerequisites/1,
         is_expired/1]).

%% The subset of meta fields that callers may supply.
-define(USER_META_FIELDS, [expires_at, description, guardrails]).

-define(SCHEMA_VERSION, 1).

-define(PREREQ_KEYS, [?CREDENTIAL_STORE_SETTINGS_KEY,
                      ?CHRONICLE_ENCR_AT_REST_SETTINGS_KEY]).
-type user_meta_map() :: #{expires_at => integer(),
                           description => binary() | string(),
                           guardrails => map()}.

%% @doc Create a new credential.
%% Fails with {error, already_exists} if a credential with the same Id already
%% exists.
-spec create(credential_id(), credential_type(), credential_fields(),
             user_meta_map(), credential_author()) ->
          {ok, credential_public_view()} | {error, credential_error_reason()}.
create(Id, Type, Fields, MetaExtra, Author) ->
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, ?PREREQ_KEYS),
    case ensure_prerequisites(Snapshot) of
        ok ->
            create_impl(Id, Type, Fields, MetaExtra, Author);
        {error, _} = Err ->
            Err
    end.

%% @doc Get a credential by Id.  Returns the public (redacted) view.
-spec get(credential_id()) ->
          {ok, credential_public_view()} | {error, credential_error_reason()}.
get(Id) ->
    Key = build_key(Id),
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, [Key | ?PREREQ_KEYS]),
    case ensure_prerequisites(Snapshot) of
        ok ->
            get_impl(Id, Snapshot);
        {error, _} = Err ->
            Err
    end.

%% @doc List all credentials whose Id starts with Prefix.
%% Pass "" to list everything.
%%
%% Reads the credential_ids index to discover which credentials exist, filters
%% by prefix, then fetches only the matching credential keys via get_snapshot.
-spec list(string()) ->
          {ok, [credential_public_view()], [binary()]} |
          {error, credential_error_reason()}.
list(Prefix) ->
    IdxKeys = [?CREDENTIAL_IDS_KEY | ?PREREQ_KEYS],
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, IdxKeys),
    case ensure_prerequisites(Snapshot) of
        ok ->
            {ok, Creds} = list_impl(Prefix, Snapshot),
            Warnings = get_credential_warnings(Snapshot),
            {ok, Creds, Warnings};
        {error, _} = Err ->
            Err
    end.

%% @doc Replace an existing credential's fields.
%% The type must match the stored type.
-spec update(credential_id(), credential_type(), credential_fields(),
             user_meta_map(), credential_author()) ->
          {ok, credential_public_view()} | {error, credential_error_reason()}.
update(Id, Type, Fields, MetaExtra, Author) ->
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, ?PREREQ_KEYS),
    case ensure_prerequisites(Snapshot) of
        ok ->
            update_impl(Id, Type, Fields, MetaExtra, Author);
        {error, _} = Err ->
            Err
    end.

%% @doc Delete a credential by Id.
-spec delete(credential_id()) ->
          ok | {error, credential_error_reason()}.
delete(Id) ->
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, ?PREREQ_KEYS),
    case ensure_prerequisites(Snapshot) of
        ok ->
            delete_impl(Id);
        {error, _} = Err ->
            Err
    end.

%% @doc Retrieve the full (unredacted) credential for internal consumption by
%% services. Unlike get/1, this returns the complete fields map including the
%% plaintext sensitive field (e.g. secret_access_key).
%%
%% Returns {error, unsupported_schema_version} if the stored record was written
%% by a newer version of the server that this node does not yet understand,
%% preventing silent misuse of ciphertext as a plaintext value.
%%
%% Returns {error, expired} if the credential has an expires_at timestamp in
%% the past.
-spec consume_credential(credential_id()) ->
          {ok, credential_full_view()} | {error, credential_error_reason()}.
consume_credential(Id) ->
    Key = build_key(Id),
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, [Key | ?PREREQ_KEYS]),
    case ensure_prerequisites(Snapshot) of
        ok ->
            consume_credential_impl(Id, Snapshot);
        {error, _} = Err ->
            Err
    end.

-spec ensure_prerequisites(map()) -> ok | {error, credential_error_reason()}.
ensure_prerequisites(Snapshot) ->
    menelaus_util:assert_is_totoro(),
    menelaus_util:assert_is_enterprise(),
    maybe
        ok ?= ensure_config_encryption(Snapshot),
        ok ?= ensure_n2n_encryption(Snapshot)
    end.

-spec ensure_config_encryption(map()) -> ok |
          {error, config_encryption_required}.
ensure_config_encryption(Snapshot) ->
    case is_overridden(config_encryption_override, Snapshot) of
        true  -> ok;
        false ->
            case menelaus_web_encr_at_rest:is_encryption_enabled(
                   config_encryption, Snapshot) of
                true  -> ok;
                false -> {error, config_encryption_required}
            end
    end.

-spec ensure_n2n_encryption(map()) -> ok | {error, n2n_encryption_required}.
ensure_n2n_encryption(Snapshot) ->
    case is_overridden(n2n_encryption_override, Snapshot) of
        true  -> ok;
        false ->
            case misc:is_cluster_encryption_fully_enabled() of
                true  -> ok;
                false -> {error, n2n_encryption_required}
            end
    end.

-spec is_overridden(atom(), map()) -> boolean().
is_overridden(Flag, Snapshot) ->
    case maps:find(?CREDENTIAL_STORE_SETTINGS_KEY, Snapshot) of
        {ok, {Settings, _Rev}} -> maps:get(Flag, Settings, false);
        error                  -> false
    end.

-spec credentials_requiring_config_encryption(map()) -> [credential_id()].
credentials_requiring_config_encryption(Snapshot) ->
    credentials_unless_overridden(config_encryption_override, Snapshot).

-spec credentials_requiring_n2n_encryption() -> [credential_id()].
credentials_requiring_n2n_encryption() ->
    Keys = [?CREDENTIAL_IDS_KEY, ?CREDENTIAL_STORE_SETTINGS_KEY],
    {ok, {Snapshot, _}} = chronicle_kv:get_snapshot(kv, Keys),
    credentials_unless_overridden(n2n_encryption_override, Snapshot).

-spec credentials_unless_overridden(config_encryption_override |
                                    n2n_encryption_override, map()) ->
          [credential_id()].
credentials_unless_overridden(OverrideFlag, Snapshot) ->
    case is_overridden(OverrideFlag, Snapshot) of
        true  -> [];
        false -> get_index(Snapshot)
    end.

%% Internal functions

%% @doc Build the chronicle_kv key for a credential.
-spec build_key(string()) -> {credentials, string()}.
build_key(Id) ->
    {credentials, Id}.

%% @doc Extract the sorted set of credential IDs from a snapshot.
-spec get_index(map()) -> ordsets:ordset(credential_id()).
get_index(Snapshot) ->
    case maps:find(?CREDENTIAL_IDS_KEY, Snapshot) of
        {ok, {Ids, _Rev}} -> Ids;
        error -> []
    end.

%% @doc Stamp the chronicle revision into the credential's meta as
%% `payload_version`.  This distinguishes distinct payload contents for the same
%% credential id.
-spec with_version(map(), chronicle:revision()) -> map().
with_version(#{meta := Meta} = Cred, Rev) ->
    Cred#{meta => Meta#{payload_version => Rev}}.

%% @doc Return the set of sensitive (secret) field keys for a credential type.
%% Delegates to the central type registry.
-spec sensitive_fields(credential_type()) -> [atom()].
sensitive_fields(Type) -> cb_credential_types:sensitive_fields(Type).

-define(REDACTED, <<"********">>).

%% @doc Return the public (secret-redacted) view of a stored credential.
-spec redact_credential(credential_full_view()) -> credential_public_view().
redact_credential(#{id := Id, schema_version := SV, type := Type, meta := Meta,
                    fields := Fields}) ->
    Sensitive = sensitive_fields(Type),
    MaskedFields = maps:map(
                     fun (K, _V) ->
                             case lists:member(K, Sensitive) of
                                 true  -> ?REDACTED;
                                 false -> _V
                             end
                     end, Fields),
    #{id             => Id,
      schema_version => SV,
      type           => Type,
      meta           => Meta,
      fields         => MaskedFields}.

create_impl(Id, Type, Fields, MetaExtra, Author) ->
    Key  = build_key(Id),
    Now  = os:system_time(millisecond),
    BaseMeta = #{created_at => Now, created_by => Author},
    Meta = maps:merge(BaseMeta, maps:with(?USER_META_FIELDS, MetaExtra)),
    Cred = #{
             id             => Id,
             schema_version => ?SCHEMA_VERSION,
             type           => Type,
             meta           => Meta,
             fields         => Fields
            },
    Fun = fun (Snapshot) ->
                  case ensure_config_encryption(Snapshot) of
                      ok ->
                          case maps:find(Key, Snapshot) of
                              {ok, _} ->
                                  {abort, already_exists};
                              error ->
                                  Ids = get_index(Snapshot),
                                  NewIds = ordsets:add_element(Id, Ids),
                                  {commit,
                                   [{set, Key, Cred},
                                    {set, ?CREDENTIAL_IDS_KEY, NewIds}]}
                          end;
                      {error, Reason} ->
                          {abort, {prereq_failed, Reason}}
                  end
          end,
    TxnKeys = [Key, ?CREDENTIAL_IDS_KEY | ?PREREQ_KEYS],
    case chronicle_kv:transaction(kv, TxnKeys, Fun, #{}) of
        {ok, Rev} ->
            {ok, redact_credential(with_version(Cred, Rev))};
        already_exists ->
            {error, already_exists};
        {prereq_failed, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, {txn_failed, Reason}}
    end.

get_impl(Id, Snapshot) ->
    Key = build_key(Id),
    case maps:find(Key, Snapshot) of
        {ok, {Cred, Rev}} ->
            {ok, redact_credential(with_version(Cred, Rev))};
        error ->
            {error, not_found}
    end.

list_impl(Prefix, Snapshot) ->
    AllIds = get_index(Snapshot),
    Filtered = [Id || Id <- AllIds, matches_prefix(Id, Prefix)],
    case Filtered of
        [] ->
            {ok, []};
        _ ->
            CredKeys = [build_key(Id) || Id <- Filtered],
            {ok, {CredSnap, _}} = chronicle_kv:get_snapshot(kv, CredKeys),
            Creds =
                lists:filtermap(
                  fun (K) ->
                          case maps:find(K, CredSnap) of
                              {ok, {Cred, Rev}} ->
                                  {true, redact_credential(
                                           with_version(Cred, Rev))};
                              error ->
                                  false
                          end
                  end, CredKeys),
            Sorted = lists:sort(
                       fun (A, B) ->
                               maps:get(id, A) =< maps:get(id, B)
                       end, Creds),
            {ok, Sorted}
    end.

update_impl(Id, Type, Fields, MetaExtra, Author) ->
    Key = build_key(Id),
    Now = os:system_time(millisecond),
    Fun = fun (Snapshot) ->
                  case ensure_config_encryption(Snapshot) of
                      ok ->
                          case maps:find(Key, Snapshot) of
                              error ->
                                  {abort, not_found};
                              {ok, {Current, _Rev}} ->
                                  do_update(Key, Current, Type, Fields,
                                            MetaExtra, Author, Now)
                          end;
                      {error, Reason} ->
                          {abort, {prereq_failed, Reason}}
                  end
          end,
    case chronicle_kv:transaction(kv, [Key | ?PREREQ_KEYS], Fun, #{}) of
        {ok, Rev, Updated} ->
            {ok, redact_credential(with_version(Updated, Rev))};
        not_found ->
            {error, not_found};
        invalid_type ->
            {error, invalid_type};
        {prereq_failed, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, {txn_failed, Reason}}
    end.

do_update(Key, #{type := Type, meta := ExistingMeta} = Current,
          Type, Fields, MetaExtra, Author, Now) ->
    UpdatedMeta = build_updated_meta(ExistingMeta, MetaExtra, Author, Now),
    Updated = Current#{fields => Fields, meta  => UpdatedMeta},
    {commit, [{set, Key, Updated}], Updated};
do_update(_Key, _Current, _Type, _Fields, _MetaExtra, _Author, _Now) ->
    {abort, invalid_type}.

%% @doc Build the meta map for an update operation.
%%
%% Keeps immutable fields (created_at, created_by) from the existing
%% meta, merges in user-supplied optional fields (?USER_META_FIELDS),
%% and stamps the update timestamp and author.
build_updated_meta(ExistingMeta, MetaExtra, Author, Now) ->
    Immutable = maps:with([created_at, created_by], ExistingMeta),
    UserSupplied = maps:with(?USER_META_FIELDS, MetaExtra),
    ServerStamped = #{updated_at => Now, updated_by => Author},
    maps:merge(Immutable, maps:merge(UserSupplied, ServerStamped)).

delete_impl(Id) ->
    Key = build_key(Id),
    Fun = fun (Snapshot) ->
                  case ensure_config_encryption(Snapshot) of
                      ok ->
                          case maps:find(Key, Snapshot) of
                              error ->
                                  {abort, not_found};
                              {ok, _} ->
                                  Ids = get_index(Snapshot),
                                  NewIds = ordsets:del_element(Id, Ids),
                                  {commit,
                                   [{delete, Key},
                                    {set, ?CREDENTIAL_IDS_KEY, NewIds}]}
                          end;
                      {error, Reason} ->
                          {abort, {prereq_failed, Reason}}
                  end
          end,
    TxnKeys = [Key, ?CREDENTIAL_IDS_KEY | ?PREREQ_KEYS],
    case chronicle_kv:transaction(kv, TxnKeys, Fun, #{}) of
        {ok, _}                 -> ok;
        not_found               -> {error, not_found};
        {prereq_failed, Reason} -> {error, Reason};
        {error, Reason}         -> {error, {txn_failed, Reason}}
    end.

consume_credential_impl(Id, Snapshot) ->
    Key = build_key(Id),
    case maps:find(Key, Snapshot) of
        {ok, {#{schema_version := ?SCHEMA_VERSION, meta := Meta} = Cred,
              Rev}} ->
            case is_expired(Meta) of
                true ->
                    ?log_error("consume_credential: credential ~p has expired",
                               [Id]),
                    {error, expired};
                false ->
                    {ok, with_version(Cred, Rev)}
            end;
        {ok, {#{schema_version := SV}, _Rev}} when SV > ?SCHEMA_VERSION ->
            ?log_error("consume_credential: unsupported schema_version ~p for "
                       "credential ~p", [SV, Id]),
            {error, unsupported_schema_version};
        error ->
            {error, not_found}
    end.

-spec is_expired(map()) -> boolean().
is_expired(#{expires_at := ExpiresAt}) ->
    os:system_time(millisecond) > ExpiresAt;
is_expired(_Meta) ->
    false.

matches_prefix(_Id, "") ->
    true;
matches_prefix(Id, Prefix) ->
    lists:prefix(Prefix, Id).

%% @doc Compute credential warnings from a snapshot.
%% Returns warnings when credentials exist and config encryption
%% or n2n encryption is not enabled.
%% This chronicle snapshot contains credential_store_settings, credential_ids,
%% chronicle_encryption_at_rest_settings keys.
-spec get_credential_warnings(map()) -> [binary()].
get_credential_warnings(Snapshot) ->
    case get_index(Snapshot) of
        [] -> [];
        [_ | _] ->
            [W || {ok, W} <-
                      [config_encryption_warning(Snapshot),
                       n2n_encryption_warning()]]
    end.

config_encryption_warning(Snapshot) ->
    case menelaus_web_encr_at_rest:is_encryption_enabled(config_encryption,
                                                         Snapshot) of
        true ->
            undefined;
        false ->
            {ok,
             <<"Stored credentials are not protected by config encryption at "
               "rest">>}
    end.

n2n_encryption_warning() ->
    case misc:is_cluster_encryption_fully_enabled() of
        true ->
            undefined;
        false ->
            {ok,
             <<"Stored credentials risk being sent unencrypted unless "
               "node-to-node encryption is enabled on every node in the "
               "cluster">>}
    end.

-ifdef(TEST).

build_key_test() ->
    ?assertEqual({credentials, "test_id"}, build_key("test_id")).

sensitive_fields_test() ->
    ?assertEqual([secret_access_key, session_token], sensitive_fields(aws)).

redact_credential_test() ->
    Fields = #{access_key_id     => "AKIAIOSFODNN7EXAMPLE",
               secret_access_key => "SECRET_KEY",
               region            => "us-east-1",
               endpoint          => "https://s3.amazonaws.com"},
    Cred = #{id             => "test_aws",
             schema_version => ?SCHEMA_VERSION,
             type           => aws,
             meta           => #{created_at => 1234567890,
                                 created_by => #{user => <<"admin">>,
                                                 domain => local}},
             fields         => Fields},
    Redacted = redact_credential(Cred),
    ?assertEqual("test_aws", maps:get(id, Redacted)),
    ?assertEqual(aws, maps:get(type, Redacted)),
    ?assertEqual(1, maps:get(schema_version, Redacted)),
    ?assertMatch(#{created_at := 1234567890}, maps:get(meta, Redacted)),
    RedactedFields = maps:get(fields, Redacted),
    ?assertEqual("AKIAIOSFODNN7EXAMPLE",
                 maps:get(access_key_id, RedactedFields)),
    ?assertEqual("us-east-1",
                 maps:get(region, RedactedFields)),
    ?assertEqual(<<"********">>,
                 maps:get(secret_access_key, RedactedFields)).

with_version_test() ->
    Rev = {<<"e12e6c751a3f7c7ea73b833324ce70b1">>, 152},
    Cred = #{id => "test", meta => #{created_at => 0}},
    Versioned = with_version(Cred, Rev),
    ?assertEqual(Rev, maps:get(payload_version, maps:get(meta, Versioned))).

matches_prefix_test() ->
    ?assert(matches_prefix("backup/aws/prod", "")),
    ?assert(matches_prefix("backup/aws/prod", "backup")),
    ?assert(matches_prefix("backup/aws/prod", "backup/aws")),
    ?assertNot(matches_prefix("backup/aws/prod", "backup/other")).

-endif.
