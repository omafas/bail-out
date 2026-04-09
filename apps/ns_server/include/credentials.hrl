%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-define(CREDENTIAL_IDS_KEY, credential_ids).

-define(CREDENTIAL_TYPES,
        [aws, azure_shared, azure_ad, azure_sas, azure_managed, gcp, http,
         couchbase]).

-define(CREDENTIAL_CONSUMER_SERVICES,
        [n1ql, backup, index, xdcr, fts, eventing, cbas]).

-type credential_type() :: aws | azure_shared | azure_ad | azure_sas |
                           azure_managed | gcp | http | couchbase.

-type credential_author() :: #{user := binary(), domain := atom()}.
-type credential_error_reason() :: config_encryption_required |
                                   n2n_encryption_required |
                                   not_found |
                                   already_exists |
                                   unsupported_schema_version |
                                   invalid_type |
                                   {txn_failed, Reason :: term()} |
                                   expired |
                                   service_not_allowed |
                                   access_denied.
-type credential_id() :: string().

-type credential_meta() :: #{created_at := integer(),
                             created_by := credential_author(),
                             updated_at => integer(),
                             updated_by => credential_author(),
                             expires_at => integer(),
                             description => binary() | string(),
                             guardrails => map(),
                             payload_version := chronicle:revision()}.

-type credential_fields() :: #{atom() => string() | integer() | boolean() |
                               binary()}.

-type credentials_map() :: #{id := string(),
                             schema_version := pos_integer(),
                             type := credential_type(),
                             meta := credential_meta(),
                             fields := credential_fields()}.

-type credential_public_view() :: credentials_map().
-type credential_full_view() :: credentials_map().

