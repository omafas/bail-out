%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc Types for rbac code.
%%

-ifndef(_RBAC__HRL_).
-define(_RBAC__HRL_,).

-type mochiweb_request() :: {mochiweb_request, [any()]}.
-type mochiweb_response() :: {mochiweb_response, [any()]}.
-type auth_token() :: binary() | string().

-type rbac_user_id() :: string().
-type rbac_password() :: string().
-type rbac_identity_type() :: rejected | wrong_token | anonymous | admin |
                              ro_admin | bucket | external | local |
                              local_token | stats_reader.
-type rbac_identity() :: {rbac_user_id(), rbac_identity_type()}.
-type rbac_role_param() :: string() | {string(), binary()} | any.
-type rbac_role_name() :: atom().
-type rbac_role() :: rbac_role_name() |
                     {rbac_role_name(), nonempty_list(rbac_role_param())}.
-type rbac_user_name() :: string() | undefined.

-type rbac_group_id() :: string().

-type rbac_operation() :: atom().
-type rbac_permission_pattern_operations() :: none | all |
                                              nonempty_list(rbac_operation()).
-type rbac_permission_pattern_vertex_raw() ::
        atom() | {bucket, bucket_name | any} |
        {collection, [bucket_name | scope_name | collection_name]}.
-type rbac_permission_pattern_object_raw() ::
        [rbac_permission_pattern_vertex_raw()].
-type rbac_permission_pattern_raw() :: {rbac_permission_pattern_object_raw(),
                                        rbac_permission_pattern_operations()}.

-type rbac_permission_pattern_vertex_param() :: string() | any.
-type rbac_permission_pattern_vertex() ::
        atom() | {bucket, rbac_permission_pattern_vertex_param()} |
        {collection, [rbac_permission_pattern_vertex_param()]}.
-type rbac_permission_pattern_object() :: [rbac_permission_pattern_vertex()].
-type rbac_permission_pattern() :: {rbac_permission_pattern_object(),
                                    rbac_permission_pattern_operations()}.
-type rbac_compiled_role() :: [rbac_permission_pattern()].

-type rbac_role_props() :: [{name | desc, binary()}].
-type rbac_role_def_param() :: bucket_name | scope_name | collection_name.
-type rbac_role_def() :: {rbac_role_name(), [rbac_role_def_param()],
                          rbac_role_props(),
                          nonempty_list(rbac_permission_pattern_raw())}.

-type rbac_permission_vertex_param() :: string() | any.
-type rbac_permission_vertex() ::
        atom() | {bucket, rbac_permission_vertex_param()} |
        {scope, [rbac_permission_vertex_param()]} |
        {collection, [rbac_permission_vertex_param()]}.
-type rbac_permission_object() :: [rbac_permission_vertex(), ...].
-type rbac_permission() ::
        {rbac_permission_object(), rbac_operation()}.
-type rbac_all_param_values() :: [{[atom()], [[rbac_role_param()]]}].

-define(RBAC_SCOPE_PARAMS, [bucket_name, scope_name]).
-define(RBAC_COLLECTION_PARAMS, [bucket_name, scope_name, collection_name]).

-endif.
