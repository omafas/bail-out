%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2018 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
-type rbac_identity_type() :: rejected | wrong_token | anonymous | admin | ro_admin | bucket |
                              external | local | local_token.
-type rbac_identity() :: {rbac_user_id(), rbac_identity_type()}.
-type rbac_role_param() :: string() | {string(), binary()} | any.
-type rbac_role_name() :: atom().
-type rbac_role() :: rbac_role_name() | {rbac_role_name(), nonempty_list(rbac_role_param())}.
-type rbac_user_name() :: string() | undefined.

-type rbac_group_id() :: string().

-type rbac_operation() :: atom().
-type rbac_permission_pattern_operations() :: none | all | nonempty_list(rbac_operation()).
-type rbac_permission_pattern_vertex_param_raw() :: atom().
-type rbac_permission_pattern_vertex_raw() ::
        atom() | {atom(), rbac_permission_pattern_vertex_param_raw()}.
-type rbac_permission_pattern_object_raw() :: [rbac_permission_pattern_vertex_raw()].
-type rbac_permission_pattern_raw() :: {rbac_permission_pattern_object_raw(),
                                        rbac_permission_pattern_operations()}.

-type rbac_permission_pattern_vertex_param() :: string() | any.
-type rbac_permission_pattern_vertex() :: atom() | {atom(), rbac_permission_pattern_vertex_param()}.
-type rbac_permission_pattern_object() :: [rbac_permission_pattern_vertex()].
-type rbac_permission_pattern() :: {rbac_permission_pattern_object(),
                                    rbac_permission_pattern_operations()}.
-type rbac_compiled_role() :: [rbac_permission_pattern()].

-type rbac_role_props() :: [{name | desc, binary()}].
-type rbac_role_def() :: {rbac_role_name(), [atom()], rbac_role_props(),
                          nonempty_list(rbac_permission_pattern_raw())}.

-type rbac_permission_vertex_param() :: string() | any.
-type rbac_permission_vertex() :: atom() | {atom(), rbac_permission_vertex_param()}.
-type rbac_permission_object() :: [rbac_permission_vertex(), ...].
-type rbac_permission_operations() :: rbac_operation() | [rbac_operation(), ...].
-type rbac_permission() :: {rbac_permission_object(), rbac_permission_operations()}.
-type rbac_all_param_values() :: [{[atom()], [[rbac_role_param()]]}].

-endif.
