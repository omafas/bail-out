%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% 1. Permission is defined as a pair {object, operation}
%% 2. Objects are organized in the tree structure with common root []
%% 3. One vertex of this tree can be parametrized: {bucket, bucket_name},
%%    wildcard all can be used in place of bucket_name
%% 4. Permission pattern is a pair {Object pattern, Allowed operations}
%% 5. Allowed operations can be list of operations, all or none
%% 6. Object pattern is a list of vertices that define a certain subtree of the
%%    objects tree
%% 7. Object pattern vertex {bucket, bucket_name} always matches object vertex
%%    {bucket, any}, object pattern vertex {bucket, any} matches
%%    {bucket, bucket_name} with any bucket_name
%%    otherwise vertices match if they are equal
%% 8. Object matches the object pattern if all the vertices of object pattern
%%    match corresponding vertices of the object.
%% 9. Each role is defined as a list of permission patterns.
%% 10.To find which operations are allowed for certain object in certain role
%%    we look for the first permission pattern with matching object pattern in
%%    the permission pattern list of the role.
%% 11.The permission is allowed by the role if its operation is among the
%%    allowed operations for its object.
%% 12.Each user can have multiple roles assigned
%% 13.Certain permission is allowed to the user if it is allowed at least by
%%    one of the roles assigned to user.

%% @doc roles and permissions implementation

-module(menelaus_roles).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("rbac.hrl").
-include("credentials.hrl").
-include("pipes.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("ns_test.hrl").

-export([set_role_definitions/0]).
-endif.

-define(DEFAULT_EXTERNAL_ROLES_POLLING_INTERVAL, 10*60*1000).

-export([get_definitions/1,
         get_public_definitions/1,
         is_allowed/2,
         get_roles/1,
         get_compiled_roles/1,
         compile_params/3,
         compile_roles/3,
         validate_roles/1,
         validate_roles/2,
         validate_roles/3,
         params_version/1,
         filter_out_invalid_roles/3,
         is_data_vertex/1,
         is_parameterized_vertex/1,
         vertex_arity/1,
         produce_roles_by_permission/2,
         get_security_roles/1,
         get_user_admin_roles/1,
         external_auth_polling_interval/0,
         get_param_defs/2,
         ui_folders/0,
         get_visible_role_definitions/0,
         strip_ids/2,
         chronicle_upgrade_to_totoro/1,
         old_role_to_new/1,
         map_roles_for_compat/1,
         map_roles_for_compat/2,
         get_all_mutable_roles/0,
         is_mutable/1,
         get_role/1,
         set_role/1,
         delete_role/1,
         diff_roles/2,
         get_roles_snapshot/0]).

-export([start_compiled_roles_cache/0]).

%% for RPC from ns_couchdb node
-export([build_compiled_roles/1]).

-spec default_roles_totoro() -> [rbac_role_def(), ...].
default_roles_totoro() ->
    [{<<"admin">>, [],
      [{name, <<"Full Admin">>},
       {folder, admin},
       {desc, <<"Can manage all cluster features (including security). "
                "This user can access the web console. This user can read and "
                "write all data.">>},
       {ce, true}],
      [{[], all}]},
     {<<"ro_admin">>, [],
      [{name, <<"Read-Only Admin">>},
       {folder, admin},
       {desc, <<"Can view all cluster statistics. "
                "This user cannot read security-related information, "
                "including listing users or groups.">>},
       {ce, true}],
      [{[{bucket, any}, data], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, analytics], none},
       {[admin, security], none},
       {[admin, security_info], none},
       {[admin, stats_export], [read]},
       {[admin, users], none},
       {[admin], none},
       {[eventing], none},
       {[analytics], none},
       {[external_catalog], [read]},
       {[backup], [read]},
       {[{credentials, any}], none},
       {[ui], none},
       {[], [read, list]}]},
     {<<"ui_access">>, [],
      [{name, <<"Web Console Access">>},
       {folder, admin},
       {desc, <<"Can access the web console.">>},
       {ce, true}],
      [{[ui], [read]},
       {[pools], [read]}]},
     {<<"security_admin">>, [],
      [{name, <<"Security Admin">>},
       {folder, admin},
       {desc, <<"Can view all cluster statistics, manage certificates, manage "
                "credentials and security related settings. This user cannot "
                "read data. This user cannot use stored credentials.">>}],
      [{[admin, security, admin], none},
       {[admin, security], all},
       {[admin, security_info], all},
       {[admin, users], [read]},
       {[admin, logs], none},
       {[admin, event], none},
       {[admin, metakv], none},
       {[admin, settings, metrics], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, analytics], none},
       {[{bucket, any}], [read]},
       {[{credentials, any}], [read, write]},
       {[analytics], none},
       {[external_catalog], none},
       {[backup], none},
       {[eventing], none},
       {[xdcr], none},
       {[settings, fts], none},
       {[settings, metrics], none},
       {[ui], none},
       {[], [read, list]}]},
     {<<"ro_security_admin">>, [],
      [{name, <<"Read-Only Security Admin">>},
       {folder, admin},
       {desc, <<"Can view all cluster statistics. Can read security related "
                "settings and credentials (metadata) but cannot change them. "
                "This user cannot read data.">>}],
      [{[admin, security, admin], [read]},
       {[admin, security], [read]},
       {[admin, security_info], [read]},
       {[admin, users], [read]},
       {[admin, logs], none},
       {[admin, event], none},
       {[admin, metakv], none},
       {[admin, settings, metrics], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, analytics], none},
       {[{bucket, any}], [read]},
       {[{credentials, any}], [read]},
       {[analytics], none},
       {[external_catalog], none},
       {[backup], none},
       {[eventing], none},
       {[xdcr], none},
       {[settings, fts], none},
       {[settings, metrics], none},
       {[ui], none},
       {[], [read, list]}]},
     {<<"user_admin_local">>, [],
      [{name, <<"Local User Admin">>},
       {folder, admin},
       {desc, <<"Can view all cluster statistics and manage local user "
                "roles, but not grant Full Admin or Security Admin roles to "
                "other users or itself. This user cannot read data.">>}],
      [{[admin, security, admin], none},
       {[admin, security], none},
       {[admin, security_info], [read, write]},
       {[admin, users, admin], none},
       {[admin, users, external], none},
       {[admin, users], all},
       {[admin, logs], none},
       {[admin, event], none},
       {[admin, metakv], none},
       {[admin, settings, metrics], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, analytics], none},
       {[{bucket, any}], [read]},
       {[analytics], none},
       {[external_catalog], [read]},
       {[backup], none},
       {[eventing], none},
       {[xdcr], none},
       {[settings, fts], none},
       {[settings, metrics], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], [read, list]}]},
     {<<"user_admin_external">>, [],
      [{name, <<"External User Admin">>},
       {folder, admin},
       {desc, <<"Can view all cluster statistics and manage external user "
                "roles, but not grant Full Admin or Security Admin roles to "
                "other users or itself. This user cannot read data.">>}],
      [{[admin, security, admin], none},
       {[admin, security], none},
       {[admin, security_info], [read, write]},
       {[admin, users, admin], none},
       {[admin, users, local], none},
       {[admin, users], all},
       {[admin, logs], none},
       {[admin, event], none},
       {[admin, metakv], none},
       {[admin, settings], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, analytics], none},
       {[{bucket, any}], [read]},
       {[analytics], none},
       {[external_catalog], [read]},
       {[backup], none},
       {[eventing], none},
       {[xdcr], none},
       {[settings, fts], none},
       {[settings, metrics], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], [read, list]}]},
     {<<"credential_consumer">>, [credential_id],
      [{name, <<"Credential Consumer">>},
       {folder, admin},
       {desc, <<"Can read, consume (use) specific external credentials or "
                "those matching a pattern.">>}],
      [{[{credentials, credential_id}], [consume]}]},
     {<<"cluster_admin">>, [],
      [{name, <<"Cluster Admin">>},
       {folder, admin},
       {desc, <<"Can manage all cluster features except security and users. "
                "This user cannot read data.">>}],
      [{[admin, internal], none},
       {[admin, security], none},
       {[admin, security_info], none},
       {[admin, users], none},
       {[admin, diag], [read]},
       {[admin, event], none},
       {[admin, metakv], none},
       {[admin, settings, metrics], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, analytics], none},
       {[n1ql, curl], none},
       {[eventing], none},
       {[analytics], none},
       {[backup], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], all}]},
     {<<"eventing_admin">>, [],
      [{name, <<"Eventing Full Admin">>},
       {folder, admin},
       {desc, <<"Can create/manage eventing functions.">>}],
      [{[admin], none},
       {[xdcr], none},
       {[{bucket, any}, xdcr], none},
       %% This role is intentionally given this powerful permission
       %% (see MB-42835).
       {[{bucket, any}], all},
       {[n1ql], all},
       {[eventing], all},
       {[analytics], all},
       {[buckets], all},
       {[settings, metrics], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], [read]}]},
     {<<"backup_admin">>, [],
      [{name, <<"Backup Full Admin">>},
       {folder, admin},
       {desc, <<"Can perform backup related tasks.">>}],
      [{[admin], none},
       {[settings, metrics], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], all}]},
     {<<"bucket_admin">>, [bucket_name],
      [{name, <<"Bucket Admin">>},
       {folder, bucket},
       {desc, <<"Can manage ALL bucket features for a given bucket (including "
                "start/stop XDCR). This user cannot read data.">>}],
      [{[{bucket, bucket_name}, xdcr], [read, execute]},
       {[{bucket, bucket_name}, data], none},
       {[{bucket, bucket_name}, views], none},
       {[{bucket, bucket_name}, n1ql], none},
       {[{bucket, bucket_name}, fts], none},
       {[{bucket, any}, analytics], none},
       {[{bucket, bucket_name}], all},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[xdcr], none},
       {[admin], none},
       {[eventing], none},
       {[analytics], none},
       {[external_catalog], none},
       {[backup], none},
       {[settings, metrics], none},
       {[n1ql, meta], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], [read]}]},
     {<<"scope_admin">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Manage Scopes">>},
       {folder, bucket},
       {desc, <<"Can create/delete scopes and collections within a given "
                "bucket.">>}],
      [{[{collection, [bucket_name, scope_name, any]}, collections], all}]},
     {<<"bucket_full_access">>, [bucket_name],
      [{name, <<"Application Access">>},
       {folder, bucket},
       {desc, <<"Full access to bucket data. This user is intended only for "
                "application access. This user can read and write data except "
                "for the _system scope which can only be read.">>},
       {ce, true}],
      [{[{bucket, bucket_name}, data, docs], [read, insert, delete, upsert,
                                              range_scan, sread]},
       {[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[{bucket, bucket_name}], [read, flush]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"views_admin">>, [bucket_name],
      [{name, <<"Views Admin (deprecated)">>},
       {folder, admin},
       {desc, <<"Can create and manage views of a given bucket. "
                "This user can read some data. "
                "This role is deprecated and will be removed in a future "
                "release.">>}],
      [{[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, data, docs], [read, sread]},
       {[{bucket, bucket_name}, data], [read]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[xdcr], none},
       {[admin], none},
       {[eventing], none},
       {[analytics], none},
       {[external_catalog], none},
       {[backup], none},
       {[settings, metrics], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], [read]}]},
     {<<"views_reader">>, [bucket_name],
      [{name, <<"Views Reader (deprecated)">>},
       {folder, views},
       {desc, <<"Can read data from the views of a given bucket. This user "
                "is intended only for application access. This user can read "
                "some data. "
                "This role is deprecated and will be removed in a future "
                "release.">>}],
      [{[{bucket, bucket_name}, views], [read]},
       {[{bucket, bucket_name}, data, docs], [read, sread]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"replication_admin">>, [],
      [{name, <<"XDCR Admin">>},
       {folder, xdcr},
       {desc, <<"Can administer XDCR features to create cluster references and "
                "replication streams out of this cluster. "
                "This user can read some data.">>}],
      [{[{bucket, any}, xdcr], all},
       {[{bucket, any}, data, docs], [read, sread]},
       {[{bucket, any}, data], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}, stats], [read]},
       {[{bucket, any}, collections], [read]},
       {[{bucket, any}], none},
       {[xdcr, developer], [read]},
       {[xdcr], all},
       {[admin], none},
       {[eventing], none},
       {[analytics], none},
       {[external_catalog], none},
       {[backup], none},
       {[settings, metrics], none},
       {[{credentials, any}], none},
       {[ui], none},
       {[], [read]}]},
     {<<"data_reader">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Data Reader">>},
       {folder, data},
       {desc, <<"Can read data from a given bucket, scope or collection. "
                "This user is intended only for application access. This user "
                "can read data, but cannot write it.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs],
        [read, range_scan, sread]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"data_writer">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Data Writer">>},
       {folder, data},
       {desc, <<"Can write data to a given bucket, scope or collection. "
                "This user is intended only for application access. "
                "This user can write data, but cannot read it.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs],
        [insert, upsert, delete]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"data_dcp_reader">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Data DCP Reader">>},
       {folder, data},
       {desc, <<"Can initiate DCP streams for a given bucket, scope or "
                "collection. "
                "This user is intended only for application access. "
                "This user can read data.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs], [read, sread]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, data, dcpstream], [read]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, data, sxattr], [read]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, collections], [read]},
       {[{bucket, bucket_name}, data, dcp], [read]},
       {[{bucket, bucket_name}, data, docs], [sread]},
       {[{bucket, bucket_name}, settings], [read]},
       {[admin, memcached, idle], [write]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"data_backup">>, [bucket_name],
      [{name, <<"Data Backup & Restore">>},
       {folder, backup},
       {desc, <<"Can backup and restore a given bucket’s data. This user "
                "is intended only for application access. This user can "
                "read data.">>}],
      [{[{collection, [bucket_name, any, any]}, collections], all},
       {[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], [read, write]},
       {[{bucket, bucket_name}, fts], [read, write, manage]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, n1ql, index], [create, list, build]},
       {[{bucket, bucket_name}, n1ql, meta], [backup]},
       {[{bucket, bucket_name}, analytics], [manage, select]},
       {[analytics], [select, backup]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"data_monitoring">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Data Monitor">>},
       {folder, data},
       {desc, <<"Can read statistics for a given bucket, scope or collection. "
                "This user is intended only for application access. This user "
                "cannot read data.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, stats], [read]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, collections], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[tasks], [read]},
       {[pools], [read]}]},
     {<<"fts_admin">>, [bucket_name],
      [{name, <<"Search Admin">>},
       {folder, search},
       {desc, <<"Can administer all Full Text Search features. "
                "This user can read some data.">>}],
      [{[{bucket, bucket_name}, fts], [read, write, manage]},
       {[{bucket, bucket_name}, collections], [read]},
       {[{bucket, bucket_name}, data, docs], [read, sread]},
       {[settings, fts], [read, write, manage]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]}]},
     {<<"fts_searcher">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Search Reader">>},
       {folder, search},
       {desc, <<"Can query Full Text Search indexes for a given bucket, scope "
                "or collection. This user can read some data.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, fts], [read]},
       {[settings, fts], [read]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_external_catalog_select">>, [],
      [{name, <<"Query External Catalog Select">>},
       {folder, 'query'},
       {desc, <<"Can read external catalogs.">>}],
      [{[external_catalog], [read]},
       %% + pools access to let us stream pools for catalog manifest uid updates
       {[pools], [read]}]},
     {<<"query_external_catalog_update">>, [],
      [{name, <<"Query External Catalog Update">>},
       {folder, 'query'},
       {desc, <<"Can update/delete external catalogs.">>}],
      [{[external_catalog], [write]},
       %% + pools access to let us stream pools for catalog manifest uid updates
       {[pools], [read]}]},
     {<<"query_select">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Query Select">>},
       {folder, 'query'},
       {desc, <<"Can execute a SELECT statement on a given bucket, scope or "
                "collection to retrieve data. This user can read data, "
                "but not write it.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, n1ql, select], [execute]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs], [read, sread]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_update">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Query Update">>},
       {folder, 'query'},
       {desc, <<"Can execute an UPDATE statement on a given bucket, scope or "
                "collection to update data. This user can write data, "
                "but cannot read it.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, n1ql, update], [execute]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs],
        [upsert, range_scan]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_insert">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Query Insert">>},
       {folder, 'query'},
       {desc, <<"Can execute an INSERT statement on a given bucket, scope or "
                "collection to add data. This user can insert data, "
                "but cannot read it.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, n1ql, insert], [execute]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs], [insert]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_delete">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Query Delete">>},
       {folder, 'query'},
       {desc, <<"Can execute a DELETE statement on a given bucket, scope or "
                "collection to delete data. This user cannot read data. "
                "This user can delete data.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, n1ql, delete], [execute]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs],
        [delete, range_scan]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_manage_index">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Query Manage Index">>},
       {folder, 'query'},
       {desc, <<"Can manage indexes for a given bucket, scope or collection. "
                "This user can read statistics for a given bucket, scope or "
                "collection. This user cannot read data.">>
       }],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, n1ql, index], all},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, collections], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, stats], [read]},
       {[settings, indexes], [read]},
       {[pools], [read]}]},
     {<<"query_list_index">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Query List Index">>},
       {folder, 'query'},
       {desc, <<"Can list indexes for a given bucket, scope or collection. "
                "This user can read statistics for a given bucket, "
                "scope or collection. This user cannot read data.">>
       }],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, n1ql, index], [list, read]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, collections], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, stats], [read]},
       {[settings, indexes], [read]},
       {[pools], [read]}]},
     {<<"query_system_catalog">>, [],
      [{name, <<"Query System Catalog">>},
       {folder, 'query'},
       {desc, <<"Can look up system catalog information via N1QL. This user "
                "cannot read user data.">>}],
      [{[{bucket, any}, n1ql, index], [list]},
       {[{bucket, any}, settings], [read]},
       {[n1ql, meta], [read]},
       {[settings, indexes], [read]},
       {[pools], [read]}]},
     {<<"query_external_access">>, [],
      [{name, <<"Query CURL Access">>},
       {folder, 'query'},
       {desc, <<"Can execute the CURL statement from within N1QL. This user "
                "cannot read data (within Couchbase).">>}],
      [{[n1ql, curl], [execute]},
       {[{bucket, any}, settings], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_manage_global_functions">>, [],
      [{name, <<"Manage Global Functions">>},
       {folder, 'query'},
       {desc, <<"Can manage global n1ql functions">>}],
      [{[n1ql, udf], [manage]},
       {[pools], [read]}]},
     {<<"query_execute_global_functions">>, [],
      [{name, <<"Execute Global Functions">>},
       {folder, 'query'},
       {desc, <<"Can execute global n1ql functions">>}],
      [{[n1ql, udf], [execute]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_manage_functions">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Manage Scope Functions">>},
       {folder, 'query'},
       {desc, <<"Can manage n1ql functions for a given scope">>}],
      [{[{collection, [bucket_name, scope_name, any]}, n1ql, udf], [manage]},
       {[{collection, [bucket_name, scope_name, any]}, collections], [read]},
       {[pools], [read]}]},
     {<<"query_execute_functions">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Execute Scope Functions">>},
       {folder, 'query'},
       {desc, <<"Can execute n1ql functions for a given scope">>}],
      [{[{collection, [bucket_name, scope_name, any]}, n1ql, udf], [execute]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_manage_global_external_functions">>, [],
      [{name, <<"Manage Global External Functions">>},
       {folder, 'query'},
       {desc, <<"Can manage global external language functions">>}],
      [{[n1ql, udf_external], [manage]},
       {[pools], [read]}]},
     {<<"query_execute_global_external_functions">>, [],
      [{name, <<"Execute Global External Functions">>},
       {folder, 'query'},
       {desc, <<"Can execute global external language functions">>}],
      [{[n1ql, udf_external], [execute]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_manage_external_functions">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Manage Scope External Functions">>},
       {folder, 'query'},
       {desc, <<"Can manage external language functions for a given scope">>}],
      [{[{collection, [bucket_name, scope_name, any]}, n1ql,
         udf_external], [manage]},
       {[{collection, [bucket_name, scope_name, any]}, collections], [read]},
       {[pools], [read]}]},
     {<<"query_execute_external_functions">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Execute Scope External Functions">>},
       {folder, 'query'},
       {desc, <<"Can execute external language functions for a given scope">>}],
      [{[{collection, [bucket_name, scope_name, any]}, n1ql,
         udf_external], [execute]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_manage_sequences">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Manage Sequences">>},
       {folder, 'query'},
       {desc, <<"Can manage sequences for a given scope">>}],
      [{[{collection, [bucket_name, scope_name, any]}, n1ql, sequences],
        [manage]},
       {[{collection, [bucket_name, scope_name, any]}, collections], [read]},
       {[pools], [read]}]},
     {<<"query_use_sequences">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Use Sequences">>},
       {folder, 'query'},
       {desc, <<"Can use sequences for a given scope">>}],
      [{[{collection, [bucket_name, scope_name, any]}, n1ql, sequences],
        [execute]},
       {[{collection, [bucket_name, scope_name, any]}, collections], [read]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"query_use_sequential_scans">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Query Use Sequential Scans">>},
       {folder, 'query'},
       {desc, <<"Can use sequential scans for access to a given bucket, scope "
                "or collection.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, n1ql, sequential_scan],
        [execute]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, data, docs], [range_scan]},
       {[app_telemetry], [write]}]},
     {<<"query_manage_system_catalog">>, [],
      [{name, <<"Query Manage System Catalog">>},
       {folder, query},
       {desc, <<"Can manage Query system catalogs via SQL++. "
                "This user cannot read data.">>}],
      [{[n1ql, meta], [manage]},
       {[pools], [read]}]},
     {<<"replication_target">>, [bucket_name],
      [{name, <<"XDCR Inbound">>},
       {folder, xdcr},
       {desc, <<"Can create XDCR streams into a given bucket.">>}],
      [{[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, data, docs], [read, sread, upsert]},
       {[{bucket, bucket_name}, data, meta], [write]},
       {[{bucket, bucket_name}, data, sxattr], [read, write]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, collections], [read]},
       {[xdcr, c2c_communications], all},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"analytics_manager">>, [bucket_name],
      [{name, <<"Analytics Manager">>},
       {folder, analytics},
       {desc, <<"Can manage Analytics local links. Can manage datasets on a "
                "given bucket. Can query datasets created on this bucket. "
                "This user can read some data.">>}],
      [{[{bucket, bucket_name}, analytics], [manage, select]},
       {[pools], [read]}]},
     {<<"analytics_reader">>, [],
      [{name, <<"Analytics Reader">>},
       {folder, analytics},
       {desc, <<"Can query datasets. This is a global role as datasets may "
                "be created on different buckets. This user can read some "
                "data.">>}],
      [{[analytics], [select]},
       {[{bucket, any}, analytics], [select]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"analytics_select">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Analytics Select">>},
       {folder, analytics},
       {desc, <<"Can query datasets on a given bucket, scope or "
                "collection. This user can read some data.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, analytics], [select]},
       {[pools], [read]},
       {[app_telemetry], [write]}]},
     {<<"analytics_admin">>, [],
      [{name, <<"Analytics Admin">>},
       {folder, analytics},
       {desc, <<"Can manage dataverses. Can manage all Analytics links. "
                "Can manage all datasets. This user cannot read data.">>}],
      [{[analytics], [manage]},
       {[{bucket, any}, analytics], [manage]},
       {[pools], [read]}]},
     {<<"mobile_sync_gateway">>, [bucket_name],
      [{name, <<"Sync Gateway">>},
       {folder, mobile},
       {desc, <<"Full access to bucket data as required by Sync Gateway. "
                "This user is intended only for use by Sync Gateway. "
                "This user can read and write data, manage indexes and views, "
                "and read some cluster information.">>}],
      [{[{collection, [bucket_name, ?SYSTEM_SCOPE_NAME, "_mobile"]}, data],
        all},
       {[{bucket, bucket_name}, data, docs], [read, insert, delete, upsert,
                                              range_scan, sread]},
       {[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[{bucket, bucket_name}], [read, flush]},
       {[{bucket, bucket_name}, settings], [read]},
       {[admin, memcached, idle], [write]},
       {[settings, autocompaction], [read]},
       {[pools], [read]}]},
     {<<"sync_gateway_configurator">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Sync Gateway Architect">>},
       {folder, mobile},
       {desc, <<"Can manage Sync Gateway databases and users, "
                "and access Sync Gateway's /metrics endpoint. "
                "This user cannot read application data.">>}],
      [{[{collection, [any, any, any]}, sgw, appdata], none},
       {[{collection, [any, any, any]}, sgw, principal_appdata], none},
       {[{collection, [any, any, any]}, sgw, replications], none},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, sgw], all},
       {[admin, stats_export], [read]}]},
     {<<"sync_gateway_app">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Sync Gateway Application">>},
       {folder, mobile},
       {desc, <<"Can manage Sync Gateway users and roles, and "
                "read and write application data through Sync "
                "Gateway.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, auth], [configure]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, principal], [read, write]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, appdata], [read, write]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, principal_appdata],
        [read]}]},
     {<<"sync_gateway_app_ro">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Sync Gateway Application Read Only">>},
       {folder, mobile},
       {desc, <<"Can read Sync Gateway users and roles, and "
                "read application data through Sync Gateway.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, appdata], [read]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, principal], [read]},
       {[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, principal_appdata],
        [read]}]},
     {<<"sync_gateway_replicator">>, ?RBAC_COLLECTION_PARAMS,
      [{name, <<"Sync Gateway Replicator">>},
       {folder, mobile},
       {desc, <<"Can manage Inter-Sync Gateway Replications. "
                "This user cannot read application data.">>}],
      [{[{collection, ?RBAC_COLLECTION_PARAMS}, sgw, replications], all}]},
     {<<"sync_gateway_dev_ops">>, [],
      [{name, <<"Sync Gateway Dev Ops">>},
       {folder, mobile},
       {desc, <<"Can manage Sync Gateway node-level configuration, "
                "and access Sync Gateway's /metrics endpoint "
                "for Prometheus integration.">>}],
      [{[sgw, dev_ops], all},
       {[admin, stats_export], [read]}]},
     {<<"external_stats_reader">>, [],
      [{name, <<"External Stats Reader">>},
       {folder, admin},
       {desc, <<"Access to /metrics endpoint for Prometheus integration. "
                "Can read all stats for all services.">>}],
      [{[admin, stats_export], [read]}]},
     {<<"application_telemetry_writer">>, [],
      [{name, <<"Application Telemetry Writer">>},
       {folder, admin},
       {desc, <<"Can report application telemetry through the SDK.">>}],
      [{[app_telemetry], [write]}]},
     {<<"eventing_manage_functions">>, ?RBAC_SCOPE_PARAMS,
      [{name, <<"Manage Scope Functions">>},
       {folder, eventing},
       {desc, <<"Can manage eventing functions for a given scope">>}],
      [{[{collection, [bucket_name, scope_name, any]}, eventing, function],
        [manage]},
       {[{collection, [bucket_name, scope_name, any]}, collections], [read]},
       {[{bucket, bucket_name}, stats], [read]},
       {[pools], [read]}]}
    ].

role_definitions() ->
    chronicle_compat:get(role_definitions, #{required => true}).

-spec roles() -> [rbac_role_def(), ...].
roles() ->
    Roles = role_definitions(),
    %% Allow roles to be added / replaced with those from the config profile.
    ConfigRoles = config_profile:get_value(extra_roles, []),
    ConfigNames = lists:map(fun extract_role_name/1, ConfigRoles),
    %% Validate DefaultRoles and filter out any with conflicting names
    FilteredRoles = lists:filter(
                      fun(Role) ->
                                 not lists:member(
                                       extract_role_name(Role), ConfigNames)
                         end,
                      Roles),
    FilteredRoles ++ ConfigRoles.

ui_folders() ->
    [{admin, "Administrative"},
     {bucket, "Bucket"},
     {data, "Data"},
     {views, "Views (deprecated)"},
     {'query', "Query & Index"},
     {search, "Search"},
     {analytics, "Analytics"},
     {eventing, "Eventing"},
     {xdcr, "XDCR"},
     {backup, "Backup"},
     {mobile, "Mobile"},
     {external_catalog, "External Catalog"},
     {custom_roles, "Custom"}].

internal_roles() ->
    [{<<"stats_reader">>, [], [], [{[admin, internal, stats], [read]}]},
     {<<"metakv2_access">>, [], [], [{[admin, internal, metakv2], all}]},
     %% service_admin is an internal-only role implicitly assigned to service
     %% users (e.g. @backup, @cbq-engine) from totoro onwards. It has all Full
     %% Admin permissions except any credential permissions.
     {<<"service_admin">>, [], [],
      [{[{credentials, any}], none},
       {[], all}]}].

maybe_add_developer_preview_roles() ->
    DP = cluster_compat_mode:is_developer_preview(),
    add_replication_developer_roles(DP).

add_replication_developer_roles(true) ->
    [{<<"replication_developer">>, [],
      [{name, <<"XDCR Developer">>},
       {folder, xdcr},
       {desc, <<"Can read and write Custom Conflict Resolution merge "
                "functions. This user cannot access the web console.">>}],
      [{[xdcr, developer], all}]}];
add_replication_developer_roles(false) ->
    [].

-spec get_definitions(all | public) -> [rbac_role_def(), ...].
get_definitions(all) ->
    get_definitions(public) ++ internal_roles();
get_definitions(public) ->
    get_public_definitions(cluster_compat_mode:get_compat_version()).

-spec get_public_definitions(list()) -> [rbac_role_def(), ...].
get_public_definitions(Version) ->
    [{_, Fun} | _] = public_definitions(Version),
    Fun().

public_definitions(Version) ->
    lists:dropwhile(fun ({undefined, _}) ->
                            false;
                        ({V, _}) ->
                            Version >= V
                    end, public_definitions()).

public_definitions() ->
    [{?VERSION_76, fun menelaus_old_roles:roles_pre_76/0},
     {?VERSION_79, fun menelaus_old_roles:roles_pre_79/0},
     {?VERSION_TOTORO, fun menelaus_old_roles:roles_pre_totoro/0},
     {undefined, ?cut(roles() ++ maybe_add_developer_preview_roles())}].


vertex_param_match(any, _) ->
    true;
vertex_param_match(_, any) ->
    true;
vertex_param_match(A, B) ->
    A =:= B.

is_data_vertex({bucket, _}) ->
    true;
is_data_vertex({scope, _}) ->
    true;
is_data_vertex({collection, _}) ->
    true;
is_data_vertex(_) ->
    false.

%% Number of colon-separated params each parameterized vertex carries
%% in wire form (e.g. `bucket[name]' = 1, `collection[b:s:c]' = 3).
-spec vertex_arity(atom()) -> pos_integer().
vertex_arity(bucket)      -> 1;
vertex_arity(credentials) -> 1;
vertex_arity(scope)       -> 2;
vertex_arity(collection)  -> 3.

-spec is_parameterized_vertex(atom()) -> boolean().
is_parameterized_vertex(bucket)      -> true;
is_parameterized_vertex(credentials) -> true;
is_parameterized_vertex(scope)       -> true;
is_parameterized_vertex(collection)  -> true;
is_parameterized_vertex(_)           -> false.

is_credential_prefix(CredId) ->
    case lists:reverse(CredId) of
        "*" ++ Rest -> {true, lists:reverse(Rest)};
        _ -> false
    end.

get_vertex_param_list({bucket, B}) ->
    [B];
get_vertex_param_list({credentials, C}) ->
    [C];
get_vertex_param_list({_, Params}) ->
    Params;
get_vertex_param_list(_) ->
    [].

expand_vertex(V, Pad) ->
    case is_data_vertex(V) of
        true  -> expand_data_vertex(V, Pad);
        false -> [V]
    end.

%% Data vertices unfold into a [bucket, scope, collection] 3-tuple so that
%% element-wise matching against role patterns lines up. Non-data vertices
%% (atoms, parameterized non-data vertices like `{credentials, _}') have no
%% bucket/scope/collection identity to expand and match by themselves.
expand_data_vertex({bucket, B}, Pad) ->
    [{expanded_bucket, B}, {expanded_scope, Pad}, {expanded_collection, Pad}];
expand_data_vertex({scope, [B, S]}, Pad) ->
    [{expanded_bucket, B}, {expanded_scope, S}, {expanded_collection, Pad}];
expand_data_vertex({collection, [B, S, C]}, _) ->
    [{expanded_bucket, B}, {expanded_scope, S}, {expanded_collection, C}].

vertex_match({credentials, PermId}, {credentials, FilterId}) ->
    credential_match(PermId, FilterId);
vertex_match({Vertex, Param}, {Vertex, FilterParam}) ->
    vertex_param_match(Param, FilterParam);
vertex_match(Vertex, Filter) ->
    Vertex =:= Filter.

credential_match(PermId, FilterId) ->
    case FilterId of
        any ->
            true;
        _ ->
            case is_credential_prefix(FilterId) of
                {true, Prefix} ->
                    case string:slice(PermId, 0, length(Prefix)) of
                        Prefix -> true;
                        _ -> false
                    end;
                false ->
                    PermId =:= FilterId
            end
    end.

parameterised_by({_, By}, By) -> true;
parameterised_by(_, _) -> false.

-spec operation_allowed(rbac_operation(),
                        rbac_permission_pattern_operations()) ->
          boolean().
operation_allowed(_, all) ->
    true;
operation_allowed(_, none) ->
    false;
operation_allowed(any, _) ->
    true;
operation_allowed(Operation, AllowedOperations) ->
    lists:member(Operation, AllowedOperations).

permission_granted_by_role(_, _, []) ->
    false;
permission_granted_by_role(ExpandedObject, Operation,
                           [{PermObj, PermOp} | Rest]) ->
    PermObjExpanded = lists:flatmap(expand_vertex(_, any), PermObj),
    OpAllowed = operation_allowed(Operation, PermOp),
    case match_object(ExpandedObject, PermObjExpanded, strict) of
        {strict, mismatch} ->
            %% Mismatch is strict, so no other permission can provide this
            false;
        {strict, match} ->
            %% Strict match, so if this permission doesn't give the operation,
            %% then no other permission can provide this
            OpAllowed;
        {loose, mismatch} ->
            %% Mismatch is loose, so it may be that another permission can
            %% provide the operation
            permission_granted_by_role(ExpandedObject, Operation, Rest);
        {loose, match} ->
            %% Match is loose, so if this permission doesn't give the operation,
            %% then another permission may provide it
            OpAllowed orelse
                permission_granted_by_role(ExpandedObject, Operation, Rest)
    end.

match_object(_, [], Strictness) ->
    {Strictness, match};
match_object([], _, _) ->
    {loose, mismatch};
match_object([Vertex1 | ObjectToCheck], [Vertex2 | ObjectSpecified],
             Strictness) ->
    case vertex_match(Vertex1, Vertex2) of
        true ->
            %% If wildcard 'any' is used, we want to flag this up as a loose
            %% match, in case the privilege is provided by another permission
            WeakMatch = parameterised_by(Vertex1, any) andalso
                not parameterised_by(Vertex2, any),
            NewMatch =
                case Strictness =:= loose orelse WeakMatch of
                    true ->
                        loose;
                    false ->
                        Strictness
                end,
            match_object(ObjectToCheck, ObjectSpecified, NewMatch);
        false ->
            %% While the vertex hasn't matched, we need to determine if the
            %% mismatch is strict, i.e. whether we should ignore other
            %% permissions that grant the privilege after this one.
            %% This is the case if the permission being checked uses 'all',
            %% there wasn't a higher level wildcard 'any', and the rest of the
            %% permission at least loosely matches
            NewStrictness =
                case parameterised_by(Vertex1, all) andalso
                    Strictness =:= strict of
                     true ->
                         Matches = match_object(ObjectToCheck, ObjectSpecified,
                                                loose),
                         case Matches of
                              {loose, mismatch} -> loose;
                              _ -> strict
                         end;
                     false ->
                         loose
                 end,
            {NewStrictness, mismatch}
    end.

-spec is_allowed(rbac_permission(),
                 #authn_res{} | [rbac_compiled_role()]) -> boolean().
is_allowed(_Permission, #authn_res{password_expired = true}) ->
    false;
is_allowed(Permission, #authn_res{} = AuthnRes) ->
    case menelaus_auth:check_expiration(AuthnRes) of
        {error, expired} -> false;
        ok ->
            Roles = get_compiled_roles(AuthnRes),
            is_allowed(Permission, Roles)
    end;
is_allowed({Object, Operation}, Roles) ->
    ObjectExpanded = lists:flatmap(expand_vertex(_, all), Object),
    lists:any(permission_granted_by_role(ObjectExpanded, Operation, _), Roles).

-spec substitute_params([string()],
                        [atom()], [rbac_permission_pattern_raw()]) ->
          [rbac_permission_pattern()].
substitute_params([], [], Permissions) ->
    Permissions;
substitute_params(Params, ParamDefinitions, Permissions) ->
    ParamPairs = lists:zip(ParamDefinitions, Params),
    lists:map(
      fun ({ObjectPattern, AllowedOperations}) ->
              {lists:map(
                 fun ({Name, List}) when is_list(List) ->
                         {Name, [substitute_param(Param, ParamPairs) ||
                                    Param <- List]};
                     ({Name, Param}) ->
                         {Name, substitute_param(Param, ParamPairs)};
                     (Vertex) ->
                         Vertex
                 end, ObjectPattern), AllowedOperations}
      end, Permissions).

substitute_param(any, _ParamPairs) ->
    any;
substitute_param(Param, ParamPairs) ->
    case lists:keyfind(Param, 1, ParamPairs) of
        {Param, Subst} ->
            Subst;
        false ->
            Param
    end.

compile_param(bucket_name, Name, Snapshot) ->
    find_object(Name,
                fun (BucketName) ->
                        case ns_bucket:uuid(BucketName, Snapshot) of
                            not_present ->
                                undefined;
                            UUID ->
                                {UUID,
                                 collections:get_manifest(BucketName, Snapshot)}
                        end
                end);
compile_param(scope_name, Name, Manifest) ->
    find_object(Name,
                fun (ScopeName) ->
                        case Manifest of
                            undefined ->
                                undefined;
                            _ ->
                                maybe_add_id(collections:get_scope(ScopeName,
                                                                   Manifest))
                        end
                end);
compile_param(collection_name, Name, Scope) ->
    find_object(Name, ?cut(maybe_add_id(collections:get_collection(_, Scope))));
compile_param(credential_id, Name, Snapshot) ->
    case is_valid_credential_id(Name,
                                cb_credentials_store:get_index(Snapshot)) of
        true -> {Name, Snapshot};
        false -> undefined
    end.

%% A credential_id role param is valid when:
%%   - `any' (from the `[*]' wildcard in the role string) — matches everything
%%   - a bare `"*"' — equivalent to `any', no existence check required
%%   - a prefix `"foo/*"' — at least one existing credential must start with
%%     `foo/'
%%   - a specific id — must refer to an existing credential
is_valid_credential_id(any, _Ids) ->
    true;
is_valid_credential_id(Name, Ids) when is_list(Name) ->
    case is_credential_prefix(Name) of
        {true, ""} ->
            true;
        {true, Prefix} ->
            lists:any(fun (Id) -> lists:prefix(Prefix, Id) end, Ids);
        false ->
            lists:member(Name, Ids)
    end;
is_valid_credential_id(_, _) ->
    false.

maybe_add_id(undefined) ->
    undefined;
maybe_add_id(Props) ->
    {collections:get_uid(Props), Props}.

find_object(any, _Find) ->
    {any, any};
find_object({Name, Id}, Find) ->
    case find_object(Name, Find) of
        RV = {{Name, Id}, _} ->
            RV;
        _ ->
            undefined
    end;
find_object(Name, Find) when is_list(Name) ->
    case Find(Name) of
        undefined ->
            undefined;
        {UUID, Props} ->
            {{Name, UUID}, Props}
    end.

params_version() ->
    params_version(ns_bucket:get_snapshot(all, [collections, props, uuid])).

-spec params_version(map()) -> term().
params_version(Snapshot) ->
    lists:map(
      fun (Name) ->
              UUID = ns_bucket:uuid(Name, Snapshot),
              {ok, BucketConfig} = ns_bucket:get_bucket(Name, Snapshot),
              DefaultManifest = collections:default_manifest(BucketConfig),
              Manifest = collections:get_manifest(Name, Snapshot,
                                                  DefaultManifest),
              ManifestUid = collections:get_uid(Manifest),
              {Name, UUID, ManifestUid}
      end,
      ns_bucket:get_bucket_names(Snapshot)).

compile_params([], [], Acc, _) ->
    lists:reverse(Acc);
compile_params([ParamDef | RestParamDefs], [Param | RestParams], Acc, Ctx) ->
    case compile_param(ParamDef, Param, Ctx) of
        undefined ->
            false;
        {Compiled, NewCtx} ->
            compile_params(RestParamDefs, RestParams, [Compiled | Acc], NewCtx)
    end.

compile_params(ParamDefs, Params, Buckets) ->
    compile_params(ParamDefs, Params, [], Buckets).

compile_role({Name, Params}, CompileRole, Definitions, Snapshot) ->
    case lists:keyfind(Name, 1, Definitions) of
        {Name, ParamDefs, _Props, Permissions} ->
            case compile_params(ParamDefs, Params, Snapshot) of
                false ->
                    false;
                NewParams ->
                    {true, CompileRole(Name, NewParams, ParamDefs, Permissions)}
            end;
        false ->
            false
    end;
compile_role(Name, CompileRole, Definitions, Snapshot) when is_binary(Name) ->
    compile_role({Name, []}, CompileRole, Definitions, Snapshot).

compile_roles(CompileRole, Roles, Definitions, Snapshot) ->
    case do_compile_roles(CompileRole, Roles, Definitions, Snapshot) of
        try_another_version ->
            [_ | OtherDefinitions] =
                public_definitions(cluster_compat_mode:get_compat_version()),
            compile_roles_with_other_definitions(
              CompileRole, Roles, OtherDefinitions, Snapshot);
        Other ->
            Other
    end.

compile_roles_with_other_definitions(_CompileRole, _Roles, [], _Snapshot) ->
    exit(roles_impossible_to_compile);
compile_roles_with_other_definitions(
  CompileRole, Roles, [{Ver, GetDefinitions} | Rest], Snapshot) ->
    case Ver of
        undefined ->
            ?log_debug("Compile roles with latest definitions");
        _ ->
            ?log_debug(
               "Compile roles with definitions for version greater than  ~p",
               [Ver])
    end,
    case do_compile_roles(CompileRole, Roles,
                          GetDefinitions() ++ internal_roles(), Snapshot) of
        try_another_version ->
            compile_roles_with_other_definitions(
              CompileRole, Roles, Rest, Snapshot);
        Other ->
            Other
    end.

do_compile_roles(CompileRole, Roles, Definitions, Snapshot) ->
    try
        lists:filtermap(compile_role(_, CompileRole, Definitions, Snapshot),
                        Roles)
    catch
        T:E:S ->
            ?log_debug("Error compiling roles~n~p", [{T, E, S}]),
            case menelaus_users:upgrade_in_progress() of
                true ->
                    %% compilation crashed during unfinished upgrade
                    %% it could happen that the users database is already
                    %% upgraded, but we are still using old definitions
                    %% because cluster compat version is not yet updated
                    %% let's try to compile with newer definitions
                    try_another_version;
                false ->
                    error(E)
            end
    end.


-spec compile_roles([rbac_role()], [rbac_role_def()] | undefined, map()) ->
                           [rbac_compiled_role()].
compile_roles(_Roles, undefined, _Snapshot) ->
    %% can happen briefly after node joins the cluster on pre 5.0 clusters
    [];
compile_roles(Roles, Definitions, Snapshot) ->
    compile_roles(
      fun (_Name, Params, ParamDefs, Permissions) ->
              substitute_params(strip_ids(ParamDefs, Params),
                                ParamDefs, Permissions)
      end, Roles, Definitions, Snapshot).

get_roles(#authn_res{identity = Id,
                     extra_groups = Groups,
                     extra_roles = Roles}) ->
    ExtraRoles =
        lists:append([menelaus_users:get_group_roles(G) || G <- Groups]) ++
        Roles,
    IdentityRoles = get_roles_for_identity(Id),
    %% This check is done for optimization purposes.
    %% Most of the times ExtraRoles is empty, so there is no need in
    %% uniq(IdentityRoles) call which is not free.
    case ExtraRoles of
        [] -> IdentityRoles;
        _ -> lists:uniq(ExtraRoles ++ IdentityRoles)
    end;
get_roles({_, _} = Id) ->
    get_roles_for_identity(Id).

-spec get_roles_for_identity(rbac_identity()) -> [rbac_role()].
get_roles_for_identity({"", wrong_token}) ->
    case ns_config_auth:is_system_provisioned() of
        false ->
            [<<"admin">>];
        true ->
            []
    end;
get_roles_for_identity(?ANONYMOUS_IDENTITY) ->
    case ns_config_auth:is_system_provisioned() of
        false ->
            [<<"admin">>];
        true ->
            []
    end;
get_roles_for_identity({[$@ | _] = User, admin}) ->
    case cluster_compat_mode:is_cluster_totoro() of
        true ->
            %% service_admin omits credential permissions. Ongoing work to
            %% narrow service_admin to an explicit allow-list is tracked in
            %% MB-71508.
            CanonicalUser = misc:canonical_admin_identity(User),
            StoredRoles =
                try menelaus_users:get_roles({CanonicalUser, admin})
                catch _:_ ->
                        []
                end,
            [<<"service_admin">> | StoredRoles];
        false -> [<<"admin">>]
    end;
get_roles_for_identity({_User, admin}) ->
    [<<"admin">>];
get_roles_for_identity({_, local_token}) ->
    [<<"admin">>];
get_roles_for_identity({_, stats_reader}) ->
    [<<"stats_reader">>];
get_roles_for_identity({BucketName, bucket}) ->
    [{<<"bucket_full_access">>, [BucketName]}];
get_roles_for_identity({_User, external} = Identity) ->
    menelaus_users:get_roles(Identity);
get_roles_for_identity({_User, local} = Identity) ->
    menelaus_users:get_roles(Identity).

compiled_roles_cache_name() ->
    compiled_roles_cache.

start_compiled_roles_cache() ->
    UsersFilter =
        fun ({user_version, _V}) ->
                true;
            ({group_version, _V}) ->
                true;
            (_) ->
                false
        end,
    ConfigFilter =
        fun (cluster_compat_version) ->
                true;
            (rest_creds) ->
                true;
            (Key) ->
                collections:key_match(Key) =/= false orelse
                    ns_bucket:buckets_change(Key)
        end,
    GetVersion =
        fun () ->
                {cluster_compat_mode:get_compat_version(),
                 menelaus_users:get_users_version(),
                 menelaus_users:get_groups_version(),
                 ns_config_auth:is_system_provisioned(),
                 params_version()}
        end,
    GetEvents =
        case ns_node_disco:couchdb_node() == node() of
            true ->
                fun () ->
                        dist_manager:wait_for_node(
                          fun ns_node_disco:ns_server_node/0),
                        [{{user_storage_events, ns_node_disco:ns_server_node()},
                          UsersFilter},
                         {config_events, ConfigFilter}]
                end;
            false ->
                fun () ->
                        [{user_storage_events, UsersFilter},
                         {config_events, ConfigFilter}]
                end
        end,

    versioned_cache:start_link(
      compiled_roles_cache_name(), 200, fun build_compiled_roles/1,
      GetEvents, GetVersion).

get_compiled_roles(#authn_res{identity = {_, external}} = AuthnRes) ->
    roles_cache:build_compiled_roles(AuthnRes);
get_compiled_roles(#authn_res{identity = Identity,
                              extra_roles = ExtraRoles,
                              extra_groups = ExtraGroups}) ->
    %% Dropping everything but what we need for roles calculation here.
    %% Reason: We don't want things like session_id to be part of the cache
    %% key. In other words, if some user relogins, cache key should not
    %% change
    AuthnRes = #authn_res{identity = Identity,
                          extra_roles = ExtraRoles,
                          extra_groups = ExtraGroups},
    versioned_cache:get(compiled_roles_cache_name(), AuthnRes);
get_compiled_roles({_, _} = Identity) ->
    get_compiled_roles(#authn_res{identity = Identity}).

build_compiled_roles(#authn_res{identity = Identity} = AuthnRes) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            ?log_debug("Compile roles for user ~p",
                       [ns_config_log:tag_user_data(Identity)]),
            Definitions = get_definitions(all),
            compile_roles(get_roles(AuthnRes), Definitions,
                          get_roles_snapshot());
        true ->
            ?log_debug("Retrieve compiled roles for user ~p from ns_server "
                       "node", [ns_config_log:tag_user_data(Identity)]),
            rpc:call(ns_node_disco:ns_server_node(),
                     ?MODULE, build_compiled_roles, [AuthnRes])
    end.

filter_out_invalid_roles(Roles, Definitions, Snapshot) ->
    compile_roles(fun (Name, [], _, _) ->
                          Name;
                      (Name, Params, _, _) ->
                          {Name, Params}
                  end, Roles, Definitions, Snapshot).

get_permission_params({[V | _], _}) when is_tuple(V) ->
    case is_parameterized_vertex(element(1, V)) of
        true ->
            get_vertex_param_list(V);
        false ->
            []
    end;
get_permission_params(_) ->
    [].

calculate_possible_param_values(Snapshot, Combination, Permission) ->
    Len = length(Combination),
    PermissionParams = get_permission_params(Permission),
    RawParams =
        lists:usort(
          lists:map(
            fun (I) ->
                    misc:align_list(PermissionParams, I, any) ++
                        lists:duplicate(Len - I, any)
            end, lists:seq(0, length(Combination)))),
    lists:filtermap(
      fun (Params) ->
              case compile_params(Combination, Params, Snapshot) of
                  false ->
                      false;
                  Compiled ->
                      {true, Compiled}
              end
      end, RawParams).

all_params_combinations() ->
    [[], [bucket_name], ?RBAC_SCOPE_PARAMS, ?RBAC_COLLECTION_PARAMS,
     [credential_id]].

-spec calculate_possible_param_values(map(), undefined | rbac_permission()) ->
                                             rbac_all_param_values().
calculate_possible_param_values(Snapshot, Permission) ->
    [{Combination,
      calculate_possible_param_values(Snapshot, Combination, Permission)} ||
        Combination <- all_params_combinations()].

-spec get_possible_param_values([atom()], rbac_all_param_values()) ->
                                       [[rbac_role_param()]].
get_possible_param_values(ParamDefs, AllValues) ->
    {ParamDefs, Values} = lists:keyfind(ParamDefs, 1, AllValues),
    Values.

visible_roles_filter() ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            pipes:filter(fun ({_, _, Props, _}) -> Props =/= [] end);
        false ->
            pipes:filter(fun ({_, _, Props, _}) ->
                                 proplists:get_value(ce, Props, false)
                         end)
    end.

expand_params(AllPossibleValues) ->
    ?make_transducer(
       pipes:foreach(
         ?producer(),
         fun ({Role, [], Props, _}) ->
                 ?yield({Role, Props});
             ({Role, ParamDefs, Props, _}) ->
                 lists:foreach(
                   fun (Values) ->
                           ?yield({{Role, Values}, Props})
                   end, get_possible_param_values(ParamDefs, AllPossibleValues))
         end)).

filter_by_permission(undefined, _Snapshot, _Definitions) ->
    pipes:filter(fun (_) -> true end);
filter_by_permission(Permission, Snapshot, Definitions) ->
    pipes:filter(
      fun ({Role, _}) ->
              is_allowed(Permission,
                         compile_roles([Role], Definitions, Snapshot))
      end).

-spec produce_roles_by_permission(rbac_permission(), map()) ->
                                         pipes:producer(rbac_role()).
produce_roles_by_permission(Permission, Snapshot) ->
    AllValues = calculate_possible_param_values(Snapshot, Permission),
    Definitions = get_definitions(public),
    pipes:compose(
      [pipes:stream_list(Definitions),
       visible_roles_filter(),
       expand_params(AllValues),
       filter_by_permission(Permission, Snapshot, Definitions)]).

strip_id(_, any) ->
    any;
strip_id(_, {P, _Id}) ->
    P;
strip_id(bucket_name, P) ->
    %% to be removed as part of MB-38411
    P;
strip_id(credential_id, P) ->
    P.

-spec strip_ids([rbac_role_def_param()], [rbac_role_param()]) ->
                       [rbac_role_param()].
strip_ids(ParamDefs, Params) ->
    [strip_id(ParamDef, Param) || {ParamDef, Param} <-
                                      lists:zip(ParamDefs, Params)].

-spec get_param_defs(rbac_role_name(), [rbac_role_def()]) ->
                            not_found | [rbac_role_def_param()].
get_param_defs(RoleName, Definitions) ->
    case lists:keyfind(RoleName, 1, Definitions) of
        {RoleName, ParamsDef, _, _} ->
            ParamsDef;
        _ ->
            not_found
    end.

-spec validate_role(rbac_role(), [rbac_role_def()], map()) ->
          false | {ok, rbac_role()}.
validate_role(Role, Definitions, Snapshot) when is_binary(Role) ->
    validate_role(Role, [], Definitions, Snapshot);
validate_role({Role, Params}, Definitions, Snapshot) when is_binary(Role) ->
    validate_role(Role, Params, Definitions, Snapshot).

validate_role(Role, Params, Definitions, Snapshot) ->
    case lists:keyfind(Role, 1, Definitions) of
        {Role, ParamsDef, _, _} when length(Params) =:= length(ParamsDef) ->
            case compile_params(ParamsDef, Params, Snapshot) of
                false ->
                    false;
                [] ->
                    {ok, Role};
                Expanded ->
                    {ok, {Role, Expanded}}
            end;
        _ ->
            false
    end.

get_visible_role_definitions() ->
    pipes:run(pipes:stream_list(get_definitions(public)),
              visible_roles_filter(),
              pipes:collect()).

-spec validate_roles([rbac_role()]) ->
          {GoodRoles :: [rbac_role()], BadRoles :: [rbac_role()]}.
validate_roles(Roles) ->
    validate_roles(Roles, public).

-spec validate_roles([rbac_role()], public | all) ->
          {GoodRoles :: [rbac_role()], BadRoles :: [rbac_role()]}.
validate_roles(Roles, Scope) ->
    validate_roles(Roles, Scope, get_roles_snapshot()).

%% @doc Build a snapshot suitable for role validation/compilation: the bucket
%% collections/uuid snapshot plus the credential-id index used to check that
%% `credential_consumer' params refer to an existing credential.
-spec get_roles_snapshot() -> map().
get_roles_snapshot() ->
    chronicle_compat:get_snapshot(
      [ns_bucket:fetch_snapshot(all, _, [collections, uuid]),
       cb_credentials_store:fetch_index_snapshot(_)]).

-spec validate_roles([rbac_role()], public | all, map()) ->
          {GoodRoles :: [rbac_role()], BadRoles :: [rbac_role()]}.
validate_roles(Roles, Scope, Snapshot) ->
    Definitions =
        case Scope of
            public ->
                get_visible_role_definitions();
            all ->
                get_definitions(all)
        end,
    lists:foldl(fun (Role, {Validated, Unknown}) ->
                        case validate_role(Role, Definitions, Snapshot) of
                            false ->
                                {Validated, [Role | Unknown]};
                            {ok, R} ->
                                {[R | Validated], Unknown}
                        end
                end, {[], []}, Roles).

-spec get_security_roles(map()) -> [rbac_role()].
get_security_roles(Snapshot) ->
    %% A role is security-classified if it grants any operation on
    %% [admin, security], or `read'/`write' on the credentials vertex.
    %% credential CRUD is Security Admin's lane; `consume' is the User Admin
    %% delegation capability (granted via `credential_consumer').
    Collect = fun (Permission) ->
                      pipes:run(produce_roles_by_permission(Permission,
                                                            Snapshot),
                                pipes:collect())
              end,
    lists:usort(Collect({[admin, security], any}) ++
                Collect({[{credentials, any}], read}) ++
                Collect({[{credentials, any}], write})).

-spec get_user_admin_roles(map()) -> [rbac_role()].
get_user_admin_roles(Snapshot) ->
    pipes:run(produce_roles_by_permission({[admin, users], any}, Snapshot),
              pipes:collect()).

external_auth_polling_interval() ->
    ns_config:read_key_fast(external_auth_polling_interval,
                            ?DEFAULT_EXTERNAL_ROLES_POLLING_INTERVAL).

-spec extract_role_name(rbac_role_def()) -> atom().
extract_role_name(Role) ->
    {Name, _, _, _} = Role,
    Name.

-spec old_role_to_new(atom() | {atom(), nonempty_list(rbac_role_param())} |
                      rbac_role()) -> rbac_role().
old_role_to_new(RoleAtom) when is_atom(RoleAtom) ->
    atom_to_binary(RoleAtom);
old_role_to_new({RoleAtom, Params}) when is_atom(RoleAtom) ->
    {atom_to_binary(RoleAtom), Params};
old_role_to_new(RoleBinary) when is_binary(RoleBinary) ->
    RoleBinary;
old_role_to_new({RoleBinary, _} = Role) when is_binary(RoleBinary) ->
    Role.

-spec new_role_to_old(rbac_role()) ->
          atom() | {atom(), nonempty_list(rbac_role_param())} | rbac_role().
new_role_to_old(RoleAtom) when is_atom(RoleAtom) ->
    RoleAtom;
new_role_to_old({RoleAtom, _} = Role) when is_atom(RoleAtom) ->
    Role;
new_role_to_old(RoleBinary) when is_binary(RoleBinary) ->
    binary_to_atom(RoleBinary);
new_role_to_old({RoleBinary, Params}) when is_binary(RoleBinary) ->
    {binary_to_atom(RoleBinary), Params}.

%% Maps new roles with binary name to old style atom names
%% Note, this includes a call to binary_to_atom/1, so this should only be called
%% on roles that have already been validated as one of the limited builtin roles
-spec map_roles_for_compat([rbac_role()], list()) ->
    [atom() | {atom(), nonempty_list(rbac_role_param())} | rbac_role()].
map_roles_for_compat(Roles) ->
    map_roles_for_compat(Roles, cluster_compat_mode:get_compat_version()).

map_roles_for_compat(Roles, Version) ->
    case cluster_compat_mode:is_version_totoro(Version) of
        true ->
            Roles;
        false ->
            lists:map(fun new_role_to_old/1, Roles)
    end.

chronicle_upgrade_to_totoro(ChronicleTxn) ->
    RoleDefinitions = default_roles_totoro(),
    chronicle_upgrade:set_key(role_definitions, RoleDefinitions,
                              ChronicleTxn).

-spec get_role(binary()) ->
          {binary(), [], proplists:proplist(),
           proplists:proplist()} | undefined.
get_role(Name) ->
    Roles = chronicle_compat:get(role_definitions, #{required => true}),
    case lists:keyfind(Name, 1, Roles) of
        {_, _, _, _} = Role -> Role;
        false -> undefined
    end.

-spec is_mutable(rbac_role_name(), [rbac_role_def()], term()) ->
    boolean() | term().
is_mutable(RoleId, Definitions, Default) ->
    case lists:keyfind(RoleId, 1, Definitions) of
        false -> Default;
        Role -> is_mutable(Role)
    end.

-spec is_mutable(rbac_role_def() | rbac_role()) -> boolean().
is_mutable({_, _, Props, _}) ->
    proplists:get_bool(mutable, Props).

-spec get_all_mutable_roles() -> [rbac_role_def()].
get_all_mutable_roles() ->
    lists:filter(fun is_mutable/1, role_definitions()).

-spec set_role(rbac_role_def()) -> ok | {error, immutable}.
set_role({RoleId, _, _, _} = NewRole) ->
    Result = chronicle_compat:txn(
        fun (Txn) ->
                case chronicle_compat:txn_get(role_definitions, Txn) of
                    {error, _} = Err ->
                        {abort, Err};
                    {ok, {Defs, _}} ->
                        WasMutable = is_mutable(RoleId, Defs, true),
                        WillBeMutable = is_mutable(NewRole),
                        case WasMutable andalso WillBeMutable of
                            true ->
                                NewDefs = lists:keystore(RoleId, 1, Defs,
                                                         NewRole),
                                {commit, [{set, role_definitions, NewDefs}]};
                            false ->
                                {abort, {error, immutable}}
                        end
                end
        end),
    case Result of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.

-spec delete_role(rbac_role_name()) -> ok | {error, not_found | immutable}.
delete_role(RoleId) ->
    Result = chronicle_compat:txn(
        fun (Txn) ->
                case chronicle_compat:txn_get(role_definitions, Txn) of
                    {error, _} = Err ->
                        {abort, Err};
                    {ok, {Defs, _}} ->
                         case is_mutable(RoleId, Defs, not_found) of
                             true ->
                                 NewDefs = lists:keydelete(RoleId, 1, Defs),
                                 {commit, [{set, role_definitions, NewDefs}]};
                             false ->
                                 {abort, {error, immutable}};
                             not_found ->
                                 {abort, {error, not_found}}
                         end
                end
        end),
    case Result of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.

diff_roles(NewRoles, OldRoles) ->
    [{added_role, Role} || {Name, _, _, _} = Role <- NewRoles,
                           not lists:keymember(Name, 1, OldRoles)] ++
        [{deleted_role, Role} || {Name, _, _, _} = Role <- OldRoles,
                                 not lists:keymember(Name, 1, NewRoles)] ++
        [{updated_role, NewRole} ||
            {Name, _, _, _} = NewRole <- NewRoles,
            lists:keymember(Name, 1, OldRoles),
            lists:keyfind(Name, 1, OldRoles) =/= NewRole].

-ifdef(TEST).

diff_roles_test() ->
    RoleA_0 = {<<"role_a">>, [], [], []},
    RoleB_0 = {<<"role_b">>, [], [], []},
    RoleB_1 = {<<"role_b">>, [], [{desc, "added desc"}], []},
    RoleC_0 = {<<"role_c">>, [], [], []},
    ?assertEqual([],
                 diff_roles([RoleC_0, RoleB_0, RoleA_0],
                            [RoleA_0, RoleB_0, RoleC_0])),
    ?assertEqual([{added_role, RoleC_0}],
                 diff_roles([RoleC_0, RoleB_0, RoleA_0],
                            [RoleA_0, RoleB_0])),
    ?assertEqual([{deleted_role, RoleC_0}],
                 diff_roles([RoleB_0, RoleA_0],
                            [RoleA_0, RoleB_0, RoleC_0])),
    ?assertEqual([{updated_role, RoleB_1}],
                 diff_roles([RoleC_0, RoleB_1, RoleA_0],
                            [RoleA_0, RoleB_0, RoleC_0])),
    ?assertEqual([{added_role, RoleB_0}, {deleted_role, RoleC_0}],
                 diff_roles([RoleB_0, RoleA_0],
                            [RoleA_0, RoleC_0])),
    ?assertEqual([{added_role, RoleC_0},
                  {deleted_role, RoleA_0},
                  {updated_role, RoleB_1}],
                 diff_roles([RoleC_0, RoleB_1],
                            [RoleA_0, RoleB_0])).


set_role_definitions() ->
    fake_chronicle_kv:update_snapshot(
      #{role_definitions => default_roles_totoro()}).

setup_meck() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> true end),
    meck:expect(cluster_compat_mode, is_cluster_79,
        fun () -> true end),
    meck:expect(cluster_compat_mode, is_enterprise,
                fun () -> true end),
    meck:expect(cluster_compat_mode, get_compat_version,
                fun () -> ?LATEST_VERSION_NUM end),
    meck:expect(cluster_compat_mode, is_developer_preview,
                fun () -> false end),
    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, search_node_with_default,
                fun (_, Default) -> Default end),
    meck:new(ns_bucket, [passthrough]).

filter_out_invalid_roles_test() ->
    Roles = [{role1, [{"bucket1", <<"id1">>}]},
             {role2, [{"bucket2", <<"id2">>}]}],
    Definitions = [{role1, [bucket_name],
                    [{name,<<"">>},{desc, <<"">>}],
                    [{[{bucket,bucket_name},settings],[read]}]},
                   {role2, [bucket_name],
                    [{name,<<"">>},{desc, <<"">>}],
                    [{[{bucket,bucket_name},n1ql,update],[execute]}]}],
    Snapshot = ns_bucket:toy_buckets([{"bucket1", [{uuid, <<"id1">>}]}]),
    ?assertEqual([{role1, [{"bucket1", <<"id1">>}]}],
                 filter_out_invalid_roles(Roles, Definitions, Snapshot)).

object_match(Param, Filter) ->
    {_, Match} = match_object(lists:flatmap(expand_vertex(_, all), Param),
                              lists:flatmap(expand_vertex(_, any), Filter),
                              strict),
    Match =/= mismatch.

%% assertEqual is used instead of assert and assertNot to avoid
%% dialyzer warnings
object_match_test() ->
    ?assertEqual(true, object_match([], [])),
    ?assertEqual(false, object_match([], [o1, o2])),
    ?assertEqual(true, object_match([o3], [])),
    ?assertEqual(true, object_match([o1, o2], [o1, o2])),
    ?assertEqual(false, object_match([o1], [o1, o2])),
    ?assertEqual(true, object_match([o1, o2], [o1])),
    ?assertEqual(true, object_match([{bucket, "a"}], [{bucket, "a"}])),
    ?assertEqual(false, object_match([{bucket, "a"}], [{bucket, "b"}])),
    ?assertEqual(true, object_match([{bucket, any}], [{bucket, "b"}])),
    ?assertEqual(true, object_match([{bucket, "a"}], [{bucket, any}])),
    ?assertEqual(true, object_match([{bucket, any}], [{bucket, any}])),
    ?assertEqual(true, object_match([{bucket, all}], [{bucket, any}])),
    ?assertEqual(false, object_match([{bucket, all}], [{bucket, "a"}])).

object_match_with_collections_test() ->
    ?assertEqual(true, object_match([{collection, ["b", "s", "c"]}],
                                    [{bucket, "b"}])),
    ?assertEqual(true, object_match([{collection, ["b", "s", all]}],
                                    [{bucket, "b"}])),
    ?assertEqual(true, object_match([{collection, ["b", all, all]}],
                                    [{bucket, "b"}])),
    ?assertEqual(true, object_match([{scope, ["b", "s"]}], [{bucket, "b"}])),
    ?assertEqual(true, object_match([{scope, ["b", all]}], [{bucket, "b"}])),
    ?assertEqual(true, object_match([{collection, ["b", "s", "c"]}],
                                    [{collection, ["b", "s", "c"]}])),
    ?assertEqual(false, object_match([{collection, ["b", "s", "c1"]}],
                                     [{collection, ["b", "s", "c"]}])),
    ?assertEqual(false, object_match([{collection, ["b", "s", all]}],
                                     [{collection, ["b", "s", "c"]}])),
    ?assertEqual(true, object_match([{collection, ["b", "s", any]}],
                                    [{collection, ["b", "s", "c"]}])),
    ?assertEqual(false, object_match([{scope, ["b", "s"]}],
                                     [{collection, ["b", "s", "c"]}])),
    ?assertEqual(true, object_match([{scope, ["b", "s"]}],
                                    [{collection, ["b", "s", any]}])),
    ?assertEqual(false, object_match([{bucket, "b"}],
                                     [{collection, ["b", "s", "c"]}])),
    ?assertEqual(false, object_match([{bucket, "b"}],
                                     [{collection, ["b", "s", any]}])),
    ?assertEqual(true, object_match([{bucket, "b"}],
                                    [{collection, ["b", any, any]}])),
    ?assertEqual(false, object_match([{bucket, "b"}],
                                     [{collection, ["b1", any, any]}])).

credential_match_test() ->
    ?assertEqual(true, credential_match("backup/prod/s3", "backup/prod/s3")),
    ?assertEqual(true, credential_match("backup/dev/test", "backup/*")),
    ?assertEqual(false, credential_match("other/key", "backup/*")),
    ?assertEqual(true, credential_match("anything", any)),
    ?assertEqual(false, credential_match("something", "other")),
    ?assertEqual(true, credential_match("backup/prod/s3/deep",
                                        "backup/prod/s3/*")),
    ?assertEqual(false, credential_match("backup/prod", "backup/prod/s3")),
    ?assertEqual(false, credential_match("other/test", "backup/prod/*")).

object_match_with_credentials_test() ->
    ?assertEqual(true, object_match([{credentials, "backup/prod/s3"}],
                                    [{credentials, "backup/prod/s3"}])),
    ?assertEqual(false, object_match([{credentials, "backup/prod/s3"}],
                                     [{credentials, "other/key"}])),
    ?assertEqual(true, object_match([{credentials, "backup/prod/s3"}],
                                    [{credentials, any}])),
    ?assertEqual(true, object_match([{credentials, "backup/dev/test"}],
                                    [{credentials, "backup/*"}])),
    ?assertEqual(false, object_match([{credentials, "other/key"}],
                                     [{credentials, "backup/*"}])),
    ?assertEqual(true, object_match([{credentials, "backup/prod/s3/deep"}],
                                    [{credentials, "backup/prod/s3/*"}])),
    ?assertEqual(false, object_match([], [{credentials, "backup/prod/s3"}])),
    ?assertEqual(true, object_match([{credentials, "backup/prod/s3"}], [])).

is_valid_credential_id_test() ->
    Ids = ["backup/prod/s3", "backup/dev/test", "other/key"],
    ?assertEqual(true,  is_valid_credential_id(any, [])),
    ?assertEqual(true,  is_valid_credential_id(any, Ids)),
    ?assertEqual(true,  is_valid_credential_id("*", [])),
    ?assertEqual(true,  is_valid_credential_id("backup/prod/s3", Ids)),
    ?assertEqual(false, is_valid_credential_id("does/not/exist", Ids)),
    ?assertEqual(false, is_valid_credential_id("backup/prod/s3", [])),
    ?assertEqual(true,  is_valid_credential_id("backup/*", Ids)),
    ?assertEqual(true,  is_valid_credential_id("backup/prod/*", Ids)),
    ?assertEqual(false, is_valid_credential_id("nomatch/*", Ids)),
    ?assertEqual(false, is_valid_credential_id("backup/*", [])),
    ?assertEqual(false, is_valid_credential_id(<<"foo">>, Ids)).

toy_buckets_props() ->
    [{"test", [{uuid, <<"test_id">>}, {props, toy_props()}]},
     {"default", [{uuid, <<"default_id">>}, {collections, toy_manifest()},
                  {props, toy_props()}]}].

toy_buckets() ->
    maps:put(?CREDENTIAL_IDS_KEY,
             {["backup/prod/s3", "backup/dev/test", "other/key", "test"],
              {<<"toy">>, 0}},
             ns_bucket:toy_buckets(toy_buckets_props())).

toy_manifest() ->
    [{uid, 2},
     {scopes, [{"s",  [{uid, 1}, {collections, [{"c",  [{uid, 1}]},
                                                {"c1", [{uid, 2}]}]}]},
               {"s1", [{uid, 2}, {collections, [{"c",  [{uid, 3}]}]}]}]}].

toy_props() ->
    [{storage_mode, magma}, {type, membase}].

compile_roles(Roles, Definitions) ->
    compile_roles(Roles, Definitions, toy_buckets()).

compile_roles_test() ->
    StripId = fun ({N, _Id}) -> N; (N) -> N end,
    PermissionFilters =
        fun([B, S, C]) ->
                [{[{bucket, StripId(B)}], oper1},
                 {[{bucket, any}, docs], oper2},
                 {[v1, v2], oper3},
                 {[{collection, [StripId(B), StripId(S), StripId(C)]}],
                  oper4}]
        end,

    Definitions = [{<<"simple_role">>, [], [],
                    [{[admin], all}]},
                   {<<"test_role">>, [bucket_name], [],
                    [{[{bucket, bucket_name}], none}]},
                   {<<"test_role1">>, ?RBAC_COLLECTION_PARAMS, [],
                    PermissionFilters(?RBAC_COLLECTION_PARAMS)},
                   {<<"test_role2">>, ?RBAC_SCOPE_PARAMS, [],
                    PermissionFilters(?RBAC_SCOPE_PARAMS ++ [any])}],

    ?assertEqual([[{[admin], all}]],
                 compile_roles([<<"simple_role">>, <<"wrong_role">>],
                               Definitions)),
    ?assertEqual([[{[{bucket, "test"}], none}]],
                 compile_roles([{<<"test_role">>, ["test"]}], Definitions)),
    ?assertEqual([[{[{bucket, "test"}], none}]],
                 compile_roles([{<<"test_role">>, [{"test", <<"test_id">>}]}],
                               Definitions)),
    ?assertEqual([], compile_roles([{<<"test_role">>,
                                     [{"test", <<"wrong_id">>}]}],
                                   Definitions)),

    TestRole =
        fun (Success, Role, RoleParams, ParamsForExpected) ->
                Expected = [PermissionFilters(ParamsForExpected) || Success],
                ?assertEqual(Expected,
                             compile_roles([{Role, RoleParams}],
                                           Definitions))
        end,

    TestRole1 = ?cut(TestRole(_1, <<"test_role1">>, _2, _2)),
    TestRole1(true, ["default", "s", "c"]),
    TestRole1(true, [{"default", <<"default_id">>}, {"s", 1}, {"c", 1}]),
    TestRole1(true, [{"default", <<"default_id">>}, {"s", 1}, any]),
    TestRole1(true, [{"default", <<"default_id">>}, any, any]),
    TestRole1(true, [any, any, any]),
    TestRole1(false, [{"default", <<"wrong_id">>}, {"s", 1}, {"c", 1}]),
    TestRole1(false, [{"default", <<"default_id">>}, {"s", 1}, {"c", 2}]),

    TestRole2 = ?cut(TestRole(_1, <<"test_role2">>, _2, _2 ++ [any])),
    TestRole2(true, ["default", "s"]),
    TestRole2(true, [{"default", <<"default_id">>}, {"s", 1}]),
    TestRole2(true, [{"default", <<"default_id">>}, any]),
    TestRole2(true, [any, any]),
    TestRole1(false, [{"default", <<"wrong_id">>}, {"s", 1}]),
    TestRole1(false, [{"default", <<"default_id">>}, {"s", 2}]).

assert_admin_permissions(Roles) ->
    ?assertEqual(true, is_allowed({[buckets], create}, Roles)),
    ?assertEqual(true, is_allowed({[something, something], anything}, Roles)).

admin_test__() ->
    Roles = compile_roles([<<"admin">>], roles()),
    assert_admin_permissions(Roles),
    ?assertEqual(true, is_allowed({[{credentials, "test"}], read}, Roles)),
    ?assertEqual(true, is_allowed({[{credentials, "test"}], write}, Roles)),
    ?assertEqual(true, is_allowed({[{credentials, "test"}], list}, Roles)),
    ?assertEqual(true, is_allowed({[{credentials, "test"}], consume}, Roles)).

service_admin_test__() ->
    Roles = compile_roles([<<"service_admin">>],
                          roles() ++ internal_roles()),
    assert_admin_permissions(Roles),
    ?assertEqual(false, is_allowed({[{credentials, "test"}], read}, Roles)),
    ?assertEqual(false, is_allowed({[{credentials, "test"}], write}, Roles)),
    ?assertEqual(false, is_allowed({[{credentials, "test"}], list}, Roles)),
    ?assertEqual(false, is_allowed({[{credentials, "test"}], consume}, Roles)).

service_admin_with_credential_consumer_test__() ->
    Roles = compile_roles(
              [<<"service_admin">>,
               {<<"credential_consumer">>, ["test"]}],
              roles() ++ internal_roles()),
    ?assertEqual(true, is_allowed({[{credentials, "test"}], consume}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{credentials, "other"}], consume}, Roles)).

%% This guards against future drift: a new role that forgets to exclude the
%% credentials vertex (typically `{[{credentials, any}], none}' before a
%% `{[], _}' catch-all) will fail here with the offending role and op.
credentials_access_matrix_test__() ->
    Defs = roles() ++ internal_roles(),
    %% {Read, Write, Consume}; missing roles default to all-false.
    Exceptions =
        #{<<"admin">>               => {true, true,  true},
          <<"security_admin">>      => {true, true,  false},
          <<"ro_security_admin">>   => {true, false, false},
          <<"credential_consumer">> => {false, false, true}},
    lists:foreach(
      fun ({Name, ParamDefs, _Props, _Perms}) ->
              RoleSpec = case ParamDefs of
                             [] -> Name;
                             _  -> {Name, [any || _ <- ParamDefs]}
                         end,
              Compiled = compile_roles([RoleSpec], Defs),
              {ER, EW, EC} =
                  maps:get(Name, Exceptions, {false, false, false}),
              Check =
                  fun (Op, Expected) ->
                          Actual = is_allowed(
                                     {[{credentials, "test"}], Op}, Compiled),
                          ?assertEqual({Name, Op, Expected},
                                       {Name, Op, Actual})
                  end,
              Check(read, ER),
              Check(write, EW),
              Check(consume, EC)
      end, Defs).

cluster_admin_test__() ->
    Roles = compile_roles([<<"cluster_admin">>], roles()),
    ?assertEqual(true, is_allowed({[settings, metrics], any}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], any}, Roles)).

eventing_admin_test__() ->
    Roles = compile_roles([<<"eventing_admin">>], roles()),
    ?assertEqual(false, is_allowed({[admin], any}, Roles)),
    ?assertEqual(false, is_allowed({[xdcr], any}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, xdcr], any}, Roles)),
    ?assertEqual(true, is_allowed({[buckets], create}, Roles)),
    ?assertEqual(true, is_allowed({[n1ql], all}, Roles)),
    ?assertEqual(true, is_allowed({[analytics], all}, Roles)),
    ?assertEqual(true, is_allowed({[eventing], all}, Roles)),
    ?assertEqual(true, is_allowed({[anything], read}, Roles)),
    ?assertEqual(false, is_allowed({[anything], write}, Roles)).

backup_admin_test__() ->
    Roles = compile_roles([<<"backup_admin">>], roles()),
    ?assertEqual(false, is_allowed({[admin, users], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin], any}, Roles)),
    ?assertEqual(true, is_allowed({[buckets], create}, Roles)),
    ?assertEqual(true, is_allowed({[backup], all}, Roles)),
    ?assertEqual(true, is_allowed({[anything], all}, Roles)).

ro_admin_test__() ->
    Roles = compile_roles([<<"ro_admin">>], roles()),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, data], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "test"}, something], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "test"}, something], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security_info], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, metakv], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, other], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], read}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], write}, Roles)),
    ?assertEqual(true, is_allowed({[settings, metrics], read}, Roles)),
    ?assertEqual(true, is_allowed({[anything], read}, Roles)),
    ?assertEqual(true, is_allowed({[backup], read}, Roles)),
    ?assertEqual(false, is_allowed({[anything], write}, Roles)).

security_admin_test__() ->
    Roles = compile_roles([<<"security_admin">>], roles()),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, data], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "test"}, something], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "test"}, something], write}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security], write}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security_info], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, metakv], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, other], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], read}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], read}, Roles)),
    ?assertEqual(true, is_allowed({[anything], read}, Roles)),
    ?assertEqual(false, is_allowed({[backup], read}, Roles)),
    ?assertEqual(false, is_allowed({[anything], write}, Roles)).

ro_security_admin_test__() ->
    Roles = compile_roles([<<"ro_security_admin">>], roles()),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, data], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "test"}, something], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "test"}, something], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], write}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security_info], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, metakv], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, other], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], read}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], read}, Roles)),
    ?assertEqual(true, is_allowed({[anything], read}, Roles)),
    ?assertEqual(false, is_allowed({[backup], read}, Roles)),
    ?assertEqual(false, is_allowed({[anything], write}, Roles)).

user_admin_local_test__() ->
    Roles = compile_roles([<<"user_admin_local">>], roles()),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, data], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "test"}, something], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "test"}, something], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security_info], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users], write}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users, local], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users, local], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users, external], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users, external], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, metakv], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, other], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], read}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], read}, Roles)),
    ?assertEqual(true, is_allowed({[anything], read}, Roles)),
    ?assertEqual(false, is_allowed({[backup], read}, Roles)),
    ?assertEqual(false, is_allowed({[anything], write}, Roles)).

user_admin_external_test__() ->
    Roles = compile_roles([<<"user_admin_external">>], roles()),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, data], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "test"}, something], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "test"}, something], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security_info], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users, local], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, users, local], write}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users, external], read}, Roles)),
    ?assertEqual(true, is_allowed({[admin, users, external], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, metakv], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, other], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, settings, metrics], read}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], write}, Roles)),
    ?assertEqual(false, is_allowed({[settings, metrics], read}, Roles)),
    ?assertEqual(true, is_allowed({[anything], read}, Roles)),
    ?assertEqual(false, is_allowed({[backup], read}, Roles)),
    ?assertEqual(false, is_allowed({[anything], write}, Roles)).

%% Extracts all the role names and bucket argument (if applicable).
extract_all_names(Roles) ->
    lists:foldl(
      fun ({Name, [], _Description, _Filters}, AccIn) ->
              [{Name, []} | AccIn];
          ({Name, [bucket_name], _Description, _Filters}, AccIn) ->
              [{Name, ["default"]} | AccIn];
          ({Name, [credential_id], _Description, _Filters}, AccIn) ->
              [{Name, ["default_credential"]} | AccIn];
          ({Name, ?RBAC_SCOPE_PARAMS, _Description, _Filters}, AccIn) ->
              [{Name, ["default", "s"]} | AccIn];
          ({Name, ?RBAC_COLLECTION_PARAMS, _Description, _Filters},
           AccIn) ->
              [{Name, ["default", "s", "c"]} | AccIn]
      end, [], Roles).

remove_exempted_names(AllNames, ExemptedNames) ->
    lists:filter(
      fun ({Name, _}) ->
              not lists:member(Name, ExemptedNames)
      end, AllNames).

roles_with_admin_event_metakv_permissions() ->
    [<<"regulator_access">>, <<"admin">>].

%% Ensure none of the roles, except those who are granted permission,
%% have access to the admin event/metakv permission.
admin_event_metakv_permissions_test__() ->
    AllRoles = roles(),
    AllNames0 = extract_all_names(AllRoles),
    AllNames =
        remove_exempted_names(AllNames0,
                              roles_with_admin_event_metakv_permissions()),
    Roles = compile_roles(AllNames, AllRoles),

    ?assertEqual(false, is_allowed({[admin, event], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, event], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, metakv], write}, Roles)).

roles_bucket_sys_write_permissions() ->
    [<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>, <<"data_backup">>].

system_collections_write_permissions_test__() ->
    AllRoles = roles(),
    AllNames = extract_all_names(AllRoles),

    {SysWrite, NoSysWrite} =
        lists:partition(
          fun ({Name, _}) ->
                  lists:member(Name, roles_bucket_sys_write_permissions())
          end, AllNames),

    %% Ensure that all roles in SysWrite can write to all system collections.
    lists:foreach(
      fun(Name) ->
              Roles = compile_roles([Name], AllRoles),
              ?assertEqual(true, is_allowed({[{bucket, "default"},
                                              data, docs], swrite}, Roles)),
              ?assertEqual(true, is_allowed({[{collection,
                                               ["default", "s", "c"]},
                                              data, docs], swrite}, Roles))
      end, SysWrite),

    %% Ensure none of the roles in NoSysWrite can write to system collections.
    Roles0 = compile_roles(NoSysWrite, AllRoles),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, data, docs], swrite},
                                   Roles0)),
    ?assertEqual(false, is_allowed({[{collection, ["default", "s", "c"]},
                                     data, docs], swrite}, Roles0)),

    %% Ensure that mobile_sync_gateway can write only to _mobile and not to
    %% other system collections.
    Roles1 = compile_roles([{<<"mobile_sync_gateway">>, ["default"]}],
        AllRoles),
    ?assertEqual(true, is_allowed({[{collection, ["default",
                                                  ?SYSTEM_SCOPE_NAME,
                                                  "_mobile"]},
                                    data, docs], swrite}, Roles1)),

    ?assertEqual(false, is_allowed({[{collection, ["default",
                                                   ?SYSTEM_SCOPE_NAME,
                                                   "_query"]},
                                     data, docs], swrite}, Roles1)).

system_collections_read_permissions_test__() ->
    AllRoles = roles(),
    AllNames = extract_all_names(AllRoles),

    %% For now, we're retaining the ability to read from system collections -
    %% avoid filtering out system collections in DCP streams (when DCPProducer
    %% is set at the bucket level or DCPStream at the collection level). If
    %% docs can be read in non-system collections, allow reads from system
    %% collections too.
    lists:foreach(
      fun(Name)->
              Roles = compile_roles([Name], AllRoles),
              Perms0 = [{[{bucket, "default"}, data], read},
                        {[{bucket, "default"}, data, docs], read},
                        {[{bucket, "default"}, data, dcp], read}],
              Allowed0 = lists:any(is_allowed(_, Roles), Perms0),
              ?assertEqual(Allowed0,
                           is_allowed({[{bucket, "default"},
                                        data, docs], sread}, Roles)),
              Perms1 = [{[{collection, ["default", "s", "c"]},
                          data], read},
                        {[{collection, ["default", "s", "c"]},
                          data, docs], read},
                        {[{collection, ["default", "s", "c"]},
                          data, dcpstream], read}],
              Allowed1 = lists:any(is_allowed(_, Roles), Perms1),
              ?assertEqual(Allowed1,
                           is_allowed({[{collection,
                                         ["default", "s", "c"]},
                                        data, docs], sread}, Roles))
      end, AllNames),

    %% mobile_sync_gateway can read from all system collections.
    Roles = compile_roles([{<<"mobile_sync_gateway">>, ["test"]}], AllRoles),
    ?assertEqual(true, is_allowed({[{collection,
                                     ["test", ?SYSTEM_SCOPE_NAME, "_mobile"]},
                                    data, docs], sread}, Roles)),
    ?assertEqual(true, is_allowed({[{collection,
                                     ["test", ?SYSTEM_SCOPE_NAME, "_query"]},
                                    data, docs], sread}, Roles)).

bucket_views_admin_check_global(Roles) ->
    ?assertEqual(false, is_allowed({[xdcr], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin], read}, Roles)),
    ?assertEqual(true, is_allowed({[something], read}, Roles)),
    ?assertEqual(false, is_allowed({[something], write}, Roles)),
    ?assertEqual(false, is_allowed({[buckets], create}, Roles)).

bucket_views_admin_check_another(Roles) ->
    ?assertEqual(false, is_allowed({[{bucket, "another"}, xdcr], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "another"}, views], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "another"}, data], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "another"}, settings], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "another"}, settings], write}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "another"}], read}, Roles)),
    ?assertEqual(false, is_allowed({[buckets], create}, Roles)).

bucket_admin_check_default(Roles) ->
    ?assertEqual(true, is_allowed({[{bucket, "default"}, xdcr], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "default"}, xdcr], execute}, Roles)),
    ?assertEqual(
       true, is_allowed({[{bucket, "default"}, anything], anything}, Roles)),
    ?assertEqual(
       true, is_allowed({[{bucket, "default"}, anything], anything}, Roles)).

bucket_admin_test__() ->
    Roles = compile_roles([{<<"bucket_admin">>, ["default"]}], roles()),
    bucket_admin_check_default(Roles),
    bucket_views_admin_check_another(Roles),
    bucket_views_admin_check_global(Roles).

bucket_admin_wildcard_test__() ->
    Roles = compile_roles([{<<"bucket_admin">>, [any]}], roles()),
    bucket_admin_check_default(Roles),
    bucket_views_admin_check_global(Roles).

views_admin_check_default(Roles) ->
    ?assertEqual(true,
                 is_allowed({[{bucket, "default"}, views], anything}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "default"}, data], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "default"}, data], write}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "default"}, settings], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "default"}, settings], write}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}], read}, Roles)).

views_admin_test__() ->
    Roles = compile_roles([{<<"views_admin">>, ["default"]}], roles()),
    views_admin_check_default(Roles),
    bucket_views_admin_check_another(Roles),
    bucket_views_admin_check_global(Roles).

views_admin_wildcard_test__() ->
    Roles = compile_roles([{<<"views_admin">>, [any]}], roles()),
    views_admin_check_default(Roles),
    bucket_views_admin_check_global(Roles).

bucket_full_access_check(Roles, Bucket, Allowed) ->
    ?assertEqual(Allowed,
                 is_allowed({[{bucket, Bucket}, data], anything}, Roles)),
    ?assertEqual(Allowed, is_allowed({[{bucket, Bucket}], flush}, Roles)),
    ?assertEqual(Allowed, is_allowed({[{bucket, Bucket}], flush}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, Bucket}], write}, Roles)).

bucket_full_access_test__() ->
    Roles = compile_roles([{<<"bucket_full_access">>, ["default"]}], roles()),
    bucket_full_access_check(Roles, "default", true),
    bucket_full_access_check(Roles, "another", false),
    ?assertEqual(true, is_allowed({[pools], read}, Roles)),
    ?assertEqual(false, is_allowed({[another], read}, Roles)).

replication_admin_test__() ->
    Roles = compile_roles([<<"replication_admin">>], roles()),
    ?assertEqual(true,
                 is_allowed({[{bucket, "default"}, xdcr], anything}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "default"}, views], read}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "default"}, settings], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "default"}, settings], write}, Roles)),
    ?assertEqual(true,
                 is_allowed({[{bucket, "default"}, data], read}, Roles)),
    ?assertEqual(false,
                 is_allowed({[{bucket, "default"}, data], write}, Roles)),
    ?assertEqual(true, is_allowed({[xdcr, developer], read}, Roles)),
    ?assertEqual(false, is_allowed({[xdcr, developer], write}, Roles)),
    ?assertEqual(true, is_allowed({[xdcr], anything}, Roles)),
    ?assertEqual(false, is_allowed({[admin], read}, Roles)),
    ?assertEqual(true, is_allowed({[other], read}, Roles)).

compile_and_assert(Role, Permissions, Params, Results) ->
    Roles = compile_roles([{Role, Params}], roles()),
    ?assertEqual(Results, lists:map(
        fun (Permission) ->
            {Object, Operations} = Permission,
            case is_list(Operations) of
                true ->
                    lists:all(fun(Operation) ->
                                is_allowed({Object, Operation}, Roles)
                              end, Operations);
                false ->
                    is_allowed(Permission, Roles)
            end
        end, Permissions)).

collection_roles_test_() ->
    Roles = [{<<"data_reader">>, [read, range_scan]},
             {<<"query_select">>, [read]},
             {<<"query_delete">>, [delete, range_scan]},
             {<<"query_update">>, [upsert, range_scan]},
             {<<"query_use_sequential_scans">>, [range_scan]}],

    Permissions =
        fun (Role, Ops) ->
                [{P, Ops} ||
                    P <- [[{collection, ["default", "s", "c"]}, data, docs],
                          [{collection, ["default", "s", "c1"]}, data, docs],
                          [{collection, ["default", "s", "c2"]}, data, docs],
                          [{scope, ["default", "s"]}, data, docs],
                          [{scope, ["default", "s1"]}, data, docs],
                          [{scope, ["default", "s2"]}, data, docs],
                          [{bucket, "default"}, data, docs]]] ++
                    [{[{bucket, "default"}, settings], read} ||
                        Role =/= <<"query_use_sequential_scans">>]
        end,

    RolesWithPermissions = [{R, Permissions(R, Ops)} || {R, Ops} <- Roles],

    TestTemplates =
        [{"existing collection with id's",
          [{"default", <<"default_id">>}, {"s", 1}, {"c", 1}],
          [true, false, false, false, false, false, false, true]},
         {"wrong collection id",
          [{"default", <<"default_id">>}, {"s", 1}, {"c", 2}],
          [false, false, false, false, false, false, false, false]},
         {"existing collection without id's",
          ["default", "s", "c"],
          [true, false, false, false, false, false, false, true]},
         {"scope",
          ["default", "s", any],
          [true, true, true, true, false, false, false, true]},
         {"whole bucket",
          ["default", any, any],
          [true, true, true, true, true, true, true, true]},
         {"another bucket",
          ["test", any, any],
          [false, false, false, false, false, false, false, false]}
        ],

    {setup, fun default_profile_test_setup/0,
     fun default_profile_test_teardown/1,
     [{Title ++ ", role = " ++ binary_to_list(Role),
       fun () ->
               compile_and_assert(Role, Perm, Params,
                                  lists:sublist(Expected, length(Perm)))
       end} ||
         {Title, Params, Expected} <- TestTemplates,
         {Role, Perm} <- RolesWithPermissions]}.

query_functions_test_() ->
    Roles = [{<<"query_manage_functions">>, [n1ql, udf], manage},
             {<<"query_execute_functions">>, [n1ql, udf], execute},
             {<<"query_manage_external_functions">>, [n1ql, udf_external],
                 manage},
             {<<"query_execute_external_functions">>, [n1ql, udf_external],
                 execute},
             {<<"query_manage_sequences">>, [n1ql, sequences], manage},
             {<<"query_use_sequences">>, [n1ql, sequences], execute}],

    Sources = [{scope, ["default", "s"]},
               {scope, ["default", "s1"]},
               {scope, ["default", "s2"]},
               {bucket, "default"}],

    Tests =
        lists:flatmap(
          fun ({Role, Object, Oper}) ->
                  RoleStr = binary_to_list(Role),
                  Permissions = [{[S | Object], Oper} || S <- Sources],
                  Test =
                      ?cut(fun () ->
                                   compile_and_assert(Role, Permissions, _, _)
                           end),

                  [{"existing scope with id's : " ++ RoleStr,
                    Test([{"default", <<"default_id">>}, {"s", 1}],
                         [true, false, false, false])},
                   {"wrong scope id : " ++ RoleStr,
                    Test([{"default", <<"default_id">>}, {"s", 2}],
                         [false, false, false, false])},
                   {"existing scope without id's : " ++ RoleStr,
                    Test(["default", "s"],
                         [true, false, false, false])},
                   {"whole bucket",
                    Test(["default", any],
                         [true, true, true, true])},
                   {"another bucket",
                    Test(["test", any],
                         [false, false, false, false])}]
          end, Roles),

    {setup, fun default_profile_test_setup/0,
     fun default_profile_test_teardown/1, Tests}.

eventing_functions_test_() ->
    Roles = [{<<"eventing_manage_functions">>, [eventing, function], manage}],

    Sources = [{scope, ["default", "s"]},
               {scope, ["default", "s1"]},
               {bucket, "default"}],

    Tests =
        lists:flatmap(
          fun ({Role, Object, Oper}) ->
                  RoleStr = binary_to_list(Role),
                  Permissions = [{[S | Object], Oper} || S <- Sources],
                  Test =
                    ?cut(fun () ->
                                 compile_and_assert(Role, Permissions, _, _)
                         end),

                    [{"existing scope with id's : " ++ RoleStr,
                      Test([{"default", <<"default_id">>}, {"s", 1}],
                           [true, false, false])},
                     {"whole bucket",
                      Test(["default", any],
                           [true, true, true])},
                     {"another bucket",
                      Test(["test", any],
                           [false, false, false])}]
          end, Roles),

    {setup, fun default_profile_test_setup/0,
     fun default_profile_test_teardown/1, Tests}.

validate_role_test__() ->
    ValidateRole = validate_role(_, roles(), toy_buckets()),
    ?assertEqual({ok, <<"admin">>}, ValidateRole(<<"admin">>)),
    ?assertEqual({ok, {<<"bucket_admin">>, [{"test", <<"test_id">>}]}},
                 ValidateRole({<<"bucket_admin">>, ["test"]})),
    ?assertEqual({ok, {<<"views_admin">>, [any]}},
                 ValidateRole({<<"views_admin">>, [any]})),
    ?assertEqual(false, ValidateRole(<<"something">>)),
    ?assertEqual(false, ValidateRole({<<"bucket_admin">>, ["something"]})),
    ?assertEqual(false, ValidateRole({<<"something">>, ["test"]})),
    ?assertEqual(false, ValidateRole({<<"admin">>, ["test"]})),
    ?assertEqual(false, ValidateRole(<<"bucket_admin">>)),
    ?assertEqual(false, ValidateRole({<<"bucket_admin">>, ["test", "test"]})),
    ?assertEqual(false, ValidateRole({<<"data_reader">>, ["default"]})),
    ?assertEqual(false, ValidateRole({<<"data_reader">>, ["default", "s"]})),
    DataReader =
        {<<"data_reader">>,
            [{"default", <<"default_id">>}, {"s", 1}, {"c", 1}]},
    ?assertEqual({ok, DataReader},
                 ValidateRole({<<"data_reader">>, ["default", "s", "c"]})),
    ?assertEqual(false, ValidateRole({<<"data_reader">>,
        ["default", "s", "d"]})),
    ?assertEqual({ok, DataReader}, ValidateRole(DataReader)),
    ?assertEqual(false, ValidateRole(
                          {<<"data_reader">>,
                           [{"default", <<"test_id">>}, {"s", 1}, {"c", 2}]})),
    QMF = {<<"query_manage_functions">>,
        [{"default", <<"default_id">>}, {"s", 1}]},
    ?assertEqual({ok, QMF}, ValidateRole(QMF)),
    ?assertEqual({ok, QMF}, ValidateRole({<<"query_manage_functions">>,
                                          ["default", "s"]})),
    ?assertEqual(false, ValidateRole({<<"query_manage_functions">>,
                                      [{"default", <<"test_id">>}, {"s", 1}]})),
    ?assertEqual(false, ValidateRole({<<"query_manage_functions">>,
                                      [{"default", <<"default_id">>},
                                       {"s", 2}]})).

enum_roles(Roles, ParamsList) ->
    Definitions = roles(),
    lists:flatmap(
      fun (Params) ->
              lists:map(
                fun (Role) ->
                        Length = length(get_param_defs(Role, Definitions)),
                        {Role, misc:align_list(Params, Length, any)}
                end, Roles)
      end, ParamsList).

produce_roles_by_permission_test__() ->
    GetRoles =
        fun (Permission) ->
                proplists:get_keys(
                  pipes:run(produce_roles_by_permission(Permission,
                                                        toy_buckets()),
                            pipes:collect()))
        end,
    Test =
        fun (Roles, Permission) ->
                ?_assertListsEqual(Roles, GetRoles(Permission))
        end,
    TestBucket = {"test", <<"test_id">>},
    DefaultBucket = {"default", <<"default_id">>},

    [{"security permission",
      Test([<<"admin">>, <<"security_admin">>, <<"ro_security_admin">>],
           {[admin, security], any})},
     {"admin security permission (read)",
      Test([<<"admin">>, <<"ro_security_admin">>],
           {[admin, security, admin], read})},
     {"admin security permission (write)",
      Test([<<"admin">>],
           {[admin, security, admin], write})},
     {"users permission",
      Test([<<"admin">>, <<"user_admin_local">>, <<"user_admin_external">>,
            <<"security_admin">>, <<"ro_security_admin">>],
           {[admin, users], any})},
     {"security_info permission",
      Test([<<"user_admin_local">>, <<"user_admin_external">>, <<"admin">>,
            <<"ro_security_admin">>, <<"security_admin">>],
           {[admin, security_info], read})},
     {"pools read",
      fun () ->
              Roles = GetRoles({[pools], read}),
              ?assertListsEqual(
                 [],
                 [<<"admin">>, <<"analytics_reader">>,
                  {<<"data_reader">>, [any, any, any]}] -- Roles)
      end},
     {"bucket settings read",
      Test([<<"admin">>, <<"cluster_admin">>, <<"query_external_access">>,
            <<"query_system_catalog">>, <<"replication_admin">>, <<"ro_admin">>,
            <<"security_admin">>, <<"user_admin_local">>,
            <<"user_admin_external">>, <<"eventing_admin">>, <<"backup_admin">>,
            <<"ro_security_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"bucket_admin">>,
                           <<"views_admin">>, <<"data_backup">>,
                           <<"data_dcp_reader">>, <<"data_monitoring">>,
                           <<"data_writer">>, <<"data_reader">>,
                           <<"fts_admin">>, <<"fts_searcher">>,
                           <<"query_delete">>, <<"query_insert">>,
                           <<"query_manage_index">>, <<"query_list_index">>,
                           <<"query_select">>, <<"query_update">>,
                           <<"replication_target">>, <<"mobile_sync_gateway">>],
                          [[any], [TestBucket]]),
           {[{bucket, "test"}, settings], read})},
     {"docs insert for bucket",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>],
                          [[any], [TestBucket]]),
           {[{bucket, "test"}, data, docs], insert})},
     {"docs insert for wrong bucket",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>], [[any]]),
           {[{bucket, "wrong"}, data, docs], insert})},
     {"docs insert for collection",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>], [[any], [DefaultBucket]]) ++
               enum_roles([<<"data_writer">>, <<"query_insert">>],
                          [[DefaultBucket, {"s", 1}]]) ++
               enum_roles([<<"data_writer">>, <<"query_insert">>],
                          [[DefaultBucket, {"s", 1}, {"c", 1}]]),
           {[{collection, ["default", "s", "c"]}, data, docs], insert})},
     {"docs insert for wrong collection",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>], [[any], [DefaultBucket]]) ++
               enum_roles([<<"data_writer">>, <<"query_insert">>],
                          [[DefaultBucket, {"s", 1}]]),
           {[{collection, ["default", "s", "w"]}, data, docs], insert})},
     {"docs insert for scope",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>],
                          [[any], [DefaultBucket]]) ++
               enum_roles([<<"data_writer">>, <<"query_insert">>],
                          [[DefaultBucket, {"s", 1}]]),
           {[{scope, ["default", "s"]}, data, docs], insert})},
     {"docs insert for wrong scope",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>], [[any], [DefaultBucket]]),
           {[{scope, ["default", "w"]}, data, docs], insert})},
     {"any bucket",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>], [[any]]),
           {[{bucket, any}, data, docs], insert})},
     {"wrong bucket",
      Test([<<"admin">>, <<"eventing_admin">>, <<"backup_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"data_backup">>,
                           <<"data_writer">>, <<"mobile_sync_gateway">>,
                           <<"query_insert">>], [[any]]),
           {[{bucket, "wrong"}, data, docs], insert})},
     {"read indexes",
      Test([<<"admin">>, <<"ro_admin">>, <<"backup_admin">>,
            <<"eventing_admin">>] ++
               enum_roles([<<"bucket_full_access">>, <<"mobile_sync_gateway">>,
                           <<"query_list_index">>, <<"query_manage_index">>],
                          [[any]]),
           {[{bucket, any}, n1ql, index], read})},
     {"consume specific credential",
      Test([<<"admin">>] ++
               enum_roles([<<"credential_consumer">>],
                          [[any], ["backup/prod/s3"]]),
           {[{credentials, "backup/prod/s3"}], consume})},
     {"consume nonexistent credential",
      Test([<<"admin">>] ++
               enum_roles([<<"credential_consumer">>], [[any]]),
           {[{credentials, "no/such/id"}], consume})},
     {"consume any credential",
      Test([<<"admin">>] ++
               enum_roles([<<"credential_consumer">>], [[any]]),
           {[{credentials, any}], consume})}].

get_security_roles_test__() ->
    Name = fun ({{N, _}, _}) when is_binary(N) -> N;
               ({N, _})      when is_binary(N) -> N
           end,
    Names = [Name(R) || R <- get_security_roles(toy_buckets())],
    ?assertListsEqual([<<"admin">>,
                       <<"security_admin">>, <<"ro_security_admin">>],
                      Names).

params_version_get_snapshot(TestProps, _, SubKeys) ->
    PrunedProps = lists:flatmap(
                    fun({Bucket, Props}) ->
                            [{Bucket,
                              lists:filtermap(
                                fun({SubKey, _}) ->
                                        lists:member(SubKey, SubKeys)
                                end, Props)}]
                    end, TestProps),
    ns_bucket:toy_buckets(PrunedProps).

params_version_case(TestProps) ->
    meck:expect(ns_bucket, get_snapshot,
                params_version_get_snapshot(TestProps, _, _)),
    Snapshot = ns_bucket:toy_buckets(TestProps),
    Version = params_version(Snapshot),
    ?assertEqual(params_version(), Version),
    Version.

params_version_test__() ->
    Update = ?cut(lists:keyreplace("test", 1, toy_buckets_props(),
                                   {"test", _})),
    BaseVersion = params_version_case(toy_buckets_props()),
    lists:foreach(
      fun(X) -> ?assertNotEqual(params_version_case(X), BaseVersion) end,
      [lists:keydelete("test", 1, toy_buckets_props()),
       Update([{uuid, <<"test_id1">>}, {props, toy_props()}]),
       Update([{uuid, <<"test_id">>}, {collections, toy_manifest()},
               {props, toy_props()}])]).

validate_test_roles(Roles) ->
    lists:foreach(
      fun ({Name, Params, Desc, Permissions}) when is_binary(Name),
                                                   is_list(Params),
                                                   is_list(Desc),
                                                   is_list(Permissions) ->
              ?assert(lists:member(Params, all_params_combinations())),
              ?assert(lists:all(fun ({_, _}) -> true; (_) -> false end, Desc)),
              ValidateObject =
                  fun (Obj) ->
                          ?assert(lists:all(
                                    fun (A) when is_atom(A) -> true;
                                        ({A, _}) when is_atom(A) -> true;
                                        (_) -> false
                                    end, Obj))
                  end,
              lists:foreach(
                fun ({Object, all}) -> ValidateObject(Object);
                    ({Object, none}) -> ValidateObject(Object);
                    ({Object, Ops}) when is_list(Ops) ->
                        ?assert(lists:all(fun (A) -> is_atom(A) end, Ops)),
                        ValidateObject(Object)
                end, Permissions)
      end, Roles).

roles_format_test__() ->
    validate_test_roles(roles()).

roles_pre_76_format_test__() ->
    validate_test_roles(menelaus_old_roles:roles_pre_76()).

roles_pre_79_format_test__() ->
    validate_test_roles(menelaus_old_roles:roles_pre_79()).

roles_pre_totoro_format_test__() ->
    validate_test_roles(menelaus_old_roles:roles_pre_totoro()).

extended_roles_test__() ->
    MyRoles = [{<<"superman">>, [],
                [{name, <<"Superman">>},
                 {folder, admin},
                 {desc, <<"Able to leap tall buildings in a single bound!">>},
                 {ce, true}],
                [{[admin, security_info], none},
                 {[], all}]},
               {<<"analytics_select">>, [],
                [{name, <<"Analytics Select">>},
                 {folder, analytics},
                 {desc, <<"This user can access the web console.">>}],
                [{[ui], [read]}]}],
    meck:expect(config_profile, get,
                fun () ->
                        [{name, "my_profile"},
                         {extra_roles, MyRoles}]
                end),
    validate_test_roles(roles()),
    Roles = compile_roles([<<"superman">>], roles()),
    ?assertEqual(true, is_allowed({[anything], access}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security_info], read}, Roles)),
    Roles2 = compile_roles([<<"analytics_select">>], roles()),
    ?assertEqual(true, is_allowed({[ui], read}, Roles2)),
    ?assertEqual(false, is_allowed({[pools], read}, Roles2)).

unexpanded_permission_granted_by_role(Object, Operation, Role) ->
    ObjectExpanded = lists:flatmap(expand_vertex(_, all), Object),
    permission_granted_by_role(ObjectExpanded, Operation, Role).

allowed_operations_with_any_test_() ->
    [?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["default", "s", any]}],
                   operation,
                   [{[{collection, ["default", "s", "c"]}], none}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", "s", any]}],
                all,
                [{[{collection, ["default", "s", "c"]}], all}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["default", any, any]}],
                   operation,
                   [{[{collection, ["default", "s", "c"]}], none}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}],
                all,
                [{[{collection, ["default", "s", "c"]}], all}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["default", "s", any]}],
                   operation,
                   [{[{scope, ["default", "s"]}], none},
                    {[{bucket, "default"}], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", "s", any]}],
                all,
                [{[{scope, ["default", "s"]}], all},
                 {[{bucket, "default"}], none}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["default", "s", any]}, collections],
                   operation,
                   [{[{scope, ["default", "s"]}, collections], none},
                    {[{bucket, "default"}], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", "s", any]}, collections],
                all,
                [{[{scope, ["default", "s"]}, collections], all},
                 {[{bucket, "default"}], none}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["default", any, any]}, data, docs],
                   operation,
                   [{[{collection, ["default", "s", "c2"]}], none},
                    {[{scope, ["default", "s"]}, data], none},
                    {[{bucket, "default"}, data], none},
                    {[{bucket, "other"}, data], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, data, docs],
                op1,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{scope, ["default", "s"]}, data], [op1]},
                 {[{bucket, "default"}, data], [op2]},
                 {[{bucket, "other"}, data], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, data, docs],
                op2,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{scope, ["default", "s"]}, data], [op1]},
                 {[{bucket, "default"}, data], [op2]},
                 {[{bucket, "other"}, data], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, data, docs],
                all,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{scope, ["default", "s"]}, data], [op1]},
                 {[{bucket, "default"}, data], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}],
                op1,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{collection, ["default", "s", "c3"]}], [op1]}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections],
                op1,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{collection, ["default", "s", "c3"]}], [op1]}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", "s", any]}],
                op1,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{collection, ["default", "s", "c3"]}], [op1]},
                 {[{collection, ["default", "s", "c4"]}], [op2]}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", "s", any]}],
                op2,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{collection, ["default", "s", "c3"]}], [op1]},
                 {[{collection, ["default", "s", "c4"]}], [op2]}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", "s", any]}, collections],
                op1,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{collection, ["default", "s", "c3"]}], [op1]},
                 {[{collection, ["default", "s", "c4"]}], [op2]}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", "s", any]}, collections],
                op2,
                [{[{collection, ["default", "s", "c2"]}], none},
                 {[{collection, ["default", "s", "c3"]}], [op1]},
                 {[{collection, ["default", "s", "c4"]}], [op2]}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{bucket, any}, n1ql],
                   operation,
                   [{[{bucket, any}, n1ql], none},
                    {[], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{bucket, any}, collections],
                all,
                [{[{bucket, any}, n1ql], none},
                 {[], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections],
                all,
                [{[{collection, ["default", "s1", any]}], all},
                 {[{collection, ["default", any, any]}], none}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["default", any, any]}, collections],
                   operation,
                   [{[{collection, ["default", any, any]}], none},
                    {[], all}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["default", any, any]}, data],
                   operation,
                   [{[{collection, ["default", any, any]}, data], none},
                    {[], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections],
                all,
                [{[{collection, ["default", any, any]}, data], none},
                 {[], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections],
                all,
                [{[{collection, ["default", "s", any]}], none},
                 {[{collection, ["default", "s1", any]}], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections],
                all,
                [{[{collection, ["default", "s", any]}], none},
                 {[], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections],
                all,
                [{[{collection, ["default", any, "c1"]}], none},
                 {[], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, "c1"]}, collections],
                all,
                [{[{collection, ["default", any, "c1"]}], all},
                 {[{collection, ["default", any, any]}], none}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections],
                all,
                [{[{collection, ["default", any, "c1"]}], all},
                 {[{collection, ["default", any, any]}], none}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}, collections], all,
                [{[{collection, ["default", "s1", any]}], none},
                 {[{collection, ["default", any, "c1"]}], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, ["default", any, any]}], all,
                [{[{collection, ["default", any, "c1"]}], none},
                 {[], all}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, [any, "s", any]}],
                   operation,
                   [{[{collection, [any, "s", any]}], none},
                    {[], all}])),
     ?_assert(unexpanded_permission_granted_by_role(
                [{collection, [any, "s", any]}], all,
                [{[{collection, [any, "s", "c1"]}], none},
                 {[], all}])),
     ?_assertNot(unexpanded_permission_granted_by_role(
                   [{collection, ["b1", "s", all]}], all,
                   [{[{collection, [any, "s", "c1"]}], none},
                    {[], all}]))].

simple_custom_roles_test__() ->
    SimpleRole = [{[custom, x, y], [z]},
                  {[{collection, ["default", "s", "c1"]}], none},
                  {[{collection, ["default", "s", any]}], all},
                  {[{bucket, "default"}, data, dcp], [read]}],
    ok = set_role({<<"simple_role">>, [], [{mutable, true}], SimpleRole}),
    Roles = compile_roles([<<"simple_role">>,
                           <<"wrong_role">>,
                           {<<"scope_admin">>, ["default", "s1"]}],
                          roles()),
    ?assertEqual([SimpleRole,
                  [{[{collection, ["default", "s1", any]}, collections], all}]],
                 Roles),
    ?assertEqual(true, is_allowed({[custom, x, y], z}, Roles)),
    ?assertEqual(false, is_allowed({[custom, x, y], w}, Roles)),
    ?assertEqual(true, is_allowed({[{collection, ["default", "s", "c2"]}], r},
                                  Roles)),
    %% While the "c1" exclusion matches first (since it is more specific), we
    %% continue checking for a separate privilege giving access, which it does
    ?assertEqual(true, is_allowed({[{collection, ["default", "s", any]}], r},
                                  Roles)),
    ?assertEqual(false, is_allowed({[{collection, ["default", "s", "c1"]}], r},
                                   Roles)),
    ?assertEqual(false, is_allowed({[{collection, ["default", "s", "c1"]}], r},
                                   Roles)),

    %% Because data.dcp!read is not being allowed for default:s:c1, it is not
    %% allowed at the bucket level
    ?assertEqual(false, is_allowed({[{bucket, "default"}, data, dcp], read},
                                   Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, data, dcp], write},
                                   Roles)),

    ?assertEqual({error, not_found}, delete_role(<<"unknown_id">>)),
    ?assertEqual({error, immutable}, delete_role(<<"admin">>)),
    ?assertEqual(ok, delete_role(<<"simple_role">>)).

complex_custom_roles_test__() ->
    ComplexRole = [{[{collection, ["default", "s2", "c1"]}], all},
                   {[{collection, ["default", "s2", any]}], none},
                   {[{collection, ["default", "s1", "c1"]}], none},
                   {[{collection, ["default", any, any]}], all}],
    ok = set_role({<<"complex_role">>, [], [{mutable, true}],
                   ComplexRole}),
    Roles = compile_roles([<<"complex_role">>, <<"wrong_role">>], roles()),
    ?assertEqual([ComplexRole], Roles),
    ?assertEqual(true, is_allowed({[{collection, ["default", "s2", "c1"]}], r},
                                  Roles)),
    ?assertEqual(false, is_allowed({[{collection, ["default", "s2", "c2"]}], r},
                                   Roles)),
    ?assertEqual(false, is_allowed({[{collection, ["default", "s2", all]}], r},
                                   Roles)),
    ?assertEqual(false, is_allowed({[{collection, ["default", "s1", "c1"]}], r},
                                   Roles)),
    ?assertEqual(true, is_allowed({[{collection, ["default", "s1", "c2"]}], r},
                                  Roles)),
    ?assertEqual(false, is_allowed({[{collection, ["default", "s1", all]}], r},
                                   Roles)),
    ?assertEqual(false, is_allowed({[{collection, ["default", all, all]}], r},
                                   Roles)).

default_profile_test_setup() ->
    setup_meck(),
    fake_chronicle_kv:setup(),
    set_role_definitions(),
    config_profile:load_default_profile_for_test().

default_profile_test_teardown(_) ->
    default_profile_test_teardown().

default_profile_test_teardown() ->
    config_profile:unload_profile_for_test(),
    fake_chronicle_kv:teardown(),
    meck:unload().

default_profile_test_() ->
    {setup,
     fun default_profile_test_setup/0,
     fun default_profile_test_teardown/1,
     [fun admin_test__/0,
      fun service_admin_test__/0,
      fun service_admin_with_credential_consumer_test__/0,
      fun credentials_access_matrix_test__/0,
      fun cluster_admin_test__/0,
      fun eventing_admin_test__/0,
      fun backup_admin_test__/0,
      fun ro_admin_test__/0,
      fun security_admin_test__/0,
      fun ro_security_admin_test__/0,
      fun user_admin_local_test__/0,
      fun user_admin_external_test__/0,
      fun admin_event_metakv_permissions_test__/0,
      fun system_collections_write_permissions_test__/0,
      fun system_collections_read_permissions_test__/0,
      fun bucket_admin_test__/0,
      fun bucket_admin_wildcard_test__/0,
      fun views_admin_test__/0,
      fun views_admin_wildcard_test__/0,
      fun bucket_full_access_test__/0,
      fun replication_admin_test__/0,
      {generator, fun produce_roles_by_permission_test__/0},
      fun get_security_roles_test__/0,
      fun params_version_test__/0,
      fun validate_role_test__/0,
      fun roles_format_test__/0,
      fun roles_pre_76_format_test__/0,
      fun roles_pre_79_format_test__/0,
      fun roles_pre_totoro_format_test__/0,
      fun extended_roles_test__/0,
      fun simple_custom_roles_test__/0,
      fun complex_custom_roles_test__/0]}.

analytics_admin_empty_profile_test_() ->
    %% use "default" explicitly here so that this test passes when run on a
    %% workspace based on enterprise-analytics manifest
    {setup,
     fun () ->
             fake_chronicle_kv:setup(),
             set_role_definitions(),
             config_profile:load_profile_for_test("default")
     end,
     fun (_) ->
             config_profile:unload_profile_for_test(),
             fake_chronicle_kv:teardown()
     end,
     [fun () ->
              Roles = compile_roles([<<"analytics_admin">>], roles()),
              ?assertEqual(true,
                           is_allowed(
                             {[{bucket, "foobar"}, analytics], manage}, Roles)),
              ?assertEqual(false, is_allowed({[analytics], access}, Roles))
      end]}.

analytics_access_test__() ->
    Roles = compile_roles([<<"analytics_access">>], roles()),
    ?assertEqual(true, is_allowed({[analytics], access}, Roles)),
    ?assertEqual(false, is_allowed(
                          {[admin, settings, metrics], any}, Roles)).

analytics_admin_test__() ->
    Roles = compile_roles([<<"analytics_admin">>], roles()),
    ?assertEqual(false,
                 is_allowed(
                   {[{bucket, "foobar"}, analytics], manage}, Roles)),
    ?assertEqual(true, is_allowed({[analytics], access}, Roles)).

analytics_profile_test_setup() ->
    setup_meck(),
    fake_chronicle_kv:setup(),
    set_role_definitions(),
    config_profile:load_profile_for_test(?ANALYTICS_PROFILE_STR).

analytics_profile_test_teardown(_) ->
    config_profile:unload_profile_for_test(),
    fake_chronicle_kv:teardown(),
    meck:unload().

analytics_profile_test_() ->
    {setup,
     fun analytics_profile_test_setup/0,
     fun analytics_profile_test_teardown/1,
     [fun analytics_access_test__/0,
      fun analytics_admin_test__/0]}.

-endif.
