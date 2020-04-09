%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2019 Couchbase, Inc.
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
%% 1. Permission is defined as a pair {object, operation}
%% 2. Objects are organized in the tree structure with common root []
%% 3. One vertex of this tree can be parametrized: {bucket, bucket_name},
%%    wildcard all can be used in place of bucket_name
%% 4. Permission pattern is a pair {Object pattern, Allowed operations}
%% 5. Allowed operations can be list of operations, all or none
%% 6. Object pattern is a list of vertices that define a certain subtree of the objects tree
%% 7. Object pattern vertex {bucket, bucket_name} always matches object vertex {bucket, any},
%%    object pattern vertex {bucket, any} matches {bucket, bucket_name} with any bucket_name
%%    otherwise vertices match if they are equal
%% 8. Object matches the object pattern if all the vertices of object pattern match
%%    corresponding vertices of the object.
%% 9. Each role is defined as a list of permission patterns.
%% 10.To find which operations are allowed for certain object in certain role we look for the
%%    first permission pattern with matching object pattern in the permission pattern list of
%%    the role.
%% 11.The permission is allowed by the role if its operation is among the allowed operations
%%    for its object.
%% 12.Each user can have multiple roles assigned
%% 13.Certain permission is allowed to the user if it is allowed at least by one of the roles
%%    assigned to user.

%% @doc roles and permissions implementation

-module(menelaus_roles).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("rbac.hrl").
-include("pipes.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_EXTERNAL_ROLES_POLLING_INTERVAL, 10*60*1000).

-export([get_definitions/0,
         get_definitions/1,
         is_allowed/2,
         get_roles/1,
         get_compiled_roles/1,
         compile_roles/3,
         validate_roles/2,
         calculate_possible_param_values/1,
         filter_out_invalid_roles/3,
         produce_roles_by_permission/3,
         get_security_roles/0,
         external_auth_polling_interval/0]).

-export([start_compiled_roles_cache/0]).

%% for RPC from ns_couchdb node
-export([build_compiled_roles/1]).

-spec roles_50() -> [rbac_role_def(), ...].
roles_50() ->
    [{admin, [],
      [{name, <<"Admin">>},
       {desc, <<"Can manage ALL cluster features including security.">>},
       {ce, true}],
      [{[], all}]},
     {ro_admin, [],
      [{name, <<"Read Only Admin">>},
       {desc, <<"Can view ALL cluster features.">>},
       {ce, true}],
      [{[{bucket, any}, password], none},
       {[{bucket, any}, data], none},
       {[admin, security], [read]},
       {[admin], none},
       {[], [read, list]}]},
     {cluster_admin, [],
      [{name, <<"Cluster Admin">>},
       {desc, <<"Can manage all cluster features EXCEPT security.">>}],
      [{[admin, internal], none},
       {[admin, security], none},
       {[admin, diag], read},
       {[n1ql, curl], none},
       {[], all}]},
     {bucket_admin, [bucket_name],
      [{name, <<"Bucket Admin">>},
       {desc, <<"Can manage ALL bucket features for specified buckets (incl. start/stop XDCR)">>}],
      [{[{bucket, bucket_name}, xdcr], [read, execute]},
       {[{bucket, bucket_name}], all},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[xdcr], none},
       {[admin], none},
       {[], [read]}]},
     {bucket_full_access, [bucket_name],
      [{name, <<"Bucket Full Access">>},
       {desc, <<"Full access to bucket data">>},
       {ce, true}],
      [{[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[{bucket, bucket_name}], [read, flush]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {views_admin, [bucket_name],
      [{name, <<"Views Admin">>},
       {desc, <<"Can manage views for specified buckets">>}],
      [{[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, data], [read]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[xdcr], none},
       {[admin], none},
       {[], [read]}]},
     {views_reader, [bucket_name],
      [{name, <<"Views Reader">>},
       {desc, <<"Can read data from the views of specified bucket">>}],
      [{[{bucket, bucket_name}, views], [read]},
       {[{bucket, bucket_name}, data, docs], [read]},
       {[pools], [read]}]},
     {replication_admin, [],
      [{name, <<"Replication Admin">>},
       {desc, <<"Can manage ONLY XDCR features (cluster AND bucket level)">>}],
      [{[{bucket, any}, xdcr], all},
       {[{bucket, any}, data], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}, stats], [read]},
       {[{bucket, any}], none},
       {[xdcr], all},
       {[admin], none},
       {[], [read]}]},
     {data_reader, [bucket_name],
      [{name, <<"Data Reader">>},
       {desc, <<"Can read information from specified bucket">>}],
      [{[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, data, meta], [read]},
       {[{bucket, bucket_name}, data, xattr], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {data_writer, [bucket_name],
      [{name, <<"Data Writer">>},
       {desc, <<"Can write information from/to specified bucket">>}],
      [{[{bucket, bucket_name}, data, docs], [insert, upsert, delete]},
       {[{bucket, bucket_name}, data, xattr], [write]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {data_dcp_reader, [bucket_name],
      [{name, <<"Data DCP Reader">>},
       {desc, <<"Can read DCP data streams">>}],
      [{[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, data, meta], [read]},
       {[{bucket, bucket_name}, data, dcp], [read]},
       {[{bucket, bucket_name}, data, sxattr], [read]},
       {[{bucket, bucket_name}, data, xattr], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[admin, memcached, idle], [write]},
       {[pools], [read]}]},
     {data_backup, [bucket_name],
      [{name, <<"Data Backup">>},
       {desc, <<"Can backup and restore bucket data">>}],
      [{[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], [read, write]},
       {[{bucket, bucket_name}, fts], [read, write, manage]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, n1ql, index], [create, list, build]},
       {[pools], [read]}]},
     {data_monitoring, [bucket_name],
      [{name, <<"Data Monitoring">>},
       {desc, <<"Can read full bucket stats">>}],
      [{[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {fts_admin, [bucket_name],
      [{name, <<"FTS Admin">>},
       {desc, <<"Can administer all FTS features">>}],
      [{[{bucket, bucket_name}, fts], [read, write, manage]},
       {[settings, fts], [read, write, manage]},
       {[ui], [read]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]}]},
     {fts_searcher, [bucket_name],
      [{name, <<"FTS Searcher">>},
       {desc, <<"Can query FTS indexes if they have bucket permissions">>}],
      [{[{bucket, bucket_name}, fts], [read]},
       {[settings, fts], [read]},
       {[ui], [read]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]}]},
     {query_select, [bucket_name],
      [{name, <<"Query Select">>},
       {desc, <<"Can execute SELECT statement on bucket to retrieve data">>}],
      [{[{bucket, bucket_name}, n1ql, select], [execute]},
       {[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_update, [bucket_name],
      [{name, <<"Query Update">>},
       {desc, <<"Can execute UPDATE statement on bucket to update data">>}],
      [{[{bucket, bucket_name}, n1ql, update], [execute]},
       {[{bucket, bucket_name}, data, docs], [upsert]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_insert, [bucket_name],
      [{name, <<"Query Insert">>},
       {desc, <<"Can execute INSERT statement on bucket to add data">>}],
      [{[{bucket, bucket_name}, n1ql, insert], [execute]},
       {[{bucket, bucket_name}, data, docs], [insert]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_delete, [bucket_name],
      [{name, <<"Query Delete">>},
       {desc, <<"Can execute DELETE statement on bucket to delete data">>}],
      [{[{bucket, bucket_name}, n1ql, delete], [execute]},
       {[{bucket, bucket_name}, data, docs], [delete]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_manage_index, [bucket_name],
      [{name, <<"Query Manage Index">>},
       {desc, <<"Can manage indexes for the bucket">>}],
      [{[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_system_catalog, [],
      [{name, <<"Query System Catalog">>},
       {desc, <<"Can lookup system catalog information">>}],
      [{[{bucket, any}, n1ql, index], [list]},
       {[{bucket, any}, settings], [read]},
       {[n1ql, meta], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_external_access, [],
      [{name, <<"Query External Access">>},
       {desc, <<"Can execute CURL statement">>}],
      [{[n1ql, curl], [execute]},
       {[{bucket, any}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {replication_target, [bucket_name],
      [{name, <<"Replication Target">>},
       {desc, <<"XDC replication target for bucket">>}],
      [{[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, data, meta], [read, write]},
       {[{bucket, bucket_name}, stats], [read]},
       {[pools], [read]}]}].

-spec roles_55() -> [rbac_role_def(), ...].
roles_55() ->
    [{admin, [],
      [{name, <<"Full Admin">>},
       {desc, <<"Can manage all cluster features (including security). "
                "This user can access the web console. This user can read and "
                "write all data.">>},
       {ce, true}],
      [{[], all}]},
     {ro_admin, [],
      [{name, <<"Read-Only Admin">>},
       {desc, <<"Can view all cluster statistics. This user can access the "
                "web console. This user can read some data.">>},
       {ce, true}],
      [{[{bucket, any}, password], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, fts], none},
       {[admin, security], [read]},
       {[admin], none},
       {[eventing], none},
       {[], [read, list]}]},
     {security_admin, [],
      [{name, <<"Security Admin">>},
       {desc, <<"Can view all cluster statistics and manage user roles, but "
                "not grant Full Admin or Security Admin roles to other users "
                "or alter their own role. This user can access the web "
                "console. This user cannot read data.">>}],
      [{[admin, security, admin], none},
       {[admin, security], all},
       {[admin, logs], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, password], none},
       {[{bucket, any}], [read]},
       {[], [read, list]}]},
     {cluster_admin, [],
      [{name, <<"Cluster Admin">>},
       {desc, <<"Can manage all cluster features except security. This user "
                "can access the web console. This user cannot read data.">>}],
      [{[admin, internal], none},
       {[admin, security], none},
       {[admin, diag], read},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, password], none},
       {[n1ql, curl], none},
       {[eventing], none},
       {[], all}]},
     {bucket_admin, [bucket_name],
      [{name, <<"Bucket Admin">>},
       {desc, <<"Can manage ALL bucket features for a given bucket (including "
                "start/stop XDCR). This user can access the web console. This "
                "user cannot read data.">>}],
      [{[{bucket, bucket_name}, xdcr], [read, execute]},
       {[{bucket, bucket_name}, data], none},
       {[{bucket, bucket_name}, views], none},
       {[{bucket, bucket_name}, n1ql], none},
       {[{bucket, bucket_name}, password], none},
       {[{bucket, bucket_name}, fts], none},
       {[{bucket, bucket_name}], all},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[xdcr], none},
       {[admin], none},
       {[eventing], none},
       {[], [read]}]},
     {bucket_full_access, [bucket_name],
      [{name, <<"Application Access">>},
       {desc, <<"Full access to bucket data. This user cannot access the web "
                "console and is intended only for application access. This "
                "user can read and write data.">>},
       {ce, true}],
      [{[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[{bucket, bucket_name}], [read, flush]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {views_admin, [bucket_name],
      [{name, <<"Views Admin">>},
       {desc, <<"Can create and manage views of a given bucket. This user can "
                "access the web console. This user can read some data.">>}],
      [{[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, data], [read]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[xdcr], none},
       {[admin], none},
       {[eventing], none},
       {[], [read]}]},
     {views_reader, [bucket_name],
      [{name, <<"Views Reader">>},
       {desc, <<"Can read data from the views of a given bucket. This user "
                "cannot access the web console and is intended only for "
                "application access. This user can read some data.">>}],
      [{[{bucket, bucket_name}, views], [read]},
       {[{bucket, bucket_name}, data, docs], [read]},
       {[pools], [read]}]},
     {replication_admin, [],
      [{name, <<"XDCR Admin">>},
       {desc, <<"Can administer XDCR features to create cluster references and "
                "replication streams out of this cluster. This user can "
                "access the web console. This user can read some data.">>}],
      [{[{bucket, any}, xdcr], all},
       {[{bucket, any}, data], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}, stats], [read]},
       {[{bucket, any}], none},
       {[xdcr], all},
       {[admin], none},
       {[eventing], none},
       {[], [read]}]},
     {data_reader, [bucket_name],
      [{name, <<"Data Reader">>},
       {desc, <<"Can read data from a given bucket. This user cannot access "
                "the web console and is intended only for application access. "
                "This user can read data, but cannot write it.">>}],
      [{[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, data, meta], [read]},
       {[{bucket, bucket_name}, data, xattr], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {data_writer, [bucket_name],
      [{name, <<"Data Writer">>},
       {desc, <<"Can write data to a given bucket. This user cannot access the "
                "web console and is intended only for application access. This "
                "user can write data, but cannot read it.">>}],
      [{[{bucket, bucket_name}, data, docs], [insert, upsert, delete]},
       {[{bucket, bucket_name}, data, xattr], [write]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {data_dcp_reader, [bucket_name],
      [{name, <<"Data DCP Reader">>},
       {desc, <<"Can initiate DCP streams for a given bucket. This user cannot "
                "access the web console and is intended only for application "
                "access. This user can read data.">>}],
      [{[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, data, meta], [read]},
       {[{bucket, bucket_name}, data, dcp], [read]},
       {[{bucket, bucket_name}, data, sxattr], [read]},
       {[{bucket, bucket_name}, data, xattr], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[admin, memcached, idle], [write]},
       {[pools], [read]}]},
     {data_backup, [bucket_name],
      [{name, <<"Data Backup & Restore">>},
       {desc, <<"Can backup and restore a given bucket’s data. This user "
                "cannot access the web console and is intended only for "
                "application access. This user can read data.">>}],
      [{[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], [read, write]},
       {[{bucket, bucket_name}, fts], [read, write, manage]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, n1ql, index], [create, list, build]},
       {[{bucket, bucket_name}, analytics], [manage]},
       {[analytics], [select, backup]},
       {[pools], [read]}]},
     {data_monitoring, [bucket_name],
      [{name, <<"Data Monitor">>},
       {desc, <<"Can read statistics for a given bucket. This user cannot "
                "access the web console and is intended only for application "
                "access. This user cannot read data.">>}],
      [{[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[tasks], [read]},
       {[pools], [read]}]},
     {fts_admin, [bucket_name],
      [{name, <<"Search Admin">>},
       {desc, <<"Can administer all Full Text Search features. This user can "
                "access the web console. This user can read some data.">>}],
      [{[{bucket, bucket_name}, fts], [read, write, manage]},
       {[settings, fts], [read, write, manage]},
       {[ui], [read]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]}]},
     {fts_searcher, [bucket_name],
      [{name, <<"Search Reader">>},
       {desc, <<"Can query Full Text Search indexes for a given bucket. This "
                "user can access the web console. This user can read some "
                "data.">>}],
      [{[{bucket, bucket_name}, fts], [read]},
       {[settings, fts], [read]},
       {[ui], [read]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]}]},
     {query_select, [bucket_name],
      [{name, <<"Query Select">>},
       {desc, <<"Can execute a SELECT statement on a given bucket to retrieve "
                "data. This user can access the web console and can read data, "
                "but not write it.">>}],
      [{[{bucket, bucket_name}, n1ql, select], [execute]},
       {[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_update, [bucket_name],
      [{name, <<"Query Update">>},
       {desc, <<"Can execute an UPDATE statement on a given bucket to update "
                "data. This user can access the web console and write data, "
                "but cannot read it.">>}],
      [{[{bucket, bucket_name}, n1ql, update], [execute]},
       {[{bucket, bucket_name}, data, docs], [upsert]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_insert, [bucket_name],
      [{name, <<"Query Insert">>},
       {desc, <<"Can execute an INSERT statement on a given bucket to add "
                "data. This user can access the web console and insert data, "
                "but cannot read it.">>}],
      [{[{bucket, bucket_name}, n1ql, insert], [execute]},
       {[{bucket, bucket_name}, data, docs], [insert]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_delete, [bucket_name],
      [{name, <<"Query Delete">>},
       {desc, <<"Can execute a DELETE statement on a given bucket to delete "
                "data. This user can access the web console, but cannot read "
                "data. This user can delete data.">>}],
      [{[{bucket, bucket_name}, n1ql, delete], [execute]},
       {[{bucket, bucket_name}, data, docs], [delete]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_manage_index, [bucket_name],
      [{name, <<"Query Manage Index">>},
       {desc, <<"Can manage indexes for a given bucket. This user can access "
                "the web console, but cannot read data.">>}],
      [{[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_system_catalog, [],
      [{name, <<"Query System Catalog">>},
       {desc, <<"Can look up system catalog information via N1QL. This user "
                "can access the web console, but cannot read user data.">>}],
      [{[{bucket, any}, n1ql, index], [list]},
       {[{bucket, any}, settings], [read]},
       {[n1ql, meta], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_external_access, [],
      [{name, <<"Query CURL Access">>},
       {desc, <<"Can execute the CURL statement from within N1QL. This user "
                "can access the web console, but cannot read data (within "
                "Couchbase).">>}],
      [{[n1ql, curl], [execute]},
       {[{bucket, any}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {replication_target, [bucket_name],
      [{name, <<"XDCR Inbound">>},
       {desc, <<"Can create XDCR streams into a given bucket. This user cannot "
                "access the web console or read any data.">>}],
      [{[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, data, meta], [read, write]},
       {[{bucket, bucket_name}, stats], [read]},
       {[pools], [read]}]},
     {analytics_manager, [bucket_name],
      [{name, <<"Analytics Manager">>},
       {desc, <<"Can manage Analytics links. Can manage datasets on a given bucket. "
                "Can query datasets. This user can access the web console and read "
                "some data.">>}],
      [{[{bucket, bucket_name}, analytics], [manage]},
       {[analytics], [select]},
       {[ui], [read]},
       {[pools], [read]}]},
     {analytics_reader, [],
      [{name, <<"Analytics Reader">>},
       {desc, <<"Can query datasets. This is a global role as datasets may "
                "be created on different buckets. This user can access the "
                "web console and read some data.">>}],
      [{[analytics], [select]},
       {[ui], [read]},
       {[pools], [read]}]}].

-spec roles_66() -> [rbac_role_def(), ...].
roles_66() ->
    [{admin, [],
      [{name, <<"Full Admin">>},
       {desc, <<"Can manage all cluster features (including security). "
                "This user can access the web console. This user can read and "
                "write all data.">>},
       {ce, true}],
      [{[], all}]},
     {ro_admin, [],
      [{name, <<"Read-Only Admin">>},
       {desc, <<"Can view all cluster statistics. This user can access the "
                "web console. This user can read some data.">>},
       {ce, true}],
      [{[{bucket, any}, password], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, fts], none},
       {[admin, security], [read]},
       {[admin], none},
       {[eventing], none},
       {[], [read, list]}]},
     {security_admin, [],
      [{name, <<"Security Admin">>},
       {desc, <<"Can view all cluster statistics and manage user roles, but "
                "not grant Full Admin or Security Admin roles to other users "
                "or alter their own role. This user can access the web "
                "console. This user cannot read data.">>}],
      [{[admin, security, admin], none},
       {[admin, security], all},
       {[admin, logs], none},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, password], none},
       {[{bucket, any}], [read]},
       {[], [read, list]}]},
     {cluster_admin, [],
      [{name, <<"Cluster Admin">>},
       {desc, <<"Can manage all cluster features except security. This user "
                "can access the web console. This user cannot read data.">>}],
      [{[admin, internal], none},
       {[admin, security], none},
       {[admin, diag], read},
       {[{bucket, any}, data], none},
       {[{bucket, any}, views], none},
       {[{bucket, any}, n1ql], none},
       {[{bucket, any}, fts], none},
       {[{bucket, any}, password], none},
       {[n1ql, curl], none},
       {[eventing], none},
       {[], all}]},
     {bucket_admin, [bucket_name],
      [{name, <<"Bucket Admin">>},
       {desc, <<"Can manage ALL bucket features for a given bucket (including "
                "start/stop XDCR). This user can access the web console. This "
                "user cannot read data.">>}],
      [{[{bucket, bucket_name}, xdcr], [read, execute]},
       {[{bucket, bucket_name}, data], none},
       {[{bucket, bucket_name}, views], none},
       {[{bucket, bucket_name}, n1ql], none},
       {[{bucket, bucket_name}, password], none},
       {[{bucket, bucket_name}, fts], none},
       {[{bucket, bucket_name}], all},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[xdcr], none},
       {[admin], none},
       {[eventing], none},
       {[], [read]}]},
     {bucket_full_access, [bucket_name],
      [{name, <<"Application Access">>},
       {desc, <<"Full access to bucket data. This user cannot access the web "
                "console and is intended only for application access. This "
                "user can read and write data.">>},
       {ce, true}],
      [{[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[{bucket, bucket_name}], [read, flush]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {views_admin, [bucket_name],
      [{name, <<"Views Admin">>},
       {desc, <<"Can create and manage views of a given bucket. This user can "
                "access the web console. This user can read some data.">>}],
      [{[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, data], [read]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}], none},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[xdcr], none},
       {[admin], none},
       {[eventing], none},
       {[], [read]}]},
     {views_reader, [bucket_name],
      [{name, <<"Views Reader">>},
       {desc, <<"Can read data from the views of a given bucket. This user "
                "cannot access the web console and is intended only for "
                "application access. This user can read some data.">>}],
      [{[{bucket, bucket_name}, views], [read]},
       {[{bucket, bucket_name}, data, docs], [read]},
       {[pools], [read]}]},
     {replication_admin, [],
      [{name, <<"XDCR Admin">>},
       {desc, <<"Can administer XDCR features to create cluster references and "
                "replication streams out of this cluster. This user can "
                "access the web console. This user can read some data.">>}],
      [{[{bucket, any}, xdcr], all},
       {[{bucket, any}, data], [read]},
       {[{bucket, any}, settings], [read]},
       {[{bucket, any}, stats], [read]},
       {[{bucket, any}], none},
       {[xdcr], all},
       {[admin], none},
       {[eventing], none},
       {[], [read]}]},
     {data_reader, [bucket_name],
      [{name, <<"Data Reader">>},
       {desc, <<"Can read data from a given bucket. This user cannot access "
                "the web console and is intended only for application access. "
                "This user can read data, but cannot write it.">>}],
      [{[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, data, meta], [read]},
       {[{bucket, bucket_name}, data, xattr], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {data_writer, [bucket_name],
      [{name, <<"Data Writer">>},
       {desc, <<"Can write data to a given bucket. This user cannot access the "
                "web console and is intended only for application access. This "
                "user can write data, but cannot read it.">>}],
      [{[{bucket, bucket_name}, data, docs], [insert, upsert, delete]},
       {[{bucket, bucket_name}, data, xattr], [write]},
       {[{bucket, bucket_name}, settings], [read]},
       {[pools], [read]}]},
     {data_dcp_reader, [bucket_name],
      [{name, <<"Data DCP Reader">>},
       {desc, <<"Can initiate DCP streams for a given bucket. This user cannot "
                "access the web console and is intended only for application "
                "access. This user can read data.">>}],
      [{[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, data, meta], [read]},
       {[{bucket, bucket_name}, data, dcp], [read]},
       {[{bucket, bucket_name}, data, sxattr], [read]},
       {[{bucket, bucket_name}, data, xattr], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[admin, memcached, idle], [write]},
       {[pools], [read]}]},
     {data_backup, [bucket_name],
      [{name, <<"Data Backup & Restore">>},
       {desc, <<"Can backup and restore a given bucket’s data. This user "
                "cannot access the web console and is intended only for "
                "application access. This user can read data.">>}],
      [{[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], [read, write]},
       {[{bucket, bucket_name}, fts], [read, write, manage]},
       {[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, n1ql, index], [create, list, build]},
       {[{bucket, bucket_name}, analytics], [manage, select]},
       {[analytics], [select, backup]},
       {[pools], [read]}]},
     {data_monitoring, [bucket_name],
      [{name, <<"Data Monitor">>},
       {desc, <<"Can read statistics for a given bucket. This user cannot "
                "access the web console and is intended only for application "
                "access. This user cannot read data.">>}],
      [{[{bucket, bucket_name}, stats], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[tasks], [read]},
       {[pools], [read]}]},
     {fts_admin, [bucket_name],
      [{name, <<"Search Admin">>},
       {desc, <<"Can administer all Full Text Search features. This user can "
                "access the web console. This user can read some data.">>}],
      [{[{bucket, bucket_name}, fts], [read, write, manage]},
       {[settings, fts], [read, write, manage]},
       {[ui], [read]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]}]},
     {fts_searcher, [bucket_name],
      [{name, <<"Search Reader">>},
       {desc, <<"Can query Full Text Search indexes for a given bucket. This "
                "user can access the web console. This user can read some "
                "data.">>}],
      [{[{bucket, bucket_name}, fts], [read]},
       {[settings, fts], [read]},
       {[ui], [read]},
       {[pools], [read]},
       {[{bucket, bucket_name}, settings], [read]}]},
     {query_select, [bucket_name],
      [{name, <<"Query Select">>},
       {desc, <<"Can execute a SELECT statement on a given bucket to retrieve "
                "data. This user can access the web console and can read data, "
                "but not write it.">>}],
      [{[{bucket, bucket_name}, n1ql, select], [execute]},
       {[{bucket, bucket_name}, data, docs], [read]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_update, [bucket_name],
      [{name, <<"Query Update">>},
       {desc, <<"Can execute an UPDATE statement on a given bucket to update "
                "data. This user can access the web console and write data, "
                "but cannot read it.">>}],
      [{[{bucket, bucket_name}, n1ql, update], [execute]},
       {[{bucket, bucket_name}, data, docs], [upsert]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_insert, [bucket_name],
      [{name, <<"Query Insert">>},
       {desc, <<"Can execute an INSERT statement on a given bucket to add "
                "data. This user can access the web console and insert data, "
                "but cannot read it.">>}],
      [{[{bucket, bucket_name}, n1ql, insert], [execute]},
       {[{bucket, bucket_name}, data, docs], [insert]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_delete, [bucket_name],
      [{name, <<"Query Delete">>},
       {desc, <<"Can execute a DELETE statement on a given bucket to delete "
                "data. This user can access the web console, but cannot read "
                "data. This user can delete data.">>}],
      [{[{bucket, bucket_name}, n1ql, delete], [execute]},
       {[{bucket, bucket_name}, data, docs], [delete]},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_manage_index, [bucket_name],
      [{name, <<"Query Manage Index">>},
       {desc, <<"Can manage indexes for a given bucket. This user can access "
                "the web console, but cannot read data.">>}],
      [{[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_system_catalog, [],
      [{name, <<"Query System Catalog">>},
       {desc, <<"Can look up system catalog information via N1QL. This user "
                "can access the web console, but cannot read user data.">>}],
      [{[{bucket, any}, n1ql, index], [list]},
       {[{bucket, any}, settings], [read]},
       {[n1ql, meta], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {query_external_access, [],
      [{name, <<"Query CURL Access">>},
       {desc, <<"Can execute the CURL statement from within N1QL. This user "
                "can access the web console, but cannot read data (within "
                "Couchbase).">>}],
      [{[n1ql, curl], [execute]},
       {[{bucket, any}, settings], [read]},
       {[ui], [read]},
       {[pools], [read]}]},
     {replication_target, [bucket_name],
      [{name, <<"XDCR Inbound">>},
       {desc, <<"Can create XDCR streams into a given bucket. This user cannot "
                "access the web console or read any data.">>}],
      [{[{bucket, bucket_name}, settings], [read]},
       {[{bucket, bucket_name}, data, meta], [read, write]},
       {[{bucket, bucket_name}, stats], [read]},
       {[pools], [read]}]},
     {analytics_manager, [bucket_name],
      [{name, <<"Analytics Manager">>},
       {desc, <<"Can manage Analytics local links. Can manage datasets on a "
                "given bucket. Can query datasets created on this bucket. "
                "This user can access the web console and read some data.">>}],
      [{[{bucket, bucket_name}, analytics], [manage, select]},
       {[ui], [read]},
       {[pools], [read]}]},
     {analytics_reader, [],
      [{name, <<"Analytics Reader">>},
       {desc, <<"Can query datasets. This is a global role as datasets may "
                "be created on different buckets. This user can access the "
                "web console and read some data.">>}],
      [{[analytics], [select]},
       {[{bucket, any}, analytics], [select]},
       {[ui], [read]},
       {[pools], [read]}]},
     {analytics_select, [bucket_name],
      [{name, <<"Analytics Select">>},
       {desc, <<"Can query datasets created on this bucket. This user can "
                "access the web console and read some data.">>}],
      [{[{bucket, bucket_name}, analytics], [select]},
       {[ui], [read]},
       {[pools], [read]}]},
     {analytics_admin, [],
      [{name, <<"Analytics Admin">>},
       {desc, <<"Can manage dataverses. Can manage all Analytics links. "
                "Can manage all datasets. This user can access the web "
                "console but cannot read data.">>}],
      [{[analytics], [manage]},
       {[{bucket, any}, analytics], [manage]},
       {[ui], [read]},
       {[pools], [read]}]},
     {mobile_sync_gateway, [bucket_name],
      [{name, <<"Sync Gateway">>},
       {desc, <<"Full access to bucket data as required by Sync Gateway. "
                "This user cannot access the web console and is intended "
                "only for use by Sync Gateway. This user can read and "
                "write data, manage indexes and views, and read some "
                "cluster information.">>}],
      [{[{bucket, bucket_name}, data], all},
       {[{bucket, bucket_name}, views], all},
       {[{bucket, bucket_name}, n1ql, index], all},
       {[{bucket, bucket_name}, n1ql], [execute]},
       {[{bucket, bucket_name}], [read, flush]},
       {[{bucket, bucket_name}, settings], [read]},
       {[admin, memcached, idle], [write]},
       {[settings, autocompaction], [read]},
       {[pools], [read]}]}].

-spec get_definitions() -> [rbac_role_def(), ...].
get_definitions() ->
    get_definitions(ns_config:latest()).

-spec get_definitions(ns_config()) -> [rbac_role_def(), ...].
get_definitions(Config) ->
    case cluster_compat_mode:is_cluster_55(Config) of
        true ->
            case cluster_compat_mode:is_cluster_66(Config) of
                true ->
                    roles_66();
                false ->
                    roles_55()
            end;
        false ->
            roles_50()
    end.

-spec object_match(rbac_permission_object(), rbac_permission_pattern_object()) ->
                          boolean().
object_match(_, []) ->
    true;
object_match([], [_|_]) ->
    false;
object_match([{_Same, _} | RestOfObject], [{_Same, any} | RestOfObjectPattern]) ->
    object_match(RestOfObject, RestOfObjectPattern);
object_match([{_Same, any} | RestOfObject], [{_Same, _} | RestOfObjectPattern]) ->
    object_match(RestOfObject, RestOfObjectPattern);
object_match([_Same | RestOfObject], [_Same | RestOfObjectPattern]) ->
    object_match(RestOfObject, RestOfObjectPattern);
object_match(_, _) ->
    false.

-spec get_allowed_operations(rbac_permission_object(), [rbac_permission_pattern()]) ->
                                    rbac_permission_pattern_operations().
get_allowed_operations(_Object, []) ->
    none;
get_allowed_operations(Object, [{ObjectPattern, AllowedOperations} | Rest]) ->
    case object_match(Object, ObjectPattern) of
        true ->
            AllowedOperations;
        false ->
            get_allowed_operations(Object, Rest)
    end.

-spec operation_allowed(rbac_operation(), rbac_permission_pattern_operations()) ->
                               boolean().
operation_allowed(_, all) ->
    true;
operation_allowed(_, none) ->
    false;
operation_allowed(any, _) ->
    true;
operation_allowed(Operation, AllowedOperations) ->
    lists:member(Operation, AllowedOperations).

-spec is_allowed(rbac_permission(), rbac_identity() | [rbac_compiled_role()]) -> boolean().
is_allowed(Permission, {_, _} = Identity) ->
    Roles = get_compiled_roles(Identity),
    is_allowed(Permission, Roles);
is_allowed({Object, Operation}, Roles) ->
    lists:any(fun (Role) ->
                      Operations = get_allowed_operations(Object, Role),
                      operation_allowed(Operation, Operations)
              end, Roles).

-spec substitute_params([string()], [atom()], [rbac_permission_pattern_raw()]) ->
                               [rbac_permission_pattern()].
substitute_params([], [], Permissions) ->
    Permissions;
substitute_params(Params, ParamDefinitions, Permissions) ->
    ParamPairs = lists:zip(ParamDefinitions, Params),
    lists:map(fun ({ObjectPattern, AllowedOperations}) ->
                      {lists:map(fun ({Name, any}) ->
                                         {Name, any};
                                     ({Name, Param}) ->
                                         {Param, Subst} = lists:keyfind(Param, 1, ParamPairs),
                                         {Name, Subst};
                                     (Vertex) ->
                                         Vertex
                                 end, ObjectPattern), AllowedOperations}
              end, Permissions).

-spec compile_params([atom()], [rbac_role_param()], rbac_all_param_values()) ->
                            false | [[rbac_role_param()]].
compile_params(ParamDefs, Params, AllParamValues) ->
    PossibleValues = get_possible_param_values(ParamDefs, AllParamValues),
    case find_matching_value(ParamDefs, Params, PossibleValues) of
        false ->
            false;
        Values ->
            Values
    end.

compile_roles(CompileRole, Roles, Definitions, AllParamValues) ->
    lists:filtermap(fun (Name) when is_atom(Name) ->
                            case lists:keyfind(Name, 1, Definitions) of
                                {Name, [], _Props, Permissions} ->
                                    {true, CompileRole(Name, [], [], Permissions)};
                                false ->
                                    false
                            end;
                        ({Name, Params}) ->
                            case lists:keyfind(Name, 1, Definitions) of
                                {Name, ParamDefs, _Props, Permissions} ->
                                    case compile_params(ParamDefs, Params, AllParamValues) of
                                        false ->
                                            false;
                                        NewParams ->
                                            {true, CompileRole(Name, NewParams, ParamDefs, Permissions)}
                                    end;
                                false ->
                                    false
                            end
                    end, Roles).

-spec compile_roles([rbac_role()], [rbac_role_def()] | undefined, rbac_all_param_values()) ->
                           [rbac_compiled_role()].
compile_roles(_Roles, undefined, _AllParamValues) ->
    %% can happen briefly after node joins the cluster on pre 5.0 clusters
    [];
compile_roles(Roles, Definitions, AllParamValues) ->
    compile_roles(
      fun (_Name, Params, ParamDefs, Permissions) ->
              substitute_params(strip_ids(ParamDefs, Params),
                                ParamDefs, Permissions)
      end, Roles, Definitions, AllParamValues).

-spec get_roles(rbac_identity()) -> [rbac_role()].
get_roles({"", wrong_token}) ->
    case ns_config_auth:is_system_provisioned() of
        false ->
            [admin];
        true ->
            []
    end;
get_roles({"", anonymous}) ->
    case ns_config_auth:is_system_provisioned() of
        false ->
            [admin];
        true ->
            [{bucket_full_access, [BucketName]} ||
                BucketName <- ns_config_auth:get_no_auth_buckets(ns_config:latest())]
    end;
get_roles({_, admin}) ->
    [admin];
get_roles({BucketName, bucket}) ->
    [{bucket_full_access, [BucketName]}];
get_roles({_User, external} = Identity) ->
    menelaus_users:get_roles(Identity);
get_roles({_User, local} = Identity) ->
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
        fun ({buckets, _}) ->
                true;
            ({cluster_compat_version, _}) ->
                true;
            ({rest_creds, _}) ->
                true;
            (_) ->
                false
        end,
    GetVersion =
        fun () ->
                {cluster_compat_mode:get_compat_version(ns_config:latest()),
                 menelaus_users:get_users_version(),
                 menelaus_users:get_groups_version(),
                 ns_config_auth:is_system_provisioned(),
                 [{Name, ns_bucket:bucket_uuid(BucketConfig)} ||
                     {Name, BucketConfig} <- ns_bucket:get_buckets(ns_config:latest())]}
        end,
    GetEvents =
        case ns_node_disco:couchdb_node() == node() of
            true ->
                fun () ->
                        dist_manager:wait_for_node(fun ns_node_disco:ns_server_node/0),
                        [{{user_storage_events, ns_node_disco:ns_server_node()}, UsersFilter},
                         {ns_config_events, ConfigFilter}]
                end;
            false ->
                fun () ->
                        [{user_storage_events, UsersFilter},
                         {ns_config_events, ConfigFilter}]
                end
        end,

    versioned_cache:start_link(
      compiled_roles_cache_name(), 200, fun build_compiled_roles/1,
      GetEvents, GetVersion).

-spec get_compiled_roles(rbac_identity()) -> [rbac_compiled_role()].
get_compiled_roles({_, external} = Identity) ->
    roles_cache:build_compiled_roles(Identity);
get_compiled_roles(Identity) ->
    versioned_cache:get(compiled_roles_cache_name(), Identity).

build_compiled_roles(Identity) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            ?log_debug("Compile roles for user ~p",
                       [ns_config_log:tag_user_data(Identity)]),
            Definitions = get_definitions(),
            AllPossibleValues = calculate_possible_param_values(ns_bucket:get_buckets()),
            compile_roles(get_roles(Identity), Definitions, AllPossibleValues);
        true ->
            ?log_debug("Retrieve compiled roles for user ~p from ns_server "
                       "node", [ns_config_log:tag_user_data(Identity)]),
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE, build_compiled_roles, [Identity])
    end.

filter_out_invalid_roles(Roles, Definitions, AllPossibleValues) ->
    compile_roles(fun (Name, [], _, _) ->
                          Name;
                      (Name, Params, _, _) ->
                          {Name, Params}
                  end, Roles, Definitions, AllPossibleValues).

calculate_possible_param_values(_Buckets, []) ->
    [[]];
calculate_possible_param_values(Buckets, [bucket_name]) ->
    [[any] | [[{Name, ns_bucket:bucket_uuid(Props)}] || {Name, Props} <- Buckets]].

all_params_combinations() ->
    [[], [bucket_name]].

-spec calculate_possible_param_values(list()) -> rbac_all_param_values().
calculate_possible_param_values(Buckets) ->
    [{Combination, calculate_possible_param_values(Buckets, Combination)} ||
        Combination <- all_params_combinations()].

-spec get_possible_param_values([atom()], rbac_all_param_values()) -> [[rbac_role_param()]].
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

filter_by_permission(undefined, _ParamValues, _Definitions) ->
    pipes:filter(fun (_) -> true end);
filter_by_permission(Permission, ParamValues, Definitions) ->
    pipes:filter(
      fun ({Role, _}) ->
              menelaus_roles:is_allowed(
                Permission, compile_roles([Role], Definitions, ParamValues))
      end).

-spec produce_roles_by_permission(rbac_permission(), ns_config(), list()) ->
                                         pipes:producer(rbac_role()).
produce_roles_by_permission(Permission, Config, Buckets) ->
    AllValues = calculate_possible_param_values(Buckets),
    Definitions = get_definitions(Config),
    pipes:compose(
      [pipes:stream_list(Definitions),
       visible_roles_filter(),
       expand_params(AllValues),
       filter_by_permission(Permission, AllValues, Definitions)]).

strip_id(bucket_name, {P, _Id}) ->
    P;
strip_id(bucket_name, P) ->
    P.

strip_ids(ParamDefs, Params) ->
    [strip_id(ParamDef, Param) || {ParamDef, Param} <- lists:zip(ParamDefs, Params)].

match_param(bucket_name, P, P) ->
    true;
match_param(bucket_name, P, {P, _Id}) ->
    true;
match_param(bucket_name, _, _) ->
    false.

match_params([], [], []) ->
    true;
match_params(ParamDefs, Params, Values) ->
    case lists:dropwhile(
           fun ({ParamDef, Param, Value}) ->
                   match_param(ParamDef, Param, Value)
           end, lists:zip3(ParamDefs, Params, Values)) of
        [] ->
            true;
        _ ->
            false
    end.

-spec find_matching_value([atom()], [rbac_role_param()], [[rbac_role_param()]]) ->
                                 false | [rbac_role_param()].
find_matching_value(ParamDefs, Params, PossibleValues) ->
    case lists:dropwhile(
           fun (Values) ->
                   not match_params(ParamDefs, Params, Values)
           end, PossibleValues) of
        [] ->
            false;
        [V | _] ->
            V
    end.

-spec validate_role(rbac_role(), [rbac_role_def()], [[rbac_role_param()]]) ->
                           false | {ok, rbac_role()}.
validate_role(Role, Definitions, AllValues) when is_atom(Role) ->
    validate_role(Role, [], Definitions, AllValues);
validate_role({Role, Params}, Definitions, AllValues) ->
    validate_role(Role, Params, Definitions, AllValues).

validate_role(Role, Params, Definitions, AllValues) ->
    case lists:keyfind(Role, 1, Definitions) of
        {Role, ParamsDef, _, _} when length(Params) =:= length(ParamsDef) ->
            PossibleValues = get_possible_param_values(ParamsDef, AllValues),
            case find_matching_value(ParamsDef, Params, PossibleValues) of
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

validate_roles(Roles, Config) ->
    Definitions = pipes:run(pipes:stream_list(get_definitions(Config)),
                            visible_roles_filter(),
                            pipes:collect()),
    AllValues = calculate_possible_param_values(ns_bucket:get_buckets(Config)),
    lists:foldl(fun (Role, {Validated, Unknown}) ->
                        case validate_role(Role, Definitions, AllValues) of
                            false ->
                                {Validated, [Role | Unknown]};
                            {ok, R} ->
                                {[R | Validated], Unknown}
                        end
                end, {[], []}, Roles).

get_security_roles() ->
    get_security_roles(ns_config:latest()).

get_security_roles(Config) ->
    pipes:run(produce_roles_by_permission({[admin, security], any}, Config, []),
              pipes:collect()).

external_auth_polling_interval() ->
    ns_config:read_key_fast(external_auth_polling_interval,
                            ?DEFAULT_EXTERNAL_ROLES_POLLING_INTERVAL).


-ifdef(TEST).
filter_out_invalid_roles_test() ->
    Roles = [{role1, [{"bucket1", <<"id1">>}]},
             {role2, [{"bucket2", <<"id2">>}]}],
    Definitions = [{role1, [bucket_name],
                    [{name,<<"">>},{desc, <<"">>}],
                    [{[{bucket,bucket_name},settings],[read]}]},
                   {role2, [bucket_name],
                    [{name,<<"">>},{desc, <<"">>}],
                    [{[{bucket,bucket_name},n1ql,update],[execute]}]}],
    PossibleValues = [{[],[[]]},
                      {[bucket_name],
                       [[any],
                       [{"bucket1",<<"id1">>}]]}],
    ?assertEqual([{role1, [{"bucket1", <<"id1">>}]}],
                 filter_out_invalid_roles(Roles, Definitions, PossibleValues)).

%% assertEqual is used instead of assert and assertNot to avoid
%% dialyzer warnings
object_match_test() ->
    ?assertEqual(true, object_match([o1, o2], [o1, o2])),
    ?assertEqual(false, object_match([o1], [o1, o2])),
    ?assertEqual(true, object_match([o1, o2], [o1])),
    ?assertEqual(true, object_match([{b, "a"}], [{b, "a"}])),
    ?assertEqual(false, object_match([{b, "a"}], [{b, "b"}])),
    ?assertEqual(true, object_match([{b, any}], [{b, "b"}])),
    ?assertEqual(true, object_match([{b, "a"}], [{b, any}])),
    ?assertEqual(true, object_match([{b, any}], [{b, any}])).

toy_config() ->
    [[{buckets,
       [{configs,
         [{"test", [{uuid, <<"test_id">>}]},
          {"default", [{uuid, <<"default_id">>}]}]}]}]].

compile_roles(Roles, Definitions) ->
    AllPossibleValues = calculate_possible_param_values(ns_bucket:get_buckets(toy_config())),
    compile_roles(Roles, Definitions, AllPossibleValues).

compile_roles_test() ->
    ?assertEqual([[{[{bucket, "test"}], none}]],
                 compile_roles([{test_role, ["test"]}],
                               [{test_role, [bucket_name], [], [{[{bucket, bucket_name}], none}]}])).

admin_test() ->
    Roles = compile_roles([admin], roles_50()),
    ?assertEqual(true, is_allowed({[buckets], create}, Roles)),
    ?assertEqual(true, is_allowed({[something, something], anything}, Roles)).

ro_admin_test() ->
    Roles = compile_roles([ro_admin], roles_50()),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, password], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, data], read}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "test"}, something], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "test"}, something], write}, Roles)),
    ?assertEqual(false, is_allowed({[admin, security], write}, Roles)),
    ?assertEqual(true, is_allowed({[admin, security], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin, other], write}, Roles)),
    ?assertEqual(true, is_allowed({[anything], read}, Roles)),
    ?assertEqual(false, is_allowed({[anything], write}, Roles)).

bucket_views_admin_check_global(Roles) ->
    ?assertEqual(false, is_allowed({[xdcr], read}, Roles)),
    ?assertEqual(false, is_allowed({[admin], read}, Roles)),
    ?assertEqual(true, is_allowed({[something], read}, Roles)),
    ?assertEqual(false, is_allowed({[something], write}, Roles)),
    ?assertEqual(false, is_allowed({[buckets], create}, Roles)).

bucket_views_admin_check_another(Roles) ->
    ?assertEqual(false, is_allowed({[{bucket, "another"}, xdcr], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "another"}, views], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "another"}, data], read}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "another"}, settings], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "another"}, settings], write}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "another"}], read}, Roles)),
    ?assertEqual(false, is_allowed({[buckets], create}, Roles)).

bucket_admin_check_default(Roles) ->
    ?assertEqual(true, is_allowed({[{bucket, "default"}, xdcr], read}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, xdcr], execute}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, anything], anything}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, anything], anything}, Roles)).

bucket_admin_test() ->
    Roles = compile_roles([{bucket_admin, ["default"]}], roles_50()),
    bucket_admin_check_default(Roles),
    bucket_views_admin_check_another(Roles),
    bucket_views_admin_check_global(Roles).

bucket_admin_wildcard_test() ->
    Roles = compile_roles([{bucket_admin, [any]}], roles_50()),
    bucket_admin_check_default(Roles),
    bucket_views_admin_check_global(Roles).

views_admin_check_default(Roles) ->
    ?assertEqual(true, is_allowed({[{bucket, "default"}, views], anything}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, data], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, data], write}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, settings], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, settings], write}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}], read}, Roles)).

views_admin_test() ->
    Roles = compile_roles([{views_admin, ["default"]}], roles_50()),
    views_admin_check_default(Roles),
    bucket_views_admin_check_another(Roles),
    bucket_views_admin_check_global(Roles).

views_admin_wildcard_test() ->
    Roles = compile_roles([{views_admin, [any]}], roles_50()),
    views_admin_check_default(Roles),
    bucket_views_admin_check_global(Roles).

bucket_full_access_check(Roles, Bucket, Allowed) ->
    ?assertEqual(Allowed, is_allowed({[{bucket, Bucket}, data], anything}, Roles)),
    ?assertEqual(Allowed, is_allowed({[{bucket, Bucket}], flush}, Roles)),
    ?assertEqual(Allowed, is_allowed({[{bucket, Bucket}], flush}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, Bucket}], write}, Roles)).

bucket_full_access_test() ->
    Roles = compile_roles([{bucket_full_access, ["default"]}], roles_50()),
    bucket_full_access_check(Roles, "default", true),
    bucket_full_access_check(Roles, "another", false),
    ?assertEqual(true, is_allowed({[pools], read}, Roles)),
    ?assertEqual(false, is_allowed({[another], read}, Roles)).

replication_admin_test() ->
    Roles = compile_roles([replication_admin], roles_50()),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, xdcr], anything}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, password], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, views], read}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, settings], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, settings], write}, Roles)),
    ?assertEqual(true, is_allowed({[{bucket, "default"}, data], read}, Roles)),
    ?assertEqual(false, is_allowed({[{bucket, "default"}, data], write}, Roles)),
    ?assertEqual(true, is_allowed({[xdcr], anything}, Roles)),
    ?assertEqual(false, is_allowed({[admin], read}, Roles)),
    ?assertEqual(true, is_allowed({[other], read}, Roles)).

validate_role_test() ->
    Config = toy_config(),
    Definitions = roles_50(),
    AllParamValues = calculate_possible_param_values(ns_bucket:get_buckets(Config)),
    ?assertEqual({ok, admin}, validate_role(admin, Definitions, AllParamValues)),
    ?assertEqual({ok, {bucket_admin, [{"test", <<"test_id">>}]}},
                 validate_role({bucket_admin, ["test"]}, Definitions, AllParamValues)),
    ?assertEqual({ok, {views_admin, [any]}},
                 validate_role({views_admin, [any]}, Definitions, AllParamValues)),
    ?assertEqual(false, validate_role(something, Definitions, AllParamValues)),
    ?assertEqual(false, validate_role({bucket_admin, ["something"]}, Definitions, AllParamValues)),
    ?assertEqual(false, validate_role({something, ["test"]}, Definitions, AllParamValues)),
    ?assertEqual(false, validate_role({admin, ["test"]}, Definitions, AllParamValues)),
    ?assertEqual(false, validate_role(bucket_admin, Definitions, AllParamValues)),
    ?assertEqual(false, validate_role({bucket_admin, ["test", "test"]}, Definitions, AllParamValues)).
-endif.
