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
%% @doc handling of memcached permissions file

-module(memcached_permissions).

-behaviour(memcached_cfg).

-export([start_link/0, sync/0, jsonify_user/3, spec_users/0]).

%% callbacks
-export([init/0, filter_event/1, handle_event/2, producer/1, refresh/0]).

-include("ns_common.hrl").
-include("pipes.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {buckets,
                param_values,
                roles,
                users,
                cluster_admin}).

bucket_permissions_to_check(Bucket) ->
    [{{[{bucket, Bucket}, data, docs], read},   'Read'},
     {{[{bucket, Bucket}, data, docs], insert}, 'Insert'},
     {{[{bucket, Bucket}, data, docs], delete}, 'Delete'},
     {{[{bucket, Bucket}, data, docs], upsert}, 'Upsert'},
     {{[{bucket, Bucket}, stats], read},        'SimpleStats'},
     {{[{bucket, Bucket}, data, dcp], read},    'DcpProducer'},
     {{[{bucket, Bucket}, data, dcp], write},   'DcpConsumer'},
     {{[{bucket, Bucket}, data, meta], read},   'MetaRead'},
     {{[{bucket, Bucket}, data, meta], write},  'MetaWrite'},
     {{[{bucket, Bucket}, data, xattr], read},  'XattrRead'},
     {{[{bucket, Bucket}, data, xattr], write}, 'XattrWrite'},
     {{[{bucket, Bucket}, data, sxattr], read}, 'SystemXattrRead'},
     {{[{bucket, Bucket}, data, sxattr], write},'SystemXattrWrite'}].

global_permissions_to_check() ->
    [{{[stats, memcached], read},           'Stats'},
     {{[buckets], create},                  'BucketManagement'},
     {{[admin, memcached, node], write},    'NodeManagement'},
     {{[admin, memcached, session], write}, 'SessionManagement'},
     {{[admin, memcached, idle], write},    'IdleConnection'},
     {{[admin, security, audit], write},    'AuditManagement'},
     {{[pools], read},                      'SystemSettings'}].

start_link() ->
    Path = ns_config:search_node_prop(ns_config:latest(), memcached, rbac_file),
    memcached_cfg:start_link(?MODULE, Path).

sync() ->
    memcached_cfg:sync(?MODULE).

init() ->
    Config = ns_config:get(),
    AdminUser =
        case ns_config:search(Config, rest_creds) of
            {value, {U, _}} -> U;
            _ -> undefined
        end,
    #state{buckets = ns_bucket:get_bucket_names(ns_bucket:get_buckets(Config)),
           param_values =
               menelaus_roles:calculate_possible_param_values(ns_bucket:get_buckets(Config)),
           users = spec_users(Config),
           roles = menelaus_roles:get_definitions(Config),
           cluster_admin = AdminUser}.

spec_users() -> spec_users(ns_config:get()).
spec_users(Config) ->
    [ns_config:search_node_prop(Config, memcached, admin_user) |
     ns_config:search_node_prop(Config, memcached, other_users, [])].

filter_event({buckets, _V}) ->
    true;
filter_event({cluster_compat_version, _V}) ->
    true;
filter_event({group_version, _V}) ->
    true;
filter_event({user_version, _V}) ->
    true;
filter_event({rest_creds, _V}) ->
    true;
filter_event(_) ->
    false.

handle_event({buckets, V}, #state{buckets = Buckets, param_values = ParamValues} = State) ->
    Configs = proplists:get_value(configs, V),
    case {ns_bucket:get_bucket_names(Configs),
          menelaus_roles:calculate_possible_param_values(Configs)} of
        {Buckets, ParamValues} ->
            unchanged;
        {NewBuckets, NewParamValues} ->
            {changed, State#state{buckets = NewBuckets, param_values = NewParamValues}}
    end;
handle_event({user_version, _V}, State) ->
    {changed, State};
handle_event({group_version, _V}, State) ->
    {changed, State};
handle_event({cluster_compat_version, _V}, #state{roles = Roles} = State) ->
    case menelaus_roles:get_definitions() of
        Roles ->
            unchanged;
        NewRoles ->
            {changed, State#state{roles = NewRoles}}
    end;
handle_event({rest_creds, {ClusterAdmin, _}}, #state{cluster_admin = ClusterAdmin}) ->
    unchanged;
handle_event({rest_creds, {ClusterAdmin, _}}, State) ->
    {changed, State#state{cluster_admin = ClusterAdmin}};
handle_event({rest_creds, _}, #state{cluster_admin = undefined}) ->
    unchanged;
handle_event({rest_creds, _}, State) ->
    {changed, State#state{cluster_admin = undefined}}.

refresh() ->
    memcached_refresh:refresh(rbac).

bucket_permissions(Bucket, CompiledRoles) ->
    lists:usort([MemcachedPermission ||
                    {Permission, MemcachedPermission} <- bucket_permissions_to_check(Bucket),
                    menelaus_roles:is_allowed(Permission, CompiledRoles)]).

global_permissions(CompiledRoles) ->
    lists:usort([MemcachedPermission ||
                    {Permission, MemcachedPermission} <- global_permissions_to_check(),
                    menelaus_roles:is_allowed(Permission, CompiledRoles)]).

format_permissions(Buckets, CompiledRoles) ->
    [{global, global_permissions(CompiledRoles)} |
     [{Bucket, bucket_permissions(Bucket, CompiledRoles)}
         || Bucket <- Buckets]].

permissions_for_role(Buckets, ParamValues, RoleDefinitions, Role) ->
    CompiledRoles = menelaus_roles:compile_roles([Role], RoleDefinitions, ParamValues),
    format_permissions(Buckets, CompiledRoles).

permissions_for_role(Buckets, ParamValues, RoleDefinitions, Role, RolesDict) ->
    case dict:find(Role, RolesDict) of
        {ok, Permissions} ->
            {Permissions, RolesDict};
        error ->
            Permissions = permissions_for_role(Buckets, ParamValues, RoleDefinitions, Role),
            {Permissions, dict:store(Role, Permissions, RolesDict)}
    end.

zip_permissions(Permissions, PermissionsAcc) ->
    lists:zipwith(fun ({Bucket, Perm}, {Bucket, PermAcc}) ->
                          {Bucket, [Perm | PermAcc]}
                  end, Permissions, PermissionsAcc).

permissions_for_user(Roles, Buckets, ParamValues, RoleDefinitions, RolesDict) ->
    Acc0 = [{global, []} | [{Bucket, []} || Bucket <- Buckets]],
    {ZippedPermissions, NewRolesDict} =
        lists:foldl(fun (Role, {Acc, Dict}) ->
                            {Permissions, NewDict} =
                                permissions_for_role(Buckets, ParamValues, RoleDefinitions, Role, Dict),
                            {zip_permissions(Permissions, Acc), NewDict}
                    end, {Acc0, RolesDict}, Roles),
    MergedPermissions = [{Bucket, lists:umerge(Perm)} || {Bucket, Perm} <- ZippedPermissions],
    {MergedPermissions, NewRolesDict}.

jsonify_user(Identity, CompiledRoles, Buckets) ->
    jsonify_user(Identity, format_permissions(Buckets, CompiledRoles)).

jsonify_user({UserName, Domain}, [{global, GlobalPermissions} | BucketPermissions]) ->
    Buckets = {buckets, {[{list_to_binary(BucketName), Permissions} ||
                             {BucketName, Permissions} <- BucketPermissions]}},
    Global = {privileges, GlobalPermissions},
    {list_to_binary(UserName), {[Buckets, Global, {domain, Domain}]}}.

memcached_admin_json(AU) ->
    jsonify_user({AU, local}, [{global, [all]}, {"*", [all]}]).

jsonify_users(Users, Buckets, ParamValues, RoleDefinitions, ClusterAdmin) ->
    ?make_transducer(
       begin
           ?yield(object_start),
           lists:foreach(fun (U) ->
                                 ?yield({kv, memcached_admin_json(U)})
                         end, Users),

           EmitUser =
               fun (Identity, Roles, Dict) ->
                       {Permissions, NewDict} =
                           permissions_for_user(Roles, Buckets, ParamValues, RoleDefinitions, Dict),
                       ?yield({kv, jsonify_user(Identity, Permissions)}),
                       NewDict
               end,

           Dict1 =
               case ClusterAdmin of
                   undefined ->
                       dict:new();
                   _ ->
                       Roles1 = menelaus_roles:get_roles({ClusterAdmin, admin}),
                       EmitUser({ClusterAdmin, local}, Roles1, dict:new())
               end,

           Dict2 =
               lists:foldl(
                 fun (Bucket, Dict) ->
                         LegacyName = Bucket ++ ";legacy",
                         Roles2 = menelaus_roles:get_roles({Bucket, bucket}),
                         EmitUser({LegacyName, local}, Roles2, Dict)
                 end, Dict1, Buckets),

           pipes:fold(
             ?producer(),
             fun ({{user, {UserName, _} = Identity}, Props}, Dict) ->
                     case UserName of
                         ClusterAdmin ->
                             TagCA = ns_config_log:tag_user_name(ClusterAdmin),
                             ?log_warning("Encountered user ~p with the same
                                          name as cluster administrator",
                                          [TagCA]),
                             Dict;
                         _ ->
                             Roles3 = proplists:get_value(roles, Props, []),
                             EmitUser(Identity, Roles3, Dict)
                     end
             end, Dict2),
           ?yield(object_end)
       end).

producer(#state{buckets = Buckets,
                param_values = ParamValues,
                roles = RoleDefinitions,
                users = Users,
                cluster_admin = ClusterAdmin}) ->
    pipes:compose([menelaus_users:select_users({'_', local}, [roles]),
                   jsonify_users(Users, Buckets, ParamValues, RoleDefinitions, ClusterAdmin),
                   sjson:encode_extended_json([{compact, false},
                                               {strict, false}])]).
