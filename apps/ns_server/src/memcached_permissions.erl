%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc handling of memcached permissions file

-module(memcached_permissions).

-behaviour(memcached_cfg).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0, sync/0, sync_reload/0, jsonify_user_with_cache/2,
         spec_users/0, get_key_ids_in_use/0]).

%% callbacks
-export([init/0, filter_event/1, handle_event/2, producer/1, refresh/0]).

-include("ns_common.hrl").
-include("pipes.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("rbac.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("ns_test.hrl").
-endif.

-record(state, {buckets,
                users,
                cluster_admin,
                prometheus_user}).

standard_collection_permissions([B, S, C]) ->
    [{{[{collection, [B, S, C]}, data, docs], read},      'Read'},
     {{[{collection, [B, S, C]}, data, docs], insert},    'Insert'},
     {{[{collection, [B, S, C]}, data, docs], delete},    'Delete'},
     {{[{collection, [B, S, C]}, data, docs], upsert},    'Upsert'},
     {{[{collection, [B, S, C]}, data, docs], range_scan}, 'RangeScan'},
     {{[{collection, [B, S, C]}, data, meta], write},     'MetaWrite'},
     {{[{collection, [B, S, C]}, data, sxattr], read},    'SystemXattrRead'},
     {{[{collection, [B, S, C]}, data, sxattr], write},   'SystemXattrWrite'},
     {{[{collection, [B, S, C]}, data, dcpstream], read}, 'DcpStream'},
     {{[{collection, [B, S, C]}, stats], read},           'SimpleStats'}].

system_collection_permissions([B, S, C]) ->
    [{{[{collection, [B, S, C]}, data, docs], sread},
      'SystemCollectionLookup'},
     {{[{collection, [B, S, C]}, data, docs], swrite},
      'SystemCollectionMutation'}] ++
        standard_collection_permissions([B, S, C]).

%% If C is a filter (any/all), include system collection permissions -
%% for example, we may be specifying a bucket-level system collection
%% permission. This will be called as [B, all, all]:
collection_permissions_to_check([B, S, C]) when is_atom(C) ->
    system_collection_permissions([B, S, C]);

%% A system collection is defined in kv (7.6) as a collection that begins with
%% "_" (there are exceptions like [B, _default, _default]). The set of system
%% collection permissions is a superset of standard ones, so ignore outliers.
collection_permissions_to_check([B, S, [$_|Name]]) ->
    system_collection_permissions([B, S, [$_|Name]]);

collection_permissions_to_check([B, S, C]) ->
    standard_collection_permissions([B, S, C]).

bucket_permissions_to_check(Bucket) ->
    [{{[admin, internal, stats], read},         'SimpleStats'},
     {{[{bucket, Bucket}, data, dcp], read},    'DcpProducer'},
     {{[{bucket, Bucket}, data, dcp], write},   'DcpConsumer'}].

global_permissions_to_check() ->
    [{{[stats, memcached], read},              'Stats'},
     {{[admin, internal, stats], read},        'Stats'},
     {{[admin, memcached, idle], write},       'IdleConnection'},
     {{[admin, memcached], all},               'Administrator'},
     {{[pools], read},                         'SystemSettings'},
     {{[admin, security, admin], impersonate}, 'Impersonate'}].

metered_users() ->
    ["@cbq-engine", "@goxdcr", "@backup"].

bucket_throttle_users() ->
    %% The KV regulator runs in the projector process, and (currently) uses
    %% the "@projector" user to interact with KV.
    ["@projector"].

metered_privilege(User) ->
    case lists:member(User, metered_users()) of
        true -> [];
        false -> ['Unmetered']
    end.

bucket_throttle_privilege(User) ->
    case lists:member(User, bucket_throttle_users()) of
        true -> ['BucketThrottleManagement'];
        false -> []
    end.

other_users_privileges(User) ->
    ['Administrator', 'Audit', 'IdleConnection', 'Impersonate',
     'SystemSettings', 'Stats'] ++ metered_privilege(User)
        ++ bucket_throttle_privilege(User).

admin_user_privileges() ->
    ['NodeSupervisor', 'BucketThrottleManagement', 'Unthrottled'].

start_link() ->
    Path = ns_config:search_node_prop(ns_config:latest(), memcached, rbac_file),
    memcached_cfg:start_link(?MODULE, Path).

sync() ->
    memcached_cfg:sync(?MODULE).

sync_reload() ->
    memcached_cfg:sync_reload(?MODULE).

get_key_ids_in_use() ->
    memcached_cfg:get_key_ids_in_use(?MODULE).

init() ->
    #state{buckets = ns_bucket:uuids(),
           users = spec_users(),
           cluster_admin = ns_config_auth:get_user(admin),
           prometheus_user = prom_user()}.

prom_user() ->
    case prometheus_cfg:get_auth_info() of
        {U, _} ->
            U;
        undefined ->
            undefined
    end.

spec_users() ->
    [ns_config:search_node_prop(ns_config:latest(), memcached, admin_user) |
     ns_config:search_node_prop(ns_config:latest(), memcached,
                                other_users, [])].

filter_event(group_version) ->
    true;
filter_event(user_version) ->
    true;
filter_event(rest_creds) ->
    true;
filter_event({node, Node, prometheus_auth_info}) ->
    Node =:= node();
filter_event(Key) ->
    (collections:key_match(Key) =/= false)
        orelse ns_bucket:buckets_change(Key).

handle_event(user_version, State) ->
    {changed, State};
handle_event(group_version, State) ->
    {changed, State};
handle_event(rest_creds, #state{cluster_admin = ClusterAdmin} = State) ->
    case ns_config_auth:get_user(admin) of
        ClusterAdmin ->
            unchanged;
        Other ->
            {changed, State#state{cluster_admin = Other}}
    end;
handle_event({node, Node, prometheus_auth_info},
             #state{prometheus_user = User} = State) when Node =:= node() ->
    case prom_user() of
        User ->
            unchanged;
        Other ->
            {changed, State#state{prometheus_user = Other}}
    end;
handle_event({node, _OtherNode, prometheus_auth_info}, #state{} = _State) ->
    unchanged;
handle_event(Key, #state{buckets = Buckets} = State) ->
    case collections:key_match(Key) of
        false ->
            true = ns_bucket:buckets_change(Key),
            case ns_bucket:uuids() of
                Buckets ->
                    unchanged;
                NewBuckets ->
                    {changed, State#state{buckets = NewBuckets}}
            end;
        {true, _} ->
            {changed, State}
    end.

refresh() ->
    memcached_refresh:refresh(rbac).

-record(priv_object, {privileges, children}).

priv_set(Privilege, Object, PrivTree) ->
    priv_set(Privilege, Object, PrivTree, #{}).

%% Note that privileges can be set at the bucket, scope or collection level.
%% If the same privilege is applied to a parent level, priv_set retains the
%% privilege in its children (as they *do not* inherit the parent's privileges).
%% For example, specifying:
%% {[{collection, [bucket1, S, C]}, data, docs], [sread]} and
%% {[{bucket, bucket1}, data, docs], [sread]}
%% sets SystemCollectionLookup at both the bucket and collection levels.
priv_set(Privilege, [Object], PrivTree, ParentPrivileges) ->
    maps:update_with(
      Object,
      fun (#priv_object{privileges = Privileges} = PrivObj) ->
              PrivObj#priv_object{privileges = Privileges#{Privilege => true}}
      end,
      #priv_object{privileges = ParentPrivileges#{Privilege => true},
                   children = #{}},
      PrivTree);
priv_set(Privilege, [Object | Rest], PrivTree, ParentPrivileges) ->
    PrivTree#{Object =>
             case maps:find(Object, PrivTree) of
                 {ok, #priv_object{privileges = NextParentPrivileges,
                                   children = Children} = Old} ->
                     Old#priv_object{
                       children = priv_set(Privilege, Rest, Children,
                                           NextParentPrivileges)};
                 error ->
                     #priv_object{privileges = ParentPrivileges,
                                  children = priv_set(Privilege, Rest, #{},
                                                      ParentPrivileges)}
             end}.

priv_unset(Privilege, Object, PrivTree) ->
    priv_unset(Privilege, Object, PrivTree, #{}).

priv_unset(Privilege, [Object], PrivTree, ParentPrivileges) ->
    maps:update_with(
      Object,
      fun (#priv_object{privileges = Privileges} = PrivObj) ->
              PrivObj#priv_object{privileges = maps:remove(Privilege,
                  Privileges)}
      end,
      #priv_object{privileges = maps:remove(Privilege, ParentPrivileges),
          children = #{}},
      PrivTree);
priv_unset(Privilege, [Object | Rest], PrivTree, ParentPrivileges) ->
    PrivTree#{Object =>
             case maps:find(Object, PrivTree) of
                 {ok, #priv_object{privileges = NextParentPrivileges,
                                   children = Children} = Old} ->
                     Old#priv_object{
                       children = priv_unset(Privilege, Rest, Children,
                           NextParentPrivileges)};
                 error ->
                     #priv_object{privileges = ParentPrivileges,
                                  children = priv_unset(Privilege, Rest, #{},
                                      ParentPrivileges)}
             end}.

bucket_permissions(BucketName, CompiledRoles, Acc) ->
    lists:foldl(
      fun ({Permission, MemcachedPrivilege}, Acc1) ->
              case menelaus_roles:is_allowed(Permission, CompiledRoles) of
                  true ->
                      priv_set(MemcachedPrivilege, [BucketName], Acc1);
                  false ->
                      Acc1
              end
      end, Acc, bucket_permissions_to_check(BucketName)).

get_priv_object_key([Bucket | Rest]) ->
    [case Bucket of
         {N, _} ->
             N;
         _ ->
             Bucket
     end | [Id || {_, Id} <- Rest]].

%% Params is a collection param [bucket_name, scope_name, collection_name]
%% where bucket_name cannot be any.
collection_permissions({[{collection, Params} | ObjRest], Ops}, PrivTree) ->
    ToCheck = menelaus_roles:strip_ids(?RBAC_COLLECTION_PARAMS, Params),
    ObjStripped = [{collection, ToCheck} | ObjRest],
    ToStore = get_priv_object_key(Params),
    lists:foldl(
      fun ({{ObjToCheck, OpToCheck}, MemcachedPrivilege}, Acc1) ->
              ObjMatches = lists:sublist(ObjToCheck, length(ObjStripped)) =:=
                  ObjStripped,
              OpMatches = Ops =:= all orelse
                        (Ops =/= none andalso lists:member(OpToCheck, Ops)),
              case {ObjMatches, OpMatches} of
                  {true, true} ->
                      priv_set(MemcachedPrivilege, ToStore, Acc1);
                  {true, false} ->
                      priv_unset(MemcachedPrivilege, ToStore, Acc1);
                  _ ->
                      Acc1
              end
      end, PrivTree, collection_permissions_to_check(ToCheck)).

global_permissions(CompiledRoles) ->
    lists:usort(
      [MemcachedPermission ||
          {Permission, MemcachedPermission} <- global_permissions_to_check(),
          menelaus_roles:is_allowed(Permission, CompiledRoles)]).

remove_trailing_any([any, any, any]) ->
    [];
remove_trailing_any([B, any, any]) ->
    [B];
remove_trailing_any([B, S, any]) ->
    [B, S];
remove_trailing_any([B, S, C]) ->
    [B, S, C].

get_params([{bucket, B} | Rest]) ->
    {[B, any, any], Rest};
get_params([{scope, [B, S]} | Rest]) ->
    {[B, S, any], Rest};
get_params([{collection, [B, S, C]} | Rest]) ->
    {[B, S, C], Rest};
get_params(Rest) ->
    {[any, any, any], Rest}.

expand_collection_params({PermissionObject, Operations}, Snapshot) ->
    {CollectionParams, ObjRest} = get_params(PermissionObject),
    %% memcached permissions are specified for a [bucket_name, scope uid,
    %% collection uid]. If either scope or collection uid are not specified, the
    %% permissions apply to all otherwise unspecified scopes and collections in
    %% the bucket (or all collections in the scope). These do not override the
    %% set of permissions specified at a deeper level.
    %%
    %% CollectionParams is of the form [bucket_name, scope_name,
    %% collection_name] where (bucket|scope|collection)_name may be "any".
    %%
    %% A wildcard "any" that is not trailing must be expanded to enumerate all
    %% buckets (scopes) to which it applies.
    %% e.g. [any, "_system", "_mobile"] must be expanded to:
    %% [["b1", "_system", "_mobile"], ["b2", "_system", "_mobile"]...]
    %% for every bucket in the system.
    %%
    %% Why?
    %% (1) The uids of a scope (like "_system") or a collection (like "_mobile")
    %%     are not constant across all buckets or collections.
    %% (2) MB-61241: memcached doesn't allow specifying a permission with a
    %%     wildcard * bucket_name and a specific scope or collection uid.
    %%
    %% Why do we need this now?
    %% For all parameterized roles (except mobile_sync_gateway), setting a * for
    %% bucket automatically sets * for scopes and collections and setting a
    %% wildcard * for scope sets * for collection. They exclusively use trailing
    %% wildcards, so we omit the scope and/or collection uids when set to *.
    %%
    %% mobile_sync_gateway introduces a bucket-parameterized permission:
    %% [{bucket, bucket_name}, "_system", "_mobile"] where a wildcard * can be
    %% specified by the user for bucket_name. [any, "_system", _mobile"] is the
    %% only case that has a non-trailing wildcard which needs to be expanded.
    Params = remove_trailing_any(CollectionParams),
    case lists:member(any, Params) of
        true ->
            Expanded = split_and_expand_collection_params(Params, Snapshot),
            lists:map(fun (NewParams) ->
                {{misc:align_list(NewParams,
                                  length(?RBAC_COLLECTION_PARAMS),
                                  any),
                    ObjRest}, Operations}
                end, Expanded);
        false -> [{{CollectionParams, ObjRest}, Operations}]
    end.

split_and_expand_collection_params(CollectionParams, Snapshot) ->
    split_and_expand_collection_params(bucket, CollectionParams, Snapshot).
split_and_expand_collection_params(bucket, CollectionParams, Snapshot) ->
    Buckets =
        case hd(CollectionParams) of
            any -> ns_bucket:get_bucket_names(Snapshot);
            Bucket -> [Bucket]
        end,
    Rest = tl(CollectionParams),
    [[B | R] || B <- Buckets,
                (Manifest = collections:get_manifest(B, Snapshot))
                    =/= undefined,
                R <- split_and_expand_collection_params(scope, Rest, Manifest)];

split_and_expand_collection_params(scope, RemainingParams, Manifest) ->
    Scopes =
        case hd(RemainingParams) of
            any -> [Scope || {Scope, _} <- collections:get_scopes(Manifest)];
            Scope -> [Scope]
        end,
    Rest = tl(RemainingParams),
    [[Scope | Rest] || Scope <- Scopes].

check_permissions(Snapshot, CompiledRoles) ->
    CollectionPrivileges =
        lists:foldl(
             fun (CompiledRole, Acc) ->
                     privileges_merge(
                       Acc,
                       check_permissions_for_role(Snapshot, CompiledRole))
             end,
        #{}, CompiledRoles),
    {global_permissions(CompiledRoles),
     CollectionPrivileges}.

check_permissions_for_role(Snapshot, CompiledRole) ->
    ExpandedPerms = lists:reverse(
        lists:flatmap(
            expand_collection_params(_, Snapshot), CompiledRole)),
    FinalPartialRoles =
        %% TODO: memcached allows specifying a wildcard * for bucket_name but we
        %% don't use it. collection_permissions() doesn't handle bucket "any".
        lists:flatmap(
                      fun({{[any, any, any], ObjRest}, Ops}) ->
                              [{{[B, any, any], ObjRest}, Ops} ||
                                  B <- ns_bucket:get_bucket_names(Snapshot)];
                         (Other) -> [Other]
                      end, ExpandedPerms),

    %% Use compile_params to get uids of scopes/collections (required by
    %% memcached).
    Collections =
        lists:filtermap(
          fun({{Params, ObjRest}, Ops}) ->
                  %% TODO: The collection need not be found (and isn't checked
                  %% for during compilation of roles nor is it accounted for in
                  %% fixup_collection_params). So compile_params can return
                  %% false. Should probably check (and cache) these while
                  %% compiling roles.
                  case menelaus_roles:compile_params(?RBAC_COLLECTION_PARAMS,
                                                     Params, Snapshot) of
                      false ->
                          false;
                      NewParams ->
                          %% Convert back to parameterised object, now with
                          %% uids for comprehension by memcached
                          {true, {[{collection, NewParams} | ObjRest], Ops}}
                  end
          end, FinalPartialRoles),

    BucketPrivileges =
        lists:foldl(bucket_permissions(_, [CompiledRole], _), #{},
                    ns_bucket:get_bucket_names(Snapshot)),
    CollectionPrivileges =
        privileges_merge(BucketPrivileges,
                         lists:foldl(fun collection_permissions/2, #{},
                                     Collections)),
    maps:filter(
        fun (_, #priv_object{privileges = Privileges, children = Children}) ->
            map_size(Privileges) > 0 orelse map_size(Children) > 0
        end, CollectionPrivileges).

privileges_merge(Priv1, Priv2) ->
    maps:merge_with(
        fun (_,
             #priv_object{privileges = Privileges1, children = Children1},
             #priv_object{privileges = Privileges2, children = Children2}) ->
            #priv_object{privileges = maps:merge(Privileges1, Privileges2),
                         children = privileges_merge(Children1, Children2)}
        end, Priv1, Priv2).

permissions_for_user(Roles, Snapshot, RoleDefinitions) ->
    CompiledRoles = menelaus_roles:compile_roles(Roles, RoleDefinitions,
                                                 Snapshot),
    check_permissions(Snapshot, CompiledRoles).

jsonify_user_with_cache(AuthnRes, Snapshot) ->
    jsonify_user(AuthnRes,
                 check_permissions(
                   Snapshot,
                   menelaus_roles:get_compiled_roles(AuthnRes))).

jsonify_key(String) when is_list(String) ->
    list_to_binary(String);
jsonify_key(Num) when is_number(Num) ->
    collections:convert_uid_to_memcached(Num).

jsonify_privs([SectionKey | Rest], Map) ->
    {SectionKey,
     {maps:fold(
        fun (Key, #priv_object{privileges = Privileges, children = Children},
             Acc) ->
                [{jsonify_key(Key),
                  {[{privileges, maps:keys(Privileges)}] ++
                       [jsonify_privs(Rest, Children) ||
                           map_size(Children) =/= 0]}} | Acc]
        end, [], Map)}}.

jsonify_user(#authn_res{identity = {UserName, Domain}},
             {GlobalPermissions, BucketPermissions}) ->
    Buckets =
        case BucketPermissions of
            all ->
                {buckets, {[{<<"*">>, [all]}]}};
            _ ->
                jsonify_privs([buckets, scopes, collections], BucketPermissions)
        end,
    Global = {privileges, GlobalPermissions},
    {list_to_binary(UserName), {[Buckets, Global, {domain, Domain}]}}.

memcached_admin_json("@ns_server" = User) ->
    Privileges =
        lists:usort(admin_user_privileges() ++ other_users_privileges(User)),
    jsonify_user(#authn_res{identity={User, local}}, {Privileges, all});
memcached_admin_json(User) ->
    Privileges = other_users_privileges(User),
    jsonify_user(#authn_res{identity={User, local}}, {Privileges, all}).

jsonify_users(Users, RoleDefinitions, ClusterAdmin, PromUser) ->
    Snapshot = ns_bucket:get_snapshot(all, [collections, uuid]),
    ?make_transducer(
       begin
           ?yield(object_start),
           lists:foreach(fun (U) ->
                                 ?yield({kv, memcached_admin_json(U)})
                         end, Users),

           EmitUser =
               fun (Identity, Roles) ->
                       %% We exclude locked users from memcached.rbac, so that
                       %% existing connections by the user are disconnected.
                       %% This gets updated whether the user is locked
                       %% with a PATCH or a PUT, since both of them update
                       %% the user_version.
                       %% We also delete the entry from isasl.pw, since this is
                       %% required to prevent authentication.
                       case menelaus_users:is_user_locked(Identity) of
                           false ->
                               Permissions =
                                   permissions_for_user(Roles, Snapshot,
                                                        RoleDefinitions),
                               ?yield({kv,
                                       jsonify_user(
                                         #authn_res{identity=Identity},
                                         Permissions)});
                           true ->
                               ok
                       end
               end,

           EmitLocalUser =
               fun (undefined, _) ->
                       ok;
                   (Name, Identity) ->
                       EmitUser({Name, local},
                                menelaus_roles:get_roles(Identity))
               end,

           EmitLocalUser(ClusterAdmin, {ClusterAdmin, admin}),
           EmitLocalUser(PromUser, {PromUser, stats_reader}),

           pipes:foreach(
             ?producer(),
             fun ({{user, {UserName, _} = Identity}, Props}) ->
                     case UserName of
                         ClusterAdmin ->
                             TagCA = ns_config_log:tag_user_name(ClusterAdmin),
                             ?log_warning("Encountered user ~p with the same
                                          name as cluster administrator",
                                          [TagCA]);
                         _ ->
                             EmitUser(Identity,
                                      proplists:get_value(roles, Props, []))
                     end
             end),
           ?yield(object_end)
       end).

producer(#state{users = Users,
                cluster_admin = ClusterAdmin,
                prometheus_user = PromUser}) ->
    case menelaus_users:upgrade_in_progress() of
        true ->
            ?log_debug("Skipping update during users upgrade"),
            undefined;
        false ->
            RoleDefinitions = menelaus_roles:get_definitions(all),
            pipes:compose([menelaus_users:select_users({'_', local}, [roles]),
                           jsonify_users(Users, RoleDefinitions, ClusterAdmin,
                                         PromUser),
                           sjson:encode_extended_json([{compact, false},
                                                       {strict, false}])])
    end.

-ifdef(TEST).
flatten_priv_object(Map) ->
    flatten_priv_object([], [], Map).

flatten_priv_object(Prefix, Acc, Map) ->
    maps:fold(
      fun (Key, #priv_object{
                   privileges = Privileges,
                   children = Children}, Acc1) when map_size(Privileges) =:= 0,
                                                    map_size(Children) =:= 0 ->
              [{lists:reverse([Key | Prefix]), empty} | Acc1];
          (Key, #priv_object{privileges = Privileges,
                             children = Children}, Acc1) ->
              NewPrefix = [Key | Prefix],
              NewPrefixReversed = lists:reverse(NewPrefix),
              flatten_priv_object(
                NewPrefix,
                Acc1 ++ [{NewPrefixReversed, K} || K <- maps:keys(Privileges)],
                Children)
      end, Acc, Map).


priv_is_granted(Privilege, Key, PrivTree) ->
    %% Check for whether Privilege is granted for Key in , assuming
    priv_is_granted(Privilege, Key, PrivTree, false).

%% Privilege is granted if the deepest matching object has that privilege
%% granted. If the object has undefined privilege, then it inherits the parent
%% privileges.
priv_is_granted(Privilege, [Object | Rest], PrivTree, GrantedByParent) ->
    case maps:find(Object, PrivTree) of
        {ok, #priv_object{privileges = Privileges, children = Children}} ->
            GrantedAtThisLevel = maps:is_key(Privilege, Privileges),
            case Rest of
                [] ->
                    %% If privileges are specified at this level, and the
                    %% object we are looking for is exactly at this level, then
                    %% the privilege is granted if it is granted at this level
                    GrantedAtThisLevel;
                _ ->
                    %% If privileges are specified at this level, but the object
                    %% we are looking for is more specific, then we check the
                    %% next level in case the object matches at a lower level
                    priv_is_granted(Privilege, Rest, Children,
                                    GrantedAtThisLevel)
            end;
        error ->
            %% If privileges aren't specified at the specific level, then the
            %% lowest level determines whether it is granted
            GrantedByParent
    end.
priv_object_test() ->
    PrivObject =
        functools:chain(
          #{},
          [priv_set(p3, [b1], _),
             priv_set(p1, [b1, s1, c1], _),
           priv_set(p2, [b1, s1, c1], _),
           priv_set(p3, [b1, s1, c1], _),
           priv_set(p2, [b1, s1, c2], _),
           priv_unset(p3, [b1, s1, c2], _),
           priv_set(p2, [b1, s2], _),
           priv_unset(p3, [b1, s2], _),
           priv_set(p2, [b1, s2, c3], _),
           priv_unset(p3, [b1, s2, c3], _),
           priv_set(p1, [b2], _)]),
    ?assert(priv_is_granted(p1, [b1, s1, c1], PrivObject)),
    ?assertNot(priv_is_granted(p1, [b1, s1], PrivObject)),
    ?assertNot(priv_is_granted(p1, [b1], PrivObject)),
    ?assert(priv_is_granted(p1, [b2, any, any], PrivObject)),
    ?assert(priv_is_granted(p1, [b2, any], PrivObject)),
    ?assert(priv_is_granted(p1, [b2], PrivObject)),
    ?assertNot(priv_is_granted(wrong, [b2], PrivObject)),
    ?assertNot(priv_is_granted(p1, [wrong], PrivObject)),
    ?assert(priv_is_granted(p3, [b1, s1], PrivObject)),
    ?assertListsEqual([{[b1, s1, c1], p1},
                       {[b2], p1},
                       {[b1, s1, c1], p2},
                       {[b1, s1, c2], p2},
                       {[b1, s2, c3], p2},
                       {[b1, s2], p2},
                       {[b1, s1, c1], p3},
                       {[b1], p3},
                       {[b1, s1], p3}], flatten_priv_object(PrivObject)).

fixup_collection_params_test() ->
    Manifest1 =
        [{uid,3},
         {scopes,[{"_default",
                   [{uid,0},{collections,[{"_default",[{uid,0}]}]}]},
                  {"def",
                   [{uid,8},{collections,[{"xyz",[{uid,10}]}]}]},
                  {"_system",
                   [{uid,9},
                    {collections,
                     [{"_query",[{uid,11}]},
                      {"_mobile",[{uid,12}]}]}]}]}],
    Manifest2 =
        [{uid,3},
         {scopes,[{"_default",
                   [{uid,0},{collections,[{"_default",[{uid,0}]}]}]},
                  {"ghi",
                   [{uid,7},{collections,[{"xyz",[{uid,10}]}]}]},
                  {"_system",
                   [{uid,8},
                    {collections,
                     [{"_query",[{uid,8}]},
                      {"_mobile",[{uid,9}]}]}]}]}],
    Snapshot =
        ns_bucket:toy_buckets(
          [{"test", [{uuid, <<"test_id">>}]},
           {"default", [{uuid, <<"default_id">>}, {collections, Manifest1}]},
           {"abc", [{uuid, <<"abc_id">>}, {collections, Manifest2}]}]),

    ?assertEqual([{{[any, any, any],
        []}, op}],
                 expand_collection_params({[{bucket, any}],
                                            op}, Snapshot)),
    ?assertEqual([{{["default", any, any],
        []}, op}],
                 expand_collection_params({[{bucket, "default"}],
                                           op}, Snapshot)),
    ?assertEqual([{{["default", "def", any],
        []}, op}],
                 expand_collection_params({[{scope, ["default", "def"]}],
                                           op}, Snapshot)),
    ?assertEqual([{{["default", "_system", "_mobile"],
        []}, op}],
                 expand_collection_params(
                     {[{collection, ["default", "_system", "_mobile"]}], op},
                     Snapshot)),

    Result1 = [{{["default", "_system", "_mobile"],
        []}, op},
               {{["abc", "_system", "_mobile"],
                   []}, op}],
    ?assertEqual(Result1,
                 expand_collection_params({[{collection,
                     [any, "_system", "_mobile"]}],
                                           op}, Snapshot)),

    Result2 = [{{["default", "_system", any],
        []}, op},
               {{["abc", "_system", any],
                   []}, op}],
    ?assertEqual(Result2,
                 expand_collection_params(
                     {[{collection, [any, "_system", any]}], op}, Snapshot)),

    Result3 = [{{["default", "_default", "_mobile"],
        []}, op},
               {{["default", "def", "_mobile"],
                   []}, op},
               {{["default", "_system", "_mobile"],
                   []}, op}],
    ?assertEqual(Result3,
                 expand_collection_params(
                     {[{collection, ["default", any, "_mobile"]}], op},
                     Snapshot)),

    %% Note the expansion populates all combinations, although _mobile doesn't
    %% exist in any scope other than "_system". Non-existent combinations are
    %% weeded out when compile_params returns false.
    Result4 = [{{["default", "_default", "_mobile"], []}, op},
               {{["default", "def", "_mobile"], []}, op},
               {{["default", "_system", "_mobile"], []}, op},
               {{["abc", "_default", "_mobile"], []}, op},
               {{["abc", "ghi", "_mobile"], []}, op},
               {{["abc", "_system", "_mobile"], []}, op}],
    ?assertEqual(expand_collection_params(
                                {[{collection, [any, any, "_mobile"]}], op},
        Snapshot),
                 Result4).

permissions_for_user_test_() ->
    Manifest =
        [{uid, 2},
         {scopes, [{"s",  [{uid, 1}, {collections, [{"_c",  [{uid, 1}]},
                                                    {"c1", [{uid, 2}]}]}]},
                   {"s1", [{uid, 2}, {collections, [{"_c",  [{uid, 3}]},
                                                    {"c1",  [{uid, 4}]}]}]},
                   {?SYSTEM_SCOPE_NAME, [{uid, 3},
                                         {collections,
                                          [{"_mobile", [{uid, 5}]},
                                           {"_query", [{uid, 6}]}]}]}]}],
    Snapshot =
        ns_bucket:toy_buckets(
          [{"test", [{uuid, <<"test_id">>}]},
           {"default", [{uuid, <<"default_id">>}, {collections, Manifest}]}]),

    All = fun (L) -> lists:usort([P || {_, P} <- L]) end,
    Read = fun (L) -> lists:usort([P || {{_, read}, P} <- L]) end,
    DataRead = fun (L) ->
                       lists:usort([P || {{[_, data | _], read}, P} <- L])
               end,
    SysRead = fun (L) ->
                      lists:usort([P || {{[_, data, docs ], sread}, P} <- L])
              end,
    SysWrite = fun (L) ->
                       lists:usort([P || {{[_, data, docs ], swrite}, P} <- L])
               end,
    BucketsPlusCollections = bucket_permissions_to_check(undefined) ++
        collection_permissions_to_check([x, x, "_x"]),
    JustCollections = collection_permissions_to_check([x, x, "_x"]),

    AllBucketPermissions = All(BucketsPlusCollections),

    Test =
        fun (Roles, ExpectedGlobal, ExpectedBuckets) ->
                {lists:flatten(io_lib:format("~p", [Roles])),
                 fun () ->
                         {GlobalRes, BucketsRes} =
                             permissions_for_user(
                               Roles, Snapshot,
                               menelaus_roles:get_definitions(all)),
                         ?assertListsEqual(ExpectedGlobal, GlobalRes),
                         FlatExpected =
                             lists:flatmap(
                               fun ({Key, List}) ->
                                       [{Key, El} || El <- List]
                               end, ExpectedBuckets),
                         ?assertListsEqual(FlatExpected,
                                           flatten_priv_object(BucketsRes))
                 end}
        end,
    {foreach,
     fun() ->
             meck:new(cluster_compat_mode, [passthrough]),
             meck:expect(cluster_compat_mode, is_enterprise,
                         fun () -> true end),
             meck:expect(cluster_compat_mode, get_compat_version,
                         fun () -> ?LATEST_VERSION_NUM end),
             meck:expect(cluster_compat_mode, is_developer_preview,
                         fun () -> false end),
             meck:new(ns_bucket, [passthrough]),
             meck:expect(ns_bucket, get_snapshot,
                         fun (_, _) -> Snapshot end),
             config_profile:load_default_profile_for_test(),
             fake_chronicle_kv:setup(),
             menelaus_roles:set_role_definitions(),
             SimpleRole0 = [{[{collection, ["default", "s", any]}], all}],
             ok = menelaus_roles:set_role({<<"simple_role">>,
                                           [], [{mutable, true}], SimpleRole0}),
             SimpleRole1 = [{[{collection, ["default", "s", "c1"]}], none},
                            {[{collection, ["default", "s", any]}], all}],
             ok = menelaus_roles:set_role({<<"simple_role_with_exclusion">>,
                                          [], [{mutable, true}], SimpleRole1}),
             ComplexRole0 = [{[{collection, ["default", "s", "_c"]}], none},
                            {[{collection, ["default", "s1", "_c"]}], all},
                            {[{collection, ["default", "s1", any]}], none},
                            {[{collection, ["default", any, any]}], all}],
             ok = menelaus_roles:set_role({<<"complex_role">>,
                                          [], [{mutable, true}], ComplexRole0}),
             ComplexRole1 =
                 [{[{collection, ["default", any, any]}, data], none},
                  {[{collection, ["default", "s1", any]}], all},
                  %% This line has no effect since the 'none' above overrides it
                  {[{bucket, "default"}, data], [delete]},
                  %% This line has minimal effect as only stats is uncovered by
                  %% the 'none' line
                  {[{bucket, "default"}], [read]}],
             ok = menelaus_roles:set_role(
                    {<<"complex_role_with_mixed_parameterisation">>,
                     [], [{mutable, true}], ComplexRole1})
     end,
     fun (_) ->
             fake_chronicle_kv:teardown(),
             meck:unload(),
             config_profile:unload_profile_for_test()
     end,
     [Test([<<"admin">>],
           All(global_permissions_to_check()),
           [{["default"], AllBucketPermissions},
            {["test"], AllBucketPermissions}]),
      Test([<<"ro_admin">>],
           ['Stats','SystemSettings'],
           [{["default"], ['SimpleStats']},
            {["test"], ['SimpleStats']}]),
      Test([{<<"bucket_admin">>, [{"test", <<"test_id">>}]}],
           ['Stats','SystemSettings'],
           [{["test"], ['SimpleStats']}]),
      Test([{<<"bucket_full_access">>, ["test"]}],
           ['SystemSettings'],
           [{["test"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)}]),
      Test([{<<"views_admin">>, [{"test", <<"test_id">>}]},
            {<<"views_reader">>, [{"default", <<"default_id">>}]}],
           ['Stats', 'SystemSettings'],
           [{["default"], ['Read', 'SystemCollectionLookup']},
            {["test"],
             Read(BucketsPlusCollections) ++ SysRead(BucketsPlusCollections)}]),
      Test([{<<"data_reader">>, [{"test", <<"test_id">>}, any, any]}],
           ['SystemSettings'],
           [{["test"], ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([{<<"data_reader">>, [{"default", <<"default_id">>}, {"s", 1}, any]}],
           ['SystemSettings'],
           [{["default", 1], ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([{<<"data_reader">>, [{"default", <<"default_id">>}, {"s", 1},
                                 {"_c", 1}]}],
           ['SystemSettings'],
           [{["default", 1, 1],
             ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([{<<"data_reader">>, [{"default", <<"default_id">>}, {"s", 1},
                                 {"c1", 2}]}],
           ['SystemSettings'],
           [{["default", 1, 2],
             ['Read', 'RangeScan']}]),
      Test([{<<"data_dcp_reader">>, [{"test", <<"test_id">>}, any, any]}],
           ['IdleConnection','SystemSettings'],
           [{["test"], DataRead(BucketsPlusCollections) ++
                 SysRead(BucketsPlusCollections)}]),
      Test([{<<"data_dcp_reader">>,
             [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]}],
           ['IdleConnection','SystemSettings'],
           [{["default"], ['DcpProducer', 'SystemCollectionLookup']},
            {["default", 1], ['SystemCollectionLookup']},
            {["default", 1, 1], ['SystemCollectionLookup' |
                                 DataRead(JustCollections)]}]),
      Test([{<<"data_dcp_reader">>,
             [{"default", <<"default_id">>}, {"s", 1}, {"c1", 2}]}],
           ['IdleConnection','SystemSettings'],
           [{["default"], ['DcpProducer', 'SystemCollectionLookup']},
            {["default", 1], ['SystemCollectionLookup']},
            {["default", 1, 2], ['SystemCollectionLookup' |
            DataRead(JustCollections)]}]),
      Test([{<<"data_monitoring">>,
             [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]}],
           ['SystemSettings'],
           [{["default", 1, 1], ['SimpleStats']}]),
      Test([{<<"data_monitoring">>,
             [{"default", <<"default_id">>}, {"s", 1}, {"c1", 2}]}],
           ['SystemSettings'],
           [{["default", 1, 2], ['SimpleStats']}]),
      Test([{<<"data_backup">>, [{"test", <<"test_id">>}]},
            {<<"data_monitoring">>, [{"default", <<"default_id">>}, any, any]}],
           ['SystemSettings'],
           [{["default"], ['SimpleStats']},
            {["test"], AllBucketPermissions}]),
      Test([{<<"mobile_sync_gateway">>, [{"test", <<"test_id">>}]}],
           ['IdleConnection','SystemSettings'],
           [{["test"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)}]),
      Test([{<<"mobile_sync_gateway">>, [{"default", <<"default_id">>}]}],
           ['IdleConnection','SystemSettings'],
           [{["default"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)},
            {["default", 3],
             All(JustCollections) -- SysWrite(JustCollections)},
            {["default", 3, 5], All(JustCollections)}]),
      Test([{<<"mobile_sync_gateway">>, [any]}],
           ['IdleConnection','SystemSettings'],
           [{["default"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)},
            {["default", 3],
             All(JustCollections) -- SysWrite(BucketsPlusCollections)},
            {["default", 3, 5], All(JustCollections)},
            {["test"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)}]),
      Test([{<<"data_writer">>, [{"test", <<"test_id">>}, any, any]}],
           ['SystemSettings'],
           [{["test"], ['Delete', 'Insert', 'Upsert']}]),
      Test([{<<"data_writer">>, [{"test", <<"test_id">>}, any, any]},
            {<<"data_writer">>, [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]},
            {<<"data_writer">>, [{"default", <<"default_id">>}, {"s", 1}, any]},
            {<<"data_reader">>, [{"default", <<"default_id">>}, {"s1", 2}, any]}],
           ['SystemSettings'],
           [{["test"], ['Delete', 'Insert', 'Upsert']},
            {["default", 1], ['Delete', 'Insert', 'Upsert']},
            {["default", 1, 1], ['Delete', 'Insert', 'Upsert']},
            {["default", 2],
             ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([<<"simple_role">>],
           [],
           [{["default", 1], All(JustCollections)}
           ]),
      Test([<<"simple_role_with_exclusion">>],
           [],
           [{["default", 1, 2], ['SystemCollectionLookup',
                                 'SystemCollectionMutation']},
            {["default", 1], All(JustCollections)}
           ]),
      Test([<<"complex_role">>],
           [],
           [{["default", 1, 1], [empty]},
            {["default", 2, 3], All(JustCollections)},
            {["default", 1], All(JustCollections)},
            {["default"], All(JustCollections)}]),
      Test([<<"complex_role_with_mixed_parameterisation">>],
           [],
           [{["default"], ['SimpleStats']},
            {["default", 2], All(JustCollections)}])]}.
-endif.
