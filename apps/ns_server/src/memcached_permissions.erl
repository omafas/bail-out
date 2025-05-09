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

priv_is_granted(_Privilege, [], _Map) ->
    false;
priv_is_granted(Privilege, [Object | Rest], Map) ->
    case maps:find(Object, Map) of
        {ok, #priv_object{privileges = Privileges, children = Children}} ->
            case maps:is_key(Privilege, Privileges) of
                true ->
                    true;
                false ->
                    priv_is_granted(Privilege, Rest, Children)
            end;
        error ->
            false
    end.

%% Note that privileges can be set at the bucket, scope or collection level.
%% If the same privilege is applied to a parent level, priv_set erases the
%% privilege from its children (as they inherit the parent's privileges).
%% For example, specifying:
%% {[{collection, [bucket1, S, C]}, data, docs], [sread]} and
%% {[{bucket, bucket1}, data, docs], [sread]}
%% only sets SystemCollectionLookup at the bucket level.
priv_set(Privilege, [Object], Map) ->
    maps:update_with(
      Object,
      fun (#priv_object{privileges = Privileges, children = Children}) ->
              #priv_object{privileges = Privileges#{Privilege => true},
                           children = priv_erase(Privilege, Children)}
      end,
      #priv_object{privileges = #{Privilege => true}, children = #{}},
      Map);
priv_set(Privilege, [Object | Rest], Map) ->
    Map#{Object =>
             case maps:find(Object, Map) of
                 {ok, #priv_object{privileges = Privileges,
                                   children = Children} = Old} ->
                     case maps:is_key(Privilege, Privileges) of
                         true ->
                             Old;
                         false ->
                             Old#priv_object{
                               children = priv_set(Privilege, Rest, Children)}
                     end;
                 error ->
                     #priv_object{privileges = #{},
                                  children = priv_set(Privilege, Rest, #{})}
             end}.

priv_erase(Privilege, Map) ->
    maps:fold(
      fun (Key, #priv_object{privileges = Privileges, children = Children},
           Acc) ->
              case #priv_object{privileges = maps:remove(Privilege, Privileges),
                                children = priv_erase(Privilege, Children)} of
                  #priv_object{privileges = P, children = C} when
                        map_size(P) =:= 0,
                        map_size(C) =:= 0 ->
                      Acc;
                  NotEmpty ->
                      Acc#{Key => NotEmpty}
              end
      end, #{}, Map).

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
collection_permissions(Params, CompiledRoles, Acc) ->
    ToCheck = lists:map(
                fun (any) ->
                        all;
                    (X) ->
                        X
                end, menelaus_roles:strip_ids(?RBAC_COLLECTION_PARAMS, Params)),
    ToStore = get_priv_object_key(Params),
    lists:foldl(
      fun ({Permission, MemcachedPrivilege}, Acc1) ->
              case priv_is_granted(MemcachedPrivilege, ToStore, Acc1) of
                  true ->
                      Acc1;
                  false ->
                      case menelaus_roles:is_allowed(Permission,
                                                     CompiledRoles) of
                          true ->
                              priv_set(MemcachedPrivilege, ToStore, Acc1);
                          false ->
                              Acc1
                      end
              end
      end, Acc, collection_permissions_to_check(ToCheck)).

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

fixup_collection_params(CollectionParams, Snapshot) ->
    %% memcached permissions are specified for a [bucket_name, scope uid,
    %% collection uid]. If either scope or collection uid are not specified, the
    %% permissions apply to all scopes and collections in the bucket (or all
    %% collections in the scope).
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
            lists:map(misc:align_list(_, length(?RBAC_COLLECTION_PARAMS), any),
                      Expanded);
        false -> [CollectionParams]
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
    Params = menelaus_roles:get_params_from_permissions(CompiledRoles),
    ExpandedParams = lists:flatmap(fixup_collection_params(_, Snapshot),
                                   Params),
    FinalParams =
        %% TODO: memcached allows specifying a wildcard * for bucket_name but we
        %% don't use it. collection_permissions() doesn't handle bucket "any".
        lists:usort(lists:flatmap(
                      fun([any, any, any]) ->
                              [[B, any, any] ||
                                  B <- ns_bucket:get_bucket_names(Snapshot)];
                         (X) -> [X]
                      end, ExpandedParams)),

    %% Use compile_params to get uids of scopes/collections (required by
    %% memcached).
    CollectionParams =
        lists:filtermap(
          fun(P) ->
                  %% TODO: The collection need not be found (and isn't checked
                  %% for during compilation of roles nor is it accounted for in
                  %% fixup_collection_params). So compile_params can return
                  %% false. Should probably check (and cache) these while
                  %% compiling roles.
                  case menelaus_roles:compile_params(?RBAC_COLLECTION_PARAMS, P,
                                                     Snapshot) of
                      false -> false;
                      NewParams -> {true, NewParams}
                  end
          end, FinalParams),

    BucketPrivileges =
        lists:foldl(bucket_permissions(_, CompiledRoles, _), #{},
                    ns_bucket:get_bucket_names(Snapshot)),
    {global_permissions(CompiledRoles),
     lists:foldl(collection_permissions(_, CompiledRoles, _), BucketPrivileges,
                 CollectionParams)}.

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
                  {[{privileges, maps:keys(Privileges)} ||
                       map_size(Privileges) =/= 0] ++
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

priv_object_test() ->
    PrivObject =
        functools:chain(
          #{},
          [priv_set(p1, [b1, s1, c1], _),
           priv_set(p1, [b2], _),
           priv_set(p2, [b1, s1, c1], _),
           priv_set(p2, [b1, s1, c2], _),
           priv_set(p2, [b1, s2, c3], _),
           priv_set(p2, [b1, s2], _),
           priv_set(p3, [b1, s1, c1], _),
           priv_set(p3, [b1], _)]),
    ?assert(priv_is_granted(p1, [b1, s1, c1], PrivObject)),
    ?assertNot(priv_is_granted(p1, [b1, s1], PrivObject)),
    ?assertNot(priv_is_granted(p1, [b1], PrivObject)),
    ?assert(priv_is_granted(p1, [b2, any, any], PrivObject)),
    ?assert(priv_is_granted(p1, [b2, any], PrivObject)),
    ?assert(priv_is_granted(p1, [b2], PrivObject)),
    ?assertNot(priv_is_granted(wrong, [b2], PrivObject)),
    ?assertNot(priv_is_granted(p1, [wrong], PrivObject)),
    ?assertListsEqual([{[b1, s1, c1], p1},
                       {[b2], p1},
                       {[b1, s1, c1], p2},
                       {[b1, s1, c2], p2},
                       {[b1, s2], p2},
                       {[b1], p3}], flatten_priv_object(PrivObject)).

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

    ?assertEqual(fixup_collection_params([any, any, any], Snapshot),
                 [[any, any, any]]),
    ?assertEqual(fixup_collection_params(["default", any, any], Snapshot),
                 [["default", any, any]]),
    ?assertEqual(fixup_collection_params(["default", "def", any], Snapshot),
                 [["default", "def", any]]),
    ?assertEqual(fixup_collection_params(["default", "_system", "_mobile"],
                                        Snapshot),
                 [["default", "_system", "_mobile"]]),

    Result1 = [["default", "_system", "_mobile"],
               ["abc", "_system", "_mobile"]],
    ?assertEqual(fixup_collection_params([any, "_system", "_mobile"], Snapshot),
                 Result1),

    Result2 = [["default", "_system", any],
               ["abc", "_system", any]],
    ?assertEqual(fixup_collection_params([any, "_system", any], Snapshot),
                 Result2),

    Result3 = [["default", "_default", "_mobile"],
               ["default", "def", "_mobile"],
               ["default", "_system", "_mobile"]],
    ?assertEqual(fixup_collection_params(["default", any, "_mobile"], Snapshot),
                 Result3),

    %% Note the expansion populates all combinations, although _mobile doesn't
    %% exist in any scope other than "_system". Non-existent combinations are
    %% weeded out when compile_params returns false.
    Result4 = [["default", "_default", "_mobile"],
               ["default", "def", "_mobile"],
               ["default", "_system", "_mobile"],
               ["abc", "_default", "_mobile"],
               ["abc", "ghi", "_mobile"],
               ["abc", "_system", "_mobile"]],
    ?assertEqual(fixup_collection_params([any, any, "_mobile"], Snapshot),
                 Result4).

permissions_for_user_test_() ->
    Manifest =
        [{uid, 2},
         {scopes, [{"s",  [{uid, 1}, {collections, [{"_c",  [{uid, 1}]},
                                                    {"c1", [{uid, 2}]}]}]},
                   {"s1", [{uid, 2}, {collections, [{"_c",  [{uid, 3}]}]}]},
                   {?SYSTEM_SCOPE_NAME, [{uid, 3},
                                         {collections,
                                          [{"_mobile", [{uid, 4}]},
                                           {"_query", [{uid, 5}]}]}]}]}],
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
             config_profile:mock_default_profile()
     end,
     fun (_) ->
             meck:unload()
     end,
     [Test([admin],
           All(global_permissions_to_check()),
           [{["default"], AllBucketPermissions},
            {["test"], AllBucketPermissions}]),
      Test([ro_admin],
           ['Stats','SystemSettings'],
           [{["default"], ['SimpleStats']},
            {["test"], ['SimpleStats']}]),
      Test([{bucket_admin, [{"test", <<"test_id">>}]}],
           ['Stats','SystemSettings'],
           [{["test"], ['SimpleStats']}]),
      Test([{bucket_full_access, ["test"]}],
           ['SystemSettings'],
           [{["test"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)}]),
      Test([{views_admin, [{"test", <<"test_id">>}]},
            {views_reader, [{"default", <<"default_id">>}]}],
           ['Stats', 'SystemSettings'],
           [{["default"], ['Read', 'SystemCollectionLookup']},
            {["test"],
             Read(BucketsPlusCollections) ++ SysRead(BucketsPlusCollections)}]),
      Test([{data_reader, [{"test", <<"test_id">>}, any, any]}],
           ['SystemSettings'],
           [{["test"], ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([{data_reader, [{"default", <<"default_id">>}, {"s", 1}, any]}],
           ['SystemSettings'],
           [{["default", 1], ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([{data_reader, [{"default", <<"default_id">>}, {"s", 1},
                           {"_c", 1}]}],
           ['SystemSettings'],
           [{["default", 1, 1],
             ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([{data_reader, [{"default", <<"default_id">>}, {"s", 1},
                           {"c1", 2}]}],
           ['SystemSettings'],
           [{["default", 1, 2],
             ['Read', 'RangeScan']}]),
      Test([{data_dcp_reader, [{"test", <<"test_id">>}, any, any]}],
           ['IdleConnection','SystemSettings'],
           [{["test"], DataRead(BucketsPlusCollections) ++
                 SysRead(BucketsPlusCollections)}]),
      Test([{data_dcp_reader,
             [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]}],
           ['IdleConnection','SystemSettings'],
           [{["default"], ['DcpProducer', 'SystemCollectionLookup']},
            {["default", 1, 1], DataRead(JustCollections)}]),
      Test([{data_dcp_reader,
             [{"default", <<"default_id">>}, {"s", 1}, {"c1", 2}]}],
           ['IdleConnection','SystemSettings'],
           [{["default"], ['DcpProducer', 'SystemCollectionLookup']},
            {["default", 1, 2], DataRead(JustCollections)}]),
      Test([{data_monitoring,
             [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]}],
           ['SystemSettings'],
           [{["default", 1, 1], ['SimpleStats']}]),
      Test([{data_monitoring,
             [{"default", <<"default_id">>}, {"s", 1}, {"c1", 2}]}],
           ['SystemSettings'],
           [{["default", 1, 2], ['SimpleStats']}]),
      Test([{data_backup, [{"test", <<"test_id">>}]},
            {data_monitoring, [{"default", <<"default_id">>}, any, any]}],
           ['SystemSettings'],
           [{["default"], ['SimpleStats']},
            {["test"], AllBucketPermissions}]),
      Test([{mobile_sync_gateway, [{"test", <<"test_id">>}]}],
           ['IdleConnection','SystemSettings'],
           [{["test"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)}]),
      Test([{mobile_sync_gateway, [{"default", <<"default_id">>}]}],
           ['IdleConnection','SystemSettings'],
           [{["default"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)},
           {["default", 3, 4], ['SystemCollectionMutation']}]),
      Test([{mobile_sync_gateway, [any]}],
           ['IdleConnection','SystemSettings'],
           [{["default"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)},
           {["default", 3, 4], ['SystemCollectionMutation']},
            {["test"],
             AllBucketPermissions -- SysWrite(BucketsPlusCollections)}]),
      Test([{data_writer, [{"test", <<"test_id">>}, any, any]}],
           ['SystemSettings'],
           [{["test"], ['Delete', 'Insert', 'Upsert']}]),
      Test([{data_writer, [{"test", <<"test_id">>}, any, any]},
            {data_writer, [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]},
            {data_writer, [{"default", <<"default_id">>}, {"s", 1}, any]},
            {data_reader, [{"default", <<"default_id">>}, {"s1", 2}, any]}],
           ['SystemSettings'],
           [{["test"], ['Delete', 'Insert', 'Upsert']},
            {["default", 1], ['Delete', 'Insert', 'Upsert']},
            {["default", 2],
             ['Read', 'RangeScan', 'SystemCollectionLookup']}])]}.
-endif.
