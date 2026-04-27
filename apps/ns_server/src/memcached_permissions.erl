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
-include("ns_test.hrl").
-endif.

-include("ns_common.hrl").
-include("pipes.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("rbac.hrl").

-export([start_link/0, sync/0, sync_reload/0, jsonify_user_with_cache/2,
         spec_users/0, get_key_ids_in_use/0]).

%% callbacks
-export([init/0, filter_event/1, handle_event/2, producer/1, refresh/0]).

-ifdef(TEST).
-import(menelaus_permissions,
        [flatten_priv_object/1]).
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

global_permissions(CompiledRoles) ->
    lists:usort(
      [MemcachedPermission ||
          {Permission, MemcachedPermission} <- global_permissions_to_check(),
          menelaus_roles:is_allowed(Permission, CompiledRoles)]).

get_priv_object_key([Bucket | Rest]) ->
    [case Bucket of
         {N, _} ->
             N;
         _ ->
             Bucket
     end | [Id || {_, Id} <- Rest]].

check_permissions(Snapshot, CompiledRoles) ->
    {global_permissions(CompiledRoles),
     menelaus_permissions:check_collection_permissions(
         Snapshot, CompiledRoles,
         fun bucket_permissions_to_check/1,
         fun collection_permissions_to_check/1, fun get_priv_object_key/1)}.

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
        fun (Key, PrivObject, Acc) ->
            Privileges = menelaus_permissions:privileges(PrivObject),
            Children = menelaus_permissions:children(PrivObject),
            [{jsonify_key(Key),
              {[{privileges, sets:to_list(Privileges)}] ++
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
             CustomRole0 = [{[{collection, ["default", "s", any]}], all}],
             ok = menelaus_roles:set_role({<<"custom_role_0">>,
                                           [], [{mutable, true}], CustomRole0}),
             CustomRole1 = [{[{collection, ["default", "s", "c1"]}], none},
                            {[{collection, ["default", "s", any]}], all}],
             ok = menelaus_roles:set_role({<<"custom_role_1">>,
                                           [], [{mutable, true}], CustomRole1}),
             CustomRole2 = [{[{collection, ["default", "s", "_c"]}], none},
                            {[{collection, ["default", "s1", "_c"]}], all},
                            {[{collection, ["default", "s1", any]}], none},
                            {[{collection, ["default", any, any]}], all}],
             ok = menelaus_roles:set_role({<<"custom_role_2">>,
                                           [], [{mutable, true}], CustomRole2}),
             CustomRole3 =
                 [{[{collection, ["default", any, any]}, data], none},
                  {[{collection, ["default", "s1", any]}], all},
                  %% This line has no effect since the 'none' above overrides it
                  {[{bucket, "default"}, data], [delete]},
                  %% This line has minimal effect as only stats is uncovered by
                  %% the 'none' line
                  {[{bucket, "default"}], [read]}],
             ok = menelaus_roles:set_role(
                    {<<"custom_role_3">>,
                     [], [{mutable, true}], CustomRole3}),
             CustomRole4 =
                 [{[{collection, ["default", "s1", "c1"]}], none},
                  {[{collection, ["default", "s1", any]}], all}],
             ok = menelaus_roles:set_role(
                    {<<"custom_role_4">>,
                     [], [{mutable, true}], CustomRole4}),
             CustomRole5 =
                 [{[{collection, ["default", "s", "_c"]}, data], none},
                  {[{collection, ["default", "s", "_c"]}, stats], all},
                  {[{collection, ["default", "s1", "_c"]}, data], all},
                  {[{collection, ["default", "s1", "_c"]}, stats], none},
                  {[{collection, ["default", "s", any]}, stats], none},
                  {[{collection, ["default", "s1", any]}, data], none},
                  {[{collection, ["default", any, any]}], all}],
             ok = menelaus_roles:set_role({<<"custom_role_5">>,
                                           [], [{mutable, true}],
                                           CustomRole5})
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
      Test([{<<"data_writer">>,
             [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]},
            {<<"data_writer">>, [{"default", <<"default_id">>}, any, any]}],
           ['SystemSettings'],
           [{["default"], ['Delete', 'Insert', 'Upsert']}]),
      Test([{<<"data_writer">>, [{"test", <<"test_id">>}, any, any]},
            {<<"data_writer">>, [{"default", <<"default_id">>}, {"s", 1}, {"_c", 1}]},
            {<<"data_writer">>, [{"default", <<"default_id">>}, {"s", 1}, any]},
            {<<"data_reader">>, [{"default", <<"default_id">>}, {"s1", 2}, any]}],
           ['SystemSettings'],
           [{["test"], ['Delete', 'Insert', 'Upsert']},
            {["default", 1], ['Delete', 'Insert', 'Upsert']},
            {["default", 2],
             ['Read', 'RangeScan', 'SystemCollectionLookup']}]),
      Test([<<"custom_role_0">>],
           [],
           [{["default", 1], All(JustCollections)}
           ]),
      Test([<<"custom_role_1">>],
           [],
           [{["default", 1, 2],
             ['SystemCollectionLookup', 'SystemCollectionMutation']},
            {["default", 1], All(JustCollections)}
           ]),
      Test([<<"custom_role_2">>],
           [],
           [{["default", 1, 1], [empty]},
            {["default", 2, 3], All(JustCollections)},
            {["default", 1], All(JustCollections)},
            {["default"], All(JustCollections)}]),
      Test([<<"custom_role_3">>],
           [],
           [{["default"], ['SimpleStats']},
            {["default", 2], All(JustCollections)}]),
      Test([<<"custom_role_4">>],
           [],
           [{["default", 2, 4],
             ['SystemCollectionLookup', 'SystemCollectionMutation']},
            {["default", 2], All(JustCollections)}]),
      Test([<<"custom_role_5">>],
           [],
           [{["default", 1, 1], ['SimpleStats']},
            {["default", 1], All(JustCollections) -- ['SimpleStats']},
            {["default", 2, 3], All(JustCollections) -- ['SimpleStats']},
            {["default", 2], ['SimpleStats']},
            {["default"], All(JustCollections)}])]}.
-endif.
