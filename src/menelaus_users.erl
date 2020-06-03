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
%% @doc implementation of local and external users

-module(menelaus_users).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("rbac.hrl").
-include("pipes.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
%% User management:
         store_user/5,
         delete_user/1,
         select_users/1,
         select_users/2,
         select_auth_infos/1,
         user_exists/1,
         get_roles/1,
         get_user_name/1,
         get_users_version/0,
         get_auth_version/0,
         get_auth_info/1,
         get_user_props/1,
         get_user_props/2,
         change_password/2,

%% Group management:
         store_group/4,
         delete_group/1,
         select_groups/1,
         select_groups/2,
         get_group_roles/1,
         get_group_props/1,
         group_exists/1,
         get_groups_version/0,

%% UI Profiles
         get_profile/1,
         store_profile/2,
         delete_profile/1,
         select_profiles/0,

%% Actions:
         authenticate/2,
         build_scram_auth/1,
         build_plain_auth/1,
         build_plain_auth/2,
         empty_storage/0,
         cleanup_bucket_roles/1,
         get_passwordless/0,
         get_salt_and_mac/1,
         user_auth_info/2,

%% Backward compatibility:
         upgrade_to_55/2,
         config_upgrade_to_55/0,
         upgrade_status/0
        ]).

%% callbacks for replicated_dets
-export([init/1, on_save/2, on_empty/1, handle_call/4, handle_info/2]).

-export([start_storage/0, start_replicator/0, start_auth_cache/0]).

%% RPC'd from ns_couchdb node
-export([get_auth_info_on_ns_server/1]).

-define(MAX_USERS_ON_CE, 20).
-define(LDAP_GROUPS_CACHE_SIZE, 1000).
-define(DEFAULT_PROPS, [name, user_roles, group_roles, passwordless,
                        password_change_timestamp, groups, external_groups]).
-define(DEFAULT_GROUP_PROPS, [description, roles, ldap_group_ref]).

-record(state, {base, passwordless}).

replicator_name() ->
    users_replicator.

storage_name() ->
    users_storage.

versions_name() ->
    menelaus_users_versions.

auth_cache_name() ->
    menelaus_users_cache.

start_storage() ->
    Replicator = erlang:whereis(replicator_name()),
    Path = filename:join(path_config:component_path(data, "config"), "users.dets"),
    replicated_dets:start_link(?MODULE, [], storage_name(), Path, Replicator).

get_users_version() ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            [{user_version, V, Base}] = ets:lookup(versions_name(), user_version),
            {V, Base};
        true ->
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE, get_users_version, [])
    end.

get_groups_version() ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            [{group_version, V, Base}] = ets:lookup(versions_name(),
                                                    group_version),
            {V, Base};
        true ->
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE,
                     get_groups_version, [])
    end.

get_auth_version() ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            [{auth_version, V, Base}] = ets:lookup(versions_name(), auth_version),
            {V, Base};
        true ->
            rpc:call(ns_node_disco:ns_server_node(), ?MODULE, get_auth_version, [])
    end.

start_replicator() ->
    GetRemoteNodes =
        fun () ->
                ns_node_disco:nodes_actual_other()
        end,
    doc_replicator:start_link(replicator_name(), GetRemoteNodes,
                              storage_name()).

start_auth_cache() ->
    versioned_cache:start_link(
      auth_cache_name(), 200,
      fun (I) ->
              ?log_debug("Retrieve user ~p from ns_server node",
                         [ns_config_log:tag_user_data(I)]),
              rpc:call(ns_node_disco:ns_server_node(), ?MODULE, get_auth_info_on_ns_server, [I])
      end,
      fun () ->
              dist_manager:wait_for_node(fun ns_node_disco:ns_server_node/0),
              [{{user_storage_events, ns_node_disco:ns_server_node()}, fun (_) -> true end}]
      end,
      fun () ->
              {get_auth_version(), get_users_version(), get_groups_version()}
      end).

empty_storage() ->
    replicated_dets:empty(storage_name()).

get_passwordless() ->
    gen_server:call(storage_name(), get_passwordless, infinity).

init([]) ->
    _ = ets:new(versions_name(), [protected, named_table]),
    mru_cache:new(ldap_groups_cache, ?LDAP_GROUPS_CACHE_SIZE),
    #state{base = init_versions()}.

init_versions() ->
    Base = misc:rand_uniform(0, 16#100000000),
    ets:insert_new(versions_name(), [{user_version, 0, Base},
                                     {auth_version, 0, Base},
                                     {group_version, 0, Base}]),
    gen_event:notify(user_storage_events, {user_version, {0, Base}}),
    gen_event:notify(user_storage_events, {group_version, {0, Base}}),
    gen_event:notify(user_storage_events, {auth_version, {0, Base}}),
    Base.

on_save(Docs, State) ->
    ProcessDoc =
        fun ({group, _}, _Doc, S) ->
                {{change_version, group_version}, S};
            ({user, _}, _Doc, S) ->
                {{change_version, user_version}, S};
            ({auth, Identity}, Doc, S) ->
                {{change_version, auth_version},
                 maybe_update_passwordless(
                   Identity,
                   replicated_dets:get_value(Doc),
                   replicated_dets:is_deleted(Doc),
                   S)};
            (_, _, S) ->
                {undefined, S}
        end,

    {MessagesToSend, NewState} =
        lists:foldl(
          fun (Doc, {MessagesAcc, StateAcc}) ->
                  {Message, NewState} =
                      ProcessDoc(replicated_dets:get_id(Doc), Doc, StateAcc),
                  {sets:add_element(Message, MessagesAcc), NewState}
          end, {sets:new(), State}, Docs),
    case sets:is_element({change_version, group_version}, MessagesToSend) of
        true -> mru_cache:flush(ldap_groups_cache);
        false -> ok
    end,
    [self() ! Msg || Msg <- sets:to_list(MessagesToSend), Msg =/= undefined],
    NewState.

handle_info({change_version, Key} = Msg, #state{base = Base} = State) ->
    misc:flush(Msg),
    Ver = ets:update_counter(versions_name(), Key, 1),
    gen_event:notify(user_storage_events, {Key, {Ver, Base}}),
    {noreply, State}.

on_empty(_State) ->
    true = ets:delete_all_objects(versions_name()),
    #state{base = init_versions()}.

maybe_update_passwordless(_Identity, _Value, _Deleted, State = #state{passwordless = undefined}) ->
    State;
maybe_update_passwordless(Identity, _Value, true, State = #state{passwordless = Passwordless}) ->
    State#state{passwordless = lists:delete(Identity, Passwordless)};
maybe_update_passwordless(Identity, Auth, false, State = #state{passwordless = Passwordless}) ->
    NewPasswordless =
        case authenticate_with_info(Auth, "") of
            true ->
                case lists:member(Identity, Passwordless) of
                    true ->
                        Passwordless;
                    false ->
                        [Identity | Passwordless]
                end;
            false ->
                lists:delete(Identity, Passwordless)
        end,
    State#state{passwordless = NewPasswordless}.

handle_call(get_passwordless, _From, TableName, #state{passwordless = undefined} = State) ->
    Passwordless =
        pipes:run(
          replicated_dets:select(TableName, {auth, '_'}, 100, true),
          ?make_consumer(
             pipes:fold(?producer(),
                        fun ({{auth, Identity}, Auth}, Acc) ->
                                case authenticate_with_info(Auth, "") of
                                    true ->
                                        [Identity | Acc];
                                    false ->
                                        Acc
                                end
                        end, []))),
    {reply, Passwordless, State#state{passwordless = Passwordless}};
handle_call(get_passwordless, _From, _TableName, #state{passwordless = Passwordless} = State) ->
    {reply, Passwordless, State}.

select_users(KeySpec) ->
    select_users(KeySpec, ?DEFAULT_PROPS).

select_users(KeySpec, ItemList) ->
    pipes:compose([replicated_dets:select(storage_name(), {user, KeySpec}, 100),
                   make_props_transducer(ItemList)]).

make_props_transducer(ItemList) ->
    PropsState = make_props_state(ItemList),
    pipes:map(fun ({{user, Id}, Props}) ->
                      {{user, Id}, make_props(Id, Props, ItemList, PropsState)}
              end).

make_props(Id, Props, ItemList) ->
    make_props(Id, Props, ItemList, make_props_state(ItemList)).

make_props(Id, Props, ItemList, {Passwordless, Definitions,
                                 AllPossibleValues}) ->

    %% Groups calculation might be heavy, so we want to make sure they
    %% are calculated only once
    GetDirtyGroups = fun (#{dirty_groups := Groups} = Cache) ->
                             {Groups, Cache};
                         (Cache) ->
                             Groups = get_dirty_groups(Id, Props),
                             {Groups, Cache#{dirty_groups => Groups}}
                     end,

    GetGroups = fun (#{groups := Groups} = Cache) ->
                        {Groups, Cache};
                    (Cache) ->
                        {DirtyGroups, NewCache} = GetDirtyGroups(Cache),
                        Groups = clean_groups(DirtyGroups),
                        {Groups, NewCache#{groups => Groups}}
                end,

    EvalProp =
      fun (password_change_timestamp, Cache) ->
              {replicated_dets:get_last_modified(
                 storage_name(), {auth, Id}, undefined), Cache};
          (group_roles, Cache) ->
              {Groups, NewCache} = GetGroups(Cache),
              Roles = get_groups_roles(Groups, Definitions, AllPossibleValues),
              {Roles, NewCache};
          (user_roles, Cache) ->
              UserRoles = get_user_roles(Props, Definitions, AllPossibleValues),
              {UserRoles, Cache};
          (roles, Cache) ->
              {DirtyGroups, NewCache} = GetDirtyGroups(Cache),
              UserRoles = get_user_roles(Props, Definitions, AllPossibleValues),
              GroupsAndRoles = get_groups_roles(DirtyGroups, Definitions,
                                                AllPossibleValues),
              GroupRoles = lists:concat([R || {_, R} <- GroupsAndRoles]),
              {lists:usort(UserRoles ++ GroupRoles), NewCache};
          (passwordless, Cache) ->
              {lists:member(Id, Passwordless), Cache};
          (groups, Cache) ->
              {{Groups, _}, NewCache} = GetGroups(Cache),
              {Groups, NewCache};
          (external_groups, Cache) ->
              {{_, ExtGroups}, NewCache} = GetGroups(Cache),
              {ExtGroups, NewCache};
          (dirty_groups, Cache) ->
              {DirtyGroups, NewCache} = GetDirtyGroups(Cache),
              {DirtyGroups, NewCache};
          (Name, Cache) ->
              {proplists:get_value(Name, Props), Cache}
        end,

    {Res, _} = lists:mapfoldl(
                   fun (Key, Cache) ->
                           {Value, NewCache} = EvalProp(Key, Cache),
                           {{Key, Value}, NewCache}
                   end, #{}, ItemList),
    Res.

make_props_state(ItemList) ->
    Passwordless = lists:member(passwordless, ItemList) andalso
                       menelaus_users:get_passwordless(),
    {Definitions, AllPossibleValues} =
        case lists:member(roles, ItemList) orelse
             lists:member(user_roles, ItemList) orelse
             lists:member(group_roles, ItemList) of
            true -> {menelaus_roles:get_definitions(),
                     menelaus_roles:calculate_possible_param_values(
                       ns_bucket:get_buckets())};
            false -> {undefined, undefined}
        end,
    {Passwordless, Definitions, AllPossibleValues}.

select_auth_infos(KeySpec) ->
    replicated_dets:select(storage_name(), {auth, KeySpec}, 100).

build_auth(false, undefined) ->
    password_required;
build_auth(false, Password) ->
    build_scram_auth(Password);
build_auth({_, _}, undefined) ->
    same;
build_auth({_, CurrentAuth}, Password) ->
    {Salt, Mac} = get_salt_and_mac(CurrentAuth),
    case ns_config_auth:hash_password(Salt, Password) of
        Mac ->
            case has_scram_hashes(CurrentAuth) of
                false ->
                    build_scram_auth(Password);
                _ ->
                    same
            end;
        _ ->
            build_scram_auth(Password)
    end.

-spec store_user(rbac_identity(), rbac_user_name(), rbac_password(),
                 [rbac_role()], [rbac_group_id()]) -> run_txn_return().
store_user({_UserName, Domain} = Identity, Name, Password, Roles, Groups) ->
    Props = [{name, Name} || Name =/= undefined] ++
            [{groups, Groups} || Groups =/= undefined],
    Config = ns_config:get(),

    CurrentAuth = replicated_dets:get(storage_name(), {auth, Identity}),
    case check_limit(Identity) of
        true ->
            case Domain of
                external ->
                    store_user_with_auth(Identity, Props, same, Roles, Config);
                local ->
                    case build_auth(CurrentAuth, Password) of
                        password_required ->
                            {abort, password_required};
                        Auth ->
                            store_user_with_auth(Identity, Props, Auth, Roles, Config)
                    end
            end;
        false ->
            {abort, too_many}
    end.

count_users() ->
    pipes:run(menelaus_users:select_users('_', []),
              ?make_consumer(
                 pipes:fold(?producer(),
                            fun (_, Acc) ->
                                    Acc + 1
                            end, 0))).

check_limit(Identity) ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            true;
        false ->
            case count_users() >= ?MAX_USERS_ON_CE of
                true ->
                    user_exists(Identity);
                false ->
                    true
            end
    end.

store_user_with_auth(Identity, Props, Auth, Roles, Config) ->
    case menelaus_roles:validate_roles(Roles, Config) of
        {NewRoles, []} ->
            ok = store_user_validated(Identity, [{roles, NewRoles} | Props], Auth),
            {commit, ok};
        {_, BadRoles} ->
            {abort, {error, roles_validation, BadRoles}}
    end.

store_user_validated(Identity, Props, Auth) ->
    case replicated_dets:get(storage_name(), {user, Identity}) of
        false ->
            _ = delete_profile(Identity);
        _ ->
            ok
    end,
    ok = replicated_dets:set(storage_name(), {user, Identity}, Props),
    case store_auth(Identity, Auth) of
        ok ->
            ok;
        unchanged ->
            ok
    end.

store_auth(_Identity, same) ->
    unchanged;
store_auth(Identity, Auth) when is_list(Auth) ->
    ok = replicated_dets:set(storage_name(), {auth, Identity}, Auth).

change_password({_UserName, local} = Identity, Password) when is_list(Password) ->
    case replicated_dets:get(storage_name(), {user, Identity}) of
        false ->
            user_not_found;
        _ ->
            CurrentAuth = replicated_dets:get(storage_name(), {auth, Identity}),
            Auth = build_auth(CurrentAuth, Password),
            store_auth(Identity, Auth)
    end.

-spec delete_user(rbac_identity()) -> run_txn_return().
delete_user({_, Domain} = Identity) ->
    case Domain of
        local ->
            _ = replicated_dets:delete(storage_name(), {auth, Identity}),
            _ = delete_profile(Identity);
        external ->
            ok
    end,
    case replicated_dets:delete(storage_name(), {user, Identity}) of
        {not_found, _} ->
            {abort, {error, not_found}};
        ok ->
            {commit, ok}
    end.

get_salt_and_mac(Auth) ->
    SaltAndMacBase64 = binary_to_list(proplists:get_value(<<"plain">>, Auth)),
    <<Salt:16/binary, Mac:20/binary>> = base64:decode(SaltAndMacBase64),
    {Salt, Mac}.

has_scram_hashes(Auth) ->
    proplists:is_defined(<<"sha1">>, Auth).

-spec authenticate(rbac_user_id(), rbac_password()) -> boolean().
authenticate(Username, Password) ->
    Identity = {Username, local},
    case get_auth_info(Identity) of
        false ->
            false;
        Auth ->
            authenticate_with_info(Auth, Password)
    end.

get_auth_info(Identity) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            get_auth_info_on_ns_server(Identity);
        true ->
            versioned_cache:get(auth_cache_name(), Identity)
    end.

get_auth_info_on_ns_server(Identity) ->
    case replicated_dets:get(storage_name(), {user, Identity}) of
        false ->
            false;
        _ ->
            case replicated_dets:get(storage_name(), {auth, Identity}) of
                false ->
                    false;
                {_, Auth} ->
                    Auth
            end
    end.

-spec authenticate_with_info(list(), rbac_password()) -> boolean().
authenticate_with_info(Auth, Password) ->
    {Salt, Mac} = get_salt_and_mac(Auth),
    misc:compare_secure(ns_config_auth:hash_password(Salt, Password), Mac).

get_user_props(Identity) ->
    get_user_props(Identity, ?DEFAULT_PROPS).

get_user_props(Identity, ItemList) ->
    make_props(Identity, get_user_props_raw(Identity), ItemList).

get_user_props_raw(Identity) ->
    replicated_dets:get(storage_name(), {user, Identity}, []).

-spec user_exists(rbac_identity()) -> boolean().
user_exists(Identity) ->
    false =/= replicated_dets:get(storage_name(), {user, Identity}).

-spec get_roles(rbac_identity()) -> [rbac_role()].
get_roles(Identity) ->
    proplists:get_value(roles, get_user_props(Identity, [roles]), []).

%% Groups functions

store_group(Identity, Description, Roles, LDAPGroup) ->
    case menelaus_roles:validate_roles(Roles, ns_config:get()) of
        {NewRoles, []} ->
            Props = [{description, Description} || Description =/= undefined] ++
                    [{ldap_group_ref, LDAPGroup} || LDAPGroup =/= undefined] ++
                    [{roles, NewRoles}],
            ok = replicated_dets:set(storage_name(), {group, Identity}, Props),
            ok;
        {_, BadRoles} ->
            {error, {roles_validation, BadRoles}}
    end.

delete_group(GroupId) ->
    UpdateFun =
        fun ({user, Key}, Props) ->
                Groups = proplists:get_value(groups, Props, []),
                case lists:member(GroupId, Groups) of
                    true ->
                        NewProps = misc:key_update(groups, Props,
                                                   lists:delete(GroupId, _)),
                        ?log_debug("Updating user ~p groups: ~p -> ~p",
                                   [Key, Props, NewProps]),
                        {update, NewProps};
                    false ->
                        skip
                end
        end,

    case replicated_dets:select_with_update(storage_name(), {user, '_'},
                                            100, UpdateFun) of
        [] -> ok;
        Error -> ?log_warning("Failed to remove users from group: ~p", [Error])
    end,
    case replicated_dets:delete(storage_name(), {group, GroupId}) of
        ok -> ok;
        {not_found, _} -> {error, not_found}
    end.

select_groups(KeySpec) ->
    select_groups(KeySpec, ?DEFAULT_GROUP_PROPS).

select_groups(KeySpec, Items) ->
    pipes:compose(
        [replicated_dets:select(storage_name(), {group, KeySpec}, 100),
         make_group_props_transducer(Items)]).

make_group_props_transducer(Items) ->
    PropsState = make_props_state(Items),
    pipes:map(fun ({Id, Props}) ->
                      {Id, make_group_props(Props, Items, PropsState)}
              end).

get_group_props(GroupId) ->
    get_group_props(GroupId, ?DEFAULT_GROUP_PROPS).

get_group_props(GroupId, Items) ->
    Props = replicated_dets:get(storage_name(), {group, GroupId}, []),
    make_group_props(Props, Items).

get_group_props(GroupId, Items, Definitions, AllPossibleValues) ->
    Props = replicated_dets:get(storage_name(), {group, GroupId}, []),
    make_group_props(Props, Items, {[], Definitions, AllPossibleValues}).

group_exists(GroupId) ->
    false =/= replicated_dets:get(storage_name(), {group, GroupId}).

get_group_roles(GroupId) ->
    proplists:get_value(roles, get_group_props(GroupId, [roles]), []).

get_group_roles(GroupId, Definitions, AllPossibleValues) ->
    Props = get_group_props(GroupId, [roles], Definitions, AllPossibleValues),
    proplists:get_value(roles, Props, []).

make_group_props(Props, Items) ->
    make_group_props(Props, Items, make_props_state(Items)).

make_group_props(Props, Items, {_, Definitions, AllPossibleValues}) ->
    lists:map(
      fun (roles = Name) ->
              Roles = proplists:get_value(roles, Props, []),
              Roles2 = menelaus_roles:filter_out_invalid_roles(
                         Roles, Definitions, AllPossibleValues),
              {Name, Roles2};
          (Name) ->
              {Name, proplists:get_value(Name, Props)}
      end, Items).

get_user_roles(UserProps, Definitions, AllPossibleValues) ->
    menelaus_roles:filter_out_invalid_roles(
      proplists:get_value(roles, UserProps, []),
      Definitions, AllPossibleValues).

clean_groups({DirtyLocalGroups, DirtyExtGroups}) ->
    {lists:filter(group_exists(_), DirtyLocalGroups),
     lists:filter(group_exists(_), DirtyExtGroups)}.

get_dirty_groups(Id, Props) ->
    LocalGroups = proplists:get_value(groups, Props, []),
    ExternalGroups =
        case Id of
            {_, local} -> [];
            {User, external} ->
                case ldap_util:get_setting(authorization_enabled) of
                    true -> get_ldap_groups(User);
                    false -> []
                end
        end,
    {LocalGroups, ExternalGroups}.

get_groups_roles({LocalGroups, ExtGroups}, Definitions, AllPossibleValues) ->
    [{G, get_group_roles(G, Definitions, AllPossibleValues)}
        || G <- LocalGroups ++ ExtGroups].

get_ldap_groups(User) ->
    try ldap_auth_cache:user_groups(User) of
        LDAPGroups ->
            GroupsMap =
                lists:foldl(
                  fun (LDAPGroup, Acc) ->
                          Groups = get_groups_by_ldap_group(LDAPGroup),
                          lists:foldl(?cut(_2#{_1 => true}), Acc, Groups)
                  end, #{}, LDAPGroups),
            maps:keys(GroupsMap)
    catch
        error:Error ->
            ?log_error("Failed to get ldap groups for ~p: ~p",
                       [ns_config_log:tag_user_name(User), Error]),
            []
    end.

get_groups_by_ldap_group(LDAPGroup) ->
    case mru_cache:lookup(ldap_groups_cache, LDAPGroup) of
        {ok, Value} -> Value;
        false ->
            GroupFilter =
                fun ({_, Props}) ->
                        LDAPGroup == proplists:get_value(ldap_group_ref, Props)
                end,
            Groups = pipes:run(select_groups('_', [ldap_group_ref]),
                               [pipes:filter(GroupFilter),
                                pipes:map(fun ({{group, G}, _}) -> G end)],
                               pipes:collect()),
            mru_cache:add(ldap_groups_cache, LDAPGroup, Groups),
            Groups
    end.

%% ui profiles

profile_key(Identity) ->
    {ui_profile, Identity}.

get_profile(Identity) ->
    replicated_dets:get(storage_name(), profile_key(Identity), undefined).

store_profile(Identity, Json) ->
    ok = replicated_dets:set(storage_name(), profile_key(Identity), Json).

delete_profile(Identity) ->
    case replicated_dets:delete(storage_name(), profile_key(Identity)) of
        ok -> ok;
        {not_found, _} -> {error, not_found}
    end.

select_profiles() ->
    replicated_dets:select(storage_name(), profile_key('_'), 100).

-spec get_user_name(rbac_identity()) -> rbac_user_name().
get_user_name({_, Domain} = Identity) when Domain =:= local orelse Domain =:= external ->
    proplists:get_value(name, get_user_props(Identity, [name]));
get_user_name(_) ->
    undefined.

user_auth_info(User, Auth) ->
    {[{<<"n">>, list_to_binary(User)} | Auth]}.

build_scram_auth(Password) ->
    BuildAuth =
        fun (Type) ->
                {S, H, I} = scram_sha:hash_password(Type, Password),
                {scram_sha:auth_info_key(Type),
                    {[{<<"h">>, base64:encode(H)},
                      {<<"s">>, base64:encode(S)},
                      {<<"i">>, I}]}}
        end,
    build_plain_auth(Password) ++
        [BuildAuth(Sha) || Sha <- scram_sha:supported_types()].

build_plain_auth(Password) ->
    {Salt, Mac} = ns_config_auth:hash_password(Password),
    build_plain_auth(Salt, Mac).

build_plain_auth(Salt, Mac) ->
    SaltAndMac = <<Salt/binary, Mac/binary>>,
    [{<<"plain">>, base64:encode(SaltAndMac)}].

rbac_55_upgrade_key() ->
    {rbac_upgrade, ?VERSION_55}.

config_upgrade_to_55() ->
    [{delete, rbac_55_upgrade_key()}].

upgrade_status() ->
    UserUpgrade = ns_config:read_key_fast(users_upgrade, undefined),
    RolesUpgrade = ns_config:read_key_fast(rbac_55_upgrade_key(), undefined),
    case {UserUpgrade, RolesUpgrade} of
        {undefined, undefined} -> no_upgrade;
        _ -> upgrade_in_progress
    end.

filter_out_invalid_roles(Props, Definitions, AllPossibleValues) ->
    Roles = proplists:get_value(roles, Props, []),
    FilteredRoles = menelaus_roles:filter_out_invalid_roles(Roles, Definitions, AllPossibleValues),
    lists:keystore(roles, 1, Props, {roles, FilteredRoles}).

cleanup_bucket_roles(BucketName) ->
    ?log_debug("Delete all roles for bucket ~p", [BucketName]),
    Buckets = lists:keydelete(BucketName, 1, ns_bucket:get_buckets()),
    Definitions = menelaus_roles:get_definitions(),
    AllPossibleValues = menelaus_roles:calculate_possible_param_values(Buckets),

    UpdateFun =
        fun ({Type, Key}, Props) when Type == user; Type == group ->
                case filter_out_invalid_roles(Props, Definitions,
                                              AllPossibleValues) of
                    Props ->
                        skip;
                    NewProps ->
                        ?log_debug("Changing properties of ~p ~p from ~p "
                                   "to ~p due to deletion of ~p",
                                   [Type, Key, Props, NewProps, BucketName]),
                        {update, NewProps}
                end
        end,

    UpdateRecords = replicated_dets:select_with_update(storage_name(), _, 100,
                                                       UpdateFun),

    case {UpdateRecords({user, '_'}), UpdateRecords({group, '_'})} of
        {[], []} -> ok;
        {UserErrors, GroupErrors} ->
            ?log_warning("Failed to cleanup some roles: ~p ~p",
                         [UserErrors, GroupErrors]),
            ok
    end.

upgrade_to_55(Config, Nodes) ->
    try
        case ns_config:search(Config, rbac_55_upgrade_key()) of
            false ->
                ns_config:set(rbac_55_upgrade_key(), started);
            {value, started} ->
                ?log_debug("Found unfinished roles upgrade. Continue.")
        end,
        do_upgrade_to_55(Nodes),
        ok
    catch T:E ->
              ale:error(?USER_LOGGER, "Unsuccessful user storage upgrade.~n~p",
                        [{T, E, erlang:get_stacktrace()}]),
              error
    end.

upgrade_roles_to_55(OldRoles) ->
    lists:flatmap(fun maybe_upgrade_role_to_55/1, OldRoles).

maybe_upgrade_role_to_55(cluster_admin) ->
    [cluster_admin, {bucket_full_access, [any]}];
maybe_upgrade_role_to_55({bucket_admin, Buckets}) ->
    [{bucket_admin, Buckets}, {bucket_full_access, Buckets}];
maybe_upgrade_role_to_55(Role) ->
    [Role].

upgrade_user_roles_to_55(Props) ->
    OldRoles = proplists:get_value(user_roles, Props),
    %% Convert roles and remove duplicates.
    NewRoles = lists:usort(upgrade_roles_to_55(OldRoles)),
    [{roles, NewRoles} | lists:keydelete(roles, 1, Props)].

user_roles_require_upgrade({{user, _Identity}, Props}) ->
    Roles = proplists:get_value(user_roles, Props),
    lists:any(?cut(maybe_upgrade_role_to_55(_1) =/= [_1]), Roles).

fetch_users_for_55_upgrade() ->
    pipes:run(menelaus_users:select_users({'_', '_'}, [user_roles]),
              [pipes:filter(fun user_roles_require_upgrade/1),
               pipes:map(fun ({{user, I}, _}) -> I end)],
              pipes:collect()).

do_upgrade_to_55(Nodes) ->
    %% propagate rbac_55_upgrade_key to nodes
    ok = ns_config_rep:ensure_config_seen_by_nodes(Nodes),

    replicated_storage:sync_to_me(
      storage_name(), Nodes, ?get_timeout(rbac_55_upgrade_key(), 60000)),

    UpdateUsers = fetch_users_for_55_upgrade(),
    lists:foreach(fun (Identity) ->
                          OldProps = get_user_props_raw(Identity),
                          NewProps = upgrade_user_roles_to_55(OldProps),
                          store_user_validated(Identity, NewProps, same)
                  end, UpdateUsers).
