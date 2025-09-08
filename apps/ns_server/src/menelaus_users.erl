%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc implementation of local and external users

-module(menelaus_users).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("rbac.hrl").
-include("pipes.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         %% User management:
         store_user/7,
         store_users/2,
         delete_user/1,
         select_users/1,
         select_users/2,
         select_auth_infos/1,
         user_exists/1,
         get_roles/1,
         maybe_substitute_user_roles/1,
         maybe_substitute_roles/1,
         get_user_name/1,
         get_users_version/0,
         get_auth_version/0,
         get_auth_info/1,
         get_user_props/1,
         get_user_props/2,
         get_user_uuid/1,
         change_password/2,
         is_user_locked/1,
         store_lock/2,

         %% Group management:
         store_group/4,
         delete_group/1,
         select_groups/1,
         select_groups/2,
         get_group_roles/1,
         has_group_ldap_ref/1,
         is_empty_ldap_group_ref/1,
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
         authenticate_with_info/2,
         build_internal_auth/1,
         build_regular_auth/2,
         maybe_update_auth/4,
         migrate_local_user_auth/2,
         format_plain_auth/1,
         delete_storage_offline/0,
         cleanup_bucket_roles/1,
         get_salt_and_mac/1,

         %% Backward compatibility:
         upgrade/3,
         config_upgrade/0,
         upgrade_in_progress/0,
         upgrade_props/4,

         %% Misc:
         allow_hash_migration_during_auth_default/0,
         store_activity/1,
         delete_all_activity/0,
         clear_activity_for_untracked_users/1
        ]).

%% callbacks for replicated_dets
-export([init/1, on_save/2, handle_call/4, handle_info/2]).

-export([start_storage/0, start_replicator/0, start_auth_cache/0,
         start_lock_cache/0, apply_keys_and_resave/0, get_key_ids_in_use/0]).

%% RPC'd from ns_couchdb node
-export([get_auth_info_on_ns_server/1,
         is_user_locked_on_ns_server/1]).

-define(MAX_USERS_ON_CE, 20).
-define(DEFAULT_PROPS, [name, uuid, user_roles, group_roles, passwordless,
                        password_change_timestamp, groups, external_groups,
                        locked, temporary_password, last_activity_time]).
-define(DEFAULT_GROUP_PROPS, [description, roles, ldap_group_ref]).

-record(state, {base, user_lists, cache_size = ?LDAP_GROUPS_CACHE_SIZE}).

replicator_name() ->
    users_replicator.

storage_name() ->
    users_storage.

versions_name() ->
    menelaus_users_versions.

auth_cache_name() ->
    menelaus_users_cache.

lock_cache_name() ->
    menelaus_users_lock_cache.

path() ->
    filename:join(path_config:component_path(data, "config"), "users.dets").

start_storage() ->
    Replicator = erlang:whereis(replicator_name()),
    replicated_dets:start_link(?MODULE, [], storage_name(), path(), Replicator).

get_users_version() ->
    ?call_on_ns_server_node(
       begin
           [{user_version, V, Base}] = ets:lookup(versions_name(),
                                                  user_version),
           {V, Base}
       end, []).

get_groups_version() ->
    ?call_on_ns_server_node(
       begin
           [{group_version, V, Base}] = ets:lookup(versions_name(),
                                                   group_version),
           {V, Base}
       end, []).

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

start_lock_cache() ->
    versioned_cache:start_link(
      lock_cache_name(), 200,
      fun (I) ->
              ?log_debug("Retrieve lock for user ~p from ns_server node",
                         [ns_config_log:tag_user_data(I)]),
              rpc:call(ns_node_disco:ns_server_node(), ?MODULE,
                       is_user_locked_on_ns_server, [I])
      end,
      fun () ->
              dist_manager:wait_for_node(fun ns_node_disco:ns_server_node/0),
              [{{user_storage_events, ns_node_disco:ns_server_node()},
                fun (_) -> true end}]
      end,
      fun get_auth_version/0).

delete_storage_offline() ->
    Path = path(),
    case file:delete(Path) of
        ok ->
            ?log_info("User storage ~p was deleted", [Path]);
        {error, enoent} ->
            ?log_info("User storage ~p does not exist. Nothing to delete",
                      [Path])
    end.

get_user_lists() ->
    gen_server:call(storage_name(), get_user_lists, infinity).

init([]) ->
    _ = ets:new(versions_name(), [protected, named_table]),

    %% This will handle restarting cache if we change the size.
    Self = self(),
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({ldap_settings, _}) ->
              case cluster_compat_mode:is_cluster_76() of
                  true -> Self ! maybe_reinit_cache;
                  false -> ok
              end;
          (_) -> ok
      end),

    CacheSize =
        case cluster_compat_mode:is_cluster_76() of
            true -> ldap_util:get_setting(max_group_cache_size);
            false -> ?LDAP_GROUPS_CACHE_SIZE
        end,
    mru_cache:new(ldap_groups_cache, CacheSize),
    #state{base = init_versions(), cache_size = CacheSize}.

init_versions() ->
    Base = misc:rand_uniform(0, 16#100000000),
    Versions =
        [{V, 0, Base} ||
            V <- [user_version, group_version, auth_version]],
    ets:insert_new(versions_name(), Versions),
    [gen_event:notify(user_storage_events, {V, {0, Base}}) ||
        {V, _, _} <- Versions],
    Base.

on_save(Docs, State) ->
    ProcessDoc =
        fun ({group, _}, _Doc, S) ->
                {[{change_version, group_version}], S};
            ({limits, _}, _Doc, S) ->
                {[{change_version, limits_version}], S};
            ({user, _}, _Doc, S) ->
                {[{change_version, user_version}], S};
            ({auth, Identity}, Doc, S) ->
                {[{change_version, auth_version}],
                 maybe_update_user_lists(
                   Identity,
                   replicated_dets:get_value(Doc),
                   replicated_dets:is_deleted(Doc),
                   S)};
            ({locked, _}, _Doc, S) ->
                %% Both auth_version and user_version need updating when a user
                %% gets locked/unlocked.
                %% The auth_version needs to be updated, so that services
                %% correctly fail connection attempts with an authentication
                %% error, not an authorisation error.
                %% The user_version also needs to be updated, so that existing
                %% memcached connections are not permitted to continue being
                %% used, as memcached does not re-check authentication on each
                %% operation, only authorisation.
                {[{change_version, auth_version},
                  {change_version, user_version}], S};
            (_, _, S) ->
                {[], S}
        end,

    {MessagesToSend, NewState} =
        lists:foldl(
          fun (Doc, {MessagesAcc, StateAcc}) ->
                  {Messages, NewState} =
                      ProcessDoc(replicated_dets:get_id(Doc), Doc, StateAcc),
                  {lists:foldl(fun (Message, Acc) ->
                                       sets:add_element(Message, Acc)
                               end, MessagesAcc, Messages),
                   NewState}
          end, {sets:new(), State}, Docs),
    case sets:is_element({change_version, group_version}, MessagesToSend) of
        true -> mru_cache:flush(ldap_groups_cache);
        false -> ok
    end,
    [self() ! Msg || Msg <- sets:to_list(MessagesToSend)],
    NewState.

handle_info(maybe_reinit_cache, #state{cache_size = CurrentSize} = State) ->
    %% TODO: this check for undefined can be removed when 7.6 is no longer
    %% supported.
    NewSize = case ldap_util:get_setting(max_group_cache_size) of
                  undefined -> ?LDAP_GROUPS_CACHE_SIZE;
                  Value -> Value
              end,
    case NewSize =/= CurrentSize of
        true ->
            ?log_warning("LDAP groups cache size updated from ~p to ~p. "
                         "Reinitializing...", [CurrentSize, NewSize]),
            mru_cache:dispose(ldap_groups_cache),
            mru_cache:new(ldap_groups_cache, NewSize),
            {noreply, State#state{cache_size = NewSize}};
        false ->
            {noreply, State}
    end;
handle_info({change_version, Key} = Msg, #state{base = Base} = State) ->
    misc:flush(Msg),
    Ver = ets:update_counter(versions_name(), Key, 1),
    gen_event:notify(user_storage_events, {Key, {Ver, Base}}),
    {noreply, State}.

maybe_update_user_lists(_Identity, _Value, _Deleted,
                        State = #state{user_lists = undefined}) ->
    State;
maybe_update_user_lists(Identity, _Value, _Deleted = true,
                          State = #state{user_lists = {Passwordless,
                                                       TemporaryPassword}}) ->
    State#state{user_lists = {lists:delete(Identity, Passwordless),
                              lists:delete(Identity, TemporaryPassword)}};
maybe_update_user_lists(Identity, Auth, false,
                        State = #state{user_lists = {Passwordless,
                                                     TemporaryPassword}}) ->
    IsPasswordless = authenticate_with_info(Auth, ""),
    NewPasswordless = update_list(Identity, IsPasswordless, Passwordless),
    IsTemporaryPassword = is_temporary_password(Auth),
    NewTemporaryPassword = update_list(Identity, IsTemporaryPassword,
                                       TemporaryPassword),
    State#state{user_lists = {NewPasswordless, NewTemporaryPassword}}.

update_list(Value, _Include = true, List) ->
    case lists:member(Value, List) of
        true ->
            List;
        false ->
            [Value | List]
    end;
update_list(Value, _Include = false, List) ->
    lists:delete(Value, List).

handle_call(get_user_lists, _From, TableName,
            #state{user_lists = undefined} = State) ->
    {Passwordless, TemporaryPassword} =
        pipes:run(
          replicated_dets:select(TableName, {auth, '_'}, 100),
          ?make_consumer(
             pipes:fold(?producer(),
                        fun ({{auth, Identity}, Auth},
                             {AccPasswordless, AccTemporaryPassword}) ->
                                NewPasswordless =
                                    case authenticate_with_info(Auth, "") of
                                        true ->
                                            [Identity | AccPasswordless];
                                        false ->
                                            AccPasswordless
                                    end,
                                NewTemporaryPassword =
                                    case is_temporary_password(Auth) of
                                        true ->
                                            [Identity | AccTemporaryPassword];
                                        false ->
                                            AccTemporaryPassword
                                    end,
                                {NewPasswordless, NewTemporaryPassword}
                        end, {[], []}))),
    {reply, {Passwordless, TemporaryPassword},
     State#state{user_lists = {Passwordless, TemporaryPassword}}};
handle_call(get_user_lists, _From, _TableName,
            #state{user_lists = UserLists} = State) ->
    {reply, UserLists, State}.

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

make_props(Id, Props, ItemList, {Passwordless, TemporaryPassword, Definitions,
                                 Snapshot}) ->

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
                Roles = get_groups_roles(Groups, Definitions, Snapshot),
                {Roles, NewCache};
            (user_roles, Cache) ->
                UserRoles = get_user_roles(Props, Definitions, Snapshot),
                {UserRoles, Cache};
            (roles, Cache) ->
                {DirtyGroups, NewCache} = GetDirtyGroups(Cache),
                UserRoles = get_user_roles(Props, Definitions, Snapshot),
                GroupsAndRoles = get_groups_roles(DirtyGroups, Definitions,
                                                  Snapshot),
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
            (locked, Cache) ->
                {is_user_locked(Id), Cache};
            (temporary_password, Cache) ->
                {lists:member(Id, TemporaryPassword), Cache};
            (last_activity_time, Cache) ->
                {get_last_activity_time(Id), Cache};
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
    {Passwordless, TemporaryPassword} =
        case lists:member(passwordless, ItemList) orelse
            lists:member(temporary_password, ItemList) of
            false -> {[], []};
            true -> get_user_lists()
        end,
    {Definitions, Snapshot} =
        case lists:member(roles, ItemList) orelse
            lists:member(user_roles, ItemList) orelse
            lists:member(group_roles, ItemList) of
            true -> {menelaus_roles:get_definitions(public),
                     ns_bucket:get_snapshot(all, [collections, uuid])};
            false -> {undefined, undefined}
        end,
    {Passwordless, TemporaryPassword, Definitions, Snapshot}.

select_auth_infos(KeySpec) ->
    replicated_dets:select(storage_name(), {auth, KeySpec}, 100).

%% Build a new auth entry with the new password, unless the password is the same
rebuild_auth(false, Password) ->
    build_regular_auth([Password], false);
rebuild_auth({_, CurrentAuth}, Password) ->
    case is_password_reused(CurrentAuth, Password) of
        true ->
            %% Password is the same, so no need to rebuild
            same;
        false ->
            %% This is a new password, so we must build a new auth entry.
            %% The password should not be temporary, because the password is new
            build_regular_auth([Password], false)
    end.

is_password_reused(Auth, Password) ->
    authenticate_with_info(Auth, Password).

rebuild_auth(false, undefined, _TemporaryPassword) ->
    password_required;
rebuild_auth(false, Password, TemporaryPassword) ->
    build_regular_auth([Password], TemporaryPassword);
rebuild_auth({_, CurrentAuth}, undefined, TemporaryPassword) ->
    case {is_temporary_password(CurrentAuth), TemporaryPassword} of
        {true, true} -> same;
        {false, false} -> same;
        {false, true} ->
            expire_password(CurrentAuth);
        {true, false} ->
            remove_password_expiry(CurrentAuth)
    end;
rebuild_auth({_, _CurrentAuth}, Password, TemporaryPassword) ->
    build_regular_auth([Password], TemporaryPassword).

-spec store_user(rbac_identity(), rbac_user_name(),
                 {password, rbac_password()} | {auth, rbac_auth()},
                 [rbac_role()], [rbac_group_id()], boolean(), boolean()) ->
          ok | {error, {roles_validation, _}} |
          {error, password_required} | {error, too_many}.
store_user(Identity, Name, PasswordOrAuth, Roles, Groups, Locked,
           TemporaryPassword) ->
    Props = [{name, Name} || Name =/= undefined] ++
        [{groups, Groups} || Groups =/= undefined] ++
        [{pass_or_auth, PasswordOrAuth},
         {roles, Roles},
         {locked, Locked}] ++
        [{temporary_password, TemporaryPassword}
         || TemporaryPassword =/= undefined],
    case store_users([{Identity, Props}], true) of
        {ok, _UpdatedUsers} -> ok;
        {error, _} = Error -> Error
    end.

store_users(Users, CanOverwrite) ->
    Snapshot = ns_bucket:get_snapshot(all, [collections, uuid]),
    case prepare_store_users_docs(Snapshot, Users, CanOverwrite) of
        {ok, {UpdatedUsers, PreparedDocs}} ->
            FinalDocs = maybe_set_high_priority_user_changes(PreparedDocs),
            ok = replicated_dets:change_multiple(storage_name(), FinalDocs),
            {ok, UpdatedUsers};
        {error, _} = Error ->
            Error
    end.

%% Sets high priority for auth and user documents in the context of user-
%% related changes (store_user(s), delete_user).
%% For auth docs, high priority is required for user-initiated changes (password
%% changes, user creates or deletes) to take precedence over concurrent hash
%% migration updates (which use normal priority).
%% user docs were inadvertently marked as high priority in earlier releases. To
%% ensure backward compatibility, user documents must now be explicitly set to
%% high priority on sets and deletes (to be prioritized equally).
%% All other document operations default to normal priority.
maybe_set_high_priority_user_changes(Docs) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            lists:map(
              fun ({set, Key, Val}) when element(1, Key) =:= user;
                                         element(1, Key) =:= auth ->
                      {set, Key, Val, [{priority,
                                        ?REPLICATED_DETS_HIGH_PRIORITY}]};
                  ({delete, Key}) when element(1, Key) =:= user;
                                       element(1, Key) =:= auth ->
                      {delete, Key, [{priority,
                                      ?REPLICATED_DETS_HIGH_PRIORITY}]};
                  (Other) ->
                      Other
              end, Docs);
        false ->
            Docs
    end.

prepare_store_users_docs(Snapshot, Users, CanOverwrite) ->
    try
        Res =
            lists:mapfoldr(
              fun (U, Updates) ->
                  case prepare_store_user(Snapshot, CanOverwrite, U) of
                      %% Currently only skipped if user exists and we're
                      %% not overwriting
                      skipped ->
                          {{skipped, U}, Updates};
                      {added, NewUpdates} ->
                          {{added, U}, NewUpdates ++ Updates};
                      {updated, NewUpdates} ->
                          {{updated, U}, NewUpdates ++ Updates}
                  end
              end, [], Users),
        {ok, Res}
    catch
        throw:{error, _} = Error -> Error
    end.

prepare_store_user(Snapshot, CanOverwrite, {{_, Domain} = Identity, Props}) ->
    Exists = case replicated_dets:get(storage_name(), {user, Identity}) of
                 false -> false;
                 _ -> true
             end,
    case Exists andalso (not CanOverwrite) of
        true -> skipped;
        false ->
            UUID = get_user_uuid(Identity, misc:uuid_v4()),
            Name = proplists:get_value(name, Props),
            Groups = proplists:get_value(groups, Props),
            PasswordOrAuth = proplists:get_value(pass_or_auth, Props),
            TemporaryPassword = proplists:get_bool(temporary_password, Props),
            Roles = proplists:get_value(roles, Props),
            Locked = proplists:get_value(locked, Props, false),

            UserProps = [{name, Name} || Name =/= undefined] ++
                [{uuid, UUID} || UUID =/= undefined] ++
                [{groups, Groups} || Groups =/= undefined],

            UserProps2 =
                case menelaus_roles:validate_roles(Roles, public, Snapshot) of
                    {NewRoles, []} -> [{roles, NewRoles} | UserProps];
                    {_, BadRoles} ->
                        throw({error, {roles_validation, BadRoles}})
                end,

            case check_limit(Identity) of
                true -> ok;
                false -> throw({error, too_many})
            end,

            Auth =
                case {Domain, PasswordOrAuth} of
                    {external, _} -> same;
                    {local, {password, Password}} ->
                        CurrentAuth = replicated_dets:get(storage_name(),
                                                          {auth, Identity}),
                        case rebuild_auth(CurrentAuth, Password,
                                          TemporaryPassword) of
                            password_required ->
                                throw({error, password_required});
                            A -> A
                        end;
                    {local, {auth, A}} -> A
                end,
            Res = case Exists of
                      true -> updated;
                      false -> added
                  end,
            {Res,
             store_user_changes(Identity, UserProps2, Auth, Locked, Exists)}
    end.

count_users() ->
    pipes:run(select_users('_', []),
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

store_user_changes(Identity, Props, Auth, Locked, Exists) ->
    case Exists of
        false ->
            [{delete, {limits, Identity}},
             {delete, profile_key(Identity)}];
        true ->
            []
    end ++
    [{set, {user, Identity}, Props}] ++
    [{set, {auth, Identity}, Auth} || Auth /= same] ++
    %% Only store a locked entry for locked users
    [{set, {locked, Identity}, Locked} || Locked =:= true] ++
    [{delete, {locked, Identity}} || Locked =:= false].

-spec get_last_activity_time(rbac_identity()) -> undefined | non_neg_integer().
get_last_activity_time(Id) ->
    case replicated_dets:get(storage_name(), {activity, Id}) of
        {{activity, Id}, Time} -> Time;
        false -> undefined
    end.

-spec store_activity(#{rbac_identity() => non_neg_integer()}) -> ok.
store_activity(ActivityMap) ->
    PreparedDocs = lists:filtermap(
                     fun ({Identity, NewTimestamp}) ->
                             case get_last_activity_time(Identity) of
                                 OldTimestamp
                                   when OldTimestamp =:= undefined;
                                        OldTimestamp < NewTimestamp ->
                                     {true, {set, {activity, Identity},
                                             NewTimestamp}};
                                 _ ->
                                     false
                             end
                     end, maps:to_list(ActivityMap)),
    case PreparedDocs of
        [] -> ok;
        _ -> replicated_dets:change_multiple(storage_name(), PreparedDocs)
    end.

delete_all_activity() ->
    UpdateFun =
        fun ({activity, User}, _Props) ->
                ?log_debug("Deleting last activity time for user ~p",
                           [ns_config_log:tag_user_data(User)]),
                delete
        end,
    case replicated_dets:select_with_update(storage_name(), {activity, '_'},
                                            100, UpdateFun) of
        [] -> ok;
        Error -> ?log_warning("Failed to delete activity: ~p", [Error])
    end.

clear_activity_for_untracked_users(IsTracked) ->
    UpdateFun =
        fun ({activity, User}, _Props) ->
                case IsTracked(User) of
                    false ->
                        ?log_debug("Deleting last activity time for untracked user"
                                   "~p", [ns_config_log:tag_user_data(User)]),
                        delete;
                    true ->
                        skip
                end
        end,
    case replicated_dets:select_with_update(storage_name(), {activity, '_'},
                                            100, UpdateFun) of
        [] -> ok;
        Error -> ?log_warning("Failed to delete activity: ~p", [Error])
    end.

store_auth(_Identity, same, _Priority) ->
    unchanged;
store_auth(Identity, Auth, Priority) when is_list(Auth) ->
    case cluster_compat_mode:is_cluster_76() of
        true ->
            ok = replicated_dets:set(
                   storage_name(), {auth, Identity}, Auth,
                   [{priority, Priority}]);
        false ->
            ok = replicated_dets:set(
                   storage_name(), {auth, Identity}, Auth)
    end.

-spec change_password(rbac_identity(), rbac_password()) ->
    user_not_found | unchanged | ok.
change_password({_UserName, local} = Identity, Password)
  when is_list(Password) ->
    case replicated_dets:get(storage_name(), {user, Identity}) of
        false ->
            user_not_found;
        _ ->
            CurrentAuth = replicated_dets:get(storage_name(), {auth, Identity}),
            Auth = rebuild_auth(CurrentAuth, Password),
            case Auth of
                same ->
                    unchanged;
                _ ->
                    store_auth(Identity, Auth, ?REPLICATED_DETS_HIGH_PRIORITY)
            end
    end.

is_user_locked({_, external} = _Identity) ->
    %% External users can't be locked
    false;
is_user_locked(Identity) ->
    case ns_node_disco:couchdb_node() == node() of
        false ->
            is_user_locked_on_ns_server(Identity);
        true ->
            versioned_cache:get(lock_cache_name(), Identity)
    end.

is_user_locked_on_ns_server(Identity) ->
    case replicated_dets:get(storage_name(), {locked, Identity}) of
           {{locked, Identity}, true} -> true;
           false -> false
    end.

store_lock(Identity, Locked) ->
    case is_user_locked(Identity) of
        Locked ->
            %% No change
            ok;
        _ ->
            case Locked of
                true ->
                    ok = replicated_dets:set(storage_name(),
                                             {locked, Identity}, true);
                false ->
                    ok = replicated_dets:delete(storage_name(),
                                                {locked, Identity})
            end
    end.

-spec delete_user(rbac_identity()) ->
          {commit, ok} |
          {abort, {error, not_found}}.
delete_user({_, Domain} = Identity) ->
    case user_exists(Identity) of
        true ->
            Docs0 =
                case Domain of
                    local ->
                        [{delete, {auth, Identity}},
                         {delete, profile_key(Identity)},
                         {delete, {locked, Identity}},
                         {delete, {activity, Identity}}];
                    external ->
                        []
                end,
            Docs1 = [{delete, {user, Identity}} | Docs0],

            Docs2 = maybe_set_high_priority_user_changes(Docs1),
            ok = replicated_dets:change_multiple(storage_name(), Docs2),
            {commit, ok};
        false ->
            {abort, {error, not_found}}
    end.

get_salt_and_mac(Auth) ->
    case proplists:get_value(<<"hash">>, Auth) of
        undefined -> obsolete_get_salt_and_mac(Auth);
        {Props} -> Props
    end.

obsolete_get_salt_and_mac(Auth) ->
    SaltAndMacBase64 = binary_to_list(proplists:get_value(<<"plain">>, Auth)),
    <<Salt:16/binary, Mac:20/binary>> = base64:decode(SaltAndMacBase64),
    [{?HASH_ALG_KEY, ?SHA1_HASH},
     {?SALT_KEY, base64:encode(Salt)},
     {?HASHES_KEY, [base64:encode(Mac)]}].

-spec authenticate(rbac_user_id(), rbac_password()) ->
    {ok | expired, rbac_identity()} | {error, auth_failure}.
authenticate(Username, Password) ->
    Identity = {Username, local},
    case get_auth_info(Identity) of
        false ->
            {error, auth_failure};
        Auth ->
            Res = authenticate_with_info(Auth, Password),
            case Res of
                true ->
                    maybe_migrate_password_hashes(Auth, Identity,
                                                  Password),
                    case proplists:get_value(<<"expiry">>, Auth) of
                        0 -> {expired, Identity};
                        _ -> {ok, Identity}
                    end;
                false ->
                    {error, auth_failure}
            end
    end.

%% Note: this functions assumes CurrentAuth is in 7.6 format
maybe_update_plain_auth_hashes(CurrentAuth, Password, Type) ->
    {HashInfo} = proplists:get_value(<<"hash">>, CurrentAuth),
    HashAlg = proplists:get_value(?HASH_ALG_KEY, HashInfo),
    CurrentHashAlg = ns_config:read_key_fast(
                       password_hash_alg, ?DEFAULT_PWHASH),
    Migrate =
        case HashAlg of
            CurrentHashAlg ->
                Settings = ns_config_auth:configurable_hash_alg_settings(
                             HashAlg, Type),
                lists:any(
                  fun ({K, V}) ->
                          proplists:get_value(K, HashInfo) =/= V
                  end, Settings);
            _ ->
                true
        end,
    TemporaryPassword = is_temporary_password(CurrentAuth),

    case Migrate of
        false ->
            CurrentAuth;
        true ->
            misc:update_proplist(
              CurrentAuth, build_plain_auth([Password], Type, TemporaryPassword))
    end.

is_temporary_password(false) ->
    false;
is_temporary_password(Auth) ->
    %% expiry=0 is used for temporary password
    proplists:get_value(<<"expiry">>, Auth) == 0.

allow_hash_migration_during_auth_default() ->
    cluster_compat_mode:is_cluster_76() andalso
        config_profile:get_bool(allow_hash_migration_during_auth).

%% Returns updated auth info iff any of the hash parameters has changed
update_auth(CurrentAuth, Password, Type) ->
    functools:chain(
      CurrentAuth,
      [maybe_update_plain_auth_hashes(_, Password, Type),
       scram_sha:maybe_update_hashes(_, Password, Type)]).

%% Returns updated auth info iff any of the hash parameters has changed and
%% hash migration is allowed
maybe_update_auth(CurrentAuth, Identity, Password, Type) ->
    try
        AllowHashMigrationDefault =
            allow_hash_migration_during_auth_default(),
        MigrateHashes =
            cluster_compat_mode:is_cluster_76() andalso
                ns_config:read_key_fast(
                  allow_hash_migration_during_auth, AllowHashMigrationDefault),

        Pre76Auth =
            (undefined /= proplists:get_value(<<"plain">>, CurrentAuth)),

        case MigrateHashes of
            true when not Pre76Auth ->
                Auth = update_auth(CurrentAuth, Password, Type),
                case Auth /= CurrentAuth of
                    true ->
                        {new_auth, Auth};
                    false ->
                        no_change
                end;
            true ->
                ?log_debug("Skipping hash migration for identity - ~p "
                           "(pre-7.6 format)",
                           [ns_config_log:tag_user_data(Identity)]),
                no_change;
            false ->
                no_change
        end
    catch
        E:T:S ->
            ?log_debug("Password migration failed. Identity - ~p.~n"
                       "Error - ~p",
                       [ns_config_log:tag_user_data(Identity), {E, T, S}]),
            no_change
    end.

maybe_migrate_password_hashes(CurrentAuth, {_, local} = Identity, Password) ->
    case maybe_update_auth(CurrentAuth, Identity, Password, regular) of
        {new_auth, Auth} ->
            migrate_local_user_auth(Identity, Auth);
        no_change ->
            ok
    end.

migrate_local_user_auth(Identity, NewAuth) ->
    ?call_on_ns_server_node(
       try
           store_auth(Identity, NewAuth, ?REPLICATED_DETS_NORMAL_PRIORITY),
           ns_server_stats:notify_counter(<<"pass_hash_migration">>)
       catch
           E:T:S ->
               ?log_debug("Auth store for auth migration failed. "
                          "Identity - ~p.~nError - ~p",
                          [ns_config_log:tag_user_data(Identity),
                           {E, T, S}])
       end, [Identity, NewAuth]).

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
    ns_config_auth:check_hash(get_salt_and_mac(Auth), Password).

get_user_props(Identity) ->
    get_user_props(Identity, ?DEFAULT_PROPS).

get_user_props(Identity, ItemList) ->
    make_props(Identity, get_props_raw(user, Identity), ItemList).

get_props_raw(Type, Identity) when Type == user; Type == group; Type == auth ->
    replicated_dets:get(storage_name(), {Type, Identity}, []).

-spec user_exists(rbac_identity()) -> boolean().
user_exists(Identity) ->
    ?call_on_ns_server_node(
       begin
           false =/= replicated_dets:get(storage_name(), {user, Identity})
       end, [Identity]).

-spec get_roles(rbac_identity()) -> [rbac_role()].
get_roles(Identity) ->
    proplists:get_value(roles, get_user_props(Identity, [roles]), []).

%% Groups functions

store_group(Identity, Description, Roles, LDAPGroup) ->
    Snapshot = ns_bucket:get_snapshot(all, [collections, uuid]),
    case menelaus_roles:validate_roles(Roles, public, Snapshot) of
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
                                   [ns_config_log:tag_user_data(Key),
                                    Props, NewProps]),
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
    Props = get_props_raw(group, GroupId),
    make_group_props(Props, Items).

get_group_props(GroupId, Items, Definitions, Buckets) ->
    Props = get_props_raw(group, GroupId),
    make_group_props(Props, Items, {[], [], Definitions, Buckets}).

group_exists(GroupId) ->
    false =/= replicated_dets:get(storage_name(), {group, GroupId}).

get_group_ldap_ref(GroupId) ->
    proplists:get_value(ldap_group_ref,
                        get_group_props(GroupId, [ldap_group_ref])).

has_group_ldap_ref(GroupId) ->
    not is_empty_ldap_group_ref(get_group_ldap_ref(GroupId)).

%% Unfortunately we allow ldap_group_ref as "".
is_empty_ldap_group_ref(Ref) ->
    undefined =:= Ref orelse [] =:= Ref.

get_group_roles(GroupId) ->
    proplists:get_value(roles, get_group_props(GroupId, [roles]), []).

get_group_roles(GroupId, Definitions, Snapshot) ->
    Props = get_group_props(GroupId, [roles], Definitions, Snapshot),
    proplists:get_value(roles, Props, []).

make_group_props(Props, Items) ->
    make_group_props(Props, Items, make_props_state(Items)).

make_group_props(Props, Items, {_, _, Definitions, Snapshot}) ->
    lists:map(
      fun (roles = Name) ->
              Roles = proplists:get_value(roles, Props, []),
              Roles2 = menelaus_roles:filter_out_invalid_roles(
                         Roles, Definitions, Snapshot),
              {Name, Roles2};
          (Name) ->
              {Name, proplists:get_value(Name, Props)}
      end, Items).

get_user_roles(UserProps, Definitions, Snapshot) ->
    menelaus_roles:filter_out_invalid_roles(
      proplists:get_value(roles, UserProps, []), Definitions, Snapshot).

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

get_groups_roles({LocalGroups, ExtGroups}, Definitions, Snapshot) ->
    [{G, get_group_roles(G, Definitions, Snapshot)}
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

-spec get_user_uuid(rbac_identity()) -> binary() | undefined.
get_user_uuid(Identity) ->
    get_user_uuid(Identity, undefined).

-spec get_user_uuid(rbac_identity(), binary() | undefined) -> binary() |
          undefined.
get_user_uuid({_, local} = Identity, Default) ->
    proplists:get_value(uuid, get_props_raw(user, Identity), Default);
get_user_uuid(_, _) ->
    undefined.

%% Build auth entry for internal users
build_internal_auth(Passwords) ->
    build_auth(Passwords, internal, false).

build_regular_auth(Passwords, TemporaryPassword) ->
    build_auth(Passwords, regular, TemporaryPassword).

build_auth(Passwords, AuthType, TemporaryPassword) ->
    build_plain_auth(Passwords, AuthType, TemporaryPassword) ++
        scram_sha:build_auth(Passwords, AuthType).

build_plain_auth(Passwords, AuthType, TemporaryPassword)
  when AuthType =:= regular; AuthType =:= internal ->
    Auth =
        case cluster_compat_mode:is_cluster_76() of
            true ->
                HashType = ns_config:read_key_fast(password_hash_alg,
                                                   ?DEFAULT_PWHASH),
                format_plain_auth(ns_config_auth:new_password_hash(HashType,
                                                                   AuthType,
                                                                   Passwords));
            false ->
                format_pre_76_plain_auth(
                  ns_config_auth:new_password_hash(?SHA1_HASH, AuthType, Passwords))
        end,
    case TemporaryPassword of
        true -> expire_password(Auth);
        false -> Auth
    end.

expire_password(Auth) ->
    Auth ++ [{<<"expiry">>, 0}].

remove_password_expiry(Auth) ->
    proplists:delete(<<"expiry">>, Auth).

format_plain_auth(HashInfo) ->
    [{<<"hash">>, {HashInfo}}].

format_pre_76_plain_auth(HashInfo) ->
    Salt = base64:decode(proplists:get_value(?SALT_KEY, HashInfo)),
    [Hash | _] = proplists:get_value(?HASHES_KEY, HashInfo),
    Mac = base64:decode(Hash),
    SaltAndMac = <<Salt/binary, Mac/binary>>,
    [{<<"plain">>, base64:encode(SaltAndMac)}].

rbac_upgrade_key(_) ->
    rbac_upgrade_key().

rbac_upgrade_key() ->
    rbac_upgrade.

config_upgrade() ->
    [{delete, rbac_upgrade_key()}].

upgrade_in_progress() ->
    ns_config:search(rbac_upgrade_key()) =/= false.

filter_out_invalid_roles(Props, Definitions, Snapshot) ->
    Roles = proplists:get_value(roles, Props, []),
    FilteredRoles = menelaus_roles:filter_out_invalid_roles(Roles, Definitions,
                                                            Snapshot),
    lists:keystore(roles, 1, Props, {roles, FilteredRoles}).

cleanup_bucket_roles(BucketName) ->
    ?log_debug("Delete all roles for bucket ~p", [BucketName]),
    Snapshot = ns_bucket:remove_from_snapshot(
                 BucketName, ns_bucket:get_snapshot(all, [collections, uuid])),

    Definitions = menelaus_roles:get_definitions(all),
    UpdateFun =
        fun ({Type, Key}, Props) when Type == user; Type == group ->
                case filter_out_invalid_roles(Props, Definitions, Snapshot) of
                    Props ->
                        skip;
                    NewProps ->
                        ?log_debug("Changing properties of ~p ~p from ~p "
                                   "to ~p due to deletion of ~p",
                                   [Type, ns_config_log:tag_user_data(Key),
                                    Props, NewProps, BucketName]),
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

sync_with_remotes(Nodes, Version) ->
    replicated_storage:sync_to_me(
      storage_name(), Nodes, ?get_timeout(rbac_upgrade_key(Version), 60000)).

upgrade(Version, Config, Nodes) ->
    try
        ?log_info("Upgrading users database to ~p", [Version]),
        Key = rbac_upgrade_key(Version),
        case ns_config:search(Config, Key) of
            false ->
                ns_config:set(Key, started);
            {value, started} ->
                ?log_info("Found unfinished roles upgrade. Continue.")
        end,

        %% propagate upgrade key to nodes
        ok = ns_config_rep:ensure_config_seen_by_nodes(Nodes),
        sync_with_remotes(Nodes, Version),

        do_upgrade(Version),
        ?log_info("Users database was upgraded to ~p", [Version]),
        sync_with_remotes(Nodes, Version),
        ?log_info("Users database upgrade was delivered to ~p", [Nodes]),
        ok
    catch T:E:S ->
            ale:error(?USER_LOGGER, "Unsuccessful user storage upgrade.~n~p",
                      [{T, E, S}]),
            error
    end.

upgrade_props(?VERSION_76, auth, _Key, AuthProps) ->
    {ok, functools:chain(AuthProps,
                         [scram_sha:fix_pre_76_auth_info(_),
                          get_rid_of_plain_key(_)])};
upgrade_props(?VERSION_79, user, _Key, UserProps) ->
    {ok, functools:chain(UserProps,
                         [maybe_substitute_user_roles(_)])};
upgrade_props(_Vsn, _RecType, _Key, _Props) ->
    skip.

%% For roles that have been eliminated or changed, substitute in their
%% replacements.
maybe_substitute_user_roles(User) ->
    lists:map(
      fun ({roles, Roles}) ->
              {roles, maybe_substitute_roles(Roles)};
          (Other) ->
              Other
      end, User).

%% Replacement roles for ones that have been removed or changed. Only
%% done for the same releases as supported in our upgrade matrix. These
%% replacements are being done in 7.9. Once 7.9 is the oldest supported
%% release we no longer have to support this replacement.
maybe_substitute_roles(Roles) ->
    lists:flatmap(
      fun (security_admin_local) ->
              [security_admin, user_admin_local];
          (security_admin_external) ->
              [security_admin, user_admin_external];
          (ro_admin) ->
              [ro_admin, ro_security_admin];
          (Role) ->
              [Role]
      end, Roles).

get_rid_of_plain_key(Auth) ->
    lists:map(
      fun ({<<"plain">>, _}) ->
              {<<"hash">>, {obsolete_get_salt_and_mac(Auth)}};
          (Other) ->
              Other
      end, Auth).

do_upgrade(Version) ->
    UpdateFun =
        fun ({RecType, Key}, Props) ->
                case upgrade_props(Version, RecType, Key, Props) of
                    skip ->
                        skip;
                    {ok, Props} ->
                        skip;
                    {ok, NewProps} ->
                        ?log_debug("Upgrade ~p from ~p to ~p",
                                   [{RecType, ns_config_log:tag_user_data(Key)},
                                    ns_config_log:tag_user_props(Props),
                                    ns_config_log:tag_user_props(NewProps)]),
                        {update, NewProps}
                end
        end,

    [] = replicated_dets:select_with_update(
           storage_name(), '_', 100, UpdateFun).

apply_keys_and_resave() ->
    replicated_dets:apply_keys_and_resave(storage_name()).

get_key_ids_in_use() ->
    replicated_dets:get_key_ids_in_use(storage_name()).

-ifdef(TEST).

upgrade_test_() ->
    CheckAuth =
        fun (User, AuthType, Expected) ->
            fun () ->
                Props = get_props_raw(auth, {User, local}),
                {Actual} = proplists:get_value(AuthType, Props, []),
                ?assertEqual(lists:sort(Expected),
                             lists:sort(Actual))
            end
        end,
    CheckUser =
        fun (User, UserType, Expected) ->
            fun () ->
                Props = get_props_raw(user, {User, local}),
                Actual = proplists:get_value(UserType, Props, []),
                ?assertEqual(lists:sort(Expected),
                             lists:sort(Actual))
            end
        end,

    Test =
        fun (Version, Users, Checks) ->
                {lists:flatten(io_lib:format("Upgrade to ~p", [Version])),
                 fun () ->
                         [replicated_dets:toy_set(
                            storage_name(), Id, Props) ||
                             {Id, Props} <- Users],
                         do_upgrade(Version),
                         [C() || C <- Checks]
                 end}
        end,
    {foreach,
     fun() ->
             meck:new(replicated_dets, [passthrough]),
             meck:expect(replicated_dets, select_with_update,
                         fun replicated_dets:toy_select_with_update/4),
             replicated_dets:toy_init(storage_name())
     end,
     fun (_) ->
             meck:unload(replicated_dets),
             ets:delete(storage_name())
     end,
     [Test(?VERSION_76,
           [{{auth, {"migrated-user", local}},
             [{<<"hash">>, {[anything]}},
              {<<"scram-sha-1">>, {[anything]}}]}],
           [CheckAuth("migrated-user", <<"hash">>, [anything]),
            CheckAuth("migrated-user", <<"scram-sha-1">>, [anything])]),
      Test(?VERSION_76,
           [{{auth, {"not-migrated-user", local}},
             [{<<"hash">>, {[anything]}},
              {<<"sha1">>,
               {[{?OLD_SCRAM_SALT_KEY, <<"0ues3mfZqA4OjuljBI/uQY5L0jI=">>},
                 {?OLD_SCRAM_HASH_KEY, <<"kZlCBy+TU+meqxR7rJfg9mS1LZA=">>},
                 {?OLD_SCRAM_ITERATIONS_KEY, 4000}]}}]}],
           [CheckAuth("not-migrated-user", <<"hash">>, [anything]),
            CheckAuth("not-migrated-user", <<"scram-sha-1">>,
                      [{?HASHES_KEY, [{[{?SCRAM_STORED_KEY_KEY,
                                         <<"APXjupUS+LktBEirfdNtNtCYChk=">>},
                                        {?SCRAM_SERVER_KEY_KEY,
                                         <<"Vkelr1rzrz9tT0Z/AhLvKJVuWJs=">>}]}]},
                       {?SCRAM_SALT_KEY, <<"0ues3mfZqA4OjuljBI/uQY5L0jI=">>},
                       {?SCRAM_ITERATIONS_KEY, 4000}])]),
      Test(?VERSION_79,
           [{{user, {"local-security-admin", local}},
             [{roles, [security_admin_local]}]}],
           [CheckUser("local-security-admin", roles,
                      [security_admin, user_admin_local])]),
      Test(?VERSION_79,
           [{{user, {"external-security-admin", local}},
             [{roles, [security_admin_external]}]}],
           [CheckUser("external-security-admin", roles,
                      [security_admin, user_admin_external])]),
      Test(?VERSION_79,
           [{{user, {"ro-admin", local}},
             [{roles, [ro_admin]}]}],
           [CheckUser("ro-admin", roles,
                      [ro_admin, ro_security_admin])]),
      Test(?VERSION_79,
           [{{user, {"unchanged-admin", local}},
            [{roles, [eventing_admin]}]}],
           [CheckUser("unchanged-admin", roles,
                      [eventing_admin])])]}.

meck_ns_config_read_key_fast(Settings) ->
    meck:expect(
      ns_config, read_key_fast,
      fun (K, Default) ->
              proplists:get_value(K, Settings, Default)
      end).

maybe_update_plain_auth_hashes_test__(
  OldAuth, Password, OldSettings, NewSettings) ->
    meck_ns_config_read_key_fast(NewSettings),
    NewAuth = maybe_update_plain_auth_hashes(OldAuth, Password, regular),

    NewSaltAndMac = get_salt_and_mac(NewAuth),
    OldSaltAndMac = get_salt_and_mac(OldAuth),

    %% Assert all new updates are correctly applied.
    lists:foreach(
      fun ({password_hash_alg, NV}) ->
              ?assertEqual(
                 NV, proplists:get_value(?HASH_ALG_KEY, NewSaltAndMac));
          ({pbkdf2_sha512_iterations, NV}) ->
              ?assertEqual(
                 NV, proplists:get_value(?PBKDF2_ITER_KEY, NewSaltAndMac));
          ({argon2id_time, NV}) ->
              ?assertEqual(
                 NV, proplists:get_value(?ARGON_TIME_KEY, NewSaltAndMac));
          ({argon2id_mem, NV}) ->
              ?assertEqual(
                 NV, proplists:get_value(?ARGON_MEM_KEY, NewSaltAndMac))
      end, NewSettings),

    %% If OldSettings and NewSettings are same, assert the Auth isn't updated.
    case OldSettings =:= NewSettings of
        true ->
            ?assertEqual(NewAuth, OldAuth);
        false ->
            ok
    end,

    %% Check none of the scram-shas have been touched (they are enabled by
    %% default).
    ?assertEqual(
       NewAuth -- format_plain_auth(NewSaltAndMac),
       OldAuth -- format_plain_auth(OldSaltAndMac)).

maybe_update_plain_auth_hashes_test_() ->
    Password = "dummy-password",
    BuildSettings =
        fun ({Alg, Settings}) ->
                [{password_hash_alg, Alg}] ++
                    case Alg of
                        ?SHA1_HASH ->
                            [];
                        ?PBKDF2_HASH ->
                            [{pbkdf2_sha512_iterations,
                              proplists:get_value(
                                iterations, Settings, ?DEFAULT_PBKDF2_ITER)}];
                        ?ARGON2ID_HASH ->
                            [{argon2id_time,
                              proplists:get_value(
                                time, Settings, ?DEFAULT_ARG2ID_TIME)},
                             {argon2id_mem,
                              proplists:get_value(
                                mem, Settings, ?DEFAULT_ARG2ID_MEM)}]
                    end
        end,

    Sha1Settings = {?SHA1_HASH, []},
    PBKDFSettings = {?PBKDF2_HASH, [{iterations, ?PBKDF2_ITER_MIN}]},
    PBKDFSettings1 = {?PBKDF2_HASH, [{iterations, ?PBKDF2_ITER_MIN + 1}]},
    ArgonSettings =
        {?ARGON2ID_HASH, [{time, ?ARGON_TIME_MIN}, {mem, ?ARGON_MEM_MIN}]},
    ArgonSettings1 =
        {?ARGON2ID_HASH, [{time, ?ARGON_TIME_MIN + 1}, {mem, ?ARGON_MEM_MIN}]},

    TestArgs =
        [%% Simple one to one hash alg change.
         [{old_settings, Sha1Settings}, {new_settings, ArgonSettings}],
         [{old_settings, ArgonSettings}, {new_settings, Sha1Settings}],
         [{old_settings, Sha1Settings}, {new_settings, PBKDFSettings}],
         [{old_settings, PBKDFSettings}, {new_settings, Sha1Settings}],
         [{old_settings, PBKDFSettings}, {new_settings, ArgonSettings}],
         [{old_settings, ArgonSettings}, {new_settings, PBKDFSettings}],
         %% Don't change settings.
         [{old_settings, Sha1Settings}, {new_settings, Sha1Settings}],
         [{old_settings, PBKDFSettings}, {new_settings, PBKDFSettings}],
         [{old_settings, ArgonSettings}, {new_settings, ArgonSettings}],
         %% Don't change hash alg type, but change settings.
         [{old_settings, PBKDFSettings}, {new_settings, PBKDFSettings1}],
         [{old_settings, ArgonSettings}, {new_settings, ArgonSettings1}]],

    TestFun =
        fun ([OldSettings, NewSettings], _R) ->
                {lists:flatten(io_lib:format("Old Settings - ~p,~n"
                                             "New Settings - ~p.~n",
                                             [OldSettings, NewSettings])),
                 fun () ->
                         maybe_update_plain_auth_hashes_test__(
                           %% Build both plain auth hashes and scram-sha
                           %% hashes. scram-sha hashes are enabled by
                           %% default.
                           build_auth([Password], regular, false),
                           Password, OldSettings, NewSettings)
                 end}
        end,

    {foreachx,
     fun ([OldSettings, _NewSettings]) ->
             meck:new([ns_config, cluster_compat_mode], [passthrough]),
             meck_ns_config_read_key_fast(OldSettings),
             meck:expect(
               cluster_compat_mode, is_cluster_76,
               fun () -> true end),
             meck:expect(
               cluster_compat_mode, is_cluster_79,
               fun () -> true end)
     end,
     fun (_X, _R) ->
             meck:unload([ns_config, cluster_compat_mode])
     end,
     [{lists:map(fun ({_, S}) -> BuildSettings(S) end, TestArg), TestFun}
      || TestArg <- TestArgs]}.

maybe_update_auth_test() ->
    CommonSettings = [{allow_hash_migration_during_auth, true}],
    Sha1Settings = [{password_hash_alg, ?SHA1_HASH} | CommonSettings],
    PbkdfSettings = fun (I1, I2) ->
                            [{password_hash_alg, ?PBKDF2_HASH},
                             {pbkdf2_sha512_iterations, I1},
                             {pbkdf2_sha512_iterations_internal, I2}
                            | CommonSettings]
                    end,
    ArgonSettings = fun (T, M, T2, M2) ->
                            [{password_hash_alg, ?ARGON2ID_HASH},
                             {argon2id_time, T},
                             {argon2id_mem, M},
                             {argon2id_time_internal, T2},
                             {argon2id_mem_internal, M2}
                            | CommonSettings]
                    end,
    ScramSettings = fun (I1, I2) ->
                            [{memcached_password_hash_iterations, I1},
                             {memcached_password_hash_iterations_internal, I2}
                            | CommonSettings]
                    end,
    %% Just change the order in all lists without actually changing
    %% anything meaningful
    Rearrange = fun (A) ->
                        generic:maybe_transform(
                          fun (L) when is_list(L) -> {continue, lists:reverse(L)};
                              (T) -> {continue, T}
                          end, A)
                end,
    %% Generate auth record then change settings, and verify that auth
    %% changes when it is expected to change
    Check = fun (OldSettings, NewSettings, Type, ExpectChange) ->
                    Pass = "abc",
                    meck_ns_config_read_key_fast(OldSettings),
                    Auth = build_auth([Pass], Type, false),
                    meck_ns_config_read_key_fast(NewSettings),
                    Res = maybe_update_auth(Rearrange(Auth), {"test", local},
                                            Pass, Type),
                    case ExpectChange of
                        true -> ?assertMatch({new_auth, _}, Res);
                        false -> ?assertEqual(no_change, Res)
                    end
            end,
    ShouldChange = Check(_, _, _, true),
    ShouldNotChange = Check(_, _, _, false),
    meck:new([ns_config, cluster_compat_mode], [passthrough]),
    config_profile:load_default_profile_for_test(),
    try
        meck:expect(cluster_compat_mode, is_cluster_76,
                    fun () -> true end),
        meck:expect(cluster_compat_mode, is_cluster_79,
                    fun () -> true end),

        %% Testing 2 things here:
        %% - the fact auth doesn't change when unrelated settings change
        %% - the fact that auth doesn't change if auth list is simply rearranged
        ShouldNotChange(Sha1Settings, Sha1Settings, regular),
        ShouldNotChange(Sha1Settings, Sha1Settings, internal),

        ShouldChange(PbkdfSettings(10, 15), PbkdfSettings(11, 15), regular),
        ShouldChange(PbkdfSettings(10, 15), PbkdfSettings(10, 16), internal),
        ShouldNotChange(PbkdfSettings(10, 15), PbkdfSettings(10, 16), regular),
        ShouldNotChange(PbkdfSettings(10, 15), PbkdfSettings(11, 15), internal),

        ShouldChange(ArgonSettings(1, 8192, 1, 8192),
                     ArgonSettings(2, 8192, 1, 8192), regular),
        ShouldNotChange(ArgonSettings(1, 8192, 1, 8192),
                        ArgonSettings(1, 8192, 2, 8192), regular),
        ShouldChange(ArgonSettings(1, 8192, 1, 8192),
                     ArgonSettings(1, 8193, 1, 8192), regular),
        ShouldNotChange(ArgonSettings(1, 8192, 1, 8192),
                        ArgonSettings(1, 8192, 1, 8193), regular),
        ShouldChange(ArgonSettings(1, 8192, 1, 8192),
                     ArgonSettings(1, 8192, 1, 8193), internal),
        ShouldNotChange(ArgonSettings(1, 8192, 1, 8192),
                        ArgonSettings(1, 8193, 1, 8192), internal),

        ShouldChange(ScramSettings(10, 15), ScramSettings(11, 15), regular),
        ShouldNotChange(ScramSettings(10, 15), ScramSettings(10, 16), regular),
        ShouldChange(ScramSettings(10, 15), ScramSettings(10, 16), internal),
        ShouldNotChange(ScramSettings(10, 15), ScramSettings(11, 15), internal)
    after
        meck:unload([ns_config, cluster_compat_mode]),
        config_profile:unload_profile_for_test()
    end.
-endif.
