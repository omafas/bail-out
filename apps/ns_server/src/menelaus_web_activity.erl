%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(menelaus_web_activity).

-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([default/0,
         handle_get/1,
         handle_post/1,
         get_config/0,
         is_config_key/1,
         is_enabled/1]).

-define(CONFIG_KEY, user_activity).

-define(DEFAULT_TRACKED_ROLES,
        [admin, ro_admin, security_admin, user_admin_local, user_admin_external,
         cluster_admin, eventing_admin, backup_admin, views_admin,
         replication_admin, fts_admin, analytics_admin]).

-spec default() -> [{atom(), any()}].
default() ->
    [{enabled, false},
     {tracked_roles, ?DEFAULT_TRACKED_ROLES},
     {tracked_groups, []}].

handle_get(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_morpheus(),

    menelaus_web_settings2:handle_get([], params(), fun type_spec/1,
                                      get_config(), Req).

handle_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_morpheus(),

    menelaus_web_settings2:handle_post(
      fun (Params, Req2) ->
              case Params of
                  [] -> ok;
                  _ ->
                      Props = lists:map(fun ({[K], V}) -> {K, V} end, Params),
                      Values = set_config(Props),

                      %% Convert to json, to avoid groups getting formatted as
                      %% lists of integers
                      {AuditJson} = menelaus_web_settings2:prepare_json(
                                      [], params(), fun type_spec/1, Values),
                      ns_audit:user_activity_settings(Req, AuditJson)
              end,
              handle_get(Req2)
      end, [], params(), fun type_spec/1, get_config(), [], Req).

set_config(Changes) ->
    OldConfig = get_config(),

    NewConfig = misc:update_proplist(OldConfig, Changes),
    ns_config:set(?CONFIG_KEY, NewConfig),
    NewConfig.

params() ->
    [{"enabled",
      #{type => bool,
        cfg_key => enabled}},
     {"trackedRoles",
      #{type => tracked_roles,
        cfg_key => tracked_roles}},
     {"trackedGroups",
      #{type => tracked_groups,
        cfg_key => tracked_groups}}].

type_spec(tracked_roles) ->
    #{validators => [{string_list, ","},
                     validator:validate(fun get_roles/1, _, _)],
      formatter => fun (L) -> {value, [atom_to_binary(M) || M <- L]} end};
type_spec(tracked_groups) ->
    #{validators => [{string_list, ","},
                     validator:validate(fun get_groups/1, _, _)],
      formatter => fun (L) -> {value, [list_to_binary(M) || M <- L]} end}.

parse_role(RoleStr, Definitions) ->
    try
        RoleName = menelaus_web_rbac:role_to_atom(RoleStr),
        case lists:keyfind(RoleName, 1, Definitions) of
            false ->
                {error, RoleStr};
            _ -> RoleName
        end
    catch
        error:badarg -> {error, RoleStr}
    end.

get_roles(RolesRaw) ->
    Definitions = menelaus_roles:get_definitions(public),
    Roles = [parse_role(string:trim(RoleRaw), Definitions)
             || RoleRaw <- RolesRaw],

    %% Gather erroneous roles
    BadRoles = [BadRole || {error, BadRole} <- Roles],
    case BadRoles of
        [] -> {value, Roles};
        _ -> {error,
              lists:flatten(io_lib:format("The following roles are invalid: ~s",
                                          [string:join(BadRoles, ",")]))}
    end.

-ifdef(TEST).
bad_roles_test() ->
    config_profile:load_default_profile_for_test(),
    meck:expect(cluster_compat_mode, get_compat_version, ?cut([8, 0])),
    meck:expect(cluster_compat_mode, is_developer_preview, ?cut(false)),
    ?assertEqual({value, []}, get_roles([])),
    ?assertEqual({value, [cluster_admin]}, get_roles(["cluster_admin"])),
    ?assertEqual({value, [cluster_admin, data_reader]},
                 get_roles(["cluster_admin", "data_reader"])),
    ?assertEqual({error, "The following roles are invalid: nonsense_role"},
                 get_roles(["nonsense_role"])),
    %% We only expect role names, without any parameterisation
    ?assertEqual({error, "The following roles are invalid: cluster_admin[*]"},
                 get_roles(["cluster_admin[*]"])),
    ?assertEqual({error,
                  "The following roles are invalid: "
                  "nonsense_role,another_nonsense_role"},
                 get_roles(["nonsense_role", "another_nonsense_role"])),
    meck:unload(),
    config_profile:unload_profile_for_test().
-endif.

get_groups(Groups) ->
    UnexpectedGroups = [Group || Group <- Groups,
                                 not menelaus_users:group_exists(Group)],
    case UnexpectedGroups of
        [] -> {value, Groups};
        _ -> {error,
              lists:flatten(
                  io_lib:format("The following groups do not exist: ~s",
                                [string:join(UnexpectedGroups, ",")]))}
    end.

-ifdef(TEST).
bad_groups_test() ->
    meck:expect(menelaus_users, group_exists,
                fun ("real_group1") -> true;
                    ("real_group2") -> true;
                    (_) -> false
                end),
    ?assertEqual({value, []}, get_groups([])),
    ?assertEqual({value, ["real_group1"]}, get_groups(["real_group1"])),
    ?assertEqual({value, ["real_group1", "real_group2"]},
                 get_groups(["real_group1", "real_group2"])),
    ?assertEqual({error, "The following groups do not exist: fake_group"},
                 get_groups(["fake_group"])),
    ?assertEqual({error,
                  "The following groups do not exist: fake_group1,fake_group2"},
                 get_groups(["fake_group1", "fake_group2"])),
    meck:unload().
-endif.

-spec get_config() -> proplists:proplist().
get_config() ->
    ns_config:read_key_fast(?CONFIG_KEY, []).

-spec is_config_key(term()) -> boolean().
is_config_key(?CONFIG_KEY) -> true;
is_config_key(_) -> false.

-spec is_enabled(proplists:proplist()) -> boolean().
is_enabled(Config) ->
    proplists:get_bool(enabled, Config).
