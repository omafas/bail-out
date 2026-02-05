%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(activity_tracker).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("rbac.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0,
         handle_activity/1,
         is_tracked/1,
         is_user_covered/2,
         get_activity_from_node/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, ?MODULE).

%% 60s should be sufficient time that we don't timeout when a node is busy
-define(RPC_TIMEOUT, ?get_timeout(rpc_timeout, 60000)).

-record(state, {}).

%%%===================================================================
%%% External interface
%%%===================================================================

-spec handle_activity(#authn_res{}) -> ok.
handle_activity(#authn_res{identity = Identity}) ->
    case ns_node_disco:couchdb_node() =/= node() andalso is_tracked(Identity) of
        true ->
            note_identity(Identity);
        false ->
            ok
    end.

-spec is_tracked(rbac_identity()) -> boolean().
is_tracked({_, local} = Identity) ->
    Config = menelaus_web_activity:get_config(),
    case menelaus_web_activity:is_enabled(Config) of
        true ->
            is_user_covered(Identity, Config);
        false ->
            false
    end;
is_tracked(_) ->
    %% Non-local users aren't tracked
    false.

-spec get_activity_from_node(node()) -> [{rbac_identity(), non_neg_integer()}].
get_activity_from_node(Node) ->
    %% This function will be called from remote nodes, so changing the message
    %% will require backwards compatibility handling
    gen_server:call({?SERVER, Node}, last_activity, ?RPC_TIMEOUT).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ets:new(?TABLE, [named_table, set, public]),
    ns_pubsub:subscribe_link(user_storage_events, fun user_storage_event/1),
    ns_pubsub:subscribe_link(ns_config_events, fun config_event/1),
    {ok, #state{}}.

%% This will be used by the activity_aggregator to fetch each node's latest
%% activity time for each user.
%% By going through a gen_server, we need to be careful that we don't risk a
%% backlog in the queue, from the potentially heavy calls to ets:tab2list/1
%% being serialised.
%% However, since the aggregator will only make one of these calls at a time
%% (every 15 mins when it fetches the latest information), this isn't really a
%% serious risk.
handle_call(last_activity, _From, State) ->
    Config = menelaus_web_activity:get_config(),
    case menelaus_web_activity:is_enabled(Config) of
        true ->
            {reply, ets:tab2list(?TABLE), State};
        false ->
            {reply, [], State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
handle_info(clear_activity_for_untracked_users, State) ->
    misc:flush(clear_activity_for_untracked_users),
    Config = menelaus_web_activity:get_config(),
    case menelaus_web_activity:is_enabled(Config) of
        true ->
            %% Get non-existent/untracked users which we have activity for
            DeletedUsers =
                ets:foldl(
                  fun ({User, _}, Users) ->
                          Exists = menelaus_users:user_exists(User),
                          case Exists andalso is_user_covered(User, Config) of
                              true -> Users;
                              false -> [User | Users]
                          end
                  end, [], ?TABLE),
            case DeletedUsers of
                [] ->
                    ok;
                _ ->
                    ?log_debug("Clearing activity timestamp for "
                               "deleted/untracked users: ~p",
                               [ns_config_log:tag_user_data(DeletedUsers)]),
                    %% Delete any activity for those users
                    ets:select_delete(?TABLE, [{{User, '_'}, [], [true]}
                                               || User <- DeletedUsers])
            end;
        false ->
            ets:delete_all_objects(?TABLE)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

note_identity(Identity) ->
    Time = calendar:datetime_to_gregorian_seconds(
             calendar:universal_time()),
    ets:insert(?TABLE, {Identity, Time}).

user_storage_event({user_version, _}) ->
    ?SERVER ! clear_activity_for_untracked_users;
user_storage_event(_) ->
    ok.

config_event({Key, _}) ->
    case menelaus_web_activity:is_config_key(Key) of
        true -> ?SERVER ! clear_activity_for_untracked_users;
        false -> ok
    end;
config_event(_) ->
    ok.

is_user_covered({_, local} = Identity, Config) ->
    Props = menelaus_users:get_user_props(Identity, [groups, roles]),
    UserGroups = proplists:get_value(groups, Props, []),
    UserRolesRaw = proplists:get_value(roles, Props, []),
    UserRoles = lists:map(fun strip_role_parameterisation/1,
                          UserRolesRaw),

    TrackedGroups = proplists:get_value(tracked_groups, Config, []),
    TrackedRoles = proplists:get_value(tracked_roles, Config, []),

    do_lists_intersect(TrackedGroups, UserGroups) orelse
        do_lists_intersect(TrackedRoles, UserRoles);
is_user_covered(_Identity, _) ->
    %% Non-local users aren't tracked
    false.

strip_role_parameterisation({Role, _}) when is_atom(Role) ->
    Role;
strip_role_parameterisation(Role) when is_atom(Role) ->
    Role.

do_lists_intersect(List1, List2) ->
    lists:any(fun (X) -> lists:member(X, List2) end, List1).

%%%===================================================================
%%% Tests
%%%===================================================================


-ifdef(TEST).

-define(GROUP_A, a).
-define(GROUP_B, b).
-define(ROLE_X, x).
-define(ROLE_Y, y).
-define(ROLE_Z, z).

-define(LOCAL_USER_IN_A, {"user_in_a", local}).
-define(LOCAL_USER_IN_B, {"user_in_b", local}).
-define(LOCAL_USER_WITH_X, {"user_with_x", local}).
-define(LOCAL_USER_WITH_X_1, {"user_with_x_1", local}).
-define(LOCAL_USER_WITH_Y, {"user_with_y", local}).
-define(LOCAL_USER_WITH_PARAMETERISED_ROLE,
        {"user_with_parameterised_role`", local}).
-define(EXTERNAL_USER_IN_A, {"user_in_a", external}).
-define(EXTERNAL_USER_WITH_X, {"user_with_x", external}).

groups_map() ->
    #{?LOCAL_USER_IN_A => [?GROUP_A],
        ?LOCAL_USER_WITH_X => [],
        ?LOCAL_USER_WITH_X_1 => [],
        ?LOCAL_USER_IN_B => [?GROUP_B],
        ?LOCAL_USER_WITH_Y => [],
        ?LOCAL_USER_WITH_PARAMETERISED_ROLE => [],
        ?EXTERNAL_USER_IN_A => [?GROUP_A],
        ?EXTERNAL_USER_WITH_X => []}.

roles_map() ->
    #{?LOCAL_USER_IN_A => [],
        ?LOCAL_USER_WITH_X => [?ROLE_X],
        ?LOCAL_USER_WITH_X_1 => [?ROLE_X],
        ?LOCAL_USER_IN_B => [],
        ?LOCAL_USER_WITH_Y => [?ROLE_Y],
        ?LOCAL_USER_WITH_PARAMETERISED_ROLE => [{?ROLE_Z, [any]}],
        ?EXTERNAL_USER_IN_A => [],
        ?EXTERNAL_USER_WITH_X => [?ROLE_X]}.

setup_user_props_meck(RolesMap, GroupsMap) ->
    meck:expect(menelaus_users, user_exists,
                fun (U) ->
                    lists:member(U, maps:keys(RolesMap))
                end),
    meck:expect(menelaus_users, get_user_props,
                fun (User, [groups, roles]) ->
                        [{groups, maps:get(User, GroupsMap)},
                         {roles, maps:get(User, RolesMap)}]
                end).

setup() ->
    fake_ns_config:setup(),

    meck:new(calendar, [unstick, passthrough]),

    %% couchdb node handling is covered by
    %% SampleBucketTestSet.post_with_couchdb_sample_test
    meck:expect(ns_node_disco, couchdb_node, fun () -> other_node end),

    setup_user_props_meck(roles_map(), groups_map()),

    gen_event:start_link({local, user_storage_events}),

    configure(menelaus_web_activity:default()),
    start_link().

teardown(_) ->
    gen_server:stop(?SERVER),
    fake_ns_config:teardown(),
    meck:unload().

configure(Settings) ->
    fake_ns_config:update_snapshot(user_activity, Settings).

-define(auth(Identity), #authn_res{identity = Identity}).

get_last_activity(Identity) ->
    Activity = gen_server:call(?SERVER, last_activity),
    proplists:get_value(Identity, Activity).

track_covered_user_test__() ->
    CoveredGroups = [?GROUP_A],
    CoveredRoles = [?ROLE_X, ?ROLE_Z],
    configure([{enabled, true},
               {tracked_groups, CoveredGroups},
               {tracked_roles, CoveredRoles}]),
    meck:expect(calendar, universal_time, 0,
                meck:seq([{{2024,8,19},{14,12,I}} || I <- lists:seq(1, 6)])),
    lists:foreach(
      fun (Identity) ->
              ?assert(is_tracked(Identity)),

              %% Expect that auth is tracked for covered users
              Time0 = get_last_activity(Identity),
              ?assertEqual(undefined, Time0),
              handle_activity(?auth(Identity)),
              Time1 = get_last_activity(Identity),
              ?assertNotEqual(undefined, Time1),
              handle_activity(?auth(Identity)),
              Time2 = get_last_activity(Identity),
              ?assert(Time1 < Time2)
      end, [?LOCAL_USER_WITH_X, ?LOCAL_USER_IN_A,
            ?LOCAL_USER_WITH_PARAMETERISED_ROLE]).

dont_track_uncovered_user_test__() ->
    CoveredGroups = [?GROUP_A],
    CoveredRoles = [?ROLE_X],
    configure([{enabled, true},
               {tracked_groups, CoveredGroups},
               {tracked_roles, CoveredRoles}]),
    lists:foreach(
      fun (Identity) ->
              ?assertNot(is_tracked(Identity)),

              %% Expect that auth is not tracked for uncovered users
              Time0 = get_last_activity(Identity),
              ?assertEqual(undefined, Time0),
              handle_activity(?auth(Identity)),
              Time1 = get_last_activity(Identity),
              ?assertEqual(undefined, Time1)
      end, [?LOCAL_USER_IN_B,
            ?LOCAL_USER_WITH_Y,
            ?EXTERNAL_USER_IN_A,
            ?EXTERNAL_USER_WITH_X]).

clear_deleted_users_test__() ->
    ExistingUser = ?LOCAL_USER_WITH_X,
    DeletedUser = ?LOCAL_USER_WITH_X_1,
    CoveredRoles = [?ROLE_X],
    configure([{enabled, true},
               {tracked_roles, CoveredRoles},
               {tracked_groups, []}]),

    lists:foreach(
      fun (Identity) ->
              ?assert(is_tracked(Identity)),
              handle_activity(?auth(Identity)),
              Time = get_last_activity(Identity),
              ?assertNotEqual(undefined, Time)
      end, [ExistingUser,
            DeletedUser]),

    try
        meck:expect(menelaus_users, user_exists,
                    fun (U) when U =:= ExistingUser -> true;
                        (_) -> false
                    end),

        %% Clear call history from prior tests
        meck:reset(menelaus_users),
        %% Notify with event and wait for activity to be cleared
        gen_event:notify(user_storage_events, {user_version, {0, 1}}),
        meck:wait(menelaus_users, user_exists, ['_'], 5000),

        ?assertNotEqual(undefined, get_last_activity(ExistingUser)),
        ?assertEqual(undefined, get_last_activity(DeletedUser))
    after
        setup_user_props_meck(roles_map(), groups_map())
    end.

clear_when_user_switched_role_test__() ->
    try
        User = ?LOCAL_USER_WITH_X,
        CoveredRoles = [?ROLE_X],
        configure([{enabled, true},
                   {tracked_roles, CoveredRoles},
                   {tracked_groups, []}]),
        ?assert(is_tracked(User)),
        handle_activity(?auth(User)),
        Time = get_last_activity(User),
        ?assertNotEqual(undefined, Time),

        RolesMap0 = roles_map(),
        RolesMap1 = RolesMap0#{User => [?ROLE_Y]},
        setup_user_props_meck(RolesMap1, groups_map()),

        %% Clear call history from prior tests
        meck:reset(menelaus_users),
        %% Notify with event and wait for activity to be cleared
        gen_event:notify(user_storage_events, {user_version, {0, 1}}),
        meck:wait(menelaus_users, user_exists, ['_'], 5000),

        ?assertEqual(undefined, get_last_activity(User))
    after
        %% Put roles and groups back to their original values, for other tests
        setup_user_props_meck(roles_map(), groups_map())
    end.

clear_when_user_switched_group_test__() ->
    try
        User = ?LOCAL_USER_IN_A,
        CoveredGroups = [?GROUP_A],
        configure([{enabled, true},
                   {tracked_roles, []},
                   {tracked_groups, CoveredGroups}]),
        ?assert(is_tracked(User)),
        handle_activity(?auth(User)),
        Time = get_last_activity(User),
        ?assertNotEqual(undefined, Time),

        GroupsMap0 = groups_map(),
        GroupsMap1 = GroupsMap0#{User => [?GROUP_B]},
        setup_user_props_meck(roles_map(), GroupsMap1),

        %% Clear call history from prior tests
        meck:reset(menelaus_users),
        %% Notify with event and wait for activity to be cleared
        gen_event:notify(user_storage_events, {user_version, {0, 1}}),
        meck:wait(menelaus_users, user_exists, ['_'], 5000),

        ?assertEqual(undefined, get_last_activity(User))
    after
        %% Put roles and groups back to their original values, for other tests
        setup_user_props_meck(roles_map(), groups_map())
    end.

all_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [fun track_covered_user_test__/0,
      fun dont_track_uncovered_user_test__/0,
      fun clear_deleted_users_test__/0,
      fun clear_when_user_switched_role_test__/0,
      fun clear_when_user_switched_group_test__/0]}.

-endif.
