%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc handling of memcached passwords file
-module(memcached_passwords).

-behaviour(memcached_cfg).

-export([start_link/0, sync/0]).

%% callbacks
-export([format_status/1, init/0, filter_event/1, handle_event/2, producer/1, refresh/0]).

-include("ns_common.hrl").
-include("pipes.hrl").

-record(state, {users,
                admin_pass,
                rest_creds,
                prometheus_auth}).

start_link() ->
    Path = ns_config:search_node_prop(ns_config:latest(), isasl, path),
    memcached_cfg:start_link(?MODULE, Path).

sync() ->
    memcached_cfg:sync(?MODULE).

format_status(State) ->
    State#state{admin_pass="*****"}.

init() ->
    Config = ns_config:get(),
    AU = ns_config:search_node_prop(Config, memcached, admin_user),
    Users = ns_config:search_node_prop(Config, memcached, other_users, []),
    AP = ns_config:search_node_prop(Config, memcached, admin_pass),

    #state{users = [AU | Users],
           admin_pass = AP,
           rest_creds = ns_config_auth:get_admin_user_and_auth(),
           prometheus_auth = prometheus_cfg:get_auth_info()}.

filter_event(limits_version) ->
    true;
filter_event(enforce_limits) ->
    true;
filter_event(auth_version) ->
    true;
filter_event(rest_creds) ->
    true;
filter_event({node, Node, prometheus_auth_info}) when Node =:= node() ->
    true;
filter_event(_Key) ->
    false.

handle_event(limits_version, State) ->
    {changed, State};
handle_event(enforce_limits, State) ->
    {changed, State};
handle_event(auth_version, State) ->
    %% auth_version also takes care of UUID changes when deleting and adding
    %% same user.
    {changed, State};
handle_event(rest_creds, #state{rest_creds = Creds} = State) ->
    case ns_config_auth:get_admin_user_and_auth() of
        Creds ->
            unchanged;
        Other ->
            {changed, State#state{rest_creds = Other}}
    end;
handle_event({node, Node, prometheus_auth_info},
             #state{prometheus_auth = Auth} = State) when Node =:= node() ->
    case prometheus_cfg:get_auth_info() of
        Auth ->
            unchanged;
        Other ->
            {changed, State#state{prometheus_auth = Other}}
    end.

producer(#state{users = Users,
                admin_pass = AP,
                rest_creds = RestCreds,
                prometheus_auth = PromAuth}) ->
    pipes:compose([menelaus_users:select_auth_infos({'_', local}),
                   jsonify_auth(Users, AP, RestCreds, PromAuth),
                   sjson:encode_extended_json([{compact, false},
                                               {strict, false}])]).

get_admin_auth_json({User, {password, {Salt, Mac}}}) ->
    %% this happens after upgrade to 5.0, before the first password change
    {User, menelaus_users:build_plain_auth(Salt, Mac)};
get_admin_auth_json({User, {auth, Auth}}) ->
    {User, Auth};
get_admin_auth_json(_) ->
    undefined.

jsonify_kv_user_limits(undefined) ->
    [];
jsonify_kv_user_limits(Limits) ->
    [{limits, {Limits}}].

get_user_limits_json(false, _Identity) ->
    [];
get_user_limits_json(true, Identity) ->
    Limits = menelaus_users:get_user_limits(Identity),
    case Limits of
        undefined ->
            [];
        _ ->
            KVLimits = proplists:get_value(kv, Limits),
            jsonify_kv_user_limits(KVLimits)
    end.

get_user_uuid(false, _Identity) ->
    [];
get_user_uuid(true, Identity) ->
    case menelaus_users:get_user_uuid(Identity) of
        undefined ->
            [];
        UUID ->
            [{uuid, UUID}]
    end.

jsonify_auth(Users, AdminPass, RestCreds, PromAuth) ->
    MakeAuthInfo = fun menelaus_users:memcached_user_info/2,
    EnforceLimits = cluster_compat_mode:should_enforce_limits(),
    ?make_transducer(
       begin
           ?yield(object_start),
           ?yield({kv_start, <<"users">>}),
           ?yield(array_start),

           ClusterAdmin =
               case get_admin_auth_json(RestCreds) of
                   undefined ->
                       undefined;
                   {User, Auth} ->
                       ?yield({json, MakeAuthInfo(User, Auth)}),
                       User
               end,

           case PromAuth of
               {PUser, PAuth} ->
                   ?yield({json, MakeAuthInfo(PUser, PAuth)}),
                   PUser;
               undefined -> ok
           end,

           AdminAuth = menelaus_users:build_scram_auth(AdminPass),
           [?yield({json, MakeAuthInfo(U, AdminAuth)}) || U <- Users],

           pipes:foreach(
             ?producer(),
             fun ({{auth, {UserName, _Type} = Identity}, Auth}) ->
                     case UserName of
                         ClusterAdmin ->
                             TagCA = ns_config_log:tag_user_name(ClusterAdmin),
                             ?log_warning("Encountered user ~p with the same "
                                          "name as cluster administrator",
                                          [TagCA]),
                             ok;
                         _ ->
                             Limits = get_user_limits_json(EnforceLimits,
                                                           Identity),
                             UUID = get_user_uuid(EnforceLimits, Identity),
                             ?yield({json, MakeAuthInfo(
                                             UserName,
                                             UUID ++ Limits ++ Auth)})
                     end
             end),
           ?yield(array_end),
           ?yield(kv_end),
           ?yield(object_end)
       end).

refresh() ->
    memcached_refresh:refresh(isasl).
