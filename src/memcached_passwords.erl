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
                admin_passwords,
                rest_creds,
                prometheus_auth}).

start_link() ->
    Path = ns_config:search_node_prop(ns_config:latest(), isasl, path),
    memcached_cfg:start_link(?MODULE, Path).

sync() ->
    memcached_cfg:sync(?MODULE).

format_status(State) ->
    State#state{admin_passwords="*****"}.

init() ->
    Config = ns_config:get(),

    #state{users = extract_users(Config),
           admin_passwords = extract_passwords(Config),
           rest_creds = ns_config_auth:get_admin_user_and_auth(),
           prometheus_auth = prometheus_cfg:get_auth_info()}.

filter_event(auth_version) ->
    true;
filter_event(rest_creds) ->
    true;
filter_event({node, Node, prometheus_auth_info}) when Node =:= node() ->
    true;
filter_event({node, Node, memcached}) ->
    Node =:= dist_manager:this_node();
filter_event(_Key) ->
    false.

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
    end;
handle_event({node, _Node, memcached}, #state{} = State) ->
    Config = ns_config:get(),
    NewState = State#state{users = extract_users(Config),
                           admin_passwords = extract_passwords(Config)},
    case NewState =/= State of
        true ->
            {changed, NewState};
        false ->
            unchanged
    end.

producer(#state{users = Users,
                admin_passwords = AdminPasswords,
                rest_creds = RestCreds,
                prometheus_auth = PromAuth}) ->
    pipes:compose([menelaus_users:select_auth_infos({'_', local}),
                   jsonify_auth(Users, AdminPasswords, RestCreds, PromAuth),
                   sjson:encode_extended_json([{compact, false},
                                               {strict, false}])]).

get_admin_auth_json({User, {password, {Salt, Mac}}}) ->
    %% this happens after upgrade to 5.0, before the first password change
    {User, menelaus_users:format_plain_auth(
             [{?HASH_ALG_KEY, ?SHA1_HASH},
              {?SALT_KEY, base64:encode(Salt)},
              {?HASHES_KEY, [base64:encode(Mac)]}])};
get_admin_auth_json({User, {auth, Auth}}) ->
    {User, Auth};
get_admin_auth_json(_) ->
    undefined.

memcached_user_info(User, Auth) ->
    {ok, NewAuth} =
        menelaus_users:upgrade_props(?VERSION_76, auth, User, Auth),
    {list_to_binary(User), {NewAuth}}.

jsonify_auth(Users, AdminPasswords, RestCreds, PromAuth) ->
    ?make_transducer(
       begin
           ?yield(object_start),
           ?yield({kv, {<<"@@version@@">>, 2}}),

           ClusterAdmin =
               case get_admin_auth_json(RestCreds) of
                   undefined ->
                       undefined;
                   {User, Auth} ->
                       ?yield({kv, memcached_user_info(User, Auth)}),
                       User
               end,

           case PromAuth of
               {PUser, PAuth} ->
                   ?yield({kv, memcached_user_info(PUser, PAuth)}),
                   PUser;
               undefined -> ok
           end,

           AdminAuth = menelaus_users:build_internal_auth(AdminPasswords),
           [?yield({kv, memcached_user_info(U, AdminAuth)}) || U <- Users],

           pipes:foreach(
             ?producer(),
             fun ({{auth, {UserName, _Type}}, Auth}) ->
                     case UserName of
                         ClusterAdmin ->
                             TagCA = ns_config_log:tag_user_name(ClusterAdmin),
                             ?log_warning("Encountered user ~p with the same "
                                          "name as cluster administrator",
                                          [TagCA]),
                             ok;
                         _ ->
                             ?yield({kv, memcached_user_info(UserName, Auth)})
                     end
             end),
           ?yield(object_end)
       end).

refresh() ->
    memcached_refresh:refresh(isasl).

extract_passwords(Config) ->
    lists:usort(
      ns_config_auth:get_special_passwords(dist_manager:this_node(), Config)).

extract_users(Config) ->
    [ns_config:search_node_prop(Config, memcached, admin_user) |
     ns_config:search_node_prop(Config, memcached, other_users, [])].
