%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2018 Couchbase, Inc.
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
%% @doc handling of memcached passwords file
-module(memcached_passwords).

-behaviour(memcached_cfg).

-export([start_link/0, sync/0]).

%% callbacks
-export([format_status/1, init/0, filter_event/1, handle_event/2, producer/1, refresh/0]).

-include("ns_common.hrl").
-include("pipes.hrl").

-record(state, {buckets,
                users,
                admin_pass,
                rest_creds}).

start_link() ->
    Path = ns_config:search_node_prop(ns_config:latest(), isasl, path),
    memcached_cfg:start_link(?MODULE, Path).

sync() ->
    memcached_cfg:sync(?MODULE).

format_status(State) ->
    Buckets = lists:map(fun ({U, _P}) ->
                                {U, "*****"}
                        end,
                        State#state.buckets),
    State#state{buckets=Buckets, admin_pass="*****"}.

init() ->
    Config = ns_config:get(),
    AU = ns_config:search_node_prop(Config, memcached, admin_user),
    Users = ns_config:search_node_prop(Config, memcached, other_users, []),
    AP = ns_config:search_node_prop(Config, memcached, admin_pass),
    Buckets = extract_creds(ns_config:search(Config, buckets, [])),
    RestCreds = ns_config:read_key_fast(rest_creds, undefined),

    #state{buckets = Buckets,
           users = [AU | Users],
           admin_pass = AP,
           rest_creds = RestCreds}.

filter_event({buckets, _V}) ->
    true;
filter_event({auth_version, _V}) ->
    true;
filter_event({rest_creds, _V}) ->
    true;
filter_event(_) ->
    false.

handle_event({buckets, V}, #state{buckets = Buckets} = State) ->
    case extract_creds(V) of
        Buckets ->
            unchanged;
        NewBuckets ->
            {changed, State#state{buckets = NewBuckets}}
    end;
handle_event({auth_version, _V}, State) ->
    {changed, State};
handle_event({rest_creds, Creds}, #state{rest_creds = Creds}) ->
    unchanged;
handle_event({rest_creds, Creds}, State) ->
    {changed, State#state{rest_creds = Creds}}.

producer(#state{buckets = Buckets,
                users = Users,
                admin_pass = AP,
                rest_creds = RestCreds}) ->
    pipes:compose([menelaus_users:select_auth_infos({'_', local}),
                   jsonify_auth(Users, AP, Buckets, RestCreds),
                   sjson:encode_extended_json([{compact, false},
                                               {strict, false}])]).

get_admin_auth_json({User, {password, {Salt, Mac}}}) ->
    %% this happens after upgrade to 5.0, before the first password change
    {User, menelaus_users:build_plain_auth(Salt, Mac)};
get_admin_auth_json({User, {auth, Auth}}) ->
    {User, Auth};
get_admin_auth_json(_) ->
    undefined.

jsonify_auth(Users, AdminPass, Buckets, RestCreds) ->
    MakeAuthInfo = fun menelaus_users:user_auth_info/2,
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

           AdminAuth = menelaus_users:build_scram_auth(AdminPass),
           [?yield({json, MakeAuthInfo(U, AdminAuth)}) || U <- Users],

           lists:foreach(
             fun ({Bucket, Password}) ->
                     BucketAuth = menelaus_users:build_plain_auth(Password),
                     AInfo = MakeAuthInfo(Bucket ++ ";legacy", BucketAuth),
                     ?yield({json, AInfo})
             end, Buckets),

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
                             ?yield({json, MakeAuthInfo(UserName, Auth)})
                     end
             end),
           ?yield(array_end),
           ?yield(kv_end),
           ?yield(object_end)
       end).

refresh() ->
    memcached_refresh:refresh(isasl).

extract_creds(ConfigList) ->
    Configs = proplists:get_value(configs, ConfigList),
    lists:sort([{BucketName,
                 proplists:get_value(sasl_password, BucketConfig, "")}
                || {BucketName, BucketConfig} <- Configs]).
