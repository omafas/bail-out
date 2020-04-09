%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2018 Couchbase, Inc.
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
%% @doc access api for admin credentials

-module(ns_config_auth).

-include("ns_common.hrl").

-export([authenticate/2,
         set_admin_credentials/2,
         get_user/1,
         get_password/1,
         admin_credentials_changed/2,
         get_admin_user_and_auth/0,
         get_admin_creds/1,
         is_system_provisioned/0,
         is_system_provisioned/1,
         get_no_auth_buckets/1,
         hash_password/1,
         hash_password/2]).

admin_cfg_key() ->
    rest_creds.

set_admin_credentials(User, Password) ->
    Auth = {auth, menelaus_users:build_scram_auth(Password)},
    ns_config:set(admin_cfg_key(), {User, Auth}).

get_admin_user_and_auth() ->
    get_admin_user_and_auth(ns_config:latest()).

get_admin_user_and_auth(Config) ->
    {value, UserAuth} = ns_config:search(Config, admin_cfg_key()),
    UserAuth.

is_system_provisioned() ->
    is_system_provisioned(ns_config:latest()).

is_system_provisioned(Config) ->
    case get_admin_user_and_auth(Config) of
        {_, _} ->
            true;
        _ ->
            false
    end.

get_user(special) ->
    "@";
get_user(admin) ->
    case get_admin_user_and_auth() of
        {U, _} ->
            U;
        _ ->
            undefined
    end.

get_password(special) ->
    ns_config:search_node_prop(ns_config:latest(), memcached, admin_pass).

get_salt_and_mac({password, {Salt, Mac}}) ->
    {Salt, Mac};
get_salt_and_mac({auth, Auth}) ->
    menelaus_users:get_salt_and_mac(Auth).

get_admin_creds(Config) ->
    case get_admin_user_and_auth(Config) of
        {User, Auth} ->
            {User, get_salt_and_mac(Auth)};
        _ ->
            undefined
    end.

admin_credentials_changed(User, Password) ->
    case get_admin_creds(ns_config:latest()) of
        {User, {Salt, Mac}} ->
            hash_password(Salt, Password) =/= Mac;
        _ ->
            true
    end.

authenticate_as_admin([$@ | _] = User, Password) ->
    MemcachedPassword =
        ns_config:search_node_prop(ns_config:latest(), memcached, admin_pass),
    misc:compare_secure(MemcachedPassword, Password)
        orelse authenticate_non_special(User, Password);
authenticate_as_admin(User, Password) ->
    authenticate_non_special(User, Password).

authenticate(Username, Password) ->
    case authenticate_as_admin(Username, Password) of
        true ->
            {ok, {Username, admin}};
        false ->
            case menelaus_users:authenticate(Username, Password) of
                true ->
                    {ok, {Username, local}};
                false ->
                    case is_bucket_auth(Username, Password) of
                        true ->
                            {ok, {Username, bucket}};
                        false ->
                            false
                    end
            end
    end.

authenticate_non_special(User, Password) ->
    case get_admin_user_and_auth() of
        null ->
            true;
        {User, Auth} ->
            {Salt, Mac} = get_salt_and_mac(Auth),
            misc:compare_secure(hash_password(Salt, Password), Mac);
        {_, _} ->
            false
    end.

hash_password(Password) ->
    Salt = crypto:strong_rand_bytes(16),
    {Salt, hash_password(Salt, Password)}.

hash_password(Salt, Password) ->
    crypto:hmac(sha, Salt, list_to_binary(Password)).

is_bucket_auth(User, Password) ->
    case ns_bucket:get_bucket(User) of
        {ok, BucketConf} ->
            case {proplists:get_value(auth_type, BucketConf),
                  proplists:get_value(sasl_password, BucketConf)} of
                {none, _} ->
                    Password =:= "";
                {sasl, P} ->
                    misc:compare_secure(Password, P)
            end;
        not_present ->
            false
    end.

get_no_auth_buckets(Config) ->
    [BucketName ||
        {BucketName, BucketProps} <- ns_bucket:get_buckets(Config),
        proplists:get_value(auth_type, BucketProps) =:= none orelse
            proplists:get_value(sasl_password, BucketProps) =:= ""].
