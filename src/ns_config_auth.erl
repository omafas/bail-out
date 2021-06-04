%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
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
         get_no_auth_buckets/0,
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

authenticate_special("@prometheus" = User, Password) ->
    prometheus_cfg:authenticate(User, Password);
authenticate_special([$@ | _] = User, Password) ->
    MemcachedPassword =
        ns_config:search_node_prop(ns_config:latest(), memcached, admin_pass),
    case misc:compare_secure(MemcachedPassword, Password) of
        true ->
            {ok, {User, admin}};
        false ->
            authenticate_admin(User, Password)
    end;
authenticate_special(User, Password) ->
    authenticate_admin(User, Password).

authenticate(Username, Password) ->
    case authenticate_special(Username, Password) of
        {ok, Id} ->
            {ok, Id};
        false ->
            case menelaus_users:authenticate(Username, Password) of
                true ->
                    {ok, {Username, local}};
                false ->
                    false
            end
    end.

authenticate_admin(User, Password) ->
    case get_admin_user_and_auth() of
        null ->
            {ok, {User, admin}};
        {User, Auth} ->
            {Salt, Mac} = get_salt_and_mac(Auth),
            case misc:compare_secure(hash_password(Salt, Password), Mac) of
                true -> {ok, {User, admin}};
                false -> false
            end;
        {_, _} ->
            false
    end.

hash_password(Password) ->
    Salt = crypto:strong_rand_bytes(16),
    {Salt, hash_password(Salt, Password)}.

hash_password(Salt, Password) ->
    crypto:hmac(sha, Salt, list_to_binary(Password)).

get_no_auth_buckets() ->
    get_no_auth_buckets(ns_bucket:get_snapshot()).

get_no_auth_buckets(Snapshot) ->
    [BucketName ||
        {BucketName, BucketProps} <- ns_bucket:get_buckets(Snapshot),
        proplists:get_value(auth_type, BucketProps) =:= none orelse
            proplists:get_value(sasl_password, BucketProps) =:= ""].
