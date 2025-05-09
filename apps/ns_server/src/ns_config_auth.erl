%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc access api for admin credentials

-module(ns_config_auth).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([authenticate/2,
         set_admin_credentials/2,
         set_admin_with_auth/2,
         get_user/1,
         get_password/1,
         get_password/2,
         get_password/3,
         get_special_passwords/2,
         admin_credentials_changed/2,
         get_admin_user_and_auth/0,
         get_admin_creds/1,
         is_system_provisioned/0,
         is_system_provisioned/1,
         new_password_hash/3,
         hash_password/2,
         check_hash/2,
         config_upgrade_to_76/1,
         configurable_hash_alg_settings/2,
         migrate_admin_auth/3]).

admin_cfg_key() ->
    rest_creds.

set_admin_credentials(User, Password) ->
    set_admin_with_auth(User,
                        menelaus_users:build_regular_auth([Password], false)).

set_admin_with_auth(User, Auth) ->
    ns_config:set(admin_cfg_key(), {User, {auth, Auth}}).

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
    get_password(node(), special).

get_password(Node, special) ->
    get_password(Node, ns_config:latest(), special).

get_password(Node, Config, special) ->
    case get_special_passwords(Node, Config) of
        [] -> undefined;
        [Pass | _] -> Pass
    end.

get_special_passwords(Node, Config) when is_atom(Node)->
    case ns_config:search_node_prop(Node, Config, memcached, admin_pass) of
        undefined -> [];
        {v2, Passwords} -> Passwords;
        Password when is_list(Password) -> [Password]
    end.

get_salt_and_mac({password, {Salt, Mac}}) ->
    [{?HASH_ALG_KEY, ?SHA1_HASH},
     {?SALT_KEY, base64:encode(Salt)},
     {?HASHES_KEY, [base64:encode(Mac)]}];
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
        {User, HashInfo} ->
            not check_hash(HashInfo, Password);
        _ ->
            true
    end.

authenticate_special("@prometheus" = User, Password) ->
    prometheus_cfg:authenticate(User, Password);
authenticate_special("@localtoken" = User, Password) ->
    case menelaus_local_auth:check_token(Password) of
        true ->
            {ok, {User, local_token}};
        false ->
            {error, auth_failure}
    end;
authenticate_special([$@ | _] = User, Password) ->
    case get_special_passwords(node(), ns_config:latest()) of
        [] ->
            {error, temporary_failure};
        SpecPasswords ->
            case misc:compare_secure_many(Password, SpecPasswords) of
                true ->
                    {ok, {User, admin}};
                false ->
                    authenticate_admin(User, Password)
            end
    end;
authenticate_special(User, Password) ->
    authenticate_admin(User, Password).

authenticate(Username, Password) ->
    case authenticate_special(Username, Password) of
        {ok, Id} ->
            {ok, Id};
        {error, auth_failure} ->
            menelaus_users:authenticate(Username, Password);
        {error, Reason} ->
            {error, Reason}
    end.

authenticate_admin(User, Password) ->
    case get_admin_user_and_auth() of
        null ->
            {ok, {User, admin}};
        {User, Auth} = CurUserAndAuth ->
            case check_hash(get_salt_and_mac(Auth), Password) of
                true ->
                    Identity = {User, admin},
                    case Auth of
                        {auth, AuthInfo} ->
                            case menelaus_users:maybe_update_auth(AuthInfo,
                                                                  Identity,
                                                                  Password,
                                                                  regular) of
                                {new_auth, NewAuth} ->
                                    migrate_admin_auth(User, NewAuth,
                                                       CurUserAndAuth);
                                no_change -> ok
                            end;
                        {password, _} ->
                            %% Not yet upgraded auth
                            ok
                    end,
                    {ok, Identity};
                false ->
                    {error, auth_failure}
            end;
        {_, _} ->
            {error, auth_failure}
    end.

migrate_admin_auth(User, NewAuth, CurUserAndAuth) ->
    ?call_on_ns_server_node(
       begin
           case ns_config:update_if_unchanged(admin_cfg_key(), CurUserAndAuth,
                                              {User, {auth, NewAuth}}) of
               ok ->
                   ns_server_stats:notify_counter(<<"pass_hash_migration">>);
               {error, changed} ->
                   %% Something else has already changed it
                   ok;
               {error, retry_needed} ->
                   ?log_error("Hash migration transaction for admin failed")
           end,
           ok
       end, [User, NewAuth, CurUserAndAuth]).

check_hash(HashInfo, Password) ->
    Base64Hashes = proplists:get_value(?HASHES_KEY, HashInfo),
    Hashes = [base64:decode(H) || H <- Base64Hashes],
    Hash1 = hash_password(HashInfo, Password),
    misc:compare_secure_many(Hash1, Hashes).

new_password_hash(HashType, AuthType, Passwords) ->
    Info = new_hash_info(HashType, AuthType),
    Hashes = [base64:encode(hash_password(Info, P)) || P <- Passwords],
    [{?HASHES_KEY, Hashes} | Info].

configurable_hash_alg_settings(?ARGON2ID_HASH, AuthType) ->
    {Time, Mem} = case AuthType of
                      regular ->
                          {argon2id_time, argon2id_mem};
                      internal ->
                          {argon2id_time_internal, argon2id_mem_internal}
                  end,
    [{?ARGON_TIME_KEY, ns_config:read_key_fast(Time, ?DEFAULT_ARG2ID_TIME)},
     {?ARGON_MEM_KEY, ns_config:read_key_fast(Mem, ?DEFAULT_ARG2ID_MEM)}];
configurable_hash_alg_settings(?PBKDF2_HASH, AuthType) ->
    IterKey = case AuthType of
                  regular -> pbkdf2_sha512_iterations;
                  internal -> pbkdf2_sha512_iterations_internal
              end,
    [{?PBKDF2_ITER_KEY, ns_config:read_key_fast(IterKey,
                                                ?DEFAULT_PBKDF2_ITER)}];
configurable_hash_alg_settings(?SHA1_HASH, _AuthType) ->
    [].

new_hash_info(HashType, AuthType) ->
    [{?HASH_ALG_KEY, HashType} | new_hash_info_int(HashType, AuthType)].

new_hash_info_int(?ARGON2ID_HASH, AuthType) ->
    SaltSize = enacl:pwhash_SALTBYTES(),
    [{?SALT_KEY, base64:encode(crypto:strong_rand_bytes(SaltSize))},
     %% we support only p=1 because enacl+libsodium always uses 1
     {?ARGON_THREADS_KEY, 1} |
     configurable_hash_alg_settings(?ARGON2ID_HASH, AuthType)];
new_hash_info_int(?PBKDF2_HASH, AuthType) ->
    [{?SALT_KEY, base64:encode(crypto:strong_rand_bytes(64))} |
     configurable_hash_alg_settings(?PBKDF2_HASH, AuthType)];
new_hash_info_int(?SHA1_HASH, AuthType) ->
    [{?SALT_KEY, base64:encode(crypto:strong_rand_bytes(16))} |
     configurable_hash_alg_settings(?SHA1_HASH, AuthType)].

hash_password(HashInfo, Password) ->
    case proplists:get_value(?HASH_ALG_KEY, HashInfo) of
        ?ARGON2ID_HASH ->
            Salt = base64:decode(proplists:get_value(?SALT_KEY, HashInfo)),
            Ops = proplists:get_value(?ARGON_TIME_KEY, HashInfo),
            Mem = proplists:get_value(?ARGON_MEM_KEY, HashInfo),
            1 = proplists:get_value(?ARGON_THREADS_KEY, HashInfo),
            enacl:pwhash(Password, Salt, Ops, Mem, argon2id13);
        ?PBKDF2_HASH ->
            Salt = base64:decode(proplists:get_value(?SALT_KEY, HashInfo)),
            Iterations = proplists:get_value(?PBKDF2_ITER_KEY, HashInfo),
            scram_sha:pbkdf2(sha512, Password, Salt, Iterations);
        ?SHA1_HASH ->
            Salt = base64:decode(proplists:get_value(?SALT_KEY, HashInfo)),
            crypto:mac(hmac, sha, Salt, list_to_binary(Password))
    end.

config_upgrade_to_76(Config) ->
    case get_admin_user_and_auth(Config) of
        null -> [];
        {User, Info} ->
            Auth =
                case Info of
                    {password, _} ->
                        Hash = get_salt_and_mac(Info),
                        menelaus_users:format_plain_auth(Hash);
                    {auth, A} ->
                        A
                end,
            {ok, NewAuth} = menelaus_users:upgrade_props(?VERSION_76, auth,
                                                         User, Auth),
            [{set, admin_cfg_key(), {User, {auth, NewAuth}}}]
    end.

-ifdef(TEST).

upgrade_null_admin_auth_to_76_test() ->
    ?assertEqual([], config_upgrade_to_76([[{rest_creds, null}]])).

upgrade_legacy_admin_auth_to_76_test() ->
    LegacyAuth = {password,
                  {<<97,38,90,8,100,179,170,77,6,75,118,147,14,129,30,211>>,
                   <<191,201,210,9,232,187,214,20,247,75,45,50,157,66,102,
                     243,228,31,167,175>>}},
    [Upgrade] = config_upgrade_to_76([[{rest_creds,
                                             {"admin", LegacyAuth}}]]),
    {set, rest_creds, {"admin", NewAuth}} = Upgrade,
    check_hash(get_salt_and_mac(LegacyAuth), "asdasd"),
    check_hash(get_salt_and_mac(NewAuth), "asdasd").

upgrade_7_2_admin_auth_to_76_test() ->
    Auth72 =
        {auth,[{<<"plain">>,
                <<"wzs7PI/FUZ7PtPUXEnHp9qezfkTvveJ6In3xZ/wxUF25FokA">>},
               {<<"sha512">>,
                {[{<<"h">>,
                   <<"NKgHCIAQNo4EPf0iRTOPvXHF8X8G6QAjrsgOKztJtJs6"
                     "gczyyfASTWoeHRQIVmB18LXtQ56p5M7zj2ieC75iUw==">>},
                  {<<"s">>,
                   <<"WTp2FilmNevHaBmEqkrvAhK5PV4eSV0DkjiXVAA0K9dh"
                     "26iLM6m/8I525oHJDbH/CKiIaWN/wgxCYoSONb6SmA==">>},
                  {<<"i">>, 4000}]}},
               {<<"sha256">>,
                {[{<<"h">>,
                   <<"iRqA1NSe0l6HmLPgR5zuO8MAJV1WCvB/NLC+cEKjrXo=">>},
                  {<<"s">>, <<"YGscBx8RaBd72KrmL4x4OaYpZnPyxULg0Tp+ej/7GGc=">>},
                  {<<"i">>, 4000}]}},
               {<<"sha1">>,
                {[{<<"h">>, <<"Lg8O+6gckoGI6BgaIyaaVbqbR7U=">>},
                  {<<"s">>, <<"jvqilB34e2UAw6QXdIFGhKtYxuQ=">>},
                  {<<"i">>, 4000}]}}]},
    [Upgrade] = config_upgrade_to_76([[{rest_creds,
                                             {"admin", Auth72}}]]),
    {set, rest_creds, {"admin", NewAuth}} = Upgrade,
    check_hash(get_salt_and_mac(Auth72), "asdasd"),
    check_hash(get_salt_and_mac(NewAuth), "asdasd").

enacl_dirty_schedulers_test() ->
    PrevWallTimeValue = erlang:system_flag(scheduler_wall_time, true),
    try
        Salt = <<175,163,193,248,24,140,199,101,242,135,111,31,184,88,204,206>>,
        S1 = scheduler:get_sample(),
        enacl:pwhash("asdasd", Salt, 1000, 8000000, argon2id13),
        S2 = scheduler:get_sample(),
        Res = scheduler:utilization(S1, S2),

        %% Make sure there is at least one dirty scheduler that was used
        %% during hash calculation. It is possible that something else
        %% actually used that dirty scheduler but I don't know how we can test
        %% it better
        AnyDirtySchedulersUsed =
            lists:any(fun ({cpu, _, Util, _}) when Util > 0.5 -> true;
                          (_) -> false
                      end, Res),
        io:format("Schedulers utilization: ~p", [Res]),
        ?assert(AnyDirtySchedulersUsed)
    after
        erlang:system_flag(scheduler_wall_time, PrevWallTimeValue)
    end.

-endif.
