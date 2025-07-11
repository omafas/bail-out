%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc handlers for settings related REST API's

-module(menelaus_web_settings).

-include("ns_common.hrl").
-include("ns_bucket.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([build_kvs/1,
         handle_get/2,
         handle_get/3,
         handle_post/2,
         handle_post/3,
         handle_delete/3]).

-export([handle_settings_web/1,
         handle_settings_web_post/1,
         handle_settings_web_post/2,

         handle_get_cgroup_overrides/1,
         handle_post_cgroup_overrides/1,
         handle_delete_cgroup_override/2,

         handle_reset_alerts/1,

         handle_settings_stats/1,
         handle_settings_stats_post/1,

         handle_settings_rebalance/1,
         handle_settings_rebalance_post/1,
         get_rebalance_moves_per_node/0,

         handle_settings_auto_reprovision/1,
         handle_settings_auto_reprovision_post/1,
         handle_settings_auto_reprovision_reset_count/1,

         handle_settings_max_parallel_indexers/1,
         handle_settings_max_parallel_indexers_post/1,

         handle_settings_view_update_daemon/1,
         handle_settings_view_update_daemon_post/1,

         handle_settings_data_service/1,
         handle_settings_data_service_post/1,

         handle_reset_ciphers_suites/1,

         services_with_security_settings/0,
         settings_stats_validators/0,
         apply_stats_settings/1,
         settings_web_post_validators/0,
         validate_allowed_hosts_list/1,
         get_tls_version/1,
         parse_allowed_host/1,

         get_throttle_limit_attributes/0,
         get_throttle_capacity_attributes/0,
         get_storage_limit_attributes/0]).

-import(menelaus_util,
        [parse_validate_number/3,
         is_valid_positive_integer/1,
         parse_validate_port_number/1,
         reply_json/2,
         reply_json/3,
         reply_text/3,
         reply/2]).

-define(DEFAULT_KV_THROTTLE_CAPACITY, 25000).
-define(DEFAULT_INDEX_THROTTLE_CAPACITY, 1000000).
-define(DEFAULT_FTS_THROTTLE_CAPACITY, 900000).
-define(DEFAULT_N1QL_THROTTLE_CAPACITY, 6000000).
-define(DISABLE_UI_OVER_HTTP_DEFAULT, false).
-define(DISABLE_UI_OVER_HTTPS_DEFAULT, false).
-define(NONNEGATIVE_INT_OR_MAX_ERR,
        "Value '~0p' must be equal to or above 0 or the string 'max'.").

get_bool("true") ->
    {ok, true};
get_bool("false") ->
    {ok, false};
get_bool(_) ->
    {error, "Accepted values are 'true' and 'false'."}.

only_true("true") -> {ok, true};
only_true(_) -> {error, "Only accepted value is 'true'."}.

read_only(_) -> {error, "Property is readonly"}.

parse_validate_number_wrapper(Num, Min, Max) ->
    case parse_validate_number(Num, Min, Max) of
        {ok, _} = OK -> OK;
        _ ->
            M = io_lib:format("The value must be between ~p and ~p.",
                              [Min, Max]),
            {error, lists:flatten(M)}
    end.

get_number(Min, Max) ->
    fun (SV) ->
            parse_validate_number_wrapper(SV, Min, Max)
    end.

get_number(Min, Max, Default) ->
    fun (SV) ->
            case SV of
                "" ->
                    {ok, Default};
                _ ->
                    parse_validate_number_wrapper(SV, Min, Max)
            end
    end.

get_string(SV) ->
    {ok, list_to_binary(string:strip(SV))}.

get_tls_version(SV) ->
    TlsVersions = ns_ssl_services_setup:get_tls_version_map(),
    SupportedStr = [atom_to_list(S) || S <- lists:sort(maps:keys(TlsVersions))],
    case lists:member(SV, SupportedStr) of
        true -> {ok, list_to_atom(SV)};
        false ->
            M = io_lib:format("Supported TLS versions are ~s",
                              [string:join(SupportedStr, ", ")]),
            {error, lists:flatten(M)}
    end.

verify_hsts(Str) ->
    Props = lists:map(
              fun (P) ->
                      P1 = [string:trim(S) || S <- string:split(P, "=")],
                      case P1 of
                          [K] ->
                              {K, undefined};
                          [K, V] ->
                              {K, V}
                      end
              end, string:split(Str, ";", all)),
    {Valid, Invalid} = lists:partition(
                         fun ({"includeSubDomains", undefined}) ->
                                 true;
                             ({"preload", undefined}) ->
                                 true;
                             ({"max-age", Val}) ->
                                 Int = (catch erlang:list_to_integer(Val)),
                                 (is_integer(Int) andalso (Int >= 0));
                             (_) ->
                                 false
                         end, Props),
    case Invalid of
        [] ->
            case proplists:get_value("max-age", Valid) of
                undefined ->
                    {error, "max-age directive is required"};
                _ ->
                    case length(lists:ukeysort(1, Valid)) =:= length(Valid) of
                        false ->
                            {error, "Cannot have duplicate directives"};
                        true ->
                            ok
                    end
            end;
        _ ->
            InvalidDir = [K || {K, _V} <- Invalid],
            M = io_lib:format("Invalid directives ~s",
                              [lists:join("; ", InvalidDir)]),
            {error, lists:flatten(M)}
    end.

get_secure_headers(Json) ->
    try ejson:decode(Json) of
        {[{<<"Strict-Transport-Security">>, BinStr}]} ->
            Str = binary_to_list(BinStr),
            case verify_hsts(Str) of
                ok -> {ok, [{"Strict-Transport-Security", Str}]};
                Err -> Err
            end;
        _ ->
            {error, "Only \"Strict-Transport-Security\" header allowed"}
    catch
        _:_ ->
            {error, "Invalid format. Expecting a json."}
    end.

get_pwhash(AlgStr) ->
    AlgBin = list_to_binary(AlgStr),
    case AlgBin of
        ?ARGON2ID_HASH -> {ok, AlgBin};
        ?SHA1_HASH -> {ok, AlgBin};
        ?PBKDF2_HASH -> {ok, AlgBin};
        _ -> {error, "not supported hash"}
    end.

get_cipher_suites(Str) ->
    try ejson:decode(Str) of
        L when is_list(L) ->
            %% Note that the following lists:filter allows you to set TLS 1.3
            %% ciphers in the cipher list.
            %% However, golang TLS used by services index, ftx, n1ql, and
            %% eventing, doesn't allow configuring TLS 1.3 cipherSuites, see,
            %% https://golang.org/pkg/crypto/tls/#Config.
            %%
            %% This means that golang will,
            %% 1. Honor TLS 1.2 and TLS 1.1 cipherSuites if specified, i.e.,
            %%    only the TLS 1.2, and TLS 1.1 ciphers on this list are used.
            %% 2. If only TLS 1.3 cipher are specified in cipherSuites,
            %%    TLS 1.2 and TLS 1.1 ciphers are not used.
            %% 3. Allow all TLS 1.3 ciphers to be used, even if just a
            %%    few/none are specified.
            %%
            %% This behavior is fine by us, so we will allow setting TLS1.3
            %% ciphers in the cipherSuites.
            InvalidNames = lists:filter(?cut(not ciphers:is_valid_name(_)), L),
            case InvalidNames of
                [] -> {ok, L};
                _ ->
                    M = io_lib:format("Invalid cipher suite names ~s",
                                      [lists:join(", ", InvalidNames)]),
                    {error, lists:flatten(M)}
            end;
        _ -> {error, "Invalid format. Expecting a list of ciphers."}
    catch
        _:_ -> {error, "Invalid format. Expecting a list of ciphers."}
    end.

get_allowed_hosts(Str) ->
    try ejson:decode(Str) of
        L when is_list(L) ->
            case validate_allowed_hosts_list(L) of
                ok -> {ok, L};
                {error, Msg} -> {error, Msg}
            end;
        _ ->
            {error, "Invalid format. Expecting a list of strings"}
    catch
        _:_ -> {error, "Invalid format. Expecting JSON list"}
    end.


validate_allowed_hosts_list(AllowedHostsList) ->
    case validate_allowed_hosts_list(AllowedHostsList, []) of
        ok ->
            CurrentHostnames = [H || N <- ns_node_disco:nodes_wanted(),
                                     {_, H} <- [misc:node_name_host(N)],
                                     H =/= misc:localhost_alias()],
            lists:foldl(
              fun (_, {error, _} = Error) -> Error;
                  (Hostname, ok) ->
                      case ns_cluster:is_host_allowed(Hostname,
                                                      AllowedHostsList) of
                          true -> ok;
                          false ->
                              Msg = io_lib:format(
                                      "At least one cluster node (~s) doesn't "
                                      "match the allowed hosts", [Hostname]),
                              {error, lists:flatten(Msg)}
                      end
              end, ok, CurrentHostnames);
        {error, _} = Error ->
            Error
    end.

validate_allowed_hosts_list([], List) ->
    case (length(List) > 1) andalso lists:member(any, List) of
        false ->
            ok;
        true ->
            {error, "'*' when present must be the only element in the list"}
    end;
validate_allowed_hosts_list([E | Tail], Acc) ->
    case parse_allowed_host(E) of
        {error, Msg} -> {error, Msg};
        Res ->
            case lists:member(Res, Acc) of
                false -> validate_allowed_hosts_list(Tail, [Res | Acc]);
                true -> {error, "Repetitions are not allowed"}
            end
    end.

parse_allowed_host(<<"*">>) -> any;
parse_allowed_host(<<>>) -> {error, "empty string not supported"};
parse_allowed_host(BinStr) when is_binary(BinStr) ->
    ErrorMsg = fun (Msg) ->
                   lists:flatten(io_lib:format("\"~s\" - ~s", [BinStr, Msg]))
               end,
    TrimmedBinStr = string:trim(BinStr),
    case inet:parse_address(binary_to_list(TrimmedBinStr)) of
        {ok, IP} -> {ip, IP};
        _ ->
            case string:split(TrimmedBinStr, "/") of
                [TrimmedBinStr] ->
                    DomainLabels = string:split(string:lowercase(TrimmedBinStr),
                                                ".", all),
                    case DomainLabels of
                        [_] ->
                            {error, ErrorMsg("FQDN must contain more than "
                                             "one label")};
                        [HeadLabel | Tail] ->
                            Checks =
                                [?cut(check_fqdn_label(HeadLabel, 1))] ++
                                %% We only support wildcards in the left-most
                                %% label, see RFC6125 section-6.4.3
                                [?cut(check_fqdn_label(L, 0)) || L <- Tail] ++
                                [fun () ->
                                     try binary_to_integer(lists:last(Tail)) of
                                         _ ->
                                            {error, "highest-level label in "
                                                    "FQDN can't be entirely "
                                                    "numeric"}
                                     catch
                                          _:_ -> ok
                                     end
                                 end],
                            case functools:sequence_(Checks) of
                                ok -> {fqdn, DomainLabels};
                                {error, Msg} -> {error, ErrorMsg(Msg)}
                            end
                    end;
                [IPBin, BitSuffixBin] ->
                    case inet:parse_address(binary_to_list(IPBin)) of
                        {ok, IP} ->
                            IsIPv6 = misc:is_raw_ipv6(binary_to_list(IPBin)),
                            try binary_to_integer(BitSuffixBin) of
                                N when N >= 0, N =< 128, IsIPv6  ->
                                    {cidr, IP, N};
                                N when N >= 0, N =< 32 ->
                                    {cidr, IP, N};
                                _ ->
                                    {error, ErrorMsg("invalid number of bits "
                                                     "in CIDR")}
                            catch
                                _:_ ->
                                    {error, ErrorMsg("not integer number of "
                                                     "bits in CIDR")}
                            end;
                        _ ->
                            {error, ErrorMsg("invalid IP in CIDR")}
                    end
            end
    end.

-ifdef(TEST).
parse_allowed_host_test() ->
    ?assertEqual(any, parse_allowed_host(<<"*">>)),
    ?assertEqual({ip,{127,0,0,1}},
                 parse_allowed_host(<<"127.0.0.1">>)),
    ?assertEqual({cidr,{198,51,100,0},22},
                 parse_allowed_host(<<"198.51.100.0/22">>)),
    ?assertEqual({cidr,{8193,18528,18528,0,0,0,0,34952},32},
                 parse_allowed_host(<<"2001:4860:4860::8888/32">>)),
    ?assertEqual({ip,{8193,18528,18528,0,0,0,0,34952}},
                 parse_allowed_host(<<"2001:4860:4860::8888">>)),
    ?assertEqual({cidr,{8193,18528,18528,0,0,0,0,34952},128},
                 parse_allowed_host(<<"2001:4860:4860::8888/128">>)),
    ?assertEqual({fqdn,[<<"example">>,<<"com">>]},
                 parse_allowed_host(<<"example.com">>)),
    ?assertEqual({fqdn,[<<"*">>,<<"example">>,<<"com">>]},
                 parse_allowed_host(<<"*.example.com">>)),
    ?assertEqual({fqdn,[<<"test*test">>,<<"example">>,<<"com">>]},
                 parse_allowed_host(<<"test*test.example.com">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<>>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"127.0.0.257">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"127.0.0.257/23">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"/">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"/foo">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"foo/">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"//f/oo//">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"198.51.100.0/33">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"2001:4860:4860::888/129">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"2001:4860:4860:::888">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"test">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"test*test*.example.com">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"test.*.example.com">>)),
    ?assertMatch({error, _}, parse_allowed_host(<<"*.*.example.com">>)).
-endif.

check_fqdn_label(_, Wildcards) when Wildcards < 0 ->
    {error, "Too many wildcard characters, "
            "or wildcard characters in wrong places"};
check_fqdn_label(<<"xn--", _/binary>>, _) ->
    ok;
check_fqdn_label(Label, Wildcards) when is_binary(Label) ->
    case string:split(Label, "*") of
        [_] -> ok;
        [_, Left] -> check_fqdn_label(Left, Wildcards - 1)
    end.

get_cluster_encryption(Level) ->
    SupportedLevels = ["control", "all", "strict"],
    IsCEncryptEnabled = misc:is_cluster_encryption_fully_enabled(),
    ValidLevel = lists:member(Level, SupportedLevels),
    IsMandatory = (ns_ssl_services_setup:client_cert_auth_state() =:=
                       "mandatory"),
    Is76 = cluster_compat_mode:is_cluster_76(),
    IsStrictPossibleUnencDist = misc:cluster_has_external_unencrpted_dist(),
    if
        not IsCEncryptEnabled  ->
            M = "Can't set cluster encryption level when cluster encryption "
                "is disabled.",
            {error, M};
        not ValidLevel ->
            M = io_lib:format("Cluster encryption level must be one of ~p",
                              [SupportedLevels]),
            {error, lists:flatten(M)};
        IsMandatory andalso (Level =:= "all" orelse Level =:= "strict") andalso
        not Is76 ->
            M = "Can't set cluster encryption level to '" ++ Level ++
                "' when client certificate authentication state is set "
                "to 'mandatory'.",
            {error, M};
        Level =:= "strict" andalso IsStrictPossibleUnencDist ->
            M = "Can't set cluster encryption level to 'strict' when "
                "unencrypted distributions have not yet been torn down. Re-run "
                "node-to-node-encryption setup to disable unencrypted "
                "distibutions.",
            {error, M};
        true ->
            LevelAtom = list_to_atom(Level),
            N2NClientCert = misc:is_n2n_client_cert_verification_enabled(
                              ns_config:latest()),
            case menelaus_web_cert:validate_client_cert_CAs(
                   LevelAtom,
                   ns_ssl_services_setup:client_cert_auth_state(),
                   N2NClientCert) of
                ok -> {ok, LevelAtom};
                {error, BinMsg} -> {error, binary_to_list(BinMsg)}
            end
    end.

services_with_security_settings() ->
    [kv, fts, index, eventing, n1ql, cbas, backup, ns_server, xdcr].

is_allowed_on_cluster([password_hash_alg]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([allow_hash_migration_during_auth]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([scram_sha1_enabled]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([scram_sha256_enabled]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([scram_sha512_enabled]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([oauthbearer_enabled]) ->
    cluster_compat_mode:is_cluster_79() andalso
        cluster_compat_mode:is_developer_preview();
is_allowed_on_cluster([argon2id_time]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([argon2id_mem]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([pbkdf2_sha512_iterations]) ->
    cluster_compat_mode:is_cluster_76();
is_allowed_on_cluster([{serverless, bucket_weight_limit}]) ->
    bucket_placer:is_enabled();
is_allowed_on_cluster([{serverless, tenant_limit}]) ->
    bucket_placer:is_enabled();
is_allowed_on_cluster([{serverless, storage_limit, _}]) ->
    config_profile:get_bool(enable_storage_limits);
is_allowed_on_cluster([{serverless, throttle_limit, _}]) ->
    config_profile:get_bool(enable_throttle_limits);
is_allowed_on_cluster([{serverless, throttle_capacity, _}]) ->
    config_profile:get_bool(enable_throttle_limits);
is_allowed_on_cluster([{node, _, {serverless, throttle_capacity, _}}]) ->
    config_profile:get_bool(enable_throttle_limits);
is_allowed_on_cluster([resource_promql_override | _]) ->
    config_profile:get_bool({resource_management, configure_promql});
is_allowed_on_cluster(_) ->
    true.

is_allowed_setting(OpType, Req, K) ->
    functools:sequence_(
      [fun () ->
           case cluster_compat_mode:is_enterprise() orelse
                not ee_only_settings(K) of
               true -> ok;
               false -> {error, <<"not supported in community edition">>}
           end
       end,
       fun () ->
           case is_allowed_on_cluster(K) of
               true -> ok;
               false ->
                   {error, <<"not supported or is not available until entire "
                             "cluster is upgraded">>}
           end
       end,
       fun () ->
           case (OpType =/= get) andalso localhost_only_settings(K) of
               true ->
                   try menelaus_util:ensure_local(Req) of
                       ok -> ok
                   catch
                       throw:{web_exception, _, _, _} ->
                           {error, <<"can be modified from localhost only for "
                                     "security reasons">>}
                   end;
               false ->
                   ok
           end
       end]).

localhost_only_settings([allow_non_local_ca_upload]) -> true;
localhost_only_settings([allow_http_node_addition]) -> true;
localhost_only_settings([allowed_hosts]) -> true;
localhost_only_settings(_) -> false.

ee_only_settings([ssl_minimum_protocol]) -> true;
ee_only_settings([internal_ssl_minimum_protocol]) -> true;
ee_only_settings([cipher_suites]) -> true;
ee_only_settings([honor_cipher_order]) -> true;
ee_only_settings([magma_min_memory_quota]) -> true;
ee_only_settings([{security_settings, _} | _]) -> true;
ee_only_settings([allow_non_local_ca_upload]) -> true;
ee_only_settings([secure_headers]) -> true;
ee_only_settings(_) -> false.

get_storage_limit_attributes() ->
    [{dataStorageLimit, {serverless, storage_limit, kv},
      ?DEFAULT_KV_STORAGE_LIMIT, ?MIN_KV_STORAGE_LIMIT,
      ?MAX_KV_STORAGE_LIMIT},
     {indexStorageLimit, {serverless, storage_limit, index},
      ?DEFAULT_INDEX_STORAGE_LIMIT, ?MIN_INDEX_STORAGE_LIMIT,
      ?MAX_INDEX_STORAGE_LIMIT},
     {searchStorageLimit, {serverless, storage_limit, fts},
      ?DEFAULT_FTS_STORAGE_LIMIT, ?MIN_FTS_STORAGE_LIMIT,
      ?MAX_FTS_STORAGE_LIMIT}].

get_throttle_limit_attributes() ->
    [{dataThrottleLimit,
      {serverless, throttle_limit, kv},
      ?DEFAULT_KV_THROTTLE_LIMIT, ?MIN_THROTTLE_LIMIT, ?MAX_THROTTLE_LIMIT},
     {indexThrottleLimit,
      {serverless, throttle_limit, index},
      ?DEFAULT_INDEX_THROTTLE_LIMIT, ?MIN_THROTTLE_LIMIT, ?MAX_THROTTLE_LIMIT},
     {searchThrottleLimit,
      {serverless, throttle_limit, fts},
      ?DEFAULT_FTS_THROTTLE_LIMIT, ?MIN_THROTTLE_LIMIT, ?MAX_THROTTLE_LIMIT},
     {queryThrottleLimit,
      {serverless, throttle_limit, n1ql},
      ?DEFAULT_N1QL_THROTTLE_LIMIT, ?MIN_THROTTLE_LIMIT, ?MAX_THROTTLE_LIMIT}].

get_throttle_capacity_attributes() ->
     [{dataNodeCapacity,
      {serverless, throttle_capacity, kv},
      ?DEFAULT_KV_THROTTLE_CAPACITY, ?MIN_THROTTLE_LIMIT,
      ?MAX_THROTTLE_LIMIT},
     {indexNodeCapacity,
      {serverless, throttle_capacity, index},
      ?DEFAULT_INDEX_THROTTLE_CAPACITY, ?MIN_THROTTLE_LIMIT,
      ?MAX_THROTTLE_LIMIT},
     {searchNodeCapacity,
      {serverless, throttle_capacity, fts},
      ?DEFAULT_FTS_THROTTLE_CAPACITY, ?MIN_THROTTLE_LIMIT,
      ?MAX_THROTTLE_LIMIT},
     {queryNodeCapacity,
      {serverless, throttle_capacity, n1ql},
      ?DEFAULT_N1QL_THROTTLE_CAPACITY, ?MIN_THROTTLE_LIMIT,
      ?MAX_THROTTLE_LIMIT}].

conf(security) ->
    [{disable_ui_over_http, disableUIOverHttp,
      ?DISABLE_UI_OVER_HTTP_DEFAULT, fun get_bool/1},
     {disable_ui_over_https, disableUIOverHttps,
      ?DISABLE_UI_OVER_HTTPS_DEFAULT, fun get_bool/1},
     {disable_www_authenticate, disableWWWAuthenticate, false, fun get_bool/1},
     {secure_headers, responseHeaders, [], fun get_secure_headers/1},
     {ui_session_timeout, uiSessionTimeout, undefined,
      get_number(60, 1000000, undefined)},
     {ssl_minimum_protocol, tlsMinVersion,
      ns_ssl_services_setup:ssl_minimum_protocol([]), get_tls_version(_)},
     {cipher_suites, cipherSuites,
      ns_ssl_services_setup:configured_ciphers_names(undefined, []),
      fun get_cipher_suites/1},
     {honor_cipher_order, honorCipherOrder,
      ns_ssl_services_setup:honor_cipher_order(undefined, []), fun get_bool/1},
     {cluster_encryption_level, clusterEncryptionLevel, control,
      fun get_cluster_encryption/1},
     {allow_non_local_ca_upload, allowNonLocalCACertUpload, false,
      fun get_bool/1},
     {allowed_hosts, allowedHosts, [<<"*">>], fun get_allowed_hosts/1},
     {password_hash_alg, passwordHashAlg, ?DEFAULT_PWHASH, fun get_pwhash/1},
     {allow_hash_migration_during_auth, allowHashMigrationDuringAuth,
      menelaus_users:allow_hash_migration_during_auth_default(),
      fun get_bool/1},
     {scram_sha1_enabled, scramSha1Enabled, true, fun get_bool/1},
     {scram_sha256_enabled, scramSha256Enabled, true, fun get_bool/1},
     {scram_sha512_enabled, scramSha512Enabled, true, fun get_bool/1},
     {oauthbearer_enabled, oauthBearerEnabled, false, fun get_bool/1},
     {argon2id_time, argon2idTime, ?DEFAULT_ARG2ID_TIME,
      get_number(?ARGON_TIME_MIN, ?ARGON_TIME_MAX)},
     {argon2id_mem, argon2idMem, ?DEFAULT_ARG2ID_MEM,
      get_number(?ARGON_MEM_MIN, ?ARGON_MEM_MAX)},
     {pbkdf2_sha512_iterations, pbkdf2HmacSha512Iterations,
      ?DEFAULT_PBKDF2_ITER, get_number(?PBKDF2_ITER_MIN, ?PBKDF2_ITER_MAX)},
     {memcached_password_hash_iterations, scramShaIterations,
      ?DEFAULT_SCRAM_ITER, get_number(?PBKDF2_ITER_MIN, ?PBKDF2_ITER_MAX)},
     {?INT_CREDS_ROTATION_INT_KEY,
      intCredsRotationInterval,
      ?INT_CREDS_ROTATION_INT_DEFAULT,
      fun parse_int_creds_rotation_int/1},
     {automatically_encrypt_pkeys, autoEncryptPKeys, true, fun get_bool/1},
     {validate_node_cert_san, validateNodeCertSan, true, fun get_bool/1}] ++
        [{{security_settings, S}, ns_cluster_membership:json_service_name(S),
          [{cipher_suites, cipherSuites, undefined, fun get_cipher_suites/1},
           {ssl_minimum_protocol, tlsMinVersion, undefined, get_tls_version(_)},
           {honor_cipher_order, honorCipherOrder, undefined, fun get_bool/1},
           {supported_ciphers, supportedCipherSuites, ciphers:supported(S),
            fun read_only/1}]} || S <- services_with_security_settings()];
conf(internal) ->
    [{index_aware_rebalance_disabled, indexAwareRebalanceDisabled, false,
      fun get_bool/1},
     {rebalance_index_waiting_disabled, rebalanceIndexWaitingDisabled, false,
      fun get_bool/1},
     {index_pausing_disabled, rebalanceIndexPausingDisabled, false,
      fun get_bool/1},
     {rebalance_ignore_view_compactions, rebalanceIgnoreViewCompactions, false,
      fun get_bool/1},
     {internal_ssl_minimum_protocol, internalTlsMinVersion,
      ns_ssl_services_setup:internal_ssl_minimum_protocol(), get_tls_version(_)},
     {rebalance_moves_per_node, rebalanceMovesPerNode, 4, get_number(1, 1024)},
     {rebalance_moves_before_compaction, rebalanceMovesBeforeCompaction, 64,
      get_number(1, 1024)},
     {{couchdb, max_parallel_indexers}, maxParallelIndexers, <<>>,
      get_number(1, 1024)},
     {{couchdb, max_parallel_replica_indexers}, maxParallelReplicaIndexers,
      <<>>, get_number(1, 1024)},
     {max_bucket_count, maxBucketCount, ns_bucket:get_max_buckets_supported(),
      get_number(1, 8192)},
     {magma_min_memory_quota, magmaMinMemoryQuota, 100,
      get_number(100, 1024, 100)},
     {event_logs_limit, eventLogsLimit, 10000,
      get_number(3000, 20000, 10000)},
     {gotraceback, gotraceback, <<"single">>, fun get_string/1},
     {failover_bulk_buckets_janitor_factor, failoverBulkJanitorFactor,
      1, get_number(1, ?MAX_BUCKETS_SUPPORTED)},
     {{cert, use_sha1}, certUseSha1, false, fun get_bool/1},
     {{client_cert, not_after_duration_s}, clientCertNotAfterDurationSeconds,
      ?INTERNAL_CERT_MAX_NOT_AFTER_DURATION,
      get_number(?INTERNAL_CERT_MIN_NOT_AFTER_DURATION,
                 ?INTERNAL_CERT_MAX_NOT_AFTER_DURATION)},
     {allow_http_node_addition, httpNodeAddition, false, fun get_bool/1},
     {resource_promql_override,
      'resourcePromQLOverride',
      %% The values here are directly sent in queries to prometheus, so they
      %% must be valid PromQL. To maintain compatibility with the default query,
      %% specific labels are required, as noted below:
      [
       %% Resident ratio query - requires label "bucket", scaled for 0-100%
       {kv_resident_ratio, dataResidentRatio, ?KvResidentRatioQuery,
        fun get_string/1},
       %% Data size in TB query - requires label "bucket"
       %% This is for the data growth guard rail
       {kv_data_size_tb, dataSizePerNodeTB, ?KvDataSizeTBQuery,
        fun get_string/1},
       %% Data size in bytes query - requires label "bucket"
       %% This is for resident ratio calculation to check rebalance safeness
       {kv_data_size_raw, dataSizePerNodeBytes, ?KvDataSizeRawQuery,
        fun get_string/1},
       %% Index resident ratio query - node level percentage, scaled for 0-100%
       %% This is for notifying index resident ratio issue from index growth
       {index_resident_ratio, indexResidentRatio, ?IndexResidentRatioQuery,
        fun get_string/1}
      ]
     },
     {app_telemetry_min_scrape_interval_seconds,
      appTelemetryMinScrapeIntervalSeconds,
      ?APP_TELEMETRY_MIN_SCRAPE_INTERVAL_SECONDS,
      get_number(1, infinity)},
     {app_telemetry_max_scrape_interval_seconds,
      appTelemetryMaxScrapeIntervalSeconds,
      ?APP_TELEMETRY_MAX_SCRAPE_INTERVAL_SECONDS,
      get_number(1, infinity)},
     {argon2id_time_internal, argon2idTime, ?DEFAULT_ARG2ID_TIME,
      get_number(?ARGON_TIME_MIN, ?ARGON_TIME_MAX)},
     {argon2id_mem_internal, argon2idMem, ?DEFAULT_ARG2ID_MEM,
      get_number(?ARGON_MEM_MIN, ?ARGON_MEM_MAX)},
     {pbkdf2_sha512_iterations_internal, pbkdf2HmacSha512Iterations,
      ?DEFAULT_PBKDF2_ITER, get_number(?PBKDF2_ITER_MIN, ?PBKDF2_ITER_MAX)},
     {memcached_password_hash_iterations_internal, scramShaIterations,
      ?DEFAULT_SCRAM_ITER, get_number(?PBKDF2_ITER_MIN, ?PBKDF2_ITER_MAX)},
     {use_relative_web_redirects, useRelativeWebRedirects,
      cluster_compat_mode:is_cluster_79(),
      fun get_bool/1},
     {max_docs_skip, maxDocsSkip, ?DEFAULT_MAX_DOCS_SKIP,
      get_number(?LOWEST_ALLOWED_MAX_DOCS_SKIP,
                 ?HIGHEST_ALLOWED_MAX_DOCS_SKIP)},
     {max_docs_limit, maxDocsLimit, ?DEFAULT_MAX_DOCS_LIMIT,
      get_number(?LOWEST_ALLOWED_MAX_DOCS_LIMIT,
                 ?HIGHEST_ALLOWED_MAX_DOCS_LIMIT)}] ++
        [{include_username_in_ui_cookie, includeUsernameInUICookie, false,
          fun get_bool/1}] ++
        %% This key is available in mixed-compat-mode. We don't need to worry
        %% about guarding the cross-node call with a compat-check because the
        %% erpc library will convert any of these failures to a standardized
        %% {error|throw|exit, _} return. Only 'throws' when an argument isn't
        %% correctly formatted and any other exceptions (regardless of where
        %% they take place) will be converted to a particular return type.
        case cluster_compat_mode:is_enterprise()
            andalso cgroups:supported_on_all_nodes() of
            true ->
                [{enable_cgroups, enableCgroups, false, fun get_bool/1}];
            false ->
                []
        end;


conf(developer_preview) ->
    [{developer_preview_enabled, enabled, false, fun only_true/1}];
conf(failover) ->
    [{{failover, preserve_durable_mutations}, preserveDurableMutations,
      true, fun get_bool/1}];
conf(serverless) ->
    [{{serverless, bucket_weight_limit}, bucketWeightLimit, 10000,
      get_number(1, 100000)},
     {{serverless, tenant_limit}, tenantLimit, 25, get_number(1, 10000)},
     {{serverless, max_concurrent_sample_loads}, maxConcurrentSampleLoads, 1,
      get_number(1, 10000)}] ++
        [{Key, Param, Default, get_number(Min, Max)} ||
            {Param, Key, Default, Min, Max} <- get_storage_limit_attributes()]
        ++
        [{Key, Param, Default, get_number(Min, Max)} ||
            {Param, Key, Default, Min, Max} <- get_throttle_limit_attributes()]
        ++
        [{Key, Param, Default, get_number(Min, Max)} ||
            {Param,
             Key, Default, Min, Max} <- get_throttle_capacity_attributes()];
conf(serverless_node) ->
    [{{node, node(), Key}, Param, Default, get_number(Min, Max)} ||
        {Param, Key, Default, Min, Max} <- get_throttle_capacity_attributes()].

build_kvs(Type) ->
    build_kvs(conf(Type), ns_config:get(), fun (_, _) -> true end).

build_kvs(Conf, Config, Filter) ->
    lists:filtermap(
      fun ({CK, JK, DV, _}) ->
              Val = case ns_config:search(Config, CK, DV) of
                        undefined -> DV;
                        V -> V
                    end,
              case Filter([CK], Val) of
                  true ->
                      case Val of
                          [{_K, _V} | _] ->
                              CVal = [{K, list_to_binary(V)} || {K, V} <- Val],
                              {true, {JK, {CVal}}};
                          _ ->
                              {true, {JK, Val}}
                      end;
                  false ->
                      false
              end;
          ({CK, JK, SubKeys}) when is_list(SubKeys) ->
              List = lists:filtermap(
                       fun ({SubCK, SubJK, DV, _}) ->
                               Val = ns_config:search_prop(Config, CK,
                                                           SubCK, DV),
                               case Filter([CK, SubCK], Val) of
                                   true -> {true, {SubJK, Val}};
                                   false -> false
                               end
                       end,
                       SubKeys),
              case Filter([CK], folder) of
                  true -> {true, {JK, {List}}};
                  false -> false
              end
      end, Conf).

handle_get(Type, Keys, Req) ->
    Filter = fun (_, undefined) ->
                     false;
                 ([cluster_encryption_level = K], _) ->
                     (ok == is_allowed_setting(get, Req, K)) andalso
                         misc:is_cluster_encryption_fully_enabled();
                 (K, _) ->
                     ok == is_allowed_setting(get, Req, K)
              end,
    Settings = build_kvs(conf(Type), ns_config:get(), Filter),

    Res =
        lists:foldl(
          fun (_, undefined) -> undefined;
              (_, {val, _}) -> undefined;
              (K, {props, Acc}) ->
                  try proplists:get_value(list_to_existing_atom(K), Acc) of
                      undefined -> undefined;
                      {L} when is_list(L) -> {props, L};
                      V -> {val, V}
                  catch
                      error:badarg -> undefined
                  end
          end, {props, Settings}, Keys),
    case Res of
        undefined -> reply_json(Req, <<"Not found">>, 404);
        {props, Props} -> reply_json(Req, {Props});
        {val, V} -> reply_json(Req, V)
    end.

handle_get(Type, Req) ->
    handle_get(Type, [], Req).

handle_post(Type, Req) ->
    handle_post(Type, [], Req).

jsonify_security_settings(Settings) ->
    Format = fun ({cipher_suites, deleted}) -> {cipher_suites, deleted};
                 ({cipher_suites, List}) -> {cipher_suites, {list, List}};
                 ({secure_headers, deleted}) -> {secure_headers, deleted};
                 ({secure_headers, List}) -> {secure_headers, {propset, List}};
                 (KV) -> KV
             end,
    json_builder:prepare_list([Format(S) || S <- Settings]).

event_log_security_settings_changed(OldProps, NewProps) ->
    OldPropsJSON = jsonify_security_settings(OldProps),
    NewPropsJSON = jsonify_security_settings(NewProps),
    event_log:maybe_add_log_settings_changed(
      security_cfg_changed,
      OldPropsJSON,
      NewPropsJSON, []).

maybe_log_saml_enabled_warning(false, _OldProps, _NewProps) ->
    ok;
maybe_log_saml_enabled_warning(true, OldProps, NewProps) ->
    UIDisabledOverHttp =
        ns_config:read_key_fast(
          disable_ui_over_http, ?DISABLE_UI_OVER_HTTP_DEFAULT),
    UIDisabledOverHttps =
        ns_config:read_key_fast(
          disable_ui_over_https, ?DISABLE_UI_OVER_HTTPS_DEFAULT),

    SettingsToggled =
        lists:any(
          fun (K) ->
                  proplists:get_value(K, OldProps) =/=
                      proplists:get_value(K, NewProps)
          end, [disable_ui_over_http, disable_ui_over_https]),

    UIDisabledOverHttp andalso UIDisabledOverHttps andalso SettingsToggled
        andalso ?log_warning("UI disabled while SAML is enabled.").

handle_post(Type, Keys, Req) ->
    menelaus_util:survive_web_server_restart(
      fun () ->
              OriginalCfg = conf(Type),
              case parse_post_data(OriginalCfg, Keys,
                                   mochiweb_request:recv_body(Req),
                                   is_allowed_setting(post, Req, _)) of
                  {ok, ToSet} ->
                      handle_post_with_parsed_data(Type, ToSet, Req);
                  {error, Errors} ->
                      reply_json(Req, {[{errors, Errors}]}, 400)
              end
      end).

handle_post_with_parsed_data(Type, ToSet, Req) ->
      %% Validate all keys together because keys may depend on each other.
      %% They also may depend on values in ns_config.
      %% It would be easier to do it inside of transaction,
      %% but validation can be slow, and it is bad to run slow code inside of
      %% a transaction. For that reason, we validate keys outside of transaction
      %% but then (in transaction) we check that ns_config values
      %% that we used during validation (ValuesToCheckInTxn)
      %% have not changed. If those values change, user should retry.
      %% Note that by doing so we will rerun only in case when very
      %% specific keys change. While if we run the validation inside of
      %% transaction, we will rerun in case of any ns_config key change.
      case validate_all_keys(Type, ToSet, ns_config:latest()) of
          {ok, ValuesToCheckInTxn} ->
              Cfg = conf(Type),
              case ns_config:run_txn(
                     ?cut(set_keys_in_txn(_1, _2, ToSet, Cfg,
                                          ValuesToCheckInTxn))) of
                  {commit, _, {OldProps, NewProps}} ->
                      case Type of
                          security ->
                              NewPropsJSON =
                                  jsonify_security_settings(NewProps),
                              ns_audit:settings(Req, security,
                                                {json, NewPropsJSON}),
                              event_log_security_settings_changed(
                                OldProps, NewProps),
                              maybe_log_saml_enabled_warning(
                                menelaus_web_saml:is_enabled(),
                                OldProps, NewProps);
                          _ ->
                              ns_audit:settings(Req, Type, NewProps)
                      end,
                      reply_json(Req, []);
                  retry_needed ->
                      Msg = <<"Temporary error occurred. "
                              "Please try again later.">>,
                      menelaus_util:reply_json(Req, Msg, 503)
              end;
          {error, Errors} ->
              reply_json(Req, {[{errors, Errors}]}, 400)
      end.

validate_all_keys(security, ToSet, Config) ->
    ExtractVal = fun (Key, Default) ->
                     case proplists:lookup([Key], ToSet) of
                         {_, V} ->
                             %% Using the value that is about to be set
                             {V, []};
                         none ->
                             %% We are not trying to set value for this key,
                             %% so extracting the value from ns_config, and
                             %% remember these values that we used from
                             %% ns_config
                             CfgValue = ns_config:search(Config, Key),
                             Val = case CfgValue of
                                       {value, V} -> V;
                                       false -> Default
                                   end,
                             {Val, [{Key, CfgValue}]}
                     end
                 end,
    validate_argon2id_params(ToSet, ExtractVal);

validate_all_keys(_Type, _ToSet, _Config) ->
    {ok, []}.

validate_argon2id_params(ToSet, ExtractVal) ->
    case proplists:is_defined([argon2id_time], ToSet) orelse
         proplists:is_defined([argon2id_mem], ToSet) of
        true ->
            {Time, ToCheck1} = ExtractVal(argon2id_time,
                                          ?DEFAULT_ARG2ID_TIME),
            {Mem, ToCheck2} = ExtractVal(argon2id_mem,
                                         ?DEFAULT_ARG2ID_MEM),
            {MaxExecTime, ToCheck3} = ExtractVal(argon2id_max_exec_time,
                                                 ?DEFAULT_ARG2ID_MAX_EXEC_TIME),
            {MaxProduct, ToCheck4} = ExtractVal(argon2id_max_params_product,
                                                ?DEFAULT_ARG2ID_MAX_PRODUCT),

            case try_argon2id_hash(Time, Mem, MaxExecTime, MaxProduct) of
                ok -> {ok, ToCheck1 ++ ToCheck2 ++ ToCheck3 ++ ToCheck4};
                {error, Error} -> {error, [Error]}
            end;
        false ->
            {ok, []}
    end.

try_argon2id_hash(Time, Mem, _MaxExecTime, MaxProduct)
                                                when Time * Mem > MaxProduct ->
    Msg = io_lib:format("The product of argon2id time and memory parameters "
                        "must not exceed ~b", [MaxProduct]),
    {error, iolist_to_binary(Msg)};
try_argon2id_hash(Time, Mem, MaxExecTime, _MaxProduct) ->
    ?log_debug("Testing argon2id hash with parameters: Time=~p, Mem=~p",
               [Time, Mem]),
    Res = async:run_with_timeout(
            fun () ->
                Str = "abcdefghijk",
                Salt = crypto:strong_rand_bytes(enacl:pwhash_SALTBYTES()),
                try
                    {ok, enacl:pwhash(Str, Salt, Time, Mem, argon2id13)}
                catch
                    error:badarg -> {error, badarg}
                end
            end, MaxExecTime),
    case Res of
        {ok, {ok, _}} ->
            ?log_debug("Test successful"),
            ok;
        {ok, {error, badarg}} ->
            ?log_error("Test failed (badarg)"),
            {error, <<"Invalid argon2id hash parameters">>};
        {error, timeout} ->
            ?log_error("Test failed (took too long)"),
            Msg = io_lib:format(
                    "Argon2id test hash calculation with provided parameters "
                    "took more than ~b ms", [MaxExecTime]),
            {error, iolist_to_binary(Msg)}
    end.

set_keys_in_txn(Cfg, SetFn, ToSet, DefaultCfg, CfgValuesToCheck) ->
    UpdateKey =
        fun (Key, NewVal, CurCfg) ->
                case ns_config:search(CurCfg, Key) of
                    {value, NewVal} ->
                        {{[Key], {NewVal, NewVal}}, CurCfg};
                    {value, OldVal} ->
                        {{[Key], {OldVal, NewVal}},
                         SetFn(Key, NewVal, CurCfg)};
                    _ ->
                        {_, _, DefaultV, _} = proplists:lookup(Key, DefaultCfg),
                        {{[Key], {DefaultV, NewVal}},
                         SetFn(Key, NewVal, CurCfg)}
                end
        end,
    UpdateSubKey =
        fun (Key, SubKey, NewVal, CurCfg) ->
                CurProps = ns_config:search(CurCfg, Key, []),
                NewProps = misc:update_proplist(CurProps, [{SubKey, NewVal}]),
                case proplists:lookup(SubKey, CurProps) of
                    {SubKey, NewVal} ->
                        {{[Key, SubKey], {NewVal, NewVal}}, CurCfg};
                    {SubKey, OldVal} ->
                        {{[Key, SubKey], {OldVal, NewVal}},
                         SetFn(Key, NewProps, CurCfg)};
                    _ ->
                        {_, _, DefaultProps} =
                            proplists:lookup(Key, DefaultCfg),
                        {_, _, DefaultV, _} =
                            proplists:lookup(SubKey, DefaultProps),
                        {{[Key, SubKey], {DefaultV, NewVal}},
                         SetFn(Key, NewProps, CurCfg)}
                end
        end,
    {Changes, NewCfg} =
        lists:mapfoldl(
          fun ({[K], V}, CfgAcc) -> UpdateKey(K, V, CfgAcc);
              ({[K, SubK], V}, CfgAcc) -> UpdateSubKey(K, SubK, V, CfgAcc)
          end, Cfg, ToSet),

    SomeCfgValuesChanged = lists:any(fun ({K, V}) ->
                                         V /= ns_config:search(Cfg, K)
                                     end, CfgValuesToCheck),

    case SomeCfgValuesChanged of
        true ->
            {abort, retry_needed};
        false ->
            {commit, NewCfg, rearrange_changes(Changes)}
    end.

rearrange_changes(Changes) ->
    RealChanges = lists:filter(fun ({_, {V, V}}) -> false;
                                   ({_, {_, _}}) -> true
                               end, Changes),
    OldValues = [{K, V} || {K, {V, _}} <- RealChanges],
    NewValues = [{K, V} || {K, {_, V}} <- RealChanges],
    Rearrage = fun (L) ->
                      L2 = misc:groupby_map(
                             fun ({[K], V}) -> {{single, K}, V};
                                 ({[K1, K2], V}) -> {{multi, K1}, {K2, V}}
                             end, L),
                      lists:map(fun ({{single, K}, [V]}) -> {K, V};
                                    ({{multi, K}, Props}) -> {K, {Props}}
                                end, L2)
               end,
    {Rearrage(OldValues), Rearrage(NewValues)}.

-ifdef(TEST).

rearrange_changes_test() ->
                         %%  Key      OldVal, NewVal
    Res = rearrange_changes([{[a],    {    1,      1}}, %% No change, top key
                             {[a, b], {    1,      1}}, %% No change, subkey
                             {[c],    {    1,      2}}, %% Change, top key
                             {[d, e], {    3,      4}}, %% Change, subkey
                             {[d, f], {    5,      6}}]),
    ?assertEqual({[{d, {[{e, 3}, {f, 5}]}}, {c, 1}],
                  [{d, {[{e, 4}, {f, 6}]}}, {c, 2}]}, Res).

set_keys_in_txn_test() ->
    SetFn = fun (K, V, [Cfg]) -> [lists:keystore(K, 1, Cfg, {K, V})] end,
    ?assertEqual({commit, cfg, {[], []}},
                 set_keys_in_txn(cfg, SetFn, [], [], [])),
    CfgBefore = [[{k1, 1},
                  {k2, 0},
                  {k4, [{k5, 5}, {k99, 99}, {k6, 0}]}]],
    ToSet = [{[k1], 1}, % No change
             {[k2], 2}, % Change
             {[k3], 3}, % New key
             {[k4, k5], 5},  % Existing subkey, no change
             {[k4, k6], 6},  % Existing subkey, change
             {[k7, k8], 8},  % New key and new subkey
             {[k7, k9], 9}], % Existing key, new subkey
    Defaults = [{k1, 'K1', k1_default, undefined},
                {k2, 'K2', 2, undefined},
                {k3, 'K3', k3_default, undefined},
                {k4, 'K4', [{k5, 'K5', k5_default, undefined},
                            {k6, 'K6', 6, undefined}]},
                {k7, 'K7', [{k8, 'K8', k8_default, undefined},
                            {k9, 'K9', 9, undefined}]}],
    CfgAfter = [[{k1, 1},
                 {k2, 2},
                 {k4, [{k6, 6}, {k5, 5}, {k99, 99}]},
                 {k3, 3},
                 {k7, [{k9, 9}, {k8, 8}]}]],
    ChangedOldValues = [{k4, {[{k6, 0}]}},
                        {k7, {[{k8, k8_default}]}},
                        {k2, 0},
                        {k3, k3_default}],
    ChangedNewValues = [{k4, {[{k6, 6}]}},
                        {k7, {[{k8, 8}]}},
                        {k2, 2},
                        {k3, 3}],

    ?assertEqual({commit, CfgAfter, {ChangedOldValues, ChangedNewValues}},
                 set_keys_in_txn(CfgBefore, SetFn, ToSet, Defaults, [])),

    %% Nothing changed in config between validation and txn:
    ValuesToCheck1 = [{k1, {value, 1}}, {new_key, false}],
    ?assertEqual({commit, CfgAfter, {ChangedOldValues, ChangedNewValues}},
                 set_keys_in_txn(CfgBefore, SetFn, ToSet, Defaults,
                                 ValuesToCheck1)),
    %% Some key changed value:
    ToCheck2 = [{k1, {value, 2}}, {new_key, false}],
    ?assertEqual({abort, retry_needed},
                 set_keys_in_txn(CfgBefore, SetFn, ToSet, Defaults, ToCheck2)),
    %% Some key was undefined, now it is defined:
    ToCheck3 = [{k1, {value, 2}}, {new_key, {value, 1}}],
    ?assertEqual({abort, retry_needed},
                 set_keys_in_txn(CfgBefore, SetFn, ToSet, Defaults, ToCheck3)),
    %% Some key was defined, now it is undefined
    ToCheck4 = [{k1, false}, {new_key, false}],
    ?assertEqual({abort, retry_needed},
                 set_keys_in_txn(CfgBefore, SetFn, ToSet, Defaults, ToCheck4)).

validate_all_keys_test() ->
    ?assertEqual({ok, []},
                 validate_all_keys(security, [{[unknown], 1}], [[]])),
    ?assertEqual({ok, [{argon2id_mem, false},
                       {argon2id_max_exec_time, false},
                       {argon2id_max_params_product, false}]},
                 validate_all_keys(security,
                                   [{[argon2id_time], 1}],
                                   [[]])),
    ?assertEqual({ok, [{argon2id_time, false},
                       {argon2id_max_exec_time, false},
                       {argon2id_max_params_product, false}]},
                 validate_all_keys(security,
                                   [{[argon2id_mem], 8192}],
                                   [[]])),
    ?assertEqual({ok, [{argon2id_max_exec_time, false},
                       {argon2id_max_params_product, false}]},
                 validate_all_keys(security,
                                   [{[argon2id_mem], 8192},
                                    {[argon2id_time], 1}],
                                   [[]])),
    ?assertEqual({ok, [{argon2id_mem, {value, 8192}},
                       {argon2id_max_exec_time, false},
                       {argon2id_max_params_product, false}]},
                 validate_all_keys(security,
                                   [{[argon2id_time], 1}],
                                   [[{argon2id_time, 2},
                                     {argon2id_mem, 8192}]])),
    ?assertEqual({ok, [{argon2id_time, {value, 1}},
                       {argon2id_max_exec_time, false},
                       {argon2id_max_params_product, false}]},
                 validate_all_keys(security,
                                   [{[argon2id_mem], 8192}],
                                   [[{argon2id_time, 1},
                                     {argon2id_mem, 8193}]])),
    ?assertEqual({ok, [{argon2id_max_exec_time, false},
                       {argon2id_max_params_product, false}]},
                 validate_all_keys(security,
                                   [{[argon2id_mem], 8192},
                                    {[argon2id_time], 1}],
                                   [[{argon2id_time, 2},
                                     {argon2id_mem, 8193}]])),
    ?assertEqual({error, [<<"The product of argon2id time and memory "
                            "parameters must not exceed 16385">>]},
                 validate_all_keys(security,
                                   [{[argon2id_mem], 8193}],
                                   [[{argon2id_time, 2},
                                     {argon2id_mem, 8192},
                                     {argon2id_max_params_product,
                                      8193 * 2 - 1}]])),
    ?assertEqual({error, [<<"The product of argon2id time and memory "
                            "parameters must not exceed 16385">>]},
                 validate_all_keys(security,
                                   [{[argon2id_time], 2}],
                                   [[{argon2id_time, 1},
                                     {argon2id_mem, 8193},
                                     {argon2id_max_params_product,
                                      8193 * 2 - 1}]])),
    ?assertEqual({error, [<<"The product of argon2id time and memory "
                            "parameters must not exceed 16385">>]},
                 validate_all_keys(security,
                                   [{[argon2id_time], 2},
                                    {[argon2id_mem], 8193}],
                                   [[{argon2id_time, 1},
                                     {argon2id_mem, 8192},
                                     {argon2id_max_params_product,
                                      8193 * 2 - 1}]])),
    ?assertEqual({error, [<<"Argon2id test hash calculation with provided "
                            "parameters took more than 0 ms">>]},
                 validate_all_keys(security,
                                   [{[argon2id_time], 100},
                                    {[argon2id_mem], 10000000}],
                                   [[{argon2id_max_exec_time, 0}]])).

-endif.

parse_post_data(Conf, Keys, Data, KeyValidator) ->
    InvertedConf = invert_conf(Conf),
    Params =
        case maps:find(Keys, InvertedConf) of
            {ok, _} -> [{"", binary_to_list(Data)}];
            error -> mochiweb_util:parse_qs(Data)
        end,

    Duplicates = proplists:get_keys(Params -- lists:ukeysort(1, Params)),

    Params2 = [{Keys ++ string:tokens(SJK, "."), SV}|| {SJK, SV} <- Params],
    Res = [parse_post_for_key(SJK, SV, InvertedConf, KeyValidator)
              || {SJK, SV} <- Params2],

    Errors = [M || {error, M} <- Res] ++
             [iolist_to_binary(io_lib:format("~s - duplicate key", [D]))
                 || D <- Duplicates],
    case Errors == [] of
        true ->
            {ok, [ToSet || {ok, ToSet} <- Res]};
        false ->
            {error, Errors}
    end.

parse_post_for_key(StrJKey, StrVal, InvertedConf, KeyValidator) ->
    case maps:find(StrJKey, InvertedConf) of
        {ok, {CK, Parser}} ->
            case KeyValidator(CK) of
                ok ->
                    case Parser(StrVal) of
                        {ok, V} ->
                            {ok, {CK, V}};
                        {error, Msg} ->
                            M = io_lib:format("~s - ~s",
                                              [string:join(StrJKey, "."), Msg]),
                            {error, iolist_to_binary(M)}
                    end;
                {error, Msg} ->
                    M = io_lib:format("~s - ~s",
                                      [string:join(StrJKey, "."), Msg]),
                    {error, iolist_to_binary(M)}
            end;
        error ->
            M = io_lib:format("Unknown key ~s", [string:join(StrJKey, ".")]),
            {error, iolist_to_binary(M)}
    end.

invert_conf(Conf) ->
    lists:foldr(
      fun ({CK, JK, _, Parser}, Acc) ->
              Acc#{[atom_to_list(JK)] => {[CK], Parser}};
          ({CK, JK, List}, Acc) when is_list(List) ->
              lists:foldl(
                fun ({SubCK, SubJK, _, Parser}, Acc2) ->
                      StrJK = [atom_to_list(JK), atom_to_list(SubJK)],
                      Acc2#{StrJK => {[CK, SubCK], Parser}}
                end, Acc, List)
      end, #{}, Conf).

find_key_to_delete(_Conf, []) ->
    {error, not_supported};
find_key_to_delete(Conf, PKeys) ->
    InvertedConf = maps:to_list(invert_conf(Conf)),
    ToDelete = [lists:sublist(CKeys, length(PKeys))
                    || {Keys, {CKeys, _}} <- InvertedConf,
                       lists:prefix(PKeys, Keys)],
    case lists:usort(ToDelete) of
        [] -> {error, not_found};
        [ConfigKeys] -> {ok, ConfigKeys}
    end.

handle_delete(Type, PKeys, Req) ->
    case find_key_to_delete(conf(Type), PKeys) of
        {ok, Keys} ->
            case is_allowed_setting(delete, Req, Keys) of
                ok ->
                    case Keys of
                        [K] ->
                            ns_config:delete(K),
                            ns_audit:settings(Req, Type, [{K, deleted}]),
                            reply_json(Req, []);
                        [K, SK] ->
                            ns_config:update_key(K, proplists:delete(SK, _)),
                            ns_audit:settings(Req, Type,
                                              [{K, {[{SK, deleted}]}}]),
                            reply_json(Req, [])
                    end;
                {error, Msg} ->
                    M = io_lib:format("~s - ~s",
                                      [string:join(PKeys, "."), Msg]),
                    reply_json(Req, {[{errors, [iolist_to_binary(M)]}]}, 400)
            end;
        {error, not_found} ->
            M = io_lib:format("Unknown key ~s", [string:join(PKeys, ".")]),
            reply_json(Req, {[{errors, [iolist_to_binary(M)]}]}, 404);
        {error, not_supported} ->
            reply_json(Req, {[{errors, [<<"Not supported">>]}]}, 400)
    end.

handle_settings_max_parallel_indexers(Req) ->
    Config = ns_config:get(),

    GlobalValue =
        case ns_config:search(Config, {couchdb, max_parallel_indexers}) of
            false ->
                null;
            {value, V} ->
                V
        end,
    ThisNodeValue =
        case ns_config:search_node(node(), Config, {couchdb, max_parallel_indexers}) of
            false ->
                null;
            {value, V2} ->
                V2
        end,

    reply_json(Req, {[{globalValue, GlobalValue},
                      {nodes, {[{node(), ThisNodeValue}]}}]}).

handle_settings_max_parallel_indexers_post(Req) ->
    Params = mochiweb_request:parse_post(Req),
    V = proplists:get_value("globalValue", Params, ""),
    case parse_validate_number(V, 1, 1024) of
        {ok, Parsed} ->
            ns_config:set({couchdb, max_parallel_indexers}, Parsed),
            handle_settings_max_parallel_indexers(Req);
        Error ->
            reply_json(
              Req, {[{'_',
                      iolist_to_binary(io_lib:format("Invalid globalValue: ~p",
                                                     [Error]))}]}, 400)
    end.

handle_settings_view_update_daemon(Req) ->
    {value, Config} = ns_config:search(set_view_update_daemon),

    UpdateInterval = proplists:get_value(update_interval, Config),
    UpdateMinChanges = proplists:get_value(update_min_changes, Config),
    ReplicaUpdateMinChanges = proplists:get_value(replica_update_min_changes, Config),

    true = (UpdateInterval =/= undefined),
    true = (UpdateMinChanges =/= undefined),
    true = (UpdateMinChanges =/= undefined),

    reply_json(Req, {[{updateInterval, UpdateInterval},
                      {updateMinChanges, UpdateMinChanges},
                      {replicaUpdateMinChanges, ReplicaUpdateMinChanges}]}).

handle_settings_view_update_daemon_post(Req) ->
    Params = mochiweb_request:parse_post(Req),

    {Props, Errors} =
        lists:foldl(
          fun ({Key, RestKey}, {AccProps, AccErrors} = Acc) ->
                  Raw = proplists:get_value(RestKey, Params),

                  case Raw of
                      undefined ->
                          Acc;
                      _ ->
                          case parse_validate_number(Raw, 0, undefined) of
                              {ok, Value} ->
                                  {[{Key, Value} | AccProps], AccErrors};
                              Error ->
                                  Msg = io_lib:format("Invalid ~s: ~p",
                                                      [RestKey, Error]),
                                  {AccProps, [{RestKey, iolist_to_binary(Msg)}]}
                          end
                  end
          end, {[], []},
          [{update_interval, "updateInterval"},
           {update_min_changes, "updateMinChanges"},
           {replica_update_min_changes, "replicaUpdateMinChanges"}]),

    case Errors of
        [] ->
            {value, CurrentProps} = ns_config:search(set_view_update_daemon),
            MergedProps = misc:update_proplist(CurrentProps, Props),
            ns_config:set(set_view_update_daemon, MergedProps),
            handle_settings_view_update_daemon(Req);
        _ ->
            reply_json(Req, {Errors}, 400)
    end.

handle_settings_web(Req) ->
    reply_json(Req, build_settings_web()).

build_settings_web() ->
    Port = menelaus_web:webconfig(port),
    User = case ns_config_auth:get_user(admin) of
               undefined ->
                   "";
               U ->
                   U
           end,
    {[{port, Port}, {username, list_to_binary(User)}]}.

%% @doc Settings to en-/disable stats sending to some remote server
handle_settings_stats(Req) ->
    reply_json(Req, {build_settings_stats()}).

build_settings_stats() ->
    Defaults =
        default_settings_stats_config(cluster_compat_mode:is_enterprise(),
                                      cluster_compat_mode:is_cluster_76()),
    [{send_stats, SendStats}] = ns_config:search_prop(
                                  ns_config:get(), settings, stats, Defaults),
    [{sendStats, SendStats}].

default_settings_stats_config(false = _IsEnterprise, true = _Is76) ->
    [{send_stats, true}];
default_settings_stats_config(_, _) ->
    [{send_stats, false}].

handle_settings_stats_post(Req) ->
    validator:handle(
      fun (Props) ->
          apply_stats_settings(Props),
          reply(Req, 200)
      end,
      Req, form, [validator:required(sendStats, _) |
                  settings_stats_validators()]).

apply_stats_settings(Props) ->
    apply_stats_settings(cluster_compat_mode:is_enterprise(),
                         cluster_compat_mode:is_cluster_76(), Props).

apply_stats_settings(false = _IsEnterprise, true = _Is76, _Props) ->
    ok;
apply_stats_settings(_, _, Props) ->
    SendStats = proplists:get_value(sendStats, Props),
    case SendStats of
        undefined -> ok;
        _ -> ns_config:set(settings, [{stats, [{send_stats, SendStats}]}])
    end.

settings_stats_validators() ->
    [validator:boolean(sendStats, _)].

%% @doc Settings to en-/disable auto-reprovision
handle_settings_auto_reprovision(Req) ->
    reply_json(Req, auto_reprovision:jsonify_cfg()).

handle_settings_auto_reprovision_post(Req) ->
    PostArgs = mochiweb_request:parse_post(Req),
    ValidateOnly = proplists:get_value("just_validate",
                                       mochiweb_request:parse_qs(Req)) =:= "1",
    Enabled = proplists:get_value("enabled", PostArgs),
    MaxNodes = proplists:get_value("maxNodes", PostArgs),

    case {ValidateOnly,
          validate_settings_auto_reprovision(Enabled, MaxNodes)} of
        {false, [true, MaxNodes2]} ->
            ok = auto_reprovision:enable(MaxNodes2),
            ns_audit:enable_auto_reprovision(Req, MaxNodes2),
            reply(Req, 200);
        {false, false} ->
            ok = auto_reprovision:disable(),
            ns_audit:disable_auto_reprovision(Req),
            reply(Req, 200);
        {false, {error, Errors}} ->
            Errors2 = [<<Msg/binary, "\n">> || {_, Msg} <- Errors],
            reply_text(Req, Errors2, 400);
        {true, {error, Errors}} ->
            reply_json(Req, {[{errors, {Errors}}]}, 400);
        %% Validation only and no errors
        {true, _}->
            reply_json(Req, {[{errors, null}]}, 200)
    end.

validate_settings_auto_reprovision(Enabled, MaxNodes) ->
    Enabled2 = case Enabled of
        "true" -> true;
        "false" -> false;
        _ -> {enabled, <<"The value of \"enabled\" must be true or false">>}
    end,
    case Enabled2 of
        true ->
            case is_valid_positive_integer(MaxNodes) of
                true ->
                    [Enabled2, list_to_integer(MaxNodes)];
                false ->
                    {error, [{maxNodes,
                              <<"The value of \"maxNodes\" must be a positive integer">>}]}
            end;
        false ->
            Enabled2;
        Error ->
            {error, [Error]}
    end.

%% @doc Resets the number of nodes that were automatically reprovisioned to zero
handle_settings_auto_reprovision_reset_count(Req) ->
    auto_reprovision:reset_count(),
    reply(Req, 200).

is_valid_port_number_or_error("SAME") -> true;
is_valid_port_number_or_error(StringPort) ->
    case (catch parse_validate_port_number(StringPort)) of
        {error, [Error]} ->
            Error;
        _ ->
            true
    end.

is_port_free("SAME") ->
    true;
is_port_free(StringPort) ->
    Port = list_to_integer(StringPort),
    Port =/= service_ports:get_port(memcached_port)
        andalso Port =/= service_ports:get_port(memcached_dedicated_port)
        andalso Port =/= service_ports:get_port(memcached_ssl_port)
        andalso Port =/= service_ports:get_port(capi_port)
        andalso Port =/= 4369 %% default epmd port
        andalso is_not_a_kernel_port(Port)
        andalso Port =/= service_ports:get_port(ssl_capi_port)
        andalso Port =/= service_ports:get_port(ssl_rest_port).

is_not_a_kernel_port(Port) ->
    Env = application:get_all_env(kernel),
    MinPort = case lists:keyfind(inet_dist_listen_min, 1, Env) of
                  false ->
                      1000000;
                  {_, P} ->
                      P
              end,
    MaxPort = case lists:keyfind(inet_dist_listen_max, 1, Env) of
                  false ->
                      0;
                  {_, P1} ->
                      P1
              end,
    Port < MinPort orelse Port > MaxPort.

%% These represent settings for a cluster.  Node settings should go
%% through the /node URIs
handle_settings_web_post(Req) ->
    validator:handle(
      fun (Props) ->
          reply_json(Req, handle_settings_web_post(Req, Props)),
          exit(normal)
      end, Req, form, settings_web_post_validators()).

settings_web_post_validators() ->
    [validator:required(port, _),
     validator:required(username, _),
     validator:required(password, _),
     validator:validate(
       fun (Port) ->
          case is_valid_port_number_or_error(Port) of
              true ->
                  case is_port_free(Port) of
                      true -> ok;
                      false -> {error, "Port is already in use"}
                  end;
              Error ->
                  {error, Error}
         end
       end, port, _),
     validate_cred(username, username, _),
     validate_cred(password, password, _)].

validate_cred(Name, Type, State) ->
    validator:validate(
      fun (Password) ->
          case menelaus_web_rbac:validate_cred(Password, Type) of
              true -> ok;
              Error -> {error, Error}
          end
      end, Name, State).

cgroup_validators() ->
    [validator:required(service, _),
     validator:required(hard, _),
     validator:required(soft, _),
     validator:one_of(service, cgroups:all_services(), _),
     validator:convert(service, fun list_to_existing_atom/1, _),
     validator:validate(fun non_negative_integer_or_max/1, hard, _),
     validator:convert(hard, fun to_integer_or_max/1, _),
     validator:validate(fun non_negative_integer_or_max/1, soft, _),
     validator:convert(soft, fun to_integer_or_max/1, _)].

to_integer_or_max("max") ->
    max;
to_integer_or_max(Str) when is_list(Str) ->
    list_to_integer(Str);
to_integer_or_max(Int) when is_integer(Int) ->
    Int.

non_negative_integer_or_max("max") ->
    ok;
non_negative_integer_or_max(String) when is_list(String) ->
    case (catch list_to_integer(String)) of
        Int when is_integer(Int) andalso Int >= 0 ->
            ok;
        _ ->
            {error,
             lists:flatten(io_lib:format(?NONNEGATIVE_INT_OR_MAX_ERR,
                                         [String]))}
    end;
non_negative_integer_or_max(Value) ->
    {error, lists:flatten(io_lib:format(?NONNEGATIVE_INT_OR_MAX_ERR, [Value]))}.

handle_delete_cgroup_override(ServiceName, Req) ->
    case cgroups:supported_and_79() of
        true ->
            ServiceStrings = [atom_to_list(X) || X <- cgroups:all_services()],
            case lists:member(ServiceName, ServiceStrings) of
                true ->
                    ServiceAtom = list_to_existing_atom(ServiceName),
                    case ns_cgroups_manager:delete_override(ServiceAtom) of
                        true ->
                            reply(Req, 200);
                        false ->
                            %% it means it didn't exist
                            reply(Req, 404)
                    end;
                false ->
                    reply(Req, 404)
            end;
        false ->
            reply(Req, 404)
    end.

handle_post_cgroup_overrides(Req) ->
    case cgroups:supported_and_79() of
        true ->
            validator:handle(
              fun (Props) ->
                      case proplists:get_value(service, Props, none) of
                          none ->
                              reply(Req, 500);
                          ActualService ->
                              Hard = proplists:get_value(hard, Props, max),
                              Soft = proplists:get_value(soft, Props, max),
                              Limits = #limits{hard=Hard, soft=Soft},
                              Overrides = ns_cgroups_manager:override_profile(
                                            ActualService, Limits),
                              case Soft > Hard of
                                  true ->
                                      reply_text(Req,
                                                 "Warning: Soft limit set "
                                                 "higher than hard limit. This "
                                                 "is 'valid' in the kernel but "
                                                 "doesn't provide any useful "
                                                 "functionality.", 200);
                                  false ->
                                      reply_json(Req, {prep_json_encoding(
                                                         Overrides)})
                              end
                      end
              end, Req, form, cgroup_validators());
        false ->
            reply(Req, 404)
    end.

handle_get_cgroup_overrides(Req) ->
    case cgroups:supported_and_79() of
        true ->
            reply_json(Req,
                       {prep_json_encoding(
                          ns_cgroups_manager:get_overrides())});
        false ->
            reply(Req, 404)
    end.

prep_json_encoding(List) ->
    lists:map(fun ({Key, #limits{hard=H, soft=S}}) ->
                      {Key, {[{hard, H}, {soft, S}]}}
              end, List).

handle_settings_web_post(Req, Args) ->
    Port = proplists:get_value(port, Args),
    U = proplists:get_value(username, Args),
    P = proplists:get_value(password, Args),
    CurPort = menelaus_web:webconfig(port),
    PortInt = case Port of
                  "SAME" -> CurPort;
                  _      -> list_to_integer(Port)
              end,

    %% In case we set the 'rest' port in config, the web-server will be
    %% restarted (via menelaus_event). Protecting ourselves so that the
    %% HTTP request at hand can be completed.
    process_flag(trap_exit, true),

    case PortInt =/= CurPort orelse
        ns_config_auth:admin_credentials_changed(U, P) of
        false -> ok; % No change.
        true ->
            menelaus_web_buckets:maybe_cleanup_old_buckets(),
            ns_config:set(rest, [{port, PortInt}]),
            ns_config_auth:set_admin_credentials(U, P),
            case ns_config:search(uuid) of
                false ->
                    Uuid = couch_uuids:random(),
                    ns_config:set(uuid, Uuid);
                _ ->
                    ok
            end,
            ns_audit:password_change(Req, {U, admin}),

            menelaus_ui_auth:reset()

            %% No need to restart right here, as our ns_config
            %% event watcher will do it later if necessary.
    end,
    {PureHostName, _} = misc:split_host_port(mochiweb_request:get_header_value("host", Req), ""),
    NewHost = misc:join_host_port(PureHostName, PortInt),
    %% TODO: detect and support https when time will come
    {[{newBaseUri, list_to_binary("http://" ++ NewHost ++ "/")}]}.

handle_reset_alerts(Req) ->
    Params = mochiweb_request:parse_qs(Req),
    Token = list_to_binary(proplists:get_value("token", Params, "")),
    reply_json(Req, menelaus_web_alerts_srv:consume_alerts(Token)).

reset_per_service_cipher_suites(Req) ->
    lists:foreach(
      fun (S) ->
              case ns_config:update_key({security_settings, S},
                                        proplists:delete(cipher_suites, _)) of
                  ok ->
                      ns_audit:settings(
                        Req, security, [{{security_settings, S},
                                         {[{cipher_suites, deleted}]}}]);
                  {throw, {config_key_not_found, _}, _} ->
                      not_found
              end
      end, services_with_security_settings()).

handle_reset_ciphers_suites(Req) ->
    menelaus_util:assert_is_enterprise(),
    ns_config:set(cipher_suites, []),
    ns_audit:settings(Req, security, [{cipher_suites, {list, []}}]),
    reset_per_service_cipher_suites(Req),
    reply_json(Req, {[]}).

get_rebalance_moves_per_node() ->
    ns_config:read_key_fast(rebalance_moves_per_node,
                            ?DEFAULT_MAX_MOVES_PER_NODE).

handle_settings_rebalance(Req) ->
    reply_json(Req,
               {[{rebalanceMovesPerNode, get_rebalance_moves_per_node()}]},
               200).

handle_settings_rebalance_post(Req) ->
    validator:handle(
      fun (Values) ->
              Num = proplists:get_value(rebalanceMovesPerNode, Values),
              ns_config:set(rebalance_moves_per_node, Num),
              reply_json(Req, {[{rebalanceMovesPerNode, Num}]}, 200)
      end, Req, form,
      [validator:required(rebalanceMovesPerNode, _),
       validator:integer(rebalanceMovesPerNode, ?MIN_OF_MAX_MOVES_PER_NODE,
                         ?MAX_OF_MAX_MOVES_PER_NODE, _),
       validator:no_duplicates(_),
       validator:unsupported(_)]).

handle_settings_data_service(Req) ->
    reply_json(Req,
               {[{minReplicasCount, ns_bucket:get_min_replicas()}]},
               200).

handle_settings_data_service_post(Req) ->
    menelaus_util:assert_is_76(),
    validator:handle(
      fun (Values) ->
              Num = proplists:get_value(minReplicasCount, Values),
              ns_config:set(min_replicas_count, Num),
              reply_json(Req, {[{minReplicasCount, Num}]}, 200)
      end, Req, form,
      [validator:required(minReplicasCount, _),
       validator:integer(minReplicasCount, ?MIN_REPLICAS_SUPPORTED,
                         ?MAX_NUM_REPLICAS, _),
       validator:no_duplicates(_),
       validator:unsupported(_)]).

parse_int_creds_rotation_int("0") ->
    %% Zero means "disabled"
    {ok, 0};
parse_int_creds_rotation_int(Str) ->
    %% We need an upper limit because there is a limit for the time param in
    %% erlang:send_after/3. It can be pretty big but for us 1 year should
    %% be enough.
    (get_number(cb_creds_rotation:extract_protection_sleep(), 31536000000))(Str).

-ifdef(TEST).
build_kvs_test() ->
    Cfg = [[{key2, value},
            {key3, [{sub_key2, value}]},
            {key4, [{key1, "value1"}, {key2, "value2"}]}]],
    Conf = [{key1, jsonKey1, default, fun (V) -> {ok, V} end},
            {key2, jsonKey2, default, fun (V) -> {ok, V} end},
            {key3, jsonKey3,
              [{sub_key1, subKey1, default, fun (V) -> {ok, V} end},
               {sub_key2, subKey2, default, fun (V) -> {ok, V} end}]},
            {key4, jsonKey4, default, fun (V) -> {ok, V} end}],
    ?assertEqual([], build_kvs([], [], fun (_, _) -> true end)),
    ?assertEqual([{jsonKey1, default}, {jsonKey2, value},
                  {jsonKey3, {[{subKey1, default}, {subKey2, value}]}},
                  {jsonKey4, {[{key1, <<"value1">>}, {key2, <<"value2">>}]}}],
                 build_kvs(Conf, Cfg, fun (_, _) -> true end)),
    ?assertEqual([{jsonKey2, value},
                  {jsonKey3, {[{subKey2, value}]}},
                  {jsonKey4, {[{key1, <<"value1">>}, {key2, <<"value2">>}]}}],
                 build_kvs(Conf, Cfg,
                           fun (_, default) -> false; (_, _) -> true end)),
    ok.

test_conf() ->
    [{ssl_minimum_protocol,tlsMinVersion,unused, get_tls_version(_)},
     {cipher_suites,cipherSuites,unused, fun get_cipher_suites/1},
     {secure_headers, responseHeaders, [], fun get_secure_headers/1},
     {honor_cipher_order,honorCipherOrder,unused, fun get_bool/1},
     {{security_settings, kv}, data,
      [{cipher_suites, cipherSuites, unused, fun get_cipher_suites/1},
       {ssl_minimum_protocol, tlsMinVersion, unused, get_tls_version(_)},
       {honor_cipher_order, honorCipherOrder, unused, fun get_bool/1},
       {supported_ciphers, supportedCipherSuites, unused, fun read_only/1}]},
     {storage_limit, storageLimit, 5, get_number(0, 15)},
     {not_allowed, notAllowed, nope,
      fun (_) -> error(should_not_be_called) end}].

parse_post_data_test() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode,
                is_cluster_76,
                fun() ->
                        true
                end),

    Conf = test_conf(),
    RH = ejson:encode({[{"Strict-Transport-Security",
                         <<"max-age=10%3Bpreload%3BincludeSubDomains">>}]}),
    ResponseHeaders = <<"responseHeaders=", RH/binary, "&">>,
    KeyValidator = fun ([not_allowed]) -> {error, <<"not allowed">>};
                       (_) -> ok
                   end,
    GetNumberErr = <<"storageLimit - The value must be between 0 and 15.">>,
    ?assertEqual({ok, []}, parse_post_data(Conf, [], <<>>, KeyValidator)),
    ?assertEqual({ok, [{[secure_headers],
                      [{"Strict-Transport-Security",
                        "max-age=10;preload;includeSubDomains"}]},
                       {[ssl_minimum_protocol], 'tlsv1.2'},
                       {[cipher_suites], []},
                       {[honor_cipher_order], true},
                       {[{security_settings, kv}, ssl_minimum_protocol],
                        'tlsv1.3'},
                       {[{security_settings, kv}, cipher_suites], []},
                       {[{security_settings, kv}, honor_cipher_order], false}]},
                 parse_post_data(Conf, [],
                                 <<ResponseHeaders/binary,
                                   "tlsMinVersion=tlsv1.2&"
                                   "cipherSuites=[]&"
                                   "honorCipherOrder=true&"
                                   "data.tlsMinVersion=tlsv1.3&"
                                   "data.cipherSuites=[]&"
                                   "data.honorCipherOrder=false">>,
                                 KeyValidator)),
    ?assertEqual({ok, [{[{security_settings, kv}, ssl_minimum_protocol],
                        'tlsv1.3'},
                       {[{security_settings, kv}, cipher_suites], []},
                       {[{security_settings, kv}, honor_cipher_order], true}]},
                 parse_post_data(Conf, ["data"],
                                 <<"tlsMinVersion=tlsv1.3&"
                                   "cipherSuites=[]&"
                                   "honorCipherOrder=true">>,
                                 KeyValidator)),
    ?assertEqual({ok, [{[{security_settings, kv}, ssl_minimum_protocol],
                        'tlsv1.3'}]},
                 parse_post_data(Conf, ["data", "tlsMinVersion"],
                                 <<"tlsv1.3">>,
                                 KeyValidator)),
    ?assertEqual({ok, [{[storage_limit], 0}]},
                  parse_post_data(Conf, [], "storageLimit=0", KeyValidator)),
    ?assertEqual({ok, [{[storage_limit], 15}]},
                  parse_post_data(Conf, [], "storageLimit=15", KeyValidator)),
    ?assertEqual({error, [GetNumberErr]},
                  parse_post_data(Conf, [], "storageLimit=-1", KeyValidator)),
    ?assertEqual({error, [GetNumberErr]},
                  parse_post_data(Conf, [], "storageLimit=16", KeyValidator)),
    ?assertEqual({error, [<<"Unknown key unknown1">>,
                          <<"Unknown key unknown2.tlsMinVersion">>,
                          <<"data.cipherSuites - Invalid format. "
                            "Expecting a list of ciphers.">>,
                          <<"Unknown key data.unkwnown3">>]},
                 parse_post_data(Conf, [],
                                 <<"unknown1=tlsv1.2&"
                                   "cipherSuites=[]&"
                                   "unknown2.tlsMinVersion=tlsv1.3&"
                                   "data.cipherSuites=bad&"
                                   "data.unkwnown3=false">>,
                                 KeyValidator)),
    ?assertEqual({error, [<<"Unknown key data.unknown1">>,
                          <<"data.cipherSuites - Invalid format. "
                            "Expecting a list of ciphers.">>,
                          <<"cipherSuites - duplicate key">>]},
                 parse_post_data(Conf, ["data"],
                                 <<"unknown1=tlsv1.2&"
                                   "cipherSuites=[]&"
                                   "cipherSuites=bad">>,
                                 KeyValidator)),
    ?assertEqual({error, [<<"responseHeaders - Invalid format. "
                            "Expecting a json.">>]},
                 parse_post_data(Conf, ["responseHeaders"],
                                 <<"bad">>, KeyValidator)),
    ?assertEqual({error, [<<"data.cipherSuites - Invalid format. "
                            "Expecting a list of ciphers.">>]},
                 parse_post_data(Conf, ["data", "cipherSuites"],
                                 <<"bad">>, KeyValidator)),
    ?assertEqual({error, [<<"Unknown key data.unknown.cipherSuites">>]},
                 parse_post_data(Conf, ["data", "unknown"],
                                 <<"cipherSuites=bad">>, KeyValidator)),
    ?assertEqual({error, [<<"notAllowed - not allowed">>]},
                 parse_post_data(Conf, [],
                                 <<"tlsMinVersion=tlsv1.2&notAllowed=1">>,
                                 KeyValidator)),
    ?assertEqual({error, [<<"tlsMinVersion - Supported TLS versions are "
                            "tlsv1.2, tlsv1.3">>]},
                 parse_post_data(Conf, [],
                                 <<"tlsMinVersion=tlsv1.1">>,
                                 KeyValidator)),
    ?assertEqual({error, [<<"data.tlsMinVersion - Supported TLS versions are "
                            "tlsv1.2, tlsv1.3">>]},
                 parse_post_data(Conf, ["data"],
                                 <<"cipherSuites=[]&"
                                   "honorCipherOrder=true&"
                                   "tlsMinVersion=tlsv1.1">>,
                                 KeyValidator)),
    meck:expect(cluster_compat_mode,
                is_cluster_76,
                fun() ->
                        false
                end),
    ?assertEqual({ok, [{[secure_headers],
                      [{"Strict-Transport-Security",
                        "max-age=10;preload;includeSubDomains"}]},
                       {[ssl_minimum_protocol], 'tlsv1'}]},
                 parse_post_data(Conf, [],
                                 <<ResponseHeaders/binary,
                                   "tlsMinVersion=tlsv1">>,
                                 KeyValidator)),
    ?assertEqual({ok, [{[{security_settings, kv}, cipher_suites], []},
                       {[{security_settings, kv}, honor_cipher_order], true},
                       {[{security_settings, kv}, ssl_minimum_protocol],
                        'tlsv1.1'}]},
                 parse_post_data(Conf, ["data"],
                                 <<"cipherSuites=[]&"
                                   "honorCipherOrder=true&"
                                   "tlsMinVersion=tlsv1.1">>,
                                 KeyValidator)),

    meck:unload(cluster_compat_mode),
    ok.

find_key_to_delete_test() ->
    Conf = test_conf(),

    ?assertEqual({ok, [cipher_suites]},
                 find_key_to_delete(Conf, ["cipherSuites"])),
    ?assertEqual({ok, [{security_settings, kv}, cipher_suites]},
                 find_key_to_delete(Conf, ["data", "cipherSuites"])),
    ?assertEqual({ok, [{security_settings, kv}]},
                 find_key_to_delete(Conf, ["data"])),
    ?assertEqual({error, not_supported},
                 find_key_to_delete(Conf, [])),
    ?assertEqual({error, not_found},
                 find_key_to_delete(Conf, ["unknown"])),
    ?assertEqual({error, not_found},
                 find_key_to_delete(Conf, ["data", "unknown"])),
    ok.

non_negative_integer_or_max_test() ->
    ?assertEqual(ok, non_negative_integer_or_max("max")),
    ?assertEqual(ok, non_negative_integer_or_max("10")),
    ?assertEqual(ok, non_negative_integer_or_max("0")),
    ?assertEqual(
       {error,
        "Value '\"-1\"' must be equal to or above 0 or the string 'max'."},
       non_negative_integer_or_max("-1")),
    ?assertEqual(
       {error,
        "Value '\"abcd\"' must be equal to or above 0 or the string 'max'."},
       non_negative_integer_or_max("abcd")).

-endif.
