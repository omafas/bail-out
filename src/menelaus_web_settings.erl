%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc handlers for settings related REST API's

-module(menelaus_web_settings).

-include("ns_common.hrl").
-include("cut.hrl").

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

         handle_reset_ciphers_suites/1,

         services_with_security_settings/0,
         settings_stats_validators/0,
         apply_stats_settings/1,
         settings_web_post_validators/0,
         validate_allowed_hosts_list/1,
         parse_allowed_host/1]).

-import(menelaus_util,
        [parse_validate_number/3,
         is_valid_positive_integer/1,
         parse_validate_port_number/1,
         reply_json/2,
         reply_json/3,
         reply_text/3,
         reply/2]).

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
    Supported = ['tlsv1.3', 'tlsv1.2', 'tlsv1.1', tlsv1],
    SupportedStr = [atom_to_list(S) || S <- Supported],
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
    IsStrictPossible = cluster_compat_mode:is_cluster_70(),

    if
        not IsCEncryptEnabled  ->
            M = "Can't set cluster encryption level when cluster encryption "
                "is disabled.",
            {error, M};
        not ValidLevel ->
            M = io_lib:format("Cluster encryption level must be one of ~p",
                              [SupportedLevels]),
            {error, lists:flatten(M)};
        IsMandatory andalso (Level =:= "all" orelse Level =:= "strict") ->
            M = "Can't set cluster encryption level to '" ++ Level ++
                "' when client certificate authentication state is set "
                "to 'mandatory'.",
            {error, M};
        Level =:= "strict" andalso not IsStrictPossible ->
            M = "Can't set cluster encryption level to 'strict' "
                "in mixed version clusters.",
            {error, M};
        true ->
            {ok, list_to_atom(Level)}
    end.

services_with_security_settings() ->
    [kv, fts, index, eventing, n1ql, cbas, backup, ns_server].

is_allowed_on_cluster([secure_headers]) ->
    cluster_compat_mode:is_cluster_70();
is_allowed_on_cluster([event_logs_limit]) ->
    cluster_compat_mode:is_cluster_71();
is_allowed_on_cluster([enforce_limits]) ->
    cluster_compat_mode:is_cluster_71();
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
               false -> {error, <<"not supported in mixed version clusters">>}
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
ee_only_settings([cipher_suites]) -> true;
ee_only_settings([honor_cipher_order]) -> true;
ee_only_settings([{security_settings, _} | _]) -> true;
ee_only_settings(_) -> false.

conf(security) ->
    [{disable_ui_over_http, disableUIOverHttp, false, fun get_bool/1},
     {disable_ui_over_https, disableUIOverHttps, false, fun get_bool/1},
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
     {allowed_hosts, allowedHosts, [<<"*">>], fun get_allowed_hosts/1}] ++
    [{{security_settings, S}, ns_cluster_membership:json_service_name(S),
      [{cipher_suites, cipherSuites, undefined, fun get_cipher_suites/1},
       {ssl_minimum_protocol, tlsMinVersion, undefined, get_tls_version(_)},
       {honor_cipher_order, honorCipherOrder, undefined, fun get_bool/1},
       {supported_ciphers, supportedCipherSuites, ciphers:supported(S),
        fun read_only/1}]} || S <- services_with_security_settings()];
conf(internal) ->
    [{enforce_limits, enforceLimits, false, fun get_bool/1},
     {index_aware_rebalance_disabled, indexAwareRebalanceDisabled, false,
      fun get_bool/1},
     {rebalance_index_waiting_disabled, rebalanceIndexWaitingDisabled, false,
      fun get_bool/1},
     {index_pausing_disabled, rebalanceIndexPausingDisabled, false,
      fun get_bool/1},
     {rebalance_ignore_view_compactions, rebalanceIgnoreViewCompactions, false,
      fun get_bool/1},
     {rebalance_moves_per_node, rebalanceMovesPerNode, 4, get_number(1, 1024)},
     {rebalance_moves_before_compaction, rebalanceMovesBeforeCompaction, 64,
      get_number(1, 1024)},
     {{couchdb, max_parallel_indexers}, maxParallelIndexers, <<>>,
      get_number(1, 1024)},
     {{couchdb, max_parallel_replica_indexers}, maxParallelReplicaIndexers,
      <<>>, get_number(1, 1024)},
     {max_bucket_count, maxBucketCount, ?MAX_BUCKETS_SUPPORTED,
      get_number(1, 8192)},
     {event_logs_limit, eventLogsLimit, 10000,
      get_number(3000, 20000, 10000)},
     {gotraceback, gotraceback, <<"single">>, fun get_string/1},
     {{auto_failover_disabled, index}, indexAutoFailoverDisabled, true,
      fun get_bool/1},
     {{cert, use_sha1}, certUseSha1, false, fun get_bool/1},
     {allow_http_node_addition, httpNodeAddition, false, fun get_bool/1}];
conf(developer_preview) ->
    [{developer_preview_enabled, enabled, false, fun only_true/1}];
conf(failover) ->
    [{{failover, preserve_durable_mutations}, preserveDurableMutations,
      true, fun get_bool/1}].

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

audit_fun(Type) ->
    list_to_atom(atom_to_list(Type) ++ "_settings").

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

handle_post(Type, Keys, Req) ->
    menelaus_util:survive_web_server_restart(
      fun () ->
              OriginalCfg = conf(Type),
              case parse_post_data(OriginalCfg, Keys,
                                   mochiweb_request:recv_body(Req),
                                   is_allowed_setting(post, Req, _)) of
                  {ok, ToSet} ->
                      case ns_config:run_txn(
                             ?cut(set_keys_in_txn(_1, _2, ToSet,
                                                  OriginalCfg))) of
                          {commit, _, {OldProps, NewProps}} ->
                              case Type of
                                  security ->
                                      NewPropsJSON =
                                          jsonify_security_settings(NewProps),
                                      ns_audit:security_settings(Req,
                                                                 NewPropsJSON),
                                      OldPropsJSON =
                                          jsonify_security_settings(OldProps),
                                      event_log:add_log(
                                        security_cfg_changed,
                                        [{old_settings, {OldPropsJSON}},
                                         {new_settings, {NewPropsJSON}}]);
                                  _ ->
                                      AuditFun = audit_fun(Type),
                                      ns_audit:AuditFun(Req, NewProps)
                              end,
                              reply_json(Req, []);
                          retry_needed ->
                              erlang:error(exceeded_retries)
                      end;
                  {error, Errors} ->
                      reply_json(Req, {struct, [{errors, Errors}]}, 400)
              end
      end).

set_keys_in_txn(Cfg, SetFn, ToSet, DefaultCfg) ->
    {NewCfg, OldProps, NewProps} =
        lists:foldl(
          fun ({[K], V}, {CfgAcc, OldPropsAcc0, NewPropsAcc0}) ->
                  case ns_config:search(CfgAcc, K) of
                      {value, V} ->
                          {CfgAcc, OldPropsAcc0, NewPropsAcc0};
                      {value, OV} ->
                          {SetFn(K, V, CfgAcc), OldPropsAcc0#{K => OV},
                           NewPropsAcc0#{K => V}};
                      _ ->
                          {_, _, DefaultV, _} = proplists:lookup(K, DefaultCfg),
                          %% Don't display values that didn't actually change
                          {NewP, OldP} = case V =:= DefaultV of
                                             true ->
                                                 {NewPropsAcc0, OldPropsAcc0};
                                             false ->
                                                 {NewPropsAcc0#{K => V},
                                                  OldPropsAcc0#{K => DefaultV}}
                                         end,
                          {SetFn(K, V, CfgAcc), OldP, NewP}
                  end;
              ({[K, SubK], V}, {CfgAcc, OldPropsAcc0, NewPropsAcc0}) ->
                  CurProps = ns_config:search(CfgAcc, K, []),
                  AppendSubKFn = fun(Var) ->
                                         fun({L}) -> {[{SubK, Var} | L]} end
                                 end,
                  case proplists:lookup(SubK, CurProps) of
                      {SubK, V} ->
                          {CfgAcc, OldPropsAcc0, NewPropsAcc0};
                      {SubK, OV} ->
                          OldPropAcc =
                              maps:update_with(K, AppendSubKFn(OV),
                                               {[{SubK, OV}]}, OldPropsAcc0),
                          NewProps = misc:update_proplist(CurProps,
                                                          [{SubK, V}]),
                          NewPropsAcc =
                              maps:update_with(K, AppendSubKFn(V),
                                               {[{SubK, V}]}, NewPropsAcc0),
                          {SetFn(K, NewProps, CfgAcc), OldPropAcc, NewPropsAcc};
                      _ ->
                          {_, _, DefaultProps} =
                              proplists:lookup(K, DefaultCfg),
                          {_, _, DefaultV, _} =
                              proplists:lookup(SubK, DefaultProps),
                          case DefaultV of
                              V ->
                                  {CfgAcc, OldPropsAcc0, NewPropsAcc0};
                              _ ->
                                  OldPropAcc =
                                      maps:update_with(K,
                                                       AppendSubKFn(DefaultV),
                                                       {[{SubK, DefaultV}]},
                                                       OldPropsAcc0),
                                  NewProps = misc:update_proplist(CurProps,
                                                                  [{SubK, V}]),
                                  NewPropsAcc =
                                      maps:update_with(K, AppendSubKFn(V),
                                                       {[{SubK, V}]},
                                                       NewPropsAcc0),
                                  {SetFn(K, NewProps, CfgAcc), OldPropAcc,
                                   NewPropsAcc}
                          end
                  end
          end, {Cfg, #{}, #{}}, ToSet),
    {commit, NewCfg, {maps:to_list(OldProps), maps:to_list(NewProps)}}.

parse_post_data(Conf, Keys, Data, KeyValidator) ->
    InvertedConf = invert_conf(Conf),
    Params =
        case maps:find(Keys, InvertedConf) of
            {ok, _} -> [{"", binary_to_list(Data)}];
            error -> mochiweb_util:parse_qs(Data)
        end,

    Params2 = [{Keys ++ string:tokens(SJK, "."), SV}|| {SJK, SV} <- Params],
    Res = [parse_post_for_key(SJK, SV, InvertedConf, KeyValidator)
              || {SJK, SV} <- Params2],

    case [M || {error, M} <- Res] of
        [] ->
            {ok, [ToSet || {ok, ToSet} <- Res]};
        Errors ->
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
                            AuditFun = audit_fun(Type),
                            ns_audit:AuditFun(Req, [{K, deleted}]),
                            reply_json(Req, []);
                        [K, SK] ->
                            ns_config:update_key(K, proplists:delete(SK, _)),
                            AuditFun = audit_fun(Type),
                            ns_audit:AuditFun(Req, [{K, {[{SK, deleted}]}}]),
                            reply_json(Req, [])
                    end;
                {error, Msg} ->
                    M = io_lib:format("~s - ~s",
                                      [string:join(PKeys, "."), Msg]),
                    reply_json(Req, {struct, [{errors, [iolist_to_binary(M)]}]},
                               400)
            end;
        {error, not_found} ->
            M = io_lib:format("Unknown key ~s", [string:join(PKeys, ".")]),
            reply_json(Req, {struct, [{errors, [iolist_to_binary(M)]}]}, 404);
        {error, not_supported} ->
            reply_json(Req, {struct, [{errors, [<<"Not supported">>]}]}, 400)
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

    reply_json(Req, {struct, [{globalValue, GlobalValue},
                              {nodes, {struct, [{node(), ThisNodeValue}]}}]}).

handle_settings_max_parallel_indexers_post(Req) ->
    Params = mochiweb_request:parse_post(Req),
    V = proplists:get_value("globalValue", Params, ""),
    case parse_validate_number(V, 1, 1024) of
        {ok, Parsed} ->
            ns_config:set({couchdb, max_parallel_indexers}, Parsed),
            handle_settings_max_parallel_indexers(Req);
        Error ->
            reply_json(
              Req, {struct, [{'_', iolist_to_binary(io_lib:format("Invalid globalValue: ~p", [Error]))}]}, 400)
    end.

handle_settings_view_update_daemon(Req) ->
    {value, Config} = ns_config:search(set_view_update_daemon),

    UpdateInterval = proplists:get_value(update_interval, Config),
    UpdateMinChanges = proplists:get_value(update_min_changes, Config),
    ReplicaUpdateMinChanges = proplists:get_value(replica_update_min_changes, Config),

    true = (UpdateInterval =/= undefined),
    true = (UpdateMinChanges =/= undefined),
    true = (UpdateMinChanges =/= undefined),

    reply_json(Req, {struct, [{updateInterval, UpdateInterval},
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
            reply_json(Req, {struct, Errors}, 400)
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
    {struct, [{port, Port},
              {username, list_to_binary(User)}]}.

%% @doc Settings to en-/disable stats sending to some remote server
handle_settings_stats(Req) ->
    reply_json(Req, {struct, build_settings_stats()}).

build_settings_stats() ->
    Defaults = default_settings_stats_config(),
    [{send_stats, SendStats}] = ns_config:search_prop(
                                  ns_config:get(), settings, stats, Defaults),
    [{sendStats, SendStats}].

default_settings_stats_config() ->
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
            reply_json(Req, {struct, [{errors, {struct, Errors}}]}, 400);
        %% Validation only and no errors
        {true, _}->
            reply_json(Req, {struct, [{errors, null}]}, 200)
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

handle_settings_web_post(Req, Args) ->
    Port = proplists:get_value(port, Args),
    U = proplists:get_value(username, Args),
    P = proplists:get_value(password, Args),
    PortInt = case Port of
                  "SAME" -> menelaus_web:webconfig(port);
                  _      -> list_to_integer(Port)
              end,

    %% In case we set the 'rest' port in config, the web-server will be
    %% restarted (via menelaus_event). Protecting ourselves so that the
    %% HTTP request at hand can be completed.
    process_flag(trap_exit, true),

    case Port =/= PortInt orelse
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
    {struct, [{newBaseUri, list_to_binary("http://" ++ NewHost ++ "/")}]}.

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
                      ns_audit:security_settings(
                        Req, [{{security_settings, S},
                               {[{cipher_suites, deleted}]}}]);
                  {throw, {config_key_not_found, _}, _} ->
                      not_found
              end
      end, services_with_security_settings()).

handle_reset_ciphers_suites(Req) ->
    menelaus_util:assert_is_enterprise(),
    ns_config:set(cipher_suites, []),
    ns_audit:security_settings(Req, [{cipher_suites, []}]),
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
    menelaus_util:assert_is_66(),
    validator:handle(
      fun (Values) ->
              Num = proplists:get_value(rebalanceMovesPerNode, Values),
              ns_config:set(rebalance_moves_per_node, Num),
              reply_json(Req, {[{rebalanceMovesPerNode, Num}]}, 200)
      end, Req, form,
      [validator:required(rebalanceMovesPerNode, _),
       validator:integer(rebalanceMovesPerNode, ?MIN_OF_MAX_MOVES_PER_NODE,
                         ?MAX_OF_MAX_MOVES_PER_NODE, _),
       validator:unsupported(_)]).

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
     {not_allowed, notAllowed, nope,
      fun (_) -> error(should_not_be_called) end}].

parse_post_data_test() ->
    Conf = test_conf(),
    RH = ejson:encode({[{"Strict-Transport-Security",
                         <<"max-age=10%3Bpreload%3BincludeSubDomains">>}]}),
    ResponseHeaders = <<"responseHeaders=", RH/binary, "&">>,
    KeyValidator = fun ([not_allowed]) -> {error, <<"not allowed">>};
                       (_) -> ok
                   end,
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
                            "Expecting a list of ciphers.">>]},
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
-endif.
