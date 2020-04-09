%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-2018 Couchbase, Inc.
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
%% @doc REST api's for handling ssl certificates

-module(menelaus_web_cert).

-include("ns_common.hrl").

-export([handle_cluster_certificate/1,
         handle_regenerate_certificate/1,
         handle_upload_cluster_ca/1,
         handle_reload_node_certificate/1,
         handle_get_node_certificate/2,
         handle_client_cert_auth_settings/1,
         handle_client_cert_auth_settings_post/1]).

-define(MAX_CLIENT_CERT_PREFIXES, ?get_param(max_prefixes, 10)).

handle_cluster_certificate(Req) ->
    menelaus_util:assert_is_enterprise(),

    case proplists:get_value("extended", mochiweb_request:parse_qs(Req)) of
        "true" ->
            handle_cluster_certificate_extended(Req);
        _ ->
            handle_cluster_certificate_simple(Req)
    end.

handle_cluster_certificate_simple(Req) ->
    Cert = case ns_server_cert:cluster_ca() of
               {GeneratedCert, _} ->
                   GeneratedCert;
               {UploadedCAProps, _, _} ->
                   proplists:get_value(pem, UploadedCAProps)
           end,
    menelaus_util:reply_ok(Req, "text/plain", Cert).

format_time(UTCSeconds) ->
    LocalTime = calendar:universal_time_to_local_time(
                  calendar:gregorian_seconds_to_datetime(UTCSeconds)),
    menelaus_util:format_server_time(LocalTime, 0).

warning_props({expires_soon, UTCSeconds}) ->
    [{message, ns_error_messages:node_certificate_warning(expires_soon)},
     {expires, format_time(UTCSeconds)}];
warning_props(Warning) ->
    [{message, ns_error_messages:node_certificate_warning(Warning)}].

translate_warning({Node, Warning}) ->
    [{node, Node} | warning_props(Warning)];
translate_warning(Warning) ->
    warning_props(Warning).

jsonify_cert_props(Props) ->
    lists:map(fun ({expires, UTCSeconds}) ->
                      {expires, format_time(UTCSeconds)};
                  ({K, V}) when is_list(V) ->
                      {K, list_to_binary(V)};
                  (Pair) ->
                      Pair
              end, Props).

handle_cluster_certificate_extended(Req) ->
    {Cert, WarningsJson} =
        case ns_server_cert:cluster_ca() of
            {GeneratedCert, _} ->
                {[{type, generated},
                  {pem, GeneratedCert}], [{translate_warning(self_signed)}]};
            {UploadedCAProps, _, _} ->
                Warnings = ns_server_cert:get_warnings(UploadedCAProps),
                {[{type, uploaded} | UploadedCAProps],
                 [{translate_warning(Pair)} || Pair <- Warnings]}
        end,
    menelaus_util:reply_json(Req, {[{cert, {jsonify_cert_props(Cert)}},
                                    {warnings, WarningsJson}]}).

handle_regenerate_certificate(Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_n2n_encryption_is_disabled(),

    ns_server_cert:generate_and_set_cert_and_pkey(),
    ns_ssl_services_setup:sync_local_cert_and_pkey_change(),
    ?log_info("Completed certificate regeneration"),
    ns_audit:regenerate_certificate(Req),
    handle_cluster_certificate_simple(Req).

reply_error(Req, Error) ->
    menelaus_util:reply_json(
      Req, {[{error, ns_error_messages:cert_validation_error_message(Error)}]}, 400).

handle_upload_cluster_ca(Req) ->
    menelaus_util:assert_is_enterprise(),
    assert_n2n_encryption_is_disabled(),

    case mochiweb_request:recv_body(Req) of
        undefined ->
            reply_error(Req, empty_cert);
        PemEncodedCA ->
            case ns_server_cert:set_cluster_ca(PemEncodedCA) of
                {ok, Props} ->
                    ns_audit:upload_cluster_ca(Req,
                                               proplists:get_value(subject, Props),
                                               proplists:get_value(expires, Props)),
                    handle_cluster_certificate_extended(Req);
                {error, Error} ->
                    reply_error(Req, Error)
            end
    end.

assert_n2n_encryption_is_disabled() ->
    case misc:is_cluster_encryption_fully_disabled() of
        true -> ok;
        false ->
            M = "Operation requires node-to-node encryption to be disabled",
            erlang:throw({web_exception, 400, M, []})
    end.

handle_reload_node_certificate(Req) ->
    menelaus_util:assert_is_enterprise(),
    Nodes = nodes(),
    case ns_server_cert:apply_certificate_chain_from_inbox() of
        {ok, Props} ->
            ns_audit:reload_node_certificate(Req,
                                             proplists:get_value(subject, Props),
                                             proplists:get_value(expires, Props)),
            ns_ssl_services_setup:sync_local_cert_and_pkey_change(),
            case netconfig_updater:ensure_tls_dist_started(Nodes) of
                ok ->
                    menelaus_util:reply(Req, 200);
                {error, ErrorMsg} ->
                    menelaus_util:reply_json(Req, ErrorMsg, 400)
            end;
        {error, Error} ->
            ?log_error("Error reloading node certificate: ~p", [Error]),
            menelaus_util:reply_json(
              Req, ns_error_messages:reload_node_certificate_error(Error), 400)
    end.

handle_get_node_certificate(NodeId, Req) ->
    menelaus_util:assert_is_enterprise(),

    case menelaus_web_node:find_node_hostname(NodeId, Req) of
        {ok, Node} ->
            case ns_server_cert:get_node_cert_info(Node) of
                [] ->
                    menelaus_util:reply_text(Req, <<"Certificate is not set up on this node">>, 404);
                Props ->
                    menelaus_util:reply_json(Req, {jsonify_cert_props(Props)})
            end;
        {error, {invalid_node, Reason}} ->
            menelaus_util:reply_text(Req, Reason, 400);
        {error, not_found} ->
            menelaus_util:reply_text(
              Req,
              <<"Node is not found, make sure the ip address/hostname matches the ip address/hostname used by Couchbase">>,
              404)
    end.

allowed_values(Key) ->
    Values = [{"state", ["enable", "disable", "mandatory"]},
              {"path", ["subject.cn", "san.uri", "san.dnsname", "san.email"]},
              {"prefix", any},
              {"delimiter", any}],
    proplists:get_value(Key, Values, none).

handle_client_cert_auth_settings(Req) ->
    Cca = ns_ssl_services_setup:client_cert_auth(),
    Out = case cluster_compat_mode:is_cluster_51() of
              true ->
                  State = list_to_binary(proplists:get_value(state, Cca)),
                  Prefixes = [begin
                                  {struct, [{list_to_binary(atom_to_list(K)), list_to_binary(V)}
                                            || {K, V} <- Triple]}
                              end || Triple <- proplists:get_value(prefixes, Cca, [])],

                  {struct, [{<<"state">>, State}, {<<"prefixes">>, Prefixes}]};
              false ->
                  {[{K, list_to_binary(V)} || {K,V} <- Cca]}
          end,
    menelaus_util:reply_json(Req, Out).

validate_client_cert_auth_param(Key, Val) ->
    Values = allowed_values(Key),
    case Values == any orelse lists:member(Val, Values) of
        true ->
            {ok, {list_to_atom(Key), Val}};
        false ->
            {error, io_lib:format("Invalid value '~s' for key '~s'", [Val, Key])}
    end.

validate_client_cert_auth_state(StateVal, Prefixes, Cfg, Errors) ->
    case validate_client_cert_auth_param("state", StateVal) of
        {ok, CfgPair} ->
            case StateVal =/= "disable" andalso Prefixes =:= [] of
                true ->
                    E = {error, io_lib:format("'prefixes' cannot be empty when the "
                                              "'state' is '~s'", [StateVal])},
                    {Cfg, [E | Errors]};
                false ->
                    case StateVal =:= "mandatory" andalso
                        misc:should_cluster_data_be_encrypted() of
                        false -> {[CfgPair | Cfg], Errors};
                        true ->
                            M = "Cannot set 'state' to 'mandatory' when "
                                "cluster encryption level has been set to "
                                "'all'",
                            E = {error, M},
                            {Cfg, [E | Errors]}
                    end
            end;
        Err ->
            {Cfg, [Err | Errors]}
    end.

validate_triple(Triple) ->
    Triple1 = lists:sort(Triple),
    case [K || {K, _V} <- Triple1] =:= ["delimiter", "path", "prefix"] of
        true ->
            case validate_client_cert_auth_param("path", proplists:get_value("path", Triple1)) of
                {ok, _} ->
                    {[{list_to_atom(K), V} || {K, V} <- Triple1], []};
                E ->
                    {[], [E]}
            end;
        false ->
            E = {error, io_lib:format("Invalid prefixes entry (~p). Must contain "
                                      "'path', 'prefix' & 'delimiter' fields.",
                                      [Triple1])},
            {[], [E]}
    end.

check_for_duplicate_prefixes(_PrefixCfg, Errors) when Errors =/= [] ->
    Errors;
check_for_duplicate_prefixes(PrefixCfg, Errors) ->
    {_, NewErrors} =
        lists:foldl(
          fun(Triple, {Set, EAcc}) ->
                  Path = proplists:get_value(path, Triple),
                  Prefix = proplists:get_value(prefix, Triple),

                  case sets:is_element({Path, Prefix}, Set) of
                      true ->
                          E = {error,
                               io_lib:format("Multiple entries with same path & prefix "
                                             "(~p) are not allowed", [{Path, Prefix}])},
                          {Set, [E | EAcc]};
                      false ->
                          {sets:add_element({Path, Prefix}, Set), EAcc}
                  end
          end, {sets:new(), Errors}, PrefixCfg),

    NewErrors.

validate_client_cert_auth_prefixes(Prefixes, Cfg, Errors) ->
    %% Prefixes are represented as a list of lists. Each list contains
    %% tuples representing the path, prefix and delimiter.
    {PCfg, PErrs0} = lists:foldr(
                      fun({C, E}, {CAcc, EAcc}) ->
                              {[C | CAcc], E ++ EAcc}
                      end, {[], []}, [validate_triple(Triple) || Triple <- Prefixes]),

    PErrs = check_for_duplicate_prefixes(PCfg, PErrs0),
    {Cfg ++ [{prefixes, PCfg}], PErrs ++ Errors}.

validate_client_cert_auth_settings({Key, Val}, Params, OldVal, Acc) ->
    case Key == "state" andalso Val =/= "disable" of
        true ->
            case {proplists:get_value("path", Params), proplists:get_value(path, OldVal)} of
                {undefined, undefined} ->
                    [{error, {400, io_lib:format("'path' must be defined when 'state' is "
                                                 "being set to '~s'", [Val])}}] ++ Acc;
                _ ->
                    Acc
            end;
        false ->
            Acc
    end.

handle_client_cert_auth_settings_post(Req) ->
    menelaus_util:assert_is_enterprise(),

    case cluster_compat_mode:is_cluster_51() of
        true ->
            try
                JSON = menelaus_util:parse_json(Req),
                do_handle_client_cert_auth_settings_5_1_post(Req, JSON)
            catch
                throw:{error, Msg} ->
                    menelaus_util:reply_json(Req, Msg, 400);
                _:_ ->
                    menelaus_util:reply_json(Req, <<"Invalid JSON">>, 400)
            end;
        false ->
            do_handle_client_cert_auth_settings_5_0_post(Req)
    end.

%% The client_cert_auth settings will be a JSON payload 5.1 onwards and it'll look like
%% the following:
%%
%% {
%%     "state": "enable",
%%     "prefixes": [
%%       {
%%         "path": "san.uri",
%%         "prefix": "www.cb-",
%%         "delimiter": ".,;"
%%       },
%%       {
%%         "path": "san.email",
%%         "prefix": "a",
%%         "delimiter": "@"
%%       }
%%     ]
%% }
do_handle_client_cert_auth_settings_5_1_post(Req, JSON) ->
    {struct, Data} = JSON,
    StateRaw = proplists:get_value(<<"state">>, Data),
    PrefixesRaw = proplists:get_value(<<"prefixes">>, Data),

    case StateRaw =:= undefined orelse PrefixesRaw =:= undefined of
        true ->
            throw({error,
                   <<"Unsupported format: Must contain 'state' and 'prefixes' "
                     "fields">>});
        false ->
            case length(proplists:get_keys(Data)) > 2 of
                true ->
                    throw({error, <<"Unsupported fields: Must contain 'state' "
                                    "and 'prefixes' fields only">>});
                false -> ok
            end
    end,

    State = binary_to_list(StateRaw),
    case length(PrefixesRaw) > ?MAX_CLIENT_CERT_PREFIXES of
        true ->
            Err = io_lib:format("Maximum number of prefixes supported is ~p",
                                [?MAX_CLIENT_CERT_PREFIXES]),
            menelaus_util:reply_json(Req, list_to_binary(Err), 400);
        false ->
            Prefixes = [[{binary_to_list(K), binary_to_list(V)} || {K, V} <- Triple]
                        || {struct, Triple} <- PrefixesRaw],

            {Cfg0, Errors0} = validate_client_cert_auth_state(State, Prefixes, [], []),
            {Cfg, Errors} = validate_client_cert_auth_prefixes(Prefixes, Cfg0, Errors0),

            case Errors of
                [] ->
                    case ns_ssl_services_setup:client_cert_auth() of
                        Cfg ->
                            menelaus_util:reply(Req, 200);
                        _ ->
                            ns_config:set(client_cert_auth, Cfg),
                            ns_audit:client_cert_auth(Req, Cfg),
                            menelaus_util:reply(Req, 202)
                    end;
                _ ->
                    Out = [list_to_binary(Msg) || {error, Msg} <- Errors],
                    menelaus_util:reply_json(Req, Out, 400)
            end
    end.

do_handle_client_cert_auth_settings_5_0_post(Req) ->
    Params = mochiweb_request:parse_post(Req),
    OldVal = ns_ssl_services_setup:client_cert_auth(),
    AccumulateChanges =
        fun({Key, Val} = Pair, Acc) ->
                case allowed_values(Key) of
                    none ->
                        [{error, {400, io_lib:format("Invalid key: '~s'", [Key])}}]
                            ++ Acc;
                    Values ->
                        Acc1 = validate_client_cert_auth_settings(Pair, Params, OldVal, Acc),
                        case Values == any orelse lists:member(Val, Values) of
                            true ->
                                NewKey = list_to_atom(Key),
                                case proplists:get_value(NewKey, OldVal) =/= Val of
                                    true ->
                                        [{NewKey, Val}] ++ Acc1;
                                    _Else ->
                                        Acc1
                                end;
                            false ->
                                [{error, {400, io_lib:format("Invalid value '~s' "
                                                             "for key '~s'",
                                                             [Val, Key])
                                         }
                                 }] ++ Acc1
                        end
                end
        end,
    KeyChanged = lists:foldl(AccumulateChanges, [], Params),
    case proplists:get_value(error, KeyChanged, none) of
        none when length(KeyChanged) > 0 ->
            NewValue = lists:ukeysort(1, KeyChanged ++ OldVal),
            ns_config:set(client_cert_auth, NewValue),
            ns_audit:client_cert_auth(Req, NewValue),
            menelaus_util:reply(Req, 202);
        {Code, Error} ->
            menelaus_util:reply_json(Req, list_to_binary(Error), Code);
        _Else ->
            menelaus_util:reply(Req, 200)
    end.
