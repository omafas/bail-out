%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc REST api's for handling ssl certificates

-module(menelaus_web_cert).

-include("ns_common.hrl").
-include("cut.hrl").

-export([handle_get_trustedCAs/1,
         handle_delete_trustedCA/2,
         handle_cluster_certificate/1,
         handle_regenerate_certificate/1,
         handle_load_ca_certs/1,
         handle_upload_cluster_ca/1, %% deprecated
         handle_reload_node_certificate/1,
         handle_get_node_certificate/2,
         handle_get_node_certificates/1,
         handle_client_cert_auth_settings/1,
         handle_client_cert_auth_settings_post/1]).

-define(MAX_CLIENT_CERT_PREFIXES, ?get_param(max_prefixes, 10)).

handle_get_trustedCAs(Req) ->
    menelaus_util:assert_is_enterprise(),
    %% Security admins should get all the information,
    %% Everybody else should get only certificates
    Extended = menelaus_roles:is_allowed({[admin, security], read},
                                         menelaus_auth:get_identity(Req)),
    Warnings = case Extended of
                    true -> ns_server_cert:get_warnings();
                    false -> []
               end,
    Nodes = ns_node_disco:nodes_wanted(),

    Json = lists:map(
             fun (Props) ->
                 CAId = proplists:get_value(id, Props),
                 Pem = proplists:get_value(pem, Props, <<>>),
                 Is71 = cluster_compat_mode:is_cluster_71(),
                 CANodes =
                     case Extended of
                         true -> ns_server_cert:filter_nodes_by_ca(Nodes, Pem);
                         false -> []
                     end,
                 CAWarnings = [W || {{ca, Id}, W} <- Warnings, Id =:= CAId],
                 jsonify_cert_props(
                   maybe_filter_cert_props(
                     Props ++
                     [{warnings, CAWarnings}] ++
                     [{nodes, CANodes} || Is71], not Extended))
             end, ns_server_cert:trusted_CAs(props)),
    menelaus_util:reply_json(Req, Json).

%% Leaving only info that can be extracted from certificates and ids
maybe_filter_cert_props(Props, _ShouldFilter = true) ->
    lists:filter(
      fun ({id, _}) -> true;
          ({pem, _}) -> true;
          ({subject, _}) -> true;
          ({not_before, _}) -> true;
          ({not_after, _}) -> true;
          (_) -> false
      end, Props);
maybe_filter_cert_props(Props, _ShouldFilter = false) ->
    Props.

handle_delete_trustedCA(IdStr, Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_71(),
    CurNodes = nodes(),
    try list_to_integer(IdStr) of
        Id ->
            menelaus_util:survive_web_server_restart(
              fun () ->
                  case ns_server_cert:remove_CA(Id) of
                      {ok, Props} ->
                          Subject = proplists:get_value(subject, Props),
                          ns_audit:delete_cluster_ca(Req, Subject),
                          ns_ssl_services_setup:sync(),
                          case netconfig_updater:ensure_tls_dist_started(
                                 CurNodes) of
                              ok -> menelaus_util:reply(Req, 204);
                              {error, ErrorMsg} ->
                                  menelaus_util:reply_global_error(Req,
                                                                   ErrorMsg)
                          end;
                      {error, not_found} ->
                          menelaus_util:reply_text(Req, "Not found", 404);
                      {error, {in_use, Nodes}} ->
                          Hosts = [misc:extract_node_address(N) || N <- Nodes],
                          HostsStr = lists:join(", ", Hosts),
                          Msg = io_lib:format("The CA certificate is in use by "
                                              "the following nodes: ~s",
                                              [HostsStr]),
                          MsgBin = iolist_to_binary(Msg),
                          menelaus_util:reply_global_error(Req, MsgBin)
                  end
              end)
    catch
        error:badarg ->
            menelaus_util:reply_text(Req, "Not found", 404)
    end.

handle_cluster_certificate(Req) ->
    menelaus_util:assert_is_enterprise(),
    %% This API doesn't make sense anymore because it is now possible that
    %% different nodes in the cluster might have different CAs (not that it's
    %% good idea in general but it is definitely the case during CA rotation)
    %% So, the idea is to keep this endpoint working for backward compat reasons
    %% until at least one CA is loaded. When the CA is added  this endpoint is
    %% not correct anymore and should start returning error
    assert_old_CAs(),

    case proplists:get_value("extended", mochiweb_request:parse_qs(Req)) of
        "true" ->
            handle_cluster_certificate_extended(Req);
        _ ->
            handle_cluster_certificate_simple(Req)
    end.

assert_old_CAs() ->
    NoNewCAsUploaded =
        lists:all(
          fun (P) ->
              case proplists:get_value(origin, P) of
                  upgrade -> true;
                  upload_api -> true;
                  _ -> proplists:get_value(type, P) == generated
              end
          end, ns_server_cert:trusted_CAs(props)),
    case NoNewCAsUploaded of
        true -> ok;
        false ->
            Err = <<"this API is disabled, "
                    "please use GET /pools/default/trustedCAs, "
                    "see documentation for details">>,
            menelaus_util:web_exception(400, Err)
    end.

handle_cluster_certificate_simple(Req) ->
    Cert = lists:last(ns_server_cert:trusted_CAs(pem)),
    menelaus_util:reply_ok(Req, "text/plain", Cert).

format_time(UTCSeconds) ->
    DateTime = calendar:gregorian_seconds_to_datetime(UTCSeconds),
    menelaus_util:format_server_time(DateTime, 0).

%% 'negligible' and 'severe' are not used currently hence the suppressing of
%% the dialyzer warning
-dialyzer({no_match, warning_severity_level/1}).
warning_severity_level(negligible) -> 1;
warning_severity_level(minimal) -> 2;
warning_severity_level(significant) -> 3;
warning_severity_level(serious) -> 4;
warning_severity_level(severe) -> 5.

warning_severity_props(N) ->
    [{severity, warning_severity_level(N)},
     {severityName, N}].

warning_severity(expires_soon) -> significant;
warning_severity(unused) -> minimal;
warning_severity(mismatch) -> minimal;
warning_severity(expired) -> serious;
warning_severity(self_signed) -> minimal.

warning_props({expires_soon, UTCSeconds}) ->
    [{name, expires_soon},
     {message, ns_error_messages:node_certificate_warning(expires_soon)},
     {expires, format_time(UTCSeconds)} |
     warning_severity_props(warning_severity(expires_soon))];
warning_props(Warning) ->
    [{name, Warning},
     {message, ns_error_messages:node_certificate_warning(Warning)} |
     warning_severity_props(warning_severity(Warning))].

translate_warning({{node, Node}, Warning}) ->
    [{node, Node} | warning_props(Warning)];
translate_warning({{ca, Id}, Warning}) ->
    [{ca, Id} | warning_props(Warning)].

jsonify_cert_props(Props) ->
    {lists:filtermap(
       fun ({expires, UTCSeconds}) ->
               {true, {expires, format_time(UTCSeconds)}};
           ({not_after, UTCSeconds}) ->
               {true, {notAfter, format_time(UTCSeconds)}};
           ({not_before, UTCSeconds}) ->
               {true, {notBefore, format_time(UTCSeconds)}};
           ({load_timestamp, UTCSeconds}) ->
               {true, {loadTimestamp, format_time(UTCSeconds)}};
           ({load_host, H}) ->
               {true, {loadHost, H}};
           ({load_file, F}) ->
               {true, {loadFile, F}};
           ({K, _}) when K =:= subject;
                         K =:= pem;
                         K =:= id;
                         K =:= type ->
               true;
           ({pkey_passphrase_settings, PKeySettings}) ->
               PKeySettingsJSON =
                   {lists:map(
                      fun ({password, _}) ->
                              {password, <<"********">>};
                          ({args, Args}) ->
                              {args, [iolist_to_binary(A) || A <- Args]};
                          ({httpsOpts, Opts}) ->
                              {httpsOpts, {Opts}};
                          ({headers, Headers}) ->
                              {headers, {Headers}};
                          (KV) ->
                              KV
                      end, PKeySettings)},
               {true, {privateKeyPassphrase, PKeySettingsJSON}};
           ({warnings, Warnings}) ->
               {true, {warnings, [{warning_props(W)} || W <- Warnings]}};
           ({nodes, Nodes}) ->
               BuildHostname = menelaus_web_node:build_node_hostname(
                                 ns_config:latest(), _, misc:localhost()),
               {true, {nodes, [BuildHostname(N) || N <- Nodes]}};
           ({_, _}) ->
               false
       end, Props)}.

handle_cluster_certificate_extended(Req) ->
    CertProps =
        lists:filtermap( %% to be compatible with pre-7.1
          fun ({not_after, V}) -> {true, {expires, V}};
              ({not_before, _}) -> false;
              ({id, _}) -> false;
              ({K, V}) -> {true, {K, V}}
          end, lists:last(ns_server_cert:trusted_CAs(props))),
    CertJson = jsonify_cert_props(CertProps),
    Warnings = [{translate_warning(W)} || W <- ns_server_cert:get_warnings()],
    menelaus_util:reply_json(Req, {[{cert, CertJson}, {warnings, Warnings}]}).

handle_regenerate_certificate(Req) ->
    menelaus_util:assert_is_enterprise(),
    case cluster_compat_mode:is_cluster_71() of
        true -> ok;
        false -> assert_n2n_encryption_is_disabled()
    end,
    menelaus_util:survive_web_server_restart(
      fun () ->
          ns_server_cert:generate_and_set_cert_and_pkey(),
          ns_ssl_services_setup:sync(),
          ?log_info("Completed certificate regeneration"),
          ns_audit:regenerate_certificate(Req),
          handle_cluster_certificate_simple(Req)
      end).

%% deprecated, use menelaus_util:reply_global_error/2 instead
reply_error(Req, Error) ->
    menelaus_util:reply_json(
      Req, {[{error, ns_error_messages:cert_validation_error_message(Error)}]}, 400).

handle_load_ca_certs(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_71(),
    Nodes = nodes(),
    menelaus_util:survive_web_server_restart(
      fun () ->
          case ns_server_cert:load_CAs_from_inbox() of
              {ok, NewCertsProps} ->
                  lists:foreach(
                    fun (Props) ->
                        Subject = proplists:get_value(subject, Props),
                        Expire = proplists:get_value(not_after, Props),
                        ns_audit:upload_cluster_ca(Req, Subject, Expire)
                    end, NewCertsProps),
                  ns_ssl_services_setup:sync(),
                  case netconfig_updater:ensure_tls_dist_started(Nodes) of
                      ok ->
                          CertsJson = [jsonify_cert_props(C) ||
                                       C <- NewCertsProps],
                          menelaus_util:reply_json(Req, CertsJson, 200);
                      {error, ErrorMsg} ->
                          menelaus_util:reply_json(Req, ErrorMsg, 400)
                  end;
              {error, Error} ->
                  ?log_error("Error loading CA certificates: ~p", [Error]),
                  menelaus_util:reply_json(
                    Req,
                    ns_error_messages:load_CAs_from_inbox_error(Error), 400)
          end
      end).

handle_upload_cluster_ca(Req) ->
    menelaus_util:assert_is_enterprise(),
    case (not cluster_compat_mode:is_cluster_71()) orelse
         ns_config:read_key_fast(allow_non_local_ca_upload, false) of
        true -> ok;
        false ->
            Msg = "this behavior can be changed by means of "
                  "POST /settings/security/allowNonLocalCACertUpload, "
                  "see documentation for details",
            menelaus_util:ensure_local(Req, Msg)
    end,

    case mochiweb_request:recv_body(Req) of
        undefined ->
            reply_error(Req, empty_cert);
        PemEncodedCA ->
            menelaus_util:survive_web_server_restart(
              fun () ->
                  case cluster_compat_mode:is_cluster_71() of
                      true ->
                          AddOpts = [{single_cert, true},
                                     {extra_props, [{origin, upload_api}]}],
                          case ns_server_cert:add_CAs(uploaded, PemEncodedCA,
                                                      AddOpts) of
                              {ok, []} ->
                                  reply_error(Req, already_in_use);
                              {ok, [Props]} ->
                                  ns_audit:upload_cluster_ca(
                                    Req,
                                    proplists:get_value(subject, Props),
                                    proplists:get_value(not_after, Props)),
                                  handle_cluster_certificate_extended(Req);
                              {error, Error} ->
                                  reply_error(Req, Error)
                          end;
                      false ->
                          assert_n2n_encryption_is_disabled(),
                          case ns_server_cert:set_cluster_ca(PemEncodedCA) of
                              {ok, Props} ->
                                  ns_audit:upload_cluster_ca(
                                    Req,
                                    proplists:get_value(subject, Props),
                                    proplists:get_value(expires, Props)),
                                  handle_cluster_certificate_extended(Req);
                              {error, Error} ->
                                  reply_error(Req, Error)
                          end
                  end
              end)
    end.

assert_n2n_encryption_is_disabled() ->
    case misc:is_cluster_encryption_fully_disabled() of
        true -> ok;
        false ->
            menelaus_util:web_exception(
              400, "Operation requires node-to-node encryption to be disabled")
    end.

handle_reload_node_certificate(Req) ->
    menelaus_util:assert_is_enterprise(),
    Nodes = nodes(),
    JSONData =
        case mochiweb_request:recv_body(Req) of
            undefined -> {[]};
            <<>> -> {[]};
            Bin when is_binary(Bin) ->
                try ejson:decode(Bin)
                catch _:_ ->
                    menelaus_util:global_error_exception(400,
                                                         <<"Invalid Json">>)
                end
        end,
    validator:handle(
      fun (Params) ->
          PassphraseSettings =
              proplists:get_value(privateKeyPassphrase, Params, []),

          menelaus_util:survive_web_server_restart(
            fun () ->
                case ns_server_cert:load_node_certs_from_inbox(
                       PassphraseSettings) of
                    {ok, Props} ->
                        ns_audit:reload_node_certificate(
                          Req,
                          proplists:get_value(subject, Props),
                          proplists:get_value(not_after, Props)),
                        ns_ssl_services_setup:sync(),
                        case netconfig_updater:ensure_tls_dist_started(Nodes) of
                            ok ->
                                menelaus_util:reply(Req, 200);
                            {error, ErrorMsg} ->
                                menelaus_util:reply_json(Req, ErrorMsg, 400)
                        end;
                    {error, Error} ->
                        ?log_error("Error reloading node certificate: ~p",
                                   [Error]),
                        Msg = ns_error_messages:reload_node_certificate_error(
                                Error),
                        menelaus_util:reply_json(Req, Msg, 400)
                end
            end)
      end, Req, JSONData,
      [validator:decoded_json(
         privateKeyPassphrase,
         [validator:required(type, _),
          validator:one_of(type, ["script", "rest", "plain"], _),
          validator:convert(type, binary_to_atom(_, latin1), _),
          validate_required_keys(type, _)], _),
       validator:unsupported(_)]).

validate_required_keys(Name, State) ->
    validator:validate(
      fun (T, S) -> {ok, functools:chain(S, validators(T))} end, Name, State).

%% Note that defaults are put here for a reason: if we change the defaults in
%% future it should not affect existing installations as it may lead to a break.
validators(script) ->
    [validator:required(path, _),
     validate_script_path(path, _),
     validator:boolean(trim, _),
     validator:default(trim, true, _),
     validator:string_array(args, _),
     validator:default(args, [], _),
     validator:integer(timeout, _),
     validator:default(timeout, 5000, _),
     validator:unsupported(_)];
validators(rest) ->
    [validator:required(url, _),
     validate_rest_url(url, _),
     validator:validate(
       fun (<<"https://", _/binary>>, S) ->
               Validators =
                   [validator:default(httpsOpts, {[]}, _),
                    validator:decoded_json(httpsOpts, https_opts_validators(),
                                           _)],
               {ok, functools:chain(S, Validators)};
           (_T, S) ->
               {ok, S}
       end, url, _),
     validator:one_of(addressFamily, ["inet", "inet6"], _),
     validator:convert(addressFamily, binary_to_atom(_, latin1), _),
     validator:integer(timeout, _),
     validator:default(timeout, 5000, _),
     validator:validate(
       fun ({Headers}) when is_list(Headers) ->
               case lists:all(
                      fun ({_, V}) when is_binary(V) -> true;
                          ({_, _}) -> false
                      end, Headers) of
                   true -> {value, Headers};
                   false -> {error, "Header values must be strings"}
               end;
           (_) -> {error, "Headers must be a JSON object"}
       end, headers, _),
     validator:default(headers, [], _),
     validator:unsupported(_)];
validators(plain) ->
    [validator:required(password, _),
     validate_password(password, _),
     validator:unsupported(_)].

validate_script_path(Name, State) ->
    validator:validate(
      fun (Path) when is_binary(Path) ->
            case lists:member(<<"..">>, filename:split(Path)) of
                true ->
                    {error, "Path must not contain parent directory "
                            "segments (..) for security reasons"};
                false ->
                    ScriptsDir = iolist_to_binary(user_scripts_dir()),
                    AbsPath = filename:join(ScriptsDir, Path),
                    AbsPathTokens = filename:split(AbsPath),
                    ScriptsDirTokens = filename:split(ScriptsDir),
                    case lists:prefix(ScriptsDirTokens, AbsPathTokens) of
                        true ->
                            case filelib:is_regular(AbsPath) of
                                true ->
                                    {value, AbsPath};
                                false ->
                                    {error, io_lib:format(
                                              "File ~s doesn't exist or not a "
                                              "regular file", [AbsPath])}
                            end;
                        false ->
                            {error, io_lib:format(
                                      "Script must reside in ~s for security "
                                      "reasons", [ScriptsDir])}
                    end
            end
      end, Name, State).

user_scripts_dir() ->
    path_config:component_path(data, "scripts").

validate_rest_url(Name, State) ->
    validator:validate(
      fun (URL) ->
            case uri_string:parse(URL) of
                {error, _, _} -> {error, "Invalid url"};
                #{host := Host} when Host =/= [] ->
                    case URL of
                        <<"http://", _/binary>> -> {value, URL};
                        <<"https://", _/binary>> -> {value, URL};
                        _ -> {error, "Invalid scheme in the URL"}
                    end;
                #{} ->
                    {error, "Invalid url"}
            end
      end, Name, State).

https_opts_validators() ->
    [validator:boolean(verifyPeer, _),
     validator:default(verifyPeer, true, _),
     validator:unsupported(_)].

validate_password(Name, State) ->
    validator:validate(
      fun (Pass) when is_binary(Pass)->
              {value, Pass};
          (_) ->
              {error, "Password must be a string"}
      end, Name, State).

handle_get_node_certificates(Req) ->
    Nodes = ns_node_disco:nodes_wanted(),
    Localhost = misc:localhost(),
    AllWarnings = ns_server_cert:get_warnings(),
    NodeCerts =
        lists:filtermap(
          fun (N) ->
              Hostname = menelaus_web_node:build_node_hostname(
                           ns_config:latest(), N, Localhost),
              case prepare_node_cert_info(N, AllWarnings) of
                  {ok, {JsonObjProplist}} ->
                      {true, {[{node, Hostname} | JsonObjProplist]}};
                  {error, not_found} ->
                      false
              end
          end, Nodes),
    menelaus_util:reply_json(Req, NodeCerts).

handle_get_node_certificate(NodeId, Req) ->
    menelaus_util:assert_is_enterprise(),

    case menelaus_web_node:find_node_hostname(NodeId, Req) of
        {ok, Node} ->
            case prepare_node_cert_info(Node, ns_server_cert:get_warnings()) of
                {ok, CertJson} ->
                    menelaus_util:reply_json(Req, CertJson);
                {error, not_found} ->
                    menelaus_util:reply_text(
                      Req, <<"Certificate is not set up on this node">>, 404)
            end;
        {error, {invalid_node, Reason}} ->
            menelaus_util:reply_text(Req, Reason, 400);
        {error, not_found} ->
            menelaus_util:reply_text(
              Req,
              <<"Node is not found, make sure the ip address/hostname matches the ip address/hostname used by Couchbase">>,
              404)
    end.

prepare_node_cert_info(Node, AllWarnings) ->
    case ns_server_cert:get_node_cert_info(Node) of
        [] -> {error, not_found};
        Props ->
            Warnings = [W || {{node, WarnNode}, W} <- AllWarnings,
                             WarnNode =:= Node],
            Filtered =
                lists:filtermap(
                    fun ({subject, _}) -> true;
                        %% Backward compat:
                        ({not_after, V}) -> {true, {expires, V}};
                        ({pem, _}) -> true;
                        ({type, _}) -> true;
                        ({pkey_passphrase_settings, _}) -> true;
                        (_) -> false
                    end, Props),
            {ok, jsonify_cert_props([{warnings, Warnings} | Filtered])}
    end.

allowed_values(Key) ->
    Values = [{"state", ["enable", "disable", "mandatory"]},
              {"path", ["subject.cn", "san.uri", "san.dnsname", "san.email"]},
              {"prefix", any},
              {"delimiter", any}],
    proplists:get_value(Key, Values, none).

handle_client_cert_auth_settings(Req) ->
    Cca = ns_ssl_services_setup:client_cert_auth(),
    State = list_to_binary(proplists:get_value(state, Cca)),
    Prefixes = [begin
                    {struct, [{list_to_binary(atom_to_list(K)), list_to_binary(V)}
                              || {K, V} <- Triple]}
                end || Triple <- proplists:get_value(prefixes, Cca, [])],

    Out = {struct, [{<<"state">>, State}, {<<"prefixes">>, Prefixes}]},
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

handle_client_cert_auth_settings_post(Req) ->
    menelaus_util:assert_is_enterprise(),

    menelaus_util:survive_web_server_restart(
      fun () ->
          try
              JSON = menelaus_util:parse_json(Req),
              do_handle_client_cert_auth_settings_post(Req, JSON)
          catch
              throw:{error, Msg} ->
                  menelaus_util:reply_json(Req, Msg, 400);
              _:_ ->
                  menelaus_util:reply_json(Req, <<"Invalid JSON">>, 400)
          end
      end).

%% The client_cert_auth settings will be a JSON payload and it'll look like
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
do_handle_client_cert_auth_settings_post(Req, JSON) ->
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
