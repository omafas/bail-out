%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

%% @doc rest api's for ldap support
-module(menelaus_web_ldap).

-include("ns_common.hrl").
-include("cut.hrl").

-export([handle_ldap_settings/1,
         handle_ldap_settings_post/1,
         handle_ldap_settings_validate_post/2,
         handle_invalidate_ldap_cache/1,
         invalidate_ldap_cache/0]).

handle_ldap_settings(Req) ->
    menelaus_web_rbac:assert_groups_and_ldap_enabled(),
    Settings = ldap_util:build_settings(),
    menelaus_web_settings2:handle_get([], params(), fun type_spec/1,
                                      Settings, Req).

prepare_ldap_settings(Settings) ->
    {Props} = menelaus_web_settings2:prepare_json([], params(),
                                                  fun type_spec/1, Settings),
    Props.

redact_ldap_cfg_keys() ->
    [cacert, client_tls_cert, bind_dn].

handle_ldap_settings_post(Req) ->
    menelaus_web_rbac:assert_groups_and_ldap_enabled(),
    menelaus_web_settings2:handle_post(
      fun (Props, Req2) ->
          Props2 = [{Key, Val} || {[Key], Val} <- Props],
          ns_audit:ldap_settings(Req, prepare_ldap_settings(Props2)),
          OldSettings = ldap_util:build_settings(),
          ldap_util:set_settings(Props2),
          NewSettings = misc:update_proplist(OldSettings, Props2),
          TFun = fun (Settings) ->
                         RedactedSettings = event_log:redact_keys(
                                              Settings, redact_ldap_cfg_keys()),
                         prepare_ldap_settings(RedactedSettings)
                 end,
          event_log:maybe_add_log_settings_changed(ldap_cfg_changed,
                                                   OldSettings,
                                                   NewSettings,
                                                   TFun),
          handle_ldap_settings(Req2)
      end, [], params(), fun type_spec/1, Req).

build_validation_settings(Props) ->
    Current = ldap_util:build_settings(),
    SetReuseSessions =
        %% Set reuse_sessions to false, unless it's set to another value
        %% in settings explicitly
        fun (undefined) -> [{reuse_sessions, false}];
            (Opts) -> misc:update_proplist([{reuse_sessions, false}], Opts)
        end,
    Settings = misc:key_update(extra_tls_opts, Current, SetReuseSessions),
    {Current, misc:update_proplist(Settings, Props)}.

handle_ldap_settings_validate_post(Type, Req) when Type =:= "connectivity";
                                                   Type =:= "authentication";
                                                   Type =:= "groupsQuery" ->
    menelaus_web_rbac:assert_groups_and_ldap_enabled(),
    menelaus_web_settings2:handle_post(
      fun (Props, Req2) ->
              ParsedProps = [{Key, Val} || {[Key], Val} <- Props],
              {CurrProps, NewProps} = build_validation_settings(ParsedProps),
              ?log_debug("Validating ~p for LDAP settings: ~n~p~n"
                         "Modified settings:~n~p~n"
                         "Full list of settings:~n~p",
                         [Type,
                          ns_config_log:tag_user_data(
                            prepare_ldap_settings(ParsedProps)),
                          ns_config_log:tag_user_data(
                            prepare_ldap_settings(ParsedProps -- CurrProps)),
                          ns_config_log:tag_user_data(
                            prepare_ldap_settings(NewProps))]),
              Res = validate_ldap_settings(Type, NewProps),
              menelaus_util:reply_json(Req2, {Res})
      end, [], params() ++ validation_params(Type), fun type_spec/1, Req);
handle_ldap_settings_validate_post(_Type, Req) ->
    menelaus_util:reply_json(Req, <<"Unknown validation type">>, 404).

validate_ldap_settings("connectivity", Settings) ->
    case ldap_auth:with_query_connection(Settings, fun (_) -> ok end) of
        ok ->
            [{result, success}];
        {error, Error} ->
            Bin = iolist_to_binary(ldap_auth:format_error(Error)),
            [{result, error},
             {reason, Bin}]
    end;
validate_ldap_settings("authentication", Settings) ->
    User = proplists:get_value(auth_user, Settings),
    Pass = proplists:get_value(auth_pass, Settings),
    case ldap_auth:authenticate_with_cause(User, Pass, Settings) of
        {ok, DN} ->
            [{result, success}, {dn, iolist_to_binary(DN)}];
        {error, Error} ->
            Bin = iolist_to_binary(ldap_auth:format_error(Error)),
            [{result, error}, {reason, Bin}]
    end;
validate_ldap_settings("groupsQuery", Settings) ->
    GroupsUser = proplists:get_value(groups_query_user, Settings),
    case ldap_auth:user_groups(GroupsUser, Settings) of
        {ok, Groups} ->
            [{result, success},
             {groups, [list_to_binary(G) || G <- Groups]}];
        {error, Error2} ->
            Bin2 = iolist_to_binary(ldap_auth:format_error(Error2)),
            [{result, error},
             {reason, Bin2}]
    end.

params() ->
    [{"authenticationEnabled", #{cfg_key => authentication_enabled,
                                 type => bool}},
     {"authorizationEnabled", #{cfg_key => authorization_enabled,
                                type => bool}},
     {"hosts", #{type => ldap_hosts}},
     {"port", #{type => {int, 0, 65535}}},
     {"encryption", #{type => {one_of, existing_atom,
                               ["TLS", "StartTLSExtension", "None"]}}},
     {"userDNMapping", #{cfg_key => user_dn_mapping,
                         type => user_dn_mapping}},
     {"bindMethod", #{cfg_key => bind_method,
                      type => bind_method}},
     {"bindDN", #{cfg_key => bind_dn, type => ldap_dn}},
     {"bindPass", #{cfg_key => bind_pass, type => password}},
     {"groupsQuery", #{cfg_key => groups_query, type => ldap_groups_query}},
     {"maxParallelConnections", #{cfg_key => max_parallel_connections,
                                  type => {int, 1, 1000}}},
     {"maxCacheSize", #{cfg_key => max_cache_size, type => {int, 0, 10000}}},
     {"cacheValueLifetime", #{cfg_key => cache_value_lifetime,
                              type => pos_int}},
     {"requestTimeout", #{cfg_key => request_timeout, type => pos_int}},
     {"nestedGroupsEnabled", #{cfg_key => nested_groups_enabled, type => bool}},
     {"nestedGroupsMaxDepth", #{cfg_key => nested_groups_max_depth,
                                type => {int, 1, 100}}},
     {"failOnMaxDepth", #{cfg_key => fail_on_max_depth, type => bool}},
     {"serverCertValidation", #{cfg_key => server_cert_validation,
                                type => bool}},
     {"cacert", #{type => certificate}},
     {"clientTLSCert", #{cfg_key => client_tls_cert, type => certificate}},
     {"clientTLSKey", #{cfg_key => client_tls_key, type => pkey}},
     {"extraTLSOpts", #{cfg_key => extra_tls_opts, type => tls_opts}}].

validation_params("connectivity") -> [];
validation_params("authentication") ->
    [{"authUser", #{cfg_key => auth_user, type => string}},
     {"authPass", #{cfg_key => auth_pass, type => string}}];
validation_params("groupsQuery") ->
    [{"groupsQueryUser", #{cfg_key => groups_query_user, type => string}}].

type_spec(ldap_hosts) ->
    #{validators => [fun validate_ldap_hosts/2],
      formatter => ?cut({value, [list_to_binary(H) || H <- _]})};
type_spec(user_dn_mapping) ->
    #{validators => [fun validate_user_dn_mapping/2],
      formatter => fun ({Obj, _}) -> {value, Obj} end};
type_spec(ldap_dn) ->
    #{validators => [string, fun validate_ldap_dn/2],
      formatter => fun (<<"redacted">>) ->
                           {value, <<"redacted">>};
                       (Dn) ->
                           {value, list_to_binary(Dn)}
                   end};
type_spec(ldap_groups_query) ->
    #{validators => [string, fun validate_ldap_groups_query/2],
      formatter => fun (undefined) -> ignore;
                       (Str) -> {value, list_to_binary(Str)}
                   end};
type_spec(certificate) ->
    #{validators => [string, fun validate_cert/2],
      formatter => fun (undefined) -> ignore;
                       (<<"redacted">>) -> {value, <<"redacted">>};
                       ({Cert, _Decoded}) -> {value, Cert}
                   end};
type_spec(pkey) ->
    #{validators => [string, fun validate_key/2],
      formatter => fun (undefined) -> ignore;
                       ({password, {_, _, not_encrypted}}) ->
                           {value, <<"**********">>}
                   end};
type_spec(tls_opts) ->
    #{validators => [not_supported],
      formatter => fun (undefined) -> ignore;
                       (List) ->
                           Sanitize = fun ({password, _}) -> <<"********">>;
                                          (V) -> V
                                      end,
                           Sanitized = [{K, Sanitize(V)} || {K, V} <- List],
                           Str = io_lib:format("~p", [Sanitized]),
                           {value, iolist_to_binary(Str)}
                   end};
type_spec(bind_method) ->
    #{validators => [{one_of, existing_atom, ["Simple", "SASLExternal",
                                              "None"]}],
      formatter => fun (undefined) -> ignore;
                       (Atom) -> {value, atom_to_binary(Atom, latin1)}
                   end}.

validate_key(Name, State) ->
    validator:validate(
      fun ("") -> {value, undefined};
          (Key) ->
              case ns_server_cert:validate_pkey(iolist_to_binary(Key),
                                                fun () -> undefined end) of
                  {ok, DecodedKey} -> {value, {password, DecodedKey}};
                  {error, _} -> {error, "invalid key"}
              end
      end, Name, State).

validate_cert(Name, State) ->
    validator:validate(
      fun ("") -> {value, undefined};
          (Cert) ->
              BinCert = iolist_to_binary(Cert),
              case ns_server_cert:decode_single_certificate(BinCert) of
                  {error, _} -> {error, "invalid certificate"};
                  Decoded -> {value, {BinCert, Decoded}}
              end
      end, Name, State).

validate_ldap_hosts(Name, State) ->
    IsJson = validator:is_json(State),
    validator:validate(
      fun (List) when IsJson, is_list(List) ->
              case lists:all(fun is_binary/1, List) of
                  true -> {value, [binary_to_list(E) || E <- List]};
                  false -> {error, "must be a list of strings"}
              end;
          (HostsRaw) ->
              {value, [string:trim(T) || T <- string:tokens(HostsRaw, ",")]}
      end, Name, State).

validate_user_dn_mapping(Name, State) ->
    IsJson = validator:is_json(State),
    validator:validate(
      fun (Obj) when IsJson ->
              try {value, {Obj, parse_dn_mapping(Obj)}}
              catch throw:{error, _} = Err -> Err end;
          (Str) ->
              try
                  Obj = try ejson:decode(Str)
                        catch _:_ ->
                            throw({error, "Invalid JSON"})
                        end,
                  {value, {Obj, parse_dn_mapping(Obj)}}
              catch
                  throw:{error, _} = Err -> Err
              end
      end, Name, State).

parse_dn_mapping({[{<<"query">>, Q}]}) ->
    has_username_var(Q),
    QueryTempl = iolist_to_binary(string:replace(Q, "%u", "{0}", all)),
    case ldap_util:parse_url(<<"ldap:///", QueryTempl/binary>>,
                             [{"\\{\\d+\\}", [{default, "testuser"}]}]) of
        {ok, _} -> ok;
        {error, Reason} ->
            throw({error, io_lib:format(
                            "Invalid LDAP query '~s'. ~s",
                            [Q, ldap_auth:format_error(Reason)])})
    end,
    [{<<"(.+)">>, {'query', QueryTempl}}];
parse_dn_mapping({[{<<"template">>, T}]}) ->
    has_username_var(T),
    Template = iolist_to_binary(string:replace(T, "%u", "{0}", all)),
    DN = re:replace(Template, "\\{\\d+\\}", "placeholder",
                    [{return, list}, global]),
    case ldap_util:parse_dn(DN) of
        {ok, _} -> ok;
        {error, Error} ->
            ErrorStr = ldap_auth:format_error(Error),
            throw({error, io_lib:format("Template is not a valid LDAP "
                                        "distinguished name: ~s", [ErrorStr])})
    end,
    [{<<"(.+)">>, {template, Template}}];
parse_dn_mapping(_) ->
    throw({error, "JSON object must contain either query or template"}).

has_username_var(Str) ->
    case re:run(Str, "%u", []) of
        {match, _} -> Str;
        nomatch ->
            throw({error, "Template or query should contain at least one %u"})
    end.

validate_ldap_dn(Name, State) ->
    validator:validate(
      fun (DN) ->
              case ldap_util:parse_dn(DN) of
                  {ok, _} -> {value, DN};
                  {error, Error} ->
                      ErrorStr = ldap_auth:format_error(Error),
                      Msg = io_lib:format("Should be valid LDAP distinguished "
                                          "name: ~s", [ErrorStr]),
                      {error, Msg}
              end
      end, Name, State).

validate_ldap_groups_query(Name, State) ->
    validator:validate(
      fun (Query) ->
              case ldap_util:parse_url(
                     "ldap:///" ++ Query,
                     [{"%u", [{default, "test_user"}]},
                      {"%D", [{default, "uid=testdn"}]}]) of
                  {ok, URLProps} ->
                      Base = proplists:get_value(dn, URLProps, ""),
                      Attrs = proplists:get_value(attributes, URLProps, []),
                      if
                          length(Attrs) > 1 ->
                              {error, "Only one attribute can be specified"};
                          Base == "" ->
                              {error, "Search base cannot be empty"};
                          true ->
                              ok
                      end;
                  {error, _} ->
                      {error, "Invalid LDAP query"}
              end
      end, Name, State).

handle_invalidate_ldap_cache(Req) ->
    case remote_api:invalidate_ldap_cache(ns_node_disco:nodes_actual()) of
        {_, []} -> menelaus_util:reply_json(Req, {[]});
        {_, BadNodes} ->
            Msg = io_lib:format("Invalidation failed on nodes: ~p", [BadNodes]),
            menelaus_util:reply_json(Req, iolist_to_binary(Msg), 500)
    end.

invalidate_ldap_cache() ->
    ldap_auth_cache:flush(),
    roles_cache:renew(),
    memcached_permissions:refresh().
