%% @author Couchbase <info@couchbase.com>
%% @copyright 2019 Couchbase, Inc.
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
    menelaus_util:reply_json(Req, {prepare_ldap_settings(Settings)}).

prepare_ldap_settings(Settings) ->
    SettingsDesc = ldap_settings_desc(),
    Fun =
      fun (_, undefined) -> undefined;
          (K, V) ->
            case lists:keyfind(K, 1, SettingsDesc) of
                %% Removing parameters that are used for validation only,
                %% like authUser and authPass
                false -> undefined;
                {_, JsonKey, _, Formatter} -> {JsonKey, Formatter(V)}
            end
      end,
    [{ResK, ResV} || {K, V} <- Settings, {ResK, ResV} <- [Fun(K, V)]].

handle_ldap_settings_post(Req) ->
    menelaus_web_rbac:assert_groups_and_ldap_enabled(),
    validator:handle(
      fun (Props) ->
              ParsedProps = parse_ldap_settings_keys(Props),
              ns_audit:ldap_settings(Req, prepare_ldap_settings(ParsedProps)),
              ldap_util:set_settings(ParsedProps),
              handle_ldap_settings(Req)
      end, Req, form, ldap_settings_validators()).

parse_ldap_settings_keys(Props) ->
    lists:map(
      fun ({JsonKey, Value}) ->
          case lists:keyfind(JsonKey, 2, ldap_settings_desc()) of
              false -> {JsonKey, Value};
              {Key, _, _, _} -> {Key, Value}
          end
      end, Props).

build_new_ldap_settings(Props) ->
    Current = ldap_util:build_settings(),
    {Current, misc:update_proplist(Current, Props)}.

handle_ldap_settings_validate_post(Type, Req) when Type =:= "connectivity";
                                                   Type =:= "authentication";
                                                   Type =:= "groupsQuery" ->
    menelaus_web_rbac:assert_groups_and_ldap_enabled(),
    validator:handle(
      fun (Props) ->
              ParsedProps = parse_ldap_settings_keys(Props),
              {CurrProps, NewProps} = build_new_ldap_settings(ParsedProps),
              ?log_debug("Validating ~p for LDAP settings: ~n~p~n"
                         "Modified settings:~n~p~n"
                         "Full list of settings:~n~p",
                         [Type,
                          prepare_ldap_settings(ParsedProps),
                          prepare_ldap_settings(ParsedProps -- CurrProps),
                          prepare_ldap_settings(NewProps)]),
              Res = validate_ldap_settings(Type, NewProps),
              menelaus_util:reply_json(Req, {Res})
      end, Req, form, ldap_settings_validator_validators(Type) ++
                      ldap_settings_validators());
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
    User = proplists:get_value(authUser, Settings),
    Pass = proplists:get_value(authPass, Settings),
    case ldap_auth:authenticate_with_cause(User, Pass, Settings) of
        {ok, DN} ->
            [{result, success}, {dn, iolist_to_binary(DN)}];
        {error, Error} ->
            Bin = iolist_to_binary(ldap_auth:format_error(Error)),
            [{result, error}, {reason, Bin}]
    end;
validate_ldap_settings("groupsQuery", Settings) ->
    GroupsUser = proplists:get_value(groupsQueryUser, Settings),
    case ldap_auth:user_groups(GroupsUser, Settings) of
        {ok, Groups} ->
            [{result, success},
             {groups, [list_to_binary(G) || G <- Groups]}];
        {error, Error2} ->
            Bin2 = iolist_to_binary(ldap_auth:format_error(Error2)),
            [{result, error},
             {reason, Bin2}]
    end.

ldap_settings_desc() ->
    Curry = fun functools:curry/1,
    Id = fun functools:id/1,
    %% [{ConfigKey, JSONKey, ValidatorFun, Formatter}]
    [{authentication_enabled, authenticationEnabled,
      Curry(fun validator:boolean/2), Id},
     {authorization_enabled, authorizationEnabled,
      Curry(fun validator:boolean/2), Id},
     {hosts, hosts,
      Curry(fun validate_ldap_hosts/2),
      fun (Hosts) -> [list_to_binary(H) || H <- Hosts] end},
     {port, port,
      fun (N) -> validator:integer(N, 0, 65535, _) end, Id},
     {encryption, encryption,
      fun (N) ->
          functools:compose(
            [validator:one_of(N, ["TLS", "StartTLSExtension", "None"], _),
             validator:convert(N, fun list_to_atom/1, _)])
      end, Id},
     {user_dn_mapping, userDNMapping,
      Curry(fun validate_user_dn_mapping/2), fun ({Obj, _}) -> Obj end},
     {bind_method, bindMethod,
      fun (N) ->
          functools:compose(
            [validator:one_of(N, ["Simple", "SASLExternal", "None"], _),
             validator:convert(N, fun list_to_atom/1, _)])
      end, Id},
     {bind_dn, bindDN,
      Curry(fun validate_ldap_dn/2), list_to_binary(_)},
     {bind_pass, bindPass,
      fun (N) ->
          functools:compose(
            [validator:touch(N, _),
             validator:convert(N, fun (P) -> ({password, P}) end, _)])
      end,
      fun ({password, ""}) -> <<>>;
          ({password, _}) -> <<"**********">>
      end},
     {groups_query, groupsQuery,
      Curry(fun validate_ldap_groups_query/2), list_to_binary(_)},
     {max_parallel_connections, maxParallelConnections,
      fun (N) -> validator:integer(N, 1, 1000, _) end, Id},
     {max_cache_size, maxCacheSize,
      fun (N) -> validator:integer(N, 0, 10000, _) end, Id},
     {cache_value_lifetime, cacheValueLifetime,
      fun (N) -> validator:integer(N, 0, infinity, _) end, Id},
     {request_timeout, requestTimeout,
      fun (N) -> validator:integer(N, 0, infinity, _) end, Id},
     {nested_groups_enabled, nestedGroupsEnabled,
      Curry(fun validator:boolean/2), Id},
     {nested_groups_max_depth, nestedGroupsMaxDepth,
      fun (N) -> validator:integer(N, 1, 100, _) end, Id},
     {fail_on_max_depth, failOnMaxDepth,
      Curry(fun validator:boolean/2), Id},
     {server_cert_validation, serverCertValidation,
      Curry(fun validator:boolean/2), Id},
     {cacert, cacert,
      Curry(fun validate_cert/2), fun ({CA, _Decoded}) -> CA end},
     {client_tls_cert, clientTLSCert,
      Curry(fun validate_cert/2), fun ({CA, _Decoded}) -> CA end},
     {client_tls_key, clientTLSKey,
      Curry(fun validate_key/2),
      fun (undefined) -> <<>>;
          ({password, {_, _, not_encrypted}}) -> <<"**********">>
      end}
    ].

ldap_settings_validators() ->
    [Validator(JsonKey) || {_, JsonKey, Validator, _} <- ldap_settings_desc()]
    ++ [validator:unsupported(_)].

validate_key(Name, State) ->
    validator:validate(
      fun ("") -> {value, undefined};
          (Key) ->
              case ns_server_cert:validate_pkey(iolist_to_binary(Key)) of
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

ldap_settings_validator_validators("connectivity") -> [];
ldap_settings_validator_validators("authentication") ->
    [validator:required(authUser, _),
     validator:required(authPass, _)];
ldap_settings_validator_validators("groupsQuery") ->
    [validator:required(groupsQueryUser, _)].

validate_ldap_hosts(Name, State) ->
    validator:validate(
      fun (HostsRaw) ->
              {value, [string:trim(T) || T <- string:tokens(HostsRaw, ",")]}
      end, Name, State).

validate_user_dn_mapping(Name, State) ->
    validator:validate(
      fun (Str) ->
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
    QueryTempl = iolist_to_binary(string:replace(Q, "%u", "{0}")),
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
    Template = iolist_to_binary(string:replace(T, "%u", "{0}")),
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
