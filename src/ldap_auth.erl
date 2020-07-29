%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-2019 Couchbase, Inc.
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
-module(ldap_auth).

-include("ns_common.hrl").

-include_lib("eldap/include/eldap.hrl").
-include("cut.hrl").

-export([authenticate/2,
         authenticate/3,
         authenticate_with_cause/3,
         user_groups/1,
         user_groups/2,
         format_error/1,
         with_query_connection/2,
         lookup_user/1]).

authenticate(Username, Password) ->
    authenticate(Username, Password, ldap_util:build_settings()).

authenticate(Username, Password, Settings) ->
    case authenticate_with_cause(Username, Password, Settings) of
        {ok, _} -> true;
        {error, _} -> false
    end.

authenticate_with_cause(Username, Password, Settings) ->
    case proplists:get_value(authentication_enabled, Settings) of
        true ->
            case get_user_DN(Username, Settings, #{}) of
                {ok, DN, _} ->
                    case ldap_util:with_simple_bind(
                           DN, Password, Settings, fun (_) -> ok end) of
                        ok -> {ok, DN};
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error -> Error
            end;
        false ->
            ?log_debug("LDAP authentication is disabled"),
            {error, authentication_disabled}
    end.

with_query_connection(Settings, Fun) ->
    BindMethod =
        case proplists:get_value(bind_method, Settings) of
            undefined ->
                %% we need 'undefined' to support migration from 6.6 and pre-6.6
                %% versions which doesn't have bind_method setting at all
                case ldap_util:client_cert_auth_enabled(Settings) of
                    true -> 'SASLExternal';
                    false -> 'Simple'
                end;
            M -> M
        end,
    case BindMethod of
        'Simple' ->
            DN = proplists:get_value(bind_dn, Settings),
            {password, Pass} = proplists:get_value(bind_pass, Settings),
            ldap_util:with_simple_bind(DN, Pass, Settings, Fun);
        'SASLExternal' ->
            ldap_util:with_external_bind(Settings, Fun);
        'None' ->
            ldap_util:with_connection(
              Settings,
              fun (Handle) ->
                  ?log_debug("Skipping binding as it is turned off"),
                  Fun(Handle)
              end)
    end.

lookup_user(Username) ->
    Settings = ldap_util:build_settings(),
    Timeout = proplists:get_value(request_timeout, Settings),
    with_query_connection(
      Settings,
      fun (Handle) ->
            case get_user_DN(Username, Settings, #{query_handle => Handle}) of
                {ok, DN, query} ->
                    {ok, DN};
                {ok, DN, _} ->
                    Query = DN ++ "?objectClass?base?(objectClass=*)",
                    dn_query_with_handle(Handle, Query, [], Timeout);
                {error, _} = Error ->
                    Error
            end
      end).

get_user_DN(Username, Settings, Context) ->
    {_, Map} = proplists:get_value(user_dn_mapping, Settings),
    case map_user_to_DN(Username, Settings, Map, Context) of
        {ok, DN, ResolveType} ->
            ?log_debug("Username->DN: Constructed DN: ~p for ~p",
                       [ns_config_log:tag_user_name(DN),
                        ns_config_log:tag_user_name(Username)]),
            {ok, DN, ResolveType};
        {error, Error} ->
            ?log_error("Username->DN: Mapping username to LDAP DN failed for "
                       "username ~p with reason ~p",
                       [ns_config_log:tag_user_name(Username), Error]),
            {error, Error}
    end.

map_user_to_DN(Username, _Settings, [], _) ->
    ?log_debug("Username->DN: rule not found for ~p",
               [ns_config_log:tag_user_name(Username)]),
    {ok, Username, default};
map_user_to_DN(Username, Settings, [{Re, {Type, Template}} = Rule|T],
               Context) ->
    case re:run(Username, Re, [{capture, all_but_first, list}]) of
        nomatch -> map_user_to_DN(Username, Settings, T, Context);
        {match, Captured} ->
            ?log_debug("Username->DN: using rule ~p for ~p",
                       [Rule, ns_config_log:tag_user_name(Username)]),
            ReplaceRe = ?cut(lists:flatten(io_lib:format("\\{~b\\}", [_]))),
            Subs = [{ReplaceRe(N), [{default, ldap_util:escape(S)}]} ||
                        {N, S} <- misc:enumerate(Captured, 0)],
            case Type of
                template ->
                    [Res] = ldap_util:replace_expressions([{dn, Template}],
                                                          Subs),
                    {ok, Res, template};
                'query' ->
                    case dn_query(Template, Subs, Settings, Context) of
                        {ok, DN} -> {ok, DN, query};
                        {error, _} = Error -> Error
                    end
            end
    end.

dn_query(QueryTemplate, ReplacePairs, Settings, #{query_handle := Handle}) ->
    Timeout = proplists:get_value(request_timeout, Settings),
    dn_query_with_handle(Handle, QueryTemplate, ReplacePairs, Timeout);
dn_query(QueryTemplate, ReplacePairs, Settings, _) ->
    Timeout = proplists:get_value(request_timeout, Settings),
    with_query_connection(
      Settings,
      fun (Handle) ->
              dn_query_with_handle(Handle, QueryTemplate, ReplacePairs, Timeout)
      end).

dn_query_with_handle(Handle, QueryTemplate, ReplacePairs, Timeout) ->
    case ldap_util:parse_url("ldap:///" ++ QueryTemplate, ReplacePairs) of
        {ok, URLProps} ->
            Base = proplists:get_value(dn, URLProps, ""),
            Scope = proplists:get_value(scope, URLProps, "one"),
            Filter = proplists:get_value(filter, URLProps, "(objectClass=*)"),
            case ldap_util:search(Handle, Base, ["objectClass"], Scope, Filter,
                                  Timeout) of
                {ok, [#eldap_entry{object_name = DN}]} -> {ok, DN};
                {ok, []} -> {error, dn_not_found};
                {ok, [_|_]} -> {error, not_unique_username};
                {error, Reason} -> {error, {dn_search_failed, Reason}}
            end;
        {error, Error} ->
            {error, {ldap_url_parse_error, QueryTemplate, Error}}
    end.

user_groups(User) ->
    user_groups(User, ldap_util:build_settings()).
user_groups(User, Settings) ->
    with_query_connection(
      Settings,
      fun (Handle) ->
              Query = proplists:get_value(groups_query, Settings),
              Res = get_groups(Handle, User, Settings, Query),
              ?log_debug("Groups search for ~p: ~p",
                         [ns_config_log:tag_user_name(User), Res]),
              Res
      end).

get_groups(Handle, Username, Settings, QueryStr) ->
    Timeout = proplists:get_value(request_timeout, Settings),
    GetDN =
        fun () ->
                Context = #{query_handle => Handle},
                case get_user_DN(Username, Settings, Context) of
                    {ok, DN, _} ->
                        [{filter, ldap_util:escape(DN)}, {default, DN}];
                    {error, Reason} ->
                        throw({error, {username_to_dn_map_failed, Reason}})
                end
        end,
    QueryFun =
        fun (G) ->
                Replace = [{"%D", [{filter, ldap_util:escape(G)},
                                   {default, G}]},
                           {"%u", ?cut(throw({error, user_placeholder}))}],
                run_query(Handle, QueryStr, Replace, Timeout)
        end,
    EscapedUser = ldap_util:escape(Username),
    MaxDepth = proplists:get_value(nested_groups_max_depth, Settings),
    FailOnMaxDepth = proplists:get_value(fail_on_max_depth, Settings),
    NestedEnabled = proplists:get_bool(nested_groups_enabled, Settings),
    try
        UserGroups = run_query(Handle, QueryStr,
                               [{"%u", [{default, EscapedUser}]},
                                {"%D", GetDN}], Timeout),
        case NestedEnabled of
            true -> {ok, get_nested_groups(QueryFun, UserGroups, UserGroups,
                                           FailOnMaxDepth, MaxDepth)};
            false -> {ok, UserGroups}
        end
    catch
        throw:{error, _} = Error -> Error
    end.

get_nested_groups(_QueryFun, [], Discovered, _, _MaxDepth) -> Discovered;
get_nested_groups(_QueryFun, _, _, true, 0) -> throw({error, max_depth});
get_nested_groups(_QueryFun, _, Discovered, false, 0) -> Discovered;
get_nested_groups(QueryFun, Groups, Discovered, FailOnMaxDepth, MaxDepth) ->
    NewGroups = lists:flatmap(QueryFun, Groups),
    NewUniqueGroups = lists:usort(NewGroups) -- Discovered,
    ?log_debug("Discovered new groups: ~p (~p)", [NewUniqueGroups, Discovered]),
    get_nested_groups(QueryFun, NewUniqueGroups, NewUniqueGroups ++ Discovered,
                      FailOnMaxDepth, MaxDepth - 1).

run_query(_Handle, undefined, _ReplacePairs, _Timeout) -> [];
run_query(Handle, Query, ReplacePairs, Timeout) ->
    URLProps =
        case ldap_util:parse_url("ldap:///" ++ Query, ReplacePairs) of
            {ok, Props} -> Props;
            {error, Reason} ->
                throw({error, {invalid_groups_query, Query, Reason}})
        end,

    Base = proplists:get_value(dn, URLProps, ""),
    Scope = proplists:get_value(scope, URLProps, "base"),
    Attrs = proplists:get_value(attributes, URLProps, ["objectClass"]),
    Filter = proplists:get_value(filter, URLProps, "(objectClass=*)"),
    case ldap_util:search(Handle, Base, Attrs, Scope, Filter, Timeout) of
        {ok, L} -> groups_search_res(L, search_type(URLProps));
        {error, Reason2} -> throw({error, {ldap_search_failed, Reason2}})
    end.

groups_search_res([], {attribute, _}) -> [];
groups_search_res([#eldap_entry{attributes = Attrs}], {attribute, GroupAttr}) ->
    AttrsLower = [{string:to_lower(K), V} || {K, V} <- Attrs],
    proplists:get_value(string:to_lower(GroupAttr), AttrsLower, []);
groups_search_res([_|_], {attribute, _}) ->
    throw({error, not_unique_username});
groups_search_res(Entries, entries) when is_list(Entries) ->
    [DN || #eldap_entry{object_name = DN} <- Entries].

search_type(URLProps) ->
    case proplists:get_value(attributes, URLProps, []) of
        [] -> entries;
        [Attr] -> {attribute, Attr}
    end.

format_error({ldap_search_failed, Reason}) ->
    io_lib:format("LDAP search returned error: ~s", [format_error(Reason)]);
format_error({connect_failed, _}) ->
    "Can't connect to the server";
format_error({start_tls_failed, _}) ->
    "Failed to use StartTLS extension";
format_error({ldap_url_parse_error, URL, Error}) ->
    io_lib:format("Failed to parse LDAP url ~p (~s)",
                  [URL, format_error(Error)]);
format_error({dn_search_failed, Reason}) ->
    io_lib:format("LDAP search for user distinguished name failed with reason:"
                  " ~s", [format_error(Reason)]);
format_error(dn_not_found) ->
    "LDAP distinguished name not found";
format_error(not_unique_username) ->
    "Search returned more than one entry for given username";
format_error({invalid_filter, Filter, Reason}) ->
    io_lib:format("Invalid LDAP filter ~p (~s)", [Filter, Reason]);
format_error({username_to_dn_map_failed, R}) ->
    io_lib:format("Failed to map username to LDAP distinguished name: ~s",
                  [format_error(R)]);
format_error({invalid_scheme, S}) ->
    io_lib:format("Invalid scheme ~p", [S]);
format_error(malformed_url) ->
    "Malformed LDAP URL";
format_error({invalid_dn, DN, ParseError}) ->
    io_lib:format("Invalid LDAP distinguished name \"~s\": ~s",
                  [DN, format_error(ParseError)]);
format_error({invalid_scope, Scope}) ->
    io_lib:format("Invalid LDAP scope: ~p, possible values are one, "
                  "base or sub", [Scope]);
format_error(user_placeholder) ->
    "%u placeholder is not allowed in nested groups search";
format_error(max_depth) ->
    "Nested search max depth has been reached";
format_error({invalid_groups_query, Query, Reason}) ->
    io_lib:format("Invalid LDAP query: \"~s\". ~s", [Query, format_error(Reason)]);
format_error({bind_failed, DN, Bind}) ->
    io_lib:format("Bind failed for \"~s\". ~s", [DN, format_error(Bind)]);
format_error(invalidCredentials) ->
    "Invalid username or password";
format_error(anonymous_auth) ->
    "Anonymous bind is not supported by LDAP server";
format_error(unwillingToPerform) ->
    "LDAP server cannot process the request because of server-defined "
    "restrictions";
format_error(invalidDNSyntax) ->
    "Invalid LDAP distinguished name syntax";
format_error({parse_error, expecting_attribute_type, Rest}) ->
    io_lib:format("expecting attribute type near '~s'", [Rest]);
format_error({parse_error, invalid_attribute_type, Rest}) ->
    io_lib:format("invalid attribute near '~s', make sure reserved "
                  "characters are escaped in attribute values ", [Rest]);
format_error({parse_error, expecting_double_quote_mark, _}) ->
    "missing closing double quote mark";
format_error({parse_error, expecting_equal_sign, Rest}) ->
    io_lib:format("expecting equal sign after attribute type near '~s', "
                  "make sure reserved characters are escaped in attribute "
                  "values", [Rest]);
format_error({parse_error, starting_comma, _}) ->
    "cannot start with comma";
format_error({parse_error, Err, _}) ->
    io_lib:format("~p", [Err]);
format_error(authMethodNotSupported) ->
    "Authentication method not supported";
format_error(referral_not_supported) ->
    "Referrals are not supported";
format_error(Error) ->
    io_lib:format("~p", [Error]).
