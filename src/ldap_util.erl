%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ldap_util).

-include("ns_common.hrl").
-include_lib("eldap/include/eldap.hrl").
-include("cut.hrl").

%% Remove by OTP25
-compile([{nowarn_deprecated_function, [{ http_uri,parse,2 }]}]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([with_simple_bind/4,
         with_external_bind/2,
         with_connection/2,
         search/6,
         parse_url/1,
         parse_url/2,
         escape/1,
         get_setting/1,
         build_settings/0,
         set_settings/1,
         replace_expressions/2,
         parse_dn/1,
         client_cert_auth_enabled/1]).

ssl_options(Host, Settings) ->
    ClientAuthOpts =
        case client_cert_auth_enabled(Settings) of
            true ->
                ClientCert = proplists:get_value(client_tls_cert, Settings),
                ClientKey = proplists:get_value(client_tls_key, Settings),
                [{cert, DecodedCert} || {_, DecodedCert} <- [ClientCert]] ++
                [{key, {T, D}} || {password, {T, D, _}} <- [ClientKey]];
            false ->
                []
        end,
    PeerVerificationOpts =
        case proplists:get_value(server_cert_validation, Settings) of
            true ->
                [{verify, verify_peer}, {cacerts, get_cacerts(Settings)},
                 {server_name_indication, Host}, {log_alert, true},
                 {depth, ?ALLOWED_CERT_CHAIN_LENGTH}];
            false ->
                [{verify, verify_none}]
        end,
    ExtraOptsUnprepared =
        case proplists:get_value(extra_tls_opts, Settings) of
            undefined -> []; %% not a default, but value == undefined
            L -> L
        end,
    %% Remove {password, _} wrap.
    %% In case if a value in opts contains sensitive information (like
    %% a password or a private key) it might be protected by such a wrap.
    %% That would prevent the value from being printed to logs or returned as
    %% an API response. We need to drop it before use, because ssl knows nothing
    %% about it.
    ExtraOpts = lists:map(
                  fun ({K, {password, V}}) -> {K, V};
                      (KV) -> KV
                  end, ExtraOptsUnprepared),
    misc:update_proplist(ClientAuthOpts ++ PeerVerificationOpts, ExtraOpts).

client_cert_auth_enabled(Settings) ->
    Encryption = proplists:get_value(encryption, Settings),
    EncryptionEnabled = (Encryption == 'TLS') or
                        (Encryption == 'StartTLSExtension'),
    case EncryptionEnabled of
        true ->
            case proplists:get_value(client_tls_cert, Settings) of
                {_, _} -> true;
                _ -> false
            end;
        false -> false
    end.

get_cacerts(Settings) ->
    ExtraCerts =
        case proplists:get_value(cacert, Settings) of
            {_Cert, DecodedCert} -> [DecodedCert];
            undefined -> []
        end,
    ExtraCerts ++ ns_server_cert:trusted_CAs(der).

%% Can't just pass the list of hosts to eldap:open/2 because it is impossible
%% to get the peer's hostname later, so we have to iterate over the list
%% of hosts and memorize the one we were able to connect.
%% We need the peer's hostname information for server name validation
%% later in StartTLS
open_ldap_connection([], _Port, _SSL, _Timeout, _Settings) ->
    {error,"connect failed"};
open_ldap_connection([Host|Hosts], Port, SSL, Timeout, Settings) ->
    SSLOpts = case SSL of
                  true -> [{ssl, true}, {sslopts, ssl_options(Host, Settings)}];
                  false -> []
              end,
    %% Note: timeout option sets not only connect timeout but a timeout for any
    %%       request to ldap server
    Opts = [{port, Port}, {timeout, Timeout} | SSLOpts],
    case do_open_ldap_connection(Host, Opts) of
        {ok, Handle} -> {ok, Handle, Host};
        {error, _} -> open_ldap_connection(Hosts, Port, SSL, Timeout, Settings)
    end.

do_open_ldap_connection(Host, Opts) ->
    ToTry = case {misc:is_raw_ip(Host), misc:is_raw_ipv6(Host)} of
                {_, true} ->
                    [inet6];
                {true, _} ->
                    [inet];
                _ ->
                    case misc:get_net_family() of
                        inet ->
                            [inet, inet6];
                        inet6 ->
                            [inet6, inet]
                    end
            end,
    lists:foldl(fun (_Afamily, {ok, Handle}) ->
                        {ok, Handle};
                    (Afamily, _) ->
                        eldap:open([Host], [{tcpopts, [Afamily]} | Opts])
                end, undefined, ToTry).

with_connection(Settings, Fun) ->
    Hosts = proplists:get_value(hosts, Settings),
    Port = proplists:get_value(port, Settings),
    Timeout = proplists:get_value(request_timeout, Settings),
    Encryption = proplists:get_value(encryption, Settings),
    SSL = Encryption == 'TLS',

    case open_ldap_connection(Hosts, Port, SSL, Timeout, Settings) of
        {ok, Handle, Host} ->
            ?log_debug("Connected to LDAP server: ~p (port: ~p, SSL: ~p)",
                       [Host, Port, SSL]),
            try
                %% The upgrade is done in two phases: first the server is asked
                %% for permission to upgrade. Second, if the request is
                %% acknowledged, the upgrade to tls is performed.
                %% The Timeout parameter is for the actual tls upgrade (phase 2)
                %% while the timeout in eldap:open/2 is used for the initial
                %% negotiation about upgrade (phase 1).
                case Encryption == 'StartTLSExtension' andalso
                     eldap:start_tls(Handle,
                                     ssl_options(Host, Settings),
                                     Timeout) of
                    Res when Res == ok; Res == false ->
                        Fun(Handle);
                    {error, Reason} ->
                        ?log_error("LDAP TLS start failed: ~p", [Reason]),
                        {error, {start_tls_failed, Reason}}
                end
            after
                eldap:close(Handle)
            end;
        {error, Reason} ->
            ?log_error("Connect to ldap ~p (port: ~p, SSL: ~p} failed: ~p",
                       [Hosts, Port, SSL, Reason]),
            {error, {connect_failed, Reason}}
    end.

with_external_bind(Settings, Fun) ->
    with_connection(Settings,
                    fun (Handle) ->
                            Bind = eldap:sasl_external_bind(Handle),
                            ?log_debug("SASL EXTERNAL bind res: ~p", [Bind]),
                            case Bind of
                                ok -> Fun(Handle);
                                {ok, {referral, _}} ->
                                    {error, referral_not_supported};
                                {error, Error} ->
                                    {error, {bind_failed, "<external>", Error}}
                            end
                    end).

with_simple_bind(DN, [], _Settings, _Fun) when DN =/= [] ->
    %% Prevent "Unauthenticated Authentication Mechanism of Simple Bind".
    %% Since we use simple_bind for authentication purposes only we should
    %% prevent use of empty passwords.
    %% See, https://datatracker.ietf.org/doc/html/rfc4513#section-5.1.2, for
    %% more info.
    ?log_debug("Simple bind prohibited for DN ~p with empty password",
               [ns_config_log:tag_user_name(DN)]),
    {error, unwillingToPerform};
with_simple_bind(DN, Password, Settings, Fun) ->
    with_connection(Settings,
                    fun (Handle) ->
                            PasswordBin = iolist_to_binary(Password),
                            Bind = eldap:simple_bind(Handle, DN, PasswordBin),
                            ?log_debug("Simple bind for DN ~p: ~p",
                                       [ns_config_log:tag_user_name(DN), Bind]),
                            case Bind of
                                ok -> Fun(Handle);
                                {ok, {referral, _}} ->
                                    {error, referral_not_supported};
                                {error, Error} ->
                                    {error, {bind_failed, DN, Error}}
                            end
                    end).

get_setting(Prop) ->
    proplists:get_value(Prop, build_settings()).

default_settings() ->
    [{authentication_enabled, false},
     {authorization_enabled, false},
     {hosts, []},
     {port, 389},
     {encryption, 'None'},
     {user_dn_mapping, {'None', []}},
     {bind_dn, ""},
     {bind_pass, {password, ""}},
     {client_tls_cert, undefined},
     {client_tls_key, undefined},
     {groups_query, undefined},
     {max_parallel_connections, 100},
     {max_cache_size, 10000},
     {request_timeout, 5000},
     {nested_groups_enabled, false},
     {nested_groups_max_depth, 10},
     {fail_on_max_depth, false},
     {cache_value_lifetime,
      round(0.5*menelaus_roles:external_auth_polling_interval())},
     {cacert, undefined},
     {server_cert_validation, true},
     {bind_method, undefined},
     {extra_tls_opts, undefined}].

build_settings() ->
    case ns_config:search(ldap_settings) of
        {value, Settings} ->
            misc:update_proplist(default_settings(), Settings);
        false ->
            default_settings()
    end.

set_settings(Settings) ->
    OldProps = ns_config:read_key_fast(ldap_settings, []),
    ns_config:set(ldap_settings, misc:update_proplist(OldProps, Settings)).

parse_scope("base") -> eldap:baseObject();
parse_scope("one") -> eldap:singleLevel();
parse_scope("sub") -> eldap:wholeSubtree().

search(Handle, Base, Attributes, Scope, Filter, Timeout) ->
    case ldap_filter_parser:parse(Filter) of
        {ok, EldapFilter} ->
            %% The timeout option in the SearchOptions is for the ldap server,
            %% while the timeout in eldap:open/2 is used for each individual
            %% request in the search operation
            SearchProps = [{base, Base}, {attributes, Attributes},
                           {scope, parse_scope(Scope)}, {filter, EldapFilter},
                           {timeout, Timeout}],
            eldap_search(Handle, SearchProps);
        {error, E} ->
            {error, {invalid_filter, Filter, E}}
    end.

eldap_search(Handle, SearchProps) ->
    case eldap:search(Handle, SearchProps) of
        {ok, #eldap_search_result{
                entries = Entries,
                referrals = Refs}} ->
            Refs == [] orelse ?log_error("LDAP search continuations are not "
                                         "supported yet, ignoring: ~p", [Refs]),
            ?log_debug("LDAP search returned ~b entries", [length(Entries)]),
            {ok, Entries};
        {ok, {referral, Refs}} ->
            ?log_error("LDAP referrals are not supported yet, ignoring: ~p",
                       [Refs]),
            {ok, []};
        {error, Reason} ->
            ?log_error("LDAP search failed: ~p", [Reason]),
            {error, Reason}
    end.

parse_url(Template) ->
    parse_url(Template, []).

%% RFC4516 ldap url parsing
parse_url(Bin, ReplacePairs) when is_binary(Bin) ->
    parse_url(binary_to_list(Bin), ReplacePairs);
parse_url(Str, ReplacePairs) ->
    SchemeValidator = fun (S) ->
                              case string:to_lower(S) of
                                  "ldap" -> valid;
                                  _ -> {error, {invalid_scheme, S}}
                              end
                      end,
    try
        {Scheme, _UserInfo, Host, Port, "/" ++ EncodedDN, Query} =
            case http_uri:parse(Str, [{scheme_defaults, [{ldap, 389}]},
                                 {scheme_validation_fun, SchemeValidator}]) of
                {ok, R} -> R;
                {error, _} -> throw({error, malformed_url})
            end,
        [[], AttrsStr, Scope, FilterEncoded, Extensions | _] =
            string:split(Query ++ "?????", "?", all),

        [EncodedDN2, AttrsStr2, Scope2, FilterEncoded2, Extensions2] =
            replace_expressions([{base, EncodedDN}, {attrs, AttrsStr},
                                 {scope, Scope}, {filter, FilterEncoded},
                                 {extensions, Extensions}], ReplacePairs),

        Attrs = [mochiweb_util:unquote(A) || A <- string:tokens(AttrsStr2, ",")],

        DN = mochiweb_util:unquote(EncodedDN2),
        case parse_dn(DN) of
            {ok, _} -> ok;
            {error, Err} -> throw({error, {invalid_dn, DN, Err}})
        end,

        ScopeLower = string:lowercase(Scope2),
        try
            ScopeLower =:= "" orelse parse_scope(ScopeLower)
        catch
            _:_ -> throw({error, {invalid_scope, Scope2}})
        end,

        Filter = mochiweb_util:unquote(FilterEncoded2),
        case Filter =:= "" orelse ldap_filter_parser:parse(Filter) of
            true -> ok;
            {ok, _} -> ok;
            {error, Reason2} ->
                throw({error, {invalid_filter, Filter, Reason2}})
        end,

        {ok,
         [{scheme, Scheme}] ++
         [{host, Host} || Host =/= ""] ++
         [{port, Port}] ++
         [{dn, DN} || DN =/= ""] ++
         [{attributes, Attrs} || Attrs =/= []] ++
         [{scope, ScopeLower} || ScopeLower =/= ""] ++
         [{filter, Filter} || Filter =/= ""] ++
         [{extensions, Extensions2} || Extensions2 =/= ""]
        }
    catch
        throw:{error, _} = Error -> Error
    end.

%% for some reason eldap doesn't handle this case correctly
parse_dn("," ++ Rest) -> {error, {parse_error, starting_comma, Rest}};
parse_dn(DN) ->
    case eldap:parse_dn(DN) of
        {ok, Res} -> {ok, Res};
        {parse_error, _, _} = Error -> {error, Error}
    end.

-define(ALLOWED_CHARS, "abcdefghijklmnopqrstuvwxyz"
                       "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                       "0123456789 ").

%% Escapes any special chars (RFC 4515) from a string representing a
%% a search filter assertion value
%% Based on https://ldap.com/2018/05/04/understanding-and-defending-against-ldap-injection-attacks/
escape(Input) ->
    Characters = unicode:characters_to_list(iolist_to_binary(Input)),
    S = lists:map(
          fun (C) ->
                  case lists:member(C, ?ALLOWED_CHARS) of
                      true -> C;
                      false ->
                          Hex = misc:hexify(unicode:characters_to_binary([C])),
                          [[$\\, H, L] || <<H:8, L:8>> <= Hex]
                  end
          end, Characters),

    FlatStr = lists:flatten(S),

    %% If a value has any leading or trailing spaces, then you should escape
    %% those spaces by prefixing them with a backslash or as \20. Spaces in
    %% the middle of a value don’t need to be escaped.
    {Trail, Rest} = lists:splitwith(_ =:= $\s, lists:reverse(FlatStr)),
    {Lead, Rest2} = lists:splitwith(_ =:= $\s, lists:reverse(Rest)),
    lists:flatten(["\\20" || _ <- Lead] ++ Rest2 ++ ["\\20" || _ <- Trail]).

replace_expressions(Strings, Substitutes) ->
    Res = lists:foldl(
            fun ({Re, ValueFun}, Acc) ->
                replace(Acc, Re, ValueFun, [])
            end, Strings, Substitutes),
    [Str || {_, Str} <- Res].

replace([], _, _, Res) -> lists:reverse(Res);
replace([{Type, Str} | Tail], Re, Fun, Res) when is_function(Fun) ->
    case re:run(Str, Re, [{capture, none}]) of
        match -> replace([{Type, Str} | Tail], Re, Fun(), Res);
        nomatch -> replace(Tail, Re, Fun, [{Type, Str} | Res])
    end;
replace([{Type, Str} | Tail], Re, ValuesPropList, Res) ->
    %% Replace \ with \\ to prevent re skipping all hex bytes from Value
    %% which are specified as \\XX according to LDAP RFC. Example:
    %%   Str = "(uid=%u)",
    %%   Re  = "%u",
    %%   Value = "abc\\23def",
    %%   Without replacing the result is "(uid=abcdef)"
    %%   With replacing it is "(uid=abc\\23def)"
    Value = proplists:get_value(Type, ValuesPropList,
                                proplists:get_value(default, ValuesPropList)),
    Value2 = re:replace(Value, "\\\\", "\\\\\\\\", [{return, list}, global]),
    ResStr = re:replace(Str, Re, Value2, [global, {return, list}]),
    replace(Tail, Re, ValuesPropList, [{Type, ResStr} | Res]).


-ifdef(TEST).
escape_test() ->
%% Examples from RFC 4515
    ?assertEqual("Parens R Us \\28for all your parenthetical needs\\29",
                 escape("Parens R Us (for all your parenthetical needs)")),
    ?assertEqual("\\2a", escape("*")),
    ?assertEqual("C\\3a\\5cMyFile", escape("C:\\MyFile")),
    ?assertEqual("\\00\\00\\00\\04", escape([16#00, 16#00, 16#00, 16#04])),
    ?assertEqual("Lu\\c4\\8di\\c4\\87", escape(<<"Lučić"/utf8>>)).

parse_url_test_() ->
    Parse = fun (S) -> {ok, R} = parse_url(S), R end,
    [
        ?_assertEqual([{scheme, ldap}, {port, 389}], Parse("ldap://")),
        ?_assertEqual([{scheme, ldap}, {host, "127.0.0.1"}, {port, 389}],
                      Parse("ldap://127.0.0.1")),
        ?_assertEqual([{scheme, ldap}, {host, "127.0.0.1"}, {port, 636}],
                      Parse("ldap://127.0.0.1:636")),
        ?_assertEqual([{scheme, ldap}, {host, "127.0.0.1"}, {port, 636},
                       {dn,"uid=al,ou=users,dc=example"}],
                      Parse("ldap://127.0.0.1:636"
                            "/uid%3Dal%2Cou%3Dusers%2Cdc%3Dexample")),
        ?_assertEqual([{scheme, ldap}, {host, "127.0.0.1"}, {port, 636},
                       {dn,"uid=al,ou=users,dc=example"}],
                      Parse("ldap://127.0.0.1:636"
                            "/uid%3Dal%2Cou%3Dusers%2Cdc%3Dexample")),
        ?_assertEqual([{scheme, ldap}, {host, "127.0.0.1"}, {port, 636},
                       {dn,"uid=al,ou=users,dc=example"},
                       {attributes, ["attr1", "attr2"]}],
                      Parse("ldap://127.0.0.1:636"
                            "/uid%3Dal%2Cou%3Dusers%2Cdc%3Dexample"
                            "?attr1,attr2")),
        ?_assertEqual([{scheme, ldap}, {host, "127.0.0.1"}, {port, 636},
                       {dn,"uid=al,ou=users,dc=example"},
                       {attributes, ["attr1", "attr2"]},
                       {scope, "base"},
                       {filter, "(!(&(uid=%u)(email=%u@%d)))"}],
                      Parse("ldap://127.0.0.1:636"
                            "/uid%3Dal%2Cou%3Dusers%2Cdc%3Dexample?attr1,attr2"
                            "?base?%28%21%28%26%28uid%3D%25u%29%28"
                            "email%3D%25u%40%25d%29%29%29")),

%% Tests from RFC4516 examples:
        ?_assertEqual([{scheme, ldap}, {port, 389},
                       {dn, "o=University of Michigan,c=US"}],
                      Parse("ldap:///o=University%20of%20Michigan,c=US")),
        ?_assertEqual([{scheme, ldap}, {host, "ldap1.example.net"}, {port, 389},
                       {dn, "o=University of Michigan,c=US"}],
                      Parse("ldap://ldap1.example.net"
                            "/o=University%20of%20Michigan,c=US")),
        ?_assertEqual([{scheme, ldap}, {host, "ldap1.example.net"}, {port, 389},
                       {dn, "o=University of Michigan,c=US"},
                       {scope, "sub"}, {filter, "(cn=Babs Jensen)"}],
                      Parse("ldap://ldap1.example.net/o=University%20of%2"
                             "0Michigan,c=US??sub?(cn=Babs%20Jensen)")),
        ?_assertEqual([{scheme, ldap}, {host, "ldap1.example.com"}, {port, 389},
                       {dn, "c=GB"}, {attributes, ["objectClass"]},
                       {scope, "one"}],
                      Parse("LDAP://ldap1.example.com/c=GB?objectClass?ONE")),
        ?_assertEqual([{scheme, ldap}, {host, "ldap2.example.com"}, {port, 389},
                       {dn, "o=Question?,c=US"}, {attributes, ["mail"]}],
                      Parse("ldap://ldap2.example.com"
                            "/o=Question%3f,c=US?mail")),
        ?_assertEqual([{scheme, ldap}, {host, "ldap3.example.com"}, {port, 389},
                       {dn, "o=Babsco,c=US"},
                       {filter, "(four-octet=\\00\\00\\00\\04)"}],
                      Parse("ldap://ldap3.example.com/o=Babsco,c=US"
                            "???(four-octet=%5c00%5c00%5c00%5c04)")),
        ?_assertEqual([{scheme, ldap}, {host, "ldap.example.com"}, {port, 389},
                       {dn, "o=An Example\\2C Inc.,c=US"}],
                      Parse("ldap://ldap.example.com"
                            "/o=An%20Example%5C2C%20Inc.,c=US"))
    ].
-endif.
