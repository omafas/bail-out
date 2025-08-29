%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(ns_server_cert).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-include_lib("public_key/include/public_key.hrl").
-include_lib("public_key/include/PKCS-FRAME.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([decode_cert_chain/1,
         decode_single_certificate/1,
         generate_cluster_CA/2,
         this_node_uses_self_generated_certs/0,
         this_node_uses_self_generated_certs/1,
         this_node_uses_self_generated_client_certs/0,
         this_node_uses_self_generated_client_certs/1,
         self_generated_ca/0,
         load_certs_from_inbox/2,
         load_certs_from_inbox/3,
         is_cert_loaded_from_file/1,
         load_CAs_from_inbox/0,
         add_CAs/2,
         add_CAs/3,
         remove_CA/1,
         get_warnings/0,
         get_subject_fields_by_type/2,
         get_sub_alt_names_by_type/2,
         get_cert_info/2,
         set_generated_ca/1,
         validate_pkey/2,
         get_chain_info/2,
         trusted_CAs/1,
         generate_certs/2,
         filter_nodes_by_ca/3,
         inbox_chain_path/1,
         expiration_warnings/1,
         split_certs/1,
         cert_props/1,
         cert_expiration_warning_days/0,
         extract_internal_client_cert_user/1,
         invalid_client_cert_nodes/3,
         verify_cert_hostname_strict/2,
         encrypt_pkey/2,
         chronicle_upgrade_to_phoenix/1]).

inbox_ca_path() ->
    filename:join(path_config:component_path(data, "inbox"), "CA").

inbox_chain_path(node_cert) ->
    filename:join(path_config:component_path(data, "inbox"), "chain.pem");
inbox_chain_path(client_cert) ->
    filename:join(path_config:component_path(data, "inbox"),
                  "client_chain.pem").

inbox_pkey_path(node_cert) ->
    filename:join(path_config:component_path(data, "inbox"), "pkey.key");
inbox_pkey_path(client_cert) ->
    filename:join(path_config:component_path(data, "inbox"), "client_pkey.key").

inbox_p12_path(node_cert) ->
    filename:join(path_config:component_path(data, "inbox"), "couchbase.p12");
inbox_p12_path(client_cert) ->
    filename:join(path_config:component_path(data, "inbox"),
                  "couchbase_client.p12").

this_node_uses_self_generated_certs() ->
    this_node_uses_self_generated_certs(ns_config:latest()).

this_node_uses_self_generated_certs(Config) ->
    CertProps = ns_config:search(Config, {node, node(), node_cert}, []),
    generated == proplists:get_value(type, CertProps).

this_node_uses_self_generated_client_certs() ->
    this_node_uses_self_generated_client_certs(ns_config:latest()).

this_node_uses_self_generated_client_certs(Config) ->
    CertProps = ns_config:search(Config, {node, node(), client_cert}, []),
    generated == proplists:get_value(type, CertProps).

self_generated_ca() ->
    case chronicle_kv:get(kv, root_cert_and_pkey) of
        {ok, {{CA, _}, _}} -> CA;
        {error, not_found} ->
            {CA, _} = ensure_cluster_CA(),
            CA
    end.

self_generated_ca_and_pkey() ->
    case chronicle_kv:get(kv, root_cert_and_pkey) of
        {ok, {Pair, _}} -> Pair;
        {error, not_found} -> ensure_cluster_CA()
    end.

ensure_cluster_CA() ->
    generate_cluster_CA(false, false).

generate_cluster_CA(ForceRegenerateCA, DropUploadedCerts) ->
    NewPair = generate_cert_and_pkey(),
    {ok, AddCA} = add_CAs_txn_fun(generated, element(1, NewPair), []),
    ReadEpoch =
        fun (Txn) ->
                case chronicle_kv:txn_get(cluster_certs_epoch, Txn) of
                    {ok, {N, _}} -> N;
                    {error, not_found} -> 0
                end
        end,
    {ok, _, Pair} =
        chronicle_kv:txn(
          kv,
          fun (Txn) ->
                  case chronicle_kv:txn_get(root_cert_and_pkey, Txn) of
                      {ok, {{_, OldKey} = OldPair, _}}
                        when not ForceRegenerateCA,
                             not DropUploadedCerts,
                             OldKey /= undefined ->
                          {abort, {ok, undefined, OldPair}};
                      {ok, {{OldCert, OldKey} = OldPair, _}}
                        when not ForceRegenerateCA,
                             OldKey /= undefined ->
                          %% In case the CA cert is not trusted, we attempt to
                          %% add it here. Note that add_CAs_txn_fun will check
                          %% for the cert already being trusted, so there's no
                          %% need for such a check here
                          {ok, AddOldCA} =
                              add_CAs_txn_fun(generated, OldCert, []),
                          {commit, Changes0, _} = AddOldCA(Txn),
                          Epoch = ReadEpoch(Txn) + 1,
                          Changes1 = [{set, cluster_certs_epoch, Epoch}],
                          {commit, Changes0 ++ Changes1, OldPair};
                      _ ->
                          Changes0 =
                              case DropUploadedCerts of
                                  true ->
                                      Epoch = ReadEpoch(Txn) + 1,
                                      [{set, cluster_certs_epoch, Epoch}];
                                  false ->
                                      []
                              end,
                          Changes1 = [{set, root_cert_and_pkey, NewPair}],
                          {commit, Changes2, _} = AddCA(Txn),
                          Changes = Changes0 ++ Changes1 ++ Changes2,
                          {commit, Changes, NewPair}
                  end
          end),
    Pair.

generate_cert_and_pkey() ->
    StartTS = os:timestamp(),
    Sha1 = ns_config:read_key_fast({cert, use_sha1}, false),
    RV = generate_certs(#{use_sha1 => Sha1,
                          common_name_prefix =>
                              cluster_compat_mode:prod_name()}),
    EndTS = os:timestamp(),

    Diff = timer:now_diff(EndTS, StartTS),
    ?log_debug("Generated certificate and private key in ~p us", [Diff]),

    RV.

generate_certs(Type, Arg) ->
    case self_generated_ca_and_pkey() of
        {_CAPem, undefined} ->
            no_private_key;
        {CAPem, _} = CACerts ->
            {Cert, Key} = generate_certs(Type, Arg, CACerts),
            {CAPem, Cert, Key}
    end.

generate_certs(node_cert, Host, CACerts) ->
    SanArg = case misc:is_raw_ip(Host) of
                 true -> san_ip_addrs;
                 false -> san_dns_names
             end,
    %% CN can't be longer than 64 characters. Since it will be used for
    %% displaying purposing only, it doesn't make sense to make it even
    %% that long
    HostShortened = case string:slice(Host, 0, 20) of
                        Host -> Host;
                        Shortened -> Shortened ++ "..."
                    end,
    CommonName = lists:flatten(io_lib:format("~s Node (~s)",
                                             [cluster_compat_mode:prod_name(),
                                              HostShortened])),
    generate_certs(
      #{client => false,
        common_name => CommonName,
        common_name_prefix => cluster_compat_mode:prod_name(),
        generate_leaf => CACerts,
        SanArg => [Host]});
generate_certs(client_cert, "@" ++ Name, CACerts) ->
    N = integer_to_list(erlang:phash2(erlang:system_time())),
    Opts0 = #{client => true,
              common_name => "Couchbase Internal Client (" ++ N ++ ")",
              common_name_prefix => cluster_compat_mode:prod_name(),
              generate_leaf => CACerts,
              san_emails => [Name ++ "@"?INTERNAL_CERT_EMAIL_DOMAIN]},
    Opts1 =
        case ns_config:read_key_fast({client_cert, not_after_duration_s},
                                     undefined) of
            undefined -> Opts0;
            Duration -> Opts0#{not_after_duration => Duration}
        end,
    generate_certs(Opts1).

generate_certs(Cert) when is_map(Cert) ->
    {Args, Env} =
        maps:fold(
          fun (common_name, CN, {A, E}) ->
                  {["--common-name=" ++ CN | A], E};
              (common_name_prefix, CNP, {A, E}) ->
                  {["--common-name-prefix=" ++ CNP | A], E};
              (client, true, {A, E}) ->
                  {["--client" | A], E};
              (client, false, {A, E}) ->
                  {A, E};
              (generate_leaf, {CAPem, PKey}, {A, E}) ->
                  {["--generate-leaf" | A],
                   [{"CACERT", binary_to_list(CAPem)},
                    {"CAPKEY", binary_to_list(PKey)} | E]};
              (san_emails, Emails, {A, E}) ->
                  EmailsStr = lists:flatten(lists:join(",", Emails)),
                  {["--san-emails=" ++ EmailsStr | A], E};
              (san_ip_addrs, Addrs, {A, E}) ->
                  AddrsStr = lists:flatten(lists:join(",", Addrs)),
                  {["--san-ip-addrs=" ++ AddrsStr | A], E};
              (san_dns_names, Names, {A, E}) ->
                  NamesStr = lists:flatten(lists:join(",", Names)),
                  {["--san-dns-names=" ++ NamesStr | A], E};
              (use_sha1, true, {A, E}) ->
                  {["--use-sha1" | A], E};
              (use_sha1, false, {A, E}) ->
                  {A, E};
              (not_after_duration, Duration, {A, E}) ->
                  DurationStr = integer_to_list(Duration),
                  {["--not-after-duration=" ++ DurationStr | A], E}
          end, {[], []}, Cert),

    do_generate_cert_and_pkey(Args, Env).

do_generate_cert_and_pkey(Args, Env) ->
    {Status, Output} = misc:run_external_tool(path_config:component_path(bin, "generate_cert"), Args, Env),
    case Status of
        0 ->
            extract_cert_and_pkey(Output);
        _ ->
            erlang:exit({bad_generate_cert_exit, Status, Output})
    end.

decode_cert_chain(CertPemBin) ->
    try split_certs(CertPemBin) of
        Certs -> decode_cert_chain(Certs, [])
    catch
        _:_ -> {error, malformed_cert}
    end.

decode_cert_chain([], Res) -> {ok, lists:reverse(Res)};
decode_cert_chain([Cert | Tail], Res) ->
    case decode_single_certificate(Cert) of
        {ok, Der} -> decode_cert_chain(Tail, [Der | Res]);
        {error, _} = Err -> Err
    end.

decode_single_certificate(CertPemBin) ->
    case do_decode_certificates(CertPemBin) of
        malformed_cert ->
            {error, malformed_cert};
        [PemEntry] ->
            validate_cert_pem_entry(PemEntry);
        [] ->
            {error, malformed_cert};
        [_|_] ->
            {error, too_many_entries}
    end.

decode_certificates(CertPemBin) ->
    case do_decode_certificates(CertPemBin) of
        malformed_cert ->
            {error, malformed_cert};
        PemEntries ->
            lists:foldl(
              fun (_E, {error, R}) -> {error, R};
                  (E, {ok, Acc}) ->
                      case validate_cert_pem_entry(E) of
                          {ok, DerCert} -> {ok, [DerCert | Acc]};
                          {error, R} -> {error, R}
                      end
              end, {ok, []}, PemEntries)
    end.

do_decode_certificates(CertPemBin) ->
    try
        public_key:pem_decode(CertPemBin)
    catch T:E:S ->
            ?log_error("Unknown error while parsing certificate:~n~p",
                       [{T, E, S}]),
            malformed_cert
    end.

validate_cert_pem_entry({'Certificate', Der, not_encrypted}) ->
    {ok, Der};
validate_cert_pem_entry({'Certificate', _, _}) ->
    {error, encrypted_certificate};
validate_cert_pem_entry({BadType, _, _}) ->
    {error, {invalid_certificate_type, BadType}}.

-define(SUPPORTED_PKEY_TYPE(T), (T) == 'RSAPrivateKey';
                                (T) == 'DSAPrivateKey';
                                (T) == 'ECPrivateKey').


encrypt_pkey(PKeyPemBin, Pass) ->
    %% We expect the key to be valid and unencrypted here
    [Entry] = public_key:pem_decode(PKeyPemBin),
    Entity = public_key:pem_entry_decode(Entry),
    PemEntry = public_key:pem_entry_encode('PrivateKeyInfo', Entity,
                                           {cipher_info(), ?UNHIDE(Pass)}),
    public_key:pem_encode([PemEntry]).

cipher_info() ->
    ASN1OctetStrTag = 4,
    IVLen = 16,
    IV = crypto:strong_rand_bytes(IVLen),
    Params = <<ASN1OctetStrTag, IVLen:8/unsigned-big-integer, IV/binary>>,
    EncryptionScheme = #'PBES2-params_encryptionScheme'{
                         algorithm = ?'id-aes256-CBC',
                         parameters = {asn1_OPENTYPE, Params}
                       },
    KDF = #'PBES2-params_keyDerivationFunc'{
            algorithm = ?'id-PBKDF2',
            parameters = #'PBKDF2-params'{
                           salt = {specified, crypto:strong_rand_bytes(32)},
                           iterationCount = 2048,
                           prf = #'PBKDF2-params_prf'{
                                   algorithm = ?'id-hmacWithSHA1',
                                   parameters = 'NULL'
                                 }
                         }
          },

    {"AES-256-CBC", #'PBES2-params'{
                      keyDerivationFunc = KDF,
                      encryptionScheme = EncryptionScheme
                    }}.

validate_pkey(PKeyPemBin, PassFun) ->
    try public_key:pem_decode(PKeyPemBin) of
        [{Type, _, not_encrypted} = Entry] ->
            case Type of
                'PrivateKeyInfo' ->
                    try element(1, public_key:pem_entry_decode(Entry)) of
                        T when ?SUPPORTED_PKEY_TYPE(T) ->
                            {ok, Entry};
                        Other ->
                            ?log_debug("Invalid pkey type: ~p", [Other]),
                            {error, {invalid_pkey, Other}}
                    catch
                        _:Ex:ST ->
                            ?log_error("Failed to decode pem entry: ~p~n~p",
                                       [Ex, ST]),
                            {error, malformed_pkey}
                    end;
                _ when ?SUPPORTED_PKEY_TYPE(Type) ->
                    {ok, Entry};
                Other ->
                    ?log_debug("Invalid pkey type: ~p", [Other]),
                    {error, {invalid_pkey, Type}}
            end;
        [{_Type, _, CipherInfo} = Entry] ->
            try {supported_pkey_cipher(CipherInfo),
                 element(1, public_key:pem_entry_decode(Entry, PassFun))} of
                {true, T} when ?SUPPORTED_PKEY_TYPE(T) ->
                    {ok, Entry};
                {false, _} ->
                    ?log_error("Unsupported pkey cipher: ~p", [CipherInfo]),
                    {error, {invalid_pkey_cipher, CipherInfo}};
                {true, Other} ->
                    ?log_debug("Invalid pkey type: ~p", [Other]),
                    {error, {invalid_pkey, Other}}
            catch
                _:_ ->
                    ?log_error("Could not decrypt private key, password might "
                               "be wrong"),
                    {error, could_not_decrypt}
            end;
        [] ->
            {error, malformed_pkey};
        Other ->
            ?log_debug("Too many (~p) pkey entries.", [length(Other)]),
            {error, too_many_pkey_entries}
    catch T:E:S ->
            ?log_error("Unknown error while parsing private key:~n~p",
                       [{T, E, S}]),
            {error, malformed_pkey}
    end.

%% Support PKCS-5v2 only
supported_pkey_cipher({_Name, #'PBES2-params'{}}) -> true;
supported_pkey_cipher({_Name, _}) -> false.

validate_cert_and_pkey({'Certificate', DerCert, not_encrypted},
                       PKey, PassphraseFun) ->
    case validate_pkey(PKey, PassphraseFun) of
        {ok, DerKey} ->
            DecodedKey = public_key:pem_entry_decode(DerKey, PassphraseFun),

            Msg = <<"1234567890">>,
            Signature = public_key:sign(Msg, sha, DecodedKey),
            verify_signature(Msg, sha, Signature, DerCert);
        Err ->
            Err
    end.

verify_signature(Msg, DigestType, Signature, DerCert) ->
    DecodedCert = public_key:pkix_decode_cert(DerCert, otp),

    TBSCert = DecodedCert#'OTPCertificate'.tbsCertificate,
    PublicKeyInfo = TBSCert#'OTPTBSCertificate'.subjectPublicKeyInfo,
    Verify =
        fun (PublicKey) ->
            case public_key:verify(Msg, DigestType, Signature, PublicKey) of
                true -> ok;
                false -> {error, cert_pkey_mismatch}
            end
        end,
    case PublicKeyInfo#'OTPSubjectPublicKeyInfo'.subjectPublicKey of
        #'ECPoint'{} = ECP ->
            case PublicKeyInfo#'OTPSubjectPublicKeyInfo'.algorithm of
                #'PublicKeyAlgorithm'{parameters = Params} ->
                    Verify({ECP, Params});
                _ ->
                    ?log_debug("Failed to extract EC parameters from "
                               "PublicKeyInfo: ~p", [PublicKeyInfo]),
                    {error, no_ec_parameters}
            end;
        PK ->
            Verify(PK)
    end.


split_certs(PEMCerts) ->
    Begin = <<"-----BEGIN">>,
    [<<>> | Parts0] = binary:split(PEMCerts, Begin, [global]),
    [<<Begin/binary,P/binary>> || P <- Parts0].

extract_cert_and_pkey(Output) ->
    case split_certs(Output) of
        [Cert, PKey] ->
            case decode_single_certificate(Cert) of
                {ok, _} ->
                    %% We assume this function is used for self-generated
                    %% certs only, hence no password is used
                    case validate_pkey(PKey, fun () -> undefined end) of
                        {ok, _} ->
                            {Cert, PKey};
                        Err ->
                            erlang:exit({bad_generated_pkey, PKey, Err})
                    end;
                {error, Error} ->
                    erlang:exit({bad_generated_cert, Cert, Error})
            end;
        Parts ->
            erlang:exit({bad_generate_cert_output, Parts})
    end.

attribute_string(?'id-at-countryName') ->
    "C";
attribute_string(?'id-at-stateOrProvinceName') ->
    "ST";
attribute_string(?'id-at-localityName') ->
    "L";
attribute_string(?'id-at-organizationName') ->
    "O";
attribute_string(?'id-at-organizationalUnitName') ->
    "OU";
attribute_string(?'id-at-commonName') ->
    "CN";
attribute_string(_) ->
    undefined.

format_attribute(MultiAttrs, Acc) when is_list(MultiAttrs) ->
    Format = fun (#'AttributeTypeAndValue'{type = Type, value = Value}) ->
                 case attribute_string(Type) of
                     undefined -> false;
                     Str -> {true, [Str, "=", format_value(Value)]}
                 end
             end,
    FormattedAttrs = lists:filtermap(Format, MultiAttrs),
    [lists:join("+", FormattedAttrs) || length(FormattedAttrs) > 0] ++ Acc.

format_value({utf8String, Utf8Value}) ->
    unicode:characters_to_list(Utf8Value);
format_value({_, Value}) when is_list(Value) ->
    Value;
format_value(Value) when is_list(Value) ->
    Value;
format_value(Value) ->
    io_lib:format("~p", [Value]).

format_name({rdnSequence, STVList}) ->
    Attributes = lists:foldl(fun format_attribute/2, [], STVList),
    lists:flatten(string:join(lists:reverse(Attributes), ", ")).


-ifdef(TEST).

format_name_test() ->
    Attr = fun (T, V) ->
               #'AttributeTypeAndValue'{type = T, value = {printableString, V}}
           end,
    CN = Attr(?'id-at-commonName', "test"),
    OU1 = Attr(?'id-at-organizationalUnitName', "ou1"),
    OU2 = Attr(?'id-at-organizationalUnitName', "ou2"),
    Unknown = Attr(unknown, "unknown"),
    ?assertEqual("CN=test", format_name({rdnSequence, [[CN]]})),
    ?assertEqual("OU=ou1, CN=test", format_name({rdnSequence, [[OU1], [CN]]})),
    ?assertEqual("OU=ou1+OU=ou2, CN=test",
                 format_name({rdnSequence, [[OU1, OU2], [CN]]})),
    ?assertEqual("OU=ou1+OU=ou2, CN=test",
                 format_name({rdnSequence, [[OU1, Unknown, OU2], [Unknown],
                                            [CN]]})).


extract_fields_by_type_test() ->
    Attr = fun (T, V) ->
               #'AttributeTypeAndValue'{type = T, value = {printableString, V}}
           end,
    Seq = {rdnSequence, [[Attr(a, "a1"), Attr(b, "b1"), Attr(b, "b2")],
                         [Attr(a, "a2")],
                         [Attr(b, "b3")]]},
    ?assertEqual(["a1", "a2"], extract_fields_by_type(Seq, a)),
    ?assertEqual(["b1", "b2", "b3"], extract_fields_by_type(Seq, b)).

-endif.

extract_fields_by_type({rdnSequence, STVList}, Type) ->
    [format_value(V) || List <- STVList,
                        #'AttributeTypeAndValue'{type = T, value = V} <- List,
                        T =:= Type];
extract_fields_by_type(_, _) ->
    [].

convert_date(Year, Rest) ->
    {ok, [Month, Day, Hour, Min, Sec], "Z"} = io_lib:fread("~2d~2d~2d~2d~2d", Rest),
    calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Min, Sec}}).

convert_date({utcTime, [Y1, Y2 | Rest]}) ->
    Year =
        case list_to_integer([Y1, Y2]) of
            YY when YY < 50 ->
                YY + 2000;
            YY ->
                YY + 1900
        end,
    convert_date(Year, Rest);
convert_date({generalTime, [Y1, Y2, Y3, Y4 | Rest]}) ->
    Year = list_to_integer([Y1, Y2, Y3, Y4]),
    convert_date(Year, Rest).

get_der_info(DerCert) ->
    Decoded = public_key:pkix_decode_cert(DerCert, otp),
    TBSCert = Decoded#'OTPCertificate'.tbsCertificate,
    Subject = format_name(TBSCert#'OTPTBSCertificate'.subject),

    Validity = TBSCert#'OTPTBSCertificate'.validity,
    NotBefore = convert_date(Validity#'Validity'.notBefore),
    NotAfter = convert_date(Validity#'Validity'.notAfter),
    {Subject, NotBefore, NotAfter}.

-spec get_subject_fields_by_type(binary(), term()) -> list() | {error, not_found}.
get_subject_fields_by_type(Cert, Type) ->
    OtpCert = public_key:pkix_decode_cert(Cert, otp),
    TBSCert = OtpCert#'OTPCertificate'.tbsCertificate,
    case extract_fields_by_type(TBSCert#'OTPTBSCertificate'.subject, Type) of
        [] ->
            {error, not_found};
        Vals ->
            Vals
    end.

-spec get_sub_alt_names_by_type(binary(), term()) -> list() | {error, not_found}.
get_sub_alt_names_by_type(Cert, Type) ->
    OtpCert = public_key:pkix_decode_cert(Cert, otp),
    TBSCert = OtpCert#'OTPCertificate'.tbsCertificate,
    TBSExts = TBSCert#'OTPTBSCertificate'.extensions,
    Exts = ssl_certificate:extensions_list(TBSExts),
    case ssl_certificate:select_extension(?'id-ce-subjectAltName', Exts) of
        {'Extension', _, _, Vals} ->
            case [N || {T, N} <- Vals, T == Type] of
                [] ->
                    {error, not_found};
                V ->
                    V
            end;
        _ ->
            {error, not_found}
    end.

set_generated_ca(CA) ->
    chronicle_kv:set(kv, root_cert_and_pkey, {CA, undefined}),
    {ok, _} = add_CAs(generated, CA),
    ok.

-record(verify_state, {last_subject, root_cert, chain_len}).

get_subject(Cert) ->
    TBSCert = Cert#'OTPCertificate'.tbsCertificate,
    format_name(TBSCert#'OTPTBSCertificate'.subject).

verify_fun(Cert, Event, State) ->
    Subject = get_subject(Cert),
    ?log_debug("Certificate verification event:~n~p", [{Subject, Event}]),

    case Event of
        {bad_cert, invalid_issuer} ->
            case State#verify_state.last_subject of
                undefined ->
                    RootOtpCert = public_key:pkix_decode_cert(State#verify_state.root_cert, otp),
                    RootSubject = get_subject(RootOtpCert),
                    {fail, {invalid_root_issuer, Subject, RootSubject}};
                LastSubject ->
                    {fail, {invalid_issuer, Subject, LastSubject}}
            end;
        {bad_cert, Error} ->
            ?log_error("Certificate ~p validation failed with reason: ~p",
                       [Subject, Error]),

            Trace = erlang:process_info(self(), [current_stacktrace]),
            OtpCert = public_key:pkix_decode_cert(State#verify_state.root_cert, otp),
            InitValidationState =
                pubkey_cert:init_validation_state(OtpCert, State#verify_state.chain_len, []),

            ?log_debug("Certificate validation trace:~n"
                       "     Initial Context: ~p~n"
                       "     Cert: ~p~n"
                       "     Stack: ~p~n",
                       [InitValidationState, Cert, Trace]),
            {fail, {Error, Subject}};
        {extension, Ext} ->
            ?log_warning(
               "Certificate ~p validation spotted an unknown extension:~n~p",
               [Subject, Ext]),
            {unknown, State};
        valid ->
            {valid, State#verify_state{last_subject = Subject}};
        valid_peer ->
            {valid, State}
    end.

decode_chain(Chain) ->
    try
        lists:reverse(public_key:pem_decode(Chain))
    catch T:E:S ->
            ?log_error("Unknown error while parsing certificate chain:~n~p",
                       [{T, E, S}]),
            {error, {bad_chain, malformed_cert}}
    end.

validate_chain([]) ->
    ok;
validate_chain([Entry | Rest]) ->
    case validate_cert_pem_entry(Entry) of
        {error, Error} ->
            {error, {bad_chain, Error}};
        {ok, _} ->
            validate_chain(Rest)
    end.

validate_chain_signatures([], _Chain) ->
    {error, no_ca};
validate_chain_signatures([CAProps | Tail], Chain) ->
    CA = proplists:get_value(pem, CAProps),
    CAId = proplists:get_value(id, CAProps),
    [{'Certificate', RootCertDer, not_encrypted}] = public_key:pem_decode(CA),
    DerChain = [Der || {'Certificate', Der, not_encrypted} <- Chain],
    State = #verify_state{root_cert = RootCertDer,
                          chain_len = length(Chain)},
    Options = [{verify_fun, {fun verify_fun/3, State}}],
    case public_key:pkix_path_validation(RootCertDer, DerChain, Options) of
        {ok, _} -> {ok, CA};
        {error, Reason} ->
            ?log_warning("Chain validation failed with root cert #~p: ~p",
                         [CAId, Reason]),
            validate_chain_signatures(Tail, Chain)
    end.

decode_and_validate_chain(CAs, Chain) ->
    case decode_chain(Chain) of
        {error, _} = Err ->
            Err;
        [] ->
            {error, {bad_chain, malformed_cert}};
        PemEntriesReversed ->
            case validate_chain(PemEntriesReversed) of
                {error, _} = Err ->
                    Err;
                ok ->
                    case validate_chain_signatures(CAs, PemEntriesReversed) of
                        {error, _} = Err ->
                            Err;
                        {ok, ChainCA} ->
                            [ChainCADecoded] = public_key:pem_decode(ChainCA),
                            case PemEntriesReversed of
                                [ChainCADecoded | Rest] -> {ok, ChainCA, Rest};
                                _ -> {ok, ChainCA, PemEntriesReversed}
                            end
                    end
            end
    end.

get_chain_info(Chain, CA) when is_binary(Chain), is_binary(CA) ->
    lists:foldl(
                fun ({'Certificate', DerCert, not_encrypted}, Acc) ->
                    {NewSub, _, NewExpiration} = get_der_info(DerCert),
                    case Acc of
                        undefined ->
                            {NewSub, NewExpiration};
                        {_Sub, Expiration} when Expiration > NewExpiration ->
                            {NewSub, NewExpiration};
                        {_Sub, Expiration} ->
                            {NewSub, Expiration}
                    end
                end, undefined, public_key:pem_decode(CA) ++
                                lists:reverse(public_key:pem_decode(Chain))).

%% Note that this function guarantees the order of certificates.
%% Current order: reversed order of addition.
%% Reasons:
%%   It is imporatant to check most recently added certificates first when
%%   looking for node certificate's CA; in case if two CA certs are using
%%   the same private key, we want the node cert to use the most recent
%%   CA cert to support it's rotation;
trusted_CAs(Format) ->
    Certs =
        case chronicle_kv:get(kv, ca_certificates) of
            {ok, {Cs, _}} -> Cs;
            {error, not_found} -> []
        end,

    SortedCerts = lists:sort(fun (PL1, PL2) ->
                                 Id1 = proplists:get_value(id, PL1),
                                 Id2 = proplists:get_value(id, PL2),
                                 Id1 >= Id2
                             end, Certs),
    case Format of
        props ->
            SortedCerts;
        pem ->
            [proplists:get_value(pem, Props) || Props <- SortedCerts];
        der ->
            lists:map(
              fun (Props) ->
                  Pem = proplists:get_value(pem, Props),
                  {ok, Der} = decode_single_certificate(Pem),
                  Der
              end, SortedCerts)
    end.

load_certs_from_inbox(Type, PassphraseSettings) ->
    %% Maintain behavior for existing callers where certs are always loaded.
    load_certs_from_inbox(Type, PassphraseSettings, true).

load_certs_from_inbox(Type, PassphraseSettings, ForceReload)
  when Type == node_cert; Type == client_cert ->
    P12Path = inbox_p12_path(Type),
    ChainPath = inbox_chain_path(Type),
    KeyPath = inbox_pkey_path(Type),
    case {filelib:is_file(P12Path), filelib:is_file(ChainPath)} of
        {true, false} ->
            ?log_info("Loading ~p from PKCS12 file ~s", [Type, P12Path]),
            load_certs_from_p12_file(Type, P12Path, PassphraseSettings,
                                     ForceReload);
        {true, true} ->
            ?log_error("Aborting ~p loading because both types of certs are "
                       "present: ~p and ~p", [Type, P12Path, ChainPath]),
            {error, {conflicting_certs, ChainPath, P12Path}};
        {false, _} ->
            ?log_info("Loading ~p from ~s and ~s", [Type, ChainPath, KeyPath]),
            load_certs_from_files(Type, ChainPath, KeyPath, PassphraseSettings,
                                  ForceReload)
    end.

load_certs_from_p12_file(Type, P12Path, PassphraseSettings, ForceReload) ->
    Dir = filename:dirname(P12Path),
    with_tmp_files(
      Dir, ["chain", "key"],
      fun ([TmpChainPath, TmpKeyPath]) ->
          case convert_p12_to_pem(P12Path, TmpChainPath, TmpKeyPath,
                                  PassphraseSettings) of
              ok ->
                  load_certs_from_files(Type, TmpChainPath, TmpKeyPath,
                                        PassphraseSettings, ForceReload);
              {error, Reason} ->
                  {error, Reason}
          end
      end).

with_tmp_files(Dir, Prefixes, Fun) ->
    TmpFiles = [path_config:tempfile(Dir, P, ".tmp")  || P <- Prefixes],
    try
        Fun(TmpFiles)
    after
        [catch file:delete(F) || F <- TmpFiles]
    end.

load_certs_from_files(Type, ChainFile, KeyFile, PassphraseSettings,
                      ForceReload) ->
    case file:read_file(ChainFile) of
        {ok, Chain} ->
            case file:read_file(KeyFile) of
                {ok, PKey} ->
                    set_certificate_chain(Type, Chain, PKey,
                                          PassphraseSettings, ForceReload);
                {error, Reason} ->
                    {error, {read_pkey, KeyFile, Reason}}
            end;
        {error, Reason} ->
            {error, {read_chain, ChainFile, Reason}}
    end.

convert_p12_to_pem(InP12Path, OutCertPath, OutKeyPath, PassphraseSettings) ->
    case ns_secrets:extract_pkey_pass(PassphraseSettings) of
        {ok, PassFun} ->
            case PassFun() =/= "" of
                true ->
                    extract_pem_from_p12(InP12Path, OutCertPath, OutKeyPath,
                                         PassFun);
                false ->
                    %% Empty out password is not supported by openssl pkcs12
                    %% when extracting keys from p12 file.
                    {error, empty_pass}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

extract_pem_from_p12(P12Path, OutCertPath, OutKeyPath, PassFun) ->
    {EncArgs, Env} =
        case PassFun() of
            undefined ->
                %% -nodes should be replaced with -noenc when we switch
                %% to OpenSSL 3.0
                %% Passing empty password just to avoid openssl asking for
                %% password
                {["-nodes", "-passin", "pass:"], []};
            Pass ->
                {["-passin", "env:PASS", "-passout", "env:PASS"],
                 [{"PASS", Pass}]}
        end,
    LegacyOpt = ns_config:read_key_fast(pkcs12_allow_legacy_alg, false),
    Args = ["-in", P12Path | EncArgs] ++ ["-legacy" || LegacyOpt],
    case pkcs12(["-nokeys", "-out", OutCertPath | Args], Env) of
        {ok, _} ->
            case pkcs12(["-nocerts", "-out", OutKeyPath | Args], Env) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    {error, {p12key, P12Path, Reason}}
            end;
        {error, Reason} ->
            {error, {p12cert, P12Path, Reason}}
    end.

pkcs12(Args, Env) ->
    call_openssl("pkcs12", Args, Env).

call_openssl(OpensslCmd, Args, Env) ->
    Path = path_config:component_path(bin, "openssl"),
    case os:find_executable(Path) of
        false -> {error, {no_openssl, Path}};
        OpensslPath ->
            AllArgs = [OpensslCmd | Args],
            CmdStr = lists:join(" ", [OpensslPath | AllArgs]),
            ?log_debug("Invoking OpenSSL: `~s`", [CmdStr]),
            try misc:run_external_tool(OpensslPath, AllArgs, Env, []) of
                {0, Output} ->
                    ?log_debug("OpenSSl call `~s` returned 0", [CmdStr]),
                    {ok, Output};
                {Status, Output} ->
                    ?log_error("OpenSSL call `~s` returned ~b:~n~s",
                               [CmdStr, Status, Output]),
                    {error, {openssl_error, CmdStr, {Status, Output}}}
            catch
                _:Reason:ST ->
                    ?log_error("OpenSSL call `~s` failed:~n~p~nStacktrace:~n~p",
                               [CmdStr, Reason, ST]),
                    {error, {openssl_error, CmdStr, Reason}}
            end
    end.

is_cert_loaded_from_file(ChainPath) ->
    case file:read_file(ChainPath) of
        {ok, Chain} ->
            CurChain =
                proplists:get_value(pem, get_cert_info(node_cert, node()), <<>>),
            [CurNodePemEntry | _] = public_key:pem_decode(CurChain),
            case public_key:pem_decode(Chain) of
                [CurNodePemEntry | _] -> true;
                _ -> false
            end;
        {error, _} ->
            false
    end.

set_certificate_chain(Type, Chain, PKey, PassphraseSettings, ForceReload) ->
    case decode_and_validate_chain(trusted_CAs(props), Chain) of
        {ok, CAPem, ChainEntriesReversed} ->
            %% ChainReversed :: [Int cert,..., Node cert] (without CA)
            ChainEntries = lists:reverse(ChainEntriesReversed),
            LeafCert = hd(ChainEntries),
            ChainPem = public_key:pem_encode(ChainEntries),
            ValidationRes =
                case ns_secrets:extract_pkey_pass(PassphraseSettings) of
                    {ok, PassFun} ->
                        ValidationResult =
                            functools:sequence_([
                                fun () ->
                                    validate_cert_and_pkey(
                                      LeafCert, PKey, PassFun)
                                end,
                                fun () ->
                                    validate_otp_certs(
                                      Type, ChainPem, PKey, PassFun)
                                end]),
                        case ValidationResult of
                            ok -> validate_cert_identity(Type, LeafCert);
                            {error, _} -> ValidationResult
                        end;
                    {error, _} = Error ->
                        Error
                end,

            case ValidationRes of
                {error, Reason} ->
                    {error, Reason};
                {ok, WarningList} ->
                    {ok, Props} =
                        ns_ssl_services_setup:set_certificate_chain(
                            Type,
                            CAPem,
                            ChainPem,
                            PKey,
                            PassphraseSettings,
                            ForceReload),

                    {ok, Props, WarningList}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

validate_otp_certs(client_cert, ChainPem, PKeyPem, PassphraseFun) ->
    case cb_dist:external_encryption() of
        true ->
            Node = node(),
            ChainEntries = public_key:pem_decode(ChainPem),
            ChainDer = [D || {'Certificate', D, not_encrypted} <- ChainEntries],
            [{KeyType, _, _} = PKeyEntry] = public_key:pem_decode(PKeyPem),
            PrivateKey = public_key:pem_entry_decode(PKeyEntry,
                                                     PassphraseFun),
            {_, KeyDer, _} = public_key:pem_entry_encode(KeyType, PrivateKey),
            Opts = [{cert, ChainDer}, {key, {KeyType, KeyDer}}],
            case ns_cluster:verify_otp_connectivity(Node, Opts) of
                {ok, _} -> ok;
                {error, _, {_, Msg}} ->
                    ?log_error(
                      "Could not establish test connection to "
                      "otp server with reason: ~p", [Msg]),
                    Host = misc:extract_node_address(
                             Node, cb_dist:address_family()),
                    {error, {test_cert_failed, client, Host, Msg}}
            end;
        false -> ok
    end;
validate_otp_certs(node_cert, ChainPem, PKeyPem, PassphraseFun) ->
    Node = node(),
    %% It doesn't make much sense to verify OTP connectivity for the case
    %% when node is cb.local
    NodenameSet = not misc:is_cb_local_nodename(Node),
    case cb_dist:external_encryption() andalso NodenameSet of
        true ->
            with_test_otp_server(
              fun (Port) ->
                  Opts = [{port, Port}],
                  case ns_cluster:verify_otp_connectivity(Node, Opts) of
                      {ok, _} -> ok;
                      {error, _, {_Error, Msg}} ->
                          ?log_error(
                            "Could not establish test connection to "
                            "test otp server at port ~p with reason: ~p",
                            [Port, Msg]),
                          Host = misc:extract_node_address(
                                   Node, cb_dist:address_family()),
                          {error, {test_cert_failed, server, Host, Msg}}
                  end
              end, ChainPem, PKeyPem, PassphraseFun);
        false -> ok
    end.

with_test_otp_server(Fun, ChainPem, PKeyPem, PassphraseFun) ->
    [{server, CurrentServerOpts}] = ets:lookup(ssl_dist_opts, server),
    CurrentServerOptsWithAF =
        [cb_dist:address_family() | CurrentServerOpts],

    ChainEntries = public_key:pem_decode(ChainPem),
    ChainDer = [D || {'Certificate', D, not_encrypted} <- ChainEntries],

    [{KeyType, _, _} = PKeyEntry] = public_key:pem_decode(PKeyPem),
    PrivateKey = public_key:pem_entry_decode(PKeyEntry, PassphraseFun),
    {_, KeyDer, _} = public_key:pem_entry_encode(KeyType, PrivateKey),

    ServerOpts = lists:map(
                   fun ({certfile, _}) -> {cert, ChainDer};
                       ({keyfile, _}) -> {key, {KeyType, KeyDer}};
                       (O) -> O
                   end, CurrentServerOptsWithAF),
    case ssl:listen(0, ServerOpts) of
        {ok, LSocket} ->
            Accepter = spawn(fun () ->
                                 {ok, HS} = ssl:transport_accept(LSocket,
                                                                 30000),
                                 {ok, S} = ssl:handshake(HS, 30000),
                                 receive
                                    stop -> catch ssl:close(S)
                                 after 30000 -> ok
                                 end
                             end),
            try
                {ok, {_, Port}} = ssl:sockname(LSocket),
                ?log_info("Started test server on port ~p for certs "
                          "validation", [Port]),
                Fun(Port)
            catch
                _:E:ST ->
                    ?log_error("Unexpected exception: ~p~n~p", [E, ST]),
                    {error, {test_server_error, unexpected_exception}}
            after
                Accepter ! stop,
                catch ssl:close(LSocket)
            end;
        {error, Reason} ->
            {error, {test_server_error, Reason}}
    end.

add_CAs(Type, Pem) ->
    add_CAs(Type, Pem, []).

add_CAs(Type, Pem, Opts) ->
    case add_CAs_txn_fun(Type, Pem, Opts) of
        {ok, F} ->
            {ok, _, R} = chronicle_kv:txn(kv, F),
            {ok, R};
        {error, _} = Error ->
            Error
    end.

add_CAs_txn_fun(Type, Pem, Opts) when is_binary(Pem),
                                 (Type =:= uploaded) or (Type =:= generated) ->
    SingleCert = proplists:get_bool(single_cert, Opts),
    ExtraCertProps = proplists:get_value(extra_props, Opts, []),
    case decode_certificates(Pem) of
        {ok, []} ->
            {error, empty_cert};
        {ok, DerCerts} when SingleCert,
                            length(DerCerts) > 1 ->
            {error, too_many_entries};
        {ok, DerCerts} ->
            CAProps = [cert_props(Type, E, ExtraCertProps) || E <- DerCerts],
            {ok, load_CAs_txn(CAProps, _)};
        {error, Reason} ->
            {error, Reason}
    end.

cluster_uses_client_certs(Config) ->
    cluster_uses_client_certs(
      ns_config:search(Config, cluster_encryption_level, control),
      ns_ssl_services_setup:client_cert_auth_state(Config),
      misc:is_n2n_client_cert_verification_enabled(Config)).

cluster_uses_client_certs(strict, "mandatory",
                          _N2NClientVerification) -> true;
cluster_uses_client_certs(strict, "hybrid",
                          _N2NClientVerification) -> true;
cluster_uses_client_certs(all, "mandatory",
                          _N2NClientVerification) -> true;
cluster_uses_client_certs(all, "hybrid",
                          _N2NClientVerification) -> true;
cluster_uses_client_certs(_DataEncryption, _ClientCertAuth,
                          true) -> true;
cluster_uses_client_certs(_DataEncryption, _ClientCertAuth,
                          _N2NClientVerification) -> false.


invalid_client_cert_nodes(DataEncryption, ClientCertAuth,
                          N2NClientVerification) ->
    ClientCertIsUsed =
        cluster_uses_client_certs(
          DataEncryption, ClientCertAuth, N2NClientVerification),

    case ClientCertIsUsed of
        true ->
            Nodes = ns_node_disco:nodes_wanted(),
            NodesWithUntrustedCA = filter_out_trusted_client_cert_CAs(Nodes),
            NodesOldCA = filter_out_nodes_where_CA_has_no_server_auth(Nodes),
            #{untrusted_ca => NodesWithUntrustedCA,
              ca_with_server_auth_EKU => NodesOldCA};
        false ->
            #{untrusted_ca => [],
              ca_with_server_auth_EKU => []}
    end.

cert_contains_server_auth_EKU(Pem) ->
    [{'Certificate', Der, not_encrypted}] = public_key:pem_decode(Pem),
    DecodedCert = public_key:pkix_decode_cert(Der, otp),
    TBSCert = DecodedCert#'OTPCertificate'.tbsCertificate,
    Extensions = TBSCert#'OTPTBSCertificate'.extensions,
    lists:any(
      fun (#'Extension'{extnID = ?'id-ce-extKeyUsage', extnValue = L})
                                                            when is_list(L) ->
              lists:member(?'id-kp-serverAuth', L);
          (_) ->
              false
      end, Extensions).

filter_out_nodes_where_CA_has_no_server_auth(Nodes) ->
    IsOldCA = cert_contains_server_auth_EKU(self_generated_ca()),
    lists:filter(
      fun (N) ->
          CProps = ns_config:read_key_fast({node, N, client_cert}, []),
          case proplists:get_value(type, CProps) of
              generated when IsOldCA -> true;
              _ -> false
          end
      end, Nodes).

filter_out_trusted_client_cert_CAs(Nodes) ->
    DecodedTrustedCAs = [public_key:pem_decode(CA) || CA <- trusted_CAs(pem)],
    lists:filter(
      fun (N) ->
          CProps = ns_config:read_key_fast({node, N, client_cert}, []),
          CertCAPem = proplists:get_value(ca, CProps, <<>>),
          DecodedCertCA = public_key:pem_decode(CertCAPem),
          not lists:member(DecodedCertCA, DecodedTrustedCAs)
      end, Nodes).

remove_CA(Id) ->
    Res =
        chronicle_kv:transaction(
          kv, [ca_certificates, nodes_wanted],
          fun (Snapshot) ->
              {CAs, _Rev} = maps:get(ca_certificates, Snapshot,
                                     {[], undefined}),
              {Nodes, _NodesRev} = maps:get(nodes_wanted, Snapshot),
              case lists:search(lists:member({id, Id}, _), CAs) of
                  {value, Props} ->
                      CA = proplists:get_value(pem, Props, <<>>),
                      %% If a node cert is being uploaded at the same time,
                      %% it might not be added in ns_config yet by the time
                      %% we do this check. Because of this race condition it is
                      %% actually possible that we remove CA that is "in use"
                      %% by some node. It seems to be pretty hard to avoid this
                      %% race with node_cert stored in ns_config, as we don't
                      %% have common chronicle-ns_config transactions.
                      ClusterUsesClientCert =
                          cluster_uses_client_certs(ns_config:latest()),
                      NodesThatUseCA =
                          filter_nodes_by_ca(node_cert, Nodes, CA) ++
                          case ClusterUsesClientCert of
                              true ->
                                  filter_nodes_by_ca(client_cert, Nodes, CA);
                              false ->
                                  []
                          end,
                      case NodesThatUseCA of
                          [] ->
                              ToSet = lists:delete(Props, CAs),
                              {commit, [{set, ca_certificates, ToSet}], Props};
                          [_ | _] ->
                              UniqueNodes = lists:usort(NodesThatUseCA),
                              {abort, {error, {in_use, UniqueNodes}}}
                      end;
                  false ->
                      {abort, {error, not_found}}
              end
          end, #{}),
    case Res of
        {ok, _, Props} -> {ok, Props};
        {error, Reason} -> {error, Reason}
    end.

filter_nodes_by_ca(CertType, Nodes, CAPem) when CertType == node_cert;
                                                CertType == client_cert ->
    CA = public_key:pem_decode(CAPem),
    lists:filter(
      fun (N) ->
          CProps = ns_config:read_key_fast({node, N, CertType}, []),
          CertCAPem = proplists:get_value(ca, CProps, <<>>),
          CA =:= public_key:pem_decode(CertCAPem)
      end, Nodes).

load_CAs_from_inbox() ->
    CAInbox = inbox_ca_path(),
    case read_CAs(CAInbox) of
        {ok, []} ->
            ?log_warning("Appending empty list of certs"),
            {error, {CAInbox, empty}};
        {ok, NewCAs} ->
            ?log_info("Trying to load the following CA certificates:~n~p",
                      [NewCAs]),
            load_CAs(NewCAs);
        {error, R} ->
            {error, R}
    end.

load_CAs(CAPropsList) ->
    {ok, _, R} = chronicle_kv:txn(kv, load_CAs_txn(CAPropsList, _)),
    {ok, R}.

load_CAs_txn(CAPropsList, ChronicleTxn) ->
    UTCTime = calendar:universal_time(),
    LoadTime = calendar:datetime_to_gregorian_seconds(UTCTime),
    CAs = case chronicle_kv:txn_get(ca_certificates, ChronicleTxn) of
              {ok, {V, _}} -> V;
              {error, not_found} -> []
          end,
    ToSet = maybe_append_CA_certs(CAs, CAPropsList, LoadTime),
    NewCAs = ToSet -- CAs,
    {commit, [{set, ca_certificates, ToSet}], NewCAs}.

maybe_append_CA_certs(CAs, [], _) ->
    CAs;
maybe_append_CA_certs(CAs, CAPropsList, LoadTime) ->
    MaxId = lists:max([-1] ++ [proplists:get_value(id, CA) || CA <- CAs]),
    DecodedCAs = lists:concat(
                   [public_key:pem_decode(proplists:get_value(pem, CA))
                    || CA <- CAs]),
    {_, Res, _} = lists:foldl(
                    fun (NewCA, {NextId, Acc, DecodedAcc}) ->
                        NewPem = proplists:get_value(pem, NewCA),
                        [NewPemDecoded] = public_key:pem_decode(NewPem),
                        case lists:member(NewPemDecoded, DecodedAcc) of
                            true ->
                                {NextId, Acc, DecodedAcc};
                            false ->
                                NewCA2 = [{id, NextId},
                                          {load_timestamp, LoadTime} | NewCA],
                                {NextId + 1, [NewCA2 | Acc],
                                 [NewPemDecoded | DecodedAcc]}
                        end
                    end, {MaxId + 1, CAs, DecodedCAs}, CAPropsList),
    Res.

read_CAs(CAPath) ->
    case file:list_dir(CAPath) of
        {ok, Files} ->
            lists:foldl(
              fun (_, {error, R}) -> {error, R};
                  %% Ignore filenames that start with dot, in order to avoid
                  %% problems with files and dirs like .git, ..data, etc...
                  ("." ++ _ = F, {ok, Acc}) ->
                      ?log_debug("Ignoring file '~s'", [F]),
                      {ok, Acc};
                  (F, {ok, Acc}) ->
                      FullPath = filename:join(CAPath, F),
                      ?log_debug("Reading file '~s'", [FullPath]),
                      case read_ca_file(FullPath) of
                          {ok, CAPropsList} -> {ok, CAPropsList ++ Acc};
                          {error, R} -> {error, {FullPath, R}}
                      end
              end, {ok, []}, Files);
        {error, Reason} -> {error, {CAPath, {read, Reason}}}
    end.

read_ca_file(Path) ->
    case file:read_file(Path) of
        {ok, CertPemBin} ->
            case decode_certificates(CertPemBin) of
                {ok, DerCerts} ->
                    Host = misc:extract_node_address(node()),
                    Extras = [{load_host, iolist_to_binary(Host)},
                              {load_file, iolist_to_binary(Path)}],
                    {ok, [cert_props(uploaded, E, Extras) || E <- DerCerts]};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, {read, Reason}}
    end.

cert_props(DerCert) ->
    cert_props(undefined, DerCert, []).

cert_props(Type, DerCert, Extras) when is_binary(DerCert) ->
    {Sub, NotBefore, NotAfter} = get_der_info(DerCert),
    [{subject, iolist_to_binary(Sub)},
     {not_before, NotBefore},
     {not_after, NotAfter},
     {type, Type},
     {pem, public_key:pem_encode([{'Certificate', DerCert, not_encrypted}])}]
     ++ Extras.

get_warnings() ->
    Config = ns_config:get(),
    Nodes = ns_node_disco:nodes_wanted(),
    TrustedCAs = trusted_CAs(pem),
    Is76 = cluster_compat_mode:is_cluster_76(),
    ClientWarnings =
        lists:flatmap(
            fun (Node) ->
                Warnings =
                    case ns_config:search(Config, {node, Node, client_cert}) of
                        {value, Props} ->
                            node_cert_warnings(client_cert, Node, TrustedCAs,
                                               Props);
                        false -> []
                    end,
                [{{client_cert, Node}, W} || W <- Warnings]
            end, Nodes),
    NodeWarnings =
        lists:flatmap(
          fun (Node) ->
              Warnings =
                  case ns_config:search(Config, {node, Node, node_cert}) of
                      {value, Props} ->
                          node_cert_warnings(node_cert, Node, TrustedCAs,
                                             Props);
                      false ->
                          []
                  end,
              [{{node_cert, Node}, W} || W <- Warnings]
          end, Nodes),
    ClusterUsesClientCert = cluster_uses_client_certs(Config),
    CAWarnings =
        lists:flatmap(
          fun (CAProps) ->
                  SelfSignedWarnings =
                      case proplists:get_value(type, CAProps) of
                          generated -> [self_signed];
                          _ -> []
                      end,
                  {_, ExpWarnings} = expiration_warnings(CAProps),
                  Id = proplists:get_value(id, CAProps),
                  UnusedWarnings =
                      case proplists:get_value(type, CAProps) of
                          generated ->
                              CAPem = proplists:get_value(pem, CAProps, <<>>),
                              UnusedNode =
                                  case filter_nodes_by_ca(node_cert, Nodes,
                                                          CAPem) of
                                      [] -> true;
                                      _ -> false
                                  end,
                              UnusedClient =
                                  case ClusterUsesClientCert and Is76 of
                                      true ->
                                          case filter_nodes_by_ca(
                                                 client_cert, Nodes, CAPem) of
                                              [] -> true;
                                              _ -> false
                                          end;
                                      false ->
                                          true
                                  end,
                              [unused || UnusedNode and UnusedClient];
                          _ -> []
                      end,
                  [{{ca, Id}, W} || W <- SelfSignedWarnings ++ ExpWarnings ++
                                         UnusedWarnings]
          end, trusted_CAs(props)),
    ClientWarnings ++ NodeWarnings ++ CAWarnings.

expiration_warnings(CertProps) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    WarningDays = cert_expiration_warning_days(),
    WarningSeconds = WarningDays * 24 * 60 * 60,
    WarningThreshold = Now + WarningSeconds,

    NotAfter = proplists:get_value(not_after, CertProps),
    case NotAfter of
        A when is_integer(A) andalso A =< Now ->
            {infinity, [expired]};
        A when is_integer(A) andalso A =< WarningThreshold ->
            {NotAfter, [{expires_soon, A}]};
        _ ->
            {NotAfter - WarningSeconds, []}
    end.

is_trusted(CAPem, TrustedCAs) ->
    case decode_single_certificate(CAPem) of
        {ok, Decoded} ->
            lists:any(
              fun (C) ->
                  {ok, Decoded} == decode_single_certificate(C)
              end, TrustedCAs);
        {error, _} ->
            false
    end.

node_cert_warnings(Type, Node, TrustedCAs, NodeCertProps) ->
    MissingCAWarnings =
        case is_trusted(proplists:get_value(ca, NodeCertProps), TrustedCAs) of
            true -> [];
            false -> [mismatch]
        end,

    {_, ExpirationWarnings} = expiration_warnings(NodeCertProps),

    SelfSignedWarnings =
        case proplists:get_value(type, NodeCertProps) of
            generated -> [self_signed];
            _ -> []
        end,

    NodeNameNotMatchWarnings =
        case Type of
            node_cert ->
                case verify_cert_hostname(Node, NodeCertProps) of
                    {ok, WarningList} -> WarningList;
                    {error, _Err} -> []
                end;
            client_cert ->
                []
        end,

    MissingCAWarnings ++ ExpirationWarnings ++
        SelfSignedWarnings ++ NodeNameNotMatchWarnings.

get_cert_info(node_cert, Node) ->
    ns_config:read_key_fast({node, Node, node_cert}, []);

get_cert_info(client_cert, Node) ->
    ns_config:read_key_fast({node, Node, client_cert}, []).

cert_expiration_warning_days() ->
    ns_config:read_key_fast({cert, expiration_warning_days}, 30).

extract_internal_client_cert_user(Cert) ->
    case get_sub_alt_names_by_type(Cert, rfc822Name) of
        {error, not_found} ->
            {error, not_found};
        Emails ->
            fun FindInternalEmail ([]) -> {error, not_found};
                FindInternalEmail ([Email | T]) ->
                    case string:split(Email, "@") of
                        [Name, ?INTERNAL_CERT_EMAIL_DOMAIN] ->
                            {ok, "@" ++ Name};
                        _ ->
                            FindInternalEmail(T)
                    end
            end (Emails)
    end.

-spec validate_cert_identity(node_cert|client_cert, tuple()) ->
    {ok, WarningList::list()} | {error, atom()}.

validate_cert_identity(client_cert, {'Certificate', DerCert, not_encrypted}) ->
    case extract_internal_client_cert_user(DerCert) of
        {ok, _UserName} -> {ok, []};
        {error, not_found} -> {error, bad_cert_identity}
    end;

validate_cert_identity(node_cert, NodeCert) ->
    verify_cert_hostname(node(), NodeCert).

% function name: verify_cert_hostname_strict
% Possible outputs of verify_cert_hostname are:
% ok with empty/not empty warning list and error. Sometimes, we are
% only interested in ok/error, so we map anything other than ok to error.

verify_cert_hostname_strict(Node, NodeCertProps) ->
    case verify_cert_hostname(Node, NodeCertProps) of
        {ok, []} -> ok;
        {ok, _WarningList} -> error;
        {error, _Err} -> error
    end.

verify_cert_hostname(Node, NodeCert) ->
    NeedsValidation =
        ns_config:read_key_fast(validate_node_cert_san, true) andalso
        cluster_compat_mode:is_enterprise(),
    verify_cert_hostname(NeedsValidation, Node, NodeCert).

verify_cert_hostname(true = NeedsValidation, Node, CertProps)
    when is_list(CertProps) ->
    Chain = proplists:get_value(pem, CertProps, <<>>),
    case decode_chain(Chain) of
        {error, _} ->
            {error, invalid_chain};
        [] ->
            {error, invalid_chain};
        PemEntriesReversed ->
            ChainEntries = lists:reverse(PemEntriesReversed),
            verify_cert_hostname(NeedsValidation, Node, hd(ChainEntries))
    end;

verify_cert_hostname(true, Node, {'Certificate', DerCert, not_encrypted}) ->
    ValidReferenceIDs = prepare_reference_ids(Node),
    case public_key:pkix_verify_hostname(DerCert, ValidReferenceIDs) of
        true -> {ok, []};
        false ->
            NodeNameIsFixed = not ns_cluster_membership:system_joinable(),
            case NodeNameIsFixed of
                true -> {error, bad_server_cert_san};
                false -> {ok, [cert_san_invalid]}
            end
    end;

verify_cert_hostname(false, _Node, _DerCert) ->
    {ok, []}.

prepare_reference_ids(Node) ->
    Host = misc:extract_node_address(Node),
    case inet:parse_address(Host) of
        {ok, IP} ->
            [{ip, IP}];
        {error, einval} ->
            [{dns_id, Host}]
    end.

chronicle_upgrade_to_phoenix(ChronicleTxn) ->
    maybe
        {ok, OldCerts} ?= chronicle_upgrade:get_key(ca_certificates,
                                                    ChronicleTxn),
        {true, NewCerts} ?= chronicle_upgrade_certs_to_phoenix(OldCerts),
        chronicle_upgrade:set_key(ca_certificates, NewCerts, ChronicleTxn)
    else _ -> ChronicleTxn
    end.

chronicle_upgrade_certs_to_phoenix(OldCerts) ->
    NewCerts = lists:map(fun chronicle_upgrade_cert_to_phoenix/1, OldCerts),
    case NewCerts of
        OldCerts ->
            false;
        _ ->
            {true, NewCerts}
    end.

chronicle_upgrade_cert_to_phoenix(Cert) ->
    maybe
        Pem = proplists:get_value(pem, Cert, <<>>),
        {ok, DerCert} ?= decode_single_certificate(Pem),
        {Subject, _, _} = get_der_info(DerCert),
        lists:keystore(subject, 1, Cert, {subject, iolist_to_binary(Subject)})
    else
        {error, Reason} ->
            ?log_error("Couldn't upgrade cert to Phoenix (error ~w), keeping "
                       "existing:~n~p", [Reason, Cert]),
            Cert
    end.

-ifdef(TEST).
-define(PEM_DEFAULT,
        <<"-----BEGIN CERTIFICATE-----\n"
          "MIIDDDCCAfSgAwIBAgIIGDcduZ0c+xAwDQYJKoZIhvcNAQELBQAwJDEiMCAGA1UE"
          "AxMZQ291Y2hiYXNlIFNlcnZlciAzZjRiMmJiMDAeFw0xMzAxMDEwMDAwMDBaFw00"
          "OTEyMzEyMzU5NTlaMCQxIjAgBgNVBAMTGUNvdWNoYmFzZSBTZXJ2ZXIgM2Y0YjJi"
          "YjAwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC/ak/CV/3FP43gYQ8W"
          "pOrOwLZxbiPNGHijv1yF8ltTq7htwIFBx+XCwuXhvtWTJOoPa7GbOutjHKrTRquW"
          "tNNZEKQTVp2PPyMIACI+Cbm0RjmbTHq5XzET19pDn35lsDaG5qbMWfoK9OIYm1Gm"
          "yDc6iT+MHXP77FPpJFxuwCOZ6Flm+xySPoLU4vckaZehs7naxiCFufszJ+IHi/Ve"
          "14h4vHH+OncYmC3xnTLCuZZr0KyL0QWFs2N2x6YJmcR8j8KVOYHi9Tcz3VPfpSdF"
          "ZKZdps6IxIR5escAnMVDtgpMu4+bna7jDk39PCdjmH945Ai9Gxz2/a7s/otkoHhd"
          "XdVPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBhjAPBgNVHRMBAf8EBTADAQH/MB0G"
          "A1UdDgQWBBQzIE8JmPTzf7FwwAXKEop/E06OvTANBgkqhkiG9w0BAQsFAAOCAQEA"
          "c07hyzFbOvuTfCfgW4bc/FUj5NLwG5s7svpiMI9U+8pNSFOOPv8CtkJ5BjchQrxs"
          "8lj7/Q4jtSDxanKuuslTPH4h+FGNB7zOjunZzyQmfRu7xQE2jEe7Cc68HxUVJbRC"
          "wDNAgAmwxuWmQPDTD7oe1kQf1YTz1St6EZZEG8pFVnLRhoZbTZTwMlyPPMSpK/gd"
          "+Meo4LRV7EPIorzJ+ZuAnJ0GtdvxINqd2aBP7WWD7vO4ow6RwLadlem8yw29cMKq"
          "c6E5qMePI8bM32uTzDwjVmy7RMmP+P0o5n5Xy27vQBsqMXVp1qO+HP9akWbukiQ0"
          "6Cc+kL8oh9vQqmlfZ48mcQ==\n"
          "-----END CERTIFICATE-----">>).

-define(PEM_WITH_OU,
        <<"-----BEGIN CERTIFICATE-----\n"
          "MIIDTjCCAjagAwIBAgIIGDccYQ+W5AgwDQYJKoZIhvcNAQELBQAwRTEfMB0GA1UE"
          "CxMWT3JnYW5pemF0aW9uYWxVbml0VGVzdDEiMCAGA1UEAxMZQ291Y2hiYXNlIFNl"
          "cnZlciA0ZTBhZTk2MzAeFw0xMzAxMDEwMDAwMDBaFw00OTEyMzEyMzU5NTlaMEUx"
          "HzAdBgNVBAsTFk9yZ2FuaXphdGlvbmFsVW5pdFRlc3QxIjAgBgNVBAMTGUNvdWNo"
          "YmFzZSBTZXJ2ZXIgNGUwYWU5NjMwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK"
          "AoIBAQCm9Q0cw17QKwI1hFG7R2sPMQpqQfcYPaKzQR27E0MuXLj1zP7JZauG/6ty"
          "9maXBrhJyRNL/6RYQ8JNfzIFxLbWAtQpQRQ3kIc0h43r3r8vo4iroV69WL7aWkdh"
          "iWbJhSvNVf7pt+lBRnSdWGH4pzPs/3ojakCw5ocrKmmDcohw3rjVGCrXSZsIS9HT"
          "gHm+6ZgU9EPJg1C0vTgGcrBIHsAwuBoZfJ2K6WbAn4LwR2TNI2vqjaJ/nRVYOVpx"
          "0+Q8hi97h08Jxxl/OQJ/HpB/HRAdn8TQc3IKFU7oryGzgEwEb2C7PKW4kY1E/HoT"
          "QGDBwlwNu3K5ypeMFJXJIyAvLHvxAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBhjAP"
          "BgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBS6uBVauvlaUqDR6u2nUohBwr7vPjAN"
          "BgkqhkiG9w0BAQsFAAOCAQEAFobRqciHfhfYzBBtlUQ8p7pCtScmLNN3APiNI+GL"
          "e6hL75WdT/rVtQgBTR4ptAK+gBbfLVdWvF+c7H5UmODDTNuclg2Y/dF23FiexL6t"
          "ckz1cfao5ueetXcLuzQi2dk0qtbTYgaCHNwYHpW7D6Hfc0g9xjlRtQoiDJ4vma57"
          "KlaaA7MlI8soWYS7f4YqbKDBdKQekEdhW6tsj1cZrlIsUO34AXHhn8uwOAk+Yai1"
          "PXCblzKwY8o4WvPdBWjS1SDEfYPubygyh3dSdX92kYJvlRthIgrtsgZg1vc4w35g"
          "Qvlx+xG9le4klA5R0kI5LYlCQ1nL4rmrOqBxbvhnG6rH+Q==\n"
          "-----END CERTIFICATE-----">>).

chronicle_upgrade_cert_to_phoenix_test() ->
    CertWithoutPEM = [{subject, <<"CN=Couchbase Server 3f4b2bb0">>}],
    CertWithoutOU = [{pem, ?PEM_DEFAULT},
                     {subject, <<"CN=Couchbase Server 3f4b2bb0">>}],
    CertWithCorrectOU = [{pem, ?PEM_WITH_OU},
                         {subject,
                          <<"OU=OrganizationalUnitTest, "
                            "CN=Couchbase Server 4e0ae963">>}],
    CertWithMissingOU = [{pem, ?PEM_WITH_OU},
                         {subject, <<"CN=Couchbase Server 4e0ae963">>}],
    CertWithoutSubject = [{pem, ?PEM_DEFAULT}],

    %% Don't make changes if they're not necessary
    false = chronicle_upgrade_certs_to_phoenix(
              [CertWithoutPEM,
               CertWithoutOU,
               CertWithCorrectOU]),

    %% Update the chronicle key if any cert needs updating
    {true, NewCerts} = chronicle_upgrade_certs_to_phoenix(
                         [CertWithoutPEM,
                          CertWithoutOU,
                          CertWithCorrectOU,
                          CertWithMissingOU,
                          CertWithoutSubject]),

    %% Don't change certificate without PEM (even though it's clearly broken)
    ?assertEqual([{subject, <<"CN=Couchbase Server 3f4b2bb0">>}],
                 lists:nth(1, NewCerts)),
    %% Don't change certificate without OU attribute
    ?assertEqual([{pem, ?PEM_DEFAULT},
                  {subject, <<"CN=Couchbase Server 3f4b2bb0">>}],
                 lists:nth(2, NewCerts)),
    %% Don't change certificate with correct OU attribute
    ?assertEqual([{pem, ?PEM_WITH_OU},
                  {subject,
                   <<"OU=OrganizationalUnitTest, "
                     "CN=Couchbase Server 4e0ae963">>}],
                 lists:nth(3, NewCerts)),
    %% Fix subject with missing OU attribute
    ?assertEqual([{pem, ?PEM_WITH_OU},
                  {subject,
                   <<"OU=OrganizationalUnitTest, "
                     "CN=Couchbase Server 4e0ae963">>}],
                 lists:nth(4, NewCerts)),
    %% Fix certificate with missing subject
    ?assertEqual([{pem, ?PEM_DEFAULT},
                  {subject, <<"CN=Couchbase Server 3f4b2bb0">>}],
                 lists:nth(5, NewCerts)),
    %% Confirm no extra certs added
    ?assertEqual(5, length(NewCerts)).

-endif.
