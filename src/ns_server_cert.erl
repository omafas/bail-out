%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2018 Couchbase, Inc.
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

-module(ns_server_cert).

-include("ns_common.hrl").

-include_lib("public_key/include/public_key.hrl").

-export([do_generate_cert_and_pkey/2,
         decode_cert_chain/1,
         decode_single_certificate/1,
         generate_and_set_cert_and_pkey/0,
         cluster_ca/0,
         set_cluster_ca/1,
         apply_certificate_chain_from_inbox/0,
         apply_certificate_chain_from_inbox/1,
         get_warnings/1,
         get_subject_fields_by_type/2,
         get_sub_alt_names_by_type/2,
         get_node_cert_info/1,
         tls_server_validation_options/0,
         set_generated_public_ca/1,
         validate_pkey/1]).

inbox_chain_path() ->
    filename:join(path_config:component_path(data, "inbox"), "chain.pem").

inbox_pkey_path() ->
    filename:join(path_config:component_path(data, "inbox"), "pkey.key").

cluster_ca() ->
    case ns_config:search(cert_and_pkey) of
        {value, Tuple} ->
            Tuple;
        false ->
            Pair = generate_cert_and_pkey(),
            RV = ns_config:run_txn(
                   fun (Config, SetFn) ->
                           case ns_config:search(Config, cert_and_pkey) of
                               {value, OtherPair} ->
                                   {abort, OtherPair};
                               false ->
                                   {commit, SetFn(cert_and_pkey, Pair, Config)}
                           end
                   end),

            case RV of
                {abort, OtherPair} ->
                    OtherPair;
                _ ->
                    Pair
            end
    end.

generate_and_set_cert_and_pkey() ->
    Pair = generate_cert_and_pkey(),
    ns_config:set(cert_and_pkey, Pair).

generate_cert_and_pkey() ->
    StartTS = os:timestamp(),
    Args = case ns_config:read_key_fast({cert, use_sha1}, false) of
               true ->
                   ["--use-sha1"];
               false ->
                   []
           end,
    RV = do_generate_cert_and_pkey(Args, []),
    EndTS = os:timestamp(),

    Diff = timer:now_diff(EndTS, StartTS),
    ?log_debug("Generated certificate and private key in ~p us", [Diff]),

    RV.

do_generate_cert_and_pkey(Args, Env) ->
    {Status, Output} = misc:run_external_tool(path_config:component_path(bin, "generate_cert"), Args, Env),
    case Status of
        0 ->
            extract_cert_and_pkey(Output);
        _ ->
            erlang:exit({bad_generate_cert_exit, Status, Output})
    end.

decode_cert_chain(CertPemBin) ->
    Certs = split_certs(CertPemBin),
    decode_cert_chain(Certs, []).

decode_cert_chain([], Res) -> {ok, lists:reverse(Res)};
decode_cert_chain([Cert | Tail], Res) ->
    case decode_single_certificate(Cert) of
        {error, _} = Err -> Err;
        Der -> decode_cert_chain(Tail, [Der | Res])
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

do_decode_certificates(CertPemBin) ->
    try
        public_key:pem_decode(CertPemBin)
    catch T:E ->
            ?log_error("Unknown error while parsing certificate:~n~p", [{T,E,erlang:get_stacktrace()}]),
            malformed_cert
    end.

validate_cert_pem_entry({'Certificate', DerCert, not_encrypted}) ->
    DerCert;
validate_cert_pem_entry({'Certificate', _, _}) ->
    {error, encrypted_certificate};
validate_cert_pem_entry({BadType, _, _}) ->
    {error, {invalid_certificate_type, BadType}}.

validate_pkey(PKeyPemBin) ->
    try public_key:pem_decode(PKeyPemBin) of
        [{Type, _, not_encrypted} = Entry] ->
            case Type of
                'RSAPrivateKey' ->
                    {ok, Entry};
                'DSAPrivateKey' ->
                    {ok, Entry};
                Other ->
                    ?log_debug("Invalid pkey type: ~p", [Other]),
                    {error, {invalid_pkey, Type}}
            end;
        [{_, _, _}] ->
            {error, encrypted_pkey};
        [] ->
            {error, malformed_pkey};
        Other ->
            ?log_debug("Too many (~p) pkey entries.", [length(Other)]),
            {error, too_many_pkey_entries}
    catch T:E ->
            ?log_error("Unknown error while parsing private key:~n~p", [{T,E,erlang:get_stacktrace()}]),
            {error, malformed_pkey}
    end.

validate_cert_and_pkey({'Certificate', DerCert, not_encrypted}, PKey) ->
    case validate_pkey(PKey) of
        {ok, DerKey} ->
            DecodedCert = public_key:pkix_decode_cert(DerCert, otp),

            TBSCert = DecodedCert#'OTPCertificate'.tbsCertificate,
            PublicKeyInfo = TBSCert#'OTPTBSCertificate'.subjectPublicKeyInfo,
            PublicKey = PublicKeyInfo#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
            DecodedKey = public_key:pem_entry_decode(DerKey),

            Msg = <<"1234567890">>,
            Signature = public_key:sign(Msg, sha, DecodedKey),
            case public_key:verify(Msg, sha, Signature, PublicKey) of
                true ->
                    ok;
                false ->
                    {error, cert_pkey_mismatch}
            end;
        Err ->
            Err
    end.

split_certs(PEMCerts) ->
    Begin = <<"-----BEGIN">>,
    [<<>> | Parts0] = binary:split(PEMCerts, Begin, [global]),
    [<<Begin/binary,P/binary>> || P <- Parts0].

extract_cert_and_pkey(Output) ->
    case split_certs(Output) of
        [Cert, PKey] ->
            case decode_single_certificate(Cert) of
                {error, Error} ->
                    erlang:exit({bad_generated_cert, Cert, Error});
                _ ->
                    case validate_pkey(PKey) of
                        {ok, _} ->
                            {Cert, PKey};
                        Err ->
                            erlang:exit({bad_generated_pkey, PKey, Err})
                    end
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
attribute_string(?'id-at-commonName') ->
    "CN";
attribute_string(_) ->
    undefined.

format_attribute([#'AttributeTypeAndValue'{type = Type,
                                           value = Value}], Acc) ->
    case attribute_string(Type) of
        undefined ->
            Acc;
        Str ->
            [[Str, "=", format_value(Value)] | Acc]
    end.

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

extract_fields_by_type({rdnSequence, STVList}, Type) ->
    [format_value(V) || [#'AttributeTypeAndValue'{type = T, value = V}] <- STVList,
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

get_info(DerCert) ->
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

parse_cluster_ca(CA) ->
    case decode_single_certificate(CA) of
        {error, Error} ->
            {error, Error};
        RootCertDer ->
            try
                {Subject, NotBefore, NotAfter} = get_info(RootCertDer),
                UTC = calendar:datetime_to_gregorian_seconds(
                        calendar:universal_time()),
                case NotBefore > UTC orelse NotAfter < UTC of
                    true ->
                        {error, not_valid_at_this_time};
                    false ->
                        {ok, [{pem, CA},
                              {subject, Subject},
                              {expires, NotAfter}]}
                end
            catch T:E ->
                    ?log_error("Failed to get certificate info:~n~p~n~p",
                               [RootCertDer, {T,E,erlang:get_stacktrace()}]),
                    {error, malformed_cert}
            end
    end.

set_cluster_ca(CA) ->
    case parse_cluster_ca(CA) of
        {ok, Props} ->
            NewCert = proplists:get_value(pem, Props),
            RV = ns_config:run_txn(
                   fun (Config, SetFn) ->
                           CurCerts =
                               case ns_config:search(Config, cert_and_pkey) of
                                   {value, {NewCert, _}} ->
                                       {error, already_in_use};
                                   {value, {_, _} = Pair} ->
                                       {ok, Pair};
                                   {value, {_, GeneratedCert1, GeneratedKey1}} ->
                                       {ok, {GeneratedCert1, GeneratedKey1}};
                                   false ->
                                       {ok, generate_cert_and_pkey()}
                               end,

                           case CurCerts of
                               {ok, {GeneratedCert, GeneratedKey}} ->
                                   NewCertPKey = {Props, GeneratedCert,
                                                  GeneratedKey},
                                   {commit, SetFn(cert_and_pkey, NewCertPKey,
                                                  Config)};
                               {error, Reason} ->
                                   {abort, Reason}
                           end
                   end),
            case RV of
                {commit, _} ->
                    {ok, Props};
                {abort, Reason} ->
                    {error, Reason};
                retry_needed ->
                    erlang:error(exceeded_retries)
            end;
        {error, Error} ->
            ?log_error("Certificate authority validation failed with ~p", [Error]),
            {error, Error}
    end.

set_generated_public_ca(CA) ->
    ns_config:set(cert_and_pkey, {CA, undefined}).

-record(verify_state, {last_subject, root_cert, chain_len}).

get_subject(Cert) ->
    TBSCert = Cert#'OTPCertificate'.tbsCertificate,
    format_name(TBSCert#'OTPTBSCertificate'.subject).

verify_fun(Cert, Event, State) ->
    Subject = get_subject(Cert),
    ?log_debug("Certificate verification event : ~p", [{Subject, Event}]),

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
            ?log_error("Certificate ~p validation failed with reason ~p",
                       [Subject, Error]),

            Trace = erlang:process_info(self(), [current_stacktrace]),
            OtpCert = public_key:pkix_decode_cert(State#verify_state.root_cert, otp),
            InitValidationState =
                pubkey_cert:init_validation_state(OtpCert, State#verify_state.chain_len, []),

            ?log_debug("Certificate validation trace:~n     Initial Context: ~p~n"
                       "     Cert: ~p~n     Stack: ~p~n",
                       [InitValidationState, Cert, Trace]),
            {fail, {Error, Subject}};
        {extension, Ext} ->
            ?log_warning(
               "Certificate ~p validation spotted an unknown extension ~p",
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
    catch T:E ->
            ?log_error("Unknown error while parsing certificate chain:~n~p", [{T,E,erlang:get_stacktrace()}]),
            {error, {bad_chain, malformed_cert}}
    end.

validate_chain([]) ->
    ok;
validate_chain([Entry | Rest]) ->
    case validate_cert_pem_entry(Entry) of
        {error, Error} ->
            {error, {bad_chain, Error}};
        _ ->
            validate_chain(Rest)
    end.

validate_chain_signatures({'Certificate', RootCertDer, not_encrypted}, Chain) ->
    DerChain = [Der || {'Certificate', Der, not_encrypted} <- Chain],
    public_key:pkix_path_validation(
      RootCertDer, DerChain,
      [{verify_fun, {fun verify_fun/3, #verify_state{root_cert = RootCertDer,
                                                     chain_len = length(Chain)}}}]).

decode_and_validate_chain(CA, Chain) ->
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
                    case validate_chain_signatures(CA, PemEntriesReversed) of
                        {error, _} = Err ->
                            Err;
                        {ok, _} ->
                            {ok, PemEntriesReversed}
                    end
            end
    end.

get_chain_info(Chain) ->
    lists:foldl(fun ({'Certificate', DerCert, not_encrypted}, Acc) ->
                        Decoded = public_key:pkix_decode_cert(DerCert, otp),
                        TBSCert = Decoded#'OTPCertificate'.tbsCertificate,
                        Validity = TBSCert#'OTPTBSCertificate'.validity,
                        NotAfter = convert_date(Validity#'Validity'.notAfter),
                        case Acc of
                            undefined ->
                                {format_name(TBSCert#'OTPTBSCertificate'.subject), NotAfter};
                            {Sub, Expiration} when Expiration > NotAfter  ->
                                {Sub, NotAfter};
                            Pair ->
                                Pair
                        end
                end, undefined, lists:reverse(Chain)).

set_node_certificate_chain(ClusterCA, Chain, PKey) ->
    [CAPemEntry] = public_key:pem_decode(ClusterCA),

    case decode_and_validate_chain(CAPemEntry, Chain) of
        {ok, PemEntriesReversed} ->
            {Subject, Expiration} =
                get_chain_info(PemEntriesReversed),

            %% both chains Cert -> I1 -> I2 and Cert -> I1 -> I2 -> Root
            %% are valid and will pass the validation
            [NodeCert | CAChain] =
                lists:reverse(
                  case PemEntriesReversed of
                      [CAPemEntry | _] ->
                          PemEntriesReversed;
                      _ ->
                          [CAPemEntry | PemEntriesReversed]
                  end),
            case validate_cert_and_pkey(NodeCert, PKey) of
                ok ->
                    NodeCertPemEncoded = public_key:pem_encode([NodeCert]),
                    Props = [{subject, Subject},
                             {expires, Expiration},
                             {verified_with, erlang:md5(ClusterCA)},
                             {pem, NodeCertPemEncoded}],
                    case ns_ssl_services_setup:set_node_certificate_chain(
                           Props,
                           public_key:pem_encode(CAChain),
                           NodeCertPemEncoded, PKey) of
                        ok ->
                            {ok, Props};
                        Err ->
                            Err
                    end;
                Err ->
                    Err
            end;
        {error, _} = Error ->
            Error
    end.

apply_certificate_chain_from_inbox() ->
    case ns_config:search(cert_and_pkey) of
        {value, {ClusterCAProps, _, _}} ->
            ClusterCA = proplists:get_value(pem, ClusterCAProps),
            apply_certificate_chain_from_inbox(ClusterCA);
        _ ->
            {error, no_cluster_ca}
    end.

apply_certificate_chain_from_inbox(ClusterCA) ->
    case file:read_file(inbox_chain_path()) of
        {ok, ChainPEM} ->
            case file:read_file(inbox_pkey_path()) of
                {ok, PKeyPEM} ->
                    set_node_certificate_chain(ClusterCA, ChainPEM, PKeyPEM);
                {error, Reason} ->
                    {error, {read_pkey, inbox_pkey_path(), Reason}}
            end;
        {error, Reason} ->
            {error, {read_chain, inbox_chain_path(), Reason}}
    end.

get_warnings(CAProps) ->
    ClusterCA = proplists:get_value(pem, CAProps),
    false = ClusterCA =:= undefined,

    VerifiedWith = erlang:md5(ClusterCA),

    Config = ns_config:get(),
    Nodes = ns_node_disco:nodes_wanted(Config),

    lists:foldl(
      fun (Node, Acc) ->
              case ns_config:search(Config, {node, Node, cert}) of
                  {value, Props} ->
                      case proplists:get_value(verified_with, Props) of
                          VerifiedWith ->
                              Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
                              WarningDays = ns_config:read_key_fast({cert, expiration_warning_days}, 7),
                              WarningThreshold = Now + WarningDays * 24 * 60 * 60,

                              case proplists:get_value(expires, Props) of
                                  A when is_integer(A) andalso A =< Now ->
                                      [{Node, expired} | Acc];
                                  A when  is_integer(A) andalso A =< WarningThreshold ->
                                      [{Node, {expires_soon, A}} | Acc];
                                  A when is_integer(A) ->
                                      Acc
                              end;
                          _ ->
                              [{Node, mismatch} | Acc]
                      end;
                  false ->
                      [{Node, mismatch} | Acc]
              end
      end, [], Nodes).

get_node_cert_info(Node) ->
    Props = ns_config:read_key_fast({node, Node, cert}, []),
    proplists:delete(verified_with, Props).

tls_server_validation_options() ->
    case ns_server_cert:cluster_ca() of
        {_, _} -> [];
        {UploadedCAProps, _, _} ->
            Pem = proplists:get_value(pem, UploadedCAProps),
            [{verify, verify_peer},
             {cacerts, [ns_server_cert:decode_single_certificate(Pem)]},
             {depth, ?ALLOWED_CERT_CHAIN_LENGTH}]
    end.
