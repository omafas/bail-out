%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc implementation of server side SCRAM-SHA according to
%%      https://tools.ietf.org/html/rfc5802
%%      https://tools.ietf.org/html/rfc7804

-module(scram_sha).

-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SHA_DIGEST_SIZE, 20).
-define(SHA256_DIGEST_SIZE, 32).
-define(SHA512_DIGEST_SIZE, 64).

-export([start_link/0,
         authenticate/1,
         meta_header/0,
         get_resp_headers_from_req/1,
         hash_password/2,
         auth_info_key/1,
         supported_types/0,
         get_fallback_salt/0]).

%% callback for token_server
-export([init/0]).

start_link() ->
    token_server:start_link(?MODULE, 1024, 15, undefined).

init() ->
    ok.

meta_header() ->
    "menelaus-auth-scram-sha_reply".

get_resp_headers_from_req(Req) ->
    get_resp_headers(mochiweb_request:get_header_value(meta_header(), Req)).

get_resp_headers(undefined) ->
    [];
get_resp_headers("A" ++ Value) ->
    [{"WWW-Authenticate", Value}];
get_resp_headers("I" ++ Value) ->
    [{"Authentication-Info", Value}].

server_first_message(Nonce, Salt, IterationCount) ->
    "r=" ++ Nonce ++ ",s=" ++ Salt ++ ",i=" ++ integer_to_list(IterationCount).

encode_with_sid(Sid, Message) ->
    "sid=" ++ base64:encode_to_string(Sid) ++
        ",data=" ++ base64:encode_to_string(Message).

reply_success(Sid, Identity, ServerProof) ->
    {ok, Identity,
     [{meta_header(),
       "I" ++ encode_with_sid(
                Sid,
                "v=" ++ base64:encode_to_string(ServerProof))}]}.

reply_first_step(Sha, Sid, Msg) ->
    Hdr = "A" ++ www_authenticate_prefix(Sha) ++ " " ++
        encode_with_sid(Sid, Msg),
    {first_step, [{meta_header(), Hdr}]}.

www_authenticate_prefix(sha512) ->
    "SCRAM-SHA-512";
www_authenticate_prefix(sha256) ->
    "SCRAM-SHA-256";
www_authenticate_prefix(sha) ->
    "SCRAM-SHA-1".

parse_authorization_header_prefix("SHA-512 " ++ Rest) ->
    {sha512, Rest};
parse_authorization_header_prefix("SHA-256 " ++ Rest) ->
    {sha256, Rest};
parse_authorization_header_prefix("SHA-1 " ++ Rest) ->
    {sha, Rest};
parse_authorization_header_prefix(_) ->
    error.

auth_info_key(sha512) ->
    <<"sha512">>;
auth_info_key(sha256) ->
    <<"sha256">>;
auth_info_key(sha) ->
    <<"sha1">>.

parse_authorization_header(Value) ->
    Sections = string:tokens(Value, ","),
    ParsedParams =
        lists:keysort(
          1,
          lists:filtermap(
            fun ("data=" ++ Rest) ->
                    {true, {data, Rest}};
                ("sid=" ++ Rest) ->
                    {true, {sid, Rest}};
                (_) ->
                    false
            end, Sections)),
    case ParsedParams of
        [{data, D}] ->
            {undefined, D};
        [{data, D}, {sid, S}] ->
            {S, D};
        _ ->
            error
    end.

parse_client_first_message("n,," ++ Bare) ->
    Sections = string:tokens(Bare, ","),
    WithoutReserved =
        lists:dropwhile(?cut(not lists:prefix("n=", _)), Sections),
    case WithoutReserved of
        ["n=" ++ Name, "r=" ++ Nonce | _] ->
            {Name, Nonce, Bare};
        _ ->
            error
    end;
parse_client_first_message(_) ->
    error.

parse_client_final_message(Msg) ->
    Sections = string:tokens(Msg, ","),
    case Sections of
        %% <<"n,,">> = base64:decode("biws")
        ["c=biws", "r=" ++ Nonce | Rest = [_|_]] ->
            case lists:last(Rest) of
                "p=" ++ Proof ->
                    MsgWithoutProof =
                        lists:sublist(Msg, length(Msg) - length(Proof) - 3),
                    {Nonce, Proof, MsgWithoutProof};
                _ ->
                    error
            end;
        _ ->
            error
    end.

authenticate(AuthHeader) ->
    case parse_authorization_header_prefix(AuthHeader) of
        {Sha, Rest} ->
            case parse_authorization_header(Rest) of
                error ->
                    auth_failure;
                {EncodedSid, EncodedData} ->
                    case (catch {case EncodedSid of
                                     undefined ->
                                         undefined;
                                     _ ->
                                         base64:decode(EncodedSid)
                                 end,
                                 base64:decode_to_string(EncodedData)}) of
                        {'EXIT', _} ->
                            auth_failure;
                        {Sid, Data} ->
                            authenticate(Sha, Sid, Data)
                    end
            end;
        error ->
            auth_failure
    end.

authenticate(Sha, undefined, Data) ->
    case parse_client_first_message(Data) of
        error ->
            auth_failure;
        {Name, Nonce, Bare} ->
            handle_client_first_message(Sha, Name, Nonce, Bare)
    end;
authenticate(Sha, Sid, Data) ->
    case parse_client_final_message(Data) of
        error ->
            auth_failure;
        {Nonce, Proof, ClientFinalMessage} ->
            handle_client_final_message(Sha, Sid, Nonce, Proof,
                                        ClientFinalMessage)
    end.

gen_nonce() ->
    [misc:rand_uniform(48,125) || _ <- lists:seq(1,15)].

find_auth(Name) ->
    case ns_config_auth:get_admin_user_and_auth() of
        {Name, {auth, Auth}} ->
            {Auth, admin};
        _ ->
            {menelaus_users:get_auth_info({Name, local}), local}
    end.

find_auth_info(Sha, Name) ->
    case find_auth(Name) of
        {false, _} ->
            undefined;
        {AuthInfo, Domain} ->
            case proplists:get_value(auth_info_key(Sha), AuthInfo) of
                undefined ->
                    undefined;
                {Info} ->
                    {Info, Domain}
            end
    end.

get_fallback_salt() ->
    ns_config:read_key_fast(scramsha_fallback_salt, <<"salt">>).

get_salt_and_iterations(Sha, Name) ->
    %% calculating it here to avoid performance shortcut
    FallbackSalt = base64:encode_to_string(
                     crypto:mac(hmac, Sha, Name, get_fallback_salt())),
    case find_auth_info(Sha, Name) of
        undefined ->
            {FallbackSalt, iterations()};
        {Props, _} ->
            {binary_to_list(proplists:get_value(<<"s">>, Props)),
             proplists:get_value(<<"i">>, Props)}
    end.

get_salted_password_and_domain(Sha, Name) ->
    case find_auth_info(Sha, Name) of
        undefined ->
            {<<"anything">>, undefined};
        {Props, Domain} ->
            {base64:decode(proplists:get_value(<<"h">>, Props)), Domain}
    end.

-record(memo, {auth_message,
               name,
               nonce}).

handle_client_first_message(Sha, Name, Nonce, Bare) ->
    {Salt, IterationCount} = get_salt_and_iterations(Sha, Name),
    ServerNonce = Nonce ++ gen_nonce(),
    ServerMessage =
        server_first_message(ServerNonce, Salt, IterationCount),
    Memo = #memo{auth_message = Bare ++ "," ++ ServerMessage,
                 name = Name,
                 nonce = ServerNonce},
    Sid = token_server:generate(?MODULE, Memo),
    reply_first_step(Sha, Sid, ServerMessage).

calculate_client_proof(Sha, SaltedPassword, AuthMessage) ->
    ClientKey = crypto:mac(hmac, Sha, SaltedPassword, <<"Client Key">>),
    StoredKey = crypto:hash(Sha, ClientKey),
    ClientSignature = crypto:mac(hmac, Sha, StoredKey, AuthMessage),
    misc:bin_bxor(ClientKey, ClientSignature).

calculate_server_proof(Sha, SaltedPassword, AuthMessage) ->
    ServerKey = crypto:mac(hmac, Sha, SaltedPassword, <<"Server Key">>),
    crypto:mac(hmac, Sha, ServerKey, AuthMessage).

handle_client_final_message(Sha, Sid, Nonce, Proof, ClientFinalMessage) ->
    case token_server:take(?MODULE, Sid) of
        false ->
            auth_failure;
        {ok, #memo{auth_message = AuthMessage,
                   name = Name,
                   nonce = ServerNonce}} ->
            {SaltedPassword, Domain} =
                get_salted_password_and_domain(Sha, Name),
            FullAuthMessage = AuthMessage ++ "," ++ ClientFinalMessage,
            ServerProof = handle_proofs(Sha, SaltedPassword, Proof,
                                        FullAuthMessage),
            case {misc:compare_secure(Nonce, ServerNonce), Domain,
                  ServerProof} of
                {true, D, P} when D =/= undefined, P =/= error ->
                    reply_success(Sid, {Name, Domain}, ServerProof);
                _ ->
                    auth_failure
            end
    end.

handle_proofs(Sha, SaltedPassword, Proof, AuthMessage) ->
    ClientProof = calculate_client_proof(Sha, SaltedPassword, AuthMessage),
    case misc:compare_secure(Proof, base64:encode_to_string(ClientProof)) of
        false ->
            error;
        true ->
            calculate_server_proof(Sha, SaltedPassword, AuthMessage)
    end.

pbkdf2(Sha, Password, Salt, Iterations) ->
    Initial = crypto:mac(hmac, Sha, Password, <<Salt/binary, 1:32/integer>>),
    pbkdf2_iter(Sha, Password, Iterations - 1, Initial, Initial).

pbkdf2_iter(_Sha, _Password, 0, _Prev, Acc) ->
    Acc;
pbkdf2_iter(Sha, Password, Iteration, Prev, Acc) ->
    Next = crypto:mac(hmac, Sha, Password, Prev),
    pbkdf2_iter(Sha, Password, Iteration - 1, Next, crypto:exor(Next, Acc)).

hash_password(Type, Password) ->
    Iterations = iterations(),
    Len = case Type of
              sha -> ?SHA_DIGEST_SIZE;
              sha256 -> ?SHA256_DIGEST_SIZE;
              sha512 -> ?SHA512_DIGEST_SIZE
          end,
    Salt = crypto:strong_rand_bytes(Len),
    Hash = pbkdf2(Type, Password, Salt, Iterations),
    {Salt, Hash, Iterations}.

iterations() ->
    ns_config:read_key_fast(memcached_password_hash_iterations, 4000).

supported_types() ->
    [sha512, sha256, sha].


-ifdef(TEST).
build_client_first_message(Sha, Nonce, User) ->
    Bare = "n=" ++ User ++ ",r=" ++ Nonce,
    "SCRAM-" ++ Prefix  = www_authenticate_prefix(Sha),
    {Prefix ++ " data=" ++ base64:encode_to_string("n,," ++ Bare), Bare}.

parse_server_first_response(Sha, Nonce, Header) ->
    Prefix = "A" ++ www_authenticate_prefix(Sha) ++ " ",
    Message = string:prefix(Header, Prefix),
    ["sid=" ++ Sid, "data=" ++ Data] = string:tokens(Message, ","),

    DecodedData = base64:decode_to_string(Data),

    ["r=" ++ ServerNonce, "s=" ++ Salt, "i=" ++ Iter] =
        string:tokens(DecodedData, ","),

    ?assertNotEqual(nomatch, string:prefix(ServerNonce, Nonce)),
    {Sid, base64:decode(Salt), list_to_integer(Iter), ServerNonce, DecodedData}.

build_client_final_message(Sha, Sid, Nonce, SaltedPassword, Message) ->
    WithoutProof = "c=biws,r=" ++ Nonce,
    FullMessage = Message ++ "," ++ WithoutProof,

    Proof = base64:encode_to_string(calculate_client_proof(
                                      Sha, SaltedPassword, FullMessage)),

    Data = WithoutProof ++ ",p=" ++ Proof,

    "SCRAM-" ++ Prefix  = www_authenticate_prefix(Sha),
    {Prefix ++ " data=" ++ base64:encode_to_string(Data) ++ ",sid=" ++ Sid,
     FullMessage}.

check_server_proof(Sha, Sid, SaltedPassword, Message, Header) ->
    Prefix = "Isid=" ++ Sid ++ ",data=",
    "v=" ++ ProofFromServer =
        base64:decode_to_string(string:prefix(Header, Prefix)),

    Proof = calculate_server_proof(Sha, SaltedPassword, Message),
    ?assertEqual(ProofFromServer, base64:encode_to_string(Proof)).


client_auth(Sha, User, Password, Nonce) ->
    {ToSend, ClientFirstMessage} =
        build_client_first_message(Sha, Nonce, User),

    MetaHeader = meta_header(),
    case authenticate(ToSend) of
        {first_step, [{MetaHeader, Header}]} ->
            {Sid, Salt, Iterations, ServerNonce, ServerFirstMessage} =
                parse_server_first_response(Sha, Nonce, Header),

            SaltedPassword = pbkdf2(Sha, Password, Salt, Iterations),
            {ToSend1, ForProof} =
                build_client_final_message(
                  Sha, Sid, ServerNonce, SaltedPassword,
                  ClientFirstMessage ++ "," ++ ServerFirstMessage),
            case authenticate(ToSend1) of
                {ok, {User, admin}, [{MetaHeader, Header1}]} ->
                    check_server_proof(Sha, Sid, SaltedPassword,
                                       ForProof, Header1),
                    ok;
                auth_failure ->
                    auth_failure
            end;
        auth_failure ->
            first_stage_failed
    end.

pbkdf2_test() ->
    PBKDF2 = fun (T, S) ->
                     H = pbkdf2(T, "4149a7598deb1a04e2ea7ac8d915f6c3",
                                base64:decode(S), 4000),
                     base64:encode_to_string(H)
             end,
    ?assertEqual("ZIlutBvCilUTSMtgRRHzkomuNbc=",
                 PBKDF2(sha,
                        "mtlJBgjGNa7S63biAXQ06EPzhXg=")),
    ?assertEqual("ELrpMLYTEg2BrqsAE+33vpIjk/3a8mc8cBVcE06G38k=",
                 PBKDF2(sha256,
                        "SPPCz+lBjL6WNvsssl04Wr6FlffsYMxFXlUM+LwdiY8=")),
    ?assertEqual("GB1lsONsRtKXyL59L84/2sYQTTVL6d7dplmhNN2dpys+"
                 "wJr5UY3hfAj4zK3ZatjQkUZHjnlAtrZzvjpzQboNcg==",
                 PBKDF2(sha512,
                        "31uxbpP++gOzRdXBY3iOdIeTm/3dutkz/58VFKfZzffc"
                        "YrNAm8D1YNDLjjf1AfUGckWFB63nQjUQHyo2fXZC/g==")).

calculate_client_proof_regression_test() ->
    ?assertEqual(
       "nNoiOTTsg6xXguLqGhW21taip2Ec/iSyrxmQunnB5o4FFHJ1uOrqO6NHR5i0llfFNgkc"
       "XkgArkX3HEzUv8pSuA==",
       base64:encode_to_string(
         calculate_client_proof(sha512, "asdsvdbxgfbdf", "ggkjhlhiuyfhcf"))).

calculate_server_proof_regression_test() ->
    ?assertEqual(
       "psZBJnp2+qyiPJOICKNvaYIMbg1hl3RqH613PG03zFFN4EQQLDA/Xg5hMHxGBK2y2nTxk"
       "xYW7EiK5/PrZve/yg==",
       base64:encode_to_string(
         calculate_server_proof(sha512, "asdsvdbxgfbdf", "ggkjhlhiuyfhcf"))).

scram_sha_t({User, Password, Nonce, _}) ->
    lists:flatmap(
      fun (Sha) ->
              Postfix = " test for " ++ atom_to_list(Sha),
              [{"Successful auth" ++ Postfix,
                ?_assertEqual(ok, client_auth(Sha, User, Password, Nonce))},
               {"Wrong password" ++ Postfix,
                ?_assertEqual(auth_failure,
                              client_auth(Sha, User, "wrong", Nonce))},
               {"Unknown user" ++ Postfix,
                ?_assertEqual(auth_failure,
                              client_auth(Sha, "wrong", "wrong", Nonce))}]
      end, supported_types()).

setup_t() ->
    meck:new(menelaus_users, [passthrough]),
    meck:expect(menelaus_users, get_auth_info, fun(_) -> false end),

    ns_config:test_setup([]),
    {ok, Pid} = start_link(),

    User = "testuser",
    Password = "qwerty",
    Nonce = gen_nonce(),

    Auth = menelaus_users:build_scram_auth(Password),
    ns_config:test_setup([{rest_creds, {User, {auth, Auth}}}]),
    {User, Password, Nonce, Pid}.

cleanup_t({_, _, _, Pid}) ->
    unlink(Pid),
    misc:terminate_and_wait(Pid, normal),
    meck:unload(menelaus_users).

scram_sha_test_() ->
    {setup, fun setup_t/0, fun cleanup_t/1, fun scram_sha_t/1}.
-endif.
