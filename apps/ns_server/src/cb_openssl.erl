-module(cb_openssl).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([kbkdf_hmac/5, ensure_nif_is_loaded/0]).

-nifs([kbkdf_hmac/5]).

%% For unknown reason compiler thinks inlining is on, but it is actually off
%% Disabling the warning for now as we treat warnings as errors.
-compile(nowarn_nif_inline).

%%%===================================================================
%%% NIF loading
%%%===================================================================

-on_load(load_nif/0).

load_nif() ->
    PrivDir = case code:priv_dir(ns_server) of
                  {error, bad_name} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      filename:join([EbinDir, "..", "priv"]);
                  Path ->
                       Path
              end,
    SoName = filename:join(PrivDir, "cb_openssl_nif"),
    erlang:load_nif(SoName, 0).

%%%===================================================================
%%% API
%%%===================================================================

-spec ensure_nif_is_loaded() -> ok.
ensure_nif_is_loaded() ->
    %% Make sure the openssl NIF is loaded
    %% If something is wrong with the NIF, we want to crash immediately.
    {ok, _} = cb_openssl:kbkdf_hmac(sha256, <<"key">>, <<"info">>, <<"salt">>,
                                    32),
    ok.

%% First argument is crypto:hmac_hash_algorithm(), but it can't be specified in
%% the spec because it is not exported from the crypto module
-spec kbkdf_hmac(atom(), binary(), binary(), binary(), non_neg_integer()) ->
          {ok, binary()} | {error, term()}.
%% @doc Key-Based Key Derivation Function (KBKDF) using OpenSSL.
%%
%% Parameters:
%% - HMAC hash algorithm: crypto:hmac_hash_algorithm()
%% - Key: The key material (binary) up to 512 bytes
%% - Info: Information/label parameter (binary, can be empty)
%% - Salt: Salt parameter (binary, can be empty)
%% - OutLen: Length of the derived key in bytes (up to 512 bytes)
%% Salt + Info length is limited to 524288 bytes
kbkdf_hmac(_HashAlgorithm, _Key, _Info, _Salt, _OutLen) ->
    erlang:nif_error({not_loaded,
                      [{module, ?MODULE},
                       {function, kbkdf_hmac},
                       {arity, 5}]}).


-ifdef(TEST).
test_kbkdf_regression_test() ->
    K32_0 = <<0:256>>,
    K32_random = base64:decode("xauBr+osO9BflY0hh6a+IcSub3J4EzAA0ZVWrRh4aWE="),
    K64_random = base64:decode("8CSNWij4qeMXJmCLHgF4qD6G1pEi1n2pt7hlG36C3FAn"
                               "8dq7YIgSiDLZhSInPo4UcqnKdYiDBwK7Q1D/yOsA5Q=="),
    Derive = fun (H, K, I, S, L) ->
                 {ok, Out} = kbkdf_hmac(H, K, I, S, L),
                 base64:encode_to_string(Out)
             end,
    ?assertEqual("e0mP8pHxWSaCYhV29u0BThZv5hgQpW0DnHZaWe6YwMk=",
                 Derive(sha256, K32_0, <<>>, <<>>, 32)),
    ?assertEqual("2iGWpXna4rlot1RyzemFDH01maPjbXHDIdcPMQUtzBg=",
                 Derive(sha256, K32_random, <<"test">>, <<"encryption">>, 32)),
    ?assertEqual("VVQdMaqcdLVaWP+5RwSG1zAzcKc9PWwmKXO5cnOT8Zoe"
                 "1CLdAhgRi6mlTLoIrA6rZIQbUxlLAty77CGXuEWf4w==",
                 Derive(sha512, K64_random, <<"test">>, <<"encryption">>, 64)).
-endif.
