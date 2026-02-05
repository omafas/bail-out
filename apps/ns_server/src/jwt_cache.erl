%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

%% @doc
%% This module implements the JWT key cache responsible for managing all types
%% of JWT keys: PEM files, static JWKS objects, JWKS URIs, and internal keys.
%%
%% The cache stores parsed jose_jwk objects for all issuer types to avoid
%% repeated parsing during authentication requests:
%%   - PEM keys are parsed once and stored in the cache
%%   - Static JWKS objects are converted to jose_jwk objects and cached
%%   - JWKS URI responses are parsed and cached with expiry times
%%
%% Cache Consistency:
%% The cache may not be immediately synchronized with the latest issuer
%% properties. When issuer properties change (e.g., during settings updates),
%% there may be a brief period where the cache contains stale keys. During
%% this period:
%%   - The cache will continue to serve the old keys if available
%%   - If a key lookup fails, the client is expected to retry
%%   - The cache will eventually be updated via the settings_update handler
%%   - No guarantees are made about using the latest available keys
%%
%% The cache is refreshed in the following ways:
%%   - Settings Updates:
%%     * All cache entries are invalidated when JWT settings change
%%     * All static keys (PEM and JWKS) are parsed and cached immediately
%%     * JWKS URI issuers are refreshed immediately after settings update
%%
%%   - Internal Key Updates:
%%     * Internal keys are refreshed when they change via internal_key_update
%%     * Internal keys are also refreshed during settings_update (as all entries
%%       are invalidated)
%%
%%   - On-demand refresh (JWKS URI only):
%%     * If a JWKS URI key lookup fails or is expired, a refresh is attempted
%%     * A cooldown period of 1 minute prevents excessive network requests
%%
%%   - Periodic refresh (JWKS URI only):
%%     * Background timer refreshes all JWKS URIs periodically
%%     * Random jitter prevents all nodes from refreshing at the same time
%%
%% All updates are serialized via gen_server.
%% All updates are atomic. Old keys are removed and replaced with new keys for
%% a given issuer in a single operation.
%%
%% Consistency:
%% - Each node maintains its own cache and caches are refreshed independently
%%   The refresh times need not be consistent across nodes if a node restarts,
%%   for instance.
%%
%% Upgrade Considerations:
%% - The ETS cache is in-memory and rebuilt when a node starts up (after an
%%   upgrade) or settings change.
%% - The JWT settings stored in chronicle_kv are format-independent (not tied to
%%   erlang-jose or Erlang pubkey() formats)

-module(jwt_cache).
-behaviour(gen_server).

-include("ns_common.hrl").
-include("jwt.hrl").
-include_lib("jose/include/jose_jwk.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0,
         get_jwk/2,
         get_jwks/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(cache_state, {
                      refresh_timer_ref :: undefined | reference(),
                      refresh_interval_ms :: undefined | pos_integer()
                     }).

%% Cache entry for a single JWKS URI issuer
-record(jwks_cache_entry, {
                           kid_to_jwk :: jwt_kid_to_jwk(),
                           fetch_time :: integer(),
                           expiry :: undefined | integer()
                          }).

-define(JWKS_DEFAULT_EXPIRY_S, 6 * 60 * 60). %% 6 hours
-define(JWKS_SYNC_TIMEOUT_MS, 10000). %% 10 seconds
-define(JWKS_COOLDOWN_INTERVAL_MS,
        ?get_param(jwks_cooldown_interval_ms, 60000)). %% 1 minute
-define(JWKS_FETCH_RETRY_COUNT, 1).
-define(JWKS_REFRESH_JITTER_MS, 30000). %% 30 seconds of maximum jitter
-define(JWKS_REFRESH_TIMEOUT_MS, 90000).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_jwk(IssuerProps :: map(), Kid :: binary() | undefined) ->
          {ok, jose_jwk:key()} | {error, binary()}.
get_jwk(#{name := Issuer} = Props, Kid) ->
    case ets:lookup(?MODULE, Issuer) of
        [{Issuer, #jwks_cache_entry{kid_to_jwk = KidToJWKMap,
                                    expiry = Expiry}}] ->
            IsExpired = Expiry /= undefined andalso
                erlang:monotonic_time(millisecond) > Expiry,

            case IsExpired of
                true ->
                    ?log_debug("JWT issuer ~p entries have expired", [Issuer]),
                    direct_get_jwk(Props, Kid);
                false ->
                    case maps:find(Kid, KidToJWKMap) of
                        {ok, JWK} -> {ok, JWK};
                        error -> direct_get_jwk(Props, Kid)
                    end
            end;
        [] ->
            direct_get_jwk(Props, Kid)
    end.

%% This is used only for manual/air-gapped OIDC issuers.
-spec get_jwks(IssuerProps :: map()) ->
          {ok, jose_jwk:key()} | {error, binary()}.
get_jwks(#{public_key_source := jwks_uri, name := Issuer} = Props) ->
    case ets:lookup(?MODULE, Issuer) of
        [{Issuer, #jwks_cache_entry{kid_to_jwk = KidToJWKMap,
                                    expiry = Expiry}}] ->
            IsExpired = Expiry /= undefined andalso
                erlang:monotonic_time(millisecond) > Expiry,

            case IsExpired of
                false ->
                    jwks_from_kid_map(KidToJWKMap);
                true ->
                    refresh_and_build_jwks(Props)
            end;
        [] ->
            refresh_and_build_jwks(Props)
    end;
get_jwks(#{name := Issuer} = _Props) ->
    %% Static sources are cached during settings_update and do not expire.
    lookup_and_build_jwks(Issuer).

%% Construct a jose_jwk set from a cached kid->jwk map (values already
%% jose_jwk:key()).
-spec jwks_from_kid_map(map()) -> {ok, jose_jwk:key()} | {error, binary()}.
jwks_from_kid_map(KidToJWKMap) when is_map(KidToJWKMap) ->
    Keys = maps:values(KidToJWKMap),
    case Keys of
        [] ->
            {error, <<"No keys found">>};
        _ ->
            {ok, #jose_jwk{keys = {jose_jwk_set, Keys}}}
    end.

-spec lookup_and_build_jwks(string()) ->
          {ok, jose_jwk:key()} | {error, binary()}.
lookup_and_build_jwks(Issuer) ->
    case ets:lookup(?MODULE, Issuer) of
        [{Issuer, #jwks_cache_entry{kid_to_jwk = KidToJWKMap}}] ->
            jwks_from_kid_map(KidToJWKMap);
        [] ->
            {error, <<"No cached JWKS found">>}
    end.

%% For jwks_uri sources, refresh and then rebuild the full JWKS set from ETS.
-spec refresh_and_build_jwks(map()) ->
          {ok, jose_jwk:key()} | {error, binary()}.
refresh_and_build_jwks(#{name := Issuer} = Props) ->
    case gen_server:call(?MODULE, {refresh_issuer, Props},
                         ?JWKS_REFRESH_TIMEOUT_MS) of
        ok ->
            lookup_and_build_jwks(Issuer);
        {error, cooldown} ->
            {error, <<"JWKS refresh cooldown">>};
        {error, _} ->
            {error, <<"JWKS refresh failed">>}
    end.

%% Direct lookup fallbacks for each issuer type
-spec direct_get_jwk(IssuerProps :: map(), Kid :: binary() | undefined) ->
          {ok, jose_jwk:key()} | {error, binary()}.
%% Static JWKS - direct lookup from settings
direct_get_jwk(#{public_key_source := jwks, jwks := {_, KidToJWKMap}} = _Props,
               Kid) ->
    case maps:find(Kid, KidToJWKMap) of
        {ok, JWKMap} ->
            {ok, jose_jwk:from_map(JWKMap)};
        error ->
            format_kid_not_found_error(Kid)
    end;
%% Static PEM - direct lookup from settings
direct_get_jwk(#{public_key_source := pem, public_key := PEM}, undefined) ->
    try
        JWK = jose_jwk:from_pem(PEM),
        {ok, JWK}
    catch _:Error ->
            ?log_error("Failed to parse PEM: ~p", [Error]),
            {error, <<"Invalid PEM key">>}
    end;
%% Dynamic JWKS URI - lookup from cache with retry if needed
direct_get_jwk(#{public_key_source := jwks_uri} = Props, Kid) ->
    get_jwk_with_retry(Props, Kid, ?JWKS_FETCH_RETRY_COUNT).

-spec get_jwk_with_retry(IssuerProps :: map(), Kid :: binary() | undefined,
                         RetryCount :: integer()) ->
          {ok, jose_jwk:key()} | {error, binary()}.
get_jwk_with_retry(#{public_key_source := jwks_uri} = Props, Kid, RetryCount) ->
    Issuer = maps:get(name, Props),
    Map = case ets:lookup(?MODULE, Issuer) of
              [{Issuer, #jwks_cache_entry{kid_to_jwk = KidToJWKMap,
                                          expiry = Expiry}}] ->
                  case erlang:monotonic_time(millisecond) > Expiry of
                      true ->
                          ?log_debug("JWT issuer ~p entries have expired",
                                     [Issuer]),
                          #{};
                      false -> KidToJWKMap
                  end;
              [] -> #{}
          end,
    case maps:find(Kid, Map) of
        {ok, JWK} -> {ok, JWK};
        error when RetryCount > 0 ->
            gen_server:call(?MODULE, {refresh_issuer, Props},
                            ?JWKS_REFRESH_TIMEOUT_MS),
            get_jwk_with_retry(Props, Kid, RetryCount - 1);
        error ->
            format_kid_not_found_error(Kid)
    end.

-spec format_kid_not_found_error(Kid :: binary() | undefined) ->
          {error, binary()}.
format_kid_not_found_error(Kid) when is_binary(Kid) ->
    {error, <<"Key with kid: ", Kid/binary, " not found">>};
format_kid_not_found_error(_) ->
    {error, <<"Key (no kid specified) not found">>}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update_internal_keys() -> ok.
update_internal_keys() ->
    maps:foreach(
      fun(Issuer, Props) ->
              cache_issuer_settings(Issuer, Props)
      end, jwt_issuer:settings()).

%% Extract the max-age from the Cache-Control header. This is used to set the
%% expiry time for the JWKS in the cache.
-spec extract_max_age(CacheControl :: binary()) -> integer() | undefined.
extract_max_age(CacheControl) ->
    case re:run(CacheControl, "max-age=(\\d+)", [{capture, [1], list}]) of
        {match, [MaxAgeStr]} ->
            try
                list_to_integer(MaxAgeStr)
            catch
                error:badarg ->
                    ?log_warning("Invalid max-age value in Cache-Control: ~p",
                                 [MaxAgeStr]),
                    undefined
            end;
        nomatch ->
            undefined
    end.

-spec get_tls_connect_options(URL :: string(), AddressFamilyKey :: atom(),
                              VerifyPeerKey :: atom(), CAKey :: atom(),
                              SNIKey :: atom(), Settings :: map()) ->
          list().
get_tls_connect_options(URL, AddressFamilyKey, VerifyPeerKey, CAKey, SNIKey,
                        Settings) ->
    AddressFamily = maps:get(AddressFamilyKey, Settings, undefined),
    VerifyPeer = maps:get(VerifyPeerKey, Settings, true),
    {_, Certs} = maps:get(CAKey, Settings, {<<>>, []}),
    SNI = maps:get(SNIKey, Settings, ""),

    misc:tls_connect_options(URL, AddressFamily, VerifyPeer, Certs, SNI, []).

-spec extract_connect_options(URL :: string(), IssuerProps :: map()) -> list().
extract_connect_options(URL, IssuerProps) ->
    get_tls_connect_options(
      URL,
      jwks_uri_address_family,
      jwks_uri_tls_verify_peer,
      jwks_uri_tls_ca,
      jwks_uri_tls_sni,
      IssuerProps
     ).

-spec extract_oidc_connect_options(URL :: string(), OidcSettings :: map()) ->
          list().
extract_oidc_connect_options(URL, OidcSettings) ->
    get_tls_connect_options(
      URL,
      address_family,
      tls_verify_peer,
      tls_ca,
      tls_sni,
      OidcSettings
     ).

%% TODO: OIDCC fetches the JWKS URI at periodic intervals but we
%% use the existing JWKS path. We need to either disable OIDCC refresh or
%% piggyback off their refresh (only for OIDCC JWKSes).
fetch_jwks_from_oidc_discovery(DiscoveryURL, IssuerProps) ->
    OidcSettings = maps:get(oidc_settings, IssuerProps),
    DiscoveryTimeout = maps:get(http_timeout_ms, OidcSettings),
    DiscoveryOpts = extract_oidc_connect_options(DiscoveryURL, OidcSettings),

    try rest_utils:request(<<"oidc_discovery">>, DiscoveryURL,
                           "GET", [], <<>>, DiscoveryTimeout,
                           [{connect_options, DiscoveryOpts}]) of
        {ok, {{200, _}, _RespHeaders, Body}} ->
            case jose:decode(Body) of
                Map when is_map(Map) ->
                    JwksUriBin = maps:get(<<"jwks_uri">>, Map),
                    JwksURL = binary_to_list(JwksUriBin),
                    fetch_jwks_from_url(JwksURL, IssuerProps);
                _ ->
                    throw(invalid_discovery)
            end;
        {ok, {{Status, _Reason}, _RespHeaders, _RespBody}} ->
            throw({rest_failed, DiscoveryURL, {status, Status}});
        {error, Reason} ->
            throw({rest_failed, DiscoveryURL, {error, Reason}})
    catch
        throw:Error ->
            ?log_error("Failed to get OIDC discovery from ~p.~nReason: ~p",
                       [DiscoveryURL, Error]),
            {error, Error}
    end.

-spec fetch_jwks(IssuerProps :: map()) ->
          {Json :: binary(), MaxAge :: integer() | undefined} | {error, term()}.
fetch_jwks(IssuerProps) ->
    case maps:get(oidc_settings, IssuerProps, undefined) of
        #{oidc_discovery_uri := DiscoveryURL} ->
            fetch_jwks_from_oidc_discovery(DiscoveryURL, IssuerProps);
        _ ->
            URL = maps:get(jwks_uri, IssuerProps),
            fetch_jwks_from_url(URL, IssuerProps)
    end.

-spec fetch_jwks_from_url(string(), map()) ->
          {Json :: binary(), MaxAge :: integer() | undefined} | {error, term()}.
fetch_jwks_from_url(URL, IssuerProps) ->
    try
        TimeoutMs = maps:get(jwks_uri_http_timeout_ms, IssuerProps),
        ConnectOptions = extract_connect_options(URL, IssuerProps),
        case rest_utils:request(<<"jwks">>, URL, "GET", [], <<>>, TimeoutMs,
                                [{connect_options, ConnectOptions}]) of
            {ok, {{200, _}, RespHeaders, Bin}} ->
                LowercaseHeaders = [{string:lowercase(K), V} ||
                                       {K, V} <- RespHeaders],
                MaxAge = case proplists:get_value("cache-control",
                                                  LowercaseHeaders) of
                             undefined -> undefined;
                             CacheCtrl -> extract_max_age(CacheCtrl)
                         end,
                ?log_debug("Received JWKS from ~s:~n~s MaxAge:~p",
                           [URL, Bin, MaxAge]),
                {Bin, MaxAge};
            {ok, {{Status, _Reason}, _RespHeaders, _RespBody}} ->
                throw({rest_failed, URL, {status, Status}});
            {error, Reason} ->
                throw({rest_failed, URL, {error, Reason}})
        end
    catch
        throw:Error ->
            ?log_error("Failed to get JWKS from ~p.~nReason: ~p", [URL, Error]),
            {error, Error}
    end.

-spec fetch_and_cache_jwks(IssuerProps :: map()) ->
          ok | {error, binary()}.
fetch_and_cache_jwks(IssuerProps = #{public_key_source := jwks_uri}) ->
    FetchTime = erlang:monotonic_time(millisecond),
    case fetch_jwks(IssuerProps) of
        {error, _} ->
            {error, <<"JWKS fetch failed">>};
        {JwksJson, MaxAge} ->
            case validate_jwks(JwksJson, IssuerProps) of
                {ok, KidToJWKMap} ->
                    Expiry = FetchTime +
                        (case MaxAge of
                             undefined -> ?JWKS_DEFAULT_EXPIRY_S;
                             Age -> Age
                         end) * 1000,
                    cache_jwks_entry(IssuerProps, KidToJWKMap, FetchTime,
                                     Expiry);
                {error, _} ->
                    {error, <<"Invalid JWKS">>}
            end
    end.

-spec check_cooldown(Issuer :: string()) -> ok | {error, cooldown}.
check_cooldown(Issuer) ->
    Now = erlang:monotonic_time(millisecond),
    case ets:lookup(?MODULE, Issuer) of
        [{_, #jwks_cache_entry{fetch_time = LastTime}}] ->
            case Now - LastTime < ?JWKS_COOLDOWN_INTERVAL_MS of
                true ->
                    ?log_debug("JWT issuer ~p cooldown period not yet met",
                               [Issuer]),
                    {error, cooldown};
                false ->
                    ok
            end;
        _ ->
            ok
    end.

-spec validate_jwks(JwksJson :: binary(), IssuerProps :: map()) ->
          {ok, jwt_kid_to_jwk()} | {error, term()}.
validate_jwks(JwksJson, IssuerProps) ->
    Issuer = maps:get(name, IssuerProps),
    try jose:decode(JwksJson) of
        JsonMap ->
            Algo = maps:get(signing_algorithm, IssuerProps),
            case menelaus_web_jwt_key:validate_jwks_algorithm(JsonMap, Algo) of
                {ok, KidToJWKMap} ->
                    ?log_debug("Parsed JWKS for ~p~n", [Issuer]),
                    {ok, KidToJWKMap};
                {error, Reason} ->
                    ?log_error("Error validating JWKS: ~p", [Reason]),
                    {error, Reason}
            end
    catch _:_ ->
            ?log_error("Error parsing JWKS:~p", [JwksJson]),
            {error, <<"Invalid JWKS JSON">>}
    end.

schedule_refresh(Interval) ->
    Jitter = rand:uniform(?JWKS_REFRESH_JITTER_MS),
    erlang:send_after(Interval + Jitter, self(), periodic_refresh).

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

-spec cache_jwks_entry(#{name := string(),
                         public_key_source := jwks | jwks_uri,
                         _ => _},
                       jwt_kid_to_jwk(),
                       integer(),
                       undefined | integer()) -> ok.
cache_jwks_entry(#{name := Issuer, public_key_source := Source} = _IssuerProps,
                 KidToJWKMap, FetchTime, Expiry) ->
    try
        JWKMap = maps:map(
                   fun(_Kid, JWKMap) ->
                           jose_jwk:from_map(JWKMap)
                   end, KidToJWKMap),
        CacheEntry = #jwks_cache_entry{
                        kid_to_jwk = JWKMap,
                        fetch_time = FetchTime,
                        expiry = Expiry
                       },
        ets:insert(?MODULE, {Issuer, CacheEntry}),
        Msg = case Source of
                  jwks -> "Cached static JWKS for issuer ~p with ~p keys";
                  jwks_uri -> "Cached dynamic JWKS for issuer ~p with ~p keys"
              end,
        ?log_debug(Msg, [Issuer, maps:size(JWKMap)])
    catch _:Error ->
            ?log_error("Failed to process JWKS for issuer ~p: ~p",
                       [Issuer, Error])
    end.

-spec cache_issuer_settings(Issuer :: string(), Props :: map()) -> ok.
cache_issuer_settings(Issuer, #{public_key_source := pem,
                                public_key := PEM} = _Props) ->
    try
        JWK = jose_jwk:from_pem(PEM),
        FetchTime = erlang:monotonic_time(millisecond),
        CacheEntry = #jwks_cache_entry{
                        kid_to_jwk = #{undefined => JWK},
                        fetch_time = FetchTime,
                        expiry = undefined
                       },
        ets:insert(?MODULE, {Issuer, CacheEntry}),
        ?log_debug("Cached PEM key for issuer ~p", [Issuer])
    catch _:Error ->
            ?log_error("Failed to parse PEM for issuer ~p: ~p",
                       [Issuer, Error])
    end;
cache_issuer_settings(Issuer, #{public_key_source := jwks,
                                jwks := {_, KidToJWKMap}} = Props) ->
    FetchTime = erlang:monotonic_time(millisecond),
    cache_jwks_entry(Props#{name => Issuer}, KidToJWKMap, FetchTime, undefined);
cache_issuer_settings(_Issuer, #{public_key_source := jwks_uri}) ->
    %% For JWKS URI, we'll fetch in the refresh handler immediately
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?MODULE, [named_table, set, public, {read_concurrency, true}]),
    Self = self(),
    chronicle_compat_events:subscribe(
      fun (jwt_settings) -> Self ! settings_update;
          (?JWT_SIGNING_KEYS_KEY) -> Self ! internal_key_update;
          (_) -> ok
      end),
    self() ! settings_update,
    self() ! internal_key_update,
    {ok, #cache_state{}}.

%% On demand node refresh. When a lookup fails, the node will refresh the JWKS.
handle_call({refresh_issuer, #{name := Issuer} = Props}, _From, State) ->
    case check_cooldown(Issuer) of
        {error, cooldown} ->
            {reply, {error, cooldown}, State};
        ok ->
            case fetch_and_cache_jwks(Props) of
                ok ->
                    {reply, ok, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end
    end;
handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call(Request, _From, State) ->
    ?log_warning("Unhandled call: ~p", [Request]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?log_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info(internal_key_update, State) ->
    ?log_debug("JWT internal key changed, updating cache", []),
    update_internal_keys(),
    {noreply, State};
handle_info(settings_update,
            #cache_state{refresh_timer_ref = TimerRef} = State) ->
    ?log_debug("JWT settings init/change, invalidating cache.", []),
    ets:delete_all_objects(?MODULE),
    misc:flush(settings_update),
    misc:flush(internal_key_update),
    misc:flush(periodic_refresh),
    cancel_timer(TimerRef),

    update_internal_keys(),
    case chronicle_kv:get(kv, jwt_settings) of
        {ok, {#{enabled := true, issuers := IssuersMap} = Settings, _Rev}} ->
            maps:foreach(
              fun(Issuer, #{signing_algorithm := Algo} = Props) ->
                      case menelaus_web_jwt_key:is_symmetric_algorithm(Algo) of
                          true ->
                              ok;
                          false ->
                              cache_issuer_settings(Issuer, Props)
                      end
              end, IssuersMap),
            HasJwksUri =
                lists:any(fun(#{public_key_source := jwks_uri}) -> true;
                             (_) -> false
                          end, maps:values(IssuersMap)),
            case HasJwksUri of
                true ->
                    Interval =
                        maps:get(jwks_uri_refresh_interval_s, Settings) * 1000,
                    %% Schedule the first refresh immediately to populate the
                    %% cache, subsequent refreshes will use the configured
                    %% interval with jitter
                    {noreply, State#cache_state{
                                refresh_timer_ref = schedule_refresh(0),
                                refresh_interval_ms = Interval}};
                false ->
                    {noreply,
                     State#cache_state{refresh_timer_ref = undefined,
                                       refresh_interval_ms = undefined}}
            end;
        _ ->
            {noreply, State#cache_state{refresh_timer_ref = undefined,
                                        refresh_interval_ms = undefined}}
    end;
handle_info(periodic_refresh,
            #cache_state{refresh_timer_ref = Ref} = State) ->
    misc:flush(periodic_refresh),
    cancel_timer(Ref),
    ?log_debug("Initiating JWKS refresh.", []),

    case chronicle_kv:get(kv, jwt_settings) of
        {ok, {#{enabled := true, issuers := IssuersMap}, _}} ->
            maps:foreach(
              fun(Issuer, #{public_key_source := jwks_uri} = Props) ->
                      case fetch_and_cache_jwks(Props#{name => Issuer}) of
                          ok -> ok;
                          {error, _} -> ok
                      end;
                 (_, _) -> ok
              end, IssuersMap),

            case State#cache_state.refresh_interval_ms of
                undefined -> {noreply, State};
                Interval ->
                    {noreply, State#cache_state{
                                refresh_timer_ref = schedule_refresh(Interval)}}
            end;
        _ ->
            {noreply, State}
    end;
handle_info(Request, State) ->
    ?log_warning("Unhandled info: ~p", [Request]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-ifdef(TEST).

cache_lookup_test() ->
    meck:new(chronicle_compat_events, [passthrough]),
    meck:new(chronicle_kv, [passthrough]),
    meck:new(ns_config, [passthrough]),
    meck:new(jwt_issuer, [passthrough]),

    meck:expect(ns_config, search_node_with_default,
                fun({jwt_cache, jwks_cooldown_interval_ms}, _Default) ->
                        60000;
                   (_Key, Default) ->
                        Default
                end),

    meck:expect(chronicle_compat_events, subscribe, fun(_) -> ok end),
    meck:expect(chronicle_kv, get,
                fun(kv, jwt_settings) ->
                        {ok, {#{enabled => true,
                                issuers => #{}}, '_'}};
                   (_, _) ->
                        meck:passthrough()
                end),

    meck:expect(jwt_issuer, settings, fun() -> #{} end),

    TestJWKS =
        <<
          "{",
          "    \"keys\": [",
          "        {",
          "            \"kty\": \"EC\",",
          "            \"crv\": \"P-256\",",
          "            \"x\": \"wS5Whg7la7uUcmgyDn2UrA4ZpUF7tBsCidd90AkYn00\",",
          "            \"y\": \"_XPJU549tLUCFgaKUD9IbBQDSweeT4t7EEXsC3sJHwM\",",
          "            \"alg\": \"ES256\",",
          "            \"use\": \"sig\",",
          "            \"kid\": \"key1\"",
          "        },",
          "        {",
          "            \"kty\": \"EC\",",
          "            \"crv\": \"P-256\",",
          "            \"x\": \"A29hrKJ7zXQ3P8iV5n0qLmBCm2T4NB9eQo5E6J8gXx0\",",
          "            \"y\": \"B5J3F92mLvX0hTLKo7M4Qj9VwY6eTR3mPk0xvZKfXQY\",",
          "            \"alg\": \"ES256\",",
          "            \"use\": \"sig\",",
          "            \"kid\": \"key2\"",
          "        }",
          "    ]",
          "}"
        >>,

    meck:new(rest_utils, [passthrough]),
    meck:expect(rest_utils, request,
                fun(<<"jwks">>, "https://www.googleapis.com/oauth2/v3/certs",
                    "GET", [], <<>>, 5000, _) ->
                        {ok, {{200, []}, [{"cache-control",
                                           "max-age=21600"}], TestJWKS}}
                end),
    IssuerProps = #{
                    name => "test1",
                    public_key_source => jwks_uri,
                    jwks_uri => "https://www.googleapis.com/oauth2/v3/certs",
                    signing_algorithm => 'ES256',
                    jwks_uri_http_timeout_ms => 5000,
                    jwks_uri_tls_verify_peer => false
                   },

    StartTime = erlang:monotonic_time(millisecond),

    {ok, _Pid} = jwt_cache:start_link(),

    {ok, JWK} = get_jwk(IssuerProps, <<"key1">>),
    ?assert(is_tuple(JWK)),
    ?assertEqual(jose_jwk, element(1, JWK)),
    [{_, #jwks_cache_entry{kid_to_jwk = KidToJWKMap,
                           fetch_time = FetchTime,
                           expiry = Expiry}}] = ets:lookup(?MODULE, "test1"),
    ?assert(is_map(KidToJWKMap)),
    ?assertEqual(2, maps:size(KidToJWKMap)),
    ?assert(Expiry > FetchTime),

    %% First lookup should fetch
    {ok, JWK1} = get_jwk(IssuerProps, <<"key1">>),
    ?assertEqual(meck:num_calls(rest_utils, request, ['_', '_', '_', '_', '_',
                                                      '_', '_']), 1),

    %% Set the cache entry to expired
    ExpiredEntry = {"test1", #jwks_cache_entry{kid_to_jwk = KidToJWKMap,
                                               fetch_time = FetchTime,
                                               expiry = StartTime}},
    ets:insert(?MODULE, ExpiredEntry),

    %% Cooldown period is not met, should not fetch
    {error, _} = get_jwk(IssuerProps, <<"key2">>),
    ?assertEqual(meck:num_calls(rest_utils, request, ['_', '_', '_', '_', '_',
                                                      '_', '_']), 1),

    %% Invalidate the cache and wait until it is handled
    jwt_cache ! settings_update,
    gen_server:call(jwt_cache, sync, 1000),
    ?assertEqual(0, ets:info(?MODULE, size)),

    %% Should trigger fetch
    {ok, JWK2} = get_jwk(IssuerProps, <<"key2">>),
    ?assertEqual(meck:num_calls(rest_utils, request, ['_', '_', '_', '_', '_',
                                                      '_', '_']), 2),

    %% Cache hits, should not fetch
    {ok, JWK2} = get_jwk(IssuerProps, <<"key2">>),
    {ok, JWK1} = get_jwk(IssuerProps, <<"key1">>),
    ?assertEqual(meck:num_calls(rest_utils, request, ['_', '_', '_', '_', '_',
                                                      '_', '_']), 2),

    meck:unload(jwt_issuer),
    meck:unload(chronicle_kv),
    meck:unload(chronicle_compat_events),
    meck:unload(rest_utils),
    meck:unload(ns_config),
    gen_server:stop(jwt_cache).

cache_refresh_failure_test() ->
    meck:new(chronicle_compat_events, [passthrough]),
    meck:new(chronicle_kv, [passthrough]),
    meck:new(ns_config, [passthrough]),
    meck:new(rest_utils, [passthrough]),
    meck:new(menelaus_web_jwt_key, [passthrough]),
    meck:new(jwt_issuer, [passthrough]),

    meck:expect(ns_config, search_node_with_default,
                fun({jwt_cache, jwks_cooldown_interval_ms}, _Default) ->
                        60000;
                   (_Key, Default) ->
                        Default
                end),
    meck:expect(jwt_issuer, settings, fun() -> #{} end),

    meck:expect(chronicle_compat_events, subscribe, fun(_) -> ok end),

    TestJWKS =
        <<
          "{",
          "    \"keys\": [",
          "        {",
          "            \"kty\": \"EC\",",
          "            \"crv\": \"P-256\",",
          "            \"x\": \"wS5Whg7la7uUcmgyDn2UrA4ZpUF7tBsCidd90AkYn00\",",
          "            \"y\": \"_XPJU549tLUCFgaKUD9IbBQDSweeT4t7EEXsC3sJHwM\",",
          "            \"alg\": \"ES256\",",
          "            \"use\": \"sig\",",
          "            \"kid\": \"key1\"",
          "        }",
          "    ]",
          "}"
        >>,

    %% Invalid JWKS data for testing parse failure
    InvalidJWKS = <<"{\"not_keys\": [{}]}">>,

    meck:expect(menelaus_web_jwt_key, validate_jwks_algorithm,
                fun(JsonMap, Algorithm) ->
                        case maps:get(<<"keys">>, JsonMap, undefined) of
                            undefined ->
                                {error, <<"Invalid JWKS format">>};
                            _ ->
                                meck:passthrough([JsonMap, Algorithm])
                        end
                end),

    %% Set up JWT settings with two issuers
    Issuer1 = "test-issuer1",
    Issuer2 = "test-issuer2",
    Issuer1Props = #{
                     public_key_source => jwks_uri,
                     jwks_uri => "https://example.com/jwks1",
                     signing_algorithm => 'ES256',
                     jwks_uri_http_timeout_ms => 5000,
                     jwks_uri_tls_verify_peer => false
                    },
    Issuer2Props = #{
                     public_key_source => jwks_uri,
                     jwks_uri => "https://example.com/jwks2",
                     signing_algorithm => 'ES256',
                     jwks_uri_http_timeout_ms => 5000,
                     jwks_uri_tls_verify_peer => false
                    },

    IssuersMap = #{
                   Issuer1 => Issuer1Props,
                   Issuer2 => Issuer2Props
                  },

    meck:expect(chronicle_kv, get,
                fun(kv, jwt_settings) ->
                        {ok, {#{enabled => true,
                                jwks_uri_refresh_interval_s => 3600,
                                issuers => IssuersMap}, '_'}}
                end),

    %% First request succeeds for both issuers
    meck:expect(rest_utils, request,
                fun(<<"jwks">>, "https://example.com/jwks1", "GET", [], <<>>,
                    5000, _) ->
                        {ok, {{200, []}, [{"cache-control", "max-age=21600"}],
                              TestJWKS}};
                   (<<"jwks">>, "https://example.com/jwks2", "GET", [], <<>>,
                    5000, _) ->
                        {ok, {{200, []}, [{"cache-control", "max-age=21600"}],
                              TestJWKS}}
                end),

    {ok, Pid} = jwt_cache:start_link(),

    {ok, JWK1} = get_jwk(Issuer1Props#{name => Issuer1}, <<"key1">>),
    {ok, JWK2} = get_jwk(Issuer2Props#{name => Issuer2}, <<"key1">>),
    ?assert(is_tuple(JWK1)),
    ?assert(is_tuple(JWK2)),

    [{_, Issuer1Entry}] = ets:lookup(?MODULE, Issuer1),
    [{_, Issuer2Entry}] = ets:lookup(?MODULE, Issuer2),

    %% Simulate failures during refresh
    meck:expect(rest_utils, request,
                fun(<<"jwks">>, "https://example.com/jwks1", "GET", [], <<>>,
                    5000, _) ->
                        {error, econnrefused};
                   (<<"jwks">>, "https://example.com/jwks2", "GET", [], <<>>,
                    5000, _) ->
                        {ok, {{200, []}, [{"cache-control", "max-age=21600"}],
                              InvalidJWKS}}
                end),

    Pid ! periodic_refresh,

    %% Ensures that gen_server has processed all messages in its mailbox
    gen_server:call(?MODULE, sync, 1000),

    %% Verify the cache entries are unchanged
    [{_, Issuer1EntryAfter}] = ets:lookup(?MODULE, Issuer1),
    [{_, Issuer2EntryAfter}] = ets:lookup(?MODULE, Issuer2),

    ?assertEqual(Issuer1Entry#jwks_cache_entry.kid_to_jwk,
                 Issuer1EntryAfter#jwks_cache_entry.kid_to_jwk),
    ?assertEqual(Issuer2Entry#jwks_cache_entry.kid_to_jwk,
                 Issuer2EntryAfter#jwks_cache_entry.kid_to_jwk),

    %% Verify we can still use the cached keys
    {ok, JWK1After} = get_jwk(Issuer1Props#{name => Issuer1}, <<"key1">>),
    {ok, JWK2After} = get_jwk(Issuer2Props#{name => Issuer2}, <<"key1">>),
    ?assertEqual(JWK1, JWK1After),
    ?assertEqual(JWK2, JWK2After),

    meck:unload(jwt_issuer),
    meck:unload(chronicle_kv),
    meck:unload(chronicle_compat_events),
    meck:unload(rest_utils),
    meck:unload(ns_config),
    meck:unload(menelaus_web_jwt_key),
    gen_server:stop(jwt_cache).

pem_cache_test() ->
    meck:new(chronicle_compat_events, [passthrough]),
    meck:new(chronicle_kv, [passthrough]),
    meck:new(jwt_issuer, [passthrough]),

    meck:expect(jwt_issuer, settings, fun() -> #{} end),
    meck:expect(chronicle_compat_events, subscribe, fun(_) -> ok end),

    JWK = jose_jwk:generate_key({rsa, 2048}),
    PublicJWK = jose_jwk:to_public(JWK),
    PEM = jose_jwk:to_pem(PublicJWK),

    TestIssuer = "test-pem-issuer",
    IssuerProps = #{
                    public_key_source => pem,
                    public_key => PEM,
                    signing_algorithm => 'RS256'
                   },

    %% Set up with empty issuers for direct lookup tests
    meck:expect(chronicle_kv, get,
                fun(kv, jwt_settings) ->
                        {ok, {#{enabled => true, issuers => #{}}, '_'}};
                   (_, _) ->
                        meck:passthrough()
                end),

    {ok, Pid} = jwt_cache:start_link(),

    %% Verify ETS is empty (empty issuers shouldn't populate cache)
    ?assertEqual(0, ets:info(?MODULE, size)),

    %% Perform direct lookup (bypassing cache)
    IssuerPropsWithName = maps:merge(IssuerProps, #{name => TestIssuer}),
    {ok, DirectJWK} = get_jwk(IssuerPropsWithName, undefined),
    ?assert(is_tuple(DirectJWK)),
    ?assertEqual(jose_jwk, element(1, DirectJWK)),

    %% Verify ETS is still empty (direct lookup doesn't populate cache)
    ?assertEqual(0, ets:info(?MODULE, size)),

    %% Now update mocks to return our test issuer
    meck:expect(chronicle_kv, get,
                fun(kv, jwt_settings) ->
                        {ok, {#{
                                enabled => true,
                                issuers => #{TestIssuer => IssuerProps}
                               }, '_'}};
                   (_, _) ->
                        meck:passthrough()
                end),

    %% Trigger settings update to populate cache
    jwt_cache ! settings_update,

    %% Ensures that gen_server has processed all messages in its mailbox
    gen_server:call(?MODULE, sync, 1000),

    %% Verify cache is now populated
    ?assertEqual(1, ets:info(?MODULE, size)),

    %% Verify key can be retrieved from cache
    {ok, CachedJWK} = get_jwk(IssuerPropsWithName, undefined),
    ?assert(is_tuple(CachedJWK)),
    ?assertEqual(jose_jwk, element(1, CachedJWK)),

    %% Verify the cache entry exists with expected content
    [{_, #jwks_cache_entry{kid_to_jwk = KidToJWKMap}}] =
        ets:lookup(?MODULE, TestIssuer),
    ?assert(maps:is_key(undefined, KidToJWKMap)),
    ?assert(is_tuple(maps:get(undefined, KidToJWKMap))),

    gen_server:stop(Pid),
    meck:unload(jwt_issuer),
    meck:unload(chronicle_kv),
    meck:unload(chronicle_compat_events).

static_jwks_cache_test() ->
    meck:new(chronicle_compat_events, [passthrough]),
    meck:new(chronicle_kv, [passthrough]),
    meck:new(jwt_issuer, [passthrough]),

    meck:expect(jwt_issuer, settings, fun() -> #{} end),
    meck:expect(chronicle_compat_events, subscribe, fun(_) -> ok end),

    %% Generate two test keys with different key IDs
    Key1 = jose_jwk:generate_key({ec, <<"P-256">>}),
    Key2 = jose_jwk:generate_key({ec, <<"P-256">>}),
    Key1Map = element(2, jose_jwk:to_map(Key1)),
    Key2Map = element(2, jose_jwk:to_map(Key2)),

    %% Create a static JWKS with both keys
    Kid1 = <<"key1">>,
    Kid2 = <<"key2">>,
    KidToJWKMap = #{Kid1 => Key1Map, Kid2 => Key2Map},
    TestIssuer = "test-jwks-issuer",
    IssuerProps = #{
                    public_key_source => jwks,
                    jwks => {undefined, KidToJWKMap},
                    signing_algorithm => 'ES256'
                   },

    %% Set up with empty issuers for direct lookup tests
    meck:expect(chronicle_kv, get,
                fun(kv, jwt_settings) ->
                        {ok, {#{enabled => true, issuers => #{}}, '_'}};
                   (_, _) ->
                        meck:passthrough()
                end),

    {ok, Pid} = jwt_cache:start_link(),

    %% Verify ETS is empty (empty issuers shouldn't populate cache)
    ?assertEqual(0, ets:info(?MODULE, size)),

    %% Perform direct lookups (bypassing cache)
    IssuerPropsWithName = maps:merge(IssuerProps, #{name => TestIssuer}),
    {ok, DirectJWK1} = get_jwk(IssuerPropsWithName, Kid1),
    {ok, DirectJWK2} = get_jwk(IssuerPropsWithName, Kid2),
    ?assert(is_tuple(DirectJWK1)),
    ?assert(is_tuple(DirectJWK2)),
    ?assertEqual(jose_jwk, element(1, DirectJWK1)),
    ?assertEqual(jose_jwk, element(1, DirectJWK2)),

    %% Verify ETS is still empty (direct lookup doesn't populate cache)
    ?assertEqual(0, ets:info(?MODULE, size)),

    %% Now update mocks to return our test issuer
    meck:expect(chronicle_kv, get,
                fun(kv, jwt_settings) ->
                        {ok, {#{
                                enabled => true,
                                issuers => #{TestIssuer => IssuerProps}
                               }, '_'}};
                   (_, _) ->
                        meck:passthrough()
                end),

    %% Trigger settings update to populate cache
    jwt_cache ! settings_update,

    %% Ensures that gen_server has processed all messages in its mailbox
    gen_server:call(?MODULE, sync, 1000),

    %% Verify cache is now populated
    ?assertEqual(1, ets:info(?MODULE, size)),

    %% Verify keys can be retrieved from cache
    {ok, CachedJWK1} = get_jwk(IssuerPropsWithName, Kid1),
    {ok, CachedJWK2} = get_jwk(IssuerPropsWithName, Kid2),
    ?assert(is_tuple(CachedJWK1)),
    ?assert(is_tuple(CachedJWK2)),
    ?assertEqual(jose_jwk, element(1, CachedJWK1)),
    ?assertEqual(jose_jwk, element(1, CachedJWK2)),

    %% Verify the cache entry exists with expected content
    [{_, #jwks_cache_entry{kid_to_jwk = CachedMap}}] =
        ets:lookup(?MODULE, TestIssuer),
    ?assertEqual(2, maps:size(CachedMap)),
    ?assert(maps:is_key(Kid1, CachedMap)),
    ?assert(maps:is_key(Kid2, CachedMap)),

    gen_server:stop(Pid),
    meck:unload(jwt_issuer),
    meck:unload(chronicle_kv),
    meck:unload(chronicle_compat_events).

-endif.
