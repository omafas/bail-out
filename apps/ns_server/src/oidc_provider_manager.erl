%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Manages OIDC provider configuration workers (oidcc) per issuer with
%% discovery URI.

-module(oidc_provider_manager).

-behavior(gen_server).

-include("ns_common.hrl").

-export([start_link/0,
         restart_workers/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
                name_to_pid = #{} :: map()
               }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

restart_workers() ->
    gen_server:cast(?MODULE, restart).

init([]) ->
    process_flag(trap_exit, true),
    Self = self(),
    chronicle_compat_events:subscribe(
      fun (jwt_settings) -> Self ! restart;
          (_) -> ok
      end),

    {ok, start_all_workers(#state{})}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(restart, State0) ->
    stop_all_workers(State0),
    case menelaus_web_jwt:is_enabled() of
        true ->
            {noreply, start_all_workers(#state{})};
        false ->
            {noreply, #state{}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, #state{name_to_pid = Map0} = State0) ->
    ?log_warning("OIDC provider worker ~p exited", [Pid]),
    Map = maps:filter(fun(_, V) -> V =/= Pid end, Map0),
    {noreply, State0#state{name_to_pid = Map}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    stop_all_workers(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal

start_all_workers(#state{} = State0) ->
    Issuers = get_oidc_issuers_with_discovery(),
    NameToPid = lists:foldl(fun start_worker_fold/2, #{}, Issuers),
    State0#state{name_to_pid = NameToPid}.

stop_all_workers(#state{name_to_pid = Map}) ->
    misc:terminate_and_wait([Pid || {_, Pid} <- maps:to_list(Map)], shutdown).

start_worker_fold(#{name := IssuerName} = IssuerMap, Acc) ->
    WorkerName = {local, list_to_atom(IssuerName)},
    case start_provider_worker(WorkerName, IssuerMap) of
        {ok, Pid} -> Acc#{IssuerName => Pid};
        {error, Reason} ->
            ?log_warning("Failed to start OIDC provider worker for ~p: ~p",
                         [IssuerName, Reason]),
            Acc
    end.

-spec start_provider_worker(WorkerName :: {local, atom()},
                            IssuerMap :: map()) ->
          {ok, pid()} | {error, term()}.
start_provider_worker(WorkerName, IssuerMap) ->
    DiscoveryUri = maps:get(discovery_uri, IssuerMap),
    case derive_issuer_from_discovery(DiscoveryUri) of
        undefined ->
            {error, invalid_discovery_uri};
        IssuerUri when is_binary(IssuerUri) ->
            %% Allow unsafe HTTP only for localhost development
            ParsedIssuer = uri_string:parse(IssuerUri),
            Scheme = maps:get(scheme, ParsedIssuer, <<>>),
            Host = maps:get(host, ParsedIssuer, <<>>),
            AllowUnsafe = (Scheme =:= <<"http">>) andalso
                lists:member(Host, [<<"localhost">>, <<"127.0.0.1">>]),
            Quirks0 =
                case AllowUnsafe of
                    true -> #{allow_unsafe_http => true};
                    false -> #{}
                end,
            DisablePAR = maps:get(disable_par, IssuerMap, false),
            Quirks =
                case DisablePAR of
                    true ->
                        %% This is to bypass Keycloak Bug #43034.
                        %% https://github.com/keycloak/keycloak/issues/43034
                        DocOverrides =
                            #{
                              %% Force-disable PAR regardless of discovery doc
                              <<"require_pushed_authorization_requests">> =>
                                  false,
                              <<"pushed_authorization_request_endpoint">> =>
                                  undefined
                             },
                        maps:put(document_overrides, DocOverrides, Quirks0);
                    false ->
                        Quirks0
                end,
            HttpTimeoutMs = maps:get(http_timeout_ms, IssuerMap),
            SslOpts = maps:get(ssl_opts, IssuerMap, []),
            RequestOpts = #{timeout => HttpTimeoutMs, ssl => SslOpts},
            ProviderOpts = #{quirks => Quirks, request_opts => RequestOpts},
            try oidcc_provider_configuration_worker:start_link(
                  #{issuer => IssuerUri,
                    name => WorkerName,
                    provider_configuration_opts => ProviderOpts}) of
                {ok, Pid} -> {ok, Pid};
                {error, Reason} -> {error, Reason}
            catch T:E ->
                    {error, {T, E}}
            end
    end.
-spec derive_issuer_from_discovery(DiscoveryUri :: list()) -> binary() |
          undefined.
derive_issuer_from_discovery(DiscoveryUri) when is_list(DiscoveryUri) ->
    DiscoveryUriBin = list_to_binary(DiscoveryUri),
    case uri_string:parse(DiscoveryUriBin) of
        {error, Reason, Info} ->
            ?log_warning("Failed to parse discovery URI ~p: ~p ~p",
                         [DiscoveryUri, Reason, Info]),
            undefined;
        URI ->
            Path = maps:get(path, URI, <<>>),
            case binary:split(Path, <<"/.well-known/openid-configuration">>) of
                [Prefix, <<>>] ->
                    NewURI = URI#{path => Prefix},
                    uri_string:recompose(NewURI);
                _ ->
                    ?log_warning("Discovery URI ~p does not have the expected "
                                 "path", [DiscoveryUri]),
                    undefined
            end
    end.

get_oidc_issuers_with_discovery() ->
    case chronicle_kv:get(kv, jwt_settings) of
        {ok, {Settings, _Rev}} ->
            IssuersMap = maps:get(issuers, Settings, #{}),
            lists:foldl(
              fun({Name, Props}, Acc) ->
                      case maps:get(oidc_settings, Props, undefined) of
                          undefined -> Acc;
                          OIDC ->
                              case build_discovery_issuer(Name, OIDC) of
                                  ignore -> Acc;
                                  Map -> [Map | Acc]
                              end
                      end
              end, [], maps:to_list(IssuersMap));
        _ -> []
    end.

-spec build_discovery_issuer(Name :: string(), OIDC :: map()) ->
          map() | ignore.
build_discovery_issuer(Name, OIDC) ->
    case maps:get(oidc_discovery_uri, OIDC, undefined) of
        undefined ->
            ignore;
        Disc ->
            HttpTimeoutMs = maps:get(http_timeout_ms, OIDC),
            SslOpts = extract_oidc_connect_options(Disc, OIDC),
            DisablePAR =
                maps:get(disable_pushed_authorization_requests, OIDC, false),
            #{name => Name,
              discovery_uri => Disc,
              http_timeout_ms => HttpTimeoutMs,
              ssl_opts => SslOpts,
              disable_par => DisablePAR}
    end.

-spec extract_oidc_connect_options(URL :: string(), OidcSettings :: map()) ->
          list().
extract_oidc_connect_options(URL, OidcSettings) ->
    AddressFamily = maps:get(address_family, OidcSettings, undefined),
    VerifyPeer = maps:get(tls_verify_peer, OidcSettings, true),
    {_, Certs} = maps:get(tls_ca, OidcSettings, {<<>>, []}),
    SNI = maps:get(tls_sni, OidcSettings, ""),
    misc:tls_connect_options(URL, AddressFamily, VerifyPeer, Certs, SNI, []).
