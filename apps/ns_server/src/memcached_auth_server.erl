%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(memcached_auth_server).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").
-include("rbac.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(s, {
    mcd_socket = undefined,
    data = <<>>,
    snapshot = #{},
    rbac_updater_ref = undefined,
    enabled = false
}).

-define(RECONNECT_TIMEOUT, 1000).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Self = self(),
    Self ! reconnect,

    chronicle_compat_events:subscribe(
      fun (cluster_compat_version) ->
              gen_server:cast(Self, update_snapshot);
          (jwt_settings) ->
              gen_server:cast(Self, check_enabled);
          (ldap_settings) ->
              gen_server:cast(Self, check_enabled);
          (saslauthd_auth_settings) ->
              gen_server:cast(Self, check_enabled);
          (saml_settings) ->
              gen_server:cast(Self, check_enabled);
          (Key) ->
              case collections:key_match(Key) =/= false orelse
                  ns_bucket:names_change(Key) of
                  true ->
                      gen_server:cast(Self, update_snapshot);
                  false ->
                      ok
              end
      end),

    {ok, #s{snapshot = ns_bucket:get_snapshot(all, [collections, uuid]),
            enabled = memcached_config_mgr:is_external_auth_service_enabled()}}.

handle_call(_Request, _From, State) ->
   {reply, unhandled, State}.

handle_cast(update_snapshot, State) ->
    {noreply, State#s{snapshot = ns_bucket:get_snapshot(all,
                                                        [collections, uuid])}};

handle_cast(check_enabled, #s{enabled = Enabled} = State) ->
    NewEnabled = memcached_config_mgr:is_external_auth_service_enabled(),
    case NewEnabled =:= Enabled of
        true -> {noreply, State};
        false -> {noreply, reconnect(State#s{enabled = NewEnabled})}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(reconnect, State) ->
    {noreply, reconnect(State)};

handle_info({tcp, Sock, Data}, #s{mcd_socket = Sock, data = Rest} = State) ->
    NewState = process_data(State#s{data = <<Rest/binary, Data/binary>>}),
    inet:setopts(Sock, [{active, once}]),
    {noreply, NewState};

handle_info({tcp_closed, Sock}, #s{mcd_socket = Sock} = State) ->
    ?log_debug("Memcached 'auth provider' connection is closed"),
    {noreply, reconnect(State)};

handle_info({tcp_error, Sock, Reason}, #s{mcd_socket = Sock} = State) ->
    ?log_debug("Error occured on the memcached 'auth provider' socket: ~p",
               [Reason]),
    {noreply, reconnect(State)};

handle_info({'DOWN', Ref, _, _, _}, #s{rbac_updater_ref = {_, Ref}} = State) ->
    {noreply, State#s{rbac_updater_ref = undefined}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

process_data(#s{mcd_socket = Sock, data = Data} = State) ->
    case mc_binary:decode_packet_ext(Data) of
        {Header, Entry, Rest} ->
            {RespHeader, RespEntry, State2} = process_req(Header, Entry, State),
            case mc_binary:send(Sock, server_res, RespHeader, RespEntry) of
                ok -> process_data(State2#s{data = Rest});
                _ -> reconnect(State2)
            end;
        need_more_data -> State
    end.

process_req(#mc_header{opcode = ?MC_AUTH_REQUEST} = Header,
            #mc_entry{data = Data}, #s{snapshot = Snapshot} = State) ->
    {AuthReq} = ejson:decode(Data),
    Mechanism = proplists:get_value(<<"mechanism">>, AuthReq),
    NeedRBAC = not proplists:get_bool(<<"authentication-only">>, AuthReq),
    case authenticate(Mechanism, AuthReq) of
        {ok, #authn_res{expiration_datetime_utc = ExpirationTime} = AuthnRes,
         AuditProps} ->
            {User, Record} = memcached_permissions:jsonify_user_with_cache(
                               AuthnRes, Snapshot),
            Expiry =
                case ExpirationTime of
                    undefined ->
                        undefined;
                    DateTime ->
                        misc:datetime_to_unix_timestamp(DateTime)
                end,
            BaseResp =
                case Mechanism of
                    <<"PLAIN">> -> [{rbac, {[{User, Record}]}} || NeedRBAC];
                    <<"OAUTHBEARER">> ->
                        [{token,
                          {[{rbac, {[{User, Record}]}} || NeedRBAC] ++
                               [{exp, Expiry}]}}]
                end,
            Resp = maybe_add_audit_props(BaseResp, AuditProps),
            {Header#mc_header{status = ?SUCCESS},
             #mc_entry{data = ejson:encode({Resp})},
             State};
        {error, ReasonStr, AuditProps} ->
            UUID = misc:hexify(crypto:strong_rand_bytes(16)),
            ?log_info("Auth failed with reason: '~s' (UUID: ~s)",
                      [ReasonStr, UUID]),
            ReasonBin = list_to_binary("Authentication failed: " ++ ReasonStr),
            BaseJson = [{error, {[{context, ReasonBin},
                                  {ref, UUID}]}}],
            Json = maybe_add_audit_props(BaseJson, AuditProps),
            {Header#mc_header{status = ?KEY_ENOENT},
             #mc_entry{data = ejson:encode({Json})},
             State}
    end;

process_req(#mc_header{opcode = ?MC_AUTHORIZATION_REQUEST} = Header,
            #mc_entry{key = UserBin}, #s{snapshot = Snapshot} = State) ->
    User = binary_to_list(UserBin),
    Resp = [{rbac, get_user_rbac_record_json(
                     #authn_res{identity={User, external}}, Snapshot)}],
    {Header#mc_header{status = ?SUCCESS},
     #mc_entry{data = ejson:encode({Resp})},
     State};

process_req(#mc_header{opcode = ?MC_ACTIVE_EXTERNAL_USERS} = Header,
            #mc_entry{data = Data}, #s{snapshot = Snapshot,
                                       rbac_updater_ref = undefined} = State) ->
    ?log_debug("Received active external users: ~p", [Data]),
    Ids = [{binary_to_list(User), external} || User <- ejson:decode(Data)],
    Ref = spawn_opt(
            fun () -> update_mcd_rbac(Ids, Snapshot) end,
            [link, monitor]),
    {Header#mc_header{status = ?SUCCESS}, #mc_entry{},
     State#s{rbac_updater_ref = Ref}};

process_req(#mc_header{opcode = ?MC_ACTIVE_EXTERNAL_USERS} = Header,
            #mc_entry{data = Data},
            #s{rbac_updater_ref = {Pid, _}} = State) when is_pid(Pid) ->
    ?log_warning("Received active external users: ~p. Skipping rbac update "
                 "because we already have one handler spawned", [Data]),
    {Header#mc_header{status = ?SUCCESS}, #mc_entry{}, State};

process_req(Header, Data, State) ->
    ?log_error("Received unknown auth command from memcached: ~p ~p",
               [Header, Data]),
    {Header#mc_header{status = ?UNKNOWN_COMMAND}, #mc_entry{}, State}.


%% Note that JWT authentication requests alone may return ExtraGroups and
%% ExtraRoles in AuthnRes. These must be accoounted for in the RBAC record.
authenticate(<<"PLAIN">>, AuthReq) ->
    Challenge = proplists:get_value(<<"challenge">>, AuthReq),
    case sasl_decode_plain_challenge(Challenge) of
        {ok, {Authzid, Username, Password}} when Authzid == "";
                                                 Authzid == Username ->
            case menelaus_auth:authenticate({Username, Password}) of
                {ok, AuthnRes, _, AuditProps} ->
                    ?log_debug("Successful ext authentication for ~p",
                               [ns_config_log:tag_user_name(Username)]),
                    {ok, AuthnRes, AuditProps};
                {error, _, AuditProps} ->
                    {error, "Invalid username or password", AuditProps}
            end;
        {ok, {_Authzid, _, _}} ->
            {error, "Authzid is not supported", []};
        error ->
            {error, "Invalid challenge", []}
    end;
authenticate(<<"OAUTHBEARER">>, AuthReq) ->
    case cluster_compat_mode:is_cluster_totoro() andalso
        ns_config:read_key_fast(oauthbearer_enabled, true) of
        true ->
            Challenge = proplists:get_value(<<"challenge">>, AuthReq),
            case sasl_decode_oauthbearer_challenge(Challenge) of
                {ok, {Id, Token}} ->
                    case menelaus_auth:authenticate({jwt, Token}) of
                        {ok, #authn_res{identity={User, external}}=AuthnRes, _,
                         AuditProps} when Id =:= undefined orelse Id =:= User ->
                            ?log_debug("JWT Successful authentication for ~p",
                                       [ns_config_log:tag_user_name(Id)]),
                            {ok, AuthnRes, AuditProps};
                        {ok, #authn_res{identity={User, external}}, _,
                         AuditProps} ->
                            {error, "JWT Invalid user " ++ User ++ " in token",
                             AuditProps};
                        {error, _RespHeaders, AuditProps} ->
                            Reason =
                                case proplists:get_value(reason, AuditProps,
                                                         undefined) of
                                    ReasonBin when is_binary(ReasonBin) ->
                                        binary_to_list(ReasonBin);
                                    _ ->
                                        "JWT rejected"
                                end,
                            {error, Reason, AuditProps}
                    end;
                error ->
                    {error, "Invalid oauthbearer challenge", []}
            end;
        false -> {error, "Oauthbearer mechanism disabled", []}
    end;
authenticate(Unknown, _) ->
    {error, io_lib:format("Unknown mechanism: ~p", [Unknown]), []}.

get_user_rbac_record_json(AuthnRes, Snapshot) ->
    {[memcached_permissions:jsonify_user_with_cache(AuthnRes, Snapshot)]}.

cmd_auth_provider(Sock) ->
    Resp = mc_client_binary:cmd_vocal(?MC_AUTH_PROVIDER, Sock,
                                      {#mc_header{},
                                       #mc_entry{}}),
    case Resp of
        {ok, #mc_header{status = ?SUCCESS}, _} ->
            ok;
        {ok, #mc_header{status = Status}, #mc_entry{data = ErrorBin}} ->
            {error, {Status, ErrorBin}}
    end.

reconnect(State = #s{mcd_socket = OldSock, enabled = Enabled}) ->
    catch gen_tcp:close(OldSock),
    NewState = State#s{mcd_socket = undefined, data = <<>>},
    case Enabled of
        true ->
            case connect() of
                {ok, Socket} ->
                    NewState#s{mcd_socket = Socket};
                {error, _} ->
                    erlang:send_after(?RECONNECT_TIMEOUT, self(), reconnect),
                    NewState
            end;
        false ->
            ?log_debug("Skipping creation of 'Auth provider' connection "
                       "because external users are disabled"),
            NewState
    end.

connect() ->
    case ns_memcached:connect(?MODULE_STRING, [{retries, 1}, duplex]) of
        {ok, Sock} ->
            case cmd_auth_provider(Sock) of
                ok ->
                    ?log_debug("Auth provider connection established "
                               "(socket info: ~p)", [inet:sockname(Sock)]),
                    inet:setopts(Sock, [{active, once}]),
                    {ok, Sock};
                {error, Error} ->
                    gen_tcp:close(Sock),
                    ?log_error("Failed to enable 'Auth provider' feature on "
                               "the memcached connection: ~p", [Error]),
                    {error, Error}
            end;
        {error, Reason} ->
            ?log_error("Failed to establish 'Auth provider' connection "
                       "to memcached: ~p", [Reason]),
            {error, Reason}
    end.

%% RFC4616
sasl_decode_plain_challenge(undefined) -> error;
sasl_decode_plain_challenge(Challenge) ->
    try base64:decode(Challenge) of
        Decoded ->
            case binary:split(Decoded, <<0>>, [global]) of
                [Authzid, Authcid, Passwd] ->
                    {ok, {binary_to_list(Authzid),
                          binary_to_list(Authcid),
                          binary_to_list(Passwd)}};
                _ ->
                    error
            end
    catch
        _:_ -> error
    end.

find_bearer_token(Fields) ->
    [Token ||
        Field <- Fields,
        <<"auth=", AuthRest/binary>> <- [Field],
        [Type, Token] <- [binary:split(AuthRest, <<" ">>, [trim_all])],
        string:equal(Type, "bearer", true)
    ].

%% RFC7628 (oauthbearer), RFC5801 (gs2-header)
sasl_decode_oauthbearer_challenge(undefined) -> error;
sasl_decode_oauthbearer_challenge(Challenge) ->
    try base64:decode(Challenge, #{mode => urlsafe}) of
        FullMessage ->
            case binary:split(FullMessage, <<1>>, [global, trim_all]) of
                [GS2Header | AuthFields] ->
                    case GS2Header of
                        <<"n,", Rest/binary>> ->
                            User =
                                case binary:split(Rest, <<",">>, [trim_all]) of
                                    [<<"a=", UserBin/binary>> | _] ->
                                        binary_to_list(UserBin);
                                    _ ->
                                        %% No authzid in header
                                        undefined
                                end,
                            case find_bearer_token(AuthFields) of
                                [Token] ->
                                    {ok, {User, binary_to_list(Token)}};
                                _ ->
                                    error
                            end;
                        _ ->
                            error % Invalid GS2 header
                    end
            end
    catch _:_ ->
            error
    end.

update_mcd_rbac([], _) -> ok;
update_mcd_rbac([Id|Tail], Snapshot) ->
    RBACJson = get_user_rbac_record_json(#authn_res{identity=Id}, Snapshot),
    ?log_debug("Updating rbac record for user ~p",
               [ns_config_log:tag_user_data(Id)]),
    case mcd_update_user_permissions(RBACJson) of
        ok -> ok;
        Error -> ?log_error("Failed to update permissions for ~p: ~p",
                            [ns_config_log:tag_user_data(Id), Error])
    end,
    update_mcd_rbac(Tail, Snapshot).

mcd_update_user_permissions(RBACJson) ->
    ns_memcached_sockets_pool:executing_on_socket(
      fun (Sock) ->
              try
                  mc_client_binary:update_user_permissions(Sock, RBACJson)
              catch
                  _:E -> {error, E}
              end
      end).

maybe_add_audit_props(Base, []) -> Base;
maybe_add_audit_props(Base, AuditProps) ->
    Base ++ [{audit_props, {AuditProps}}].

-ifdef(TEST).
process_data_test() ->
    Roles = [[{[admin, memcached], all}],
             [{[{bucket, "b1"}, data, docs], [insert, upsert]}]],
    ExtraRoles = [[{[{bucket, "b2"}, data, docs], [read]}]],
    %% Four-tuple format: {Name, Pass}, Domain, Roles, ExtraRoles
    Users = [{{"User1", "foo"}, local, [], []},
             {{"User2", "bar"}, external, Roles, []},
             {{"User3", "jwt"}, external, Roles, ExtraRoles}],

    %% PLAIN challenges
    User1Challenge = <<"AFVzZXIxAGZvbw==">>, % User1/foo
    User2Challenge = <<"AFVzZXIyAGJhcg==">>, % User2/bar
    InvalidUserChallenge = <<"AGpvaG4AYmFy">>, % john/bar (not in users)
    User3Challenge = <<"AFVzZXIzAGp3dA==">>, % User3/jwt

    %% OAUTHBEARER challenges
    %% n,a=User3,\01auth=Bearer jwt_token\01
    ValidWithAuthzidJWTChallenge =
        <<"bixhPVVzZXIzLAFhdXRoPUJlYXJlciBqd3RfdG9rZW4B">>,
    %% n,\01key=value1\01auth=BeArEr jwt_token\01key2=value2\01
    ValidWithoutAuthzidJWTChallenge =
        <<"biwBa2V5PXZhbHVlMQFhdXRoPUJlQXJFciBqd3RfdG9rZW4Ba2V5Mj12YWx1ZTIB">>,
    %% n,a=WrongUser,\01auth=Bearer jwt_token\01
    UserMismatchChallenge =
        <<"bixhPVdyb25nVXNlciwBYXV0aD1CZWFyZXIgand0X3Rva2VuAQ==">>,
    %% n,a=User3,\01auth=Bearer invalid_token\01
    InvalidTokenChallenge =
        <<"bixhPVVzZXIzLAFhdXRoPUJlYXJlciBpbnZhbGlkX3Rva2VuAQ==">>,
    InvalidFormatChallenge =
        <<"invalid_challenge">>,

    ValidateJWTProps =
        fun(PropsToValidate) ->
                [{<<"token">>, {TokenProps}},
                 {<<"audit_props">>, {AuditProps}}] =
                    PropsToValidate,

                Rbac = proplists:get_value(<<"rbac">>, TokenProps),
                Exp = proplists:get_value(<<"exp">>, TokenProps),
                true = is_number(Exp),
                {[{<<"User3">>, UserProps}]} = Rbac,
                {UserData} = UserProps,

                <<"external">> =
                    proplists:get_value(<<"domain">>, UserData),

                {Buckets} = proplists:get_value(<<"buckets">>,
                                                UserData),
                B1 = proplists:get_value(<<"b1">>, Buckets),
                B2 = proplists:get_value(<<"b2">>, Buckets),
                {[{<<"privileges">>, _}]} = B1,
                {[{<<"privileges">>, _}]} = B2,

                <<"jwt">> = proplists:get_value(<<"type">>, AuditProps),
                <<"test-issuer">> =
                    proplists:get_value(<<"iss">>, AuditProps),
                <<"User3">> = proplists:get_value(<<"sub">>,
                                                  AuditProps),
                <<"2099-01-01T00:00:00Z">> =
                    proplists:get_value(<<"expiry_with_leeway">>,
                                        AuditProps),
                ok
        end,

    with_mocked_users(
      Users,
      fun () ->
              %% PLAIN auth tests
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"PLAIN">>},
                   {challenge, User1Challenge}]}},
                fun (?MC_AUTH_REQUEST, ?SUCCESS, undefined,
                     {[{<<"rbac">>, {[{<<"User1">>,
                                       {[{<<"buckets">>, {[]}},
                                         {<<"privileges">>, []},
                                         {<<"domain">>, <<"local">>}]}}]}}
                      ]}) -> ok
                end),
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"PLAIN">>},
                   {challenge, User2Challenge}]}},
                fun (?MC_AUTH_REQUEST, ?SUCCESS, undefined,
                     {[{<<"rbac">>,
                        {[{<<"User2">>,
                           {[{<<"buckets">>,
                              {[{<<"b1">>, {[{<<"privileges">>, [_|_]}]}}]}},
                             {<<"privileges">>, [_|_]},
                             {<<"domain">>, <<"external">>}]}}]}}
                      ]}) -> ok
                end),
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"PLAIN">>},
                   {challenge, InvalidUserChallenge}]}},
                fun (?MC_AUTH_REQUEST, ?KEY_ENOENT, undefined,
                     {[{<<"error">>,
                        {[{<<"context">>, <<"Authentication failed: ",
                                            _/binary>>},
                          {<<"ref">>, _}]}}]}) -> ok
                end),
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"PLAIN">>},
                   {challenge, User3Challenge}]}},
                fun (?MC_AUTH_REQUEST, ?SUCCESS, undefined,
                     {[{<<"rbac">>,
                        {[{<<"User3">>,
                           {[{<<"buckets">>, Buckets},
                             {<<"privileges">>, [_|_]},
                             {<<"domain">>, <<"external">>}]}}]}}]}) ->
                        B1 = proplists:get_value(<<"b1">>, element(1, Buckets)),
                        B2 = proplists:get_value(<<"b2">>, element(1, Buckets)),
                        case {B1, B2} of
                            {{[{<<"privileges">>, [_|_]}]},
                             {[{<<"privileges">>, [_|_]}]}} -> ok;
                            _ -> error
                        end
                end),

              %% OAUTHBEARER tests - successful authentication
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"OAUTHBEARER">>},
                   {challenge, ValidWithAuthzidJWTChallenge}]}},
                fun (?MC_AUTH_REQUEST, ?SUCCESS, undefined,
                     {Props}) ->
                        ValidateJWTProps(Props)
                end),

              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"OAUTHBEARER">>},
                   {challenge, ValidWithoutAuthzidJWTChallenge}]}},
                fun (?MC_AUTH_REQUEST, ?SUCCESS, undefined,
                     {Props}) ->
                        ValidateJWTProps(Props)
                end),

              %% OAUTHBEARER tests - invalid token
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"OAUTHBEARER">>},
                   {challenge, InvalidTokenChallenge}]}},
                fun (?MC_AUTH_REQUEST, ?KEY_ENOENT, undefined,
                     {Props}) ->
                        [{<<"error">>,
                          {[{<<"context">>,
                             <<"Authentication failed: ", _/binary>>},
                            {<<"ref">>, _}]}},
                         {<<"audit_props">>, {AuditProps}}] = Props,

                        <<"jwt">> = proplists:get_value(<<"type">>, AuditProps),
                        <<"Token has expired">> =
                            proplists:get_value(<<"reason">>, AuditProps),
                        ok
                end),

              %% OAUTHBEARER tests - user mismatch
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"OAUTHBEARER">>},
                   {challenge, UserMismatchChallenge}]}},
                fun (?MC_AUTH_REQUEST, ?KEY_ENOENT, undefined,
                     {Props}) ->
                        [{<<"error">>,
                          {[{<<"context">>,
                             <<"Authentication failed: ", _/binary>>},
                            {<<"ref">>, _}]}},
                         {<<"audit_props">>, {AuditProps}}] = Props,

                        <<"jwt">> = proplists:get_value(<<"type">>, AuditProps),
                        <<"test-issuer">> =
                            proplists:get_value(<<"iss">>, AuditProps),
                        <<"User3">> = proplists:get_value(<<"sub">>,
                                                          AuditProps),
                        <<"2099-01-01T00:00:00Z">> =
                            proplists:get_value(<<"expiry_with_leeway">>,
                                                AuditProps),
                        ok
                end),

              %% OAUTHBEARER tests - invalid challenge format
              test_process_data(
                {?MC_AUTH_REQUEST, undefined,
                 {[{mechanism, <<"OAUTHBEARER">>},
                   {challenge, InvalidFormatChallenge}]}},
                fun (?MC_AUTH_REQUEST, ?KEY_ENOENT, undefined,
                     {[{<<"error">>,
                        {[{<<"context">>, <<"Authentication failed: ",
                                            _/binary>>},
                          {<<"ref">>, _}]}}]}) ->
                        ok
                end)
      end),
    ok.

test_process_data(InputMessages, Validator) when is_list(InputMessages) ->
    Encode = fun (undefined) -> undefined; (D) -> ejson:encode(D) end,
    Decode = fun (undefined) -> undefined; (D) -> ejson:decode(D) end,
    InputData =
        lists:map(
          fun ({Op, Key, Data}) ->
              Header = #mc_header{opcode = Op},
              Entry = #mc_entry{key = Encode(Key),
                                data = Encode(Data)},
              mc_binary:encode(req, Header, Entry)
          end, InputMessages),
    Bin = iolist_to_binary(InputData),

    meck:expect(
      mc_binary, send,
      fun (my_socket, server_res,
           #mc_header{opcode = Op, status = Status},
           #mc_entry{key = Key, data = Data}) ->
              ?assertEqual(
                ok, Validator(Op, Status, Decode(Key), Decode(Data)))
      end),
    ?assertMatch(#s{data = <<"rest">>},
                 process_data(#s{mcd_socket = my_socket,
                                 data = <<Bin/binary, "rest">>,
                                 snapshot = ns_bucket:toy_buckets(
                                              [{"b1", [{uuid, <<"b1id">>}]},
                                               {"b2",
                                                [{uuid, <<"b2id">>}]}])}));
test_process_data(InputMessage, Validator) ->
    test_process_data([InputMessage], Validator).

with_mocked_users(Users, Fun) ->
    meck:new(mc_binary, [passthrough]),
    meck:new(menelaus_roles, [passthrough]),
    meck:new(menelaus_auth, [passthrough]),
    meck:new(menelaus_users, [passthrough]),
    meck:new(ns_config, [passthrough]),
    meck:new(cluster_compat_mode, [passthrough]),

    try
        meck:expect(ns_config, read_key_fast,
                    fun(oauthbearer_enabled, Default) ->
                            Default;
                       (Key, Default) ->
                            meck:passthrough([Key, Default])
                    end),
        meck:expect(cluster_compat_mode, is_cluster_totoro,
                    fun () ->
                            true
                    end),

        meck:expect(menelaus_auth, authenticate,
                    fun ({jwt, Token}) ->
                            %% Mock JWT authentication
                            case Token of
                                "jwt_token" ->
                                    %% Look up User3 from test data
                                    case [U || {{N, _}, _, _, _} = U <- Users,
                                               N == "User3"] of
                                        [] ->
                                            {error, [{reason,
                                                      <<"User not found">>}],
                                             {user_not_found, []}};
                                        [{{_, _}, Domain, _, ExtraRoles}|_] ->
                                            ExpTime = {{2099,1,1},{0,0,0}},

                                            AuditProps =
                                                [
                                                 {<<"type">>, <<"jwt">>},
                                                 {<<"iss">>, <<"test-issuer">>},
                                                 {<<"sub">>, <<"User3">>},
                                                 {<<"expiry_with_leeway">>,
                                                  <<"2099-01-01T00:00:00Z">>}
                                                ],

                                            {ok,
                                             #authn_res{
                                                identity = {"User3", Domain},
                                                extra_roles = ExtraRoles,
                                                expiration_datetime_utc =
                                                    ExpTime
                                               }, [], AuditProps}
                                    end;

                                "invalid_token" ->
                                    AuditProps =
                                        [
                                         {<<"type">>, <<"jwt">>},
                                         {<<"reason">>, <<"Token has expired">>}
                                        ],
                                    {error, [], AuditProps}
                            end;
                        ({Name, Pass}) ->
                            case [{N, D, ER} || {{N, P}, D, _, ER} <- Users,
                                                N == Name, P == Pass] of
                                [{N, D, ER}] ->
                                    {ok, #authn_res{identity = {N, D},
                                                    extra_roles = ER}, [], []};
                                [] -> {error, "Invalid username or password",
                                    []}
                            end
                    end),

        meck:expect(menelaus_roles, get_compiled_roles,
                    fun (#authn_res{identity = {Name, Domain},
                                    extra_roles = ExtraRoles}) ->
                            [BaseRoles] = [R || {{N, _}, D, R, _} <- Users,
                                                N == Name, D == Domain],
                            case ExtraRoles of
                                undefined -> BaseRoles;
                                _ -> BaseRoles ++ ExtraRoles
                            end
                    end),
        %% the following 2 funs are used for checking collection specific
        %% permissions only, so returning [] from both will just result in
        %% checking none of them
        meck:expect(menelaus_roles, get_roles,
                    fun ({_Name, _Domain}) -> [] end),
        meck:expect(menelaus_roles, get_definitions, fun (all) -> [] end),
        meck:expect(menelaus_users, is_user_locked, fun (_) -> false end),
        Fun(),
        true = meck:validate(menelaus_users),
        true = meck:validate(menelaus_auth),
        true = meck:validate(menelaus_roles),
        true = meck:validate(mc_binary),
        true = meck:validate(ns_config)
    after
        meck:unload(menelaus_users),
        meck:unload(menelaus_auth),
        meck:unload(menelaus_roles),
        meck:unload(mc_binary),
        meck:unload(ns_config)
    end,
    ok.
-endif.
