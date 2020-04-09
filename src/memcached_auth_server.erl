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
-module(memcached_auth_server).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

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
    buckets = [],
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

    EventHandler =
        fun ({buckets, _V} = Event) ->
                gen_server:cast(Self, Event);
            ({ldap_settings, _} = Event) ->
                gen_server:cast(Self, Event);
            ({saslauthd_auth_settings, _} = Event) ->
                gen_server:cast(Self, Event);
            (_) -> ok
        end,
    ns_pubsub:subscribe_link(ns_config_events, EventHandler),

    Config = ns_config:get(),
    Buckets = ns_bucket:get_buckets(Config),
    {ok, #s{buckets = ns_bucket:get_bucket_names(Buckets),
            enabled = memcached_config_mgr:is_external_auth_service_enabled()}}.

handle_call(_Request, _From, State) ->
   {reply, unhandled, State}.

handle_cast({buckets, V}, State) ->
    Configs = proplists:get_value(configs, V),
    NewBuckets = ns_bucket:get_bucket_names(Configs),
    {noreply, State#s{buckets = NewBuckets}};

handle_cast({Prop, _}, #s{enabled = Enabled} = State)
        when Prop =:= ldap_settings;
             Prop =:= saslauthd_auth_settings ->
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
            #mc_entry{data = Data}, #s{buckets = Buckets} = State) ->
    {AuthReq} = ejson:decode(Data),
    Mechanism = proplists:get_value(<<"mechanism">>, AuthReq),
    NeedRBAC = not proplists:get_bool(<<"authentication-only">>, AuthReq),
    case authenticate(Mechanism, AuthReq) of
        {ok, Id} ->
            Resp = [{rbac, get_user_rbac_record_json(Id, Buckets)} || NeedRBAC],
            {Header#mc_header{status = ?SUCCESS},
             #mc_entry{data = ejson:encode({Resp})},
             State};
        {error, ReasonStr} ->
            UUID = misc:hexify(crypto:strong_rand_bytes(16)),
            ?log_info("Auth failed with reason: '~s' (UUID: ~s)",
                      [ReasonStr, UUID]),
            Json = {[{error, {[{context, <<"Authentication failed">>},
                               {ref, UUID}]}}]},
            {Header#mc_header{status = ?MC_AUTH_ERROR},
             #mc_entry{data = ejson:encode(Json)},
             State}
    end;

process_req(#mc_header{opcode = ?MC_ACTIVE_EXTERNAL_USERS} = Header,
            #mc_entry{data = Data},
            #s{buckets = Buckets, rbac_updater_ref = undefined} = State) ->
    ?log_debug("Received active external users: ~p", [Data]),
    Ids = [{binary_to_list(User), external} || User <- ejson:decode(Data)],
    Ref = spawn_opt(
            fun () -> update_mcd_rbac(Ids, Buckets) end, [link, monitor]),
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

authenticate(<<"PLAIN">>, AuthReq) ->
    Challenge = proplists:get_value(<<"challenge">>, AuthReq),
    case sasl_decode_plain_challenge(Challenge) of
        {ok, {"", Username, Password}} ->
            case menelaus_auth:authenticate({Username, Password}) of
                {ok, Id} ->
                    ?log_debug("Successful ext authentication for ~p",
                               [ns_config_log:tag_user_name(Username)]),
                    {ok, Id};
                _ ->
                    {error, "Invalid username or password"}
            end;
        {ok, {_Authzid, _, _}} ->
            {error, "Authzid is not supported"};
        error ->
            {error, "Invalid challenge"}
    end;
authenticate(Unknown, _) ->
    {error, io_lib:format("Unknown mechanism: ~p", [Unknown])}.

get_user_rbac_record_json(Identity, Buckets) ->
    Roles = menelaus_roles:get_compiled_roles(Identity),
    {[memcached_permissions:jsonify_user(Identity, Roles, Buckets)]}.

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
                    timer:send_after(?RECONNECT_TIMEOUT, self(), reconnect),
                    NewState
            end;
        false ->
            ?log_debug("Skipping creation of 'Auth provider' connection "
                       "because external users are disabled"),
            NewState
    end.

connect() ->
    case ns_memcached:connect([{retries, 1}, duplex]) of
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

update_mcd_rbac([], _) -> ok;
update_mcd_rbac([Id|Tail], Buckets) ->
    RBACJson = get_user_rbac_record_json(Id, Buckets),
    ?log_debug("Updating rbac record for user ~p",
               [ns_config_log:tag_user_data(Id)]),
    case mcd_update_user_permissions(RBACJson) of
        ok -> ok;
        Error -> ?log_error("Failed to update permissions for ~p: ~p",
                            [ns_config_log:tag_user_data(Id), Error])
    end,
    update_mcd_rbac(Tail, Buckets).

mcd_update_user_permissions(RBACJson) ->
    ns_memcached_sockets_pool:executing_on_socket(
      fun (Sock) ->
              try
                  mc_client_binary:update_user_permissions(Sock, RBACJson)
              catch
                  _:E -> {error, E}
              end
      end).


-ifdef(TEST).
process_data_test() ->
    Roles = [[{[admin, security], all},
              {[{bucket, any}], [read]}],
             [{[{bucket, "b1"}, data, docs], [insert, upsert]},
              {[{bucket, "b2"}, data, xattr], [write]}]],
    Users = [{{"User1", "foo"}, local, []},
             {{"User2", "bar"}, external, Roles}],

    with_mocked_users(
      Users,
      fun () ->
          test_process_data(
            {?MC_AUTH_REQUEST, undefined,
             {[{mechanism, <<"PLAIN">>}, {challenge, <<"AFVzZXIxAGZvbw==">>}]}},
            fun (?MC_AUTH_REQUEST, ?SUCCESS, undefined,
                 {[{<<"rbac">>, {[{<<"User1">>,
                                   {[{<<"buckets">>,{[{<<"b1">>,[]}]}},
                                     {<<"privileges">>,[]},
                                     {<<"domain">>,<<"local">>}]}}]}}
                  ]}) -> ok
            end),
          test_process_data(
            {?MC_AUTH_REQUEST, undefined,
             {[{mechanism, <<"PLAIN">>}, {challenge, <<"AFVzZXIyAGJhcg==">>}]}},
            fun (?MC_AUTH_REQUEST, ?SUCCESS, undefined,
                 {[{<<"rbac">>,
                    {[{<<"User2">>,{[{<<"buckets">>,{[{<<"b1">>,[_|_]}]}},
                                     {<<"privileges">>,[_]},
                                     {<<"domain">>,<<"external">>}]}}]}}
                  ]}) -> ok
            end),
          test_process_data(
            {?MC_AUTH_REQUEST, undefined,
             {[{mechanism, <<"PLAIN">>}, {challenge, <<"AGpvaG4AYmFy">>}]}},
            fun (?MC_AUTH_REQUEST, ?MC_AUTH_ERROR, undefined,
                 {[{<<"error">>, {[
                      {<<"context">>, <<"Authentication failed">>},
                      {<<"ref">>, _}]}}]}) -> ok
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
                                 buckets = ["b1"]}));
test_process_data(InputMessage, Validator) ->
    test_process_data([InputMessage], Validator).

with_mocked_users(Users, Fun) ->
    meck:new(mc_binary, [passthrough]),
    meck:new(menelaus_roles, [passthrough]),
    meck:new(menelaus_auth, [passthrough]),
    try
        meck:expect(menelaus_auth, authenticate,
                    fun ({Name, Pass}) ->
                            case [{N, D} || {{N, P}, D, _} <- Users,
                                            N == Name, P == Pass] of
                                [Id] -> {ok, Id};
                                [] -> false
                            end
                    end),

        meck:expect(menelaus_roles, get_compiled_roles,
                    fun ({Name, Domain}) ->
                            [Roles] = [R || {{N, _}, D, R} <- Users,
                                            N == Name, D == Domain],
                            Roles
                    end),
        Fun(),
        true = meck:validate(menelaus_auth),
        true = meck:validate(menelaus_roles),
        true = meck:validate(mc_binary)
    after
        meck:unload(menelaus_auth),
        meck:unload(menelaus_roles),
        meck:unload(mc_binary)
    end,
    ok.
-endif.
