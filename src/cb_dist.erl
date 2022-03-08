%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc Wraper for *_dist modules. Allows to start/stop listeners dynamically.
-module(cb_dist).

-behaviour(gen_server).

-include_lib("kernel/include/net_address.hrl").
-include_lib("kernel/include/dist_util.hrl").
-include_lib("kernel/include/logger.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

-include("cut.hrl").
-include("ns_common.hrl").

% dist module callbacks, called from net_kernel
-export([listen/1, accept/1, accept_connection/5,
         setup/5, close/1, select/1, is_node_name/1, childspecs/0]).

% management api
-export([start_link/0,
         get_preferred_dist/1,
         reload_config/0,
         reload_config/1,
         ensure_config/0,
         status/0,
         config_path/0,
         address_family/0,
         external_encryption/0,
         external_listeners/0,
         update_config/1,
         proto_to_encryption/1,
         format_error/1,
         netsettings2str/1,
         restart_tls/0,
         netsettings2proto/1,
         proto2netsettings/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(con, {ref :: reference(),
              mod :: protocol(),
              pid :: pid() | undefined | shutdown,
              mon :: reference() | undefined}).

-record(s, {listeners = [],
            acceptors = [],
            kernel_pid = undefined,
            config = undefined,
            name = undefined,
            ensure_config_timer = undefined,
            connections = [] :: [#con{}],
            is_pkey_encrypted = false}).

-define(family, ?MODULE).
-define(proto, ?MODULE).
-define(TERMINATE_TIMEOUT, 1000).
-define(TERMINATE_ACCEPTOR_TIMEOUT, 10000).
-define(ENSURE_CONFIG_TIMEOUT, 10000).
-define(CREATION, -1).

-type socket() :: any().
-type protocol() :: inet_tcp_dist | inet6_tcp_dist |
                    inet_tls_dist | inet6_tls_dist.

%%%===================================================================
%%% API
%%%===================================================================

childspecs() ->
    CBDistSpec = [{?MODULE, {?MODULE, start_link, []},
                   permanent, infinity, worker, [?MODULE]}],
    DistSpecs =
        lists:flatmap(
          fun (Mod) ->
                  case (catch Mod:childspecs()) of
                      {ok, Childspecs} when is_list(Childspecs) -> Childspecs;
                      _ -> []
                  end
          end, [inet_tcp_dist, inet_tls_dist]),
    {ok, DistSpecs ++ CBDistSpec}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec listen(Name :: atom()) ->
    {ok, {LSocket :: any(),
          LocalTcpAddress :: #net_address{},
          Creation :: ?CREATION}}.
listen(Name) when is_atom(Name) ->
    Pid = whereis(?MODULE),
    case gen_server:call(Pid, {listen, Name}, infinity) of
        ok ->
            Addr = #net_address{address = undefined,
                                host = undefined,
                                protocol = ?family,
                                family = ?proto},
            {ok, {Pid, Addr, ?CREATION}};
        {error, _} = Error ->
            Error
    end.

-spec accept(LSocket :: any()) -> AcceptorPid :: pid().
accept(_LSocket) ->
    gen_server:call(?MODULE, {accept, self()}, infinity).

-spec accept_connection(CBDistPid :: pid(),
                        Acceptor :: {reference(), pid(), module(), socket()},
                        MyNode :: atom(),
                        Allowed :: any(),
                        SetupTime :: any()) ->
                            {ConRef :: reference(),
                             ConPid :: pid(),
                             HandshakeProcPid :: pid()}.
accept_connection(_, {ConRef, HandshakeProcPid, Module, ConnectionSocket},
                  MyNode, Allowed, SetupTime) ->
    info_msg("Accepting connection from ~p using module ~p",
             [HandshakeProcPid, Module]),
    ConPid = Module:accept_connection(HandshakeProcPid, ConnectionSocket,
                                      MyNode, Allowed, SetupTime),
    {ConRef, ConPid, HandshakeProcPid}.

-spec select(Node :: atom()) -> true | false.
select(Node) ->
    try get_preferred_dist(Node) of
        Module -> Module:select(Node)
    catch
        _:Error ->
            error_msg("Select for ~p failed. Couldn't find preferred proto: ~p",
                      [Node, Error]),
            false
    end.

-spec setup(Node :: atom(),
            Type :: hidden | normal,
            MyNode :: atom(),
            LongOrShortNames :: any(),
            SetupTime :: any()) -> ConPid :: pid().
setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    try get_preferred_dist(Node) of
        Mod ->
            info_msg("Setting up new connection to ~p using ~p", [Node, Mod]),
            %% We can't call Mod:setup from inside of cb_dist process because
            %% dist modules expect self() to be net_kernel
            with_registered_connection(
              ?cut(Mod:setup(Node, Type, MyNode, LongOrShortNames, SetupTime)),
              Mod)
    catch
        _:Error ->
            spawn_opt(
              fun () ->
                  error_msg("** Connection to ~p failed. Couldn't find "
                            "preferred proto: ~p", [Node, Error]),
                  ?shutdown2(Node, Error)
              end, [link])
    end.

-spec is_node_name(Node :: atom()) -> true | false.
is_node_name(Node) ->
    select(Node).

-spec close(Pid :: pid()) -> ok.
close(Pid) ->
    gen_server:call(Pid, close, infinity).

-spec get_preferred_dist(TargetNode :: atom() | string()) -> protocol().
get_preferred_dist(TargetNode) ->
    case gen_server:call(?MODULE, {get_preferred, TargetNode}, infinity) of
        {ok, Res} -> Res;
        {exception, {_, E, _}} -> erlang:error(E)
    end.

reload_config() ->
    gen_server:call(?MODULE, reload_config, infinity).

reload_config(Node) when is_atom(Node) ->
    gen_server:call({?MODULE, Node}, reload_config, infinity).

ensure_config() ->
    gen_server:call(?MODULE, ensure_config, infinity).

status() ->
    gen_server:call(?MODULE, status).

config_path() ->
    case application:get_env(kernel, dist_config_file) of
        {ok, F} -> F;
        _ ->
            error_msg("Path to cb_dist config is not set", []),
            erlang:error(no_dist_config_file)
    end.

address_family() ->
    proto_to_family(conf(preferred_external_proto, get_config())).

external_encryption() ->
    proto_to_encryption(conf(preferred_external_proto, get_config())).

external_listeners() ->
    L = conf(external_listeners, get_config()),
    lists:usort([proto2netsettings(Proto) || Proto <- L]).

get_config() ->
    try status() of
        Status -> proplists:get_value(config, Status, [])
    catch
        exit:{noproc, {gen_server, call, _}} ->
            read_config(config_path(), true)
    end.

update_config(Props) ->
    gen_server:call(?MODULE, {update_config, Props}, infinity).

restart_tls() ->
    gen_server:call(?MODULE, restart_tls, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Config = read_config(config_path(), true),
    info_msg("Starting cb_dist with config ~p", [Config]),
    process_flag(trap_exit,true),
    {ok, #s{config = Config, is_pkey_encrypted = is_pkey_encrypted()}}.

handle_call({listen, Name}, _From, State) ->
    State1 = State#s{name = Name},

    Protos0 = get_protos(State1),

    %% On unpatched erlang ipv6 listener automatically tries to listen on
    %% ipv4 as well (on linux and mac only) which prevents ipv4 listener
    %% to start later. To work around that start the ipv4 listener before
    %% the ipv6 one.
    OrderFun =
        fun ({_, inet_tcp_dist}, _) -> true;
            (_, {_, inet_tcp_dist}) -> false;
            ({_, inet_tls_dist}, _) -> true;
            (_, {_, inet_tls_dist}) -> false;
            (A, B) -> A =< B
        end,
    Protos = lists:sort(OrderFun, Protos0),

    Required = [R || R <- get_required_protos(State1), lists:member(R, Protos)],

    info_msg("Initial protos: ~p, required protos: ~p", [Protos, Required]),

    Listeners =
        lists:filtermap(
            fun (Module) ->
                    case listen_proto(Module, Name) of
                        {ok, Res} -> {true, {Module, Res}};
                        _Error -> false
                    end
            end, Protos),
    NotStartedRequired = Required -- [M || {M, _} <- Listeners],
    State2 = State1#s{listeners = Listeners},
    case NotStartedRequired of
        [] -> {reply, ok, State2};
        _ ->
            error_msg("Failed to start required dist listeners ~p. "
                      "Net kernel will not start", [NotStartedRequired]),
            close_listeners(State2),
            {reply, {error, {not_started, NotStartedRequired}}, State}
    end;

handle_call({accept, KernelPid}, _From, #s{listeners = Listeners} = State) ->
    NewState = lists:foldl(
                 fun ({L, _}, AccState) ->
                     start_acceptor(L, AccState)
                 end, State, Listeners),
    {reply, self(), ensure_config(NewState#s{kernel_pid = KernelPid})};

handle_call({get_module_by_acceptor, AcceptorPid}, _From,
            #s{acceptors = Acceptors} = State) ->
    {_, Module} = proplists:get_value(AcceptorPid, Acceptors,
                                      {undefined, undefined}),
    {reply, Module, State};

handle_call({get_preferred, Target}, _From, #s{name = Name,
                                               config = Config} = State) ->
    try cb_epmd:is_local_node(Target) of
        IsLocalDest ->
            IsLocalSource = cb_epmd:is_local_node(Name),
            Res =
                case IsLocalDest or IsLocalSource of
                    true -> conf(preferred_local_proto, Config);
                    false -> conf(preferred_external_proto, Config)
                end,
            {reply, {ok, Res}, State}
    catch
        C:E:S ->
            {reply, {exception, {C, E, S}}, State}
    end;

handle_call(close, _From, State) ->
    {reply, ok, close_listeners(State)};

handle_call(reload_config, _From, State) ->
    handle_reload_config(State);

handle_call(ensure_config, _From, State) ->
    NewState = ensure_config(State),
    case not_started_required_listeners(NewState) of
        [] -> {reply, ok, NewState};
        List ->
            ProtoList = [proto2netsettings(L) || {_, L} <- List],
            {reply, {error, {not_started, ProtoList}}, NewState}
    end;

handle_call(status, _From, #s{listeners = Listeners,
                              acceptors = Acceptors,
                              name = Name,
                              config = Config,
                              connections = Connections} = State) ->
    {reply, [{name, Name},
             {config, Config},
             {listeners, Listeners},
             {acceptors, Acceptors},
             {connections, Connections}], State};

handle_call({update_config, Props}, _From, #s{config = Cfg} = State) ->
    case store_config(import_props_to_config(Props, Cfg)) of
        ok -> handle_reload_config(State);
        {error, _} = Error -> {reply, Error, State}
    end;

handle_call({register_connection, Mod}, _From,
            #s{connections = Connections} = State) ->
    maybe_update_pkey_passphrase(client, Mod, State),
    Ref = make_ref(),
    Con = #con{ref = Ref, mod = Mod, pid = undefined},
    info_msg("Added connection ~p", [Con]),
    {reply, Ref, State#s{connections = [Con | Connections]}};

handle_call({update_connection_pid, Ref, Pid}, _From, State) ->
    {reply, ok, update_connection_pid(Ref, Pid, State)};

handle_call(restart_tls, _From, #s{connections = Connections,
                                   kernel_pid = KernelPid,
                                   listeners = Listeners} = State) ->
    info_msg("Restarting tls distribution protocols (if any)", []),
    TLSListeners = [L || {{_, P} = L, _} <- Listeners, proto_to_encryption(P)],
    NewState = lists:foldl(fun remove_proto/2, State, TLSListeners),

    NewConnections =
        lists:filter(
          fun (#con{mod = Mod, pid = Pid, mon = Mon}) ->
                  case proto2netsettings(Mod) of
                      {_, true = _Encryption} ->
                          if
                              is_pid(Pid) ->
                                  info_msg("Closing connection ~p because of "
                                           "tls restart", [Pid]),
                                  close_dist_connection(Mon, Pid, KernelPid),
                                  false;
                              true ->
                                  true
                          end;
                      {_, _} ->
                          true
                  end
          end, Connections),

    gen_server:call(
      ssl_pem_cache:name(dist),
      {unconditionally_clear_pem_cache, self()},
      infinity),

    {reply, ok, ensure_config(NewState#s{
                                connections = NewConnections,
                                is_pkey_encrypted = is_pkey_encrypted()})};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({accept, HandshakeProcPid, ConSocket, Family, Protocol},
            #s{kernel_pid = KernelPid, connections = Connections} = State)
                                when Family =:= inet orelse Family =:= inet6,
                                     Protocol =:= tcp orelse Protocol =:= tls ->
    Ref = make_ref(),
    Module = netsettings2proto({Family, Protocol == tls}),
    Con = #con{ref = Ref, mod = Module},
    info_msg("Accepted new connection from ~p DistCtrl ~p: ~p",
             [HandshakeProcPid, ConSocket, Con]),
    KernelPid ! {accept, self(), {Ref, HandshakeProcPid, Module, ConSocket},
                 ?family, ?proto},
    {noreply, State#s{connections = [Con | Connections]}};

handle_info({accept, HandshakeProcPid, ConSocket, Family, Protocol}, State) ->
    error_msg("Accept for unknown family = ~p or protocol = ~p DistCtrl ~p",
              [Family, Protocol, ConSocket]),
    HandshakeProcPid ! {self(), unsupported_protocol},
    {noreply, State};

handle_info({KernelPid, controller, {ConRef, ConPid, HandshakeProcPid}},
            #s{kernel_pid = KernelPid} = State) ->
    HandshakeProcPid ! {self(), controller, ConPid},
    {noreply, update_connection_pid(ConRef, ConPid, State)};

handle_info({'EXIT', Kernel, Reason}, State = #s{kernel_pid = Kernel}) ->
    error_msg("received EXIT from kernel, stoping: ~p", [Reason]),
    {stop, Reason, State};

handle_info({'EXIT', From, Reason}, #s{acceptors = Acceptors} = State) ->
    error_msg("received EXIT from ~p, reason: ~p", [From, Reason]),
    case {is_restartable_event(Reason), lists:keyfind(From, 1, Acceptors)} of
        {true, {From, Listener}} ->
            error_msg("Restart acceptor for ~p", [Listener]),
            NewAcceptors = proplists:delete(From, Acceptors),
            {noreply, start_acceptor(Listener,
                                     State#s{acceptors = NewAcceptors})};
        _ ->
            {stop, {'EXIT', From, Reason}, State}
    end;

handle_info(ensure_config_timer, State) ->
    info_msg("received ensure_config_timer", []),
    {noreply, ensure_config(State#s{ensure_config_timer = undefined})};

handle_info({'DOWN', MonRef, process, Pid, _Reason},
            #s{connections = Connections} = State) ->
    case lists:keytake(MonRef, #con.mon, Connections) of
        {value, Con, Rest} ->
            info_msg("Connection down: ~p", [Con]),
            {noreply, State#s{connections = Rest}};
        false ->
            error_msg("Received DOWN for unknown connection: ~p ~p",
                      [MonRef, Pid]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    error_msg("received unknown message: ~p", [Info]),
    {noreply, State}.

terminate(Reason, State) ->
    error_msg("terminating with reason: ~p", [Reason]),
    close_listeners(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

close_listeners(#s{listeners = Listeners} = State) ->
    Protos = [M || {M, _} <- Listeners],
    lists:foldl(fun (M, S) -> remove_proto(M, S) end, State, Protos).

%% inet_*_dist modules use inet_dist_listen_min and inet_dist_listen_max to
%% choose port for listening. Since we need them to choose Port we set those
%% variables to Port, call listen function, and then change variables back.
with_dist_ip_and_port(_, noport, _Fun) -> ignore;
with_dist_ip_and_port(IP, Port, Fun) ->
    OldMin = application:get_env(kernel,inet_dist_listen_min),
    OldMax = application:get_env(kernel,inet_dist_listen_max),
    OldIP = application:get_env(kernel,inet_dist_use_interface),
    try
        application:set_env(kernel, inet_dist_listen_min, Port),
        application:set_env(kernel, inet_dist_listen_max, Port),
        application:set_env(kernel, inet_dist_use_interface, IP),
        Fun()
    after
        case OldMin of
            undefined -> application:unset_env(kernel, inet_dist_listen_min);
            {ok, V1} -> application:set_env(kernel, inet_dist_listen_min, V1)
        end,
        case OldMax of
            undefined -> application:unset_env(kernel, inet_dist_listen_max);
            {ok, V2} -> application:set_env(kernel, inet_dist_listen_max, V2)
        end,
        case OldIP of
            undefined ->
                application:unset_env(kernel, inet_dist_use_interface);
            {ok, V3} ->
                application:set_env(kernel, inet_dist_use_interface, V3)
        end
    end.

add_proto(Listener,
          #s{name = NodeName, listeners = Listeners} = State) ->
    case can_add_proto(Listener, State) of
        ok ->
            case listen_proto(Listener, NodeName) of
                {ok, L} ->
                    NewState = State#s{listeners = [{Listener, L} | Listeners]},
                    start_acceptor(Listener, NewState);
                {error, eafnosupport} -> State;
                {error, eprotonosupport} -> State;
                ignore -> State;
                _Error -> start_ensure_config_timer(State)
            end;
        {error, Reason} ->
            error_msg("Ignoring ~p listener, reason: ~p", [Listener, Reason]),
            State
    end.

start_acceptor({_AddrType, Mod} = Listener,
               #s{listeners = Listeners, acceptors = Acceptors} = State) ->
    case proplists:get_value(Listener, Listeners) of
        undefined ->
            error_msg("Ignoring attempt to start an acceptor for unknown "
                      "listener: ~p", [Listener]),
            State;
        {LSocket, _, _} ->
            case maybe_update_pkey_passphrase(server, Mod, State) of
                true ->
                    try
                        APid = Mod:accept(LSocket),
                        true = is_pid(APid),
                        info_msg("Started acceptor ~p: ~p", [Mod, APid]),
                        State#s{acceptors = [{APid, Listener} | Acceptors]}
                    catch
                        _:E:ST ->
                            error_msg(
                              "Accept failed for protocol ~p with reason: ~p~n"
                              "Stacktrace: ~p", [Listener, E, ST]),
                            start_ensure_config_timer(
                              remove_proto(Listener, State))
                    end;
                false ->
                    info_msg("Private key password is not "
                             "available yet or undefined, waiting...", []),
                    start_ensure_config_timer(remove_proto(Listener, State))
            end
    end.

maybe_update_pkey_passphrase(Type, Mod,
                             #s{is_pkey_encrypted = IsPKeyEncrypted}) ->
    case proto_to_encryption(Mod) andalso IsPKeyEncrypted of
        true ->
            case extract_pkey_passphrase() of
                {ok, PassFun} ->
                    Pass = PassFun(),
                    MS = ets:fun2ms(
                           fun ({T, [{password, _} | L]}) when T =:= Type ->
                                    {T, [{password, Pass} | L]};
                               ({T, L}) when T =:= Type ->
                                    {T, [{password, Pass} | L]}
                           end),
                    try
                        N = ets:select_replace(ssl_dist_opts, MS),
                        info_msg("Updated ~p ssl_dist_opts records", [N]),
                        case Pass of
                            undefined ->
                                error_msg("Extracted pkey passphrase is "
                                          "undefined", []),
                                false;
                            _ -> true
                        end
                    catch
                        error:badarg ->
                            error_msg(
                              "Failed to modify ssl_dist_opts, "
                              "it will not work on vanilla erlang",
                              []),
                            false
                    end;
                {error, not_available} ->
                    false
            end;
        false ->
            true
    end.

start_ensure_config_timer(#s{ensure_config_timer = undefined} = State) ->
    Ref = erlang:send_after(?ENSURE_CONFIG_TIMEOUT, self(),
                            ensure_config_timer),
    State#s{ensure_config_timer = Ref};
start_ensure_config_timer(#s{} = State) ->
    State.

remove_proto({_AddrType, Mod} = Listener,
             #s{listeners = Listeners, acceptors = Acceptors,
                connections = Connections} = State) ->
    case proplists:get_value(Listener, Listeners) of
        {LSocket, _, _} ->
            info_msg("Closing listener ~p", [Listener]),
            AcceptorProcs =
                lists:flatmap(
                  fun ({P, M}) when M =:= Listener ->
                          erlang:unlink(P),
                          Links = case erlang:process_info(P, links) of
                                      undefined -> [];
                                      {links, L} ->
                                          %% Acceptor can be linked to newly
                                          %% created Sockets (which are ports),
                                          %% ignore them, we are interested in
                                          %% processes only
                                          [Pid || Pid <- L, is_pid(Pid)]
                                  end,
                          [P | Links];
                      (_) ->
                          []
                  end, Acceptors),
            info_msg("Full list of processes expected to stop: ~p",
                     [AcceptorProcs]),
            catch Mod:close(LSocket),
            wait_for_acceptors(AcceptorProcs, Mod, ?TERMINATE_ACCEPTOR_TIMEOUT),
            NewConnections =
                lists:map(
                  fun (#con{mod = CM, pid = undefined} = Con) when CM == Mod ->
                          Con#con{pid = shutdown};
                      (Con) ->
                          Con
                  end, Connections),
            State#s{listeners = proplists:delete(Listener, Listeners),
                    acceptors = [{P, M} || {P, M} <- Acceptors, M =/= Listener],
                    connections = NewConnections};
        undefined ->
            info_msg("ignoring closing of ~p because listener is not started",
                     [Listener]),
            State
    end.

wait_for_acceptors(Acceptors, Mod, Timeout) ->
    MRefs = [{A, erlang:monitor(process, A)} || A <- Acceptors],
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    ok = do_wait_for_acceptors(Mod, Deadline, MRefs).

do_wait_for_acceptors(_Mod, _Deadline, []) -> ok;
do_wait_for_acceptors(Mod, Deadline, [{A, MRef} | MRefTail]) when is_pid(A) ->
    Timeout = max(Deadline - erlang:monotonic_time(millisecond), 0),
    {AFamily, EncryptionEnabled} = proto2netsettings(Mod),
    Protocol = case EncryptionEnabled of
                   true -> tls;
                   false -> tcp
               end,
    receive
        {'DOWN', MRef, process, Pid, _Reason} ->
            info_msg("Down from ~p", [Pid]),
            do_wait_for_acceptors(Mod, Deadline, MRefTail);
        {accept, AcceptorSpawn, _, AFamily, Protocol} ->
            info_msg("Received accept from ~p/~p acceptor that is being shut "
                     "down, will reply with unsupported_protocol",
                     [AFamily, Protocol]),
            AcceptorSpawn ! {self(), unsupported_protocol},
            do_wait_for_acceptors(Mod, Deadline, [{A, MRef} | MRefTail])
    after
        Timeout ->
            error_msg("Wait for acceptor: ~p timed out", [A]),
            exit(A, kill),
            receive
                {'DOWN', MRef, process, _, _} -> ok
            after 60000 ->
                 exit(must_not_happen)
            end,
            ?flush({accept, A, _, _, _}),
            do_wait_for_acceptors(Mod, Deadline, MRefTail)
    end.

listen_proto({AddrType, Module}, NodeName) ->
    NameStr = atom_to_list(NodeName),
    Port = cb_epmd:port_for_node(Module, NameStr),
    info_msg("Starting ~p listener on ~p...", [{AddrType, Module}, Port]),
    ListenAddr = get_listen_addr(AddrType, Module),
    ListenFun =
        fun () ->
                case Module:listen(NodeName, ListenAddr) of
                    {ok, _} = Res ->
                        info_msg("Started listener: ~p", [Module]),
                        maybe_register_on_epmd(Module, NodeName, Port),
                        Res;
                    Error -> Error
                end
        end,
    case with_dist_ip_and_port(ListenAddr, Port, ListenFun) of
        {ok, Res} -> {ok, Res};
        ignore ->
            info_msg("Ignoring starting dist ~p on port ~p", [Module, Port]),
            ignore;
        Error ->
            error_msg("Failed to start dist ~p on port ~p with reason: ~p",
                      [Module, Port, Error]),
            Error
    end.

get_listen_addr(AddrType, Module) ->
    AFamily = proto_to_family(Module),
    IPStr = case AddrType of
                local -> misc:localhost(AFamily, []);
                external -> misc:inaddr_any(AFamily, [])
            end,
    {ok, IPParsed} = inet:parse_address(IPStr),
    IPParsed.

%% Backward compat: we need to register ns_server non tls port on epmd to allow
%% old nodes to find this node
%% This code can be dropped if upgrade from Alice is not supported
maybe_register_on_epmd(Module, NodeName, PortNo)
  when Module =:= inet_tcp_dist;
       Module =:= inet6_tcp_dist ->
    NameStr = atom_to_list(NodeName),
    case cb_epmd:node_type(NameStr) of
        ns_server ->
            Family = proto_to_family(Module),
            case erl_epmd:register_node(NameStr, PortNo, Family) of
                {ok, _} ->
                    ok;
                {error, already_registered} ->
                    ok;
                Error ->
                    info_msg("Failed to register ~p with epmd. Reason ~p",
                             [{NodeName, PortNo, Family}, Error]),
                    Error
            end;
        _ ->
            ok
    end;
maybe_register_on_epmd(_Module, _NodeName, _PortNo) ->
    ok.

can_add_proto({_AddrType, Proto}, #s{listeners = Listeners}) ->
    case is_valid_protocol(Proto) of
        true ->
            case lists:member(Proto, [P || {{_, P}, _} <- Listeners]) of
                false ->
                    ok;
                true ->
                    {error, already_enabled}
            end;
        false ->
            {error, invalid_protocol}
    end.

is_valid_protocol(P) ->
    lists:member(P, [inet_tcp_dist, inet6_tcp_dist, inet_tls_dist,
                     inet6_tls_dist]).

conf(Prop, Conf) ->
    %% See comments for how we determine defaults.
    proplists:get_value(Prop, Conf, proplists:get_value(Prop, defaults(Conf))).

%% From 6.6.4,
%% 1. we do not start both [inet_tcp_dist, inet6_tcp_dist] protos for
%% local_listeners and external_listeners by default as we introduced the
%% address family only feature.
%% 2. we write the local_listeners to the dist_cfg file.
%%
%% In cases, like on upgrade from pre 7.0 we don't find local_listeners in the
%% dist_cfg and we always need to start the preferred proto for local and
%% external. Therefore, the defaults should reflect that for listeners.
defaults(Conf) ->
    [{preferred_external_proto, inet_tcp_dist},
     {preferred_local_proto, inet_tcp_dist},
     {local_listeners, [proplists:get_value(preferred_local_proto, Conf,
                                            inet_tcp_dist)]},
     {external_listeners, [proplists:get_value(preferred_external_proto, Conf,
                                               inet_tcp_dist)]}].

transform_old_to_new_config(Dist) ->
    DistType = list_to_atom((atom_to_list(Dist) ++ "_dist")),
    true = is_valid_protocol(DistType),
    [{preferred_external_proto, DistType},
     {preferred_local_proto, DistType}].

read_config(File, IgnoreReadError) ->
    case read_terms_from_file(File) of
        {ok, {dist_type, Dist}} ->
            transform_old_to_new_config(Dist);
        {ok, {dist_type, _, Dist}} ->
            transform_old_to_new_config(Dist);
        {ok, Val} ->
            Val;
        {error, read_error} when IgnoreReadError ->
            [];
        {error, Reason} ->
            error_msg("Can't read cb_dist config file ~p: ~p", [File, Reason]),
            erlang:error({invalid_cb_dist_config, File, Reason})
    end.

%% can't use file:consult here because file server might not be started
%% by the moment we need to read config
read_terms_from_file(F) ->
    case erl_prim_loader:get_file(F) of
        %% Backward compat: pre-6.5 installer creates empty dist_cfg file
        {ok, <<>>, _} -> {ok, []};
        {ok, Bin, _} ->
            try {ok, misc:parse_term(Bin)}
            catch
                _:_ -> {error, invalid_format}
            end;
        error -> {error, read_error}
    end.

handle_reload_config(State) ->
    try read_config(config_path(), true) of
        Cfg ->
            info_msg("Reloading configuration:~n~p", [Cfg]),
            NewState = ensure_config(State#s{config = Cfg}),
            case not_started_required_listeners(NewState) of
                [] ->
                    #s{listeners = Listeners} = NewState,
                    L = [proto2netsettings(M) || {{_, M}, _} <- Listeners],
                    {reply, {ok, L}, NewState};
                NotStartedRequired ->
                    error_msg("Failed to start required dist listeners ~p",
                              [NotStartedRequired]),
                    {reply, {error, {not_started, NotStartedRequired}},
                     NewState}
            end
    catch
        _:Error -> {reply, {error, Error}, State}
    end.

not_started_required_listeners(State) ->
    Current = [M || {M, _} <- State#s.listeners],
    Required = [R || R <- get_required_protos(State),
                     lists:member(R, get_protos(State))],
    Required -- Current.

ensure_config(#s{listeners = Listeners} = State) ->
    CurrentProtos = [M || {M, _} <- Listeners],
    NewProtos = get_protos(State),
    ToAdd = NewProtos -- CurrentProtos,
    ToRemove = CurrentProtos -- NewProtos,
    State2 = lists:foldl(fun (P, S) -> remove_proto(P, S) end,
                         State, ToRemove),
    State3 = lists:foldl(fun (P, S) -> add_proto(P, S) end,
                         State2, ToAdd),
    State3.

get_protos(#s{name = Name, config = Config}) ->
    case cb_epmd:is_local_node(Name) of
        true ->
            [{local, P} || P <- lists:usort(conf(local_listeners, Config))];
        false ->
            External = lists:usort(conf(external_listeners, Config)),
            Local = lists:usort(conf(local_listeners, Config)),
            OnlyLocal = Local -- External,
            [{local, P} || P <- OnlyLocal] ++
            [{external, P} || P <- External]
    end.

get_required_protos(#s{name = Name, config = Config}) ->
    Local = conf(preferred_local_proto, Config),
    Ext = conf(preferred_external_proto, Config),
    case {cb_epmd:node_type(atom_to_list(Name)), cb_epmd:is_local_node(Name)} of
        {executioner, _} -> [];
        {_, true} -> [{local, Local}];
        {_, false} ->
            case Ext == Local of
                true -> [{external, Ext}];
                false -> [{local, Local}, {external, Ext}]
            end
    end.

info_msg(F, A) ->
    %% Not using kernel logger here to prevent showing those messages on
    %% console during service start
    %% On babysitter cb_dist starts before ale so we simply ignore
    %% logs in this case
    try
        ale:debug(ns_server, "cb_dist: " ++ F, A)
    catch
        error:undef -> ok
    end.
error_msg(F, A) ->
    %% Preformat the message, since the default kernel logger handler
    %% doesn't format it even when error_msg/2 is used
    Msg = lists:flatten(io_lib:format("cb_dist: " ++ F, A)),
    ?LOG_ERROR(Msg).

proto_to_family(inet_tcp_dist) -> inet;
proto_to_family(inet_tls_dist) -> inet;
proto_to_family(inet6_tcp_dist) -> inet6;
proto_to_family(inet6_tls_dist) -> inet6.

proto_to_encryption(inet_tcp_dist) -> false;
proto_to_encryption(inet_tls_dist) -> true;
proto_to_encryption(inet6_tcp_dist) -> false;
proto_to_encryption(inet6_tls_dist) -> true.

validate_config_file(CfgFile) ->
    try
        Cfg = read_config(CfgFile, false),
        is_list(Cfg) orelse throw(not_list),
        ([E || {_, _} = E <- Cfg] == Cfg) orelse throw(not_proplist),
        Unknown = proplists:get_keys(Cfg) -- proplists:get_keys(defaults(Cfg)),
        (Unknown == []) orelse throw({unknown_props, Unknown}),
        ExtPreferred = conf(preferred_external_proto, Cfg),
        is_valid_protocol(ExtPreferred)
            orelse throw(invalid_preferred_external_proto),
        LocalPreferred = conf(preferred_local_proto, Cfg),
        is_valid_protocol(LocalPreferred)
            orelse throw(invalid_preferred_local_proto),
        LocalListeners = conf(local_listeners, Cfg),
        ExternalListeners = conf(external_listeners, Cfg),
        Invalid = [L || L <- LocalListeners, not is_valid_protocol(L)] ++
                  [L || L <- ExternalListeners, not is_valid_protocol(L)],
        (Invalid == []) orelse throw({invalid_listeners, Invalid}),
        length(lists:usort(LocalListeners)) == length(LocalListeners)
            orelse throw(not_unique_listeners),
        length(lists:usort(ExternalListeners)) == length(ExternalListeners)
            orelse throw(not_unique_listeners),
        lists:member(ExtPreferred, ExternalListeners)
            orelse throw({missing_preferred_external_listener, ExtPreferred}),
        lists:member(LocalPreferred, LocalListeners)
            orelse throw({missing_preferred_local_listener, LocalPreferred}),
        ok
    catch
        _:E -> {error, E}
    end.

import_props_to_config(Props, Cfg) ->
    CurListeners = proplists:get_value(external_listeners, Cfg),
    CurAFamily = proto_to_family(conf(preferred_external_proto, Cfg)),
    CurNEncr = proto_to_encryption(conf(preferred_external_proto, Cfg)),
    NewAFamily = proplists:get_value(afamily, Props, CurAFamily),
    NewNEncr = proplists:get_value(nodeEncryption, Props, CurNEncr),
    PrefExt = netsettings2proto({NewAFamily, NewNEncr}),
    PrefLocal = netsettings2proto({NewAFamily, false}),
    Listeners =
        case proplists:get_value(externalListeners, Props) of
            undefined -> CurListeners;
            L -> [netsettings2proto(E) || E <- L]
        end,
    [{external_listeners, Listeners} || Listeners =/= undefined] ++
    [{local_listeners, [PrefLocal]},
     {preferred_external_proto, PrefExt},
     {preferred_local_proto, PrefLocal}].

store_config(DCfgFile, DCfg) ->
    DirName = filename:dirname(DCfgFile),
    FileName = filename:basename(DCfgFile),
    TmpPath = path_config:tempfile(DirName, FileName, ".tmp"),
    Data = io_lib:format("~p.~n", [DCfg]),
    try
        case misc:write_file(TmpPath, Data) of
            ok ->
                case validate_config_file(TmpPath) of
                    ok -> misc:atomic_rename(TmpPath, DCfgFile);
                    Y -> Y
                end;
            X ->
                X
        end
    after
        (catch file:delete(TmpPath))
    end.

store_config(Cfg) ->
    CfgFile = cb_dist:config_path(),
    case store_config(CfgFile, Cfg) of
        ok ->
            info_msg("Updated cb_dist config ~p:~n~p", [CfgFile, Cfg]),
            ok;
        {error, Reason} ->
            error_msg("Failed to save cb_dist config to ~p with reason: ~p",
                      [CfgFile, Reason]),
            {error, Reason}
    end.

format_error({not_started, Protocols}) ->
    PS = string:join([proto2str(P) || {_, P} <- Protocols], ", "),
    io_lib:format("Failed to start the following required listeners: ~p", [PS]);
format_error({invalid_cb_dist_config, File, invalid_format}) ->
    io_lib:format("Invalid format of cb_dist config file (~s)", [File]);
format_error({invalid_cb_dist_config, File, read_error}) ->
    io_lib:format("Can't read cb_dist config file ~s", [File]);
format_error({invalid_cb_dist_config, File, Reason}) ->
    io_lib:format("Can't read cb_dist config file ~s (~p)", [File, Reason]);
format_error({missing_preferred_external_listener, Proto}) ->
    io_lib:format("Missing ~s listener (needed for external communication)",
                  [proto2str(Proto)]);
format_error({missing_preferred_local_listener, Proto}) ->
    io_lib:format("Missing ~s listener (needed for local communication)",
                  [proto2str(Proto)]);
format_error(Unknown) ->
    io_lib:format("~p", [Unknown]).

netsettings2str(S) -> proto2str(netsettings2proto(S)).

proto2str(inet_tcp_dist) -> "TCP-IPv4";
proto2str(inet_tls_dist) -> "TLS-IPv4";
proto2str(inet6_tcp_dist) -> "TCP-IPv6";
proto2str(inet6_tls_dist) -> "TLS-IPv6".

netsettings2proto({inet, false}) -> inet_tcp_dist;
netsettings2proto({inet, true}) -> inet_tls_dist;
netsettings2proto({inet6, false}) -> inet6_tcp_dist;
netsettings2proto({inet6, true}) -> inet6_tls_dist.

proto2netsettings(inet_tcp_dist) -> {inet, false};
proto2netsettings(inet6_tcp_dist) -> {inet6, false};
proto2netsettings(inet_tls_dist) -> {inet, true};
proto2netsettings(inet6_tls_dist) -> {inet6, true}.

with_registered_connection(Fun, Module) ->
    Ref = gen_server:call(?MODULE, {register_connection, Module}, infinity),
    try Fun() of
        Pid ->
            gen_server:call(?MODULE, {update_connection_pid, Ref, Pid},
                            infinity),
            Pid
    catch
        C:E:ST ->
            gen_server:call(?MODULE, {update_connection_pid, Ref, undefined},
                            infinity),
            erlang:raise(C, E, ST)
    end.

update_connection_pid(Ref, Pid, #s{connections = Connections} = State) ->
    case lists:keytake(Ref, #con.ref, Connections) of
        {value, Con, Rest} when Pid =:= undefined ->
            info_msg("Removed connection: ~p", [Con]),
            State#s{connections = Rest};
        {value, #con{pid = shutdown}, Rest} ->
            info_msg("Closing connection ~p because acceptor is dead", [Pid]),
            %% No point in using close_dist_connection here as {KernelPid,
            %% disconnect} message is only useful after we forward the
            %% controller message to AcceptorPid(which no longer exists), and it
            %% in turn sends another controller message to ConPid to proceed
            %% accepting connection.
            force_close_dist_connection(Pid),
            State#s{connections = Rest};
        {value, Con, Rest} ->
            MonRef = erlang:monitor(process, Pid),
            Con2 = Con#con{pid = Pid, mon = MonRef},
            info_msg("Updated connection: ~p", [Con2]),
            State#s{connections = [Con2|Rest]};
        false ->
            error_msg("Connection not found: ~p", [Ref]),
            State
    end.

close_dist_connection(MonRef, Pid, KernelPid) ->
    catch erlang:demonitor(MonRef, [flush]),
    Pid ! {KernelPid, disconnect},
    case misc:wait_for_process(Pid, ?TERMINATE_TIMEOUT) of
        ok -> ok;
        {error, Reason} ->
            error_msg("Close connection ~p error: ~p", [Pid, Reason]),
            force_close_dist_connection(Pid)
    end.

force_close_dist_connection(ConPid) ->
    exit(ConPid, kill).

is_restartable_event({{badmatch, {error, closed}}, _}) ->
    %% One occurence of this intermittent error is when Pid from
    %% accept_connection in inet_tls_dist crashes before we make it the
    %% controlling process.
    true;
is_restartable_event({error, closed}) ->
    true;
is_restartable_event({error, timeout}) ->
    true;
is_restartable_event({error, {tls_alert, {_, _}}}) ->
    true;
%% Passphrase doesn't match pkey or missing passphrase
%% We shouldn't crash in this case, just restart the listener (we try to update
%% the passphrase on every listener start)
is_restartable_event({error, {options, {keyfile, _, _}}}) ->
    true;
is_restartable_event(_) ->
    false.

is_pkey_encrypted() ->
    try ets:lookup(ssl_dist_opts, server) of
        [{server, TLSOpts}] ->
            case proplists:get_value(keyfile, TLSOpts) of
                undefined -> false;
                File ->
                    case erl_prim_loader:get_file(File) of
                        {ok, Pem, _} ->
                            case public_key:pem_decode(Pem) of
                                [{_, _, not_encrypted}] -> false;
                                [{_, _, _}] -> true
                            end;
                        error -> false
                    end
            end
    catch
        error:badarg ->
            info_msg("TLS dist is not available", []),
            false
    end.

extract_pkey_passphrase() ->
    case application:get_env(cb_dist_pkey_pass_mfa) of
        undefined ->
            error_msg("Missing cb_dist_pkey_pass_mfa env", []),
            {error, not_available};
        {ok, {M, F, A}} ->
            try erlang:apply(M, F, A) of
                PassFun when is_function(PassFun) ->
                    info_msg("Successfully extracted pkey passphrase", []),
                    {ok, PassFun}
            catch
                _:_ -> {error, not_available}
            end
    end.
