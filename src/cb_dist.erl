%% @author Couchbase <info@couchbase.com>
%% @copyright 2019 Couchbase, Inc.
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
%%

%% @doc Wraper for *_dist modules. Allows to start/stop listeners dynamically.
-module(cb_dist).

-behaviour(gen_server).

-include_lib("kernel/include/net_address.hrl").
-include_lib("kernel/include/dist_util.hrl").
-include("cut.hrl").

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
         restart_tls/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(con, {ref :: reference(),
              mod :: protocol(),
              pid :: pid() | undefined | shutdown,
              mon :: reference() | undefined}).

-record(s, {listeners = [],
            acceptors = [],
            creation = undefined,
            kernel_pid = undefined,
            config = undefined,
            name = undefined,
            ensure_config_timer = undefined,
            connections = [] :: [#con{}]}).

-define(family, ?MODULE).
-define(proto, ?MODULE).
-define(TERMINATE_TIMEOUT, 5000).
-define(ENSURE_CONFIG_TIMEOUT, 60000).

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
          Creation :: pos_integer()}}.
listen(Name) when is_atom(Name) ->
    Pid = whereis(?MODULE),
    case gen_server:call(Pid, {listen, Name}, infinity) of
        {ok, Creation} ->
            Addr = #net_address{address = undefined,
                                host = undefined,
                                protocol = ?family,
                                family = ?proto},
            {ok, {Pid, Addr, Creation}};
        {error, _} = Error ->
            Error
    end.

-spec accept(LSocket :: any()) -> AcceptorPid :: pid().
accept(_LSocket) ->
    gen_server:call(?MODULE, {accept, self()}, infinity).

-spec accept_connection(CBDistPid :: pid(),
                        Acceptor :: {reference(), pid(), socket()},
                        MyNode :: atom(),
                        Allowed :: any(),
                        SetupTime :: any()) ->
                            {ConRef :: reference(),
                             ConPid :: pid(),
                             AcceptorPid :: pid()}.
accept_connection(_, {ConRef, AcceptorPid, ConnectionSocket}, MyNode, Allowed,
                  SetupTime) ->
    Module = gen_server:call(?MODULE, {get_module_by_acceptor, AcceptorPid},
                             infinity),
    info_msg("Accepting connection from acceptor ~p using module ~p",
             [AcceptorPid, Module]),
    case Module =/= undefined of
        true ->
            ConPid = Module:accept_connection(AcceptorPid, ConnectionSocket,
                                              MyNode, Allowed, SetupTime),
            {ConRef, ConPid, AcceptorPid};
        false ->
            {ConRef,
             spawn_opt(
               fun () ->
                       error_msg("** Connection from unknown acceptor ~p, "
                                 "please reconnect ** ~n", [AcceptorPid]),
                       ?shutdown(no_node)
               end, [link]), AcceptorPid}
    end.

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
    {ok, #s{config = Config, creation = rand:uniform(4) - 1}}.

handle_call({listen, Name}, _From, #s{creation = Creation} = State) ->
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
        [] -> {reply, {ok, Creation}, State2};
        _ ->
            error_msg("Failed to start required dist listeners ~p. "
                      "Net kernel will not start", [NotStartedRequired]),
            close_listeners(State2),
            {reply, {error, {not_started, NotStartedRequired}}, State}
    end;

handle_call({accept, KernelPid}, _From, #s{listeners = Listeners} = State) ->
    Acceptors =
        lists:map(
            fun ({{_AType, Module} = Listener, {LSocket, _Addr, _Creation}}) ->
                {Module:accept(LSocket), Listener}
            end,
            Listeners),
    {reply, self(), ensure_config(State#s{acceptors = Acceptors,
                                          kernel_pid = KernelPid})};

handle_call({get_module_by_acceptor, AcceptorPid}, _From,
            #s{acceptors = Acceptors} = State) ->
    {_, Module} = proplists:get_value(AcceptorPid, Acceptors),
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
        C:E ->
            {reply, {exception, {C, E, erlang:get_stacktrace()}}, State}
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
        lists:filtermap(
          fun (#con{mod = Mod, pid = Pid, mon = Mon} = Con) ->
                  case proto2netsettings(Mod) of
                      {_, true = _Encryption} ->
                          case Pid of
                              undefined ->
                                  {true, Con#con{pid = shutdown}};
                              shutdown ->
                                  {true, Con};
                              _ ->
                                  info_msg("Closing connection ~p because of "
                                           "tls restart", [Pid]),
                                  catch erlang:demonitor(Mon, [flush]),
                                  close_dist_connection(Pid, KernelPid),
                                  false
                          end;
                      {_, _} ->
                          true
                  end
          end, Connections),

    gen_server:call(
      ssl_pem_cache:name(dist),
      {unconditionally_clear_pem_cache, self()},
      infinity),

    {reply, ok, ensure_config(NewState#s{connections = NewConnections})};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({accept, AcceptorPid, ConSocket, _Family, _Protocol},
            #s{kernel_pid = KernelPid,
               connections = Connections,
               acceptors = Acceptors} = State) ->
    Ref = make_ref(),
    {_, Module} = proplists:get_value(AcceptorPid, Acceptors),
    Con = #con{ref = Ref, mod = Module},
    info_msg("Accepted new connection from ~p DistCtrl ~p: ~p",
             [AcceptorPid, ConSocket, Con]),
    KernelPid ! {accept, self(), {Ref, AcceptorPid, ConSocket}, ?family, ?proto},
    {noreply, State#s{connections = [Con | Connections]}};

handle_info({KernelPid, controller, {ConRef, ConPid, AcceptorPid}},
            #s{kernel_pid = KernelPid} = State) ->
    AcceptorPid ! {self(), controller, ConPid},
    {noreply, update_connection_pid(ConRef, ConPid, State)};

handle_info({'EXIT', Kernel, Reason}, State = #s{kernel_pid = Kernel}) ->
    error_msg("received EXIT from kernel, stoping: ~p", [Reason]),
    {stop, Reason, State};

handle_info({'EXIT', From, Reason}, State) ->
    error_msg("received EXIT from ~p, stoping: ~p", [From, Reason]),
    {stop, {'EXIT', From, Reason}, State};

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

add_proto({_AddrType, Mod} = Listener,
          #s{name = NodeName, listeners = Listeners,
             acceptors = Acceptors} = State) ->
    case can_add_proto(Listener, State) of
        ok ->
            case listen_proto(Listener, NodeName) of
                {ok, L = {LSocket, _, _}} ->
                    try
                        APid = Mod:accept(LSocket),
                        true = is_pid(APid),
                        State#s{listeners = [{Listener, L}|Listeners],
                                acceptors = [{APid, Listener}|Acceptors]}
                    catch
                        _:E ->
                            ST = erlang:get_stacktrace(),
                            catch Mod:close(LSocket),
                            error_msg(
                              "Accept failed for protocol ~p with reason: ~p~n"
                              "Stacktrace: ~p", [Listener, E, ST]),
                            start_ensure_config_timer(State)
                    end;
                {error, eafnosupport} -> State;
                {error, eprotonosupport} -> State;
                ignore -> State;
                _Error -> start_ensure_config_timer(State)
            end;
        {error, Reason} ->
            error_msg("Ignoring ~p listener, reason: ~p", [Listener, Reason]),
            State
    end.

start_ensure_config_timer(#s{ensure_config_timer = undefined} = State) ->
    Ref = erlang:send_after(?ENSURE_CONFIG_TIMEOUT, self(),
                            ensure_config_timer),
    State#s{ensure_config_timer = Ref};
start_ensure_config_timer(#s{} = State) ->
    State.

remove_proto({_AddrType, Mod} = Listener,
             #s{listeners = Listeners, acceptors = Acceptors} = State) ->
    case proplists:get_value(Listener, Listeners) of
        {LSocket, _, _} ->
            info_msg("Closing listener ~p", [Listener]),
            [erlang:unlink(P) || {P, M} <- Acceptors, M =:= Listener],
            ToWait =
                case lists:member(Mod, [inet_tls_dist, inet6_tls_dist]) of
                    true ->
                        %% Extract real acceptors from tls proxy in order
                        %% to be able to wait for those processes, so the next
                        %% acceptor doesn't get eaddrinuse error
                        {TLSSockets, TLSAcceptors} = extract_tls_acceptors(),
                        %% *_tls_dist modules don't close proxy socket when
                        %% Mod:close/1 is called, so we have to restart proxy
                        %% process to make sure that those sockets are closed
                        supervisor:terminate_child(ssl_dist_sup,
                                                   ssl_tls_dist_proxy),
                        supervisor:restart_child(ssl_dist_sup,
                                                 ssl_tls_dist_proxy),
                        [catch gen_tcp:close(S) || S <- TLSSockets],
                        TLSAcceptors;
                    false ->
                        catch Mod:close(LSocket),
                        []
                end,
            lists:foreach(
              fun (Proc) ->
                      case misc:wait_for_process(Proc, ?TERMINATE_TIMEOUT) of
                          ok -> ok;
                          {error, Reason} ->
                              error_msg("Wait for acceptor: ~p failed with "
                                        "reason: ~p", [Proc, Reason]),
                              exit(Proc, kill)
                      end,
                      %% Since we killed the acceptor flush the accept messages
                      %% from it.
                      flush_accept_messages(Proc)
              end, lists:usort([P || {P, M} <- Acceptors, M =:= Listener] ++
                              ToWait)),

            State#s{listeners = proplists:delete(Listener, Listeners),
                    acceptors = [{P, M} || {P, M} <- Acceptors, M =/= Listener]};
        undefined ->
            info_msg("ignoring closing of ~p because listener is not started",
                     [Listener]),
            State
    end.

extract_tls_acceptors() ->
    case sys:get_state(ssl_tls_dist_proxy, 60000) of
        {state, undefined, undefined} ->
            {[], []};
        {state, {LocalSocket, WorldSocket}, undefined} ->
            {[LocalSocket, WorldSocket], []};
        {state, {LocalSocket, WorldSocket}, {LocalCon, WorldCon}} ->
            {[LocalSocket, WorldSocket], [LocalCon, WorldCon]}
    end.

flush_accept_messages(AcceptorPid) ->
    receive
        {accept, AcceptorPid, _, _, _} = Msg ->
            info_msg("Ignoring message from acceptor ~p", [Msg]),
            flush_accept_messages(AcceptorPid)
    after
        0 ->
            ok
    end.

listen_proto({AddrType, Module}, NodeName) ->
    NameStr = atom_to_list(NodeName),
    Port = cb_epmd:port_for_node(Module, NameStr),
    info_msg("Starting ~p listener on ~p...", [Module, Port]),
    ListenFun =
        fun () ->
                case Module:listen(NodeName) of
                    {ok, _} = Res ->
                        case maybe_register_on_epmd(Module, NodeName, Port) of
                            ok -> Res;
                            {error, _} = Error -> Error
                        end;
                    Error -> Error
                end
        end,
    ListenAddr = get_listen_addr(AddrType, Module),
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
    case cb_epmd:node_type(atom_to_list(NodeName)) of
        ns_server ->
            Family = proto_to_family(Module),
            case erl_epmd:register_node(NodeName, PortNo, Family) of
                {ok, _} -> ok;
                {error, already_registered} -> ok;
                Error -> Error
            end;
        _ ->
            ok
    end;
maybe_register_on_epmd(_Module, _NodeName, _PortNo) ->
    ok.

can_add_proto({_AddrType, Proto}, #s{listeners = Listeners}) ->
    case is_valid_protocol(Proto) of
        true ->
            Protos = [P || {{_, P}, _} <- Listeners],
            case lists:member(Proto, Protos) of
                false ->
                    HasInet6Tls = lists:member(inet6_tls_dist, Protos),
                    HasInetTls = lists:member(inet_tls_dist, Protos),
                    case Proto of
                        inet6_tls_dist when HasInetTls ->
                            {error, {already_has, inet_tls_dist}};
                        inet_tls_dist when HasInet6Tls ->
                            {error, {already_has, inet6_tls_dist}};
                        _ ->
                            ok
                    end;
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
    proplists:get_value(Prop, Conf, proplists:get_value(Prop, defaults())).

defaults() ->
    [{preferred_external_proto, inet_tcp_dist},
     {preferred_local_proto, inet_tcp_dist},
     {local_listeners, [inet_tcp_dist]},
     {external_listeners, [inet_tcp_dist]}].

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
            info_msg("Reloading configuration: ~p", [Cfg]),
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
    %% Not using error_logger here to prevent showing those messages on
    %% console during service start
    %% On babysitter cb_dist starts before ale so we simply ignore
    %% logs in this case
    try
        ale:debug(ns_server, "cb_dist: " ++ F, A)
    catch
        error:undef -> ok
    end.
error_msg(F, A) ->
    %% Preformat the message, since the default error_logger handler
    %% doesn't format it even when error_msg/2 is used
    Msg = lists:flatten(io_lib:format("cb_dist: " ++ F, A)),
    error_logger:error_msg(Msg).

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
        Unknown = proplists:get_keys(Cfg) -- proplists:get_keys(defaults()),
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
        AllListeners = ExternalListeners ++ LocalListeners,
        (lists:member(inet_tls_dist, AllListeners) and
         lists:member(inet6_tls_dist, AllListeners))
            andalso throw(ipv4_and_ipv6_tls),
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
            info_msg("Updated cb_dist config ~p: ~p", [CfgFile, Cfg]),
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
format_error(ipv4_and_ipv6_tls) ->
    "Can't change address family while node-to-node encryption is enabled";
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
        C:E ->
            ST = erlang:get_stacktrace(),
            gen_server:call(?MODULE, {update_connection_pid, Ref, undefined},
                            infinity),
            erlang:raise(C, E, ST)
    end.

update_connection_pid(Ref, Pid, #s{connections = Connections,
                                   kernel_pid = KernelPid} = State) ->
    case lists:keytake(Ref, #con.ref, Connections) of
        {value, Con, Rest} when Pid =:= undefined ->
            info_msg("Removed connection: ~p", [Con]),
            State#s{connections = Rest};
        {value, #con{pid = shutdown}, Rest} ->
            info_msg("Closing connection ~p because of tls restart", [Pid]),
            close_dist_connection(Pid, KernelPid),
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

close_dist_connection(Pid, KernelPid) ->
    Pid ! {KernelPid, disconnect},
    case misc:wait_for_process(Pid, ?TERMINATE_TIMEOUT) of
        ok -> ok;
        {error, Reason} ->
            error_msg("Close connection ~p error: ~p", [Pid, Reason]),
            exit(Pid, kill)
    end.
