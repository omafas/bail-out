%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(ns_ports_setup).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start/0, setup_body_tramp/0,
         restart_memcached/0,
         sync/0, create_erl_node_spec/4,
         shutdown_ports/0, build_cbauth_env_vars/2, get_rpc_prefix/1,
         build_cbauth_revrpc_url/2, remote_build_cbauth_revrpc_url/1]).

start() ->
    proc_lib:start_link(?MODULE, setup_body_tramp, []).

sync() ->
    gen_server:call(?MODULE, sync, infinity).

shutdown_ports() ->
    gen_server:call(?MODULE, shutdown_ports, infinity).

setup_body_tramp() ->
    misc:delaying_crash(1000, fun setup_body/0).

setup_body() ->
    Self = self(),
    erlang:register(?MODULE, Self),
    proc_lib:init_ack({ok, Self}),
    chronicle_compat_events:notify_if_key_changes(
      %% config changes for other nodes is quite obviously irrelevant
      fun ({node, N, _}) when N =/= node() ->
              false;
          (Key) ->
              not (ns_bucket:buckets_change(Key) orelse
                   ns_bucket:names_change(Key) orelse
                   collections:change(Key))
      end, check_children_update),
    ns_pubsub:subscribe_link(user_storage_events,
                             fun (_) ->
                                     Self ! check_children_update
                             end),
    Children = dynamic_children(normal),
    set_children_and_loop(Children, undefined, normal).

restart_memcached() ->
    {ok, _} = ns_ports_manager:restart_port_by_name(
                ns_server:get_babysitter_node(), kv),
    ok.

set_children(Children, Sup) ->
    Pid = ns_ports_manager:set_dynamic_children(ns_server:get_babysitter_node(), Children),
    case Sup of
        undefined ->
            {is_pid, true, Pid} = {is_pid, erlang:is_pid(Pid), Pid},
            ?log_debug("Monitor ns_child_ports_sup ~p", [Pid]),
            remote_monitors:monitor(Pid);
        Pid ->
            ok;
        _ ->
            ?log_debug("ns_child_ports_sup was restarted on babysitter node. Exit. Old pid = ~p, new pid = ~p",
                       [Sup, Pid]),
            erlang:error(child_ports_sup_died)
    end,
    Pid.

set_children_and_loop(Children, Sup, Status) ->
    NewSup = set_children(Children, Sup),
    children_loop(Children, NewSup, Status).

children_loop(Children, Sup, Status) ->
    proc_lib:hibernate(erlang, apply, [fun children_loop_continue/3, [Children, Sup, Status]]).

children_loop_continue(Children, Sup, Status) ->
    receive
        {'$gen_call', From, shutdown_ports} ->
            ?log_debug("Send shutdown to all go ports"),
            NewStatus = shutdown,
            NewChildren = dynamic_children(NewStatus),
            NewSup = set_children(NewChildren, Sup),
            gen_server:reply(From, ok),
            children_loop(NewChildren, NewSup, NewStatus);
        check_children_update ->
            do_children_loop_continue(Children, Sup, Status);
        {'$gen_call', From, sync} ->
            gen_server:reply(From, ok),
            children_loop(Children, Sup, Status);
        {remote_monitor_down, Sup, unpaused} ->
            ?log_debug("Remote monitor ~p was unpaused after node name change. Restart loop.", [Sup]),
            set_children_and_loop(dynamic_children(Status), undefined, Status);
        {remote_monitor_down, Sup, Reason} ->
            ?log_debug("ns_child_ports_sup ~p died on babysitter node with ~p. Restart.", [Sup, Reason]),
            erlang:error({child_ports_sup_died, Sup, Reason});
        X ->
            erlang:error({unexpected_message, X})
    after 0 ->
            erlang:error(expected_some_message)
    end.

do_children_loop_continue(Children, Sup, Status) ->
    %% this sets bound on frequency of checking of port_servers
    %% configuration updates. NOTE: this thing also depends on other
    %% config variables. So we're forced to react on any config
    %% change
    timer:sleep(50),
    misc:flush(check_children_update),
    case dynamic_children(Status) of
        Children ->
            children_loop(Children, Sup, Status);
        NewChildren ->
            set_children_and_loop(NewChildren, Sup, Status)
    end.

create_erl_node_spec(Type, Args, EnvArgsVar, ErlangArgs) ->
    PathArgs = ["-pa"] ++ lists:reverse(code:get_path()),
    EnvArgsTail = [{K, V}
                   || {K, V} <- application:get_all_env(ns_server),
                      case atom_to_list(K) of
                          "error_logger_mf_dir" -> true;
                          "path_config" ++ _ -> true;
                          "dont_suppress_stderr_logger" -> true;
                          "loglevel_" ++ _ -> true;
                          "disk_sink_opts" -> true;
                          "ssl_ciphers" -> true;
                          "net_kernel_verbosity" -> true;
                          "config_path" -> true;
                          _ -> false
                      end],
    EnvArgs = Args ++ EnvArgsTail,

    AllArgs = PathArgs ++ ErlangArgs,

    ErlPath = filename:join([hd(proplists:get_value(root, init:get_arguments())),
                             "bin", "erl"]),

    Env0 = case os:getenv("ERL_CRASH_DUMP_BASE") of
               false ->
                   [];
               Base ->
                   [{"ERL_CRASH_DUMP", Base ++ "." ++ atom_to_list(Type)}]
           end,

    Env = [{EnvArgsVar, misc:inspect_term(EnvArgs)} | Env0],

    Cookie = atom_to_binary(erlang:get_cookie(ns_server:get_babysitter_node())),
    LogDS = ale:get_global_log_deks_snapshot(),
    LogDeks = base64:encode(term_to_binary(cb_crypto:get_all_deks(LogDS))),
    ?COOKIE_HEX_LEN = size(Cookie),
    Options0 = [use_stdio,
                {env, Env},
                        {write_data,
                         %% Whilst we could just pass the sensitive info
                         %% manually we would also end up logging it in SASL
                         %% progress reports et. al. and we don't want to
                         %% leak it in the logs so we instead pass as
                         %% function returning the info as a result to
                         %% avoid logging the sensitive info
                         fun() ->
                                 <<"COOKIE:", Cookie/binary, "\n", "LOGDEKS:",
                                   LogDeks/binary, "\n">>
                         end}],
    Options =
        case misc:get_env_default(dont_suppress_stderr_logger, false) of
            true ->
                [ns_server_no_stderr_to_stdout | Options0];
            false ->
                Options0
        end,

    {Type, ErlPath, AllArgs, Options}.

dynamic_children(Mode) ->
    Config = ns_config:get(),

    Specs = do_dynamic_children(Mode, Config),
    expand_specs(lists:flatten(Specs), Config).

do_dynamic_children(shutdown, Config) ->
    [memcached_spec(Config),
     saslauthd_port_spec()];
do_dynamic_children(normal, Config) ->
    Snapshot = ns_cluster_membership:get_snapshot(),
    [memcached_spec(Config),
     saslauthd_port_spec()] ++
        build_goport_specs(Config, Snapshot).

expand_specs(Specs, Config) ->
    [expand_args(S, Config) || S <- Specs].

find_executable(Name) ->
    K = list_to_atom("ns_ports_setup-" ++ Name ++ "-available"),
    case erlang:get(K) of
        undefined ->
            Cmd = path_config:component_path(bin, Name),
            RV = os:find_executable(Cmd),
            erlang:put(K, RV),
            RV;
        V ->
            V
    end.

build_go_service_env_vars(Service) when Service =:= indexer;
                                        Service =:= projector ->
    [{"GODEBUG", "madvdontneed=1"} | common_go_env_vars()];
build_go_service_env_vars(_) ->
    common_go_env_vars().

common_go_env_vars() ->
    GoTraceBack0 = ns_config:search(ns_config:latest(), gotraceback, <<"single">>),
    GoTraceBack = binary_to_list(GoTraceBack0),
    TraceBack = [{"GOTRACEBACK", GoTraceBack}],
    MaxProcs = case misc:read_cpu_count_env() of
                   {ok, N} ->
                       case os:getenv("GOMAXPROCS") of
                           Env when Env == false; Env == "" ->
                               [{"GOMAXPROCS", integer_to_list(N)}];
                           _ ->
                               []
                       end;
                   undefined -> []
               end,
    TraceBack ++ MaxProcs.

build_cbauth_env_vars(Config, RPCService) when RPCService =/= undefined ->
    [{"CBAUTH_REVRPC_URL",  build_cbauth_revrpc_url(Config, RPCService)}].

build_cbauth_dynamic_remote_env_vars(RPCService) ->
    [{"CBAUTH_REVRPC_URL",
     {dynamic, {?MODULE, remote_build_cbauth_revrpc_url, [RPCService]}}}].

remote_build_cbauth_revrpc_url(RPCService) ->
    %% Here we assume:
    %% 1. this function is running in babysitter
    %% 2. babysitter is connected to a single node only, and that node is
    %%    ns_server.
    %% We can't make ns_server nodename a parameter because it will lead
    %% to service restarts during node rename.
    true = is_pid(whereis(ns_child_ports_sup)),
    [NsServer] = nodes(hidden),
    rpc:call(NsServer, ?MODULE, build_cbauth_revrpc_url, [ns_config:latest(),
                                                          RPCService]).

build_cbauth_revrpc_url(Config, RPCService) when is_atom(RPCService) ->
    RPCServiceStr = atom_to_list(RPCService),
    build_cbauth_revrpc_url(Config, RPCServiceStr);
build_cbauth_revrpc_url(Config, RPCServiceStr) ->
    RestPort = service_ports:get_port(rest_port, Config),
    User = mochiweb_util:quote_plus(ns_config_auth:get_user(special)),
    Password = mochiweb_util:quote_plus(
                 ns_config_auth:get_password(node(), Config, special)),
    misc:local_url(RestPort, RPCServiceStr, [{user_info, {User, Password}}]).

expand_args({Name, Cmd, ArgsIn, OptsIn}, Config) ->
    %% Expand arguments
    Args0 = lists:map(fun ({Format, Keys}) ->
                              format(Config, Name, Format, Keys);
                          (X) -> X
                      end,
                      ArgsIn),
    Args = Args0 ++ ns_config:search(Config, {node, node(), {Name, extra_args}}, []),
    %% Expand environment variables within OptsIn
    Opts = lists:map(
             fun ({env, Env}) ->
                     {env, lists:map(
                             fun ({Var, {dynamic, MFA}}) ->
                                     {Var, {dynamic, MFA}};
                                 ({Var, {Format, Keys}}) ->
                                     {Var, format(Config, Name, Format, Keys)};
                                 (X) -> X
                             end, Env)};
                 (X) -> X
             end, OptsIn),
    {Name, Cmd, Args, Opts}.

format(Config, Name, Format, Keys) ->
    Values = lists:map(fun ({Module, FuncName, Args}) -> erlang:apply(Module, FuncName, Args);
                           ({Key, SubKey}) -> ns_config:search_node_prop(Config, Key, SubKey);
                           (Key) -> ns_config:search_node_prop(Config, Name, Key)
                       end, Keys),
    lists:flatten(io_lib:format(Format, Values)).

build_https_args(PortName, PortArg, CertArg, KeyArg, ClientCertArg,
                 ClientKeyArg, CAArg, Config) ->
    build_https_args(PortName, PortArg, "", CertArg, KeyArg, ClientCertArg,
                     ClientKeyArg, CAArg, Config).

build_https_args(PortName, PortArg, PortPrefix, CertArg, KeyArg, ClientCertArg,
                 ClientKeyArg, CAArg, Config) ->
    case service_ports:get_port(PortName, Config) of
        undefined ->
            [];
        Port ->
            ClientCertPath = ns_ssl_services_setup:chain_file_path(client_cert),
            ClientKeyPath = ns_ssl_services_setup:pkey_file_path(client_cert),
            [PortArg ++ "=" ++ PortPrefix ++ integer_to_list(Port),
             CertArg ++ "=" ++ ns_ssl_services_setup:chain_file_path(node_cert),
             KeyArg ++ "=" ++ ns_ssl_services_setup:pkey_file_path(node_cert),
             CAArg ++ "=" ++ ns_ssl_services_setup:ca_file_path(),
             ClientCertArg ++ "=" ++ ClientCertPath,
             ClientKeyArg ++ "=" ++ ClientKeyPath]
    end.

build_port_arg(ArgName, PortName, Config) ->
    build_port_arg(ArgName, "", PortName, Config).

build_port_arg(ArgName, PortPrefix, PortName, Config) ->
    Port = service_ports:get_port(PortName, Config),
    {PortName, true} = {PortName, Port =/= undefined},
    ArgName ++ "=" ++ PortPrefix ++ integer_to_list(Port).

build_port_args(Args, Config) ->
    [build_port_arg(ArgName, PortName, Config) || {ArgName, PortName} <- Args].

get_writable_subdir(DirType, SubDirName) ->
    ParentDir = case DirType of
                    idx ->
                        {ok, IdxDir} = ns_storage_conf:this_node_ixdir(),
                        IdxDir;
                    data ->
                        {ok, DataDir} = ns_storage_conf:this_node_dbdir(),
                        DataDir
                end,
    Dir = filename:join(ParentDir, SubDirName),
    case misc:ensure_writable_dir(Dir) of
        ok ->
            ok;
        _ ->
            ?log_debug("Directory ~p is not writable", [Dir])
    end,
    Dir.

-record(def, {exe, rpc, log}).

goport_defs() ->
    #{index => #def{exe = "indexer",
                    rpc = index,
                    log = ?INDEXER_LOG_FILENAME},
      projector => #def{exe = "projector",
                        rpc = projector,
                        log = ?PROJECTOR_LOG_FILENAME},
      goxdcr => #def{exe = "goxdcr",
                     rpc = goxdcr,
                     log = ?GOXDCR_LOG_FILENAME},
      n1ql => #def{exe = "cbq-engine",
                   rpc = 'cbq-engine',
                   log = ?QUERY_LOG_FILENAME},
      fts => #def{exe = "cbft",
                  rpc = fts,
                  log = ?FTS_LOG_FILENAME},
      cbas => #def{exe = "cbas",
                   rpc = cbas,
                   log = ?CBAS_LOG_FILENAME},
      eventing => #def{exe = "eventing-producer",
                       rpc = eventing,
                       log = ?EVENTING_LOG_FILENAME},
      backup => #def{exe = "backup",
                     rpc = backup,
                     log = ?BACKUP_LOG_FILENAME},
      cont_backup => #def{exe = "cbcontbk",
                          rpc = cbcontbk,
                          log = ?CONT_BACKUP_LOG_FILENAME}}.

get_rpc_prefix(Service) ->
    #def{rpc = RPCService} = maps:get(Service, goport_defs()),
    RPCService.

should_run(goxdcr, _Snapshot) ->
    cluster_compat_mode:is_goxdcr_enabled();
should_run(projector, Snapshot) ->
    ns_cluster_membership:should_run_service(Snapshot, kv, node()) andalso
        not config_profile:search({indexer, projector_disabled}, false);
should_run(cont_backup, Snapshot) ->
    cluster_compat_mode:is_enterprise() andalso
        ns_cluster_membership:should_run_service(Snapshot, kv, node());
should_run(Service, Snapshot) ->
    ns_cluster_membership:should_run_service(Snapshot, Service, node()).

build_goport_spec(Service, #def{exe = Executable,
                                rpc = RPCService,
                                log = Log}, Config, Snapshot) ->
    Cmd = find_executable(Executable),
    NodeUUID = ns_config:search(Config, {node, node(), uuid}, false),
    case Cmd =/= false andalso NodeUUID =/= false andalso
        should_run(Service, Snapshot) of
        false ->
            [];
        _ ->
            EnvVars = build_go_service_env_vars(Service) ++
                      build_cbauth_dynamic_remote_env_vars(RPCService),
            Args = goport_args(Service, Config, Cmd, binary_to_list(NodeUUID)),
            MaybeCgroups = cgroups:maybe_service_cgroup_path(Service),
            [{Service, Cmd, Args,
              [via_goport, exit_status, stderr_to_stdout, {env, EnvVars}] ++
                  [{log, Log} || Log =/= undefined] ++ MaybeCgroups}]
    end.

build_goport_specs(Config, Snapshot) ->
    [build_goport_spec(Service, Def, Config, Snapshot) ||
        {Service, Def} <- maps:to_list(goport_defs())].

net_to_afamily(inet) ->
    "ipv4";
net_to_afamily(inet6) ->
    "ipv6".

build_afamily_requirement(Prefix) ->
    [Prefix ++ net_to_afamily(AF) ++ "=" ++
     atom_to_list(Type) || {AF, Type} <- misc:address_family_requirement()].

goport_args(n1ql, Config, _Cmd, NodeUUID) ->
    RestPort = service_ports:get_port(rest_port, Config),
    DataStoreArg = "--datastore=" ++ misc:local_url(RestPort, []),
    CnfgStoreArg = "--configstore=" ++ misc:local_url(RestPort, []),
    HttpArg = build_port_arg("--http", ":", query_port, Config),
    EntArg = "--enterprise=" ++
        atom_to_list(cluster_compat_mode:is_enterprise()),
    DeploymentModelArg = "--deploymentModel=" ++ config_profile:name(),
    HttpsArgs = build_https_args(ssl_query_port, "--https", ":",
                                 "--certfile", "--keyfile",
                                 "--clientCertFile", "--clientKeyFile",
                                 "--cafile", Config),
    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),
    LogDirArg = "-logDir=" ++ LogDir,
    [DataStoreArg, HttpArg, CnfgStoreArg, EntArg, "-uuid=" ++ NodeUUID,
     DeploymentModelArg, LogDirArg] ++ build_afamily_requirement("--") ++
    HttpsArgs;

goport_args(projector, Config, _Cmd, _NodeUUID) ->
    %% Projector is a component that is required by 2i
    RestPort = service_ports:get_port(rest_port, Config),
    LocalMemcachedPort = service_ports:get_port(memcached_port, Config),
    MinidumpDir = path_config:minidump_dir(),
    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),

    ["--deploymentModel=" ++ config_profile:name()] ++
    build_https_args(projector_ssl_port, "--httpsPort", "--certFile",
                     "--keyFile", "--clientCertFile", "--clientKeyFile",
                     "--caFile", Config) ++
    build_afamily_requirement("-") ++
    ["-kvaddrs=" ++ misc:local_url(LocalMemcachedPort, [no_scheme]),
     build_port_arg("-adminport", ":", projector_port, Config),
     "-diagDir=" ++ MinidumpDir,
     "-logDir=" ++ LogDir,
     misc:local_url(RestPort, [no_scheme])];

goport_args(goxdcr, Config, _Cmd, _NodeUUID) ->
    IsEnterprise = cluster_compat_mode:is_enterprise(Config),
    IsEnterpriseFlag = "-isEnterprise=" ++ atom_to_list(IsEnterprise),
    ClientCertPath = ns_ssl_services_setup:chain_file_path(client_cert),
    ClientKeyPath = ns_ssl_services_setup:pkey_file_path(client_cert),
    build_port_args([{"-sourceKVAdminPort", rest_port},
                     {"-xdcrRestPort", xdcr_rest_port}], Config) ++
        [IsEnterpriseFlag | build_afamily_requirement("-")] ++
        ["-caFile=" ++ ns_ssl_services_setup:ca_file_path() || IsEnterprise] ++
        ["-clientCertFile=" ++ ClientCertPath || IsEnterprise] ++
        ["-clientKeyFile=" ++ ClientKeyPath || IsEnterprise];

goport_args(index, Config, _Cmd, NodeUUID) ->
    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),
    RestPort = service_ports:get_port(rest_port, Config),
    {ok, IdxDir} = ns_storage_conf:this_node_ixdir(),
    IdxDir2 = filename:join(IdxDir, "@2i"),

    build_port_args([{"-adminPort",         indexer_admin_port},
                     {"-scanPort",          indexer_scan_port},
                     {"-httpPort",          indexer_http_port},
                     {"-streamInitPort",    indexer_stinit_port},
                     {"-streamCatchupPort", indexer_stcatchup_port},
                     {"-streamMaintPort",   indexer_stmaint_port}], Config) ++

        build_https_args(indexer_https_port, "--httpsPort",
                         "--certFile", "--keyFile",
                         "--clientCertFile", "--clientKeyFile",
                         "--caFile", Config) ++

        build_afamily_requirement("-") ++

        ["-cluster=" ++ misc:local_url(RestPort, [no_scheme]),
         "-storageDir=" ++ IdxDir2,
         "-diagDir=" ++ path_config:minidump_dir(),
         "-logDir=" ++ LogDir,
         "-nodeUUID=" ++ NodeUUID,
         "-isEnterprise=" ++ atom_to_list(cluster_compat_mode:is_enterprise()),
         "--deploymentModel=" ++ config_profile:name()];

goport_args(backup, Config, _Cmd, NodeUUID) ->
    build_port_args([{"-http-port", backup_http_port},
                     {"-grpc-port", backup_grpc_port}], Config) ++

    build_https_args(backup_https_port, "-https-port", "-cert-path",
                     "-key-path", "-client-cert-path", "-client-key-path",
                     "-ca-path", Config) ++

    build_afamily_requirement("-") ++

    ["-cbm=" ++ filename:join(path_config:component_path(bin), "cbbackupmgr"),
     "-node-uuid=" ++ NodeUUID,
     "-public-address=" ++ misc:extract_node_address(node()),
     "-admin-port=" ++ integer_to_list(service_ports:get_port(rest_port,
                                                              Config)),
     "-log-file=none",
     "-log-level=" ++ atom_to_list(ns_server:get_loglevel(backup)),
     "-integrated-mode",
     "-integrated-mode-host=" ++ misc:local_url(
                                   service_ports:get_port(rest_port, Config),
                                   []),
     "-secure-integrated-mode-host=" ++ misc:local_url(
                                          service_ports:get_port(
                                            ssl_rest_port, Config), [ssl]),
     "-integrated-mode-user=@backup",
     "-tmp-dir=" ++ path_config:component_path(tmp),
     "-cbauth-host=" ++ misc:local_url(
                          service_ports:get_port(rest_port, Config),
                          [no_scheme])];

goport_args(cont_backup, Config, _Cmd, _NodeUUID) ->
    ContBackupDir = get_writable_subdir(data, "@continuous_backup"),

    ["backup"] ++
    build_port_args([{"--stats-port", cont_backup_http_port}], Config) ++
    ["--username=" ++ "@ns_server",
     "--backup-path=" ++ ContBackupDir,
     "--local-host=" ++ misc:local_url(
                          service_ports:get_port(rest_port, Config),
                          [])];

goport_args(fts, Config, _Cmd, NodeUUID) ->
    NsRestPort = service_ports:get_port(rest_port, Config),
    Host = misc:extract_node_address(node()),

    BuildHostPortArgs =
        fun(ArgName, PortName) ->
                case service_ports:get_port(PortName, Config) of
                    undefined -> [];
                    Port ->
                        V = io_lib:format("~s=~s:~b,~s:~b",
                                          [ArgName,
                                           misc:maybe_add_brackets(Host),
                                           Port, misc:inaddr_any([url]), Port]),
                        [lists:flatten(V)]
                end
        end,

    FTSIdxDir = get_writable_subdir(idx, "@fts"),

    BindHttp = BuildHostPortArgs("-bindHttp", fts_http_port),
    BindHttps =
        build_https_args(fts_ssl_port, "-bindHttps", ":",
                         "-tlsCertFile", "-tlsKeyFile",
                         "-clientCertFile", "-clientKeyFile",
                         "-tlsCAFile", Config),

    BindGrpc = BuildHostPortArgs("-bindGrpc", fts_grpc_port),
    BindGrpcSsl = BuildHostPortArgs("-bindGrpcSsl", fts_grpc_ssl_port),

    {ok, FTSMemoryQuota} = memory_quota:get_quota(Config, fts),
    MaxReplicasAllowed = case cluster_compat_mode:is_enterprise() of
                             true -> 3;
                             false -> 0
                         end,
    BucketTypesAllowed = case cluster_compat_mode:is_enterprise() of
                             true -> "membase:ephemeral";
                             false -> "membase"
                         end,
    Options =
        "startCheckServer=skip," ++
        "slowQueryLogTimeout=5s," ++
        "defaultMaxPartitionsPerPIndex=1024," ++
        "bleveMaxResultWindow=10000," ++
        "failoverAssignAllPrimaries=false," ++
        "deploymentModel=" ++ config_profile:name() ++ "," ++
        "cbaudit=" ++ atom_to_list(cluster_compat_mode:is_enterprise()) ++
        "," ++
        "ftsMemoryQuota=" ++ integer_to_list(FTSMemoryQuota * 1024000) ++ "," ++
        "maxReplicasAllowed=" ++ integer_to_list(MaxReplicasAllowed) ++ "," ++
        "bucketTypesAllowed=" ++ BucketTypesAllowed ++ "," ++
        "http2=" ++ atom_to_list(cluster_compat_mode:is_enterprise()) ++ "," ++
        build_afamily_requirement(","),
    [
     "-cfg=metakv",
     "-uuid=" ++ NodeUUID,
     "-server=" ++ misc:local_url(NsRestPort, []),
     "-dataDir=" ++ FTSIdxDir,
     "-tags=feed,janitor,pindex,queryer,cbauth_service",
     "-auth=cbauth",
     "-extra=" ++ lists:flatten(io_lib:format("~s:~b", [Host, NsRestPort])),
     "-options=" ++ Options
    ] ++ BindHttp ++ BindHttps ++ BindGrpc ++ BindGrpcSsl ++
        case cluster_compat_mode:is_enterprise() of
            true ->
                build_port_args([{"-serverSslPort", ssl_rest_port}], Config);
            false ->
                []
        end;

goport_args(eventing, Config, _Cmd, NodeUUID) ->
    {ok, EvDir} = ns_storage_conf:this_node_evdir(),
    build_port_args([{"-adminport", eventing_http_port},
                     {"-kvport", memcached_port},
                     {"-restport", rest_port},
                     {"-debugPort", eventing_debug_port}], Config) ++

        build_https_args(eventing_https_port, "-adminsslport",
                         "-certfile", "-keyfile", "-clientcertfile",
                         "-clientkeyfile", "-cafile", Config) ++

        build_afamily_requirement("-") ++

        ["-dir=" ++ filename:join(EvDir, "@eventing"),
         "-uuid=" ++ NodeUUID,
         "-diagdir=" ++ path_config:minidump_dir()];

goport_args(cbas, Config, Cmd, NodeUUID) ->
    Columnar = config_profile:search({cbas, columnar}, false),
    CBASDirs = [filename:join([Token],  "@analytics") ||
                   Token <- ns_storage_conf:this_node_cbas_dirs()],
    case misc:ensure_writable_dirs(CBASDirs) of
        ok ->
            ok;
        _ ->
            ?log_debug("Service cbas's directories (~p) are not "
                       "writable on node ~p", [CBASDirs, node()])
    end,

    JavaHome = ns_storage_conf:this_node_java_home(),

    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),
    Host = misc:extract_node_address(node()),
    {ok, MemoryQuota} = memory_quota:get_quota(Config, cbas),
    RotationConf = ns_server:get_disk_sink_rotation_opts(cbas),
    RotSize = proplists:get_value(size, RotationConf),
    RotNumFiles = proplists:get_value(num_files, RotationConf),
    LogLevel = ns_server:get_loglevel(cbas),

    build_port_args([{"-serverPort",           rest_port},
                     {"-serverSslPort",        ssl_rest_port},
                     {"-bindHttpPort",         cbas_http_port},
                     {"-bindAdminPort",        cbas_admin_port},
                     {"-debugPort",            cbas_debug_port},
                     {"-ccHttpPort",           cbas_cc_http_port},
                     {"-ccClusterPort",        cbas_cc_cluster_port},
                     {"-ccClientPort",         cbas_cc_client_port},
                     {"-consolePort",          cbas_console_port},
                     {"-clusterPort",          cbas_cluster_port},
                     {"-dataPort",             cbas_data_port},
                     {"-resultPort",           cbas_result_port},
                     {"-messagingPort",        cbas_messaging_port},
                     {"-metadataPort",         cbas_metadata_port},
                     {"-metadataCallbackPort", cbas_metadata_callback_port},
                     {"-parentPort",           cbas_parent_port},
                     {"-bindReplicationPort",  cbas_replication_port}],
                    Config) ++
        build_https_args(cbas_ssl_port, "-bindHttpsPort",
                         "-tlsCertFile", "-tlsKeyFile", "-clientCertFile",
                         "-clientKeyFile", "-tlsCAFile", Config) ++
        [
         "-uuid=" ++ NodeUUID,
         "-serverAddress=" ++ misc:localhost(),
         "-bindHttpAddress=" ++ Host,
         "-cbasExecutable=" ++ Cmd,
         "-memoryQuotaMb=" ++ integer_to_list(MemoryQuota),
         "-logDir=" ++ LogDir,
         "-tmpDir=" ++ path_config:component_path(tmp),
         "-logRotationSize=" ++ integer_to_list(RotSize),
         "-logRotationNumFiles=" ++ integer_to_list(RotNumFiles),
         "-logLevel=" ++ atom_to_list(LogLevel),
         "-deploymentModel=" ++ config_profile:name(),
         "-columnar=" ++ atom_to_list(Columnar)
        ] ++
        ["-dataDir=" ++ Dir || Dir <- CBASDirs] ++
        build_afamily_requirement("-") ++
        ["-javaHome=" ++ JavaHome || JavaHome =/= undefined].

saslauthd_port_spec() ->
    Cmd = find_executable("saslauthd-port"),
    case Cmd =/= false of
        true ->
            [{saslauthd_port, Cmd, [],
              [use_stdio, exit_status, stderr_to_stdout,
               {env, common_go_env_vars() ++
                     build_cbauth_dynamic_remote_env_vars(saslauthd)}]}];
        _ ->
            []
    end.

je_malloc_conf_default(Config) ->
    McdDefaults = ns_config:search(Config,
                                   {node, node(), memcached_defaults}, []),
    proplists:get_value(je_malloc_conf, McdDefaults).


%% The list of environment variables used when running memcached.
memcached_environment_variables(Config) ->
    %% Environment variables common to all execution environments.
    CommonEnv =
        [{"EVENT_NOSELECT", "1"},
         {"CBSASL_PWFILE", {"~s", [{isasl, path}]}}],

    %% Some execution environments require JE_MALLOC_CONF.
    case je_malloc_conf_default(Config) of
        undefined ->
            CommonEnv;
        Value ->
            lists:append(CommonEnv, [{"JE_MALLOC_CONF", Value}])
    end.

%% NOTE: Renamed from "memcached" -> "kv" since that's what we call it
%% everywhere else when referring to it as a service. The binary will still
%% be "memcached".
memcached_spec(Config) ->
    {kv, path_config:component_path(bin, "memcached"),
     ["-C", {"~s", [{memcached, config_path}]}, "--stdin"],
     [{env, memcached_environment_variables(Config)},
      use_stdio,
      stderr_to_stdout, exit_status,
      port_server_dont_start,
      stream]
    }.
