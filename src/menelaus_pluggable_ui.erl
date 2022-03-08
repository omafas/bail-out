%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(menelaus_pluggable_ui).

-export([find_plugins/0,
         proxy_req/4]).

-include("ns_common.hrl").
-include("cut.hrl").

-define(CONFIG_DIR, etc).
-define(DOCROOTS_DIR, lib).
-define(PLUGIN_FILE_PATTERN, "pluggable-ui-*.json").

-define(TIMEOUT, 60000).
-define(DEF_REQ_HEADERS_FILTER, {drop, ["content-length",
                                        "transfer-encoding",
                                        "ns-server-proxy-timeout"]}).
-define(DEF_RESP_HEADERS_FILTER, {drop, ["content-length",
                                         "transfer-encoding",
                                         "www-authenticate"]}).
-type module_name()    :: string() | undefined.
-type proxy_strategy() :: local | sticky.
-type filter_op()      :: keep | drop.
-type ui_compat_version() :: [integer()].
-record(plugin,
        {proxy_strategy         :: proxy_strategy(),
         module_prefix          :: string(),
         doc_roots              :: [string()],
         version_dirs           :: undefined |
                                   [{ui_compat_version(), string()}],
         request_headers_filter :: {filter_op(), [string()]},
         module                 :: module_name()}).
-record(prefix, {port_name :: atom(),
                 service   :: atom()}).
-record(config, {prefixes :: dict:dict(), plugins  :: dict:dict()}).

-spec find_plugins() -> #config{}.
find_plugins() ->
    SpecFiles = find_plugin_spec_files(),
    read_and_validate_plugin_specs(SpecFiles, hardcoded_plugins()).

hardcoded_plugins() ->
    #config{prefixes =
                add_prefix(views, {"couchBase",
                                   port_name_by_service_name(views)},
                           dict:new()),
            plugins =
                dict:from_list([{views,
                                 #plugin{proxy_strategy = sticky,
                                         module_prefix = "couchBase",
                                         doc_roots = [],
                                         request_headers_filter =
                                             {keep, ["accept",
                                                     "accept-encoding",
                                                     "accept-language",
                                                     "cache-control",
                                                     "connection",
                                                     "content-type",
                                                     "pragma",
                                                     "user-agent",
                                                     "referer"]}}}])}.

%% The plugin files passed via the command line are processed first so it is
%% possible to override the standard files.
find_plugin_spec_files() ->
    find_plugin_spec_files_from_env() ++ find_plugin_spec_files_std().

%% The plugin files from the standard configuration dir are sorted to get a
%% well defined order when loading the files.
find_plugin_spec_files_std() ->
    lists:sort(
      filelib:wildcard(
        filename:join(
          path_config:component_path(?CONFIG_DIR),
          ?PLUGIN_FILE_PATTERN))).

%% The plugin files passed via the command line are not sorted, so it is
%% possible to change the order of then in case there are any strange
%% dependencies. This is just expected to be done during development.
find_plugin_spec_files_from_env() ->
    case application:get_env(ns_server, ui_plugins) of
        {ok, Raw} ->
            string:tokens(Raw, ",");
        _ ->
            []
    end.

read_and_validate_plugin_specs(SpecFiles, InitPlugins) ->
    lists:foldl(fun read_and_validate_plugin_spec/2, InitPlugins, SpecFiles).

decode_json(Bin) ->
    try ejson:decode(Bin) of
        {KVs} ->
            KVs;
        _ ->
            panic("Incorrect json")
    catch
        throw:{invalid_json, E} ->
            panic("Error parsing json: ~p", [E])
    end.

add_prefix(Service, {Prefix, PortName}, Dict) ->
    case dict:is_key(Prefix, Dict) of
        true ->
            panic("Prefix ~p is already defined", [Prefix]);
        false ->
            dict:store(Prefix, #prefix{port_name = PortName,
                                       service = Service}, Dict)
    end.

read_and_validate_plugin_spec(File, #config{plugins = Plugins,
                                            prefixes = Prefixes} =
                                  PluginsConfig) ->
    {ok, Bin} = file:read_file(File),
    try
        KVs = decode_json(Bin),
        {Service, PrefixesList, NewPlugin} = validate_plugin_spec(KVs, Plugins),

        NewPrefixes =
            lists:foldl(add_prefix(Service, _, _), Prefixes, PrefixesList),
        ?log_info("Loaded pluggable UI specification for ~p from ~p",
                  [Service, File]),
        #config{prefixes = NewPrefixes,
                plugins = dict:store(Service, NewPlugin, Plugins)}
    catch
        throw:{error, Error} ->
            ?log_error("Error parsing file ~s. ~s", [File, Error]),
            error({error, pluggable_ui_not_loaded});
        throw:{skip, Message} ->
            ?log_info("Skipped file ~s. ~s", [File, Message]),
            PluginsConfig
    end.

validate_plugin_spec(KVs, Plugins) ->
    ServiceName = binary_to_atom(get_element(<<"service">>, KVs), latin1),
    case lists:member(ServiceName,
                      ns_cluster_membership:supported_services()) of
        true -> ok;
        false -> skip("Unsupported service ~p", [ServiceName])
    end,
    case dict:is_key(ServiceName, Plugins) of
        false -> ok;
        true -> skip("Duplicate service ~p", [ServiceName])
    end,

    ProxyStrategy = decode_proxy_strategy(get_element(<<"proxy-strategy">>,
                                                      KVs)),
    {RestApiPrefixes, ModulePrefix} =
        decode_prefixes(ServiceName, get_element(<<"rest-api-prefixes">>, KVs)),

    DocRoots = decode_docroots(get_element(<<"doc-root">>, KVs)),
    VersionDirs = get_element(<<"version-dirs">>, KVs,
                              fun decode_version_dirs/1, []),
    ReqHdrFilter = get_element(<<"request-headers-filter">>, KVs,
                               fun decode_request_headers_filter/1,
                               ?DEF_REQ_HEADERS_FILTER),
    Module = proplists:get_value(<<"module">>, KVs),

    {ServiceName, RestApiPrefixes,
     #plugin{proxy_strategy = ProxyStrategy,
             module_prefix = ModulePrefix,
             doc_roots = DocRoots,
             version_dirs = VersionDirs,
             request_headers_filter = ReqHdrFilter,
             module = Module}}.

decode_prefixes(Service, {KeyValues}) ->
    case do_decode_prefixes(Service, KeyValues) of
        {[], undefined} ->
            panic("No REST API prefixes specified");
        {[{Prefix, _}] = Prefixes, undefined} ->
            {Prefixes, Prefix};
        {_, undefined} ->
            panic("One REST API prefix must be specified as a module prefix");
        Other ->
            Other
    end.

do_decode_prefixes(Service, KeyValues) ->
    lists:foldl(
      fun ({PrefixBin, {Props}}, {Acc, ModulePrefix}) ->
              Prefix = binary_to_list(PrefixBin),
              Port =
                  get_element(<<"portName">>, Props, binary_to_atom(_, latin1),
                              port_name_by_service_name(Service)),
              IsModulePrefix =
                  get_element(
                    <<"isModulePrefix">>, Props,
                    fun (true) ->
                            true;
                        (V) ->
                            panic("Incorrect value ~p of key isModulePrefix",
                                  [V])
                    end, false),
              NewModulePrefix =
                  case {IsModulePrefix, ModulePrefix} of
                      {true, undefined} ->
                          Prefix;
                      {true, _} ->
                          panic("Duplicate module prefixes");
                      {false, _} ->
                          ModulePrefix
                  end,
              {[{Prefix, Port} | Acc], NewModulePrefix}
      end, {[], undefined}, KeyValues).

panic(Str) ->
    panic(Str, []).

panic(Format, Params) ->
    abort(error, Format, Params).

skip(Format, Params) ->
    abort(skip, Format, Params).

abort(Type, Format, Params) ->
    throw({Type, lists:flatten(io_lib:format(Format, Params))}).

get_element(Key, KVs) ->
    case proplists:get_value(Key, KVs) of
        undefined ->
            panic("Missing required key ~p", [binary_to_list(Key)]);
        Value ->
            Value
    end.

get_element(Key, KVs, Decode, Default) ->
    case proplists:get_value(Key, KVs) of
        undefined ->
            Default;
        Value ->
            Decode(Value)
    end.

decode_proxy_strategy(<<"sticky">>) -> sticky;
decode_proxy_strategy(<<"local">>) -> local.

%% When run from cluster_run doc-root may be a list of directories.
%% DocRoot has to be a list in order for mochiweb to be able to guess
%% the MIME type.
decode_docroots(Roots) ->
    Prefix = path_config:component_path(?DOCROOTS_DIR),
    decode_docroots(Prefix, Roots).

decode_docroots(Prefix, Roots) when is_list(Roots) ->
    [create_docroot(Prefix, Root) || Root <- Roots];
decode_docroots(Prefix, Root) ->
    [create_docroot(Prefix, Root)].

create_docroot(Prefix, Root) ->
    filename:join(Prefix, binary_to_list(Root)).

decode_version_dirs(VersionDirs) ->
    [{get_element(<<"version">>, VersionDir),
      binary_to_list(get_element(<<"dir">>, VersionDir))} ||
        {VersionDir} <- VersionDirs].

decode_request_headers_filter({[{Op, BinNames}]}) ->
    Names = [string:to_lower(binary_to_list(Name)) || Name <- BinNames],
    {binary_to_existing_atom(Op, latin1), Names}.


%% When we are adding the cb-on-behalf-of Headers we have to drop
%% "ns-server-ui" and other menelaus-auth-* Headers.
%% If the "ns-server-ui" Header is present cbauth library
%% deduces the "ns-server-auth-token" is present.

add_filter_headers({drop, Hdrs}) ->
    {drop, Hdrs ++ ["ns-server-ui",
                    "menelaus-auth-user",
                    "menelaus-auth-domain",
                    "menelaus-auth-token",
                    "authorization"]};
add_filter_headers(_HdrFilter) ->
    _HdrFilter.

%%% =============================================================
%%%
proxy_req(RestPrefix, Path, PluginsConfig, Req) ->
    case find_prefix_info(RestPrefix, PluginsConfig) of
        {#prefix{port_name = Port, service = Service},
         #plugin{request_headers_filter = HdrFilter} = Plugin} ->
            case choose_node(Service, Plugin, Req) of
                {ok, Node, Remote} ->
                    HostPort = address_and_port(Port, Node),
                    Timeout = get_timeout(Service, Req),

                    % choose_node/3 makes sure that the version of
                    % this node and the destination node are same.
                    % But we still need to make the following
                    % cluster_compat_mode check, since some services like
                    % Analytics can forward this request to an Analytics
                    % service on a node with lower version.

                    {FwdHeader, HdrFilter1} =
                        case cluster_compat_mode:is_cluster_70() of
                            false ->
                                {auth_token(Req, Remote), HdrFilter};
                            true ->
                                Identity = menelaus_auth:get_identity(Req),
                                {[menelaus_rest:on_behalf_header(Identity),
                                  menelaus_rest:special_auth_header(Node)],
                                 add_filter_headers(HdrFilter)}
                        end,

                    Headers = FwdHeader ++ convert_headers(Req, HdrFilter1) ++
                        forwarded_headers(Req),
                    RespHeaderFilter =
                        fun (H) ->
                            filter_headers(H, ?DEF_RESP_HEADERS_FILTER)
                        end,
                    menelaus_util:proxy_req(HostPort, Path, Headers, Timeout,
                                            RespHeaderFilter, Req);
                {error, Error} ->
                    server_error(Req, Error)
            end;
        false ->
            server_error(Req, service_not_found)
    end.

find_prefix_info(RestPrefix, #config{plugins = Plugins, prefixes = Prefixes}) ->
    case dict:find(RestPrefix, Prefixes) of
        {ok, #prefix{service = Service} = Prefix} ->
            {ok, Plugin} = dict:find(Service, Plugins),
            {Prefix, Plugin};
        error ->
            false
    end.

choose_node(views, Plugin, Req) ->
    choose_node(kv, Plugin, Req);
choose_node(Service, #plugin{proxy_strategy = local}, _Req) ->
    Node = node(),
    case ns_cluster_membership:should_run_service(Service, Node) of
        true -> {ok, Node, local};
        false -> {error, {service_not_running, Service}}
    end;
choose_node(Service, #plugin{proxy_strategy = sticky}, Req) ->
    Node = node(),
    case service_nodes(Service) of
        {ok, Nodes} ->
            case lists:member(Node, Nodes) of
                true -> {ok, Node, local};
                false ->
                    {ok, menelaus_util:choose_node_consistently(Req, Nodes),
                     remote}
            end;
        Err ->
            Err
    end.

%% We don't want to proxy requests to nodes of different versions
%% because "ui <-> service" protocol might change
service_nodes(Service) ->
    Nodes = ns_cluster_membership:service_active_nodes(Service),
    NodesInfoDict = ns_doctor:get_nodes(),
    Versions = dict:map(?cut(proplists:get_value(advertised_version, _2)),
                        NodesInfoDict),
    {ok, LocalVsn} = dict:find(node(), Versions),
    SameVersionNodes = lists:usort(
                         [N || N <- Nodes,
                               {ok, LocalVsn} =:= dict:find(N, Versions)]),
    case SameVersionNodes =:= [] of
        true ->
            case Nodes =:= [] of
                true ->
                    {error, {service_not_running, Service}};
                false ->
                    {error, {no_compatible_service_running, Service}}
            end;
        false ->
            {ok, SameVersionNodes}
    end.

address_and_port(UnsecurePortName, Node) ->
    Addr = node_address(Node),
    NodeEncryption = misc:is_node_encryption_enabled(ns_config:latest(),
                                                     node()),
    PortName =
        case NodeEncryption of
            true -> service_ports:portname_to_secure_portname(UnsecurePortName);
            false -> UnsecurePortName
        end,
    Port = service_ports:get_port(PortName, ns_config:latest(), Node),
    true = Port =/= undefined,
    Scheme =
        case NodeEncryption of
            true -> https;
            false -> http
        end,
    AFamily = ns_config:read_key_fast({node, Node, address_family}, inet),
    {Scheme, Addr, Port, AFamily}.

node_address(Node) ->
    misc:extract_node_address(Node).

port_name_by_service_name(fts) -> fts_http_port;
port_name_by_service_name(backup) -> backup_http_port;
port_name_by_service_name(cbas) -> cbas_http_port;
port_name_by_service_name(n1ql) -> query_port;
port_name_by_service_name(views) -> capi_port;
port_name_by_service_name(eventing) -> eventing_http_port;
port_name_by_service_name(Service) -> panic("Unknown service ~p", [Service]).

get_timeout(views, Req) ->
    Params = mochiweb_request:parse_qs(Req),
    list_to_integer(proplists:get_value("connection_timeout", Params, "30000"));
get_timeout(_Service, Req) ->
    case mochiweb_request:get_header_value("ns-server-proxy-timeout", Req) of
        undefined ->
            ?TIMEOUT;
        Val ->
            list_to_integer(Val)
    end.

auth_token(Req, Remote) ->
    case menelaus_auth:extract_ui_auth_token(Req) of
        undefined ->
            [];
        Token ->
            %% if we go agains local node, there's no reason to pack node name
            %% into the token. In fact it causes the race with node rename that
            %% results in 401

            %% Services running on the local node, can forward pluggable UI
            %% requests to remote nodes. That necessitates packing the
            %% local-node in the "ns-server-auth-token" when the compat version
            %% is less than 7.0.0, else authentication of the request on the remote
            %% node will fail.

            %% Starting 7.0.0, Services use "cb-on-behalf-of" Headers which doesn't
            %% need the local-node info to authenticate a request.

            NodeToken = case cluster_compat_mode:is_cluster_70()
                             andalso Remote =:= local of
                            true ->
                                Token;
                            false ->
                                menelaus_ui_auth:set_token_node(Token, node())
                        end,
            [{"ns-server-ui","yes"},
             {"ns-server-auth-token", NodeToken}]
    end.

convert_headers(Req, Filter) ->
    RawHeaders = mochiweb_headers:to_list(mochiweb_request:get(headers, Req)),
    Headers = [{convert_header_name(Name), Val} || {Name, Val} <- RawHeaders],
    filter_headers(Headers, Filter).

forwarded_headers(Req) ->
    Socket = mochiweb_request:get(socket, Req),
    {ok, {Host, Port}} = mochiweb_socket:peername(Socket),
    For = misc:join_host_port(inet_parse:ntoa(Host), Port),
    Proto = mochiweb_request:get(scheme, Req),
    Forwarded = lists:flatten(io_lib:format("for=~s;proto=~s", [For, Proto])),
    [{"Forwarded", Forwarded}].

convert_header_name(Header) when is_atom(Header) ->
    atom_to_list(Header);
convert_header_name(Header) when is_list(Header) ->
    Header.

filter_headers(Headers, {keep, Names}) ->
    [Hdr || {Name, _} = Hdr <- Headers, member(Name, Names)];
filter_headers(Headers, {drop, Names}) ->
    [Hdr || {Name, _} = Hdr <- Headers, not member(Name, Names)].

member(Name, Names) ->
    lists:member(string:to_lower(Name), Names).

server_error(Req, {service_not_running, Service}) ->
    Msg = list_to_binary("Service " ++ atom_to_list(Service)
                         ++ " not running on this node"),
    send_server_error(Req, Msg, 404);
server_error(Req, {no_compatible_service_running, Service}) ->
    Msg = list_to_binary("Service " ++ atom_to_list(Service)
                         ++ " not running on this node,"
                         ++ " and compatible service is not found."),
    send_server_error(Req, Msg, 503);
server_error(Req, service_not_found) ->
    send_server_error(Req, <<"Not found">>, 404).

send_server_error(Req, Msg, Code) ->
    menelaus_util:reply_text(Req, Msg, Code).
