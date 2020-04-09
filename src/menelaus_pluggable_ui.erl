%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-2018 Couchbase, Inc.
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
-module(menelaus_pluggable_ui).

-export([find_plugins/0,
         inject_head_fragments/3,
         proxy_req/4,
         maybe_serve_file/4]).

-include("ns_common.hrl").
-include("cut.hrl").

-define(CONFIG_DIR, etc).
-define(DOCROOTS_DIR, lib).
-define(PLUGIN_FILE_PATTERN, "pluggable-ui-*.json").

-define(HEAD_FRAG_HTML, <<"head.frag.html">>).
-define(HEAD_MARKER, <<"<!-- Inject head.frag.html file content for Pluggable UI components here -->">>).
-define(TIMEOUT, 60000).
-define(PART_SIZE, 100000).
-define(WINDOW_SIZE, 5).
-define(DEF_REQ_HEADERS_FILTER, {drop, ["content-length",
                                        "transfer-encoding",
                                        "ns-server-proxy-timeout"]}).
-define(DEF_RESP_HEADERS_FILTER, {drop, ["content-length",
                                         "transfer-encoding",
                                         "www-authenticate"]}).
-type service_name()   :: atom().
-type proxy_strategy() :: local | sticky.
-type filter_op()      :: keep | drop.
-type ui_compat_version() :: [integer()].
-record(prefix_props, { port_name :: atom() }).
-record(plugin, { name                   :: service_name(),
                  proxy_strategy         :: proxy_strategy(),
                  rest_api_prefixes      :: dict:dict(),
                  doc_roots              :: [string()],
                  version_dirs           :: undefined | [{ui_compat_version(), string()}],
                  request_headers_filter :: {filter_op(), [string()]}}).
-type plugin()  :: #plugin{}.
-type plugins() :: [plugin()].

-spec find_plugins() -> plugins().
find_plugins() ->
    SpecFiles = find_plugin_spec_files(),
    read_and_validate_plugin_specs(SpecFiles, [view_plugin()]).

view_plugin() ->
    ViewPortName = port_name_by_service_name(views),
    Prefixes = [{"couchBase", #prefix_props{port_name = ViewPortName}}],
    #plugin{name = views,
            proxy_strategy = sticky,
            rest_api_prefixes = dict:from_list(Prefixes),
            doc_roots = [],
            request_headers_filter = {keep, ["accept",
                                             "accept-encoding",
                                             "accept-language",
                                             "authorization",
                                             "cache-control",
                                             "connection",
                                             "content-type",
                                             "pragma",
                                             "user-agent",
                                             "referer"]}}.

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

read_and_validate_plugin_spec(File, Acc) ->
    {ok, Bin} = file:read_file(File),
    {KVs} = ejson:decode(Bin),
    validate_plugin_spec(KVs, Acc).

-spec validate_plugin_spec([{binary(), binary()}], plugins()) -> plugins().
validate_plugin_spec(KVs, Plugins) ->
    ServiceName = binary_to_atom(get_element(<<"service">>, KVs), latin1),
    ProxyStrategy = decode_proxy_strategy(get_element(<<"proxy-strategy">>,
                                                      KVs)),
    Prefixes = decode_prefixes(get_opt_element(<<"rest-api-prefixes">>, KVs)),
    %% Backward compatibility code
    %% remove this code when all the services switch to using
    %% rest-api-prefixes instead of rest-api-prefix
    RestApiPrefixes = case Prefixes of
                          undefined ->
                              decode_obsolete_prefixes(
                                get_element(<<"rest-api-prefix">>, KVs),
                                ServiceName);
                          _ -> Prefixes
                      end,
    %% End of backward compatibility code
    DocRoots = decode_docroots(get_element(<<"doc-root">>, KVs)),
    VersionDirs = decode_version_dirs(get_opt_element(<<"version-dirs">>, KVs)),
    ReqHdrFilter = decode_request_headers_filter(
                     get_opt_element(<<"request-headers-filter">>, KVs)),
    case {valid_service(ServiceName),
          check_prefix_uniqueness(RestApiPrefixes, Plugins)} of
        {true, ok} ->
            ?log_info("Loaded pluggable UI specification for ~p",
                      [ServiceName]),
            [#plugin{name = ServiceName,
                     proxy_strategy = ProxyStrategy,
                     rest_api_prefixes = dict:from_list(RestApiPrefixes),
                     doc_roots = DocRoots,
                     version_dirs = VersionDirs,
                     request_headers_filter = ReqHdrFilter} | Plugins];
        {true, {error, {duplicates, Duplicates}}} ->
            ?log_info("Pluggable UI specification for ~p not loaded, "
                      "duplicate REST API prefixes ~p",
                      [ServiceName, Duplicates]),
            Plugins;
        {false, _} ->
            ?log_info("Pluggable UI specification for ~p not loaded",
                      [ServiceName]),
            Plugins
    end.

check_prefix_uniqueness(Prefixes, Plugins) ->
    PrefixNames = [P || {P, _} <- Prefixes],
    Duplicates = misc:duplicates(PrefixNames) ++
        lists:filter(is_plugin(_, Plugins), PrefixNames),
    case Duplicates of
        [] -> ok;
        _  -> {error, {duplicates, Duplicates}}
    end.

decode_prefixes(undefined) -> undefined;
decode_prefixes({KeyValues}) ->
    lists:map(
      fun ({PrefixBin, {Props}}) ->
              Prefix = binary_to_list(PrefixBin),
              Port = binary_to_atom(get_element(<<"portName">>, Props), latin1),
              {Prefix, #prefix_props{port_name = Port}}
      end, KeyValues).

decode_obsolete_prefixes(PrefixBin, Service) ->
    Prefix = binary_to_list(PrefixBin),
    [{Prefix, #prefix_props{port_name = port_name_by_service_name(Service)}}].

valid_service(ServiceName) ->
    lists:member(ServiceName,
                 ns_cluster_membership:supported_services()).

get_element(Key, KVs) ->
    {Key, Val} = lists:keyfind(Key, 1, KVs),
    Val.

get_opt_element(Key, KVs) ->
    case lists:keyfind(Key, 1, KVs) of
        {Key, Val} ->
            Val;
        false ->
            undefined
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

decode_version_dirs(undefined) ->
    [];
decode_version_dirs(VersionDirs) ->
    [{get_element(<<"version">>, VersionDir),
      binary_to_list(get_element(<<"dir">>, VersionDir))} ||
        {VersionDir} <- VersionDirs].

decode_request_headers_filter(undefined) ->
    ?DEF_REQ_HEADERS_FILTER;
decode_request_headers_filter({[{Op, BinNames}]}) ->
    Names = [string:to_lower(binary_to_list(Name)) || Name <- BinNames],
    {binary_to_existing_atom(Op, latin1), Names}.

%%% =============================================================
%%%
proxy_req(RestPrefix, Path, Plugins, Req) ->
    case find_plugin_by_prefix(RestPrefix, Plugins) of
        #plugin{name = Service,
                request_headers_filter = HdrFilter,
                rest_api_prefixes = Prefixes} = Plugin ->
            {ok, PrefixProps} = dict:find(RestPrefix, Prefixes),
            case choose_node(Plugin, Req) of
                {ok, Node} ->
                    HostPort = address_and_port(PrefixProps, Node),
                    Timeout = get_timeout(Service, Req),
                    AuthToken = auth_token(Req),
                    Headers = AuthToken ++ convert_headers(Req, HdrFilter) ++
                              forwarded_headers(Req),
                    do_proxy_req(HostPort, Path, Headers, Timeout, Req);
                {error, Error} ->
                    server_error(Req, Error)
            end;
        false ->
            server_error(Req, service_not_found)
    end.

choose_node(#plugin{name = views} = Plugin, Req) ->
    choose_node(Plugin#plugin{name = kv}, Req);
choose_node(#plugin{name = Service, proxy_strategy = local}, _Req) ->
    case ns_cluster_membership:should_run_service(Service, node()) of
        true -> {ok, node()};
        false -> {error, {service_not_running, Service}}
    end;
choose_node(#plugin{name = Service, proxy_strategy = sticky}, Req) ->
    case service_nodes(Service) of
        [] -> {error, {service_not_running, Service}};
        Nodes ->
            case lists:member(node(), Nodes) of
                true -> {ok, node()};
                false ->
                    {ok, menelaus_util:choose_node_consistently(Req, Nodes)}
            end
    end.

%% We don't want to proxy requests to nodes of different versions
%% because "ui <-> service" protocol might change
service_nodes(Service) ->
    Nodes = ns_cluster_membership:service_active_nodes(Service),
    NodesInfoDict = ns_doctor:get_nodes(),
    Versions = dict:map(?cut(proplists:get_value(advertised_version, _2)),
                        NodesInfoDict),
    {ok, LocalVsn} = dict:find(node(), Versions),
    lists:usort([N || N <- Nodes, {ok, LocalVsn} =:= dict:find(N, Versions)]).

address_and_port(#prefix_props{port_name = UnsecurePortName}, Node) ->
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
port_name_by_service_name(cbas) -> cbas_http_port;
port_name_by_service_name(n1ql) -> query_port;
port_name_by_service_name(views) -> capi_port;
port_name_by_service_name(eventing) -> eventing_http_port.

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

auth_token(Req) ->
    case menelaus_auth:extract_ui_auth_token(Req) of
        undefined ->
            [];
        Token ->
            NodeToken = menelaus_ui_auth:set_token_node(Token, node()),
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

do_proxy_req({Scheme, Host, Port, AFamily}, Path, Headers, Timeout, Req) ->
    Method = mochiweb_request:get(method, Req),
    Body = get_body(Req),
    Options = [{partial_download, [{window_size, ?WINDOW_SIZE},
                                   {part_size, ?PART_SIZE}]},
               {connect_options, [AFamily]}],
    Resp = lhttpc:request(Host, Port, Scheme =:= https, Path, Method, Headers,
                          Body, Timeout, Options),
    handle_resp(Resp, Req).

get_body(Req) ->
    case mochiweb_request:recv_body(Req) of
        Body when is_binary(Body) ->
            Body;
        undefined ->
            <<>>
    end.

handle_resp({ok, {{StatusCode, _ReasonPhrase}, RcvdHeaders, Pid}}, Req)
  when is_pid(Pid) ->
    SendHeaders = filter_headers(RcvdHeaders, ?DEF_RESP_HEADERS_FILTER),
    Resp = menelaus_util:reply(Req, chunked, StatusCode, SendHeaders),
    stream_body(Pid, Resp);
handle_resp({ok, {{StatusCode, _ReasonPhrase}, RcvdHeaders, undefined = _Body}},
            Req) ->
    SendHeaders = filter_headers(RcvdHeaders, ?DEF_RESP_HEADERS_FILTER),
    menelaus_util:reply_text(Req, <<>>, StatusCode, SendHeaders);
handle_resp({error, timeout}, Req) ->
    menelaus_util:reply_text(Req, <<"Gateway Timeout">>, 504);
handle_resp({error, _Reason}=Error, Req) ->
    ?log_error("http client error ~p~n", [Error]),
    menelaus_util:reply_text(Req, <<"Unexpected server error">>, 500).

stream_body(Pid, Resp) ->
    case lhttpc:get_body_part(Pid) of
        {ok, Part} when is_binary(Part) ->
            mochiweb_response:write_chunk(Part, Resp),
            stream_body(Pid, Resp);
        {ok, {http_eob, _Trailers}} ->
            mochiweb_response:write_chunk(<<>>, Resp)
    end.

server_error(Req, {service_not_running, Service}) ->
    Msg = list_to_binary("Service " ++ atom_to_list(Service)
                         ++ " not running on this node"),
    send_server_error(Req, Msg);
server_error(Req, service_not_found) ->
    send_server_error(Req, <<"Not found">>).

send_server_error(Req, Msg) ->
    menelaus_util:reply_text(Req, Msg, 404).

%%% =============================================================
%%%
maybe_serve_file(RestPrefix, Plugins, Path, Req) ->
    case doc_roots(RestPrefix, Plugins) of
        DocRoots when is_list(DocRoots) ->
            serve_file_multiple_roots(Req, Path, DocRoots);
        undefined ->
            menelaus_util:reply_not_found(Req)
    end.

doc_roots(RestPrefix, Plugins) ->
    case find_plugin_by_prefix(RestPrefix, Plugins) of
        #plugin{doc_roots = DocRoots} ->
            DocRoots;
        false ->
            undefined
    end.

serve_file_multiple_roots(Req, Path, [DocRoot]) ->
    serve_file(Req, Path, DocRoot);
serve_file_multiple_roots(Req, Path, [DocRoot | DocRoots]) ->
    File = filename:join(DocRoot, Path),
    case filelib:is_regular(File) of
        true ->
            serve_file(Req, Path, DocRoot);
        false ->
            serve_file_multiple_roots(Req, Path, DocRoots)
    end.

serve_file(Req, Path, DocRoot) ->
    menelaus_util:serve_file(Req,
                             Path,
                             DocRoot,
                             [{"Cache-Control", "max-age=10"}]).

%%% =============================================================
%%%
-spec is_plugin(string(), plugins()) -> boolean().
is_plugin(Prefix, Plugins) ->
    case find_plugin_by_prefix(Prefix, Plugins) of
        #plugin{} ->
            true;
        _ ->
            false
    end.

find_plugin_by_prefix(_Prefix, []) ->
    false;
find_plugin_by_prefix(Prefix, [Plugin|Tail]) ->
    case dict:find(Prefix, Plugin#plugin.rest_api_prefixes) of
        {ok, _} -> Plugin;
        error -> find_plugin_by_prefix(Prefix, Tail)
    end.

%%% =============================================================
%%%

-spec inject_head_fragments(file:filename_all(), ui_compat_version(), plugins()) -> [binary()].
inject_head_fragments(File, UiCompatVersion, Plugins) ->
    {ok, Index} = file:read_file(File),
    [Head, Tail] = split_index(Index),
    [Head, head_fragments(UiCompatVersion, Plugins), Tail].

split_index(Bin) ->
    binary:split(Bin, ?HEAD_MARKER).

head_fragments(UiCompatVersion, Plugins) ->
    [head_fragment(UiCompatVersion, P) || P <- Plugins].

head_fragment(_UiCompatVersion, #plugin{doc_roots = []}) ->
    [];
head_fragment(UiCompatVersion, #plugin{name = Service, doc_roots = DocRoots,
                                       version_dirs = VersionDirs}) ->
    VersionDir = proplists:get_value(UiCompatVersion, VersionDirs),
    create_service_block(Service, find_head_fragments(Service, DocRoots, VersionDir)).

find_head_fragments(Service, _, undefined) ->
    Msg = io_lib:format("Pluggable component for service ~p is not supported for "
                        "this UI compat version", [Service]),
    ?log_error(Msg),
    html_comment(Msg);
find_head_fragments(Service, [DocRoot|DocRoots], VersionDir) ->
    [must_get_fragment(Service, DocRoot, VersionDir)
     | maybe_get_fragments(DocRoots, VersionDir)].

maybe_get_fragments(DocRoots, VersionDir) ->
    [maybe_get_head_fragment(DocRoot, VersionDir) || DocRoot <- DocRoots].

must_get_fragment(Service, DocRoot, VersionDir) ->
    Path = filename:join([DocRoot, VersionDir, ?HEAD_FRAG_HTML]),
    handle_must_get_fragment(Service, Path, file:read_file(Path)).

handle_must_get_fragment(_Service, File, {ok, Bin}) ->
    create_fragment_block(File, Bin);
handle_must_get_fragment(Service, File, {error, Reason}) ->
    Msg = lists:flatten(io_lib:format(
                          "Failed to read ~s for service ~p, reason '~p'",
                          [File, Service, Reason])),
    ?log_error(Msg),
    html_comment(Msg).

maybe_get_head_fragment(DocRoot, VersionDir) ->
    Path = filename:join([DocRoot, VersionDir, ?HEAD_FRAG_HTML]),
    handle_maybe_get_fragment(Path, file:read_file(Path)).

handle_maybe_get_fragment(File, {ok, Bin}) ->
    create_fragment_block(File, Bin);
handle_maybe_get_fragment(_File, {error, _Reason}) ->
    [].

create_service_block(Service, Fragments) ->
    SBin = atom_to_binary(Service, latin1),
    [start_of_service_fragment(SBin),
     Fragments,
     end_of_service_fragment(SBin)].

create_fragment_block(File, Bin) ->
    [start_of_docroot_fragment(File),
     Bin,
     end_of_docroot_fragment(File)].

start_of_service_fragment(Service) ->
    html_comment([<<"Beginning of head html fragments for service ">>, Service]).

end_of_service_fragment(Service) ->
    html_comment([<<"End of head html fragments for service ">>, Service]).

start_of_docroot_fragment(File) ->
    html_comment([<<"Beginning of ">>, File]).

end_of_docroot_fragment(File) ->
    html_comment([<<"End of ">>, File]).

html_comment(Content) ->
    [<<"<!-- ">>, Content, <<" -->\n">>].
