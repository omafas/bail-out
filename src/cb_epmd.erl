%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

%% @doc Replaces standard erl_epmd module. Assigns static ports for cb nodes
%%      based on node names.
-module(cb_epmd).

-include_lib("kernel/include/logger.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% External exports
-export([start/0, start_link/0, stop/0, port_for_node/2, port_please/2,
         port_please/3, names/0, names/1,
         register_node/2, register_node/3, is_local_node/1, node_type/1,
         get_port/4]).

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

%% Starting erl_epmd only for backward compat
%% Old clusters will use epmd to discover this nodes. When upgrade is finished
%% epmd is not needed.
start() ->
    load_configuration(),
    erl_epmd:start().
start_link() ->
    load_configuration(),
    erl_epmd:start_link().
stop() -> erl_epmd:stop().

%% Node here comes without hostname and as string
%% (for example: "n_1", but not 'n_1@127.0.0.1')
port_please(Node, Hostname) ->
    port_please(Node, Hostname, infinity).

port_please(NodeStr, Hostname, Timeout) ->
    try cb_dist:get_preferred_dist(NodeStr) of
        Module ->
            {AFamily, Encryption} = cb_dist:proto2netsettings(Module),
            get_port(NodeStr, Hostname, AFamily, Encryption, Timeout)
    catch
        error:Error ->
            {error, Error}
    end.

get_port(Node, AFamily, Encryption, Timeout) ->
    {Name, Host} = misc:node_name_host(Node),
    get_port(Name, Host, AFamily, Encryption, Timeout).

get_port(NodeName, NodeHost, AFamily, Encryption, Timeout) ->
    Module = cb_dist:netsettings2proto({AFamily, Encryption}),
    try {node_type(NodeName), Encryption} of
        %% needed for backward compat: old ns_server nodes use dynamic
        %% ports so the only way to know those ports is to ask real epmd
        %% for this reason we also keep registering new static ports on
        %% epmd because old nodes doesn't know anything about those
        %% ports
        {ns_server, false} ->
            case inet:getaddr(NodeHost, AFamily) of
                {ok, IpAddr} ->
                    case erl_epmd:port_please(NodeName, IpAddr, Timeout) of
                        {port, _, _} = R -> R;
                        _ ->
                            case port_for_node(Module, NodeName) of
                                noport -> noport;
                                P -> {port, P, 5}
                            end
                    end;
                {error, _} = Error -> Error
            end;
        {_, _} ->
            case port_for_node(Module, NodeName) of
                noport -> noport;
                P -> {port, P, 5}
            end
    catch
        error:Error ->
            {error, Error}
    end.

names() -> erl_epmd:names().

names(EpmdAddr) -> erl_epmd:names(EpmdAddr).

register_node(Name, PortNo) ->
    register_node(Name, PortNo, inet).

register_node(_Name, _PortNo, _Family) ->
    %% Since ports are static we don't need to register them, but
    %% there is one exception: because of backward compatibility
    %% we register non tls ns_server ports in order to let pre-6.5
    %% nodes find this node. The registering itself is done on cb_dist.
    %% 'Creation' is zero because we don't use it anyway
    %% real 'creation' is generated in cb_dist.erl
    {ok, 0}.

port_for_node(Module, NodeStr) ->
    case parse_node(NodeStr) of
        {executioner, _} -> noport;
        {Type, N} -> base_port(Type, cb_dist:proto_to_encryption(Module)) + N
    end.

is_local_node(Node) when is_atom(Node) -> is_local_node(atom_to_list(Node));
is_local_node(Node) ->
    [NodeName | _] = string:tokens(Node, "@"),
    case node_type(NodeName) of
        ns_server -> false;
        _ -> true
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

base_port(ns_server, false) ->
    application:get_env(kernel, external_tcp_port, 21100);
base_port(ns_server, true) ->
    application:get_env(kernel, external_tls_port, 21150);
base_port(babysitter, false) ->
    element(1, application:get_env(kernel, internal_tcp_ports, {21200, -1}));
base_port(babysitter, true) ->
    element(1, application:get_env(kernel, internal_tls_ports, {21250, -1}));
base_port(couchdb, false) ->
    element(2, application:get_env(kernel, internal_tcp_ports, {-1, 21300}));
base_port(couchdb, true) ->
    element(2, application:get_env(kernel, internal_tls_ports, {-1, 21350})).

node_type(NodeStr) ->
    {Type, _} = parse_node(NodeStr),
    Type.

parse_node("ns_1") -> {ns_server, 0};
parse_node("babysitter_of_ns_1") -> {babysitter, 0};
parse_node("couchdb_ns_1") -> {couchdb, 0};

parse_node("n_" ++ Nstr) -> {ns_server, list_to_integer(Nstr)};
parse_node("babysitter_of_n_" ++ Nstr) -> {babysitter, list_to_integer(Nstr)};
parse_node("couchdb_n_" ++ Nstr) -> {couchdb, list_to_integer(Nstr)};

parse_node("executioner") -> {executioner, 0};
parse_node("ctl-" ++ _) -> {executioner, 0};

parse_node(Name) -> erlang:error({unknown_node, Name}).

load_configuration() ->
    ConfigFile = config_path(),
    try read_ports_config(ConfigFile) of
        Config ->
            lists:map(
                fun ({Key, Val}) ->
                    application:set_env(kernel, Key, Val)
                end, Config)
    catch _:Error:ST ->
            ?LOG_ERROR("Invalid config ~p: ~p~n~p",
                       [ConfigFile, Error, ST]),
            erlang:error({invalid_format, ConfigFile})
    end.

read_ports_config(File) ->
    case erl_prim_loader:get_file(File) of
        {ok, Bin, _} -> parse_config(binary_to_list(Bin));
        error -> []
    end.

parse_config(Str) ->
    Lines = [string:trim(S) || L <- string:tokens(Str, "\r\n"),
                               [S | _] <- [string:split(L, ";")],
                               "" =/= string:trim(S)],

    ToInt = fun (S) ->
                try list_to_integer(S)
                catch
                    _:_ -> erlang:error({not_integer, S})
                end
            end,
    lists:map(
      fun (L) ->
              [Left, Right] =
                  case string:tokens(L, "=") of
                      [Op1, Op2] -> [Op1, Op2];
                      _ -> erlang:error({syntax_error, L})
                  end,
              case string:trim(Left) of
                  K when K =:= "external_tcp_port";
                         K =:= "external_tls_port" ->
                      {list_to_atom(K), ToInt(string:trim(Right))};
                  K when K =:= "internal_tcp_ports";
                         K =:= "internal_tls_ports" ->
                      case string:tokens(Right, ",") of
                          [Port1, Port2] ->
                              {list_to_atom(K), {ToInt(string:trim(Port1)),
                                                 ToInt(string:trim(Port2))}};
                          _ -> erlang:error({two_ports_expected, Right})
                      end;
                  K ->
                      erlang:error({unknown_key, K})
              end
      end, Lines).

config_path() ->
    filename:join(filename:dirname(cb_dist:config_path()), "dist_ports.cfg").

-ifdef(TEST).
parse_config_test() ->
    ?assertEqual([], parse_config("")),
    ?assertEqual([{external_tcp_port, 123}],
                 parse_config("external_tcp_port=123")),
    ?assertEqual([{external_tcp_port, 123}, {external_tls_port, 234}],
                 parse_config("external_tcp_port=123\nexternal_tls_port=234")),
    ?assertEqual([{internal_tcp_ports, {123,234}}],
                 parse_config("internal_tcp_ports=123,234")),
    ?assertEqual([{external_tcp_port, 123},
                  {internal_tls_ports, {321, 432}},
                  {internal_tcp_ports, {456, 678}}],
                 parse_config("  \n\r\nexternal_tcp_port= 123\r\n"
                              " internal_tls_ports =321,432 ; comment;comment\r"
                              " ;  comment\n"
                              "; comment\n"
                              "internal_tcp_ports = 456 , 678\r\n  \r\n   ")),
    ?assertException(error, {unknown_key, _}, parse_config("unknown=123")),
    ?assertException(error, {syntax_error, "unknown"}, parse_config("unknown")),
    ?assertException(error, {two_ports_expected, "123"},
                     parse_config("internal_tcp_ports=123")),
    ?assertException(error, {not_integer, "str"},
                     parse_config("internal_tcp_ports=123,str")),
    ?assertException(error, _, parse_config("internal_tcp_ports=123")),
    ?assertException(error, _, parse_config("internal_tcp_ports=123=1234")).
-endif.
