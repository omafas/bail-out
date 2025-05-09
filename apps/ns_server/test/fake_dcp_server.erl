%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc
%% A gen_server that manages fake DCP connections. This runs for a given Node
%% and effectively pretends to be memcached. This lets us spin up and test proxy
%% connections in their entirety. Writes the required memcached config to
%% fake_ns_config so that must be setup first. The DCP implementation exists in
%% fake_dcp_client.
-module(fake_dcp_server).

-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([get_connections/1]).

-define(SERVER(Node),
        list_to_atom(?MODULE_STRING ++ "-" ++ atom_to_list(Node))).

-record(fake_dcp_server_state,
        {debug_logging = false :: boolean(),
         listen_socket = undefined :: gen_tcp:listen_socket(),
         port = undefined :: inet:port_number(),
         %% We track two structures here, the former have not yet been connected
         %% to by the server. The latter have. We spin up more endpoints than
         %% may connect such that this code doesn't have to consider:
         %% 1) How many connections between nodes ns_server will establish
         %% 2) If connections will be removed by tests (if all replications
         %% between nodes go away
         fake_dcp_endpoint = [] :: [pid()],
         connected_fake_dcp_endpoint = [] :: [pid()]}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Node, DebugLogging) ->
    %% We need to spawn multiple of these so they must be named uniquely.
    gen_server:start_link({local, ?SERVER(Node)}, ?MODULE,
                          [Node, DebugLogging], []).

init([Node, DebugLogging]) ->
    %% We don't want to break other things from running, so bind to some
    %% ephemeral port.
    {ok, ListenSocket} =
        gen_tcp:listen(0, [binary, {packet, 0}, {active, once}]),

    %% We are now listening on some port... we need to work out which.
    {ok, Port} = inet:port(ListenSocket),

    %% To update our config for this node, so that the tests can connect.
    update_config_with_port(Node, Port),

    %% Trapping exits, when a connection goes away (when we remove all
    %% vBucket replications) we want to handle that gracefully.
    process_flag(trap_exit, true),

    %% Spawn a separate process to accept connections on the listen socket. We
    %% don't want to block up this server. We'll spawn another once this
    %% connects in-case the replication_manager utilizes multiple connections.
    {ok, Client} = fake_dcp_endpoint:start_link(ListenSocket, DebugLogging),

    {ok, #fake_dcp_server_state{debug_logging = DebugLogging,
                                listen_socket = ListenSocket,
                                port = Port,
                                fake_dcp_endpoint = [Client]}}.

handle_call(get_connections, _From,
            #fake_dcp_server_state{connected_fake_dcp_endpoint = Connections} =
                State) ->
    {reply, Connections, State}.

handle_cast(_Request, State = #fake_dcp_server_state{}) ->
    {noreply, State}.

%% Connection spun up by the ns_server side
handle_info({listening, Pid},
            #fake_dcp_server_state{connected_fake_dcp_endpoint = Connections,
                                   fake_dcp_endpoint = Clients,
                                   listen_socket = ListenSocket,
                                   debug_logging = DebugLogging} = State)
  when is_pid(Pid)->

    {ok, Client} = fake_dcp_endpoint:start_link(ListenSocket, DebugLogging),
    {noreply,
     State#fake_dcp_server_state{
       connected_fake_dcp_endpoint = [Pid | Connections],
       fake_dcp_endpoint = [Client | Clients]}};
%% Connection gone away (socket closed by the server)
handle_info({'EXIT', Pid, socket_closed},
            #fake_dcp_server_state{connected_fake_dcp_endpoint = Connections,
                                   fake_dcp_endpoint = Clients} = State) ->
    NewConnections = Connections -- [Pid],
    NewClients = Clients -- [Pid],
    {noreply,
     State#fake_dcp_server_state{
       connected_fake_dcp_endpoint = NewConnections,
       fake_dcp_endpoint = NewClients}}.

terminate(Reason,
    #fake_dcp_server_state{connected_fake_dcp_endpoint = Connections}) ->
    %% We're shutting down, nuke any connections still established.
    lists:foreach(
        fun(Connection) ->
            misc:terminate_and_wait(Connection, Reason)
        end, Connections),
    ok.

code_change(_OldVsn, State = #fake_dcp_server_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% API
%%%===================================================================

get_connections(Node) ->
    gen_server:call(?SERVER(Node), get_connections, 30000).

%%%===================================================================
%%% Internal functions
%%%===================================================================

update_config_with_port(Node, Port) ->
    Key = {node, Node, memcached},
    NewCfg= [{dedicated_port, Port},
        {admin_user, "@ns_server"},
        {admin_pass, "asdasd"}],
    fake_ns_config:update_snapshot(Key, NewCfg).
