%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc serializes starting, terminating and restarting ports
%%      serves as a frontend for ns_child_ports_sup
%%

-module(ns_ports_manager).

-behavior(gen_server).

-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, restart_port_by_name/2, restart_port_by_name/3,
         find_port/2, send_command/3, set_dynamic_children/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, []}.

restart_port_by_name(Node, Name) ->
    restart_port_by_name(Node, Name, infinity).

restart_port_by_name(Node, Name, Timeout) ->
    ?log_debug("Requesting restart of port ~p", [Name]),
    gen_server:call({?MODULE, Node}, {restart_port_by_name, Name}, Timeout).

find_port(Node, Name) ->
    gen_server:call({?MODULE, Node}, {find_port, Name}, infinity).

send_command(Node, Name, Command) ->
    ?log_debug("Sending command ~p to port ~p", [Command, Name]),
    gen_server:call({?MODULE, Node}, {send_command, Name, Command}, infinity).

set_dynamic_children(Node, Children) ->
    ?log_debug("Setting children ~p", [get_names(Children)]),
    ChildrenThunk = fun () -> Children end,
    gen_server:call({?MODULE, Node},
                    {set_dynamic_children, ChildrenThunk}, infinity).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(Msg, State) ->
    ?log_info("Unhandled cast: ~p" , [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(Msg, State) ->
    ?log_info("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_call({restart_port_by_name, Name}, _From, State) ->
    ?log_debug("Restart of port ~p is requested", [Name]),
    Id = lists:keyfind(Name, 1, ns_child_ports_sup:current_ports()),
    RV = case Id of
             false ->
                 ?log_debug("Port ~p is not found. Nothing to restart", [Name]),
                 {ok, not_found};
             _ ->
                 ns_child_ports_sup:restart_port(Id)
         end,
    {reply, RV, State};
handle_call({find_port, Name}, _From, State) ->
    {reply, (catch ns_child_ports_sup:find_port(Name)), State};
handle_call({send_command, Name, Command}, _From, State) ->
    ?log_debug("Command ~p is about to be sent to port ~p", [Command, Name]),
    {reply, ns_child_ports_sup:send_command(Name, Command), State};
handle_call({set_dynamic_children, ChildrenThunk}, _From, State) ->
    Children = ChildrenThunk(),
    ?log_debug("New list of children is received: ~p", [get_names(Children)]),
    {reply, ns_child_ports_sup:set_dynamic_children(Children), State}.

get_names(Children) ->
    [element(1, Child) || Child <- Children].
