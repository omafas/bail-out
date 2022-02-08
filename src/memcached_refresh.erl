%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc server for refreshing memcached configuration files
%%
-module(memcached_refresh).

-behaviour(gen_server).

-export([start_link/0, refresh/1, apply_to_file/2]).

-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_call/3,
         handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

refresh(Item) ->
    gen_server:cast(?MODULE, {refresh, Item}).

apply_to_file(TmpPath, Path) ->
    gen_server:call(?MODULE, {apply_to_file, TmpPath, Path}).

init([]) ->
    ToRestart =
        case ns_ports_manager:find_port(ns_server:get_babysitter_node(), memcached) of
            Pid when is_pid(Pid) ->
                ?log_debug("Starting during memcached lifetime. Try to refresh all files."),
                self() ! refresh,
                [isasl, rbac];
            _ ->
                []
        end,
    {ok, ToRestart}.

code_change(_OldVsn, State, _) -> {ok, State}.
terminate(_Reason, _State) -> ok.

handle_call({apply_to_file, TmpPath, Path}, _From, State) ->
    ?log_debug("File rename from ~p to ~p is requested", [TmpPath, Path]),
    {reply, file:rename(TmpPath, Path), State};
handle_call(_Msg, _From, State) ->
    {reply, not_implemented, State}.

handle_cast({refresh, Item}, ToRefresh) ->
    ?log_debug("Refresh of ~p requested", [Item]),
    self() ! refresh,
    {noreply, case lists:member(Item, ToRefresh) of
                  true ->
                      ToRefresh;
                  false ->
                      [Item | ToRefresh]
              end}.

handle_info(refresh, []) ->
    {noreply, []};
handle_info(refresh, ToRefresh) ->
    ToRetry =
        case ns_memcached:connect([{retries, 1}]) of
            {ok, Sock} ->
                NewToRefresh =
                    lists:filter(
                      fun (Item) ->
                              RefreshFun = refresh_fun(Item),
                              case (catch mc_client_binary:RefreshFun(Sock)) of
                                  ok ->
                                      false;
                                  Error ->
                                      ?log_debug("Error executing ~p: ~p", [RefreshFun, Error]),
                                      true
                              end
                      end, ToRefresh),
                gen_tcp:close(Sock),
                NewToRefresh;
            _ ->
                ToRefresh
        end,
    case ToRetry of
        [] ->
            ?log_debug("Refresh of ~p succeeded", [ToRefresh]),
            ok;
        _ ->
            RetryAfter = ns_config:read_key_fast(memcached_file_refresh_retry_after, 1000),
            ?log_debug("Refresh of ~p failed. Retry in ~p ms.", [ToRetry, RetryAfter]),
            erlang:send_after(RetryAfter, self(), refresh)
    end,
    {noreply, ToRetry};

%% Handle a late arriving response from a gen_server:call which may have
%% timed out.
handle_info(_Msg, State) ->
    {noreply, State}.

refresh_fun(Item) ->
    list_to_atom("refresh_" ++ atom_to_list(Item)).
