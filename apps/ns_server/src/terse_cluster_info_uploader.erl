%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc This service watches changes of terse cluster info and uploads
%% it to ep-engine
-module(terse_cluster_info_uploader).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).

-record(state, {
          port_pid :: pid()
         }).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_call/3, handle_cast/2]).

start_link() ->
    proc_lib:start_link(?MODULE, init, [[]]).

%% callbacks
init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(bucket_info_cache_invalidations,
                             fun ('*') ->
                                     Self ! refresh;
                                 (_) ->
                                     ok
                             end),
    %% Free up our parent to continue on. This is needed as the rest of
    %% this function might take some time to complete.
    proc_lib:init_ack({ok, Self}),

    Pid = memcached_config_mgr:memcached_port_pid(),
    remote_monitors:monitor(Pid),

    Self ! refresh,
    gen_server:enter_loop(?MODULE, [], #state{port_pid = Pid}).

handle_info(refresh, State) ->
    misc:flush(refresh),
    try bucket_info_cache:build_node_services() of
        {Rev, RevEpoch, Bin, _NodesExtHash} ->
            ?log_debug("Refreshing terse cluster info with ~p", [Bin]),
            ok = ns_memcached:set_cluster_config(Rev, RevEpoch, Bin)
    catch T:E:Stack ->
            ?log_error("Got exception trying to get terse cluster info: ~p",
                       [{T, E, Stack}]),
            timer:sleep(10000),
            erlang:raise(T, E, Stack)
    end,
    {noreply, State};
handle_info({remote_monitor_down, Pid, Reason},
            #state{port_pid = Pid} = State) ->
    ?log_debug("Got DOWN with reason: ~p from memcached port server: ~p. "
               "Shutting down", [Reason, Pid]),
    {stop, {shutdown, {memcached_port_server_down, Pid, Reason}}, State};
handle_info(Info, State) ->
    ?log_debug("Got unknown message: ~p", [Info]),
    {noreply, State}.

handle_call(Msg, _From, _State) ->
    erlang:error({unknown_msg, Msg}).

handle_cast(Msg, _State) ->
    erlang:error({unknown_msg, Msg}).
