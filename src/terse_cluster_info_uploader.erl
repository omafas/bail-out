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

-behaviour(gen_server2).

-include("ns_common.hrl").

-export([start_link/0]).

%% gen_server2 callbacks
-export([init/1, handle_info/2]).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

%% callbacks
init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(bucket_info_cache_invalidations,
                             fun ('*') ->
                                     Self ! refresh;
                                 (_) ->
                                     ok
                             end),
    Self ! refresh,
    {ok, []}.

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
handle_info(_Info, State) ->
    {noreply, State}.
