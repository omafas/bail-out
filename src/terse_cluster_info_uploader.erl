%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2019 Couchbase, Inc.
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
        {Rev, Bin} ->
            ?log_debug("Refreshing terse cluster info with ~p", [Bin]),
            ok = ns_memcached:set_cluster_config(Rev, Bin)
    catch T:E ->
            Stack = erlang:get_stacktrace(),
            ?log_error("Got exception trying to get terse cluster info: ~p",
                       [{T, E, Stack}]),
            timer:sleep(10000),
            erlang:raise(T, E, Stack)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.
