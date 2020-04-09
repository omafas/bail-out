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
-module(goxdcr_status_keeper).

-include("ns_common.hrl").

-behavior(gen_server).

%% API
-export([start_link/0,
         get_replications/1,
         get_replications_with_remote_info/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(REFRESH_INTERVAL, ?get_param(refresh_interval, 5000)).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_replications_with_remote_info(Bucket) ->
    case ets:lookup(?MODULE, Bucket) of
        [{Bucket, Reps}] ->
            Reps;
        [] ->
            []
    end.

get_replications(Bucket) ->
    [Id || {Id, _, _} <- get_replications_with_remote_info(Bucket)].

init([]) ->
    ets:new(?MODULE, [protected, named_table, set]),
    self() ! refresh,
    {ok, []}.

handle_call(sync, _From, State) ->
    {reply, ok, State}.

handle_cast(Cast, State) ->
    ?log_warning("Ignoring unknown cast: ~p", [Cast]),
    {noreply, State}.

handle_info(refresh, State) ->
    refresh(),
    erlang:send_after(?REFRESH_INTERVAL, self(), refresh),
    {noreply, State};
handle_info(Msg, State) ->
    ?log_warning("Ignoring unknown msg: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

refresh() ->
    try goxdcr_rest:get_replications_with_remote_info() of
        Reps ->
            update_ets_table(Reps)
    catch
        T:E ->
            ?log_warning("Failed to refresh goxdcr replications: ~p",
                         [{T, E, erlang:get_stacktrace()}])
    end.

update_ets_table(Reps) ->
    D = lists:foldl(
          fun ({Id, Bucket, ClusterName, RemoteBucket}, Acc) ->
                  Rep = {Id, ClusterName, RemoteBucket},

                  dict:update(
                    Bucket,
                    fun (BucketReps) ->
                            [Rep | BucketReps]
                    end, [Rep], Acc)
          end, dict:new(), Reps),

    Buckets = dict:fetch_keys(D),
    CachedBuckets = [B || {B, _} <- ets:tab2list(?MODULE)],

    ToDelete = CachedBuckets -- Buckets,
    lists:foreach(
      fun (Bucket) ->
              ets:delete(?MODULE, Bucket)
      end, ToDelete),

    dict:fold(
      fun (Bucket, BucketReps, _) ->
              ets:insert(?MODULE, {Bucket, lists:reverse(BucketReps)})
      end, unused, D).
