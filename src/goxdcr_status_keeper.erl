%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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
        T:E:S ->
            ?log_warning("Failed to refresh goxdcr replications: ~p",
                         [{T, E, S}])
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
