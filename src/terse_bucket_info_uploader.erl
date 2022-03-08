%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc This service watches changes of terse bucket info and uploads
%% it to ep-engine
-module(terse_bucket_info_uploader).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(BucketName) ->
    ns_bucket_sup:ignore_if_not_couchbase_bucket(
      BucketName,
      fun (_) ->
              Name = server_name(BucketName),
              gen_server:start_link({local, Name}, ?MODULE, BucketName, [])
      end).

server_name(BucketName) ->
    list_to_atom("terse_bucket_info_uploader-" ++ BucketName).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init(BucketName) ->
    Self = self(),
    ns_pubsub:subscribe_link(bucket_info_cache_invalidations, fun invalidation_loop/2, {BucketName, Self}),
    submit_refresh(BucketName, Self),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({refresh, BucketName}, State) ->
    flush_refresh_msgs(BucketName),
    refresh_cluster_config(BucketName),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
flush_refresh_msgs(BucketName) ->
    case misc:flush({refresh, BucketName}) of
        N when N > 0 ->
            ?log_debug("Flushed ~p refresh messages", [N]);
        _ ->
            ok
    end.

refresh_cluster_config(BucketName) ->
    case bucket_info_cache:terse_bucket_info(BucketName) of
        {ok, Rev, RevEpoch, Blob} ->
            ok = ns_memcached:set_cluster_config(BucketName, Rev, RevEpoch, Blob);
        not_present ->
            ?log_debug("Bucket ~s is dead", [BucketName]),
            ok;
        {T, E, Stack} = Exception ->
            ?log_error("Got exception trying to get terse bucket info: ~p",
                       [Exception]),
            timer:sleep(10000),
            erlang:raise(T, E, Stack)
    end.

submit_refresh(BucketName, Process) ->
    Process ! {refresh, BucketName}.

invalidation_loop(BucketName, {BucketName, Parent}) ->
    submit_refresh(BucketName, Parent),
    {BucketName, Parent};
invalidation_loop('*', {BucketName, Parent}) ->
    submit_refresh(BucketName, Parent),
    {BucketName, Parent};
invalidation_loop(_, State) ->
    State.
