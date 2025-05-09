%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(couch_stats_reader).

-include("couch_db.hrl").
-include("ns_common.hrl").
-include("ns_stats.hrl").

%% included to import #config{} record only
-include("ns_config.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).


%% API
-export([start_link_remote/2, grab_raw_stats/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {bucket, refresh_interval}).

%% Amount of time to wait between fetching stats
-define(SAMPLE_INTERVAL, 5000).


start_link_remote(Node, Bucket) ->
    misc:start_link(Node, misc, turn_into_gen_server,
                    [{local, server(Bucket)},
                     ?MODULE,
                     [Bucket], []]).

init([Bucket]) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
    case ns_bucket:bucket_type(BucketConfig) of
        membase ->
            self() ! refresh_stats;
        memcached ->
            ok
    end,
    RefreshInterval = ?SAMPLE_INTERVAL,
    ets:new(server(Bucket), [protected, named_table, set]),
    {ok, #state{bucket = Bucket, refresh_interval = RefreshInterval}}.

handle_call(_, _From, State) ->
    {reply, erlang:nif_error(unhandled), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_stats, #state{bucket = Bucket,
                                  refresh_interval = Interval} = State) ->
    BinBucket = ?l2b(Bucket),

    TS = erlang:monotonic_time(millisecond),
    {ok, ViewRoot} = ns_storage_conf:this_node_ixdir(),

    ViewsActualDiskSize = dir_size:get(couch_set_view:set_index_dir(ViewRoot,
                                                                    BinBucket,
                                                                    prod)),

    Stats = [{couch_views_actual_disk_size, ViewsActualDiskSize}],

    TableName = server(Bucket),
    IsFirstCalculation = not ets:member(TableName, stats),
    ets:insert(TableName, {stats, Stats}),

    NowTS = erlang:monotonic_time(millisecond),

    SendAfterInterval =
        case IsFirstCalculation of
            true ->
                rand:uniform(Interval);
            false ->
                max(0, Interval - (NowTS - TS))
        end,
    erlang:send_after(SendAfterInterval, self(), refresh_stats),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

views_collection_loop_iteration(Mod, BinBucket, NameToStatsETS,  DDocId, MinFileSize) ->
    case (catch couch_set_view:get_group_data_size(
                  Mod, BinBucket, DDocId)) of
        {ok, PList} ->
            {_, Signature} = lists:keyfind(signature, 1, PList),
            case ets:lookup(NameToStatsETS, Signature) of
                [] ->
                    {_, DiskSize} = lists:keyfind(disk_size, 1, PList),
                    {_, DataSize0} = lists:keyfind(data_size, 1, PList),
                    {_, Accesses} = lists:keyfind(accesses, 1, PList),

                    DataSize = maybe_adjust_data_size(DataSize0, DiskSize, MinFileSize),

                    ets:insert(NameToStatsETS, {Signature, DiskSize, DataSize, Accesses});
                _ ->
                    ok
            end;
        Why ->
            ?log_debug("Get group info (~s/~s) failed:~n~p", [BinBucket, DDocId, Why])
    end.

collect_view_stats(Mod, BinBucket, DDocIdList, MinFileSize) ->
    NameToStatsETS = ets:new(ok, []),
    try
        [views_collection_loop_iteration(Mod, BinBucket, NameToStatsETS, DDocId, MinFileSize)
         || DDocId <- DDocIdList],
        ets:tab2list(NameToStatsETS)
    after
        ets:delete(NameToStatsETS)
    end.

maybe_adjust_data_size(DataSize, DiskSize, MinFileSize) ->
    case DiskSize < MinFileSize of
        true ->
            DiskSize;
        false ->
            DataSize
    end.

grab_raw_stats(Bucket) ->
    MinFileSize = ns_config:search_node_prop(ns_config:latest(),
                                             compaction_daemon,
                                             min_view_file_size),
    true = (MinFileSize =/= undefined),
    grab_raw_stats(Bucket, MinFileSize).

grab_raw_stats(Bucket, MinFileSize) ->
    BinBucket = ?l2b(Bucket),

    DDocIdList = capi_utils:fetch_ddoc_ids(BinBucket),
    ViewStats = collect_view_stats(mapreduce_view, BinBucket, DDocIdList,
                                   MinFileSize),
    SpatialStats = collect_view_stats(spatial_view, BinBucket, DDocIdList,
                                      MinFileSize),

    %% Heavy stats, that we don't want to compute on demand
    PrecomputedStats =
        try ets:lookup(server(Bucket), stats) of
            [] -> [];
            [{stats, R}] -> R
        catch
            error:badarg ->
                ?log_warning("Stats reader ets doesn't exist"),
                []
        end,

    [{views_per_ddoc_stats, lists:sort(ViewStats)},
     {spatial_per_ddoc_stats, lists:sort(SpatialStats)} | PrecomputedStats].

