%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

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

-type per_ddoc_stats() :: {Sig::binary(),
                           DiskSize::integer(),
                           DataSize::integer(),
                           Accesses::integer()}.

-record(ns_server_couch_stats, {couch_docs_actual_disk_size,
                                couch_views_actual_disk_size,
                                couch_views_disk_size,
                                couch_views_data_size,
                                couch_spatial_disk_size,
                                couch_spatial_data_size,
                                views_per_ddoc_stats :: [per_ddoc_stats()],
                                spatial_per_ddoc_stats :: [per_ddoc_stats()]}).


%% API
-export([start_link_remote/2, fetch_stats/1, grab_raw_stats/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {bucket, last_ts, last_view_stats}).

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
    ets:new(server(Bucket), [protected, named_table, set]),
    ets:insert(server(Bucket), {stuff, []}),
    {ok, #state{bucket=Bucket}}.

handle_call(_, _From, State) ->
    {reply, erlang:nif_error(unhandled), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_stats, #state{bucket = Bucket,
                                  last_ts = LastTS,
                                  last_view_stats = LastViewStats} = State) ->
    TS = erlang:monotonic_time(millisecond),

    Config = ns_config:get(),
    MinFileSize = ns_config:search_node_prop(Config,
                                             compaction_daemon, min_view_file_size),
    true = (MinFileSize =/= undefined),

    NewStats = grab_couch_stats(Bucket, MinFileSize),
    {ProcessedSamples, NewLastViewStats} = parse_couch_stats(TS, NewStats, LastTS,
                                                             LastViewStats, MinFileSize),
    ets:insert(server(Bucket), {stuff, ProcessedSamples}),

    NowTS = erlang:monotonic_time(millisecond),
    Delta = min(?SAMPLE_INTERVAL, NowTS - TS),
    erlang:send_after(?SAMPLE_INTERVAL - Delta, self(), refresh_stats),

    {noreply, State#state{last_view_stats = NewLastViewStats,
                          last_ts = TS}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

fetch_stats(Bucket) ->
    [{_, CouchStats}] = ets:lookup(server(Bucket), stuff),
    {ok, CouchStats}.

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

aggregate_view_stats_loop(DiskSize, DataSize, [{_, ThisDiskSize, ThisDataSize, _ThisAccesses} | RestViewStats]) ->
    aggregate_view_stats_loop(DiskSize + ThisDiskSize,
                              DataSize + ThisDataSize,
                              RestViewStats);
aggregate_view_stats_loop(DiskSize, DataSize, []) ->
    {DiskSize, DataSize}.

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
    {ok, CouchDir} = ns_storage_conf:this_node_dbdir(),
    {ok, ViewRoot} = ns_storage_conf:this_node_ixdir(),

    DocsActualDiskSize = dir_size:get(filename:join([CouchDir, Bucket])),
    ViewsActualDiskSize = dir_size:get(couch_set_view:set_index_dir(ViewRoot, BinBucket, prod)),

    [{couch_docs_actual_disk_size, DocsActualDiskSize},
     {couch_views_actual_disk_size, ViewsActualDiskSize},
     {views_per_ddoc_stats, lists:sort(ViewStats)},
     {spatial_per_ddoc_stats, lists:sort(SpatialStats)}].

-spec grab_couch_stats(bucket_name(), integer()) -> #ns_server_couch_stats{}.
grab_couch_stats(Bucket, MinFileSize) ->
    Raw = grab_raw_stats(Bucket, MinFileSize),
    ViewsStats = proplists:get_value(views_per_ddoc_stats, Raw),
    SpatialStats = proplists:get_value(spatial_per_ddoc_stats, Raw),
    DocsActualDiskSize = proplists:get_value(couch_docs_actual_disk_size, Raw),
    ViewsActualDiskSize = proplists:get_value(couch_views_actual_disk_size,
                                              Raw),
    {ViewsDiskSize, ViewsDataSize} =
        aggregate_view_stats_loop(0, 0, ViewsStats),
    {SpatialDiskSize, SpatialDataSize} =
        aggregate_view_stats_loop(0, 0, SpatialStats),

    #ns_server_couch_stats{couch_docs_actual_disk_size = DocsActualDiskSize,
                           couch_views_actual_disk_size = ViewsActualDiskSize,
                           couch_views_disk_size = ViewsDiskSize,
                           couch_views_data_size = ViewsDataSize,
                           couch_spatial_disk_size = SpatialDiskSize,
                           couch_spatial_data_size = SpatialDataSize,
                           views_per_ddoc_stats = ViewsStats,
                           spatial_per_ddoc_stats = SpatialStats}.

find_not_less_sig(Sig, [{CandidateSig, _, _, _} | RestViewStatsTuples] = VS) ->
    case CandidateSig < Sig of
        true ->
            find_not_less_sig(Sig, RestViewStatsTuples);
        false ->
            VS
    end;
find_not_less_sig(_Sig, []) ->
    [].

diff_view_accesses_loop(TSDelta, LastVS, [{Sig, DiskS, DataS, AccC} | VSRest]) ->
    NewLastVS = find_not_less_sig(Sig, LastVS),
    PrevAccC = case NewLastVS of
                   [{Sig, _, _, X} | _] -> X;
                   _ -> AccC
               end,
    Res0 = (AccC - PrevAccC) * 1000 / TSDelta,
    Res = case Res0 < 0 of
              true -> 0;
              _ -> Res0
          end,
    NewTuple = {Sig, DiskS, DataS, Res},
    [NewTuple | diff_view_accesses_loop(TSDelta, NewLastVS, VSRest)];
diff_view_accesses_loop(_TSDelta, _LastVS, [] = _ViewStats) ->
    [].

build_basic_couch_stats(CouchStats) ->
    #ns_server_couch_stats{couch_docs_actual_disk_size = DocsActualDiskSize,
                           couch_views_actual_disk_size = ViewsActualDiskSize,
                           couch_views_disk_size = ViewsDiskSize,
                           couch_views_data_size = ViewsDataSize,
                           couch_spatial_disk_size = SpatialDiskSize,
                           couch_spatial_data_size = SpatialDataSize} = CouchStats,
    [{couch_docs_actual_disk_size, DocsActualDiskSize},
     {couch_views_actual_disk_size, ViewsActualDiskSize},
     {couch_views_disk_size, ViewsDiskSize},
     {couch_views_data_size, ViewsDataSize},
     {couch_spatial_disk_size, SpatialDiskSize},
     {couch_spatial_data_size, SpatialDataSize}].

parse_per_ddoc_stats(Prefix, TS, Stats, LastTS, LastStats0, MinFileSize) ->
    LastStats = case LastStats0 of
                    undefined ->
                        [];
                    _ ->
                        LastStats0
                end,
    TSDelta = TS - LastTS,
    WithDiffedOps =
        case TSDelta > 0 of
            true ->
                diff_view_accesses_loop(TSDelta, LastStats, Stats);
            false ->
                [{Sig, DiskS, DataS, 0} || {Sig, DiskS, DataS, _} <- Stats]
        end,
    AggregatedOps = lists:sum([Ops || {_, _, _, Ops} <- WithDiffedOps]),
    ProcessedStats =
        [begin
             DiskKey = iolist_to_binary([Prefix, Sig, <<"/disk_size">>]),
             DataKey = iolist_to_binary([Prefix, Sig, <<"/data_size">>]),
             OpsKey = iolist_to_binary([Prefix, Sig, <<"/accesses">>]),
             DataS = maybe_adjust_data_size(DataS0, DiskS, MinFileSize),

             [{DiskKey, DiskS},
              {DataKey, DataS},
              {OpsKey, OpsSec}]
         end || {Sig, DiskS, DataS0, OpsSec} <- WithDiffedOps],
    {AggregatedOps, lists:append(ProcessedStats)}.

parse_couch_stats(_TS, CouchStats, undefined = _LastTS, _, _) ->
    Basic = build_basic_couch_stats(CouchStats),
    {lists:sort([{couch_views_ops, 0.0} | [{couch_spatial_ops, 0.0} | Basic]]), {[], []}};
parse_couch_stats(TS, CouchStats, LastTS, {LastViewsStats, LastSpatialStats}, MinFileSize) ->
    BasicThings = build_basic_couch_stats(CouchStats),
    #ns_server_couch_stats{views_per_ddoc_stats = ViewsStats,
                           spatial_per_ddoc_stats = SpatialStats} = CouchStats,
    {ViewsAggregatedOps, ViewsProcessedStats} =
        parse_per_ddoc_stats(<<"views/">>, TS, ViewsStats, LastTS, LastViewsStats, MinFileSize),
    {SpatialAggregatedOps, SpatialProcessedStats} =
        parse_per_ddoc_stats(<<"spatial/">>, TS, SpatialStats, LastTS, LastSpatialStats, MinFileSize),

    {lists:sort(lists:append([[{couch_views_ops, ViewsAggregatedOps},
                               {couch_spatial_ops, SpatialAggregatedOps}],
                              BasicThings, ViewsProcessedStats, SpatialProcessedStats])),
     {ViewsStats, SpatialStats}}.


-ifdef(TEST).
basic_parse_couch_stats_test() ->
    CouchStatsRecord = #ns_server_couch_stats{couch_docs_actual_disk_size = 1,
                                              couch_views_actual_disk_size = 2,
                                              couch_views_disk_size = 5,
                                              couch_views_data_size = 6,
                                              couch_spatial_disk_size = 3,
                                              couch_spatial_data_size = 4,
                                              views_per_ddoc_stats = [{<<"a">>, 8, 9, 10},
                                                                      {<<"b">>, 11, 12, 13}],
                                              spatial_per_ddoc_stats = [{<<"c">>, 20, 21, 22}]},
    ExpectedOut1Pre = [{couch_docs_actual_disk_size, 1},
                       {couch_views_actual_disk_size, 2},
                       {couch_views_disk_size, 5},
                       {couch_views_data_size, 6},
                       {couch_spatial_disk_size, 3},
                       {couch_spatial_data_size, 4},
                       {couch_views_ops, 0.0},
                       {couch_spatial_ops, 0.0}]
        ++ [{<<"views/a/disk_size">>, 8},
            {<<"views/a/data_size">>, 9},
            {<<"views/a/accesses">>, 0.0},
            {<<"views/b/disk_size">>, 11},
            {<<"views/b/data_size">>, 12},
            {<<"views/b/accesses">>, 0.0},
            {<<"spatial/c/disk_size">>, 20},
            {<<"spatial/c/data_size">>, 21},
            {<<"spatial/c/accesses">>, 0.0}],
    ExpectedOut1 = lists:sort([{K, V} || {K, V} <- ExpectedOut1Pre,
                                         not is_binary(K)]),
    ExpectedOut2 = lists:sort(ExpectedOut1Pre),
    {Out1, State1} = parse_couch_stats(1000, CouchStatsRecord, undefined, undefined, 0),
    ?debugFmt("Got first result~n~p~n~p", [Out1, State1]),
    {Out2, State2} = parse_couch_stats(2000, CouchStatsRecord, 1000, State1, 0),
    ?debugFmt("Got second result~n~p~n~p", [Out2, State2]),
    ?assertEqual({CouchStatsRecord#ns_server_couch_stats.views_per_ddoc_stats,
                  CouchStatsRecord#ns_server_couch_stats.spatial_per_ddoc_stats}, State2),
    ?assertEqual(ExpectedOut1, Out1),
    ?assertEqual(ExpectedOut2, Out2).

-endif.
