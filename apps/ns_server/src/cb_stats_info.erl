%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(cb_stats_info).

-include("ns_common.hrl").

-export([init_info/0,
         get_info/1,
         delete_info/0]).

-record(stat_info, {name = <<>> :: binary(),
                    type = <<>> :: binary(),
                    help = <<>> :: binary()
                   }).

-define(MISSING_TYPE, <<"untyped">>).
-define(MISSING_HELP_MSG, <<"Help is missing">>).

init_info() ->
    %% Table to hold info (e.g. type & help) for each stat found in the
    %% ns_server metrics_metadata.json file.
    ets:new(?MODULE, [public, named_table, set, {keypos, #stat_info.name}]),
    process_metrics_metadata().

%% Used by unit tests
delete_info() ->
    ets:delete(?MODULE).

%% Read the ns_server metrics_metadata.json file which contains information
%% such as the TYPE and HELP for stats.
process_metrics_metadata() ->
    CmPath = path_config:component_path(etc, "cm"),
    MetaPath0 = filename:join(CmPath, "metrics_metadata.json"),
    MetaPath = case filelib:is_regular(MetaPath0) of
                   true ->
                       MetaPath0;
                   false ->
                       %% cluster_run configurations
                       BinPath = path_config:component_path(bin),
                       filename:join([filename:dirname(BinPath),
                                      "etc", "couchbase", "cm",
                                      "metrics_metadata.json"])
               end,
    ?log_debug("Reading metrics metadata from ~p", [MetaPath]),
    MetaBin = case file:read_file(MetaPath) of
                  {ok, Bin} ->
                      Bin;
                  {error, enoent} ->
                      ?log_error("Metric file '~p' not found", [MetaPath]),
                      <<>>
              end,

    try ejson:decode(MetaBin) of
        {MetaJson} ->
            lists:foreach(
              fun ({StatName, {StatInfo}}) ->
                      Type = proplists:get_value(<<"type">>, StatInfo,
                                                 ?MISSING_TYPE),
                      Help = proplists:get_value(<<"help">>, StatInfo,
                                                 ?MISSING_HELP_MSG),
                      ets:insert(?MODULE,
                                 #stat_info{name = StatName,
                                            type = Type,
                                            help = Help})
              end, MetaJson)
    catch
        _:_ ->
            ?log_error("Metric file '~p' contains invalid json", [MetaPath])
    end.

get_info_helper(StatName) ->
    case ets:lookup(?MODULE, StatName) of
        [] ->
            {error, not_found};
        [#stat_info{type = Type, help = Help}] ->
            {ok, {Type, Help}}
    end.

-spec get_info(binary()) -> {binary(), binary()} | not_found.
get_info(StatName) when is_binary(StatName) ->
    case get_info_helper(StatName) of
        {ok, StatInfo} ->
            StatInfo;
        {error, not_found} ->
            %% Someone added a stat and didn't add an entry to the
            %% metrics_metadata.json file.
            ?log_error("Failed to find '~p' in the metrics_metadata.json "
                       "file", [StatName]),
            %% Save it away so we only warn once
            ets:insert(?MODULE,
                       #stat_info{name = StatName,
                                  type = ?MISSING_TYPE,
                                  help = ?MISSING_HELP_MSG}),
            not_found
    end.
