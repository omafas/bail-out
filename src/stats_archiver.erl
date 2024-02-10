%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc Store and aggregate statistics collected from stats_collector into a
%% collection of ETS tables, emitting 'sample_archived' events when aggregates
%% are created. The contents of ETS table is periodically dumped to files that
%% then used to restore ETS tables after restart.
%%

-module(stats_archiver).

-export([archives/0]).

%% This is old stats backward compat code
%% @doc the type of statistics collected
%% {Period, Seconds, Samples}
archives() ->
    [{minute, 1,     60},
     {hour,   4,     900},
     {day,    60,    1440}, % 24 hours
     {week,   600,   1152}, % eight days (computer weeks)
     {month,  1800,  1488}, % 31 days
     {year,   21600, 1464}]. % 366 days
