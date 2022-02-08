%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(couch_log).

% public API
-export([start_link/0]).
-export([debug/2, info/2, error/2]).
-export([debug_on/0, info_on/0, get_level/0, get_level_integer/0, set_level/1]).
-export([read/2]).

-export([pre_db_open/1]).

-include("ns_common.hrl").

-define(LEVEL_ERROR, 3).
-define(LEVEL_INFO, 2).
-define(LEVEL_DEBUG, 1).

debug(Format, Args) ->
    ale:debug(?COUCHDB_LOGGER, Format, Args).

info(Format, Args) ->
    ale:info(?COUCHDB_LOGGER, Format, Args).

error(Format, Args) ->
    ale:error(?COUCHDB_LOGGER, Format, Args).

level_integer(error) -> ?LEVEL_ERROR;
level_integer(info)  -> ?LEVEL_INFO;
level_integer(debug) -> ?LEVEL_DEBUG;
level_integer(_Else) -> ?LEVEL_ERROR. % anything else default to ERROR level

start_link() ->
    ignore.

debug_on() ->
    true.

info_on() ->
    true.

set_level(LevelAtom) ->
    ale:set_loglevel(?COUCHDB_LOGGER, LevelAtom).

get_level() ->
    ale:get_loglevel(?COUCHDB_LOGGER).

get_level_integer() ->
    level_integer(get_level()).

read(_Bytes, _Offset) ->
    {ok, ""}.

pre_db_open(DbName) ->
    case re:run(DbName, "/[0-9]+$") of
        nomatch -> ok;
        _ ->
            try
                erlang:error({you_cannot_open_vbucket_dbs_anymore, DbName})
            catch T:E:Stack ->
                    couch_log:error("Something attempted to open database vbucket: ~s~nAt:~p",
                                    [DbName, Stack]),
                    erlang:raise(T, E, Stack)
            end
    end.
