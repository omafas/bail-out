%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2015 Couchbase, Inc.
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

-module(couch_log).
-behaviour(gen_event).

% public API
-export([start_link/0, stop/0]).
-export([debug/2, info/2, error/2]).
-export([debug_on/0, info_on/0, get_level/0, get_level_integer/0, set_level/1]).
-export([read/2]).

-export([pre_db_open/1]).

% gen_event callbacks
-export([init/1, handle_event/2, terminate/2, code_change/3]).
-export([handle_info/2, handle_call/2]).

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
    couch_event_sup:start_link({local, couch_log}, error_logger, couch_log, []).

stop() ->
    couch_event_sup:stop(couch_log).

init([]) ->
    ok.

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

handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

read(_Bytes, _Offset) ->
    {ok, ""}.

pre_db_open(DbName) ->
    case re:run(DbName, "/[0-9]+$") of
        nomatch -> ok;
        _ ->
            try
                erlang:error({you_cannot_open_vbucket_dbs_anymore, DbName})
            catch T:E ->
                    Stack = erlang:get_stacktrace(),
                    couch_log:error("Something attempted to open database vbucket: ~s~nAt:~p",
                                    [DbName, Stack]),
                    erlang:raise(T, E, Stack)
            end
    end.
