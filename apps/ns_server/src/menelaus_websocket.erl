%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(menelaus_websocket).

-include("ns_common.hrl").
-include("rbac.hrl").
-include("websocket.hrl").

%% API
-export([handle_upgrade/2,
         enter/2,
         send_bytes/2,
         send_ping/1,
         send_pong/1,
         close/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% Upgrade the connection to websocket, supplying the request, and a function to
%% be called for handling data received from the client.
%% Returns a connection, which have data sent
-spec handle_upgrade(mochiweb_request(), Body) ->
          {ok, {RespTuple, ReEntry, ReplyChannel}} | {error, any()} when
      Body :: {M, F} | {M, F, A}
            | fun ((Payload, State, ReplyChannel) -> State),
      M :: module(),
      F :: atom(),
      A :: [_],
      Payload :: any(),
      ReplyChannel :: reply_channel(),
      State :: any(),
      RespTuple :: {integer(), [{string(), string()}], string()},
      ReEntry :: re_entry().
handle_upgrade(Req, Body) ->
    maybe
        ok ?= validate_header_connection(Req),
        ok ?= validate_header_upgrade(Req),
        ok ?= validate_websocket_version(Req),
        ok ?= validate_websocket_key(Req),
        {ok, upgrade(Req, Body)}
    else
        {error, E} ->
            ?log_warning("Error with websocket request: ~p", [E]),
            menelaus_util:reply(Req, 400),
            {error, E}
    end.

-spec enter(re_entry(), any()) -> no_return().
enter(ReEntry, State) ->
    ?log_debug("Websocket connection established. Listening on socket"),
    ReEntry(State).

-spec send_bytes(reply_channel(), binary()) -> ok | {error, any()}.
send_bytes(ReplyChannel, Payload) ->
    ReplyChannel({?OPCODE_BINARY, Payload}).

-spec send_ping(reply_channel()) -> ok | {error, any()}.
send_ping(ReplyChannel) ->
    ReplyChannel({?OPCODE_PING, <<>>}).

-spec send_pong(reply_channel()) -> ok | {error, any()}.
send_pong(ReplyChannel) ->
    ReplyChannel({?OPCODE_PONG, <<>>}).

-spec close(reply_channel()) -> ok | {error, any()}.
close(ReplyChannel) ->
    ReplyChannel({?OPCODE_CLOSE, <<>>}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

validate_header_connection(Req) ->
    Connection = mochiweb_request:get_header_value("Connection", Req),
    case string:equal(Connection, "upgrade", true) of
        true -> ok;
        false -> {error, {bad_connection, Connection}}
    end.

validate_header_upgrade(Req) ->
    Upgrade = mochiweb_request:get_header_value("Upgrade", Req),
    case string:equal(Upgrade, "websocket", true) of
        true -> ok;
        false -> {error, {bad_upgrade, Upgrade}}
    end.

validate_websocket_version(Req) ->
    case mochiweb_request:get_header_value("Sec-WebSocket-Version", Req) of
        "13" -> ok;
        V -> {error, {bad_version, V}}
    end.

validate_websocket_key(Req) ->
    case mochiweb_request:get_header_value("Sec-WebSocket-Key", Req) of
        undefined -> {error, missing_key};
        _Key -> ok
    end.

-spec upgrade(mochiweb_request(), Body) ->
          {RespTuple, ReEntry, ReplyChannel} when
      Body :: {M, F} | {M, F, A}
            | fun ((Payload, State, ReplyChannel) -> State),
      M :: module(),
      F :: atom(),
      A :: [_],
      Payload :: any(),
      ReplyChannel :: reply_channel(),
      State :: any(),
      RespTuple :: {integer(), [{string(), string()}], string()},
      ReEntry :: re_entry().
upgrade(Req, Body) ->
    mochiweb_websocket:upgrade_connection(Req, Body).
