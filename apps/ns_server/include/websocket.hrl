%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-define(websocket_globally_unique_identifier,
        "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").
-define(websocket_response,
        "Upgrade: WebSocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: ").

-define(OPCODE_BINARY, 16#2).
-define(OPCODE_CLOSE, 16#8).
-define(OPCODE_PING, 16#9).
-define(OPCODE_PONG, 16#A).

-type socket() :: any().
-type re_entry() :: fun ((any()) -> no_return()).
-type reply_channel() :: fun ((any()) -> ok | {error, any()}).
