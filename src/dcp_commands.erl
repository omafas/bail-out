%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2018 Couchbase, Inc.
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
%%
%% @doc commands of the DCP protocol
%%
-module(dcp_commands).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-export([open_connection/5,
         add_stream/4, close_stream/3, stream_request/8,
         process_response/2, format_packet_nicely/1,
         command_2_atom/1]).

-spec process_response(#mc_header{}, #mc_entry{}) -> any().
process_response(#mc_header{opcode = ?DCP_ADD_STREAM, status = ?SUCCESS} = Header, Body) ->
    {ok, get_opaque(Header, Body)};
process_response(#mc_header{opcode = ?DCP_STREAM_REQ, status = ?ROLLBACK} = Header, Body) ->
    {rollback, get_body_as_int(Header, Body)};
process_response(#mc_header{status=?SUCCESS}, #mc_entry{}) ->
    ok;
process_response(#mc_header{status=Status}, #mc_entry{data=Msg}) ->
    {dcp_error, mc_client_binary:map_status(Status), Msg}.

process_response({ok, Header, Body}) ->
    process_response(Header, Body).

-spec open_connection(port(), dcp_conn_name(), dcp_conn_type(), list(),
                      node()) -> ok | dcp_error().
open_connection(Sock, ConnName, Type, RepFeatures, Node) ->
    Flags = case Type of
                consumer ->
                    ?DCP_CONNECTION_FLAG_CONSUMER;
                producer ->
                    ?DCP_CONNECTION_FLAG_PRODUCER;
                notifier ->
                    ?DCP_CONNECTION_FLAG_NOTIFIER
            end,

    NewFlags =
        lists:foldl(
          fun({F, Val}, FAcc) ->
                  case proplists:get_bool(F, RepFeatures) of
                      true -> FAcc bor Val;
                      false -> FAcc
                  end
          end, Flags, [{xattr, ?DCP_CONNECTION_FLAG_XATTR},
                       {del_times, ?DCP_CONNECTION_FLAG_INCL_DEL_TIMES}]),

    Extra = <<0:32, NewFlags:32>>,

    {Datatype, Encoded} = case proplists:get_bool(json, RepFeatures)
                               andalso proplists:get_bool(set_consumer_name,
                                                          RepFeatures)
                               andalso Type =:= consumer of
                              true ->
                                  {?MC_DATATYPE_JSON,
                                   ejson:encode({[{"consumer_name", Node}]})};
                              false ->
                                  {?MC_DATATYPE_RAW_BYTES, undefined}
                          end,
    ?log_debug("Open ~p connection ~p on socket ~p: Body ~p",
               [Type, ConnName, Sock, Encoded]),
    process_response(
      mc_client_binary:cmd_vocal(?DCP_OPEN, Sock,
                                 {#mc_header{}, #mc_entry{key = ConnName,
                                                          ext = Extra,
                                                          datatype = Datatype,
                                                          data = Encoded}})).

-spec add_stream(port(), vbucket_id(), integer(), add | takeover) -> {ok, quiet}.
add_stream(Sock, Partition, Opaque, Type) ->
    ?log_debug("Add stream for partition ~p, opaque = ~.16X, type = ~p",
               [Partition, Opaque, "0x", Type]),
    Ext = case Type of
              add ->
                  0;
              takeover ->
                  1
          end,

    {ok, quiet} = mc_client_binary:cmd_quiet(?DCP_ADD_STREAM, Sock,
                                             {#mc_header{opaque = Opaque,
                                                         vbucket = Partition},
                                              #mc_entry{ext = <<Ext:32>>}}).

-spec close_stream(port(), vbucket_id(), integer()) -> {ok, quiet}.
close_stream(Sock, Partition, Opaque) ->
    ?log_debug("Close stream for partition ~p, opaque = ~.16X", [Partition, Opaque, "0x"]),
    {ok, quiet} = mc_client_binary:cmd_quiet(?DCP_CLOSE_STREAM, Sock,
                                             {#mc_header{opaque = Opaque,
                                                         vbucket = Partition},
                                              #mc_entry{}}).

-spec stream_request(port(), vbucket_id(), integer(), seq_no(),
                     seq_no(), integer(), seq_no(), seq_no()) -> {ok, quiet}.
stream_request(Sock, Partition, Opaque, StartSeqNo, EndSeqNo,
               PartitionUUID, SnapshotStart, SnapshotEnd) ->
    Extra = <<0:64, StartSeqNo:64, EndSeqNo:64, PartitionUUID:64,
              SnapshotStart:64, SnapshotEnd:64>>,
    {ok, quiet} = mc_client_binary:cmd_quiet(?DCP_STREAM_REQ, Sock,
                                             {#mc_header{opaque = Opaque,
                                                         vbucket = Partition},
                                              #mc_entry{ext = Extra}}).

-spec command_2_atom(integer()) -> atom().
command_2_atom(?DCP_OPEN) ->
    dcp_open;
command_2_atom(?DCP_ADD_STREAM) ->
    dcp_add_stream;
command_2_atom(?DCP_CLOSE_STREAM) ->
    dcp_close_stream;
command_2_atom(?DCP_STREAM_REQ) ->
    dcp_stream_req;
command_2_atom(?DCP_GET_FAILOVER_LOG) ->
    dcp_get_failover_log;
command_2_atom(?DCP_STREAM_END) ->
    dcp_stream_end;
command_2_atom(?DCP_SNAPSHOT_MARKER) ->
    dcp_snapshot_marker;
command_2_atom(?DCP_MUTATION) ->
    dcp_mutation;
command_2_atom(?DCP_DELETION) ->
    dcp_deletion;
command_2_atom(?DCP_EXPIRATION) ->
    dcp_expiration;
command_2_atom(?DCP_FLUSH) ->
    dcp_flush;
command_2_atom(?DCP_SET_VBUCKET_STATE) ->
    dcp_set_vbucket_state;
command_2_atom(?DCP_CONTROL) ->
    dcp_control;
command_2_atom(?DCP_WINDOW_UPDATE) ->
    dcp_window_update;
command_2_atom(?DCP_NOP) ->
    dcp_nop;
command_2_atom(?DCP_PREPARE) ->
    dcp_prepare;
command_2_atom(?DCP_SEQNO_ACKNOWLEDGED) ->
    dcp_seqno_acknowledged;
command_2_atom(?DCP_COMMIT) ->
    dcp_commit;
command_2_atom(?DCP_ABORT) ->
    dcp_abort;
command_2_atom(_) ->
    not_dcp.

-spec format_packet_nicely(binary()) -> nonempty_string().
format_packet_nicely(<<?REQ_MAGIC:8, _Rest/binary>> = Packet) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),
    format_packet_nicely("REQUEST", "", Header, Packet);
format_packet_nicely(<<?RES_MAGIC:8, _Opcode:8, _KeyLen:16, _ExtLen:8,
                       _DataType:8, Status:16, _Rest/binary>> = Packet) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),
    format_packet_nicely("RESPONSE",
                         io_lib:format(" status = ~.16X (~w)",
                                       [Status, "0x", mc_client_binary:map_status(Status)]),
                         Header, Packet).

format_packet_nicely(Type, Status, Header, Packet) ->
    lists:flatten(
      io_lib:format("~s: ~.16X (~w) vbucket = ~w opaque = ~.16X~s~n~s",
                    [Type,
                     Header#mc_header.opcode, "0x",
                     command_2_atom(Header#mc_header.opcode),
                     Header#mc_header.vbucket,
                     Header#mc_header.opaque, "0x",
                     Status,
                     format_hex_strings(hexlify(Packet))])).

hexlify(<<>>, Acc) ->
    Acc;
hexlify(<<Byte:8, Rest/binary>>, Acc) ->
    hexlify(Rest, [lists:flatten(io_lib:format("~2.16.0B", [Byte])) | Acc]).

hexlify(Binary) ->
    lists:reverse(hexlify(Binary, [])).

format_hex_strings([], _, Acc) ->
    Acc;
format_hex_strings([String | Rest], 3, Acc) ->
    format_hex_strings(Rest, 0, Acc ++ String ++ "\n");
format_hex_strings([String | Rest], Count, Acc) ->
    format_hex_strings(Rest, Count + 1, Acc ++ String ++ " ").

format_hex_strings(Strings) ->
    format_hex_strings(Strings, 0, "").

get_body_as_int(Header, Body) ->
    Len = Header#mc_header.bodylen * 8,
    <<Ext:Len/big>> = Body#mc_entry.data,
    Ext.

get_opaque(Header, Body) ->
    Len = Header#mc_header.extlen * 8,
    <<Ext:Len/little>> = Body#mc_entry.ext,
    Ext.
