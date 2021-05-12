%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc producer side of the UPR proxy
%%
-module(dcp_producer_conn).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-export([start_link/4, init/2, handle_packet/5, handle_call/4, handle_cast/3]).

start_link(ConnName, ProducerNode, Bucket, RepFeatures) ->
    dcp_proxy:start_link(producer, ConnName, ProducerNode,
                         Bucket, ?MODULE, [RepFeatures]).

init([RepFeatures], ParentState) ->
    {[], dcp_proxy:maybe_connect(ParentState, RepFeatures)}.

handle_packet(request, ?DCP_SET_VBUCKET_STATE, Packet, State, ParentState) ->
    Consumer = dcp_proxy:get_partner(ParentState),
    gen_server:cast(Consumer, {set_vbucket_state, Packet}),
    {proxy, State, ParentState};

handle_packet(response, ?DCP_CLOSE_STREAM, Packet, State, ParentState) ->
    Consumer = dcp_proxy:get_partner(ParentState),
    gen_server:cast(Consumer, {producer_stream_closed, Packet}),
    {block, State, ParentState};

handle_packet(request, ?DCP_STREAM_END, Packet, State, ParentState) ->
    Consumer = dcp_proxy:get_partner(ParentState),
    gen_server:cast(Consumer, {producer_stream_end, Packet}),
    {proxy, State, ParentState};

handle_packet(_, _, _, State, ParentState) ->
    {proxy, State, ParentState}.

handle_call(Msg, _From, State, ParentState) ->
    ?rebalance_warning("Unhandled call: Msg = ~p, State = ~p", [Msg, State]),
    {reply, refused, State, ParentState}.

handle_cast({close_stream, Partition}, State, ParentState) ->
    close_stream(Partition, Partition, ParentState),
    {noreply, State, ParentState};

handle_cast(Msg, State, ParentState) ->
    ?rebalance_warning("Unhandled cast: Msg = ~p, State = ~p", [Msg, State]),
    {noreply, State, ParentState}.

close_stream(Partition, Opaque, ParentState) ->
    Sock = dcp_proxy:get_socket(ParentState),
    Bucket = dcp_proxy:get_bucket(ParentState),
    ConnName = dcp_proxy:get_conn_name(ParentState),

    dcp_commands:close_stream(Sock, Partition, Opaque),
    master_activity_events:note_dcp_close_stream(Bucket, ConnName,
                                                 Partition, Opaque, producer).
