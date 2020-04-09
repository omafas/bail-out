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
