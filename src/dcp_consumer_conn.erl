%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc consumer side of the DCP proxy
%%
-module(dcp_consumer_conn).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-export([start_link/4,
         setup_streams/2, takeover/2, maybe_close_stream/2, shut_connection/2]).

-export([init/2, handle_packet/5, handle_call/4, handle_cast/3]).

-record(stream_state, {owner :: {pid(), any()},
                       to_add :: [vbucket_id()],
                       to_close :: [vbucket_id()],
                       to_close_on_producer :: [vbucket_id()] | ignore,
                       errors :: [{non_neg_integer(), vbucket_id()}],
                       next_state = idle :: idle | shut
                      }).

-record(takeover_state, {owner :: {pid(), any()},
                         state :: requested | opaque_known | active,
                         opaque :: integer(),
                         partition :: vbucket_id(),
                         requested_partition_state = none :: none | int_vb_state(),
                         open_ack = false :: boolean()
                        }).

-record(state, {state :: idle | shut | #stream_state{} | #takeover_state{},
                partitions :: [vbucket_id()]
               }).

start_link(ConnName, ConsumerNode, Bucket, RepFeatures) ->
    dcp_proxy:start_link(consumer, ConnName,
                         ConsumerNode, Bucket, ?MODULE, [RepFeatures]).

init([RepFeatures], ParentState) ->
    {#state{
        partitions = [],
        state = idle
       }, dcp_proxy:maybe_connect(ParentState, RepFeatures)}.


handle_packet(response, ?DCP_ADD_STREAM, Packet,
              #state{state = #stream_state{to_add = ToAdd, errors = Errors} = StreamState}
              = State, ParentState) ->
    {Header, Body} = mc_binary:decode_packet(Packet),

    {Partition, NewToAdd, NewErrors} =
        process_add_stream_response(Header, ToAdd, Errors, ParentState),
    NewStreamState = StreamState#stream_state{to_add = NewToAdd, errors = NewErrors},

    NewState =
        case Partition of
            error ->
                State;
            _ ->
                {ok, Opaque} = dcp_commands:process_response(Header, Body),
                ?rebalance_debug("Stream has been added for partition ~p, stream opaque = ~.16X",
                                 [Partition, Opaque, "0x"]),
                add_partition(Partition, State)
        end,
    {block, maybe_reply_setup_streams(NewState#state{state = NewStreamState}), ParentState};

handle_packet(response, ?DCP_ADD_STREAM, Packet,
              #state{state = #takeover_state{owner = From, partition = Partition, open_ack = false}
                     = TakeoverState} = State, ParentState) ->

    {Header, Body} = mc_binary:decode_packet(Packet),
    case {dcp_commands:process_response(Header, Body), Header#mc_header.opaque} of
        {{ok, _}, Partition} ->
            NewTakeoverState = TakeoverState#takeover_state{open_ack = true},
            {block, maybe_reply_takeover(From, NewTakeoverState, State), ParentState};
        {Error, Partition} ->
            gen_server:reply(From, Error),
            {block, State#state{state = idle}, ParentState};
        {_, WrongOpaque} ->
            ?rebalance_error("Unexpected response. Unrecognized opaque ~p~nHeader: ~p~nPartition: ~p",
                             [WrongOpaque, Header, Partition]),
            erlang:error({unrecognized_opaque, WrongOpaque, Partition})
    end;

handle_packet(request, ?DCP_STREAM_REQ, Packet,
              #state{state = #takeover_state{state = requested, partition = Partition}
                     = TakeoverState} = State, ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),
    Partition = Header#mc_header.vbucket,
    NewTakeoverState = TakeoverState#takeover_state{state = opaque_known,
                                                    opaque = Header#mc_header.opaque},
    {proxy, State#state{state = NewTakeoverState}, ParentState};

handle_packet(response, ?DCP_CLOSE_STREAM, Packet,
              #state{state = #stream_state{to_close = ToClose, errors = Errors} = StreamState}
              = State, ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),

    {Partition, NewToClose, NewErrors} =
        process_close_stream_response(Header, ToClose, Errors, consumer, ParentState),
    NewStreamState = StreamState#stream_state{to_close = NewToClose, errors = NewErrors},

    NewState =
        case Partition of
            error ->
                State;
            _ ->
                del_partition(Partition, State)
        end,
    {block, maybe_reply_setup_streams(NewState#state{state = NewStreamState}), ParentState};

handle_packet(response, ?DCP_SET_VBUCKET_STATE, Packet,
              #state{state = #takeover_state{opaque = Opaque, state = opaque_known,
                                             partition = Partition,
                                             owner = From,
                                             requested_partition_state = VbState} = TakeoverState}
              = State, ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),

    case {Header#mc_header.opaque, Header#mc_header.status} of
        {Opaque, ?SUCCESS} ->
            note_set_vbucket_state(Partition, VbState, ParentState),

            ?rebalance_debug("Partition ~p changed status to ~p",
                             [Partition, mc_client_binary:vbucket_state_to_atom(VbState)]),
            case VbState of
                ?VB_STATE_ACTIVE ->
                    NewTakeoverState = TakeoverState#takeover_state{state = active},
                    {proxy, maybe_reply_takeover(From, NewTakeoverState, State), ParentState};
                _ ->
                    {proxy, State#state{state =
                                            TakeoverState#takeover_state{
                                              requested_partition_state = none
                                             }
                                       },
                     ParentState}
            end;
        _ ->
            {proxy, State, ParentState}
    end;

handle_packet(Type, OpCode, _, #state{state = shut} = State, ParentState) ->
    ?log_debug("Ignoring packet ~p in shut state",
               [{Type, dcp_commands:command_2_atom(OpCode)}]),
    {block, State, ParentState};

handle_packet(_, _, _, State, ParentState) ->
    {proxy, State, ParentState}.

handle_call(get_partitions, _From, State, ParentState) ->
    {reply, get_partitions(State), State, ParentState};

handle_call({maybe_close_stream, Partition}, From,
            #state{state=idle} = State, ParentState) ->
    CurrentPartitions = get_partitions(State),

    StreamsToSet = lists:delete(Partition, CurrentPartitions),
    handle_call({setup_streams, StreamsToSet}, From, State, ParentState);

handle_call({setup_streams, Partitions}, From,
            #state{state=idle} = State, ParentState) ->
    Sock = dcp_proxy:get_socket(ParentState),
    CurrentPartitions = get_partitions(State),

    StreamsToStart = Partitions -- CurrentPartitions,
    StreamsToStop = CurrentPartitions -- Partitions,

    case {StreamsToStart, StreamsToStop} of
        {[], []} ->
            {reply, ok, State, ParentState};
        _ ->
            StartStreamRequests =
                lists:map(fun (Partition) ->
                                  add_stream(Sock, Partition, Partition, add, ParentState),
                                  {Partition}
                          end, StreamsToStart),

            StopStreamRequests = lists:map(fun (Partition) ->
                                                   Producer = dcp_proxy:get_partner(ParentState),
                                                   close_stream(Sock, Partition, Partition, ParentState),
                                                   gen_server:cast(Producer, {close_stream, Partition}),
                                                   {Partition}
                                           end, StreamsToStop),

            ?log_debug("Setup DCP streams:~nCurrent ~w~nStreams to open ~w~nStreams to close ~w~n",
                       [CurrentPartitions, StreamsToStart, StreamsToStop]),

            {noreply, State#state{state = #stream_state{
                                             owner = From,
                                             to_add = StartStreamRequests,
                                             to_close = StopStreamRequests,
                                             to_close_on_producer = StopStreamRequests,
                                             errors = []
                                            }
                                 }, ParentState}
    end;

handle_call(shut_connection, From,
            #state{state = idle,
                   partitions = Partitions} = State, ParentState) ->
    ?log_debug("Shutting the connection. Partitions to close:~n~p", [Partitions]),

    case Partitions of
        [] ->
            {reply, ok, State#state{state = shut}, ParentState};
        _ ->
            Sock = dcp_proxy:get_socket(ParentState),

            lists:foreach(
              fun (Partition) ->
                      close_stream(Sock, Partition, Partition, ParentState)
              end, Partitions),

            {noreply, State#state{state = #stream_state{
                                             owner = From,
                                             to_add = [],
                                             to_close = [{P} || P <- Partitions],
                                             to_close_on_producer = [],
                                             errors = [],
                                             next_state = shut
                                            }}, ParentState}
    end;

handle_call(shut_connection, From,
            #state{state = #stream_state{to_add = ToAdd,
                                         to_close = ToClose} = StreamState,
                   partitions = Partitions} = State, ParentState) ->
    ?log_debug("Discovered stuck replicator with state: ~p and partitions ~p.~n"
               "Shutting the connection.",
               [StreamState, Partitions]),

    MoreToClose = ToAdd ++ [{P} || P <- Partitions] -- ToClose,
    Sock = dcp_proxy:get_socket(ParentState),
    [close_stream(Sock, P, P, ParentState) || {P} <- MoreToClose],

    {noreply, State#state{state = StreamState#stream_state{
                                    owner = From,
                                    to_close = ToClose ++ MoreToClose,
                                    to_close_on_producer = ignore,
                                    next_state = shut
                                   }}, ParentState};

handle_call({takeover, Partition}, From, #state{state=idle} = State, ParentState) ->
    Sock = dcp_proxy:get_socket(ParentState),
    case has_partition(Partition, State) of
        true ->
            {reply, {error, takeover_on_open_stream_is_not_allowed}, State, ParentState};
        false ->
            add_stream(Sock, Partition, Partition, takeover, ParentState),
            {noreply, State#state{state = #takeover_state{
                                             owner = From,
                                             state = requested,
                                             opaque = Partition,
                                             partition = Partition
                                            }
                                 },
             ParentState}
    end;

handle_call(Msg, _From, State, ParentState) ->
    ?rebalance_warning("Unhandled call: Msg = ~p, State = ~p", [Msg, State]),
    {reply, refused, State, ParentState}.

handle_cast({producer_stream_closed, Packet},
            #state{state = #stream_state{to_close_on_producer = ToClose,
                                         errors = Errors} = StreamState} = State,
            ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),

    {Partition, NewToClose, NewErrors} =
        process_close_stream_response(Header, ToClose, Errors, producer, ParentState),
    NewStreamState = StreamState#stream_state{to_close_on_producer = NewToClose,
                                              errors = NewErrors},

    NewState =
        case Partition of
            error ->
                State;
            _ ->
                del_partition(Partition, State)
        end,
    {noreply, maybe_reply_setup_streams(NewState#state{state = NewStreamState}), ParentState};

handle_cast({set_vbucket_state, Packet},
            #state{state = #takeover_state{opaque = Opaque, state = opaque_known,
                                           partition = Partition,
                                           requested_partition_state = none} = TakeoverState}
            = State, ParentState) ->
    {Header, Body} = mc_binary:decode_packet(Packet),
    <<VbState:8>> = Body#mc_entry.ext,

    case Header#mc_header.opaque of
        Opaque ->
            ?rebalance_debug("Partition ~p is about to change status to ~p",
                             [Partition, mc_client_binary:vbucket_state_to_atom(VbState)]),
            {noreply, State#state{state =
                                      TakeoverState#takeover_state{requested_partition_state = VbState}},
             ParentState};
        _ ->
            {noreply, State, ParentState}
    end;

%%
%% Producer has ended a stream.
%%
handle_cast({producer_stream_end, _Packet}, #state{state = shut} = State,
            ParentState) ->
    %% Do nothing if the connection is already shutdown.
    {noreply, State, ParentState};
handle_cast({producer_stream_end, Packet}, State, ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),

    %% Does the vBucket have an active stream?
    StreamToEnd = Header#mc_header.vbucket,
    NewState = case has_partition(StreamToEnd, State) of
                   true ->
                       %% There is an active stream for the vbucket.
                       %% Consumer's KV engine processes the stream end when the
                       %% producer "proxies" the request to it.
                       %% So all we need to do here is cleanup the state in
                       %% ns_server.
                       %% Although very unlikely, it is possible to have race
                       %% between DCP_CLOSE_STREAM and DCP_STREAM_END for
                       %% the same vBucket. Consider consumer has
                       %% sent a DCP_CLOSE_STREAM request for a vbucket but not
                       %% processed a response for it yet when it receives
                       %% producer_stream_end message. We will still go ahead
                       %% and do the cleanup below - i.e. remove the vbucket
                       %% from list of active partitions.
                       ?log_debug("Processed stream end for vbucket ~p ~n",
                                  [StreamToEnd]),
                       del_partition(StreamToEnd, State);
                   false ->
                       %%
                       %% The vbucket might not be in the list of "partitions"
                       %% because:
                       %% 1. The vbucket does not have an active stream.
                       %% 2. Consumer is in process of adding a stream for this
                       %%    vbucket.
                       %% 3. This is a takeover stream.
                       %%
                       %% In all 3 cases, we do nothing.
                       %%
                       %% Case 2: We will not handle race scenarios where
                       %% consumer is trying to add a stream for a vbucket
                       %% for which producer sends a stream end.
                       %%
                       %% Case 3:
                       %% The producer sends stream end message at the end of
                       %% dcp takeover.
                       %% But, we do not need to process it because ns_server
                       %% uses DCP_SET_VBUCKET_STATE change to indentify
                       %% end of takeover.
                       %%
                       State
               end,
    {noreply, NewState, ParentState};

handle_cast(Msg, State, ParentState) ->
    ?rebalance_warning("Unhandled cast: Msg = ~p, State = ~p", [Msg, State]),
    {noreply, State, ParentState}.

process_stream_response(Header, ignore, Errors, close_stream, producer,
                        _ParentState) ->
    ?rebalance_warning("Ignore producer close stream response with opaque ~p, "
                       "status = ~p", [Header#mc_header.opaque]),
    {error, ignore, Errors};
process_stream_response(Header, PendingPartitions, Errors, Type, Side, ParentState) ->
    case lists:keytake(Header#mc_header.opaque, 1, PendingPartitions) of
        {value, {Partition}, N} ->
            Status = Header#mc_header.status,
            Success = is_successful_stream_response(Type, Status),

            note_stream_response(Type, Partition, Partition, Side,
                                 Status, Success, ParentState),

            case Success of
                true ->
                    {Partition, N, Errors};
                false ->
                    {error, N, [{Status, Partition} | Errors]}
            end;
        false ->
            ?rebalance_error("Unexpected response. Unrecognized opaque ~p~nHeader: ~p~nPartitions: ~p~nErrors: ~p",
                             [Header#mc_header.opaque, Header, PendingPartitions, Errors]),
            erlang:error({unrecognized_opaque, Header#mc_header.opaque, PendingPartitions})
    end.

is_successful_stream_response(add_stream, Status) ->
    Status =:= ?SUCCESS;
is_successful_stream_response(close_stream, Status) ->
    %% It's possible that the stream is already closed in the following cases:
    %%
    %%   - on the producer and consumer sides, if the vbucket has been moved
    %%   to a different node from the producer node
    %%
    %%   - on the consumer side, if the close stream request that we sent to
    %%   the producer was processed first by the producer, it sent a
    %%   notification to the consumer and the consumer processed the
    %%   notification before handling our close stream request
    %%
    %% Because of this we ignore ?KEY_ENOENT errors.
    Status =:= ?SUCCESS orelse Status =:= ?KEY_ENOENT.

process_add_stream_response(Header, PendingPartitions, Errors, ParentState) ->
    process_stream_response(Header, PendingPartitions, Errors,
                            add_stream, consumer, ParentState).

process_close_stream_response(Header, PendingPartitions, Errors, Side, ParentState) ->
    process_stream_response(Header, PendingPartitions, Errors,
                            close_stream, Side, ParentState).

maybe_reply_setup_streams(#state{state = StreamState} = State) ->
    ToCloseOnProducer = case StreamState#stream_state.to_close_on_producer of
                            ignore ->
                                [];
                            Other ->
                                Other
                        end,
    case {StreamState#stream_state.to_add, StreamState#stream_state.to_close,
          ToCloseOnProducer} of
        {[], [], []} ->
            Reply =
                case StreamState#stream_state.errors of
                    [] ->
                        ok;
                    Errors ->
                        {errors, Errors}
                end,
            gen_server:reply(StreamState#stream_state.owner, Reply),

            NextState = StreamState#stream_state.next_state,

            ?log_debug("Setup stream request completed with ~p. Moving to ~p state",
                       [Reply, NextState]),
            State#state{state = NextState};
        _ ->
            State
    end.

maybe_reply_takeover(From, #takeover_state{open_ack = true, state = active}, State) ->
    gen_server:reply(From, ok),
    State#state{state = idle};
maybe_reply_takeover(_From, TakeoverState, State) ->
    State#state{state = TakeoverState}.

setup_streams(Pid, Partitions) ->
    gen_server:call(Pid, {setup_streams, Partitions}, infinity).

maybe_close_stream(Pid, Partition) ->
    gen_server:call(Pid, {maybe_close_stream, Partition}, infinity).

takeover(Pid, Partition) ->
    gen_server:call(Pid, {takeover, Partition}, infinity).

shut_connection(Pid, Timeout) ->
    gen_server:call(Pid, shut_connection, Timeout).

add_partition(Partition, #state{partitions = CurrentPartitions} = State) ->
    State#state{partitions = ordsets:add_element(Partition, CurrentPartitions)}.

del_partition(Partition, #state{partitions = CurrentPartitions} = State) ->
    State#state{partitions = ordsets:del_element(Partition, CurrentPartitions)}.

get_partitions(#state{partitions = CurrentPartitions}) ->
    CurrentPartitions.

has_partition(Partition, #state{partitions = CurrentPartitions}) ->
    ordsets:is_element(Partition, CurrentPartitions).

add_stream(Sock, Partition, Opaque, Type, ParentState) ->
    dcp_commands:add_stream(Sock, Partition, Opaque, Type),

    Bucket = dcp_proxy:get_bucket(ParentState),
    ConnName = dcp_proxy:get_conn_name(ParentState),

    master_activity_events:note_dcp_add_stream(Bucket, ConnName,
                                               Partition, Opaque, Type, consumer).

close_stream(Sock, Partition, Opaque, ParentState) ->
    dcp_commands:close_stream(Sock, Partition, Opaque),

    Bucket = dcp_proxy:get_bucket(ParentState),
    ConnName = dcp_proxy:get_conn_name(ParentState),

    master_activity_events:note_dcp_close_stream(Bucket, ConnName,
                                                 Partition, Opaque, consumer).

note_stream_response(Type, Partition, Opaque, Side, Status, Success, ParentState) ->
    NoteFun = get_note_stream_response_fun(Type),

    Bucket = dcp_proxy:get_bucket(ParentState),
    ConnName = dcp_proxy:get_conn_name(ParentState),

    NoteFun(Bucket, ConnName, Partition, Opaque, Side, Status, Success).

get_note_stream_response_fun(add_stream) ->
    fun master_activity_events:note_dcp_add_stream_response/7;
get_note_stream_response_fun(close_stream) ->
    fun master_activity_events:note_dcp_close_stream_response/7.

note_set_vbucket_state(Partition, RawVbState, ParentState) ->
    Bucket = dcp_proxy:get_bucket(ParentState),
    ConnName = dcp_proxy:get_conn_name(ParentState),
    VbState = mc_client_binary:vbucket_state_to_atom(RawVbState),

    master_activity_events:note_dcp_set_vbucket_state(Bucket, ConnName, Partition, VbState).
