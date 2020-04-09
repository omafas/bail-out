%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2019 Couchbase, Inc.
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
-module(goport).

-include("cut.hrl").
-include("ns_common.hrl").

-behavior(gen_server).

-export([start_link/1, start_link/2,
         deliver/1, write/2, close/2, shutdown/1]).

%% gen_server
-export([init/1,
         handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(config, {
          cmd :: string(),
          args :: [string()],
          stderr_to_stdout :: boolean(),
          env :: [{string(), string()}],
          cd :: undefined | string(),
          exit_status :: boolean(),
          line :: undefined | pos_integer(),

          window_size :: pos_integer(),
          graceful_shutdown :: boolean()}).

-record(decoding_context, {
          data = <<>> :: binary(),
          length = undefined :: undefined |
                                decoding_error |
                                non_neg_integer()}).
-record(line_context, {
          data = <<>> :: binary(),
          max_size :: pos_integer()}).

-type stream() :: stdin | stdout | stderr.
-type op() :: ack_pending |
              {ack, pos_integer()} |
              {write, binary()} |
              {close, stream()} |
              shutdown.
-type op_result() :: ok | {error, binary()}.
-type delivery() :: {stream(),
                     {eol, binary()} | {noeol, binary()} | binary(),
                     pos_integer()}.
-type op_handler() :: fun((op(), op_result()) -> any()).

-record(state, {
          port  :: undefined | port(),
          owner :: pid(),

          ctx :: #decoding_context{},

          stdout_ctx :: undefined | #line_context{},
          stderr_ctx :: undefined | #line_context{},

          deliver       :: boolean(),
          deliver_queue :: queue:queue(delivery()),

          current_op  :: undefined | {op(), op_handler()},
          pending_ops :: queue:queue({op(), op_handler()}),

          delivered_bytes   :: non_neg_integer(),
          unacked_bytes     :: non_neg_integer(),
          pending_ack_bytes :: non_neg_integer(),
          have_pending_ack  :: boolean(),

          config :: #config{}}).

-define(DEFAULT_WINDOW_SIZE, 512 * 1024).
-define(TRY_WAIT_FOR_EXIT_TIMEOUT, 1000).

start_link(Path) ->
    start_link(Path, []).

start_link(Path, Opts) ->
    Args0 = [?MODULE, [self(), Path, Opts], []],
    Args = case process_name(Path, Opts) of
               false ->
                   Args0;
               Name when is_atom(Name) ->
                   [{local, Name} | Args0]
           end,

    erlang:apply(gen_server, start_link, Args).

deliver(Pid) ->
    gen_server:cast(Pid, deliver).

write(Pid, Data) ->
    gen_server:call(Pid, {op, {write, Data}}, infinity).

close(Pid, Stream) ->
    gen_server:call(Pid, {op, {close, Stream}}, infinity).

shutdown(Pid) ->
    gen_server:call(Pid, shutdown, infinity).

%% callbacks
init([Owner, Path, Opts]) ->
    case build_config(Path, Opts) of
        {ok, Config} ->
            process_flag(trap_exit, true),
            Port = start_port(Config),

            State = #state{port = Port,
                           owner = Owner,
                           ctx = #decoding_context{},
                           stdout_ctx = make_packet_context(Config),
                           stderr_ctx = make_packet_context(Config),
                           deliver = false,
                           deliver_queue = queue:new(),
                           current_op = undefined,
                           pending_ops = queue:new(),
                           unacked_bytes = 0,
                           delivered_bytes = 0,
                           pending_ack_bytes = 0,
                           have_pending_ack = false,
                           config = Config},
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({op, Op}, From, State) ->
    {noreply, handle_op(Op, From, State)};
handle_call(shutdown, _From, State) ->
    {stop, normal, ok, State};
handle_call(Call, From, State) ->
    ?log_debug("Unexpected call ~p from ~p", [Call, From]),
    {reply, nack, State}.

handle_cast(deliver, #state{deliver = true} = State) ->
    {noreply, State};
handle_cast(deliver, #state{deliver = false} = State0) ->
    State = ack_delivered(State0),
    NewState = maybe_deliver_queued(mark_delivery_wanted(State)),
    {noreply, NewState};
handle_cast({ack_result, Bytes, ok},
            #state{unacked_bytes = Unacked,
                   pending_ack_bytes = Pending} = State) ->
    true = (Unacked >= Bytes),
    true = (Pending >= Bytes),

    NewState = State#state{unacked_bytes = Unacked - Bytes,
                           pending_ack_bytes = Pending - Bytes,
                           have_pending_ack = false},
    {noreply, maybe_send_ack(NewState)};
handle_cast({ack_result, Bytes, Error}, State) ->
    ?log_warning("Failed to ACK ~b bytes: ~p", [Bytes, Error]),
    {stop, {ack_failed, Error}, State};
handle_cast(Cast, State) ->
    ?log_debug("Unexpected cast ~p", [Cast]),
    {noreply, State}.

handle_info({Port, {data, Data}}, #state{port = Port} = State) ->
    NewState = append_data(Data, State),
    case handle_port_data(NewState) of
        {ok, NewState1} ->
            {noreply, NewState1};
        {stop, _Reason, _NewState} = Stop ->
            Stop;
        {{error, _} = Error, NewState1} ->
            %% typically this means that the process just terminated, so we
            %% wait a little bit just in case; otherwise we'd likely get epipe
            %% when trying to send shutdown to the process
            erlang:send_after(?TRY_WAIT_FOR_EXIT_TIMEOUT, self(),
                              {invalid_data, Data, Error}),
            {noreply, mark_decoding_error(NewState1)}
    end;
handle_info({invalid_data, Data, Error} = Msg, State) ->
    ?log_error("Can't decode port data: ~p. "
               "Terminating. Data:~n~s", [Error, Data]),
    {stop, Msg, State};
handle_info({Port, Msg}, #state{port = Port} = State) ->
    ?log_warning("Received unexpected message from port: ~p", [Msg]),
    {noreply, State};
handle_info({'EXIT', Port, Reason}, #state{port = Port} = State) ->
    {stop, Reason, handle_port_erlang_exit(Reason, State)};
handle_info(Msg, State) ->
    ?log_debug("Unexpected message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, #state{port = undefined}) ->
    ok;
terminate(Reason, #state{port = Port} = State) when is_port(Port) ->
    case misc:is_normal_termination(Reason) of
        true ->
            {ok, PortReason, _} = terminate_port(State),
            case PortReason of
                normal ->
                    ok;
                _ ->
                    exit(PortReason)
            end;
        false ->
            ?log_warning("Terminating with reason ~p "
                         "when port is still alive.", [Reason]),
            catch port_close(Port)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
start_port(Config) ->
    open_port({spawn_executable, goport_path()}, goport_spec(Config)).

goport_spec(Config) ->
    Args = goport_args(Config),
    Env = goport_env(Config),
    Cd = Config#config.cd,

    Spec = [stream, binary, hide,
            stderr_to_stdout,
            use_stdio,
            {args, Args},
            {env, Env}],

    case Cd of
        undefined ->
            Spec;
        _ ->
            [{cd, Cd} | Spec]
    end.

goport_path() ->
    path_config:component_path(bin, goport_name()).

goport_name() ->
    case erlang:system_info(system_architecture) of
        "win32" ->
            "goport.exe";
        _ ->
            "goport"
    end.

goport_env(Config) ->
    PortEnv = Config#config.env,
    Cmd = Config#config.cmd,
    Args = Config#config.args,

    Encoded = ejson:encode([list_to_binary(L) || L <- [Cmd | Args]]),

    [{"GOPORT_ARGS", binary_to_list(Encoded)} | PortEnv].

goport_args(Config) ->
    WindowSize = Config#config.window_size,
    GracefulShutdown = Config#config.graceful_shutdown,

    ["-graceful-shutdown=" ++ atom_to_list(GracefulShutdown),
     "-window-size=" ++ integer_to_list(WindowSize)].

build_config(Cmd, Opts) ->
    Args = proplists:get_value(args, Opts, []),
    WindowSize = proplists:get_value(window_size, Opts, ?DEFAULT_WINDOW_SIZE),
    GracefulShutdown = proplists:get_bool(graceful_shutdown, Opts),

    StderrToStdout = proplists:get_bool(stderr_to_stdout, Opts),
    Env = proplists:get_value(env, Opts, []),
    Cd = proplists:get_value(cd, Opts),
    Line = proplists:get_value(line, Opts),
    ExitStatus = proplists:get_bool(exit_status, Opts),

    %% we don't support non-binary and non-streaming modes
    true = proplists:get_bool(binary, Opts),
    true = proplists:get_bool(stream, Opts),

    case Line of
        undefined ->
            ok;
        LineLen when is_integer(LineLen) ->
            %% this needs to be smaller or equal to window_size, otherwise
            %% it's possible to deadlock
            true = (WindowSize >= LineLen)
    end,

    LeftoverOpts = [Opt || {Name, _} = Opt <- proplists:unfold(Opts),
                           not lists:member(Name,
                                            [window_size, graceful_shutdown,
                                             stderr_to_stdout, env, cd,
                                             exit_status, line, args, name,
                                             binary, stream])],

    case LeftoverOpts of
        [] ->
            Config = #config{cmd = Cmd,
                             args = Args,
                             stderr_to_stdout = StderrToStdout,
                             env = Env,
                             cd = Cd,
                             exit_status = ExitStatus,
                             line = Line,
                             window_size = WindowSize,
                             graceful_shutdown = GracefulShutdown},
            {ok, Config};
        _ ->
            {error, {unsupported_opts, proplists:get_keys(LeftoverOpts)}}
    end.

ack_delivered(#state{delivered_bytes = Delivered,
                     unacked_bytes = Unacked,
                     pending_ack_bytes = Pending} = State) ->
    true = (Unacked >= Delivered),

    NewState = State#state{delivered_bytes = 0,
                           pending_ack_bytes = Pending + Delivered},
    maybe_send_ack(NewState).

maybe_send_ack(#state{have_pending_ack = true} = State) ->
    State;
maybe_send_ack(#state{pending_ack_bytes = 0} = State) ->
    State;
maybe_send_ack(State) ->
    enqueue_ack(State).

enqueue_ack(State) ->
    Self = self(),
    Handler = fun ({ack, Bytes}, OpResult) ->
                      gen_server:cast(Self, {ack_result, Bytes, OpResult})
              end,
    NewState = State#state{have_pending_ack = true},
    enqueue_op(ack_pending, Handler, NewState).

send_shutdown(State) ->
    Self = self(),
    Handler = fun (_, OpResult) ->
                      Self ! {shutdown_result, OpResult}
              end,
    enqueue_op(shutdown, Handler, State).

handle_op(Op, From, State) ->
    Handler = fun (_Op, R) ->
                      gen_server:reply(From, R)
              end,
    enqueue_op(Op, Handler, State).

enqueue_op(Op, Handler, #state{pending_ops = Ops} = State) ->
    NewOps = queue:in({Op, Handler}, Ops),
    maybe_send_next_op(State#state{pending_ops = NewOps}).

maybe_send_next_op(#state{current_op = undefined,
                          pending_ops = Ops} = State) ->
    case queue:out(Ops) of
        {empty, _} ->
            State;
        {{value, OpHandler0}, NewOps} ->
            OpHandler = maybe_rewrite_op(OpHandler0, State),
            send_op(OpHandler, State#state{pending_ops = NewOps})
    end;
maybe_send_next_op(State) ->
    State.

maybe_rewrite_op({ack_pending, Handler}, #state{pending_ack_bytes = Bytes}) ->
    {{ack, Bytes}, Handler};
maybe_rewrite_op(OpHandler, _State) ->
    OpHandler.

send_op({Op, _} = OpHandler,
        #state{current_op = undefined,
               port = Port} = State) ->
    Data = netstring_encode(encode_op(Op)),
    Port ! {self(), {command, Data}},
    State#state{current_op = OpHandler}.

encode_op({write, Data}) ->
    ["write:", Data];
encode_op({ack, Bytes}) ->
    ["ack:", integer_to_list(Bytes)];
encode_op({close, Stream}) ->
    ["close:", encode_stream(Stream)];
encode_op(shutdown) ->
    "shutdown".

encode_stream(stdin) ->
    "stdin";
encode_stream(stdout) ->
    "stdout";
encode_stream(stderr) ->
    "stderr".

netstring_encode(Data) ->
    Size = iolist_size(Data),
    [integer_to_list(Size), $:, Data, $,].

terminate_port(State) ->
    wait_for_exit(send_shutdown(State), undefined).

wait_for_exit(#state{port = Port} = State, Reason) ->
    receive
        {shutdown_result, Result} ->
            undefined = Reason,
            NewReason = handle_shutdown_result(Result, State),
            wait_for_exit(State, NewReason);
        {Port, {data, Data}} ->
            NewState = append_data(Data, State),
            case handle_port_data(NewState) of
                {ok, S} ->
                    wait_for_exit(S, Reason);
                {stop, StopReason, StopState} ->
                    {ok, pick_exit_reason(Reason, StopReason), StopState};
                {{error, _} = Error, S} ->
                    {ok, {invalid_data, Data, Error},
                     mark_decoding_error(S)}
            end;
        {'EXIT', Port, PortReason} ->
            {ok, pick_exit_reason(Reason, PortReason),
             handle_port_erlang_exit(PortReason, State)}
    end.

pick_exit_reason(undefined, PortReason) ->
    PortReason;
pick_exit_reason(Reason, _PortReason) ->
    Reason.

handle_shutdown_result(ok, _State) ->
    undefined;
handle_shutdown_result(Other, #state{port = Port}) ->
    ?log_error("Port returned an error to shutdown request: ~p. "
               "Forcefully closing the port.", [Other]),
    R = (catch port_close(Port)),
    ?log_debug("port_close result: ~p", [R]),
    {shutdown_failed, Other}.

handle_port_erlang_exit(Reason, State) ->
    case Reason =/= normal of
        true ->
            ?log_error("Port terminated abnormally: ~p", [Reason]);
        false ->
            ?log_debug("Port terminated")
    end,

    %% This is typically done on reception of {exit_status, _}. But if port
    %% terminates not because the underlying process died, we won't get that
    %% message. But if we already flushed everything, nothing will happen.
    NewState = flush_everything(State),
    NewState#state{port = undefined}.

flush_everything(State) ->
    functools:chain(State,
                    [maybe_interrupt_pending_ops(_),
                     flush_packet_context(stdout, _),
                     flush_packet_context(stderr, _),
                     maybe_flush_invalid_data(_),
                     flush_queue(_)]).

flush_queue(#state{deliver_queue = Queue} = State) ->
    lists:foreach(
      fun ({Msg, _}) ->
              deliver_message(Msg, State)
      end, queue:to_list(Queue)),

    State#state{deliver_queue = queue:new()}.

maybe_interrupt_pending_ops(#state{current_op = undefined} = State) ->
    State;
maybe_interrupt_pending_ops(#state{current_op = Current,
                                   pending_ops = Pending} = State) ->
    lists:foreach(
      fun ({Op, Handler}) ->
              Handler(Op, {error, interrupted})
      end, [Current | queue:to_list(Pending)]),

    State#state{current_op = undefined,
                pending_ops = queue:new()}.

maybe_flush_invalid_data(State) ->
    case have_decoding_error(State) of
        true ->
            flush_invalid_data(State);
        false ->
            State
    end.

flush_invalid_data(#state{ctx = Ctx} = State) ->
    #decoding_context{data = Data} = Ctx,

    case byte_size(Data) > 0 of
        true ->
            NewCtx = Ctx#decoding_context{data = <<>>},
            NewState = State#state{ctx = NewCtx},

            %% pretend it's the regular port output; that way, we'll even try
            %% to break it into lines if requested
            NewState1 = handle_port_output(stdout, Data, NewState),
            flush_packet_context(stdout, NewState1);
        false ->
            State
    end.

handle_port_data(State) ->
    case have_decoding_error(State) of
        true ->
            {ok, State};
        false ->
            do_handle_port_data(State)
    end.

do_handle_port_data(#state{ctx = Ctx} = State) ->
    {Result, NewCtx} = netstring_decode(Ctx),
    NewState = State#state{ctx = NewCtx},

    case Result of
        {ok, Packet} ->
            case handle_port_packet(Packet, NewState) of
                {ok, NewState1} ->
                    do_handle_port_data(NewState1);
                {stop, _, _} = Stop ->
                    Stop
            end;
        {error, need_more_data} ->
            {ok, NewState};
        {error, _} = Error ->
            {Error, NewState}
    end.

handle_port_packet(Packet, State) ->
    case binary:split(Packet, <<":">>) of
        [Type, Rest] ->
            process_port_packet(Type, Rest, State);
        [Type] ->
            process_port_packet(Type, <<>>, State)
    end.

process_port_packet(<<"ok">>, <<>>, State) ->
    handle_op_response(ok, State);
process_port_packet(<<"error">>, Error, State) ->
    handle_op_response({error, Error}, State);
process_port_packet(<<"stdout">>, Data, State) ->
    handle_port_output(stdout, Data, State);
process_port_packet(<<"stderr">>, Data, State) ->
    handle_port_output(stderr, Data, State);
process_port_packet(<<"eof">>, Data, State) ->
    handle_eof(Data, State);
process_port_packet(<<"exit">>, Data, State) ->
    handle_process_exit(Data, State);
process_port_packet(Type, Arg, State) ->
    ?log_warning("Unrecognized packet from port:~nType: ~s~nArg: ~s",
                 [Type, Arg]),
    {stop, {unrecognized_packet, Type, Arg}, State}.

handle_process_exit(StatusBinary, #state{port = Port} = State) ->
    Status = binary_to_integer(StatusBinary),

    NewState = flush_everything(State),
    maybe_deliver_exit_status(Status, NewState),
    ?log_info("Port exited with status ~b.", [Status]),

    port_close(Port),
    receive
        {'EXIT', Port, Reason} ->
            {stop, Reason, NewState#state{port = undefined}}
    end.

maybe_deliver_exit_status(Status, #state{config = Config} = State) ->
    case Config#config.exit_status of
        true ->
            deliver_message({exit_status, Status}, State);
        false ->
            ok
    end.

handle_eof(Data, State) ->
    case binary:split(Data, <<":">>) of
        [Stream] ->
            ?log_debug("Stream '~s' closed", [Stream]),
            {ok, State};
        [Stream, Error] ->
            ?log_warning("Stream '~s' closed with error: ~s", [Stream, Error]),
            {stop, {stream_error, Stream, Error}, State}
    end.

handle_op_response(Response, #state{current_op = {Op, Handler}} = State) ->
    Handler(Op, Response),
    NewState = State#state{current_op = undefined},
    {ok, maybe_send_next_op(NewState)}.

handle_port_output(Stream, Data, State) ->
    {Packets, NewState0} = packetize(Stream, Data,
                                     update_unacked_bytes(Data, State)),
    NewState = queue_packets(Stream, Packets, NewState0),
    {ok, maybe_deliver_queued(NewState)}.

update_unacked_bytes(Data, #state{unacked_bytes = Unacked} = State) ->
    State#state{unacked_bytes = Unacked + byte_size(Data)}.

queue_packets(_Stream, [], State) ->
    State;
queue_packets(Stream, [Packet|Rest], State) ->
    queue_packets(Stream, Rest, queue_packet(Stream, Packet, State)).

queue_packet(Stream, {Msg0, Size}, #state{deliver_queue = Queue} = State) ->
    Msg = make_output_message(Stream, Msg0, State),
    State#state{deliver_queue = queue:in({Msg, Size}, Queue)}.

make_output_message(Stream, Data, #state{config = Config}) ->
    case Config#config.stderr_to_stdout of
        true ->
            {data, Data};
        false ->
            {data, {Stream, Data}}
    end.

maybe_deliver_queued(#state{deliver = false} = State) ->
    State;
maybe_deliver_queued(#state{deliver = true,
                            deliver_queue = Queue,
                            delivered_bytes = Delivered} = State) ->
    case queue:out(Queue) of
        {empty, _} ->
            State;
        {{value, {Msg, Size}}, NewQueue} ->
            deliver_message(Msg, State),
            State#state{deliver_queue = NewQueue,
                        delivered_bytes = Delivered + Size,
                        deliver = false}
    end.

deliver_message(Message, #state{owner = Owner}) ->
    Owner ! {self(), Message}.

mark_delivery_wanted(#state{deliver = false} = State) ->
    State#state{deliver = true}.

netstring_decode(#decoding_context{length = undefined,
                                   data = Data} = Ctx) ->
    TrimmedData = string:trim(Data, leading),
    case get_length(TrimmedData) of
        {ok, Len, RestData} ->
            NewCtx = Ctx#decoding_context{length = Len,
                                          data = RestData},
            netstring_decode(NewCtx);
        Error ->
            NewCtx = Ctx#decoding_context{data = TrimmedData},
            {Error, NewCtx}
    end;
netstring_decode(#decoding_context{length = Length,
                                   data = Data} = Ctx) ->
    Size = byte_size(Data),
    %% extra byte for trailing comma
    Need = Length + 1,
    case Size >= Need of
        true ->
            case binary:at(Data, Need - 1) of
                $, ->
                    Packet = binary:part(Data, 0, Length),
                    RestData = binary:part(Data, Need, Size - Need),
                    NewCtx = Ctx#decoding_context{length = undefined,
                                                  data = RestData},
                    {{ok, Packet}, NewCtx};
                _ ->
                    {{error, invalid_netstring}, Ctx}
            end;
        false ->
            {{error, need_more_data}, Ctx}
    end.

append_data(MoreData, #state{ctx = Ctx} = State) ->
    #decoding_context{data = Data} = Ctx,

    NewData = <<Data/binary, MoreData/binary>>,
    NewCtx = Ctx#decoding_context{data = NewData},
    State#state{ctx = NewCtx}.

mark_decoding_error(#state{ctx = Ctx} = State) ->
    NewCtx = Ctx#decoding_context{length = decoding_error},
    State#state{ctx = NewCtx}.

have_decoding_error(#state{ctx = Ctx}) ->
    Ctx#decoding_context.length =:= decoding_error.

get_length(Data) ->
    Limit = 100,
    Size = byte_size(Data),

    case binary:match(Data, <<":">>, [{scope, {0, min(Limit, Size)}}]) of
        nomatch ->
            get_length_nomatch(Limit, Size);
        {Pos, 1} ->
            extract_length(Pos, Data, Size)
    end.

get_length_nomatch(Limit, Size)
  when Size < Limit ->
    {error, need_more_data};
get_length_nomatch(_, _) ->
    {error, invalid_netstring}.

extract_length(Pos, Data, DataSize) ->
    Length = binary:part(Data, 0, Pos),

    try
        binary_to_integer(Length)
    of L ->
            RestStart = Pos + 1,
            RestData = binary:part(Data, RestStart, DataSize - RestStart),
            {ok, L, RestData}
    catch
        error:badarg ->
            {error, invalid_netstring}
    end.

process_name(Path, Opts) ->
    case proplists:get_value(name, Opts) of
        false ->
            false;
        Other ->
            BaseName = case Other of
                           undefined ->
                               filename:basename(Path);
                           Name ->
                               atom_to_list(Name)
                       end,

            list_to_atom(BaseName ++ "-goport")
    end.

make_packet_context(#config{line = undefined}) ->
    undefined;
make_packet_context(#config{line = MaxSize}) ->
    #line_context{max_size = MaxSize}.

packet_context(stdout) ->
    #state.stdout_ctx;
packet_context(stderr) ->
    #state.stderr_ctx.

flush_packet_context(Stream, State) ->
    N = packet_context(Stream),

    case do_flush_packet_context(element(N, State)) of
        false ->
            State;
        {Msg, NewCtx} ->
            NewState = setelement(N, State, NewCtx),
            queue_packet(Stream, Msg, NewState)
    end.

do_flush_packet_context(undefined) ->
    false;
do_flush_packet_context(#line_context{data = <<>>}) ->
    false;
do_flush_packet_context(#line_context{data = Data} = Ctx) ->
    Msg = {{noeol, Data}, byte_size(Data)},
    {Msg, Ctx#line_context{data = <<>>}}.

packetize(Stream, NewData, State) ->
    N = packet_context(Stream),

    Ctx = element(N, State),
    {Packets, NewCtx} = do_packetize(NewData, Ctx),
    {Packets, setelement(N, State, NewCtx)}.

do_packetize(NewData, undefined) ->
    {[{NewData, byte_size(NewData)}], undefined};
do_packetize(NewData, #line_context{max_size = MaxSize,
                                    data = PrevData} = Ctx) ->
    Data = <<PrevData/binary, NewData/binary>>,

    Pattern = binary:compile_pattern([<<"\n">>, <<"\r\n">>]),
    {Packets, LeftoverData} = extract_lines_loop(Data, Pattern, MaxSize),
    {Packets, Ctx#line_context{data = LeftoverData}}.

extract_lines_loop(Data, Pattern, MaxSize) ->
    case extract_line(Data, Pattern, MaxSize) of
        {ok, Packet, RestData} ->
            {Packets, LeftoverData} =
                extract_lines_loop(RestData, Pattern, MaxSize),
            {[Packet | Packets], LeftoverData};
        need_more ->
            {[], Data}
    end.

extract_line(Data, Pattern, MaxSize) when is_binary(Data) ->
    Size = byte_size(Data),
    Limit = min(MaxSize, Size),

    case binary:match(Data, Pattern, [{scope, {0, Limit}}]) of
        nomatch ->
            case Limit =:= MaxSize of
                true ->
                    ToAck = MaxSize,
                    {Line, Rest} = split_at(Data, MaxSize),
                    {ok, {{noeol, Line}, ToAck}, Rest};
                false ->
                    need_more
            end;
        {Pos, Len} ->
            ToAck = Pos + Len,
            {Line0, Rest} = split_at(Data, ToAck),

            Line = binary:part(Line0, 0, Pos),
            {ok, {{eol, Line}, ToAck}, Rest}
    end.

split_at(Binary, Pos) ->
    Size = byte_size(Binary),
    case Size =< Pos of
        true ->
            {Binary, <<>>};
        false ->
            X = binary:part(Binary, 0, Pos),
            Y = binary:part(Binary, Pos, Size - Pos),
            {X, Y}
    end.
