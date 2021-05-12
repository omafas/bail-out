%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_port_server).

-behavior(gen_server).

-include("ns_common.hrl").

%% API
-export([start_link/1, start_link_named/2,
         is_active/1, activate/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% Keep on 1KiB worth of recent messages to log if process crashes.
-define(KEEP_MESSAGES_BYTES, 1024).

%% we're passing port stdout/stderr messages to log after delay of
%% INTERVAL milliseconds. Dropping messages once MAX_MESSAGES is
%% reached. Thus we're limiting rate of messages to
%% MAX_MESSAGES/INTERVAL msg/millisecond
%%
%% The following gives us 300 lines/second in bursts up to 60 lines
-define(MAX_MESSAGES, 60). % Max messages per interval
-define(INTERVAL, 200). % Interval over which to throttle

%% Server state
-record(state, {port :: port() | pid() | undefined,
                params :: tuple(),
                messages,
                logger :: atom(),
                log_tref :: undefined | timer:tref(),
                log_buffer = [],
                dropped=0 :: non_neg_integer()}).

%% API

start_link(Fun) ->
    gen_server:start_link(?MODULE,
                          Fun, []).
start_link_named(Name, Fun) ->
    gen_server:start_link({local, Name}, ?MODULE, Fun, []).

is_active(Pid) ->
    gen_server:call(Pid, is_active, infinity).

activate(Pid) ->
    gen_server:call(Pid, activate, infinity).

%% gen_server callbacks
init(Fun) ->
    {Name, _Cmd, _Args, Opts} = Params = Fun(),
    process_flag(trap_exit, true), % Make sure terminate gets called
    Opts2 = lists:foldl(fun proplists:delete/2, Opts,
                        [port_server_dont_start, log]),
    DontStart = proplists:get_bool(port_server_dont_start, Opts),
    Log = proplists:get_value(log, Opts),
    Params2 = erlang:setelement(4, Params, Opts2),

    Logger =
        case Log of
            undefined ->
                undefined;
            _ when is_list(Log) ->
                Sink = Name,

                ok = ns_server:start_disk_sink(Sink, Log),

                ale:stop_logger(Name),
                ok = ale:start_logger(Name, debug, ale_noop_formatter),

                ok = ale:add_sink(Name, Sink, debug),

                Name
        end,

    State = #state{logger = Logger,
                   port = undefined,
                   params = Params2,
                   messages = ringbuffer:new(?KEEP_MESSAGES_BYTES)},

    Port = case DontStart of
               false -> port_open(Params2, State);
               true -> undefined
           end,
    {ok, State#state{port = Port}}.

handle_info({send_to_port, Msg}, #state{port = undefined} = State) ->
    ?log_debug("Got send_to_port when there's no port running yet. Will kill myself."),
    {stop, {unexpected_send_to_port, {send_to_port, Msg}}, State};
handle_info({send_to_port, Msg}, State) ->
    ?log_debug("Sending the following to port: ~p", [Msg]),
    port_write(State#state.port, Msg),
    {noreply, State};
handle_info({Port, {data, Data}}, #state{port = Port,
                                         logger = Logger} = State) ->
    %% Store the last messages in case of a crash
    Msg = extract_message(Data),
    Messages = ringbuffer:add(Msg, byte_size(Msg), State#state.messages),
    State1 = State#state{messages=Messages},

    NewState =
        case Logger of
            undefined ->
                {Buf, Dropped} = case {State#state.log_buffer, State#state.dropped} of
                                     {B, D} when length(B) < ?MAX_MESSAGES ->
                                         {[Msg|B], D};
                                     {B, D} ->
                                         {B, D + 1}
                                 end,
                TRef = case State#state.log_tref of
                           undefined ->
                               erlang:send_after(?INTERVAL, self(), log);
                           T ->
                               T
                       end,
                State1#state{log_buffer=Buf, log_tref=TRef, dropped=Dropped};
            _ ->
                %% This makes sure that disk sink's buffer is flushed. Since
                %% logging to it asynchronous, this is needed for goport's
                %% flow control to actually do its job.
                ale:sync_sink(Logger),
                ale:debug(Logger, Msg),
                State1
        end,

    port_deliver(Port),
    {noreply, NewState};
handle_info(log, State) ->
    State1 = log(State),
    {noreply, State1};
handle_info({_Port, {exit_status, 0} = Msg}, State) ->
    ?log_info("Got ~p from port ~p. Exiting normally", [Msg, port_name(State)]),
    {stop, normal, State};
handle_info({_Port, {exit_status, Status}}, State) ->
    ns_crash_log:record_crash({port_name(State),
                               Status,
                               get_death_messages(State)}),
    {stop, {abnormal, Status}, State};
handle_info({'EXIT', Port, Reason} = Exit, #state{port=Port} = State) ->
    ?log_error("Got unexpected exit signal from port: ~p. Exiting.", [Exit]),
    {stop, Reason, State}.

handle_call(is_active, _From, #state{port = Port} = State) ->
    {reply, Port =/= undefined, State};

handle_call(activate, _From, #state{port = undefined,
                                    params = Params} = State) ->
    State2 = State#state{port = port_open(Params, State)},
    {reply, ok, State2};
handle_call(activate, _From, State) ->
    {reply, {error, already_active}, State}.

handle_cast(unhandled, unhandled) ->
    erlang:exit(unhandled).

wait_for_child_death(State) ->
    receive
        {Port, _} = Msg when Port =:= State#state.port ->
            wait_for_child_death_process_info(Msg, State);
        {'EXIT', Port, _} = Msg when Port =:= State#state.port ->
            wait_for_child_death_process_info(Msg, State);
        log = Msg ->
            wait_for_child_death_process_info(Msg, State);
        X ->
            ?log_error("Ignoring unknown message while shutting down child: ~p~n", [X]),
            wait_for_child_death(State)
    end.

wait_for_child_death_process_info(Msg, State) ->
    case handle_info(Msg, State) of
        {noreply, State2} -> wait_for_child_death(State2);
        {stop, _, State2} -> State2
    end.

terminate(Reason, #state{port = undefined} = _State) ->
    ?log_debug("doing nothing in terminate(~p) because port is not active", [Reason]),
    ok;
terminate(shutdown, #state{port = Port} = State) ->
    ?log_debug("Shutting down port ~p", [port_name(State)]),
    port_shutdown(Port),
    State2 = wait_for_child_death(State),
    ?log_debug("~p has exited", [port_name(State)]),
    log(State2); % Log any remaining messages
terminate(_Reason, State) ->
    log(State). % Log any remaining messages


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% Internal functions

%% @doc Fetch up to Max messages from the queue, discarding any more
%% received up to Timeout. The goal is to remove messages from the
%% queue as fast as possible if the port is spamming, avoiding
%% spamming the log server.
format_lines(Name, Lines) ->
    Prefix = io_lib:format("~p~p: ", [Name, self()]),
    [[Prefix, Line, $\n] || Line <- Lines].

log(#state{logger = undefined} = State) ->
    case State#state.log_buffer of
        [] ->
            ok;
        Buf ->
            ?log_info(format_lines(port_name(State), lists:reverse(Buf))),
            case State#state.dropped of
                0 ->
                    ok;
                Dropped ->
                    ?log_warning("Dropped ~p log lines from ~p",
                                 [Dropped, port_name(State)])
            end
    end,
    State#state{log_tref=undefined, log_buffer=[], dropped=0};
log(_) ->
    ok.

port_open({_Name, Cmd, Args, OptsIn}, #state{logger = Logger}) ->
    %% Incoming options override existing ones (specified in proplists docs)
    Opts0 = OptsIn ++ [{args, Args}, exit_status,
                       stream, binary, stderr_to_stdout],

    WriteDataArg = proplists:get_value(write_data, Opts0),
    Opts1 = lists:keydelete(write_data, 1, Opts0),
    Opts3 = case lists:delete(ns_server_no_stderr_to_stdout, Opts1) of
                Opts1 ->
                    Opts1;
                Opts2 ->
                    lists:delete(stderr_to_stdout, Opts2)
            end,
    ViaGoport = proplists:get_value(via_goport, Opts3, false),
    Opts4 = proplists:delete(via_goport, Opts3),

    %% don't split port output into lines if all we need to do is to redirect
    %% it into a file
    Opts = case Logger of
               undefined ->
                   [{line, 8192} | Opts4];
               _ ->
                   Opts4
           end,

    Port =
        case ViaGoport of
            true ->
                {ok, P} = goport:start_link(Cmd, Opts),
                P;
            false ->
                erlang:open_port({spawn_executable, Cmd}, Opts)
        end,

    case WriteDataArg of
        undefined ->
            ok;
        Data ->
            port_write(Port, Data)
    end,

    %% initiate initial delivery
    port_deliver(Port),

    Port.

port_name(#state{params = Params}) ->
    {Name, _, _, _} = Params,
    Name.

port_write(Port, Data) when is_pid(Port) ->
    ok = goport:write(Port, Data);
port_write(Port, Data) when is_port(Port) ->
    Port ! {self(), {command, Data}}.

port_shutdown(Port) when is_pid(Port) ->
    ok = goport:shutdown(Port);
port_shutdown(Port) when is_port(Port) ->
    ShutdownCmd = misc:get_env_default(ns_babysitter,
                                       port_shutdown_command, "shutdown"),
    ?log_debug("Shutdown command: ~p", [ShutdownCmd]),
    port_command(Port, [ShutdownCmd, $\n]).

port_deliver(Port) when is_pid(Port) ->
    goport:deliver(Port);
port_deliver(Port) when is_port(Port) ->
    ok.

extract_message({eol, Msg}) ->
    Msg;
extract_message({noeol, Msg}) ->
    Msg;
extract_message(Msg) ->
    Msg.

get_death_messages(#state{messages = Messages, logger = undefined}) ->
    %% we use line splitting for the processes whose output is not forwarded
    %% to disk
    misc:intersperse(ringbuffer:to_list(Messages), $\n);
get_death_messages(#state{messages = Messages}) ->
    %% Since the messages here are not split in lines, they can be quite big
    %% and hence the total size of them can be quite a bit larger than
    %% ?KEEP_MESSAGES_BYTES. So we manually split them into lines here and
    %% select as few as possible.
    Combined = iolist_to_binary(ringbuffer:to_list(Messages)),
    Lines = binary:split(Combined, [<<"\n">>, <<"\r\n">>], [global]),

    R = lists:foldl(
          fun (Line, Acc) ->
                  ringbuffer:add(Line, byte_size(Line), Acc)
          end, ringbuffer:new(?KEEP_MESSAGES_BYTES), Lines),

    misc:intersperse(ringbuffer:to_list(R), $\n).
