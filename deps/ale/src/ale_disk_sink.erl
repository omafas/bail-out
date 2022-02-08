%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ale_disk_sink).

-behaviour(gen_server).

%% API
-export([start_link/2, start_link/3, meta/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/file.hrl").
-include("ale.hrl").

-record(state, {
          buffer :: binary(),
          buffer_size :: integer(),
          outstanding_size :: integer(),
          buffer_overflow :: boolean(),
          dropped_messages :: non_neg_integer(),
          flush_timer :: undefined | reference(),
          worker :: undefined | pid(),

          batch_size :: pos_integer(),
          batch_timeout :: pos_integer(),
          buffer_size_max :: pos_integer()
         }).

-record(worker_state, {
          sink_name :: atom(),
          path :: string(),
          file :: undefined | file:io_device(),
          file_size :: undefined | integer(),
          file_inode :: undefined | integer(),
          parent :: pid(),

          rotation_size :: non_neg_integer(),
          rotation_num_files :: pos_integer(),
          rotation_compress :: boolean(),
          rotation_check_interval :: non_neg_integer()
         }).

start_link(Name, Path) ->
    start_link(Name, Path, []).

start_link(Name, Path, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Path, Opts], []).

meta() ->
    [{async, true},
     {type, preformatted}].

init([Name, Path, Opts]) ->
    process_flag(trap_exit, true),

    BatchSize = proplists:get_value(batch_size, Opts, 524288),
    BatchTimeout = proplists:get_value(batch_timeout, Opts, 1000),
    BufferSizeMax = proplists:get_value(buffer_size_max, Opts, BatchSize * 10),

    RotationConf = proplists:get_value(rotation, Opts, []),

    RotSize = proplists:get_value(size, RotationConf, 10485760),
    RotNumFiles = proplists:get_value(num_files, RotationConf, 20),
    RotCompress = proplists:get_value(compress, RotationConf, true),
    RotCheckInterval = proplists:get_value(check_interval, RotationConf, 10000),

    ok = remove_unnecessary_log_files(Path, RotNumFiles),

    WorkerState = #worker_state{sink_name = Name,
                                path = Path,
                                parent = self(),
                                rotation_size = RotSize,
                                rotation_num_files = RotNumFiles,
                                rotation_compress = RotCompress,
                                rotation_check_interval = RotCheckInterval},
    Worker = spawn_worker(WorkerState),

    State = #state{buffer = <<>>,
                   buffer_size = 0,
                   outstanding_size = 0,
                   buffer_overflow = false,
                   dropped_messages = 0,
                   batch_size = BatchSize,
                   batch_timeout = BatchTimeout,
                   buffer_size_max = BufferSizeMax,
                   worker = Worker},

    {ok, State}.

handle_call(sync, From, #state{worker = Worker} = State) ->
    NewState = flush_buffer(State),

    Parent = self(),
    proc_lib:spawn_link(
      fun () ->
              gen_server:reply(From, gen_server:call(Worker, sync, infinity)),
              erlang:unlink(Parent)
      end),

    {noreply, NewState};
handle_call(Request, _From, State) ->
    {stop, {unexpected_call, Request}, State}.

handle_cast({log, Msg}, State) ->
    {noreply, log_msg(Msg, State)};
handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(flush_buffer, State) ->
    {noreply, flush_buffer(State)};
handle_info({written, Written}, #state{buffer_size = BufferSize,
                                       outstanding_size = OutstandingSize,
                                       buffer_overflow = Overflow,
                                       buffer_size_max = BufferSizeMax,
                                       dropped_messages = Dropped} = State) ->
    true = OutstandingSize >= Written,
    NewOutstandingSize = OutstandingSize - Written,

    NewTotalSize = BufferSize + NewOutstandingSize,
    NewState =
        case Overflow =:= true andalso 2 * NewTotalSize =< BufferSizeMax of
            true ->
                Msg = <<"Dropped ",
                        (list_to_binary(integer_to_list(Dropped)))/binary,
                        " messages\n">>,
                State1 = do_log_msg(Msg, State),
                State1#state{buffer_overflow = false,
                             dropped_messages = 0,
                             outstanding_size = NewOutstandingSize};
            false ->
                State#state{outstanding_size = NewOutstandingSize}
        end,
    {noreply, NewState};
handle_info({'EXIT', Worker, Reason}, #state{worker = Worker} = State) ->
    {stop, {worker_died, Worker, Reason}, State#state{worker = undefined}};
handle_info({'EXIT', Pid, Reason}, State) ->
    {stop, {child_died, Pid, Reason}, State};
handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

terminate(_Reason, #state{worker = Worker} = State) when Worker =/= undefined ->
    flush_buffer(State),
    ok = gen_server:call(Worker, sync, infinity),
    exit(Worker, kill),
    ok;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal functions
log_msg(_Msg, #state{buffer_overflow = true,
                     dropped_messages = Dropped} = State)->
    State#state{dropped_messages = Dropped + 1};
log_msg(_Msg, #state{buffer_size = BufferSize,
                     outstanding_size = OutstandingSize,
                     buffer_size_max = BufferSizeMax} = State)
  when BufferSize + OutstandingSize >= BufferSizeMax ->
    Msg = <<"Dropping consequent messages on the floor because of buffer overflow\n">>,
    do_log_msg(Msg, State#state{buffer_overflow = true,
                                dropped_messages = 1});
log_msg(Msg, State) ->
    do_log_msg(Msg, State).

do_log_msg(Msg, #state{buffer = Buffer,
                       buffer_size = BufferSize} = State) ->
    NewBuffer = <<Buffer/binary, Msg/binary>>,
    NewBufferSize = BufferSize + byte_size(Msg),

    NewState = State#state{buffer = NewBuffer,
                           buffer_size = NewBufferSize},

    maybe_flush_buffer(NewState).

maybe_flush_buffer(#state{buffer_size = BufferSize,
                          batch_size = BatchSize} = State) ->
    case BufferSize >= BatchSize of
        true ->
            flush_buffer(State);
        false ->
            maybe_arm_flush_timer(State)
    end.

flush_buffer(#state{worker = Worker,
                    buffer = Buffer,
                    buffer_size = BufferSize,
                    outstanding_size = OutstandingSize} = State) ->
    Worker ! {write, Buffer, BufferSize},
    cancel_flush_timer(State#state{buffer = <<>>,
                                   buffer_size = 0,
                                   outstanding_size = OutstandingSize + BufferSize}).

maybe_arm_flush_timer(#state{flush_timer = undefined,
                             batch_timeout = Timeout} = State) ->
    TRef = erlang:send_after(Timeout, self(), flush_buffer),
    State#state{flush_timer = TRef};
maybe_arm_flush_timer(State) ->
    State.

cancel_flush_timer(#state{flush_timer = TRef} = State) when TRef =/= undefined ->
    erlang:cancel_timer(TRef),
    receive
        flush_buffer -> ok
    after
        0 -> ok
    end,
    State#state{flush_timer = undefined};
cancel_flush_timer(State) ->
    State.

rotate_files(#worker_state{path = Path,
                           rotation_num_files = NumFiles,
                           rotation_compress = Compress}) ->
    rotate_files_loop(Path, Compress, NumFiles - 1).

rotate_files_loop(Path, _Compress, 0) ->
    case file:delete(Path) of
        ok ->
            ok;
        Error ->
            Error
    end;
rotate_files_loop(Path, _Compress, 1) ->
    To = Path ++ ".1",
    case do_rotate_file(Path, To, false) of
        ok ->
            ok;
        Error ->
            Error
    end;
rotate_files_loop(Path, Compress, N) ->
    From = Path ++ "." ++ integer_to_list(N - 1),
    To = Path ++ "." ++ integer_to_list(N),

    R = case do_rotate_file(From, To, Compress) of
            ok ->
                ok;
            {error, enoent} ->
                %% also try with flipped Compress flag to rotate files from
                %% invocations with different parameters
                case do_rotate_file(From, To, not Compress) of
                    ok ->
                        ok;
                    {error, enoent} ->
                        ok;
                    Error ->
                        Error
                end;
            Error ->
                Error
        end,

    case R of
        ok ->
            rotate_files_loop(Path, Compress, N - 1);
        Error1 ->
            Error1
    end.

do_rotate_file(From0, To0, Compress) ->
    {From, To, Cleanup} =
        case Compress of
            true ->
                {From0 ++ ".gz", To0 ++ ".gz", To0};
            false ->
                {From0, To0, To0 ++ ".gz"}
        end,

    case file:rename(From, To) of
        ok ->
            %% we successfully moved the file; let's ensure that there's no
            %% leftover file with flipped Compress flag from some previous
            %% invocation
            file:delete(Cleanup),
            ok;
        Error ->
            Error
    end.

maybe_rotate_files(#worker_state{sink_name = Name,
                                 file_size = FileSize,
                                 rotation_size = RotSize} = State)
  when RotSize =/= 0, FileSize >= RotSize ->
    time_stat(Name, rotation_time,
              fun () ->
                      ok = rotate_files(State),
                      ok = maybe_compress_post_rotate(State),
                      open_log_file(State)
              end);
maybe_rotate_files(State) ->
    State.

open_log_file(#worker_state{path = Path,
                            file = OldFile} = State) ->
    case OldFile of
        undefined ->
            ok;
        _ ->
            file:close(OldFile)
    end,

    {ok, File, #file_info{size = Size,
                          inode = Inode}} = open_file(Path),
    State#worker_state{file = File,
                       file_size = Size,
                       file_inode = Inode}.

check_log_file(#worker_state{path = Path,
                             file_inode = FileInode} = State) ->
    case file:read_file_info(Path) of
        {error, enoent} ->
            open_log_file(State);
        {ok, #file_info{inode = NewInode}} ->
            case FileInode =:= NewInode of
                true ->
                    State;
                false ->
                    open_log_file(State)
            end
    end.

open_file(Path) ->
    case file:read_file_info(Path) of
        {ok, Info} ->
            do_open_file(Path, Info);
        {error, enoent} ->
            case file:open(Path, [raw, append, binary]) of
                {ok, File} ->
                    file:close(File),
                    open_file(Path);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

do_open_file(Path, #file_info{inode = Inode}) ->
    case file:open(Path, [raw, append, binary]) of
        {ok, File} ->
            case file:read_file_info(Path) of
                {ok, #file_info{inode = Inode} = Info} -> % Inode is bound
                    {ok, File, Info};
                {ok, OtherInfo} ->
                    file:close(File),
                    do_open_file(Path, OtherInfo);
                Error ->
                    file:close(File),
                    Error
            end;
        Error ->
            Error
    end.

maybe_compress_post_rotate(#worker_state{sink_name = Name,
                                         path = Path,
                                         rotation_num_files = NumFiles,
                                         rotation_compress = true})
  when NumFiles > 1 ->
    UncompressedPath = Path ++ ".1",
    CompressedPath = Path ++ ".1.gz",

    time_stat(Name, compression_time,
              fun () ->
                      compress_file(UncompressedPath, CompressedPath)
              end);
maybe_compress_post_rotate(_) ->
    ok.

-define(GZIP_FORMAT, 16#10).
compress_file(FromPath, ToPath) ->
    {ok, From} = file:open(FromPath, [raw, binary, read]),

    try
        {ok, To} = file:open(ToPath, [raw, binary, write]),
        Z = zlib:open(),

        try
            ok = zlib:deflateInit(Z, default, deflated,
                                  15 bor ?GZIP_FORMAT, 8, default),
            compress_file_loop(From, To, Z),
            ok = zlib:deflateEnd(Z),
            ok = file:delete(FromPath)
        after
            file:close(To),
            zlib:close(Z)
        end
    after
        file:close(From)
    end.

compress_file_loop(From, To, Z) ->
    {Compressed, Continue} =
        case file:read(From, 1024 * 1024) of
            eof ->
                {zlib:deflate(Z, <<>>, finish), false};
            {ok, Data} ->
                {zlib:deflate(Z, Data), true}
        end,

    case iolist_to_binary(Compressed) of
        <<>> ->
            ok;
        CompressedBinary ->
            ok = file:write(To, CompressedBinary)
    end,

    case Continue of
        true ->
            compress_file_loop(From, To, Z);
        false ->
            ok
    end.

spawn_worker(WorkerState) ->
    proc_lib:spawn_link(
      fun () ->
              worker_init(WorkerState)
      end).

worker_init(#worker_state{rotation_check_interval = RotCheckInterval} = State0) ->
    case RotCheckInterval > 0 of
        true ->
            erlang:send_after(RotCheckInterval, self(), check_file);
        false ->
            ok
    end,
    worker_loop(open_log_file(State0)).

worker_loop(State) ->
    NewState =
        receive
            {write, Data0, DataSize0} ->
                {Data, DataSize} = receive_more_writes(Data0, DataSize0),
                write_data(Data, DataSize, State);
            check_file ->
                erlang:send_after(State#worker_state.rotation_check_interval,
                                  self(), check_file),
                check_log_file(State);
            {'$gen_call', From, sync} ->
                gen_server:reply(From, ok),
                State;
            Msg ->
                exit({unexpected_msg, Msg})
        end,

    worker_loop(NewState).

receive_more_writes(Data, DataSize) ->
    receive
        {write, MoreData, MoreDataSize} ->
            receive_more_writes(<<Data/binary, MoreData/binary>>,
                                DataSize + MoreDataSize)
    after
        0 ->
            {Data, DataSize}
    end.

write_data(Data, DataSize,
           #worker_state{sink_name = Name,
                         file = File,
                         file_size = FileSize,
                         parent = Parent} = State) ->
    broadcast_stat(Name, write_size, DataSize),

    time_stat(Name, write_time,
              fun () ->
                      ok = file:write(File, Data)
              end),

    Parent ! {written, DataSize},
    NewState = State#worker_state{file_size = FileSize + DataSize},
    maybe_rotate_files(NewState).

remove_unnecessary_log_files(LogFilePath, NumFiles) ->
    Dir = filename:dirname(LogFilePath),
    Name = filename:basename(LogFilePath),
    {ok, RegExp} = re:compile("^" ++ Name ++ "\.([1-9][0-9]*)(?:\.gz)?$"),

    {ok, DirFiles} = file:list_dir(Dir),
    lists:foreach(
      fun (File) ->
              FullPath = filename:join(Dir, File),
              case filelib:is_regular(FullPath) of
                  true ->
                      case re:run(File, RegExp,
                                  [{capture, all_but_first, list}]) of
                          {match, [I]} ->
                              case list_to_integer(I) >= NumFiles of
                                  true ->
                                      file:delete(FullPath);
                                  false ->
                                      ok
                              end;
                          _ ->
                              ok
                      end;
                  false ->
                      ok
              end
      end, DirFiles).

broadcast_stat(Name, StatName, Value) ->
    gen_event:notify(ale_stats_events, {{?MODULE, Name}, StatName, Value}).

time_stat(Name, StatName, Body) ->
    StartTS = os:timestamp(),
    R = Body(),
    EndTS = os:timestamp(),

    broadcast_stat(Name, StatName, timer:now_diff(EndTS, StartTS)),
    R.
