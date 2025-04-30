%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(ale_disk_sink).

-behaviour(gen_server).

%% API
-export([start_link/2, start_link/3, meta/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/file.hrl").
-include("ale.hrl").

%% Schedule to run once a day with 1 hour margin variability to avoid
%% running on all ale disk sinks at the same time
-define(IN_USE_DEKS_REFRESH_INTERVAL_MS,
        (23*60*60*1000 + rand:uniform(60*60*1000))).

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
          rotation_check_interval :: non_neg_integer(),
          in_use_deks :: list(),
          encr_state :: any()
         }).

start_link(Name, Path) ->
    start_link(Name, Path, []).

start_link(Name, Path, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Path, Opts], []).

meta() ->
    [{async, true},
     {type, preformatted},
     {encryption_supported, true}].

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
                                rotation_check_interval = RotCheckInterval,
                                in_use_deks = []},
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

do_work(Worker, Call, From, Timeout) ->
    Parent = self(),
    proc_lib:spawn_link(
      fun () ->
              gen_server:reply(From, gen_server:call(Worker, Call, Timeout)),
              erlang:unlink(Parent)
      end).

handle_call(sync, From, #state{worker = Worker} = State) ->
    NewState = flush_buffer(State),
    do_work(Worker, sync, From, infinity),
    {noreply, NewState};
handle_call(notify_active_key_updt, From,
            #state{worker = Worker} = State) ->
    do_work(Worker, notify_active_key_updt, From, infinity),
    {noreply, State};
handle_call({drop_log_deks, DekIdsToDrop, WorkSzThresh}, From,
            #state{worker = Worker} = State) ->
    do_work(Worker, {drop_log_deks, DekIdsToDrop, WorkSzThresh},
            From, infinity),
    {noreply, State};
handle_call(get_in_use_deks, From,
            #state{worker = Worker} = State) ->
    do_work(Worker, get_in_use_deks, From, infinity),
    {noreply, State};
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

do_rotate_files(#worker_state{sink_name = Name} = State0) ->
    time_stat(Name, rotation_time,
              fun () ->
                      ok = rotate_files(State0),
                      ok = maybe_compress_post_rotate(State0),
                      State1  = open_log_file(State0),
                      update_in_use_deks(State1)
              end).

maybe_rotate_files(#worker_state{file_size = FileSize,
                                 rotation_size = RotSize} = State)
  when RotSize =/= 0, FileSize >= RotSize ->
    do_rotate_files(State);
maybe_rotate_files(State) ->
    State.

get_all_files_with_path(LogFilePath) ->
    Dir = filename:dirname(LogFilePath),
    BaseName = filename:basename(LogFilePath),

    DirFiles = [BaseName | filelib:wildcard(BaseName ++ ".*", Dir)],
    lists:map(
      fun(FileName) ->
              filename:join(Dir, FileName)
      end, DirFiles).

update_in_use_deks(#worker_state{path = LogFilePath} = State) ->
    FilePaths = get_all_files_with_path(LogFilePath),
    State#worker_state{in_use_deks = ale:get_in_use_deks(FilePaths)}.

maybe_write_header(_SinkName, _File, <<>>) ->
    ok;
maybe_write_header(SinkName, File, Header) ->
    time_stat(SinkName, write_time,
              fun () ->
                      ok = file:write(File, Header)
              end).

update_file_encr_state(true = _ShouldContinue,
                       #worker_state{path = Path} = State, DS) ->
    {ok, File, #file_info{size = Size, inode = Inode}} = open_file(Path),
    {ok, ContEncrState} = ale:file_encrypt_cont(Path, Size, DS),
    State#worker_state{file = File,
                       file_size = Size,
                       file_inode = Inode,
                       encr_state = ContEncrState};
update_file_encr_state(false = _ShouldContinue, State, _DS) ->
    do_rotate_files(State).

open_with_encr_state(true = _IsNewFile,
                     #worker_state{sink_name = SinkName,
                                   path = Path} = State, DS) ->
    {ok, File, #file_info{size = 0, inode = Inode}} = open_file(Path),
    {ok, {Header, EncrState}} = ale:file_encrypt_init(DS),
    maybe_write_header(SinkName, File, Header),
    State#worker_state{file = File,
                       file_size = byte_size(Header),
                       file_inode = Inode,
                       encr_state = EncrState};
open_with_encr_state(false = _IsNewFile,
                     #worker_state{path = Path} = State, DS) ->
    ShouldContinue = ale:validate_encr_file_with_ds(Path, DS),
    update_file_encr_state(ShouldContinue, State, DS).

maybe_close_encr_state(#worker_state{encr_state = undefined} = State) ->
    State;
maybe_close_encr_state(#worker_state{encr_state = EncrState} = State) ->
    catch ale:file_encrypt_finish(EncrState),
    State#worker_state{encr_state = undefined}.

open_log_file(#worker_state{path = Path,
                            file = OldFile,
                            sink_name = SinkName} = State) ->
    case OldFile of
        undefined ->
            ok;
        _ ->
            file:close(OldFile)
    end,

    IsNewFile = not filelib:is_file(Path) orelse
                filelib:file_size(Path) =:= 0,

    DS = ale:get_sink_ds(SinkName),
    NewState = maybe_close_encr_state(State#worker_state{file = undefined}),
    open_with_encr_state(IsNewFile, NewState, DS).

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

compress(false = _IsEncrypted, _Name, UncompressPth) ->
    CompressPth = UncompressPth ++ ".gz",
    compress_file(UncompressPth, CompressPth);
compress(true = _IsEncrypted, Name, Path) ->
    reencrypt_and_compress(Name, Path, #{}).

reencrypt_and_compress(Name, Path, Opts) ->
    CompressPth = Path ++ ".tmp",

    IsGzipMagicHeader =
        fun(FileName) ->
                {ok, F} = file:open(FileName, [read, raw, binary]),
                try
                    case file:read(F, 2) of
                        {ok, <<16#1f, 16#8b>>} ->
                            true;
                        _ ->
                            false
                    end
                after
                    file:close(F)
                end
        end,

    RenameEnsureGzExtention =
        fun(ResultFilePath, OrigPath) ->
                case filename:extension(OrigPath) of
                    ".gz" ->
                        file:rename(ResultFilePath, OrigPath);
                    _ ->
                        NewPath = OrigPath ++ ".gz",
                        maybe
                            ok ?= file:rename(ResultFilePath, NewPath),
                            ok ?= file:delete(OrigPath)
                        end
                end
        end,

    RenameStripGzExtention =
        fun(ResultFilePath, OrigPath) ->
                case filename:extension(OrigPath) of
                    ".gz" ->
                        NewPath = filename:rootname(OrigPath),
                        maybe
                            ok ?= file:rename(ResultFilePath, NewPath),
                            ok ?= file:delete(OrigPath)
                        end;
                    _  ->
                        file:rename(ResultFilePath, OrigPath)
                end
        end,

    %% When re-encrypting and compressing an unencrypted ".gz" file, we need to
    %% remove the .gz extension to match existing file convention for
    %% encrypted files. If the resulting file is a gzip decrypted, we need to
    %% ensure we add the .gz extension
    RenameFunc =
        fun(ResultFilePath, OrigPath, RencrOpts) ->
                %% If a file was re-encrypted with allow_decrypt true and with
                %% decr_compression as gzip and the resulting file was
                %% a decrypted file and has a valid gzip magic header, it
                %% is safe to say that file is a gzip format file
                DecrWithGzip =
                    maps:get(allow_decrypt, RencrOpts, false) andalso
                    maps:get(decr_compression, RencrOpts, false) =:= gzip,
                case DecrWithGzip andalso
                     not ale:is_file_encrypted(ResultFilePath) andalso
                     IsGzipMagicHeader(ResultFilePath)  of
                    true ->
                        RenameEnsureGzExtention(ResultFilePath, OrigPath);
                    false ->
                        RenameStripGzExtention(ResultFilePath, OrigPath)
                end
        end,

    try
        maybe
            %% Note that it is possible that DS is not the same DS that was used
            %% to encrypt this file. If active key has changed, reencrypt_file
            %% will use the new key to reencrypt the file. If encryption has
            %% been disabled, reencrypt_file will continue using old key
            %% to reencrypt the file (we should not decrypt this file in this
            %% case).
            DS = ale:get_sink_ds(Name),
            RencrOpts = Opts#{encr_compression => {zlib, 5, none},
                              ignore_incomplete_last_chunk => true},
            ok ?= ale:reencrypt_file(Path, CompressPth, DS, RencrOpts),
            ok ?= RenameFunc(CompressPth, Path, RencrOpts)
        else
            {error, key_not_found} ->
                %% We don't have a key for this file anymore, so we can't
                %% decrypt it. We should not remove it either, so we do nothing.
                ok;
            {error, Reason} ->
                {error, Reason}
        end
    after
        (catch file:delete(CompressPth))
    end.

maybe_compress_post_rotate(#worker_state{sink_name = Name,
                                         path = Path,
                                         rotation_num_files = NumFiles,
                                         rotation_compress = true})
  when NumFiles > 1 ->
    UncompressedPath = Path ++ ".1",

    IsEncrypted = ale:is_file_encrypted(UncompressedPath),
    time_stat(Name, compression_time,
              fun () ->
                      compress(IsEncrypted, Name, UncompressedPath)
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

worker_init(#worker_state{
               rotation_check_interval = RotCheckInterval} = State0) ->
    case RotCheckInterval > 0 of
        true ->
            erlang:send_after(RotCheckInterval, self(), check_file);
        false ->
            ok
    end,

    erlang:send_after(?IN_USE_DEKS_REFRESH_INTERVAL_MS, self(), in_use_refresh),

    State1 = open_log_file(State0),
    worker_loop(
      update_in_use_deks(State1)).

worker_loop(#worker_state{sink_name = SinkName} = State) ->
    NewState =
        receive
            {write, Data0, DataSize0} ->
                {Data, DataSize} = receive_more_writes(Data0, DataSize0),
                write_data(Data, DataSize, State);
            check_file ->
                erlang:send_after(State#worker_state.rotation_check_interval,
                                  self(), check_file),
                check_log_file(State);
            in_use_refresh ->
                erlang:send_after(?IN_USE_DEKS_REFRESH_INTERVAL_MS, self(),
                                  in_use_refresh),
                update_in_use_deks(State);
            {'$gen_call', From, sync} ->
                gen_server:reply(From, ok),
                State;
            {'$gen_call', From, notify_active_key_updt} ->
                #worker_state{encr_state = EncrState} = State,
                DS = ale:get_sink_ds(SinkName),
                UpdtReq = not ale:file_encrypt_state_match(DS, EncrState),
                UpdatedState = process_key_update_work(UpdtReq, State),
                gen_server:reply(From, ok),
                UpdatedState;
            {'$gen_call', From, {drop_log_deks, DekIdsToDrop, WorkSzThresh}} ->
                #worker_state{in_use_deks = CurrInUseDeks} = State,
                InUseDekIdsToDrop =
                    sets:to_list(
                        sets:intersection(
                            sets:from_list(CurrInUseDeks),
                            sets:from_list(DekIdsToDrop))
                    ),
                {Rv, UpdatedState} =
                    process_drop_dek_work(InUseDekIdsToDrop, WorkSzThresh,
                                          State),
                gen_server:reply(From, Rv),
                UpdatedState;
            {'$gen_call', From, get_in_use_deks} ->
                #worker_state{in_use_deks = InUseDeks} = State,
                gen_server:reply(From, {ok, InUseDeks}),
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

maybe_encrypt_data(InputData, EncrState) ->
    ale:file_encrypt_chunk(InputData, EncrState).

write_data(InputData, InputDataSize,
           #worker_state{sink_name = Name,
                         file = File,
                         file_size = FileSize,
                         parent = Parent,
                         encr_state = EncrState} = State) ->
    {WriteData, NewEncrState} = maybe_encrypt_data(InputData, EncrState),
    WriteDataSize = iolist_size(WriteData),
    broadcast_stat(Name, write_size, WriteDataSize),
    time_stat(Name, write_time,
              fun () ->
                      ok = file:write(File, WriteData)
              end),

    Parent ! {written, InputDataSize},
    NewState = State#worker_state{file_size = FileSize + WriteDataSize,
                                  encr_state = NewEncrState},
    maybe_rotate_files(NewState).

process_key_update_work(false = _UpdtReq, State) ->
    State;
process_key_update_work(true = _UpdtReq, State) ->
    do_rotate_files(State).

process_drop_dek_work([] = _DekIdsToDrop, _WorkSzThresh, State) ->
    {{ok, 0}, State};
process_drop_dek_work(DekIdsToDrop, WorkSzThresh,
                      #worker_state{
                         sink_name = SinkName,
                         path = LogFilePath} = State) ->
    FilePaths = get_all_files_with_path(LogFilePath),
    ActiveInUse = ale:get_in_use_deks([LogFilePath]),
    RotatedFilePaths = FilePaths -- [LogFilePath],

    {FilesInfo, RotatedInUse} =
        lists:foldl(
          fun(FPath, {Acc0, Acc1}) ->
                  InUsDekIds = ale:get_in_use_deks([FPath]),
                  {[{FPath, filelib:file_size(FPath), InUsDekIds} | Acc0],
                   Acc1 ++ InUsDekIds}
          end, {[], []}, RotatedFilePaths),

    CurrInUseDekIds = RotatedInUse ++ ActiveInUse,

    %% Although not strictly necessary, we just sort the files here from lowest
    %% size to highest size because it allows a larger number of files to be
    %% processed first, if there are a bunch of smaller size files that fit
    %% the WorkSizeThresh and some larger ones that don't. This would mean that
    %% if another call is needed to drop the DEKs that is attempted right
    %% after, the subsequent call would have to deal with less files, so less
    %% overhead to read the in use DEKs again for those files
    SortedFilesInfo =
        lists:sort(
          fun({_, SizeA, _}, {_, SizeB, _}) ->
                  SizeA =< SizeB
          end, FilesInfo),

    ReEncrFn =
        fun(FPath, Size, DekIds, {FilesAndDeks, InUse, AccSize, Errors}) ->
                case reencrypt_and_compress(
                       SinkName, FPath, #{allow_decrypt => true,
                                          decr_compression => gzip}) of
                    ok ->
                        NewDekIds = ale:get_in_use_deks([FPath]),
                        NewFilesAndDeks = [{FPath, NewDekIds} | FilesAndDeks],
                        NewInuseAcc = InUse ++ NewDekIds,
                        NewAccSize = AccSize + Size,
                        {NewFilesAndDeks, NewInuseAcc, NewAccSize, Errors};
                    {error, E} ->
                        {[{FPath, DekIds} | FilesAndDeks],
                         InUse ++ DekIds, AccSize,
                         [{filename:basename(FPath), E} | Errors]}
                end
        end,

    InUseIdsToDropSet =
        sets:intersection(sets:from_list(DekIdsToDrop),
                          sets:from_list(CurrInUseDekIds)),

    {UpdtFilesAndDeks, FinalInuseIds, _, Errors} =
        lists:foldl(
          fun({FPath, Size, DekIds}, {FilesAndDeks, InUse, AccSize, Errs} = Acc)
                when AccSize < WorkSzThresh ->
                  DropIds = sets:intersection(InUseIdsToDropSet,
                                              sets:from_list(DekIds)),
                  case sets:is_empty(DropIds) of
                      true ->
                          {[{FPath, DekIds} | FilesAndDeks], InUse ++ DekIds,
                           AccSize, Errs};
                      false ->
                          ReEncrFn(FPath, Size, DekIds, Acc)

                  end;
             ({FPath, _Size, DekIds}, {FilesAndDeks, InUse, AccSize, Errs}) ->
                  {[{FPath, DekIds} | FilesAndDeks], InUse ++ DekIds,
                   AccSize, Errs}
          end, {[], [], 0, []}, SortedFilesInfo),

    UpdtInuseIds =
        lists:usort(FinalInuseIds ++ ActiveInUse),

    UpdtIdsToDropSet =
        sets:intersection(sets:from_list(UpdtInuseIds), InUseIdsToDropSet),

    FilesUsingToDropDeksCount =
        length(lists:filter(
                 fun({_FPath, InUseDekIds}) ->
                         IdsSet = sets:from_list(InUseDekIds),
                         not sets:is_empty(
                               sets:intersection(IdsSet, UpdtIdsToDropSet))
                 end, UpdtFilesAndDeks ++ [{LogFilePath, ActiveInUse}])),

    NewState = State#worker_state{in_use_deks = UpdtInuseIds},
    case Errors of
        [] ->
            {{ok, FilesUsingToDropDeksCount}, NewState};
        Errors ->
            {{{error, Errors}, FilesUsingToDropDeksCount}, NewState}
    end.

remove_unnecessary_log_files(LogFilePath, NumFiles) ->
    Dir = filename:dirname(LogFilePath),
    Name = filename:basename(LogFilePath),
    {ok, RegExp} = re:compile("^" ++ Name ++ "\.([1-9][0-9]*)(?:\.gz)?$"),

    DirFiles = filelib:wildcard(Name ++ ".*", Dir),
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
