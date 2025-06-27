%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_crypto).

-include("ns_common.hrl").
-include("cb_cluster_secrets.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(ENCRYPTED_FILE_HEADER_LEN, 80).
-define(IV_LEN, 12).
-define(TAG_LEN, 16).
-define(CHUNKSIZE_ATTR_SIZE, 4).
-define(ENCRYPTED_FILE_MAGIC, 0, "Couchbase Encrypted", 0).
-define(NO_COMPRESSION, 0).
-define(SNAPPY_COMPRESSION, 1).
-define(ZLIB_COMPRESSION, 2).
-define(GZIP_COMPRESSION, 3).
-define(ZSTD_COMPRESSION, 4).
-define(BZIP2_COMPRESSION, 5).
-define(GZIP_FORMAT, 16#10).

-export([%% Encryption/decryption functions:
         encrypt/3,
         decrypt/3,

         atomic_write_file/3,
         atomic_write_file/4,
         file_encrypt_init/1,
         file_encrypt_init/2,
         file_encrypt_cont/3,
         file_encrypt_chunk/2,
         file_encrypt_finish/1,

         read_file/2,
         read_file_chunks/5,
         file_encrypt_state_match/2,
         validate_encr_file_with_ds/2,
         can_ds_decrypt_file/2,
         is_file_encr_by_active_key/2,
         is_file_encrypted/1,
         get_file_dek_ids/1,
         get_in_use_deks/1,
         file_decrypt_init/2,
         file_decrypt_next_chunk/2,
         file_decrypt_finish/1,

         new_aes_gcm_iv/1,

         %% Manage deks in persistent_term storage:
         fetch_deks_snapshot/1,
         active_key_ok/1,
         all_keys_ok/1,
         create_deks_snapshot/3,
         reset_dek_cache/2,
         get_all_deks/1,
         get_dek_id/1,
         get_dek/1,
         without_historical_deks/1,
         same_snapshots/2,
         get_deks_snapshot_hash/1,

         %% Other:
         get_encryption_method/3,
         get_dek_kind_lifetime/2,
         get_dek_rotation_interval/2,
         get_drop_keys_timestamp/2,
         get_force_encryption_timestamp/2,
         reencrypt_file/4
        ]).

-export_type([dek_snapshot/0, encryption_type/0]).

-record(dek_snapshot, {iv_random :: binary(),
                       iv_atomic_counter :: atomics:atomics_ref(),
                       active_key :: cb_deks:dek() | undefined,
                       all_keys :: [cb_deks:dek()],
                       created_at :: calendar:datetime()}).

-record(file_encr_state, {ad_prefix = <<>> :: binary(),
                          key :: cb_deks:dek() | undefined,
                          iv_random :: binary(),
                          iv_atomic_counter :: atomics:atomics_ref() ,
                          offset :: non_neg_integer(),
                          compression_state :: undefined |
                                               {deflate, none | sync | full,
                                                zlib:zstream()}}).

-record(file_decr_state, {vsn :: non_neg_integer(),
                          ad_prefix = <<>> :: binary(),
                          key :: cb_deks:dek(),
                          offset = 0 :: non_neg_integer(),
                          decompression_state :: undefined |
                                                 {deflate, zlib:zstream()}}).

-type dek_snapshot() :: #dek_snapshot{}.
-type encryption_type() :: config_encryption | log_encryption |
                           audit_encryption.
-type fetch_deks_res() :: {ok, #dek_snapshot{}} | {error, term()}.
-type encr_compression_cfg() :: {zlib,
                                 Level :: pos_integer(),
                                 Flush :: none | sync | full} | undefined.
-type decr_compression_cfg() :: gzip | undefined.

%%%===================================================================
%%% API
%%%===================================================================

-spec encrypt(binary(), binary(), #dek_snapshot{}) ->
          {ok, binary()} | {error, term()}.
encrypt(_Data, _AD, #dek_snapshot{active_key = undefined}) ->
    {error, no_active_key};
encrypt(Data, AD, #dek_snapshot{active_key = ActiveDek,
                                iv_random = IVRandom,
                                iv_atomic_counter = IVAtomic}) ->
    encrypt_internal(Data, AD, IVRandom, IVAtomic, ActiveDek).

-spec decrypt(binary(), binary(), #dek_snapshot{}) ->
          {ok, binary()} | {error, term()}.
decrypt(Data, AD, #dek_snapshot{all_keys = AllKeys}) ->
    decrypt_internal(Data, AD, AllKeys).

-spec atomic_write_file(string(), binary() | iolist(), #dek_snapshot{}) ->
          ok | {error, term()}.
atomic_write_file(Path, Data, DekSnapshot) ->
    atomic_write_file(Path, Data, DekSnapshot, #{}).

-spec atomic_write_file(string(), binary() | iolist(), #dek_snapshot{},
                        #{max_chunk_size => pos_integer(),
                          encr_compression => encr_compression_cfg()}) ->
          ok | {error, term()}.
atomic_write_file(Path, List, DekSnapshot, Opts) when is_list(List) ->
    atomic_write_file(Path, iolist_to_binary(List), DekSnapshot, Opts);
atomic_write_file(Path, Bytes, DekSnapshot, Opts) when is_binary(Bytes) ->
    MaxChunkSize = maps:get(max_chunk_size, Opts, 65536),
    Compression = maps:get(encr_compression, Opts, undefined),
    misc:atomic_write_file(
      Path,
      fun (IODevice) ->
          encrypt_to_file(IODevice, Bytes, MaxChunkSize, Compression, DekSnapshot)
      end).

-spec reencrypt_file(string(), string(), #dek_snapshot{},
                     #{encr_compression => encr_compression_cfg(),
                       decr_compression => decr_compression_cfg(),
                       ignore_incomplete_last_chunk => boolean(),
                       allow_decrypt => boolean()}) ->
          {ok, ResultFileFormat :: couchbase_encrypted | gzip | unencrypted} |
          {error, term()}.
%% Note1: This function does not support reencryption with different chunk size,
%% because it is not really needed at this point.
%% When compression is used, it is possible to achieve bigger chunks by using
%% Flush == none. In this case zlib will accumulate data in its internal buffer.
%%
%% Note2: By default, this function assumes that the source file is encrypted
%% and will error out if reencryption is attempted on a decrypted file, unless
%% "allow_decrypt" option is used. allow_decrypt" is described in Note5 in more
%% detail.
%%
%% Note3: If the file is encrypted with historic key, current active key will be
%% used for reencryption.
%%
%% Note4: If DS does not have active key, the file will be reencrypted with the
%% same key as before, if allow_decrypt option is not true.
%%
%% Note5: "allow_decrypt" option, if not false, specifically allows:
%%          a) The file to be decrypted if encryption is
%%             disabled(no active key), hence the resulting file will be left
%%             decrypted in that case.
%%          b) Disables default assumption that the source file is encrypted,
%%             and thus will reencrypt any decrypted file based on active key.
%%             If encryption is disabled(no active key), the decrypted file will
%%             be left decrypted as expected in that case.
%%          c) Defaults to false which means option is not used
%%          d) Lastly when using "allow_decrypt" one can further specify if the
%%             decrypted files should be gzip compressed or not by setting
%%             decr_compression => gzip | undefined. By default decrypted files
%%             are left uncompressed
reencrypt_file(FromPath, ToPath, DS, Opts) ->
    AllowDecr = maps:get(allow_decrypt, Opts, false),
    EncrEnabled = get_dek_id(DS) =/= undefined,
    DSRes =
        case {EncrEnabled, AllowDecr} of
            %% Encryption is disabled and decryption is not allowed, so we
            %% should continue using the same key reencryption.
            {false, false} ->
                case get_file_dek_ids(FromPath) of
                    {ok, [undefined]} -> %% File is not encrypted
                        {error, unknown_magic};
                    {ok, []} ->
                        {error, enoent};
                    {ok, [KeyId]} ->
                        case find_key(KeyId, DS) of
                            {ok, Dek} ->
                                %% We don't need other keys actually
                                {ok, create_deks_snapshot(Dek, [Dek], DS)};
                            {error, E} ->
                                {error, E}
                        end
                end;
            _ ->
                {ok, DS}
        end,

    ExpectedDecrFormat =
        case maps:get(decr_compression, Opts, undefined) of
            gzip ->
                gzip;
            undefined ->
                unencrypted
        end,

    maybe
        {ok, DSToUse} ?= DSRes,
        ok ?= misc:atomic_write_file(
                ToPath,
                fun (IO) ->
                        reencrypt_file_to_iodevice(IO, FromPath, DSToUse, Opts)
                end),
        FileFormat =
            case get_dek_id(DSToUse) == undefined of
                true ->
                    ExpectedDecrFormat;
                false ->
                    couchbase_encrypted
            end,
        {ok, FileFormat}
    else
        {error, _Reason} = Error ->
            Error
    end.

reencrypt_file_to_iodevice(IODevice, FromPath, DS, Opts) ->
    IgnoreIncomplete = maps:get(ignore_incomplete_last_chunk, Opts, false),
    AllowDecrypted = maps:get(allow_decrypt, Opts, false),

    maybe
        {ok, {Header, State}} ?= file_encrypt_init(DS, Opts),
        case file:write(IODevice, Header) of
            ok ->
                Res = read_file_chunks(
                        FromPath,
                        fun (Data, StateAcc) ->
                            {Chunk, StateAcc1} = file_encrypt_chunk(Data,
                                                                    StateAcc),
                            case misc:iolist_is_empty(Chunk) of
                                true ->
                                    {ok, StateAcc1};
                                false ->
                                    maybe
                                        ok ?= file:write(IODevice, Chunk),
                                        {ok, StateAcc1}
                                    end
                            end
                        end, State, DS,
                        #{read_chunk_size => 65536,
                          ignore_incomplete_last_chunk => IgnoreIncomplete,
                          allow_decrypted => AllowDecrypted}),
                case Res of
                    {ok, FinalState} ->
                        FinalData = file_encrypt_finish(FinalState),
                        file:write(IODevice, FinalData);
                    {error, Reason, FinalState} ->
                        _ = file_encrypt_finish(FinalState),
                        {error, Reason}
                end;
            {error, Reason} ->
                _ = file_encrypt_finish(State),
                {error, Reason}
        end
    end.

-spec file_encrypt_init(#dek_snapshot{}) ->
          {ok, {binary(), #file_encr_state{}}} | {error, term()}.
file_encrypt_init(DekSnapshot) ->
    file_encrypt_init(DekSnapshot, #{}).

-spec file_encrypt_init(#dek_snapshot{},
                        #{encr_compression => encr_compression_cfg(),
                          decr_compression => decr_compression_cfg()}) ->
          {ok, {binary(), #file_encr_state{}}} | {error, term()}.
file_encrypt_init(#dek_snapshot{active_key = ?DEK_ERROR_PATTERN(_, _)}, _) ->
    {error, key_not_available};
file_encrypt_init(#dek_snapshot{active_key = ActiveKey,
                                iv_random = IVRandom,
                                iv_atomic_counter = IVCounter}, Opts) ->
    CompressOption =
       case ActiveKey of
           undefined ->
               maps:get(decr_compression, Opts, undefined);
           _ ->
               maps:get(encr_compression, Opts, undefined)
       end,

    Salt = crypto:strong_rand_bytes(16),
    {CompressionType, CompressionState} =
        case CompressOption of
            undefined ->
                {?NO_COMPRESSION, undefined};
            {zlib, Level, Flush} ->
                Z = zlib:open(),
                zlib:deflateInit(Z, Level),
                {?ZLIB_COMPRESSION, {deflate, Flush, Z}};
            gzip ->
                Z = zlib:open(),
                %% Although we could pass these in parameters via options
                %% for gzip compression, no strong need right now so they are
                %% fixed here to keep the usage simpler
                Level = 8,
                Flush = none,
                zlib:deflateInit(Z, default, deflated, 15 bor ?GZIP_FORMAT,
                                 Level, default),
                {?GZIP_COMPRESSION, {deflate, Flush, Z}}
        end,
    Header =
        case ActiveKey of
            undefined -> <<>>;
            #{id := Id} ->
                Len = size(Id),
                Vsn = 0,
                H = <<?ENCRYPTED_FILE_MAGIC, Vsn, CompressionType, 0, 0, 0, 0,
                      Len, Id/binary, Salt/binary>>,
                ?ENCRYPTED_FILE_HEADER_LEN = size(H),
                H
        end,
    {ok, {Header, #file_encr_state{ad_prefix = Header,
                                   key = ActiveKey,
                                   iv_random = IVRandom,
                                   iv_atomic_counter = IVCounter,
                                   offset = size(Header),
                                   compression_state = CompressionState}}}.

-spec file_encrypt_cont(string(), non_neg_integer(), #dek_snapshot{}) ->
          {ok, #file_encr_state{}} | {error, _}.

%% Note: We can't continue writing compressed file
file_encrypt_cont(Path, FileSize,
                  #dek_snapshot{active_key = ActiveKey,
                                iv_random = IVRandom,
                                iv_atomic_counter = IVCounter}) ->
    maybe
        {ok, Header} ?= read_file_header(Path),
        {ok, {_Vsn, KeyId, ADPrefix, _, CompressionType, <<>>}} ?=
            parse_header(Header),
        ok ?= case CompressionType of
                  ?NO_COMPRESSION -> ok;
                  _ -> {error, {unsupported_compression_type, CompressionType}}
              end,
        case ActiveKey of
            ?DEK_ERROR_PATTERN(KeyId, _) ->
                {error, key_not_available};
            #{id := KeyId} ->
                {ok, #file_encr_state{ad_prefix = ADPrefix,
                                      key = ActiveKey,
                                      iv_random = IVRandom,
                                      iv_atomic_counter = IVCounter,
                                      offset = FileSize}};
            _ ->
                {error, key_mismatch}
        end
    else
        {error, unknown_magic} when ActiveKey == undefined ->
            %% File is not encrypted
            {ok, #file_encr_state{ad_prefix = <<>>,
                                  key = undefined,
                                  iv_random = IVRandom,
                                  iv_atomic_counter = IVCounter,
                                  offset = FileSize}};
        {error, _} = Error ->
            Error
    end.

-spec file_encrypt_chunk(erlang:iodata(), #file_encr_state{}) ->
          {erlang:iodata(), #file_encr_state{}}.
file_encrypt_chunk(
    Data, #file_encr_state{key = undefined,
                           compression_state = undefined} = State) ->
    {Data, State};
file_encrypt_chunk(
    Data, #file_encr_state{key = undefined,
                           compression_state = CompressionState} = State) ->
    {deflate, Flush, Z} = CompressionState,
    CompressedData = zlib:deflate(Z, Data, Flush),
    {CompressedData, State};
file_encrypt_chunk(Data, State) ->
    maybe
        {empty, false} ?= {empty, misc:iolist_is_empty(Data)},
        #file_encr_state{key = Dek,
                         iv_random = IVRandom,
                         iv_atomic_counter = IVAtomic,
                         offset = Offset,
                         compression_state = CompressionState} = State,
        AD = file_assoc_data(State),
        CompressedData = case CompressionState of
                             undefined -> Data;
                             {deflate, Flush, Z} -> zlib:deflate(Z, Data, Flush)
                         end,
        {empty, false} ?= {empty, misc:iolist_is_empty(CompressedData)},
        {ok, Chunk} = encrypt_internal(CompressedData, AD, IVRandom, IVAtomic,
                                       Dek),
        ChunkSize = size(Chunk),
        ChunkWithSize = <<ChunkSize:32/big-unsigned-integer, Chunk/binary>>,
        NewOffset = Offset + size(ChunkWithSize),
        {ChunkWithSize, State#file_encr_state{offset = NewOffset}}
    else
        {empty, true} ->
            {<<>>, State}
    end.

-spec file_encrypt_finish(#file_encr_state{}) -> binary().
file_encrypt_finish(#file_encr_state{compression_state = undefined}) ->
    <<>>;
file_encrypt_finish(
        #file_encr_state{compression_state = {deflate, _, Z}} = State) ->
    FinalData = zlib:deflate(Z, <<>>, finish),
    ok = zlib:deflateEnd(Z),
    zlib:close(Z),
    NewState = State#file_encr_state{compression_state = undefined},
    {EncryptedData, _} = file_encrypt_chunk(FinalData, NewState),
    EncryptedData.

-spec file_encrypt_state_match(#dek_snapshot{},
                               #file_encr_state{}) -> boolean().
file_encrypt_state_match(#dek_snapshot{active_key = ActiveKey},
                         #file_encr_state{key = FileActiveKey}) ->
    get_key_id(ActiveKey) =:= get_key_id(FileActiveKey).

-spec validate_encr_file_with_ds(string(), #dek_snapshot{}) -> true | false.
validate_encr_file_with_ds(Path, #dek_snapshot{active_key = undefined}) ->
    not is_file_encrypted(Path);
validate_encr_file_with_ds(Path, DS) ->
    is_file_encr_by_active_key(Path, DS) andalso validate_encr_file(Path).

-spec is_file_encr_by_active_key(string(), #dek_snapshot{}) -> true | false.
is_file_encr_by_active_key(Path, #dek_snapshot{active_key = undefined}) ->
    not is_file_encrypted(Path);
is_file_encr_by_active_key(Path, #dek_snapshot{active_key = #{id := KeyId}}) ->
    header_key_match(read_file_header(Path), KeyId).

-spec can_ds_decrypt_file(string(), #dek_snapshot{}) -> true | false.
can_ds_decrypt_file(Path, DS) ->
    {ok, FileDekIds} = get_file_dek_ids(Path),
    {ActiveKey, OtherKeys} = get_all_deks(DS),
    AllKeyIds = [get_dek_id(Key) || Key <- OtherKeys] ++
        [get_dek_id(ActiveKey) || ActiveKey =/= undefined],
    lists:all(
      fun(DekId) ->
              lists:member(DekId, AllKeyIds)
      end, FileDekIds).

-spec is_file_encrypted(string()) -> true | false.
is_file_encrypted(Path) ->
    is_valid_encr_header(read_file_header(Path)).

-spec get_file_dek_ids(Path :: string()) ->
          {ok, [cb_deks:dek_id() | undefined]} | {error, _}.
get_file_dek_ids(Path) ->
    case read_file_header(Path) of
        {ok, Bin} ->
            case parse_header(Bin) of
                {ok, {_Vsn, KeyId, _ADPrefix, _Offset, _CompressionType,
                      _Rest}} ->
                    {ok, [KeyId]};
                incomplete_magic -> {ok, [undefined]};
                need_more_data -> {ok, [undefined]};
                {error, unknown_magic} -> {ok, [undefined]};
                {error, E} -> {error, E}
            end;
        eof -> {ok, []};
        {error, enoent} -> {ok, []};
        {error, R} -> {error, R}
    end.

-spec get_in_use_deks([string()]) -> [cb_deks:dek_id()].
get_in_use_deks(FilePaths) ->
    InUseDeks =
        lists:flatmap(
            fun(FilePath) ->
                {ok, DekIds} = get_file_dek_ids(FilePath),
                DekIds
            end, FilePaths),
    lists:usort(InUseDeks).

-spec read_file(string(),
                cb_deks:dek_kind() |
                #dek_snapshot{} |
                fun(() -> fetch_deks_res()) |
                fun((cb_deks:dek_id()) -> cb_deks:dek())) ->
          {decrypted, binary()} | {raw, binary()} | {error, term()}.
read_file(Path, GetDekSnapshotFun) when is_function(GetDekSnapshotFun) ->
    maybe
        {ok, Data} ?= file:read_file(Path),
        case decrypt_file_data(Data, GetDekSnapshotFun) of
            {ok, Decrypted} ->
                {decrypted, Decrypted};
            {error, unknown_magic} ->
                %% File is not encrypted?
                {raw, Data};
            {error, _} = Error ->
                Error
        end
    end;
read_file(Path, #dek_snapshot{} = DekSnapshot) ->
    read_file(Path, fun () -> {ok, DekSnapshot} end);
read_file(Path, DekKind) ->
    GetSnapshotFun = fun () -> fetch_deks_snapshot(DekKind) end,
    read_file(Path, GetSnapshotFun).

-spec read_file_chunks(Path, Fun, Acc, Deks, Opts) -> {ok, Acc} |
                                                      {error, _, Acc}
          when Path :: string(),
               Fun :: fun ( (erlang:iodata(), Acc) -> {ok, Acc} | {error, _} ),
               Acc :: term(),
               Deks :: #dek_snapshot{},
               Opts :: #{read_chunk_size => pos_integer(),
                         ignore_incomplete_last_chunk => boolean(),
                         allow_decrypted => boolean()}.
read_file_chunks(Path, Fun, AccInit, Deks, Opts) ->
    ReadChunkSize = maps:get(read_chunk_size, Opts, 65536),
    IgnoreIncomplete = maps:get(ignore_incomplete_last_chunk, Opts, false),

    %% If allow_decrypted option is not set to true, the file will processed
    %% with default assumption that it is encrypted.
    IsEncrypted = case maps:get(allow_decrypted, Opts, false) of
                      true ->
                          is_file_encrypted(Path);
                      false ->
                          true
                  end,

    InitEncrState =
        fun(Data, {init, Acc}, true) ->
                case file_decrypt_init(Data, Deks) of
                    {ok, {EncrState, Rest}} ->
                        {ok, {process, EncrState, Acc}, Rest};
                    incomplete_magic -> need_more_data;
                    need_more_data -> need_more_data;
                    {error, _} = E -> E
                end;
           (Data, {init, Acc}, false) ->
                {ok, {process, unencrypted, Acc}, Data}
        end,

    F = fun (Data, {init, _} = InitAcc) ->
                InitEncrState(Data, InitAcc, IsEncrypted);
            (Data, {process, unencrypted, Acc}) ->
                maybe
                    {ok, NewAcc} ?= Fun(Data, Acc),
                    {ok, {process, unencrypted, NewAcc}, <<>>}
                end;
            (Data, {process, EncrState, Acc}) ->
                maybe
                    {ok, {NewEncrState, Chunk, Rest}} ?=
                        file_decrypt_next_chunk(Data, EncrState),
                    {ok, NewAcc} ?= Fun(Chunk, Acc),
                    {ok, {process, NewEncrState, NewAcc}, Rest}
                end
        end,
    Finalize = fun ({init, Acc}) -> {ok, Acc};
                   ({process, unencrypted, Acc}) -> {ok, Acc};
                   ({process, DecrState, Acc}) ->
                       case file_decrypt_finish(DecrState) of
                           ok -> {ok, Acc};
                           {error, R} -> {error, R, Acc}
                       end
               end,
    FinalAcc = fun ({init, Acc}) -> Acc;
                   ({process, _, Acc}) -> Acc
               end,
    case misc:fold_file(Path, F, {init, AccInit}, ReadChunkSize) of
        {ok, ResAcc} ->
            Finalize(ResAcc);
        {unhandled_data, _Data, ResAcc} when IgnoreIncomplete ->
            case Finalize(ResAcc) of
                {ok, Acc} -> {ok, Acc};
                {error, incomplete_data, Acc} -> {ok, Acc}
            end;
        {unhandled_data, _Data, ResAcc} ->
            _ = Finalize(ResAcc),
            {error, invalid_file_encryption, FinalAcc(ResAcc)};
        {error, Reason, ResAcc} ->
            _ = Finalize(ResAcc),
            {error, Reason, FinalAcc(ResAcc)}
    end.

-spec file_decrypt_init(binary(),
                        #dek_snapshot{} |
                        fun(() -> fetch_deks_res()) |
                        fun((cb_deks:dek_id()) -> cb_deks:dek())) ->
          {ok, {#file_decr_state{}, RestData :: binary()}} |
          need_more_data |
          incomplete_magic |
          {error, term()}.
file_decrypt_init(Data, #dek_snapshot{} = DekSnapshot) ->
    file_decrypt_init(Data, fun () -> {ok, DekSnapshot} end);
file_decrypt_init(Data, GetDekSnapshotFun)
                                    when is_function(GetDekSnapshotFun, 0) ->
    GetKey = fun (Id) ->
                 maybe
                     {ok, DekSnapshot} ?= GetDekSnapshotFun(),
                     find_key(Id, DekSnapshot)
                 end
             end,
    file_decrypt_init(Data, GetKey);
file_decrypt_init(Data, GetKeyFun) when is_function(GetKeyFun, 1) ->
    maybe
        {ok, {Vsn, Id, ADPrefix, Offset, CompressionType, Chunks}} ?=
            parse_header(Data),
        {ok, DecompressionState} ?=
            case CompressionType of
                ?NO_COMPRESSION ->
                    {ok, undefined};
                ?ZLIB_COMPRESSION ->
                    Z = zlib:open(),
                    ok = zlib:inflateInit(Z),
                    {ok, {deflate, Z}};
                _ ->
                    {error, {unsupported_compression_type, CompressionType}}
            end,
        {ok, Dek} ?= GetKeyFun(Id),
        State = #file_decr_state{vsn = Vsn,
                                 ad_prefix = ADPrefix,
                                 key = Dek,
                                 offset = Offset,
                                 decompression_state = DecompressionState},
        {ok, {State, Chunks}}
    end.

-spec file_decrypt_next_chunk(binary(), #file_decr_state{}) ->
          {ok, {NewState :: #file_decr_state{},
                DecryptedChunk :: erlang:iodata(),
                RestData :: binary()}} |
          need_more_data | eof | {error, term()}.
file_decrypt_next_chunk(Data, State) ->
    #file_decr_state{vsn = 0,
                     key = Dek,
                     offset = Offset,
                     decompression_state = DecompressionState} = State,
    case bite_next_chunk(Data) of
        {ok, {SizeEaten, Chunk}, Rest} ->
            AD = file_assoc_data(State),
            case decrypt_internal(Chunk, AD, [Dek]) of
                {ok, DecryptedData} ->
                    NewOffset = Offset + SizeEaten,
                    NewState = State#file_decr_state{offset = NewOffset},
                    case DecompressionState of
                        undefined ->
                            {ok, {NewState, DecryptedData, Rest}};
                        {deflate, Z} ->
                            Inflated = zlib:inflate(Z, DecryptedData),
                            {ok, {NewState, Inflated, Rest}}
                    end;
                {error, _} = Error ->
                    Error
            end;
        eof -> eof;
        need_more_data -> need_more_data;
        {error, _} = Error -> Error
    end.

-spec file_decrypt_finish(#file_decr_state{}) -> ok | {error, incomplete_data}.
file_decrypt_finish(#file_decr_state{decompression_state = undefined}) ->
    ok;
file_decrypt_finish(#file_decr_state{decompression_state = {deflate, Z}}) ->
    try
        ok = zlib:inflateEnd(Z)
    catch
        error:data_error ->
            %% no end of stream was found meaning that not all data has been
            %% uncompressed
            {error, incomplete_data}
    after
        zlib:close(Z)
    end.

-spec new_aes_gcm_iv(#dek_snapshot{}) -> binary().
new_aes_gcm_iv(#dek_snapshot{iv_random = IVRandom,
                             iv_atomic_counter = IVAtomic}) ->
    new_aes_gcm_iv(IVRandom, IVAtomic).

-spec fetch_deks_snapshot(cb_deks:dek_kind()) -> fetch_deks_res().
fetch_deks_snapshot(DekKind) ->
    cb_atomic_persistent_term:get_or_set_if_invalid(
      {encryption_keys, DekKind},
      fun (#dek_snapshot{created_at = CreatedAt} = DS) ->
          AllKeysAreValid = (all_keys_ok(DS) == ok),
          AllKeysAreValid orelse
              (calendar:universal_time() < misc:datetime_add(CreatedAt, 10))
      end,
      fun (undefined) ->
              read_deks(DekKind, undefined);
          ({value, #dek_snapshot{} = PrevSnapshot}) ->
              read_deks(DekKind, PrevSnapshot)
      end).

active_key_ok(#dek_snapshot{active_key = undefined}) ->
    ok;
active_key_ok(#dek_snapshot{active_key = ?DEK_ERROR_PATTERN(_, _)}) ->
    {error, key_not_available};
active_key_ok(#dek_snapshot{active_key = #{}}) ->
    ok.

all_keys_ok(#dek_snapshot{all_keys = AllKeys} = Snapshot) ->
    case active_key_ok(Snapshot) of
        ok ->
            case lists:all(fun (?DEK_ERROR_PATTERN(_, _)) -> false;
                               (#{type := _}) -> true
                      end, AllKeys) of
                true ->
                    ok;
                false ->
                    {error, some_keys_not_available}
            end;
        {error, _} = Error ->
            Error
    end.

-spec create_deks_snapshot(cb_deks:dek() | undefined, [cb_deks:dek()],
                           #dek_snapshot{} | undefined) -> #dek_snapshot{}.
create_deks_snapshot(ActiveDek, AllDeks, PrevDekSnapshot) ->
    %% Random 4 byte base + unique 8 byte integer = unique 12 byte IV
    %% (note that atomics are 8 byte integers)
    IVBase = crypto:strong_rand_bytes(4),
    IVAtomic =
        case PrevDekSnapshot of
            undefined -> atomics:new(1, [{signed, false}]);
            #dek_snapshot{iv_atomic_counter = OldIVAtomic} -> OldIVAtomic
        end,
    PrevAllKeys =
        case PrevDekSnapshot of
            undefined -> [];
            #dek_snapshot{all_keys = PK} ->
                [{Id, K} || #{id := Id, type := T} = K <- PK, T /= error]
        end,
    PrevAllKeysMap = maps:from_list(PrevAllKeys),
    %% If "new" key is not available, take that key from the previous snapshot
    %% Basically, new snapshot should not get "worse" than the previous one
    GetKey = fun (undefined) ->
                     undefined;
                 (?DEK_ERROR_PATTERN(Id, _) = K) ->
                     maps:get(Id, PrevAllKeysMap, K);
                 (#{id := _Id, type := _Type} = K) ->
                     K
                 end,
    #dek_snapshot{iv_random = IVBase,
                  iv_atomic_counter = IVAtomic,
                  active_key = GetKey(ActiveDek),
                  all_keys = lists:map(GetKey, AllDeks),
                  created_at = calendar:universal_time()}.

-spec reset_dek_cache(cb_deks:dek_kind(),
                      Reason :: {new_active, cb_deks:dek()} | cleanup) ->
          {ok, changed | unchanged} | {error, term()}.
reset_dek_cache(DekKind, Reason) ->
    %% Technically we can simply erase the persistent term, but it will result
    %% in two global GC's then (erase and set). So we read and set new value
    %% instead, which gives us only one global GC.
    cb_atomic_persistent_term:set(
      {encryption_keys, DekKind},
      fun (Prev) ->
          PrevSnapshot = case Prev of
                             undefined -> undefined;
                             {value, S} -> S
                         end,
          Changed =
              case Reason of
                  {new_active, NewActiveDek} ->
                      get_dek_id(PrevSnapshot) =/= get_dek_id(NewActiveDek);
                  cleanup ->
                      true
              end,
          case Changed of
              true ->
                  case read_deks(DekKind, PrevSnapshot) of
                      {ok, NewSnapshot} -> {set, NewSnapshot, {ok, changed}};
                      {error, _} = Error -> {ignore, Error}
                  end;
              false ->
                  {ignore, {ok, unchanged}}
          end
      end).

-spec get_all_deks(#dek_snapshot{}) ->
          {cb_deks:dek() | undefined, [cb_deks:dek()]}.
get_all_deks(#dek_snapshot{active_key = ActiveKey, all_keys = AllKeys}) ->
    {ActiveKey, AllKeys}.

-spec get_dek_id(#dek_snapshot{} | cb_deks:dek() | undefined) ->
          cb_deks:dek_id() | undefined.
get_dek_id(#dek_snapshot{active_key = Key}) ->
    get_dek_id(Key);
get_dek_id(undefined) ->
    undefined;
get_dek_id(#{id := Id}) ->
    Id.

-spec get_dek(#dek_snapshot{}) -> cb_deks:dek() | undefined.
get_dek(#dek_snapshot{active_key = Key}) -> Key.

-spec without_historical_deks(#dek_snapshot{}) -> #dek_snapshot{}.
without_historical_deks(#dek_snapshot{active_key = undefined} = Snapshot) ->
    Snapshot#dek_snapshot{all_keys = []};
without_historical_deks(#dek_snapshot{active_key = Dek} = Snapshot) ->
    Snapshot#dek_snapshot{all_keys = [Dek]}.

-spec same_snapshots(#dek_snapshot{}, #dek_snapshot{}) -> boolean().
same_snapshots(#dek_snapshot{active_key = AK1, all_keys = All1},
               #dek_snapshot{active_key = AK2, all_keys = All2}) ->
    (AK1 == AK2) andalso (lists:usort(All1) == lists:usort(All2)).

-spec get_deks_snapshot_hash(#dek_snapshot{}) -> non_neg_integer().
get_deks_snapshot_hash(#dek_snapshot{active_key = AK, all_keys = All}) ->
    AllIds = {get_dek_id(AK),
              lists:usort([{Id, Type} || #{id := Id, type := Type} <- All])},
    erlang:phash2(AllIds).

-spec get_encryption_method(encryption_type(),
                            cluster | node,
                            cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, cb_deks:encryption_method()} | {error, not_found}.
get_encryption_method(Type, _Scope, Snapshot) ->
    case menelaus_web_encr_at_rest:get_settings(Snapshot) of
        #{Type := EncryptionSettings}  ->
            {ok, case EncryptionSettings of
                     #{encryption := disabled} -> disabled;
                     #{encryption := encryption_service} -> encryption_service;
                     #{encryption := secret, secret_id := Id} -> {secret, Id}
                 end};
        #{} ->
            {error, not_found}
    end.

-spec get_dek_kind_lifetime(encryption_type(),
                            cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, pos_integer()} | {error, not_found}.
get_dek_kind_lifetime(Type, Snapshot) ->
    case menelaus_web_encr_at_rest:get_settings(Snapshot) of
        #{Type := #{dek_lifetime_in_sec := ?DEK_INFINITY_LIFETIME}} ->
            {ok, undefined};
        #{Type := #{dek_lifetime_in_sec := Lifetime}} ->
            {ok, Lifetime};
        #{} ->
            {error, not_found}
    end.

-spec get_dek_rotation_interval(encryption_type(),
                                cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | pos_integer()} | {error, not_found}.
get_dek_rotation_interval(Type, Snapshot) ->
    case menelaus_web_encr_at_rest:get_settings(Snapshot) of
        #{Type := #{dek_rotation_interval_in_sec := 0}} -> {ok, undefined};
        #{Type := #{dek_rotation_interval_in_sec := Int}} -> {ok, Int};
        #{} -> {error, not_found}
    end.

-spec get_drop_keys_timestamp(encryption_type(),
                              cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_drop_keys_timestamp(Type, Snapshot) ->
    case menelaus_web_encr_at_rest:get_settings(Snapshot) of
        #{Type := #{dek_drop_datetime := {set, DT}}} ->
            {ok, DT};
        #{Type := #{dek_drop_datetime := {not_set, _}}} ->
            {ok, undefined};
        #{} ->
            {error, not_found}
    end.

-spec get_force_encryption_timestamp(encryption_type(),
                                    cb_cluster_secrets:chronicle_snapshot()) ->
          {ok, undefined | calendar:datetime()} | {error, not_found}.
get_force_encryption_timestamp(Type, Snapshot) ->
    maybe
        Settings = menelaus_web_encr_at_rest:get_settings(Snapshot),
        TypeSettings = maps:get(Type, Settings, undefined),
        {ok, ForceDT} ?=
            case TypeSettings of
                undefined ->
                    {error, not_found};
                #{force_encryption_datetime := {set, DT}} ->
                    {ok, DT};
                #{force_encryption_datetime := {not_set, _}} ->
                    get_drop_keys_timestamp(Type, Snapshot)
            end,
        LastToggleDT = maps:get(encryption_last_toggle_datetime, TypeSettings,
                                undefined),
        if
            ForceDT =:= undefined -> {ok, undefined};
            LastToggleDT =:= undefined -> {ok, ForceDT};
            LastToggleDT > ForceDT -> {ok, undefined};
            true -> {ok, ForceDT}
        end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

is_valid_encr_header({ok, HeaderData}) ->
    case parse_header(HeaderData) of
        {ok, _Parsed} ->
            true;
        _ ->
            false
    end;
is_valid_encr_header({error, enoent}) ->
    false;
is_valid_encr_header(eof) ->
    false.

header_key_match({ok, HeaderData}, KeyId) ->
    case parse_header(HeaderData) of
        {ok, {_Vsn, KeyId, _ADPrefix, _Offset, _CompressionType, _Rest}} ->
            true;
        _ ->
            false
    end;
header_key_match({error, enoent}, _KeyId) ->
    false;
header_key_match(eof, _KeyId) ->
    false.

get_key_id(undefined) ->
    undefined;
get_key_id(#{id := KeyId} = _Key) ->
    KeyId.

check_next_chunk_exists(_File, {error, _}, _CurrentOffset) ->
    false;
check_next_chunk_exists(File, eof, CurrentExpectedOffset) ->
    case file:position(File, eof) of
        {ok, CurrentExpectedOffset} ->
            true;
        _ ->
            false
    end;
check_next_chunk_exists(File,
                        {ok, <<ChunkSize:32/big-unsigned-integer>>},
                        CurrentOffset) ->
    NewExpectedOffset = ChunkSize + CurrentOffset + ?CHUNKSIZE_ATTR_SIZE,
    NextSizeRead = file:pread(File, NewExpectedOffset, ?CHUNKSIZE_ATTR_SIZE),
    check_next_chunk_exists(File, NextSizeRead, NewExpectedOffset);
check_next_chunk_exists(_File, {ok, _}, _CurrExpectedOffset) ->
    false.

validate_encr_file(FilePath) ->
    {ok, File} = file:open(FilePath, [raw, binary, read]),
    try
        Header = file:read(File, ?ENCRYPTED_FILE_HEADER_LEN),
        case is_valid_encr_header(Header) of
            true ->
                NextSizeRead =  file:pread(File, ?ENCRYPTED_FILE_HEADER_LEN,
                                           ?CHUNKSIZE_ATTR_SIZE),
                check_next_chunk_exists(File, NextSizeRead,
                                        ?ENCRYPTED_FILE_HEADER_LEN);
            false ->
                false
        end
    after
        file:close(File)
    end.


encrypt_internal(_Data, _AD, _IVRandom, _IVAtomic, ?DEK_ERROR_PATTERN(_, _)) ->
    {error, key_not_available};
encrypt_internal(Data, AD, IVRandom, IVAtomic, #{type := 'raw-aes-gcm',
                                                 info := #{key := KeyFun}}) ->
    IV = new_aes_gcm_iv(IVRandom, IVAtomic),
    %% Tag size is 16 bytes as it is specified in requirements
    {EncryptedData, Tag} =
        crypto:crypto_one_time_aead(
          aes_256_gcm, KeyFun(), IV, Data, AD, ?TAG_LEN, true),
    ?TAG_LEN = size(Tag),
    {ok, <<IV/binary, EncryptedData/binary, Tag/binary>>}.

decrypt_internal(_Data, _AD, []) ->
    {error, decrypt_error};
decrypt_internal(Data, AD, DekList) ->
    DataSize = size(Data) - ?IV_LEN - ?TAG_LEN,
    case DataSize > 0 of
        true ->
            <<IV:?IV_LEN/binary,
              EncryptedData:DataSize/binary,
              Tag:?TAG_LEN/binary>> = Data,
            try_decrypt(IV, EncryptedData, Tag, AD, DekList);
        false ->
            {error, invalid_data_chunk}
    end.

try_decrypt(_IV, _Data, _Tag, _AD, []) -> {error, decrypt_error};
try_decrypt(IV, Data, Tag, AD, [?DEK_ERROR_PATTERN(_, _) | T]) ->
    try_decrypt(IV, Data, Tag, AD, T);
try_decrypt(IV, Data, Tag, AD, [#{type := 'raw-aes-gcm', info := #{key := K}} | T]) ->
    case crypto:crypto_one_time_aead(aes_256_gcm, K(), IV, Data, AD,
                                     Tag, false) of
        error -> try_decrypt(IV, Data, Tag, AD, T);
        Decrypted -> {ok, Decrypted}
    end.

-spec read_deks(cb_deks:dek_kind(), #dek_snapshot{} | undefined) ->
          {ok, #dek_snapshot{}} | {error, _}.
read_deks(DekKind, PrevDekSnapshot) ->
    maybe
        {ok, {ActiveId, Ids, IsEnabled}} ?= cb_deks:list(DekKind),
        PrevKeys =
            case PrevDekSnapshot of
                undefined -> [];
                #dek_snapshot{all_keys = PK} -> PK
            end,
        %% No need to reread these keys:
        GoodPrevKeys = [K || #{id := Id, type := T} = K <- PrevKeys,
                             T =/= error, lists:member(Id, Ids)],
        %% Note that GoodPrevKeysIds is a subset of Ids
        %% so, Ids == IdsMissing U GoodPrevKeysIds
        GoodPrevKeysIds = lists:usort([get_dek_id(K) || K <- GoodPrevKeys]),
        IdsMissing = lists:usort(Ids) -- GoodPrevKeysIds,
        Keys = cb_deks:read(DekKind, IdsMissing) ++ GoodPrevKeys,
        {ok, ActiveKey} ?=
            case IsEnabled of
                true ->
                    case lists:search(fun (#{id := Id}) ->
                                          Id == ActiveId
                                      end, Keys) of
                        {value, AK} ->
                            {ok, AK};
                        false ->
                            {error, {missing_active_key, ActiveId}}
                    end;
                false ->
                    {ok, undefined}
            end,
        {ok, create_deks_snapshot(ActiveKey, Keys, PrevDekSnapshot)}
    else
        {error, Reason} ->
            ?log_error("Failed to read and set encryption keys for ~p: ~p",
                       [DekKind, Reason]),
            {error, Reason}
    end.

encrypt_to_file(IODevice, Bytes, MaxChunkSize, Compression, DekSnapshot) ->
    WriteChunks =
        fun EncryptAndWrite(<<>>, StateAcc) ->
                FinalData = file_encrypt_finish(StateAcc),
                case file:write(IODevice, FinalData) of
                    ok -> ok;
                    {error, _} = Error -> Error
                end;
            EncryptAndWrite(DataToWrite, StateAcc) ->
                {Chunk, Rest} =
                    case DataToWrite of
                        <<C:MaxChunkSize/binary, R/binary>> -> {C, R};
                        <<R/binary>> -> {R, <<>>}
                    end,
                {EncryptedChunk, NewStateAcc} =
                    file_encrypt_chunk(Chunk, StateAcc),
                case file:write(IODevice, EncryptedChunk) of
                    ok ->
                        EncryptAndWrite(Rest, NewStateAcc);
                    {error, _} = Error ->
                        _ = file_encrypt_finish(NewStateAcc),
                        Error
                end
        end,
    maybe
        {ok, {Header, State}} ?= file_encrypt_init(
                                   DekSnapshot,
                                   #{encr_compression => Compression}),
        ok ?= file:write(IODevice, Header),
        ok ?= WriteChunks(Bytes, State)
    end.

new_aes_gcm_iv(IVRandom, IVAtomic) ->
    %% For GCM IV must be unique, and don't have to be unpredictable
    IVCounter = atomics:add_get(IVAtomic, 1, 1),
    IV = <<IVRandom/binary, IVCounter:64/big-unsigned-integer>>,
    ?IV_LEN = size(IV),
    IV.

file_assoc_data(State) ->
    {Prefix, Offset} =
        case State of
            #file_encr_state{ad_prefix = P, offset = O} -> {P, O};
            #file_decr_state{ad_prefix = P, offset = O} -> {P, O}
        end,
    <<Prefix/binary, Offset:64/big-unsigned-integer>>.

decrypt_file_data(Data, GetDekSnapshotFun) ->
    case file_decrypt_init(Data, GetDekSnapshotFun) of
        {ok, {State, Chunks}} -> decrypt_all_chunks(State, Chunks, []);
        incomplete_magic -> {error, unknown_magic};
        %% Header is incomplete, but we know there is no more data, so we treat
        %% it as bad header
        need_more_data -> {error, invalid_file_encryption};
        {error, _} = E -> E
    end.

decrypt_all_chunks(State, Data, Acc) ->
    case file_decrypt_next_chunk(Data, State) of
        {ok, {NewState, Chunk, Rest}} ->
            decrypt_all_chunks(NewState, Rest, [Chunk | Acc]);
        eof ->
            maybe
                ok ?= file_decrypt_finish(State),
                {ok, iolist_to_binary(lists:reverse(Acc))}
            end;
        need_more_data ->
            file_decrypt_finish(State),
            {error, invalid_file_encryption}
    end.

-spec parse_header(binary()) ->
    {ok, {Vsn, KeyId, ADPrefix, Offset, CompressionType, Rest}} |
    incomplete_magic |
    need_more_data |
    {error, unknown_magic |
            bad_header |
            {unsupported_encryption_version, _}} when
    Vsn :: non_neg_integer(),
    KeyId :: binary(),
    ADPrefix :: binary(),
    Offset :: non_neg_integer(),
    CompressionType :: non_neg_integer(),
    Rest :: binary().
parse_header(<<Header:?ENCRYPTED_FILE_HEADER_LEN/binary, Rest/binary>>) ->
    %% Full header is present
    case Header of
        <<?ENCRYPTED_FILE_MAGIC, 0, CompressionType, _, _, _, _,
          36, Key:36/binary, _Salt:16/binary>> ->
            {ok, {0, Key, Header, ?ENCRYPTED_FILE_HEADER_LEN, CompressionType,
                  Rest}};
        <<?ENCRYPTED_FILE_MAGIC, 0, _/binary>> ->
            {error, bad_header};
        <<?ENCRYPTED_FILE_MAGIC, V, _/binary>> ->
            {error, {unsupported_encryption_version, V}};
        _ ->
            {error, unknown_magic}
    end;
parse_header(<<?ENCRYPTED_FILE_MAGIC, _/binary>>) ->
    %% File is encrypted but header is incomplete
    need_more_data;
parse_header(Data) ->
    DataSize = byte_size(Data),
    case DataSize > byte_size(<<?ENCRYPTED_FILE_MAGIC>>) of
        true -> {error, unknown_magic};
        false ->
            CommonPrefixSize = binary:longest_common_prefix(
                                 [Data, <<?ENCRYPTED_FILE_MAGIC>>]),
            case CommonPrefixSize == DataSize of
                true -> incomplete_magic;
                false -> {error, unknown_magic}
            end
    end.

-ifdef(TEST).

parse_header_test() ->
    Salt = crypto:strong_rand_bytes(16),
    Key = "7c8b0d58-2977-429c-8c0c-f9adaaac0919",
    KeyBin = list_to_binary(Key),
    Header = <<?ENCRYPTED_FILE_MAGIC, 0, ?NO_COMPRESSION, 0, 0, 0, 0,
               36, KeyBin/binary, Salt/binary>>,
    HeaderSize = byte_size(Header),
    ?assertEqual(?ENCRYPTED_FILE_HEADER_LEN, HeaderSize),
    LongData = crypto:strong_rand_bytes(HeaderSize * 2),
    ?assertEqual({ok, {0, KeyBin, Header, HeaderSize, ?NO_COMPRESSION, <<>>}},
                 parse_header(Header)),
    ?assertEqual({ok, {0, KeyBin, Header, HeaderSize, ?NO_COMPRESSION,
                       <<"Rest">>}},
                 parse_header(<<Header/binary, "Rest">>)),

    ?assertEqual(incomplete_magic, parse_header(<<>>)),
    ?assertEqual(incomplete_magic, parse_header(<<0>>)),
    ?assertEqual(incomplete_magic, parse_header(<<0, "Couchbase">>)),
    ?assertEqual({error, unknown_magic}, parse_header(<<1>>)),
    ?assertEqual({error, unknown_magic}, parse_header(<<1, LongData/binary>>)),

    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 0>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 0, 0, 0>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 0, 0, 0, 0, 0, 36>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 0, 0, 0, 0, 0, 36,
                                1>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 0, 0, 0, 0, 0, 36,
                                KeyBin/binary>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 0, 0, 0, 0, 0, 36,
                                KeyBin/binary, 1,2,3>>)),

    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 2>>)),
    ?assertEqual({error, {unsupported_encryption_version, 2}},
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 2, LongData/binary>>)),
    ?assertEqual(need_more_data,
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 2>>)),
    ?assertEqual({error, bad_header},
                 parse_header(<<?ENCRYPTED_FILE_MAGIC, 0, 2, 0, 0, 0, 0, 37,
                              LongData/binary>>)).

-endif.

bite_next_chunk(<<ChunkSize:32/big-unsigned-integer, Chunk:ChunkSize/binary,
                   Rest/binary>>) ->
    DataSize = ChunkSize - ?IV_LEN - ?TAG_LEN,
    case DataSize > 0 of
        true -> {ok, {ChunkSize + ?CHUNKSIZE_ATTR_SIZE, Chunk}, Rest};
        false -> {error, invalid_data_chunk}
    end;
bite_next_chunk(<<>>) ->
    eof;
bite_next_chunk(<<_/binary>>) ->
    need_more_data.

find_key(WantedId, #dek_snapshot{all_keys = AllDeks}) ->
    case lists:search(fun (#{id := Id}) -> Id == WantedId end, AllDeks) of
        {value, ?DEK_ERROR_PATTERN(_, _)} -> {error, key_not_available};
        {value, Key} -> {ok, Key};
        false -> {error, key_not_found}
    end.

read_file_header(Path) ->
    maybe
        {ok, File} ?= file:open(Path, [read, raw, binary]),
        try
            file:read(File, ?ENCRYPTED_FILE_HEADER_LEN)
        after
            file:close(File)
        end
    end.

-ifdef(TEST).

decrypt_file_data_test() ->
    DS = generate_test_deks(),
    Data1 = <<"123">>,
    Data2 = <<"456">>,
    LongData = crypto:strong_rand_bytes(?ENCRYPTED_FILE_HEADER_LEN * 2),
    {ok, {Header, State0}} = file_encrypt_init(DS),
    {Chunk1, State1} = file_encrypt_chunk(Data1, State0),
    {Chunk2, _State2} = file_encrypt_chunk(Data2, State1),
    {ok, <<>>} = decrypt_file_data(Header, DS),
    {ok, Data1} = decrypt_file_data(<<Header/binary, Chunk1/binary>>, DS),
    Data1Data2 = <<Data1/binary, Data2/binary>>,
    {ok, Data1Data2} =
        decrypt_file_data(<<Header/binary, Chunk1/binary, Chunk2/binary>>, DS),

    {error, unknown_magic} = decrypt_file_data(<<>>, DS),
    {error, unknown_magic} = decrypt_file_data(<<0>>, DS),
    {error, unknown_magic} = decrypt_file_data(<<0, "Couch">>, DS),
    {error, unknown_magic} = decrypt_file_data(<<1, 2, 3>>, DS),
    {error, invalid_file_encryption} =
        decrypt_file_data(<<?ENCRYPTED_FILE_MAGIC>>, DS),
    {error, invalid_file_encryption} =
        decrypt_file_data(<<?ENCRYPTED_FILE_MAGIC, 123>>, DS),
    {error, invalid_file_encryption} =
        decrypt_file_data(<<?ENCRYPTED_FILE_MAGIC, 0, 123>>, DS),
    {error, invalid_file_encryption} =
        decrypt_file_data(<<?ENCRYPTED_FILE_MAGIC, 0, ?NO_COMPRESSION>>, DS),
    {error, {unsupported_compression_type, 23}} =
        decrypt_file_data(<<?ENCRYPTED_FILE_MAGIC, 0, 23, 0, 0, 0, 0, 36,
                            LongData/binary>>, DS).

read_write_encr_file_test() ->
    Path = path_config:tempfile("cb_crypto_test", ".tmp"),
    Bin = iolist_to_binary(lists:seq(0,255)),
    DS = generate_test_deks(),
    try
        ok = misc:atomic_write_file(Path, Bin),
        {raw, Bin} = read_file(Path, DS),
        ok = atomic_write_file(Path, Bin, DS, #{max_chunk_size => 7}),
        {decrypted, Bin} = read_file(Path, DS),
        %% Note that chunks are actually reversed here:
        {ok, Chunks1} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS, #{read_chunk_size => 11}),
        {ok, Chunks2} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS, #{read_chunk_size => 7}),
        {ok, Chunks3} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS, #{read_chunk_size => 3}),
        {ok, Chunks4} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS,
                                         #{read_chunk_size => 1000000000}),
        %% All chunks should be the same. It should not matter how we read them
        Chunks1 = Chunks2,
        Chunks1 = Chunks3,
        Chunks1 = Chunks4,
        %% Last chunk should be 4 byte chunk, other chunks should be 7 bytes.
        [LastChunk | OtherChunks] = Chunks1,
        4 = byte_size(LastChunk),
        [7] = lists:uniq([byte_size(C) || C <- OtherChunks]),

        %% If we concatenate all chunks, we should get the original data
        Bin = iolist_to_binary(lists:reverse(Chunks1))
    after
        file:delete(Path)
    end.

read_write_decr_file_test() ->
    Path = path_config:tempfile("cb_crypto_test", ".tmp"),
    Bin = iolist_to_binary(lists:seq(0,255)),
    DS0 = generate_test_deks(),
    {_, AllDeks} = get_all_deks(DS0),
    DS = create_deks_snapshot(undefined, AllDeks, undefined),
    Opt = #{allow_decrypted => true},
    try
        ok = misc:atomic_write_file(Path, Bin),
        {raw, Bin} = read_file(Path, DS),
        ok = atomic_write_file(Path, Bin, DS, #{max_chunk_size => 7}),
        {raw, Bin} = read_file(Path, DS),
        %% Note that chunks are actually reversed here:
        {ok, Chunks1} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS, Opt#{read_chunk_size => 11}),
        {ok, Chunks2} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS, Opt#{read_chunk_size => 7}),
        {ok, Chunks3} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS, Opt#{read_chunk_size => 3}),
        {ok, Chunks4} = read_file_chunks(Path,
                                         fun (C, Acc) -> {ok, [C | Acc]} end,
                                         [], DS,
                                         Opt#{read_chunk_size => 1000000000}),

        %% allow_decrypted is not passed in here, it should error out
        {error,unknown_magic,[]} =
            read_file_chunks(Path,
                             fun (C, Acc) -> {ok, [C | Acc]} end,
                             [], DS, #{read_chunk_size => 3}),

        %% allow_decrypted is explicitly false in here, it should error out
        {error,unknown_magic,[]} =
            read_file_chunks(Path,
                             fun (C, Acc) -> {ok, [C | Acc]} end,
                             [], DS, #{read_chunk_size => 3,
                                       allow_decrypted => false}),

        %% If we concatenate all chunks, we should get the original data
        Bin = iolist_to_binary(lists:reverse(Chunks1)),
        Bin = iolist_to_binary(lists:reverse(Chunks2)),
        Bin = iolist_to_binary(lists:reverse(Chunks3)),
        Bin = iolist_to_binary(lists:reverse(Chunks4))
    after
        file:delete(Path)
    end.

read_write_compressed_file_test_() ->
    Bin = rand:bytes(1024 * 1024),
    DS = generate_test_deks(),
    Chunks = [1024, 1024 * 1024 * 2, 101],
    Levels = [1, 9],
    FlushTypes = [full, sync, none],
    [?_test(read_write_compressed_file_test_parametrized(Bin, DS, X, Y, Z, W))
     || X <- Chunks, Y <- Chunks, Z <- Levels, W <- FlushTypes].

read_write_compressed_file_test_parametrized(Bin, DS, WriteChunkSize,
                                             ReadChunkSize, Level, FlushType) ->
    Path = path_config:tempfile("cb_crypto_test", ".tmp"),
    try
        ok = atomic_write_file(Path, Bin, DS,
                               #{max_chunk_size => WriteChunkSize,
                                 encr_compression => {zlib, Level, FlushType}}),
        {decrypted, Bin} = read_file(Path, DS),
        %% Note that chunks are actually reversed here:
        {ok, Chunks1} = read_file_chunks(
                          Path,
                          fun (C, Acc) -> {ok, [C | Acc]} end,
                          [], DS, #{read_chunk_size => ReadChunkSize}),
        %% If we concatenate all chunks, we should get the original data
        Bin = iolist_to_binary(lists:reverse(Chunks1))
    after
        file:delete(Path)
    end.

validate_encr_file_test() ->
    %% Generate valid header and chunk data and assemble and test for various
    %% cases
    Path = path_config:tempfile("cb_crypto_validate_encr_file_test", ".tmp"),
    try
        DS = generate_test_deks(),
        {ok, {ValidHeader, State}} = file_encrypt_init(DS),

        Bin1 = iolist_to_binary(lists:seq(0,255)),
        {Data1, State1} = file_encrypt_chunk(Bin1, State),

        Bin2 = iolist_to_binary(lists:seq(0,1)),
        {Data2,State2} = file_encrypt_chunk(Bin2, State1),

        Bin3 = iolist_to_binary(lists:seq(0,64)),
        {Data3, _} = file_encrypt_chunk(Bin3, State2),

        %% File with just header is valid encrypted file
        ok = misc:atomic_write_file(Path, ValidHeader),
        ?assert(validate_encr_file(Path)),

        %% File with trash after header is invalid
        ok = misc:atomic_write_file(Path, <<ValidHeader/binary, "t">>),
        ?assertNot(validate_encr_file(Path)),

        %% Full valid file
        FullValid =
            <<ValidHeader/binary, Data1/binary, Data2/binary, Data3/binary>>,
        ok = misc:atomic_write_file(Path, FullValid),
        ?assert(validate_encr_file(Path)),

        %% Full valid file with trailing trash is invalid
        ok = misc:atomic_write_file(Path, <<FullValid/binary, "t">>),
        ?assertNot(validate_encr_file(Path)),

        %% Partial first chunk with everything else in place is invalid
        InvalidSize = byte_size(Data1) - 1,
        InvalidFileData =
            <<ValidHeader/binary, Data1:InvalidSize/binary, Data2/binary,
              Data3/binary>>,
        ok = misc:atomic_write_file(Path, InvalidFileData),
        ?assertNot(validate_encr_file(Path))
    after
        file:delete(Path)
    end.

reencrypt_with_opts_mix_test() ->
    Path = path_config:tempfile("reencrypt_with_opts_mix_test", ".tmp"),
    Size = 10 * 1024 * 1024,
    Data = rand:bytes(Size),
    DS = generate_test_deks(),
    {_, AllDeks} = get_all_deks(DS),
    DSEmptyActive = create_deks_snapshot(undefined, AllDeks, undefined),

    ReadFileData =
        fun (P, Opts) ->
            {ok, IO} = file:open(P, [raw, binary] ++ Opts),
            try
                {ok, Data1} = file:read(IO, Size+100),
                Data1
            after
                file:close(IO)
            end
        end,

    try
        %% Encrypt file with encr_compression but with an empty DS hence the
        %% resulting file should be decrypted, and it should not be compressed
        ok = atomic_write_file(Path, Data, DSEmptyActive,
                               #{encr_compression => {zlib, 5, none}}),
        ?assert(Data =:= ReadFileData(Path, [])),

        %% Encrypt a file and reencrypt it with allow_decrypt=true and no
        %% decr_compression option and with no active key DS, and ensure
        %% resulting file is unencrypted and not compressed
        ok = atomic_write_file(Path, Data, DS,
                               #{encr_compression => {zlib, 5, none}}),

        %% Note: The encr_compression option is a no OP here since allow_decrypt
        %% is used with an emptyDS and thus the resulting file will be
        %% unencrypted, and since there is no decr_compression option, we
        %% verify that it must not be compressed
        {ok, unencrypted} =
            reencrypt_file(Path, Path, DSEmptyActive,
                           #{allow_decrypt => true,
                             encr_compression => {zlib, 5, none}}),
        {ok, DataRead0} = misc:raw_read_file(Path),
        ?assert(Data =:= DataRead0),

        %% Now try reencrypt on an encrypted file, but with explicit option of
        %% decr_compression => undefined combined with allow_decrypt => true,
        %% and using emptyDS, the result must be a unencrypted file that is not
        %% compressed(the same as no decr_compression opt being specified)
        ok = atomic_write_file(Path, Data, DS,
                               #{encr_compression => {zlib, 5, none}}),
        {ok, unencrypted} =
            reencrypt_file(Path, Path, DSEmptyActive,
                           #{allow_decrypt => true,
                             encr_compression => {zlib, 5, none},
                             decr_compression => undefined}),
        {ok, DataRead0} = misc:raw_read_file(Path),
        ?assert(Data =:= DataRead0),

        %% Try to encrypt file with active DS but without the allow_decrypt
        %% option, by default reencrypt_file() defined behavior is that only
        %% encrypted files be passed to it, so it should error out in this
        %% case
        Rv = reencrypt_file(Path, Path, DS, #{}),
        ?assertEqual(Rv, {error,unknown_magic}),

        %% Now rencrypt the decrypted file with allow_decrypt => true using
        %% active key DS, and the decrypted file should get re-encrypted into
        %% an encrypted file
        {ok, couchbase_encrypted} =
            reencrypt_file(Path, Path, DS,
                           #{allow_decrypt => true}),
        ?assert(is_file_encrypted(Path)),

        %% Take the encrypted file and reencrypt it with zlib compression and
        %% finally reencrypt it again with emptyDS and allow_decrypt => true,
        %% and decr_compression => gzip. The resulting file should be
        %% unencrypted and gzip compressed, we should be able to read the
        %% data from this file via "compressed" option using file:open() and
        %% the resulting data should match exactly the original source data
        {ok, couchbase_encrypted} =
            reencrypt_file(Path, Path, DS,
                           #{encr_compression => {zlib, 5, none}}),
        ?assert(is_file_encrypted(Path)),
        {ok, gzip} =
            reencrypt_file(Path, Path, DSEmptyActive,
                           #{allow_decrypt => true,
                             decr_compression => gzip,
                             encr_compression => {zlib, 5, none}}),
        ?assertNot(is_file_encrypted(Path)),
        CompressedData = ReadFileData(Path, []),
        UnCompressed = ReadFileData(Path, [compressed]),
        ?assertNot(CompressedData =:= Data),
        ?assert(UnCompressed =:= Data)
    after
        file:delete(Path)
    end.

reencrypt_on_disabled_enrc_test() ->
    Path = path_config:tempfile("cb_crypto_reencrypt_on_disabled_test", ".tmp"),
    Data = rand:bytes(1024),
    DS = generate_test_deks(),
    {_, AllDeks} = get_all_deks(DS),
    DSEmptyActive = create_deks_snapshot(undefined, AllDeks, undefined),
    try
        ok = atomic_write_file(Path, Data, DS, #{}),
        {ok, couchbase_encrypted} =
            reencrypt_file(Path, Path,
                           DSEmptyActive,#{}),
        ?assert(is_file_encrypted(Path)),
        {ok, unencrypted} =
            reencrypt_file(Path, Path, DSEmptyActive,
                           #{allow_decrypt => true}),
        ?assertNot(is_file_encrypted(Path)),
        {ok, Bin} = misc:raw_read_file(Path),
        ?assert(Bin =:= Data)
    after
        file:delete(Path)
    end.

reencrypt_decrypted_file_test() ->
    Path = path_config:tempfile("cb_crypto_reencrypt_decrypted_test", ".tmp"),
    Data = rand:bytes(10 * 1024 * 1024),
    DS = generate_test_deks(),
    {_, AllDeks} = get_all_deks(DS),
    DSEmptyActive = create_deks_snapshot(undefined, AllDeks, undefined),

    try
        ok = misc:atomic_write_file(Path, Data),

        {error, unknown_magic} = reencrypt_file(Path, Path, DSEmptyActive, #{}),
        {error, unknown_magic} = reencrypt_file(Path, Path, DS,#{}),
        ?assertNot(is_file_encrypted(Path)),

        {ok, unencrypted} =
            reencrypt_file(Path, Path, DSEmptyActive,
                           #{allow_decrypt => true}),
        ?assertNot(is_file_encrypted(Path)),

        {ok, couchbase_encrypted}
            = reencrypt_file(Path, Path, DS,
                             #{allow_decrypt => true,
                               max_chunk_size => 293}),
        ?assert(is_file_encrypted(Path)),

        {ok, unencrypted} =
            reencrypt_file(Path, Path, DSEmptyActive,
                           #{allow_decrypt => true}),
        ?assertNot(is_file_encrypted(Path)),
        {ok, Bin} = misc:raw_read_file(Path),
        ?assert(Bin =:= Data)
    after
        file:delete(Path)
    end.

reencrypt_file_test_() ->
    Bin = rand:bytes(10 * 1024 * 1024),
    DS = generate_test_deks(),
    {_, AllDeks} = get_all_deks(DS),
    DSEmptyActive = create_deks_snapshot(undefined, AllDeks, undefined),
    FromOpts = [#{max_chunk_size => N} || N <- [293, 1031, 7829, 1024 * 1024,
                                                1024 * 1024 * 100]],
    ToOpts = [#{encr_compression => undefined},
              #{encr_compression => {zlib, 1, full}},
              #{encr_compression => {zlib, 5, none}}],
    [?_test(reencrypt_file_test_parametrized(Bin, From, To, DS, ReencryptDS))
     || From <- FromOpts, To <- ToOpts, ReencryptDS <- [DS, DSEmptyActive]].

reencrypt_file_test_parametrized(Bin, FromOpts, ToOpts, DS1, DS2) ->
    Path1 = path_config:tempfile("cb_crypto_reencrypt_file_test1", ".tmp"),
    Path2 = path_config:tempfile("cb_crypto_reencrypt_file_test2", ".tmp"),
    try
        ok = atomic_write_file(Path1, Bin, DS1, FromOpts),
        {ok, couchbase_encrypted} =
            reencrypt_file(Path1, Path2, DS2, ToOpts),
        {decrypted, Bin} = read_file(Path2, DS2)
    after
        file:delete(Path1),
        file:delete(Path2)
    end.

reencrypt_file_negative_cases_test() ->
    DS = generate_test_deks(),
    {_, AllDeks1} = get_all_deks(DS),
    DSNoActive = create_deks_snapshot(undefined, AllDeks1, DS),
    WrongDS = generate_test_deks(),
    {_, AllDeks2} = get_all_deks(WrongDS),
    WrongDSNoActive = create_deks_snapshot(undefined, AllDeks2, WrongDS),
    Path1 = path_config:tempfile("cb_crypto_reencrypt_file_test1", ".tmp"),
    Path2 = path_config:tempfile("cb_crypto_reencrypt_file_test2", ".tmp"),
    Opts = #{encr_compression => undefined},

    try
        {error, enoent} = reencrypt_file(Path1, Path2, DS, Opts),
        {error, enoent} = reencrypt_file(Path1, Path2, DSNoActive, Opts),

        Bin = rand:bytes(1024 * 1024),
        ok = atomic_write_file(Path1, Bin, DS),
        {error, key_not_found} = reencrypt_file(Path1, Path2, WrongDS, Opts),
        {error, key_not_found} = reencrypt_file(Path1, Path2, WrongDSNoActive,
                                                Opts),

        %% incomplete last chunk
        {ok, H} = file:open(Path1, [append, raw, binary]),
        L = ?IV_LEN + ?TAG_LEN + 100,
        ok = file:write(H, <<L:32/big-integer, (rand:bytes(L - 1))/binary>>),
        ok = file:close(H),
        {error, invalid_file_encryption} = reencrypt_file(Path1, Path2, DS,
                                                          Opts),

        {ok, couchbase_encrypted} =
            reencrypt_file(Path1, Path2, DS,
                           Opts#{ignore_incomplete_last_chunk => true}),

        ok = misc:atomic_write_file(Path1, Bin), %% unencrypted
        {error, unknown_magic} = reencrypt_file(Path1, Path2, WrongDS, Opts),
        {error, unknown_magic} = reencrypt_file(Path1, Path2, WrongDSNoActive,
                                                Opts),

        ok = misc:atomic_write_file(Path1, <<0,1>>), %% data shorter than magic
        {error, unknown_magic} = reencrypt_file(Path1, Path2, WrongDS, Opts),
        {error, unknown_magic} = reencrypt_file(Path1, Path2, WrongDSNoActive,
                                                Opts)
    after
        file:delete(Path1),
        file:delete(Path2)
    end.

generate_test_deks() ->
    Key = generate_test_key(),
    cb_crypto:create_deks_snapshot(Key, [Key], undefined).

generate_test_key() ->
    KeyBin = cb_cluster_secrets:generate_raw_key(aes_256_gcm),
    encryption_service:new_dek_record(
      cb_cluster_secrets:new_key_id(), 'raw-aes-gcm',
      encryption_service:new_raw_aes_dek_info(KeyBin, <<"encryptionService">>,
                                              {{2024, 01, 01}, {22, 00, 00}})).

test_error_key(Id) ->
    encryption_service:new_dek_record(Id, error, {test_error, "test error"}).

create_deks_snapshot_test() ->
    K1 = generate_test_key(),
    K2 = generate_test_key(),
    K3 = generate_test_key(),
    EK1 = test_error_key(get_key_id(K1)),
    EK2 = test_error_key(get_key_id(K2)),
    EK3 = test_error_key(get_key_id(K3)),

    ?assertNotMatch(?DEK_ERROR_PATTERN(_, _), K1),
    ?assertNotMatch(?DEK_ERROR_PATTERN(_, _), K2),
    ?assertNotMatch(?DEK_ERROR_PATTERN(_, _), K3),

    ?assertMatch(?DEK_ERROR_PATTERN(_, _), EK1),
    ?assertMatch(?DEK_ERROR_PATTERN(_, _), EK2),
    ?assertMatch(?DEK_ERROR_PATTERN(_, _), EK3),

    %% No errors:
    ?assertMatch(#dek_snapshot{active_key = undefined, all_keys = [K1]},
                 create_deks_snapshot(undefined, [K1], undefined)),
    ?assertMatch(#dek_snapshot{active_key = K1, all_keys = [K1]},
                 create_deks_snapshot(K1, [K1], undefined)),
    ?assertMatch(#dek_snapshot{active_key = K1, all_keys = [K1, K2]},
                 create_deks_snapshot(K1, [K1, K2], undefined)),

    %% Keys with errors:
    ?assertMatch(#dek_snapshot{active_key = undefined, all_keys = [K1, EK2]},
                 create_deks_snapshot(undefined, [K1, EK2], undefined)),
    ?assertMatch(#dek_snapshot{active_key = EK2, all_keys = [K1, EK2]},
                 create_deks_snapshot(EK2, [K1, EK2], undefined)),

    %% Keys with errors and with prev DS:
    PrevDS = create_deks_snapshot(K1, [K1, K2], undefined),
    ?assertMatch(#dek_snapshot{active_key = K1, all_keys = [K1, K2]},
                 create_deks_snapshot(K1, [K1, EK2], PrevDS)),
    ?assertMatch(#dek_snapshot{active_key = K1, all_keys = [K2, K1]},
                 create_deks_snapshot(EK1, [K2, EK1], PrevDS)),
    ?assertMatch(#dek_snapshot{active_key = undefined, all_keys = [K2, K1]},
                 create_deks_snapshot(undefined, [K2, EK1], PrevDS)),
    ?assertMatch(#dek_snapshot{active_key = undefined, all_keys = [K2, K1]},
                 create_deks_snapshot(undefined, [EK2, EK1], PrevDS)),

    %% New DS and prev DS have different keys:
    ?assertMatch(#dek_snapshot{active_key = K2, all_keys = [K2, K3]},
                 create_deks_snapshot(K2, [EK2, K3], PrevDS)),
    ?assertMatch(#dek_snapshot{active_key = K3, all_keys = [K1, K2, K3]},
                 create_deks_snapshot(K3, [K1, EK2, K3], PrevDS)),
    ?assertMatch(#dek_snapshot{active_key = K3, all_keys = [K3]},
                 create_deks_snapshot(K3, [K3], PrevDS)),
    ?assertMatch(#dek_snapshot{active_key = EK3, all_keys = [EK3, K1]},
                 create_deks_snapshot(EK3, [EK3, EK1], PrevDS)),
    ?assertMatch(#dek_snapshot{active_key = K1, all_keys = [EK3, K1]},
                 create_deks_snapshot(EK1, [EK3, EK1], PrevDS)),

    %% Check that the IV counter gets preserved, while the IV random gets reset:
    NewDS = create_deks_snapshot(EK1, [EK3, EK1], PrevDS),
    ?assertEqual(PrevDS#dek_snapshot.iv_atomic_counter,
                 NewDS#dek_snapshot.iv_atomic_counter),
    ?assertNotEqual(PrevDS#dek_snapshot.iv_random,
                    NewDS#dek_snapshot.iv_random).

read_deks_test() ->
    K1 = generate_test_key(),
    K1Id = get_key_id(K1),
    K2 = generate_test_key(),
    K2Id = get_key_id(K2),
    K3 = generate_test_key(),
    K3Id = get_key_id(K3),
    EK1 = test_error_key(K1Id),
    EK2 = test_error_key(K2Id),
    EK3 = test_error_key(K3Id),

    meck:new(cb_deks, [passthrough]),
    try
        meck:expect(cb_deks, read,
                    fun (testDek, Ids) ->
                        %% K1 - ok
                        %% K2 - error
                        %% K3 - crash (should not be called)
                        ?assertNot(lists:member(K3Id, Ids)),
                        lists:filter(fun (#{id := Id}) ->
                                         lists:member(Id, Ids)
                                     end, [K1, EK2])
                    end),

        %% Active key is ok, historic key is bad:
        meck:expect(cb_deks, list,
                    fun (testDek) -> {ok, {K1Id, [K1Id, K2Id], true}} end),

        ?assertMatch({ok, #dek_snapshot{active_key = K1, all_keys = [K1, EK2]}},
                     read_deks(testDek, undefined)),

        PrevDS1 = create_deks_snapshot(undefined, [EK2, K3], undefined),
        ?assertMatch({ok, #dek_snapshot{active_key = K1, all_keys = [K1, EK2]}},
                     read_deks(testDek, PrevDS1)),

        PrevDS2 = create_deks_snapshot(undefined, [K2, EK3], undefined),
        ?assertMatch({ok, #dek_snapshot{active_key = K1, all_keys = [K1, K2]}},
                     read_deks(testDek, PrevDS2)),

        %% Active key is bad, historic key is ok:
        meck:expect(cb_deks, list,
                    fun (testDek) -> {ok, {K2Id, [K1Id, K2Id], true}} end),

        ?assertMatch({ok, #dek_snapshot{active_key = EK2,
                                        all_keys = [K1, EK2]}},
                     read_deks(testDek, PrevDS1)),

        ?assertMatch({ok, #dek_snapshot{active_key = K2, all_keys = [K1, K2]}},
                     read_deks(testDek, PrevDS2)),

        %% Do not re-read K3 (read_deks() has assert for K3 presence):
        meck:expect(cb_deks, list,
                    fun (testDek) -> {ok, {K3Id, [K2Id, K3Id], true}} end),

        PrevDS3 = create_deks_snapshot(EK1, [EK1, K3], undefined),
        ?assertMatch({ok, #dek_snapshot{active_key = K3,
                                        all_keys = [EK2, K3]}},
                     read_deks(testDek, PrevDS3)),

        %% Do not re-read K3 and encryption is disabled:
        meck:expect(cb_deks, list,
                    fun (testDek) ->
                        {ok, {K3Id, [K1Id, K2Id, K3Id], false}}
                    end),

        ?assertMatch({ok, #dek_snapshot{active_key = undefined,
                                        all_keys = [K1, EK2, K3]}},
                     read_deks(testDek, PrevDS3))
    after
        meck:unload(cb_deks)
    end.

-endif.
