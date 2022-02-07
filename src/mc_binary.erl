%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(mc_binary).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-export([bin/1, recv/2, recv/3,  quick_active_recv/3,
         send/4, send/2, encode/3,
         quick_stats/4, quick_stats/5, quick_stats_append/3,
         decode_packet/1, decode_packet_ext/1,
         get_keys/6, get_xattrs/5,
         maybe_encode_uid_in_key/3]).

-define(RECV_TIMEOUT,             ?get_timeout(recv, 120000)).
-define(QUICK_STATS_RECV_TIMEOUT, ?get_timeout(stats_recv, 300000)).

%% Functions to work with memcached binary protocol packets.

recv_with_data(Sock, Len, TimeoutRef, Data) ->
    DataSize = erlang:size(Data),
    case DataSize >= Len of
        true ->
            RV = binary_part(Data, 0, Len),
            Rest = binary_part(Data, Len, DataSize-Len),
            {ok, RV, Rest};
        false ->
            ok = inet:setopts(Sock, [{active, once}]),
            receive
                {tcp, Sock, NewData} ->
                    recv_with_data(Sock, Len, TimeoutRef, <<Data/binary, NewData/binary>>);
                {tcp_closed, Sock} ->
                    throw({error, closed});
                TimeoutRef ->
                    throw({error, timeout})
            end
    end.

quick_active_recv(Sock, Data, TimeoutRef) ->
    {ok, Hdr, Rest} = recv_with_data(Sock, ?HEADER_LEN, TimeoutRef, Data),
    {Header, Entry} = decode_header(res, Hdr),
    #mc_header{extlen = ExtLen,
               keylen = KeyLen,
               bodylen = BodyLen} = Header,
    case BodyLen > 0 of
        true ->
            true = BodyLen >= (ExtLen + KeyLen),
            {ok, Ext, Rest2} = recv_with_data(Sock, ExtLen, TimeoutRef, Rest),
            {ok, Key, Rest3} = recv_with_data(Sock, KeyLen, TimeoutRef, Rest2),
            RealBodyLen = erlang:max(0, BodyLen - (ExtLen + KeyLen)),
            {ok, BodyData, Rest4} = recv_with_data(Sock, RealBodyLen, TimeoutRef, Rest3),
            {ok, Header, Entry#mc_entry{ext = Ext, key = Key, data = BodyData}, Rest4};
        false ->
            {ok, Header, Entry, Rest}
    end.

quick_stats_append(K, V, Acc) ->
    [{K, V} | Acc].

quick_stats(Sock, Key, CB, CBState) ->
    quick_stats(Sock, Key, CB, CBState, ?QUICK_STATS_RECV_TIMEOUT).

%% quick_stats is like mc_client_binary:stats but with own buffering
%% of stuff and thus much faster. Note: we don't expect any request
%% pipelining here
quick_stats(Sock, Key, CB, CBState, Timeout) ->
    Req = encode(req, #mc_header{opcode=?STAT}, #mc_entry{key=Key}),
    Ref = make_ref(),
    MaybeTimer = case Timeout of
                     infinity ->
                         [];
                     _ ->
                         erlang:send_after(Timeout, self(), Ref)
                 end,
    try
        send(Sock, Req),
        quick_stats_loop_enter(Sock, CB, CBState, Ref, <<>>)
    after
        case MaybeTimer of
            [] ->
                [];
            T ->
                erlang:cancel_timer(T)
        end,
        receive
            Ref -> ok
        after 0 -> ok
        end
    end.

quick_stats_loop_enter(Sock, CB, CBState, TimeoutRef, Data) ->
    {ok, Header, Entry, Rest} = quick_active_recv(Sock, Data, TimeoutRef),
    %% Assume that only first entry might indicate an error
    case Header#mc_header.status of
        ?SUCCESS ->
            quick_stats_loop_process_entry(Sock, CB, CBState, TimeoutRef,
                                           Header, Entry, Rest);
        Status ->
            {memcached_error, mc_client_binary:map_status(Status), Entry#mc_entry.data}
    end.

quick_stats_loop(Sock, CB, CBState, TimeoutRef, Data) ->
    {ok, Header, Entry, Rest} = quick_active_recv(Sock, Data, TimeoutRef),
    quick_stats_loop_process_entry(Sock, CB, CBState, TimeoutRef,
                                   Header, Entry, Rest).

quick_stats_loop_process_entry(Sock, CB, CBState, TimeoutRef,
                               Header, Entry, Rest) ->
    #mc_header{keylen = RKeyLen} = Header,
    case RKeyLen =:= 0 of
        true ->
            <<>> = Rest,
            {ok, CBState};
        false ->
            NewState = CB(Entry#mc_entry.key, Entry#mc_entry.data, CBState),
            quick_stats_loop(Sock, CB, NewState, TimeoutRef, Rest)
    end.

send({OutPid, CmdNum}, Kind, Header, Entry) ->
    OutPid ! {send, CmdNum, encode(Kind, Header, Entry)},
    ok;

send(Sock, Kind, Header, Entry) ->
    send(Sock, encode(Kind, Header, Entry)).

recv(Sock, HeaderKind) ->
    recv(Sock, HeaderKind, undefined).

recv(Sock, HeaderKind, undefined) ->
    recv(Sock, HeaderKind, ?RECV_TIMEOUT);

recv(Sock, HeaderKind, Timeout) ->
    case recv_data(Sock, ?HEADER_LEN, Timeout) of
        {ok, HeaderBin} ->
            {Header, Entry} = decode_header(HeaderKind, HeaderBin),
            recv_body(Sock, Header, Entry, Timeout);
        Err -> Err
    end.

recv_body(Sock, #mc_header{extlen = ExtLen,
                           keylen = KeyLen,
                           bodylen = BodyLen} = Header, Entry, Timeout) ->
    case BodyLen > 0 of
        true ->
            true = BodyLen >= (ExtLen + KeyLen),
            {ok, Ext} = recv_data(Sock, ExtLen, Timeout),
            {ok, Key} = recv_data(Sock, KeyLen, Timeout),
            {ok, Data} = recv_data(Sock,
                                   erlang:max(0, BodyLen - (ExtLen + KeyLen)),
                                   Timeout),
            {ok, Header, Entry#mc_entry{ext = Ext, key = Key, data = Data}};
        false ->
            {ok, Header, Entry}
    end.

%%  Protocol Specification:
%%  http://src.couchbase.org/source/xref/trunk/kv_engine/docs/
%%  BinaryProtocol.md#92-104

encode_frame_info(#mc_frame_info{obj_id = ObjId,
                                 obj_data = ObjDataBin}) ->
    ObjDataBinLen = bin_size(ObjDataBin),
    case {ObjId, ObjDataBinLen} of
        {Id, Len} when Id < ?FRAME_INFO_ESCAPE, Len < ?FRAME_INFO_ESCAPE ->
            [<<Id:4, Len:4>>, ObjDataBin];
        {Id, Len} when Id >= ?FRAME_INFO_ESCAPE, Len < ?FRAME_INFO_ESCAPE ->
            RestId = Id - ?FRAME_INFO_ESCAPE,
            [<<?FRAME_INFO_ESCAPE:4, Len:4, RestId:8>>, ObjDataBin];
        {Id, Len} when Id < ?FRAME_INFO_ESCAPE, Len >= ?FRAME_INFO_ESCAPE ->
            RestLen = Len - ?FRAME_INFO_ESCAPE,
            [<<Id:4, ?FRAME_INFO_ESCAPE:4, RestLen:8>>, ObjDataBin];
        {Id, Len} ->
            RestId = Id - ?FRAME_INFO_ESCAPE,
            RestLen = Len - ?FRAME_INFO_ESCAPE,
            [<<?FRAME_INFO_ESCAPE:4, ?FRAME_INFO_ESCAPE:4, RestId:8,
               RestLen:8>>, ObjDataBin]
    end.

encode_frame_infos(undefined) ->
    [];
encode_frame_infos(FrameInfos) ->
    [bin(encode_frame_info(FrameInfo)) || FrameInfo <- FrameInfos].

encode(req, #mc_header{frame_infos = undefined} = Header, Entry) ->
    encode(?REQ_MAGIC, Header, Entry);
encode(req, Header, Entry) ->
    encode(?ALT_CLIENT_REQ_MAGIC, Header, Entry);
encode(res, Header, Entry) ->
    encode(?RES_MAGIC, Header, Entry);
encode(server_res, Header, Entry) ->
    encode(?SERVER_RESP_MAGIC, Header, Entry);
encode(Magic,
       #mc_header{opcode = Opcode, opaque = Opaque,
                  vbucket = VBucket, status = Status,
                  frame_infos = FrameInfos},
       #mc_entry{ext = Ext, key = Key, cas = CAS,
                 data = Data, datatype = DataType}) ->
    ExtLen = bin_size(Ext),
    KeyLen = bin_size(Key),
    VBucketOrStatus =
        case is_response(Magic) of
            true -> Status;
            false -> VBucket
        end,

    FrameInfosEncoded = encode_frame_infos(FrameInfos),
    FrameInfosEncodedLen = bin_size(FrameInfosEncoded),

    BodyLen = ExtLen + KeyLen + FrameInfosEncodedLen + bin_size(Data),

    [<<Magic:8, Opcode:8, FrameInfosEncodedLen:8, KeyLen:8, ExtLen:8,
       DataType:8, VBucketOrStatus:16, BodyLen:32, Opaque:32, CAS:64>>,
     bin(FrameInfosEncoded), bin(Ext), bin(Key), bin(Data)].

is_response(?REQ_MAGIC) -> false;
is_response(?ALT_CLIENT_REQ_MAGIC) -> false;
is_response(?SERVER_REQ_MAGIC) -> false;
is_response(?RES_MAGIC) -> true;
is_response(?SERVER_RESP_MAGIC) -> true.

decode_header(<<?REQ_MAGIC:8, _Rest/binary>> = Header) ->
    decode_header(req, Header);
decode_header(<<?SERVER_REQ_MAGIC:8, _Rest/binary>> = Header) ->
    decode_header(server_req, Header);
decode_header(<<?RES_MAGIC:8, _Rest/binary>> = Header) ->
    decode_header(res, Header).

decode_header(req, <<?REQ_MAGIC:8, Opcode:8, KeyLen:16, ExtLen:8,
                     DataType:8, Reserved:16, BodyLen:32,
                     Opaque:32, CAS:64>>) ->
    {#mc_header{opcode = Opcode, status = Reserved, opaque = Opaque,
                keylen = KeyLen, extlen = ExtLen, bodylen = BodyLen,
                vbucket = Reserved},
     #mc_entry{datatype = DataType, cas = CAS}};

decode_header(server_req, <<?SERVER_REQ_MAGIC:8, Opcode:8, KeyLen:16, ExtLen:8,
                            DataType:8, Reserved:16, BodyLen:32,
                            Opaque:32, CAS:64>>) ->
    {#mc_header{opcode = Opcode, status = Reserved, opaque = Opaque,
                keylen = KeyLen, extlen = ExtLen, bodylen = BodyLen,
                vbucket = Reserved},
     #mc_entry{datatype = DataType, cas = CAS}};

decode_header(res, <<?RES_MAGIC:8, Opcode:8, KeyLen:16, ExtLen:8,
                     DataType:8, Status:16, BodyLen:32,
                     Opaque:32, CAS:64>>) ->
    {#mc_header{opcode = Opcode, status = Status, opaque = Opaque,
                keylen = KeyLen, extlen = ExtLen, bodylen = BodyLen},
     #mc_entry{datatype = DataType, cas = CAS}}.

decode_packet(Bin) ->
    {H, B, <<>>} = decode_packet_ext(Bin),
    {H, B}.

decode_packet_ext(<<HeaderBin:?HEADER_LEN/binary, Body/binary>>) ->
    {Header, Entry} = decode_header(HeaderBin),
    #mc_header{extlen = ExtLen, keylen = KeyLen, bodylen = BodyLen} = Header,
    DataLen = BodyLen - KeyLen - ExtLen,
    case Body of
        <<Ext:ExtLen/binary, Key:KeyLen/binary, Data:DataLen/binary,
          Rest/binary>> ->
            {Header, Entry#mc_entry{ext = Ext, key = Key, data = Data}, Rest};
        _ ->
            need_more_data
    end;
decode_packet_ext(_) -> need_more_data.

bin(undefined) -> <<>>;
bin(X)         -> iolist_to_binary(X).

bin_size(undefined) -> 0;
bin_size(X)         -> iolist_size(X).

send({OutPid, CmdNum}, Data) when is_pid(OutPid) ->
    OutPid ! {send, CmdNum, Data};

send(undefined, _Data)              -> ok;
send(_Sock, <<>>)                   -> ok;
send(Sock, List) when is_list(List) -> send(Sock, iolist_to_binary(List));
send(Sock, Data)                    -> network:socket_send(Sock, Data).

%% @doc Receive binary data of specified number of bytes length.
recv_data(_, 0, _)                 -> {ok, <<>>};
recv_data(Sock, NumBytes, Timeout) -> network:socket_recv(Sock, NumBytes,
                                                          Timeout).

-record(get_keys_params, {
          start_key :: binary(),
          end_key :: undefined | binary(),
          collections_enabled :: boolean(),
          collection_uid :: undefined | non_neg_integer(),
          inclusive_end :: boolean(),
          limit :: non_neg_integer(),
          include_docs :: boolean()
         }).

-record(heap_item, {
          vbucket :: vbucket_id(),
          key :: binary(),
          collection_uid :: undefined | non_neg_integer(),
          rest_keys :: binary()
         }).

decode_first_key(<<Len:16, First:Len/binary, Rest/binary>>) ->
    {First, Rest}.

mk_heap_item(VBucket, Data,
             #get_keys_params{collections_enabled = Enabled,
                              collection_uid = Uid}) ->
    {Key, Rest} = decode_first_key(Data),
    {Uid, DKey} = maybe_decode_key(Enabled, Key),
    #heap_item{vbucket = VBucket,
               key = DKey,
               collection_uid = Uid,
               rest_keys = Rest}.

heap_item_from_rest(#heap_item{rest_keys = Rest} = Item,
                    #get_keys_params{collections_enabled = Enabled,
                                     collection_uid = Uid}) ->
    {Key, Rest2} = decode_first_key(Rest),
    {Uid, DKey} = maybe_decode_key(Enabled, Key),
    Item#heap_item{key = DKey, collection_uid = Uid, rest_keys = Rest2}.

encode_get(Key, VBucket, CollectionsEnabled, CollectionUid, Identity) ->
    Header0 = #mc_header{opcode = ?GET, vbucket = VBucket},
    Header = ns_memcached:maybe_add_impersonate_user_frame_info(Identity,
                                                                Header0),
    mc_binary:encode(req,
                     Header,
                     #mc_entry{key = maybe_encode_uid_in_key(CollectionsEnabled,
                                                             CollectionUid,
                                                             Key)}).

encode_get_keys_mc_header(Identity, V) ->
    Header = #mc_header{opcode = ?CMD_GET_KEYS, vbucket = V},
    ns_memcached:maybe_add_impersonate_user_frame_info(Identity, Header).

encode_get_keys(VBuckets,
                Identity,
                #get_keys_params{start_key = StartKey,
                                 collections_enabled = Enabled,
                                 collection_uid = Uid},
                Limit) ->
    [mc_binary:encode(req,
                      encode_get_keys_mc_header(Identity, V),
                      #mc_entry{key = maybe_encode_uid_in_key(Enabled, Uid,
                                                              StartKey),
                                ext = <<Limit:32>>})
     || V <- VBuckets].

get_keys_recv(Sock, TRef, F, InitAcc, List) ->
    {Acc, <<>>, Status} =
        lists:foldl(
          fun (Elem, {Acc, Data, ok}) ->
                  {ok, Header, Entry, Data2} = quick_active_recv(Sock, Data, TRef),
                  try
                      Acc2 = F(Elem, Header, Entry, Acc),
                      {Acc2, Data2, ok}
                  catch
                      throw:Error when element(1, Error) =:= memcached_error ->
                          {Error, Data2, failed}
                  end;
              (_Elem, {Acc, Data, failed}) ->
                  {ok, _Header, _Entry, Data2} = quick_active_recv(Sock, Data, TRef),
                  {Acc, Data2, failed}
          end, {InitAcc, <<>>, ok}, List),

    case Status of
        ok ->
            Acc;
        failed ->
            throw(Acc)
    end.

is_interesting_item(_, #get_keys_params{end_key = undefined}) ->
    true;
is_interesting_item(#heap_item{key = Key},
                    #get_keys_params{end_key = EndKey,
                                     inclusive_end = InclusiveEnd}) ->
    Key < EndKey orelse (Key =:= EndKey andalso InclusiveEnd).

handle_get_keys_response(VBucket, Header, Entry, Params) ->
    #mc_header{status = Status} = Header,
    #mc_entry{data = Data} = Entry,

    case Status of
        ?SUCCESS ->
            case Data of
                undefined ->
                    false;
                _ ->
                    Item = mk_heap_item(VBucket, Data, Params),
                    case is_interesting_item(Item, Params) of
                        true ->
                            Item;
                        false ->
                            false
                    end
            end;
        _ ->
            throw({memcached_error, mc_client_binary:map_status(Status)})
    end.

fetch_more(Sock, TRef, Number, Params, Identity,
           #heap_item{vbucket = VBucket, key = LastKey, rest_keys = <<>>}) ->
    ok = network:socket_send(Sock,
                             encode_get_keys(
                               [VBucket],
                               Identity,
                               Params#get_keys_params{start_key = LastKey},
                               Number + 1)),

    get_keys_recv(
      Sock, TRef,
      fun (_, Header, Entry, unused) ->
              case handle_get_keys_response(VBucket, Header, Entry, Params) of
                  false ->
                      false;
                  #heap_item{key = Key, rest_keys = RestKeys} = Item ->
                      case {Key, RestKeys} of
                          {LastKey, <<>>} ->
                              false;
                          {LastKey, _} ->
                              heap_item_from_rest(Item, Params);
                          _ ->
                              Item
                      end
              end
      end, unused, [VBucket]).

encode_uid_in_key(CollectionUid, Key) ->
    BinUid = misc:encode_unsigned_leb128(CollectionUid),
    <<BinUid/binary, Key/binary>>.

maybe_encode_uid_in_key(_, _CollectionUid, undefined) ->
    undefined;
maybe_encode_uid_in_key(true, CollectionUid, Key) ->
    encode_uid_in_key(CollectionUid, Key);
maybe_encode_uid_in_key(false, undefined, Key) ->
    Key.

maybe_decode_key(true, Key) ->
    misc:decode_unsigned_leb128(Key);
maybe_decode_key(false, Key) ->
    {undefined, Key}.

get_keys(Sock, Features, VBuckets, Props, Timeout, Identity) ->
    IncludeDocs = proplists:get_value(include_docs, Props),
    InclusiveEnd = proplists:get_value(inclusive_end, Props),
    Limit = proplists:get_value(limit, Props),
    CollectionUid = proplists:get_value(collection_uid, Props),
    CollectionsEnabled = proplists:get_bool(collections, Features),
    StartKey = case proplists:get_value(start_key, Props) of
                   %% undefined is valid start_key and can be passed to us.
                   undefined -> <<0>>;
                   K -> K
               end,
    EndKey = proplists:get_value(end_key, Props),

    Params = #get_keys_params{include_docs = IncludeDocs,
                              inclusive_end = InclusiveEnd,
                              limit = Limit,
                              collections_enabled = CollectionsEnabled,
                              collection_uid = CollectionUid,
                              start_key = StartKey,
                              end_key = EndKey},

    TRef = make_ref(),
    Timer = erlang:send_after(Timeout, self(), TRef),

    try
        do_get_keys(Sock, VBuckets, Params, TRef, Identity)
    catch
        throw:Error ->
            Error
    after
        erlang:cancel_timer(Timer),
        misc:flush(TRef)
    end.

do_get_keys(Sock, VBuckets, Params, TRef, Identity) ->
    #get_keys_params{limit = Limit,
                     include_docs = IncludeDocs} = Params,

    PrefetchLimit = 2 * (Limit div length(VBuckets) + 1),
    proc_lib:spawn_link(
      fun () ->
              ok = network:socket_send(Sock,
                                       encode_get_keys(VBuckets, Identity,
                                                       Params, PrefetchLimit))
      end),

    Heap0 =
        get_keys_recv(
          Sock, TRef,
          fun (VBucket, Header, Entry, Acc) ->
                  case handle_get_keys_response(VBucket, Header, Entry, Params) of
                      false ->
                          Acc;
                      Item ->
                          heap_insert(Acc, Item)
                  end
          end, couch_skew:new(), VBuckets),

    {KeyTuples, Heap1} = handle_limit(Heap0, Sock, TRef, PrefetchLimit,
                                      Identity, Params),

    R = case IncludeDocs of
            true ->
                handle_include_docs(Sock, TRef, PrefetchLimit,
                                    Params, KeyTuples, Heap1, Identity);
            false ->
                [{K, undefined} || {K, _VB, _CollectionUid} <- KeyTuples]
        end,


    {ok, R}.

heap_insert(Heap, Item) ->
    couch_skew:in(Item, fun heap_item_less/2, Heap).

heap_item_less(#heap_item{key = X}, #heap_item{key = Y}) ->
    X < Y.

fold_heap(Heap, 0, _Sock, _TRef, _FetchLimit, _Params, _, _F, Acc) ->
    {Acc, Heap};
fold_heap(Heap, N, Sock, TRef, FetchLimit, Params, Identity, F, Acc) ->
    case couch_skew:size(Heap) of
        0 ->
            {Acc, Heap};
        _ ->
            {MinItem, Heap1} = couch_skew:out(fun heap_item_less/2, Heap),

            case is_interesting_item(MinItem, Params) of
                true ->
                    #heap_item{key = Key,
                               collection_uid = CollectionUid,
                               rest_keys = RestKeys,
                               vbucket = VBucket} = MinItem,
                    Heap2 =
                        case RestKeys of
                            <<>> ->
                                case fetch_more(Sock, TRef,
                                                FetchLimit, Params, Identity,
                                                MinItem) of
                                    false ->
                                        Heap1;
                                    NewItem ->
                                        heap_insert(Heap1, NewItem)
                                end;
                            _ ->
                                heap_insert(
                                  Heap1, heap_item_from_rest(MinItem, Params))
                        end,
                    fold_heap(Heap2, N - 1, Sock, TRef, FetchLimit, Params,
                              Identity, F, F(Key, VBucket, CollectionUid, Acc));
                false ->
                    {Acc, couch_skew:new()}
            end

    end.

handle_limit(Heap, Sock, TRef, FetchLimit, Identity,
             #get_keys_params{limit = Limit} = Params) ->
    {KeyTuples, Heap1} =
        fold_heap(Heap, Limit, Sock, TRef, FetchLimit, Params, Identity,
                  fun (Key, VBucket, CollectionUid, Acc) ->
                          [{Key, VBucket, CollectionUid} | Acc]
                  end, []),
    {lists:reverse(KeyTuples), Heap1}.

retrieve_values(Sock, TRef, KeyTuples,
                #get_keys_params{collections_enabled = CollectionsEnabled},
                Identity) ->
    proc_lib:spawn_link(
      fun () ->
              ok = network:socket_send(
                     Sock, [encode_get(K, VB, CollectionsEnabled, CUid,
                                       Identity) || {K, VB, CUid} <- KeyTuples])
      end),

    {KVs, Missing} =
        get_keys_recv(
          Sock, TRef,
          fun ({K, _VB, _CollUid}, Header, Entry, {AccKV, AccMissing}) ->
                  #mc_header{status = Status} = Header,
                  #mc_entry{data = Data} = Entry,

                  case Status of
                      ?SUCCESS ->
                          {[{K, annotate_value(Data)} | AccKV], AccMissing};
                      ?KEY_ENOENT ->
                          {AccKV, AccMissing + 1};
                      _ ->
                          throw({memcached_error,
                                 mc_client_binary:map_status(Status)})
                  end
          end, {[], 0}, KeyTuples),
    {lists:reverse(KVs), Missing}.


annotate_value(Value) ->
    case capi_crud:is_valid_json(Value) of
        true ->
            {json, Value};
        false ->
            Value1 = case Value of
                         <<Stripped:128/binary, _/binary>> ->
                             Stripped;
                         _ ->
                             Value
                     end,
            {binary, Value1}
    end.

handle_include_docs(Sock, TRef, FetchLimit, Params, KeyTuples, Heap, Identity) ->
    {KVs, Missing} = retrieve_values(Sock, TRef, KeyTuples, Params, Identity),
    case Missing =:= 0 of
        true ->
            KVs;
        false ->
            {NewKeyTuples, NewHeap} =
                handle_limit(Heap, Sock, TRef, FetchLimit, Identity,
                             Params#get_keys_params{limit=Missing}),
            KVs ++ handle_include_docs(Sock, TRef, FetchLimit, Params,
                                       NewKeyTuples, NewHeap, Identity)
    end.

get_xattrs(Sock, DocId, VBucket, Permissions, Identity) ->
    try
        {Keys, CAS} = try_get_xattr(Sock, DocId, VBucket, <<"$XTOC">>,
                                    Identity),
        AllowedKeys = lists:filter(
                        fun (K) ->
                                check_xattr_read_permission(K, Permissions)
                        end, Keys),
        %% Subdoc_multi_lookup does not support retriving several xattrs
        %% at once
        {Values, ResCAS} =
            lists:foldr(
                fun (K, {Acc, _}) ->
                        {V, NewCAS} = try_get_xattr(Sock, DocId, VBucket, K,
                                                    Identity),
                        {[{K, V}|Acc], NewCAS}
                end, {[], CAS}, AllowedKeys),
        {ok, ResCAS, Values}
    catch
        error:{memcached_error, _, _} = Error -> Error
    end.

try_get_xattr(Sock, DocId, VBucket, Key, Identity) ->
    case mc_client_binary:subdoc_multi_lookup(Sock, DocId, VBucket,
                                              [Key], [xattr_path], Identity) of
        {ok, CAS, [JSON]} -> {ejson:decode(JSON), CAS};
        {memcached_error, _, _} = Error ->
            ?log_error("Subdoc multi lookup error: arguments: ~p ~p ~p ~p,"
                       " response: ~p",
                       [Sock, DocId, VBucket, Key, Error]),
            error(Error)
    end.

%% X-Keys starting with a leading dollar sign are considered virtual XATTRs
%% and can only be accessed if the client holds the XATTR_READ privilege.
check_xattr_read_permission(<<"$", _binary>>, Permissions) ->
    lists:member(user_read, Permissions);
%% X-Keys starting with a leading underscore are considered system XATTRs
%% and can only be read if the client holds the SYSTEM_XATTR read privilege.
check_xattr_read_permission(<<"_", _/binary>>, Permissions) ->
    lists:member(server_read, Permissions);
%% X-Keys not starting with a leading underscore (and not starting with a
%% reserved symbol) are user XATTRs and may be read by clients with the XATTR_READ
check_xattr_read_permission(_XKey, Permissions) ->
    lists:member(user_read, Permissions).
