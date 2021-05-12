%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% Infrastructure for incremental modular data processing. Loosely based on
%% http://okmij.org/ftp/continuations/PPYield/index.html.
%%
-module(pipes).

-include("pipes.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([compose/1, compose/2, run/3, run/2,
         fold/3, foreach/2, filter/1, filterfold/2, map/1, collect/0,
         stream_list/1]).

-export([read_file/1, read_file/2,
         write_file/1, write_file/2,
         gzip/0, gzip/1, gunzip/0, gunzip/1,
         buffer/1, simple_buffer/1,
         marshal_terms/0, marshal_terms/1, unmarshal_terms/0,
         stream_table/1, unstream_table/1,
         marshal_table/1, marshal_table/2, unmarshal_table/1]).

-export_type([producer/1, transducer/2, consumer/2, yield/1]).

-type producer(Event) :: fun ((yield(Event)) -> any()).
-type transducer(Event1, Event2) :: fun ((producer(Event1)) -> producer(Event2)).
-type consumer(Event, Result) :: fun ((producer(Event)) -> Result).
-type yield(Event) :: fun ((Event) -> any()).

%% API
-spec fold(producer(Event), fun ((Event, State) -> State), State) -> State.
fold(Producer, Handler, InitState) ->
    with_state(
      fun (Get, Put) ->
              Producer(
                fun (Event) ->
                        State = Get(),
                        NewState = Handler(Event, State),
                        Put(NewState)
                end)
      end, InitState).

-spec foreach(producer(Event), fun ((Event) -> any())) -> ok.
foreach(Producer, Fun) ->
    fold(Producer,
         fun (Event, _) ->
                 Fun(Event)
         end, ok).

-spec filter(fun ((Event) -> boolean())) -> transducer(Event, Event).
filter(Pred) ->
    ?make_transducer(
       foreach(?producer(),
               fun (Event) ->
                       case Pred(Event) of
                           true ->
                               ?yield(Event);
                           false ->
                               ok
                       end
               end)).

-spec filterfold(fun ((Event, Acc :: term()) -> {boolean(), NewAcc :: term()}),
                 InitAcc :: term()) -> transducer(Event, Event).
filterfold(Fun, AccInit) ->
    ?make_transducer(
       pipes:fold(?producer(),
                  fun (User, Acc) ->
                          case Fun(User, Acc) of
                              {true, NewAcc} ->
                                  ?yield(User),
                                  NewAcc;
                              {false, NewAcc} ->
                                  NewAcc
                          end
                  end, AccInit)).

-spec map(fun ((Event1) -> Event2)) -> transducer(Event1, Event2).
map(Fun) ->
    ?make_transducer(foreach(?producer(), ?cut(?yield(Fun(_))))).

-spec stream_list([Elem]) -> producer(Elem).
stream_list(List) ->
    ?make_producer(lists:foreach(?yield(), List)).

-spec collect() -> consumer(Event, [Event]).
collect() ->
    ?make_consumer(
       begin
           R = fold(?producer(),
                    fun (Event, Acc) ->
                            [Event | Acc]
                    end, []),
           lists:reverse(R)
       end).

-spec run(producer(any()),
          [transducer(any(), any())] | transducer(any(), any()),
          consumer(any(), Result)) -> Result.
run(Producer, Transducers, Consumer) when is_list(Transducers) ->
    Pipeline = compose([Producer | Transducers]),
    compose(Pipeline, Consumer);
run(Producer, Transducer, Consumer) ->
    run(Producer, [Transducer], Consumer).

-spec run(producer(Event), consumer(Event, Result)) -> Result.
run(Producer, Consumer) ->
    run(Producer, [], Consumer).

%% this is basically a synonym to run/2, but is useful on occasion when
%% combining producer and multiple transducers, but no obvious consumer
-spec compose(producer(Event), consumer(Event, Result)) -> Result.
compose(Producer, Consumer) ->
    Consumer(Producer).

-spec compose([producer(any()) | transducer(any(), any())]) -> producer(any()).
compose([Producer | Transducers]) ->
    lists:foldl(fun (Transducer, Acc) ->
                        compose(Acc, Transducer)
                end, Producer, Transducers).

%% Misc consumers, producers, transducers.
read_file(File) ->
    read_file(File, []).

read_file(File, Options) ->
    BufSize = proplists:get_value(bufsize, Options, 1024 * 1024),
    ?make_producer(read_file_loop(File, BufSize, ?yield())).

read_file_loop(F, BufSize, Yield) ->
    case file:read(F, BufSize) of
        {ok, Data} ->
            Yield(Data),
            read_file_loop(F, BufSize, Yield);
        eof ->
            ok
    end.

write_file(File) ->
    write_file(File, []).

write_file(File, Options) ->
    BufSize = proplists:get_value(bufsize, Options, 1024 * 1024),

    ?make_consumer(
       begin
           Producer = run(?producer(), simple_buffer(BufSize)),
           foreach(Producer,
                   fun (Data) ->
                           ok = file:write(File, Data)
                   end)
       end).

simple_buffer(BufSize) ->
    ?make_transducer(
       foreach(run(?producer(), buffer(BufSize)),
               fun ({Data, _, _}) ->
                       ?yield(Data)
               end)).

buffer(BufSize) ->
    ?make_transducer(
       begin
           Leftovers =
               fold(?producer(),
                    fun (MoreData, State) ->
                            NewState0 = append_data(MoreData, State),
                            maybe_yield(?yield(), BufSize, NewState0)
                    end, {[], 0, 0}),
           %% yield only if data size is greater than 0
           maybe_yield(?yield(), 1, Leftovers)
       end).

append_data(MoreData, {Data, Size, NumItems}) ->
    N = iolist_size(MoreData),
    {[Data | MoreData], Size + N, NumItems + 1}.

maybe_yield(Yield, BufSize, {_, Size, _} = State) when Size >= BufSize ->
    Yield(State),
    {[], 0, 0};
maybe_yield(_, _, State) ->
    State.

-define(GZIP_FORMAT, 16#10).
gzip() ->
    gzip([]).

gzip(Options) ->
    Level = proplists:get_value(compression_level, Options, default),

    ?make_transducer(
       begin
           Z = zlib:open(),

           try
               ok = zlib:deflateInit(Z, Level, deflated,
                                     15 bor ?GZIP_FORMAT, 8, default),
               gzip_loop(?producer(), ?yield(), Z)
           after
               zlib:close(Z)
           end
       end).

gzip_loop(Producer, Yield, Z) ->
    foreach(Producer,
            fun (Data) ->
                    Compressed = zlib:deflate(Z, Data),
                    case misc:iolist_is_empty(Compressed) of
                        true ->
                            ok;
                        false ->
                            Yield(Compressed)
                    end
            end),

    Yield(zlib:deflate(Z, <<>>, finish)).

gunzip() ->
    gunzip([]).

gunzip(Options) ->
    BufSize = proplists:get_value(bufsize, Options, 1024 * 1024),

    ?make_transducer(
       begin
           Z = zlib:open(),

           try
               zlib:inflateInit(Z, 15 bor ?GZIP_FORMAT),
               zlib:setBufSize(Z, BufSize),
               gunzip_loop(?producer(), ?yield(), Z),
               zlib:inflateEnd(Z)
           after
               zlib:close(Z)
           end
       end).

gunzip_loop(Producer, Yield, Z) ->
    foreach(Producer,
            fun (Data) ->
                    Decompressed = zlib:inflate(Z, Data),
                    case misc:iolist_is_empty(Decompressed) of
                        true ->
                            ok;
                        false ->
                            Yield(Decompressed)
                    end
            end),
    Yield(zlib:inflate(Z, <<>>)).

stream_table(Table) ->
    ?make_producer(
       ets:foldl(
         fun (Element, unused) ->
                 ?yield(Element),
                 unused
         end, unused, Table)).

unstream_table(Table) ->
    ?make_consumer(
       begin
           true = (ets:info(Table, size) =:= 0),
           foreach(?producer(),
                   fun (Term) ->
                           true = ets:insert_new(Table, Term)
                   end)
       end).

marshal_table(Table) ->
    marshal_table(Table, []).

marshal_table(Table, Options) ->
    MarshalOpts =
        case lists:keyfind(bufsize, 1, Options) of
            false ->
                [];
            {_, BufSize} ->
                [{bufsize, BufSize}]
        end,

    ?make_producer(foreach(run(stream_table(Table),
                               marshal_terms(MarshalOpts)), ?yield())).


unmarshal_table(Table) ->
    ?make_consumer(
       begin
           true = (ets:info(Table, size) =:= 0),
           run(?producer(), unmarshal_terms(), unstream_table(Table))
       end).

marshal_terms() ->
    marshal_terms([]).

marshal_terms(Options) ->
    BufSize = proplists:get_value(bufsize, Options, 1024 * 1024),

    ?make_transducer(
       begin
           Producer = run(?producer(),
                          map(?cut(strip_tag(term_to_binary(_)))),
                          buffer(BufSize)),
           foreach(Producer,
                   fun (Data) ->
                           {Packed, PackedSize} = pack_to_list(Data),
                           true = (PackedSize < 1 bsl 32),

                           Final = [<<PackedSize:32/big>>|Packed],
                           ?yield(Final)
                   end)
       end).

unmarshal_terms() ->
    ?make_transducer(
       begin
           {Left, _} = fold(?producer(),
                            fun (Data, State) ->
                                    unmarshal_loop(?yield(),
                                                   iolist_to_binary(Data), State)
                            end, {<<>>, undefined}),
           true = (byte_size(Left) =:= 0)
       end).

unmarshal_loop(Yield, MoreData, {DataSoFar, undefined}) ->
    Data = <<DataSoFar/binary, MoreData/binary>>,

    case unmarshal_get_size(Data) of
        {error, need_more} ->
            {Data, undefined};
        {ok, Size, RestData} ->
            unmarshal_loop(Yield, <<>>, {RestData, Size})
    end;
unmarshal_loop(Yield, MoreData, {DataSoFar, ExpectedSize}) ->
    Data = <<DataSoFar/binary, MoreData/binary>>,
    DataSize = byte_size(Data),

    case DataSize >= ExpectedSize of
        true ->
            Chunk = binary:part(Data, 0, ExpectedSize),
            unmarshal_chunk(Yield, Chunk),

            RestData0 = binary:part(Data, DataSize, -(DataSize - ExpectedSize)),
            RestData = binary:copy(RestData0),
            unmarshal_loop(Yield, <<>>, {RestData, undefined});
        false ->
            {Data, ExpectedSize}
    end.

unmarshal_chunk(Yield, Chunk) ->
    Terms = binary_to_term(Chunk),
    true = is_list(Terms),
    lists:foreach(Yield, Terms).

unmarshal_get_size(Data) when byte_size(Data) < 4 ->
    {error, need_more};
unmarshal_get_size(Data) ->
    <<Size:32/big, Rest/binary>> = Data,
    {ok, Size, Rest}.

%% http://erlang.org/doc/apps/erts/erl_ext_dist.html
-define(VERSION_TAG, 131).
-define(NIL, 106).
-define(LIST_HEAD, 108).

strip_tag(<<?VERSION_TAG, Rest/binary>>) ->
    Rest.

pack_to_list({Data, Bytes, N}) ->
    Packed = [[<<?VERSION_TAG, ?LIST_HEAD, N:32/big>> | Data] | <<?NIL>>],
    PackedSize = Bytes + 7,
    {Packed, PackedSize}.

%% internal
with_state(Body, InitState) ->
    Ref = make_ref(),
    erlang:put(Ref, InitState),

    Get = fun () -> erlang:get(Ref) end,
    Put = fun (S) -> erlang:put(Ref, S) end,

    try
        Body(Get, Put),
        Get()
    after
        erlang:erase(Ref)
    end.


-ifdef(TEST).
even(I) ->
    I rem 2 =/= 0.

prefixes() ->
    ?make_transducer(
       fold(?producer(),
            fun (Event, Acc) ->
                    NewAcc = [Event | Acc],
                    ?yield(lists:reverse(NewAcc)),
                    NewAcc
            end, [])).

simple_test() ->
    L = [1,2,3,4,5],
    R0 = run(stream_list(L),
             collect()),
    ?assertEqual(R0, L),

    R1 = run(stream_list(L),
             filter(fun even/1),
             collect()),
    ?assertEqual(R1, lists:filter(fun even/1, L)),

    R2 = run(stream_list(L),
             prefixes(),
             collect()),
    ?assertEqual(length(lists:usort(R2)), length(L)),
    lists:foreach(
      fun (P) ->
              ?assertEqual(lists:prefix(P, L), true)
      end, R2),

    ok.

marshal_test() ->
    Terms = [1, ok, {k, v}],
    RecoveredTerms = run(stream_list(Terms),
                         [marshal_terms([{bufsize, 1}]),
                          unmarshal_terms()],
                         collect()),
    ?assertEqual(Terms, RecoveredTerms).

stream_table_test() ->
    T1 = ets:new(ok, []),
    T2 = ets:new(ok, []),

    try
        KVs = [{k, v}, {1, 2}, {1.0, 2.0}, {<<$a>>, <<$b>>}],
        ets:insert(T1, KVs),
        run(stream_table(T1), unstream_table(T2)),
        ?assertEqual(lists:sort(ets:tab2list(T1)),
                     lists:sort(ets:tab2list(T2)))
    after
        ets:delete(T1),
        ets:delete(T2)
    end.

marshal_table_test() ->
    T1 = ets:new(ok, []),
    T2 = ets:new(ok, []),

    try
        KVs = [{k, v}, {1, 2}, {1.0, 2.0}, {<<$a>>, <<$b>>}],
        ets:insert(T1, KVs),
        run(marshal_table(T1, [{bufsize, 1}]),
            unmarshal_table(T2)),
        ?assertEqual(lists:sort(ets:tab2list(T1)),
                     lists:sort(ets:tab2list(T2)))
    after
        ets:delete(T1),
        ets:delete(T2)
    end.

stream_binary(Binary, Size) ->
    ?make_producer(
       lists:foreach(
         fun (Chunk) ->
                 ?yield(Chunk)
         end, [Chunk || <<Chunk:Size/binary>> <= Binary])).

buffer_test() ->
    Bin = <<1,2,3,4,5,6,7,8>>,

    do_test_buffer(Bin, 1, 2,
                   fun (C) ->
                           ?assertEqual(iolist_size(C), 2)
                   end),

    do_test_buffer(Bin, 2, 1,
                   fun (C) ->
                           ?assertEqual(iolist_size(C), 2)
                   end),

    do_test_buffer(Bin, 2, 2,
                   fun (C) ->
                           ?assertEqual(iolist_size(C), 2)
                   end),

    do_test_buffer(Bin, 2, 3,
                   fun (C) ->
                           Size = iolist_size(C),
                           ?assertEqual((Size =:= 3 orelse Size =:= 4), true)
                   end).

do_test_buffer(Binary, ChunkSize, BufferSize, Validator) ->
    Chunks = run(stream_binary(Binary, ChunkSize),
                 simple_buffer(BufferSize),
                 collect()),
    lists:foreach(
      fun (Chunk) ->
              Validator(Chunk)
      end, Chunks).
-endif.
