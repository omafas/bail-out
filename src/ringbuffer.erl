%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ringbuffer).

-export([new/1, to_list/1, add/2, add/3]).
-export_type([ringbuffer/0]).

-record(ringbuffer, {
          size :: non_neg_integer(),
          max_size :: pos_integer(),
          queue :: queue:queue({term(), pos_integer()})}).
-type ringbuffer() :: #ringbuffer{}.

% Create a ringbuffer that can hold at most Size items.
-spec new(pos_integer()) -> ringbuffer().
new(MaxSize) ->
    true = (MaxSize > 0),

    #ringbuffer{
       size = 0,
       max_size = MaxSize,
       queue = queue:new()}.

% Convert the ringbuffer to a list (oldest items first).
-spec to_list(ringbuffer()) -> [term()].
to_list(RingBuffer) ->
    [E || {E, _} <- queue:to_list(RingBuffer#ringbuffer.queue)].

% Add an element to a ring buffer.
-spec add(term(), ringbuffer()) -> ringbuffer().
add(Elem, RingBuffer) ->
    add(Elem, 1, RingBuffer).

-spec add(term(), pos_integer(), ringbuffer()) -> ringbuffer().
add(Elem, ElemSize, RingBuffer) ->
    maybe_truncate(enqueue(Elem, ElemSize, RingBuffer)).

%% private
maybe_truncate(#ringbuffer{size = Size,
                           max_size = MaxSize,
                           queue = Queue} = RingBuffer)
  when Size > MaxSize ->
    %% assuming max size is non-zero, the queue must not be empty then
    false = queue:is_empty(Queue),

    {NewQueue, NewSize} = truncate_nonempty_queue(Size, MaxSize, Queue),
    RingBuffer#ringbuffer{size = NewSize,
                          queue = NewQueue};
maybe_truncate(RingBuffer) ->
    RingBuffer.

truncate_nonempty_queue(OldSize, MaxSize, Queue) ->
    {{value, {_, ElemSize}}, NewQueue} = queue:out(Queue),

    NewSize = OldSize - ElemSize,
    true = (NewSize >= 0),

    case NewSize < MaxSize of
        true ->
            %% Somewhat counterintuitively, we actually return the old
            %% queue. So the resulting queue size is either exactly
            %% the max size or *greater* than max size. The latter is
            %% preferred if message sizes are used to control
            %% ringbuffer size. If messages are typically on the order
            %% of ringbuffer max size, then we'll almost never have
            %% anything in it. This is undesireable. The behavior is
            %% unchanged when just the number of elements is used.
            {Queue, OldSize};
        false ->
            truncate_nonempty_queue(NewSize, MaxSize, NewQueue)
    end.

enqueue(Elem, ElemSize, #ringbuffer{size = Size,
                                    queue = Queue} = RingBuffer) ->
    RingBuffer#ringbuffer{size = Size + ElemSize,
                          queue = queue:in({Elem, ElemSize}, Queue)}.
