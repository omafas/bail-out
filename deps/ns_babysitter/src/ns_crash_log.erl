%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
-module(ns_crash_log).

-include("ns_common.hrl").

-behavior(gen_server).

-export([start_link/0, record_crash/1,
         consume_oldest_message/1, consume_oldest_message_from_inside_ns_server/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(MAX_CRASHES_LEN, 100).

-record(state, {file_path :: file:filename(),
                crashes :: queue:queue(),
                crashes_len :: non_neg_integer(),
                crashes_saved :: queue:queue(),
                consumer_from = undefined :: undefined | {pid(), reference()},
                consumer_mref = undefined :: undefined | reference()
               }).

-type crash() :: {PortName :: atom(), StatusCode :: integer(), RecentMessages :: string()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec record_crash(crash()) -> ok.
record_crash(Crash) ->
    gen_server:cast(?MODULE, {crash, Crash}).

-spec consume_oldest_message(_) -> crash() | superseded.
consume_oldest_message(Server) ->
    gen_server:call(Server, consume, infinity).

consume_oldest_message_from_inside_ns_server() ->
    consume_oldest_message({?MODULE, ns_server:get_babysitter_node()}).


init([]) ->
    Dir = path_config:component_path(data, "logs"),
    Path = filename:join(Dir, "crash_log_v2.bin"),
    ?log_info("crash_log path: ~s", [Path]),
    ok = filelib:ensure_dir(Path),
    Q = read_crash_log(Path),
    {ok, #state{file_path = Path,
                crashes = Q,
                crashes_len = queue:len(Q),
                crashes_saved = Q}}.

handle_call(consume, {Pid, _} = From, State) ->
    State1 = reset_consumer(State),
    State2 = State1#state{consumer_from = From,
                          consumer_mref = erlang:monitor(process, Pid)},
    {noreply, maybe_consume(State2)}.

handle_cast({crash, Crash}, #state{crashes = Q,
                                   crashes_len = Len} = State) ->
    Q2 = queue:in(Crash, Q),
    NewLen = Len + 1,
    State1 = case NewLen > ?MAX_CRASHES_LEN of
                 true ->
                     ?log_debug("Dropping oldest unconsumed crash: ~p", [queue:get(Q2)]),
                     State#state{crashes = queue:drop(Q2)};
                 _ ->
                     State#state{crashes = Q2,
                                 crashes_len = NewLen}
         end,
    State2 = maybe_consume(State1),
    {noreply, State2}.

handle_info({'DOWN', MRef, _, _, _}, #state{consumer_mref = CMRef} = State)
  when CMRef =:= MRef ->
    {noreply, reset_consumer(State)};
handle_info(consider_save, #state{file_path = Path,
                                  crashes = Q,
                                  crashes_saved = OldQ} = State) ->
    misc:flush(consider_save),
    case Q =/= OldQ of
        true ->
            save_crash_log(Path, Q),
            {noreply, State#state{crashes_saved = Q}};
        _ ->
            {noreply, State}
    end;
handle_info(_, State) ->
    {noreply, State}.

reset_consumer(#state{consumer_mref = undefined} = State) ->
    State;
reset_consumer(#state{consumer_mref = MRef,
                      consumer_from = From} = State) ->
    erlang:demonitor(MRef, [flush]),
    gen_server:reply(From, superseded),
    State#state{consumer_mref = undefined,
                consumer_from = undefined}.

do_maybe_consume(#state{consumer_from = undefined} = State) ->
    State;
do_maybe_consume(#state{crashes_len = 0} = State) ->
    State;
do_maybe_consume(#state{consumer_from = From,
                        crashes = Q,
                        crashes_len = Len} = State) ->
    gen_server:reply(From, queue:get(Q)),
    Q1 = queue:drop(Q),
    reset_consumer(State#state{crashes = Q1,
                               crashes_len = Len - 1}).

maybe_consume(State) ->
    maybe_save(do_maybe_consume(State)).

maybe_save(#state{crashes = Q,
                  crashes_saved = OldQ} = State)
  when Q =/= OldQ ->
    self() ! consider_save,
    State;
maybe_save(State) ->
    State.

read_crash_log(Path) ->
    case file:read_file(Path) of
        {ok, <<>>} -> queue:new();
        {ok, B} ->
            try
                Q = misc:decompress(B),
                true = queue:is_queue(Q),
                Q
            catch T:E ->
                    ?log_error("Couldn't load crash_log from ~s: ~p:~p. Apparently crash_log file is corrupted", [Path, T, E]),
                    queue:new()
            end;
        E ->
            ?log_warning("Couldn't load crash_log from ~s (perhaps it's first startup): ~p", [Path, E]),
            queue:new()
    end.

save_crash_log(Path, Q) ->
    Compressed = misc:compress(Q),
    case misc:atomic_write_file(Path, Compressed) of
        ok -> ok;
        E ->
            ?log_error("unable to write crash log to ~s: ~p. Ignoring", [Path, E])
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
