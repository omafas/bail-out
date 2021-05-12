%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_log).

-include("ns_log.hrl").

-define(RB_SIZE, 3000). % Number of recent log entries
-define(DUP_TIME, 15000000). % 15 secs in microsecs
-define(GC_TIME, 60000). % 60 secs in millisecs
-define(SAVE_DELAY, 5000). % 5 secs in millisecs

-behaviour(gen_server).
-behaviour(ns_log_categorizing).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([start_link_crash_consumer/0]).

-export([log/6, log/7, recent/0, recent/1, delete_log/0]).

-export([code_string/2, prepare_message/3]).

-export([ns_log_cat/1, ns_log_code_string/1]).

-include("ns_common.hrl").

-record(state, {unique_recent,
                dedup,
                save_tref,
                filename,
                pending_recent = [],
                pending_length = 0}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link_crash_consumer() ->
    {ok, proc_lib:spawn_link(fun crash_consumption_loop_tramp/0)}.

crash_consumption_loop_tramp() ->
    misc:delaying_crash(1000, fun crash_consumption_loop/0).

crash_consumption_loop() ->
    {Name, Status, Messages} = ns_crash_log:consume_oldest_message_from_inside_ns_server(),
    LogLevel = case Status of
                 0 ->
                     debug;
                 _ ->
                     info
             end,
    ale:log(?USER_LOGGER, LogLevel,
            "Service '~p' exited with status ~p. Restarting. Messages:~n~s",
            [Name, Status, Messages]),
    crash_consumption_loop().


log_filename() ->
    ns_config:search_node_prop(ns_config:get(), ns_log, filename).

read_logs(Filename) ->
    case file:read_file(Filename) of
        {ok, <<>>} -> [];
        {ok, B} ->
            try misc:decompress(B) of
                B2 ->
                    B2
            catch error:Error ->
                    ?log_error("Couldn't load logs from ~p. Apparently ns_logs file is corrupted: ~p",
                               [Filename, Error]),
                    []
            end;
        E ->
            ?log_warning("Couldn't load logs from ~p (perhaps it's first startup): ~p", [Filename, E]),
            []
    end.

init([]) ->
    send_garbage_collect_msg(),
    Filename = log_filename(),
    Recent = read_logs(Filename),
    %% initiate log syncing
    self() ! sync,
    erlang:process_flag(trap_exit, true),
    {ok, #state{unique_recent=Recent,
                dedup=dict:new(),
                filename=Filename}}.

delete_log() ->
    file:delete(log_filename()).

tail_of_length(List, N) ->
    case length(List) - N of
        X when X > 0 ->
            lists:nthtail(X, List);
        _ ->
            List
    end.

order_entries(A = #log_entry{}, B = #log_entry{}) ->
    A#log_entry{server_time = undefined} =< B#log_entry{server_time = undefined}.

flush_pending(#state{pending_recent = []} = State) ->
    State;
flush_pending(#state{unique_recent = Recent,
                     pending_recent = Pending} = State) ->
    NewRecent = tail_of_length(lists:umerge(fun order_entries/2, lists:sort(fun order_entries/2, Pending),
                                            Recent), ?RB_SIZE),
    State#state{unique_recent = NewRecent,
                pending_recent = [],
                pending_length = 0}.

add_pending(#state{pending_length = Length,
                   pending_recent = Pending} = State, Entry) ->
    NewState = State#state{pending_recent = [Entry | Pending],
                           pending_length = Length+1},
    case Length >= ?RB_SIZE of
        true ->
            flush_pending(NewState);
        _ -> NewState
    end.

%% Request for recent items.
handle_call(recent, _From, StateBefore) ->
    State = flush_pending(StateBefore),
    {reply, State#state.unique_recent, State, hibernate}.

%% Inbound logging request.
handle_cast({log, Module, Node, Time, Code, Category, Fmt, Args},
            State = #state{dedup=Dedup}) ->
    Key = {Module, Code, Category, Fmt, Args},
    case dict:find(Key, Dedup) of
        {ok, {Count, FirstSeen, LastSeen}} ->
            ?log_info("suppressing duplicate log ~p:~p(~p) because it's been "
                      "seen ~p times in the past ~p secs (last seen ~p secs ago",
                      [Module, Code, lists:flatten(io_lib:format(Fmt, Args)),
                       Count+1, timer:now_diff(Time, FirstSeen) / 1000000,
                       timer:now_diff(Time, LastSeen) / 1000000]),
            Dedup2 = dict:store(Key, {Count+1, FirstSeen, Time}, Dedup),
            {noreply, State#state{dedup=Dedup2}, hibernate};
        error ->
            Entry = #log_entry{node=Node, module=Module, code=Code, msg=Fmt,
                               args=Args, cat=Category, tstamp=Time},
            do_log(Entry),

            Dedup2 = dict:store(Key, {0, Time, Time}, Dedup),
            {noreply, State#state{dedup=Dedup2}, hibernate}
    end;
handle_cast({do_log, Entry}, State) ->
    {noreply, schedule_save(add_pending(State, Entry)), hibernate};
handle_cast({sync, SrcNode, Compressed}, StateBefore) ->
    State = flush_pending(StateBefore),
    Recent = State#state.unique_recent,
    case misc:decompress(Compressed) of
        Recent ->
            {noreply, State, hibernate};
        Logs ->
            State1 = schedule_save(State),
            NewRecent = tail_of_length(lists:umerge(fun order_entries/2, Recent, Logs),
                                       ?RB_SIZE),
            case NewRecent =/= Logs of
                %% send back sync with fake src node. To avoid
                %% infinite sync exchange just in case.
                true -> send_sync_to(NewRecent, SrcNode, SrcNode);
                _ -> nothing
            end,
            {noreply, State1#state{unique_recent=NewRecent}, hibernate}
    end;
handle_cast(_, State) ->
    {noreply, State, hibernate}.

send_sync_to(Recent, Node) ->
    send_sync_to(Recent, Node, node()).

send_sync_to(Recent, Node, Src) ->
    gen_server:cast({?MODULE, Node}, {sync, Src, misc:compress(Recent)}).

%% Not handling any other state.

%% Nothing special.
handle_info(garbage_collect, State) ->
    send_garbage_collect_msg(),
    {noreply, gc(State), hibernate};
handle_info(sync, StateBefore) ->
    State = flush_pending(StateBefore),
    Recent = State#state.unique_recent,
    erlang:send_after(5000 + rand:uniform(55000), self(), sync),
    case nodes() of
        [] -> ok;
        Nodes ->
            Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
            send_sync_to(Recent, Node)
    end,
    {noreply, State, hibernate};
handle_info(save, StateBefore = #state{filename=Filename}) ->
    State = flush_pending(StateBefore),
    Recent = State#state.unique_recent,
    Compressed = misc:compress(Recent),
    case misc:write_file(Filename, Compressed) of
        ok -> ok;
        E ->
            ?log_error("unable to write log to ~p: ~p", [Filename, E])
    end,
    {noreply, State#state{save_tref=undefined}, hibernate};
handle_info(_Info, State) ->
    {noreply, State, hibernate}.

terminate(shutdown, State) ->
    handle_info(save, State);
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

gc(State = #state{dedup=Dupes}) ->
    DupesList = gc(erlang:timestamp(), dict:to_list(Dupes), []),
    State#state{dedup=dict:from_list(DupesList)}.

gc(_Now, [], DupesList) -> DupesList;
gc(Now, [{Key, Value} | Rest], DupesList) ->
    {Count, FirstSeen, LastSeen} = Value,
    case timer:now_diff(Now, FirstSeen) >= ?DUP_TIME of
        true ->
            {Module, Code, Category, Fmt, Args} = Key,
            case Count of
                0 -> ok;
                _ ->
                    DiffLast = timer:now_diff(Now, LastSeen)/1000000,
                    Entry = #log_entry{node=node(), module=Module,
                                       code=Code,
                                       msg=Fmt ++ " (repeated ~p times, "
                                                  "last seen ~p secs ago)",
                                       args=Args ++ [Count, DiffLast],
                                       cat=Category,

                                       tstamp=Now},
                    do_log(Entry)
            end,
            gc(Now, Rest, DupesList);
        false -> gc(Now, Rest, [{Key, Value} | DupesList])
    end.

schedule_save(State = #state{save_tref=undefined}) ->
    TRef = erlang:send_after(?SAVE_DELAY, self(), save),
    State#state{save_tref=TRef};
schedule_save(State) ->
    %% Don't reschedule if a save is already scheduled.
    State.

do_log(#log_entry{code=undefined} = Entry) ->
    %% Code can be undefined if logging module doesn't define ns_log_cat
    %% function. We change the code to 0 for such cases. Note that it must be
    %% done before abcast-ing (not in handle_cast) because some of the nodes
    %% in the cluster can be of the older version (thus this case won't be
    %% handled there).
    do_log(Entry#log_entry{code=0});
do_log(#log_entry{code=Code, tstamp=TStamp} = Entry) when is_integer(Code) ->
    EntryNew = Entry#log_entry{server_time=calendar:now_to_local_time(TStamp)},

    Nodes = ns_node_disco:nodes_actual(),
    gen_server:abcast(Nodes, ?MODULE, {do_log, EntryNew}).

send_garbage_collect_msg() ->
    erlang:send_after(?GC_TIME, self(), garbage_collect).

%% API

-spec code_string(atom(), integer()) -> string().
code_string(Module, Code) ->
    case catch(Module:ns_log_code_string(Code)) of
        S when is_list(S) -> S;
        _                 -> "message"
    end.

-spec prepare_message(atom(), integer(), string()) -> string().
prepare_message(Module, Code, Msg) ->
    try Module:ns_log_prepare_message(Code, Msg) of
        S when is_list(S) ->
            S
    catch
        error:undef ->
            Msg
    end.

-spec log(atom(), node(), Time, log_classification(), iolist(), list()) -> ok
       when Time :: {integer(), integer(), integer()}.
log(Module, Node, Time, Category, Fmt, Args) ->
    log(Module, Node, Time, undefined, Category, Fmt, Args).

%% A Code is an number which is module-specific.
-spec log(atom(), node(), Time,
          Code, log_classification(), iolist(), list()) -> ok
      when Time :: {integer(), integer(), integer()},
           Code :: integer() | undefined.
log(Module, Node, Time, Code, Category, Fmt, Args) ->
    gen_server:cast(?MODULE,
                    {log, Module, Node, Time, Code, Category, Fmt, Args}).

-spec recent() -> list(#log_entry{}).
recent() ->
    gen_server:call(?MODULE, recent).

-spec recent(atom()) -> list(#log_entry{}).
recent(Module) ->
    [E || E <- gen_server:call(?MODULE, recent),
          E#log_entry.module =:= Module ].

%% Example categorization -- pretty much exists for the test below, but
%% this is what any module that logs should look like.
ns_log_cat(1) ->
    crit;
ns_log_cat(2) ->
    warn;
ns_log_cat(3) ->
    info.

ns_log_code_string(1) ->
    "logging could not foobar";
ns_log_code_string(2) ->
    "logging hit max baz".
