%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

-module(gossip_replicator).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% API
-export([start_link/2,
         add_log/2,
         replicate_log/2,
         change_config/3,
         recent/1,
         delete_logs/1]).

-include("ns_common.hrl").
-include("cut.hrl").

-record(state, {unique_recent,
                recent_len_max,
                save_tref = undefined,
                notify_tref = undefined,
                filename,
                pending_list = [],
                pending_length = 0,
                pending_max,
                child_module,
                child_servername,
                child_state}).

-type child_state() :: term().
-type log() :: term().
-type logs() :: [log()].

-callback init(logs()) -> child_state().
-callback handle_add_log(log(), child_state(), fun()) -> child_state().
-callback add_local_node_metadata(logs(), child_state()) ->
    {logs(), child_state()}.
-callback strip_local_node_metadata(logs()) -> logs().
-callback merge_remote_logs(logs(), logs(), integer()) -> logs().
-callback merge_pending_list(logs(), logs(), integer()) -> logs().
-callback modify_recent(logs(), integer()) -> logs().
-callback handle_notify(child_state()) -> child_state().
-callback handle_info(term(), child_state(), fun()) -> child_state().

-define(SAVE_DELAY, 5000). % 5 secs in millisecs
-define(NOTIFY_DELAY, 2000).

start_link(ServerName, Args) ->
    gen_server:start_link({local, ServerName}, ?MODULE,
                          Args ++ [ServerName], []).

init([Mod, FileName, PendingLenMax, RecentMax, ServerName]) ->
    Recent = read_logs(FileName),
    self() ! periodic_sync_full,
    %% Set trap_exit = true, to ensure terminate/2 is called
    %% when the gen_server is being terminated.
    erlang:process_flag(trap_exit, true),
    ChildState = Mod:init(Recent),
    {ok, #state{unique_recent = Recent,
                filename = FileName,
                child_module = Mod,
                child_state = ChildState,
                child_servername = ServerName,
                pending_max = PendingLenMax,
                recent_len_max = RecentMax}}.

%% Request for recent items.
handle_call(recent, _From, State0) ->
    State = flush_pending_list(State0),
    {reply, State#state.unique_recent, State}.

%% Inbound logging request.
handle_cast({add_log, Log}, State0 = #state{child_module = Mod,
                                            child_servername = ServerName,
                                            child_state = ChildState0}) ->
    %% handle_add_log checks if the log is duplicate and calls the ReplicateFun
    %% if the log isn't a duplicate.
    ReplicateFun = ?cut(gossip_replicator:replicate_log(ServerName, _)),
    ChildState = Mod:handle_add_log(Log, ChildState0, ReplicateFun),
    {noreply, State0#state{child_state = ChildState}};
handle_cast({log, Log}, #state{child_state = ChildState} = State0) ->
    State = State0#state{child_state = ChildState},
    {noreply, schedule_save(add_pending(State, Log))};
%% The following function is intensive both computational and memory-wise.
%% The memory spike to perform this merge operation in the worst case  will be
%% approximately 3 times the size of the local logs (or remote logs, basically
%% the larger of the two).
handle_cast({sync_all, SrcNode, RemoteLogs0},
            #state{child_module = Mod,
                   child_servername = ServerName,
                   recent_len_max = RecentLenMax} = State0) ->
    State = flush_pending_list(State0),
    LocalLogs = State#state.unique_recent,


    StrippedLocalLogs = Mod:strip_local_node_metadata(LocalLogs),
    %% Remote logs are already stripped of the metadata.
    StrippedRemoteLogs = misc:decompress(RemoteLogs0),

    {SendToRemote, NewStrippedLocalLogs} =
        case StrippedRemoteLogs of
            StrippedLocalLogs ->
                {false, StrippedLocalLogs};
            _ ->
                %% StrippedRemoteLogs and LocalLogs are already sorted by
                %% order_entries/2.
                NewLocalLogs = Mod:merge_remote_logs(StrippedLocalLogs,
                                                     StrippedRemoteLogs,
                                                     RecentLenMax),
                {NewLocalLogs =/= StrippedRemoteLogs, NewLocalLogs}
        end,
    %% Compute the diff between the new local logs and the prev local logs
    NewLogs = NewStrippedLocalLogs -- StrippedLocalLogs,

    StateNew = case NewLogs of
                   [] ->
                       State;
                   _ ->
                       schedule_save(
                         flush_pending_list(
                           add_pending_many(State, NewLogs)))
               end,

    case SendToRemote of
        true ->
            %% send back sync with fake src node. To avoid
            %% infinite sync exchange just in case.
            send_sync_to(ServerName, NewStrippedLocalLogs, SrcNode, SrcNode);
        false ->
            ok
    end,
    {noreply, StateNew};
handle_cast({change_config, RecentMaxLen, PendingMaxLen},
            #state{child_module = Mod} = State0) ->
    %% flushing any pending logs and trim the unique_recent to
    %% hold MaxRecent entries.
    State = flush_pending_list(State0),
    RecentNew = Mod:modify_recent(State#state.unique_recent, RecentMaxLen),
    State1 = State#state{unique_recent = RecentNew,
                         recent_len_max = RecentMaxLen,
                         pending_max = PendingMaxLen},
    StateNew = schedule_save(State1),
    {noreply, StateNew};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(periodic_sync_full,
            #state{child_module = Mod,
                   child_servername = ServerName} = State0) ->
    State = flush_pending_list(State0),
    Recent = State#state.unique_recent,
    erlang:send_after(5000 + rand:uniform(55000), self(), periodic_sync_full),
    case nodes() of
        [] -> ok;
        Nodes ->
            Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
            send_sync_to(ServerName, Mod:strip_local_node_metadata(Recent),
                         Node)
    end,
    {noreply, State};

handle_info(save, #state{filename = Filename} = State0) ->
    State = flush_pending_list(State0),
    Recent = State#state.unique_recent,
    Compressed = misc:compress(Recent),
    case misc:atomic_write_file(Filename, Compressed) of
        ok -> ok;
        E ->
            ?log_error("unable to write log to ~p: ~p", [Filename, E])
    end,
    {noreply, State#state{save_tref = undefined}};
handle_info(notify, #state{child_module = Mod,
                           child_state = ChildState0} = State0) ->
    ChildState = Mod:handle_notify(ChildState0),
    {noreply, State0#state{child_state = ChildState, notify_tref = undefined}};
%% Funnel other msg'es via the child Mod:handle_info/2.
handle_info(Info, #state{child_module = Mod,
                         child_servername = ServerName,
                         child_state = ChildState0} = State0) ->
    ReplicatorFun = ?cut(gossip_replicator:replicate_log(ServerName, _)),
    ChildState = Mod:handle_info(Info, ChildState0, ReplicatorFun),
    {noreply, State0#state{child_state = ChildState}}.

terminate(shutdown, State) ->
    handle_info(save, State);
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

add_log(ServerName, Log) ->
    gen_server:cast(ServerName, {add_log, Log}).

replicate_log(ServerName, Log) ->
    Nodes = ns_node_disco:nodes_actual(),
    gen_server:abcast(Nodes, ServerName, {log, Log}).

change_config(ServerName, MaxRecent, MaxPending) ->
    gen_server:cast(ServerName, {change_config, MaxRecent, MaxPending}).

delete_logs(FileConfigKey) ->
    file:delete(log_filename(FileConfigKey)).

recent(ServerName) ->
    gen_server:call(ServerName, recent).

%%%===================================================================
%%% Internal functions
%%%===================================================================

log_filename(Key) ->
    ns_config:search_node_prop(ns_config:get(), Key, filename).

read_logs(Filename) ->
    case file:read_file(Filename) of
        {ok, <<>>} -> [];
        {ok, B} ->
            try misc:decompress(B) of
                B2 ->
                    B2
            catch error:Error ->
                    ?log_error("Couldn't load logs from ~p. event_logs
                               file maybe corrupted: ~p",
                               [Filename, Error]),
                    []
            end;
        E ->
            ?log_warning("Couldn't load logs from ~p (perhaps it's first
                         startup): ~p", [Filename, E]),
            []
    end.

send_sync_to(ServerName, Recent, Node) ->
    send_sync_to(ServerName, Recent, Node, node()).

send_sync_to(ServerName, Logs, Node, Src) ->
    gen_server:cast({ServerName, Node}, {sync_all, Src, misc:compress(Logs)}).

flush_pending_list(#state{pending_list = []} = State) ->
    State;
flush_pending_list(#state{child_module = Mod} = State) ->
    NewRecent = Mod:merge_pending_list(State#state.unique_recent,
                                       State#state.pending_list,
                                       State#state.recent_len_max),
    State#state{unique_recent = NewRecent,
                pending_list = [],
                pending_length = 0}.

add_pending(State, Log) ->
    add_pending_many(State, [Log]).

add_pending_many(#state{pending_length = Length,
                        pending_list = Pending,
                        pending_max = Limit,
                        child_module = Mod,
                        child_state = ChildState0} = State0, NewLogs) ->

    {NewMetaLogs, ChildState} = Mod:add_local_node_metadata(NewLogs,
                                                            ChildState0),
    NewLogsLen = length(NewMetaLogs),
    State = State0#state{pending_list = Pending ++ NewMetaLogs,
                         pending_length = Length + NewLogsLen,
                         child_state = ChildState},

    NewState = case Length + NewLogsLen >= Limit of
                   true ->
                        flush_pending_list(State);
                   _ -> State
               end,
    maybe_notify(NewState).

maybe_notify(State = #state{notify_tref = undefined}) ->
    TRef = erlang:send_after(?NOTIFY_DELAY, self(), notify),
    State#state{notify_tref = TRef};
maybe_notify(State) ->
    %% Don't schedule a notify timer if there is one already scheduled.
    State.

schedule_save(#state{save_tref = undefined} = State) ->
    TRef = erlang:send_after(?SAVE_DELAY, self(), save),
    State#state{save_tref = TRef};
schedule_save(State) ->
    %% Don't reschedule if a save is already scheduled.
    State.
