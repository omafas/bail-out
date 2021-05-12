%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc server that allows to defer "DOWN" message from remote monitor in case
%%      if the net kernel is restarted till the start of the net kernel
%%
-module(remote_monitors).

-behaviour(gen_server).

-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, monitor/1, register_node_renaming_txn/1, wait_for_net_kernel/0]).

-record(state, {node_renaming_txn_mref :: undefined | reference(),
                monitors :: [] | [pid()]
               }).

init([]) ->
    {ok, maybe_monitor_rename_txn(dist_manager:get_rename_txn_pid(),
                                  #state{monitors = []})}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_node_renaming_txn(Pid) ->
    ok = gen_server:call(?MODULE, {register_node_renaming_txn, Pid}).

monitor(Pid) ->
    gen_server:call(?MODULE, {monitor, Pid}).

wait_for_net_kernel() ->
    case gen_server:call(?MODULE, monitor_net_kernel) of
        unpaused ->
            ignore;
        ok ->
            receive
                {remote_monitor_down, undefined, unpaused} ->
                    ignore
            end
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_call({register_node_renaming_txn, Pid}, _From, #state{monitors = Monitors} = State) ->
    case State of
        #state{node_renaming_txn_mref = undefined} ->
            [gen_server:call(MonPid, pause) || MonPid <- Monitors],
            {reply, ok, maybe_monitor_rename_txn(Pid, State)};
        _ ->
            {reply, already_doing_renaming, State}
    end;

handle_call({monitor, Pid}, {FromPid, _}, State) ->
    MonState = case State of
                   #state{node_renaming_txn_mref = undefined} ->
                       unpaused;
                   _ ->
                       paused
               end,
    do_add_monitor(MonState, Pid, FromPid, State);

handle_call(monitor_net_kernel, _From, #state{node_renaming_txn_mref = undefined} = State) ->
    {reply, unpaused, State};

handle_call(monitor_net_kernel, {FromPid, _}, State) ->
    do_add_monitor(paused, undefined, FromPid, State).

handle_info({'DOWN', MRef, _, _, _}, #state{node_renaming_txn_mref = MRef,
                                            monitors = Monitors} = State) ->
    [MonPid ! unpause || MonPid <- Monitors],
    ?log_debug("Node renaming transaction ended. MRef = ~p", [MRef]),
    {noreply, State#state{node_renaming_txn_mref = undefined,
                          monitors = []}}.

handle_cast({remove_monitor, Pid}, #state{monitors = Monitors} = State) ->
    {noreply, State#state{monitors = lists:delete(Pid, Monitors)}}.


remove_monitor(Pid) ->
    gen_server:cast(?MODULE, {remove_monitor, Pid}).

do_add_monitor(MonState, Pid, FromPid, #state{monitors = Monitors} = State) ->
    MonPid = proc_lib:start_link(erlang, apply,
                                 [fun init_monitor/3, [MonState, Pid, FromPid]]),
    {reply, ok, State#state{monitors = [MonPid | Monitors]}}.

init_monitor(unpaused, Pid, FromPid) ->
    process_flag(trap_exit, true),
    MRef = erlang:monitor(process, Pid),
    erlang:monitor(process, FromPid),
    proc_lib:init_ack(self()),
    monitor_loop(MRef, Pid, FromPid);

init_monitor(paused, Pid, FromPid) ->
    process_flag(trap_exit, true),
    erlang:monitor(process, FromPid),
    proc_lib:init_ack(self()),
    monitor_paused_loop(Pid, FromPid).

monitor_loop(MRef, Pid, ReplyTo) ->
    receive
        {'$gen_call', From, pause} ->
            erlang:demonitor(MRef, [flush]),
            gen_server:reply(From, ok),
            monitor_paused_loop(Pid, ReplyTo);
        {'DOWN', MRef, _Type, Pid, Reason} ->
            ?log_debug("Monitored remote process ~p went down with: ~p", [Pid, Reason]),
            ReplyTo ! {remote_monitor_down, Pid, Reason},
            remove_monitor(self()),
            erlang:unlink(ReplyTo),
            exit(normal);
        {'DOWN', _MRef, _Type, ReplyTo, Reason} ->
            handle_down(ReplyTo, Reason);
        {'EXIT', ExitPid, Reason} ->
            handle_exit(ReplyTo, Pid, ExitPid, Reason)
    end.

monitor_paused_loop(Pid, ReplyTo) ->
    receive
        unpause ->
            ReplyTo ! {remote_monitor_down, Pid, unpaused},
            erlang:unlink(ReplyTo),
            exit(normal);
        {'DOWN', _MRef, _Type, ReplyTo, Reason} ->
            handle_down(ReplyTo, Reason);
        {'EXIT', ExitPid, Reason} ->
            handle_exit(ReplyTo, Pid, ExitPid, Reason)
    end.

handle_exit(ReplyTo, Pid, ExitPid, Reason) ->
    ?log_debug("Remote monitor got exit signal ~p from ~p. Exiting", [Reason, ExitPid]),
    ReplyTo ! {remote_monitor_down, Pid, {'EXIT', ExitPid, Reason}},
    exit(Reason).

handle_down(Caller, Reason) ->
    ?log_debug("Caller of remote monitor ~p died with ~p. Exiting", [Caller, Reason]),
    remove_monitor(self()),
    exit(normal).

maybe_monitor_rename_txn(undefined, State) ->
    State;
maybe_monitor_rename_txn(Pid, State = #state{node_renaming_txn_mref = undefined}) ->
    MRef = erlang:monitor(process, Pid),
    ?log_debug("Monitor node renaming transaction. Pid = ~p, MRef = ~p",
               [Pid, MRef]),
    State#state{node_renaming_txn_mref = MRef}.
