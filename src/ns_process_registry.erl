%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_process_registry).

-behaviour(gen_server).

-include("ns_common.hrl").

%% API
-export([start_link/2, lookup_pid/2, register_pid/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          name :: atom(),
          pids2ids,
          options
         }).


lookup_pid(Name, Id) ->
    try ets:lookup(Name, Id) of
        [{_, Pid}] ->
            Pid;
        [] ->
            missing
    catch error:badarg ->
            missing
    end.

register_pid(Registry, Id, Pid) when is_pid(Registry) ->
    gen_server:call(Registry, {register, Id, Pid});
register_pid(Name, Id, Pid) ->
    case lookup_pid(Name, ?MODULE) of
        Registry when is_pid(Registry) ->
            register_pid(Registry, Id, Pid)
    end.

start_link(Name, Options) ->
    gen_server:start_link(?MODULE, [Name, Options], []).

init([Name, Options]) ->
    ets:new(Name, [public, named_table]),
    PidsToIds = ets:new(none, [private, set]),
    ets:insert(Name, {?MODULE, self()}),
    erlang:process_flag(trap_exit, true),

    {ok, #state{name = Name,
                pids2ids = PidsToIds,
                options = Options}}.


consume_death_of(Pid, State) ->
    [Parent | _] = misc:get_ancestors(),
    receive
        {'EXIT', Parent, Reason} = ExitMsg ->
            ?log_debug("Got exit signal from parent: ~p", [ExitMsg]),
            exit(Reason);
        {'EXIT', Pid, _Reason} = PidExitMsg ->
            {noreply, NewState} = handle_info(PidExitMsg, State),
            NewState
    end.

handle_call({register, Id, Pid} = Call, From, #state{name = Name,
                                                     pids2ids = PidsToIds} = State) ->
    case ets:lookup(Name, Id) of
        [] ->
            ets:insert(Name, {Id, Pid}),
            erlang:link(Pid),
            ets:insert(PidsToIds, {Pid, Id}),
            {reply, ok, State};
        [{_, OtherPid}] ->
            case erlang:is_process_alive(OtherPid) of
                true ->
                    {reply, busy, State};
                false ->
                    NewState = consume_death_of(OtherPid, State),
                    handle_call(Call, From, NewState)
            end
    end.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason} = ExitMsg, #state{name = Name,
                                                    pids2ids = PidsToIds} = State) ->
    case misc:is_normal_termination(Reason) of
        true ->
            ok;
        false ->
            ?log_debug("~p detected abnormal exit: ~p", [Name, ExitMsg])
    end,
    case ets:lookup(PidsToIds, Pid) of
        [] ->
            ?log_error("~p detected exit from the unknown process. Crashing...~n~p", [Name, ExitMsg]),
            exit({bad_exit, ExitMsg});
        [{_, Id}] ->
            erlang:unlink(Pid),
            ets:delete(Name, Id),
            ets:delete(PidsToIds, Pid)
    end,
    {noreply, State}.

terminate(_Reason, #state{name = Name,
                          options = Options}) ->
    TerminateCommand = proplists:get_value(terminate_command, Options, shutdown),
    [begin
         erlang:exit(Pid, TerminateCommand),
         misc:wait_for_process(Pid, infinity)
     end || {_, Pid} <- ets:tab2list(Name),
            Pid =/= self()],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
