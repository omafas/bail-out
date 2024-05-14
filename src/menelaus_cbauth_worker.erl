%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(menelaus_cbauth_worker).

-behaviour(gen_server).

-export([start_monitor/4, notify/2, collect_stats/1, sync/1,
         strip_cbauth_suffix/1]).

-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {label :: string(),
                version :: string() | internal,
                mref :: reference(),
                connection :: pid(),
                heartbeat_interval :: integer() | undefined,
                timer :: misc:timer()}).

-include("ns_common.hrl").

server(Label) ->
    list_to_atom(atom_to_list(?MODULE) ++ "-" ++ Label).

start_monitor(Label, Version, Pid, Params) ->
    gen_server:start_monitor({local, server(Label)}, ?MODULE,
                             [Label, Version, Pid, Params], []).

notify(Pid, Info) ->
    Pid ! {notify, Info}.

collect_stats(Pid) ->
    gen_server:call(Pid, collect_stats).

sync(Pid) ->
    gen_server:call(Pid, sync).

init([Label, Version, Pid, Params]) ->
    MRef = erlang:monitor(process, Pid),
    {Timer, Interval} =
        case proplists:get_value(heartbeat, Params) of
            undefined ->
                {undefined, undefined};
            I ->
                T = misc:create_timer(heartbeat),
                Int = I * 1000,
                misc:arm_timer(Int, T),
                {T, Int}
        end,
    {ok, #state{label = Label, version = Version, mref = MRef,
                heartbeat_interval = Interval, timer = Timer,
                connection = Pid}}.

handle_info(heartbeat, State = #state{label = Label, connection = Pid,
                                      heartbeat_interval = Interval,
                                      timer = Timer}) ->
    misc:flush(heartbeat),
    misc:arm_timer(Interval, Timer),
    case send_heartbeat(Label, Pid) of
        error ->
            terminate_jsonrpc_connection(Label, Pid),
            misc:wait_for_process(Pid, infinity),
            {stop, heartbeat_failed, State};
        ok ->
            {noreply, State}
    end;
handle_info({notify, Info}, State = #state{label = Label, connection = Pid,
                                           version = Version}) ->
    Method = case Version of
                 internal ->
                     "AuthCacheSvc.UpdateDB";
                 _ ->
                     "AuthCacheSvc.UpdateDBExt"
             end,
    case invoke_no_return_method(Label, Method, Pid, Info) of
        error ->
            terminate_jsonrpc_connection(Label, Pid),
            misc:wait_for_process(Pid, infinity),
            {stop, cannot_notify_client, State};
        ok ->
            {noreply, State}
    end;
handle_info({'DOWN', MRef, _, Pid, Reason},
            State = #state{mref = MRef, connection = Pid}) ->
    ?log_info("Observed json rpc process ~p died with reason ~p",
              [Pid,  Reason]),
    {stop, shutdown, State}.

handle_call(collect_stats, _From, State = #state{label = Label,
                                                 connection = Pid,
                                                 version = internal}) ->
    Res = case perform_call(Label, "AuthCacheSvc.GetStats", Pid, {[]}, true) of
              {ok, {[Stats]}} ->
                  {ok, {atom_to_binary(label_to_service(Label)), Stats}};
              error ->
                  error
          end,
    {reply, Res, State};
handle_call(sync, _From, State) ->
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {stop, {not_implemented, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _) -> {ok, State}.

send_heartbeat(Label, Pid) ->
    TestCondition = list_to_atom(atom_to_list(?MODULE) ++ "_skip_heartbeats"),
    case testconditions:get(TestCondition) of
        false ->
            invoke_no_return_method(Label, "AuthCacheSvc.Heartbeat", Pid, []);
        true ->
            ?log_debug("Skip heartbeat for label ~p", [Label])
    end.

invoke_no_return_method(Label, Method, Pid, Info) ->
    case perform_call(Label, Method, Pid, {Info}, false) of
        {ok, Res} when Res =:= true orelse Res =:= null ->
            ok;
        {ok, Res} ->
            ?log_error("Unexpected result ~p.", [Res]),
            error;
        error ->
            error
    end.

perform_call(Label, Method, Pid, Params, Silent) ->
    Opts = #{silent => Silent, timeout => ?get_timeout(perform_call, 60000)},
    try json_rpc_connection:perform_call(Label, Method, Params, Opts) of
        {error, method_not_found} ->
            ?log_error("Method ~p is not found", [Method]),
            error;
        {error, {rpc_error, Error}} ->
            ?log_error("RPC Error: ~p", [Error]),
            error;
        {error, Error} ->
            ?log_error("Error returned from go component ~p: ~p.",
                       [{Label, Pid}, Error]),
            error;
        {ok, Res} ->
            {ok, Res}
    catch exit:{noproc, _} ->
            ?log_debug("Process ~p is already dead", [{Label, Pid}]),
            error;
          exit:{Reason, _} ->
            ?log_error("Process ~p has exited during the call with reason ~p",
                       [{Label, Pid}, Reason]),
            error
    end.

terminate_jsonrpc_connection(Label, Pid) ->
    ?log_info("Killing connection ~p with pid = ~p", [Label, Pid]),
    exit(Pid, shutdown).

label_to_service("cbq-engine-cbauth") ->
    n1ql;
label_to_service("goxdcr-cbauth") ->
    xdcr;
label_to_service(Label) ->
    list_to_atom(strip_cbauth_suffix(Label)).

strip_cbauth_suffix(Label) ->
    "htuabc-" ++ ReversedTrimmedLabel = lists:reverse(Label),
    lists:reverse(ReversedTrimmedLabel).
