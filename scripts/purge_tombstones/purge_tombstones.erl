% Copyright 2021-Present Couchbase, Inc.
%
% Use of this software is governed by the Business Source License included in
% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
% file, in accordance with the Business Source License, use of this software
% will be governed by the Apache License, Version 2.0, included in the file
% licenses/APL2.txt.

-module(purge_tombstones).

-compile(nowarn_export_all).
-compile(export_all).

%% dynamic_compile does not support ?MODULE
-define(MOD, purge_tombstones).
-define(SERVER, purge_tombstones_server).

-define(CAS_RETRIES, 20).

-define(TIMEOUT, 180000).

start(Req) ->
    multicall(Req, ?MOD, do_start, []).

do_start() ->
    do_kill(),
    gen_server:start({local, ?SERVER}, ?MOD, [], []).

do_kill() ->
    case whereis(?SERVER) of
        undefined ->
            ok;
        Pid ->
            log("Found tombstone purger process ~p. Killing it.", [Pid]),
            misc:terminate_and_wait(Pid, kill)
    end.

purge(Req) ->
    multicall(Req, ?MOD, do_purge, []).

do_purge() ->
    gen_server:call(?SERVER, purge, 60000).

complete(Req) ->
    multicall(Req, ?MOD, do_complete, []).

do_complete() ->
    gen_server:call(?SERVER, complete, 20000).

init([]) ->
    log("Suspending ns_config replication"),

    RepPid = whereis(ns_config_rep),
    MergerPid = whereis(ns_config_rep_merger),

    erlang:suspend_process(RepPid),
    erlang:suspend_process(MergerPid),

    log("Suspended ns_config replication"),

    erlang:send_after(?TIMEOUT, self(), timeout),

    {ok, {RepPid, MergerPid}}.

handle_call(purge, _From, State) ->
    log("Purging ns_config tombstones"),

    case perform_purge() of
        {ok, Status} ->
            case Status of
                purged ->
                    log("Reloading config."),
                    ok = ns_config:reload();
                not_purged ->
                    ok
            end,

            log("Purging completed."),

            {reply, ok, State};
        {error, _} = Error ->
            {stop, normal, Error, State}
    end;
handle_call(complete, _From, {RepPid, MergerPid} = State) ->
    log("Restarting ns_config replication"),

    true = whereis(ns_config_rep) =:= RepPid,
    true = whereis(ns_config_rep_merger) =:= MergerPid,

    %% Restarting merger will automatically restart the replicator itself.
    exit(MergerPid, kill),

    log("Restarted ns_config replication"),

    {stop, normal, ok, State};
handle_call(_Call, _From, State) ->
    {reply, nack, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    log("Timeout. Terminating."),
    {stop, normal, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

log(Msg) ->
    log(Msg, []).

log(Fmt, Args) ->
    ale:info(ns_server, Fmt, Args).

multicall(Req, M, F, A) ->
    Nodes = ns_node_disco:nodes_wanted(),
    {Results, BadNodes} = rpc:multicall(Nodes, M, F, A),
    GoodNodes = Nodes -- BadNodes,

    BadResults =
        lists:filter(
          fun ({_, R}) ->
                  case R of
                      {ok, _} ->
                          false;
                      ok ->
                          false;
                      _ ->
                          true
                  end
          end, lists:zip(GoodNodes, Results)),

    case BadResults ++ BadNodes of
        [] ->
            menelaus_util:reply_text(Req, "", 200);
        _ ->
            Bad = BadResults ++ [{N, bad_node} || N <- BadNodes],
            Msg = io_lib:format("Failed nodes:~n~100p~n", [Bad]),
            menelaus_util:reply_text(Req, Msg, 500)
    end,

    done.

perform_purge() ->
    perform_purge(?CAS_RETRIES).

perform_purge(0) ->
    {error, exceeded_retries};
perform_purge(I) ->
    KVList = ns_config:get_kv_list(),
    {NewKVList, Tombstones} =
        lists:foldr(
          fun ({Key, FullValue} = Pair, {AccNewKVList, AccTombstones}) ->
                  case ns_config:strip_metadata(FullValue) of
                      '_deleted' ->
                          {AccNewKVList, [Key | AccTombstones]};
                      _ ->
                          {[Pair | AccNewKVList], AccTombstones}
                  end
          end, {[], []}, KVList),

    case Tombstones of
        [] ->
            {ok, not_purged};
        _ ->
            case ns_config:cas_local_config(NewKVList, KVList) of
                true ->
                    log("Purged ~b tombstones:~n"
                        "~200P",
                        [length(Tombstones), Tombstones, 100]),
                    {ok, purged};
                false ->
                    log("Failed to apply new config. Retrying."),
                    perform_purge(I - 1)
            end
    end.
