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
-include("ns_common.hrl").

-define(PENDING_MAX_SIZE, 3000). % Number of recent log entries
-define(RECENT_MAX_SIZE, 3000). % Number of recent log entries
-define(DUP_TIME, 15000000). % 15 secs in microsecs
-define(DEDUP_TIME, 60000). % 60 secs in millisecs
-define(SAVE_DELAY, 5000). % 5 secs in millisecs

% The server_name used by gossip_replicator to register this server.
-define(SERVER, ?MODULE).

-behaviour(gossip_replicator).
-behaviour(ns_log_categorizing).

%% API
-export([start_link/0]).

-export([start_link_babysitter_log_consumer/0]).

-export([log/6, log/7, recent/0, recent/1, delete_log/0]).

-export([code_string/2, prepare_message/3]).

-export([ns_log_cat/1, ns_log_code_string/1]).

%% exports for gossip_replicator.
-export([init/1,
         handle_add_log/3,
         add_local_node_metadata/2,
         strip_local_node_metadata/1,
         merge_remote_logs/3,
         merge_pending_list/3,
         modify_recent/2,
         handle_info/3,
         handle_notify/1]).

-include("ns_common.hrl").

-record(ns_log_state, {dedup}).

start_link() ->
    FileName = ns_config:search_node_prop(ns_config:get(), ns_log, filename),
    gossip_replicator:start_link(?SERVER, [?MODULE, FileName, ?PENDING_MAX_SIZE,
                                           ?RECENT_MAX_SIZE]).

start_link_babysitter_log_consumer() ->
    {ok, proc_lib:spawn_link(fun babysitter_log_consumption_loop_tramp/0)}.

babysitter_log_consumption_loop_tramp() ->
    misc:delaying_crash(1000, fun babysitter_log_consumption_loop/0).

babysitter_log_consumption_loop() ->
    consume_log(
      ns_babysitter_log:get_oldest_message_from_inside_ns_server()),
    babysitter_log_consumption_loop().

consume_log({crash, {Name, OsPid, Status, Messages}}) ->
    LogLevel = case Status of
                 0 ->
                     debug;
                 _ ->
                     info
             end,
    ale:log(?USER_LOGGER, LogLevel,
            "Service '~p' exited with status ~p. Restarting. Messages:~n~s",
            [Name, Status, Messages]),

    OsPidJSON = case OsPid of
                    undefined ->
                        [];
                    _ ->
                        [{os_pid, OsPid}]
                end,

    event_log:add_log(service_crashed, [{name, Name},
                                        {exit_status, Status}] ++ OsPidJSON);
consume_log({service_started, Name}) ->
    event_log:add_log(service_started, [{name, Name}]).

%%--------------------------------------------------------------------
%%% callbacks for gossip_replicator.
%%--------------------------------------------------------------------

init(_Recent) ->
    send_dedup_logs_msg(),
    #ns_log_state{dedup=dict:new()}.

handle_add_log(Log, State0, ReplicateFun) ->
    {Dup, State} = is_duplicate_log(Log, State0),
    case Dup of
        true ->
            ok;
        false ->
            ReplicateFun(Log)
    end,
    State.

add_local_node_metadata(Logs, State) ->
    {Logs, State}.

strip_local_node_metadata(Logs) ->
    Logs.

%% We currently don't support changing the max number of user logs
%% we store.

modify_recent(Recent, _MaxLen) ->
    Recent.

%% Merge Logs received from remote node with the logs on the local node.
merge_remote_logs(LocalLogs, RemoteLogs, MaxLen) ->
    misc:tail_of_length(lists:umerge(fun order_entries/2,
                                     LocalLogs,
                                     RemoteLogs), MaxLen).

%% NOTE: merge_pending_list/2 is minorly different from merge_remote_logs,
%% in that the Pending list has to be sorted by order_entries/2, before the
%% umerge/3 function is applied.
merge_pending_list(Recent, Pending, MaxLen) ->
    misc:tail_of_length(lists:umerge(fun order_entries/2,
                                     lists:sort(fun order_entries/2, Pending),
                                                Recent), MaxLen).

handle_info(dedup_logs, State, ReplicatorFun) ->
    send_dedup_logs_msg(),
    handle_dedup_logs(State, ReplicatorFun);
handle_info(_Info, State, _ReplicatorFun) ->
    State.

handle_notify(State) ->
    State.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

is_duplicate_log(#log_entry{module = Module,
                            code = Code, msg = Fmt, args = Args,
                            cat = Category, tstamp = Time},
                 #ns_log_state{dedup = Dedup} = State) ->
    Key = {Module, Code, Category, Fmt, Args},
    case dict:find(Key, Dedup) of
        {ok, {Count, FirstSeen, LastSeen}} ->
            ?log_info("suppressing duplicate log ~p:~p(~p) because it's been "
                      "seen ~p times in the past ~p secs (last seen ~p secs "
                      "ago", [Module, Code,
                              lists:flatten(io_lib:format(Fmt, Args)),
                              Count + 1,
                              timer:now_diff(Time, FirstSeen) / 1000000,
                              timer:now_diff(Time, LastSeen) / 1000000]),
            Dedup2 = dict:store(Key, {Count+1, FirstSeen, Time}, Dedup),
            {true, State#ns_log_state{dedup = Dedup2}};
        error ->
            Dedup2 = dict:store(Key, {0, Time, Time}, Dedup),
            {false, State#ns_log_state{dedup = Dedup2}}
    end.

order_entries(A = #log_entry{}, B = #log_entry{}) ->
    A#log_entry{server_time = undefined} =<
        B#log_entry{server_time = undefined}.

handle_dedup_logs(State = #ns_log_state{dedup=Dupes}, ReplicatorFun) ->
    DupesList = dedup_logs(erlang:timestamp(), dict:to_list(Dupes), [],
                           ReplicatorFun),
    State#ns_log_state{dedup=dict:from_list(DupesList)}.

dedup_logs(_Now, [], DupesList, _ReplicatorFun) -> DupesList;
dedup_logs(Now, [{Key, Value} | Rest], DupesList, ReplicatorFun) ->
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

                                       tstamp=Now,
                                       server_time =
                                       calendar:now_to_local_time(Now)},
                    do_log(Entry, ReplicatorFun)
            end,
            dedup_logs(Now, Rest, DupesList, ReplicatorFun);
        false -> dedup_logs(Now, Rest, [{Key, Value} | DupesList],
                            ReplicatorFun)
    end.

add_server_time(#log_entry{tstamp = TStamp} = Log) ->
    Log#log_entry{server_time=calendar:now_to_local_time(TStamp)}.

do_log(#log_entry{code=undefined} = Log, ReplicatorFun) ->
    %% Code can be undefined if logging module doesn't define ns_log_cat
    %% function. We change the code to 0 for such cases. Note that it must be
    %% done before abcast-ing (not in handle_cast) because some of the nodes
    %% in the cluster can be of the older version (thus this case won't be
    %% handled there).
    do_log(Log#log_entry{code=0}, ReplicatorFun);
do_log(#log_entry{code=Code} = Log0, ReplicatorFun) when is_integer(Code) ->
    Log = add_server_time(Log0),
    %% The ReplicatorFun is essentially gossip_replicator:replicate_log/2.
    ReplicatorFun(Log).

send_dedup_logs_msg() ->
    erlang:send_after(?DEDUP_TIME, self(), dedup_logs).

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
%% Code can be undefined if logging module doesn't define ns_log_cat
%% function. We change the code to 0 for such cases. Note that it must be
%% done before abcast-ing (not in handle_cast) because some of the nodes
%% in the cluster can be of the older version (thus this case won't be
%% handled there)
log(Module, Node, Time, undefined, Category, Fmt, Args) ->
    log(Module, Node, Time, 0, Category, Fmt, Args);
log(Module, Node, Time, Code, Category, Fmt, Args) ->
    Log = #log_entry{module = Module, node = Node, tstamp = Time,
                     code = Code, cat = Category,
                     msg = Fmt, args = Args,
                     server_time = calendar:now_to_local_time(Time)},
    gossip_replicator:add_log(?SERVER, Log).

-spec recent() -> list(#log_entry{}).
recent() ->
    gossip_replicator:recent(?SERVER).

-spec recent(atom()) -> list(#log_entry{}).
recent(Module) ->
    [E || E <- recent(),
          E#log_entry.module =:= Module].

delete_log() ->
    gossip_replicator:delete_logs(ns_log).

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
