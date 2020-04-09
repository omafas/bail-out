%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
-module(cluster_logs_collection_task).

-include("ns_common.hrl").

-export([start_link/3, start_link_ets_holder/0]).

-export([maybe_build_cluster_logs_task/0]).

-export([preflight_base_url/2,
         preflight_proxy_url/1]).

%% called remotely
-export([start_collection_per_node/3,
         start_upload_per_node/4]).

start_link(Nodes, BaseURL, Options) ->
    proc_lib:start_link(erlang, apply, [fun collect_cluster_logs/3, [Nodes,
                                                                     BaseURL,
                                                                     Options]]).

start_link_ets_holder() ->
    misc:start_event_link(
      fun () ->
              _ = ets:new(cluster_logs_collection_task_status, [named_table, public])
      end).


maybe_build_cluster_logs_task() ->
    Tasks = try ets:tab2list(cluster_logs_collection_task_status)
            catch T:E ->
                    ?log_debug("Ignoring exception trying to read cluster_logs_collection_task_status table: ~p:~p", [T,E]),
                    []
            end,

    case lists:keyfind(cluster, 1, Tasks) of
        false ->
            [];
        {cluster, Nodes, BaseURL, Timestamp, PidOrCompleted} ->
            [build_cluster_logs_task_tail(Tasks, Nodes, BaseURL, Timestamp, PidOrCompleted)]
    end.

build_cluster_logs_task_tail(Tasks, Nodes, BaseURL, Timestamp, PidOrCompleted) ->
    Status = case PidOrCompleted of
                 completed ->
                     completed;
                 _ ->
                     case is_process_alive(PidOrCompleted) of
                         true ->
                             running;
                         _ ->
                             cancelled
                     end
             end,

    NodeStatuses = [{N, build_node_task_status(Tasks, BaseURL, N)} || N <- Nodes],
    CompletedNodes = [ok || {_, NS} <- NodeStatuses,
                            case proplists:get_value(status, NS) of
                                failed -> true;
                                collected -> true;
                                uploaded -> true;
                                failedUpload -> true;
                                _ -> false
                            end],

    Progress = case length(NodeStatuses) of
                   0 -> 100;
                   Total -> length(CompletedNodes) * 100 div Total
               end,

    [{type, cluster_logs_collect},
     {status, Status},
     {progress, Progress},
     {timestamp, Timestamp},
     {perNode, NodeStatuses}].

build_node_task_status(Tasks, BaseURL, Node) ->
    case lists:keyfind({Node, collection}, 1, Tasks) of
        false ->
            [{status, starting}];
        {_, started, Path} ->
            [{status, started},
             {path, Path}];
        {_, died, killed} ->
            [{status, cancelled}];
        {_, died, _Reason} ->
            [{status, failed}];
        {_, {ok, Path, _Output}} ->
            case BaseURL =:= false of
                true ->
                    [{status, collected},
                     {path, Path}];
                _ ->
                    [{path, Path} | build_node_upload_task_status(Tasks, Node)]
            end;
        {_, {error, Status, Output}} ->
            [{status, failed},
             {collectionStatusCode, Status},
             {collectionOutput, Output}]
    end.

build_node_upload_task_status(Tasks, Node) ->
    case lists:keyfind({Node, upload}, 1, Tasks) of
        false ->
            [{status, startingUpload}];
        {_, started, URL} ->
            [{status, startedUpload},
             {url, URL}];
        {_, died, _Reason} ->
            [{status, failedUpload}];
        {_, {ok, URL}} ->
            [{status, uploaded},
             {url, URL}];
        {_, {error, URL, Status, Output}} ->
            [{status, failedUpload},
             {url, URL},
             {uploadStatusCode, Status},
             {uploadOutput, Output}]
    end.

format_timestamp({{Year,Month,Day},{Hour,Min,Sec}}) ->
    lists:flatten(
      io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B~2.10.0B~2.10.0B",
                    [Year, Month, Day, Hour, Min, Sec])).

collect_cluster_logs(Nodes, BaseURL, Options) ->
    Timestamp = erlang:universaltime(),
    TimestampS = format_timestamp(Timestamp),
    ets:delete_all_objects(cluster_logs_collection_task_status),
    update_ets_status({cluster, Nodes, BaseURL, Timestamp, self()}),

    CollectConcurrency = ns_config:read_key_fast(cluster_logs_collection_concurrency, 1024),
    {ok, CollectThrottle} = new_concurrency_throttle:start_link({CollectConcurrency, log_collection}, undefined),
    UploadConcurrency = ns_config:read_key_fast(cluster_logs_upload_concurrency, 1),
    {ok, UploadThrottle} = new_concurrency_throttle:start_link({UploadConcurrency, log_upload}, undefined),

    update_ets_status({collect_throttle, CollectThrottle}),
    update_ets_status({upload_throttle, UploadThrottle}),

    proc_lib:init_ack({ok, self()}),

    ns_heart:force_beat(),

    misc:parallel_map(
      fun (N) ->
              run_node_collection(N, BaseURL, TimestampS, Options)
      end, Nodes, infinity),

    update_ets_status({cluster, Nodes, BaseURL, Timestamp, completed}).


run_node_collection(Node, BaseURL, TimestampS, Options) ->
    misc:executing_on_new_process(
      fun () ->
              erlang:process_flag(trap_exit, true),
              do_run_node_collection(Node, BaseURL, TimestampS, Options)
      end).

update_ets_status(Tuple) ->
    ets:insert(cluster_logs_collection_task_status, Tuple).

lookup_throttle(ThrottleName) ->
    [{_, Pid}] = ets:lookup(cluster_logs_collection_task_status, ThrottleName),
    Pid.

get_token(ThrottleName) ->
    Pid = lookup_throttle(ThrottleName),
    new_concurrency_throttle:send_back_when_can_go(Pid, go),
    receive
        {'EXIT', _, Reason} = ExitMsg ->
            ?log_debug("Got exit waiting for token: ~p", [ExitMsg]),
            erlang:exit(Reason);
        go ->
            ok
    end.

put_token(ThrottleName) ->
    new_concurrency_throttle:is_done(lookup_throttle(ThrottleName)).

start_subtask(Node, Subtask, M, F, A) ->
    Pid = proc_lib:spawn_link(Node, M, F, A),
    receive
        {ack, Pid, Return} ->
            Return;
        {'EXIT', Pid, Reason} ->
            update_ets_status({{Node, Subtask}, died, Reason}),
            erlang:exit(Reason);
        {'EXIT', _Other, Reason} = Msg ->
            ?log_info("Got exit ~p while waiting for collectinfo slave on node ~p", [Msg, Node]),
            erlang:exit(Reason)
    end.

wait_child(P, Node, Subtask) ->
    ?log_debug("Got child to wait: ~p, node: ~p, task: ~p", [P, Node, Subtask]),
    receive
        {'EXIT', P, Reason} ->
            receive
                {P, Status} ->
                    update_ets_status({{Node, Subtask}, Status}),
                    ok
            after 0 ->
                    update_ets_status({{Node, Subtask}, died, Reason}),
                    error
            end;
        {'EXIT', _Other, Reason} = Msg ->
            ?log_info("got exit ~p while waiting for collectinfo slave on node ~p", [Msg, Node]),
            update_ets_status({{Node, Subtask}, died, Reason}),
            erlang:exit(Reason)
    end.

do_run_node_collection(Node, BaseURL, TimestampS, Options) ->
    get_token(collect_throttle),
    ?log_debug("Starting collection task for: ~p", [{Node, BaseURL, TimestampS, Options}]),
    OptionsWithSalt =
        case proplists:get_value(redact_salt_fun, Options) of
            undefined ->
                Options;
            Fun ->
                [{redact_salt, Fun()} | Options]
        end,

    {ok, P, Path} = start_subtask(Node, collection,
                                  cluster_logs_collection_task,
                                  start_collection_per_node,
                                  [TimestampS, self(), OptionsWithSalt]),
    update_ets_status({{Node, collection}, started, Path}),
    case wait_child(P, Node, collection) of
        ok ->
            put_token(collect_throttle),
            maybe_upload_node_result(Node, Path, BaseURL, Options);
        Err ->
            Err
    end.

maybe_upload_node_result(_Node, _Path, false, _Options) ->
    ok;
maybe_upload_node_result(Node, Path, BaseURL, Options) ->
    get_token(upload_throttle),
    ?log_debug("Starting upload task for: ~p", [{Node, Path, BaseURL, Options}]),
    {ok, P, URL} = start_subtask(Node, upload,
                                 cluster_logs_collection_task,
                                 start_upload_per_node,
                                 [Path, BaseURL, self(), Options]),
    update_ets_status({{Node, upload}, started, URL}),
    wait_child(P, Node, upload).

start_collection_per_node(TimestampS, Parent, Options) ->
    Basename = "collectinfo-" ++ TimestampS ++ "-" ++ atom_to_list(node()),
    InitargsFilename = path_config:component_path(data, "initargs"),

    LogPath = case proplists:get_value(log_dir, Options) of
                  undefined -> path_config:component_path(tmp);
                  Val -> Val
              end,
    Filename = filename:join(LogPath, Basename ++ ".zip"),

    {UploadFilename, MaybeLogRedaction} =
        case proplists:get_value(redact_level, Options) of
            partial ->
                Salt = proplists:get_value(redact_salt, Options),
                ?log_debug("Partial redaction requested. Hashed salt = ~p",
                           [crypto:hash(sha, Salt)]),
                {filename:join(LogPath, Basename ++ "-redacted" ++ ".zip"),
                 ["--log-redaction-level=partial",
                  "--log-redaction-salt=" ++ Salt]};
            _ ->
                {Filename, []}
        end,
    proc_lib:init_ack(Parent, {ok, self(), UploadFilename}),

    MaybeTmpDir = case proplists:get_value(tmp_dir, Options) of
                      undefined -> [];
                      Value -> ["--tmp-dir=" ++ Value]
                  end,

    Args0 = ["--watch-stdin"] ++ MaybeLogRedaction ++
        MaybeTmpDir ++ ["--initargs=" ++ InitargsFilename, Filename],

    ExtraArgs = ns_config:search_node_with_default(cbcollect_info_extra_args, []),
    Env = ns_config:search_node_with_default(cbcollect_info_extra_env, []),

    Args = Args0 ++ ExtraArgs,

    ?log_debug("spawning collectinfo:~n"
               "  Args: ~p~n"
               "  Env: ~p", [Args -- MaybeLogRedaction, Env]),
    {Status, Output} =
        misc:run_external_tool(
          path_config:component_path(bin, "cbcollect_info"),
          Args, Env, [graceful_shutdown]),
    case Status of
        0 ->
            ?log_debug("Done"),
            Parent ! {self(), {ok, UploadFilename, Output}};
        _ ->
            ?log_error("Log collection failed with status: ~p.~nOutput:~n~s",
                       [Status, Output]),
            Parent ! {self(), {error, Status, Output}}
    end.

start_upload_per_node(Path, BaseURL, Parent, Options) ->
    URL = BaseURL ++ mochiweb_util:quote_plus(filename:basename(Path)),
    proc_lib:init_ack(Parent, {ok, self(), URL}),
    MaybeUploadProxy = case proplists:get_value(upload_proxy, Options) of
                           undefined -> [];
                           V -> ["--upload-proxy=" ++ V]
                       end,

    Args = MaybeUploadProxy ++
        ["--watch-stdin", "--just-upload-into=" ++ URL, Path],
    ?log_debug("Spawning upload cbcollect_info: ~p", [Args]),
    {Status, Output} =
        misc:run_external_tool(
          path_config:component_path(bin, "cbcollect_info"),
          Args, [], [graceful_shutdown]),
    case Status of
        0 ->
            ?log_debug("uploaded ~s to ~s successfully. Deleting it", [Path, URL]),
            _ = file:delete(Path),
            Parent ! {self(), {ok, URL}};
        _ ->
            ?log_debug("upload of ~s to ~s failed with status ~p~n~s", [Path, URL, Status, Output]),
            Parent ! {self(), {error, URL, Status, Output}}
    end.

preflight_lhttpc_request(Type, URL, Options) ->
    case lhttpc:request(URL, head, [], [], 20000, Options) of
        {ok, Result} ->
            ?log_debug("~p url check received '~p' from '~s'",
                       [Type, Result, URL]),
            ok;
        {error, {Reason, Stack}} ->
            ?log_debug("~p url check unable to access '~s' (~p): ~p",
                       [Type, URL, Reason, Stack]),
            Msg = io_lib:format("Unable to access '~s' : ~p",
                                [URL, {error, Reason}]),
            {error, iolist_to_binary(Msg)};
        {error, Reason} ->
            Msg = io_lib:format("Unable to access '~s' : ~p", [URL, Reason]),
            ?log_debug(Msg),
            {error, iolist_to_binary(Msg)}
    end.

preflight_base_url(false, false) ->
    ok;
preflight_base_url(BaseURL, {upload_proxy, URL}) ->
    preflight_lhttpc_request("Base", BaseURL, [{proxy, URL}]);
preflight_base_url(BaseURL, false) ->
    preflight_lhttpc_request("Base", BaseURL, []).

preflight_proxy_url(false) ->
    ok;
preflight_proxy_url({upload_proxy, URL}) ->
    preflight_lhttpc_request("Proxy", URL, []);
preflight_proxy_url(ProxyInfo) ->
    Msg = io_lib:format("Invalid proxy '~s'", [ProxyInfo]),
    {error, iolist_to_binary(Msg)}.
