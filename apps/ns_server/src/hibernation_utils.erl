%% @author Couchbase <info@couchbase.com>
%% @copyright 2022-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(hibernation_utils).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("bucket_hibernation.hrl").

-export([supported_services/0,
         run_hibernation_op/3,
         get_snapshot/1,
         check_allow_pause_op/1,
         check_allow_resume_op/2,
         set_hibernation_status/2,
         update_hibernation_status/1,
         build_hibernation_task/0,
         unpause_bucket/1,
         unpause_bucket/2,
         sync_s3/3,
         upload_metadata_to_s3/2,
         get_metadata_from_s3/1,
         get_bucket_config/1,
         get_bucket_uuid/1,
         get_bucket_manifest/1,
         get_data_remote_path/1,
         get_node_data_remote_path/2,
         get_bucket_data_remote_path/3,
         log_hibernation_event/3]).

supported_services() ->
    [index, fts].

run_hibernation_op(_Body, _Args = [], _Timeout) ->
    ok;
run_hibernation_op(Body, Args, Timeout) ->
    case async:run_with_timeout(
           fun () ->
                   async:foreach(
                     Body, Args, [exit_on_first_error])
           end, Timeout) of
        {ok, Result} ->
            Result;
        {error, timeout} ->
            exit(timeout)
    end.

get_snapshot(Bucket) ->
    chronicle_compat:get_snapshot(
      [ns_bucket:fetch_snapshot(Bucket, _),
       ns_cluster_membership:fetch_snapshot(_)]).

check_map_and_servers(BucketConfig) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    Map = proplists:get_value(map, BucketConfig),
    Rv = lists:all(
           fun (Chain) ->
                   ordsets:is_subset(ordsets:from_list(Chain), Servers)
           end, Map),
    case Rv of
        true ->
            ok;
        _ ->
            {error, map_servers_mismatch}
    end.

check_failed_over_service_nodes(Snapshot) ->
    InactiveFailedNodes = ns_cluster_membership:inactive_failed_nodes(Snapshot),
    SupportedServices = sets:from_list(supported_services()),
    Rv = lists:any(
           fun (Node) ->
                   not(sets:is_disjoint(
                         sets:from_list(
                           ns_cluster_membership:node_services(Node)),
                         SupportedServices))
           end, InactiveFailedNodes),

    case Rv of
        true ->
            {error, failed_service_nodes};
        _ ->
            ok
    end.

check_servers(BucketConfig) ->
    LiveServers = ns_bucket:live_bucket_nodes_from_config(BucketConfig),
    Servers = ns_bucket:get_servers(BucketConfig),
    case length(LiveServers) =:= length(Servers) of
        true ->
            ok;
        false ->
            {error, full_servers_unavailable}
    end.

check_placement_balance(BucketConfig) ->
    Servers = ns_bucket:get_servers(BucketConfig),
    DesiredServers = ns_bucket:get_desired_servers(BucketConfig),
    case bucket_placer:is_balanced(BucketConfig, Servers, DesiredServers) of
        true ->
            ok;
        _ ->
            {error, requires_rebalance}
    end.

check_width_present(BucketConfig) ->
    case ns_bucket:get_width(BucketConfig) of
        undefined ->
            {error, no_width_parameter};
        _ ->
            ok
    end.

check_bucket_type(BucketConfig) ->
    case ns_bucket:kv_bucket_type(BucketConfig) of
        persistent ->
            ok;
        ephemeral ->
            {error, bucket_type_not_supported}
    end.

check_allow_pause_op(Bucket) ->
    Snapshot = get_snapshot(Bucket),
    case ns_bucket:get_bucket(Bucket, Snapshot) of
        {ok, BucketCfg} ->
            Res = functools:sequence_(
                    [?cut(check_bucket_type(BucketCfg)),
                     ?cut(check_width_present(BucketCfg)),
                     ?cut(check_placement_balance(BucketCfg)),
                     ?cut(check_servers(BucketCfg)),
                     ?cut(check_failed_over_service_nodes(Snapshot)),
                     ?cut(check_map_and_servers(BucketCfg))]),
            case Res of
                ok ->
                    {ok, [Snapshot]};
                _ ->
                    Res
            end;
        _ ->
            {error, bucket_not_found}
    end.

get_new_bucket_config(Bucket, PausedBucketCfg,
                      BucketVersion) when BucketVersion =:= ?VERSION_76 ->
    Filter = [servers, desired_servers, map],
    NewConfig = lists:filter(fun ({K, _V}) ->
                                     not lists:member(K, Filter)
                             end, PausedBucketCfg) ++
        [{servers, []}, {hibernation_state, resuming}],
    bucket_placer:place_bucket(Bucket, NewConfig).

get_metadata_from_s3(#bucket_hibernation_op_args{
                        remote_path = RemotePath} = Args) ->
    try
        KvRemotePath = get_data_remote_path(RemotePath),
        {ok, get_bucket_metadata_from_s3(
               Args#bucket_hibernation_op_args{remote_path = KvRemotePath})}
    catch
        _:{_, {error, {cbobjutils_status, _}, Error}} ->
            {error, iolist_to_binary(io_lib:format("s3_get_failure: ~p",
                                                   [Error]))};
        T:E:S ->
            ?log_error("get_metadata_from_s3 failed.~n~p.", [{T, E, S}]),
            {error, s3_get_failure}
    end.

get_paused_bucket_cfg(Metadata) ->
    BucketVersion = get_bucket_version(Metadata),
    PausedBucketCfg = get_bucket_config(Metadata),
    {BucketVersion, PausedBucketCfg}.

check_allow_resume_op(Bucket, Metadata) ->
    ActiveBuckets = ns_bucket:get_bucket_names(),
    ShutdownBuckets = ns_bucket:get_bucket_names_marked_for_shutdown(),
    case ns_bucket:name_conflict(ActiveBuckets ++ ShutdownBuckets) of
        false ->
            {Version, PausedBucketCfg} = get_paused_bucket_cfg(Metadata),
            case get_new_bucket_config(Bucket, PausedBucketCfg, Version) of
                {ok, NewBucketConfig} ->
                    {ok, [NewBucketConfig]};
                {error, BadZones} ->
                    {error, {need_more_space, BadZones}}

            end;
        true ->
            {error, bucket_exists}
    end.

get_hibernation_status(Snapshot) ->
    chronicle_compat:get(Snapshot, hibernation_status, #{default => undefined}).

set_hibernation_status(Bucket, Status) ->
    chronicle_compat:set_multiple(
      [{hibernation_status, Status},
       {hibernation_uuid, couch_uuids:random()},
       {hibernation_bucket, Bucket}]).

update_hibernation_status(Status) ->
    chronicle_kv:transaction(
      kv, [hibernation_status],
      fun (Snapshot) ->
              case get_hibernation_status(Snapshot) of
                  {Op, running} ->
                      {commit, [{set, hibernation_status, {Op, Status}}]};
                  _ ->
                      {abort, ok}
              end
      end),
    ok.

keys() ->
    [hibernation_status, hibernation_uuid, hibernation_bucket].

fetch_snapshot(Txn) ->
    chronicle_compat:txn_get_many(keys(), Txn).

build_task_prop(_, undefined) ->
    [];
build_task_prop(hibernation_status, {Op, Status}) when is_atom(Status) ->
    [{op, Op}, {status, Status}];
build_task_prop(hibernation_bucket, Bucket) when is_list(Bucket) ->
    [{bucket, list_to_binary(Bucket)}];
build_task_prop(hibernation_uuid, UUID) when is_binary(UUID) ->
    [{id, UUID}].

build_hibernation_task() ->
    Snapshot = chronicle_compat:get_snapshot([fetch_snapshot(_)]),
    TaskProps = [begin
                     Val = chronicle_compat:get(Snapshot, Key,
                                                #{default => undefined}),
                     build_task_prop(Key, Val)
                 end
                 || Key <- keys()],
    Task = lists:flatten(TaskProps),
    case Task of
        [] ->
            [];
        _ ->
            [[{type, hibernation} | Task] ++
             [{isStale,
               leader_registry:whereis_name(ns_orchestrator) =:= undefined}]]

    end.

unpause_bucket(Bucket) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
    BucketNodes = ns_bucket:get_servers(BucketConfig),
    unpause_bucket(Bucket, BucketNodes).

unpause_bucket(Bucket, BucketNodes) when BucketNodes =/= [] ->
    misc:with_trap_exit(
      fun () ->
              {Worker, Ref} =
                  misc:spawn_monitor(
                    ?cut(unpause_bucket_body(Bucket, BucketNodes))),
              receive
                  {'DOWN', Ref, process, Worker, Reason} ->
                      case Reason of
                          normal ->
                              ?log_debug("unpause_bucket for bucket ~p "
                                         "completed successfully on nodes: ~p.",
                                         [Bucket, BucketNodes]),
                              ok;
                          _ ->
                              ?log_error("unpause_bucket for bucket ~p failed. "
                                         "BucketNodes: ~p, Reason: ~p.",
                                         [Bucket, BucketNodes, Reason]),
                              failure
                      end;
                  {'EXIT', _Pid, Reason} ->
                      ?log_debug("Received 'EXIT' while unpausing bucket: ~p. "
                                 "Terminating worker: ~p. Reason: ~p.",
                                 [Bucket, Worker, Reason]),
                      misc:terminate_and_wait(Worker, Reason)
              end
      end).

unpause_bucket_body(Bucket, BucketNodes) ->
    Timeout = ?get_timeout(unpause_bucket, 5000),
    %% Make a best-case effort to unpause bucket on the BucketNodes.
    Results =
        misc:parallel_map_partial(
          fun (BucketNode) ->
                  try
                      kv_hibernation_agent:unpause_bucket(
                        Bucket, BucketNode)
                  catch
                      E:T:S ->
                          ?log_error("unpause_bucket for bucket: ~p "
                                     "failed on node: ~p. "
                                     "Error: {~p, ~p, ~p} ",
                                     [Bucket, BucketNode,
                                      E, T, S]),
                          {error, unpause_bucket_failed}
                  end
          end,
          BucketNodes, Timeout),

    OkNodes =
        lists:filtermap(
          fun ({Node, {ok, ok}}) ->
                  {true, Node};
              (_) ->
                  false
          end, lists:zip(BucketNodes, Results)),

    case BucketNodes -- OkNodes of
        [] ->
            ok;
        FailedNodes ->
            exit({unpause_bucket_failed, {failed_nodes, FailedNodes}})
    end.


-spec sync_s3(#bucket_hibernation_op_args{}, string(), to | from)->
          ok | {error, non_neg_integer(), binary()}.
sync_s3(#bucket_hibernation_op_args{
           remote_path = RemotePath,
           blob_storage_region = BlobStorageRegion,
           rate_limit = RateLimit},
        LocalPath, SyncCode) ->
    Cmd = path_config:component_path(bin, "cbobjutil"),

    {SrcPath, DstPath} = case SyncCode of
                         to ->
                             {LocalPath, RemotePath};
                         from ->
                             {RemotePath, LocalPath}
                     end,

    ExtraArgs =
        case os:getenv("LOCALSTACK_ENDPOINT") of
            false ->
                [];
            Endpoint ->
                ["--endpoint", Endpoint, "--s3-force-path-style"]
        end,

    Args = lists:append(
             [["--region", BlobStorageRegion],
              ["--rate-limit", integer_to_list(RateLimit)],
              ["sync", SrcPath, DstPath], ExtraArgs]),

    {Status, Output} = misc:run_external_tool(Cmd, Args, [],
                                              [graceful_shutdown]),
    case Status of
        0 ->
            ok;
        _ ->
            ?log_error("cbobjutil call `~s` returned ~b:~n~s",
                       [Cmd, Status, Output]),
            {error, {cbobjutils_status, Status}, Output}
    end.

get_data_remote_path(RemotePath) ->
    RemotePath ++ "/data/".

get_node_data_remote_path(RemotePath, Node) ->
    RemotePath ++ atom_to_list(Node).

get_bucket_data_remote_path(RemotePath, Node, Bucket) ->
    get_node_data_remote_path(RemotePath, Node) ++ "/" ++ Bucket.

get_bucket_metadata_filename() ->
    "Metadata".

get_version_filename() ->
    "version.json".

write_to_temp_file(FileName, Data) ->
    TempFile = path_config:component_path(tmp, FileName),
    ok = misc:write_file(TempFile, Data),
    TempFile.

s3_upload_and_remove_file(Args, TempFile) ->
    try
        sync_s3(Args, TempFile, to)
    catch
        T:E:S ->
            ?log_error("sync_s3 failed.~n~p.", [{T, E, S}]),
            {error, sync_to_s3}
    after
        file:delete(TempFile)
    end.

encode_term(Term) ->
    io_lib:format("~0p.~n", [Term]).

upload_bucket_metadata(Args, Metadata) ->
    FileName = get_bucket_metadata_filename(),
    TempFile = write_to_temp_file(FileName, encode_term(Metadata)),
    s3_upload_and_remove_file(Args, TempFile).

encode_version(Version) ->
    ejson:encode({[{version, Version}]}).

upload_version_json(Args, Version) ->
    FileName = get_version_filename(),
    TempFile = write_to_temp_file(FileName, encode_version(Version)),
    s3_upload_and_remove_file(Args, TempFile).

upload_metadata_to_s3(#bucket_hibernation_op_args{
                         bucket = BucketName} = Args,
                      Snapshot) ->
    {ok, BucketCfg} = ns_bucket:get_bucket(BucketName, Snapshot),
    Manifest = collections:get_manifest(BucketName, Snapshot),
    BucketUUID = ns_bucket:uuid(BucketName, Snapshot),
    Metadata = [{bucket_cfg, BucketCfg}, {bucket_uuid, BucketUUID},
                {bucket_manifest, Manifest}],
    ok = upload_bucket_metadata(Args, Metadata),
    ok = upload_version_json(Args, cluster_compat_mode:get_compat_version()).

s3_sync_from_file(#bucket_hibernation_op_args{remote_path = RemotePath0} = Args,
                  FileName, DataFromFileFunc) ->
    RemotePath = RemotePath0 ++ FileName,
    TempFile = path_config:component_path(tmp, FileName),
    ok = sync_s3(Args#bucket_hibernation_op_args{remote_path = RemotePath},
                 TempFile, from),
    try
        DataFromFileFunc(TempFile)
    catch
        _:_ -> {error, file_sync_failure}
    after
        file:delete(TempFile)
    end.

get_bucket_metadata_from_s3(Args) ->
    FileName = get_bucket_metadata_filename(),
    {ok, Metadata} =
        s3_sync_from_file(Args, FileName,
                          fun(TempFile) ->
                                  {ok, [Data]} = file:consult(TempFile),
                                  {ok, Data}
                          end),
    Version = get_version_from_s3(Args),
    Metadata ++ [{version, Version}].

get_bucket_version(Metadata) ->
    proplists:get_value(version, Metadata).

get_bucket_config(Metadata) ->
    proplists:get_value(bucket_cfg, Metadata).

get_bucket_manifest(Metadata) ->
    proplists:get_value(bucket_manifest, Metadata).

get_bucket_uuid(Metadata) ->
    proplists:get_value(bucket_uuid, Metadata).

decode_version(Data) ->
    {Json} = ejson:decode(Data),
    proplists:get_value(<<"version">>, Json).

get_version_from_s3(Args) ->
    FileName = get_version_filename(),
    {ok, Version} =
        s3_sync_from_file(Args, FileName,
                          fun(TempFile) ->
                                  {ok, Data} = file:read_file(TempFile),
                                  {ok, decode_version(Data)}
                          end),
    Version.

get_event_id(initiated, pause_bucket) ->
    pause_bucket_initiated;
get_event_id(initiated, resume_bucket) ->
    resume_bucket_initiated;
get_event_id(stopped, pause_bucket) ->
    pause_bucket_stopped;
get_event_id(stopped, resume_bucket) ->
    resume_bucket_stopped;
get_event_id(failed, pause_bucket) ->
    pause_bucket_failed;
get_event_id(failed, resume_bucket) ->
    resume_bucket_failed;
get_event_id(completed, pause_bucket) ->
    pause_bucket_completed;
get_event_id(completed, resume_bucket) ->
    resume_bucket_completed.

log_hibernation_event(Status, Op, Bucket) ->
    EventId = get_event_id(Status, Op),
    event_log:add_log(EventId, [{bucket, list_to_binary(Bucket)}]).
