%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(service_api).

-include("ns_common.hrl").
-include("service_api.hrl").
-include("bucket_hibernation.hrl").

-export([shutdown/1, get_node_info/1,
         get_task_list/2, cancel_task/3,
         get_current_topology/2, get_params/1,
         prepare_topology_change/6, start_topology_change/6,
         prepare_pause_bucket/3, pause_bucket/3,
         prepare_resume_bucket/4, resume_bucket/4,
         health_check/1, is_safe/2, get_label/1,
         get_defragmented_utilization/2,
         validate_bucket_config/3]).

-define(RPC_TIMEOUT,       ?get_timeout(rpc, 60000)).
-define(LONG_POLL_TIMEOUT, ?get_timeout(long_poll, 30000)).

shutdown(Pid) ->
    perform_service_manager_call(Pid, "Shutdown", empty_req()).

get_node_info(Pid) ->
    perform_service_manager_call(Pid, "GetNodeInfo", empty_req()).

get_task_list(Pid, Rev) ->
    perform_service_manager_call(Pid, "GetTaskList",
                                 get_req(Rev, ?LONG_POLL_TIMEOUT)).

get_params(Service) ->
    case perform_call(get_label(Service), "GetParams", empty_req(),
                      #{timeout => ?RPC_TIMEOUT}) of
        {ok, {Params}} ->
            {ok, Params};
        Other ->
            handle_result(Other)
    end.

cancel_task(Pid, Id, Rev) ->
    perform_service_manager_call(Pid, "CancelTask", cancel_task_req(Id, Rev)).

get_current_topology(Pid, Rev) ->
    perform_service_manager_call(Pid, "GetCurrentTopology",
                           get_req(Rev, ?LONG_POLL_TIMEOUT)).

prepare_topology_change(Pid, Id, Rev, Type, KeepNodes, EjectNodes) ->
    perform_service_manager_call(
      Pid, "PrepareTopologyChange",
      topology_change_req(Id, Rev, Type, KeepNodes, EjectNodes)).

start_topology_change(Pid, Id, Rev, Type, KeepNodes, EjectNodes) ->
    perform_service_manager_call(
      Pid, "StartTopologyChange",
      topology_change_req(Id, Rev, Type, KeepNodes, EjectNodes)).

prepare_pause_bucket(Pid, Id, Args) ->
    perform_service_manager_call(
      Pid, "PreparePause", pause_req(Id, Args)).

pause_bucket(Pid, Id, Args) ->
    perform_service_manager_call(
      Pid, "Pause", pause_req(Id, Args)).

prepare_resume_bucket(Pid, Id, Args, DryRun) ->
    perform_service_manager_call(
      Pid, "PrepareResume", resume_req(Id, Args, DryRun)).

resume_bucket(Pid, Id, Args, DryRun) ->
    perform_service_manager_call(
      Pid, "Resume", resume_req(Id, Args, DryRun)).

validate_bucket_config(Pid, BucketConfig, Options) ->
    perform_service_manager_call(Pid, "ValidateBucketConfig",
                                 {[{config, list_to_binary(BucketConfig)}] ++
                                      maps:to_list(Options)}).

health_check(Service) ->
    try perform_call(
          get_label(Service), "HealthCheck", empty_req(),
          #{silent => true, timeout => ?RPC_TIMEOUT}) of
        Result ->
            handle_result(Result)
    catch T:E ->
            ?log_debug("Exception while calling HealthCheck: ~p:~p", [T, E]),
            {error, case {T, E} of
                        {exit, {timeout, _}} ->
                            "HealthCheck call resulted in timeout";
                        {exit, {noproc, _}} ->
                            "Connection to the service is lost";
                        _ ->
                            "Unknown error"
                    end}
    end.

is_safe(Service, NodeIds) ->
    handle_result(perform_call(get_label(Service), "IsSafe", NodeIds,
                               #{timeout => ?RPC_TIMEOUT})).

get_defragmented_utilization(Service, Timeout) ->
    handle_result(perform_call(get_label(Service), "GetDefragmentedUtilization",
                               empty_req(), #{timeout => Timeout})).

get_label(Service) when is_atom(Service) ->
    atom_to_list(ns_ports_setup:get_rpc_prefix(Service)) ++ "-service_api".

%% internal
perform_service_manager_call(PidOrLabel, Name, Arg) ->
    case perform_call(PidOrLabel, Name, Arg, #{timeout => ?RPC_TIMEOUT}) of
        {error, Error} when is_binary(Error) ->
            {error, map_service_manager_error(Error)};
        {error, _} = Error ->
            Error;
        Other ->
            handle_oks(Other)
    end.

perform_call(PidOrLabel, Name, Arg, Opts) ->
    FullName = "ServiceAPI." ++ Name,
    json_rpc_connection:perform_call(PidOrLabel, FullName, Arg, Opts).

handle_oks({ok, null}) ->
    ok;
handle_oks({ok, _} = Result) ->
    Result.

handle_result({error, Error}) when is_binary(Error) ->
    {error, binary_to_list(Error)};
handle_result({error, Error}) ->
    {error, lists:flatten(io_lib:format("Unexpected error: ~p", [Error]))};
handle_result(Other) ->
    handle_oks(Other).

empty_req() ->
    {[]}.

get_req(Rev, Timeout) ->
    {[{rev, encode_rev(Rev)},
      {timeout, encode_timeout(Timeout)}]}.

cancel_task_req(Id, Rev) when is_binary(Id) ->
    {[{id, Id},
      {rev, encode_rev(Rev)}]}.

topology_change_req(Id, Rev, Type, KeepNodes, EjectNodes) ->
    true = is_binary(Id),

    {[{id, Id},
      {currentTopologyRev, encode_rev(Rev)},
      {type, encode_topology_change_type(Type)},
      {keepNodes, encode_keep_nodes(KeepNodes)},
      {ejectNodes, encode_eject_nodes(EjectNodes)}]}.

pause_req(Id,
          #bucket_hibernation_op_args{
             bucket = Bucket,
             remote_path = RemotePath,
             blob_storage_region = BlobStorageRegion,
             rate_limit = RateLimit}) ->
    true = is_binary(Id),
    true = is_integer(RateLimit),

    {[{id, Id},
      {bucket, list_to_binary(Bucket)},
      {remotePath, list_to_binary(RemotePath)},
      {blobStorageRegion, list_to_binary(BlobStorageRegion)},
      {rateLimit, RateLimit}]}.

resume_req(Id,
          #bucket_hibernation_op_args{
             bucket = Bucket,
             remote_path = RemotePath,
             blob_storage_region = BlobStorageRegion,
             rate_limit = RateLimit}, DryRun) ->
    true = is_binary(Id),
    true = is_boolean(DryRun),
    true = is_integer(RateLimit),

    {[{id, Id},
      {bucket, list_to_binary(Bucket)},
      {remotePath, list_to_binary(RemotePath)},
      {blobStorageRegion, list_to_binary(BlobStorageRegion)},
      {rateLimit, RateLimit},
      {dryRun, DryRun}]}.

encode_rev(undefined) ->
    null;
encode_rev(Rev) when is_binary(Rev) ->
    Rev.

encode_timeout(infinity) ->
    0;
encode_timeout(Timeout) when is_integer(Timeout) ->
    Timeout.

encode_recovery_type(full) ->
    ?RECOVERY_FULL;
encode_recovery_type(delta) ->
    ?RECOVERY_DELTA.

encode_topology_change_type(rebalance) ->
    ?TOPOLOGY_CHANGE_REBALANCE;
encode_topology_change_type(failover) ->
    ?TOPOLOGY_CHANGE_FAILOVER.

encode_node_info(Props) ->
    {_, Id} = lists:keyfind(node_id, 1, Props),
    {_, Priority} = lists:keyfind(priority, 1, Props),
    {_, Opaque} = lists:keyfind(opaque, 1, Props),

    {[{nodeId, Id},
      {priority, Priority},
      {opaque, Opaque}]}.

encode_keep_nodes(KeepNodes) ->
    lists:map(
      fun ({NodeInfo, RecoveryType}) ->
              {[{nodeInfo, encode_node_info(NodeInfo)},
                {recoveryType, encode_recovery_type(RecoveryType)}]}
      end, KeepNodes).

encode_eject_nodes(Nodes) ->
    [encode_node_info(N) || N <- Nodes].

map_service_manager_error(?ERROR_NOT_FOUND) ->
    not_found;
map_service_manager_error(?ERROR_CONFLICT) ->
    conflict;
map_service_manager_error(?ERROR_NOT_SUPPORTED) ->
    operation_not_supported;
map_service_manager_error(?ERROR_RECOVERY_IMPOSSIBLE) ->
    recovery_impossible;
map_service_manager_error(Error) -> {unknown_error, Error}.
