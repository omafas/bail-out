%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc implementation of fusion REST API's

-module(menelaus_web_fusion).

-include_lib("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-export([handle_get_settings/1,
         handle_post_settings/1,
         handle_get_status/1,
         handle_enable/1,
         handle_disable/1,
         handle_stop/1,
         handle_prepare_rebalance/1,
         handle_abort_prepared_rebalance/1,
         handle_upload_mounted_volumes/1,
         handle_get_active_guest_volumes/1,
         handle_diag_active_guest_volumes/1,
         handle_sync_log_store/1]).

settings() ->
    [{"logStoreURI", #{type => {uri, ["s3", "local"]},
                       cfg_key => [log_store_uri]}},
     {"enableSyncThresholdMB", #{type => {int, 100, 10 * 1024 * 1024},
                                 cfg_key => [enable_sync_threshold_mb]}}].

handle_get_settings(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),
    case fusion_uploaders:get_config() of
        not_found ->
            menelaus_util:reply_not_found(Req);
        Props ->
            menelaus_web_settings2:handle_get(
              [], settings(), undefined, Props, Req)
    end.

handle_post_settings(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),
    menelaus_web_settings2:handle_post(
      fun ([], Req1) ->
              validator:report_errors_for_one(
                Req1, [{'_', "Nothing to update"}], 400);
          (Params, Req1) ->
              KV = [{K, V} || {[K], V} <- Params],
              case fusion_uploaders:update_config(KV) of
                  {ok, _} ->
                      {AuditJson} = menelaus_web_settings2:prepare_json(
                                      [], settings(), undefined, KV),
                      ns_audit:fusion_settings(Req, AuditJson),
                      menelaus_util:reply_json(Req1, [], 200);
                  log_store_uri_locked ->
                      validator:report_errors_for_one(
                        Req1,
                        [{logStoreURI, "Cannot be updated after fusion was "
                          "enabled at least once"}], 400)
              end
      end, [], settings(), undefined, Req).

handle_get_status(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),

    KVNodes = ns_cluster_membership:service_active_nodes(kv),
    NodesDict = ns_doctor:get_nodes(),
    State = fusion_uploaders:get_state(),
    Json =
        case State of
            disabled ->
                {[{state, State}]};
            _ ->
                {[{state, State},
                  {nodes,
                   {[{Node, {jsonify_node(Node, NodesDict)}} ||
                        Node <- KVNodes]}}]}
        end,
    menelaus_util:reply_json(Req, Json, 200).

jsonify_node(Node, NodesDict) ->
    maybe
        {ok, NodeInfo} ?= dict:find(Node, NodesDict),
        {down, false} ?= {down, proplists:get_bool(down, NodeInfo)},
        {stale, false} ?= {stale, proplists:get_bool(stale, NodeInfo)},
        FS = proplists:get_value(fusion_stats, NodeInfo),
        {no_stats, false} ?= {no_stats, FS =:= undefined},

        [{buckets, jsonify_buckets_status(proplists:get_value(buckets, FS))},
         {deleting, proplists:get_value(deleting, FS)}]
    else
        error ->
            [];
        {down, true} ->
            [{state, down}];
        {stale, true} ->
            [{state, stale}];
        {no_stats, true} ->
            []
    end.

jsonify_buckets_status(Buckets) ->
    {lists:map(
       fun ({BucketName, Props}) ->
               JsonProps =
                   [{snapshotPendingBytes,
                     proplists:get_value(snapshot_pending_bytes, Props)},
                    {syncSessionCompletedBytes,
                     proplists:get_value(sync_session_completed_bytes, Props)},
                    {syncSessionTotalBytes,
                     proplists:get_value(sync_session_total_bytes, Props)}],
               {list_to_binary(BucketName),
                {[{N, P} || {N, P} <- JsonProps, P =/= undefined]}}
       end, Buckets)}.

bucket_error_to_str(unknown) ->
    "not found";
bucket_error_to_str(non_magma) ->
    "not a Magma bucket";
bucket_error_to_str(continuous_backup_enabled) ->
    "bucket with continuous backup enabled".

reply_wrong_buckets(Req, BucketErrors) ->
    validator:report_errors_for_one(
      Req,
      [{buckets,
        ["Fusion cannot be enabled on the following buckets: ",
         lists:join(", ",
                    [[BucketName, " - ", bucket_error_to_str(Reason)] ||
                        {BucketName, Reason} <- BucketErrors])]}], 400).

handle_enable(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Params) ->
              %% do it in orchestrator to prevent fusion state changes during
              %% rebalances
              Buckets = proplists:get_value(buckets, Params),
              case ns_orchestrator:enable_fusion(Buckets) of
                  ok ->
                      ns_audit:request_fusion_state(Req, enable, Buckets),
                      menelaus_util:reply_json(Req, [], 200);
                  {wrong_state, State, States} ->
                      reply_wrong_state(Req, State, States);
                  {wrong_buckets, BucketErrors} ->
                      reply_wrong_buckets(Req, BucketErrors);
                  not_initialized ->
                      menelaus_util:reply_text(
                        Req, "Fusion should be initialized", 503);
                  {not_allowed, Reason} ->
                      menelaus_util:reply_text(
                        Req, "Fusion cannot be enabled. Reason: " ++ Reason,
                        400);
                  Other ->
                      reply_other(Req, "enable fusion", Other)
              end
      end, Req, form,
      [validator:token_list(buckets, ",", _),
       validator:unsupported(_)]).

handle_disable(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),
    case ns_orchestrator:disable_fusion() of
        ok ->
            ns_audit:request_fusion_state(Req, disable, undefined),
            menelaus_util:reply_json(Req, [], 200);
        {wrong_state, State, States} ->
            reply_wrong_state(Req, State, States);
        Other ->
            reply_other(Req, "disable fusion", Other)
    end.

handle_stop(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),
    case ns_orchestrator:stop_fusion() of
        ok ->
            ns_audit:request_fusion_state(Req, stop, undefined),
            menelaus_util:reply_json(Req, [], 200);
        {wrong_state, State, States} ->
            reply_wrong_state(Req, State, States);
        Other ->
            reply_other(Req, "stop fusion", Other)
    end.

reply_wrong_state(Req, State, States) ->
    menelaus_util:reply_text(
      Req, io_lib:format(
             "Fusion should be in one of the following states: ~p"
             " Current state: ~p", [States, State]), 503).

-define(JANITOR_TIMEOUT, ?get_timeout(sync_log_store_janitor, 5000)).
-define(SYNC_TIMEOUT, ?get_timeout(sync_log_store_chronicle_sync, 60000)).

reply_other(Req, What, Other) ->
    case menelaus_web_cluster:busy_reply(What, Other) of
        {Code, Msg} ->
            menelaus_util:reply_text(Req, Msg, Code);
        undefined ->
            exit(Other)
    end.

handle_prepare_rebalance(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),
    NodeValidator =
        fun (String) ->
                try {value, list_to_existing_atom(String)}
                catch error:badarg -> {error, "Unknown node"} end
        end,
    validator:handle(
      fun (Params) ->
              KeepNodes = proplists:get_value(keepNodes, Params),
              case ns_orchestrator:prepare_fusion_rebalance(KeepNodes) of
                  {ok, AccelerationPlan} ->
                      ns_audit:prepare_fusion_rebalance(Req, KeepNodes),
                      menelaus_util:reply_json(Req, AccelerationPlan, 200);
                  {unknown_nodes, Nodes} ->
                      validator:report_errors_for_one(
                        Req,
                        [{keepNodes,
                          io_lib:format("Unknown nodes ~p", [Nodes])}], 400);
                  {failed_to_get_snapshot, Node} ->
                      menelaus_util:reply_text(
                        Req,
                        io_lib:format(
                          "Failed to obtain fusion storage snapshot from ~p",
                          [Node]), 500);
                  not_enabled ->
                      menelaus_util:reply_text(
                        Req, "Fusion is not enabled", 412);
                  Other ->
                      reply_other(Req, "prepare fusion rebalance", Other)
              end
      end, Req, form,
      [validator:required(keepNodes, _),
       validator:token_list(keepNodes, ",", NodeValidator, _),
       validator:unsupported(_)]).

handle_abort_prepared_rebalance(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),
    validator:handle(
      fun (Params) ->
              PlanUUID = proplists:get_value(planUUID, Params),
              case ns_orchestrator:abort_prepared_fusion_rebalance(PlanUUID) of
                  ok ->
                      ns_audit:abort_prepared_fusion_rebalance(Req, PlanUUID),
                      menelaus_util:reply_json(Req, [], 200);
                  not_found ->
                      menelaus_util:reply_text(Req, "Not Found", 404);
                  id_mismatch ->
                      validator:report_errors_for_one(
                        Req, [{planUUID, "Doesn't match stored plan id"}], 400);
                  Other ->
                      reply_other(Req, "abort_prepared_rebalance", Other)
              end
      end, Req, qs,
      [validator:string(planUUID, _),
       validator:required(planUUID, _),
       validator:unsupported(_)]).

validate_guest_volumes(Name, State) ->
    validator:json_array(Name,
                         [validator:required(name, _),
                          validator:string(name, _),
                          validator:required(guestVolumePaths, _),
                          validator:string_array(
                            guestVolumePaths, fun (_) -> ok end,
                            true, _),
                          validator:unsupported(_)],
                         State).

handle_upload_mounted_volumes(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),
    validator:handle(
      fun (Params) ->
              PlanUUID = proplists:get_value(planUUID, Params),
              validator:handle(
                fun (Props) ->
                        Nodes = [{proplists:get_value(name, P),
                                  proplists:get_value(guestVolumePaths, P)} ||
                                    {P} <- proplists:get_value(nodes, Props)],
                        case ns_orchestrator:fusion_upload_mounted_volumes(
                               PlanUUID, Nodes) of
                            not_found ->
                                menelaus_util:reply_text(Req, "Not Found", 404);
                            id_mismatch ->
                                validator:report_errors_for_one(
                                  Req,
                                  [{planUUID, "Doesn't match stored plan id"}],
                                  400);
                            {need_nodes, N} ->
                                validator:report_errors_for_one(
                                  Req,
                                  [{nodes,
                                    io_lib:format("Absent nodes ~p", [N])}],
                                  400);
                            {extra_nodes, N} ->
                                validator:report_errors_for_one(
                                  Req,
                                  [{nodes,
                                    io_lib:format("Unneeded nodes ~p", [N])}],
                                  400);
                            ok ->
                                ns_audit:upload_mounted_volumes(
                                  Req, PlanUUID, Nodes),
                                menelaus_util:reply_json(Req, [], 200);
                            Other ->
                                reply_other(Req, "upload mounted volumes", Other)
                        end
                end, Req, json,
                [validator:required(nodes, _),
                 validate_guest_volumes(nodes, _),
                 validator:unsupported(_)])
      end, Req, qs,
      [validator:string(planUUID, _),
       validator:required(planUUID, _),
       validator:unsupported(_)]).

handle_get_active_guest_volumes(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),
    {ok, List} =
        functools:sequence(
          [?cut(janitor_agent:get_active_guest_volumes(Bucket, BucketConfig))
           || {Bucket, BucketConfig} <- ns_bucket:get_fusion_buckets()]),
    ByNodes = lists:foldl(
                fun({N, Res}, Map) ->
                        maps:update_with(N, [Res | _], [Res], Map)
                end, #{}, lists:flatten(List)),
    ToReturn =
        [{N, lists:usort(lists:flatten(L))} || {N, L} <- maps:to_list(ByNodes)],
    menelaus_util:reply_json(Req, {ToReturn}, 200).

handle_diag_active_guest_volumes(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_totoro(),
    case lists:member(kv, ns_cluster_membership:node_services(
                            direct, node())) of
        false ->
            menelaus_util:reply_text(Req, "Node should be a kv node", 400);
        true ->
            RV =
                lists:uniq(
                  lists:flatmap(
                    fun ({Bucket, _}) ->
                            case ns_memcached:get_active_guest_volumes(
                                   Bucket) of
                                {ok, List} ->
                                    List;
                                Error ->
                                    ?log_error(
                                       "Error ~p getting active guest volumes "
                                       "for bucket ~p", [Error, Bucket]),
                                    []
                            end
                    end, ns_bucket:get_fusion_buckets())),
            menelaus_util:reply_json(Req, RV, 200)
    end.

handle_sync_log_store(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_79(),

    %% quorum read the latest bucket info. This doesn't guarantee
    %% that the api won't break if the buckets are changed during
    %% the execution of this code, but we can live with it
    %% since this is a test only api and the caller can take care
    %% of buckets not being modified in parallel
    ok = chronicle_kv:sync(kv, ?SYNC_TIMEOUT),
    BucketNames = [Name || {Name, _} <- ns_bucket:get_fusion_buckets()],
    %% run janitor for all fusion buckets to make sure that all
    %% uploaders are properly started
    ?log_debug("Ensure janitor runs for ~p.", [BucketNames]),
    RV =
        functools:sequence_(
          [?cut(ns_orchestrator:ensure_janitor_run({bucket, Bucket},
                                                   ?JANITOR_TIMEOUT)) ||
              Bucket <- BucketNames] ++
              %% quorum read whatever changes janitor might have made
              [?cut(chronicle_kv:sync(kv, ?SYNC_TIMEOUT)),
               ?cut(?log_debug("Synchronize fusion log store.")),
               ?cut(janitor_agent:sync_fusion_log_store(BucketNames))]),
    ?log_debug("Sync log store returned ~p", [RV]),
    case RV of
        ok ->
            menelaus_util:reply_json(Req, [], 200);
        {failed_nodes, Nodes} ->
            menelaus_util:reply_text(
              Req, io_lib:format("Fusion log store sync failed on "
                                 "following nodes: ~p", [Nodes]),
              400);
        Error ->
            menelaus_web_cluster:busy_reply("sync fusion log store", Error)
    end.
