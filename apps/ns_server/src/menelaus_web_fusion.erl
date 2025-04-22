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
         handle_prepare_rebalance/1,
         handle_upload_mounted_volumes/1,
         handle_get_active_guest_volumes/1,
         handle_sync_log_store/1]).

settings() ->
    [{"logStoreURI", #{type => {uri, ["s3", "local"]},
                       cfg_key => [log_store_uri]}},
     {"enableSyncThresholdMB", #{type => {int, 100, 100 * 1024},
                                 cfg_key => [enable_sync_threshold_mb]}}].

handle_get_settings(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_morpheus(),
    case fusion_uploaders:get_config() of
        not_found ->
            menelaus_util:reply_not_found(Req);
        Props ->
            menelaus_web_settings2:handle_get(
              [], settings(), undefined, Props, Req)
    end.

handle_post_settings(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_morpheus(),
    menelaus_web_settings2:handle_post(
      fun ([], Req1) ->
              validator:report_errors_for_one(
                Req1, [{'_', "Nothing to update"}], 400);
          (Params, Req1) ->
              case fusion_uploaders:update_config(
                     [{K, V} || {[K], V} <- Params]) of
                  {ok, _} ->
                      menelaus_util:reply_json(Req1, [], 200);
                  log_store_uri_locked ->
                      validator:report_errors_for_one(
                        Req1,
                        [{logStoreURI, "Cannot be updated after fusion was "
                          "enabled at least once"}], 400)
              end
      end, [], settings(), undefined, Req).

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
    menelaus_util:assert_is_morpheus(),
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
                      menelaus_util:reply_json(Req, AccelerationPlan, 200);
                  {unknown_nodes, Nodes} ->
                      validator:report_errors_for_one(
                        Req,
                        [{keepNodes,
                          io_lib:format("Unknown nodes ~p", [Nodes])}], 400);
                  Other ->
                      reply_other(Req, "prepare fusion rebalance", Other)
              end
      end, Req, form,
      [validator:required(keepNodes, _),
       validator:token_list(keepNodes, ",", NodeValidator, _),
       validator:unsupported(_)]).

validate_guest_volumes(Name, State) ->
    validator:json_array(Name,
                         [validator:required(name, _),
                          validator:string(name, _),
                          validator:required(guestVolumePaths, _),
                          validator:string_array(
                            guestVolumePaths, fun (_) -> ok end,
                            false, _),
                          validator:unsupported(_)],
                         State).

handle_upload_mounted_volumes(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_morpheus(),
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
    menelaus_util:assert_is_morpheus(),
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

handle_sync_log_store(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_morpheus(),

    %% quorum read the latest bucket info. This doesn't guarantee
    %% that the api won't break if the buckets are changed during
    %% the execution of this code, but we can live with it
    %% since this is a test only api and the caller can take care
    %% of buckets not being modified in parallel
    ok = chronicle_kv:sync(kv, ?SYNC_TIMEOUT),
    BucketNames = [Name || {Name, _} <- ns_bucket:get_fusion_buckets()],
    %% run janitor for all fusion buckets to make sure that all
    %% uploaders are properly started
    RV =
        functools:sequence_(
          [?cut(ns_orchestrator:ensure_janitor_run({bucket, Bucket},
                                                   ?JANITOR_TIMEOUT)) ||
              Bucket <- BucketNames] ++
              %% quorum read whatever changes janitor might have made
              [?cut(chronicle_kv:sync(kv, ?SYNC_TIMEOUT)),
               ?cut(janitor_agent:sync_fusion_log_store(BucketNames))]),
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
