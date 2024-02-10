%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc implementation of samples REST API's

-module(menelaus_web_samples).

-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([is_http/1,
         handle_get/1,
         handle_post/1]).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3]).

-define(COUCHDB_REQUIRED_SAMPLES, ["gamesim-sample", "beer-sample"]).
-define(SAMPLE_BUCKET_QUOTA_MB, 200).
-define(SAMPLE_BUCKET_QUOTA, 1024 * 1024 * ?SAMPLE_BUCKET_QUOTA_MB).

-record(sample, {sample_name :: string(),
                 bucket_name :: string() | undefined,
                 http_cache_directory :: string() | undefined,
                 must_bucket_exist :: bucket_must_exist |
                                      bucket_must_not_exist}).

handle_get(Req) ->
    Buckets = [Bucket || {Bucket, _} <- ns_bucket:get_buckets()],

    Map = [ begin
                Name = filename:basename(Path, ".zip"),
                Installed = lists:member(Name, Buckets),
                {[{name, list_to_binary(Name)},
                  {installed, Installed},
                  {quotaNeeded, ?SAMPLE_BUCKET_QUOTA}]}
            end || Path <- list_sample_files() ],

    reply_json(Req, Map).

-spec none_use_couchdb(Samples :: [{binary(), binary()}]) -> boolean().
none_use_couchdb(Samples) ->
    lists:all(
      fun (#sample{sample_name = Sample}) ->
              samples_without_couchdb(Sample)
      end, Samples).

-spec samples_without_couchdb(Name :: string()) -> Return :: boolean().
samples_without_couchdb(Name) when is_list(Name) ->
    not lists:member(string:trim(Name), ?COUCHDB_REQUIRED_SAMPLES).

build_samples_input_list(Samples) ->
    lists:foldl(
      fun (Sample, AccIn) ->
              SampleName = proplists:get_value(sample, Sample),
              {BucketMustExist, BucketName} =
                  case proplists:get_value(bucket, Sample) of
                      undefined -> {bucket_must_not_exist, SampleName};
                      Name -> {bucket_must_exist, Name}
                  end,
              CacheDir = proplists:get_value(http_cache_directory, Sample),
              [#sample{sample_name = SampleName,
                       bucket_name = BucketName,
                       http_cache_directory = CacheDir,
                       must_bucket_exist = BucketMustExist} | AccIn]
      end, [], Samples).

%% There are three types of input to this request. The classic/original input
%% is a list of  names (e.g. ["travel-sample", "beer-sample"]) where the
%% data is found is <name>.zip and the bucket of name <name> does not
%% already exist and will be created and loaded.
%%
%% The second input is a list of json objects where each object is of the form
%% {
%%      "sample": <sample-name>,
%%      "bucket": <bucket-name>
%% }
%% <sample-name>.zip contains the data and <bucket-name> is the destination
%% bucket (which must already exist). The "bucket" property is optional, to
%% allow future deprecation of the first input type.
%%
%% The third input is a list of json objects like the second, but with one
%% extra property:
%% {
%%      "sample": <sample-name>,
%%      "bucket": <bucket-name>,
%%      "http_cache_directory": <cache-dir>
%% }
%% In this case, <sample-name> is a http(s):// address for the sample and
%% <bucket-name> is the same as in the second input, although in this case it is
%% required. The "http_cache_directory" property is a required argument for
%% cbimport.
%%
%% The three types of input are normalized into a list of records to facilitate
%% common handling, with <must-bucket-exist> depending on whether the
%% <bucket-name> was specified.
%% [#sample{<sample-name>, <bucket-name>, <http-cache-directory>,
%%          <must_bucket_exist>}]}
%%
%% To validate each json object separately, the json_array validation handler
%% is used. In order to extract the sample name in the first input type,
%% extract_internal is required, as the root of each json sub-document does not
%% have a key, and so is stored in the {internal, root} key.

%% is_http returns a boolean indicating whether the given string represents a
%% URL (i.e. has the prefix 'http(s)://').
is_http("http://" ++ _) -> true;
is_http("https://" ++ _) -> true;
is_http(_) -> false.

remote_sample_validators() ->
    [validator:required(bucket, _),
     validator:string(http_cache_directory, _),
     validator:required(http_cache_directory, _)].

%% As the validators to be used depends on one of the parameters, we cannot
%% simply append the above validators when applicable, instead they must be
%% applied within this wrapper validator, which only applies them when the
%% sample is a remote address
validate_remote_sample(State) ->
    case is_http(validator:get_value(sample, State)) of
        true ->
            functools:chain(State, remote_sample_validators());
        false ->
            State
    end.

post_validators() ->
    [validator:extract_internal(root, sample, _),
     validator:required(sample, _),
     validator:string(sample, _),
     validator:string(bucket, _),
     validate_remote_sample(_),
     validator:unsupported(_)].

handle_post(Req) ->
    menelaus_web_rbac:assert_no_users_upgrade(),
    validator:handle(handle_post_inner(Req, _), Req, json_array,
                     post_validators()).

handle_post_inner(Req, ParsedJson) ->
    Samples = build_samples_input_list(ParsedJson),
    case config_profile:get_bool({couchdb, disabled}) of
        true ->
            case none_use_couchdb(Samples) of
                true ->
                    process_post(Req, Samples);
                false ->
                    SampleNames =
                        lists:map(
                          fun (FullPath) ->
                                  list_to_binary(
                                    filename:basename(FullPath, ".zip"))
                          end, list_sample_files()),
                    Err =
                        iolist_to_binary(
                          io_lib:format(
                            "Attempted to load invalid samples for current "
                            "configuration profile. Attempted: [~s], "
                            "Valid: [~s]",
                            [lists:join(",",
                                        lists:map(?cut(_#sample.sample_name),
                                                  Samples)),
                             lists:join(",", SampleNames)])),
                    reply_json(Req, Err, 400)
            end;
        false ->
            process_post(Req, Samples)
    end.

process_post(Req, Samples) ->
    case ns_orchestrator:ensure_janitor_run(services) of
        ok ->
            ok;
        _ ->
            menelaus_util:web_exception(
              503, "System services have not completed startup. "
              "Please try again shortly.")
    end,

    {ResponsesBody, Code} = case validate_post_sample_buckets(Samples) of
                                ok ->
                                    {start_loading_samples(Req, Samples), 202};
                                X1 ->
                                    {[Msg || {error, Msg} <- X1], 400}
                            end,
    Responses = get_response_json(ResponsesBody, Code),

    reply_json(Req, Responses, Code).

-spec get_response_json([binary()] | [{[{atom(), binary()}]}], integer()) ->
          {[{tasks, [{atom(), binary()}]}] | [{errors, [binary()]}]} | [binary()].
get_response_json(ResponseBody, StatusCode) ->
    case {cluster_compat_mode:is_cluster_76(), StatusCode} of
        {true, 202} ->
            {[{tasks, ResponseBody}]};
        {true, 400} ->
            {[{errors, ResponseBody}]};
        {false, 202} ->
            [];
        {false, 400} ->
            ResponseBody
    end.

start_loading_samples(Req, Samples) ->
    lists:map(
      fun (Sample) ->
              start_loading_sample(Req, Sample)
      end, Samples).

start_loading_sample(Req, #sample{sample_name = Sample, bucket_name = Bucket,
                                  http_cache_directory = CacheDir,
                                  must_bucket_exist = BucketState}) ->
    {TaskState, TaskId} = samples_loader_tasks:start_loading_sample(
                            Sample, Bucket, ?SAMPLE_BUCKET_QUOTA_MB, CacheDir,
                            BucketState),
    case TaskState of
        newly_started ->
            ns_audit:start_loading_sample(Req, Bucket);
        already_started ->
            ok
    end,
    {[{taskId, TaskId},
      {sample, list_to_binary(Sample)},
      {bucket, list_to_binary(Bucket)}]}.

list_sample_files() ->
    BinDir = path_config:component_path(bin),
    AllSamples = filelib:wildcard(
                   filename:join([BinDir, "..", "samples", "*.zip"])),
    case config_profile:get_bool({couchdb, disabled}) of
        true ->
            lists:filter(
              ?cut(samples_without_couchdb(
                     filename:basename(_, ".zip"))), AllSamples);
        false ->
            AllSamples
    end.

sample_exists(Name) ->
    BinDir = path_config:component_path(bin),
    filelib:is_file(filename:join([BinDir, "..", "samples", Name ++ ".zip"])).

validate_post_sample_buckets(Samples) ->
    case check_valid_samples(Samples) of
        ok ->
            check_quota(Samples);
        X ->
            X
    end.

check_quota(Samples) ->
    {ExistingBuckets, BucketsToCreate} = lists:partition(
        ?cut(_#sample.must_bucket_exist =:= bucket_must_exist), Samples),
    Config = ns_config:get(),
    Snapshot =
        chronicle_compat:get_snapshot(
          [ns_bucket:fetch_snapshot(all, _, [props]),
           ns_cluster_membership:fetch_snapshot(_)], #{ns_config => Config}),

    NodesCount =
        length(ns_cluster_membership:service_active_nodes(Snapshot, kv)),
    StorageInfo = ns_storage_conf:cluster_storage_info(Config, Snapshot),
    RamQuotas = proplists:get_value(ram, StorageInfo),
    QuotaUsed = proplists:get_value(quotaUsed, RamQuotas),
    QuotaTotal = proplists:get_value(quotaTotal, RamQuotas),
    Required = ?SAMPLE_BUCKET_QUOTA * length(BucketsToCreate),

    case (QuotaTotal - QuotaUsed) <  (Required * NodesCount) of
        true ->
            Err = ["Not enough Quota, you need to allocate ", format_MB(Required),
                   " to install sample buckets"],
            [{error, list_to_binary(Err)}];
        false ->
            case lists:flatmap(check_bucket_quota(_), ExistingBuckets) of
                [] -> ok;
                Errs -> Errs
            end
    end.

%% Check an existing bucket's ram quota is sufficient to import a sample into
check_bucket_quota(#sample{bucket_name = Bucket}) ->
    {ok, BucketCfg} = ns_bucket:get_bucket(Bucket),
    case ns_bucket:raw_ram_quota(BucketCfg) < ?SAMPLE_BUCKET_QUOTA of
        true ->
            Err = ["Not enough Quota, you need to allocate ",
                   format_MB(?SAMPLE_BUCKET_QUOTA), " for bucket '", Bucket,
                   "' to install sample buckets"],
            [{error, list_to_binary(Err)}];
        false ->
            []
    end.

check_sample_exists(Sample) ->
    case is_http(Sample) of
        true ->
            %% We should let cbimport handle this
            ok;
        false ->
            case sample_exists(Sample) of
                false ->
                    Err2 = ["Sample ", Sample, " is not a valid sample."],
                    {error, list_to_binary(Err2)};
                true -> ok
            end
    end.

check_valid_samples(Samples) ->
    Errors =
        lists:foldl(
          fun (#sample{sample_name = Sample, bucket_name = Sample,
                       must_bucket_exist = bucket_must_not_exist}, AccIn) ->
                  %% Classic case where data is loaded into non-existent
                  %% bucket with the same name as the sample data.
                  RV =
                    case ns_bucket:name_conflict(Sample) of
                        true ->
                            Err1 = ["Sample bucket ", Sample,
                                    " is already loaded."],
                            {error, list_to_binary(Err1)};
                        false ->
                            check_sample_exists(Sample)
                    end,
                    [RV | AccIn];
              (#sample{sample_name = Sample, bucket_name = Bucket,
                       must_bucket_exist = bucket_must_exist}, AccIn) ->
                  %% Newer case where the bucket must already exist.
                  RV =
                    case ns_bucket:name_conflict(Bucket) of
                        false ->
                            Err1 =
                                ["Sample bucket ", Bucket,
                                 " must already exist and will be loaded "
                                 "with the sample data."],
                            {error, list_to_binary(Err1)};
                        true ->
                            check_sample_exists(Sample)
                    end,
                  [RV | AccIn]
          end, [], Samples),

    case [X || X <- Errors, X =/= ok] of
        [] ->
            ok;
        X ->
            X
    end.

format_MB(X) ->
    integer_to_list(misc:ceiling(X / 1024 / 1024)) ++ "MB".

-ifdef(TEST).
check_quota_modules() ->
    [ns_config, chronicle_compat, ns_cluster_membership, ns_storage_conf,
     ns_bucket].

check_quota_setup() ->
    meck:new(check_quota_modules(), [passthrough]),
    meck:expect(ns_config, get,
                fun () -> [] end),
    meck:expect(chronicle_compat, get_snapshot,
                fun (_, _) -> [] end),
    meck:expect(ns_cluster_membership, service_active_nodes,
                fun (_, _) -> [node] end).

check_quota_teardown() ->
    meck:unload(check_quota_modules()).

check_quota_test_() ->
    {setup,
     ?cut(check_quota_setup()),
     fun (_) -> check_quota_teardown() end,
     {"check quota test", ?cut(check_quota_test__())}}.

check_quota_test__() ->
    %% Insufficient ram quota when creating sample bucket
    meck:expect(ns_storage_conf, cluster_storage_info,
                fun (_, _) ->
                        [{ram, [{quotaUsed, 0},
                                {quotaTotal, ?SAMPLE_BUCKET_QUOTA-1}]}]
                end),
    Samples1 = [#sample{bucket_name = "test",
                        must_bucket_exist = bucket_must_not_exist}],
    Errs1 = check_quota(Samples1),
    ?assertMatch([{error, _}], Errs1),

    %% Insufficient ram quota when installing to existing sample bucket
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, [{ram_quota, ?SAMPLE_BUCKET_QUOTA-1},
                              {servers, [node]}]}
                end),
    Samples2 = [#sample{bucket_name = "test",
                        must_bucket_exist = bucket_must_exist}],
    Errs2 = check_quota(Samples2),
    ?assertMatch([{error, _}], Errs2),

    %% Insufficient ram quota when installing to existing sample bucket with
    %% 2 nodes
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, [{ram_quota, ?SAMPLE_BUCKET_QUOTA-1},
                              {servers, [node1, node2]}]}
                end),
    Samples3 = [#sample{bucket_name = "test",
                        must_bucket_exist = bucket_must_exist}],
    Errs3 = check_quota(Samples3),
    ?assertMatch([{error, _}], Errs3),

    %% Sufficient ram quota when creating sample bucket
    meck:expect(ns_storage_conf, cluster_storage_info,
                fun (_, _) ->
                        [{ram, [{quotaUsed, 0},
                                {quotaTotal, ?SAMPLE_BUCKET_QUOTA}]}]
                end),
    Samples4 = [#sample{bucket_name = "test",
                        must_bucket_exist = bucket_must_not_exist}],
    Errs4 = check_quota(Samples4),
    ?assertMatch(ok, Errs4),

    %% Sufficient ram quota when installing to existing sample bucket
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, [{ram_quota, ?SAMPLE_BUCKET_QUOTA},
                              {servers, [node]}]}
                end),
    Samples5 = [#sample{bucket_name = "test",
                        must_bucket_exist = bucket_must_exist}],
    Errs5 = check_quota(Samples5),
    ?assertMatch(ok, Errs5),

    %% Sufficient ram quota when installing to existing sample bucket with 2
    %% nodes
    meck:expect(ns_bucket, get_bucket,
                fun (_) ->
                        {ok, [{ram_quota, ?SAMPLE_BUCKET_QUOTA},
                              {servers, [node1, node2]}]}
                end),
    Samples6 = [#sample{bucket_name = "test",
                        must_bucket_exist = bucket_must_exist}],
    Errs6 = check_quota(Samples6),
    ?assertMatch(ok, Errs6).

get_response_json_modules() ->
    [cluster_compat_mode].

get_response_json_setup() ->
    meck:new(get_response_json_modules(), [passthrough]).

get_response_json_teardown() ->
    meck:unload(get_response_json_modules()).

get_response_json_test_() ->
    {setup,
     ?cut(get_response_json_setup()),
     fun (_) -> get_response_json_teardown() end,
     {"get_response_json", ?cut(get_response_json_test__())}}.

assert_response_json(Code, ResponseBody, ExpectedJSON) ->
    ResponseJSON = get_response_json(ResponseBody, Code),
    ?assertEqual(ExpectedJSON, ResponseJSON),
    %% Confirm that the response can be converted to json
    ejson:encode(ResponseJSON).

get_response_json_test__() ->
    %% Mixed cluster status 202
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> false end),
    assert_response_json(202,
                         {[{taskId, <<"test">>},
                           {sample, <<"travel-sample">>},
                           {bucket, <<"bucket">>}]},
                         []),

    %% Mixed cluster status 400
    assert_response_json(400, [<<"error">>], [<<"error">>]),

    %% 7.6 cluster status 202
    meck:expect(cluster_compat_mode, is_cluster_76,
                fun () -> true end),
    assert_response_json(202,
                         {[{taskId, <<"test">>},
                           {sample, <<"travel-sample">>},
                           {bucket, <<"bucket">>}]},
                         {[{tasks, {[{taskId, <<"test">>},
                                     {sample, <<"travel-sample">>},
                                     {bucket, <<"bucket">>}]}}]}),

    %% 7.6 cluster status 400
    assert_response_json(400, [<<"error">>], {[{errors, [<<"error">>]}]}).
-endif.
