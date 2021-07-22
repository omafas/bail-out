%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2019 Couchbase, Inc.
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

%% @doc implementation of samples REST API's

-module(menelaus_web_samples).

-include("ns_common.hrl").

-export([handle_get/1,
         handle_post/1]).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3]).

-define(SAMPLES_LOADING_TIMEOUT, 120000).
-define(SAMPLE_BUCKET_QUOTA_MB, 100).
-define(SAMPLE_BUCKET_QUOTA, 1024 * 1024 * ?SAMPLE_BUCKET_QUOTA_MB).

handle_get(Req) ->
    Buckets = [Bucket || {Bucket, _} <- ns_bucket:get_buckets(ns_config:get())],

    Map = [ begin
                Name = filename:basename(Path, ".zip"),
                Installed = lists:member(Name, Buckets),
                {struct, [{name, list_to_binary(Name)},
                          {installed, Installed},
                          {quotaNeeded, ?SAMPLE_BUCKET_QUOTA}]}
            end || Path <- list_sample_files() ],

    reply_json(Req, Map).

handle_post(Req) ->
    menelaus_web_rbac:assert_no_users_upgrade(),
    Samples = mochijson2:decode(mochiweb_request:recv_body(Req)),

    case ns_orchestrator:ensure_janitor_run(services) of
        ok ->
            ok;
        _ ->
            throw({web_exception, 503,
                   <<"System services have not completed startup. "
                     "Please try again shortly.">>,
                   []})
    end,

    Errors = case validate_post_sample_buckets(Samples) of
                 ok ->
                     start_loading_samples(Req, Samples);
                 X1 ->
                     X1
             end,


    case Errors of
        ok ->
            reply_json(Req, [], 202);
        X2 ->
            reply_json(Req, [Msg || {error, Msg} <- X2], 400)
    end.

start_loading_samples(Req, Samples) ->
    lists:foreach(fun (Sample) ->
                          start_loading_sample(Req, binary_to_list(Sample))
                  end, Samples).

start_loading_sample(Req, Name) ->
    case samples_loader_tasks:start_loading_sample(Name,
                                                   ?SAMPLE_BUCKET_QUOTA_MB) of
        ok ->
            ns_audit:start_loading_sample(Req, Name);
        already_started ->
            ok
    end.

list_sample_files() ->
    BinDir = path_config:component_path(bin),
    filelib:wildcard(filename:join([BinDir, "..", "samples", "*.zip"])).


sample_exists(Name) ->
    BinDir = path_config:component_path(bin),
    filelib:is_file(filename:join([BinDir, "..", "samples", binary_to_list(Name) ++ ".zip"])).

validate_post_sample_buckets(Samples) ->
    case check_valid_samples(Samples) of
        ok ->
            check_quota(Samples);
        X ->
            X
    end.

check_quota(Samples) ->
    Config = ns_config:get(),
    NodesCount = length(ns_cluster_membership:service_active_nodes(Config, kv)),
    StorageInfo = ns_storage_conf:cluster_storage_info(Config),
    RamQuotas = proplists:get_value(ram, StorageInfo),
    QuotaUsed = proplists:get_value(quotaUsed, RamQuotas),
    QuotaTotal = proplists:get_value(quotaTotal, RamQuotas),
    Required = ?SAMPLE_BUCKET_QUOTA * erlang:length(Samples),

    case (QuotaTotal - QuotaUsed) <  (Required * NodesCount) of
        true ->
            Err = ["Not enough Quota, you need to allocate ", format_MB(Required),
                   " to install sample buckets"],
            [{error, list_to_binary(Err)}];
        false ->
            ok
    end.


check_valid_samples(Samples) ->
    Errors = [begin
                  case ns_bucket:name_conflict(binary_to_list(Name)) of
                      true ->
                          Err1 = ["Sample bucket ", Name, " is already loaded."],
                          {error, list_to_binary(Err1)};
                      _ ->
                          case sample_exists(Name) of
                              false ->
                                  Err2 = ["Sample ", Name, " is not a valid sample."],
                                  {error, list_to_binary(Err2)};
                              _ -> ok
                          end
                  end
              end || Name <- Samples],
    case [X || X <- Errors, X =/= ok] of
        [] ->
            ok;
        X ->
            X
    end.

format_MB(X) ->
    integer_to_list(misc:ceiling(X / 1024 / 1024)) ++ "MB".
