%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Monitor and maintain the vbucket layout of each bucket.
%% There is one of these per bucket.
%%
%% @doc code for calculating fusion uploaders during the rebalance
%%

-module(fusion_uploaders).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-ifdef(TEST).
-include("ns_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([build_fast_forward_info/5,
         build_initial/1,
         get_moves/1,
         get_current/1,
         fail_nodes/2,
         get_config/0,
         get_state/0,
         get_state/1,
         get_log_store_uri/0,
         get_metadata_store_uri/0,
         update_config/1,
         advance_terms/1,
         command/1,
         maybe_grab_heartbeat_info/0,
         maybe_advance_state/0,
         config_key/0,
         create_snapshot_uuid/2,
         get_stored_snapshot_uuids/0,
         store_snapshots_uuid/3,
         get_snapshots/5,
         cleanup_snapshots/0,
         cleanup_mounted_volumes/2]).

%% used via rpc:call
-export([do_get_snapshots/4, do_delete_snapshots/3]).

-define(GET_DELETION_STATE_TIMEOUT, ?get_timeout(get_deletion_state, 5000)).
-define(GET_SNAPSHOTS_TIMEOUT, ?get_timeout(get_snapshots, 30000)).
-define(DELETE_SNAPSHOTS_TIMEOUT, ?get_timeout(delete_snapshots, 30000)).

-define(LOG_BUCKET_STATE(Bucket, State),
        begin
            ns_server_stats:notify_gauge(
              {fusion_bucket_state_timestamp_seconds,
               [{bucket, Bucket}, {state, State}]}, os:system_time(seconds)),
            ale:info(?USER_LOGGER, "Bucket ~p fusion state changed to ~p.",
                     [Bucket, State])
        end).

-define(LOG_FUSION_STATE(State),
        begin
            ns_server_stats:notify_gauge(
              {fusion_state_timestamp_seconds, [{state, State}]},
              os:system_time(seconds)),
            ale:info(?USER_LOGGER, "Fusion state changed to ~p", [State])
        end).

%% incremented starting from 1 with each uploader change
%% The purpose of Term is to help
%% s3 to recognize rogue uploaders and ignore them.
-type inc_term() :: pos_integer().
-type uploader() :: {node() | undefined, inc_term()}.
%% fusion uploaders map, one item per vbucket
-type uploaders() :: [uploader()].
-type move() :: same | uploader().
-type moves() :: [move()].
-type fast_forward_info() :: {moves(), uploaders()}.
-type state() ::
        disabled | disabling | enabled | enabling | stopped | stopping.
-type bucket_state() ::
        disabled | disabling | enabled | stopped | stopping.
-type wrong_state_error() :: {wrong_state, state(), [state()]}.
-type bucket_validation_error() ::
        unknown | non_magma | continuous_backup_enabled.
-type enable_error() ::
        not_initialized | {not_allowed, string()} | wrong_state_error() |
        {failed_nodes, [node()]} |
        {wrong_buckets, [{ns_bucket:name(), bucket_validation_error()}]}.
-type disable_or_stop_error() :: wrong_state_error().

-export_type([fast_forward_info/0, uploaders/0, enable_error/0,
              disable_or_stop_error/0, bucket_state/0, state/0]).

-spec build_fast_forward_info(ns_bucket:name(), ns_bucket:config(),
                              vbucket_map(), vbucket_map(), integer()) ->
          undefined | fast_forward_info().
build_fast_forward_info(Bucket, BucketConfig, Map, FastForwardMap, NServers) ->
    case ns_bucket:get_fusion_state(BucketConfig) of
        enabled ->
            Current = ns_bucket:get_fusion_uploaders(Bucket),
            Moves = calculate_moves(
                      Bucket, Map, FastForwardMap, Current,
                      allowance(Map, NServers)),
            ?rebalance_info("Calculated fusion uploader moves. Moves:~n~p",
                            [Moves]),
            {Moves, Current};
        _ ->
            undefined
    end.

allowance(Map, NServers) ->
    length(Map) div NServers + 1.

-spec build_initial(vbucket_map()) -> uploaders().
build_initial(VBucketMap) ->
    [{N, 1} || [N | _] <- VBucketMap].

-spec get_moves(fast_forward_info()) -> moves().
get_moves({Moves, _}) ->
    Moves.

-spec get_current(fast_forward_info()) -> uploaders().
get_current({_, Current}) ->
    Current.

%% uploader starts uploading from scratch if it is moved to a
%% node that was not filled from s3, so basically to any node that
%% is not a current uploader and is present in old chain
%%
%% we calculate uploader moves doing the best effort to minimize
%% the number of uploaders started from scratch and distribute
%% uploaders evenly between nodes
%%
%% parameter Allowance restricts how many uploaders can be
%% started from each node thus defining how much unbalance
%% we are ready to tolerate for the sake of not uploading from
%% scratch
%%
%% The algorithm works as such:
%% 1. Sort the candidates so the candidates with less choice are
%%    processed earlier
%% 2. Move through the sorted list of vbuckets and out of the array
%%    of candidates for vbucket choose the one with lesser usage.
%%    After the choice is made, increment the usage for the chosen node.
%% 3. If the node with less usage still had reached the allowance, traverse
%%    the list of vbuckets from the beginning, reconsidering earlier made
%%    choices with the node of interest being excluded.
%% 4. If some choice had changed, adjust the nodes usage accordingly
%%    and return at the position in the list where uploader is
%%    still not chosen. (back to #2)
%% 5. If it is not possible to free the usage for a node with least
%%    usage, allow the node still to be chosen as an uploader
%%    exceeding the allowance, thus resulting in unbalanced map.
calculate_moves(Bucket, Map, FastForwardMap, CurrentUploaders, Allowance) ->
    ?log_debug("Calculate moves for bucket ~p~nmap:~n~p~nffmap:~n~p~nuploaders"
               "~n~p~nallowance:~p",
               [Bucket, Map, FastForwardMap, CurrentUploaders, Allowance]),
    Zipped = lists:zip3(Map, FastForwardMap, [N || {N, _} <- CurrentUploaders]),
    build_uploaders(Bucket, Zipped, CurrentUploaders, Allowance, moves).

candidates({OldChain, NewChain, UploaderNode}) ->
    NewChainNoUndefineds = mb_map:only_defined(NewChain),
    NotFromScratch = NewChainNoUndefineds --
        lists:delete(UploaderNode, OldChain),
    FromScratch = NewChainNoUndefineds -- NotFromScratch,
    Choices = case NotFromScratch of
                  [] ->
                      FromScratch;
                  _ ->
                      NotFromScratch
              end,
    {[Choices], length(Choices)};
candidates({NodesWithUploadedData, Chain}) ->
    FromScratch = Chain -- [N || {N, _, _} <- NodesWithUploadedData],
    %% each node with data is a list of one here, because we want
    %% the term and seqno to prevail over usage during the uploader
    %% selection
    NotFromScratch =
        [[N] || {N, _, _} <- lists:sort(
                               fun ({_, TermA, SeqnoA}, {_, TermB, SeqnoB}) ->
                                       {TermA, SeqnoA} > {TermB, SeqnoB}
                               end, NodesWithUploadedData)],
    Choices = case NotFromScratch of
                  [] ->
                      length(FromScratch);
                  _ ->
                      length(NotFromScratch)
              end,
    {NotFromScratch ++ [FromScratch], Choices}.

select_uploader(Bucket, VBucket, Candidates, {CurrentUploader, Term} = CU,
                Usage, Allowance) ->
    case do_select_uploader(Candidates, Usage, Allowance) of
        {ok, Uploader} ->
            NewUsage = maps:update_with(Uploader, _ + 1, 1, Usage),
            ?log_debug("Selected uploader ~p for bucket ~p, vbucket ~p "
                       "from ~p, current uploader: ~p, usage ~p",
                       [Uploader, Bucket, VBucket, Candidates, CU,
                        maps:get(Uploader, NewUsage)]),
            NewUploaderTerm =
                case Uploader of
                    CurrentUploader ->
                        CU;
                    _ ->
                        {Uploader, Term + 1}
                end,
            {ok, NewUploaderTerm, NewUsage};
        undefined ->
            undefined
    end.

do_select_uploader([], _Usage, _Allowance) ->
    undefined;
do_select_uploader([Candidates | Rest], Usage, Allowance) ->
    Allowed =
        case Allowance of
            undefined ->
                Candidates;
            _ ->
                lists:filter(?cut(maps:get(_, Usage, 0) < Allowance),
                             Candidates)
        end,
    case Allowed of
        [] ->
            do_select_uploader(Rest, Usage, Allowance);
        _ ->
            {_, Winner} =
                lists:min([{maps:get(N, Usage, 0), N} || N <- Allowed]),
            {ok, Winner}
    end.

build_uploaders(Bucket, Infos, CurrentUploaders, Allowance, OutputFormat) ->
    ?log_debug("Building uploaders for bucket ~p~ninfos:~n~p~n"
               "uploaders:~n~p~nallowance:~p,format:~p",
               [Bucket, Infos, CurrentUploaders, Allowance, OutputFormat]),
    CandidatesList = lists:map(fun candidates/1, Infos),

    %% zip together vbucket numbers, candidates and current uploaders
    %% so the info can be processed for each vbucket
    Zipped = misc:enumerate(lists:zip(CandidatesList, CurrentUploaders), 0),

    %% the algorithm processes the vbuckets with the least number
    %% of uploader candidates first in order to have more choice
    %% at the end when Usage approaches Allowance
    Sorted = lists:sort(
               fun ({_, {{_, ChoicesA}, _}}, {_, {{_, ChoicesB}, _}}) ->
                       ChoicesA < ChoicesB
               end, Zipped),
    ?log_debug("Process following candidates for bucket ~p:~n~p",
               [Bucket, Sorted]),

    {WithUploaders, FinalUsage} =
        process_candidates(Bucket, Allowance, #{}, Sorted, []),

    ?log_debug("Selected following uploaders for bucket ~p:~n~p~nUsage:~n~p",
               [Bucket, [{I, U} || {I, _, U} <- WithUploaders], FinalUsage]),

    %% return calculated moves or uploaders in vbucket number order
    lists:map(
      fun ({_, {_, UploaderTerm}, UploaderTerm}) ->
                  case OutputFormat of
                      moves ->
                          same;
                      uploaders ->
                          UploaderTerm
                  end;
          ({_, {_, _}, UploaderTerm}) ->
              UploaderTerm
      end, lists:sort(WithUploaders)).

process_candidates(_, _, Usage, [], Acc) ->
    {Acc, Usage};
process_candidates(
  Bucket, Allowance, Usage,
  [{VBucket,
    {{Candidates, _Choices}, CurrentUploaderTerm} = VBInfo} | Rest] =
      CurrentAndRest,
  Acc) ->
    case select_uploader(Bucket, VBucket, Candidates, CurrentUploaderTerm,
                         Usage, Allowance) of
        {ok, NewUploaderTerm, NewUsage} ->
            process_candidates(Bucket, Allowance, NewUsage, Rest,
                               [{VBucket, VBInfo, NewUploaderTerm} | Acc]);
        undefined ->
            [Nodes | _] = Candidates,
            ?log_debug("Need to free usage for the following nodes ~p for "
                       "bucket ~p, vbucket ~p", [Nodes, Bucket, VBucket]),
            case free_nodes(Nodes, Bucket, Allowance, Usage, Acc, []) of
                {ok, NewAcc, NewUsage} ->
                    process_candidates(
                      Bucket, Allowance, NewUsage, CurrentAndRest, NewAcc);
                fail ->
                    ?log_debug(
                       "Unable to pick a winner for bucket ~p among ~p with "
                       "allowance = ~p, usage = ~p~nIgnore allowance.",
                       [Bucket, Candidates, Allowance, Usage]),
                    {ok, NewUploaderTerm, NewUsage} =
                        select_uploader(Bucket, VBucket, Candidates,
                                        CurrentUploaderTerm, Usage, undefined),
                    process_candidates(
                      Bucket, Allowance, NewUsage, Rest,
                      [{VBucket, VBInfo, NewUploaderTerm} | Acc])
            end
    end.

free_nodes(_, _, _, _, [], _) ->
    fail;
free_nodes(Nodes, Bucket, Allowance, Usage, [VBInfo | Rest], Acc) ->
    case free_node(Nodes, Bucket, Allowance, Usage, VBInfo) of
        {ok, NewUsage, NewVBInfo} ->
            {ok, [NewVBInfo | Acc] ++ Rest, NewUsage};
        skip ->
            free_nodes(Nodes, Bucket, Allowance, Usage, Rest, [VBInfo | Acc])
    end.

free_node([], _Bucket, _Allowance, _Usage, _VBInfo) ->
    skip;
free_node([Node | Rest], Bucket, Allowance, Usage,
          {VBucket, {{Candidates, Choices}, CurrentUploaderTerm},
           {Node, _}} = VBInfo) ->
    TrimmedCandidates = [C -- [Node] || C <- Candidates],
    case select_uploader(Bucket, VBucket, TrimmedCandidates,
                         CurrentUploaderTerm, Usage, Allowance) of
        {ok, NewUploaderTerm, NewUsage} ->
            NewUsage1 = maps:update_with(Node, _ - 1, NewUsage),
            {ok, NewUsage1,
             {VBucket, {{Candidates, Choices}, CurrentUploaderTerm},
              NewUploaderTerm}};
        undefined ->
            free_node(Rest, Bucket, Allowance, Usage, VBInfo)
    end;
free_node([_Node | Rest], Bucket, Allowance, Usage, VBInfo) ->
    free_node(Rest, Bucket, Allowance, Usage, VBInfo).

-spec fail_nodes(uploaders(), [node()]) -> uploaders().
fail_nodes(Uploaders, FailedNodes) ->
    [case lists:member(N, FailedNodes) of
         true ->
             {undefined, C};
         false ->
             {N, C}
     end || {N, C} <- Uploaders].

config_key() ->
    fusion_config.

default_config() ->
    [{enable_sync_threshold_mb, 1024 * 100},
     {state, disabled},
     {log_store_uri_locked, false}].

-spec get_config() -> proplists:proplist() | not_found.
get_config() ->
    case chronicle_kv:get(kv, config_key()) of
        {ok, {Config, _}} ->
            Config;
        {error, not_found} ->
            not_found
    end.

get_config_with_default(Source) ->
    chronicle_compat:get(Source, config_key(), #{default => default_config()}).

-spec get_state() -> state().
get_state() ->
    get_state(get_config_with_default(direct)).

-spec get_state(proplists:proplist()) -> state().
get_state(Config) ->
    proplists:get_value(state, Config).

-spec get_log_store_uri() -> string().
get_log_store_uri() ->
    proplists:get_value(log_store_uri, get_config()).

-spec get_metadata_store_uri() -> string().
get_metadata_store_uri() ->
    ns_config:read_key_fast(
      magma_fusion_metadatastore_uri,
      "chronicle://localhost:" ++
          integer_to_list(service_ports:get_port(rest_port))).

-spec update_config(proplists:proplist()) ->
          {ok, chronicle:revision()} | log_store_uri_locked.
update_config(Params) ->
    chronicle_kv:transaction(
      kv, [config_key()],
      fun (Snapshot) ->
              Config = get_config_with_default(Snapshot),
              URI = proplists:get_value(log_store_uri, Params),
              case proplists:get_bool(log_store_uri_locked, Config) andalso
                  URI =/= undefined andalso
                  URI =/= proplists:get_value(log_store_uri, Config) of
                  true ->
                      {abort, log_store_uri_locked};
                  false ->
                      {commit, [{set, config_key(),
                                 misc:update_proplist(Config, Params)}]}
              end
      end).

txn_update_state_set(Txn, State) ->
    {ok, {Config, _}} = chronicle_compat:txn_get(config_key(), Txn),
    update_state_set(Config, State).

update_state_set(Config, State) ->
    {set, config_key(), lists:keystore(state, 1, Config, {state, State})}.

re_enable_uploaders(Bucket, NServers, Map, Uploaders) ->
    case janitor_agent:get_fusion_sync_info(Bucket, Map) of
        {error, Error} ->
            {error, Error};
        {ok, NodesInfo} ->
            VBInfosArray =
                lists:foldl(
                  fun ({Node, VBSyncInfo}, Acc) ->
                          lists:foldl(
                            fun ({VB, Term, Seqno}, Acc1) ->
                                    array:set(VB, [{Node, Term, Seqno} |
                                                   array:get(VB, Acc1)], Acc1)
                            end, Acc, VBSyncInfo)
                  end, array:new(length(Map), {default, []}), NodesInfo),
            VBInfos = array:to_list(VBInfosArray),
            Allowance = allowance(Map, NServers),
            ?log_debug("The following information was retrieved from bucket "
                       "~p~n~p~nCurrent uploaders: ~p~nAllowance: ~p",
                       [Bucket, VBInfos, Uploaders, Allowance]),
            {ok, build_uploaders(Bucket, lists:zip(VBInfos, Map), Uploaders,
                                 Allowance, uploaders)}
    end.

calculate_bucket_uploaders(Bucket, BucketConfig) ->
    case proplists:get_value(map, BucketConfig, []) of
        [] ->
            %% bucket map not yet properly initialized
            %% this case will be handled by janitor
            {ok, undefined};
        Map ->
            case ns_bucket:get_fusion_uploaders(Bucket) of
                not_found ->
                    %% this bucket was never enabled for fusion
                    {ok, build_initial(Map)};
                Uploaders ->
                    case ns_bucket:is_fusion(BucketConfig) of
                        false ->
                            %% fusion was disabled on this bucket which means
                            %% that data is erased. therefore  start from
                            %% scratch, but do not go lower or equal to
                            %% existing terms
                            Zipped = lists:zip(build_initial(Map), Uploaders),
                            {ok, lists:map(
                                   fun ({{Node, _}, {Node, Term}}) ->
                                           {Node, Term};
                                       ({{Node, _}, {_, Term}}) ->
                                           {Node, Term + 1}
                                   end, Zipped)};
                        true ->
                            %% fusion was stopped for this bucket
                            %% rebuild uploaders according to existing data
                            %% trying to minimize the initial upload
                            re_enable_uploaders(
                              Bucket,
                              length(ns_bucket:get_servers(BucketConfig)),
                              Map, Uploaders)
                    end
            end
    end.

calculate_uploaders([], Acc) ->
    {ok, lists:reverse(Acc)};
calculate_uploaders([{Bucket, BucketConfig} | Rest], Acc) ->
    case calculate_bucket_uploaders(Bucket, BucketConfig) of
        {error, _} = E ->
            E;
        {ok, Uploaders} ->
            calculate_uploaders(Rest, [{Bucket, Uploaders} | Acc])
    end.

-spec advance_terms(uploaders()) -> uploaders().
advance_terms(Uploaders) ->
    [{Node, Term + 1} || {Node, Term} <- Uploaders].

validate_buckets(BucketNames, Buckets) ->
    MagmaBuckets = ns_bucket:get_buckets_of_type({membase, magma}, Buckets),
    MagmaBucketNames = ns_bucket:get_bucket_names(MagmaBuckets),
    {BucketsToEnable, UnknownOrNonMagma} =
        case BucketNames of
            undefined ->
                {MagmaBuckets, []};
            _ ->
                UnknownBucketNames =
                    BucketNames -- ns_bucket:get_bucket_names(Buckets),
                NonMagmaBucketNames =
                    (BucketNames -- UnknownBucketNames) -- MagmaBucketNames,

                MagmaBucketsToEnable =
                    [{BucketName, BucketConfig} ||
                        {BucketName, BucketConfig} <- MagmaBuckets,
                        lists:member(BucketName, BucketNames)],

                {MagmaBucketsToEnable,
                 [{BucketName, unknown} || BucketName <- UnknownBucketNames] ++
                     [{BucketName, non_magma} ||
                         BucketName <- NonMagmaBucketNames]}
        end,
    BucketsNotToEnable =
        lists:filtermap(
          fun({BucketName, BucketConfig}) ->
                  case ns_bucket:get_continuous_backup_enabled(BucketConfig) of
                      true ->
                          {true, {BucketName, continuous_backup_enabled}};
                      false ->
                          false
                  end
          end, BucketsToEnable),
    case {UnknownOrNonMagma, BucketsNotToEnable} of
        {[], []} ->
            {ok, BucketsToEnable, MagmaBucketNames};
        _ ->
            {errors, UnknownOrNonMagma ++ BucketsNotToEnable}
    end.

-spec command({enable, [ns_bucket:name()] | undefined} | disable | stop)
             -> ok | {error, enable_error()} |
          {error, disable_or_stop_error()}.
command({enable, BucketNames}) ->
    maybe
        true ?= (not ns_bucket:any_bucket_encryption_enabled(direct)) orelse
            {error, {not_allowed, "Bucket encryption is enabled."}},

        {ok, BucketsToEnable, MagmaBucketNames} ?=
            case validate_buckets(BucketNames, ns_bucket:get_buckets()) of
                {errors, Errors} ->
                    {error, {wrong_buckets, Errors}};
                RV -> RV
            end,

        ?log_debug("Enabling fusion for buckets ~p",
                   [ns_bucket:get_bucket_names(BucketsToEnable)]),

        {ok, BucketUploaders} ?= calculate_uploaders(BucketsToEnable, []),
        [?log_debug("Setting uploaders for bucket ~p:~n~p",
                    [BucketName, Uploaders]) ||
            {BucketName, Uploaders} <- BucketUploaders],

        {ok, _, {EnablingBuckets, DisablingBuckets}} ?=
            enable(BucketUploaders, MagmaBucketNames),

        [?LOG_BUCKET_STATE(Bucket, enabled) || Bucket <- EnablingBuckets],
        [?LOG_BUCKET_STATE(Bucket, disabling) || Bucket <- DisablingBuckets],
        ?LOG_FUSION_STATE(enabling),
        post_enable(BucketsToEnable)
    end;
command(Command) when Command =:= disable orelse Command =:= stop ->
    ?log_debug("Attempt to ~p fusion", [Command]),
    {StateToSet, AllowedStates, BucketStates} =
        case Command of
            disable ->
                {disabling, [enabled, enabling, stopped, stopping],
                 [enabled, stopped, stopping]};
            stop ->
                {stopping, [enabled, enabling], [enabled]}
        end,
    case chronicle_compat:txn(disable_or_stop_txn(
                                _, StateToSet, AllowedStates, BucketStates)) of
        {ok, _Rev, AffectedBuckets} ->
            [?LOG_BUCKET_STATE(B, StateToSet) || B <- AffectedBuckets],
            ?LOG_FUSION_STATE(StateToSet),
            ok;
        Other ->
            Other
    end.

enable_buckets(Snapshot, BucketUploaders) ->
    SetsAndBuckets =
        lists:flatmap(
          fun ({BucketName, Uploaders}) ->
                  {ok, BucketConfig} =
                      ns_bucket:get_bucket(BucketName, Snapshot),
                  case Uploaders of
                      undefined ->
                          [];
                      _ ->
                          [{{set, ns_bucket:fusion_uploaders_key(BucketName),
                             Uploaders}, undefined}]
                  end ++
                      case ns_bucket:get_fusion_state(BucketConfig) of
                          enabled ->
                              [];
                          _ ->
                              [{{set, ns_bucket:sub_key(BucketName, props),
                                 ns_bucket:set_fusion_state(
                                   enabled, BucketConfig)}, BucketName}]
                      end
          end, BucketUploaders),
    {[Set || {Set, _} <- SetsAndBuckets],
     [Bucket || {_, Bucket} <- SetsAndBuckets, Bucket =/= undefined]}.

post_enable(Buckets) ->
    Servers = lists:usort(lists:flatten(
                            [ns_bucket:get_servers(BC) || {_, BC} <- Buckets])),
    case chronicle_compat:push(Servers) of
        ok ->
            ok;
        {error, BadReplies} ->
            ?log_warning("Failed to "
                         "synchronize config to some nodes: ~p", [BadReplies]),
            %% returning error to the caller will be misleading, since the
            %% enabling procedure is already started
            ok
    end,
    %% proceed to start the uploaders
    [ns_orchestrator:request_janitor_run({bucket, BN}) ||
        {BN, _} <- Buckets],
    ok.

enable(BucketUploaders, MagmaBucketNames) ->
    BucketsToEnable = [BN || {BN, _} <- BucketUploaders],
    BucketsNotToEnable = MagmaBucketNames -- BucketsToEnable,
    chronicle_kv:transaction(
      kv,
      [config_key() | [ns_bucket:sub_key(BN, props) || BN <- MagmaBucketNames]],
      fun (Snapshot) ->
              try
                  Config = get_config_with_default(Snapshot),
                  proplists:get_value(log_store_uri,
                                      Config) =/= undefined orelse
                      throw(not_initialized),
                  State = proplists:get_value(state, Config),
                  (State == disabled) orelse (State == stopped) orelse
                      throw({wrong_state, State, [disabled, stopped]}),

                  {DisablingBucketSets, DisablingBuckets} =
                      update_bucket_state_sets(
                        ns_bucket:get_buckets(Snapshot, BucketsNotToEnable),
                        [stopped], disabling),

                  {EnablingBucketSets, EnablingBuckets} =
                      enable_buckets(Snapshot, BucketUploaders),

                  {commit, [{set, config_key(),
                             misc:update_proplist(
                               Config, [{state, enabling},
                                        {log_store_uri_locked, true}])} |
                            DisablingBucketSets ++ EnablingBucketSets],
                   {EnablingBuckets, DisablingBuckets}}
              catch
                  throw:Error ->
                      {abort, {error, Error}}
              end
      end).

disable_or_stop_txn(Txn, StateToSet, AllowedStates, BucketStates) ->
    Snapshot = ns_bucket:fetch_snapshot(all, Txn, [props]),
    {ok, {Config, _}} = chronicle_compat:txn_get(config_key(), Txn),
    State = proplists:get_value(state, Config),
    case lists:member(State, AllowedStates) of
        false ->
            {abort, {error, {wrong_state, State, AllowedStates}}};
        true ->
            FusionBuckets = ns_bucket:get_fusion_buckets(Snapshot),
            {BucketSets, AffectedBuckets} =
                update_bucket_state_sets(FusionBuckets, BucketStates,
                                         StateToSet),
            {commit,
             [update_state_set(Config, StateToSet) | BucketSets],
             AffectedBuckets}
    end.

update_bucket_state_sets(FusionBuckets, BucketStates, StateToSet) ->
    AffectedBuckets =
        lists:filter(
          fun ({_BucketName, BucketConfig}) ->
                  lists:member(ns_bucket:get_fusion_state(BucketConfig),
                               BucketStates)
          end, FusionBuckets),
    {[{set, ns_bucket:sub_key(BucketName, props),
       ns_bucket:set_fusion_state(StateToSet, BucketConfig)} ||
         {BucketName, BucketConfig} <- AffectedBuckets],
     [B || {B, _} <- AffectedBuckets]}.

-spec maybe_grab_heartbeat_info() -> list().
maybe_grab_heartbeat_info() ->
    maybe
        false ?= get_state() =:= disabled,
        active ?= ns_cluster_membership:get_cluster_membership(node()),
        true ?= lists:member(kv, ns_cluster_membership:node_services(node())),
        {_, Deleting} = fusion_local_agent:get_state(),
        [{fusion_stats,
          [{buckets,
            lists:filtermap(
              fun ({BucketName, BucketConfig}) ->
                      maybe_grab_heartbeat_info(
                        BucketName,
                        ns_bucket:get_fusion_state(BucketConfig))
              end, ns_bucket:get_buckets())},
           {deleting, Deleting}]}]
    else
        _ -> []
    end.

add_stat_value(Key, Value, Acc) ->
    maps:update_with(Key, fun(V) -> V + Value end, 0, Acc).

add_stat_value_from(Key, VBStats, Acc) ->
    Value = proplists:get_value(list_to_binary(atom_to_list(Key)), VBStats),
    add_stat_value(Key, Value, Acc).

process_vbucket_stats(Node, BucketName, VB, VBStats, DesiredTerm, Acc) ->
    case proplists:get_value(<<"state">>, VBStats) of
        <<"enabled">> = E ->
            Term = proplists:get_value(<<"term">>, VBStats),
            Acc1 =
                case DesiredTerm of
                    Term ->
                        Acc;
                    Other ->
                        Expected = case Other of
                                       undefined ->
                                           <<"disabled">>;
                                       OtherTerm ->
                                           {E, OtherTerm}
                                   end,
                        ?log_debug("Uploader ~p:~p state mismatch on ~p. Got: "
                                   "~p. Expected: ~p",
                                   [BucketName, VB, Node, {E, Term}, Expected]),
                        add_stat_value(uploaders_state_mismatch, 1, Acc)
                end,
            functools:chain(
              Acc1,
              [add_stat_value_from(sync_session_total_bytes, VBStats, _),
               add_stat_value_from(sync_session_completed_bytes, VBStats, _),
               add_stat_value_from(snapshot_pending_bytes, VBStats, _)]);
        Other ->
            case DesiredTerm of
                undefined ->
                    Acc;
                _ ->
                    ?log_debug("Uploader ~p:~p state mismatch on ~p. Got: "
                               "~p, Expected: ~p",
                               [BucketName, VB, Node, Other,
                                {<<"enabled">>, DesiredTerm}]),
                    add_stat_value(uploaders_state_mismatch, 1, Acc)
            end
    end.

process_bucket_stats(Node, BucketName, VBucketsInfo, ThisNodeUploaders) ->
    lists:foldl(
      fun ({VB, Term}, Acc) ->
              Key = << <<"vb_">>/binary, (integer_to_binary(VB))/binary >>,
              {VBStats} = maps:get(Key, VBucketsInfo,
                                  {[<<"state">>, <<"disabled">>]}),
              process_vbucket_stats(Node, BucketName, VB, VBStats, Term, Acc)
      end, #{}, ThisNodeUploaders).

node_uploaders(ThisNode, State, Uploaders) ->
    case State of
        enabled ->
            lists:map(
              fun ({VB, {Node, Term}}) when Node =:= ThisNode ->
                      {VB, Term};
                  ({VB, _}) ->
                      {VB, undefined}
              end, misc:enumerate(Uploaders, 0));
        _ ->
            [{VB, undefined} || {VB, _} <- misc:enumerate(Uploaders, 0)]
    end.

%% this will be fed to lists:filtermap
maybe_grab_heartbeat_info(_BucketName, disabled) ->
    false;
maybe_grab_heartbeat_info(_BucketName, stopped) ->
    false;
maybe_grab_heartbeat_info(BucketName, State) ->
    case ns_bucket:get_fusion_uploaders(BucketName) of
        not_found ->
            false;
        Uploaders ->
            ThisNodeUploaders = node_uploaders(node(), State, Uploaders),
            case ns_memcached:get_fusion_uploaders_state(BucketName) of
                {ok, {VBucketsInfo}} ->
                    StatsMap = process_bucket_stats(
                                 node(), BucketName,
                                 maps:from_list(VBucketsInfo),
                                 ThisNodeUploaders),
                    {true, {BucketName, maps:to_list(StatsMap)}};
                Error ->
                    ?log_debug(
                       "Failure to retrieve uploaders stats for bucket ~p, "
                       "Error:~p", [BucketName, Error]),
                    false
            end
    end.

-spec maybe_advance_state() -> ok.
maybe_advance_state() ->
    case maybe_advance_state(get_state()) of
        nothing_to_do ->
            ok;
        ok ->
            ok
    end.

maybe_advance_state(enabling) ->
    FusionConfig = get_config_with_default(direct),
    Threshold = proplists:get_value(enable_sync_threshold_mb, FusionConfig)
        * 1024 * 1024,

    EnabledBucketsReady =
        fun(FusionBuckets, FusionStats) ->
                EnabledBuckets =
                    ns_bucket:filter_buckets_by(
                      FusionBuckets,
                      ?cut(ns_bucket:get_fusion_state(_) =:= enabled)),
                Result =
                    analyze_fusion_stats(
                      EnabledBuckets, FusionStats,
                      fun (_BucketName, BucketInfo, Acc) ->
                              case {maps:find(snapshot_pending_bytes,
                                              BucketInfo),
                                    maps:find(uploaders_state_mismatch,
                                              BucketInfo)} of
                                  {{ok, Bytes}, error} ->
                                      case Acc + Bytes of
                                          NewAcc when NewAcc > Threshold ->
                                              false;
                                          NewAcc ->
                                              NewAcc
                                      end;
                                  _ ->
                                      false
                              end
                      end, 0),

                case Result of
                    {true, Bytes} ->
                        ?log_info("Changing fusion state to 'enabled', "
                                  "~p bytes remains to be synced", [Bytes]),
                        true;
                    false ->
                        false
                end
        end,

    %% This will advance all disabling buckets to disabled state, check the
    %% enabled buckets for uploaders being started and enough data being
    %% uploaded and finally advance the fusion state to enabled if all
    %% conditions are met
    maybe_advance_state(enabling, enabled, disabling, disabled,
                        EnabledBucketsReady);
maybe_advance_state(State) when State =:= disabling orelse State =:= stopping ->
    NextState = case State of
                    disabling ->
                        disabled;
                    stopping ->
                        stopped
                end,
    maybe_advance_state(State, NextState, State, NextState, undefined);
maybe_advance_state(_) ->
    nothing_to_do.

maybe_advance_state(State, NextState, BucketState, NextBucketState,
                    ExtraCheck) ->
    FusionBuckets = ns_bucket:get_fusion_buckets(),
    DeletionInfo = get_deletion_state(State),

    case fetch_fusion_stats(FusionBuckets, #{}) of
        error ->
            nothing_to_do;
        FusionStats ->
            RV =
                chronicle_compat:txn(
                  fun (Txn) ->
                          {Sets, AllReady, AdvancedBuckets} =
                              buckets_advance_state_sets(
                                Txn, FusionStats, BucketState, NextBucketState),
                          Sets1 =
                              case AllReady of
                                  false ->
                                      Sets;
                                  true ->
                                      case can_advance_fusion_state(
                                             Txn, State, DeletionInfo,
                                             ?cut(ExtraCheck(FusionBuckets,
                                                             FusionStats))) of
                                          true ->
                                              [txn_update_state_set(
                                                 Txn, NextState) | Sets];
                                          false ->
                                              Sets
                                      end
                              end,
                          case Sets1 of
                              [] ->
                                  {abort, nothing_to_do};
                              _ ->
                                  {commit, Sets1,
                                   {AdvancedBuckets, Sets1 =/= Sets}}
                          end
                  end),
            case RV of
                {ok, _Rev, {AdvancedBuckets, StateAdvanced}} ->
                    [?LOG_BUCKET_STATE(Bucket, NextBucketState) ||
                        Bucket <- AdvancedBuckets],
                    not StateAdvanced orelse ?LOG_FUSION_STATE(NextState),
                    ok;
                nothing_to_do ->
                    nothing_to_do
            end
    end.

get_deletion_state(stopping) ->
    undefined;
get_deletion_state(State) when State =:= enabling orelse State =:= disabling ->
    KVNodes = ns_cluster_membership:service_active_nodes(kv),
    case fusion_local_agent:get_states(KVNodes, ?GET_DELETION_STATE_TIMEOUT) of
        {error, BadNodes} ->
            ?log_debug("Failed to obtain fusion deletion state from nodes ~p",
                       [BadNodes]),
            error;
        {ok, Results} ->
            maps:from_list(Results)
    end.

can_advance_fusion_state(_Txn, stopping, _, _) ->
    true;
can_advance_fusion_state(_Txn, disabling, error, _) ->
    false;
can_advance_fusion_state(Txn, disabling, DeletionInfo, _) ->
    check_deletion_info(Txn, disabling, DeletionInfo);
can_advance_fusion_state(_Txn, enabling, error, _) ->
    false;
can_advance_fusion_state(Txn, enabling, DeletionInfo, ExtraCheck) ->
    case check_deletion_info(Txn, enabling, DeletionInfo) of
        true ->
            ExtraCheck();
        false ->
            false
    end.

check_deletion_info(Txn, State, DeletionInfo) ->
    Snapshot = ns_cluster_membership:fetch_snapshot(Txn),
    KVNodes = ns_cluster_membership:service_active_nodes(Snapshot, kv),
    %% We check 2 things here:
    %%   1. current fusion status (disabling or enabling) propagated to all
    %%      local agents (which means that all of them started deleting stuff)
    %%   2. all local agents have nothing to delete.
    lists:all(
      fun (Node) ->
              case maps:find(Node, DeletionInfo) of
                  {ok, {State, []}} ->
                      true;
                  _ ->
                      false
              end
      end, KVNodes).

buckets_with_correct_uploaders(Buckets, FusionStats) ->
    RV =
        analyze_fusion_stats(
          Buckets, FusionStats,
          fun (BucketName, BucketInfo, Acc) ->
                  case maps:is_key(uploaders_state_mismatch, BucketInfo) of
                      false ->
                          Acc;
                      true ->
                          ?log_debug("Uploaders state mismatch on bucket ~p",
                                     [BucketName]),
                          lists:keydelete(BucketName, 1, Acc)
                  end
          end, Buckets),
    case RV of
        {true, List} ->
            List;
        false ->
            []
    end.

buckets_with_no_active_volumes(Buckets) ->
    lists:filter(
      fun ({BucketName, BucketConfig}) ->
              case janitor_agent:get_active_guest_volumes(
                     BucketName, BucketConfig) of
                  {error, Error} ->
                      ?log_debug("Failure to get active guest volumes "
                                 "for bucket ~p: ~p", [BucketName, Error]),
                      false;
                  {ok, List} ->
                      lists:all(
                        fun ({_Node, []}) ->
                                true;
                            ({Node, Volumes}) ->
                                ?log_debug("Still have volumes ~p on node ~p "
                                           "for bucket ~p",
                                           [Volumes, Node, BucketName]),
                                false
                        end, List)
              end
      end, Buckets).

buckets_advance_state_sets(Txn, PerBucketPerNodeMap, State, NextState) ->
    Snapshot = ns_bucket:fetch_snapshot(all, Txn, [props]),
    Buckets = [{BucketName, BucketConfig} ||
                  {BucketName, BucketConfig} <- ns_bucket:get_buckets(Snapshot),
                  ns_bucket:get_fusion_state(BucketConfig) =:= State],
    BucketsToAdvance =
        buckets_with_no_active_volumes(
          buckets_with_correct_uploaders(Buckets, PerBucketPerNodeMap)),
    {[{set, ns_bucket:sub_key(BucketName, props),
       ns_bucket:set_fusion_state(NextState, BucketConfig)} ||
         {BucketName, BucketConfig} <- BucketsToAdvance],
     length(BucketsToAdvance) =:= length(Buckets),
     [B || {B, _} <- BucketsToAdvance]}.

analyze_fusion_stats([], _, _, Acc) ->
    {true, Acc};
analyze_fusion_stats([{Bucket, BucketConfig} | Rest],
                     PerBucketPerNodeMap, Fun, Acc) ->
    Uploaders = ns_bucket:get_fusion_uploaders(Bucket),
    FusionState = ns_bucket:get_fusion_state(BucketConfig),
    Servers = ns_bucket:get_servers(BucketConfig),

    case analyze_fusion_stats_for_bucket(
           Bucket, Uploaders, FusionState, Servers,
           PerBucketPerNodeMap, Fun, Acc) of
        false ->
            false;
        NewAcc ->
            analyze_fusion_stats(Rest, PerBucketPerNodeMap, Fun, NewAcc)
    end.

analyze_fusion_stats_for_bucket(_, _, _, [], _, _, Acc) ->
    Acc;
analyze_fusion_stats_for_bucket(Bucket, Uploaders, FusionState, [Node | Rest],
                                PerBucketPerNodeMap, Fun, Acc) ->
    {ok, PerNodeMap} = maps:find(Bucket, PerBucketPerNodeMap),
    {ok, {VBucketsInfo}} = maps:find(Node, PerNodeMap),
    ThisNodeUploaders = node_uploaders(Node, FusionState, Uploaders),
    Aggregated = process_bucket_stats(
                   Node, Bucket, maps:from_list(VBucketsInfo),
                   ThisNodeUploaders),

    case Fun(Bucket, Aggregated, Acc) of
        false ->
            false;
        NewAcc ->
            analyze_fusion_stats_for_bucket(
              Bucket, Uploaders, FusionState, Rest,
              PerBucketPerNodeMap, Fun, NewAcc)
    end.

fetch_fusion_stats([], Acc) ->
    Acc;
fetch_fusion_stats([{BucketName, BucketConfig} | Rest], Acc) ->
    case janitor_agent:get_fusion_uploaders_state(BucketName, BucketConfig) of
        {error, {failed_nodes, BadNodes}} ->
            ?log_warning("Unable to fetch uploaders state for bucket ~p from "
                         "nodes ~p", [BucketName, BadNodes]),
            error;
        {ok, PerNodeInfos} ->
            fetch_fusion_stats(
              Rest, Acc#{BucketName => maps:from_list(PerNodeInfos)})
    end.

-spec create_snapshot_uuid(string() | binary(), binary()) -> string().
create_snapshot_uuid(PlanUUID, BucketUUID) ->
    lists:flatten(io_lib:format("~s:~s", [PlanUUID, BucketUUID])).

-spec get_stored_snapshot_uuids() -> [{binary(), binary(), integer()}].
get_stored_snapshot_uuids() ->
    chronicle_compat:get(direct, snapshots_key(), #{default => []}).

snapshots_key() ->
    fusion_storage_snapshots.

-spec store_snapshots_uuid(binary(), binary(), integer()) -> ok.
store_snapshots_uuid(PlanUUID, BucketUUID, NumVBuckets) ->
    {ok, _} =
        chronicle_kv:txn(
          kv,
          fun (Txn) ->
                  Entry = {PlanUUID, BucketUUID, NumVBuckets},
                  {commit, [{set, snapshots_key(),
                             case chronicle_kv:txn_get(snapshots_key(), Txn) of
                                 {ok, {List, _}} ->
                                     [Entry | List];
                                 {error, not_found} ->
                                     [Entry]
                             end}]}
          end),
    ok.

execute_on_kv_node(KVNodes, Fun, Params, Timeout, What, Error) ->
    case ns_node_disco:only_live_nodes(KVNodes) of
        [] ->
            ?log_error("Unable to select node for ~p", [What]),
            {error, {Error, undefined}};
        [NodeToQuery | _] ->
            case (catch rpc:call(NodeToQuery, ?MODULE, Fun, Params, Timeout)) of
                {badrpc, Err} ->
                    ?log_error("~p from ~p failed with ~p",
                               [What, NodeToQuery, Err]),
                    {error, {Error, NodeToQuery}};
                Reply ->
                    {ok, Reply}
            end
    end.

-spec do_delete_snapshots(binary(), [vbucket_id()], string()) ->
          ok | mc_error().
do_delete_snapshots(BucketUUID, VBuckets, SnapshotUUID) ->
    case ns_memcached:release_fusion_storage_snapshot(
           BucketUUID, VBuckets, SnapshotUUID) of
        ok ->
            ok;
        Error ->
            ?log_debug("Deleting snapshot ~p for bucket ~p vbuckets ~p failed "
                       "with ~p", [SnapshotUUID, BucketUUID, VBuckets, Error]),
            Error
    end.

-spec get_snapshots(
        binary(), [vbucket_id()], string(), non_neg_integer(), [node()]) ->
          {ok, [{non_neg_integer(), term()}]} | {error, {term(), node()}}.
get_snapshots(_BucketUUID, [], _SnapshotUUID, _Validity, _KVNodes) ->
    {ok, []};
get_snapshots(BucketUUID, VBuckets, SnapshotUUID, Validity, KVNodes) ->
    execute_on_kv_node(KVNodes, do_get_snapshots,
                       [BucketUUID, VBuckets, SnapshotUUID, Validity],
                       ?GET_SNAPSHOTS_TIMEOUT,
                       "Getting fusion storage snapshot",
                       failed_to_get_snapshots).

-spec do_get_snapshots(binary(), [vbucket_id()], string(), non_neg_integer()) ->
          [{non_neg_integer(), term()}].
do_get_snapshots(BucketUUID, VBuckets, SnapshotUUID, Validity) ->
    {ok, Bin} = ns_memcached:get_fusion_storage_snapshot(
                  BucketUUID, VBuckets, SnapshotUUID, Validity),
    lists:zip(VBuckets, ejson:decode(Bin)).

delete_snapshots({PlanUUID, BucketUUID, NumVBuckets} = Entry) ->
    ?log_debug("Deleting snapshots for ~p", [Entry]),
    SnapshotUUID = create_snapshot_uuid(PlanUUID, BucketUUID),
    VBuckets = lists:seq(0, NumVBuckets - 1),
    KVNodes = ns_cluster_membership:service_active_nodes(kv),

    execute_on_kv_node(KVNodes, do_delete_snapshots,
                       [BucketUUID, VBuckets, SnapshotUUID],
                       ?DELETE_SNAPSHOTS_TIMEOUT,
                       "Deleting fusion storage snapshot",
                       failed_to_delete_snapshots).

remove_snapshot_entry(Entry) ->
    chronicle_kv:update(kv, snapshots_key(), lists:delete({Entry}, _)).

cleanup_snapshots() ->
    ToDelete =
        lists:filtermap(
          fun ({PlanUUID, BucketUUID, _NumVBuckets} = Entry) ->
                  case ns_bucket:uuid2bucket(BucketUUID) of
                      {error, not_found} ->
                          ?log_debug("Bucket for ~p no longer exists", [Entry]),
                          {true, {undefined, Entry}};
                      {ok, BucketName} ->
                          case ns_orchestrator:validate_rebalance_plan(
                                 binary_to_list(PlanUUID)) of
                              true ->
                                  false;
                              false ->
                                  ?log_debug(
                                     "Plan UUID ~p is no longer valid. "
                                     "Bucket = ~p", [PlanUUID, BucketName]),
                                  {true, {BucketName, Entry}}
                          end
                  end
          end, get_stored_snapshot_uuids()),

     MaybeAffectedBuckets = lists:uniq(
                             lists:filtermap(
                               fun({undefined, _}) ->
                                       false;
                                  ({BucketName, _}) ->
                                       {true, BucketName}
                               end, ToDelete)),
    UploadersVerifiedBuckets =
        case MaybeAffectedBuckets of
            [] ->
                [];
            _ ->
                BucketsWithBC =
                    lists:map(fun (Name) ->
                                      {ok, BC} = ns_bucket:get_bucket(Name),
                                      {Name, BC}
                              end, MaybeAffectedBuckets),
                case fetch_fusion_stats(BucketsWithBC, #{}) of
                    error ->
                        ?log_debug("Failed to fetch fusion stats"),
                        [];
                    FusionStats ->
                        buckets_with_correct_uploaders(
                          BucketsWithBC, FusionStats)
                end
        end,

    lists:foreach(
      fun ({undefined, Entry}) ->
              case delete_snapshots(Entry) of
                  {ok, ok} ->
                      remove_snapshot_entry(Entry);
                  _ ->
                      ok
              end;
          ({BucketName, Entry}) ->
              case proplists:is_defined(BucketName, UploadersVerifiedBuckets) of
                  false ->
                      ?log_debug("Not ready to delete snapshots for ~p, ~p",
                                 [BucketName, Entry]);
                  true ->
                      case delete_snapshots(Entry) of
                          {ok, ok} ->
                              remove_snapshot_entry(Entry);
                          _ ->
                              ok
                      end
              end
      end, ToDelete).

-spec cleanup_mounted_volumes(ns_bucket:name(), ns_bucket:config()) ->
          ok | error.
cleanup_mounted_volumes(Bucket, BucketConfig) ->
    case ns_bucket:get_fusion_state(BucketConfig) =:= enabled andalso
        not ns_orchestrator:has_rebalance_plan() of
        true ->
            case janitor_agent:cleanup_mounted_volumes(Bucket, BucketConfig) of
                ok ->
                    ok;
                Error ->
                    ?log_info(
                       "Errors cleaning up mounted volumes for bucket ~p: ~p",
                       [Bucket, Error]),
                    error
            end;
        false ->
            ok
    end.

-ifdef(TEST).

validate_buckets_test() ->
    Buckets = [{"memcached", [{type, memcached}]},
               {"couchstore", [{type, membase}, {storage_mode, couchstore}]},
               {"magma", [{type, membase}, {storage_mode, magma}]}],

    Buckets1 = Buckets ++ [{"PiTR", [{type, membase}, {storage_mode, magma},
                                     {continuous_backup_enabled, true}]}],

    RV = validate_buckets(undefined, Buckets),
    ?assertMatch({ok, [{"magma", _}], ["magma"]}, RV),

    RV1 = validate_buckets(undefined, Buckets1),
    ?assertEqual({errors, [{"PiTR", continuous_backup_enabled}]}, RV1),

    RV2 = validate_buckets(
            ["unknown", "memcached", "couchstore", "magma", "PiTR"], Buckets1),
    ?assertMatch({errors, _}, RV2),
    {errors, Errors2} = RV2,
    ?assertListsEqual([{"unknown", unknown},
                       {"memcached", non_magma},
                       {"couchstore", non_magma},
                       {"PiTR", continuous_backup_enabled}], Errors2),

    RV3 = validate_buckets(["magma"], Buckets1),
    ?assertMatch({ok, [{"magma", _}], _}, RV3),
    {ok, _, MagmaBuckets} = RV3,
    ?assertListsEqual(["magma", "PiTR"], MagmaBuckets).

generate_initial_map(NVBuckets, NReplicas, Nodes) ->
    mb_map:generate_map(mb_map:no_nodes_map(NVBuckets, NReplicas),
                        NReplicas, Nodes, []).

uploaders_assignment_test() ->
    erlang:erase(),
    AllNodes = [a, b, c, d, e, f],
    [do_moves(AllNodes, NNodes, NVBuckets, NReplicas, 5) ||
        NNodes <- [1, 2, 3, 4, 5, 6],
        NVBuckets <- [128],
        NReplicas <- [1, 2]],
    ?log_debug("Test reports:~n~p", [erlang:get()]).

do_moves(AllNodes, NumInitialNodes, NVBuckets, NReplicas, Iterations) ->
    TestKey = lists:flatten(io_lib:format(
                              "~p:~p:~p",
                              [NumInitialNodes, NVBuckets, NReplicas])),
    InitialNodes = lists:sort(
                     lists:sublist(misc:shuffle(AllNodes), NumInitialNodes)),
    InitialMap = generate_initial_map(NVBuckets, NReplicas, InitialNodes),
    InitialUploaders = build_initial(InitialMap),

    do_moves(Iterations, NReplicas, AllNodes, InitialNodes, InitialMap,
             InitialUploaders, TestKey).

do_moves(0, _, _, _, _, _, _) ->
    ok;
do_moves(Iterations, NReplicas, AllNodes, InitialNodes, InitialMap,
         InitialUploaders, TestKey) ->
    NumNodes = rand:uniform(length(AllNodes)),
    case lists:sort(lists:sublist(misc:shuffle(AllNodes), NumNodes)) of
        InitialNodes ->
            do_moves(Iterations, NReplicas, AllNodes, InitialNodes,
                     InitialMap, InitialUploaders, TestKey);
        Nodes ->
            FastForwardMap = mb_map:generate_map(
                               InitialMap, NReplicas, Nodes, []),
            Allowance = allowance(FastForwardMap, length(Nodes)),
            Moves = calculate_moves("test", InitialMap, FastForwardMap,
                                    InitialUploaders, Allowance),
            {UsageMap, UploadingFromScratch} =
                get_report(InitialMap, InitialUploaders, Moves),

            ?log_debug("Rebalance from ~p to ~p, allowance = ~p",
                       [InitialNodes, Nodes, Allowance]),
            NumNodes =< NReplicas orelse ?assertEqual([], UploadingFromScratch),
            Balanced = lists:all(_ =< Allowance, maps:values(UsageMap)),

            ReportList = case erlang:get(TestKey) of
                             undefined -> [];
                             L -> L
                         end,
            NewReportList =
                [{Balanced, UploadingFromScratch =:= [], InitialNodes, Nodes,
                  Allowance, UsageMap} | ReportList],
            erlang:put(TestKey, NewReportList),

            NewUploaders =
                lists:map(
                  fun ({same, Uploader}) ->
                          Uploader;
                      ({NewUploader, _}) ->
                          NewUploader
                  end, lists:zip(Moves, InitialUploaders)),
            do_moves(Iterations - 1, NReplicas, AllNodes, Nodes, FastForwardMap,
                     NewUploaders, TestKey)
    end.

get_report(InitialMap, InitialUploaders, Moves) ->
    lists:foldl(
      fun ([_, _, same, {Uploader, _}], {NodesMap, FromScratch}) ->
              {maps:update_with(Uploader, _ + 1, 1, NodesMap), FromScratch};
          ([N, Chain, {NewUploader, _}, {Uploader, _}],
           {NodesMap, FromScratch}) ->
              NewFromScratch =
                  case lists:member(NewUploader, Chain) of
                      true ->
                          [{N, Chain, Uploader, NewUploader} | FromScratch];
                      false ->
                          FromScratch
                  end,
              {maps:update_with(NewUploader, _ + 1, 1, NodesMap),
               NewFromScratch}
      end, {#{}, []},
      misc:zipwithN(
        fun functools:id/1,
        [lists:seq(0, length(InitialMap) - 1), InitialMap, Moves,
         InitialUploaders])).

-endif.
