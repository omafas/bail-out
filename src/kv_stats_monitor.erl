%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% kv_stats_monitor pulls stats from memcached to check for issues that feed
%% into the health monitoring subsystem of auto-failover.
%%
%% Here we check for two groups of statistics from memcached:
%%     1) disk-failures
%%     2) disk-slowness
%%
%% Stat checking behaves slightly differently between groups.
%%
%% High level steps for disk-failures stats:
%%     1. For each persistent bucket, get the disk-failure stats from memcached.
%%     2. Compare each stat sample with its previous value and count the # of
%%        times the stat has incremented during the user configured time period.
%%        If the above count is over some threshold, then it indicates sustained
%%        failure.
%%     3. If any of the stats show sustained failure then KV stats monitor will
%%        report I/O error for the corresponding bucket.
%%
%%     Since we are looking for sustained failure, we are not interested in the
%%     value of the stat itself but rather the number of samples where the stat
%%     has increased. The threshold is for the number of samples. E.g. A
%%     timePeriod of 100s has 100 stat samples (one per second). If 60 of those
%%     samples show an increment over the previous sample then that is
%%     considered a sustained failure. The KV retry policy for write failure
%%     is to retry the write immediately and indefinitely. As long as the disk
%%     failure continues to exist, the write related failure stat will continue
%%     to increase. This is irrespective of whether the client continues to
%%     perform writes or not. As a result, more or less every sample of the
%%     write related failure stats should show an increment over the previous
%%     one. KV's retry policy for reads is different. It does not retry reads
%%     on read failure. The read related failure stat will continue to increase
%%     as long as the client is performing read ops and the disk failure
%%     continues to exist.
%%
%% High level steps for disk-slowness stats:
%%    1. For each persistent bucket, get the disk-slowness X stats from
%%       memcached where X is the configured timePeriod by which we measure
%%       disk-slowness/disk non-responsiveness.
%%    2. Stats are supplied for reads and writes, and they comprise of two
%%       stats, the number of pending IOs and the number of IOs slower than X
%%       seconds. Check if the number of outstanding IO operations is equal to
%%       the number of slow operations. Yes => unhealthy.
%%
-module(kv_stats_monitor).

-behaviour(health_monitor).

-include("ns_common.hrl").

-ifdef(TEST).
-include("ns_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Percentage threshold
-define(DISK_ISSUE_THRESHOLD, ?get_param(disk_issue_threshold, 60)).

-export([start_link/0]).
-export([get_reason/1,
         analyze_status/1,
         is_failure/1]).

%% gen_server-like health_monitor API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% Rest of the health_monitor API)
-export([can_refresh/1,
         get_statuses/0]).

-export([register_tick/3,
         is_unhealthy/2,
         failure_stats/0,
         slow_stats/0]).

-ifdef(TEST).
-export([common_test_setup/0,
         common_test_teardown/0,
         health_monitor_test_setup/0,
         health_monitor_t/0,
         health_monitor_test_teardown/0]).
-endif.

start_link() ->
    health_monitor:start_link(?MODULE).

%% gen_server callbacks
init(BaseMonitorState) ->
    Self = self(),

    chronicle_compat_events:subscribe(
      fun (auto_failover_cfg) ->
              Self ! {event, auto_failover_cfg};
          (cluster_compat_version) ->
              Self ! {event, buckets};
          (Key) ->
              case ns_bucket:buckets_change(Key) of
                  false ->
                      ok;
                  true ->
                      Self ! {event, buckets}
              end
      end),
    AFOCfg = auto_failover:get_cfg(),
    {EnabledDiskIssues, NumSamplesDiskIssues} =
        get_failover_on_disk_issues(AFOCfg, BaseMonitorState),
    {EnabledDiskNonResp, DiskNonRespTimePeriod} =
        get_failover_on_disk_non_responsiveness(AFOCfg),
    maybe_spawn_stats_collector(
      BaseMonitorState#{buckets => reset_bucket_info(),
                        enabled_disk_issues => EnabledDiskIssues,
                        num_samples_disk_issues => NumSamplesDiskIssues,
                        enabled_disk_non_resp => EnabledDiskNonResp,
                        time_period_disk_non_resp => DiskNonRespTimePeriod,
                        stats_collector => undefined,
                        latest_stats => {undefined, dict:new()}}).

handle_call(get_statuses, _From, MonitorState) ->
    #{buckets := Buckets} = MonitorState,
    RV = dict:fold(
           fun(Bucket, {Status, _}, Acc) ->
                   [{Bucket, Status} | Acc]
           end, [], Buckets),
    {reply, RV, MonitorState};

handle_call(Call, From, State) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, State]),
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, State]),
    {noreply, State}.

handle_info(refresh, #{enabled_disk_issues := false,
                       enabled_disk_non_resp := false} = MonitorSate) ->
    {noreply, MonitorSate};
handle_info(refresh, MonitorState) ->
    IssuesBuckets = maybe_check_for_disk_issues(MonitorState),
    NonRespBuckets = maybe_check_for_disk_non_responsiveness(MonitorState),
    %% We club together multiple unhealthy statuses into io_failed. This keeps
    %% with past behaviour, and should be indicative enough that the
    %% Administrator needs to look into the IO sub-system.
    Merged = dict:merge(
        fun(_Bucket, DiskIssues, NonResp) ->
            {DiskIssuesState, DiskIssuesValue} = DiskIssues,
            {NonRespState, NonRespValue} = NonResp,
            NewValue = DiskIssuesValue ++ NonRespValue,
            case DiskIssuesState of
                active -> {NonRespState, NewValue};
                io_failed -> {io_failed, NewValue};
                _ ->
                    case NonRespState of
                        active -> {DiskIssuesState, NewValue};
                        _ -> {io_failed, NewValue}
                    end
            end
        end, IssuesBuckets, NonRespBuckets),

    NewState =
        maybe_spawn_stats_collector(
          MonitorState#{buckets => Merged,
                        latest_stats => {undefined, dict:new()}}),
    {noreply, NewState};

handle_info({event, buckets}, MonitorState) ->
    #{buckets := Dict} = MonitorState,
    NewBuckets0 = ns_bucket:node_bucket_names_of_type(node(), persistent),
    NewBuckets = lists:sort(NewBuckets0),
    KnownBuckets = lists:sort(dict:fetch_keys(Dict)),
    ToRemove = KnownBuckets -- NewBuckets,
    ToAdd = NewBuckets -- KnownBuckets,
    NewDict0 = lists:foldl(
                 fun (Bucket, Acc) ->
                         dict:erase(Bucket, Acc)
                 end, Dict, ToRemove),
    NewDict = lists:foldl(
                fun (Bucket, Acc) ->
                        dict:store(Bucket, {active, []}, Acc)
                end, NewDict0, ToAdd),
    {noreply, MonitorState#{buckets => NewDict}};

handle_info({event, auto_failover_cfg},
            #{enabled_disk_issues := DiskIssuesWasEnabled,
              enabled_disk_non_resp := DiskNonRespWasEnabled} = MonitorState) ->
    AFOCfg = auto_failover:get_cfg(),
    {EnabledDiskIssues, NumSamplesDiskIssues} =
        get_failover_on_disk_issues(AFOCfg, MonitorState),
    {EnabledDiskNonResp, DiskNonRespTimePeriod} =
        get_failover_on_disk_non_responsiveness(AFOCfg),

    NewState =
        case EnabledDiskIssues orelse EnabledDiskNonResp of
            false -> MonitorState;
            true ->
                %% One is enabled, if something changed then we will
                %% reset our tracked state
                case EnabledDiskIssues andalso not DiskIssuesWasEnabled orelse
                     EnabledDiskNonResp andalso not DiskNonRespWasEnabled of
                    false -> MonitorState;
                    true -> MonitorState#{buckets => reset_bucket_info()}
                end
        end,

    NewSettings = #{enabled_disk_issues => EnabledDiskIssues,
                    num_samples_disk_issues => NumSamplesDiskIssues,
                    enabled_disk_non_resp => EnabledDiskNonResp,
                    time_period_disk_non_resp => DiskNonRespTimePeriod},
    ?log_debug("auto_failover_cfg change, new settings ~p", [NewSettings]),
    {noreply, maps:merge(NewState, NewSettings)};

handle_info({Pid, BucketStats}, MonitorState) ->
    #{stats_collector := Pid} = MonitorState,
    TS = os:system_time(millisecond),
    {noreply, MonitorState#{stats_collector => undefined,
                            latest_stats => {TS, BucketStats}}};

handle_info(Info, State) ->
    ?log_warning("Unexpected message ~p when in state:~n~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% APIs

get_reason({io_failed, Buckets}) ->
    {"Disk reads and writes failed on following buckets: " ++
         string:join(Buckets, ", ") ++ ".", io_failed};
get_reason({read_failed, Buckets}) ->
    {"Disk reads failed on following buckets: " ++
         string:join(Buckets, ", ") ++ ".", read_failed};
get_reason({write_failed, Buckets}) ->
    {"Disk writes failed on following buckets: " ++
         string:join(Buckets, ", ") ++ ".", write_failed};
get_reason({write_slow, Buckets}) ->
    {"Disk writes are too slow on following buckets: " ++
        string:join(Buckets, ", ") ++ ".", write_slow};
get_reason({read_slow, Buckets}) ->
    {"Disk reads are too slow on following buckets: " ++
        string:join(Buckets, ", ") ++ ".", read_slow}.

is_failure(Failure) ->
    lists:member(Failure, get_errors()).

analyze_status(Buckets) ->
    DiskErrs = get_errors(),
    lists:foldl(
      fun ({B, State}, Acc) ->
              case lists:member(State, DiskErrs) of
                  true ->
                      case lists:keyfind(State, 1, Acc) of
                          false ->
                              [{State, [B]} | Acc];
                          {State, Bs} ->
                              lists:keyreplace(State, 1, Acc, {State, [B | Bs]})
                      end;
                  false ->
                      Acc
              end
      end, [], Buckets).

can_refresh(_State) ->
    true.

get_statuses() ->
    gen_server:call(?MODULE, get_statuses).

%% Internal functions
get_errors() ->
    [io_failed | [Err || {_, Err} <- failure_stats() ++ slow_stats()]].

reset_bucket_info() ->
    Buckets = ns_bucket:node_bucket_names_of_type(node(), persistent),
    lists:foldl(
      fun (Bucket, Acc) ->
              dict:store(Bucket, {active, []}, Acc)
      end, dict:new(), Buckets).

failure_stats() ->
    %% Memcached stat that we read, mapped to failure reason that we give.
    %% We return io_failed in the event of multiple failures.
    [{ep_data_read_failed, read_failed},
     {ep_data_write_failed, write_failed}].

slow_stats() ->
    %% Similar to failure_stats(), but we are pairing two stats together,
    %% pending_disk_..._num and pending_disk_..._slow_num. The former tracks
    %% the number of disk operations in progress, the latter, the number which
    %% are slower than the amount (in seconds) passed in the stat call.
    [{{pending_disk_read_num, pending_disk_read_slow_num}, read_slow},
     {{pending_disk_write_num, pending_disk_write_slow_num}, write_slow}].

get_latest_stats(Bucket, #{enabled_disk_issues := EFDF,
                           enabled_disk_non_resp := EFDNR,
                           time_period_disk_non_resp := TPDNR}) ->
    DFStats =
        case EFDF of
                true ->
                    get_stats(Bucket, <<"disk-failures">>);
                false -> []
        end,

    DNRStats =
        case EFDNR of
                true ->
                    Key = "disk-slowness " ++ integer_to_list(TPDNR),
                    get_stats(Bucket, list_to_binary(Key));
                false -> []
        end,

    DFStats ++ DNRStats.

get_stats(Bucket, Stat) ->
    try ns_memcached:stats(Bucket, Stat) of
        {ok, RawStats} ->
            [{binary_to_atom(K, latin1), binary_to_integer(V)}
                 || {K, V} <- RawStats];
        Err ->
            ?log_debug("Error ~p while trying to read ~p stats for "
            "bucket ~p", [Err, Stat, Bucket]),
            []
    catch
        _:E ->
            ?log_debug("Exception ~p while trying to read ~p stats "
            "for bucket ~p", [E, Stat, Bucket]),
            []
    end.

maybe_check_for_disk_non_responsiveness(#{enabled_disk_non_resp := false}) ->
    dict:new();
maybe_check_for_disk_non_responsiveness(#{enabled_disk_non_resp := true} =
                                            State) ->
    #{buckets := Buckets,
      latest_stats := {_TS, Stats}} = State,
    check_for_disk_non_responsiveness(Buckets, Stats).

check_for_disk_non_responsiveness(Buckets, Stats) ->
    dict:map(
        fun (Bucket, Info) ->
            case dict:find(Bucket, Stats) of
                {ok, BucketStats} ->
                    check_for_disk_non_responsiveness_stats(BucketStats, Info);
                error ->
                    Info
            end
        end, Buckets).

check_for_disk_non_responsiveness_stats(Stats, {_Bucket, _PastInfo}) ->
    %% Similarly to the disk_issues stats, we have extra stats in here, the disk
    %% failure ones, so we will filter them out before proceeding.
    FilteredStats =
        lists:filter(
            fun({Stat, _Value}) ->
                %% A touch weird because slow_stats() keys contain a tuple and
                %% we don't have the full context to match both sides of it
                %% because the keys come from memcached in a flat list. We want
                %% to match either side of the key tuple.
                lists:any(
                    fun({{FS, _}, _}) when Stat =:= FS -> true;
                       ({{_, FS}, _}) when Stat =:= FS-> true;
                       (_) -> false
                    end, slow_stats())
            end, Stats),

    F = lists:foldl(
        fun({{IONum, IOSlow}, FailureType}, Acc) ->
            IOCount = proplists:get_value(IONum, FilteredStats),
            IOSlowCount = proplists:get_value(IOSlow, FilteredStats),
            case proplists:get_value(IONum, FilteredStats) of
                %% We should find stats, but the disk issues code treats a lack
                %% of stats as healthy so we are doing the same.
                undefined -> Acc;
                %% 0 is a special case, no IO in progress, it should not drive
                %% a fail over.
                0 -> Acc;
                IOCount ->
                    case proplists:get_value(IOSlow, FilteredStats) of
                        %% Again, we should find the stats, but the disk issues
                        %% code treats this sort of thing as healthy so we
                        %% continue to do the same here.
                        undefined -> Acc;
                        IOSlowCount when IOCount =< IOSlowCount ->
                            Acc ++ [FailureType];
                        _ -> Acc
                    end
            end
        end, [], slow_stats()),

    BucketStatus = case F of
                       [] ->
                           active;
                       [Err] ->
                           Err;
                       [_|_] ->
                           io_failed
                   end,


    {BucketStatus, FilteredStats}.

maybe_check_for_disk_issues(#{enabled_disk_issues := false}) ->
    dict:new();
maybe_check_for_disk_issues(#{enabled_disk_issues := true,
                              num_samples_disk_issues := NumSamples} = State) ->
    #{buckets := Buckets,
      latest_stats := {TS, Stats}} = State,
    check_for_disk_issues(Buckets, TS, Stats, NumSamples).

check_for_disk_issues(Buckets, TS, LatestStats, NumSamples) ->
    dict:map(
      fun (Bucket, Info) ->
              case dict:find(Bucket, LatestStats) of
                  {ok, Stats} ->
                      check_for_disk_issues_stats(TS, Stats, Info, NumSamples);
                  error ->
                      Info
              end
      end, Buckets).

check_for_disk_issues_stats(CurrTS, Vals, {_, PastInfo}, NumSamples) ->
    %% Vals is of the form: [{stat1, CurrVal1}, {stat2, CurrVal2}, ...]}
    %% PastInfo is of the form:
    %%      [{stat1, {PrevVal1, PrevTS1, BitString}},
    %%       {stat2, {PrevVal2, PrevTS2, BitString}}, ...]
    %% If current value of a stat is greater than its previous value,
    %% then append "1" to the bit string. Otherwise append "0".
    NewStatsInfo =
        lists:filtermap(
          fun ({Stat, CurrVal}) ->
                %% We're going to filter out anything not in failure_stats. We
                %% have to process the other (slow/non-responsiveness) stats in
                %% a different way.
                case proplists:is_defined(Stat, failure_stats()) of
                    false -> false;
                    true ->
                        NewBits =
                            case lists:keyfind(Stat, 1, PastInfo) of
                                false ->
                                    register_tick(true, <<>>, NumSamples);
                                {Stat, {PrevVal, PrevTS, Bits}} ->
                                    Healthy =
                                        CurrTS =:= PrevTS orelse
                                            CurrVal =< PrevVal,
                                    register_tick(Healthy, Bits, NumSamples)
                            end,
                        {true, {Stat, {CurrVal, CurrTS, NewBits}}}
                end
          end, Vals),
    check_for_disk_issues_stats_inner(NewStatsInfo, NumSamples).

check_for_disk_issues_stats_inner(StatsInfo, NumSamples) ->
    Threshold = round(NumSamples * ?DISK_ISSUE_THRESHOLD / 100),
    Failures = lists:filtermap(
                 fun ({Stat, {_, _, Bits}}) ->
                         case is_unhealthy(Bits, Threshold) of
                             true ->
                                 Err = proplists:get_value(Stat,
                                                           failure_stats()),
                                 {true, Err};
                             false ->
                                 false
                         end
                 end, StatsInfo),
    BucketStatus = case Failures of
                       [] ->
                           active;
                       [Err] ->
                           Err;
                       [_|_] ->
                           io_failed
                   end,
    {BucketStatus, StatsInfo}.

register_tick(Healthy, Bits, NumSamples) ->
    B = case Healthy of
            true ->
                <<0:1>>;
            false ->
                <<1:1>>
        end,
    remove_old_entries(<<Bits/bits, B/bits>>, NumSamples).

remove_old_entries(Bits, NumSamples) ->
    Size = bit_size(Bits),
    case Size > NumSamples of
        true ->
            N = Size - NumSamples,
            <<_H:N/bits, Rest/bits>> = Bits,
            Rest;
        false ->
            Bits
    end.

is_unhealthy(Bits, Threshold) ->
    Size = bit_size(Bits),
    case <<0:Size>> =:= Bits of
        true ->
            false;
        false ->
            case Size < Threshold of
                true ->
                    %% Auto-failover on disk issues is disabled
                    %% by default. When user turns it ON or increases
                    %% the timeperiod, there will be a short period before
                    %% the Size catches up with the Threshold.
                    false;
                false ->
                    AllOnes = <<  <<1:1>> ||  _N <- lists:seq(1,Size)  >>,
                    case AllOnes =:= Bits of
                        true ->
                            true;
                        false ->
                            over_threshold(Bits, Threshold)
                    end
            end
    end.

over_threshold(_Bits, 0) ->
    true;
over_threshold(<<>>, _Threshold) ->
    false;
over_threshold(<<1:1, Rest/bits>>, Threshold) ->
    over_threshold(Rest, Threshold - 1);
over_threshold(<<0:1, Rest/bits>>, Threshold) ->
    over_threshold(Rest, Threshold).

get_failover_on_disk_issues(Config, MonitorState) ->
    #{refresh_interval := RefreshInterval} = MonitorState,
    case menelaus_web_auto_failover:get_failover_on_disk_issues(Config) of
        undefined ->
            {false, nil};
        {Enabled, TimePeriod} ->
            NumSamples = round((TimePeriod * 1000)/RefreshInterval),
            {Enabled, NumSamples}
    end.

get_failover_on_disk_non_responsiveness(Config) ->
    case menelaus_web_auto_failover:get_failover_on_disk_non_responsiveness(
             Config) of
        undefined ->
            {false, nil};
        {Enabled, TimePeriod} ->
            {Enabled, TimePeriod}
    end.

-spec maybe_spawn_stats_collector(map()) -> map().
maybe_spawn_stats_collector(#{stats_collector := undefined} = MonitorState) ->
    #{buckets := Buckets} = MonitorState,
    Self = self(),
    Pid = proc_lib:spawn_link(
            fun () ->
                    Res = dict:map(fun (Bucket, _Info) ->
                                           get_latest_stats(Bucket,
                                                            MonitorState)
                                   end, Buckets),
                    Self ! {self(), Res}
            end),
    MonitorState#{stats_collector => Pid};
maybe_spawn_stats_collector(#{stats_collector := Pid} = MonitorState) ->
    ?log_warning("Ignoring start of stats collector as the previous one "
                 "haven't finished yet: ~p", [Pid]),
    MonitorState.

-ifdef(TEST).
%% See health_monitor.erl for tests common to all monitors that use these
%% functions
common_test_setup() ->
    ?meckNew(chronicle_compat_events),
    meck:expect(chronicle_compat_events,
                subscribe,
                fun (_) ->
                        ok
                end),

    ?meckNew(auto_failover, [passthrough]),
    meck:expect(auto_failover, get_cfg, fun() -> [{enabled,true}] end),

    ?meckNew(ns_bucket, [passthrough]),
    meck:expect(ns_bucket, node_bucket_names, fun(_) -> [] end),
    meck:expect(ns_bucket, node_bucket_names_of_type, fun(_, persistent) -> []
                                                      end).

health_monitor_test_setup() ->
   common_test_setup().

health_monitor_t() ->
    {state, kv_stats_monitor, #{enabled_disk_issues := Enabled1}}
        = sys:get_state(?MODULE),
    ?assertNot(Enabled1),

    meck:expect(
      auto_failover, get_cfg,
      fun() ->
              [{enabled, true},
               %% timeout is the time (in seconds) a node needs to be down
               %% before it is automatically fail-overed
               {timeout, 120},
               {failover_on_data_disk_issues, [{enabled, true},
                                               {timePeriod, 120}]}]
      end),


    ?MODULE ! {event, auto_failover_cfg},

    %% Do a call to make sure that we process the previous info message
    get_statuses(),

    {state, kv_stats_monitor, #{enabled_disk_issues := Enabled2}}
        = sys:get_state(?MODULE),
    ?assert(Enabled2),

    {state, kv_stats_monitor, #{buckets := Buckets1}} = sys:get_state(?MODULE),
    ?assertEqual(dict:new(), Buckets1),

    meck:expect(ns_bucket, node_bucket_names_of_type,
                fun(_, persistent) ->
                        ["default"]
                end),

    ?MODULE ! {event, buckets},

    %% Do a call to make sure that we process the previous info message
    get_statuses(),

    {state, kv_stats_monitor, #{buckets := Buckets2}} = sys:get_state(?MODULE),
    ?assertNotEqual(dict:new(), Buckets2).

common_test_teardown() ->
    ?meckUnload(auto_failover),
    ?meckUnload(ns_bucket).

health_monitor_test_teardown() ->
    common_test_teardown().

-endif.
