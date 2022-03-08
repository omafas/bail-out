%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% High Level Steps:
%% 1. For each couchstore bucket, get the EP-engine disk failure stats
%% from the stats archiver.
%% 2. Compare each stat sample with its previous value and
%% count the # of times the stat has incremented during the user configured
%% time period.
%% If the above count is over some threshold, then it indicates sustained
%% failure.
%% 3. If any of the stats show sustained failure then KV stats monitor
%% will report I/O error for the corresponding bucket.
%%
%% Since we are looking for sustained failure, we are not interested
%% in the value of the stat itself but rather the number of samples
%% where the stat has increased. The threshold is for the number of samples.
%% E.g. A timePeriod of 100s has 100 stat samples (one per second). If 60
%% of those samples show an increment over the previous sample then that
%% is considered a sustained failure.
%% EP engine retry policy for write failure is to retry the write every second
%% and indefinitely. As long as the disk failure continues to exist,
%% the write related failure stat will continue to increase. This is
%% irrespective of whether the client continues to perform writes or not.
%% As a result, more or less every sample of the write related failure stats
%% should show an increment over the previous one.
%% EP engine's retry policy for reads is different. It does not retry reads
%% on read failure. The read related failure stat will continue to increase
%% as long as the client is performing read ops and the disk failure
%% continues to exist.
%%
-module(kv_stats_monitor).

-behaviour(gen_server).

-include("ns_common.hrl").

%% Frequency at which stats are checked
-define(REFRESH_INTERVAL, ?get_param(refresh_interval, 2000)). % 2 seconds
%% Percentage threshold
-define(DISK_ISSUE_THRESHOLD, ?get_param(disk_issue_threshold, 60)).

-export([start_link/0]).
-export([get_buckets/0,
         get_reason/1,
         analyze_status/1,
         is_failure/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([register_tick/3,
         is_unhealthy/2]).

-record(state, {
          buckets :: dict:dict(),
          %% Monitor disk failure stats only if auto-failover on
          %% disk issues is enabled.
          enabled = false :: boolean(),
          %% Number of stats samples to monitor - depends on timePeriod
          %% set by the user and the REFRESH_INTERVAL.
          numSamples = nil :: nil | integer(),
          refresh_timer_ref = undefined,
          stats_collector = undefined,
          latest_stats = {undefined, dict:new()}
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
    Self = self(),
    Self ! refresh,

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
    {Enabled, NumSamples} = get_failover_on_disk_issues(
                              auto_failover:get_cfg()),
    {ok, maybe_spawn_stats_collector(#state{buckets = reset_bucket_info(),
                                            enabled = Enabled,
                                            numSamples = NumSamples})}.

handle_call(get_buckets, _From, #state{buckets = Buckets} = State) ->
    RV = dict:fold(
           fun(Bucket, {Status, _}, Acc) ->
                   [{Bucket, Status} | Acc]
           end, [], Buckets),
    {reply, RV, State};

handle_call(Call, From, State) ->
    ?log_warning("Unexpected call ~p from ~p when in state:~n~p",
                 [Call, From, State]),
    {reply, nack, State}.

handle_cast(Cast, State) ->
    ?log_warning("Unexpected cast ~p when in state:~n~p", [Cast, State]),
    {noreply, State}.

handle_info(refresh, #state{enabled = false} = State) ->
    {noreply, State};
handle_info(refresh, #state{buckets = Buckets,
                            numSamples = NumSamples,
                            latest_stats = {TS, Stats}} = State) ->
    NewBuckets = check_for_disk_issues(Buckets, TS, Stats, NumSamples),
    NewState = maybe_spawn_stats_collector(
                 State#state{buckets = NewBuckets,
                             latest_stats = {undefined, dict:new()}}),
    {noreply, resend_refresh_msg(NewState)};

handle_info({event, buckets}, #state{buckets = Dict} = State) ->
    NewBuckets0 = ns_bucket:get_bucket_names_of_type(persistent),
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
    {noreply, State#state{buckets = NewDict}};

handle_info({event, auto_failover_cfg},
            #state{enabled = OldEnabled} = State) ->
    {Enabled, NumSamples} =
        get_failover_on_disk_issues(auto_failover:get_cfg()),
    NewState = case Enabled of
                     OldEnabled -> State;
                     false -> State#state{buckets = reset_bucket_info()};
                     true -> resend_refresh_msg(State)
                 end,
    ?log_debug("auto_failover_cfg change enabled:~p numSamples:~p ",
               [Enabled, NumSamples]),
    {noreply, NewState#state{enabled = Enabled, numSamples = NumSamples}};

handle_info({Pid, BucketStats}, #state{stats_collector = Pid} = State) ->
    TS = os:system_time(millisecond),
    {noreply, State#state{stats_collector = undefined,
                          latest_stats = {TS, BucketStats}}};

handle_info(Info, State) ->
    ?log_warning("Unexpected message ~p when in state:~n~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% APIs
get_buckets() ->
    gen_server:call(?MODULE, get_buckets).

get_reason({io_failed, Buckets}) ->
    {"Disk reads and writes failed on following buckets: " ++
         string:join(Buckets, ", ") ++ ".", io_failed};
get_reason({read_failed, Buckets}) ->
    {"Disk reads failed on following buckets: " ++
         string:join(Buckets, ", ") ++ ".", read_failed};
get_reason({write_failed, Buckets}) ->
    {"Disk writes failed on following buckets: " ++
         string:join(Buckets, ", ") ++ ".", write_failed}.

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

%% Internal functions
get_errors() ->
    [io_failed | [Err || {_, Err} <- failure_stats()]].

reset_bucket_info() ->
    Buckets = ns_bucket:node_bucket_names_of_type(node(), persistent),
    lists:foldl(
      fun (Bucket, Acc) ->
              dict:store(Bucket, {active, []}, Acc)
      end, dict:new(), Buckets).

failure_stats() ->
    [{ep_data_read_failed, read_failed},
     {ep_data_write_failed, write_failed}].

get_latest_stats(Bucket) ->
    try ns_memcached:stats(Bucket, <<"disk-failures">>) of
        {ok, RawStats} ->
            [{binary_to_atom(K, latin1), binary_to_integer(V)}
                 || {K, V} <- RawStats];
        Err ->
            ?log_debug("Error ~p while trying to read disk-failures stats for "
                       "bucket ~p", [Err, Bucket]),
            []
    catch
        _:E ->
            ?log_debug("Exception ~p while trying to read disk-failures stats "
                       "for bucket ~p", [E, Bucket]),
            []
    end.

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
        lists:map(
          fun ({Stat, CurrVal}) ->
                  NewBits =
                      case lists:keyfind(Stat, 1, PastInfo) of
                          false ->
                              register_tick(true, <<>>, NumSamples);
                          {Stat, {PrevVal, PrevTS, Bits}} ->
                              Healthy =
                                  CurrTS =:= PrevTS orelse CurrVal =< PrevVal,
                              register_tick(Healthy, Bits, NumSamples)
                      end,
                  {Stat, {CurrVal, CurrTS, NewBits}}
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

get_failover_on_disk_issues(Config) ->
    case menelaus_web_auto_failover:get_failover_on_disk_issues(Config) of
        undefined ->
            {false, nil};
        {Enabled, TimePeriod} ->
            NumSamples = round((TimePeriod * 1000)/?REFRESH_INTERVAL),
            {Enabled, NumSamples}
    end.

resend_refresh_msg(#state{refresh_timer_ref = undefined} = State) ->
    Ref = erlang:send_after(?REFRESH_INTERVAL, self(), refresh),
    State#state{refresh_timer_ref = Ref};
resend_refresh_msg(#state{refresh_timer_ref = Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    resend_refresh_msg(State#state{refresh_timer_ref = undefined}).

maybe_spawn_stats_collector(#state{stats_collector = undefined,
                                   buckets = Buckets} = State) ->
    Self = self(),
    Pid = proc_lib:spawn_link(
            fun () ->
                Res = dict:map(fun (Bucket, _Info) ->
                                   get_latest_stats(Bucket)
                               end, Buckets),
                Self ! {self(), Res}
            end),
    State#state{stats_collector = Pid};
maybe_spawn_stats_collector(#state{stats_collector = Pid} = State) ->
    ?log_warning("Ignoring start of stats collector as the previous one "
                 "haven't finished yet: ~p", [Pid]),
    State.

