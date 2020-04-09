%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2018 Couchbase, Inc.
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

-record(state, {
          buckets :: dict:dict(),
          %% Monitor disk failure stats only if auto-failover on
          %% disk issues is enabled.
          enabled = false :: boolean(),
          %% Number of stats samples to monitor - depends on timePeriod
          %% set by the user and the REFRESH_INTERVAL.
          numSamples = nil :: nil | integer()
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
    self() ! refresh,
    {Enabled, NumSamples} = get_failover_on_disk_issues(),
    ns_pubsub:subscribe_link(ns_config_events,
                             fun handle_config_event/2, self()),
    {ok, #state{buckets = reset_bucket_info(),
                enabled = Enabled, numSamples = NumSamples}}.

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
                            numSamples = NumSamples} = State) ->
    NewBuckets = check_for_disk_issues(Buckets, NumSamples),
    timer2:send_after(?REFRESH_INTERVAL, self(), refresh),
    {noreply, State#state{buckets = NewBuckets}};

handle_info({buckets, Buckets}, #state{buckets = Dict} = State) ->
    BucketConfigs = proplists:get_value(configs, Buckets, []),
    NewBuckets0 = ns_bucket:get_bucket_names_of_type({membase, couchstore},
                                                     BucketConfigs),
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

handle_info({auto_failover_cfg, NewCfg},
            #state{enabled = OldEnabled, buckets = Buckets} = State) ->
    {Enabled, NumSamples} = get_failover_on_disk_issues(NewCfg),
    NewBuckets = case Enabled of
                     OldEnabled ->
                         Buckets;
                     false ->
                         reset_bucket_info();
                     true ->
                         timer2:send_after(?REFRESH_INTERVAL, self(), refresh),
                         Buckets
                 end,
    ?log_debug("auto_failover_cfg change enabled:~p numSamples:~p ",
               [Enabled, NumSamples]),
    {noreply, State#state{buckets = NewBuckets,
                          enabled = Enabled, numSamples = NumSamples}};

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
    Buckets = ns_bucket:node_bucket_names_of_type(node(), membase, couchstore),
    lists:foldl(
      fun (Bucket, Acc) ->
              dict:store(Bucket, {active, []}, Acc)
      end, dict:new(), Buckets).

failure_stats() ->
    [{ep_data_read_failed, read_failed},
     {ep_data_write_failed, write_failed}].

get_latest_stats(Bucket, Stats) ->
    try stats_reader:latest_specific_stats(minute, node(), Bucket, Stats) of
        Result ->
            Result
    catch exit:{noproc, _} ->
            {ok, []}
    end.

check_for_disk_issues(Buckets, NumSamples) ->
    dict:map(
      fun (Bucket, Info) ->
              check_for_disk_issues_bucket(Bucket, Info, NumSamples)
      end, Buckets).

check_for_disk_issues_bucket(Bucket, Info, NumSamples) ->
    Stats = [S || {S, _} <- failure_stats()],
    case get_latest_stats(Bucket, Stats) of
        {ok, []} ->
            Info;
        {ok, {stat_entry, TS, Vals}} ->
            check_for_disk_issues_stats(TS, Vals, Info, NumSamples);
        {error, Err} ->
            ?log_debug("Error ~p while trying to read stats ~p for bucket ~p",
                       [Err, Stats, Bucket]),
            Info
    end.

check_for_disk_issues_stats(CurrTS, Vals, {_, PastInfo}, NumSamples) ->
    %% Vals is of the form: [{stat1, CurrVal1}, {stat2, CurrVal2}, ...]}
    %% PastInfo is of the form:
    %%      [{stat1, {PrevVal1, PrevTS1, BitString}},
    %%       {stat2, {PrevVal2, PrevTS2, BitString}}, ...]
    %% If current value of a stat is greater than its previous value,
    %% then append "1" to the bit string. Otherwise append "0".
    NewStatsInfo = lists:map(
                     fun ({Stat, CurrVal}) ->
                             case lists:keyfind(Stat, 1, PastInfo) of
                                 false ->
                                     {Stat, {CurrVal, CurrTS, <<0:1>>}};
                                 {Stat, PrevInfo} ->
                                     New = process_stat(CurrVal, CurrTS,
                                                        PrevInfo, NumSamples),
                                     {Stat, New}
                             end
                     end, Vals),
    check_for_disk_issues_stats_inner(NewStatsInfo, NumSamples).

check_for_disk_issues_stats_inner(StatsInfo, NumSamples) ->
    Threshold = round(NumSamples * ?DISK_ISSUE_THRESHOLD / 100),
    Failures = lists:filtermap(
                 fun ({Stat, {_, _, Bits}}) ->
                         case is_stat_increasing(Bits, Threshold) of
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

process_stat(CurrVal, CurrTS, {PrevVal, PrevTS, Bits}, NumSamples) ->
    {NewVal, NewTS, NewBits0} = case CurrTS =:= PrevTS of
                                    true ->
                                        {PrevVal, PrevTS, <<Bits/bits, 0:1>>};
                                    false ->
                                        B = case CurrVal > PrevVal of
                                                true ->
                                                    <<1:1>>;
                                                false ->
                                                    <<0:1>>
                                            end,
                                        {CurrVal, CurrTS, <<Bits/bits, B/bits>>}
                                end,
    NewBits = remove_old_entries(NewBits0, NumSamples),
    {NewVal, NewTS, NewBits}.

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

is_stat_increasing(Bits, Threshold) ->
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

is_significant_event({buckets, _}) ->
    true;
is_significant_event({auto_failover_cfg, _}) ->
    true;
is_significant_event(_) ->
    false.

handle_config_event(Event, Pid) ->
    case is_significant_event(Event) of
        true ->
            Pid ! Event;
        false ->
            ok
    end,
    Pid.

get_failover_on_disk_issues() ->
    {value, Config} = ns_config:search(ns_config:get(), auto_failover_cfg),
    get_failover_on_disk_issues(Config).

get_failover_on_disk_issues(Config) ->
    case menelaus_web_auto_failover:get_failover_on_disk_issues(Config) of
        undefined ->
            {false, nil};
        {Enabled, TimePeriod} ->
            NumSamples = round((TimePeriod * 1000)/?REFRESH_INTERVAL),
            {Enabled, NumSamples}
    end.
