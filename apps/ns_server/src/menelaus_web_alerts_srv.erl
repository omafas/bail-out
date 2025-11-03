%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(menelaus_web_alerts_srv).

-include("ns_bucket.hrl").
-include("ns_common.hrl").
-include("ns_stats.hrl").
-include_lib("ns_common/include/cut.hrl").

%% needed to mock ns_config in tests
-include("ns_config.hrl").

-define(MAX_DISK_USAGE_MISSED_CHECKS, 2).

%% Indexer resident memory percentage threshold below which an alert will
%% be generated. This is changeable via /settings/alerts/limits
-define(INDEXER_LOW_RESIDENT_PERCENTAGE, 10).

%% Percentage of indexer memory to use in calculation for the above alert.
%% This is needed during indexer restart where the resident percentage is low
%% but in reality there is sufficient memory.
-define(INDEXER_RESIDENT_MEMORY_PCT,
        ?get_param(indexer_resident_memory_pct, 90)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).
-define(SERVER, ?MODULE).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, handle_settings_alerts_limits_post/1,
         handle_settings_alerts_limits_get/1]).

-export([alert_keys/0, config_upgrade_to_76/1,
         config_upgrade_to_79/1]).

%% @doc Hold client state for any alerts that need to be shown in
%% the browser, is used by menelaus_web to piggy back for a transport
%% until a long polling transport is used, will be single user
%% until then, many checks for alerts every ?SAMPLE_RATE milliseconds

-record(state, {
          queue = [],
          history = [],
          opaque = dict:new(),
          checker_pid,
          change_counter = 0
         }).

%% Amount of time to wait between state checks (ms)
-define(SAMPLE_RATE, ?get_timeout(sample_rate, 60000)).

%% Amount of time between sending users the same alert (s)
-define(ALERT_TIMEOUT, 60 * 10).

%% Amount of time to wait between checkout out of disk (s)
-define(DISK_USAGE_TIMEOUT, 60 * 60 * 12).

%% Amount of time to wait before reporting communication issues (s)
-define(COMMUNICATION_ISSUE_TIMEOUT, 60 * 5).

%% These numbers are heuristic, and are based on two facts:
%%  - It is hard to make mem_actual_usage actually reach 100%, which is
%%    connected to the fact that there is always some part of the cache that
%%    is not reclaimable;
%%  - In practice we see that when actual_usage reaches 95%, it might already
%%    be the “car is driving off the cliff” situation, so we should react
%%    earlier.
%%
%% Default memory thresholds (in percents)
-define(MEM_NOTICE_PERC, -1).
-define(MEM_WARN_PERC, 85).
-define(MEM_CRIT_PERC, 90).

%% Default history size threshold
-define(HIST_WARN_PERC, 90).

%% Default memcached connection threshold
-define(MEMCACHED_CONNECTION_THRESHOLD, 90).

%% Number of checks seeing no progress before we trigger an alert. The time
%% between checks is defined as threshold / check frequency
-define(REBALANCE_CHECK_FREQ, ?get_param(stuck_rebalance_check_freq, 3)).

%% The stuck threshold (s) must be at least the number of checks before alerting
%% multiplied by the alert sample rate (converted from ms to s)
-define(MIN_REBALANCE_STUCK_THRESHOLD_S,
        ?REBALANCE_CHECK_FREQ * ?SAMPLE_RATE / 1000).
%% The index rebalance stuck threshold is additionally required to be no
%% less than 10 minutes
-define(MIN_INDEX_REBALANCE_STUCK_THRESHOLD_S,
        max(?get_param(min_index_rebalance_stuck_threshold_s, 600),
            ?MIN_REBALANCE_STUCK_THRESHOLD_S)).

-define(DEFAULT_KV_REBALANCE_STUCK_THRESHOLD_S,
        max(30 * 60,  %% 30mins
            ?MIN_REBALANCE_STUCK_THRESHOLD_S)).
-define(DEFAULT_INDEX_REBALANCE_STUCK_THRESHOLD_S,
        max(30 * 60,  %% 30mins
            ?MIN_INDEX_REBALANCE_STUCK_THRESHOLD_S)).

-define(DEFAULT_MAX_DATA_DISK_USED, 75).

-record(rebalance_progress, {
    progress :: {binary(), any()},
    last_timestamp :: integer(),
    stuck_timestamp :: integer()
}).

-export([start_link/0, stop/0, local_alert/2, global_alert/2,
         fetch_alerts/0, consume_alerts/1, reset/0]).

-type alert_key() :: atom() | {atom(), any()}.

%% short description for a specific error; used in email subject
short_description(ip) ->
    "IP address changed";
short_description(ep_oom_errors) ->
    "heavy memory pressure";
short_description(ep_item_commit_failed) ->
    "write commit failure";
short_description(overhead) ->
    "metadata overhead warning";
short_description(disk) ->
    "approaching full disk warning";
short_description(disk_guardrail) ->
    "approaching disk usage limit for data service mutations";
short_description(audit_dropped_events) ->
    "audit write failure";
short_description(indexer_ram_max_usage) ->
    "indexer ram approaching threshold warning";
short_description(indexer_low_resident_percentage) ->
    "indexer resident percentage is too low";
short_description(ep_clock_cas_drift_threshold_exceeded) ->
    "cas drift threshold exceeded error";
short_description(communication_issue) ->
    "communication issue among some nodes";
short_description(time_out_of_sync) ->
    "node time not in sync";
short_description(disk_usage_analyzer_stuck) ->
    "disks usage worker is stuck and unresponsive";
short_description(cert_expires_soon) ->
    "certificate will expire soon";
short_description(cert_expired) ->
    "certificate has expired";
short_description(memory_threshold) ->
    "system memory usage threshold exceeded";
short_description(history_size_warning) ->
    "history size approaching limit";
short_description(memcached_connections) ->
    "data service connections approaching limit";
short_description(stuck_rebalance) ->
    "rebalance stage appears stuck";
short_description(indexer_diverging_replicas) ->
    "index has diverging replicas";
short_description(Other) ->
    %% this case is needed for tests to work
    couch_util:to_list(Other).

%% Error constants
errors(ip) ->
    "Unable to listen on ~p. (POSIX error code: '~p')";
errors(ep_oom_errors) ->
    "Heavy memory pressure: Bucket \"~s\" on node ~s has reached its quota. The server will attempt to recover from this issue.";
errors(ep_item_commit_failed) ->
    "Write Commit Failure. Disk write failed for item in Bucket \"~s\" on node ~s.";
errors(overhead) ->
    "Metadata overhead warning. Over  ~p% of RAM allocated to bucket  \"~s\" on node \"~s\" is taken up by keys and metadata.";
errors(disk) ->
    "Approaching full disk warning. Usage of disk \"~s\" on node \"~s\" is around ~p%.";
errors(data_disk_warning) ->
    "Warning: Your data disk (~s) is ~p% full on node \"~s\", above the ~p% "
    "warning threshold.";
errors(data_disk_guardrail_warning) ->
    "Warning: Your data disk (~s) is ~p% full on node \"~s\", within 10% of "
    "the configured maximum disk usage for data service mutations (~p%).";
errors(data_disk_guardrail_critical) ->
    "Critical: Your data disk (~s) is ~p% full on node \"~s\", above the "
    "configured maximum disk usage for data service mutations (~p%). "
    "Data service mutations will no longer be permitted, until the disk "
    "usage is brought back below the limit.";
errors(audit_dropped_events) ->
    "Audit Write Failure. Attempt to write to audit log on node \"~s\" was unsuccessful";
errors(indexer_ram_max_usage) ->
    "Warning: approaching max index RAM. Indexer RAM on node \"~s\" is ~p%, which is at or above the threshold of ~p%.";
errors(indexer_low_resident_percentage) ->
    "Warning: approaching low index resident percentage. Indexer RAM "
    "percentage on node \"~s\" is ~p%, which is under the threshold of ~p%.";
errors(ep_clock_cas_drift_threshold_exceeded) ->
    "Remote or replica mutation received for bucket ~p on node ~p with timestamp more "
    "than ~p milliseconds ahead of local clock. Please ensure that NTP is set up correctly "
    "on all nodes across the replication topology and clocks are synchronized.";
errors(communication_issue) ->
    "Warning: Node \"~s\" is having issues communicating with following nodes \"~s\".";
errors(time_out_of_sync) ->
    "The time on node ~p is not synchronized. Please ensure that NTP is set "
        "up correctly on all nodes and that clocks are synchronized.";
errors(disk_usage_analyzer_stuck) ->
    "Disk usage worker is stuck on node \"~s\". Please ensure all mounts are "
        "accessible via \"df\" and consider killing any existing \"df\" "
        "processes.";
errors(node_cert_expires_soon) ->
    "Server certificate for node ~s (subject: '~s') will expire at ~s.";
errors(node_cert_expired) ->
    "Server certificate for node ~s (subject: '~s') has expired.";
errors(ca_expires_soon) ->
    "Trusted CA certificate with ID=~b (subject: '~s') will expire at ~s.";
errors(ca_expired) ->
    "Trusted CA certificate with ID=~b (subject: '~s') has expired.";
errors(client_cert_expired) ->
    "Client certificate on node ~s (subject: '~s') has expired.";
errors(client_cert_expires_soon) ->
    "Client certificate on node ~s (subject: '~s') will expire at ~s.";
errors(client_xdcr_cert_expires_soon) ->
    "Client XDCR certificate (subject: '~s') will expire at ~s.";
errors(client_xdcr_cert_expired) ->
    "Client XDCR certificate (subject: '~s') has expired.";
errors(xdcr_ca_expires_soon) ->
    "XDCR CA certificate (subject: '~s') will expire at ~s.";
errors(xdcr_ca_expired) ->
    "XDCR CA certificate (subject: '~s') has expired.";
errors(memory_critical) ->
    "CRITICAL: On node ~s ~p memory use is ~.2f% of total available "
    "memory, above the critical threshold of ~b%.";
errors(memory_warning) ->
    "Warning: On node ~s ~p memory use is ~.2f% of total available "
    "memory, above the warning threshold of ~b%.";
errors(memory_notice) ->
    "Notice: On node ~s ~p memory use is ~.2f% of total available "
    "memory, above the notice threshold of ~b%.";
errors(history_size_warning) ->
    "Warning: On bucket \"~s\" mutation history is greater than ~b% of history "
    "retention size for at least ~b/~b vbuckets. Please ensure that the "
    "history retention size is sufficiently large, in order for the mutation "
    "history to be retained for the history retention time.";
errors(stuck_rebalance) ->
    "Warning: Rebalance of '~s' (rebalance_id: ~s) appears stuck, no progress "
    "has been made for ~b seconds.";
errors(memcached_connections) ->
    "Warning: On node ~s the number of ~s connections being used by the Data "
    "Service (~p) is above the notice threshold of ~b%. The limit is ~p.";
errors(indexer_diverging_replicas) ->
    "Divergence in item counts between index partition replicas is detected. "
    "This can cause index scan to return inconsistent data. Please identify "
    "the index with GET "
    "/pools/default/stats/range/index_partn_is_diverging_replica and "
    "consider dropping and re-creating it to resolve this".

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%% @doc Send alert to all connected nodes
-spec global_alert(alert_key(), binary() | string()) -> ok.
global_alert(Type, Msg) ->
    ale:info(?USER_LOGGER, to_str(Msg)),
    [rpc:cast(Node, ?MODULE, local_alert, [{Type, node()}, Msg])
     || Node <- [node() | nodes()]],
    ok.


%% @doc Show to user on running node only
-spec local_alert({alert_key(), node()}, binary()) -> ok | ignored.
local_alert(Key, Val) ->
    gen_server:call(?MODULE, {add_alert, Key, Val}).


%% @doc fetch a list of binary string, clearing out the message
%% history
-spec fetch_alerts() -> {[{Key, Message, Time, NoUIPopUp}], Token}
  when Key :: {alert_key(), node()},
       Message :: binary(),
       Time :: pos_integer(),
       NoUIPopUp :: boolean(),
       Token :: binary().
fetch_alerts() ->
    diag_handler:diagnosing_timeouts(
      fun () ->
              gen_server:call(?MODULE, fetch_alert)
      end).

-spec consume_alerts(binary()) -> boolean().
consume_alerts(VersionCookie) ->
    gen_server:call(?MODULE, {consume_alerts, VersionCookie}).

%% Resets history of alerts on that node (used in tests)
-spec reset() -> ok.
reset() ->
    gen_server:call(?MODULE, reset).

stop() ->
    gen_server:cast(?MODULE, stop).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    start_timer(),
    ns_pubsub:subscribe_link(ns_config_events,
                             fun email_config_change_callback/1),
    {ok, #state{}}.


handle_call({consume_alerts, PassedCounter}, _From, #state{change_counter = Counter}=State) ->
    NewState = case (catch list_to_integer(binary_to_list(PassedCounter))) of
                   Counter ->
                       %% match
                       State#state{queue=[],
                                   change_counter=Counter+1};
                   _ ->
                       State
               end,
    {reply, NewState =/= State, NewState};

handle_call(fetch_alert, _From, #state{queue=Alerts0, change_counter=Counter}=State) ->
    %% List of alerts for which we show a UI pop-up.
    PopUps = menelaus_alert:popup_alerts_config(),

    %% Convert monotonic time to the system time and indicate if we want to
    %% suppress alert UI pop-ups.
    Alerts = lists:map(
               fun ({Key, Msg, Time, Offset}) ->
                       {AlertKey0, _Node} = Key,
                       AlertKey = extract_alert_key(AlertKey0),
                       NoPopUp = not lists:member(AlertKey, PopUps),
                       {Key, Msg, Offset + Time, NoPopUp}
               end, Alerts0),

    {reply, {lists:reverse(Alerts), list_to_binary(integer_to_list(Counter))}, State};
handle_call({add_alert, Key, Val}, _, #state{queue = Msgs, history = Hist,
                                             change_counter = Counter} = State) ->
    case lists:keyfind(Key, 1, Hist) of
        false ->
            Time   = erlang:monotonic_time(),
            Offset = erlang:time_offset(),

            MsgTuple = {Key, Val, Time, Offset},
            MaybeSent = maybe_send_out_email_alert(Key, Val),

            {reply, ok,
             State#state{
               history = [erlang:append_element(MsgTuple, MaybeSent) | Hist],
               queue = [MsgTuple | lists:keydelete(Key, 1, Msgs)],
               change_counter = Counter + 1
              }};
        _ ->
            {reply, ignored, State}
    end;

handle_call(reset, _From, #state{} = State) ->
    {reply, ok, State#state{queue = [], history = []}}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(flush_queued_alerts, #state{history = Hist} = State) ->
    ?flush(flush_queued_alerts),
    Config = menelaus_alert:get_config(),
    NewHistory =
        case proplists:get_bool(enabled, Config) of
            true ->
                lists:map(
                  fun ({K, V, T, O, false}) ->
                          {K, V, T, O, maybe_send_out_email_alert(K, V)};
                      (Tuple) ->
                          Tuple
                  end,
                  Hist);
            false ->
                Hist
        end,
    {noreply, State#state{history = NewHistory}};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec is_checker_active(undefined | pid()) -> true | false.
is_checker_active(undefined) -> false;
is_checker_active(Pid) ->
    erlang:is_process_alive(Pid).

handle_info(check_alerts, #state{checker_pid = Pid} = State) ->
    start_timer(),
    case is_checker_active(Pid) of
        true ->
            {noreply, State};
        _ ->
            Self = self(),
            CheckerPid = erlang:spawn_link(fun () ->
                                                   NewOpaque = do_handle_check_alerts_info(State),
                                                   Self ! {merge_opaque_from_checker, NewOpaque}
                                           end),
            {noreply, State#state{checker_pid = CheckerPid}}
    end;

handle_info({merge_opaque_from_checker, NewOpaque},
            #state{history=History} = State) ->
    {noreply, State#state{opaque = NewOpaque,
                          history = expire_history(History),
                          checker_pid = undefined}};

handle_info(_Info, State) ->
    {noreply, State}.

do_handle_check_alerts_info(#state{history=Hist, opaque=Opaque}) ->
    Stats = stats_interface:for_alerts(),
    StatsOrddict = orddict:from_list([{K, orddict:from_list(V)}
                                          || {K, V} <- Stats]),
    check_alerts(Opaque, Hist, StatsOrddict).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

alert_keys() ->
    [ip, disk, overhead, ep_oom_errors, ep_item_commit_failed,
     audit_dropped_events, indexer_ram_max_usage,
     indexer_low_resident_percentage,
     ep_clock_cas_drift_threshold_exceeded,
     communication_issue, time_out_of_sync, disk_usage_analyzer_stuck,
     cert_expires_soon, cert_expired, memory_threshold, history_size_warning,
     stuck_rebalance, memcached_connections, disk_guardrail,
     indexer_diverging_replicas].

config_upgrade_to_76(Config) ->
    Ret = case ns_config:search(Config, email_alerts) of
              false ->
                  [];
              {value, EmailAlerts} ->
                  upgrade_alerts(
                    EmailAlerts,
                    [add_proplist_list_elem(alerts, cert_expired, _),
                     add_proplist_list_elem(pop_up_alerts, cert_expired, _),
                     add_proplist_list_elem(alerts, cert_expires_soon, _),
                     add_proplist_list_elem(pop_up_alerts,
                                            cert_expires_soon, _),
                     add_proplist_list_elem(alerts, memcached_connections, _),
                     add_proplist_list_elem(pop_up_alerts,
                                            memcached_connections, _),
                     move_memory_alert_email_alerts(alerts,
                                                    memory_alert_email, _),
                     move_memory_alert_email_alerts(pop_up_alerts,
                                                    memory_alert_popup, _)] ++
                        [add_proplist_list_elem(pop_up_alerts, A, _)
                         || A <- auto_failover:alert_keys()])
          end,
    %% MB-53122 noted that upgrades from 6.6 did not enable auto failover pop up
    %% alerts. We fixed this issue via upgrades, but we had already released
    %% early versions of 7.0 and without a compat mode change could not fix the
    %% issue for customers already on 7.0 via upgrade. Instead, startup of this
    %% module would enable the alerts and write the config key
    %% "popup_alerts_auto_failover_upgrade_70_fixed" to prevent them from being
    %% enabled over and over. Upgrade removes the need for the key, and so we
    %% can tidy it up here.
    Ret ++ [{delete, popup_alerts_auto_failover_upgrade_70_fixed}].

config_upgrade_to_79(Config) ->
    case ns_config:search(Config, email_alerts) of
          false ->
              [];
          {value, EmailAlerts} ->
              upgrade_alerts(
                EmailAlerts,
                maybe_delete_stuck_rebalance_keys(Config) ++
                [add_proplist_list_elem(alerts, disk_guardrail, _),
                 add_proplist_list_elem(pop_up_alerts, disk_guardrail, _)])
    end.

%% @doc Sends any previously queued email alerts. Generally called when we first
%% enable the email alerts and we need to flush any existing alerts that haven't
%% been sent yet. Only done through gen_server callback because we need access
%% to process state.
maybe_send_queued_email() ->
    gen_server:cast(?MODULE, flush_queued_alerts).

email_config_change_callback({email_alerts, AlertCfg}) ->
    case proplists:get_bool(enabled, AlertCfg) of
        true ->
            maybe_send_queued_email();
        false ->
            ok
    end;
email_config_change_callback(_) ->
    ok.

%% @doc Remind myself to check the alert status
start_timer() ->
    erlang:send_after(?SAMPLE_RATE, self(), check_alerts).


%% @doc global checks for any server specific problems locally then
%% broadcast alerts to clients connected to any particular node
global_checks() ->
    [oom, ip, write_fail, overhead, disk, audit_write_fail,
     indexer_ram_max_usage, cas_drift_threshold, communication_issue,
     time_out_of_sync, disk_usage_analyzer_stuck, certs, xdcr_certs,
     memory_threshold, history_size_warning, indexer_low_resident_percentage,
     stuck_rebalance, memcached_connections, disk_guardrail,
     indexer_diverging_replicas].

%% @doc fires off various checks
check_alerts(Opaque, Hist, Stats) ->
    Fun = fun(X, Dict) -> check(X, Dict, Hist, Stats) end,
    lists:foldl(Fun, Opaque, global_checks()).


%% @doc if listening on a non localhost ip, detect differences between
%% external listening host and current node host
-spec check(atom(), dict:dict(), list(), [{atom(),number()}]) -> dict:dict().
check(ip, Opaque, _History, _Stats) ->
    {_Name, Host} = misc:node_name_host(node()),
    case can_listen(Host) of
        {false, Error} ->
            global_alert(ip, fmt_to_bin(errors(ip), [node(), Error]));
        true ->
            ok
    end,
    Opaque;

check(disk_usage_analyzer_stuck, Opaque, _History, _Stats) ->
    IsStale = ns_disksup:is_stale(),
    Missed = case dict:find(disk_usage_missed_checks, Opaque) of
                 {ok, M} -> M;
                 error -> 0
             end,

    NewMissed = case {IsStale, Missed} of
                    {false, _} ->
                        0;
                    {true, Missed}
                      when Missed < ?MAX_DISK_USAGE_MISSED_CHECKS ->
                        Missed + 1;
                    {true, Missed} ->
                        global_alert(disk_usage_analyzer_stuck,
                                     fmt_to_bin(
                                       errors(disk_usage_analyzer_stuck),
                                       [node()])),
                        Missed + 1
                end,
    dict:store(disk_usage_missed_checks, NewMissed, Opaque);

%% @doc check the capacity of the drives used for db and log files
check(disk, Opaque, _History, _Stats) ->
    Mounts = ns_disksup:get_disk_data(),
    UsedPre = [ns_storage_conf:this_node_dbdir(),
               ns_storage_conf:this_node_ixdir(),
               ns_storage_conf:this_node_logdir(),
               ns_audit_cfg:get_log_path()],
    ExtractStats = ns_storage_conf:extract_disk_stats_for_path(Mounts, _),
    UsedMountsTmp = [X || {ok, X} <- [begin
                                          case misc:realpath(Mnt, "/") of
                                              {ok, RealFile} ->
                                                  ExtractStats(RealFile);
                                              _ ->
                                                  none
                                          end
                                      end || {ok, Mnt} <- UsedPre]],
    UsedMounts = sets:to_list(sets:from_list(UsedMountsTmp)),
    {value, Config} = ns_config:search(alert_limits),
    MaxDiskUsed = proplists:get_value(max_disk_used, Config),
    OverDisks = [ {Disk, Used}
                  || {Disk, _Cap, Used} <- UsedMounts, Used > MaxDiskUsed],

    Fun = fun({Disk, Used}, Acc) ->
                  Key = list_to_atom("disk_check_" ++ Disk),
                  case hit_rate_limit(Key, Acc) of
                      false ->
                          Host = misc:extract_node_address(node()),
                          Err = fmt_to_bin(errors(disk), [Disk, Host, Used]),
                          global_alert(disk, Err),

                          Time = erlang:monotonic_time(),
                          dict:store(Key, Time, Acc);
                      true ->
                          Acc
                  end
          end,

    lists:foldl(Fun, Opaque, OverDisks);

%% @doc check how much overhead there is compared to data
check(overhead, Opaque, _History, Stats) ->
    [case over_threshold(fetch_bucket_stat(Stats, Bucket,
                                           ep_meta_data_memory_bytes),
                         fetch_bucket_stat(Stats, Bucket, ep_max_size)) of
         {true, X} ->
             Host = misc:extract_node_address(node()),
             Err = fmt_to_bin(errors(overhead), [erlang:trunc(X), Bucket, Host]),
             global_alert({overhead, Bucket}, Err);
         false  ->
             ok
     end || Bucket <- ns_memcached:active_buckets()],
    Opaque;

%% @doc check for indexer ram usage
check(indexer_ram_max_usage, Opaque, _History, Stats) ->
    %% the single index-related alert only applies to memory optimized
    %% indexes, so we simply ignore index stats in non memory optimized case
    StorageMode = index_settings_manager:get(storageMode),
    IsMemoryOptimized = index_settings_manager:is_memory_optimized(StorageMode),
    case proplists:get_value("@index", Stats) of
        undefined ->
            ok;
        Val when IsMemoryOptimized ->
            IndexerRam = proplists:get_value(index_ram_percent, Val),
            case IndexerRam of
                undefined ->
                    ok;
                _ ->
                    {value, Config} = ns_config:search(alert_limits),
                    Max = proplists:get_value(max_indexer_ram, Config),
                    case IndexerRam > Max of
                        true ->
                            Host = misc:extract_node_address(node()),
                            Err = fmt_to_bin(errors(indexer_ram_max_usage),
                                             [Host, erlang:trunc(IndexerRam),
                                              Max]),
                            global_alert(indexer_ram_max_usage, Err);
                        false ->
                            ok
                    end
            end;
        _ ->
            ok
    end,
    Opaque;

%% @doc check for indexer low resident percentage
check(indexer_low_resident_percentage, Opaque, _History, Stats) ->
    case proplists:get_value("@index", Stats) of
        undefined ->
            ok;
        Val ->
            AvgPct = proplists:get_value(index_avg_resident_percent, Val),
            MemoryRss = proplists:get_value(index_memory_rss, Val),
            MemoryQuota = proplists:get_value(index_memory_quota, Val),
            NumIndexes = proplists:get_value(index_num_indexes, Val),
            case AvgPct =/= undefined andalso MemoryRss =/= undefined andalso
                MemoryQuota =/= undefined andalso NumIndexes =/= undefined of
                false ->
                    ok;
                true ->
                    {value, Config} = ns_config:search(alert_limits),
                    Threshold =
                        proplists:get_value(low_indexer_resident_percentage,
                                            Config,
                                            ?INDEXER_LOW_RESIDENT_PERCENTAGE),
                    %% If there's a threshold specified for the low resident
                    %% percentage we check to see if has been reached. We do
                    %% so only if there are indexes (otherwise the Avg
                    %% Resident Percent will be zero). The memory check is
                    %% done to handle the case where Avg Resident Percent is
                    %% low due to indexer restart but there is sufficient
                    %% memory.
                    case cluster_compat_mode:is_enterprise() andalso
                        Threshold =/= undefined andalso
                        (NumIndexes > 0) andalso
                        (AvgPct < Threshold) andalso
                        (MemoryQuota > 0) andalso
                        (MemoryRss / MemoryQuota >
                             (?INDEXER_RESIDENT_MEMORY_PCT / 100)) of
                        true ->
                            Host = misc:extract_node_address(node()),
                            Err = fmt_to_bin(
                                    errors(indexer_low_resident_percentage),
                                    [Host, erlang:trunc(AvgPct), Threshold]),
                            global_alert(indexer_low_resident_percentage, Err);
                        false ->
                            ok
                    end
            end
    end,
    Opaque;

%% @doc check for write failures inside ep engine
check(write_fail, Opaque, _History, Stats) ->
    check_stat_increased(Stats, ep_item_commit_failed, Opaque);

%% @doc check for write failures in audit log
check(audit_write_fail, Opaque, _History, Stats) ->
    check_global_stat_increased(Stats, audit_dropped_events, Opaque);

%% @doc check for any oom errors an any bucket
check(oom, Opaque, _History, Stats) ->
    check_stat_increased(Stats, ep_oom_errors, Opaque);

%% @doc check for any CAS drift threshold exceeded errors on any bucket
check(cas_drift_threshold, Opaque, _History, Stats) ->
    Filter =
        fun(Bucket) ->
                case ns_bucket:get_bucket(Bucket) of
                    {ok, BCfg} ->
                        ns_bucket:conflict_resolution_type(BCfg) =:= lww orelse
                            ns_bucket:history_retention_seconds(BCfg) =/=
                            ?HISTORY_RETENTION_SECONDS_DEFAULT;
                    not_present ->
                        false
                end
        end,

    NewStats = [Item || {Bucket, _OrdDict} = Item <- Stats,
                        Bucket =/= "@global" andalso Filter(Bucket)],
    Formatter = fun(Bucket, _, Host) ->
                        Threshold =
                            case ns_bucket:get_bucket(Bucket) of
                                {ok, Cfg} ->
                                    case ns_bucket:drift_thresholds(Cfg) of
                                        {Ahead, _} ->
                                            Ahead;
                                        _ ->
                                            undefined
                                    end;
                                _ ->
                                    undefined
                            end,
                        fmt_to_bin(errors(ep_clock_cas_drift_threshold_exceeded),
                                   [Bucket, Host, Threshold])
                end,
    check_stat_increased(NewStats, ep_clock_cas_drift_threshold_exceeded, Opaque,
                         Formatter);

check(communication_issue, Opaque, _History, _Stats) ->
    ClusterStatus = case mb_master:master_node() =:= node() of
                        true ->
                            dict:to_list(node_status_analyzer:get_statuses());
                        false ->
                            []
                    end,
    lists:foreach(
      fun ({Node, Status}) ->
              case Status of
                  {{_, [{_,{potential_network_partition, Nodes}}]}, TS} ->
                      Now = erlang:monotonic_time(),
                      TimeElapsed = erlang:convert_time_unit(Now - TS,
                                                             native, second),

                      case TimeElapsed > ?COMMUNICATION_ISSUE_TIMEOUT of
                          true ->
                              Host = misc:extract_node_address(Node),
                              Others = lists:map(
                                         fun misc:extract_node_address/1,
                                         Nodes),
                              OH = string:join(Others, ", "),
                              Err = fmt_to_bin(errors(communication_issue),
                                               [Host, OH]),
                              global_alert({communication_issue, Node}, Err);
                          false ->
                              ok
                      end;
                  _ ->
                      ok
              end
      end, ClusterStatus),
    Opaque;

check(time_out_of_sync, Opaque, _History, _Stats) ->
    case mb_master:master_node() =/= node() of
        true ->
            alert_if_time_out_of_sync(ns_tick_agent:time_offset_status());
        false ->
            ok
    end,
    Opaque;

check(certs, Opaque, _History, _Stats) ->
    CAAlerts =
        case mb_master:master_node() == node() of
            true ->
                lists:flatmap(
                  fun (CAProps) ->
                          {_, ExpWarnings} =
                              ns_server_cert:expiration_warnings(CAProps),
                          Subject = proplists:get_value(subject, CAProps),
                          Id = proplists:get_value(id, CAProps),
                          [{{ca, Id, Subject}, W} || W <- ExpWarnings]
                  end, ns_server_cert:trusted_CAs(props));
            false ->
                []
        end,

    LocalAlerts =
        case ns_config:read_key_fast({node, node(), node_cert}, undefined) of
            undefined -> [];
            Props ->
                {_, ExpWarnings} = ns_server_cert:expiration_warnings(Props),
                lists:map(
                  fun (W) ->
                          Subject = proplists:get_value(subject, Props),
                          {{node, Subject}, W}
                  end, ExpWarnings)
        end,

    ClientAlerts =
        case ns_config:read_key_fast({node, node(), client_cert}, undefined) of
            undefined ->
                [];
            ClientProps ->
                {_, ClientExpWarnings} =
                    ns_server_cert:expiration_warnings(ClientProps),
                lists:map(
                  fun (W) ->
                          Subject = proplists:get_value(subject, ClientProps),
                          {{client, Subject}, W}
                  end, ClientExpWarnings)
        end,

    lists:foreach(
      fun ({{ca, Id, Subj}, expired}) ->
              Error = fmt_to_bin(errors(ca_expired), [Id, Subj]),
              global_alert({cert_expired, {ca, Id}}, Error);
          ({{node, Subj}, expired}) ->
              Host = misc:extract_node_address(node()),
              Error = fmt_to_bin(errors(node_cert_expired), [Host, Subj]),
              global_alert({cert_expired, [{node, Host}, {type, server}]},
                           Error);
          ({{client, Subj}, expired}) ->
              Host = misc:extract_node_address(node()),
              Error = fmt_to_bin(errors(client_cert_expired), [Host, Subj]),
              global_alert({cert_expired, [{node, Host}, {type, client}]},
                           Error);
          ({{ca, Id, Subj}, {expires_soon, UTCSeconds}}) ->
              Date = menelaus_web_cert:format_time(UTCSeconds),
              Error = fmt_to_bin(errors(ca_expires_soon), [Id, Subj, Date]),
              global_alert({cert_expires_soon, {ca, Id}}, Error);
          ({{node, Subj}, {expires_soon, UTCSeconds}}) ->
              Host = misc:extract_node_address(node()),
              Date = menelaus_web_cert:format_time(UTCSeconds),
              Error = fmt_to_bin(errors(node_cert_expires_soon),
                                 [Host, Subj, Date]),
              global_alert({cert_expires_soon, [{node, Host}, {type, server}]},
                           Error);
          ({{client, Subj}, {expires_soon, UTCSeconds}}) ->
              Host = misc:extract_node_address(node()),
              Date = menelaus_web_cert:format_time(UTCSeconds),
              Error = fmt_to_bin(errors(client_cert_expires_soon),
                                 [Host, Subj, Date]),
              global_alert({cert_expires_soon, [{node, Host}, {type, client}]},
                           Error)

      end, CAAlerts ++ LocalAlerts ++ ClientAlerts),

    Opaque;

check(xdcr_certs, Opaque, _History, _Stats) ->
    case mb_master:master_node() == node() andalso
        cluster_compat_mode:is_goxdcr_enabled() of
        true -> check_xdcr_certs(Opaque);
        false -> Opaque
    end;

%% @doc check if the mutation history size is over the alert threshold for at
%% least one vbucket of a bucket
check(history_size_warning, Opaque, History, _Stats) ->
    {value, Config} = ns_config:search(alert_limits),
    Threshold = proplists:get_value(history_warning_threshold, Config,
                                    ?HIST_WARN_PERC),
    lists:foreach(
      fun (Bucket) ->
              Key = {history_size_warning, Bucket},
              case other_node_already_alerted(Key, History) of
                  false ->
                      case get_history_size_alert(Bucket, Threshold) of
                          ok -> ok;
                          Err -> global_alert(Key, Err)
                      end;
                  true ->
                      ok
              end
      end, ns_bucket:get_bucket_names()),

    Opaque;

check(memory_threshold, Opaque, _History, Stats) ->
    case proplists:get_value("@system", Stats) of
        undefined ->
            ?log_debug("Skipping memory threshold check as there is no "
                       "system stats: ~p", [Stats]),
            ok;
        SysStats ->
            Free = proplists:get_value(mem_actual_free, SysStats),
            Used = proplists:get_value(mem_actual_used, SysStats),
            case is_number(Free) andalso is_number(Used) andalso
                (Free + Used) > 0 of
                true ->
                    check_memory_threshold(Used, Used + Free, system);
                false ->
                    ?log_debug("Skipping memory threshold check as there is no "
                               "mem stats: ~p", [SysStats]),
                    ok
            end,

            CGLimit = proplists:get_value(mem_cgroup_limit, SysStats),
            CGUsed = proplists:get_value(mem_cgroup_actual_used, SysStats),

            case is_number(CGUsed) andalso is_number(CGLimit) andalso
                CGLimit > 0 of
                true ->
                    check_memory_threshold(CGUsed, CGLimit, cgroup);
                false ->
                    ok
            end
    end,
    Opaque;
check(stuck_rebalance, Opaque, _History, _Stats) ->
    {Stuck, Opaque1} =
        case cluster_compat_mode:is_cluster_76() and
            %% Only check the alert from the orchestrator node, to avoid
            %% duplicate alerts and unnecessarily making ns_rebalance_observer
            %% calls from other nodes
            (mb_master:master_node() =:= node()) and rebalance:running() of
            false ->
                {0, Opaque};
            true ->
                case ns_rebalance_observer:get_current_stage() of
                    {error, E} ->
                        ?log_error("Error getting rebalance stage: ~p", [E]),
                        {0, Opaque};
                    not_running ->
                        %% If no rebalance is running then we don't need to do
                        %% anything
                        {0, Opaque};
                    Service ->
                        %% Fetch the alert threshold for this service
                        {value, Limits} = ns_config:search(alert_limits),
                        Threshold = proplists:get_value(
                                      {stuck_rebalance_threshold_secs, Service},
                                      Limits, undefined),
                        case Threshold of
                            undefined ->
                                %% If the threshold is undefined then the alert
                                %% is disabled
                                {0, Opaque};
                            _ ->
                                NowTime = erlang:monotonic_time(),
                                %% Check rebalance progress and possibly alert
                                maybe_alert_stuck_rebalance(
                                  Service, NowTime, Threshold, Opaque)
                        end
                end
        end,
    ns_server_stats:notify_gauge({<<"rebalance_stuck">>, []}, Stuck),
    Opaque1;
check(memcached_connections, Opaque, _History, Stats) ->
    case proplists:get_value("@global", Stats) of
        undefined ->
            ?log_debug("Skipping memcached connections check as there are no "
                       "global stats: ~p", [Stats]);
        GlobalStats ->
            check_memcached_connections(user, GlobalStats),
            check_memcached_connections(system, GlobalStats)
    end,
    Opaque;
check(disk_guardrail, Opaque, _History, _Stats) ->
    {Thresholds, Enforceable} =
        case guardrail_monitor:get(disk_usage) of
            undefined ->
                %% Alert thresholds for when guardrail isn't enabled
                AlertLimits = ns_config:read_key_fast(alert_limits, []),
                {[{warning, proplists:get_value(max_data_disk_used, AlertLimits,
                                                ?DEFAULT_MAX_DATA_DISK_USED)}],
                 false};
            Limits ->
                Critical = proplists:get_value(maximum, Limits),
                Warning = Critical - 10,
                {[{warning, Warning} | Limits], true}
        end,
    case {guardrail_monitor:check_alert(disk, Thresholds), Enforceable} of
        {ok, _} ->
            ok;
        {{warning, BadDisk, Value}, false} ->
            %% Data disk usage is high, writes won't be prevented
            Limit = proplists:get_value(warning, Thresholds),
            Msg = fmt_to_bin(errors(data_disk_warning),
                             [BadDisk, Value, node(), Limit]),
            global_alert(disk_guardrail, Msg);
        {{warning, BadDisk, Value}, true} ->
            %% Data disk usage is high, writes will soon be prevented
            Limit = proplists:get_value(maximum, Thresholds),
            Msg = fmt_to_bin(errors(data_disk_guardrail_warning),
                             [BadDisk, Value, node(), Limit]),
            global_alert(disk_guardrail, Msg);
        {{maximum, BadDisk, Value}, true} ->
            %% Data disk usage is high, and writes are being prevented
            Limit = proplists:get_value(maximum, Thresholds),
            Msg = fmt_to_bin(errors(data_disk_guardrail_critical),
                             [BadDisk, Value, node(), Limit]),
            global_alert(disk_guardrail, Msg)
    end,
    Opaque;
check(indexer_diverging_replicas, Opaque, _History, Stats) ->
    case proplists:get_value("@index", Stats) of
        undefined ->
            ok;
        IndexStats ->
            NumDiverging =
                proplists:get_value(index_num_diverging_replica_indexes,
                                    IndexStats, 0),
            case NumDiverging of
                0 ->
                    ok;
                _ ->
                    Msg = fmt_to_bin(errors(indexer_diverging_replicas), []),
                    global_alert(indexer_diverging_replicas, Msg)
            end
    end,
    Opaque.

-spec check_memcached_connections(user | system, term()) -> term().
check_memcached_connections(Type, GlobalStats) ->
    ThresholdName =
        list_to_atom(
          lists:concat(["memcached_", Type,
                        "_connection_warning_threshold"])),
    MaxStatName =
        list_to_atom(lists:concat(["kv_max_", Type, "_connections"])),
    CurrStatName =
        list_to_atom(lists:concat(["kv_", Type, "_connections"])),

    {value, Config} = ns_config:search(alert_limits),
    AlertPerc = proplists:get_value(ThresholdName,
                                    Config,
                                    ?MEMCACHED_CONNECTION_THRESHOLD),
    Max = proplists:get_value(MaxStatName, GlobalStats, undefined),
    case Max of
        undefined -> ok;
        _ when is_number(Max) ->
            AlertLimit = Max * AlertPerc / 100,
            Used = proplists:get_value(CurrStatName, GlobalStats, undefined),
            case Used of
                undefined -> ok;
                _ when is_number(Used) ->
                    case Used > AlertLimit of
                        false -> ok;
                        true  ->
                            Err =
                                fmt_to_bin(
                                  errors(memcached_connections),
                                  [node(), Type, Used, AlertPerc, Max]),
                            global_alert({memcached_connections, Type}, Err)
                    end;
                _ ->
                    ?log_debug("Skipping memcached connections check ~p is not "
                               "a number. global stats: ~p",
                               [CurrStatName, GlobalStats])
            end;
        _ ->
            ?log_debug("Skipping memcached connections check as ~p is not a "
                       "number. global stats: ~p", [MaxStatName, GlobalStats])
    end.

check_memory_threshold(MemUsed, MemTotal, Type) ->
    Percentage = (MemUsed * 100) / MemTotal,
    {value, Config} = ns_config:search(alert_limits),
    Threshold1 = proplists:get_value(memory_notice_threshold,
                                     Config, ?MEM_NOTICE_PERC),
    Threshold2 = proplists:get_value(memory_warning_threshold,
                                     Config, ?MEM_WARN_PERC),
    Threshold3 = proplists:get_value(memory_critical_threshold,
                                     Config, ?MEM_CRIT_PERC),

    Res =
        if
            Threshold3 >= 0 andalso Percentage >= Threshold3 ->
                {alert, memory_critical, Threshold3};
            Threshold2 >= 0 andalso Percentage >= Threshold2 ->
                {alert, memory_warning, Threshold2};
            Threshold1 >= 0 andalso Percentage >= Threshold1 ->
                {alert, memory_notice, Threshold1};
            true ->
                ok
        end,

    case Res of
        ok -> ok;
        {alert, Error, Threshold} when is_float(Percentage),
                                       is_integer(Threshold) ->
            Node = node(),
            Host = misc:extract_node_address(Node),
            Err = fmt_to_bin(errors(Error),
                             [Host, Type, Percentage, Threshold]),
            global_alert({memory_threshold, {Node, Threshold, Type}}, Err)
    end.

check_xdcr_certs(Opaque) ->
    try goxdcr_rest:get_certificates() of
        Info ->
            WarningDays = ns_config:read_key_fast(
                            {cert, expiration_warning_days}, undefined),
            Hash = erlang:phash2({Info, WarningDays}),
            Now = calendar:datetime_to_gregorian_seconds(
                    calendar:universal_time()),

            {NewOpaque, AlertsList} =
                case dict:find(xdcr_certs_check, Opaque) of
                    {ok, #{hash := Hash,
                           retry_time := RetryTime,
                           result := Res}} when Now < RetryTime ->
                        {Opaque, Res};
                    _ ->
                        {RetryTime, Alerts} = calculate_xdcr_cert_alerts(Info),
                        {dict:store(xdcr_certs_check,
                                    #{hash => Hash,
                                      retry_time => RetryTime,
                                      result => Alerts},
                                    Opaque), Alerts}
                end,

            lists:foreach(
              fun ({{ca, Subj}, expired}) ->
                      Error = fmt_to_bin(errors(xdcr_ca_expired), [Subj]),
                      global_alert({cert_expired, {xdcr, Subj}}, Error);
                  ({{client_cert, Subj}, expired}) ->
                      Error = fmt_to_bin(errors(client_xdcr_cert_expired),
                                         [Subj]),
                      global_alert({cert_expired, {client_xdcr, Subj}}, Error);
                  ({{ca, Subj}, {expires_soon, UTCSeconds}}) ->
                      Date = menelaus_web_cert:format_time(UTCSeconds),
                      Error = fmt_to_bin(errors(xdcr_ca_expires_soon),
                                         [Subj, Date]),
                      global_alert({cert_expires_soon, {xdcr, Subj}}, Error);
                  ({{client_cert, Subj}, {expires_soon, UTCSeconds}}) ->
                      Date = menelaus_web_cert:format_time(UTCSeconds),
                      Error = fmt_to_bin(errors(client_xdcr_cert_expires_soon),
                                         [Subj, Date]),
                      global_alert({cert_expires_soon, {client_xdcr, Subj}},
                                   Error)
              end, AlertsList),

            NewOpaque

    catch
        C:E:ST ->
            ?log_error("Failed to extract certs info from xdcr: ~p:~p~n~p",
                       [C, E, ST]),
            Opaque
    end.

calculate_xdcr_cert_alerts(#{trusted_certs := TrustedCerts,
                             client_certs := ClientCerts}) ->
    AlertsFun =
        fun (Certs, Type) ->
            lists:foldl(
              fun (C, {WarningsAcc, RecheckAcc}) ->
                  case ns_server_cert:decode_single_certificate(C) of
                      {ok, D} ->
                          Props = ns_server_cert:cert_props(D),
                          {RecheckTime, Warnings} =
                              ns_server_cert:expiration_warnings(Props),
                          Subject = proplists:get_value(subject, Props),
                          {[{{Type, Subject}, W} || W <- Warnings] ++
                           WarningsAcc,
                           min(RecheckTime, RecheckAcc)};
                      {error, E} ->
                          ?log_debug("Failed to decode xdcr cert: ~p",
                                     [E]),
                          {WarningsAcc, RecheckAcc}
                  end
              end, {[], infinity}, Certs)
        end,

    {CAAlerts, CARecheckTime} = AlertsFun(TrustedCerts, ca),
    {ClientAlerts, ClientRecheckTime} = AlertsFun(ClientCerts, client_cert),
    {min(CARecheckTime, ClientRecheckTime), CAAlerts ++ ClientAlerts}.

alert_if_time_out_of_sync({time_offset_status, true}) ->
    Err = fmt_to_bin(errors(time_out_of_sync), [node()]),
    global_alert(time_out_of_sync, Err);
alert_if_time_out_of_sync({time_offset_status, false}) ->
    ok.

get_history_size_alert(Bucket, Threshold) ->
    CheckVBucket = fun (MaxSize, {VB, Size}) ->
                           case (Size * 100) / MaxSize > Threshold of
                               true -> {true, VB};
                               false -> false
                           end
                   end,
    StatsParser = fun (Key, V, DiskUsages) ->
                          case string:split(binary_to_list(Key), ":") of
                              [VB, "history_disk_size"] ->
                                  [{VB, binary_to_integer(V)} | DiskUsages];
                              _ ->
                                  %% We are not interested in other disk stats
                                  DiskUsages
                          end
                  end,
    GetStats =
        fun () ->
                try
                    case ns_memcached:raw_stats(
                           node(), Bucket, <<"diskinfo detail">>,
                           StatsParser(_, _, _), []) of
                        {ok, V} -> V;
                        Error -> Error
                    end
                catch exit:{noproc, _} ->
                        ?log_debug("memcached is not started for bucket ~p yet",
                                   [Bucket]),
                        []
                end
        end,
    GetVBsOverThreshold =
        fun (MaxPerVBucket) ->
                lists:filtermap(CheckVBucket(MaxPerVBucket, _),
                                GetStats())
        end,
    case ns_bucket:get_bucket(Bucket) of
        {ok, BCfg} ->
            OnThisNode = lists:member(node(), ns_bucket:get_servers(BCfg)),
            MaxSize = ns_bucket:history_retention_bytes(BCfg),
            MaxTime = ns_bucket:history_retention_seconds(BCfg),
            TotalVBs = ns_bucket:get_num_vbuckets(BCfg),
            case OnThisNode andalso MaxSize > 0 andalso MaxTime > 0 andalso
                TotalVBs > 0 of
                true ->
                    case GetVBsOverThreshold(MaxSize / TotalVBs) of
                        [] -> ok;
                        BadVBs ->
                            ale:warn(?USER_LOGGER,
                                     "The following vbuckets have mutation "
                                     "history size above the warning threshold:"
                                     " ~p", [BadVBs]),
                            fmt_to_bin(errors(history_size_warning),
                                       [Bucket, Threshold, length(BadVBs),
                                        TotalVBs])
                    end;
                false ->
                    ok
            end;
        not_present ->
            ok
    end.

maybe_alert_stuck_rebalance(Service, NowTime, Threshold, Opaque) ->
    %% Fetch previous progress value
    LastProgress = get_last_rebalance_progress(Service, Opaque),
    %% Calculate how long it has been since progress was last checked
    {OldStuck, TimeSinceLastCheck} =
        case LastProgress of
            #rebalance_progress{last_timestamp = LastCheck} ->
                {Stuck, _} = is_stuck(LastProgress, Threshold),
                {Stuck,
                 erlang:convert_time_unit(NowTime - LastCheck, native, second)};
            not_found ->
                %% Assume we were not stuck as there is no previous progress
                {0, infinity}
        end,
    QueryPeriod = Threshold / ?REBALANCE_CHECK_FREQ,
    %% Only check progress if it has been long enough since the last check
    case progress_check_needed(TimeSinceLastCheck, QueryPeriod) of
        false ->
            %% Don't update stuckness
            {OldStuck, Opaque};
        true ->
            %% Fetch the current rebalance progress
            case ns_rebalance_observer:get_progress_for_alerting(Service) of
                {error, E} ->
                    ?log_error("Error getting rebalance progress: ~p", [E]),
                    %% Don't update stuckness
                    {OldStuck, Opaque};
                not_running ->
                    %% Rebalance is no longer running so no need to alert
                    {0, Opaque};
                {Id, _} = FetchedProgress ->
                    NewProgress = update_rebalance_progress(
                                    Service, NowTime, LastProgress,
                                    FetchedProgress),

                    %% Check if we have been stuck long enough to alert and
                    %% avoid alerting from more than one node at a time for the
                    %% same stage of the same rebalance
                    {StuckStatus, StuckTime} = is_stuck(NewProgress, Threshold),
                    case StuckStatus of
                        1 ->
                            Msg = fmt_to_bin(errors(stuck_rebalance),
                                             [Service, Id, StuckTime]),
                            global_alert({stuck_rebalance, {Service, Id}}, Msg);
                        0 ->
                            ok
                    end,
                    {StuckStatus, dict:store({rebalance_progress, Service},
                                             NewProgress,
                                             Opaque)}
            end
    end.

is_stuck(#rebalance_progress{stuck_timestamp = StuckStart,
                             last_timestamp = LastCheck}, Threshold) ->
    StuckTime = erlang:convert_time_unit(LastCheck - StuckStart,
                                         native, second),
    Status =
        case StuckTime >= Threshold of
            true -> 1;
            false -> 0
        end,
    {Status, StuckTime}.

-spec progress_check_needed(infinity | integer(), float()) -> boolean().
progress_check_needed(infinity, _QueryPeriod) ->
    true;
progress_check_needed(TimeSinceLastCheck, QueryPeriod) ->
    TimeSinceLastCheck >= QueryPeriod.

-spec get_last_rebalance_progress(atom(), dict:dict()) ->
          #rebalance_progress{} | not_found.
get_last_rebalance_progress(Service, Opaque) ->
    %% Fetch last progress entry
    case dict:find({rebalance_progress, Service}, Opaque) of
        {ok, #rebalance_progress{} = LastProgress} ->
            LastProgress;
        error ->
            not_found
    end.

update_rebalance_progress(Service, NowTime, not_found, Progress) ->
    ?log_debug("Rebalance progress initialised for ~p: ~p",
               [Service, Progress]),
    %% No progress was found in Opaque so we should
    %% initialise the rebalance_progress for this Service
    #rebalance_progress{
       progress = Progress,
       last_timestamp = NowTime,
       stuck_timestamp = NowTime};
update_rebalance_progress(Service, NowTime,
                          #rebalance_progress{progress = LastProgress,
                                              stuck_timestamp = StuckStart},
                          FetchedProgress) ->
    ProgressStuck = progress_stuck(Service, LastProgress, FetchedProgress),
    %% Update stuck_timestamp
    NewTime = case ProgressStuck of
                  true -> StuckStart;
                  false -> NowTime
              end,
    %% Update the stored progress with current progress.
    %% We store NowTime as the timestamp of this check. This may be up
    %% to 10s prior to the log message as we have a 10s timeout in
    %% get_progress_for_alerting/1
    #rebalance_progress{progress = FetchedProgress,
                        last_timestamp = NowTime,
                        stuck_timestamp = NewTime}.

progress_stuck(kv, _LastProgress, {_, []}) ->
    %% We only care about stuck vbucket moves, so if no vbuckets are moving,
    %% then none are stuck
    false;
progress_stuck(_Service, LastProgress, CurrentProgress) ->
    LastProgress =:= CurrentProgress.

%% @doc only check for disk usage if there has been no previous
%% errors or last error was over the timeout ago
-spec hit_rate_limit(atom(), dict:dict()) -> true | false.
hit_rate_limit(Key, Dict) ->
    case dict:find(Key, Dict) of
        error ->
            false;
        {ok, LastTime} ->
            TimeNow    = erlang:monotonic_time(),
            TimePassed = erlang:convert_time_unit(TimeNow - LastTime,
                                                  native, second),

            TimePassed < ?DISK_USAGE_TIMEOUT
    end.

%% @doc check if any other nodes have recently fired an alert for this alert key
-spec other_node_already_alerted(alert_key(), any()) -> true | false.
other_node_already_alerted(Key, Hist) ->
    AlertMatches =
        fun ({OldKey, _, _, _, _}) ->
                case OldKey of
                    {Key, _} ->
                        true;
                    _ ->
                        false
                end
        end,
    lists:any(AlertMatches, Hist).

%% @doc calculate percentage of overhead and if it is over threshold
-spec over_threshold(integer(), integer()) -> false | {true, float()}.
over_threshold(_Ep, 0) ->
    false;
over_threshold(EpErrs, Max) ->
    {value, Config} = ns_config:search(alert_limits),
    MaxOverheadPerc = proplists:get_value(max_overhead_perc, Config),
    Perc = (EpErrs / Max) * 100,
    case Perc > MaxOverheadPerc of
        true -> {true, Perc};
        false  -> false
    end.

%% @doc Check if the value of any statistic has increased since
%% last check
check_stat_increased(Stats, StatName, Opaque) ->
    Formatter = fun(Bucket, SName, Host) ->
                        fmt_to_bin(errors(SName), [Bucket, Host])
                end,
    check_stat_increased(Stats, StatName, Opaque, Formatter).

check_stat_increased(Stats, StatName, Opaque, Formatter) ->
    New = fetch_buckets_stat(Stats, StatName),
    case dict:is_key(StatName, Opaque) of
        false ->
            dict:store(StatName, New, Opaque);
        true ->
            Old = dict:fetch(StatName, Opaque),
            case stat_increased(New, Old) of
                [] ->
                    ok;
                Buckets ->
                    Host = misc:extract_node_address(node()),
                    [global_alert({StatName, Bucket}, Formatter(Bucket, StatName, Host))
                     || Bucket <- Buckets]
            end,
            dict:store(StatName, New, Opaque)
    end.

check_global_stat_increased(Stats, StatName, Opaque) ->
    New = fetch_bucket_stat(Stats, "@global", StatName),
    case dict:is_key(StatName, Opaque) of
        false ->
            dict:store(StatName, New, Opaque);
        true ->
            Old = dict:fetch(StatName, Opaque),
            case New > Old of
                false ->
                    ok;
                true ->
                    Host = misc:extract_node_address(node()),
                    global_alert(StatName, fmt_to_bin(errors(StatName), [Host]))
            end,
            dict:store(StatName, New, Opaque)
    end.

%% @doc check that I can listen on the current host
-spec can_listen(string()) -> true | {false, inet:posix()}.
can_listen(Host) ->
    case inet:getaddr(Host, misc:get_net_family()) of
        {error, Err} ->
            ?log_error("Cannot listen due to ~p from inet:getaddr~n", [Err]),
            {false, Err};
        {ok, IpAddr} ->
            case gen_udp:open(0, [misc:get_net_family(), {ip, IpAddr}]) of
                {error, ListErr} ->
                    ?log_error("gen_udp:open(~p) failed due to ~p", [IpAddr, ListErr]),
                    {false, ListErr};
                {ok, Socket} ->
                    gen_udp:close(Socket),
                    true
            end
    end.


%% @doc list of buckets thats measured stats have increased
-spec stat_increased(dict:dict(), dict:dict()) -> list().
stat_increased(New, Old) ->
    [Bucket || {Bucket, Val} <- dict:to_list(New), increased(Bucket, Val, Old)].


%% @doc fetch a list of a stat for all buckets
fetch_buckets_stat(Stats, StatName) ->
    dict:from_list(
      [{Bucket, fetch_bucket_stat(Stats, Bucket, StatName)}
       || {Bucket, _OrdDict} <- Stats,
          Bucket =/= "@global"]
     ).


%% @doc fetch latest value of stat for particular bucket
fetch_bucket_stat(Stats, Bucket, StatName) ->
    OrdDict = case orddict:find(Bucket, Stats) of
                  {ok, KV} ->
                      KV;
                  _ ->
                      []
              end,
    case orddict:find(StatName, OrdDict) of
        {ok, V} -> V;
        _ -> 0
    end.


%% @doc Server keeps a list of messages to check against sending
%% the same message repeatedly
-spec expire_history(list()) -> list().
expire_history(Hist) ->
    Now     = erlang:monotonic_time(),
    Timeout = erlang:convert_time_unit(?ALERT_TIMEOUT, second, native),

    [ Item || Item = {_Key, _Msg, Time, _Offset, _} <- Hist, Now - Time < Timeout ].


%% @doc Lookup old value and test for increase
-spec increased(string(), integer(), dict:dict()) -> true | false.
increased(Key, Val, Dict) ->
    case dict:find(Key, Dict) of
        error ->
            false;
        {ok, Prev} ->
            Val > Prev
    end.


%% Format the error message into a binary
fmt_to_bin(Str, Args) ->
    list_to_binary(lists:flatten(io_lib:format(Str, Args))).


-spec to_str(binary() | string()) -> string().
to_str(Msg) when is_binary(Msg) ->
    binary_to_list(Msg);
to_str(Msg) ->
    Msg.

extract_alert_key({Key, _Bucket}) ->
    Key;
extract_alert_key(Key) ->
    Key.

maybe_send_out_email_alert({Key0, Node}, Message) ->
    case Node =:= node() of
        true ->
            Key = extract_alert_key(Key0),

            Config = menelaus_alert:get_config(),
            case proplists:get_bool(enabled, Config) of
                true ->
                    Description = short_description(Key),
                    ns_mail:send_alert_async(Key, Description, Message, Config),
                    true;
                false ->
                    false
            end;
        false ->
            false
    end.

move_memory_alert_email_alerts(Key, NsConfigKey, PList) ->
    {case ns_config:read_key_fast(NsConfigKey, true) of
         true -> add_proplist_list_elem(Key, memory_threshold, PList);
         false -> PList
     end, [{delete, NsConfigKey}]}.

%% The stuck rebalance alert was added in 7.6 but with an undefined threshold by
%% default, which gave the behaviour of being disabled by default.
%% In 7.9 it is now added to the UI, which means it needs to actually be
%% disabled properly for it to show as disabled in the UI.
%% This means that we need to explicitly remove the key from pop up alerts and
%% email alerts.
maybe_delete_stuck_rebalance_keys(Config) ->
    Limits = ns_config:search(Config, alert_limits, []),
    case proplists:get_value({stuck_rebalance_threshold_secs, kv}, Limits) of
        undefined ->
            [remove_proplist_list_elem(alerts, stuck_rebalance, _),
             remove_proplist_list_elem(pop_up_alerts, stuck_rebalance, _)];
        _Value ->
            %% If a value has been set for the stuck rebalance threshold, then
            %% we keep it enabled, by not deleting the keys, since it must have
            %% been manually enabled already
            []
    end.

%% If it is already present, remove Elem from the value of the proplist
%% member {ListKey, <list_value>}, which is assumed to exist and have a
%% list value.
remove_proplist_list_elem(ListKey, Elem, PList) ->
    List = misc:expect_prop_value(ListKey, PList),
    case lists:member(Elem, List) of
        true ->
            misc:update_proplist(PList, [{ListKey, lists:delete(Elem, List)}]);
        false ->
            PList
    end.

%% If it is not already present, add Elem to the value of the proplist
%% member {ListKey, <list_value>}, which is assumed to exist and have a
%% list value.
add_proplist_list_elem(ListKey, Elem, PList) ->
    List = misc:expect_prop_value(ListKey, PList),
    misc:update_proplist(PList, [{ListKey, lists:usort([Elem | List])}]).

upgrade_alerts(EmailAlerts, Mutations) ->
    {Result, ExtraNsCfgChanges} =
        lists:foldl(
          fun (Mutation, {Acc, OtherChanges}) ->
              case Mutation(Acc) of
                  {NewAcc, Extra} -> {NewAcc, OtherChanges ++ Extra};
                  NewAcc when is_list(EmailAlerts) -> {NewAcc, OtherChanges}
              end
          end,
          {EmailAlerts, []}, Mutations),
    maybe_upgrade_email_alerts(EmailAlerts, Result) ++ ExtraNsCfgChanges.

maybe_upgrade_email_alerts(Old, New) ->
    case misc:sort_kv_list(New) =:= misc:sort_kv_list(Old) of
        true ->
            %% No change due to upgrade
            [];
        false ->
            [{set, email_alerts, New}]
    end.

type_spec(undefined) ->
    undefined.

params() ->
    [{"maxOverheadPerc", #{type => {int, 0, 100},
                           cfg_key => [alert_limits, max_overhead_perc]}},
     {"maxDiskUsedPerc", #{type => {int, 0, 100},
                           cfg_key => [alert_limits, max_disk_used]}},
     {"maxDataDiskUsedPerc", #{type => {int, 0, 100},
                               cfg_key => [alert_limits, max_data_disk_used],
                               default => ?DEFAULT_MAX_DATA_DISK_USED}},
     {"maxIndexerRamPerc", #{type => {int, 0, 100},
                             cfg_key => [alert_limits, max_indexer_ram]}},
     {"certExpirationDays", #{type => pos_int,
                              cfg_key => cert_exp_alert_days}},
     {"lowIndexerResidentPerc", #{type => {int, 0, 100},
                                  cfg_key => [alert_limits,
                                              low_indexer_resident_percentage],
                                  default => ?INDEXER_LOW_RESIDENT_PERCENTAGE}},
     {"memoryNoticeThreshold", #{type => {int, -1, 100},
                                 cfg_key => [alert_limits,
                                             memory_notice_threshold],
                                 default => ?MEM_NOTICE_PERC}},
     {"memoryWarningThreshold", #{type => {int, -1, 100},
                                  cfg_key => [alert_limits,
                                              memory_warning_threshold],
                                  default => ?MEM_WARN_PERC}},
     {"memoryCriticalThreshold", #{type => {int, -1, 100},
                                   cfg_key => [alert_limits,
                                               memory_critical_threshold],
                                   default => ?MEM_CRIT_PERC}},
     {"historyWarningThreshold", #{type => {int, 0, 100},
                                   cfg_key => [alert_limits,
                                               history_warning_threshold],
                                   default => ?HIST_WARN_PERC}},
     {"stuckRebalanceThresholdKV",
      #{type => {int, ?MIN_REBALANCE_STUCK_THRESHOLD_S, max_uint64},
        cfg_key => [alert_limits, {stuck_rebalance_threshold_secs, kv}],
        default => ?DEFAULT_KV_REBALANCE_STUCK_THRESHOLD_S}},
     {"stuckRebalanceThresholdIndex",
      #{type => {int, ?MIN_INDEX_REBALANCE_STUCK_THRESHOLD_S, max_uint64},
        cfg_key => [alert_limits, {stuck_rebalance_threshold_secs, index}],
        default => ?DEFAULT_INDEX_REBALANCE_STUCK_THRESHOLD_S}},
     {"memcachedUserConnectionWarningThreshold",
      #{type => {int, 0, 100},
        cfg_key => [alert_limits,
                    memcached_user_connection_warning_threshold],
        default => ?MEMCACHED_CONNECTION_THRESHOLD}},
     {"memcachedSystemConnectionWarningThreshold",
      #{type => {int, 0, 100},
        cfg_key => [alert_limits,
                    memcached_system_connection_warning_threshold],
        default => ?MEMCACHED_CONNECTION_THRESHOLD}}].

build_alert_limits() ->
    case ns_config:search(alert_limits) of
        false ->
            [];
        {value, Values} ->
            Values
    end.

handle_settings_alerts_limits_get(Req) ->
    CertWarnDays = ns_server_cert:cert_expiration_warning_days(),
    menelaus_web_settings2:handle_get([], params(), fun type_spec/1,
                                      [{alert_limits, build_alert_limits()},
                                       {cert_exp_alert_days, CertWarnDays}], Req).

handle_settings_alerts_limits_post(Req) ->
    menelaus_web_settings2:handle_post(
        fun (Params, Req2) ->
            case [{Key, Val} || {[alert_limits, Key], Val} <- Params] of
                [] -> ok;
                NewParams ->
                    NewLimits = misc:update_proplist(build_alert_limits(),
                                                     NewParams),
                    ns_config:set(alert_limits, NewLimits)
            end,
            case proplists:get_value([cert_exp_alert_days], Params) of
                undefined -> ok;
                CertWarningDays ->
                    ns_config:set({cert, expiration_warning_days},
                                  CertWarningDays)
            end,
            handle_settings_alerts_limits_get(Req2)
        end, [], params(), fun type_spec/1, Req).

-ifdef(TEST).
%% Cant currently test the alert timeouts as would need to mock
%% calls to the archiver
run_basic_test_do() ->
    MyNode = node(),

    ?assertEqual(ok, ?MODULE:local_alert({foo, node()}, <<"bar">>)),
    ?assertMatch({[{{foo, MyNode}, <<"bar">>, _, _}], _},
                 ?MODULE:fetch_alerts()),
    {[{{foo, MyNode}, <<"bar">>, _, _}], Opaque1} = ?MODULE:fetch_alerts(),
    ?assertMatch({true, {[], _}},
                 {?MODULE:consume_alerts(Opaque1), ?MODULE:fetch_alerts()}),

    ?assertEqual(ok, ?MODULE:local_alert({bar, node()}, <<"bar">>)),
    ?assertEqual(ignored, ?MODULE:local_alert({bar, node()}, <<"bar">>)),
    {[{{bar, MyNode}, <<"bar">>, _, _}], Opaque2} = ?MODULE:fetch_alerts(),
    true = (Opaque1 =/= Opaque2),
    ?assertEqual(false, ?MODULE:consume_alerts(Opaque1)),
    ?assertEqual(true, ?MODULE:consume_alerts(Opaque2)),
    ?assertMatch({[], _}, ?MODULE:fetch_alerts()),

    ?assertEqual(ok, ?MODULE:global_alert(fu, <<"bar">>)),
    ?assertEqual(ok, ?MODULE:global_alert(fu, <<"bar">>)),
    %% give the global_alert some time to make the call and receive the alert
    GlobalAlert = misc:poll_for_condition(
                    fun() ->
                        case ?MODULE:fetch_alerts() of
                            {[], _} -> false;
                            Alert -> Alert
                        end
                    end,
                    30000, 500),
    ?assertMatch({[{{fu, MyNode}, <<"bar">>, _, _}], _}, GlobalAlert).

basic_test_modules() ->
    [cluster_compat_mode, ns_config].

basic_test() ->
    ok = meck:new(basic_test_modules(), [passthrough]),
    ok = meck:expect(cluster_compat_mode, is_cluster_76,
                     fun () -> true end),
    meck:expect(ns_config, get_timeout,
                fun ({menelaus_web_alerts_srv, sample_rate}, Default) ->
                        Default
                end),

    {ok, _} = gen_event:start_link({local, ns_config_events}),
    {ok, Pid} = ?MODULE:start_link(),

    %% return empty alerts configuration so that no attempts to send anything
    %% are performed
    ns_config:test_setup([{email_alerts, []}]),

    try
        run_basic_test_do()
    after
        misc:unlink_terminate_and_wait(Pid, shutdown),
        ok = meck:unload(basic_test_modules())
    end.

add_proplist_list_elem_test() ->
    %% Sub-test: key "time_out_of_sync" is already present
    PL1 = [{alerts, [ip, time_out_of_sync, communication_issue]},
           {enabled, false}],
    Result1 = add_proplist_list_elem(alerts, time_out_of_sync, PL1),
    ?assertEqual(misc:sort_kv_list(PL1), misc:sort_kv_list(Result1)),

    %% Sub-test: key "time_out_of_sync" isn't present and should be added.
    PL2 = [{alerts, [ip, communication_issue]},
           {enabled, false}],
    Expected2 = [{alerts, [ip, time_out_of_sync, communication_issue]},
                 {enabled, false}],
    Result2 = add_proplist_list_elem(alerts, time_out_of_sync, PL2),
    ?assertEqual(misc:sort_kv_list(Expected2), misc:sort_kv_list(Result2)).

config_upgrade_to_76_test() ->
    Config1 = [],
    Expected1 = [{delete, popup_alerts_auto_failover_upgrade_70_fixed}],
    ?assertEqual(Expected1, config_upgrade_to_76(Config1)),

    Config2 =
        [[{email_alerts,
           [{pop_up_alerts, [ip, disk]}, {enabled, false},
            {alerts, [ip, time_out_of_sync, communication_issue]}]}]],
    Expected2 =
        [{set, email_alerts,
          [{pop_up_alerts,
            [auto_failover_cluster_too_small, auto_failover_disabled,
             auto_failover_maximum_reached, auto_failover_node,
             auto_failover_other_nodes_down, cert_expired,
             cert_expires_soon, disk, ip, memcached_connections,
             memory_threshold]},
           {alerts,
            [cert_expired, cert_expires_soon, communication_issue, ip,
             memcached_connections, memory_threshold, time_out_of_sync]},
           {enabled,false}]},
         {delete,memory_alert_email},
         {delete,memory_alert_popup}] ++ Expected1,
    ?assertEqual(Expected2, config_upgrade_to_76(Config2)).

config_upgrade_to_79_test() ->
    Config1 =
        [[{email_alerts,
           [{pop_up_alerts, [ip, disk]},
            {alerts, [ip, time_out_of_sync]}]}]],
    Expected1 = [{set, email_alerts,
                  [{pop_up_alerts,
                    [disk, disk_guardrail, ip]},
                   {alerts,
                    [disk_guardrail, ip, time_out_of_sync]}]}],
    %% Add disk_guardrail keys
    ?assertEqual(Expected1, config_upgrade_to_79(Config1)),

    Config2 =
        [[{email_alerts,
           [{pop_up_alerts, [ip, disk, stuck_rebalance]},
            {alerts, [ip, time_out_of_sync, stuck_rebalance]}]}]],
    Expected2 = [{set, email_alerts,
                  [{pop_up_alerts,
                    [disk, disk_guardrail, ip]},
                   {alerts,
                    [disk_guardrail, ip, time_out_of_sync]}]}],
    %% Remove stuck_rebalance keys if found and threshold unspecified
    ?assertEqual(Expected2, config_upgrade_to_79(Config2)),

    Config3 =
        [[{email_alerts,
           [{pop_up_alerts, [ip, disk, stuck_rebalance]},
            {alerts, [ip, time_out_of_sync, stuck_rebalance]}]},
          {alert_limits,
           [{{{stuck_rebalance_threshold_secs, kv}, undefined}}]}]],
    Expected3 =[{set, email_alerts,
                 [{pop_up_alerts,
                   [disk, disk_guardrail, ip]},
                  {alerts,
                   [disk_guardrail, ip, time_out_of_sync]}]}],
    %% Remove stuck_rebalance keys if found and threshold undefined
    ?assertEqual(Expected3, config_upgrade_to_79(Config3)),

    Config4 =
        [[{email_alerts,
           [{pop_up_alerts, [ip, disk, stuck_rebalance]},
            {alerts, [ip, time_out_of_sync, stuck_rebalance]}]},
          {alert_limits,
           [{{stuck_rebalance_threshold_secs, kv}, 100}]}]],
    Expected4 = [{set, email_alerts,
                  [{pop_up_alerts,
                    [disk, disk_guardrail, ip, stuck_rebalance]},
                   {alerts,
                    [disk_guardrail, ip, stuck_rebalance, time_out_of_sync]}]}],
    %% Don't remove stuck_rebalance keys if a threshold has been set
    ?assertEqual(Expected4, config_upgrade_to_79(Config4)).

%% Test that the stuck time is correctly updated based on rebalance progress
test_rebalance_progress(Service, Time, Progress, StuckStart, Opaque0) ->
    meck:expect(ns_rebalance_observer, get_progress_for_alerting,
                fun (S) when S =:= Service ->
                        Progress
                end),
    %% Ensure that the query period is 1s
    Threshold = ?REBALANCE_CHECK_FREQ,
    TimeNative = erlang:convert_time_unit(Time, second, native),
    StuckTime = Time - StuckStart,
    ExpectedStuck = case StuckTime >= Threshold of true -> 1; false -> 0 end,
    {Stuck, Opaque1} = maybe_alert_stuck_rebalance(Service, TimeNative,
                                                   Threshold, Opaque0),
    ?assertEqual(ExpectedStuck, Stuck),
    StuckStartNative = erlang:convert_time_unit(StuckStart, second, native),
    ?assertMatch([{{rebalance_progress, Service},
                   #rebalance_progress{stuck_timestamp = StuckStartNative}}],
                 dict:to_list(Opaque1)),
    Opaque1.

test_kv_rebalance_progress(Time, Progress, StuckTime, Opaque0) ->
    test_rebalance_progress(kv, Time, Progress, StuckTime, Opaque0).

test_index_rebalance_progress(Time, Progress, StuckTime, Opaque0) ->
    test_rebalance_progress(index, Time, Progress, StuckTime, Opaque0).

check_kv_rebalance_progress_test() ->
    Opaque0 = dict:new(),
    %% Initialisation of rebalance progress:
    %% time = 0, progress = 0,
    %% expected stuck time = 0
    Opaque1 = test_kv_rebalance_progress(
                0, {<<>>, 0},
                0, Opaque0),
    %% progress 0 -> 0 (unchanged), time 0 -> 1 =>
    %% stuck time 0 -> 0 (stuck)
    Opaque2 = test_kv_rebalance_progress(
                1, {<<>>, 0},
                0, Opaque1),
    %% progress 0 -> 1 (changed) =>
    %% stuck time 0 -> 2 (not stuck)
    Opaque3 = test_kv_rebalance_progress(
                2, {<<>>, 1},
                2, Opaque2),
    %% progress 1 -> 1 (unchanged) =>
    %% stuck time 2 -> 2 (stuck)
    Opaque4 = test_kv_rebalance_progress(
                3, {<<>>, 1},
                2, Opaque3),
    %% progress 1 -> 2 (changed) & timeout not reached =>
    %% stuck time 2 -> 2 (unchanged)
    Opaque5 = test_kv_rebalance_progress(
                3, {<<>>, 2},
                2, Opaque4),
    %% Stuck counter reset as progress changed (despite decreasing)
    %% progress 1 -> 0 (changed) =>
    %% stuck time 2 -> 4 (not stuck)
    Opaque6 = test_kv_rebalance_progress(
                4, {<<>>, 0},
                4, Opaque5),
    %% progress 0 -> 0 (unchanged) =>
    %% stuck time 4 -> 4 (stuck)
    Opaque7 = test_kv_rebalance_progress(
                5, {<<>>, 0},
                4, Opaque6),
    %% Progress doesn't need to be a number
    %% progress 0 -> {<<>>, [{0, 0}]} (changed) =>
    %% stuck time 4 -> 6 (not stuck)
    Opaque8 = test_kv_rebalance_progress(
                6, {<<>>, [{0, 0}]},
                6, Opaque7),
    %% progress unchanged => stuck time 6 -> 6 (stuck)
    Opaque9 = test_kv_rebalance_progress(
                7, {<<>>, [{0, 0}]},
                6, Opaque8),
    %% progress {<<>>, [{0, 0}]} -> {<<>>, []} (changed) =>
    %% stuck time 6 -> 8 (not stuck)
    Opaque10 = test_kv_rebalance_progress(
                 8, {<<>>, []},
                 8, Opaque9),
    %% For a kv rebalance if no moves are in progress, none are stuck
    %% progress {<<>>, []} -> {<<>>, []} (unchanged) =>
    %% stuck time 8 -> 9 (not stuck)
    Opaque11 = test_kv_rebalance_progress(
                 9,
                 {<<>>, []},
                 9, Opaque10),
    %% progress {<<>>, []} -> {<<0>>, []} (new rebalance) =>
    %% stuck time 9 -> 10 (not stuck)
    _Opaque12 = test_kv_rebalance_progress(
                  10,
                  {<<0>>, []},
                  10, Opaque11),
    meck:unload().

check_index_rebalance_progress_test() ->
    Opaque0 = dict:new(),
    %% Initialisation of rebalance progress:
    %% time = 0, progress = 0,
    %% expected stuck time = 0
    Opaque1 = test_index_rebalance_progress(
                0, {<<>>, 0},
                0, Opaque0),
    %% progress 0 -> 0 (unchanged), time 0 -> 1 =>
    %% stuck time 0 -> 0 (stuck)
    Opaque2 = test_index_rebalance_progress(
                1, {<<>>, 0},
                0, Opaque1),
    %% progress 0 -> 1 (changed) =>
    %% stuck time 0 -> 2 (not stuck)
    Opaque3 = test_index_rebalance_progress(
                2, {<<>>, 1},
                2, Opaque2),
    %% progress 1 -> 1 (unchanged) =>
    %% stuck time 2 -> 2 (stuck)
    Opaque4 = test_index_rebalance_progress(
                3, {<<>>, 1},
                2, Opaque3),
    %% progress 1 -> 2 (changed) & timeout not reached =>
    %% stuck time 2 -> 2 (unchanged)
    Opaque5 = test_index_rebalance_progress(
                3, {<<>>, 2},
                2, Opaque4),
    %% Stuck counter reset as progress changed (despite decreasing)
    %% progress 1 -> 0 (changed) =>
    %% stuck time 2 -> 4 (not stuck)
    Opaque6 = test_index_rebalance_progress(
                4, {<<>>, 0},
                4, Opaque5),
    %% progress 0 -> 0 (unchanged) =>
    %% stuck time 4 -> 4 (stuck)
    _Opaque7 = test_index_rebalance_progress(
                5, {<<>>, 0},
                4, Opaque6),
    meck:unload().
-endif.
