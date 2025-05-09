%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(menelaus_web_auto_failover).

-include_lib("ns_common/include/cut.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_settings_get/1,
         handle_settings_post/1,
         handle_settings_reset_count/1,
         get_failover_on_disk_issues/1,
         get_failover_on_disk_non_responsiveness/1,
         config_check_can_abort_rebalance/0,
         default_config/1,
         get_stats/0,
         config_upgrade_to_72/1,
         config_upgrade_to_76/1,
         config_upgrade_to_morpheus/1]).

-import(menelaus_util,
        [reply/2,
         reply_json/2]).

-define(AUTO_FAILOVER_MIN_TIMEOUT_76, 1).
-define(AUTO_FAILOVER_MIN_TIMEOUT_PRE_76,
            ?get_param(auto_failover_min_timeout, 5)).
-define(AUTO_FAILOVER_MIN_CE_TIMEOUT, 30).
-define(AUTO_FAILOVER_MAX_TIMEOUT, 3600).

-define(CAN_ABORT_REBALANCE_CONFIG_KEY, can_abort_rebalance).
-define(DATA_DISK_ISSUES_CONFIG_KEY, failover_on_data_disk_issues).
-define(MIN_DATA_DISK_ISSUES_TIMEPERIOD,
        ?get_param(min_data_disk_issues_timeperiod, 5)). %% seconds
-define(MAX_DATA_DISK_ISSUES_TIMEPERIOD, 3600). %% seconds

-define(DATA_DISK_NON_RESPONSIVENESS_CONFIG_KEY,
        failover_on_data_disk_non_responsiveness).
-define(MIN_DATA_DISK_NON_RESPONSIVENESS_TIMEPERIOD,
  ?get_param(min_data_disk_non_responsiveness_timeperiod, 5)). %% seconds
-define(DEFAULT_DATA_DISK_NON_RESPONSIVENESS_TIMEPERIOD,
  ?get_param(default_data_disk_non_responsiveness_timeperiod, 120)). %% seconds
-define(MAX_DATA_DISK_NON_RESPONSIVENESS_TIMEPERIOD, 3600). %% seconds
-define(FAILOVER_ON_DATA_DISK_NON_RESPONSIVENESS_DEFAULT,
  {?DATA_DISK_NON_RESPONSIVENESS_CONFIG_KEY,
    [{enabled, false},
     {timePeriod, ?DEFAULT_DATA_DISK_NON_RESPONSIVENESS_TIMEPERIOD}]}).

-define(FAILOVER_PRESERVE_DURABILITY_MAJORITY_CONFIG_KEY,
        failover_preserve_durability_majority).
-define(FAILOVER_PRESERVE_DURABILITY_MAJORITY_DEFAULT, false).

-define(ALLOW_FAILOVER_EPHEMERAL_NO_REPLICAS_CONFIG_KEY,
        allow_failover_ephemeral_no_replicas).

-define(ROOT_CONFIG_KEY, auto_failover_cfg).

-define(DISABLE_MAX_COUNT_CONFIG_KEY, disable_max_count).
-define(MAX_EVENTS_CONFIG_KEY, max_count).
-define(MIN_EVENTS_ALLOWED, 1).
-define(DEFAULT_EVENTS_ALLOWED, 1).

%% Default settings reflect settings of the minimum supported cluster version.
%% Settings for newer versions will be added in ns_online_config_upgrader.
%% Specify those in a config_upgrade_to_X function.
default_config(IsEnterprise) ->
    default_config(?MIN_SUPPORTED_VERSION, IsEnterprise).

default_config(?MIN_SUPPORTED_VERSION, IsEnterprise) ->
    [{?ROOT_CONFIG_KEY,
      [{enabled, true},
       % timeout is the time (in seconds) a node needs to be
       % down before it is automatically faileovered
       {timeout, 120},
       % count is the number of nodes that were auto-failovered
       {count, 0},
       {failover_on_data_disk_issues, [{enabled, false},
                                       {timePeriod, 120}]},
       {max_count, 1},
       {?CAN_ABORT_REBALANCE_CONFIG_KEY, IsEnterprise}]}].

max_events_allowed() ->
    100.

config_upgrade_to_76(Config) ->
    [{set, auto_failover_cfg,
      misc:update_proplist(auto_failover:get_cfg(Config),
                           [{?DISABLE_MAX_COUNT_CONFIG_KEY,
                             config_profile:get_bool(
                               failover_disable_max_count)}])}].

config_upgrade_to_morpheus(Config) ->
    %% Merging existing cfg over the default to retain any already configured
    %% settings (we may have configured some in 7.6.3 or newer).
    [{set, auto_failover_cfg,
      misc:update_proplist(
        [{?ALLOW_FAILOVER_EPHEMERAL_NO_REPLICAS_CONFIG_KEY,
          auto_failover:hidden_failover_ephemeral_setting()},
         ?FAILOVER_ON_DATA_DISK_NON_RESPONSIVENESS_DEFAULT],
        auto_failover:get_cfg(Config))}].

interesting_stats() ->
    [enabled, count, maxCount].

exported_stat_name(maxCount) -> max_count;
exported_stat_name(Other) -> Other.

get_stats() ->
    Settings = settings_get_inner(),
    lists:filtermap(
      fun (Name) ->
              case lists:keyfind(Name, 1, Settings) of
                  {Name, Value} ->
                      {true, {exported_stat_name(Name), Value}};
                  false ->
                      false
              end
      end, interesting_stats()).

handle_settings_get(Req) ->
    Settings = settings_get_inner(),
    reply_json(Req, {Settings}).

settings_get_inner() ->
    Config = auto_failover:get_cfg(),
    Enabled = proplists:get_value(enabled, Config),
    Timeout = proplists:get_value(timeout, Config),
    Count = proplists:get_value(count, Config),
    Settings = [{enabled, Enabled}, {timeout, Timeout}, {count, Count}],
    Settings ++ get_extra_settings(Config).

handle_settings_post_validated(Req, Props) ->
    Config = auto_failover:get_cfg(),
    case proplists:get_value(enabled, Props) of
        true ->
            Timeout = proplists:get_value(timeout, Props),
            case cluster_compat_mode:is_enterprise() of
                true ->
                    %% maxCount will not be set pre-upgrade so use the default.
                    CurrMax = proplists:get_value(?MAX_EVENTS_CONFIG_KEY,
                                                  Config,
                                                  ?DEFAULT_EVENTS_ALLOWED),
                    MaxCount = proplists:get_value(maxCount, Props, CurrMax);
                false ->
                    %% maxCount will not be set for CE so use the default.
                    MaxCount = proplists:get_value(maxCount, Props,
                                                   ?DEFAULT_EVENTS_ALLOWED)
            end,
            Extras = process_extras(Props, Config),
            auto_failover:enable(Timeout, MaxCount, Extras),
            ns_audit:enable_auto_failover(Req, Timeout, MaxCount, Extras);
        false ->
            auto_failover:disable(disable_disk_failover(Config)),
            ns_audit:disable_auto_failover(Req)
    end,
    reply(Req, 200).

handle_settings_post(Req) ->
    validator:handle(
      handle_settings_post_validated(Req, _), Req, form,
      settings_validators() ++
      [validator:no_duplicates(_),
       validator:unsupported(_)]).

%% @doc Resets the number of nodes that were automatically failovered to zero
handle_settings_reset_count(Req) ->
    auto_failover:reset_count(),
    ns_audit:reset_auto_failover_count(Req),
    reply(Req, 200).

get_failover_on_disk_issues(Config) ->
    case proplists:get_value(?DATA_DISK_ISSUES_CONFIG_KEY, Config) of
        undefined ->
            undefined;
        Val ->
            Enabled = proplists:get_value(enabled, Val),
            TimePeriod = proplists:get_value(timePeriod, Val),
            {Enabled, TimePeriod}
    end.

get_failover_on_disk_non_responsiveness(Config) ->
    case proplists:get_value(?DATA_DISK_NON_RESPONSIVENESS_CONFIG_KEY,
                             Config) of
        undefined -> undefined;
        Val ->
            Enabled = proplists:get_value(enabled, Val),
            TimePeriod = proplists:get_value(timePeriod, Val),
            {Enabled, TimePeriod}
    end.

%% Internal Functions
get_min_timeout() ->
    Is76 = cluster_compat_mode:is_cluster_76(),
    case cluster_compat_mode:is_enterprise() of
        true when Is76->
            ?AUTO_FAILOVER_MIN_TIMEOUT_76;
        true ->
            ?AUTO_FAILOVER_MIN_TIMEOUT_PRE_76;
        false ->
            ?AUTO_FAILOVER_MIN_CE_TIMEOUT
    end.

validate_enabled_param(KeyEnabled, KeyTime, State) ->
    validator:validate_multiple(
      fun ([EVal, TVal]) ->
              case {EVal, TVal =:= undefined} of
                  {true, true} ->
                      {error,
                       io_lib:format("~s is true. A value must be supplied for "
                                     "~s", [KeyEnabled, KeyTime])};
                  {undefined, false} ->
                      {error,
                       io_lib:format("~s must be true for ~s to take effect",
                                     [KeyEnabled, KeyTime])};
                  {_, _} -> ok
              end
      end, [KeyEnabled, KeyTime], State).

validate_maxcount_param(State) ->
    ErrMsg = "disableMaxCount is true. Set it to false for maxCount to take "
             "effect.",
    validator:validate_multiple(
        fun ([Disabled, Count]) ->
            case {Disabled, Count =/= undefined} of
                {true, true} -> {error, ErrMsg};
                {undefined, true} ->
                    Config = auto_failover:get_cfg(),
                    DisabledMax = proplists:get_value(
                                    ?DISABLE_MAX_COUNT_CONFIG_KEY, Config),
                    case DisabledMax of
                        true -> {error, ErrMsg};
                        false -> ok
                    end;
                {_, _} -> ok
            end
        end, [disableMaxCount, maxCount], State).

settings_validators() ->
    [validator:required(enabled, _),
     validator:boolean(enabled, _),
     validator:integer(timeout, get_min_timeout(),
                       ?AUTO_FAILOVER_MAX_TIMEOUT, _),
     validate_enabled_param(enabled, timeout, _)] ++
    settings_extras_validators().

settings_extras_validators() ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            maxcount_validators() ++
            disk_issues_validators() ++
            disk_non_responsiveness_validators() ++
            can_abort_rebalance_validators() ++
            preserve_durability_majority_validators() ++
            failover_ephemeral_no_replicas_validators();
        false ->
            []
    end.

maxcount_validators() ->
    [validator:integer(maxCount, ?MIN_EVENTS_ALLOWED,
        max_events_allowed(), _)] ++
    case cluster_compat_mode:is_cluster_76() of
        true ->
            [validator:boolean(disableMaxCount, _),
             validate_maxcount_param(_)];
        false -> []
    end.

disk_issues_validators() ->
    KeyEnabled = 'failoverOnDataDiskIssues[enabled]',
    KeyTimePeriod = 'failoverOnDataDiskIssues[timePeriod]',
    [validator:boolean(KeyEnabled, _),
     validator:integer(KeyTimePeriod, ?MIN_DATA_DISK_ISSUES_TIMEPERIOD,
                       ?MAX_DATA_DISK_ISSUES_TIMEPERIOD, _),
     validate_enabled_param(KeyEnabled, KeyTimePeriod, _)].

disk_non_responsiveness_validators() ->
    KeyEnabled = 'failoverOnDataDiskNonResponsiveness[enabled]',
    KeyTimePeriod = 'failoverOnDataDiskNonResponsiveness[timePeriod]',
    [validator:boolean(KeyEnabled, _),
     validator:integer(KeyTimePeriod,
                       ?MIN_DATA_DISK_NON_RESPONSIVENESS_TIMEPERIOD,
                       ?MAX_DATA_DISK_NON_RESPONSIVENESS_TIMEPERIOD, _),
     validate_enabled_param(KeyEnabled, KeyTimePeriod, _)].

preserve_durability_majority_validators() ->
    case cluster_compat_mode:is_cluster_72() of
        false -> [];
        true ->
            [validator:boolean(failoverPreserveDurabilityMajority, _)]
    end.

failover_ephemeral_no_replicas_validators() ->
    case cluster_compat_mode:is_cluster_morpheus() of
        false -> [];
        true ->
            [validator:boolean(allowFailoverEphemeralNoReplicas, _)]
    end.

can_abort_rebalance_validators() ->
    [validator:boolean(canAbortRebalance, _)].

process_failover_on_disk_issues(Props, Config, Extras) ->
    KeyEnabled = 'failoverOnDataDiskIssues[enabled]',
    KeyTimePeriod = 'failoverOnDataDiskIssues[timePeriod]',
    TimePeriod = proplists:get_value(KeyTimePeriod, Props),
    DiskEnabled = proplists:get_value(KeyEnabled, Props),
    case DiskEnabled of
        true ->
            Extra = set_failover_on_disk_issues(true, TimePeriod),
            add_extras(Extra, Extras);
        false ->
            {_, CurrTP} = get_failover_on_disk_issues(Config),
            Extra = disable_failover_on_disk_issues(CurrTP),
            add_extras(Extra, Extras);
        undefined ->
            Extras
    end.

process_failover_on_disk_non_responsiveness(Props, Config, Extras) ->
    KeyEnabled = 'failoverOnDataDiskNonResponsiveness[enabled]',
    KeyTimePeriod = 'failoverOnDataDiskNonResponsiveness[timePeriod]',
    TimePeriod = proplists:get_value(KeyTimePeriod, Props),
    DiskEnabled = proplists:get_value(KeyEnabled, Props),
    case DiskEnabled of
        true ->
            Extra = set_failover_on_disk_non_responsiveness(true, TimePeriod),
            add_extras(Extra, Extras);
        false ->
            {_, CurrTP} = get_failover_on_disk_non_responsiveness(Config),
            Extra = disable_failover_on_disk_non_responsiveness(CurrTP),
            add_extras(Extra, Extras);
        undefined ->
            Extras
    end.

process_boolean_extra(Props, Name, ConfigKey, Extras) ->
    CVal = proplists:get_value(Name, Props),
    case CVal of
        Val when is_boolean(Val) ->
            Extra = [{ConfigKey, Val}],
            add_extras(Extra, Extras);
        _ ->
            Extras
    end.

process_extras(Props, Config) ->
    BoolParams = [{canAbortRebalance, ?CAN_ABORT_REBALANCE_CONFIG_KEY},
                  {disableMaxCount, ?DISABLE_MAX_COUNT_CONFIG_KEY},
                  {failoverPreserveDurabilityMajority,
                   ?FAILOVER_PRESERVE_DURABILITY_MAJORITY_CONFIG_KEY},
                  {allowFailoverEphemeralNoReplicas,
                   ?ALLOW_FAILOVER_EPHEMERAL_NO_REPLICAS_CONFIG_KEY}],
    Extras = functools:chain(
               [{extras, []}],
               [process_failover_on_disk_issues(Props, Config, _) |
                [process_boolean_extra(Props, Name, ConfigKey, _) ||
                    {Name, ConfigKey} <- BoolParams]] ++
               [process_failover_on_disk_non_responsiveness(Props, Config, _)
                   || cluster_compat_mode:is_cluster_morpheus()]),
    proplists:get_value(extras, Extras).

disable_failover_on_disk_issues(TP) ->
    set_failover_on_disk_issues(false, TP).

disable_failover_on_disk_non_responsiveness(TP) ->
  set_failover_on_disk_non_responsiveness(false, TP).

set_failover_on_disk_issues(Enabled, TP) ->
    [{?DATA_DISK_ISSUES_CONFIG_KEY, [{enabled, Enabled}, {timePeriod, TP}]}].

set_failover_on_disk_non_responsiveness(Enabled, TP) ->
    [{?DATA_DISK_NON_RESPONSIVENESS_CONFIG_KEY,
     [{enabled, Enabled}, {timePeriod, TP}]}].

add_extras(Add, CurrRV) ->
    {extras, Old} = lists:keyfind(extras, 1, CurrRV),
    lists:keyreplace(extras, 1, CurrRV, {extras, Add ++ Old}).

config_check_can_abort_rebalance() ->
    proplists:get_value(?CAN_ABORT_REBALANCE_CONFIG_KEY,
                        auto_failover:get_cfg(), false).

get_extra_settings(Config) ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            DisableMaxCount = proplists:get_value(
                                ?DISABLE_MAX_COUNT_CONFIG_KEY, Config, false),
            {Enabled, TimePeriod} = get_failover_on_disk_issues(Config),
            lists:flatten(
              [{failoverOnDataDiskIssues,
                {[{enabled, Enabled}, {timePeriod, TimePeriod}]}},
               [{disableMaxCount, DisableMaxCount} ||
                   cluster_compat_mode:is_cluster_76()],
               [{maxCount, proplists:get_value(?MAX_EVENTS_CONFIG_KEY,
                                               Config)} ||
                   not DisableMaxCount],
               [{canAbortRebalance,
                 proplists:get_value(
                   ?CAN_ABORT_REBALANCE_CONFIG_KEY, Config)}],
               [{failoverPreserveDurabilityMajority,
                 proplists:get_value(
                     ?FAILOVER_PRESERVE_DURABILITY_MAJORITY_CONFIG_KEY,
                     Config)}
                   || cluster_compat_mode:is_cluster_72()],
               [{failoverOnDataDiskNonResponsiveness,
                 {[{enabled, DNREnabled}, {timePeriod, DNRTimePeriod}]}} ||
                {DNREnabled, DNRTimePeriod} <-
                    [get_failover_on_disk_non_responsiveness(Config)],
                   cluster_compat_mode:is_cluster_morpheus()],
               [{allowFailoverEphemeralNoReplicas,
                 proplists:get_value(
                   ?ALLOW_FAILOVER_EPHEMERAL_NO_REPLICAS_CONFIG_KEY,
                   Config)}
                || cluster_compat_mode:is_cluster_morpheus()]]);
        false ->
            []
    end.

disable_disk_failover(Config) ->
    case cluster_compat_mode:is_enterprise() of
        true ->
            {_, IssuesTP} = get_failover_on_disk_issues(Config),
            disable_failover_on_disk_issues(IssuesTP) ++
                case cluster_compat_mode:is_cluster_morpheus() of
                    false -> [];
                    true ->
                        {_, NonRespTP} =
                            get_failover_on_disk_non_responsiveness(Config),
                        disable_failover_on_disk_non_responsiveness(NonRespTP)
                end;
        false ->
            []
    end.

config_upgrade_to_72(Config) ->
    [{set, ?ROOT_CONFIG_KEY,
      auto_failover:get_cfg(Config) ++
          [{?FAILOVER_PRESERVE_DURABILITY_MAJORITY_CONFIG_KEY,
            ?FAILOVER_PRESERVE_DURABILITY_MAJORITY_DEFAULT}]}].

-ifdef(TEST).
config_upgrade_to_morpheus_test() ->
    meck:new(cluster_compat_mode),
    meck:expect(cluster_compat_mode, is_cluster_morpheus, fun() -> true end),

    meck:new(ns_config, [passthrough]),
    meck:expect(ns_config, search_node_with_default,
        fun(_, Default) ->
            Default
        end),
    meck:expect(ns_config, read_key_fast,
        fun(_, Default) ->
            Default
        end),

    BaseConfig = default_config(true),
    [{set, auto_failover_cfg, DefaultUpgradedCfg}] =
        config_upgrade_to_morpheus([BaseConfig]),
    ?assertEqual([{enabled, false}, {timePeriod, 120}],
                 proplists:get_value(?DATA_DISK_NON_RESPONSIVENESS_CONFIG_KEY,
                                     DefaultUpgradedCfg)),

    ExistingCfg =
        misc:update_proplist(DefaultUpgradedCfg,
                             [{?DATA_DISK_NON_RESPONSIVENESS_CONFIG_KEY,
                               [{enabled, true}, {timePeriod, 5}]}]),
    [{set, auto_failover_cfg, IgnoreExistingDiskNonRespCfg}] =
        config_upgrade_to_morpheus([[{?ROOT_CONFIG_KEY, ExistingCfg}]]),
    ?assertEqual([{enabled, true}, {timePeriod, 5}],
                 proplists:get_value(?DATA_DISK_NON_RESPONSIVENESS_CONFIG_KEY,
                                     IgnoreExistingDiskNonRespCfg)),

    meck:unload().
-endif.
