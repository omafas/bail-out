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
-module(menelaus_web_auto_failover).

-include("ns_common.hrl").

-export([handle_settings_get/1,
         handle_settings_post/1,
         handle_settings_reset_count/1,
         get_failover_on_disk_issues/1,
         config_check_can_abort_rebalance/0,
         config_upgrade_to_65/1,
         config_upgrade_to_55/1]).

-import(menelaus_util,
        [reply/2,
         reply_json/2,
         reply_json/3,
         reply_text/3,
         parse_validate_number/3,
         parse_validate_boolean_field/3]).

-define(AUTO_FAILOVER_MIN_TIMEOUT, ?get_param(auto_failover_min_timeout, 5)).
-define(AUTO_FAILOVER_MIN_CE_TIMEOUT, 30).
-define(AUTO_FAILOVER_MAX_TIMEOUT, 3600).

-define(CAN_ABORT_REBALANCE_CONFIG_KEY, can_abort_rebalance).
-define(DATA_DISK_ISSUES_CONFIG_KEY, failover_on_data_disk_issues).
-define(MIN_DATA_DISK_ISSUES_TIMEPERIOD, 5). %% seconds
-define(MAX_DATA_DISK_ISSUES_TIMEPERIOD, 3600). %% seconds
-define(DEFAULT_DATA_DISK_ISSUES_TIMEPERIOD, 120). %% seconds

-define(FAILOVER_SERVER_GROUP_CONFIG_KEY, failover_server_group).

-define(MAX_EVENTS_CONFIG_KEY, max_count).
-define(MAX_EVENTS_ALLOWED, 3).
-define(MIN_EVENTS_ALLOWED, 1).
-define(DEFAULT_EVENTS_ALLOWED, 1).

handle_settings_get(Req) ->
    {value, Config} = ns_config:search(ns_config:get(), auto_failover_cfg),
    Enabled = proplists:get_value(enabled, Config),
    Timeout = proplists:get_value(timeout, Config),
    Count = proplists:get_value(count, Config),
    Settings0 = [{enabled, Enabled}, {timeout, Timeout}, {count, Count}],
    Settings =  Settings0 ++ get_extra_settings(Config),
    reply_json(Req, {struct, Settings}).

handle_settings_post(Req) ->
    ValidateOnly = proplists:get_value("just_validate", mochiweb_request:parse_qs(Req)) =:= "1",
    {value, Config} = ns_config:search(ns_config:get(), auto_failover_cfg),
    case {ValidateOnly,
          validate_settings_auto_failover(mochiweb_request:parse_post(Req), Config)} of
        {false, false} ->
            auto_failover:disable(disable_extras(Config)),
            ns_audit:disable_auto_failover(Req),
            reply(Req, 200);
        {false, {error, Errors}} ->
            Errors1 = [<<Msg/binary, "\n">> || {_, Msg} <- Errors],
            reply_text(Req, Errors1, 400);
        {false, Params} ->
            Timeout = proplists:get_value(timeout, Params),
            %% maxCount will not be set for CE and pre-upgrade so use the
            %% default.
            MaxCount = proplists:get_value(maxCount, Params,
                                           ?DEFAULT_EVENTS_ALLOWED),
            Extras = proplists:get_value(extras, Params),
            auto_failover:enable(Timeout, MaxCount, Extras),
            ns_audit:enable_auto_failover(Req, Timeout, MaxCount, Extras),
            reply(Req, 200);
        {true, {error, Errors}} ->
            reply_json(Req, {struct, [{errors, {struct, Errors}}]}, 200);
        %% Validation only and no errors
        {true, _}->
            reply_json(Req, {struct, [{errors, null}]}, 200)
    end.

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

config_upgrade_to_55(Config) ->
    {value, Current} = ns_config:search(Config, auto_failover_cfg),
    [Val] = disable_failover_on_disk_issues(
              ?DEFAULT_DATA_DISK_ISSUES_TIMEPERIOD),
    New0 = lists:keystore(?DATA_DISK_ISSUES_CONFIG_KEY, 1, Current, Val),
    New1 = lists:keystore(?FAILOVER_SERVER_GROUP_CONFIG_KEY, 1, New0,
                          {?FAILOVER_SERVER_GROUP_CONFIG_KEY, false}),
    %% 5.0 and earlier, max_nodes was used to indicate the maximum number
    %% of nodes that can be auto-failed over before requiring reset of the
    %% quota.
    %% When server group auto-failover is enabled, the entire server
    %% group may be failed over, irrespective of the number of nodes in the
    %% group and value of max_nodes. So, max nodes is no longer an accurate
    %% term. max_nodes will be replaced with max_count.
    %% max_count refers to the maximum number of auto-failover
    %% events that are allowed.
    %% Even though max_nodes was present in the config in previous releases,
    %% the auto-failover code never used it. Max was hard coded to 1.
    %% Infact, 5.0 and earlier, the auto_failover:make_persistent()
    %% code inadvertently removed max_nodes from the config when auto-failover
    %% was enabled.
    New2 = lists:keystore(?MAX_EVENTS_CONFIG_KEY, 1, New1,
                          {?MAX_EVENTS_CONFIG_KEY, ?DEFAULT_EVENTS_ALLOWED}),
    New3 = lists:keydelete(max_nodes, 1, New2),
    New = lists:keystore(failed_over_server_groups, 1, New3,
                         {failed_over_server_groups, []}),
    [{set, auto_failover_cfg, New}].

config_upgrade_to_65(Config) ->
    {value, Current} = ns_config:search(Config, auto_failover_cfg),
    CanAbortRebalance = cluster_compat_mode:is_enterprise(),
    New = lists:keystore(?CAN_ABORT_REBALANCE_CONFIG_KEY, 1, Current,
                         {?CAN_ABORT_REBALANCE_CONFIG_KEY, CanAbortRebalance}),
    [{set, auto_failover_cfg, New}].

%% Internal Functions

validate_settings_auto_failover(Args, Config) ->
    case parse_validate_boolean_field("enabled", '_', Args) of
        [{ok, _, true}] ->
            parse_validate_other_params(Args, Config);
        [{ok, _, false}] ->
            false;
        _ ->
            {error, boolean_err_msg(enabled)}
    end.

parse_validate_other_params(Args, Config) ->
    Min = case cluster_compat_mode:is_enterprise() of
              true ->
                  ?AUTO_FAILOVER_MIN_TIMEOUT;
              false ->
                  ?AUTO_FAILOVER_MIN_CE_TIMEOUT
          end,
    Max = ?AUTO_FAILOVER_MAX_TIMEOUT,
    Timeout = proplists:get_value("timeout", Args),
    case parse_validate_number(Timeout, Min, Max) of
        {ok, Val} ->
            parse_validate_extras(Args, [{timeout, Val}, {extras, []}],
                                  Config);
        _ ->
            {error, range_err_msg(timeout, Min, Max)}
    end.

parse_validate_extras(Args, CurrRV, Config) ->
    case cluster_compat_mode:is_cluster_55() andalso
        cluster_compat_mode:is_enterprise() of
        true ->
            parse_validate_extras_inner(Args, CurrRV, Config);
        false ->
            %% TODO - Check for unsupported params
            CurrRV
    end.

parse_validate_extras_inner(Args, CurrRV, Config) ->
    NewRV0 = parse_validate_max_count(Args, CurrRV, Config),
    case NewRV0 of
        {error, _}  ->
            NewRV0;
        _ ->
            NewRV1 = parse_validate_failover_disk_issues(Args, NewRV0, Config),
            case NewRV1 of
                {error, _} ->
                    NewRV1;
                _ ->
                    NewRV2 = parse_validate_server_group_failover(Args, NewRV1),
                    case NewRV2 of
                        {error, _} ->
                            NewRV2;
                        _ ->
                            parse_validate_can_abort_rebalance(Args, NewRV2)
                    end
            end
    end.

parse_validate_can_abort_rebalance(Args, CurrRV) ->
    case cluster_compat_mode:is_cluster_65() of
        true ->
            parse_validate_can_abort_rebalance_inner(Args, CurrRV);
        false ->
            CurrRV
    end.

parse_validate_can_abort_rebalance_inner(Args, CurrRV) ->
    StrKey = "canAbortRebalance",
    case parse_validate_boolean_field(StrKey, '_', Args) of
        [] ->
            CurrRV;
        [{ok, _, Value}] ->
            add_extras([{?CAN_ABORT_REBALANCE_CONFIG_KEY, Value}], CurrRV);
        [{error, _, _}] ->
            {error, boolean_err_msg(StrKey)}
    end.

parse_validate_max_count(Args, CurrRV, Config) ->
    CurrMax = proplists:get_value(?MAX_EVENTS_CONFIG_KEY, Config),
    Min = ?MIN_EVENTS_ALLOWED,
    Max = ?MAX_EVENTS_ALLOWED,
    MaxCount = proplists:get_value("maxCount", Args, integer_to_list(CurrMax)),
    case parse_validate_number(MaxCount, Min, Max) of
        {ok, Val} ->
            [{maxCount, Val} | CurrRV];
        _->
            {error, range_err_msg(maxCount, Min, Max)}
    end.

parse_validate_failover_disk_issues(Args, CurrRV, Config) ->
    Key = "failoverOnDataDiskIssues",
    KeyEnabled = Key ++ "[enabled]",
    KeyTimePeriod = Key ++ "[timePeriod]",

    TimePeriod = proplists:get_value(KeyTimePeriod, Args),
    Min = ?MIN_DATA_DISK_ISSUES_TIMEPERIOD,
    Max = ?MAX_DATA_DISK_ISSUES_TIMEPERIOD,
    TimePeriodParsed = parse_validate_number(TimePeriod, Min, Max),

    case parse_validate_boolean_field(KeyEnabled, '_', Args) of
        [{ok, _, true}] ->
            case TimePeriodParsed of
                {ok, Val} ->
                    Extra = set_failover_on_disk_issues(true, Val),
                    add_extras(Extra, CurrRV);
                _ ->
                    {error, range_err_msg(KeyTimePeriod, Min, Max)}
            end;
        [{ok, _, false}] ->
            {_, CurrTP} = get_failover_on_disk_issues(Config),
            Extra = disable_failover_on_disk_issues(CurrTP),
            add_extras(Extra, CurrRV);
        [] ->
            case TimePeriodParsed =/= invalid of
                true ->
                    %% User has passed the timePeriod paramater
                    %% but enabled is missing.
                    {error, boolean_err_msg(KeyEnabled)};
                false ->
                    CurrRV
            end;
        _ ->
            {error, boolean_err_msg(KeyEnabled)}
    end.

disable_failover_on_disk_issues(TP) ->
    set_failover_on_disk_issues(false, TP).

set_failover_on_disk_issues(Enabled, TP) ->
    [{?DATA_DISK_ISSUES_CONFIG_KEY, [{enabled, Enabled}, {timePeriod, TP}]}].

parse_validate_server_group_failover(Args, CurrRV) ->
    Key = "failoverServerGroup",
    case parse_validate_boolean_field(Key, '_', Args) of
        [{ok, _, Val}] ->
            Extra = [{?FAILOVER_SERVER_GROUP_CONFIG_KEY, Val}],
            add_extras(Extra, CurrRV);
        [] ->
            CurrRV;
        _ ->
            {error, boolean_err_msg(Key)}
    end.

add_extras(Add, CurrRV) ->
    {extras, Old} = lists:keyfind(extras, 1, CurrRV),
    lists:keyreplace(extras, 1, CurrRV, {extras, Add ++ Old}).

range_err_msg(Key, Min, Max) ->
    [{Key, list_to_binary(io_lib:format("The value of \"~s\" must be a positive integer in a range from ~p to ~p", [Key, Min, Max]))}].

boolean_err_msg(Key) ->
    [{Key, list_to_binary(io_lib:format("The value of \"~s\" must be true or false", [Key]))}].

config_check_can_abort_rebalance() ->
    {value, Cfg} = ns_config:search(ns_config:get(), auto_failover_cfg),
    proplists:get_value(?CAN_ABORT_REBALANCE_CONFIG_KEY, Cfg, false).

get_extra_settings(Config) ->
    case cluster_compat_mode:is_cluster_55() andalso
        cluster_compat_mode:is_enterprise() of
        true ->
            SGFO = proplists:get_value(?FAILOVER_SERVER_GROUP_CONFIG_KEY,
                                       Config),
            Max = proplists:get_value(?MAX_EVENTS_CONFIG_KEY, Config),
            {Enabled, TimePeriod} = get_failover_on_disk_issues(Config),
            CanAbortRebalance =
                case cluster_compat_mode:is_cluster_65() of
                    true ->
                        V = proplists:get_value(
                              ?CAN_ABORT_REBALANCE_CONFIG_KEY, Config),
                        [{canAbortRebalance, V}];
                    false ->
                        []
                end,
            [{failoverOnDataDiskIssues,
              {struct, [{enabled, Enabled}, {timePeriod, TimePeriod}]}},
             {failoverServerGroup, SGFO},
             {maxCount, Max}] ++ CanAbortRebalance;
        false ->
            []
    end.

disable_extras(Config) ->
    case cluster_compat_mode:is_cluster_55() andalso
        cluster_compat_mode:is_enterprise() of
        true ->
            CanAbortRebalance =
                case cluster_compat_mode:is_cluster_65() of
                    true ->
                        [{?CAN_ABORT_REBALANCE_CONFIG_KEY, false}];
                    false ->
                        []
                end,
            {_, CurrTP} = get_failover_on_disk_issues(Config),
            disable_failover_on_disk_issues(CurrTP) ++ CanAbortRebalance ++
                [{?FAILOVER_SERVER_GROUP_CONFIG_KEY, false}];
        false ->
            []
    end.
