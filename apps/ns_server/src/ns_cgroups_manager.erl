%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Gen-server intended to manage the current state of the cgroups v2 on the
%% current node.

-module(ns_cgroups_manager).

-behaviour(gen_server).

-include("ns_common.hrl").
-include_lib("ns_common.hrl").

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([override_profile/2,
         get_overrides/0,
         recheck_cgroups/0,
         recheck_if_enabled/0,
         delete_override/1,
         collect_cgroup_data/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {timer = none :: none | timer:tref()}).

-type limits() :: #limits{}.

-define(DEFAULT_TIMEOUT, 5_000). %% 5 seconds
-define(CHECK_CGROUPS_RATE, 10_000). %% 10 seconds

%%%===================================================================
%%% API
%%%===================================================================

-spec(override_profile(service_name(), limits()) -> [{atom(), limits()}]).
override_profile(Service, Limits) ->
    gen_server:call(?MODULE, {set_overrides, {Service, Limits}}).

-spec(get_overrides() -> [{atom(), limits()}]).
get_overrides() ->
    gen_server:call(?MODULE, get_overrides).

-spec(delete_override(service_name()) -> boolean()).
delete_override(Service) ->
    gen_server:call(?MODULE, {delete_override, Service}).

-spec(recheck_cgroups() -> ok).
recheck_cgroups() ->
    ?MODULE ! check_cgroups,
    ok.

-spec(recheck_if_enabled() -> ok).
recheck_if_enabled() ->
    case cgroups:supported() andalso cgroups:has_feature_enabled() of
        true ->
            ns_cgroups_manager:recheck_cgroups(); %% async
        false ->
            ok
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    ns_pubsub:subscribe_link(ns_config_events, fun handle_event/1),
    {ok, Tref} = timer:send_after(?CHECK_CGROUPS_RATE, check_cgroups),
    {ok, #state{timer=Tref}}.

handle_event({enable_cgroups, _Value}) ->
    recheck_cgroups();
handle_event(_Event) ->
    ok.

handle_call({delete_override, Service}, _From, State) ->
    OldList = get_ns_config_overrides(),
    NewList = proplists:delete(Service, OldList),
    Output =
        case NewList =/= OldList of
            true ->
                ok = ns_config:set({node, node(), cgroup_overrides},
                                   NewList),
                MemoryQuota =
                    case memory_quota:get_quota(Service) of
                        {ok, Quota} ->
                            Quota;
                        _ ->
                            max
                    end,
                #limits{hard=H, soft=S} =
                    merge_with_memory_quota(
                      cgroups:service_to_limits_type(Service), MemoryQuota),
                event_log:add_log(cgroups_changed,
                                  [{Service,
                                    {[{hard, zero_to_max(H)},
                                      {soft, zero_to_max(S)}]}}]),
                true;
            false ->
                false
        end,
    {reply, Output, check_cgroups_reset_timer(State)};
handle_call(get_overrides, _From, State) ->
    {reply, get_ns_config_overrides(), State};
handle_call({set_overrides,
             {Service, #limits{hard=H, soft=S} = Limits}}, _From, State) ->
    Old = case ns_config:search_node(cgroup_overrides) of
              {value, X} ->
                  proplists:delete(Service, X);
              _ ->
                  []
          end,
    New = Old ++ [{Service, Limits}],
    ok = ns_config:set({node, node(), cgroup_overrides}, New),
    event_log:add_log(cgroups_changed, [{Service, {[{hard, H}, {soft, S}]}}]),
    {reply, New, check_cgroups_reset_timer(State)};
handle_call(_T, _From, State) ->
    {reply, ok, State}.

handle_cast(_T, State) ->
    {noreply, State}.

handle_info(check_cgroups, State) ->
    {noreply, check_cgroups_reset_timer(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_move_process(_OsPid, _IsCorrect, none) ->
    ok;
maybe_move_process(OsPid, false, Path) when is_integer(OsPid) ->
    cgroups:move_process(OsPid, Path);
maybe_move_process(_OsPid, _IsCorrect, _Path) ->
    ok.

default_systemd_cgroups_data() ->
    cgroups:read_system_cgroups(cgroups:get_cgroup_base_path()).

check_cgroups_reset_timer(#state{timer = TrefOld} = State) ->
    ?flush(check_cgroups),

    %% cleanup timer before rechecking s/t we don't crash and leak the timer
    {ok, cancel} = timer:cancel(TrefOld),
    assert_all_ok(move_and_fixup_memory(collect_cgroup_data(),
                                        default_systemd_cgroups_data())),
    {ok, Tref} = timer:send_after(?CHECK_CGROUPS_RATE, check_cgroups),
    State#state{timer=Tref}.

assert_all_ok(ListOfResponses) ->
    true =
        lists:all(fun (ok) ->
                          true;
                      (_) ->
                          false
                  end, ListOfResponses).

get_ns_config_overrides() ->
    ns_config:search_node_with_default(cgroup_overrides, []).

collect_cgroup_data() ->
    collect_cgroup_data(get_all_os_pids()).

collect_cgroup_data(ServicePids) ->
    lists:map(
      fun ({Svc, MainOsPid, CgroupPath}) ->
              case CgroupPath of
                  none ->
                      {Svc, MainOsPid, true, none, max};
                  Path ->
                      {ok, Quota} = memory_quota:get_quota(Svc),
                      {Svc, MainOsPid,
                       lists:member(MainOsPid,
                                    cgroups:read_cgroup_procs(Path)),
                       Path, zero_to_max(Quota)}
              end
      end, ServicePids).

move_and_fixup_memory(CgroupData, CurrentSettings) ->
    lists:map(
      fun ({Svc, OsPid, IsCorrect, Path, MemoryQuota}) ->
              ok = maybe_move_process(OsPid, IsCorrect, Path),
              case cgroups:has_feature_enabled() of
                  true ->
                      maybe_fix_memory(Svc, Path, CurrentSettings,
                                       MemoryQuota);
                  false ->
                      %% This case should only ever be hit if we are fully
                      %% upgraded, using provisioned profile and running linux,
                      %% but the internalSetting 'enable_cgroups' is false
                      %% (which is the default).
                      set_max_max(Svc, Path, CurrentSettings)
              end
      end, CgroupData).

set_max_max(Svc, Path, CurrentSettings) ->
    maybe_write_limits(Path, #limits{hard=max, soft=max},
                       extract_limits(Svc, CurrentSettings)).

extract_limits(Svc, CurrentSettings) ->
    case maps:get(Svc, CurrentSettings, none) of
        none -> #limits{hard=max, soft=max}; %% default max/max cgroup
        {_Path, L} -> L
    end.

maybe_fix_memory(Svc, Path, CurrentSettings, MemoryQuota) ->
    CurrentLimits = extract_limits(Svc, CurrentSettings),

    %% NOTE: this will take precedance over the memoryQuota if it exists.
    WantLimits =
        case lookup_overrides(Svc) of
            none ->
                merge_with_memory_quota(cgroups:service_to_limits_type(Svc),
                                        MemoryQuota);
            Limit ->
                Limit
        end,
    maybe_write_limits(Path, WantLimits, CurrentLimits).

merge_with_memory_quota([hard, soft], MemoryQuota) ->
    #limits{hard = MemoryQuota, soft = MemoryQuota};
merge_with_memory_quota([soft, hard], MemoryQuota) ->
    #limits{hard = MemoryQuota, soft = MemoryQuota};
merge_with_memory_quota([hard], MemoryQuota) ->
    #limits{hard = MemoryQuota, soft = max};
merge_with_memory_quota([soft], MemoryQuota) ->
    #limits{hard = max, soft = MemoryQuota};
merge_with_memory_quota([], _MemoryQuota) ->
    #limits{hard = max, soft = max}.

zero_to_max(0) ->
    max;
zero_to_max(X) ->
    X.

maybe_write_limits(Path, #limits{hard = NewHardLimit, soft = NewSoftLimit},
                   #limits{hard = CurrentHardLimit, soft = CurrentSoftLimit}) ->
    case cgroups:mb_to_bytes(NewSoftLimit) =/= CurrentSoftLimit of
        true ->
            ok = cgroups:write_memory_high(Path, NewSoftLimit);
        false ->
            ok
    end,
    case cgroups:mb_to_bytes(NewHardLimit) =/= CurrentHardLimit of
        true ->
            ok = cgroups:write_memory_max(Path, NewHardLimit);
        false ->
            ok
    end.

lookup_overrides(Svc) ->
    case ns_config:search_node(cgroup_overrides) of
        {value, Overrides} ->
            proplists:get_value(Svc, Overrides, none);
        _ ->
            none
    end.

get_all_os_pids() ->
    babysitter_os_pids() ++ additional_os_pids().

-spec(get_child_pid(gen_server:server_ref()) -> pid()).
get_child_pid(Pid) ->
    gen_server:call(Pid, child_pid, ?DEFAULT_TIMEOUT).

get_prometheus_port() ->
    gen_server:call(prometheus_cfg, get_port, ?DEFAULT_TIMEOUT).

get_os_pid_from_child(Pid) ->
    get_os_pid(get_child_pid(Pid)).

get_os_pid(Pid) ->
    gen_server:call(Pid, get_os_pid, ?DEFAULT_TIMEOUT).

babysitter_os_pids() ->
    lists:map(fun ({Name, Pid}) ->
                      {Name, get_os_pid_from_child(Pid),
                       cgroups:service_cgroup_path(Name)}
              end, ns_child_ports_sup:current_ports_pids()).

%% These have to be added manually for some reason. They aren't direct children
%% in the same way as the other services/ports.
additional_os_pids() ->
    [{ns_server, list_to_integer(os:getpid()), %% only from ns_server context
      cgroups:service_cgroup_path(ns_server)},
     {prometheus, get_os_pid(get_prometheus_port()),
      cgroups:service_cgroup_path(prometheus)}].

-ifdef(TEST).

-define(FAKE_PATH, "/path/to/file").

merge_with_memory_quota_test() ->
    ?assertEqual(#limits{hard=1024, soft=max},
                 merge_with_memory_quota([hard], 1024)),
    ?assertEqual(#limits{hard=max, soft=1024},
                 merge_with_memory_quota([soft], 1024)),
    ?assertEqual(#limits{hard=1024, soft=1024},
                 merge_with_memory_quota([hard, soft], 1024)),
    ?assertEqual(#limits{hard=1024, soft=1024},
                 merge_with_memory_quota([soft, hard], 1024)),
    ?assertEqual(#limits{hard=max, soft=max},
                 merge_with_memory_quota([], 1024)).

mb_to_bytes_test() ->
    ?assertEqual(cgroups:mb_to_bytes(1024), 1024 * 1024 * 1024).

maybe_move_process_test() ->
    meck:new(cgroups, [passthrough]),
    meck:expect(cgroups, move_process, fun (_OsPid, _Dest) -> ok end),
    maybe_move_process(1234, false, ?FAKE_PATH),
    ?assert(meck:called(cgroups, move_process, [1234, ?FAKE_PATH])),
    maybe_move_process(678, true, ?FAKE_PATH),
    ?assertEqual(meck:called(cgroups, move_process, [678, ?FAKE_PATH]), false),
    maybe_move_process(4321, false, none),
    ?assertEqual(meck:called(cgroups, move_process, [4321, none]), false),
    meck:unload(cgroups).

-endif.
