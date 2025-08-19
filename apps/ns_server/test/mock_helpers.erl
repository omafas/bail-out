%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc
%% This file contains a set of functions to help with starting up various
%% ns_server processes for the sake of testing. For example, we may want to
%% test auto-failover, or rebalance, and to do so we need to start up the
%% orchestrator which requires it's own set of processes (or mocks). We may
%% start processes or mock them, depending on general complexity and the needs
%% of the test(s) calling the functions in this module.
%%
%% The functions in this module are intended to set up processes in a generally
%% minimal and re-usable way, but this may not always be possible.
%%
%% To accomplish this in something of a re-usable way, we establish a pattern
%% where we:
%% 1. Export functions to set up some process
%% 2. These functions may compose other functions to set up their requirements
%% 3. These functions take, and return, a map of pids (accumulator style) which
%%    is used to track the pids of the processes they create, or the mocks that
%%    they set up. This lets us call each function multiple times and skip
%%    setting up a process that has already been set up, simplying test setup
%%    when we want to re-use certain functions.
%%    In addition, the mocking can be overriden by the caller, for some given
%%    function, if needed, by setting that key in the map of pids.
%% 4. We can shut down any processes set up by the functions in this module by
%%    calling `shutdown_processes/1' with the map of pids.
%%
%% This file is a work in progress, I'm not 100% sure that this is the best way
%% to do this, but it exists such that we can share these mocks between tests;
%% in particular is was added to share mocks between the failover and rebalance
%% tests. We may wish to evaluate this approach in the future if management of
%% these mocks becomes more complex.
-module(mock_helpers).

-include_lib("eunit/include/eunit.hrl").
-include_lib("ns_common/include/cut.hrl").

-include("ns_common.hrl").

%% Export everything, we need to export the functions that set up the
%% mocks/processes so that we can call them from the test setup functions so
%% most everything must be exported anyway.
-compile(export_all).
-compile(nowarn_export_all).

%%%===================================================================
%%% API functions
%%%===================================================================

%% Convenience function to set up a bunch of mocks for a list of modules.
%% Returns a map of pids (accumulator style) for the processes that are set up
%% such that they can be shutdown later.
-spec setup_mocks(list()) -> map().
setup_mocks(Modules) ->
    setup_mocks(Modules, #{}).

%% Convenience function to set up a bunch of mocks for a list of modules using
%% the provided PidMap accumulator.
-spec setup_mocks(list(), map()) -> map().
setup_mocks(Modules, PidMap) ->
    functools:chain(PidMap,
                    [setup_mock(Module, _) || Module <- Modules]).

%% Shuts down all the processes that were set up by the functions in this
%% module.
-spec shutdown_processes(map()) -> ok.
shutdown_processes(PidMap) ->
    maps:foreach(
      fun(_Process, Pid) when is_pid(Pid) ->
              erlang:unlink(Pid),
              misc:terminate_and_wait(Pid, shutdown);
         (_, _) ->
              ok
      end, PidMap).

%%%===================================================================
%%% Helper functions
%%%===================================================================

%% Skip setting up a mock if it's already been set up. This allows a caller to
%% override a mock if needed, or for us to compose test setup functions without
%% having to worry about whether a mock has already been set up.
setup_mock(Module, PidMap) ->
    case maps:get(Module, PidMap, undefined) of
        undefined ->
            ?MODULE:Module(PidMap);
        _ ->
            PidMap
    end.

%%%===================================================================
%%% Process setup functions
%%%===================================================================

leader_registry(PidMap) ->
    %% Note, this won't work if we have already set up fake_ns_pubsub, but we
    %% are going to get rid of that anyway in future changes.
    leader_registry_tests:setup(),
    gen_event:sync_notify(leader_events, {new_leader, node()}),

    PidMap#{?FUNCTION_NAME => whereis(?FUNCTION_NAME),
            leader_events => whereis(leader_events)}.

auto_reprovision(PidMap) ->
    %% TODO: Nothing needs auto_reprovision to be enabled yet, just running,
    %% so we can disable it for now.
    fake_chronicle_kv:update_snapshot(auto_reprovision_cfg, [{enabled, false}]),

    PidMap0 = setup_mocks([leader_registry],
                          PidMap),

    {ok, AutoReprovisionPid} = ?FUNCTION_NAME:start_link(),
    PidMap0#{?FUNCTION_NAME => AutoReprovisionPid}.

ns_rebalance_report_manager(PidMap) ->
    fake_ns_config:update_snapshot(rest_creds, null),

    meck:new(cb_atomic_persistent_term, [passthrough]),
    meck:expect(cb_atomic_persistent_term, get_or_set_if_undefined,
                fun(_, _, F) ->
                        F()
                end),
    {ok, RebalanceReportManagerPid} = ?FUNCTION_NAME:start_link(),
    PidMap#{?FUNCTION_NAME => RebalanceReportManagerPid}.

ns_orchestrator(PidMap) ->
    PidMap0 = setup_mocks([ns_janitor_server,
                           ns_doctor,
                           ns_rebalance_report_manager,
                           testconditions],
                          PidMap),

    {ok, OrchestratorPid} = ?FUNCTION_NAME:start_link(),
    PidMap0#{?FUNCTION_NAME => OrchestratorPid}.

compat_mode_manager(PidMap) ->
    {ok, CompatModeManagerPid} = ?FUNCTION_NAME:start_link(),
    PidMap#{?FUNCTION_NAME => CompatModeManagerPid}.

auto_failover(PidMap) ->
    PidMap0 = setup_mocks([compat_mode_events], PidMap),

    {ok, AutoFailoverPid} = ?FUNCTION_NAME:start_link(),
    PidMap0#{?FUNCTION_NAME => AutoFailoverPid}.

compat_mode_events(PidMap) ->
    {ok, CompatModeEventsPid} = gen_event:start_link({local,
                                                      ?FUNCTION_NAME}),
    PidMap#{?FUNCTION_NAME => CompatModeEventsPid}.

ns_node_disco(PidMap) ->
    PidMap0 = setup_mocks([dist_manager, ns_node_disco_events], PidMap),
    {ok, NsNodeDiscoPid} = ns_node_disco:start_link(),
    PidMap0#{?FUNCTION_NAME => NsNodeDiscoPid}.

ns_node_disco_events(PidMap) ->
    {ok, NSNodeDiscoEventsPid} = gen_event:start_link({local,
                                                       ?FUNCTION_NAME}),
    PidMap#{?FUNCTION_NAME => NSNodeDiscoEventsPid}.

json_rpc_events(PidMap) ->
    {ok, JsonRpcEventsPid} = gen_event:start_link({local, ?FUNCTION_NAME}),
    PidMap#{?FUNCTION_NAME => JsonRpcEventsPid}.

user_storage_events(PidMap) ->
    {ok, UserStorageEventsPid} = gen_event:start_link({local, ?FUNCTION_NAME}),
    PidMap#{?FUNCTION_NAME => UserStorageEventsPid}.

ssl_service_events(PidMap) ->
    {ok, SslServiceEventsPid} = gen_event:start_link({local, ?FUNCTION_NAME}),
    PidMap#{?FUNCTION_NAME => SslServiceEventsPid}.

%%%===================================================================
%%% Mock setup functions
%%%
%%% These functions just set up mocks, so we don't have a pid to add to the map.
%%% We just return an atom so that we can still handle skipping multiple
%%% setups/overriding setups.
%%%===================================================================

janitor_agent(PidMap) ->
    %% Janitor_agent mecks required to perform a full failover (with map).
    meck:new(?FUNCTION_NAME, [passthrough]),
    meck:expect(?FUNCTION_NAME, query_vbuckets,
                fun(_,_,_,_) ->
                        %% We don't need to return anything useful for this
                        %% failover, we are failing over all but one node so
                        %% we don't have to choose between any.
                        {dict:from_list([{1, []}]), []}
                end),

    meck:expect(?FUNCTION_NAME, fetch_vbucket_states,
                fun(VBucket, _) ->
                        %% We need to return some semi-valid vBucket stat map
                        %% from this. We might use a couple of different maps
                        %% for this test, so here we will generate it from
                        %% the map (assuming only 1 vBucket).
                        {ok, BucketConfig} = ns_bucket:get_bucket("default"),
                        ArrayMap = array:from_list(
                                     proplists:get_value(map, BucketConfig)),
                        [Active | Replicas] = array:get(VBucket, ArrayMap),
                        Seqnos = [{high_prepared_seqno, 1},
                                  {high_seqno, 1}],
                        A = [{Active, active, Seqnos}],
                        R = [{Replica, replica, Seqnos} || Replica <- Replicas],
                        A ++ R
                end),

    meck:expect(?FUNCTION_NAME, find_vbucket_state,
                fun(Node, States) ->
                        meck:passthrough([Node, States])
                end),

    meck:expect(?FUNCTION_NAME, apply_new_bucket_config,
                fun(_,_,_,_) ->
                        %% Just sets stuff in memcached, uninteresting here
                        ok
                end),

    meck:expect(?FUNCTION_NAME, mark_bucket_warmed,
                fun(_,_) ->
                        %% Just sets stuff in memcached, uninteresting here
                        ok
                end),
    PidMap#{?FUNCTION_NAME => mocked}.

%% auto failover for orchestrator
ns_janitor_server(PidMap) ->
    meck:new(?FUNCTION_NAME),
    meck:expect(?FUNCTION_NAME, start_cleanup,
                fun(_) -> {ok, self()} end),
    meck:expect(?FUNCTION_NAME, terminate_cleanup,
                fun(_) ->
                        CallerPid = self(),
                        CallerPid ! {cleanup_done, foo, bar},
                        ok
                end),
    PidMap#{?FUNCTION_NAME => mocked}.

%% from manual failover setup
leader_activities(PidMap) ->
    meck:new(?FUNCTION_NAME),
    meck:expect(?FUNCTION_NAME, run_activity,
                fun(_Name, _Quorum, Body) ->
                        Body()
                end),
    meck:expect(?FUNCTION_NAME, run_activity,
                fun(_Name, _Quorum, Body, _Opts) ->
                        Body()
                end),
    meck:expect(?FUNCTION_NAME, deactivate_quorum_nodes,
            fun(_) -> ok end),
    PidMap#{?FUNCTION_NAME => mocked}.

ns_doctor(PidMap) ->
    meck:new(?FUNCTION_NAME),
    meck:expect(?FUNCTION_NAME, get_nodes, fun() -> [] end),
    PidMap#{?FUNCTION_NAME => mocked}.

rebalance_quirks(PidMap) ->
    meck:new(?FUNCTION_NAME, [passthrough]),
    meck:expect(?FUNCTION_NAME, get_quirks, fun(_,_) -> [] end),
    PidMap#{?FUNCTION_NAME => mocked}.

testconditions(PidMap) ->
    meck:new(?FUNCTION_NAME, [passthrough]),
    meck:expect(?FUNCTION_NAME, get, fun(_) -> false end),
    PidMap#{?FUNCTION_NAME => mocked}.

json_rpc_connection_sup(PidMap) ->
    meck:expect(?FUNCTION_NAME, reannounce, fun () -> ok end),
    PidMap#{?FUNCTION_NAME => mocked}.

ns_ssl_services_setup(PidMap) ->
    meck:expect(?FUNCTION_NAME, client_cert_auth, fun () -> [] end),
    PidMap#{?FUNCTION_NAME => mocked}.

menelaus_users(PidMap) ->
    meck:expect(?FUNCTION_NAME, get_auth_version,
                fun () -> {0, 0} end),
    meck:expect(?FUNCTION_NAME, get_users_version,
                fun () -> {0, 0} end),
    meck:expect(?FUNCTION_NAME, get_groups_version,
                fun () -> {0, 0} end),
    PidMap#{?FUNCTION_NAME => mocked}.

ns_secrets(PidMap) ->
    meck:expect(?FUNCTION_NAME, get_pkey_pass,
                fun (_) -> ?HIDE(undefined) end),
    PidMap#{?FUNCTION_NAME => mocked}.

dist_manager(PidMap) ->
    meck:expect(?FUNCTION_NAME, get_rename_txn_pid, fun () -> undefined end),
    PidMap#{?FUNCTION_NAME => mocked}.
