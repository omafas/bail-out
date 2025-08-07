%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc
%% Tests to exercise cgroups v2 functions on a mock fileystem. Doesn't require
%% support for cgroups v2 since it's just a mock environment.
%%
%% More tests could be created for systems that support cgroup v2 but this is
%% left as a TODO until the CI machines support it.
-module(cgroup_v2_tests).

-include("ns_config.hrl").
-include("ns_test.hrl").

-include("ns_common.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ROOT_DIR, "apps/ns_server/test/cgroups2").
-define(SERVICE_DIR, "apps/ns_server/test/cgroups2/services").
-define(KV_DIR, "apps/ns_server/test/cgroups2/services/kv").
-define(N1QL_DIR, "apps/ns_server/test/cgroups2/services/n1ql").
-define(NS_SERVER_DIR, "apps/ns_server/test/cgroups2/services/ns_server").

-define(CORRECT_MTAB, "correct_mtab").
-define(INCORRECT_MTAB_FLAGS, "incorrect_mtab_flags").
-define(NO_CGROUP_MTAB, "no_cgroup_mtab").
-define(NO_PERMISSIONS_MODE, 8#00000).
-define(READ_PERMISSIONS_MODE, 8#00600).

create_cgroup_path(Filename) ->
    lists:flatten(io_lib:format("~s/cgroups2/~s",
                                [filename:dirname(?FILE), Filename])).

cgroup_read_controllers_test() ->
    HasControllers = cgroups:read_enabled_controllers(?ROOT_DIR),
    ?assertEqual(["cpuset", "cpu", "io", "memory", "pids"], HasControllers),
    HasControllers2 =
        cgroups:read_enabled_controllers(?SERVICE_DIR),
    ?assertEqual(["memory", "pids"], HasControllers2),
    HasControllers3 =
        cgroups:read_enabled_controllers(?KV_DIR),
    ?assertEqual(["memory"], HasControllers3),

    Filename = lists:flatten(
                 io_lib:format("~s/cgroup.controllers", [?N1QL_DIR])),
    with_invalid_permissions(
      Filename,
      fun () ->
              ?assertEqual({error, eacces},
                           cgroups:read_enabled_controllers(?N1QL_DIR))
      end).

with_invalid_permissions(Filename, Fun) ->
    {ok, Info} = file:read_file_info(Filename),
    try
        %% makes it not read/write for user
        Info2 = Info#file_info{mode = ?NO_PERMISSIONS_MODE},
        ok = file:write_file_info(Filename, Info2),
        Fun()
    after
        %% We have to set them back to readable otherwise we have trouble
        %% reading with the files when preparing tests to run.
        Info3 = Info#file_info{mode = ?READ_PERMISSIONS_MODE},
        ok = file:write_file_info(Filename, Info3)
    end.

cgroup_read_procs_test() ->
    RealFile = io_lib:format("~s/cgroup.procs", [?KV_DIR]),
    OsPid = 1234,
    try
        Procs = cgroups:read_cgroup_procs(?ROOT_DIR),
        ?assertEqual([], Procs),

        MoveProcResp = cgroups:move_process(OsPid, ?KV_DIR),
        ?assertEqual(ok, MoveProcResp),

        Procs2 = cgroups:read_cgroup_procs(?KV_DIR),
        ?assertEqual([1234], Procs2)
    after
        file:write_file(RealFile, <<>>),
        Procs3 = cgroups:read_cgroup_procs(?KV_DIR),
        ?assertEqual([], Procs3)
    end,

    Filename = lists:flatten(io_lib:format("~s/cgroup.procs", [?N1QL_DIR])),
    with_invalid_permissions(
      Filename,
      fun () ->
              ?assertEqual({error, eacces},
                           cgroups:read_cgroup_procs(?N1QL_DIR))
      end).

cgroups_read_memory_high_test() ->
    MemoryHighResp = cgroups:read_memory_high(?ROOT_DIR),
    ?assertEqual(123456, MemoryHighResp),
    MemoryHighResp2 = cgroups:read_memory_high(?SERVICE_DIR),
    ?assertEqual(6666, MemoryHighResp2),
    MemoryHighResp3 = cgroups:read_memory_high(?KV_DIR),
    ?assertEqual(max, MemoryHighResp3),
    MemoryHighResp4 = (catch cgroups:read_memory_high(?NS_SERVER_DIR)),
    ?assertEqual({'EXIT',
                  "The file 'apps/ns_server/test/cgroups2/services/ns_server'"
                  " was empty when an integer (or 'max') was expected."},
                 MemoryHighResp4),

    Filename = lists:flatten(io_lib:format("~s/memory.high", [?N1QL_DIR])),
    with_invalid_permissions(
      Filename,
      fun () ->
              ?assertEqual({error, eacces}, cgroups:read_memory_high(?N1QL_DIR))
      end).

cgroups_read_memory_max_test() ->
    MemoryMaxResp = cgroups:read_memory_max(?ROOT_DIR),
    ?assertEqual(max, MemoryMaxResp),
    MemoryMaxResp2 = cgroups:read_memory_max(?SERVICE_DIR),
    ?assertEqual(321, MemoryMaxResp2),
    MemoryMaxResp3 = cgroups:read_memory_max(?KV_DIR),
    ?assertEqual(123, MemoryMaxResp3),
    MemoryMaxResp4 = (catch cgroups:read_memory_max(?NS_SERVER_DIR)),
    ?assertEqual({'EXIT',
                  "The file 'apps/ns_server/test/cgroups2/services/ns_server'"
                  " was empty when an integer (or 'max') was expected."},
                 MemoryMaxResp4),
    Filename = lists:flatten(io_lib:format("~s/memory.max", [?N1QL_DIR])),
    with_invalid_permissions(
      Filename,
      fun () ->
              ?assertEqual({error, eacces}, cgroups:read_memory_max(?N1QL_DIR))
      end).


cgroups_write_memory_high_test() ->
    RealFile = io_lib:format("~s/memory.high", [?KV_DIR]),
    try
        WriteHighResp = cgroups:write_memory_high(?KV_DIR, 1234),
        ?assertEqual(ok, WriteHighResp),

        {_, Bin} = file:read_file(RealFile),
        ?assertEqual(<<"1234M\n">>, Bin)
    after
        %% other test rely on this hardcoded value, so reset it to default
        file:write_file(RealFile, <<"max\n">>)
    end,
    Filename = lists:flatten(io_lib:format("~s/memory.high", [?N1QL_DIR])),
    with_invalid_permissions(
      Filename,
      fun () ->
              ?assertEqual({error, eacces},
                           cgroups:write_memory_high(?N1QL_DIR, 1234))
      end).


cgroups_write_memory_max_test() ->
    RealFile = io_lib:format("~s/memory.max", [?KV_DIR]),
    try
        WriteHighResp = cgroups:write_memory_max(?KV_DIR, 4321),
        ?assertEqual(ok, WriteHighResp),

        {_, Bin} = file:read_file(RealFile),
        ?assertEqual(<<"4321M\n">>, Bin)
    after
        %% other test rely on this hardcoded value, so reset it to default
        file:write_file(RealFile, <<"123\n">>)
    end,
    Filename = lists:flatten(io_lib:format("~s/memory.max", [?N1QL_DIR])),
    with_invalid_permissions(
      Filename,
      fun () ->
              ?assertEqual({error, eacces},
                           cgroups:write_memory_max(?N1QL_DIR, 1234))
      end).


cgroups_move_process_test() ->
    %% this one will have to work by doing this "move process" but then just
    %% re-reading the OsPid from that file since it'll be a normal file instead
    %% of the cgroupsv2 virtual filesystem.
    RealFile = io_lib:format("~s/cgroup.procs", [?KV_DIR]),
    OsPid = 1234,
    try
        MoveProcResp = cgroups:move_process(OsPid, ?KV_DIR),
        ?assertEqual(ok, MoveProcResp),

        {_, Bin} = file:read_file(RealFile),
        ?assertEqual(<<"1234\n">>, Bin)
    after
        %% clear the cgroup.procs file
        file:write_file(RealFile, <<>>)
    end,
    Filename = lists:flatten(io_lib:format("~s/cgroup.procs", [?N1QL_DIR])),
    with_invalid_permissions(
      Filename,
      fun () ->
              ?assertEqual({error, eacces},
                           cgroups:move_process(1234, ?N1QL_DIR))
      end).


parse_mtab_test() ->
    ?assertEqual(supported,
                 cgroups:read_cgroup2_config_from_mtab(
                   create_cgroup_path(?CORRECT_MTAB))),
    ?assertEqual(unsupported, cgroups:read_cgroup2_config_from_mtab(
                                create_cgroup_path(?NO_CGROUP_MTAB))),
    ?assertEqual(unsupported, cgroups:read_cgroup2_config_from_mtab(
                                create_cgroup_path(?INCORRECT_MTAB_FLAGS))).

run_with_cgroups_supported(Fun) ->
    meck:new(cgroups, [passthrough]),
    meck:new(config_profile, [passthrough]),

    meck:expect(cgroups, os_type, fun() -> {unix, linux} end),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    meck:expect(config_profile, is_provisioned, fun() -> true end),
    meck:expect(config_profile, get_bool,
                fun (cgroup_feature_enabled) -> true end),

    %% since we have trouble testing/mocking the same module, we can just place
    %% this value into the persistent_term store and it should work largely the
    %% same as mocking that function.
    persistent_term:put({cgroups, system_info},
                        #cgroup_system_info{v2 = true, controllers = true}),
    try
        Fun()
    after

        meck:unload(config_profile),
        meck:unload(cgroups)
    end.

service_cgroup_path_test() ->
    run_with_cgroups_supported(
      fun () ->
              lists:map(
                fun (ServiceName) ->
                        Result = cgroups:service_cgroup_path(ServiceName),
                        ?assertNotEqual(none, Result)
                end, ?ALL_SERVICE_ATOMS),
              lists:map(
                fun (ServiceName) ->
                        Result = cgroups:service_cgroup_path(ServiceName),
                        ?assertEqual(none, Result)
                end, [memcached, incorrect, service_names,
                      here, ns_serverrr, kvkv]),

              %% let's double-check that the n1ql is in an extra leaf folder
              %% from where most services are. This is to allow for future
              %% js-evaluator isolation.
              ?assertNotEqual(nomatch,
                              string:find(cgroups:service_cgroup_path(n1ql),
                                          "/n1ql/n1ql"))
      end).

collect_cgroup_data_test() ->
    meck:new(memory_quota, []),
    meck:new(cgroups, []),
    meck:expect(memory_quota, get_quota, fun (_Service) -> {ok, 1024} end),
    meck:expect(cgroups, read_cgroup_procs,
                fun (_Path) ->
                        [1234, 6666, 54321]
                end),

    Result = ns_cgroups_manager:collect_cgroup_data([{kv, 1234,
                                                      "services/kv"}]),
    ?assertEqual([{kv, 1234, true, "services/kv", 1024}], Result),

    meck:expect(memory_quota, get_quota, fun (_Service) -> {ok, 0} end),
    Result2 = ns_cgroups_manager:collect_cgroup_data([{ns_server, 876,
                                                    "services/ns_server"}]),
    ?assertEqual([{ns_server, 876, false, "services/ns_server", max}], Result2),

    Result3 = ns_cgroups_manager:collect_cgroup_data([{ns_server, 876,
                                                       "services/ns_server"},
                                                      {kv, 1234,
                                                       "services/kv"}]),
    ?assertEqual([{ns_server, 876, false, "services/ns_server", max},
                  {kv, 1234, true, "services/kv", max}], Result3),

    meck:unload(memory_quota),
    meck:unload(cgroups).
