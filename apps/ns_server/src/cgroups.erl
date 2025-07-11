%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Module is meant for all cgroup utility functions. Generally anything meant to
%% directly interact with the cgroups v2 filesystem belongs in this module.

-module(cgroups).

-include("ns_common.hrl").

-export([supported/0,
         supported_and_79/0,
         all_services/0,
         write_memory_high/2,
         write_memory_max/2,
         read_system_cgroups/1,
         move_process/2,
         service_cgroup_path/1,
         maybe_service_cgroup_path/1,
         read_cgroup_procs/1,
         has_feature_enabled/0,
         mb_to_bytes/1,
         read_enabled_controllers/1,
         read_memory_high/1,
         read_memory_max/1,
         parse_mtab_file/1,
         get_cgroup_base_path/0,
         read_cgroup2_config_from_mtab/1,
         os_type/0,
         supported_on_all_nodes/0,
         service_to_limits_type/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(READ_ONLY_ERROR,
        "Cgroups V2 is mounted, but only in \"read-only\" mode. "
        "This must be addressed to use any cgroup features.").
-define(INVALID_MOUNT_FLAG_ERROR,
        "Got invalid mount flags in /etc/mtab.").
-define(READ_WRITE_SUCCESS,
        "Cgroups V2 found and mounted (correctly) in \"read-write\" mode.").
-define(SERVICE_TO_ATOM_ERR, "Service name: '~p' does not exist. Skipping...").
-define(LIST_DIR_ERR,
        "Failed to list directory '~p' (error: '~p') when trying to find cgroup"
        " leaf folders.").
-define(EMPTY_FILE_ERR,
        "The file '~s' was empty when an integer (or 'max') was expected.").
-define(MTAB_PATH, "/etc/mtab").

%% All controllers: cpu, cpuset, memory, io, pid, rdma, hugetlb, misc
-define(ENABLED_CONTROLLERS, ["memory"]).

-type limit_val() :: cgroup_val() | string().
-type write_return() ::
        ok | {error, file:posix() | badarg | terminated | system_limit}.
-type mtab_response() :: unsupported | supported.

%%%===================================================================
%%% API
%%%===================================================================

%% Checks these in order (short-circuiting):
%%   - we are running on linux
%%   - is provisioned profile
%%   - we have cgroups v2 on the system
%%     - it's mounted rw
%%     - has required controllers
%% NOTE: Called from babysitter context
-spec(supported() -> boolean()).
supported() ->
    cgroups:os_type() =:= {unix, linux} andalso
        config_profile:is_provisioned() andalso system_checks().

-spec(supported_and_79() -> boolean()).
supported_and_79() ->
    supported() andalso cluster_compat_mode:is_cluster_79() andalso
        cluster_compat_mode:is_enterprise().

-spec(supported_on_all_nodes() -> boolean()).
supported_on_all_nodes() ->
    supported_on_all_nodes([node() | nodes()]).

-spec(supported_on_all_nodes([node()]) -> boolean()).
supported_on_all_nodes(Nodes) ->
    lists:all(fun ({ok, true}) ->
                      true;
                  (_) ->
                      false
              end, erpc:multicall(Nodes, fun cgroups:supported/0, 60000)).

%% NOTE: Since this feature is meant to be disabled by default, this should only
%% be used in ns_cgroups_manager to determine if limits should be set or just
%% use max/max for all to "disable" the feature.
%%
%% Generally if it's unsupported none of the code will run or create folders,
%% and this is the case for windows/mac/non-provisioned. On linux
%% (+provisioned), the structure will still get created, but the settings will
%% be max/max if this key is not enabled.
-spec(has_feature_enabled() -> boolean()).
has_feature_enabled() ->
    cluster_compat_mode:is_cluster_79() andalso
        cluster_compat_mode:is_enterprise() andalso
        ns_config:read_key_fast(enable_cgroups, false).

-spec(has_cgroups_v2() -> boolean()).
has_cgroups_v2() ->
    case read_cgroup2_config_from_mtab() of
        supported ->
            true;
        unsupported ->
            false
    end.

-spec(has_required_controllers() -> boolean()).
has_required_controllers() ->
    case read_enabled_controllers(get_cgroup_base_path()) of
        [] ->
            false;
        {error, Reason} ->
            ?log_warning("Failed to read enabled controllers: ~p. "
                         "Disabling cgroups V2.", [Reason]),
            false;
        Controllers when is_list(Controllers) ->
            lists:all(fun (WantedController) ->
                              lists:member(WantedController, Controllers)
                      end, ?ENABLED_CONTROLLERS)
    end.

-spec(read_cgroup2_config_from_mtab() -> mtab_response()).
read_cgroup2_config_from_mtab() ->
    read_cgroup2_config_from_mtab(?MTAB_PATH).

-spec(read_cgroup2_config_from_mtab(file:name_all()) -> mtab_response()).
read_cgroup2_config_from_mtab(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            case parse_mtab_file(Bin) of
                [] ->
                    unsupported;
                [{cgroup2, readwrite} | _] ->
                    supported
            end;
        {error, Reason} ->
            ?log_error("Error reading mtab file '~s': ~p", [?MTAB_PATH,
                                                            Reason]),
            unsupported
    end.

-spec(maybe_service_cgroup_path(service_name()) -> list()).
maybe_service_cgroup_path(Svc) ->
    case service_cgroup_path(Svc) of
        none ->
            [];
        Path ->
            [{cgroup, Path}]
    end.

-spec(service_cgroup_path(atom(), Default) -> string() | Default).
service_cgroup_path(ServiceName, Default) ->
    case supported() of
        true ->
            case lists:member(ServiceName, all_services()) of
                true ->
                    format_service_path(ServiceName);
                false ->
                    Default
            end;
        false ->
            Default
    end.

-spec(service_cgroup_path(atom()) -> string() | none).
service_cgroup_path(ServiceName) ->
    service_cgroup_path(ServiceName, none).

%% BasePath should not include a trailing '/'
-spec(read_enabled_controllers(string()) -> list() | {error, atom()}).
read_enabled_controllers(BasePath) ->
    readfile(io_lib:format("~s/cgroup.controllers", [BasePath]),
             fun (Bin) -> parse_cgroup_controllers(Bin) end).

-spec(read_memory_high(string()) -> cgroup_val() | {error, atom()}).
read_memory_high(BasePath) ->
    readfile(io_lib:format("~s/memory.high", [BasePath]),
             fun (Bin) -> process_memory_file(Bin, BasePath) end).

-spec(read_memory_max(string()) -> cgroup_val() | {error, atom()}).
read_memory_max(BasePath) ->
    readfile(io_lib:format("~s/memory.max", [BasePath]),
             fun (Bin) -> process_memory_file(Bin, BasePath) end).

-spec(process_memory_file(iodata(), string()) -> cgroup_val()).
process_memory_file(Bin, FilePath) ->
    [First | _] = string:split(binary_to_list(Bin), "\n", all),
    maybe_convert_to_integer(First, FilePath).

-spec(readfile(file:name_all(), fun((iodata()) -> X)) -> X | {error, atom()}).
readfile(Path, Fun) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            Fun(Bin);
        {error, Reason} ->
            {error, Reason}
    end.

-spec(writefile(file:name_all(), iodata()) -> write_return()).
writefile(Path, Data) when is_list(Data) ->
    writefile(Path, list_to_binary(Data));
writefile(Path, Data) when is_binary(Data) ->
    file:write_file(Path, <<Data/binary, "\n">>).

-spec(write_memory_high(file:name_all(), limit_val()) -> write_return()).
write_memory_high(Base, max) ->
    write_memory_high(Base, "max");
write_memory_high(Base, Value) when is_integer(Value) ->
    write_memory_high(Base, integer_to_list(Value));
write_memory_high(Base, Value) ->
    File = format_and_flatten("~s/memory.high", [Base]),
    Val = maybe_as_megabytes(Value),
    ?log_warning("Going to update '~p' with value: ~p", [File, Val]),
    writefile(File, Val).

-spec(write_memory_max(file:name_all(), limit_val()) -> write_return()).
write_memory_max(Base, max) ->
    write_memory_max(Base, "max");
write_memory_max(Base, Value) when is_integer(Value) ->
    write_memory_max(Base, integer_to_list(Value));
write_memory_max(Base, Value) ->
    File = format_and_flatten("~s/memory.max", [Base]),
    Val = maybe_as_megabytes(Value),
    ?log_warning("Going to update '~p' with value: ~p", [File, Val]),
    writefile(File, Val).

%% From the shell you can simply do...
%%
%%     echo "1234" > $dir/cgroup.procs
%%
%% NOTE: needs to have write permissions to the "common ancestor" which in our
%% case should just be the couchbase-server.service/cgroup.procs file.
-spec(move_process(integer(), string()) -> write_return()).
move_process(OsPid, Destination) when is_integer(OsPid) ->
    ?log_warning("Moving '~p' (OS PID) to ~s/cgroup.procs",
                 [OsPid, Destination]),
    writefile(io_lib:format("~s/cgroup.procs", [Destination]),
              integer_to_binary(OsPid)).

-spec(service_to_limits_type(atom()) -> list()).
service_to_limits_type(Service) ->
    Soft = is_member(Service, fun soft_limits_profile/0, soft),
    Hard = is_member(Service, fun hard_limits_profile/0, hard),
    lists:flatten(Soft ++ Hard).

-spec(mb_to_bytes(cgroup_val()) -> cgroup_val()).
mb_to_bytes(max) ->
    max;
mb_to_bytes(Number) when is_number(Number) ->
    Number * 1024 * 1024.

%% We use this to be able to mock the os:type in tests. You normally cannot mock
%% those types of builtin modules with meck.
os_type() ->
    os:type().

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% System checks are cached after first access since they shouldn't change
%% dynamically.
system_checks() ->
    case persistent_term:get({?MODULE, system_info}, none) of
        none ->
            Entry =
                #cgroup_system_info{v2 = has_cgroups_v2(),
                                    controllers = has_required_controllers()},
            ok = persistent_term:put({?MODULE, system_info}, Entry),
            Entry#cgroup_system_info.v2 andalso
                Entry#cgroup_system_info.controllers;
        #cgroup_system_info{v2 = HasV2, controllers = HasControllers} ->
            HasV2 andalso HasControllers
    end.

%% Even though linux does not care if you use two // (slashes) next to
%% eachother, we still want to trim them for consistency.
maybe_strip_trailing_slash(Path) ->
    string:trim(Path, trailing, [$/]).

%% Customize where services are placed in the heirarchy.
%% NOTE: This is s/t the evaluator can be spawned separately. It requires
%% changes to eventing-ee but this is to pave the way for that future change.
%% Until changes are made to eventing-ee codebase, it will still spawn inside
%% the same cgroup as n1ql because it (n1ql) forks the child itself and
%% ns_server doesn't start it.
format_service_path(n1ql) ->
    format_and_flatten("~s/services/n1ql/n1ql", [get_cgroup_base_path()]);
format_service_path(Svc) ->
    format_and_flatten("~s/services/~s", [get_cgroup_base_path(), Svc]).

parse_mtab_file(Bin) ->
    lists:filtermap(
      fun (Line) ->
              case parse_mtab_line(Line) of
                  {cgroup2, Flags} ->
                      case parse_mount_flags(Flags) of
                          readonly ->
                              ?log_warning(?READ_ONLY_ERROR),
                              false;
                          readwrite ->
                              ?log_info(?READ_WRITE_SUCCESS),
                              {true, {cgroup2, readwrite}};
                          invalid ->
                              ?log_warning(?INVALID_MOUNT_FLAG_ERROR),
                              false
                      end;
                  none ->
                      false
              end
      end, string:split(erlang:binary_to_list(Bin), "\n", all)).

parse_mtab_line(Line) ->
    case string:split(Line, " ", all) of
        [_, _, "cgroup2", Flags | _] ->
            {cgroup2, Flags};
        _ ->
            none
    end.

parse_mount_flags(String) ->
    case string:split(String, ",", all) of
        ["ro" | _] -> readonly;
        ["rw" | _] -> readwrite;
        _ -> invalid
    end.

parse_cgroup_controllers(BinData) ->
    %% We always end up with an empty list for the empty 2nd line of the
    %% controllers specification so we are just ignoring it.
    [FirstLine | _] = string:split(binary_to_list(BinData), "\n", trailing),
    string:split(FirstLine, " ", all).

maybe_convert_to_integer(Value, FilePath) ->
    case Value of
        [] ->
            exit(format_and_flatten(?EMPTY_FILE_ERR, [FilePath]));
        "max" ->
            max;
        Str when is_list(Str) ->
            case lists:suffix("M", Str) of
                true ->
                    mb_to_bytes(
                      list_to_integer(string:trim(Str, trailing, "M")));
                false ->
                    list_to_integer(Str)
            end
    end.

read_cgroup_procs(Path) ->
    readfile(io_lib:format("~s/cgroup.procs", [Path]),
             fun (Bin) ->
                     lists:filtermap(
                       fun ([]) ->
                               false;
                           (Str) when is_list(Str) ->
                               {true, list_to_integer(Str)};
                           (Int) when is_integer(Int) ->
                               {true, Int}
                       end, string:split(binary_to_list(Bin), "\n", all)) end).

read_system_cgroups(Base) ->
    BaseDir = format_and_flatten("~s/services/", [Base]),
    Folders = get_leaf_folders(BaseDir),
    Output =
        lists:filtermap(fun (Folder) ->
                                ServiceName = filename:basename(Folder),
                                try
                                    {true, {list_to_existing_atom(ServiceName),
                                            get_memory_settings(Folder)}}
                                catch
                                    _E:_T ->
                                        ?log_error(?SERVICE_TO_ATOM_ERR,
                                                   [ServiceName]),
                                        false
                                end
                        end, Folders),
    maps:from_list(Output).

get_leaf_folders(Path) ->
    DirectoryListing =
        case file:list_dir(Path) of
            {ok, List} when is_list(List) ->
                List;
            {error, Reason} ->
                exit(format_and_flatten(?LIST_DIR_ERR, [Path, Reason]))
        end,
    AllDirectories =
        [Dir ++ "/" || Dir <- DirectoryListing,
                       filelib:is_dir(Path ++ "/" ++ Dir) =:= true],
    case AllDirectories of
        [] ->
            [Path];
        TheList ->
            lists:foldl(fun (Item, Acc) ->
                                Acc ++ get_leaf_folders(Path ++ Item)
                        end, [], TheList)
    end.

%% TODO: add ns_server_stats.erl statistics here? See MB-64887
get_memory_settings(Folder) ->
    NormalizedPath = maybe_strip_trailing_slash(Folder),
    High = read_memory_high(NormalizedPath),
    Max = read_memory_max(NormalizedPath),
    {NormalizedPath, #limits{hard = Max, soft = High}}.

%% We always encode the values as megabytes since that's how the memory_quota's
%% are stored.
maybe_as_megabytes("max") ->
    "max"; %% passthrough "max"
maybe_as_megabytes(Value) when is_integer(Value) ->
    maybe_as_megabytes(integer_to_list(Value));
maybe_as_megabytes(Value) when is_list(Value) ->
    format_and_flatten("~sM", [Value]).

get_cgroup_base_path() ->
    config_profile:get_value(cgroup_base_path, ?DEFAULT_SYSTEMD_CGROUP_ROOT).

format_and_flatten(Template, Params) ->
    lists:flatten(io_lib:format(Template, Params)).

is_member(Service, LimitsFun, ResultingAtom) ->
    case lists:member(Service, LimitsFun()) of
        true ->
            [ResultingAtom];
        false ->
            []
    end.

%% Hardcoded list of services.
all_services() ->
    ?ALL_SERVICE_ATOMS.

soft_limits_profile() ->
    config_profile:get_value(cgroups_memory_soft, []).

hard_limits_profile() ->
    config_profile:get_value(cgroups_memory_hard, []).

-ifdef(TEST).

%% Make sure to test a directory tree of different depths to emulate the
%% situation where n1ql needs to launch it's main program and the evaluator.
find_leaf_folders_test() ->
    try
        ok = filelib:ensure_dir("cgroup-folder-test/services/kv/"),
        ok = filelib:ensure_dir("cgroup-folder-test/services/n1ql/"),
        ok = filelib:ensure_dir("cgroup-folder-test/services/n1ql/n1ql/"),
        ok = filelib:ensure_dir("cgroup-folder-test/services/n1ql/evaluator/"),
        Correct = ["cgroup-folder-test/services/n1ql/n1ql/",
                   "cgroup-folder-test/services/n1ql/evaluator/",
                   "cgroup-folder-test/services/kv/"],
        Folders = get_leaf_folders("cgroup-folder-test/services/"),
        ?assert(lists:all(fun (Item) ->
                                  lists:member(Item, Correct)
                          end, Folders))
    after
        ok = file:del_dir_r("cgroup-folder-test")
    end.

maybe_convert_integer_test() ->
    ?assertEqual(1073741824, maybe_convert_to_integer("1024M", "fakepath")),
    ?assertEqual(1024, maybe_convert_to_integer("1024", "fakepath")),
    ?assertEqual(max, maybe_convert_to_integer("max", "fakepath")).

as_megabytes_test() ->
    ?assertEqual(maybe_as_megabytes("1234"), "1234M"),
    ?assertEqual(maybe_as_megabytes(1024), "1024M"),
    ?assertEqual(maybe_as_megabytes("1024"), "1024M").

-endif.
