%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
% A module for retrieving & configuring per-server storage paths,
% storage quotas, mem quotas, etc.
%
-module(ns_storage_conf).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include_lib("ns_common/include/cut.hrl").
-include("couch_db.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([setup_disk_storage_conf/4,
         storage_conf_from_node_status/2,
         query_storage_conf/0,
         this_node_dbdir/0, this_node_ixdir/0, this_node_logdir/0,
         this_node_evdir/0,
         this_node_bucket_dbdir/1,
         this_node_bucket_dbdir/2,
         delete_unused_buckets_db_files/0,
         delete_old_2i_indexes/0,
         setup_storage_paths/0,
         this_node_cbas_dirs/0,
         this_node_java_home/0,
         update_java_home/1,
         default_config/0,
         get_ini_files/0]).

-export([cluster_storage_info/2, nodes_storage_info/3]).

-export([extract_disk_stats_for_path/2]).

-define(ENSURE_DELETE_COMMAND_TIMEOUT,
        ?get_timeout(ensure_delete_command, 60000)).

get_ini_files() ->
    case init:get_argument(couch_ini) of
        error ->
            [];
        {ok, [Values]} ->
            Values
    end.

default_config() ->
    IniFiles = get_ini_files(),
    {DbDir, IxDir} = couch_config:get_db_and_ix_paths_from_ini_files(IniFiles),
    [{{node, node(), database_dir}, DbDir},
     {{node, node(), index_dir}, IxDir}].

setup_storage_paths() ->
    {ok, CfgDbDir} = this_node_dbdir(),
    {ok, CfgIxDir} = this_node_ixdir(),
    IniFiles = get_ini_files(),
    WriteFile = case IniFiles of
                    [_|_] -> lists:last(IniFiles);
                    _ -> undefined
                end,
    ok = couch_config_writer:save_to_file(
           {{"couchdb", "database_dir"}, CfgDbDir}, WriteFile),
    ok = couch_config_writer:save_to_file(
           {{"couchdb", "view_index_dir"}, CfgIxDir}, WriteFile),
    case ns_config:search_node(node(), ns_config:latest(), cbas_dirs) of
        false ->
            {ok, Default} = this_node_ixdir(),
            ok = update_cbas_dirs({ok, [Default]});
        {value, _V} ->
            not_changed
    end,
    case ns_config:search_node(node(), ns_config:latest(), eventing_dir) of
        false ->
            {ok, EvDefault} = this_node_ixdir(),
            ok = update_ev_dir({ok, EvDefault});
        {value, _} ->
            not_changed
    end,
    lists:foreach(
      fun ({BucketName, BucketUUID}) ->
          ok = ensure_bucket_is_in_correct_dir(BucketName, BucketUUID)
      end, ns_bucket:uuids()),
    ignore.

%% Move data from old bucket directory (bucket name) to new bucket directory
%% (bucket uuid). Can be removed after support for 7.* is dropped.
ensure_bucket_is_in_correct_dir(BucketName, BucketUUID) ->
    OldBucketDir = pre_phoenix_bucket_dbdir(BucketName),
    NewBucketDir = this_node_bucket_dbdir(BucketUUID),
    maybe
        {_, true} ?= {old_dir_exists, filelib:is_file(OldBucketDir)},

        {_, true} ?= {old_dir_is_dir, filelib:is_dir(OldBucketDir)},

        {_, false} ?= {new_dir_exists, filelib:is_file(NewBucketDir)},
        %% At this point, we know that the old directory exists, the
        %% new directory does not exist, so we should rename the old directory
        %% to the new directory.
        ?log_info("Migrating bucket ~p data from ~p to ~p",
                  [BucketName, OldBucketDir, NewBucketDir]),
        ok ?= filelib:ensure_dir(NewBucketDir),
        %% Since bucket data is not migrated yet, we need to ensure that
        %% views indexes are migrated as well
        {_, ok} ?= {indexes, maybe_migrate_views(BucketName, BucketUUID)},
        ok ?= file:rename(OldBucketDir, NewBucketDir),
        ?log_info("Bucket ~p data migration completed", [BucketName]),
        ok
    else
        {old_dir_exists, false} ->
            ?log_info("Bucket ~p data migration not needed", [BucketName]),
            ok;
        {new_dir_exists, true} ->
            % New directory exists and old directory exists - this should
            % never happen
            ?log_error("Bucket data migration failed: new directory "
                       "~p exists while old directory ~p also exists",
                       [NewBucketDir, OldBucketDir]),
            {error, {both_dirs_exist, NewBucketDir, OldBucketDir}};
        {old_dir_is_dir, false} ->
            ?log_error("Bucket ~p migration failed: old bucket directory ~p is "
                       "not a directory", [BucketName, OldBucketDir]),
            {error, {old_bucket_dir_is_file, OldBucketDir}};
        {indexes, {error, Reason}} ->
            ?log_error("Bucket ~p migration failed: failed to migrate views "
                       "indexes: ~p", [BucketName, Reason]),
            {error, Reason};
        {error, Reason} ->
            ?log_error("Bucket ~p migration failed: rename from ~p to ~p "
                       "failed: ~p",
                       [BucketName, OldBucketDir, NewBucketDir, Reason]),
            {error, Reason}
    end.

maybe_migrate_views(BucketName, BucketUUID) ->
    {ok, IxDir} = this_node_ixdir(),
    OldDir = filename:join([IxDir, "@indexes", BucketName]),
    NewDir = filename:join([IxDir, "@indexes", binary_to_list(BucketUUID)]),
    maybe
        {_, true} ?= {old_dir_exists, filelib:is_file(OldDir)},
        {_, false} ?= {new_dir_exists, filelib:is_file(NewDir)},
        {_, true} ?= {old_dir_is_dir, filelib:is_dir(OldDir)},
        ?log_info("Migrating ~p views indexes from ~p to ~p",
                  [BucketName, OldDir, NewDir]),
        ok ?= file:rename(OldDir, NewDir)
    else
        {old_dir_exists, false} ->
            %% Migration is not needed
            ok;
        {new_dir_exists, true} ->
            {error, {new_dir_exists, NewDir}};
        {old_dir_is_dir, false} ->
            {error, {old_dir_not_dir, OldDir}};
        {error, Reason} ->
            {error, Reason}
    end.

get_db_and_ix_paths() ->
    {ok, DBDir} = this_node_dbdir(),
    {ok, IXDir} = this_node_ixdir(),
    {filename:join([DBDir]), filename:join([IXDir])}.

-spec this_node_bucket_dbdir(bucket_name(), Snapshot :: map() | direct) ->
          string().
this_node_bucket_dbdir(BucketName, Snapshot) ->
    <<_/binary>> = UUID = ns_bucket:uuid(BucketName, Snapshot),
    this_node_bucket_dbdir(UUID).

-spec this_node_bucket_dbdir(binary()) -> string().
this_node_bucket_dbdir(UUID) ->
    {ok, DBDir} = this_node_dbdir(),
    filename:join(DBDir, binary_to_list(UUID)).

pre_phoenix_bucket_dbdir(BucketName) ->
    {ok, DBDir} = this_node_dbdir(),
    filename:join(DBDir, BucketName).

-spec this_node_logdir() -> {ok, string()} | {error, any()}.
this_node_logdir() ->
    logdir(ns_config:latest(), node()).

-spec logdir(any(), atom()) -> {ok, string()} | {error, any()}.
logdir(Config, Node) ->
    read_path_from_conf(Config, Node, ns_log, filename).

%% @doc read a path from the configuration, following symlinks
-spec read_path_from_conf(any(), atom(), atom(), atom()) ->
    {ok, string()} | {error, any()}.
read_path_from_conf(Config, Node, Key, SubKey) ->
    {value, PropList} = ns_config:search_node(Node, Config, Key),
    case proplists:get_value(SubKey, PropList) of
        undefined ->
            {error, undefined};
        DBDir ->
            {ok, Base} = file:get_cwd(),
            case misc:realpath(DBDir, Base) of
                {error, Atom, _, _, _} -> {error, Atom};
                {ok, _} = X -> X
            end
    end.


%% @doc sets db, index, analytics, and eventing paths of this node.
%% NOTE: ns_server restart is required to make db and index paths change fully effective.
-spec setup_disk_storage_conf(DbPath::string(), IxPath::string(),
                              CBASDirs::list(), EvPath::string()) ->
    ok | restart | not_changed | {errors, [Msg :: binary()]}.
setup_disk_storage_conf(DbPath, IxPath, CBASDirs, EvPath) ->
    NewDbDir = misc:absname(DbPath),
    NewIxDir = misc:absname(IxPath),
    NewEvDir = misc:absname(EvPath),
    NewCBASDirs = lists:usort(
                    lists:map(
                      fun (Dir) ->
                              case misc:realpath(Dir, "/") of
                                  {ok, RealPath} ->
                                      RealPath;
                                  {error, _, _, _, {error, enoent}} ->
                                      %% We create them later.
                                      undefined
                              end
                      end, CBASDirs)),
    {CurrentDbDir, CurrentIxDir} = get_db_and_ix_paths(),
    CurrentCBASDir = this_node_cbas_dirs(),
    {ok, CurrentEvDir} = this_node_evdir(),

    DbDirChanged = NewDbDir =/= CurrentDbDir,
    IxDirChanged = NewIxDir =/= CurrentIxDir,
    CBASDirChanged = NewCBASDirs =/= CurrentCBASDir,
    EvDirChanged = NewEvDir =/= CurrentEvDir,

    case DbDirChanged orelse IxDirChanged orelse CBASDirChanged orelse
         EvDirChanged of
        true ->
            case ns_config_auth:is_system_provisioned() andalso
                 not ns_cluster_membership:is_newly_added_node(node()) of
                true ->
                    %% MB-7344: we had 1.8.1 instructions allowing that. And
                    %% 2.0 works very differently making that original
                    %% instructions lose data. Thus we decided it's much safer
                    %% to un-support this path.
                    Msg = <<"Changing paths of nodes that are part of "
                            "provisioned cluster is not supported">>,
                    {errors, [Msg]};
                false ->
                    do_setup_disk_storage_conf(NewDbDir, NewIxDir, CBASDirs,
                                               NewEvDir)
            end;
        false ->
            not_changed
    end.

do_setup_disk_storage_conf(NewDbDir, NewIxDir, CBASDirs, NewEvDir) ->
    Results = [prepare_db_ix_dirs(NewDbDir, NewIxDir),
               prepare_cbas_dirs(CBASDirs),
               prepare_ev_dir(NewEvDir)],

    Errors = lists:append([E || {errors, E} <- Results]),
    case Errors of
        [] ->
            [DbIxPrep, CbasPrep, EvPrep] = Results,
            Results2 = [update_db_ix_dirs(DbIxPrep, NewDbDir, NewIxDir),
                        update_cbas_dirs(CbasPrep),
                        update_ev_dir(EvPrep)],
            case lists:member(restart, Results2) of
                true ->
                    restart;
                false ->
                    case lists:member(ok, Results2) of
                        true ->
                            ok;
                        _ ->
                            [] = [R || R <- Results2, R =/= not_changed],
                            not_changed
                    end
            end;
        _ ->
            {errors, Errors}
    end.


prepare_db_ix_dirs(NewDbDir, NewIxDir) ->
    {CurrentDbDir, CurrentIxDir} = get_db_and_ix_paths(),

    case NewDbDir =/= CurrentDbDir orelse NewIxDir =/= CurrentIxDir of
        true ->
            case misc:ensure_writable_dirs([NewDbDir, NewIxDir]) of
                ok ->
                    case NewDbDir =:= CurrentDbDir of
                        true ->
                            ok;
                        false ->
                            ?log_info("Removing all unused database files in ~p", [CurrentDbDir]),
                            case delete_unused_buckets_db_files() of
                                ok ->
                                    ok;
                                Error ->
                                    Msg = iolist_to_binary(
                                            io_lib:format(
                                              "Could not delete unused database files in ~p: ~p.",
                                              [CurrentDbDir, Error])),
                                    {errors, [Msg]}
                            end
                    end;
                {error, _} ->
                    {errors, [<<"Could not set the storage path. It must be a directory writable by 'couchbase' user.">>]}
            end;
        false ->
            not_changed
    end.

update_db_ix_dirs(not_changed, _NewDbDir, _NewIxDir) ->
    not_changed;
update_db_ix_dirs(ok, NewDbDir, NewIxDir) ->
    ale:info(?USER_LOGGER, "Setting database directory path to ~s and index "
    "directory path to ~s", [NewDbDir, NewIxDir]),
    update_db_dir(filename:join([NewDbDir])),
    update_ix_dir(filename:join([NewIxDir])),
    restart.

prepare_cbas_dirs(CBASDirs) ->
    case misc:ensure_writable_dirs(CBASDirs) of
        ok ->
            RealDirs = lists:usort(
                         lists:map(fun (Dir) ->
                                           {ok, RealPath} = misc:realpath(Dir, "/"),
                                           RealPath
                                   end, CBASDirs)),
            case length(RealDirs) =:= length(CBASDirs) of
                false ->
                    {errors,
                     [<<"Could not set analytics storage. Different directories should not resolve "
                        "into the same physical location.">>]};
                true ->
                    case this_node_cbas_dirs() of
                        RealDirs ->
                            not_changed;
                        _ ->
                            {ok, RealDirs}
                    end
            end;
        {error, _} ->
            {errors,
             [<<"Could not set analytics storage. All directories must be writable by 'couchbase' user.">>]}
    end.

update_cbas_dirs(not_changed) ->
    not_changed;
update_cbas_dirs({ok, CBASDirs}) ->
    ns_config:set({node, node(), cbas_dirs}, CBASDirs).

this_node_cbas_dirs() ->
    node_cbas_dirs(ns_config:latest(), node()).

node_cbas_dirs(Config, Node) ->
   case cluster_compat_mode:is_cbas_enabled() of
        true ->
           {value, Dirs} =  ns_config:search_node(Node, Config, cbas_dirs),
           Dirs;
        false ->
            []
    end.

this_node_java_home() ->
    node_java_home(ns_config:latest(), node()).

node_java_home(Config, Node) ->
    ns_config:search_node_with_default(Node, Config, java_home, undefined).

update_java_home(not_changed) ->
    not_changed;
update_java_home([]) ->
    case this_node_java_home() of
        undefined ->
            not_changed;
        _ ->
            ns_config:delete({node, node(), java_home})
    end;
update_java_home(JavaHome) ->
    case this_node_java_home() of
        JavaHome ->
            not_changed;
        _ ->
            ns_config:set({node, node(), java_home}, JavaHome)
    end.

prepare_ev_dir(NewEvDir) ->
    {ok, CurrentEvDir} = this_node_evdir(),
    case NewEvDir =/= CurrentEvDir of
        true ->
            case misc:ensure_writable_dirs([NewEvDir]) of
                ok ->
                    {ok, NewEvDir};
                {error, _} ->
                    {errors, [<<"Could not set eventing path.  It must be a "
                                "directory writable by 'couchbase' user.">>]}
            end;
        false ->
            not_changed
    end.

-spec get_node_dir(atom()) -> {ok, string()} | {error, not_found}.
get_node_dir(TypeDir) ->
    case ns_config:search_node(TypeDir) of
        {value, Dir} -> {ok, Dir};
        false -> {error, not_found}
    end.

update_db_dir(DbDir) ->
    ns_config:set({node, node(), database_dir}, DbDir).

-spec this_node_dbdir() -> {ok, string()} | {error, not_found}.
this_node_dbdir() ->
    get_node_dir(database_dir).

update_ix_dir(IxDir) ->
    ns_config:set({node, node(), index_dir}, IxDir).

-spec this_node_ixdir() -> {ok, string()} | {error, not_found}.
this_node_ixdir() ->
    get_node_dir(index_dir).

update_ev_dir(not_changed) ->
    not_changed;
update_ev_dir({ok, EvDir}) ->
    ns_config:set({node, node(), eventing_dir}, EvDir).

-spec this_node_evdir() -> {ok, string()} | {error, not_found}.
this_node_evdir() ->
    get_node_dir(eventing_dir).

node_ev_dir(Config, Node) ->
    {value, Dir} = ns_config:search_node(Node, Config, eventing_dir),
    Dir.

% Returns a proplist of lists of proplists.
%
% A quotaMb of none means no quota. Disks can get full, disappear,
% etc, so non-ok state is used to signal issues.
%
% NOTE: in current implementation node disk quota is supported and
% state is always ok
%
% NOTE: current node supports only single storage path and does not
% support dedicated ssd (versus hdd) path
%
% NOTE: 1.7/1.8 nodes will not have storage conf returned in current
% implementation.
%
% [{ssd, []},
%  {hdd, [[{path, /some/nice/disk/path}, {quotaMb, 1234}, {state, ok}],
%         [{path", /another/good/disk/path}, {quotaMb, 5678}, {state, ok}]]}]
%
storage_conf_from_node_status(Node, NodeStatus) ->
    StorageConf = proplists:get_value(node_storage_conf, NodeStatus, []),
    HDDInfo = case proplists:get_value(db_path, StorageConf) of
                  undefined -> [];
                  DBDir ->
                      [{path, DBDir},
                       {index_path, proplists:get_value(index_path, StorageConf, DBDir)},
                       {cbas_dirs, node_cbas_dirs(ns_config:latest(), Node)},
                       {eventing_path, node_ev_dir(ns_config:latest(), Node)},
                       {java_home, node_java_home(ns_config:latest(), Node)},
                       {quotaMb, none},
                       {state, ok}]
              end,
    [{ssd, []},
     {hdd, [HDDInfo]}].

query_storage_conf() ->
    {DbDir, IxDir} = get_db_and_ix_paths(),
    StorageConf = [{db_path, DbDir}, {index_path, IxDir}],
    lists:map(
      fun ({Key, Path}) ->
              %% db_path and index_path are guaranteed to be absolute
              {ok, RealPath} = misc:realpath(Path, "/"),
              {Key, RealPath}
      end, StorageConf).

extract_node_storage_info(Config, Node, NodeInfo) ->
    {RAMTotal, RAMUsed, _} = proplists:get_value(memory_data, NodeInfo),
    DiskStats = proplists:get_value(disk_data, NodeInfo),
    StorageConf = proplists:get_value(node_storage_conf, NodeInfo, []),
    DiskPaths = [X || {PropName, X} <- StorageConf,
                      PropName =:= db_path orelse PropName =:= index_path] ++
                 node_cbas_dirs(Config, Node) ++
                 [node_ev_dir(Config, Node)],
    {DiskTotal, DiskUsed} = extract_disk_totals(DiskPaths, DiskStats),
    [{ram, [{total, RAMTotal},
            {used, RAMUsed}
           ]},
     {hdd, [{total, DiskTotal},
            {quotaTotal, DiskTotal},
            {used, DiskUsed},
            {free, DiskTotal - DiskUsed}
           ]}].

-spec extract_disk_totals(list(), list()) -> {integer(), integer()}.
extract_disk_totals(DiskPaths, DiskStats) ->

    F = fun (Path, {UsedMounts, ATotal, AUsed} = Tuple) ->
                case extract_disk_stats_for_path(DiskStats, Path) of
                    none -> Tuple;
                    {ok, {MPoint, KBytesTotal, Cap}} ->
                        case lists:member(MPoint, UsedMounts) of
                            true -> Tuple;
                            false ->
                                Total = KBytesTotal * 1024,
                                Used = (Total * Cap) div 100,
                                {[MPoint | UsedMounts], ATotal + Total,
                                 AUsed + Used}
                        end
                end
        end,
    {_UsedMounts, DiskTotal, DiskUsed} = lists:foldl(F, {[], 0, 0}, DiskPaths),
    {DiskTotal, DiskUsed}.

%% returns cluster_storage_info for subset of nodes
nodes_storage_info(NodeNames, Config, Snapshot) ->
    NodesDict = ns_doctor:get_nodes(),
    NodesInfos = lists:foldl(fun (N, A) ->
                                     case dict:find(N, NodesDict) of
                                         {ok, V} -> [{N, V} | A];
                                         _ -> A
                                     end
                             end, [], NodeNames),
    do_cluster_storage_info(NodesInfos, Config, Snapshot).

%% returns cluster storage info. This is aggregation of various
%% storage related metrics across active nodes of cluster.
%%
%% total - total amount of this resource (ram or hdd) in bytes
%%
%% free - amount of this resource free (ram or hdd) in bytes
%%
%% used - amount of this resource used (for any purpose (us, OS, other
%% processes)) in bytes
%%
%% quotaTotal - amount of quota for this resource across cluster
%% nodes. Note hdd quota is not really supported.
%%
%% quotaUsed - amount of quota already allocated
%%
%% usedByData - amount of this resource used by our data
cluster_storage_info(Config, Snapshot) ->
    nodes_storage_info(
      ns_cluster_membership:service_active_nodes(Snapshot, kv),
      Config, Snapshot).

extract_subprop(NodeInfos, Key, SubKey) ->
    [proplists:get_value(SubKey, proplists:get_value(Key, NodeInfo, [])) ||
     NodeInfo <- NodeInfos].

interesting_stats_total_rec([], _Key, Acc) ->
    Acc;
interesting_stats_total_rec([ThisStats | RestStats], Key, Acc) ->
    case lists:keyfind(Key, 1, ThisStats) of
        false ->
            interesting_stats_total_rec(RestStats, Key, Acc);
        {_, V} ->
            interesting_stats_total_rec(RestStats, Key, Acc + V)
    end.

do_cluster_storage_info([], _, _) -> [];
do_cluster_storage_info(NodeInfos, Config, Snapshot) ->
    NodesCount = length(NodeInfos),
    RAMQuotaUsedPerNode = memory_quota:get_max_node_ram_quota(Snapshot),
    RAMQuotaUsed = RAMQuotaUsedPerNode * NodesCount,

    RAMQuotaTotalPerNode =
        case memory_quota:get_quota(Config, kv) of
            {ok, MemQuotaMB} ->
                MemQuotaMB * ?MIB;
            _ ->
                0
        end,

    StorageInfos = [extract_node_storage_info(Config, Node, NodeInfo)
                    || {Node, NodeInfo} <- NodeInfos],
    HddTotals = extract_subprop(StorageInfos, hdd, total),
    HddUsed = extract_subprop(StorageInfos, hdd, used),

    AllInterestingStats = [proplists:get_value(interesting_stats, PList, []) || {_N, PList} <- NodeInfos],

    BucketsRAMUsage = interesting_stats_total_rec(AllInterestingStats, mem_used, 0),
    BucketsDiskUsage = interesting_stats_total_rec(AllInterestingStats, couch_docs_actual_disk_size, 0)
        + interesting_stats_total_rec(AllInterestingStats, couch_views_actual_disk_size, 0)
        + interesting_stats_total_rec(AllInterestingStats, couch_spatial_disk_size, 0),

    RAMUsed = erlang:max(lists:sum(extract_subprop(StorageInfos, ram, used)),
                         BucketsRAMUsage),
    HDDUsed = erlang:max(lists:sum(HddUsed),
                         BucketsDiskUsage),

    [{ram, [{total, lists:sum(extract_subprop(StorageInfos, ram, total))},
            {quotaTotal, RAMQuotaTotalPerNode * NodesCount},
            {quotaUsed, RAMQuotaUsed},
            {used, RAMUsed},
            {usedByData, BucketsRAMUsage},
            {quotaUsedPerNode, RAMQuotaUsedPerNode},
            {quotaTotalPerNode, RAMQuotaTotalPerNode}
           ]},
     {hdd, [{total, lists:sum(HddTotals)},
            {quotaTotal, lists:sum(HddTotals)},
            {used, HDDUsed},
            {usedByData, BucketsDiskUsage},
            {free, lists:min(lists:zipwith(fun (A, B) -> A - B end,
                                           HddTotals, HddUsed)) * length(HddUsed)} % Minimum amount free on any node * number of nodes
           ]}].

extract_disk_stats_for_path_rec([], _Path) ->
    none;
extract_disk_stats_for_path_rec([{MountPoint0, _, _} = Info | Rest], Path) ->
    MountPoint = filename:join([MountPoint0]),  % normalize path. See filename:join docs
    MPath = case lists:reverse(MountPoint) of
                %% ends of '/'
                "/" ++ _ -> MountPoint;
                %% doesn't. Append it
                X -> lists:reverse("/" ++ X)
            end,
    case MPath =:= string:substr(Path, 1, length(MPath)) of
        true -> {ok, Info};
        _ -> extract_disk_stats_for_path_rec(Rest, Path)
    end.

%% Path0 must be an absolute path with all symlinks resolved.
extract_disk_stats_for_path(StatsList, Path0) ->
    Path = case filename:join([Path0]) of
               "/" -> "/";
               X -> X ++ "/"
           end,
    %% we sort by decreasing length so that first match is 'deepest'
    LessEqFn = fun (A,B) ->
                       length(element(1, A)) >= length(element(1, B))
               end,
    SortedList = lists:sort(LessEqFn, StatsList),
    extract_disk_stats_for_path_rec(SortedList, Path).

%% scan data directory for bucket names
-spec bucket_dirs_from_disk() -> [string()].
bucket_dirs_from_disk() ->
    {ok, DbDir} = this_node_dbdir(),
    Files = filelib:wildcard("*", DbDir),
    lists:filter(
      fun (MaybeBucketName) ->
              Path = filename:join(DbDir, MaybeBucketName),
              maybe
                  %% Just making sure all filenames are flat strings.
                  %% If the returned type is not a flat string, it may lead
                  %% to bucket data deletion, so it is better to crash here
                  %% than to silently delete data.
                  {_, true} = {flat, misc:verify_list(MaybeBucketName,
                                                      fun is_integer/1)},
                  true ?= filelib:is_dir(Path),
                  MaybeUUID = iolist_to_binary(MaybeBucketName),
                  true ?= ns_bucket:is_valid_bucket_uuid(MaybeUUID) orelse
                          ns_bucket:is_valid_bucket_name(MaybeBucketName),
                  true
              else
                  {error, _} -> false;
                  false -> false
              end
      end, Files).

buckets_in_use() ->
    Node = node(),
    Snapshot =
        chronicle_compat:get_snapshot(
          [ns_bucket:fetch_snapshot(all, _, [props, uuid]),
           ns_cluster_membership:fetch_snapshot(_)],
          #{read_consistency => quorum}),
    Services = ns_cluster_membership:node_services(Snapshot, Node),
    BucketConfigs = ns_bucket:get_buckets(Snapshot),
    BucketNames =
        case lists:member(kv, Services) of
            true ->
                ns_bucket:node_bucket_names_of_type(Node, membase,
                                                    BucketConfigs);
            false ->
                case ns_cluster_membership:get_cluster_membership(Node,
                                                                  Snapshot) of
                    active ->
                        ns_bucket:get_bucket_names_of_type(membase,
                                                           BucketConfigs);
                    _ ->
                        []
                end
        end,
    [{BucketName, ns_bucket:uuid(BucketName, Snapshot)}
     || BucketName <- BucketNames].

%% deletes all databases files for buckets not defined for this node
%% note: this is called remotely
%%
%% it's named a bit differently from other functions here; but this function
%% is rpc called by older nodes; so we must keep this name unchanged
delete_unused_buckets_db_files() ->
    ?log_debug("Deleting unused bucket db files"),
    {BucketsInCfg, UUIDsInCfg} = lists:unzip(buckets_in_use()),
    DirsOnDisk = bucket_dirs_from_disk(),
    %% Starting from Phoenix bucket directory should be bucket uuid,
    %% but in previous releases it was bucket name, so we need to support
    %% both cases in case if that dir hasn't been migrated yet.
    DirsToDelete = [D || D <- DirsOnDisk,
                         not lists:member(D, BucketsInCfg),
                         not lists:member(list_to_binary(D), UUIDsInCfg)],
    functools:sequence_([?cut(memcached_delete_unused_buckets(BucketsInCfg))] ++
                        [?cut(delete_unused_db_files(Dir)) ||
                            Dir <- DirsToDelete]).

memcached_delete_unused_buckets(BucketsInCfg) ->
    maybe
        Buckets = ns_memcached:get_all_buckets_details(),
        true ?= is_list(Buckets),
        BucketsInMemcached =
            lists:filtermap(fun ({Props}) ->
                            case proplists:get_value(<<"name">>, Props) of
                                <<>> -> false;
                                BucketName when is_binary(BucketName) ->
                                    {true, binary_to_list(BucketName)}
                            end
                        end, Buckets),
        BucketsToDelete = lists:usort(BucketsInMemcached) -- BucketsInCfg,
        ?log_info("Buckets to delete on memcached: ~p", [BucketsToDelete]),
        functools:sequence_([?cut(ensure_delete_command_sent(
                                  Bucket,?ENSURE_DELETE_COMMAND_TIMEOUT)) ||
                             Bucket <- BucketsToDelete])
    else
        {error, Reason} ->
            ?log_error("Failed to get all buckets details from memcached. "
                       "Reason = ~p", [Reason]),
            {error, Reason};
        {memcached_error, Reason, Msg} ->
            ?log_error("Failed to get all buckets details from memcached. "
                       "Reason = ~p, Msg = ~p", [Reason, Msg]),
            {error, {memcached_error, Reason, Msg}}
    end.

ensure_delete_command_sent(Bucket, Timeout) ->
    case async:run_with_timeout(?cut(ensure_delete_command_sent(Bucket)),
                                Timeout) of
        {ok, Res} ->
            Res;
        {error, timeout} ->
            %% bucket deletion can take arbitrary amount of time, but we
            %% don't want to block delete_unused_buckets_db_files() infinitely
            %% so we just wait some time to make sure that memcached got the
            %% bucket_delete command and then race with it deleting the actual
            %% files. I hope this race is going to be benign.
            ?log_warning("Failed to wait for memcached to delete bucket ~p",
                         [Bucket]),
            ok
    end.

ensure_delete_command_sent(Bucket) ->
    case (catch ns_memcached:delete_bucket(Bucket, [{force, true}])) of
        ok ->
            ?log_info("Bucket ~p was deleted from memcached", [Bucket]),
            ok;
        {memcached_error, key_enoent, undefined} ->
            ok;
        Error ->
            ?log_error("Failed to delete bucket ~p from memcached. Error = ~p",
                       [Bucket, Error]),
            Error
    end.

delete_unused_db_files(Dir) when is_list(Dir) ->
    %% Note that we are not sending delete_bucket command to memcached here
    %% because we assume that all buckets are already deleted from memcached
    ?log_debug("Delete old data files for bucket in dir ~p", [Dir]),
    ale:info(?USER_LOGGER, "Deleting old data files of bucket in dir ~p",
             [Dir]),
    %% We need to destroy the DEKs before deleting the bucket files
    %% otherwise the dek files will be deleted while we will continue
    %% using them (they will stay in cb_cluster_secrets state).
    %% Note that it is important to remove the bucket directory from
    %% within the cb_cluster_secrets process because otherwise
    %% cb_cluster_secrets can create new DEK files while we are
    %% deleting the directory (which will lead to rm_rf failure or
    %% removal of newly created DEK files).
    MaybeBucketUUID = iolist_to_binary(Dir),
    IsBucketDirUUID =
        case ns_bucket:is_valid_bucket_uuid(MaybeBucketUUID) of
            true -> %% Dir can be a bucket uuid or a bucket name because
                    %% uuid is a valid bucket name
                Path = this_node_bucket_dbdir(MaybeBucketUUID),
                MetadataFile = ns_memcached:bucket_metadata_file(Path),
                %% The metadata file is present only in Phoenix
                %% so if it exists Dir has to be a bucket uuid
                filelib:is_file(MetadataFile);
            false -> %% Dir has to be a bucket name, not a bucket uuid
                false
        end,
    case IsBucketDirUUID of
        true ->
            cb_cluster_secrets:destroy_deks(
                {bucketDek, MaybeBucketUUID},
                fun () ->
                    case ns_couchdb_api:delete_databases_and_files_uuid(
                           MaybeBucketUUID) of
                        ok ->
                            ok;
                        Other ->
                            ?log_error("Failed to delete old data files "
                                    "dir ~p. Error = ~p", [Dir, Other]),
                            Other
                    end
                end);
        false ->
            case ns_couchdb_api:delete_databases_and_files(Dir) of
                ok ->
                    ok;
                Other ->
                    ?log_error("Failed to delete old data files "
                               "dir ~p. Error = ~p", [Dir, Other]),
                    Other
            end
    end.

%% deletes @2i subdirectory in index directory of this node.
%%
%% NOTE: rpc-called remotely from ns_rebalancer prior to activating
%% new nodes at the start of rebalance.
%%
%% Since 4.0 compat mode.
delete_old_2i_indexes() ->
    {ok, IxDir} = this_node_ixdir(),
    Dir = filename:join(IxDir, "@2i"),
    misc:rm_rf(Dir).


-ifdef(TEST).
extract_disk_stats_for_path_test() ->
    DiskSupStats = [{"/",297994252,97},
             {"/lib/init/rw",1921120,1},
             {"/dev",10240,2},
             {"/dev/shm",1921120,0},
             {"/var/separate",1921120,0},
             {"/media/p2",9669472,81}],
    ?assertEqual({ok, {"/media/p2",9669472,81}},
                 extract_disk_stats_for_path(DiskSupStats,
                                             "/media/p2/mbdata")),
    ?assertEqual({ok, {"/", 297994252, 97}},
                 extract_disk_stats_for_path(DiskSupStats, "/")),
    ?assertEqual({ok, {"/", 297994252, 97}},
                 extract_disk_stats_for_path(DiskSupStats, "/lib/init")),
    ?assertEqual({ok, {"/dev", 10240, 2}},
                 extract_disk_stats_for_path(DiskSupStats, "/dev/sh")),
    ?assertEqual({ok, {"/dev", 10240, 2}},
                 extract_disk_stats_for_path(DiskSupStats, "/dev")).
-endif.
