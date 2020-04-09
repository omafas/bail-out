%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2019 Couchbase, Inc.
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
% A module for retrieving & configuring per-server storage paths,
% storage quotas, mem quotas, etc.
%
-module(ns_storage_conf).

-include("ns_common.hrl").
-include("ns_config.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([setup_disk_storage_conf/4,
         storage_conf_from_node_status/2,
         query_storage_conf/0,
         this_node_dbdir/0, this_node_ixdir/0, this_node_logdir/0,
         this_node_evdir/0,
         this_node_bucket_dbdir/1,
         delete_unused_buckets_db_files/0,
         delete_old_2i_indexes/0,
         setup_storage_paths/0,
         this_node_cbas_dirs/0,
         this_node_java_home/0,
         update_java_home/1]).

-export([cluster_storage_info/0, nodes_storage_info/1]).

-export([extract_disk_stats_for_path/2]).

setup_storage_paths() ->
    setup_db_and_ix_paths(ns_couchdb_api:get_db_and_ix_paths()),
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
    ignore.

setup_db_and_ix_paths(Dirs) ->
    ?log_debug("Initialize db_and_ix_paths variable with ~p", [Dirs]),
    application:set_env(ns_server, db_and_ix_paths, Dirs).

get_db_and_ix_paths() ->
    case application:get_env(ns_server, db_and_ix_paths) of
        undefined ->
            ns_couchdb_api:get_db_and_ix_paths();
        {ok, Paths} ->
            Paths
    end.

couch_storage_path(Field) ->
    try get_db_and_ix_paths() of
        PList ->
            {Field, RV} = lists:keyfind(Field, 1, PList),
            {ok, RV}
    catch T:E ->
            ?log_debug("Failed to get couch storage config field: ~p due to ~p:~p", [Field, T, E]),
            {error, iolist_to_binary(io_lib:format("couch config access failed: ~p:~p", [T, E]))}
    end.

-spec this_node_dbdir() -> {ok, string()} | {error, binary()}.
this_node_dbdir() ->
    couch_storage_path(db_path).

-spec this_node_bucket_dbdir(bucket_name()) -> {ok, string()}.
this_node_bucket_dbdir(BucketName) ->
    {ok, DBDir} = ns_storage_conf:this_node_dbdir(),
    DBSubDir = filename:join(DBDir, BucketName),
    {ok, DBSubDir}.

-spec this_node_ixdir() -> {ok, string()} | {error, binary()}.
this_node_ixdir() ->
    couch_storage_path(index_path).

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
    [{db_path, CurrentDbDir},
     {index_path, CurrentIxDir}] = lists:sort(ns_couchdb_api:get_db_and_ix_paths()),
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
    [{db_path, CurrentDbDir},
     {index_path, CurrentIxDir}] = lists:sort(ns_couchdb_api:get_db_and_ix_paths()),

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
                                              "Could not delete unused database files in ~p: ~p",
                                              [CurrentDbDir, Error])),
                                    {errors, [Msg]}
                            end
                    end;
                error ->
                    {errors, [<<"Could not set the storage path. It must be a directory writable by 'couchbase' user.">>]}
            end;
        false ->
            not_changed
    end.

update_db_ix_dirs(not_changed, _NewDbDir, _NewIxDir) ->
    not_changed;
update_db_ix_dirs(ok, NewDbDir, NewIxDir) ->
    ale:info(?USER_LOGGER,
             "Setting database directory path to ~s and index directory path to ~s",
             [NewDbDir, NewIxDir]),

    ns_couchdb_api:set_db_and_ix_paths(NewDbDir, NewIxDir),
    setup_db_and_ix_paths([{db_path, NewDbDir},
                           {index_path, NewIxDir}]),
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
                        "into the same physical location">>]};
                true ->
                    case this_node_cbas_dirs() of
                        RealDirs ->
                            not_changed;
                        _ ->
                            {ok, RealDirs}
                    end
            end;
        error ->
            {errors,
             [<<"Could not set analytics storage. All directories must be writable by 'couchbase' user.">>]}
    end.

update_cbas_dirs(not_changed) ->
    not_changed;
update_cbas_dirs({ok, CBASDirs}) ->
    ns_config:set({node, node(), cbas_dirs}, CBASDirs).

this_node_cbas_dirs() ->
    node_cbas_dirs(ns_config:latest(), node(), []).

node_cbas_dirs(Config, Node, StorageConf) ->
   case cluster_compat_mode:is_cbas_enabled() of
        true ->
            case ns_config:search_node(Node, Config, cbas_dirs) of
                {value, V} ->
                    V;
                false ->
                    %% Should only occur running mixed versions (e.g. during
                    %% upgrade)
                    %% TODO: Remove once 6.5 is the oldest supported
                    %%       release
                    IndexPath = proplists:get_value(index_path, StorageConf),
                    true = IndexPath =/= undefined,
                    [IndexPath]
            end;
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
                error ->
                    {errors, [<<"Could not set eventing path.  It must be a "
                                "directory writable by 'couchbase' user.">>]}
            end;
        false ->
            not_changed
    end.

update_ev_dir(not_changed) ->
    not_changed;
update_ev_dir({ok, EvDir}) ->
    ns_config:set({node, node(), eventing_dir}, EvDir).

this_node_evdir() ->
    {ok, node_ev_dir(ns_config:latest(), node(), [])}.

node_ev_dir(Config, Node, StorageConf) ->
    case ns_config:search_node(Node, Config, eventing_dir) of
        {value, V} ->
            V;
        false ->
            %% Should only occur running mixed versions (e.g. during upgrade).
            %% Prior to 6.5, eventing used the index path
            %% TODO: Remove once 6.5 is the oldest supported release.
            IndexPath = proplists:get_value(index_path, StorageConf),
            true = IndexPath =/= undefined,
            IndexPath
    end.

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
                       {cbas_dirs, node_cbas_dirs(ns_config:latest(),
                                                  Node, StorageConf)},
                       {eventing_path, node_ev_dir(ns_config:latest(), Node,
                                                  StorageConf)},
                       {java_home, node_java_home(ns_config:latest(), Node)},
                       {quotaMb, none},
                       {state, ok}]
              end,
    [{ssd, []},
     {hdd, [HDDInfo]}].

query_storage_conf() ->
    StorageConf = get_db_and_ix_paths(),
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
                 node_cbas_dirs(Config, Node, StorageConf) ++
                 [node_ev_dir(Config, Node, StorageConf)],
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
nodes_storage_info(NodeNames) ->
    NodesDict = ns_doctor:get_nodes(),
    NodesInfos = lists:foldl(fun (N, A) ->
                                     case dict:find(N, NodesDict) of
                                         {ok, V} -> [{N, V} | A];
                                         _ -> A
                                     end
                             end, [], NodeNames),
    do_cluster_storage_info(NodesInfos).

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
cluster_storage_info() ->
    nodes_storage_info(ns_cluster_membership:service_active_nodes(kv)).

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

do_cluster_storage_info([]) -> [];
do_cluster_storage_info(NodeInfos) ->
    Config = ns_config:get(),
    NodesCount = length(NodeInfos),
    RAMQuotaUsedPerNode = memory_quota:get_total_buckets_ram_quota(Config),
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
bucket_names_from_disk() ->
    {ok, DbDir} = this_node_dbdir(),
    Files = filelib:wildcard("*", DbDir),
    lists:foldl(
      fun (MaybeBucket, Acc) ->
              Path = filename:join(DbDir, MaybeBucket),
              case filelib:is_dir(Path) of
                  true ->
                      case ns_bucket:is_valid_bucket_name(MaybeBucket) of
                          true ->
                              [MaybeBucket | Acc];
                          {error, _} ->
                              Acc
                      end;
                  false ->
                      Acc
              end
      end, [], Files).

delete_disk_buckets_databases(Pred) ->
    Buckets = lists:filter(Pred, bucket_names_from_disk()),
    delete_disk_buckets_databases_loop(Pred, Buckets).

delete_disk_buckets_databases_loop(_Pred, []) ->
    ok;
delete_disk_buckets_databases_loop(Pred, [Bucket | Rest]) ->
    case ns_couchdb_api:delete_databases_and_files(Bucket) of
        ok ->
            delete_disk_buckets_databases_loop(Pred, Rest);
        Error ->
            Error
    end.

%% deletes all databases files for buckets not defined for this node
%% note: this is called remotely
%%
%% it's named a bit differently from other functions here; but this function
%% is rpc called by older nodes; so we must keep this name unchanged
delete_unused_buckets_db_files() ->
    Config = ns_config:get(),
    Services = ns_cluster_membership:node_services(Config, node()),
    BCfgs = ns_bucket:get_buckets(Config),
    BucketNames =
        case lists:member(kv, Services) of
            true ->
                ns_bucket:node_bucket_names_of_type(node(), membase, couchstore, BCfgs)
                    ++ ns_bucket:node_bucket_names_of_type(node(), membase, ephemeral, BCfgs);
            false ->
                case ns_cluster_membership:get_cluster_membership(node(), Config) of
                    active ->
                        ns_bucket:get_bucket_names_of_type(membase, BCfgs);
                    _ ->
                        []
                end
        end,
    delete_disk_buckets_databases(
      fun (Bucket) ->
              RV = not(lists:member(Bucket, BucketNames)),
              case RV of
                  true ->
                      ale:info(?USER_LOGGER, "Deleting old data files of bucket ~p", [Bucket]);
                  _ ->
                      ok
              end,
              RV
      end).

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
