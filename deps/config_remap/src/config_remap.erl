%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(config_remap).

-include_lib("chronicle/src/chronicle.hrl").
-include_lib("ns_server/include/ns_common.hrl").
-include_lib("ns_server/include/cut.hrl").

-export([main/1]).

-define(NODEFILE_NAME, "nodefile").

-define(IPFILE_NAMES, ["ip", "ip_start"]).

-define(CONFIG_DIR, "config").

-define(NS_CONFIG_NAME, "config/config.dat").

-define(INITARGS_NAME, "initargs").
-define(INITARGS_NS_SERVER_PROPS, ns_server).
-define(INITARGS_BIN_DIR, path_config_bindir).
-define(INITARGS_LOG_DIR, error_logger_mf_dir).
-define(INITARGS_DATA_DIR, path_config_datadir).

-define(CHRONICLE_DIR, "config/chronicle").
-define(CHRONICLE_SNAPSHOT_DIR, filename:join(?CHRONICLE_DIR, "snapshots")).
-define(CHRONICLE_LOGS_DIR, filename:join(?CHRONICLE_DIR, "logs")).
-define(CHRONICLE_KV_SNAPSHOT, "kv.snapshot").
-define(CHRONICLE_CONFIG_RSM_SNAPSHOT, "chronicle_config_rsm.snapshot").

rewrite_term(BeforeTerm, LogAs, Args) when is_list(BeforeTerm) ->
    lists:map(
      fun(Term) ->
              rewrite_term(Term, LogAs, Args)
      end, BeforeTerm);
rewrite_term(BeforeTerm, LogAs, #{node_map := NodeMap}) ->
    AfterTerm = maps:fold(
                  fun(OldNode, NewNode, Acc) ->
                          misc:rewrite_value(list_to_atom(OldNode),
                                             list_to_atom(NewNode), Acc)
                  end, BeforeTerm, NodeMap),

    %% We should avoid writing too much to the log that isn't useful, only log
    %% if different.
    case BeforeTerm of
        AfterTerm ->
            AfterTerm;
        _ ->
            ?log_debug("Rewriting ~s term ~p as ~p",
                       [LogAs, BeforeTerm, AfterTerm]),
            AfterTerm
    end.

read_term_from_file(Path) ->
    {ok, Data} = file:read_file(Path),
    erlang:binary_to_term(Data).

read_ns_config_from_file(Path) ->
    [Config | _] = read_term_from_file(Path),
    Config.

modify_ns_config_tuples(Config, Args) ->
    rewrite_term(Config, "ns_config", Args).

get_ns_config_path(#{?INITARGS_DATA_DIR := Path}) ->
    filename:join(Path, ?NS_CONFIG_NAME).

rewrite_ns_config(#{output_path := OutputPath} = Args) ->
    ?log_info("Rewriting ns_config"),

    OriginalCfg = read_ns_config_from_file(get_ns_config_path(Args)),
    NewCfg = functools:chain(OriginalCfg,
                             [modify_ns_config_tuples(_, Args),
                              maybe_rewrite_cookie(_, Args),
                              maybe_rewrite_cluster_uuid(_, Args)]),

    NsConfigPath = filename:join(OutputPath, ?NS_CONFIG_NAME),
    ok = filelib:ensure_dir(NsConfigPath),
    ok = file:write_file(NsConfigPath, term_to_binary([NewCfg])).

maybe_rewrite_cluster_uuid(Cfg, Args) ->
    case maps:find(regenerate_cluster_uuid, Args) of
        {ok, true} -> rewrite_cluster_uuid(Cfg, Args);
        _ -> Cfg
    end.

%% Generate a new UUID. We want a (relatively) unique UUID so we will generate a
%% new random one. We can't use crypto:strong_rand_bytes/1 that the normal uuid
%% generator does it won't allow us to reproduce the same random value on all
%% nodes being remapped (it mixes in some OS provided entropy). To create the
%% same UUID on all nodes we will seed a random generator with the remapping map
%% and the old UUID. The old UUID use is somewhat questionable, but ensures that
%% the state of the cluster remains the same after the remap as it was before
%% (i.e. if one node had a different UUID for some reason then it will continue
%% to do so).
generate_uuid(OldUUID, #{node_map := NodeMap}) ->
    Seed = erlang:crc32(erlang:term_to_binary({OldUUID, NodeMap})),
    rand:seed(default, Seed),
    list_to_binary(couch_util:to_hex(rand:bytes(16))).

rewrite_cluster_uuid(Cfg, Args) ->
    ?log_info("Rewriting ns_config uuid"),
    lists:map(
      fun({uuid, [VClock | OldUUID]}) ->
              NewUUID = generate_uuid(OldUUID, Args),

              ?log_debug("Replacing old uuid ~p with ~p", [OldUUID, NewUUID]),
              {uuid, [VClock | NewUUID]};
         (V) -> V
      end, Cfg).

maybe_rewrite_cookie(Cfg, Args) ->
    case maps:find(regenerate_cookie, Args) of
        {ok, true} -> rewrite_cookie(Cfg, Args);
        _ -> Cfg
    end.

rewrite_cookie(Cfg, #{go_secrets_pid := SecretsPid,
                      node_map := NodeMap}) ->
    ?log_info("Rewriting ns_config cookie"),
    lists:map(
      fun({otp, [VClock, {cookie, {encrypted, OldCookie}}]}) ->
              {ok, OldUnencryptedCookie} =
                  cb_gosecrets_runner:decrypt(SecretsPid, OldCookie),
              NewCookie =
                  term_to_binary(generate_cookie(OldUnencryptedCookie,
                                                 NodeMap)),

              {ok, EncryptedCookie} =
                  cb_gosecrets_runner:encrypt(SecretsPid,NewCookie),

              ?log_debug("Replacing encrypted cookie ~p with ~p",
                         [OldCookie, EncryptedCookie]),
              {otp, [VClock, {cookie, {encrypted, EncryptedCookie}}]};
         (V) -> V
      end, Cfg).

get_new_bucket_uuid(Bucket, OldUUID, Args) ->
    case ets:lookup(bucket_uuids, Bucket) of
        [{Bucket, UUID}] -> UUID;
        [] ->
            NewUUID = generate_uuid(OldUUID, Args),
            ets:insert(bucket_uuids, {Bucket, NewUUID}),
            NewUUID
    end.

rewrite_chronicle_set_bucket_uuid(BeforeTerm, LogAs,
                                  #{regenerate_bucket_uuids := true} = Args) ->
    AfterTerm =
        generic:transformt(
          fun (Var) ->
                  case Var of
                      {set, {bucket, Bucket, uuid}, OldUUID} ->
                          NewUUID = get_new_bucket_uuid(Bucket, OldUUID, Args),
                          {set,{bucket, Bucket, uuid}, NewUUID};
                      _ -> Var
                  end
          end, BeforeTerm),

    %% We should avoid writing too much to the log that isn't useful, only log
    %% if different.
    case BeforeTerm of
        AfterTerm ->
            AfterTerm;
        _ ->
            ?log_debug("Rewriting ~s bucket uuid ~p as ~p",
                       [LogAs, BeforeTerm, AfterTerm]),
            AfterTerm
    end;
rewrite_chronicle_set_bucket_uuid(BeforeTerm, _LogAs, _Args) ->
    BeforeTerm.

rewrite_chronicle_snapshot_bucket_uuid(BeforeTerm,
                                       #{regenerate_bucket_uuids := true}
                                       = Args) ->
    {snapshot, A, B, C, Map, D} = BeforeTerm,

    NewMap =
        maps:map(
          fun(Key, Value) ->
                  case Key of
                      {bucket, Bucket, uuid} ->

                          {OldUUID, Meta} = Value,
                          NewUUID = get_new_bucket_uuid(Bucket, OldUUID, Args),

                          ?log_debug("Rewriting bucket uuid for ~p. Old value "
                                     "~p New value ~p",
                                     [Bucket, OldUUID, NewUUID]),
                          {NewUUID, Meta};
                      _ -> Value
                  end
          end, Map),


    {snapshot, A, B, C, NewMap, D};
rewrite_chronicle_snapshot_bucket_uuid(BeforeTerm, _Args) ->
    BeforeTerm.

rewrite_chronicle(#{?INITARGS_DATA_DIR := InputDir,
                    output_path := OutputDir} = Args) ->
    ?log_info("Rewriting chronicle"),

    LogsDir = filename:join(InputDir, ?CHRONICLE_LOGS_DIR),
    {ok, Logs} = file:list_dir(LogsDir),
    lists:foreach(
      fun(Log) ->
              LogPath = filename:join(LogsDir, Log),
              rewrite_chronicle_log(LogPath, Args)
      end, Logs),

    %% Required to re-use chronicle snapshot storage write fun
    ChronicleEnvDataDir = filename:join([OutputDir, ?CONFIG_DIR]),
    ?log_debug("Rewriting chronicle files to ~p", [ChronicleEnvDataDir]),
    application:set_env(chronicle, data_dir, ChronicleEnvDataDir),

    SnapshotDir = filename:join(InputDir, ?CHRONICLE_SNAPSHOT_DIR),
    {ok, Snapshots} = file:list_dir(SnapshotDir),
    lists:foreach(
      fun(Seqno) ->
              rewrite_chronicle_rsm_snapshots(SnapshotDir, Seqno, Args)
      end, Snapshots).

rewrite_chronicle_rsm_snapshots(SnapshotsDir, Seqno, Args) ->
    ?log_info("Rewriting chronicle snapshot ~p", [Seqno]),

    SnapshotDir = filename:join(SnapshotsDir, Seqno),
    {ok, RSMSnapshots} = file:list_dir(SnapshotDir),

    lists:foreach(
      fun(RSMSnapshot) ->
              ?log_info("Rewriting ~p snapshot", [RSMSnapshot]),
              rewrite_chronicle_rsm_snapshot(Seqno, RSMSnapshot, Args)
      end,
      RSMSnapshots).

rewrite_chronicle_rsm_snapshot(Seqno, Snapshot,
                               #{?INITARGS_DATA_DIR := InputDir,
                                 output_path := OutputDir} = Args) ->
    %% Read from path directly, the seqno/snapshot function will use the env
    %% which we've set to the output path
    IntegerSeqno = list_to_integer(Seqno),
    ReadPath = filename:join([InputDir, ?CHRONICLE_SNAPSHOT_DIR, Seqno,
                              Snapshot]),
    ?log_debug("Reading chronicle snapshot at path ~p", [ReadPath]),
    {ok, Snap} = chronicle_storage:read_rsm_snapshot(ReadPath),

    NewSnapData = rewrite_chronicle_snapshot_term(Snapshot, Snap, Args),

    OutputPath = filename:join([OutputDir, ?CHRONICLE_SNAPSHOT_DIR, Seqno,
                                Snapshot]),
    filelib:ensure_dir(OutputPath),

    [SnapshotType, _] = string:split(Snapshot, "."),
    ?log_debug("Reading chronicle snapshot at path ~p", [ReadPath]),
    ok = chronicle_storage:save_rsm_snapshot(IntegerSeqno, SnapshotType,
                                             NewSnapData),

    %% Sanity, can we read it again, and is it the same?
    {ok, NewSnapData} = chronicle_storage:read_rsm_snapshot(SnapshotType,
                                                            IntegerSeqno).

%% kv snapshot may need to rewrite bucket uuids
rewrite_chronicle_snapshot_term(?CHRONICLE_KV_SNAPSHOT, Term, Args) ->
    Msg = io_lib:format("chronicle ~s snapshot", [?CHRONICLE_KV_SNAPSHOT]),

    functools:chain(Term, [rewrite_term(_, Msg, Args),
                           rewrite_chronicle_snapshot_bucket_uuid(_, Args)]);
rewrite_chronicle_snapshot_term(?CHRONICLE_CONFIG_RSM_SNAPSHOT, Term, Args) ->
    Msg = io_lib:format("chronicle ~s snapshot",
                        [?CHRONICLE_CONFIG_RSM_SNAPSHOT]),
    rewrite_term(Term, Msg, Args).


rewrite_chronicle_log_header(Header, #{output_path := OutputPath,
                                       args := Args} = State) ->
    LogNum = filename:basename(OutputPath),

    ?log_debug("Rewriting log ~p header", [LogNum]),
    Msg = io_lib:format("chronicle log ~p header", [LogNum]),
    NewHeader = rewrite_term(Header, Msg, Args),

    filelib:ensure_dir(OutputPath),
    {ok, Log} = chronicle_log:create(OutputPath, NewHeader),

    %% Log now, the log entry fun is called for every entry
    ?log_debug("Rewriting log ~p entries", [LogNum]),

    %% Return out log in the state to re-use it, rather than open the file again
    State#{log_file => Log}.

rewrite_chronicle_log_command({command, Packed},
                              #{args := Args,
                                output_path := OutputPath}) ->
    Unpacked = chronicle_rsm:unpack_command(Packed),

    LogNum = filename:basename(OutputPath),
    Msg = io_lib:format("chronicle log ~p command", [LogNum]),

    Rewritten =
        functools:chain(Unpacked,
                        [rewrite_term(_, Msg, Args),
                         rewrite_chronicle_set_bucket_uuid(_, Msg, Args)]),

    Repacked = chronicle_rsm:pack_command(Rewritten),
    {command, Repacked}.

maybe_rewrite_chronicle_rsm_command(
  #rsm_command{payload = Payload} = RSMCommand, State) ->
    case Payload of
        %% Nothing to do for noops
        noop -> RSMCommand;
        %% Commands are compressed terms and need to be unpacked
        %% to be rewritten
        {command, _} = Command ->
            NewPayload = rewrite_chronicle_log_command(Command, State),
            RSMCommand#rsm_command{payload = NewPayload}
    end.

maybe_rewrite_chronicle_log_append_value(#log_entry{value = Value} = LogEntry,
                                         State) ->
    case Value of
        #rsm_command{} = RSMCommand ->
            LogEntry#log_entry{
              value = maybe_rewrite_chronicle_rsm_command(RSMCommand, State)};
        _ -> LogEntry
    end.

maybe_rewrite_chronicle_log_append_command(Entry, State) ->
    chronicle_storage:map_append(
      fun(LogEntry) ->
              maybe_rewrite_chronicle_log_append_value(LogEntry, State)
      end, Entry).

rewrite_chronicle_log_entry(Entry, #{log_file := Log,
                                     args := Args,
                                     output_path := OutputPath} = State) ->
    LogNum = filename:basename(OutputPath),


    Msg = io_lib:format("chronicle log ~p entry", [LogNum]),

    NewEntry =
        functools:chain(Entry,
                        [
                         %% The entry itself may be an append command which
                         %% contains the config values that we set in ns_server.
                         %% Those commands are compressed terms which we need to
                         %% uncompress to rewrite.
                         maybe_rewrite_chronicle_log_append_command(_, State),
                         %% Now overwrite any metadata associated with the log
                         %% entry
                         rewrite_term(_, Msg, Args)]),
    chronicle_log:append(Log, [NewEntry]),
    State.

rewrite_chronicle_log(Path, #{output_path := OutputDir} = Args) ->
    LogNum = filename:basename(Path),
    ?log_info("Rewriting chronicle log ~p", [LogNum]),

    OutputPath = filename:join([OutputDir,
                                ?CHRONICLE_LOGS_DIR,
                                filename:basename(Path)]),
    ?log_debug("Writing chronicle log to ~p", [OutputPath]),

    case chronicle_log:read_log(Path,
                                fun rewrite_chronicle_log_header/2,
                                fun rewrite_chronicle_log_entry/2,
                                #{output_path => OutputPath, args => Args,
                                  log_num => LogNum}) of
        {ok, #{log_file := Log}} ->
            ok = chronicle_log:close(Log);
        {error, Error} ->
            erlang:exit("Failed to rewrite chronicle log ~p. Got error ~p",
                        [Path, Error])
    end,

    %% Sanity time, won't compare the results, but we should be able to read
    %% and iterate the file
    {ok, _A} = chronicle_log:read_log(OutputPath,
                                      fun(_H, S) -> S end,
                                      fun(_E, S) -> S end,
                                      []).

rewrite_string_file(File, #{?INITARGS_DATA_DIR := InputDir,
                            output_path := OutputDir,
                            node_map := NodeMap}) ->
    InputFileName = filename:join(InputDir, File),
    {ok, OldContents} = file:read_file(InputFileName),
    NewContents = maps:fold(
                    fun(Key, Value, Acc) ->
                            string:replace(Acc, Key, Value, all)
                    end, OldContents, NodeMap),

    ?log_debug("Final result for ~p file: ~p",
               [File, list_to_binary(NewContents)]),

    OutputFileName = filename:join(OutputDir, File),
    filelib:ensure_dir(OutputFileName),
    ok = file:write_file(OutputFileName, list_to_binary(NewContents)).

rewrite_nodefile(#{?INITARGS_DATA_DIR := InputDir} = Args) ->
    ?log_info("Rewriting nodefile"),

    InputFileName = filename:join(InputDir, ?NODEFILE_NAME),

    %% This does not exist in installed deployments, just cluster_run it seems
    case filelib:is_file(InputFileName) of
        false -> ok;
        true -> rewrite_string_file(?NODEFILE_NAME, Args)
    end.

remove_prefix_from_node_name(Node) ->
    case string:split(Node, "@") of
        [_Prefix, Host] -> Host;
        Other -> Other
    end.

rewrite_ip_file(File, #{?INITARGS_DATA_DIR := InputDir,
                        output_path := OutputDir,
                        node_map := NodeMap}) ->
    ?log_info("Rewriting ip file ~p", [File]),

    InputFileName = filename:join(InputDir, File),
    {ok, Contents} = file:read_file(InputFileName),
    IP = maps:fold(
           fun(FullKey, FullValue, Acc) ->
                   Key = remove_prefix_from_node_name(FullKey),
                   Value = remove_prefix_from_node_name(FullValue),
                   string:replace(Acc, Key, Value, all)
           end, Contents, NodeMap),

    ?log_debug("Final result for ip file: ~p", [list_to_binary(IP)]),

    OutputFileName = filename:join(OutputDir, File),

    ok = filelib:ensure_dir(OutputFileName),
    ok = file:write_file(OutputFileName, list_to_binary(IP)).

maybe_rewrite_ip_file(File, #{?INITARGS_DATA_DIR := InputDir} = Args) ->
    InputFileName = filename:join(InputDir, File),

    case filelib:is_file(InputFileName) of
        false -> ok;
        true -> rewrite_ip_file(File, Args)
    end.

rewrite_ip_files(Args) ->
    ?log_info("Rewriting ip files"),

    lists:foreach(
      fun(File) ->
              maybe_rewrite_ip_file(File, Args)
      end, ?IPFILE_NAMES).

rewrite_initargs(#{?INITARGS_DATA_DIR := InputDir,
                   output_path := OutDir} = Args) ->
    ?log_info("Rewriting initargs"),
    Path = filename:join(InputDir, ?INITARGS_NAME),

    Term = read_term_from_file(Path),
    NewTerm = rewrite_term(Term, "initargs", Args),
    ?log_debug("Rewriting initargs term ~p with ~p",
               [Term, NewTerm]),

    OutPath = filename:join(OutDir, ?INITARGS_NAME),
    ok = file:write_file(OutPath, term_to_binary(NewTerm)).

setup_stderr_logging() ->
    ok = application:start(ale),

    ok = ale:start_sink(stderr, ale_stderr_sink, []),

    ok = ale:start_logger(?NS_SERVER_LOGGER, info),
    ok = ale:add_sink(?NS_SERVER_LOGGER, stderr).

setup_file_logging(#{?INITARGS_LOG_DIR := LogDir}) ->
    FileName = string:concat(atom_to_list(?MODULE), ".log"),
    Path = filename:join(LogDir, FileName),

    ?log_info("Configuring file logging to ~p", [Path]),

    ok = ale:start_sink(disk, ale_disk_sink, [Path, []]),

    %% We will log all messages to disk (debug level)
    ok = ale:add_sink(?NS_SERVER_LOGGER, disk, debug),
    %% We must set the logger level to the lowest that we log at (debug)
    ok = ale:set_loglevel(?NS_SERVER_LOGGER, debug),

    ?log_info("Started & configured logging").

maybe_tweak_log_verbosity(#{log_level := Level}) ->
    ok = ale:set_loglevel(?NS_SERVER_LOGGER, Level),
    ok = ale:set_sink_loglevel(?NS_SERVER_LOGGER, stderr, Level).

start_gosecrets(#{?INITARGS_DATA_DIR := InputPath}) ->
    CfgPath = filename:join(InputPath, "config/gosecrets.cfg"),

    ?log_debug("Spawning gosecrets with cfg path ~p~n", [CfgPath]),

    %% We are assuming here that the gosecrets.cfg exists, which requires that
    %% the installation is EE.
    {ok, Pid} = cb_gosecrets_runner:start_link(CfgPath),
    ?log_debug("Gosecrets loop started with pid = ~p", [Pid]),
    Pid.

init_gosecrets(Args) ->
    ?log_info("Initializing gosecrets"),
    Pid = start_gosecrets(Args),
    Args#{go_secrets_pid => Pid}.

usage(Args) ->
    ?log_error("Invalid args specified ~p", [Args]),
    erlang:halt(1).

-spec default_args() -> map().
default_args() ->
    #{log_level => info,
      regenerate_cookie => false,
      regenerate_cluster_uuid => false,
      regenerate_bucket_uuids => false}.

-spec parse_args(list(), map()) -> map().
parse_args(["--output-path", Path | Rest], Map) ->
    parse_args(Rest, Map#{output_path => Path});
parse_args(["--log-level", Level | Rest], Map) ->
    parse_args(Rest, Map#{log_level => list_to_atom(Level)});
parse_args(["--remap", A, B | Rest], Map) ->
    CurrentNodeMap = case maps:find(node_map, Map) of
                         {ok, NodeMap} -> NodeMap;
                         _ -> #{}
                     end,
    parse_args(Rest, Map#{node_map => CurrentNodeMap#{A => B}});
parse_args(["--initargs-path", Path | Rest], Map) ->
    parse_args(Rest, Map#{initargs_path => Path});
parse_args(["--regenerate-cookie" | Rest], Map) ->
    parse_args(Rest, Map#{regenerate_cookie => true});
parse_args(["--regenerate-cluster-uuid" | Rest], Map) ->
    parse_args(Rest, Map#{regenerate_cluster_uuid => true});
parse_args(["--regenerate-bucket-uuids" | Rest], Map) ->
    parse_args(Rest, Map#{regenerate_bucket_uuids => true});
parse_args([], Map) ->
    Map;
parse_args(Args, _Map) ->
    usage(Args).

maybe_derive_output_path(#{output_path := _Path} = Args) ->
    %% Output path specified, nothing to do. Generally applicable for tests or
    %% cluster_run usage.
    Args;
maybe_derive_output_path(#{?INITARGS_DATA_DIR := Path} = Args) ->
    %% Not specified, we will overwrite the given directory. Generally
    %% applicable when run against a normal installation.
    Args#{output_path => Path}.

load_initargs(#{initargs_path := Path} = Args) ->
    ?log_info("Loading initargs"),

    InitArgs = read_term_from_file(Path),

    ?log_debug("Loaded initargs ~p", [InitArgs]),
    NsServerProps = misc:expect_prop_value(?INITARGS_NS_SERVER_PROPS, InitArgs),
    BinDir = misc:expect_prop_value(?INITARGS_BIN_DIR, NsServerProps),
    LogDir = misc:expect_prop_value(?INITARGS_LOG_DIR, NsServerProps),
    DataDir = misc:expect_prop_value(?INITARGS_DATA_DIR, NsServerProps),

    %% Required for gosecrets/cb_gosecrets_runner which uses path_config
    application:set_env(ns_server, ?INITARGS_DATA_DIR, DataDir),
    application:set_env(ns_server, ?INITARGS_BIN_DIR, BinDir),

    Args#{?INITARGS_BIN_DIR => BinDir,
          ?INITARGS_LOG_DIR => LogDir,
          ?INITARGS_DATA_DIR => DataDir}.

generate_cookie(OldCookie, Args) ->
    %% We are salting the hash with the old cookie (which we know should be
    %% random) so that:
    %%    a) any other node being remapped can reproduce the hash
    %%    b) a malicious actor cannot derive the new cookie with decrypting the
    %%       old cookie
    binary_to_atom(
      %% integer_to_atom does not exist for some reason...
      integer_to_binary(
        erlang:crc32(erlang:term_to_binary({OldCookie, Args})))).

setup(Args) ->
    setup_stderr_logging(),
    ?log_info("Starting config_remap script with args ~p", [Args]),

    ArgsMap0 = parse_args(Args, default_args()),
    ?log_debug("Parsed args map ~p", [ArgsMap0]),

    case maps:is_key(node_map, ArgsMap0) of
        true -> ok;
        false ->
            ?log_error("Must pass remap args"),
            erlang:halt(1)
    end,

    maybe_tweak_log_verbosity(ArgsMap0),

    ArgsMap1 = load_initargs(ArgsMap0),

    setup_file_logging(ArgsMap1),

    ArgsMap2 = maybe_derive_output_path(ArgsMap1),
    ArgsMap3 = init_gosecrets(ArgsMap2),

    %% We may rewrite bucket uuids. We don't know which buckets we have til we
    %% iterate the config, without iterating the data dirs anyways, and we may
    %% have multiple keys to rewrite in the case of things like chronicle log.
    %% We should ensure that we use the same uuid everywhere, so we will store
    %% them after creating them. We will put them in an ETS table instead of
    %% in the Args map, this saves us from having to update and pass new Args
    %% maps in every function, instead we can just lookup in the table.
    case maps:get(regenerate_bucket_uuids, ArgsMap3) of
        false -> ok;
        true -> ets:new(bucket_uuids, [public, named_table])
    end,

    ?log_debug("Final args map ~p", [ArgsMap3]),
    ArgsMap3.

main(CmdLineArgs) ->
    Args = setup(CmdLineArgs),

    rewrite_ip_files(Args),
    rewrite_nodefile(Args),

    rewrite_initargs(Args),
    rewrite_ns_config(Args),

    rewrite_chronicle(Args),

    ale:sync_all_sinks().
