%% @author Couchbase <info@couchbase.com>
%% @copyright 2019-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc - This module manages the recording, fetching and cleanup of
%% rebalance reports.
-module(ns_rebalance_report_manager).

-behaviour(gen_server2).

-include("ns_common.hrl").
-include_lib("kernel/include/file.hrl").

-define(NUM_REBALANCE_REPORTS, 5).
-define(FETCH_TIMEOUT, 10000).
-define(ENCR_OP_TIMEOUT, ?get_timeout(reb_report_encr_op, 2000)).
-define(MAX_DELAY, 10000).
-define(REPORT_NAME_PREFIX, "rebalance_report_").
-define(REPORT_PATTERN, ?REPORT_NAME_PREFIX ++ "[0-9]*.json").

%% APIs.
-export([start_link/0,
         get_rebalance_report/1,
         get_last_report_uuid/0,
         reencrypt_local_reports/1,
         get_in_use_deks/0,
         record_rebalance_report/2]).

%% gen_server2 callbacks.
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {report_dir}).

%% -------------------------------------------------------------
%% APIs.
%% -------------------------------------------------------------
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

get_last_report_uuid() ->
    case ns_config:read_key_fast(rebalance_reports, []) of
        [] -> undefined;
        [{UUID, _} | _] -> UUID
    end.

get_rebalance_report(ReportID) ->
    case ns_config:read_key_fast(rebalance_reports, []) of
        [] ->
            {error, enoent};
        [LastReport | _] = Reports ->
            case ReportID of
                undefined ->
                    gen_server2:call(?MODULE,
                                     {get_rebalance_report, LastReport, []},
                                     ?FETCH_TIMEOUT);
                _ ->
                    case lists:keyfind(list_to_binary(ReportID), 1, Reports) of
                        false -> {error, enoent};
                        Report ->
                            gen_server2:call(?MODULE,
                                             {get_rebalance_report, Report, []},
                                             ?FETCH_TIMEOUT)
                    end
            end
    end.

record_rebalance_report(Report, KeepNodes) ->
    case lists:member(node(), KeepNodes) of
        true ->
            gen_server2:call(?MODULE, {record_rebalance_report, Report},
                             infinity);
        false ->
            CompressedReport = zlib:compress(Report),
            [Node | _] = KeepNodes,
            gen_server2:call({?MODULE, Node},
                             {record_compressed_rebalance_report,
                              CompressedReport},
                             ?FETCH_TIMEOUT)
    end.

reencrypt_local_reports(LogDS) ->
    try
        gen_server2:call(?MODULE, {reencrypt_reports, LogDS}, ?ENCR_OP_TIMEOUT)
    catch
        exit:{noproc, {gen_server, call,
                       [?MODULE, {reencrypt_reports, _}, _]}} ->
            ?log_debug("Can't reencrypt reports: ~p is not "
                       "started yet...", [?MODULE]),
            {error, retry}
    end.

get_in_use_deks() ->
    try
        gen_server2:call(?MODULE, get_in_use_deks, ?ENCR_OP_TIMEOUT)
    catch
        exit:{noproc, {gen_server, call, [?MODULE, get_in_use_deks, _]}} ->
            ?log_debug("Can't get in use deks: ~p is not "
                       "started yet...", [?MODULE]),
            {error, retry}
    end.

%% -------------------------------------------------------------
%% gen_server2 callbacks.
%% -------------------------------------------------------------
init([]) ->
    {ok, LogDir} = application:get_env(ns_server, error_logger_mf_dir),
    Dir = filename:absname(filename:join(LogDir, "rebalance")),
    ok = misc:ensure_writable_dir(Dir),
    Self = self(),
    EventHandler = fun ({rebalance_reports, _ReqdReports}) ->
                           Self ! refresh;
                       (_) ->
                           ok
                   end,
    ns_pubsub:subscribe_link(ns_config_events, EventHandler),
    Self ! refresh,
    {ok, #state{report_dir = Dir}}.

handle_call({get_rebalance_report, Reqd, Options}, From,
            #state{report_dir = Dir} = State) ->
    Reports = ns_config:read_key_fast(rebalance_reports, []),
    case lists:member(Reqd, Reports) of
        false ->
            {reply, {error, enoent}, State};
        true ->
            gen_server2:async_job(
              fun () ->
                      fetch_rebalance_report(Reqd, Options, Dir)
              end,
              fun (RV, S) ->
                      gen_server2:reply(From, RV),
                      {noreply, S}
              end),
            {noreply, State}
    end;
handle_call({reencrypt_reports, LogDS},
            _From, #state{report_dir = Dir} = State) ->
    DirFiles = filelib:wildcard(?REPORT_PATTERN, Dir),
    EncrReportFn =
        fun(Path) ->
                Basename = filename:basename(Path),
                case fetch_rebalance_report_local(Basename, Dir) of
                    {ok, Report} ->
                        {Basename,
                         cb_crypto:atomic_write_file(Path, Report, LogDS)};
                    {error, _} = Error ->
                        {Basename, Error}
                end
        end,
    ReEncrReportFn =
        fun(Path) ->
                Basename = filename:basename(Path),
                case cb_crypto:is_file_encrypted(Path) of
                    true ->
                        {Basename,
                         cb_crypto:reencrypt_file(
                           Path, Path, LogDS,
                           #{allow_decrypt => true})};
                    false ->
                        EncrReportFn(Path)
                end
        end,
    Rvs =
        lists:map(
          fun(FileName) ->
                  Path = filename:join(Dir, FileName),
                  case cb_crypto:is_file_encr_by_active_key(Path, LogDS) of
                      true ->
                          {FileName, ok};
                      false ->
                          ReEncrReportFn(Path)
                  end
          end, DirFiles),

    ReqdReports = ns_config:read_key_fast(rebalance_reports, []),
    FilesInConfig = [proplists:get_value(filename, Info) ||
                     {_, Info} <- ReqdReports],

    Errors =
        lists:filtermap(
          fun({_FileName, ok}) ->
                  false;
              ({_FileName, {ok, _}}) ->
                  false;
             ({FileName, {error, E}}) ->
                 case lists:member(FileName, FilesInConfig) of
                     true ->
                         {true, {FileName, E}};
                     false ->
                         %% Since the file is not in ns_config, it is to be
                         %% removed anyway.
                         %% Typically this can happen if configuration is wiped
                         %% out completely manually by user (DEKs are removed
                         %% as well). In this case we will not be able to
                         %% reencrypt the file (no keys available), so just
                         %% ignore the error and wait until this file gets
                         %% removed in refresh.
                         ?log_warning("Ignoring reencryption error for ~p: ~p",
                                      [FileName, E]),
                         false
                 end
          end, Rvs),
    case Errors of
        [] ->
            {reply, ok, State};
        _ ->
            {reply, {error, Errors}, State}
    end;
handle_call(get_in_use_deks, _From, #state{report_dir = Dir} = State) ->
    DirFiles = filelib:wildcard(?REPORT_PATTERN, Dir),
    FilePaths = [filename:join(Dir, F)|| F <- DirFiles],
    {reply, {ok, cb_crypto:get_in_use_deks(FilePaths)}, State};
handle_call({record_compressed_rebalance_report, R}, From, State) ->
    Report = zlib:uncompress(R),
    handle_call({record_rebalance_report, Report}, From, State);
handle_call({record_rebalance_report, Report}, _From,
            #state{report_dir = Dir} = State) ->
    FileName = ?REPORT_NAME_PREFIX ++ misc:timestamp_utc_iso8601_basic()
        ++ ".json",
    NewReport = {couch_uuids:random(), [{node, node()}, {filename, FileName}]},
    AllReports = [NewReport | ns_config:read_key_fast(rebalance_reports, [])],
    Keep = lists:sublist(AllReports, get_num_rebalance_reports()),
    Path = filename:join(Dir, FileName),
    %% Rebalance reports are consumed by couchbase-fluent-bit, and
    %% adding a newline at the end of the report easies the parsing
    %% for them.
    {ok, DS} = cb_crypto:fetch_deks_snapshot(logDek),
    ok = cb_crypto:atomic_write_file(Path, [Report, $\n], DS),
    ns_config:set(rebalance_reports, Keep),
    {reply, ok, State};
handle_call(_, _, State) ->
    {reply, {error, enoent}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(refresh, #state{report_dir = Dir} = State) ->
    case ns_config_auth:is_system_provisioned() of
        true ->
            %% Only run refresh task if it a provisioned cluster.
            misc:flush(refresh),
            refresh(Dir);
        false ->
            ok
    end,
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------
%% Internal API's.
%% -------------------------------------------------------------

refresh(Dir) ->
    gen_server2:abort_queue(fetch_task),
    ReqdReports = ns_config:read_key_fast(rebalance_reports, []),
    {ok, ListFiles} = file:list_dir(Dir),
    Keep = [proplists:get_value(filename, Info) || {_, Info} <- ReqdReports],

    Delete = ListFiles -- Keep,
    [file:delete(filename:join(Dir, File)) || File <- Delete],

    Missing = Keep -- ListFiles,
    MissingReports = lists:filter(
                       fun ({_, Info}) ->
                          FN = proplists:get_value(filename, Info),
                          lists:member(FN, Missing)
                       end, ReqdReports),
    case MissingReports of
        [] ->
            ok;
        _ ->
            gen_server2:async_job(
              fetch_task,
              fun () ->
                      %% Delay fetch as we don't want a DDOS when
                      %% rebalance finishes.
                      %% Can be 0 - 10 seconds.
                      timer:sleep(rand:uniform(?MAX_DELAY)),
                      fetch_task(MissingReports, Dir)
              end,
              fun (_, S) -> {noreply, S} end)
    end.

%% If configured use that value. num_rebalance_reports is not set by ns_server
%% anywhere. Can only be set by explicit administrative action we provide.
get_num_rebalance_reports() ->
    ns_config:read_key_fast(num_rebalance_reports, ?NUM_REBALANCE_REPORTS).

fetch_task(MissingReports, Dir) ->
    ClusterNodes = ns_node_disco:nodes_wanted(),
    lists:foreach(
      fun ({_, Info} = Report) ->
              FN = proplists:get_value(filename, Info),
              Node = proplists:get_value(node, Info),
              case Node =/= node() andalso
                   lists:member(Node, ClusterNodes) andalso
                   not does_file_exist(Dir, FN) of
                  true ->
                      fetch_rebalance_report_remote(Report, Dir);
                  false ->
                      ok
              end
      end, MissingReports).

does_file_exist(Dir, FileName) ->
    Path = filename:join(Dir, FileName),
    does_file_exist(Path).

does_file_exist(Path) ->
    case file:read_file_info(Path) of
        {ok, Info} ->
            case Info#file_info.type of
                regular ->
                    true;
                _ ->
                    false
            end;
        _Error ->
            false
    end.

maybe_write_report(Report, Path) ->
    case does_file_exist(Path) of
        true ->
            %% Do nothing, someone already fetched it.
            ok;
        false ->
            {ok, DS} = cb_crypto:fetch_deks_snapshot(logDek),
            ok = cb_crypto:atomic_write_file(Path, Report, DS)
    end.

fetch_rebalance_report({_, Info} = ReqdReport, Options, Dir) ->
    FN = proplists:get_value(filename, Info),
    Node = proplists:get_value(node, Info),
    RV = case fetch_rebalance_report_local(FN, Dir) of
             {ok, Report} ->
                 {ok, Report};
             _ when node() =/= Node ->
                 fetch_rebalance_report_remote(ReqdReport, Dir);
             R ->
                 R
         end,
    maybe_compress(RV, Options).

fetch_rebalance_report_local(FileName, Dir) ->
    try
        Path = filename:join(Dir, FileName),
        case cb_crypto:is_file_encrypted(Path) of
            true ->
                {ok, LogDS} = cb_crypto:fetch_deks_snapshot(logDek),
                {decrypted, Decrypted} = cb_crypto:read_file(Path, LogDS),
                {ok, Decrypted};
            false ->
                misc:raw_read_file(Path)
        end
    catch
        T:E ->
            ?log_debug("Unexpected exception ~p", [{T, E}]),
            {error, unexpected_exception}
    end.

fetch_rebalance_report_remote({_, Info} = ReqdReport, Dir) ->
    Node = proplists:get_value(node, Info),
    FN = proplists:get_value(filename, Info),
    case gen_server2:call({?MODULE, Node},
                          {get_rebalance_report, ReqdReport, [{compress, true}]},
                          ?FETCH_TIMEOUT) of
        {ok, R} ->
            Report = zlib:uncompress(R),
            Path = filename:join(Dir, FN),
            maybe_write_report(Report, Path),
            {ok, Report};
        Err -> Err
    end.

maybe_compress({ok, Report} = Reply, Options) ->
    case proplists:get_bool(compress, Options) of
        true -> {ok, zlib:compress(Report)};
        false -> Reply
    end;
maybe_compress(Reply, _) ->
    Reply.
