%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(ns_babysitter).

-behavior(application).

-export([start/2, stop/1]).

-include("ns_common.hrl").
-include_lib("ale/include/ale.hrl").

start(_, _) ->
    %% we're reading environment of ns_server application. Thus we
    %% need to load it.
    ok = application:load(ns_server),

    %% if erlang:send is done toward an unconnected node, the function will
    %% not return until the connection setup had completed (or failed).
    %% It leads to some processes get stuck for 7 seconds when they are trying
    %% to handle a call from ns_server during ns_server's rename
    %% This setting forbids auto connect back to ns_server. It guarantees that
    %% attempt to send message to not existing node will not block.
    %% (blocking send is fixed in erl21 so this hack can be removed)
    application:set_env(kernel, dist_auto_connect, never),

    setup_static_config(),
    init_logging(),

    %% To initialize logging static config must be setup thus this weird
    %% machinery is required to log messages from setup_static_config().
    self() ! done,
    log_pending(),

    {have_host, true} = {have_host, ('nonode@nohost' =/= node())},

    ok = dist_manager:configure_net_kernel(),

    Cookie =
        case erlang:get_cookie() of
            nocookie ->
                NewCookie = misc:generate_cookie(),
                erlang:set_cookie(node(), NewCookie),
                NewCookie;
            SomeCookie ->
                SomeCookie
        end,

    ?log_info("babysitter cookie: ~p~n",
              [ns_cookie_manager:sanitize_cookie(Cookie)]),
    write_required_file(cookiefile, Cookie, "babysitter cookie"),
    maybe_write_file(nodefile, node(), "babysitter node name"),

    make_pidfile(),

    % Clear the HTTP proxy environment variables as they are honored, when they
    % are set, by the golang net/http package.
    true = os:unsetenv("http_proxy"),
    true = os:unsetenv("https_proxy"),

    ns_babysitter_sup:start_link().

write_required_file(Env, Content, Name) ->
    case application:get_env(Env) of
        {ok, File} ->
            do_write_file(Content, File, Name);
        _ ->
            erlang:error("A required parameter for the ns_server "
                         "application is missing!")
    end.

maybe_write_file(Env, Content, Name) ->
    case application:get_env(Env) of
        {ok, File} ->
            do_write_file(Content, File, Name);
        _ ->
            ok
    end.

do_write_file(Content, File, Name) ->
    ok = filelib:ensure_dir(File),
    ok = misc:atomic_write_file(File, erlang:atom_to_list(Content) ++ "\n"),
    ?log_info("Saved ~s to ~s", [Name, File]).

log_pending() ->
    receive
        done ->
            ok;
        {LogLevel, Fmt, Args} ->
            ?ALE_LOG(LogLevel, Fmt, Args),
            log_pending()
    end.

get_config_path() ->
    case application:get_env(ns_server, config_path) of
        {ok, V} -> V;
        _ ->
            erlang:error("config_path parameter for ns_server application is missing!")
    end.

setup_static_config() ->
    Terms = case file:consult(get_config_path()) of
                {ok, T} when is_list(T) ->
                    T;
                _ ->
                    erlang:error("failed to read static config: " ++ get_config_path() ++ ". It must be readable file with list of pairs~n")
            end,
    self() ! {info, "Static config terms:~n~p", [Terms]},
    lists:foreach(fun ({K,V}) ->
                          case application:get_env(ns_server, K) of
                              undefined ->
                                  application:set_env(ns_server, K, V);
                              _ ->
                                  self() ! {warn,
                                            "not overriding parameter ~p, which is given from command line",
                                            [K]}
                          end
                  end, Terms).

init_logging() ->
    ale:with_configuration_batching(
      fun () ->
              do_init_logging()
      end),
    ale:info(?NS_SERVER_LOGGER, "Brought up babysitter logging").

do_init_logging() ->
    {ok, Dir} = application:get_env(ns_server, error_logger_mf_dir),

    ok = misc:mkdir_p(Dir),
    ok = convert_disk_log_files(Dir),

    ok = ns_server:start_disk_sink(babysitter_sink, ?BABYSITTER_LOG_FILENAME),

    ok = ale:start_logger(?NS_SERVER_LOGGER, debug),
    ok = ale:set_loglevel(?ERROR_LOGGER, debug),
    ok = logger:set_primary_config(level, info),

    ok = ale:add_sink(?NS_SERVER_LOGGER, babysitter_sink, debug),
    ok = ale:add_sink(?ERROR_LOGGER, babysitter_sink, debug),

    case misc:get_env_default(ns_server, dont_suppress_stderr_logger, false) of
        true ->
            ale:stop_sink(stderr),
            ok = ale:start_sink(stderr, ale_stderr_sink, []),

            lists:foreach(
              fun (Logger) ->
                      ok = ale:add_sink(Logger, stderr, debug)
              end, [?NS_SERVER_LOGGER, ?ERROR_LOGGER]);
        false ->
            ok
    end.

stop(_) ->
    ale:info(?NS_SERVER_LOGGER, "Received shutdown request. Terminating."),
    ale:sync_all_sinks(),
    delete_pidfile().

convert_disk_log_files(Dir) ->
    lists:foreach(
      fun (Log) ->
              ok = convert_disk_log_file(Dir, Log)
      end,
      [?DEFAULT_LOG_FILENAME,
       ?ERRORS_LOG_FILENAME,
       ?VIEWS_LOG_FILENAME,
       ?MAPREDUCE_ERRORS_LOG_FILENAME,
       ?COUCHDB_LOG_FILENAME,
       ?DEBUG_LOG_FILENAME,
       ?XDCR_TARGET_LOG_FILENAME,
       ?STATS_LOG_FILENAME,
       ?BABYSITTER_LOG_FILENAME,
       ?REPORTS_LOG_FILENAME,
       ?ACCESS_LOG_FILENAME]).

convert_disk_log_file(Dir, Name) ->
    [OldName, "log"] = string:tokens(Name, "."),

    IdxFile = filename:join(Dir, OldName ++ ".idx"),
    SizFile = filename:join(Dir, OldName ++ ".siz"),

    case filelib:is_regular(IdxFile) of
        true ->
            {Ix, NFiles} = read_disk_log_index_file(filename:join(Dir, OldName)),
            Ixs = lists:seq(Ix, 1, -1) ++ lists:seq(NFiles, Ix + 1, -1),

            lists:foreach(
              fun ({NewIx, OldIx}) ->
                      OldPath = filename:join(Dir,
                                              OldName ++
                                                  "." ++ integer_to_list(OldIx)),
                      NewSuffix = case NewIx of
                                      0 ->
                                          ".log";
                                      _ ->
                                          ".log." ++ integer_to_list(NewIx)
                                  end,
                      NewPath = filename:join(Dir, OldName ++ NewSuffix),

                      case file:rename(OldPath, NewPath) of
                          {error, enoent} ->
                              ok;
                          ok ->
                              ok
                      end,

                      file:delete(SizFile),
                      file:delete(IdxFile)
              end, misc:enumerate(Ixs, 0));
        false ->
            ok
    end.

read_disk_log_index_file(Path) ->
    {Ix, _, _, NFiles} = disk_log_1:read_index_file(Path),

    %% Index can be one greater than number of files. This means that maximum
    %% number of files is not yet reached.
    %%
    %% Pretty weird behavior: if we're writing to the first file out of 20
    %% read_index_file returns {1, _, _, 1}. But as we move to the second file
    %% the result becomes be {2, _, _, 1}.
    case Ix =:= NFiles + 1 of
        true ->
            {Ix, Ix};
        false ->
            {Ix, NFiles}
    end.

make_pidfile() ->
    case application:get_env(ns_babysitter, pidfile) of
        {ok, PidFile} -> make_pidfile(PidFile);
        X -> X
    end.

make_pidfile(PidFile) ->
    Pid = os:getpid(),
    %% Pid is a string representation of the process id, so we append
    %% a newline to the end.
    ok = misc:write_file(PidFile, list_to_binary(Pid ++ "\n")),
    ok.

delete_pidfile() ->
    case application:get_env(ns_babysitter, pidfile) of
        {ok, PidFile} -> delete_pidfile(PidFile);
        X -> X
    end.

delete_pidfile(PidFile) ->
    ok = file:delete(PidFile).
