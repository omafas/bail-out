%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(ale).

-behaviour(gen_server).

-export([start_link/0,
         start_sink/3, stop_sink/1,
         start_logger/1, start_logger/2, start_logger/3,
         stop_logger/1,
         add_sink/2, add_sink/3,
         set_loglevel/2, get_loglevel/1,
         set_sink_loglevel/3, get_sink_loglevel/2,
         sync_sink/1,
         sync_all_sinks/0,
         init_log_encryption_ds/1,
         get_sink_ds/1,
         set_global_log_deks_snapshot/1,
         get_global_log_deks_snapshot/0,
         set_log_deks_snapshot/1,
         get_all_used_deks/0,
         drop_log_deks/3,

         %% Callbacks for encryption
         create_no_deks_snapshot/0,
         file_encrypt_state_match/2,
         validate_encr_file_with_ds/2,
         is_file_encrypted/1,
         file_encrypt_init/1,
         file_encrypt_finish/1,
         file_encrypt_cont/3,
         file_encrypt_chunk/2,
         reencrypt_file/4,
         get_in_use_deks/1,

         with_configuration_batching/1,

         capture_logging_diagnostics/0,

         %% counterparts of pseudo-functions handled by ale_transform
         get_effective_loglevel/1, is_loglevel_enabled/2, sync/1,

         debug/2, debug/3, debug/4, xdebug/5, xdebug/6,
         info/2, info/3, info/4, xinfo/5, xinfo/6,
         warn/2, warn/3, warn/4, xwarn/5, xwarn/6,
         error/2, error/3, error/4, xerror/5, xerror/6,
         critical/2, critical/3, critical/4, xcritical/5, xcritical/6]).

%% logger callbacks.
-export([adding_handler/1, removing_handler/1, log/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ale.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {compile_frozen = false :: boolean(),
                sinks                  :: dict:dict(),
                loggers                :: dict:dict(),
                historical_deks        :: dict:dict(),
                drop_deks_ref          :: undefined | {pid(), reference()}}).

-record(logger, {name      :: atom(),
                 loglevel  :: loglevel(),
                 sinks     :: dict:dict(),
                 formatter :: module()}).

-record(sink, {name     :: atom(),
               loglevel :: loglevel()}).

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_sink(Name, Module, Args) ->
    gen_server:call(?MODULE, {start_sink, Name, Module:meta(), Module, Args}, infinity).

with_configuration_batching(Body) ->
    Old = freeze_compilations(),
    case Old of
        true ->
            Body();
        false ->
            try Body()
            after
                thaw_compilations()
            end
    end.

freeze_compilations() ->
    gen_server:call(?MODULE, freeze_compilations, infinity).

thaw_compilations() ->
    ok = gen_server:call(?MODULE, thaw_compilations, infinity).

stop_sink(Name) ->
    gen_server:call(?MODULE, {stop_sink, Name}).

start_logger(Name) ->
    start_logger(Name, ?DEFAULT_LOGLEVEL).

start_logger(Name, LogLevel) ->
    start_logger(Name, LogLevel, ?DEFAULT_FORMATTER).

start_logger(Name, LogLevel, Formatter) ->
    gen_server:call(?MODULE, {start_logger, Name, LogLevel, Formatter}).

stop_logger(Name) ->
    gen_server:call(?MODULE, {stop_logger, Name}).

add_sink(LoggerName, SinkName) ->
    add_sink(LoggerName, SinkName, debug).

add_sink(LoggerName, SinkName, LogLevel) ->
    gen_server:call(?MODULE, {add_sink, LoggerName, SinkName, LogLevel}).

set_loglevel(LoggerName, LogLevel) ->
    gen_server:call(?MODULE, {set_loglevel, LoggerName, LogLevel}).

get_loglevel(LoggerName) ->
    gen_server:call(?MODULE, {get_loglevel, LoggerName}).

set_sink_loglevel(LoggerName, SinkName, LogLevel) ->
    gen_server:call(?MODULE,
                    {set_sink_loglevel, LoggerName, SinkName, LogLevel}).

get_sink_loglevel(LoggerName, SinkName) ->
    gen_server:call(?MODULE, {get_sink_loglevel, LoggerName, SinkName}).

set_log_deks_snapshot(LogDS) ->
    gen_server:call(?MODULE, {set_log_deks_snapshot, LogDS}, infinity).

drop_log_deks(DekIdsToDrop, WorkSzThresh, Timeout) ->
    try
        gen_server:call(?MODULE,
                        {drop_log_deks, DekIdsToDrop, WorkSzThresh}, Timeout)
    catch
        exit:{timeout, _} ->
            {error, {node(), timeout}}
    end.

get_all_used_deks() ->
    gen_server:call(?MODULE, get_all_used_deks, infinity).

call_disk_sink(SinkName, Call, Timeout) ->
    try
        gen_server:call(ale_utils:sink_id(SinkName), Call, Timeout)
    catch
        exit:{noproc, _} ->
            {error, unknown_sink}
    end.

sync_sink(SinkName) ->
    call_disk_sink(SinkName, sync, infinity).

sync_all_sinks() ->
    Sinks = gen_server:call(?MODULE, get_sink_names, infinity),
    [sync_sink(SinkName) || SinkName <- Sinks],
    ok.

get_encryptable_sink_names(State) ->
    EncrSinks =
        dict:filter(
          fun(_SinkName, Metadata) ->
                  proplists:get_bool(encryption_supported, Metadata)
          end, State#state.sinks),
    dict:fetch_keys(EncrSinks).

get_historical_deks(CurrLiveEcrSinks, #state{historical_deks = HistDeks}) ->
    HistSinks = dict:fetch_keys(HistDeks),
    lists:flatmap(
      fun(SinkName) ->
              {ok, Deks} = dict:find(SinkName, HistDeks),
              Deks
      end, HistSinks -- CurrLiveEcrSinks).

do_set_log_deks_snapshot(LogsDS, State) ->
    set_global_log_deks_snapshot(LogsDS),
    EncryptableSinks = get_encryptable_sink_names(State),
    RVs =
        lists:map(
          fun(SinkName) ->
                  {SinkName, call_disk_sink(SinkName,
                                            notify_active_key_updt, infinity)}
          end, EncryptableSinks),

    Failures = [Result || {_Sink, R} = Result <- RVs, R =/= ok],
    case Failures of
        [] ->
            ok;
        _ ->
            {error, Failures}
    end.

try_drop_deks_work(Work, From, #state{drop_deks_ref = undefined} = State) ->
    {Pid, Ref} =
        proc_lib:spawn_opt(
            fun() ->
                try Work() of
                    Res ->
                        gen_server:reply(From, Res)
                    catch
                        C:E:ST ->
                            ale:error(?ALE_LOGGER, "DropDeks process failed",
                                      [C,E,ST]),
                            gen_server:reply(From, {error, {C, E}})
                end
            end, [link, monitor]),
    State#state{drop_deks_ref = {Pid, Ref}};
try_drop_deks_work(_Work, From, State) ->
    gen_server:reply(From, {error, retry}),
    State.

do_drop_log_deks(DekIdsToDrop, WorkSzThresh, From, State) ->
    EncryptableSinks = get_encryptable_sink_names(State),

    CallDropSink =
        fun(SinkName) ->
            DropArgs = {drop_log_deks, DekIdsToDrop, WorkSzThresh},
            case call_disk_sink(SinkName, DropArgs, infinity) of
                {error, unknown_sink} = E ->
                    {E, 0};
                {_Rv, _Count} = Res ->
                    Res
            end
        end,

    DropDek =
        fun DoDrop(SinkName, _, _, start) ->
                {Rv, FilesLeftCount} = CallDropSink(SinkName),
                DoDrop(SinkName, FilesLeftCount + 1, FilesLeftCount, Rv);
            DoDrop(_SinkName, _PrevCount, _Count, {error, _} = Result) ->
                Result;
            DoDrop(_SinkName, _PrevCount, 0, ok) ->
                ok;
            DoDrop(SinkName, PrevCount, Count, ok) when Count < PrevCount ->
                %% Note that a caller should never attempt to drop an active
                %% DEK, and if there are files that are using the
                %% a DEK in DekIdsToDrop, each iteration at least one file
                %% will get re-encrypted with the active DEK and the NewCount
                %% will be lower than the PrevCount after the call to drop.
                %% If an attempt to drop an active DEK takes place, the
                %% subsequent case takes care of it by giving error
                {Rv, NewCount} = CallDropSink(SinkName),
                DoDrop(SinkName, Count, NewCount, Rv);
            DoDrop(_SinkName, _PrevCount, _Count, _Result) ->
                {error, drop_dek_failed}
        end,

    Work =
        fun() ->
                RVs =
                    lists:map(
                      fun(SinkName) ->
                              {SinkName, DropDek(SinkName, 0, 0, start)}
                      end, EncryptableSinks),

                Errors = [Result || {_Sink, R} = Result <- RVs, R =/= ok],
                case Errors of
                    [] ->
                        ok;
                    _ ->
                        {error, Errors}
                end
        end,

    try_drop_deks_work(Work, From, State).

do_get_all_used_deks(State) ->
    EncryptableSinks = get_encryptable_sink_names(State),
    InUseDeks =
        lists:flatmap(
          fun(SinkName) ->
                  {ok, InUseDeks} =
                      call_disk_sink(SinkName, get_in_use_deks, infinity),
                  InUseDeks
          end, EncryptableSinks),

    AddnlInUseDeks = get_historical_deks(EncryptableSinks, State),

    {ok, lists:usort(InUseDeks ++ AddnlInUseDeks)}.

init_log_encryption_ds(LogDS) ->
    set_global_log_deks_snapshot(LogDS).

get_sink_ds(_SinkId) ->
    get_global_log_deks_snapshot().

set_global_log_deks_snapshot(LogsDs) ->
    persistent_term:put(log_deks_snapshot, LogsDs).

get_global_log_deks_snapshot() ->
    persistent_term:get(log_deks_snapshot, undefined).

get_encryption_cb(CbType) ->
    CBs = persistent_term:get(ale_encryption_callbacks, undefined),
    #{CbType := Callback} = CBs,
    Callback.

create_no_deks_snapshot() ->
    Callback = get_encryption_cb(create_no_deks_snapshot),
    Callback().

file_encrypt_state_match(DS, EncrState) ->
    Callback = get_encryption_cb(file_encrypt_state_match),
    Callback(DS, EncrState).

validate_encr_file_with_ds(Path, DS) ->
    Callback = get_encryption_cb(validate_encr_file_with_ds),
    Callback(Path, DS).

is_file_encrypted(Path) ->
    Callback = get_encryption_cb(is_file_encrypted),
    Callback(Path).

file_encrypt_init(DS) ->
    Callback = get_encryption_cb(file_encrypt_init),
    Callback(DS).

file_encrypt_finish(EncrState) ->
    Callback = get_encryption_cb(file_encrypt_finish),
    Callback(EncrState).

file_encrypt_cont(Path, Offset, DS) ->
    Callback = get_encryption_cb(file_encrypt_cont),
    Callback(Path, Offset, DS).

file_encrypt_chunk(Data, EncrState) ->
    Callback = get_encryption_cb(file_encrypt_chunk),
    Callback(Data, EncrState).

reencrypt_file(FromPath, ToPath, DS, Opts) ->
    Callback = get_encryption_cb(reencrypt_file),
    Callback(FromPath, ToPath, DS, Opts).

get_in_use_deks(FilePaths) ->
    Callback = get_encryption_cb(get_in_use_deks),
    Callback(FilePaths).

get_effective_loglevel(LoggerName) ->
    call_logger_impl(LoggerName, get_effective_loglevel, []).

is_loglevel_enabled(LoggerName, LogLevel) ->
    call_logger_impl(LoggerName, is_loglevel_enabled, [LogLevel]).


debug(LoggerName, Msg) ->
    xdebug(LoggerName, undefined, Msg, []).

debug(LoggerName, Fmt, Args) ->
    xdebug(LoggerName, undefined, Fmt, Args).

debug(LoggerName, Fmt, Args, Opts) ->
    xdebug(LoggerName, undefined, Fmt, Args, Opts).

xdebug(LoggerName, Data, Fmt, Args) ->
    xdebug(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, []).

xdebug(LoggerName, {M, F, L}, Data, Fmt, Args) ->
    xdebug(LoggerName, {M, F, L}, Data, Fmt, Args, []);
xdebug(LoggerName, Data, Fmt, Args, Opts) ->
    xdebug(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, Opts).

-spec xdebug(atom(), {module(), atom(), integer()},
             term(), io:format(), [term()], Options) -> term() when
      Options :: [Option],
      Option :: {'chars_limit', integer()}.
xdebug(LoggerName, {M, F, L}, Data, Fmt, Args, Opts) ->
    call_logger_impl(LoggerName, xdebug, [M, F, L, Data, Fmt, Args, Opts]).


info(LoggerName, Msg) ->
    xinfo(LoggerName, undefined, Msg, []).

info(LoggerName, Fmt, Args) ->
    xinfo(LoggerName, undefined, Fmt, Args).

info(LoggerName, Fmt, Args, Opts) ->
    xinfo(LoggerName, undefined, Fmt, Args, Opts).

xinfo(LoggerName, Data, Fmt, Args) ->
    xinfo(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, []).

xinfo(LoggerName, {M, F, L}, Data, Fmt, Args) ->
    xinfo(LoggerName, {M, F, L}, Data, Fmt, Args, []);
xinfo(LoggerName, Data, Fmt, Args, Opts) ->
    xinfo(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, Opts).

-spec xinfo(atom(), {module(), atom(), integer()},
            term(), io:format(), [term()], Options) -> term() when
      Options :: [Option],
      Option :: {'chars_limit', integer()}.
xinfo(LoggerName, {M, F, L}, Data, Fmt, Args, Opts) ->
    call_logger_impl(LoggerName, xinfo, [M, F, L, Data, Fmt, Args, Opts]).


warn(LoggerName, Msg) ->
    xwarn(LoggerName, undefined, Msg, []).

warn(LoggerName, Fmt, Args) ->
    xwarn(LoggerName, undefined, Fmt, Args).

warn(LoggerName, Fmt, Args, Opts) ->
    xwarn(LoggerName, undefined, Fmt, Args, Opts).

xwarn(LoggerName, Data, Fmt, Args) ->
    xwarn(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, []).

xwarn(LoggerName, {M, F, L}, Data, Fmt, Args) ->
    xwarn(LoggerName, {M, F, L}, Data, Fmt, Args, []);
xwarn(LoggerName, Data, Fmt, Args, Opts) ->
    xwarn(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, Opts).

-spec xwarn(atom(), {module(), atom(), integer()},
            term(), io:format(), [term()], Options) -> term() when
      Options :: [Option],
      Option :: {'chars_limit', integer()}.
xwarn(LoggerName, {M, F, L}, Data, Fmt, Args, Opts) ->
    call_logger_impl(LoggerName, xwarn, [M, F, L, Data, Fmt, Args, Opts]).


error(LoggerName, Msg) ->
    xerror(LoggerName, undefined, Msg, []).

error(LoggerName, Fmt, Args) ->
    xerror(LoggerName, undefined, Fmt, Args).

error(LoggerName, Fmt, Args, Opts) ->
    xerror(LoggerName, undefined, Fmt, Args, Opts).

xerror(LoggerName, Data, Fmt, Args) ->
    xerror(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, []).

xerror(LoggerName, {M, F, L}, Data, Fmt, Args) ->
    xerror(LoggerName, {M, F, L}, Data, Fmt, Args, []);
xerror(LoggerName, Data, Fmt, Args, Opts) ->
    xerror(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, Opts).

-spec xerror(atom(), {module(), atom(), integer()},
             term(), io:format(), [term()], Options) -> term() when
      Options :: [Option],
      Option :: {'chars_limit', integer()}.
xerror(LoggerName, {M, F, L}, Data, Fmt, Args, Opts) ->
    call_logger_impl(LoggerName, xerror, [M, F, L, Data, Fmt, Args, Opts]).


critical(LoggerName, Msg) ->
    xcritical(LoggerName, undefined, Msg, []).

critical(LoggerName, Fmt, Args) ->
    xcritical(LoggerName, undefined, Fmt, Args).

critical(LoggerName, Fmt, Args, Opts) ->
    xcritical(LoggerName, undefined, Fmt, Args, Opts).

xcritical(LoggerName, Data, Fmt, Args) ->
    xcritical(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, []).

xcritical(LoggerName, {M, F, L}, Data, Fmt, Args) ->
    xcritical(LoggerName, {M, F, L}, Data, Fmt, Args, []);
xcritical(LoggerName, Data, Fmt, Args, Opts) ->
    xcritical(LoggerName, {unknown, unknown, -1}, Data, Fmt, Args, Opts).

-spec xcritical(atom(), {module(), atom(), integer()},
                term(), io:format(), [term()], Options) -> term() when
      Options :: [Option],
      Option :: {'chars_limit', integer()}.
xcritical(LoggerName, {M, F, L}, Data, Fmt, Args, Opts) ->
    call_logger_impl(LoggerName, xcritical, [M, F, L, Data, Fmt, Args, Opts]).

sync(LoggerName) ->
    call_logger_impl(LoggerName, sync, []).

capture_logging_diagnostics() ->
    #state{sinks = Sinks, loggers = Loggers} = gen_server:call(?MODULE, get_state),
    LoggersD = [{N,
                 [{loglevel, L},
                  {formatter, F},
                  {sinks, [{SN, SL}
                           || {_, #sink{name = SN, loglevel = SL}} <- dict:to_list(LSinks)]}]}
                || {_, #logger{name = N,
                               loglevel = L,
                               sinks = LSinks,
                               formatter = F}} <- dict:to_list(Loggers)],
    [{sinks, dict:to_list(Sinks)},
     {loggers, LoggersD}].

%%%-----------------------------------------------------------------
%%% Callbacks for logger
%%%-----------------------------------------------------------------
-spec adding_handler(logger:handler_config()) ->
    {ok, logger:handler_config()} | {error, term()}.
adding_handler(Config) ->
    {ok, Config}.

-spec removing_handler(logger:handler_config()) -> ok.
removing_handler(#{id:=Logger}) ->
    gen_server:cast(?MODULE, {removing_handler, Logger}),
    ok.

-spec log(logger:log_event(), logger:handler_config()) -> ok.
log(#{level:=Level, msg:=Msg, meta:=Meta}, #{id:=Logger}) ->
    ale_error_logger_handler:log(Logger, Level, Msg, Meta).
%%%-----------------------------------------------------------------
%%% End: Callbacks for logger
%%%-----------------------------------------------------------------

%%%-----------------------------------------------------------------
%%% Callbacks for gen_server
%%%-----------------------------------------------------------------
init([]) ->
    State = #state{sinks=dict:new(),
                   loggers=dict:new(),
                   historical_deks=dict:new(),
                   drop_deks_ref=undefined},

    {ok, State1} = do_start_logger(?ERROR_LOGGER, ?DEFAULT_LOGLEVEL,
                                   ?DEFAULT_FORMATTER, State),
    {ok, State2} = do_start_logger(?ALE_LOGGER, ?DEFAULT_LOGLEVEL,
                                   ?DEFAULT_FORMATTER, State1),
    {ok, State3} = do_start_logger(?TRACE_LOGGER,
                                   ?DEFAULT_LOGLEVEL,
                                   ?DEFAULT_FORMATTER, State2),

    lists:foreach(fun (Logger) ->
                          _ = logger:remove_handler(Logger)
                  end, [?ERROR_LOGGER, ?TRACE_LOGGER]),

    %% Erlang starts this for us when we disable default handler.
    _ = logger:remove_handler(simple),

    case application:get_env(ale, encryption_callbacks) of
        {ok, EncrCBs} ->
            persistent_term:put(ale_encryption_callbacks,
                                EncrCBs);
        _ ->
            persistent_term:put(ale_encryption_callbacks,
                                default_encr_disabled_cbs())
    end,

    ok = set_error_logger_handler(),
    ok = set_noisy_progress_reports_handler(),
    {ok, State3}.

default_encr_disabled_cbs() ->
    #{create_no_deks_snapshot =>
          fun() ->
                  #{}
          end,
      file_encrypt_state_match =>
          fun(_DS, _EncrState) ->
                  true
          end,
      validate_encr_file_with_ds =>
          fun(_Path, _DS) ->
                  true
          end,
      is_file_encrypted =>
          fun(_Path) ->
                  false
          end,
      file_encrypt_init =>
          fun(_DS) ->
                  {ok, {<<>>, #{}}}
          end,
      file_encrypt_finish =>
          fun(_EncrState) ->
                  <<>>
          end,
      file_encrypt_cont =>
          fun(_Path, _Offset, _DS) ->
                  {ok, #{}}
          end,
      file_encrypt_chunk =>
          fun(Data, EncrState) ->
                  {Data, EncrState}
          end,
      reencrypt_file =>
          fun(_FromPath, _ToPath, _DS, _Opts) ->
                  ok
          end,
      get_in_use_deks =>
          fun(_FilePaths) ->
                  []
          end}.

handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call({start_sink, Name, SinkMeta, Module, Args}, _From, State) ->
    RV = do_start_sink(Name, SinkMeta, Module, Args, State),
    handle_result(RV, State);

handle_call({stop_sink, Name}, _From, State) ->
    RV = do_stop_sink(Name, State),
    handle_result(RV, State);

handle_call({start_logger, Name, LogLevel, Formatter}, _From, State) ->
    RV = do_start_logger(Name, LogLevel, Formatter, State),
    handle_result(RV, State);

handle_call({stop_logger, Name}, _From, State) ->
    RV = do_stop_logger(Name, State),
    handle_result(RV, State);

handle_call({add_sink, LoggerName, SinkName, LogLevel},
            _From, State) ->
    RV = do_add_sink(LoggerName, SinkName, LogLevel, State),
    handle_result(RV, State);

handle_call({set_loglevel, LoggerName, LogLevel}, _From, State) ->
    RV = do_set_loglevel(LoggerName, LogLevel, State),
    handle_result(RV, State);

handle_call({get_loglevel, LoggerName}, _From, State) ->
    RV = do_get_loglevel(LoggerName, State),
    handle_result(RV, State);

handle_call({set_sink_loglevel, LoggerName, SinkName, LogLevel},
            _From, State) ->
    RV = do_set_sink_loglevel(LoggerName, SinkName, LogLevel, State),
    handle_result(RV, State);

handle_call({get_sink_loglevel, LoggerName, SinkName}, _From, State) ->
    RV = do_get_sink_loglevel(LoggerName, SinkName, State),
    handle_result(RV, State);

handle_call(get_sink_names, _From, State) ->
    {reply, dict:fetch_keys(State#state.sinks), State};

handle_call({set_log_deks_snapshot, LogDS}, _From, State) ->
    {reply, do_set_log_deks_snapshot(LogDS, State), State};

handle_call({drop_log_deks, DekIdsToDrop, WorkSzThresh}, From, State) ->
    NewState = do_drop_log_deks(DekIdsToDrop, WorkSzThresh, From, State),
    {noreply, NewState};

handle_call(get_all_used_deks, _From, State) ->
    {reply, do_get_all_used_deks(State), State};

handle_call(freeze_compilations, _From, State) ->
    {reply, State#state.compile_frozen, State#state{compile_frozen = true}};

handle_call(thaw_compilations, _From, State) ->
    case State#state.compile_frozen of
        false ->
            {reply, ok, State};
        true ->
            [just_compile_logger(State, Logger)
             || {_, Logger} <- dict:to_list(State#state.loggers)],
            {reply, ok, State#state{compile_frozen = false}}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({removing_handler, Logger}, State) ->
    ale:error(?ALE_LOGGER, "~p has been removed. Setting it up again.",
              [Logger]),
    case Logger of
        ?ERROR_LOGGER ->
            ok = set_error_logger_handler();
        ?TRACE_LOGGER ->
            ok = set_noisy_progress_reports_handler()
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _Reason},
            #state{drop_deks_ref = {Pid, Ref}} = State) ->
    {noreply, State#state{drop_deks_ref = undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%%%-----------------------------------------------------------------
%%% End: Callbacks for gen_server
%%%-----------------------------------------------------------------

ensure_sink(SinkName, #state{sinks=Sinks} = _State, Fn) ->
    case dict:find(SinkName, Sinks) of
        {ok, _} ->
            Fn();
        error ->
            {error, unknown_sink}
    end.

ensure_sink_with_meta(SinkName, #state{sinks=Sinks} = _State, Fn) ->
    case dict:find(SinkName, Sinks) of
        {ok, Meta} ->
            Fn(Meta);
        error ->
            {error, unknown_sink}
    end.

ensure_logger(LoggerName, #state{loggers=Loggers} = _State, Fn) ->
    case dict:find(LoggerName, Loggers) of
        {ok, Logger} ->
            Fn(Logger);
        error ->
            {error, unknown_logger}
    end.

handle_result(Result, OldState) ->
    case Result of
        {ok, NewState} ->
            {reply, ok, NewState};
        {{ok, RV}, NewState} ->
            {reply, {ok, RV}, NewState};
        _Other ->
            {reply, Result, OldState}
    end.

do_start_sink(Name, SinkMeta, Module, Args,
              #state{sinks=Sinks,
                     historical_deks = HistoricalDeks} = State) ->
    case dict:find(Name, Sinks) of
        {ok, _} ->
            {error, duplicate_sink};
        error ->
            SinkId = ale_utils:sink_id(Name),
            Args1 = [SinkId | Args],

            RV = ale_dynamic_sup:start_child(SinkId, Module, Args1),
            case RV of
                {ok, _} ->
                    NewSinks = dict:store(Name, SinkMeta, Sinks),
                    UpdtHistDeks = dict:erase(Name, HistoricalDeks),
                    NewState =
                        State#state{sinks=NewSinks,
                                    historical_deks = UpdtHistDeks},
                    {ok, NewState};
                _Other ->
                    RV
            end
    end.

maybe_add_historical_deks(_IsEncryptable = true, SinkName,
                          #state{historical_deks = OldHistory} = State) ->
    {ok, InUseDeks} = call_disk_sink(SinkName, get_in_use_deks, infinity),
    NewHistory = dict:store(SinkName, InUseDeks, OldHistory),
    State#state{historical_deks = NewHistory};
maybe_add_historical_deks(_IsEncryptable = false, _SinkName, State) ->
    State.

do_stop_sink(Name, #state{sinks=Sinks} = State0) ->
    ensure_sink_with_meta(
      Name, State0,
      fun (Meta) ->
              SinkId = ale_utils:sink_id(Name),
              IsEncryptable = proplists:get_bool(encryption_supported, Meta),
              State1 = maybe_add_historical_deks(IsEncryptable, Name, State0),
              ok = ale_dynamic_sup:stop_child(SinkId),
              NewSinks = dict:erase(Name, Sinks),
              NewState = State1#state{sinks=NewSinks},
              {ok, NewState}
      end).

do_start_logger(Name, LogLevel, Formatter, State) ->
    case is_valid_loglevel(LogLevel) of
        true ->
            do_start_logger_tail(Name, LogLevel, Formatter, State);
        false ->
            {error, badarg}
    end.

do_start_logger_tail(Name, LogLevel, Formatter,
                     #state{loggers=Loggers} = State) ->
    case dict:find(Name, Loggers) of
        {ok, _Logger} ->
            {error, duplicate_logger};
        error ->
            Logger = #logger{name=Name,
                             loglevel=LogLevel,
                             sinks=dict:new(),
                             formatter=Formatter},

            {ok, compile(State, Logger)}
    end.

do_stop_logger(Name, #state{loggers=Loggers} = State) ->
    ensure_logger(
      Name, State,
      fun (_Logger) ->
              NewLoggers = dict:erase(Name, Loggers),
              State1 = State#state{loggers=NewLoggers},
              {ok, State1}
      end).

do_add_sink(LoggerName, SinkName, LogLevel, State) ->
    case is_valid_loglevel(LogLevel) of
        true ->
            do_add_sink_tail(LoggerName, SinkName, LogLevel, State);
        false ->
            {error, badarg}
    end.

do_add_sink_tail(LoggerName, SinkName, LogLevel, State) ->
    ensure_logger(
      LoggerName, State,
      fun (#logger{sinks=Sinks} = Logger) ->
              ensure_sink(
                SinkName, State,
                fun () ->
                        Sink = #sink{name=SinkName,
                                     loglevel=LogLevel},

                        NewSinks = dict:store(SinkName, Sink, Sinks),
                        NewLogger = Logger#logger{sinks=NewSinks},
                        NewState = compile(State, NewLogger),

                        {ok, NewState}
                end)
      end).

do_set_loglevel(LoggerName, LogLevel, State) ->
    case is_valid_loglevel(LogLevel) of
        true ->
            do_set_loglevel_tail(LoggerName, LogLevel, State);
        false ->
            {error, badarg}
    end.

do_set_loglevel_tail(LoggerName, LogLevel, State) ->
    ensure_logger(
      LoggerName, State,
      fun (#logger{loglevel=CurrentLogLevel} = Logger) ->
              case LogLevel of
                  CurrentLogLevel ->
                      {ok, State};
                  _ ->
                      NewLogger = Logger#logger{loglevel=LogLevel},
                      NewState = compile(State, NewLogger),

                      {ok, NewState}
              end
      end).

do_get_loglevel(LoggerName, State) ->
    ensure_logger(
      LoggerName, State,
      fun (#logger{loglevel=LogLevel}) ->
              {{ok, LogLevel}, State}
      end).

do_set_sink_loglevel(LoggerName, SinkName, LogLevel, State) ->
    case is_valid_loglevel(LogLevel) of
        true ->
            do_set_sink_loglevel_tail(LoggerName, SinkName, LogLevel, State);
        false ->
            {error, badarg}
    end.

do_set_sink_loglevel_tail(LoggerName, SinkName, LogLevel, State) ->
    ensure_logger(
      LoggerName, State,
      fun (#logger{sinks=Sinks} = Logger) ->
              ensure_sink(
                SinkName, State,
                fun () ->
                        case dict:find(SinkName, Sinks) of
                            {ok, #sink{loglevel=LogLevel}} ->   % bound above
                                {ok, State};
                            {ok, Sink} ->
                                NewSink = Sink#sink{loglevel=LogLevel},

                                NewSinks = dict:store(SinkName, NewSink, Sinks),
                                NewLogger = Logger#logger{sinks=NewSinks},
                                NewState = compile(State, NewLogger),
                                {ok, NewState};
                            error ->
                                {error, bad_sink}
                        end
                end)
      end).

do_get_sink_loglevel(LoggerName, SinkName, State) ->
    ensure_logger(
      LoggerName, State,
      fun (#logger{sinks=Sinks}) ->
              ensure_sink(
                SinkName, State,
                fun () ->
                        case dict:find(SinkName, Sinks) of
                            {ok, #sink{loglevel=LogLevel}} ->
                                LogLevel;
                            error ->
                                {error, bad_sink}
                        end
                end)
      end).

noisy_supervisors() ->
    [tls_dyn_connection_sup].

perform_action(log, Log) ->
    Log;
perform_action(stop, _Log) ->
    stop.

%% sup_names could be local names: {local, Name} or global names: {global, Name}
%% or of the type used here in supervisor_bridge code:
%%
%% https://github.com/erlang/otp/blob/b5e045c8c3bbeb6ad6f66e38cf7140c045890d00/
%% lib/stdlib/src/supervisor_bridge.erl#L99

get_sup_name({Pid, Name}) when is_pid(Pid) ->
    Name;
get_sup_name(Name) ->
    Name.

%% Supervisor progress report gets generated from here:
%%
%% https://github.com/erlang/otp/blob/b5e045c8c3bbeb6ad6f66e38cf7140c045890d00/
%% lib/stdlib/src/supervisor_bridge.erl#L90

noisy_progress_reports_inner(
  #{msg := {report, #{label := {supervisor, progress}} = Report}} = Log,
  Action) ->

    #{report := ReportProps} = Report,
    ProcName = get_sup_name(proplists:get_value(supervisor, ReportProps)),

    case lists:member(ProcName, noisy_supervisors()) of
        true ->
            perform_action(Action, Log);
        false ->
            ignore
    end;
noisy_progress_reports_inner(_Log, _Action) ->
    ignore.

-spec noisy_progress_reports(Log :: logger:log_event(), Action :: log | stop) ->
          logger:filter_return().
noisy_progress_reports(Log, Action) when Action =:= log; Action =:= stop ->
    noisy_progress_reports_inner(Log, Action);
noisy_progress_reports(Log, Action) ->
    erlang:error(badarg, {Log, Action}).

set_error_logger_handler() ->
    logger:add_handler(
      ?ERROR_LOGGER, ?MODULE,
      #{level => info,
        filter_default => log,
        filters => [{remote_gl, {fun logger_filters:remote_gl/2, stop}},
                    {stop_noisy_progress_reports,
                     {fun noisy_progress_reports/2, stop}}]}).

set_noisy_progress_reports_handler() ->
    logger:add_handler(
      ?TRACE_LOGGER, ?MODULE,
      #{level => info,
        filter_default => stop,
        filters => [{log_noisy_progress_reports,
                     {fun noisy_progress_reports/2, log}}]}).

compile(#state{compile_frozen = Frozen,
               loggers=Loggers} = State,
        #logger{name=LoggerName} = Logger) ->

    case Frozen of
        false ->
            just_compile_logger(State, Logger);
        _ ->
            ok
    end,

    NewLoggers = dict:store(LoggerName, Logger, Loggers),
    State#state{loggers=NewLoggers}.

just_compile_logger(#state{sinks=SinkMetas} = _State,
                    #logger{name=LoggerName,
                            loglevel=LogLevel,
                            formatter=Formatter,
                            sinks=Sinks} = _Logger) ->
    SinksList =
        dict:fold(
          fun (SinkName,
               #sink{name=SinkName, loglevel=SinkLogLevel},
               Acc) ->
                  SinkId = ale_utils:sink_id(SinkName),
                  {ok, SinkMeta} = dict:find(SinkName, SinkMetas),
                  [{SinkName, SinkId, SinkLogLevel, SinkMeta} | Acc]
          end, [], Sinks),

    ok = ale_codegen:load_logger(LoggerName, LogLevel, Formatter, SinksList).

is_valid_loglevel(LogLevel) ->
    lists:member(LogLevel, ?LOGLEVELS).

-compile({inline, [call_logger_impl/3]}).
call_logger_impl(LoggerName, F, Args) ->
    Module = ale_codegen:logger_impl(LoggerName),
    try
        erlang:apply(Module, F, Args)
    catch
        error:undef ->
            throw(unknown_logger)
    end.

-ifdef(TEST).
generate_log({report, {ReportType, ProcType, ProcName}}) ->
    ProcName1 =
        case is_atom(ProcName) of
            true ->
                %% Generate a dummy pid.
                {erlang:list_to_pid("<0.255.0>"), ProcName};
            false ->
                ProcName
        end,
    #{msg => {report, #{label => {ProcType, ReportType},
                        report => [{ProcType, ProcName1}]}}};
generate_log({string, String}) ->
    #{msg => {string, String}}.

noisy_progress_reports_test__({Log, Action, Result}) ->
    ?assertEqual(Result, noisy_progress_reports(Log, Action)).

noisy_progress_reports_test_() ->
    TestArgs = [%% ignore reports.
                {generate_log(
                   {report, {ReportType, ProcType, ProcName}}), Action, ignore}
                || ReportType <- [progress, not_progress],
                   ProcType <- [supervisor, not_supervisor],
                   ProcName <- [alice, {local, bob}, {global, charlie}],
                   Action <- [log, stop]] ++
                %% Perform correct action on a report.
                [begin
                     Log = generate_log(
                             {report,
                              {progress, supervisor, tls_dyn_connection_sup}}),
                     case Action of
                         stop ->
                             {Log, stop, stop};
                         log ->
                             {Log, log, Log}
                     end
                 end || Action <- [log, stop]] ++
                %% ignore non-reports.
                [{generate_log({string, <<"There once was a ship ...">>}),
                  Action, ignore} || Action <- [log, stop]] ++
                [{garbage, log, ignore},
                 {garbage, stop, ignore}],

    Test = fun ({Log, Action, _Result} = Args) ->
                   {lists:flatten(
                      io_lib:format("Log: ~p, Action: ~p", [Log, Action])),
                    fun () ->
                            noisy_progress_reports_test__(Args)
                    end}
           end,

    {foreach,
     fun() -> ok end,
     [Test(TestArg) || TestArg <- TestArgs]}.

noisy_progress_reports_invalid_action_test() ->
    ?assertException(
       error, badarg,
       noisy_progress_reports(
         generate_log({report, {progress, foo, bar}}), invalid_action)).
-endif.
