%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2014 Couchbase, Inc.
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

-module(ale_error_logger_handler).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2,
         terminate/2, code_change/3]).

-compile({parse_transform, ale_transform}).

-define(MAILBOX_LIMIT, 5000).
-define(MAILBOX_RECOVER_LIMIT, 500).

-record(state, {logger :: atom(),
                logging_state :: log | drop}).

init([Logger]) ->
    State = #state{logger=Logger,
                   logging_state=log},
    {ok, State, hibernate}.

handle_event({_Type, GLeader, _Msg}, State) when node(GLeader) =/= node() ->
    {ok, State, hibernate};

handle_event({Type, _GLeader, Report},
             #state{logger=Logger} = State) when Type =:= info_report;
                                                 Type =:= warning_report;
                                                 Type =:= error_report ->
    case action(State) of
        {log, NewState} ->
            log_report(Type, Logger, Report),
            {ok, NewState, hibernate};
        {drop, NewState} ->
            {ok, NewState}
    end;

handle_event({Type, _GLeader, Msg},
             #state{logger=Logger} = State) when Type =:= info_msg;
                                                 Type =:= warning_msg;
                                                 Type =:= error ->
    case action(State) of
        {log, NewState} ->
            log_msg(Type, Logger, Msg),
            {ok, NewState, hibernate};
        {drop, NewState} ->
            {ok, NewState}
    end;

handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Query, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

action(#state{logging_state=LoggingState,
              logger=Logger} = State) ->
    {message_queue_len, MailBoxSize} = process_info(self(), message_queue_len),
    case LoggingState of
        log ->
            case MailBoxSize >= ?MAILBOX_LIMIT of
                true ->
                    ale:log(Logger, warn,
                            "Mailbox size (~p) exceeded the limit of ~p. "
                            "Starting to drop messages on the floor.",
                            [MailBoxSize, ?MAILBOX_LIMIT]),
                    {drop, State#state{logging_state=drop}};
                false ->
                    {log, State}
            end;
        drop ->
            case MailBoxSize =< ?MAILBOX_RECOVER_LIMIT of
                true ->
                    ale:log(Logger, warn,
                            "Starting to log again after mailbox size (~p) "
                            "went below recovery limit of ~p.",
                            [MailBoxSize, ?MAILBOX_RECOVER_LIMIT]),
                    {log, State#state{logging_state=log}};
                false ->
                    {drop, State}
            end
    end.

log_report(Type, Logger, {_Pid, ReportType, Report}) ->
    LogLevel = type_to_loglevel(Type),

    {FmtHeader, ArgsHeader} = format_header(Type, ReportType),
    {FmtReport, ArgsReport} = format_report(ReportType, Report),

    Fmt = FmtHeader ++ FmtReport,
    Args = ArgsHeader ++ ArgsReport,

    do_log(Logger, LogLevel, Fmt, Args).

log_msg(Type, Logger, {_Pid, Fmt, Args}) ->
    LogLevel = type_to_loglevel(Type),
    do_log(Logger, LogLevel, Fmt, Args).

type_to_loglevel(info_report) ->
    info;
type_to_loglevel(info_msg) ->
    info;
type_to_loglevel(warning_report) ->
    warn;
type_to_loglevel(warning_msg) ->
    warn;
type_to_loglevel(error) ->
    error;
type_to_loglevel(error_report) ->
    error.

format_header(Type, ReportType) ->
    {"~n=========================~s=========================~n",
     [header(Type, ReportType)]}.

format_report(supervisor_report, Report) ->
    Name = rget(supervisor, Report),
    Context = rget(errorContext, Report),
    Reason = rget(reason, Report),
    Offender = rget(offender, Report),

    FormatString =
        "     Supervisor: ~p~n"
        "     Context:    ~p~n"
        "     Reason:     ~80.18p~n"
        "     Offender:   ~80.18p~n~n",

    {FormatString, [Name, Context, Reason, Offender]};
format_report(crash_report, Report) ->
    {"~s", [proc_lib:format(Report, latin1, 50)]};
format_report(_Other, Report) when is_list(Report) ->
    case io_lib:printable_list(Report) of
        true ->
            {"    ~s", [Report]};
        false ->
            lists:foldr(
              fun ({Key, Value}, {AccFmt, AccArgs}) ->
                      {"    ~16w: ~p~n" ++ AccFmt, [Key, Value | AccArgs]};
                  (Other, {AccFmt, AccArgs}) ->
                      {"                      ~p~n" ++ AccFmt, [Other | AccArgs]}
              end, {[], []}, Report)
    end;
format_report(_Other, Report) ->
    {"~p", [Report]}.

rget(Key, Report) ->
    proplists:get_value(Key, Report, "").

header(info_report, progress) ->
    "PROGRESS REPORT";
header(info_report, _Other) ->
    "INFO REPORT";
header(error_report, crash_report) ->
    "CRASH REPORT";
header(error_report, supervisor_report) ->
    "SUPERVISOR REPORT";
header(error_report, _Other) ->
    "ERROR REPORT";
header(warning_report, _Any) ->
    "WARNING REPORT".

do_log(Logger, LogLevel, Fmt, Args) ->
    Huge = [erts_debug:flat_size(A) > 1024 * 1024 || A <- Args],

    case lists:member(true, Huge) of
        true ->
            StrippedArgs = [case H of
                                true ->
                                    <<"too huge">>;
                                false ->
                                    A
                            end || {A, H} <- lists:zip(Args, Huge)],

            ale:log(Logger, warn,
                    "Preventing an attempt to log something quite huge~n"
                    "  Format string: ~s~n"
                    "  Log level: ~p~n"
                    "  Arguments: ~p~n",
                    [Fmt, LogLevel, StrippedArgs]);
        false ->
            ale:log(Logger, LogLevel, Fmt, Args)
    end.
