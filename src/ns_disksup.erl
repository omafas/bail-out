%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2013. All Rights Reserved.
%% Copyright Couchbase, Inc 2014-2017. All Rights Reserved.
%%
%% Use of this software is governed by the Erlang Public License,
%% Version 1.1 included in the file licenses/EPL-1-1.txt.
%%
%% %CopyrightEnd%
%%
%% forked shortened version of R16 disksup. serves 2 purposes:
%% - include bind mounts into linux disk info
%% - fix OSX disksup to include the new Apple File System (apfs)

-module(ns_disksup).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([get_disk_data/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {timeout, os, diskdata = [], port}).

%%----------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------

start_link() ->
    case misc:is_linux() orelse misc:is_macos() of
        true ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, [], []);
        false ->
            ignore
    end.

get_disk_data() ->
    case misc:is_linux() orelse misc:is_macos() of
        true ->
            gen_server:call(?MODULE, get_disk_data, infinity);
        false ->
            ns_bootstrap:ensure_os_mon(),
            disksup:get_disk_data()
    end.

%%----------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    process_flag(priority, low),

    OS = os:type(),
    Port = start_portprogram(),
    ns_bootstrap:ensure_os_mon(),
    Timeout = disksup:get_check_interval(),

    %% Initiation first disk check
    self() ! timeout,

    {ok, #state{port=Port, os=OS,
                timeout=Timeout}}.

handle_call(get_disk_data, _From, State) ->
    {reply, State#state.diskdata, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    NewDiskData = check_disk_space(State#state.os, State#state.port),
    erlang:send_after(State#state.timeout, self(), timeout),
    {noreply, State#state{diskdata = NewDiskData}};
handle_info({'EXIT', _Port, Reason}, State) ->
    {stop, {port_died, Reason}, State#state{port=not_used}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.port of
        not_used ->
            ok;
        Port ->
            port_close(Port)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--Port handling functions---------------------------------------------

start_portprogram() ->
    open_port({spawn, "sh -s ns_disksup 2>&1"}, [stream]).

my_cmd(Cmd0, Port) ->
    %% Insert a new line after the command, in case the command
    %% contains a comment character
    Cmd = io_lib:format("(~s\n) </dev/null; echo  \"\^M\"\n", [Cmd0]),
    Port ! {self(), {command, [Cmd, 10]}},
    get_reply(Port, []).

get_reply(Port, O) ->
    receive
        {Port, {data, N}} ->
            case newline(N, O) of
                {ok, Str} -> Str;
                {more, Acc} -> get_reply(Port, Acc)
            end;
        {'EXIT', Port, Reason} ->
            exit({port_died, Reason})
    end.

newline([13|_], B) -> {ok, lists:reverse(B)};
newline([H|T], B) -> newline(T, [H|B]);
newline([], B) -> {more, B}.

%%--Check disk space----------------------------------------------------

check_disk_space({unix, linux}, Port) ->
    Result = my_cmd("/bin/df -alk", Port),
    check_disks_linux(skip_to_eol(Result));
check_disk_space({unix, darwin}, Port) ->
    Result = my_cmd("/bin/df -i -k -T ufs,hfs,apfs", Port),
    check_disks_susv3(skip_to_eol(Result)).

check_disks_linux("") ->
    [];
check_disks_linux("\n") ->
    [];
check_disks_linux(Str) ->
    case io_lib:fread("~s~d~d~d~d%~s", Str) of
        {ok, [_FS, KB, _Used, _Avail, Cap, MntOn], RestStr} ->
            [{MntOn, KB, Cap} |
             check_disks_linux(RestStr)];
        _Other ->
            check_disks_linux(skip_to_eol(Str))
    end.

check_disks_susv3("") ->
    [];
check_disks_susv3("\n") ->
    [];
check_disks_susv3(Str) ->
    case io_lib:fread("~s~d~d~d~d%~d~d~d%~s", Str) of
        {ok, [_FS, KB, _Used, _Avail, Cap, _IUsed, _IFree, _ICap, MntOn], RestStr} ->
            [{MntOn, KB, Cap} |
             check_disks_susv3(RestStr)];
        _Other ->
            check_disks_susv3(skip_to_eol(Str))
    end.

%%--Auxiliary-----------------------------------------------------------

skip_to_eol([]) ->
    [];
skip_to_eol([$\n | T]) ->
    T;
skip_to_eol([_ | T]) ->
    skip_to_eol(T).
