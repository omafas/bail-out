%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2015 Couchbase, Inc.
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
-module(dir_size).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([get/1, get_slow/1, start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).


godu_name() ->
    case erlang:system_info(system_architecture) of
        "win32" ->
            "godu.exe";
        _ ->
            "godu"
    end.

start_link() ->
    Name = godu_name(),

    ?log_info("Starting quick version of dir_size with program name: ~s", [Name]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, Name, []).

get(Dir) ->
    case erlang:whereis(?MODULE) of
        undefined ->
            get_slow(Dir);
        _Pid ->
            gen_server:call(?MODULE, {dir_size, Dir}, infinity)
    end.

get_slow(Dir) ->
    Fn =
        fun (File, Acc) ->
                Size = filelib:file_size(File),
                Acc + Size
        end,
    filelib:fold_files(Dir, ".*", true, Fn, 0).

init(ProgramName) ->
    DuPath = path_config:component_path(bin, filename:join("priv", ProgramName)),
    Port = erlang:open_port({spawn_executable, DuPath},
                            [stream, {args, []},
                             binary, eof, use_stdio]),
    {ok, Port}.

decode_reply(Dir, IOList) ->
    Data = erlang:iolist_to_binary(IOList),
    {struct, Decoded} = mochijson2:decode(Data),
    Size = proplists:get_value(<<"size">>, Decoded),
    ErrorCount = proplists:get_value(<<"errorCount">>, Decoded),
    case ErrorCount of
        0 ->
            ok;
        _ ->
            ?log_info("Has some errors on trying to grab aggregate size of ~s:~n~p", [Dir, Decoded])
    end,
    Size.

get_reply(Dir, Port, Acc) ->
    receive
        {Port, {data, Bin}} ->
            case binary:last(Bin) of
                $\n ->
                    {reply, decode_reply(Dir, [Acc | Bin])};
                _ ->
                    get_reply(Dir, Port, [Acc | Bin])
            end;
        {Port, eof} ->
            {stop, decode_reply(Dir, Acc)};
        {Port, _} = Unknown ->
            erlang:error({unexpected_message, Unknown})
    end.

handle_dir_size(Dir, Port) ->
    Size = integer_to_list(length(Dir)),
    port_command(Port, [Size, $:, Dir, $,]),
    case get_reply(Dir, Port, []) of
        {reply, RV} ->
            {reply, RV, Port};
        {stop, RV} ->
            {stop, port_died, RV, Port}
    end.

handle_call({dir_size, Dir}, _From, Port) ->
    handle_dir_size(Dir, Port).

handle_cast(_, _State) ->
    erlang:error(unexpected).

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
