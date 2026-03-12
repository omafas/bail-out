%%
%% %CopyrightBegin%
%%
%% SPDX-License-Identifier: Apache-2.0
%%
%% Copyright Ericsson AB 2001-2025. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%

%% This module uses some code and data structures from Erlang cover.erl
%% If internal structure of cover changes, this file likely also needs to be
%% updated.
%%
%% Mar 10, 2026 - Added export without necessity to compile file

-module(cb_cover).

-export([export_coverage/2]).

-record(bump, {module   = '_',              % atom()
               function = '_',              % atom()
               arity    = '_',              % integer()
               clause   = '_',              % integer()
               line     = '_'               % integer()
               }).

%% Write one code coverage data element to the file.
%% If the binary representation of the element is larger than 255 bytes, we
%% write a special header with the size of the element, followed by the
%% element itself.
write(Element, Fd) ->
    Bin = term_to_binary(Element, [compressed]),
    case byte_size(Bin) of
        Size when Size > 255 ->
            SizeBin = term_to_binary({'$size', Size}),
            ok = file:write(Fd, <<(byte_size(SizeBin)):8,
                                  SizeBin/binary,
                                  Bin/binary>>);
        Size ->
            ok = file:write(Fd, <<Size:8,Bin/binary>>)
    end,
    ok.

export_coverage(Modules, Dir) ->
    Timestamp = erlang:system_time(microsecond),
    Pid = os:getpid(),
    R = rand:uniform(1000000),
    Filename = io_lib:format("~s_~s_~b_~b.coverdata",
                             [node(), Pid, Timestamp, R]),
    CoverdataFile = filename:join(Dir, Filename),
    StartTime = erlang:monotonic_time(millisecond),
    try
        export(CoverdataFile, Modules)
    after
        EndTime = erlang:monotonic_time(millisecond),
        ElapsedMs = EndTime - StartTime,
        io:format("Coverdata export took ~bms~n", [ElapsedMs])
    end.

export(File, Modules) ->
    case file:open(File, [write, binary, raw, delayed_write]) of
        {ok, Fd} ->
            AvailableModules = [Mod || {Mod, _, _} <- code:all_available()],
            AvailableModulesSet = sets:from_list(AvailableModules),
            try export_coverdata_loop(Modules, Fd, AvailableModulesSet) of
                ok ->
                    io:format("Successfully wrote coverdata to ~s~n", [File]),
                    ok = file:close(Fd),
                    ok
            catch
                Error:Reason:ST ->
                    io:format("EXCEPTION during coverdata export: ~p: ~p~n~p~n",
                              [Error, Reason, ST]),
                    file:close(Fd),
                    {Error, Reason}
            end;
        {error, _Reason} = Error ->
            io:format("FAILED to open coverdata file ~s: ~p~n",
                      [File, _Reason]),
            Error
    end.

export_coverdata_loop([], _Fd, _AvailableModules) ->
    ok;
export_coverdata_loop([ModuleStr | Rest], Fd, AvailableModules) ->
    case prepare_module(ModuleStr, AvailableModules) of
        {ok, Module} ->
            try code:get_coverage(line, Module) of
                CoverageData ->
                    WhichFile = code:which(Module),
                    io:format("Exporting ~p coverage datapoints for ~p~n",
                              [length(CoverageData), Module]),
                    write_coverdata_file_header(Fd, Module, WhichFile),
                    write_coverdata_data(Fd, Module, CoverageData),
                    export_coverdata_loop(Rest, Fd, AvailableModules)
            catch
                _:Exception ->
                    io:format("Failed to get coverage for ~p "
                              "(not instrumented?), skipping~nError: ~p~n",
                              [Module, Exception]),
                    export_coverdata_loop(Rest, Fd, AvailableModules)
            end;
        skip ->
            export_coverdata_loop(Rest, Fd, AvailableModules)
    end.

prepare_module(Module, _AvailableModules) when is_atom(Module) ->
    case code:is_loaded(Module) of
        false -> %% Not loaded anyway
            io:format("Module ~p not loaded, skipping coverage export~n",
                      [Module]),
            skip;
        {file, _} ->
            {ok, Module}
    end;
prepare_module(ModuleStr, AvailableModules) when is_list(ModuleStr) ->
    case sets:is_element(ModuleStr, AvailableModules) of
        true ->
            prepare_module(list_to_atom(ModuleStr), AvailableModules);
        false ->
            io:format("Module ~s not found, skipping coverage export~n",
                      [ModuleStr]),
            skip
    end.

write_coverdata_file_header(Fd, Module, WhichFile) ->
    write({file, Module, WhichFile}, Fd).

write_coverdata_data(Fd, Module, CoverageData) ->
    lists:foreach(
      fun({Line, Count}) ->
              write({#bump{module = Module, line = Line}, Count}, Fd)
      end, CoverageData).