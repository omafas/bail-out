#!/bin/sh
#
# @author Couchbase <info@couchbase.com>
# @copyright 2013 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

user=$1
password=$2
host=$3
threshold=${4:-2147483648}

curl -X POST -u $user:$password http://$host/diag/eval -d @- <<EOF
    rpc:eval_everywhere(
      proc_lib, spawn,
      [ fun () ->
                case whereis(memory_monitor) of
                    undefined ->
                        ok;
                    Pid ->
                        error_logger:info_msg("Killing old memory monitor ~p", [Pid]),
                        catch exit(Pid, kill),
                        misc:wait_for_process(Pid, infinity)
                end,

                erlang:register(memory_monitor, self()),
                Threshold = ${threshold},

          error_logger:info_msg("Memory monitor started (pid ~p, threshold ~p)", [self(), Threshold]),

          Loop = fun (Recur) ->
                     Total = erlang:memory(total),
                     case Total > Threshold of
                         true ->
                             catch error_logger:info_msg("Total used memory ~p exceeded threshold ~p", [Total, Threshold]),
                             catch ale:sync_all_sinks(),
                             lists:foreach(
                               fun (Pid) ->
                                       try diag_handler:grab_process_info(Pid) of
                                           V ->
                                               BinBefore = proplists:get_value(binary, erlang:memory()),
                                               Res = (catch begin erlang:garbage_collect(Pid), erlang:garbage_collect(Pid), erlang:garbage_collect(Pid), erlang:garbage_collect(Pid), nothing end),
                                               BinAfter = proplists:get_value(binary, erlang:memory()),
                                               PList = V ++ [{binary_diff, BinAfter - BinBefore}, {res, Res}],
                                               error_logger:info_msg("Process ~p~n~p", [Pid, PList])
                                       catch _:_ ->
                                               ok
                                       end
                               end, erlang:processes()),
                             error_logger:info_msg("Done. Going to die"),
                             gen_event:which_handlers(error_logger),
                             catch ale:sync_all_sinks(),
                             erlang:halt("memory_monitor");
                         false ->
                             catch error_logger:info_msg("Current total memory used ~p", [Total]),
                             ok
                     end,

                     timer:sleep(5000),
                     Recur(Recur)
                 end,

          Loop(Loop)
    end ]).
EOF
