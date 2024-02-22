#!/bin/sh
#
# @author Couchbase <info@couchbase.com>
# @copyright 2013-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

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
                        logger:notice("Killing old memory monitor ~p", [Pid]),
                        catch exit(Pid, kill),
                        misc:wait_for_process(Pid, infinity)
                end,

                erlang:register(memory_monitor, self()),
                Threshold = ${threshold},

          logger:notice("Memory monitor started (pid ~p, threshold ~p)", [self(), Threshold]),

          Loop = fun (Recur) ->
                     Total = erlang:memory(total),
                     case Total > Threshold of
                         true ->
                             catch logger:notice("Total used memory ~p exceeded threshold ~p", [Total, Threshold]),
                             catch ale:sync_all_sinks(),
                             lists:foreach(
                               fun (Pid) ->
                                       try diag_handler:grab_process_info(Pid) of
                                           V ->
                                               BinBefore = proplists:get_value(binary, erlang:memory()),
                                               Res = (catch begin erlang:garbage_collect(Pid), erlang:garbage_collect(Pid), erlang:garbage_collect(Pid), erlang:garbage_collect(Pid), nothing end),
                                               BinAfter = proplists:get_value(binary, erlang:memory()),
                                               PList = V ++ [{binary_diff, BinAfter - BinBefore}, {res, Res}],
                                               logger:notice("Process ~p~n~p", [Pid, PList])
                                       catch _:_ ->
                                               ok
                                       end
                               end, erlang:processes()),
                             logger:notice("Done. Going to die"),
                             catch ale:sync_all_sinks(),
                             erlang:halt("memory_monitor");
                         false ->
                             catch logger:notice("Current total memory used ~p", [Total]),
                             ok
                     end,

                     timer:sleep(5000),
                     Recur(Recur)
                 end,

          Loop(Loop)
    end ]).
EOF
