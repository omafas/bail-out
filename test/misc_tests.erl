% Copyright (c) 2008, Cliff Moon
% Copyright (c) 2008, Powerset, Inc
% Copyright (c) 2009-2018, Couchbase, Inc.
%
% All rights reserved.
%
% Redistribution and use in source and binary forms, with or without
% modification, are permitted provided that the following conditions
% are met:
%
% * Redistributions of source code must retain the above copyright
% notice, this list of conditions and the following disclaimer.
% * Redistributions in binary form must reproduce the above copyright
% notice, this list of conditions and the following disclaimer in the
% documentation and/or other materials provided with the distribution.
% * Neither the name of Powerset, Inc nor the names of its
% contributors may be used to endorse or promote products derived from
% this software without specific prior written permission.
%
% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
% "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
% LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
% FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
% COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
% INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
% BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
% LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
% CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
% LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
% ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
% POSSIBILITY OF SUCH DAMAGE.
%
% Original Author: Cliff Moon

-module(misc_tests).
-include_lib("eunit/include/eunit.hrl").

shuffle_test() ->
  % we can really only test that they aren't equals,
  % which won't even always work, weak
  A = [a, b, c, d, e, f, g],
  B = misc:shuffle(A),
  % ?debugFmt("shuffled: ~p", [B]),
  ?assertEqual(7, length(B)).

rm_rf_test() ->
  lists:foldl(fun(N, Dir) ->
      NewDir = filename:join(Dir, N),
      File = filename:join(NewDir, "file"),
      filelib:ensure_dir(File),
      file:write_file(File, "blahblah"),
      NewDir
    end, priv_dir(), ["a", "b", "c", "d", "e"]),
  misc:rm_rf(filename:join(priv_dir(), "a")),
  ?assertEqual({ok, []}, file:list_dir(priv_dir())).

priv_dir() ->
  Dir = filename:join([t:config(priv_dir), "misc"]),
  filelib:ensure_dir(filename:join([Dir, "misc"])),
  Dir.

parallel_map_test_() ->
    {timeout, 200,
     fun () ->
             L = [0, 1, 2, 3],
             ?assertEqual(L,
                          misc:parallel_map(fun (N) ->
                                                    timer:sleep(40 - 10*N),
                                                    N
                                            end, L, infinity)),
             ?assertEqual(L,
                          misc:parallel_map(fun (N) ->
                                                    timer:sleep(40 - 10*N),
                                                    N
                                            end, L, 100)),
             ?assertEqual(L,
                          misc:parallel_map(fun (N) ->
                                                    timer:sleep(10*N),
                                                    N
                                            end, L, 100)),

             Ref = erlang:make_ref(),
             Parent = self(),
             try
                 misc:parallel_map(fun (N) ->
                                           Parent ! {Ref, {N, self()}},
                                           timer:sleep(10000)
                                   end, L, 200),
                 exit(failed)
             catch
                 exit:timeout ->
                     ok
             end,

             Loop = fun (Loop, Acc) ->
                            receive
                                {Ref, P} ->
                                    Loop(Loop, [P|Acc])
                            after 0 ->
                                    Acc
                            end
                    end,
             PidPairs = Loop(Loop, []),
             ?assertEqual(L, lists:sort([I || {I, _} <- PidPairs])),
             lists:foreach(fun ({_, Pid}) ->
                                   ?assertEqual(false, erlang:is_process_alive(Pid))
                           end, PidPairs)
     end}.

parallel_map_when_trap_exit_test() ->
    {Parent, Ref} = {self(), erlang:make_ref()},
    MapperPid = spawn_link(fun () ->
                                   erlang:process_flag(trap_exit, true),
                                   Res = misc:parallel_map(fun (N) -> N end,
                                                     [1,2,3], infinity),
                                   Parent ! {Ref, Res},
                                   receive
                                       X -> Parent ! {Ref, X}
                                   after 10 ->
                                           ok
                                   end
                           end),
    Res = receive
              {Ref, R} -> R
          end,
    ?assertEqual([1,2,3], Res),
    misc:wait_for_process(MapperPid, infinity),
    ?assertEqual(false, erlang:is_process_alive(MapperPid)),
    receive
        {Ref, _} = X ->
            exit({unexpected_message, X})
    after 0 -> ok
    end.


realpath_test_() ->
    Tests = [{"test_symlink_loop",
              ?_test(test_symlink_loop())},
             {"test_simple_symlink",
              ?_test(test_simple_symlink())},
             {"test_no_symlink",
              ?_test(test_no_symlink())}],
    EffectiveTests = case erlang:system_info(system_architecture) of
                         "win32" -> [];
                         _ -> Tests
                     end,
    {spawn, {foreach,
             fun () -> realpath_setup() end,
             fun (V) -> realpath_teardown(V) end,
             EffectiveTests}}.

realpath_setup() ->
    process_flag(trap_exit, true),
    misc:rm_rf(test_dir()),
    ok.

realpath_teardown(_) ->
    misc:rm_rf(test_dir()),
    ok.

test_dir() ->
    Dir = filename:join([t:config(priv_dir), "data", "misc"]),
    ok = filelib:ensure_dir(filename:join(Dir, "asdasd")),
    Dir.

test_symlink_loop() ->
    Dir = test_dir(),
    Cycle = filename:join(Dir, "cycle"),
    file:make_symlink(Cycle, Cycle),
    ?assertMatch({error, symlinks_limit_reached, _, _, _},
                 misc:realpath(Cycle, "/")),
    ?assertMatch({error, symlinks_limit_reached, _, _, _},
                 misc:realpath(Cycle, "/bin")),
    ok.

test_simple_symlink() ->
    Dir = test_dir(),
    Var = filename:join([Dir, "var", "opt", "membase", "data"]),
    Opt = filename:join([Dir, "opt", "membase", "xxx", "data"]),
    ok = filelib:ensure_dir(filename:join(Var, "asdasd")),
    ok = filelib:ensure_dir(Opt),
    ok = file:make_symlink(Var, Opt),
    ok = filelib:ensure_dir(filename:join([Opt, "ns_1", "asdasd"])),
    ?assertEqual({ok, filename:join([Var, "ns_1"])},
                 misc:realpath(filename:join(Opt, "ns_1"), "/")),
    ?assertEqual({ok, filename:join([Var, "ns_1"])},
                 misc:realpath(filename:join(Opt, "ns_1"), "/bin")),
    ok.

test_no_symlink() ->
    Dir = test_dir(),
    Var = filename:join([Dir, "var", "opt", "membase", "data"]),
    ok = filelib:ensure_dir(filename:join(Var, "asdasd")),
    ?assertEqual({ok, Var},
                 misc:realpath(Var, "/")),
    ?assertEqual({ok, Var},
                 misc:realpath(Var, "/bin")),
    ok.

split_binary_at_char_test() ->
    ?assertEqual(<<"asd">>, misc:split_binary_at_char(<<"asd">>, $:)),
    ?assertEqual({<<"asd">>, <<"123">>}, misc:split_binary_at_char(<<"asd:123">>, $:)),
    ?assertEqual({<<"asd">>, <<"123:567">>}, misc:split_binary_at_char(<<"asd:123:567">>, $:)).

