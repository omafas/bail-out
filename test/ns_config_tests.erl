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

-module(ns_config_tests).
-include_lib("eunit/include/eunit.hrl").
-include("ns_config.hrl").
-compile(nowarn_export_all).
-compile(export_all).

all_test_() ->
    {foreach,
     fun() -> test_setup() end,
     fun(V) -> test_teardown(V) end,
     [
      {"test_search_list",
       ?_test(test_search_list())},
      {"test_search_config",
       ?_test(test_search_config())},
      {"test_search_prop_config",
       ?_test(test_search_prop_config())},
      {"test_search_node",
       ?_test(test_search_node())},
      {"test_bin_persist",
       ?_test(test_bin_persist())},
      {"test_load_config_improper",
       ?_test(test_load_config_improper())},
      {"test_load_config",
       ?_test(test_load_config())},
      {"test_save_config",
       ?_test(test_save_config())},
      {"test_include_config",
       ?_test(test_include_config())},
      {"test_include_missing_config",
       ?_test(test_include_missing_config())},
      {spawn, {"test_svc",
               ?_test(test_svc())}}
     ]}.

default() -> [].

upgrade_config(_) -> [].

fixup(KV) -> KV.

test_search_list() ->
    ?assertMatch(false, ns_config:search([], foo)),
    ?assertMatch(false, ns_config:search([[], []], foo)),
    ?assertMatch(false, ns_config:search([[{x, 1}]], foo)),
    ?assertMatch({value, 1}, ns_config:search([[{x, 1}], [{x, 2}]], x)),
    ok.

test_search_config() ->
    ?assertMatch(false,
                 ns_config:search(#config{},
                        x)),
    ?assertMatch(false,
                 ns_config:search(#config{dynamic = [[], []],
                                static = [[], []]},
                        x)),
    ?assertMatch({value, 1},
                 ns_config:search(#config{dynamic = [[{x, 1}], [{x, 2}]],
                                static = []},
                        x)),
    ?assertMatch({value, 2},
                 ns_config:search(#config{dynamic = [[{y, 1}], [{x, 2}]],
                                static = [[], []]},
                        x)),
    ?assertMatch({value, 3},
                 ns_config:search(#config{dynamic = [[{y, 1}], [{x, 2}]],
                                static = [[{w, 4}], [{z, 3}]]},
                        z)),
    ?assertMatch({value, 2},
                 ns_config:search(#config{dynamic = [[{y, 1}], [{z, 2}]],
                                static = [[{w, 4}], [{z, 3}]]},
                        z)),
    ?assertMatch({value, [{hi, there}]},
                 ns_config:search(#config{dynamic = [[{y, 1}], [{z, [{hi, there}]}]],
                                static = [[{w, 4}], [{z, 3}]]},
                        z)),
    ?assertMatch({value, [{hi, there}]},
                 ns_config:search(#config{dynamic = [[{y, 1}], [{z, [{'_vclock', stripped},
                                                           {hi, there}]}]],
                                static = [[{w, 4}], [{z, 3}]]},
                        z)),
    ok.

test_search_prop_config() ->
    ?assertMatch(foo,
                 ns_config:search_prop(#config{},
                             x, a, foo)),
    ?assertMatch(foo,
                 ns_config:search_prop(#config{dynamic = [[], []],
                                     static = [[], []]},
                             x, a, foo)),
    ?assertMatch(foo,
                 ns_config:search_prop(#config{dynamic = [[{x, []}], [{x, []}]],
                                     static = []},
                             x, a, foo)),
    ?assertMatch(foo,
                 ns_config:search_prop(#config{dynamic = [[{x, [{b, bar}]}], [{x, []}]],
                                     static = []},
                             x, a, foo)),
    ?assertMatch(baz,
                 ns_config:search_prop(#config{dynamic = [[{x, [{b, bar},
                                                      {a, baz}]}], [{x, []}]],
                                     static = []},
                             x, a, foo)),
    ?assertMatch(foo,
                 ns_config:search_prop(#config{dynamic = [[{x, [{b, bar}]}],
                                                [{x, [{a, baz}]}]],
                                     static = []},
                             x, a, foo)),
    ok.

test_search_node() ->
    ?assertMatch(false,
                 ns_config:search_node(#config{},
                        x)),
    ?assertMatch(false,
                 ns_config:search_node(#config{dynamic = [[], []],
                                static = [[], []]},
                        x)),
    ?assertMatch({value, 1},
                 ns_config:search_node(#config{dynamic = [[{x, 11}, {{node, node(), x}, 1}], [{x, 2}]],
                                static = []},
                        x)),
    ?assertMatch({value, 2},
                 ns_config:search_node(#config{dynamic = [[{y, 1}], [{x, 22}, {{node, node(), x}, 2}]],
                                static = [[], []]},
                        x)),
    ?assertMatch({value, 3},
                 ns_config:search_node(#config{dynamic = [[{y, 1}], [{x, 2}]],
                                static = [[{w, 4}], [{z, 33}, {{node, node(), z}, 3}]]},
                        z)),
    ?assertMatch({value, 2},
                 ns_config:search_node(#config{dynamic = [[{y, 1}], [{z, 22}, {{node, node(), z}, 2}]],
                                static = [[{w, 4}], [{z, 3}]]},
                        z)),
    ?assertMatch({value, [{hi, there}]},
                 ns_config:search_node(#config{dynamic = [[{y, 1}], [{z, [{bye, there}]}, {{node, node(), z}, [{hi, there}]}]],
                                static = [[{w, 4}], [{z, 3}]]},
                        z)),
    ?assertMatch({value, [{hi, there}]},
                 ns_config:search_node(#config{dynamic = [[{y, 1}], [{z, [{'_vclock', stripped},
                                                                          {bye, there}]},
                                                                     {{node, node(), z}, [{'_vclock', stripped},
                                                                                          {hi, there}]}]],
                                static = [[{w, 4}], [{z, 3}]]},
                        z)),
    ok.

merge_kv_pairs_dynamic_test() ->
    ?assertEqual({[{y,1}], []},
                 ns_config:merge_kv_pairs(
                   [],
                   [{y,1}],
                   <<"uuid">>)),
    ?assertEqual({[{y,1}], []},
                 ns_config:merge_kv_pairs(
                   [{y,1}],
                   [],
                   <<"uuid">>)),

    Strip = fun (L) -> [{K, ns_config:strip_metadata(V)} || {K, V} <- L] end,

    {NewKVList0, Touched0} = ns_config:merge_kv_pairs([{y,1}, {a,a}],
                                                      [{y,2}, {b,b}],
                                                      <<"uuid">>),

    NewKVList = Strip(lists:sort(NewKVList0)),
    Touched = lists:sort(Touched0),

    ?assertEqual(lists:sort([{y,1},{a,a},{b,b}]), NewKVList),
    ?assertEqual([y], Touched).

merge_kv_pairs_vclock_test() ->
    VClock = vclock:fresh(),
    VClocka1 = vclock:increment(a, VClock),
    VClockab1 = vclock:increment(b, VClocka1),
    VClocka2 = vclock:increment(a, VClocka1),
    X0 = [{x,1},
          {y,[{?METADATA_VCLOCK,VClock}, yy]}],
    ?assertEqual({X0, []},
                 ns_config:merge_kv_pairs(
                   [],
                   [{y,[{?METADATA_VCLOCK,VClock}, yy]}, {x,1}],
                   <<"uuid">>)),
    X2 = [{x,1},{y,[{?METADATA_VCLOCK,VClocka1}, y2]}],
    ?assertEqual({X2, []},
                 ns_config:merge_kv_pairs(
                   [{y,[{?METADATA_VCLOCK,VClock}, y1]}, {x,1}],
                   [{y,[{?METADATA_VCLOCK,VClocka1}, y2]}],
                   <<"uuid">>)),
    X3 = [{x,[{?METADATA_VCLOCK,VClockab1}, x1]},
          {y,[{?METADATA_VCLOCK,VClocka2}, y2]}],
    ?assertEqual({X3, []},
                 ns_config:merge_kv_pairs(
                   [{x,[{?METADATA_VCLOCK,VClockab1}, x1]},
                    {y,[{?METADATA_VCLOCK,VClocka1}, y1]}],
                   [{y,[{?METADATA_VCLOCK,VClocka2}, y2]},
                    {x,[{?METADATA_VCLOCK,VClocka1}, x2]}],
                   <<"uuid">>)),
    ok.

merge_kv_pairs_timestamps_test() ->
    X0 = [{x, [{'_vclock', [{<<"uuid">>, {1, 10}}]}, {data, 1}]}],
    X1 = [{x, [{'_vclock', [{<<"uuid">>, {1, 11}}]}, {data, 2}]}],
    {MergedLeft, TouchedLeft} = ns_config:merge_kv_pairs(X0, X1, <<"uuid">>),
    ?assertEqual({value, [{data, 2}]}, ns_config:search([MergedLeft], x)),
    ?assertEqual([x], TouchedLeft),
    {MergedRight, TouchedRight} = ns_config:merge_kv_pairs(X1, X0, <<"uuid">>),
    ?assertEqual({value, [{data, 2}]}, ns_config:search([MergedRight], x)),
    ?assertEqual([x], TouchedRight),
    [{x, [{'_vclock', ClockLeft}|_]}] = MergedLeft,
    ?assertEqual([{<<"uuid">>, {1, 11}}], ClockLeft),
    [{x, [{'_vclock', ClockRight}|_]}] = MergedRight,
    ?assertEqual([{<<"uuid">>, {1, 11}}], ClockRight).

merge_kv_pairs_same_value_test() ->
    V0 = [{<<"a">>, {1, 10}}],
    X0 = [{x, [{'_vclock', V0}, {data, 1}]}],

    V1 = [{<<"b">>, {1, 11}}],
    X1 = [{x, [{'_vclock', V1}, {data, 1}]}],

    {[R1], [x]} = ns_config:merge_kv_pairs(X0, X1, <<"a">>),
    {[R2], [x]} = ns_config:merge_kv_pairs(X1, X0, <<"b">>),

    ?assertEqual(R1, R2),
    {x, Merged} = R1,
    {_, MergedClock} = ns_config:extract_vclock(Merged),

    ?assert(vclock:descends(MergedClock, V0)),
    ?assert(vclock:descends(MergedClock, V1)),
    ?assertNot(vclock:descends(V0, MergedClock)),
    ?assertNot(vclock:descends(V1, MergedClock)).

test_bin_persist() ->
    CP = data_file(),
    D = [[{x,1},{y,2},{z,3}]],
    ?assertEqual(ok, ns_config:save_file(bin, CP, D)),
    R = ns_config:load_file(bin, CP),
    ?assertEqual({ok, D}, R),
    ok.

test_load_config_improper() ->
    CP = data_file(),
    {ok, F} = file:open(CP, [write, raw]),
    ok = file:write(F, <<"improper config file">>),
    ok = file:close(F),
    R = ns_config:load_config(CP, test_dir(), ?MODULE),
    ?assertMatch({error, _}, R),
    ok.

test_load_config() ->
    CP = data_file(),
    {ok, F} = file:open(CP, [write, raw]),
    ok = file:write(F, <<"{x,1}.">>),
    ok = file:close(F),
    R = ns_config:load_config(CP, test_dir(), ?MODULE),
    ?assertMatch({ok, #config{static = [[{x,1}], []],
                              dynamic = [[{x,1}, {{node, _, uuid}, _}]],
                              policy_mod = ?MODULE,
                              uuid = _}}, R),
    ok.

test_save_config() ->
    CP = data_file(),
    {ok, F} = file:open(CP, [write, raw]),
    ok = file:write(F, <<"{x,1}.">>),
    ok = file:close(F),
    R = ns_config:load_config(CP, test_dir(), ?MODULE),
    ?assertMatch({ok, #config{static = [[{x,1}], []],
                              dynamic = [[{x,1}, {{node, _, uuid}, _}]],
                              policy_mod = ?MODULE,
                              uuid = _}}, R),
    {ok, #config{dynamic = [Dynamic]} = E} = R,
    X = E#config{dynamic = [misc:update_proplist(Dynamic, [{x,2},{y,3}])],
                 policy_mod = ?MODULE},
    ?assertEqual(ok, ns_config:save_config_sync(X, test_dir())),
    R2 = ns_config:load_config(CP, test_dir(), ?MODULE),
    ?assertMatch({ok, X}, R2),
    ok.

test_svc() ->
    try
        do_test_svc()
    catch E:T:Stack ->
            io:format("Details:~n~p~n~p~n", [{E,T}, Stack]),
            erlang:E(T)
    end.

do_test_svc() ->
    OldFlag = process_flag(trap_exit, true),
    CP = data_file(),
    D = test_dir(),
    B = <<"{x,1}.">>,
    {ok, F} = file:open(CP, [write, raw]),
    ok = file:write(F, B),
    ok = file:close(F),
    {ok, ConfigPid} = ns_config:start_link({full, CP, D, ns_config_tests}),
    (fun() ->
      C = ns_config:get(),
      R = ns_config:search(C, x),
      ?assertMatch({value, 1}, R),
      ok
     end)(),
    (fun() ->
      R = ns_config:search(x),
      ?assertMatch({value, 1}, R),
      ok
     end)(),
    (fun() ->
      R = ns_config:search(y),
      ?assertMatch(false, R),
      ok
     end)(),
    erlang:exit(ConfigPid, shutdown),
    receive
        {'EXIT', ConfigPid, Reason} ->
            ?assertEqual(shutdown, Reason)
    after 5000 ->
            io:format("timed out waiting for ns_config death. Killing~n"),
            erlang:exit(ConfigPid, kill),
            receive
                {'EXIT', ConfigPid, _} -> ok
            end,
            erlang:error(ns_config_shutdown_timeout)
    end,
    erlang:process_flag(trap_exit, OldFlag),
    ok.

test_include_config() ->
    CP1 = data_file(atom_to_list(node()) ++ "_one.cfg"),
    CP2 = data_file(atom_to_list(node()) ++ "_two.cfg"),
    {ok, F1} = file:open(CP1, [write, raw]),
    ok = file:write(F1, <<"{x,1}.\n">>),
    X = "{include,\"" ++ CP2 ++ "\"}.\n",
    ok = file:write(F1, list_to_binary(X)),
    ok = file:write(F1, <<"{y,1}.\n">>),
    ok = file:close(F1),
    {ok, F2} = file:open(CP2, [write, raw]),
    ok = file:write(F2, <<"{z,9}.">>),
    ok = file:close(F2),
    R = ns_config:load_config(CP1, test_dir(), ?MODULE),
    ?assertMatch({ok, #config{static = [[{x,1}, {z,9}, {y,1}], []],
                              policy_mod = ?MODULE}},
                 R),
    {ok, #config{dynamic = [DynamicR]}} = R,
    ?assertMatch([{x,1}, {y,1}, {z,9}, {{node, _, uuid}, _}],
                 lists:ukeysort(1, DynamicR)),
    ok.

test_include_missing_config() ->
    CP1 = data_file(atom_to_list(node()) ++ "_top.cfg"),
    {ok, F1} = file:open(CP1, [write, raw]),
    ok = file:write(F1, <<"{x,1}.\n">>),
    X = "{include,\"not_a_config_path\"}.\n",
    ok = file:write(F1, list_to_binary(X)),
    ok = file:write(F1, <<"{y,1}.\n">>),
    ok = file:close(F1),
    R = ns_config:load_config(CP1, test_dir(), ?MODULE),
    ?assertEqual({error, {bad_config_path, "not_a_config_path"}}, R),
    ok.

test_setup() ->
    process_flag(trap_exit, true),
    misc:rm_rf(test_dir()),
    ns_config:mock_tombstone_agent(),
    ok.

test_teardown(_) ->
    file:delete(data_file()),
    misc:rm_rf(test_dir()),
    ns_config:unmock_tombstone_agent(),
    meck:unload().

test_dir() ->
  Dir = filename:join([t:config(priv_dir), "data", "config"]),
  filelib:ensure_dir(filename:join(Dir, "config")),
  Dir.

data_file()     -> data_file(atom_to_list(node())).
data_file(Name) -> filename:join([test_dir(), Name]).

decrypt(Config) ->
    Config.
