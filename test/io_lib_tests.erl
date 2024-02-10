% Copyright 2021-Present Couchbase, Inc.
%
% Use of this software is governed by the Business Source License included in
% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
% file, in accordance with the Business Source License, use of this software
% will be governed by the Apache License, Version 2.0, included in the file
% licenses/APL2.txt.

%% Produce random terms, of varying depth and run them against
%% io:lib_format/3, to verify io_lib_format/3 doesn't fall into
%% a loop.
%%
%% Issue: https://github.com/erlang/otp/issues/4824

-module(io_lib_tests).

-export([generate/1]).

-define(NUM_DATATYPES, 3).
-define(CHARS_LIMIT, 1024).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

generate_term(1, X) ->
    generate_list(X);
generate_term(2, X) ->
    generate_tuple(X);
generate_term(3, X) ->
    generate_map(X).

generate_list(1) ->
    [rand:uniform(1024)];
generate_list(X) ->
    Data = generate_term(rand:uniform(?NUM_DATATYPES), rand:uniform(X)),
    [Data, generate_list(X-1)].

generate_tuple(1) ->
    {rand:uniform(1024)};
generate_tuple(X) ->
    Data = generate_term(rand:uniform(?NUM_DATATYPES), rand:uniform(X)),
    {Data, generate_tuple(X-1)}.

generate_map(1) ->
    rand:uniform(1024);
generate_map(X) ->
    Data = generate_term(rand:uniform(?NUM_DATATYPES), rand:uniform(X)),
    #{Data => generate_map(X-1)}.

generate(N) ->
    Fun = fun (A) ->
            start_execution('io_lib', 'format', A)
          end,

    [fun() ->
        Term = generate_term(rand:uniform(?NUM_DATATYPES), 10),
        %% If io_lib:format/3 falls into a loop a throw expection
        %% is generated. That will fail here.
        ok = print_single_term(Fun, Term, 10)
     end () || _ <- lists:seq(1,N)],
    ok.

print_single_term(_Fun, _Arg, 0) ->
    ok;
print_single_term(Fun, Arg, I) ->
    Fun(["~p", [Arg], [{chars_limit, rand:uniform(?CHARS_LIMIT)}]]),
    print_single_term(Fun, Arg, I-1).

%% start a child process and run it in a timer, to see if it times-out.
%% If it times-out, throw an error ...
%% else continue to execute other Funs.

start_execution(M, F, A) ->
    Parent = self(),
    Ref = erlang:make_ref(),

    Child = spawn(fun() ->
                    erlang:apply(M, F, A),
                    Parent ! {eok, Ref}
                  end),

    receive
        {eok, Ref} ->
            ok
    after 2000 ->
        %% The child process is stuck in a loop - kill it!
        %% Also flush the Parent mailbox of any stale eok messages.
        exit(Child, kill),
        receive {eok, _} -> ok after 0 -> ok end,
        throw({loop_error, A})
    end.

-ifdef(TEST).
generate_test() ->
    ?assertEqual(ok, generate(100)).
-endif.
