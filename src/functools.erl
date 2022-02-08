%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(functools).

-export([id/1,
         const/1,
         compose/1, compose/2,
         chain/2,
         curry/1,
         uncurry/1,
         alternative/2,
         sequence/1,
         sequence_/1,
         add/1, add/2,
         sub/1, sub/2,
         mul/1, mul/2,
         idiv/1, idiv/2]).

%% Identity function.
id(X) ->
    X.

%% Create a function of one argument that always returns the constant
%% passed.
const(Value) ->
    fun (_) -> Value end.

%% Compose two functions. Note that the order of the function is
%% reversed to what it normally is in such functions.
compose(First, Second) ->
    compose([First, Second]).

%% Compose many functions.
compose(Funs) when is_list(Funs) ->
    fun (X) ->
            lists:foldl(fun (F, Acc) ->
                                F(Acc)
                        end, X, Funs)
    end.

%% Compose many functions and apply the resulting function to 'X'
chain(X, Funs) ->
    (compose(Funs))(X).

%% Curry a function.
curry(F) ->
    fun (X) ->
            fun (Y) ->
                    F(X, Y)
            end
    end.

%% Uncurry a function.
uncurry(F) ->
    fun (X, Y) ->
            (F(X))(Y)
    end.

%% Apply functions in a row until one succeeds, as indicated by {ok, _} return
%% value.
alternative(_Initial, []) ->
    false;
alternative(Initial, [F | Funs]) ->
    case F(Initial) of
        {ok, _New} = R ->
            R;
        false ->
            alternative(Initial, Funs)
    end.

%% Apply functions in sequence and collect the ok results. If any function
%% fails, return the error.
sequence(Funs) ->
    sequence(Funs, []).

sequence([], Acc) ->
    {ok, lists:reverse(Acc)};
sequence([F | Rest], Acc) ->
    case F() of
        {ok, R} ->
            sequence(Rest, [R | Acc]);
        Other ->
            Other
    end.

%% Same as sequence/1, but doesn't expect functions to return anything useful
%% in ok case.
sequence_([]) ->
    ok;
sequence_([F | Rest]) ->
    case F() of
        ok ->
            sequence_(Rest);
        Other ->
            Other
    end.

%% some partially applied built-in operations
add(Y) ->
    fun (X) -> X + Y end.

sub(Y) ->
    fun (X) -> X - Y end.

mul(Y) ->
    fun (X) -> X * Y end.

idiv(Y) ->
    fun (X) -> X div Y end.

%% first-class versions of some built-in operations
add(X, Y) ->
    X + Y.

sub(X, Y) ->
    X - Y.

mul(X, Y) ->
    X * Y.

idiv(X, Y) ->
    X div Y.
