%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2018 Couchbase, Inc.
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
-module(triq_utils).

-include("triq.hrl").

-export([smaller/1, min_size/2, random_integer_fun/0]).

smaller(Domain) ->
    ?SIZED(Size,
           resize(Size div 2, Domain)).

min_size(Domain, MinSize) ->
    ?SIZED(Size,
           case Size >= MinSize of
               true ->
                   Domain;
               false ->
                   resize(MinSize, Domain)
           end).

safe_idiv(X) ->
    case X =/= 0 of
        true ->
            functools:idiv(X);
        false ->
            fun functools:id/1
    end.

random_integer_fun_spec() ->
    [oneof([fun functools:id/1] ++
                   [random_simple_fun_spec(BF) ||
                       BF <- [fun functools:const/1,
                              fun functools:add/1,
                              fun functools:sub/1,
                              fun functools:mul/1,
                              fun safe_idiv/1]]) ||
        %% Explicitly limit number of recursive applications to a low
        %% number. Otherwise, the resulting functions tend to converge to the
        %% same result irrespective of the input. That is because whenever
        %% there's a const() in the sequence, the end result is going to be
        %% the same for any input. Similarly the convergence can be caused by
        %% integer division producing same results for different inputs.
        _ <- lists:seq(1, 3)].

random_simple_fun_spec(BaseFun) ->
    ?LET(N, int(), {BaseFun, [N]}).

fun_spec_to_fun(Spec) ->
    fun (X) ->
            Funs = [case F of
                        _ when is_function(F) ->
                            F;
                        {BaseF, Args} ->
                            erlang:apply(BaseF, Args)
                    end || F <- Spec],
            functools:chain(X, Funs)
    end.

random_integer_fun() ->
    ?LET(Spec, random_integer_fun_spec(), fun_spec_to_fun(Spec)).
