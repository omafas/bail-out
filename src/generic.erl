%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% Generic programming framework loosely based on the paper "Scrap Your
%% Boilerplate: A Practical Design Pattern for Generic Programming" by Ralf
%% Lammel and Simon Peyton Jones.
%%
%% Link to the paper:
%% https://www.microsoft.com/en-us/research/wp-content/uploads/2003/01/hmap.pdf
%%
-module(generic).

-include("generic.hrl").

-ifdef(TEST).
-include("triq.hrl").
-endif.

-export([transformb/2, transformb/3,
         transformt/2, transformt/3,
         matching/2, matching/3,
         universe/1, universe/2,
         maybe_transform/2, maybe_transform/3,
         query/3]).

%% Apply a transformation everywhere in bottom-up manner.
transformb(Fun, State, Term) ->
    {NewTerm, NewState} = gmap(fun (T, S) ->
                                       transformb(Fun, S, T)
                               end, State, Term),
    Fun(NewTerm, NewState).

transformb(Fun, Term) ->
    ignoring_state(fun transformb/3, Fun, Term).

%% Apply a transformation everywhere in top-down manner.
transformt(Fun, State, Term) ->
    maybe_transform(fun (T, S) ->
                            {NewT, NewS} = Fun(T, S),
                            {continue, NewT, NewS}
                    end, State, Term).

transformt(Fun, Term) ->
    ignoring_state(fun transformt/3, Fun, Term).

%% Return the subterms matching a predicate.
matching(Pred, Term) ->
    matching(Pred, Term, fun transformb/3).

matching(Pred, Term, Traversal) ->
    {_, Result} =
        Traversal(fun (T, Acc) ->
                          NewAcc = case Pred(T) of
                                       true ->
                                           [T | Acc];
                                       false ->
                                           Acc
                                   end,
                          {T, NewAcc}
                  end, [], Term),
    lists:reverse(Result).

%% Return all possible subterms.
universe(Term) ->
    universe(Term, fun transformb/3).

universe(Term, Traversal) ->
    matching(functools:const(true), Term, Traversal).

%% Apply a transformation everywhere in top-down manner. The 'Fun'
%% function may choose to stop the recursive descent early by
%% returning {stop, ResultTerm, ResultState}. Note, there's not 't'
%% suffix here, because short-cutting doesn't make much sense in
%% bottom-up traversal.
maybe_transform(Fun, State, Term) ->
    case Fun(Term, State) of
        {continue, NewTerm, NewState} ->
            gmap(fun (T, S) ->
                         maybe_transform(Fun, S, T)
                 end, NewState, NewTerm);
        {stop, NewTerm, NewState} ->
            {NewTerm, NewState}
    end.

maybe_transform(Fun, Term) ->
    do_ignoring_state(fun maybe_transform/3,
                      fun (T, S) ->
                              {Action, NewT} = Fun(T),
                              {Action, NewT, S}
                      end, Term).

%% Run a query on the term. The 'Fun' is called on each element in the term
%% and these values are then recombined by 'K'. The traversal order is
%% top-down, left-to-right.
query(K, Fun, Term) ->
    lists:foldl(K, Fun(Term),
                gmapq(fun (T) ->
                              query(K, Fun, T)
                      end, Term)).

%% internal
gfold(Fun, State, Term) ->
    {Type, Children} = term_destructure(Term),
    Fun(Children, State,
        fun (NewChildren) ->
                try
                    term_recover(Type, NewChildren)
                catch
                    T:E:S ->
                        error({term_recover_failed,
                               {T, E, S},
                               {term, Term},
                               {type, Type},
                               {children, Children},
                               {new_children, NewChildren}})
                end
        end).

term_destructure([H|T]) ->
    {cons, [H,T]};
term_destructure(Tuple) when is_tuple(Tuple) ->
    {{tuple, tuple_size(Tuple)}, tuple_to_list(Tuple)};
term_destructure(Map) when is_map(Map) ->
    {map, lists:append([[K, V] || {K, V} <- maps:to_list(Map)])};
term_destructure(Term) ->
    {{simple, Term}, []}.

term_recover(cons, [H,T]) ->
    [H|T];
term_recover({tuple, Size}, List) ->
    Tuple = list_to_tuple(List),
    Size  = tuple_size(Tuple),
    Tuple;
term_recover(map, Values) ->
    maps:from_list(pairs(Values));
term_recover({simple, Term}, []) ->
    Term.

pairs([]) ->
    [];
pairs([K, V | Rest]) ->
    [{K, V} | pairs(Rest)].

%% Apply a transformation to direct children of a term.
gmap(Fun, State, Term) ->
    gfold(fun (Children, S, Recover) ->
                  {NewChildren, NewState} =
                      lists:foldl(
                        fun (Child, {AccChildren, AccS}) ->
                                {NewChild, NewAccS} = Fun(Child, AccS),
                                {[NewChild | AccChildren], NewAccS}
                        end, {[], S}, Children),
                  {Recover(lists:reverse(NewChildren)), NewState}
          end, State, Term).

%% Run a query on all direct children of a term. Return results as a list.
gmapq(Fun, Term) ->
    {Result, unused} = gfold(fun (Children, State, _Recover) ->
                                     {lists:map(Fun, Children), State}
                             end, unused, Term),
    Result.

ignoring_state(BaseFun, Fun, Term) ->
    do_ignoring_state(BaseFun,
                      fun (T, S) ->
                              {Fun(T), S}
                      end, Term).

do_ignoring_state(BaseFun, WrappedFun, Term) ->
    {NewTerm, unused} = BaseFun(WrappedFun, unused, Term),
    NewTerm.


-ifdef(TEST).
%% test-related helpers
random_term([]) ->
    oneof([{}, [], #{}]);
random_term([X]) ->
    oneof([[X], {X}, X, #{key => X}]);
random_term([X, Y | Rest] = Items) ->
    frequency([{2, Items},
               {2, list_to_tuple(Items)},
               {2, glue_terms(#{X => Y}, random_term(Rest))},
               {6, random_term_split(Items)}]).

random_term_split(Items) ->
    ?LET(N, choose(0, length(Items)),
         begin
             {Front, Rear} = lists:split(N, Items),
             glue_terms(random_term(Front), random_term(Rear))
         end).

glue_terms(X, Y) ->
    oneof([{X, Y}, [X, Y], [X | Y], singleton_map(X, Y)]).

singleton_map(KSpec, VSpec) ->
    %% Typically triq instantiates the random specs lazily by walking through
    %% the spec at the very end. But at the time of this writing, it doesn't
    %% know how to deal with maps. So we need to explicitly instantiate the
    %% subterms when constructing maps.
    ?LET(K, KSpec,
         ?LET(V, VSpec, #{K => V})).

%% triq properties
prop_transform_id(Transform) ->
    ?FORALL(Term, any(), Transform(fun functools:id/1, Term) =:= Term).

prop_transformt_id() ->
    prop_transform_id(fun transformt/2).

prop_transformb_id() ->
    prop_transform_id(fun transformb/2).

%% traversal order is left to right, so the order of original elements must be
%% the same as in Items list
prop_transform_items_order(Transform) ->
    forall_terms(fun (Items, Term) ->
                         Items =:= matching(fun is_integer/1, Term, Transform)
                 end).

prop_transformt_items_order() ->
    prop_transform_items_order(fun transformt/3).

prop_transformb_items_order() ->
    prop_transform_items_order(fun transformb/3).

prop_transforms_same_subterms() ->
    forall_terms(fun (_Items, Term) ->
                         AllT = universe(Term, fun transformt/3),
                         AllB = universe(Term, fun transformb/3),
                         lists:sort(AllT) =:= lists:sort(AllB)
                 end).

prop_transforms_result(Transform) ->
    Props = ?FORALL(Fun, triq_utils:random_integer_fun(),
                    forall_terms(
                      fun (Items, Term) ->
                              TransFun = ?transform(I when is_integer(I), Fun(I)),

                              Items1 = lists:map(Fun, Items),
                              Term1  = Transform(TransFun, Term),

                              Items1 =:= matching(fun is_integer/1, Term1)
                      end)),

    %% each forall multiplies number of tests by 100 (by default), so we'd
    %% have to run 10^6 number of tests which is a bit too much; here we lower
    %% it to 22^3 (yes, it's somewhat confusing) which is approximately 10000
    triq:numtests(22, Props).

prop_transformt_result() ->
    prop_transforms_result(fun transformt/2).

prop_transformb_result() ->
    prop_transforms_result(fun transformb/2).

prop_query_result(QueryK, QueryFun, ListFun) ->
    forall_terms(fun (Items, Term) ->
                         ListFun(Items) =:= query(QueryK, QueryFun, Term)
                 end).

prop_query_count() ->
    prop_query_result(fun functools:add/2, ?query(I when is_integer(I), 1, 0),
                      fun erlang:length/1).

prop_query_sum() ->
    prop_query_result(fun functools:add/2, ?query(I when is_integer(I), I, 0),
                      fun lists:sum/1).

forall_terms(Prop) ->
    ?FORALL(Items, list(int()),
            ?FORALL(Term, random_term(Items), Prop(Items, Term))).
-endif.
