%% @author Couchbase <info@couchbase.com>
%% @copyright 2024-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
-module(cb_atomic_persistent_term).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0, stop/1, set/2, get_or_set_if_invalid/3]).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    work_queue:start_link(?MODULE).

stop(Reason) ->
    misc:terminate_and_wait(whereis(?MODULE), Reason).

get_or_set_if_invalid(Name, IsValidChecker, ValueFun) ->
    GetValue = fun () ->
                   case persistent_term:get(Name, undefined) of
                       undefined -> {false, undefined};
                       {value, Value} -> {IsValidChecker(Value), {value, Value}}
                   end
               end,
    maybe
        {false, _} ?= GetValue(),
        work_queue:submit_sync_work(
          ?MODULE,
          fun () ->
              case GetValue() of
                  {false, PrevValue} ->
                      case ValueFun(PrevValue) of
                          {ok, NewValue} ->
                              persistent_term:put(Name, {value, NewValue}),
                              {ok, NewValue};
                          {error, _} = Error ->
                              Error
                      end;
                  {true, {value, PrevValue}} ->
                      {ok, PrevValue}
              end
          end)
    else
        {true, {value, V}} -> {ok, V}
    end.

set(Name, SetFun) ->
    work_queue:submit_sync_work(
      ?MODULE,
      fun () ->
          PrevValue = persistent_term:get(Name, undefined),
          case SetFun(PrevValue) of
              {set, Value, Return} ->
                  persistent_term:put(Name, {value, Value}),
                  Return;
              {ignore, Return} ->
                  Return
          end
      end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-ifdef(TEST).

test_get_or_set_if_invalid() ->
    persistent_term:erase(test),
    ?assertEqual({error, test_error},
                 get_or_set_if_invalid(
                   test,
                   fun (_) -> erlang:error(should_not_be_called) end,
                   fun (undefined) -> {error, test_error} end)),
    ?assertError(badarg, persistent_term:get(test)),

    ?assertEqual({ok, 42},
                 get_or_set_if_invalid(
                   test,
                   fun (_) -> erlang:error(should_not_be_called) end,
                   fun (undefined) -> {ok, 42} end)),
    ?assertEqual({value, 42}, persistent_term:get(test)),

    ?assertEqual({ok, 42},
                 get_or_set_if_invalid(
                   test,
                   fun (42) -> true end,
                   fun (_) -> erlang:error(should_not_be_called) end)),
    ?assertEqual({value, 42}, persistent_term:get(test)),

    ?assertEqual({error, test_error},
                 get_or_set_if_invalid(
                   test,
                   fun (42) -> false end,
                   fun ({value, 42}) -> {error, test_error} end)),
    ?assertEqual({value, 42}, persistent_term:get(test)),

    ?assertEqual({ok, 43},
                 get_or_set_if_invalid(
                   test,
                   fun (42) -> false end,
                   fun ({value, 42}) -> {ok, 43} end)),
    ?assertEqual({value, 43}, persistent_term:get(test)).


all_test_() ->
    {setup,
     fun () ->
             {ok, Pid} = start_link(),
             Pid
     end,
     fun (Pid) ->
             erlang:unlink(Pid),
             stop(kill)
     end,
     [
      fun test_get_or_set_if_invalid/0
     ]}.

-endif.