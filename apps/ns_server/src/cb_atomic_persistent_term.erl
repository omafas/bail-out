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

%% API
-export([start_link/0, stop/1, set/2, get_or_set_if_undefined/3]).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    work_queue:start_link(?MODULE).

stop(Reason) ->
    misc:terminate_and_wait(whereis(?MODULE), Reason).

get_or_set_if_undefined(Name, IsValidChecker, ValueFun) ->
    GetValue = fun () ->
                   case persistent_term:get(Name, undefined) of
                       undefined -> undefined;
                       {value, Value} ->
                           case IsValidChecker(Value) of
                               true -> {value, Value};
                               false -> undefined
                           end
                   end
               end,
    maybe
        undefined ?= GetValue(),
        work_queue:submit_sync_work(
          ?MODULE,
          fun () ->
              case GetValue() of
                  undefined ->
                      case ValueFun() of
                          {ok, Value} ->
                              persistent_term:put(Name, {value, Value}),
                              {ok, Value};
                          {error, _} = Error ->
                              Error
                      end;
                  {value, Value} ->
                      {ok, Value}
              end
          end)
    else
        {value, Value} -> {ok, Value}
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
