% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%

-module(json_builder).

-export([to_binary/1,
         prepare_list/1]).

to_binary({list, List}) ->
    [to_binary(A) || A <- List];
to_binary(A) when is_list(A) ->
    iolist_to_binary(A);
to_binary({propset, Props}) when is_list(Props) ->
    {[kv_to_binary(A) || A <- Props]};
to_binary({json, Json}) ->
    Json;
to_binary(A) ->
    A.

key_to_binary(A) when is_list(A) ->
    iolist_to_binary(A);
key_to_binary(A) when is_tuple(A) ->
    iolist_to_binary(io_lib:format("~p", [A]));
key_to_binary(A) ->
    A.

kv_to_binary({K, V}) ->
    {key_to_binary(K), to_binary(V)}.

prepare_list(List) ->
    lists:foldl(
      fun ({_Key, undefined}, Acc) ->
              Acc;
          ({_Key, "undefined"}, Acc) ->
              Acc;
          ({Key, Value}, Acc) ->
              [{key_to_binary(Key), to_binary(Value)} | Acc]
      end, [], List).
