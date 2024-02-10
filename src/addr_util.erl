%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%% @
%% @Use of this software is governed by the Business Source License included
%% @in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% @in that file, in accordance with the Business Source License, use of this
%% @software will be governed by the Apache License, Version 2.0, included in
%% @the file licenses/APL2.txt.
%% All rights reserved.

-module(addr_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_my_address/0, get_my_address/1]).

%% Find the best IP address we can find for the current host.
get_my_address() ->
    get_my_address("255.255.255.255").

get_my_address(CurrentAddress) ->
    determine_address(CurrentAddress, list_all_addresses()).

determine_address(CurrentAddress, AddrList) ->
    case lists:member(CurrentAddress, AddrList) of
        true -> CurrentAddress;
        _ -> extract_addr(AddrList)
    end.

list_all_addresses() ->
    {ok, AddrInfo} = inet:getifaddrs(),
    CandidateList = lists:map(fun(X) -> element(2, X) end,
                              lists:filter(fun(Y) -> element(1, Y) == addr end,
                                           lists:flatten(lists:map(fun(Z) -> element(2, Z) end,
                                                         AddrInfo)))),
    lists:sort(lists:map(fun(X) -> addr_to_s(X) end,
                         lists:filter(fun is_valid_ip/1,
                                      CandidateList))
               -- ["127.0.0.1"]).

%% check for some common invalid addresses
is_valid_ip(Addr) ->
    case Addr of
        {0, 0, 0, 0} -> false;
        {255, 255, 255, 255} -> false;
        {169, 254, _, _ } -> false;
        {224, _, _, _ } -> false;
        {_, _, _, _} -> true;
        _ -> false
    end.


%% [X,...] -> X
extract_addr([H|_Tl]) ->
    H;
%% [] -> "127.0.0.1"
extract_addr([]) ->
    "127.0.0.1".

%% {1,2,3,4} -> "1.2.3.4"
addr_to_s(T) ->
    string:join(lists:map(fun erlang:integer_to_list/1,
                          tuple_to_list(T)),
                ".").


-ifdef(TEST).
get_my_address_test() ->
    %% Verify the result of this looks like an IP address.
    4 = length(string:tokens(get_my_address(), ".")).

determine_address_test() ->
    "4.1.1.1" = determine_address("255.255.255.255", ["4.1.1.1", "1.2.3.4"]),
    "1.2.3.4" = determine_address("1.2.3.4", ["4.1.1.1", "1.2.3.4"]),
    "127.0.0.1" = determine_address("1.2.3.4", []).

list_all_addresses_test() ->
    %% I can't test too much here since this isn't functional, but
    %% I'll verify we go through the code and return something.
    true = is_list(list_all_addresses()).

is_valid_ip_test() ->
    true = is_valid_ip({192,168,1,1}),
    false = is_valid_ip({169,254,1,1}).

extract_addr_test() ->
    "4.1.1.1" = extract_addr(["4.1.1.1", "1.2.3.4"]),
    "4.1.1.1" = extract_addr(["4.1.1.1"]),
    "127.0.0.1" = extract_addr([]).

addr_to_s_test() ->
    "1.2.3.4" = addr_to_s({1,2,3,4}).
-endif.
