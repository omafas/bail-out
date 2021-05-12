%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(service_stats_collector).

-include("ns_common.hrl").
-include("ns_stats.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([service_prefix/1,
         service_event_name/1,
         global_stat/2,
         per_item_stat/3]).

service_prefix(Service) ->
    "@" ++ atom_to_list(Service:get_type()) ++ "-".

service_event_name(Service) ->
    "@" ++ atom_to_list(Service:get_type()).

per_item_stat(Service, Item, Metric) ->
    iolist_to_binary([atom_to_list(Service:get_type()), $/, Item, $/, Metric]).

global_stat(Service, StatName) ->
    iolist_to_binary([atom_to_list(Service:get_type()), $/, StatName]).
