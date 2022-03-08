%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(ns_babysitter_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 3, 10},
          child_specs()}}.

child_specs() ->
    [{ns_babysitter_log, {ns_babysitter_log, start_link, []},
      permanent, 1000, worker, []}] ++
        case ns_config_default:init_is_enterprise() of
            true ->
                [{encryption_service, {encryption_service, start_link, []},
                  permanent, 1000, worker, []}];
            false ->
                []
        end ++
        [{child_ns_server_sup, {child_ns_server_sup, start_link, []},
          permanent, infinity, supervisor, []},
         {ns_child_ports_sup, {ns_child_ports_sup, start_link, []},
          permanent, infinity, supervisor, []},
         {ns_ports_manager, {ns_ports_manager, start_link, []},
          permanent, 1000, worker, []}].
