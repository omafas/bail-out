%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(cb_couch_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Get arguments to pass to couch_app:start. Tries to get those from resource
%% file. In case of error used empty list.
couch_args() ->
    Args =
        try
            ok = application:load(couch),
            {ok, {couch_app, CouchArgs}} = application:get_key(couch, mod),
            CouchArgs
        catch
            _T:_E -> []
        end,
    [fake, Args].

init([]) ->
    {ok, {{one_for_one, 10, 1},
          [{cb_auth_info, {cb_auth_info, start_link, []},
            permanent, brutal_kill, worker, []},
           {couch_app, {couch_app, start, couch_args()},
            permanent, infinity, supervisor, [couch_app]}
          ]}}.
