%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-module(menelaus_web_sup).

-behaviour(supervisor).
-behaviour(ns_log_categorizing).

-define(START_OK, 1).
-define(START_FAIL, 2).

%% External exports
-export([start_link/0,
         restart_web_servers/0]).

%% supervisor callbacks
-export([init/1]).

%% ns_log_categorizing callbacks.
-export([ns_log_cat/1, ns_log_code_string/1]).

-include("ns_common.hrl").

restart_web_servers() ->
    restartable:restart(?MODULE, menelaus_web).

start_link() ->
    Result = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    Port = menelaus_web:webconfig(port),
    case Result of
        {ok, _Pid} ->
            ?user_log(?START_OK,
                      "Couchbase Server has started on web port ~p on node ~p. Version: ~p.",
                      [Port, node(), ns_info:version(ns_server)]);
        _Err ->
            %% The exact error message is not logged here since this
            %% is a supervisor start, but a more helpful message
            %% should've been logged before.
            ?user_log(?START_FAIL,
                      "Couchbase Server has failed to start on web port ~p on node ~p. " ++
                          "Perhaps another process has taken port ~p already? " ++
                          "If so, please stop that process first before trying again.",
                      [Port, node(), Port])
    end,
    Result.

init([]) ->
    %% We restart menelaus_web from menelaus_event, make sure we start
    %% menelaus_event first so that we don't miss any events where
    %% menelaus_web needs to be restarted. Also, we restart menelaus_web if
    %% menelaus_event crashes.
    Processes = [{menelaus_event,
                  {menelaus_event, start_link, []},
                  permanent, 5000, worker, dynamic},

                 restartable:spec({menelaus_web,
                                   {menelaus_web, start_link, []},
                                   permanent, infinity, supervisor, dynamic})],
    {ok, {{rest_for_one, 10, 10}, Processes}}.

ns_log_cat(?START_OK) ->
    info;
ns_log_cat(?START_FAIL) ->
    crit.

ns_log_code_string(?START_OK) ->
    "web start ok";
ns_log_code_string(?START_FAIL) ->
    "web start fail".
