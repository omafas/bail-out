%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2015 Couchbase, Inc.
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
