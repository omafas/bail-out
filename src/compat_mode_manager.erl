%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
-module(compat_mode_manager).

-behavior(gen_server2).

-include("ns_common.hrl").

-export([start_link/0, consider_switching_compat_mode/0]).
-export([init/1, handle_call/3]).

%% API
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

consider_switching_compat_mode() ->
    gen_server2:call(?MODULE, consider_switching_compat_mode, infinity).

%% gen_server2 callbacks
init([]) ->
    handle_consider_switching_compat_mode(),
    {ok, unused}.

handle_call(consider_switching_compat_mode, _From, State) ->
    {reply, handle_consider_switching_compat_mode(), State};
handle_call(Call, From, State) ->
    ?log_warning("Received unexpected call ~p from ~p", [Call, From]),
    {reply, nack, State}.

%% internal
handle_consider_switching_compat_mode() ->
    OldVersion = cluster_compat_mode:get_compat_version(),

    case cluster_compat_mode:consider_switching_compat_mode() of
        changed ->
            NewVersion = cluster_compat_mode:get_compat_version(),
            ale:warn(?USER_LOGGER, "Changed cluster compat mode from ~p to ~p",
                     [OldVersion, NewVersion]),
            gen_event:notify(compat_mode_events,
                             {compat_mode_changed, OldVersion, NewVersion}),
            {changed, OldVersion, NewVersion};
        ok ->
            unchanged
    end.
