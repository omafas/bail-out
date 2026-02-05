%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
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
