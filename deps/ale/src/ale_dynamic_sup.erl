%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ale_dynamic_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_child/3, restart_child/1, stop_child/1]).

%% Supervisor callbacks
-export([init/1]).

%% internal
-export([delay_death/2, delay_death_init/3]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Id, M, Args),
        {Id,
         {?MODULE, delay_death, [{M, start_link, Args}, 1000]},
          permanent, 5000, worker, [?MODULE, M]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Id, Module, Args) ->
    supervisor:start_child(?MODULE,
                           ?CHILD(Id, Module, Args)).

restart_child(Id) ->
    case supervisor:terminate_child(?MODULE, Id) of
        ok ->
            supervisor:restart_child(?MODULE, Id);
        Other ->
            Other
    end.

stop_child(Id) ->
    case supervisor:terminate_child(?MODULE, Id) of
        ok ->
            supervisor:delete_child(?MODULE, Id);
        Other ->
            Other
    end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one, 1000, 10},
           []} }.

%% internal
delay_death(MFA, Timeout) ->
    Parent = self(),
    proc_lib:start_link(?MODULE, delay_death_init, [MFA, Parent, Timeout]).

delay_death_init({M, F, A}, Parent, Timeout) ->
    Start = erlang:monotonic_time(),
    process_flag(trap_exit, true),

    case erlang:apply(M, F, A) of
        {ok, Pid} ->
            proc_lib:init_ack({ok, self()}),
            delay_death_loop(Pid, Parent, Start, Timeout);
        Other ->
            proc_lib:init_ack(Other)
    end.

delay_death_loop(Child, Parent, Start, Timeout) ->
    receive
        {'EXIT', Child, Reason} ->
            handle_child_exit(Reason, Parent, Start, Timeout);
        {'EXIT', Parent, Reason} ->
            handle_parent_exit(Child, Reason);
        _ ->
            delay_death_loop(Child, Parent, Start, Timeout)
    end.

handle_parent_exit(Child, Reason) ->
    exit(Child, Reason),
    receive
        {'EXIT', Child, ChildReason} ->
            exit(ChildReason)
    end.

handle_child_exit(Reason, Parent, Start, Timeout) ->
    TimeSpent = erlang:convert_time_unit(erlang:monotonic_time() - Start, native, millisecond),
    Left = erlang:max(Timeout - TimeSpent, 0),

    receive
        {'EXIT', Parent, _} ->
            %% exit immediately if we're asked to
            ok
    after
        Left ->
            ok
    end,

    exit(Reason).
