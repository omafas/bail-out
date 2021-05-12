%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

%% This module implements a global name registry. It's not a general purpose
%% name registry in that it uses certain assumptions about how we register
%% global processes. But that makes the implementation much simpler.
%%
%% The assumptions being made:
%%
%%  - processes are only registered on a master node
%%  - processes live long
%%  - there's no need to unregister processes
%%  - it's uncommon to look for a name that is not registered
%%
%% Brief summary of how things work.
%%
%%  - Each node runs a leader_registry_server process.
%%
%%  - Processes can only be registered on the master node (per mb_master
%%  determination).
%%
%%  - On non-master nodes the registry processes simply keep a read through
%%  cache of known global processes. That is, on first miss, a request to the
%%  master node is sent. Then the result is cached. The cached process is
%%  monitored and removed from the cache if the process itself or the link to
%%  the master node dies.
%%
%%  - Since processes cannot be unregistered, there's no need to do anything
%%  special about it. Cache invalidation relies on the regular Erlang
%%  monitors.

-module(leader_registry_server).

-behaviour(gen_server2).

-export([start_link/0]).

%% name service API
-export([register_name/2, unregister_name/1, whereis_name/1, send/2]).

%% gen_server2 callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([handle_job_death/3]).

-include("cut.hrl").
-include("ns_common.hrl").

-define(SERVER, leader_registry).
-define(TABLE,  leader_registry).

-define(WHEREIS_NAME_REMOTE_TIMEOUT, ?get_timeout(whereis_name_remote, 15000)).

-record(state, { leader :: node() | undefined }).

start_link() ->
    leader_utils:ignore_if_new_orchestraction_disabled(
      fun () ->
              gen_server2:start_link({local, ?SERVER}, ?MODULE, [], [])
      end).

%% actual implementation of the APIs
register_name(Name, Pid) ->
    call({if_leader, {register_name, Name, Pid}}).

unregister_name(Name) ->
    call({if_leader, {unregister_name, Name}}).

whereis_name(Name) ->
    case get_cached_name(Name) of
        {ok, Pid} ->
            Pid;
        not_found ->
            call({whereis_name, Name});
        not_running ->
            %% ETS table doesn't exist, which means the registry process is
            %% not running either. So to prevent annoying crashes in the log
            %% file just return undefined and let the caller retry.
            undefined
    end.

send(Name, Msg) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            Pid ! Msg;
        undefined ->
            exit({badarg, {Name, Msg}})
    end.

%% gen_server2 callbacks
init([]) ->
    process_flag(priority, high),

    Self = self(),
    ns_pubsub:subscribe_link(leader_events,
                             fun (Event) ->
                                     case Event of
                                         {new_leader, _} ->
                                             gen_server2:cast(Self, Event);
                                         _ ->
                                             ok
                                     end
                             end),

    ets:new(?TABLE, [named_table, set, protected]),

    %% At this point mb_master is not running yet, so we can't get the current
    %% leader, but we'll get an event with the master pretty soon.
    {ok, #state{leader = undefined}}.

handle_call({if_leader, Call}, From, State) ->
    case is_leader(State) of
        true ->
            {noreply, handle_leader_call(Call, From, State)};
        false ->
            {reply, {error, not_a_leader}, State}
    end;
handle_call({whereis_name, Name}, From, State) ->
    {noreply, handle_whereis_name(Name, From, State)};
handle_call(_Request, _From, State) ->
    {reply, nack, State}.

handle_cast({new_leader, Leader}, State) ->
    {noreply, handle_new_leader(Leader, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, Reason}, State) ->
    {noreply, handle_down(MRef, Pid, Reason, State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal functions
call(Request) ->
    case gen_server2:call(?SERVER, Request, infinity) of
        {ok, Reply} ->
            Reply;
        {error, Error} ->
            exit(Error)
    end.

reply(From, Reply) ->
    gen_server2:reply(From, {ok, Reply}).

reply_error(From, Error) ->
    gen_server2:reply(From, {error, Error}).

handle_leader_call({whereis_name, Name}, From, State) ->
    %% since this is a leader call, we can be sure that whereis_name
    %% will not result in request to another node
    handle_whereis_name(Name, From, State);
handle_leader_call({register_name, Name, Pid}, From, State) ->
    handle_register_name(Name, Pid, From, State);
handle_leader_call({unregister_name, Name}, From, State) ->
    handle_unregister_name(Name, From, State).

handle_register_name(Name, Pid, From, State) ->
    case get_cached_name(Name) of
        {ok, OtherPid} ->
            reply_error(From, {duplicate_name, Name, Pid, OtherPid}),
            State;
        not_found ->
            cache_name(Name, Pid),
            reply(From, yes),
            State
    end.

handle_unregister_name(_Name, From, State) ->
    reply_error(From, not_supported),
    State.

handle_whereis_name(Name, From, #state{leader = Leader} = State) ->
    case get_cached_name(Name) of
        {ok, Pid} ->
            reply(From, Pid),
            State;
        not_found ->
            case Leader =:= node() of
                true ->
                    reply(From, undefined),
                    State;
                false ->
                    maybe_spawn_name_resolver(Name, From, State)
            end
    end.

maybe_spawn_name_resolver(_Name, From, #state{leader = undefined} = State) ->
    reply(From, undefined),
    State;
maybe_spawn_name_resolver(Name, From, State) ->
    gen_server2:async_job({resolver, Name}, Name,
                          ?cut(resolve_name_on_leader(Name, State)),
                          fun (MaybePid, NewState) ->
                                  reply(From, MaybePid),
                                  maybe_cache_name(Name, MaybePid),
                                  {noreply, NewState}
                          end),

    State.

resolve_name_on_leader(Name, #state{leader = Leader}) ->
    case gen_server2:call({?SERVER, Leader},
                          {if_leader, {whereis_name, Name}},
                          ?WHEREIS_NAME_REMOTE_TIMEOUT) of
        {ok, Result} ->
            Result;
        {error, not_a_leader} ->
            %% It's possible that we believe somebody is a leader when they
            %% (yet or already) are not. Just say that we don't know where the
            %% name is, similarly to how we behave when we're not aware of the
            %% leader.
            ?log_warning("Failed to resolve name '~p' on node ~p. "
                         "The node is not a leader.", [Name, Leader]),
            undefined
    end.

handle_new_leader(NewLeader, #state{leader = Leader} = State) ->
    case Leader =:= NewLeader of
        true ->
            State;
        false ->
            ?log_debug("New leader is ~p. Invalidating name cache.", [NewLeader]),
            invalidate_everything(State),
            State#state{leader = NewLeader}
    end.

handle_job_death({resolver, Name}, _, Reason) ->
    ?log_error("Resolver for name '~p' failed with reason ~p", [Name, Reason]),
    {continue, undefined}.

handle_down(MRef, Pid, Reason, State) ->
    case ets:lookup(?TABLE, {pid, Pid}) of
        [{_, Name}] ->
            ?log_info("Process ~p registered as '~p' terminated.",
                      [Pid, Name]),
            invalidate_name(Name, Pid),
            State;
        [] ->
            ?log_error("Received unexpected DOWN message: ~p",
                       [{MRef, Pid, Reason}]),
            State
    end.

is_leader(#state{leader = Leader}) ->
    Leader =:= node().

maybe_cache_name(_Name, undefined) ->
    ok;
maybe_cache_name(Name, Pid) when is_pid(Pid) ->
    case get_cached_name(Name) of
        not_found ->
            cache_name(Name, Pid);
        {ok, CachedPid} ->
            true = (CachedPid =:= Pid)
    end.

cache_name(Name, Pid) ->
    _ = erlang:monitor(process, Pid),
    true = ets:insert_new(?TABLE, [{{name, Name}, Pid},
                                   {{pid, Pid}, Name}]),
    ok.

invalidate_everything(State) ->
    lists:foreach(gen_server2:abort_queue(_, undefined, State),
                  gen_server2:get_active_queues()),
    ets:delete_all_objects(?TABLE).

invalidate_name(Name, Pid) ->
    ets:delete(?TABLE, {name, Name}),
    ets:delete(?TABLE, {pid, Pid}).

get_cached_name(Name) ->
    try ets:lookup(?TABLE, {name, Name}) of
        [] ->
            not_found;
        [{_, Pid}] when is_pid(Pid) ->
            {ok, Pid}
    catch
        error:badarg ->
            not_running
    end.
