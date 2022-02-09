%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc - This module maintains the state pertaining to janitor cleanup.
%% The orchestrator module will make use of the services provided by this
%% module.
-module(ns_janitor_server).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).

% APIs.
-export([
         start_cleanup/1,
         terminate_cleanup/1,
         request_janitor_run/1,
         delete_bucket_request/1,
         run_cleanup/2
        ]).

%% gen_server callbacks.
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-type janitor_request() :: {janitor_item(), [fun()]}.

-record(state, {janitor_requests = [] :: [janitor_request()],
                unsafe_nodes = [] :: [node()],
                pid = undefined :: undefined | pid(),
                caller_pid = undefined :: undefined | pid(),
                cleanup_done_cb = undefined :: undefined | fun()}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% APIs.
start_cleanup(CB) ->
    gen_server:call(?MODULE, {start_cleanup, CB}, infinity).

terminate_cleanup(Pid) ->
    gen_server:call(?MODULE, {terminate_cleanup, Pid}, infinity).

request_janitor_run(Request) ->
    gen_server:call(?MODULE, {request_janitor_run, Request}, infinity).

delete_bucket_request(BucketName) ->
    gen_server:call(?MODULE, {delete_bucket_request, BucketName}, infinity).

%% gen_server callbacks.
init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({start_cleanup, CB}, From, #state{janitor_requests=[]} = State) ->
    [_|_] = Items = get_janitor_items(),
    Requests = [{Item, []} || Item <- Items],
    handle_call({start_cleanup, CB}, From, State#state{janitor_requests=Requests});

handle_call({start_cleanup, CB}, {CallerPid, _}, #state{janitor_requests=Requests} = State) ->
    maybe_drop_recovery_status(),
    Pid = proc_lib:spawn_link(?MODULE, run_cleanup, [self(), Requests]),
    State1 = State#state{pid = Pid, caller_pid = CallerPid, cleanup_done_cb = CB},
    {reply, {ok, Pid}, State1};

handle_call({terminate_cleanup, CleanupPid}, _From, #state{pid = CleanupPid} = State) ->
    exit(CleanupPid, shutdown),
    {noreply, State1} =
        receive
            {'EXIT', CleanupPid, _} = DeathMsg ->
                handle_info(DeathMsg, State)
        end,
    {reply, ok, State1};
handle_call({terminate_cleanup, _CleanupPid}, _From,
            #state{pid = Pid} = State) when  Pid =:= undefined ->
    %% This can happen when the 'cleanup_done' async event is yet to be processed
    %% by the orchestrator which would mean that orchestrator is still in 'janitor_running'
    %% state and not transitioned into 'idle' state. Now, if the orchestrator receives a
    %% new event then an attempt will be made the terminate.
    {reply, ok, State};

handle_call({request_janitor_run, Request}, _From, State) ->
    {RV, State1} = do_request_janitor_run(Request, State),
    {reply, RV, State1};
handle_call({delete_bucket_request, BucketName}, _From,
            #state{janitor_requests=Requests} = State) ->
    Requests1 =
        case lists:keytake({bucket, BucketName}, 1, Requests) of
            false ->
                Requests;
            {value, BucketRequest, NewRequests} ->
                do_notify_janitor_finished(BucketRequest, bucket_deleted),
                ?log_debug("Deleted bucket ~p from janitor_requests", [BucketName]),
                NewRequests
        end,
    {reply, ok, State#state{janitor_requests = Requests1}}.

handle_cast({cleanup_complete, RetValues, UnsafeNodes},
            #state{janitor_requests = Requests} = State) ->
    %% This contains the results of the janitor run performed as
    %% a batch. We transform the return values pertaining to individual
    %% bucket's run so that the result can be communicated to the
    %% interested parties.
    Out = [case Ret of
               {error, wait_for_memcached_failed, _} ->
                   {Item, warming_up};
               RV ->
                   {Item, RV}
           end || {Item, Ret} <- RetValues],
    ItemRetDict = dict:from_list(Out),

    RestRequests =
        lists:filter(fun({Item, _} = Request) ->
                             case dict:find(Item, ItemRetDict) of
                                 error ->
                                     true;
                                 {ok, Ret} ->
                                     do_notify_janitor_finished(Request, Ret),
                                     false
                             end
                     end, Requests),
    {noreply, State#state{janitor_requests = RestRequests,
                          unsafe_nodes = UnsafeNodes}};

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, #state{pid = Pid,
                                          janitor_requests = Requests,
                                          unsafe_nodes = UnsafeNodes,
                                          caller_pid = CallerPid,
                                          cleanup_done_cb = CB} = State) ->
    NewRequests = case Reason of
                      normal ->
                          %% No need to notify as it will already be done by 'cleanup_complete'.
                          Requests;
                      _ ->
                          Ret = case Reason of
                                    shutdown -> interrupted;
                                    _X       -> janitor_failed
                                end,

                          lists:map(fun({Item, _} = Request) ->
                                            %% Clear the list of callbacks once the requestors
                                            %% have been notified about the reason of exit.
                                            do_notify_janitor_finished(Request, Ret),
                                            {Item, []}
                                    end, Requests)
                  end,

    ok = CB(CallerPid, UnsafeNodes, Pid),
    {noreply, State#state{janitor_requests = NewRequests, pid = undefined, unsafe_nodes = []}};
handle_info(_, State) ->
    {noreply, State}.

terminate(Reason, #state{pid = Pid} = _State) when is_pid(Pid) ->
    misc:terminate_and_wait(Pid, Reason);
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions.
run_cleanup(Parent, Requests) ->
    true = register(cleanup_process, self()),

    {RequestsRV, Reprovision} =
        lists:foldl(
          fun({Item, _}, {OAcc, RAcc}) ->
                  case do_run_cleanup(Item) of
                      {error, unsafe_nodes, Nodes} ->
                          {OAcc, [{Item, Nodes} | RAcc]};
                      RV ->
                          {[{Item, RV} | OAcc], RAcc}
                  end
          end, {[], []}, Requests),

    UnsafeNodes = case Reprovision =/= [] of
                      true ->
                          get_unsafe_nodes_from_reprovision_list(Reprovision);
                      false ->
                          []
                  end,

    %% Return the individual cleanup status back to the parent.
    ok = gen_server:cast(Parent, {cleanup_complete, RequestsRV, UnsafeNodes}).

do_run_cleanup(compat_mode) ->
    RV = compat_mode_manager:consider_switching_compat_mode(),
    case RV of
        unchanged ->
            ok;
        {changed, OldVersion, NewVersion} ->
            Old = ns_cluster_membership:topology_aware_services_for_version(
                    OldVersion),
            New = ns_cluster_membership:topology_aware_services_for_version(
                    NewVersion),
            ActiveNodes = ns_cluster_membership:active_nodes(),
            Services = [S || S <- New -- Old,
                             ns_cluster_membership:service_nodes(
                               ActiveNodes, S) =/= []],
            case Services of
                [] ->
                    ok;
                _ ->
                    ale:info(?USER_LOGGER,
                             "The following topology-aware services will be "
                             "upgraded during the next rebalance: ~p",
                             [Services])
            end
    end,
    RV;
do_run_cleanup(services) ->
    service_janitor:cleanup();
do_run_cleanup({bucket, Bucket}) ->
    ns_janitor:cleanup(Bucket, [consider_resetting_rebalance_status]).

get_unsafe_nodes_from_reprovision_list(ReprovisionList) ->
    %% It is possible that when the janitor cleanup is working its way through
    %% the bucket list the unsafe nodes are found at different junctures. So
    %% we need to merge the unsafe node information obtained from all the bucket
    %% cleanups to determine the final list of unsafe nodes.
    sets:to_list(lists:foldl(
                   fun({_, Nodes}, Acc) ->
                           lists:foldl(
                             fun(Node, A) ->
                                     sets:add_element(Node, A)
                             end, Acc, Nodes)
                   end, sets:new(), ReprovisionList)).

get_janitor_items() ->
    Buckets = [{bucket, B} || B <- ns_bucket:get_bucket_names_of_type(membase)],
    [compat_mode, services | Buckets].

do_request_janitor_run(Request, #state{janitor_requests=Requests} = State) ->
    {Oper, NewRequests} = add_janitor_request(Request, Requests),
    {Oper, State#state{janitor_requests = NewRequests}}.

add_janitor_request(Request, Requests) ->
    add_janitor_request(Request, Requests, []).

add_janitor_request(NewRequest, [], Acc) ->
    {added, lists:reverse([NewRequest | Acc])};
add_janitor_request({NewItem, NewCBs}, [{Item, CBs} | T], Acc)
  when NewItem =:= Item ->
    {found, lists:reverse(Acc, [{Item, lists:umerge(CBs, NewCBs)} | T])};
add_janitor_request(NewRequest, [Request | T], Acc) ->
    add_janitor_request(NewRequest, T, [Request | Acc]).

do_notify_janitor_finished({_Item, Callbacks}, Reason) ->
    lists:foreach(fun(CB) ->
                          CB(Reason)
                  end, Callbacks).

maybe_drop_recovery_status() ->
    ns_config:update(
      fun ({recovery_status, Value} = P) ->
              case Value of
                  not_running ->
                      skip;
                  {running, _Bucket, _UUID} ->
                      ale:info(?USER_LOGGER, "Apparently recovery ns_orchestrator died. "
                               "Dropped stale recovery status ~p", [P]),
                      {update, {recovery_status, not_running}}
              end;
          (_Other) ->
              skip
      end).
