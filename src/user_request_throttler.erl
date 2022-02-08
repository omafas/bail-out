%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc This module is intend to track usage and enforce limits of local domain
%% users. The cluster manager limits are,
%% 1. num_concurrent_requests : Number of concurrent requests
%% 2. egress_mib_per_min: Network egress in Mib per min
%% 3. ingress_mib_per_min: Network ingress in Mib per min, only the request body
%% is counted and not the headers.
%% The enforcement and tracking are done at a per node level.

-module(user_request_throttler).

-include("cut.hrl").
-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0, start_limits_cache/0]).

%% gen server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% User limit API calls.
-export([request/2,
         note_egress/2]).

-define(ONE_MINUTE, 60000).
-define(MiB, 1024 * 1024).

%% USER_UUID_LIMITS keep the mapping of user to it's uuid and limits.
%% It's contents are skipped during ets collection, see diag_handler module.
-define(USER_UUID_LIMITS, user_uuid_limits).

%% PID_USER_TABLE tracks the request pid of the user.
-define(PID_USER_TABLE, pid_user_table).

%% USER_STATS tracks the num_concurrent_requests per user.
-define(USER_STATS, user_stats).

%% USER_TIMED_STATS tracks the network ingress and egress of users and is
%% cleared every minute.
-define(USER_TIMED_STATS, user_timed_stats).

-record(state, {cleanup_mref = undefined,
                cleanup_pending = false,
                cleanup_timer = undefined}).

start_limits_cache() ->
    LimitsFilter =
        fun ({limits_version, _V}) ->
                true;
            (_) ->
                false
        end,
    GetVersion =
        fun () ->
                {menelaus_users:get_limits_version()}
        end,
    GetEvents =
        fun () ->
                [{user_storage_events, LimitsFilter}]
        end,

    versioned_cache:start_link(
      ?USER_UUID_LIMITS, 300, fun do_get_user_props/1, GetEvents, GetVersion).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

limit_exceeded_reply(Req, Exceeded) ->
    Msg = iolist_to_binary(io_lib:format("Limit(s) exceeded ~p", [Exceeded])),
    menelaus_util:reply_text(Req, Msg, 429).

request(Req, ReqBody) ->
    case is_throttled(Req) of
        false ->
            ReqBody();
        {true, UUID, Limits} ->
            %% We have to check if user is restricted outside gen_server. If we
            %% do not do this then a rogue user can saturate the gen_server
            %% starving the other users.
            case check_user_restricted(UUID, Limits) of
                {true, Exceeded} ->
                    limit_exceeded_reply(Req, Exceeded);
                false ->
                    %% We check if user is restricted again within gen_server
                    %% because the time of check above, we may have allowed
                    %% multiple requests from the same user, however only few of
                    %% them maybe allowed according to limits set and/or the
                    %% limits may have exceeded between the check and
                    %% incrementing the user usage.
                    case note_identity_request(Req, UUID, Limits) of
                        ok ->
                            try
                                ReqBody()
                            after
                                notify_request_done()
                            end;
                        {error, LimitExceeded} ->
                            limit_exceeded_reply(Req, LimitExceeded)
                    end
            end
    end.

notify_request_done() ->
    gen_server:cast(?MODULE, {request_done, self()}).

is_throttled(Req) ->
    case cluster_compat_mode:should_enforce_limits() of
        true ->
            Identity = menelaus_auth:get_identity(Req),
            case Identity of
                {_, local} ->
                    case get_user_props(Identity) of
                        undefined ->
                            false;
                        {UUID, Limits} ->
                            {true, UUID, Limits}
                    end;
                _ ->
                    false
            end;
        false ->
            false
    end.

note_identity_request(Req, UUID, Limits) ->
    Ingress = case mochiweb_request:recv_body(Req) of
                  undefined -> 0;
                  Body -> iolist_size(Body)
              end,
    Call = {note_identity_request, self(), UUID, Ingress, Limits},
    gen_server:call(?MODULE, Call, infinity).

note_egress(Req, EgressFun) when is_function(EgressFun, 0) ->
    case is_throttled(Req) of
        false ->
            ok;
        {true, UUID, _Limits} ->
            case menelaus_users:is_deleted_user(UUID) of
                true ->
                    ok;
                false ->
                    Egress = EgressFun(),
                    Key = {UUID, egress},
                    ets:update_counter(?USER_TIMED_STATS, Key, Egress,
                                       {Key, 0}),
                    ?MODULE ! {log_stat, Key}
            end
    end;
note_egress(_Req, chunked) ->
    ok;
note_egress(_Req, "") ->
    ok;
note_egress(_Req, <<"">>) ->
    ok;
note_egress(Req, Body) ->
    note_egress(Req, ?cut(iolist_size(Body))).

user_storage_event(Pid, {local_user_deleted, _} = Msg) ->
    Pid ! Msg;
user_storage_event(Pid, {limits_version, {0, _Base}}) ->
    %% user_storage restarted.
    Pid ! cleanup_deleted_user_stats;
user_storage_event(_, _) ->
    ok.

%% gen_server callbacks
init([]) ->
    ?PID_USER_TABLE= ets:new(?PID_USER_TABLE, [named_table, set, protected]),
    ?USER_STATS = ets:new(?USER_STATS, [named_table, set, protected]),
    ?USER_TIMED_STATS = ets:new(?USER_TIMED_STATS,
                                [named_table, set, public,
                                 {write_concurrency, true}]),
    Self = self(),
    ns_pubsub:subscribe_link(user_storage_events,
                             user_storage_event(Self, _)),
    Self ! cleanup_deleted_user_stats,
    erlang:send_after(?ONE_MINUTE, Self, clear_timed_stats),
    {ok, #state{}}.

handle_call({note_identity_request, Pid, UUID, Ingress, Limits}, _From, State) ->
    case check_user_restricted(UUID, Limits) of
        {true, Exceeded} ->
            {reply, {error, Exceeded}, State};
        false ->
            MRef = erlang:monitor(process, Pid),
            case menelaus_users:is_deleted_user(UUID) of
                true ->
                    ok;
                false ->
                    ets:insert_new(?PID_USER_TABLE, {Pid, MRef, UUID}),
                    NRC = ets:update_counter(
                            ?USER_STATS,
                            {UUID, num_concurrent_requests}, 1,
                            {{UUID, num_concurrent_requests}, 0}),
                    IC = ets:update_counter(
                           ?USER_TIMED_STATS, {UUID, ingress}, Ingress,
                           {{UUID, ingress}, 0}),
                    log_stats(num_concurrent_requests, UUID, NRC),
                    log_stats(ingress, UUID, IC)
            end,
            {reply, ok, State}
    end;
handle_call(Request, _From, State) ->
    ?log_error("Got unknown request ~p", [Request]),
    {reply, unhandled, State}.

handle_cast({request_done, Pid}, State) ->
    case ets:take(?PID_USER_TABLE, Pid) of
        [] ->
            %% Can happen when enforce_user_limits is enabled after
            %% note_identity_request but before request_done.
            ok;
        [{Pid, MRef, UUID}] ->
            erlang:demonitor(MRef, [flush]),
            decrement_num_concurrent_request(UUID)
    end,
    {noreply, State};
handle_cast(Cast, State) ->
    ?log_error("Got unknown cast ~p", [Cast]),
    {noreply, State}.

handle_info({log_stat, {UUID, Stat} = Key} = Msg, State) ->
    misc:flush(Msg),
    case ets:lookup(?USER_TIMED_STATS, Key) of
        [] ->
            %% May have cleared the USER_TIMED_STATS
            ok;
        [{Key, Val}] ->
            case menelaus_users:is_deleted_user(UUID) of
                true ->
                    ok;
                false ->
                    log_stats(Stat, UUID, Val)
            end
    end,
    {noreply, State};
handle_info({'DOWN', MRef, process, _Pid, Reason},
            #state{cleanup_mref = MRef,
                   cleanup_pending = Pending} = State) ->
    TRef = case Reason of
               normal ->
                   case Pending of
                       true ->
                           ?log_debug("Re-doing cleanup_deleted_user_stats."),
                           self() ! cleanup_deleted_user_stats,
                           undefined;
                       false ->
                           undefined
                   end;
               _ ->
                   ?log_debug("Failed cleanup_deleted_user_stats, retrying."),
                   erlang:send_after(?ONE_MINUTE, self(),
                                     cleanup_deleted_user_stats)
           end,
    {noreply, State#state{cleanup_mref = undefined,
                          cleanup_pending = false,
                          cleanup_timer = TRef}};
handle_info({'DOWN', MRef, process, Pid, _Reason}, State) ->
    [{Pid, MRef, UUID}] = ets:take(?PID_USER_TABLE, Pid),
    decrement_num_concurrent_request(UUID),
    {noreply, State};
handle_info(clear_timed_stats, State) ->
    true = ets:delete_all_objects(?USER_TIMED_STATS),
    erlang:send_after(?ONE_MINUTE, self(), clear_timed_stats),
    {noreply, State};
handle_info({local_user_deleted, UUID}, State) ->
    ?log_debug("Deleting stats for ~p", [UUID]),
    delete_user_stats(binary_to_list(UUID)),
    {noreply, State};
handle_info(cleanup_deleted_user_stats,
            #state{cleanup_mref = undefined,
                   cleanup_timer = TRef} = State) ->
    cancel_timer(TRef),
    misc:flush(cleanup_deleted_user_stats),
    {Pid, MRef} = misc:spawn_monitor(fun cleanup_deleted_user_stats/0),
    ?log_debug("Clearing all user stats ~p", [{Pid, MRef}]),
    {noreply, State#state{cleanup_mref = MRef,
                          cleanup_pending = false,
                          cleanup_timer = undefined}};
handle_info(cleanup_deleted_user_stats, State) ->
    misc:flush(cleanup_deleted_user_stats),
    {noreply, State#state{cleanup_pending = true}};
handle_info(Msg, State) ->
    ?log_error("Got unknown message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal
cancel_timer(undefined) ->
    ok;
cancel_timer(Tref) ->
    erlang:cancel_timer(Tref).

decrement_num_concurrent_request(UUID) ->
    case menelaus_users:is_deleted_user(UUID) of
        true ->
            ok;
        false ->
            Count = ets:update_counter(?USER_STATS,
                                       {UUID, num_concurrent_requests},
                                       -1),
            case Count of
                0 ->
                    ets:delete(?USER_STATS, {UUID, num_concurrent_requests});
                _ ->
                    ok
            end,
            log_stats(num_concurrent_requests, UUID, Count)
    end.

check_user_restricted(UUID, Limits) ->
    RV = lists:filter(
           fun ({Table, Key}) ->
                   case ets:lookup(Table, {UUID, Key}) of
                       [] ->
                           false;
                       [{_, Cur}] ->
                           case proplists:get_value(Key, Limits) of
                               undefined ->
                                   false;
                               Limit ->
                                   Limit =< Cur
                           end
                   end
           end,
           [{?USER_STATS, num_concurrent_requests},
            {?USER_TIMED_STATS, ingress},
            {?USER_TIMED_STATS, egress}]),
    case RV of
        [] ->
            false;
        _ ->
            Exceeded = [L || {_, L} <- RV],
            [ns_server_stats:notify_counter(
               {<<"limits_exceeded">>,
                [{user_uuid, UUID}, {limit, L}]}) || L <- Exceeded],
            {true, Exceeded}
    end.

get_user_uuid(Identity) ->
    binary_to_list(menelaus_users:get_user_uuid(Identity)).

get_ns_server_limits(Identity) ->
    case menelaus_users:get_user_limits(Identity) of
        undefined ->
            [];
        Limits ->
            lists:map(fun ({ingress_mib_per_min, Val}) ->
                              {ingress, Val * ?MiB};
                          ({egress_mib_per_min, Val}) ->
                              {egress, Val * ?MiB};
                         (Prop) ->
                             Prop
                      end,
                      proplists:get_value(clusterManager, Limits, []))
    end.

get_user_props(Identity) ->
    versioned_cache:get(?USER_UUID_LIMITS, Identity).

do_get_user_props(Identity) ->
    case get_ns_server_limits(Identity) of
        [] ->
            undefined;
        Limits ->
            {get_user_uuid(Identity), Limits}
    end.

log_stats(num_concurrent_requests, UUID, Count) ->
    ns_server_stats:notify_gauge(
      {num_concurrent_requests, [{user_uuid, UUID}]}, Count);
log_stats(Stat, UUID, Count) ->
    StatBin = atom_to_binary(Stat, latin1),
    ns_server_stats:notify_max(
      {{<<StatBin/binary, "_1m_max">>, [{user_uuid, UUID}]}, 60000, 1000},
      Count).

delete_user_stats(UUID) ->
    ets:delete(?USER_STATS, {UUID, num_concurrent_requests}),
    ets:delete(?USER_TIMED_STATS, {UUID, ingress}),
    ets:delete(?USER_TIMED_STATS, {UUID, egress}),
    ns_server_stats:delete_gauge({num_concurrent_requests,
                                  [{user_uuid, UUID}]}),
    lists:foreach(
      fun (Stat) ->
              StatBin = atom_to_binary(Stat, latin1),
              ns_server_stats:delete_max(
                {<<StatBin/binary, "_1m_max">>, [{user_uuid, UUID}]}, 60000)
      end, [egress, ingress]),
    lists:foreach(
      fun (L) ->
              ns_server_stats:delete_counter({<<"limits_exceeded">>,
                                              [{user_uuid, UUID}, {limit, L}]})
      end, [num_concurrent_requests, ingress, egress]).

cleanup_deleted_user_stats() ->
    ns_server_stats:prune(
      fun (Name, Labels) when Name =:= <<"num_concurrent_requests">> orelse
                              Name =:= <<"ingress_1m_max">> orelse
                              Name =:= <<"egress_1m_max">> orelse
                              Name =:= <<"limits_exceeded">> ->
              case proplists:get_value(user_uuid, Labels) of
                  undefined ->
                      false;
                  UUID ->
                      menelaus_users:is_deleted_user(UUID)
              end;
          (_Name, _Labels) ->
              false
      end).
