%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% local server that is responsible for janitoring fusion namespaces on the
%% node if the bucket is deleted or fusion on the bucket is disabled
-module(fusion_local_agent).

-behaviour(gen_server2).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").

-record(state, {state :: fusion_uploaders:state(),
                queue :: pid(),
                deleting :: [binary()],
                failed :: [binary()],
                to_reply :: [{binary(), {pid(), gen_server:reply_tag()}}]}).

-define(DELETE_NAMESPACE, ?get_timeout(delete_namespace, 60000)).
-define(RETRY_INTERVAL, ?get_timeout(retry_interval, 30000)).

-export([start_link/0, get_state/0, get_states/2, delete_namespace/1]).
-export([init/1, handle_info/2, handle_call/3]).

-type state() :: {fusion_uploaders:state(), [binary()]}.

-define(UPLOADERS_STOP_TIMEOUT, ?get_timeout(uploaders_stop, 60000)).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_state() -> state().
get_state() ->
    gen_server2:call(?MODULE, get_state).

-spec get_states([node()], integer()) ->
          {error, [node()]} | {ok, [{node(), state()}]}.
get_states(Nodes, Timeout) ->
    {Replies, BadNodes} =
        gen_server:multi_call(Nodes, ?MODULE, get_state, Timeout),
    case BadNodes of
        [] ->
            {ok, Replies};
        _ ->
            {error, BadNodes}
    end.

-spec delete_namespace(ns_bucket:name()) -> ok | error.
delete_namespace(BucketName) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    case should_have_namespace(BucketConfig) of
        true ->
            ?log_info("Deleting fusion namespace for ~p", [BucketName]),
            gen_server2:call(?MODULE, {delete, BucketName}, ?DELETE_NAMESPACE);
        false ->
            ok
    end.

init([]) ->
    Self = self(),
    FusionSettingsKey = fusion_uploaders:config_key(),
    chronicle_compat_events:subscribe(
      fun (Key) when Key =:= FusionSettingsKey ->
              Self ! check_state_and_buckets;
          (Key) ->
              case ns_bucket:buckets_change(Key) of
                  false ->
                      ok;
                  true ->
                      Self ! check_state_and_buckets
              end
      end),

    Self ! check_state_and_buckets,
    {ok, Pid} = work_queue:start_link(),
    {ok, #state{state = disabled,
                queue = Pid,
                deleting = [],
                failed = [],
                to_reply = []}}.

handle_call(get_state, _From, State = #state{state = FusionState,
                                             deleting = Deleting,
                                             failed = Failed}) ->
    {reply, {FusionState, Deleting ++ Failed}, State};
handle_call({delete, BucketName}, From,
            State = #state{deleting = Deleting, to_reply = ToReply}) ->
    Namespace = namespace(BucketName, direct),
    ?log_debug("Requesting to delete namespace ~p", [Namespace]),
    case lists:member(Namespace, Deleting) of
        true ->
            {noreply, State#state{to_reply = [{Namespace, From} | ToReply]}};
        false ->
            case lists:member(Namespace, get_namespaces()) of
                true ->
                    NewState = schedule_deletes([Namespace], State),
                    {noreply,
                     NewState#state{to_reply = [{Namespace, From} | ToReply]}};
                false ->
                    ?log_debug("Request to delete non-existent namespace ~p",
                               [Namespace]),
                    {reply, ok, State}
            end
    end.

handle_info({delete_finished, Namespace, Result},
            State = #state{deleting = Deleting, to_reply = ToReply,
                           failed = Failed}) ->
    [gen_server2:reply(From, Result) || {NS, From} <- ToReply,
                                        NS =:= Namespace],

    NewState =
        case Result of
            ok ->
                State;
            error ->
                erlang:send_after(?RETRY_INTERVAL, self(),
                                  check_state_and_buckets),
                State#state{failed = [Namespace | Failed]}
        end,
    {noreply,
     NewState#state{
       deleting = Deleting -- [Namespace],
       to_reply = [{NS, From} || {NS, From} <- ToReply, NS =/= Namespace]}};
handle_info(check_state_and_buckets, State) ->
    misc:flush(check_state_and_buckets),
    FusionState = fusion_uploaders:get_state(),
    NewState = State#state{state = FusionState},
    NewState1 =
        case FusionState of
            disabled ->
                NewState;
            _ ->
                maybe_schedule_deletes(NewState)
        end,
    {noreply, NewState1#state{failed = []}}.

get_namespaces() ->
    {ok, Json} =
        ns_memcached:get_fusion_namespaces(
          fusion_uploaders:get_metadata_store_uri()),
    {Parsed} = ejson:decode(Json),
    proplists:get_value(<<"namespaces">>, Parsed).

should_have_namespace(BucketConfig) ->
    lists:member(ns_bucket:get_fusion_state(BucketConfig),
                 [enabled, stopped, stopping]).

namespace(BucketName, Snapshot) ->
    iolist_to_binary(["kv/", ns_bucket:uuid(BucketName, Snapshot)]).

maybe_schedule_deletes(#state{deleting = Deleting} = State) ->
    Snapshot = ns_bucket:get_snapshot(all, [props, uuid]),
    Namespaces = get_namespaces(),

    BucketsThatNeedData =
        [BucketName || {BucketName, BucketConfig} <-
                           ns_bucket:get_buckets(Snapshot),
                       should_have_namespace(BucketConfig)],

    NamespacesToKeep = [namespace(BucketName, Snapshot) ||
                           BucketName <- BucketsThatNeedData],
    ToDelete = (Namespaces -- NamespacesToKeep) -- Deleting,
    schedule_deletes(ToDelete, State).

schedule_deletes([], State) ->
    State;
schedule_deletes(ToDelete, #state{queue = Queue,
                                  deleting = Deleting} = State) ->
    Self = self(),
    ?log_debug("Schedule the following namespaces for deletion: ~p",
               [ToDelete]),
    [work_queue:submit_work(
       Queue, ?cut(delete_data(Self, NS))) || NS <- ToDelete],
    State#state{deleting = Deleting ++ ToDelete}.

wait_for_uploaders_to_stop(_BucketName, 0) ->
    {error, timeout};
wait_for_uploaders_to_stop(BucketName, Tries) ->
    case ns_memcached:get_fusion_uploaders_state(BucketName) of
        {ok, null} ->
            %% this is returned when there are no vbuckets
            ok;
        {ok, {[]}} ->
            ok;
        {ok, {VBucketsInfo}} ->
            case lists:any(
                   fun ({_VBName, {VBStats}}) ->
                           proplists:get_value(<<"state">>, VBStats) =/=
                               <<"disabled">>
                   end, VBucketsInfo) of
                true ->
                    timer:sleep(1000),
                    wait_for_uploaders_to_stop(BucketName, Tries - 1);
                false ->
                    ok
            end;
        bucket_not_found ->
            ok;
        Error ->
            {error, Error}
    end.

pre_delete_data(Namespace) ->
    [_, BucketUUID] = binary:split(Namespace, [<<"/">>]),
    case ns_bucket:uuid2bucket(BucketUUID) of
        {ok, BucketName} ->
            ?log_info("Waiting for bucket ~p uploaders to stop",
                      [BucketName]),
            case wait_for_uploaders_to_stop(
                   BucketName, ?UPLOADERS_STOP_TIMEOUT div 1000) of
                {error, Err} ->
                    ?log_error("Error waiting for uploaders for bucket "
                               "~p to stop: ~p", [BucketName, Err]),
                    error;
                ok ->
                    {ok, lists:flatten(io_lib:format("~p for bucket ~p",
                                                     [Namespace, BucketName]))}
            end;
        {error, not_found} ->
            ?log_info("Bucket with uuid = ~p is not found.", [BucketUUID]),
            {ok, lists:flatten(io_lib:format("~p", [Namespace]))}
    end.

delete_data(Parent, Namespace) ->
    ?log_info("Attempt to delete namespace ~p.", [Namespace]),
    RV =
        case pre_delete_data(Namespace) of
            {ok, NamespaceString} ->
                ?log_info("Start deleting namespace ~s", [NamespaceString]),
                case ns_memcached:delete_fusion_namespace(
                       fusion_uploaders:get_log_store_uri(),
                       fusion_uploaders:get_metadata_store_uri(), Namespace) of
                    ok ->
                        ?log_info("Namespace ~s deleted succesfully",
                                  [NamespaceString]),
                        ok;
                    Error ->
                        ?log_error("Error deleting namespace ~s: ~p",
                                   [NamespaceString, Error]),
                        error
                end;
            error ->
                error
        end,
    Parent ! {delete_finished, Namespace, RV}.
