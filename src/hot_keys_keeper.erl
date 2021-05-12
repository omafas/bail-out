%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc keeps recent hot keys for easy access
%%
-module(hot_keys_keeper).

-include("ns_common.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0,
         bucket_hot_keys/1, bucket_hot_keys/2,
         all_local_hot_keys/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {bucket_hot_keys, local_hot_keys, keys_updater}).

-define(TOP_KEYS_NUMBER, 10).
%% Frequency (in milliseconds) at which to fetch keys.
-define(FETCH_KEYS_PERIOD, 15000).

%%%===================================================================
%%% API
%%%===================================================================

bucket_hot_keys(Bucket) ->
    gen_server:call(?MODULE, {get_keys, Bucket}).

all_local_hot_keys() ->
    gen_server:call(?MODULE, all_local_hot_keys).

bucket_hot_keys(Bucket, Node) ->
    gen_server:call({?MODULE, Node}, {get_local_keys, Bucket}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    send_fetch_keys_msg(),
    {ok, #state{bucket_hot_keys = [], local_hot_keys = []}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({get_keys, BucketName}, _From, State) ->
    Reply = proplists:get_value(BucketName, State#state.bucket_hot_keys),
    {reply, Reply, State};
handle_call(all_local_hot_keys, _From, State) ->
    {reply, State#state.local_hot_keys, State};
handle_call({get_local_keys, BucketName}, _From, State) ->
    Reply = proplists:get_value(BucketName, State#state.local_hot_keys),
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({set_keys, Keys, LocalKeys}, State) ->
    {noreply, State#state{bucket_hot_keys = Keys, local_hot_keys = LocalKeys}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(fetch_keys, #state{keys_updater = P} = State) ->
    send_fetch_keys_msg(),
    Pid = case is_pid(P) andalso is_process_alive(P) of
              true -> P;
              _ -> spawn_link(fun keys_updater_body/0)
          end,
    {noreply, State#state{keys_updater = Pid}};
handle_info(_, State) ->
    {noreply, State}.

send_fetch_keys_msg() ->
    erlang:send_after(?FETCH_KEYS_PERIOD, self(), fetch_keys).

aggregate_key_ops(KeyStats) ->
    OpsTotal = lists:foldl(fun ({StatName, Value}, Acc) ->
                                   case lists:member(StatName, [get_hits, get_misses, cmd_set, incr_hits, incr_misses,
                                                                decr_hits, decr_misses, delete_hits, delete_misses]) of
                                       true -> Acc + Value;
                                       _ -> Acc
                                   end
                           end, 0, KeyStats),
    Time = proplists:get_value(ctime, KeyStats),
    try (OpsTotal/Time) of
        X -> X
    catch
        error:badarith ->
            0
    end.

grab_bucket_topkeys(BucketName) ->
    {ok, RawKeys} = ns_memcached:topkeys(BucketName),
    [{K, [{ops, aggregate_key_ops(V)}]} || {K,V} <- RawKeys].

ops_desc_comparator({_, ValsA}, {_, ValsB}) ->
    proplists:get_value(ops, ValsA) > proplists:get_value(ops, ValsB).

%% @private
%% @doc Merge proplists containing lists.
merge_list_proplists(PL1, PL2) ->
    RL1 = case PL1 of undefined -> []; _ -> PL1 end,
    RL2 = case PL2 of undefined -> []; _ -> PL2 end,
    misc:ukeymergewith(fun ({K, V1}, {_, V2}) -> {K, lists:append(V1,V2)} end, 1, RL1, RL2).

%% primitive, but working. We don't expect many items.
sort_with_limit(Comparator, Limit, Items) ->
    lists:sublist(lists:sort(Comparator, Items),
                  Limit).

keys_updater_body() ->
    {ClusterKeys, LocalKeys} = get_all_keys(),
    gen_server:cast(?MODULE, {set_keys, ClusterKeys, LocalKeys}).

get_all_keys() ->
    LocalKeys = get_local_keys(),
    RemoteKeys = get_remote_keys(),
    MergedKeys = merge_keys(LocalKeys, RemoteKeys),
    {MergedKeys, LocalKeys}.

get_local_keys() ->
    HotKeys = [{Name, top_keys(grab_bucket_topkeys(Name))}
               || {Name, _} <- local_buckets()],
    orddict:from_list(HotKeys).

local_buckets() ->
    ns_bucket:filter_ready_buckets(ns_bucket:get_buckets()).

get_remote_keys() ->
    {RemoteKeys, _BadNodes} =
        mb_grid:aggregate_call(ns_node_disco:nodes_actual_other(), ?MODULE,
                               all_local_hot_keys, fun merge_list_proplists/2,
                               2000),
    orddict:from_list(RemoteKeys).

merge_keys(LocalKeys, RemoteKeys) ->
    [{BucketName, top_keys(Keys)}
     || {BucketName, Keys} <- do_merge_keys(LocalKeys, RemoteKeys)].

do_merge_keys([], RemoteKeys) ->
    RemoteKeys;
do_merge_keys(LocalKeys, RemoteKeys) ->
    orddict:merge(fun append_keys/3, LocalKeys, RemoteKeys).

append_keys(_Bucket, LocalKeys, RemoteKeys) ->
    LocalKeys ++ RemoteKeys.

top_keys(Keys) ->
    sort_with_limit(fun ops_desc_comparator/2,
                    ?TOP_KEYS_NUMBER, Keys).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
