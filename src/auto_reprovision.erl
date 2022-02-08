%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc - This module contains the logic to reprovision a bucket when
%% the active vbuckets are found to be in "missing" state. Typically,
%% such a scenario would arise in case of ephemeral buckets when the
%% memcached process on a node restarts within the auto-failover
%% timeout.
-module(auto_reprovision).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0]).

%% APIs.
-export([
         enable/1,
         disable/0,
         reset_count/0,
         reprovision_buckets/2,
         get_cleanup_options/0,
         jsonify_cfg/0
        ]).

%% gen_server callbacks.
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-define(SERVER, {via, leader_registry, ?MODULE}).
-define(DEFAULT_MAX_NODES_SUPPORTED, 1).

-record(state, {enabled = false :: boolean(),
                max_nodes = ?DEFAULT_MAX_NODES_SUPPORTED :: integer(),
                count = 0 :: integer()}).

start_link() ->
    misc:start_singleton(gen_server, ?MODULE, [], []).

%% APIs.
-spec enable(integer()) -> ok.
enable(MaxNodes) ->
    call({enable, MaxNodes}).

-spec disable() -> ok.
disable() ->
    call(disable).

-spec reset_count() -> ok.
reset_count() ->
    call(reset_count).

-spec reprovision_buckets([bucket_name()], [node()]) -> ok | {error, term()}.
reprovision_buckets([], _UnsafeNodes) ->
    ok;
reprovision_buckets(Buckets, UnsafeNodes) ->
    call({reprovision_buckets, Buckets, UnsafeNodes}).

-spec get_cleanup_options() -> [term()].
get_cleanup_options() ->
    call(get_cleanup_options).

call(Msg) ->
    misc:wait_for_global_name(?MODULE),
    gen_server:call(?SERVER, Msg, 5000).

%% gen_server callbacks.
init([]) ->
    {Enabled, MaxNodes, Count} = get_reprovision_info(),
    {ok, #state{enabled = Enabled, max_nodes = MaxNodes, count = Count}}.

handle_call({enable, MaxNodes}, _From, #state{count = Count} = State) ->
    ale:info(?USER_LOGGER, "Enabled auto-reprovision config with max_nodes set to ~p", [MaxNodes]),
    ok = persist_config(true, MaxNodes, Count),
    {reply, ok, State#state{enabled = true, max_nodes = MaxNodes, count = Count}};
handle_call(disable, _From, _State) ->
    ok = persist_config(false, ?DEFAULT_MAX_NODES_SUPPORTED, 0),
    {reply, ok, #state{}};
handle_call(reset_count, _From, #state{count = 0} = State) ->
    {reply, ok, State};
handle_call(reset_count, _From, State) ->
    {Enabled, MaxNodes, Count} = get_reprovision_info(),
    ale:info(?USER_LOGGER, "auto-reprovision count reset from ~p", [Count]),
    ok = persist_config(Enabled, MaxNodes, 0),
    {reply, ok, State#state{count = 0}};
handle_call({reprovision_buckets, Buckets, UnsafeNodes}, _From,
            #state{enabled = Enabled, max_nodes = MaxNodes,
                   count = Count} = State) ->
    RCount = MaxNodes - Count,
    Candidates = lists:sublist(UnsafeNodes, RCount),
    NewCount = Count + length(Candidates),

    %% Update the count in auto_reprovision_cfg
    RCfg = [{enabled, Enabled}, {max_nodes, MaxNodes}, {count, NewCount}],

    %% As part of auto-reprovision operation, we mend the maps of all the
    %% affected buckets and update the ns_config along with the adjusted
    %% auto-reprovision count as part of a single transaction. The updated
    %% buckets will be brought online as part of the next janitor cleanup.
    UpdateRV = ns_bucket:update_maps(
                 Buckets,
                 do_reprovision_bucket(_, _, UnsafeNodes, Candidates),
                 [{auto_reprovision_cfg, RCfg}]),

    {RV, State1} =
        case UpdateRV of
            ok ->
                case NewCount >= MaxNodes of
                    true ->
                        ale:info(
                          ?USER_LOGGER,
                          "auto-reprovision is disabled as maximum number of "
                          "nodes (~p) that can be auto-reprovisioned has "
                          "been reached.", [MaxNodes]);
                    false ->
                        ok
                end,

                UUIDs = ns_bucket:uuids(),
                %% Add event log for all the buckets.
                lists:foreach(fun (Bucket) ->
                                      UUID = proplists:get_value(Bucket, UUIDs),
                                      event_log:add_log(
                                        bucket_autoreprovision,
                                        [{bucket, list_to_binary(Bucket)},
                                         {bucket_uuid, UUID},
                                         {nodes, Candidates},
                                         {restarted_on, UnsafeNodes}])
                              end, Buckets),
                {ok, State#state{count = NewCount}};
            Other ->
                {{error, {reprovision_failed, Other}}, State}
        end,
    {reply, RV, State1};

handle_call(get_cleanup_options, _From,
            #state{enabled = Enabled, max_nodes = MaxNodes, count = Count} = State) ->
    {reply, [{check_for_unsafe_nodes, Enabled =:= true andalso Count < MaxNodes}], State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions.
persist_config(Enabled, MaxNodes, Count) ->
    ok = chronicle_compat:set(auto_reprovision_cfg,
                              [{enabled, Enabled},
                               {max_nodes, MaxNodes},
                               {count, Count}]).

get_reprovision_cfg() ->
    chronicle_compat:get(direct, auto_reprovision_cfg, #{required => true}).

jsonify_cfg() ->
    {struct, get_reprovision_cfg()}.

get_reprovision_info() ->
    RCfg = get_reprovision_cfg(),
    {proplists:get_value(enabled, RCfg, true),
     proplists:get_value(max_nodes, RCfg, ?DEFAULT_MAX_NODES_SUPPORTED),
     proplists:get_value(count, RCfg, 0)}.

do_reprovision_bucket(Bucket, Map, UnsafeNodes, Candidates) ->
    %% Since this will be called by the orchestrator immediately after the
    %% cleanup (map fixup would have happened as part of cleanup) we are
    %% just reusing the map in the bucket config.
    true = (Map =/= []),

    NewMap =
        fix_vbucket_map(cluster_compat_mode:preserve_durable_mutations(),
                        Bucket, Map, Candidates),

    case [I || {I, [N|_]} <- misc:enumerate(NewMap, 0),
               N =:= undefined orelse lists:member(N, UnsafeNodes)] of
        [] -> ok;
        MissingVBs ->
            ale:info(?USER_LOGGER,
                     "During auto-reprovision data has been lost on ~B% of "
                     "vbuckets in bucket ~p.",
                     [length(MissingVBs) * 100 div length(NewMap), Bucket])
    end,

    ale:info(?USER_LOGGER,
             "Bucket ~p has been reprovisioned on following nodes: ~p. "
             "Nodes on which the data service restarted: ~p.",
             [Bucket, Candidates, UnsafeNodes]),
    NewMap.

fix_vbucket_map(false, _Bucket, Map, Candidates) ->
    [promote_replica(C, Candidates) || C <- Map];
fix_vbucket_map(true, Bucket, Map, Candidates) ->
    failover:promote_max_replicas(
      Candidates, Bucket, Map, ?cut(promote_replica(_, Candidates))).

promote_replica([Master | Rest] = Chain, Candidates) ->
    case lists:member(Master, Candidates) of
        true ->
            NewChain = Rest ++ [undefined],
            promote_replica(NewChain, Candidates);
        false ->
            Chain
    end.

-ifdef(TEST).

fix_vbucket_map_test_() ->
    failover:fix_vbucket_map_test_wrapper(
      [{"not durability aware",
        fun () ->
                meck:delete(janitor_agent, query_vbuckets, 4, true),

                Map = [[a, c, d, b],
                       [a, b, c, e],
                       [d, c, b, e],
                       [c, d, e, undefined]],

                ?assertEqual(
                   [[c, d, b, undefined],
                    [c, e, undefined, undefined],
                    [d, c, b, e],
                    [c, d, e, undefined]],
                   fix_vbucket_map(false, "test", Map, [a, b])),
                ?assert(meck:validate(janitor_agent))
        end},
       {"durability aware",
        fun () ->
                failover:meck_query_vbuckets(
                  [{c, [0, 1, 2]}, {d, [0, 1, 2]}],
                  [{0, [{c, 8, 8}, {d, 3, 3}]},
                   {1, [{c, 1, 1}, {d, 3, 3}]},
                   {2, [{c, 1, 1}, {d, 3, 3}]}]),

                Map = [[a, b, c, d],
                       [a, b, c, d],
                       [a, c, d, b]],

                ?assertEqual(
                   [[c, d, undefined, undefined],
                    [d, c, undefined, undefined],
                    [d, c, b, undefined]],
                   fix_vbucket_map(true, "test", Map, [a, b])),
                ?assert(meck:validate(janitor_agent))
        end}]).
-endif.
