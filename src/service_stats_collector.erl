%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015-2019 Couchbase, Inc.
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
%%
-module(service_stats_collector).

-include("ns_common.hrl").
-include("ns_stats.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/1, service_prefix/1, service_event_name/1,
         global_stat/2, per_item_stat/3]).

%% callbacks
-export([init/1, handle_info/2, grab_stats/1, process_stats/5]).

-record(state, {status :: starting | started,
                service :: atom(),
                default_stats,
                buckets,

                recent_names,
                stale_names}).

-record(stats_accumulators, {
          gauges = [],
          counters = [],
          sys_gauges = [],
          sys_counters = [],
          status = []
         }).

-define(CHECK_STATUS_INTERVAL, 1000).
-define(ROTATE_NAMES_INTERVAL,
        ?get_param(rotate_names_interval, 10 * 60 * 1000)).

server_name(Service) ->
    list_to_atom(?MODULE_STRING "-" ++ atom_to_list(Service:get_type())).

start_link(Service) ->
    base_stats_collector:start_link({local, server_name(Service)}, ?MODULE,
                                    Service).

service_prefix(Service) ->
    "@" ++ atom_to_list(Service:get_type()) ++ "-".

service_stat_prefix(Service) ->
    atom_to_list(Service:get_type()) ++ "_".

service_event_name(Service) ->
    "@" ++ atom_to_list(Service:get_type()).

per_item_stat(Service, Item, Metric) ->
    iolist_to_binary([atom_to_list(Service:get_type()), $/, Item, $/, Metric]).

global_stat(Service, StatName) ->
    iolist_to_binary([atom_to_list(Service:get_type()), $/, StatName]).

init(Service) ->
    Self = self(),
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({buckets, Buckets}) ->
              BucketConfigs = proplists:get_value(configs, Buckets, []),
              BucketsList =
                  ns_bucket:get_bucket_names_of_type(membase, BucketConfigs),
              Self ! {buckets, BucketsList};
          (_) ->
              ok
      end),

    Buckets = lists:map(fun list_to_binary/1,
                        ns_bucket:get_bucket_names_of_type(membase)),
    Defaults = [{global_stat(Service, atom_to_binary(Stat, latin1)), 0}
                || Stat <- Service:get_gauges() ++ Service:get_counters() ++
                       Service:get_computed()],

    self() ! check_status,
    schedule_names_rotation(),

    RecentNames = ets:new(ok, [protected]),
    StaleNames  = ets:new(ok, [protected]),

    {ok, #state{status = starting,
                service = Service,
                buckets = Buckets,
                default_stats = finalize_stats(Defaults),
                recent_names = RecentNames,
                stale_names = StaleNames}}.

find_type(_, []) ->
    not_found;
find_type(Name, [{Type, Metrics} | Rest]) ->
    MaybeMetric = [Name || M <- Metrics,
                           atom_to_binary(M, latin1) =:= Name],

    case MaybeMetric of
        [_] ->
            Type;
        _ ->
            find_type(Name, Rest)
    end.

global_types(Service) ->
    [{#stats_accumulators.sys_gauges, Service:get_service_gauges()},
     {#stats_accumulators.sys_counters, Service:get_service_counters()}].

bucket_types(Service) ->
    [{#stats_accumulators.gauges, Service:get_gauges()},
     {#stats_accumulators.counters, Service:get_counters()}].

do_recognize_name(_Service, <<"needs_restart">>) ->
    {#stats_accumulators.status, index_needs_restart};
do_recognize_name(_Service, <<"num_connections">>) ->
    {#stats_accumulators.status, index_num_connections};
do_recognize_name(Service, K) when is_binary(K) ->
    case find_type(K, global_types(Service)) of
        not_found ->
            do_recognize_complex_name(Service, K);
        Type ->
            NewKey = list_to_binary(service_stat_prefix(Service) ++
                                        binary_to_list(K)),
            {Type, NewKey}
    end;
do_recognize_name(Service, K) ->
    do_recognize_complex_name(Service, K).

do_recognize_complex_name(Service, K) ->
    case Service:split_stat_name(K) of
        [Bucket, Item, Metric] ->
            case find_type(Metric, bucket_types(Service)) of
                not_found ->
                    undefined;
                Type ->
                    {Type, {Bucket, Item, Metric}}
            end;
        [Item, Metric] ->
            case find_type(Metric, global_types(Service)) of
                not_found ->
                    undefined;
                Type ->
                    {Type, {Item, Metric}}
            end;
        _ ->
            undefined
    end.

recognize_name(Name, #state{service = Service} = State) ->
    case lookup_name(Name, State) of
        {ok, Value} ->
            Value;
        not_found ->
            Value = do_recognize_name(Service, Name),
            cache_name(Name, Value, State),
            Value
    end.

lookup_name(Name, #state{stale_names = Stale,
                         recent_names = Recent} = State) ->
    case ets:lookup(Recent, Name) of
        [{_, Value}] ->
            {ok, Value};
        [] ->
            case ets:take(Stale, Name) of
                [{_, Value}] ->
                    cache_name(Name, Value, State),
                    {ok, Value};
                [] ->
                    not_found
            end
    end.

cache_name(Name, Value, #state{recent_names = Recent}) ->
    true = ets:insert_new(Recent, {Name, Value}).

rotate_names(#state{stale_names = Stale,
                    recent_names = Recent} = State) ->
    ets:delete_all_objects(Stale),
    State#state{stale_names = Recent,
                recent_names = Stale}.

schedule_names_rotation() ->
    erlang:send_after(?ROTATE_NAMES_INTERVAL, self(), rotate_names).

massage_stats(GrabbedStats, State) ->
    massage_stats(GrabbedStats, State, #stats_accumulators{}).

massage_stats([], _State, Acc) ->
    Acc;
massage_stats([{K, V} | Rest], State, Acc) ->
    case recognize_name(K, State) of
        undefined ->
            massage_stats(Rest, State, Acc);
        {Pos, NewK} ->
            massage_stats(Rest, State,
                          setelement(Pos, Acc, [{NewK, V} | element(Pos, Acc)]))
    end.

grab_stats(#state{status = starting}) ->
    [];
grab_stats(#state{status = started, service = Service}) ->
    case Service:grab_stats() of
        {ok, {Stats}} when is_list(Stats) ->
            Stats;
        {ok, Other} ->
            ?log_error("Got invalid stats response for ~p:~n~p",
                       [Service, Other]),
            empty_stats;
        {error, _} ->
            empty_stats
    end.

process_stats(TS, GrabbedStats, PrevCounters, PrevTS,
              #state{service = Service,
                     buckets = KnownBuckets,
                     default_stats = Defaults} = State) ->
    MassagedStats = massage_stats(GrabbedStats, State),

    CalculateStats =
        fun (GaugesPos, CountersPos, ComputeGauges) ->
                Gauges0 = element(GaugesPos, MassagedStats),
                Gauges = Service:ComputeGauges(Gauges0) ++ Gauges0,
                Counters = element(CountersPos, MassagedStats),
                base_stats_collector:calculate_counters(TS, Gauges, Counters, PrevCounters, PrevTS)
        end,

    service_status_keeper:update(Service,
                                 MassagedStats#stats_accumulators.status),

    {Stats, SortedBucketCounters} =
        CalculateStats(#stats_accumulators.gauges, #stats_accumulators.counters, compute_gauges),
    {ServiceStats1, SortedServiceCounters} =
        CalculateStats(#stats_accumulators.sys_gauges, #stats_accumulators.sys_counters,
                       compute_service_gauges),

    ServiceStats2 = aggregate_service_stats(Service, ServiceStats1),

    ServiceStats = [{service_event_name(Service),
                     finalize_stats(ServiceStats2)}],
    Prefix = service_prefix(Service),
    AggregatedStats =
        [{Prefix ++ binary_to_list(Bucket), Values} ||
            {Bucket, Values} <-
                aggregate_stats(Service, Stats, KnownBuckets, Defaults)] ++
        ServiceStats,

    AllCounters = SortedBucketCounters ++ SortedServiceCounters,
    SortedCounters = lists:sort(AllCounters),
    {AggregatedStats, SortedCounters, State}.

aggregate_item_stat(Service, Item, Name, Value, Acc) ->
    Global = global_stat(Service, Name),
    PerItem = per_item_stat(Service, Item, Name),

    Acc1 =
        case lists:keyfind(Global, 1, Acc) of
            false ->
                [{Global, Value} | Acc];
            {_, OldV} ->
                lists:keyreplace(Global, 1, Acc, {Global, OldV + Value})
        end,

    [{PerItem, Value} | Acc1].

aggregate_service_stats(Service, Stats) ->
    lists:foldl(
      fun ({{Item, Name}, V}, Acc) ->
              aggregate_item_stat(Service, Item, Name, V, Acc);
          ({Name, V}, Acc) when is_binary(Name) ->
              [{Name, V} | Acc]
      end, [], Stats).

aggregate_stats(Service, Stats, Buckets, Defaults) ->
    do_aggregate_stats(Service, Stats, Buckets, Defaults, []).

do_aggregate_stats(_Service, [], Buckets, Defaults, Acc) ->
    [{B, Defaults} || B <- Buckets] ++ Acc;
do_aggregate_stats(Service, [{{Bucket, _, _}, _} | _] = Stats,
                   Buckets, Defaults, Acc) ->
    {BucketStats, RestStats} =
        aggregate_bucket_stats(Service, Bucket, Stats, Defaults),

    OtherBuckets = lists:delete(Bucket, Buckets),
    do_aggregate_stats(Service, RestStats, OtherBuckets, Defaults,
                       [{Bucket, BucketStats} | Acc]).

aggregate_bucket_stats(Service, Bucket, Stats, Defaults) ->
    do_aggregate_bucket_stats(Service, Defaults, Bucket, Stats).

do_aggregate_bucket_stats(_Service, Acc, _, []) ->
    {finalize_stats(Acc), []};
do_aggregate_bucket_stats(Service, Acc, Bucket,
                          [{{Bucket, Item, Name}, V} | Rest]) ->
    NewAcc = aggregate_item_stat(Service, Item, Name, V, Acc),
    do_aggregate_bucket_stats(Service, NewAcc, Bucket, Rest);
do_aggregate_bucket_stats(_Service, Acc, _, Stats) ->
    {finalize_stats(Acc), Stats}.

finalize_stats(Acc) ->
    lists:keysort(1, Acc).

handle_info({buckets, NewBuckets}, State) ->
    NewBuckets1 = lists:map(fun list_to_binary/1, NewBuckets),
    {noreply, State#state{buckets = NewBuckets1}};
handle_info(check_status, #state{status = starting} = State) ->
    {noreply, check_status(State)};
handle_info(rotate_names, State) ->
    schedule_names_rotation(),
    {noreply, rotate_names(State)};
handle_info(_Info, State) ->
    {noreply, State}.

check_status(#state{service = Service} = State) ->
    ?log_debug("Checking if service ~p is started...", [Service]),
    NewStatus =
        case Service:is_started() of
            true ->
                ?log_debug("Service ~p is started", [Service]),
                started;
            false ->
                erlang:send_after(?CHECK_STATUS_INTERVAL, self(), check_status),
                starting
        end,
    State#state{status = NewStatus}.


-ifdef(TEST).
aggregate_stats_test() ->
    In = [{{<<"a">>, <<"idx1">>, <<"m1">>}, 1},
          {{<<"a">>, <<"idx1">>, <<"m2">>}, 2},
          {{<<"b">>, <<"idx2">>, <<"m1">>}, 3},
          {{<<"b">>, <<"idx2">>, <<"m2">>}, 4},
          {{<<"b">>, <<"idx3">>, <<"m1">>}, 5},
          {{<<"b">>, <<"idx3">>, <<"m2">>}, 6}],
    Out = aggregate_stats(service_index, In, [], []),

    AStats0 = [{<<"index/idx1/m1">>, 1},
               {<<"index/idx1/m2">>, 2},
               {<<"index/m1">>, 1},
               {<<"index/m2">>, 2}],
    BStats0 = [{<<"index/idx2/m1">>, 3},
               {<<"index/idx2/m2">>, 4},
               {<<"index/idx3/m1">>, 5},
               {<<"index/idx3/m2">>, 6},
               {<<"index/m1">>, 3+5},
               {<<"index/m2">>, 4+6}],

    AStats = lists:keysort(1, AStats0),
    BStats = lists:keysort(1, BStats0),

    ?assertEqual(Out,
                 [{<<"b">>, BStats},
                  {<<"a">>, AStats}]).
-endif.
