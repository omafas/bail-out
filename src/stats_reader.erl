%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc Read locally stored stats
%%

-module(stats_reader).

-include("cut.hrl").
-include_lib("stdlib/include/qlc.hrl").

-include("ns_common.hrl").
-include("ns_stats.hrl").

-define(TIMEOUT, 5000).

%% For how long to maintain the most recent ns_tick timestamps, in ms.
%% Despite the fact that we need to cover only the most recent minute, we add
%% some time (5 seconds) in order to make sure we are not losing the last
%% timestamp in a minute.
-define(TS_TRACKING_TIME_MSEC, 65000).

-record(state, {bucket,
                last_timestamps}).

-export([start_link/1,
         latest/3, latest/4, latest/5,
         latest_specific_stats/4, latest_specific_stats/5, latest_specific_stats/6]).
-export([code_change/3, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

%%
%% API
%%

%% OTP-20 type spec for gen_server:start_link/4 omits hibernate_after option
%% even though it's supported. This suppresses the dialyzer warning resulting
%% from this.
-dialyzer({no_fail_call, start_link/1}).
start_link(Bucket) ->
    %% After the removal of old stats collectors (and stats_archiver
    %% in particular), atoms from stats_archiver:archives() are not created
    %% anymore, so code that assumes those atoms exist starts to break.
    %% Since all that code is actually backward compatibility code,
    %% it's easier to just create the atoms by calling the function
    %% instead of fixing the code that will be removed.
    case lists:keyfind(Bucket, 1, stats_archiver:archives()) of
        false -> ok;
        _ -> ?log_error("Will never happen")
    end,
    gen_server:start_link({local, server(Bucket)}, ?MODULE, Bucket,
                          [{hibernate_after,
                            ?get_param(hibernate_after, 10000)}]).



%% @doc Get the latest samples for a given interval from the archive
latest(Period, Node, Bucket) when is_atom(Node) ->
    single_node_call(Bucket, Node, {latest, Period});
latest(Period, Nodes, Bucket) when is_list(Nodes), is_list(Bucket) ->
    multi_node_call(Bucket, Nodes, {latest, Period}).

latest(Period, Node, Bucket, N) when is_atom(Node), is_list(Bucket) ->
    single_node_call(Bucket, Node, {latest, Period, N});
latest(Period, Nodes, Bucket, N) when is_list(Nodes), is_list(Bucket) ->
    multi_node_call(Bucket, Nodes, {latest, Period, N}).

latest(Period, Node, Bucket, 1, N) ->
    latest(Period, Node, Bucket, N);
latest(Period, Node, Bucket, Step, N) when is_atom(Node) ->
    single_node_call(Bucket, Node, {latest, Period, Step, N});
latest(Period, Nodes, Bucket, Step, N) when is_list(Nodes) ->
    multi_node_call(Bucket, Nodes, {latest, Period, Step, N}).


%% Get latest values for only the stats specified by the user.
latest_specific_stats(Period, Node, Bucket, all) ->
    latest(Period, Node, Bucket);
latest_specific_stats(Period, Node, Bucket, StatList) when is_atom(Node) ->
    single_node_call(Bucket, Node,  {latest_specific, Period, StatList});
latest_specific_stats(Period, Nodes, Bucket, StatList) when is_list(Nodes), is_list(Bucket) ->
    multi_node_call(Bucket, Nodes,  {latest_specific, Period, StatList}).

latest_specific_stats(Period, Node, Bucket, N, all) ->
    latest(Period, Node, Bucket, N);
latest_specific_stats(Period, Node, Bucket, N, StatList) when is_atom(Node), is_list(Bucket) ->
    single_node_call(Bucket, Node, {latest_specific, Period, N, StatList});
latest_specific_stats(Period, Nodes, Bucket, N, StatList) when is_list(Nodes), is_list(Bucket) ->
    multi_node_call(Bucket, Nodes, {latest_specific, Period, N, StatList}).

latest_specific_stats(Period, Node, Bucket, 1, N, StatList) ->
    latest_specific_stats(Period, Node, Bucket, N, StatList);
latest_specific_stats(Period, Node, Bucket, Step, N, all) ->
    latest(Period, Node, Bucket, Step, N);
latest_specific_stats(Period, Node, Bucket, Step, N, StatList) when is_atom(Node) ->
    single_node_call(Bucket, Node, {latest_specific, Period, Step, N, StatList});
latest_specific_stats(Period, Nodes, Bucket, Step, N, StatList) when is_list(Nodes) ->
    multi_node_call(Bucket, Nodes, {latest_specific, Period, Step, N, StatList}).

%%
%% gen_server callbacks
%%

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


init(Bucket) ->
    ns_pubsub:subscribe_link(ns_tick_event),
    {ok, #state{bucket=Bucket,
                last_timestamps = queue:new()}}.


handle_call({latest, Period}, _From, State) ->
    Res =
        case get_stats(Period, default_step(Period), 1, all, State) of
            %% Converting results for backward compatibility reasons
            {ok, []} -> {error, no_samples};
            {ok, [E]} -> {ok, E};
            {error, _} = Error -> Error
        end,
    {reply, Res, State};

handle_call({latest, Period, N}, _From, State) ->
    {reply, get_stats(Period, default_step(Period), N, all, State), State};

handle_call({latest, Period, Step, N}, _From, State) ->
    {reply, get_stats(Period, Step, N, all, State), State};

handle_call({latest_specific, Period, StatList}, _From, State) ->
    Res =
        case get_stats(Period, default_step(Period), 1, StatList, State) of
            %% Converting results for backward compatibility reasons
            {ok, []} -> {error, no_samples};
            {ok, [E]} -> {ok, E};
            {error, _} = Error -> Error
        end,
    {reply, Res, State};

handle_call({latest_specific, Period, N, StatList}, _From, State) ->
    StatEntries = get_stats(Period, default_step(Period), N, StatList, State),
    {reply, StatEntries, State};

handle_call({latest_specific, Period, Step, N, StatList}, _From, State) ->
    {reply, get_stats(Period, Step, N, StatList, State), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Keep track of last minute ns_tick timestamps, needed for backward compat
%% only. Can be removed when support for all pre-7.0 stats endpoints is dropped.
handle_info({tick, TS}, #state{last_timestamps = LastTSQ} = State) ->
    Now = timestamp_ms(),
    Threshold = Now - ?TS_TRACKING_TIME_MSEC,
    NewLastTSQ = prune_old_ts(queue:in({Now, TS}, LastTSQ), Threshold),
    {noreply, State#state{last_timestamps = NewLastTSQ}};

handle_info(_Msg, State) -> % Don't crash on delayed responses from calls
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


%%
%% Internal functions
%%

single_node_call(Bucket, Node, CallParams) ->
    gen_server:call({server(Bucket), Node}, CallParams).

multi_node_call(Bucket, Nodes, CallParams) ->
    R = {Replies, _} = gen_server:multi_call(Nodes, server(Bucket),
                                             CallParams, ?TIMEOUT),
    log_bad_responses(R),
    Replies.

log_bad_responses({Replies, Zombies}) ->
    case lists:filter(fun ({_, {ok, _}}) -> false; (_) -> true end, Replies) of
        [] ->
            ok;
        BadReplies ->
            ?stats_error("Bad replies: ~p", [BadReplies])
    end,
    case Zombies of
        [] ->
            ok;
        _ ->
            ?stats_error("Some nodes didn't respond: ~p", [Zombies])
    end.


%% @doc Generate a suitable name for the per-bucket gen_server.
server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).

timestamp_ms() ->
    os:system_time(millisecond).

prune_old_ts(Q, Threshold) ->
    case queue:out(Q) of
        {empty, _} ->
            Q;
        {{value, {TS, _}}, NewQ} when TS < Threshold ->
            prune_old_ts(NewQ, Threshold);
        {{value, {_TS, _}}, _NewQ} ->
            Q
    end.

default_step(Period) ->
    {Period, Step, _} = lists:keyfind(Period, 1, stats_archiver:archives()),
    Step.

max_samples_num(Period) ->
    {Period, _, Count} = lists:keyfind(Period, 1, stats_archiver:archives()),
    Count.

%% Backward compatibility function
%% Retrieves stats from prometheus and returns results in pre-7.0 format
%% StatList is the list of pre-7.0 stat names (or atom 'all')
get_stats(Period, Step, N, StatList, #state{bucket=Bucket,
                                            last_timestamps = LastTSQ}) ->
    SamplesNum = min(N, max_samples_num(Period)),
    Now = timestamp_ms(),
    StartTSms = misc:trunc_ts(Now - Step * (SamplesNum + 1) * 1000, Step),
    StartTS = StartTSms / 1000,
    EndTS = Now / 1000,
    Queries = stat_names_mappings:pre_70_stats_to_prom_query(Bucket, StatList),
    case Queries of
        [<<>>] ->
            %% Happens when menelaus_stats tries to guess stat section by
            %% stat name
            {ok, []};
        _ ->
            Settings = prometheus_cfg:settings(),
            case run_queries(Queries, StartTS, EndTS, Step, Settings, []) of
                {ok, JSONList} ->
                    StatEntries = parse_matrix(JSONList, Bucket),
                    Aligned = align_timestamps(StatEntries, EndTS, Period,
                                               Step, LastTSQ),
                    StartIndex = max(length(Aligned) - SamplesNum, 0) + 1,
                    Res = lists:sublist(Aligned, StartIndex, SamplesNum),
                    SortedRes =
                        [E#stat_entry{values = lists:sort(E#stat_entry.values)}
                            || E <- Res],
                    {ok, SortedRes};
                {error, _} = Error ->
                    Error
            end
    end.

run_queries([], _StartTS, _EndTS, _Step, _Settings, Res) -> {ok, Res};
run_queries([Q | Tail], StartTS, EndTS, Step, Settings, Res) ->
    case prometheus:query_range(Q, StartTS, EndTS, Step, 5000, Settings) of
        {ok, JSONList} ->
            run_queries(Tail, StartTS, EndTS, Step, Settings, JSONList ++ Res);
        {error, Error} ->
            {error, Error}
    end.

parse_matrix(JSONList, Bucket) ->
    lists:foldl(
      fun ({JSONProps}, Acc) ->
            add_prom_entry_to_stat_entry_list(Bucket, JSONProps, Acc)
      end, [], JSONList).

add_prom_entry_to_stat_entry_list(Bucket, JSONProps, StatEntries) ->
    JSONMetric = proplists:get_value(<<"metric">>, JSONProps),
    case stat_names_mappings:prom_name_to_pre_70_name(Bucket, JSONMetric) of
        {ok, Name} ->
            JSONValues = proplists:get_value(<<"values">>, JSONProps),
            add_stat_entry(Name, JSONValues, StatEntries, []);
        {error, not_found} ->
            StatEntries
    end.

add_stat_entry(_Name, [], Rest, Res) -> lists:reverse(Res, Rest);
add_stat_entry(Name, [[TS, ValStr] | Tail1], Stats, Res) ->
    NewVal = promQL:parse_value(ValStr),
    NewTS = round(TS * 1000), %% prometheus returns seconds, but stat_entry
                              %% record uses milliseconds
    case Stats of
        [] ->
            NewEntry = #stat_entry{timestamp = NewTS,
                                   values = [{Name, NewVal}]},
            add_stat_entry(Name, Tail1, Stats, [NewEntry | Res]);
        [#stat_entry{timestamp = NewTS, values = Values} | Tail2] ->
            NewEntry = #stat_entry{timestamp = NewTS,
                                   values = [{Name, NewVal}|Values]},
            add_stat_entry(Name, Tail1, Tail2, [NewEntry | Res]);
        [#stat_entry{timestamp = AnotherTS} | _] when AnotherTS > NewTS ->
            NewEntry = #stat_entry{timestamp = NewTS,
                                   values = [{Name, NewVal}]},
            add_stat_entry(Name, Tail1, Stats, [NewEntry | Res]);
        [#stat_entry{timestamp = AnotherTS} = Entry | Tail2]
                                                when AnotherTS < NewTS ->
            add_stat_entry(Name, [[TS, ValStr] | Tail1], Tail2,
                           [Entry | Res])
    end.

%% Pre-7.0 nodes expect us to use exactly ns_tick timestamps for the last
%% minute stats, but prometheus knows nothing about ns_tick. As a workaround,
%% for backward compatibility reasons, we replace prometheus timestamps with
%% ones that were received from ns_tick. Since both of them are for the last
%% minute and retrieved with 1s granularity, the real difference between them
%% should be less then 1s which is acceptable
align_timestamps(StatEntries, EndTS, minute, 1, TimestampsQ) ->
    Timestamps = lists:reverse([T || {_, T} <- queue:to_list(TimestampsQ)]),
    Length = length(Timestamps),
    {Results, _, _} =
        lists:foldr(
          fun (_, {Res, [], PassedNum}) ->
                  {Res, [], PassedNum};
              (#stat_entry{timestamp = TS} = E,
               {Res, LeftTimestamps, PassedNum}) ->
                  N = round(EndTS * 1000 - TS) div 1000 - PassedNum,
                  case (N >= 0) and (N < Length - PassedNum - N) of
                      true ->
                          NewLeftTimestamps = lists:nthtail(N, LeftTimestamps),
                          [NewTS | _] = NewLeftTimestamps,
                          {[E#stat_entry{timestamp = NewTS} | Res],
                           NewLeftTimestamps, PassedNum + N};
                      false ->
                          {Res, [], PassedNum}
                  end
          end, {[], Timestamps, 0}, StatEntries),
    Results;
%% Pre-7.0 nodes expect all timestamps (except last minute stats) to be aligned
%% with respect to stats collection step. For example, if step is 4s
%% the timestamps should be as follows:
%% 1589849732000, 1589849736000, 1589849740000
%% Another thing that needs to be handled here is time correction for the case
%% when nodes are not syncronized. Since pre-7.0 nodes use ns_tick timestamps,
%% they don't need the time to be synchronized accross cluster. In 7.0 each node
%% uses its own system time when collecting stats and it's required to
%% syncronize time, but to stay compatible with previous versions we have to
%% mimic pre-7.0 behavior and handle the situation when time is out of sync.
align_timestamps(StatEntries, _EndTS, _Period, Step, TimestampsQ) ->
    case queue:is_empty(TimestampsQ) of
        true ->
            [];
        false ->
            Timestamps = queue:to_list(TimestampsQ),
            %% Estimation for time difference between nodes
            Corr = lists:max([T2 - T1 || {T1, T2} <- Timestamps]),
            {value, {MinTS, _}} = queue:peek(TimestampsQ),
            OneMinAgoTS = timestamp_ms() - 60000,
            lists:filtermap(
              %% Don't return stats from interval Now - 1m < X < MinTS
              %% to avoid artifacts on minute graphs when node starts
              fun (#stat_entry{timestamp = TS} = E) when TS < OneMinAgoTS;
                                                         TS > MinTS ->
                      NewTS = misc:trunc_ts(TS + Corr, Step),
                      {true, E#stat_entry{timestamp = NewTS}};
                  (_) ->
                      false
              end, StatEntries)
    end.

