%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
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
%% @doc Read locally stored stats
%%

-module(stats_reader).

-include_lib("stdlib/include/qlc.hrl").

-include("ns_common.hrl").
-include("ns_stats.hrl").

-define(TIMEOUT, 5000).

-record(state, {bucket}).

-export([start_link/1,
         latest/3, latest/4, latest/5,
         latest_specific_stats/4, latest_specific_stats/5, latest_specific_stats/6]).
-export([code_change/3, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-import(stats_archiver, [avg/2]).

%%
%% API
%%

%% OTP-20 type spec for gen_server:start_link/4 omits hibernate_after option
%% even though it's supported. This suppresses the dialyzer warning resulting
%% from this.
-dialyzer({no_fail_call, start_link/1}).
start_link(Bucket) ->
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
    {ok, #state{bucket=Bucket}}.

handle_call({latest, Period}, _From, #state{bucket=Bucket} = State) ->
    Reply = get_latest_sample(Bucket, Period),
    {reply, Reply, State};
handle_call({latest, Period, N}, _From, #state{bucket=Bucket} = State) ->
    Reply = fetch_latest_sample(Bucket, Period, N),
    {reply, Reply, State};
handle_call({latest, Period, Step, N}, _From, #state{bucket=Bucket} = State) ->
    Reply = resample_latest_sample(Bucket, Period, Step, N),
    {reply, Reply, State};

handle_call({latest_specific, Period, StatList}, _From, #state{bucket=Bucket} = State) ->
    RV = get_latest_sample(Bucket, Period),
    Reply = extract_stats(StatList, RV),
    {reply, Reply, State};

handle_call({latest_specific, Period, N, StatList}, _From, #state{bucket=Bucket} = State) ->
    RV = fetch_latest_sample(Bucket, Period, N),
    Reply = extract_stats(StatList, RV),
    {reply, Reply, State};

handle_call({latest_specific, Period, Step, N, StatList}, _From, #state{bucket=Bucket} = State) ->
    RV = resample_latest_sample(Bucket, Period, Step, N),
    Reply = extract_stats(StatList, RV),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


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

get_latest_sample(Bucket, Period) ->
    stats_archiver:latest_sample(Bucket, Period).

fetch_latest_sample(Bucket, Period, N) ->
    try fetch_latest(Bucket, Period, N) of
        Result -> Result
    catch Type:Err ->
            {error, {Type, Err}}
    end.

resample_latest_sample(Bucket, Period, Step, N) ->
    try resample(Bucket, Period, Step, N) of
        Result -> Result
    catch Type:Err ->
            {error, {Type, Err}}
    end.

extract_stats(StatList, {ok, AllStats}) when is_list(AllStats) ->
    {ok, extract_specific_stats(StatList, AllStats)};
extract_stats(StatList, {ok, AllStats}) ->
    [RV] = extract_specific_stats(StatList, [AllStats]),
    {ok, RV};
extract_stats(_StatList, Other) ->
    Other.

%% Extract values for stats specified by the user from AllStats.
%% AllStats is a list of one or more samples as shown below:
%%    [{stat_entry, timestamp1,
%%                  [{stat1,stat1-val},
%%                   {stat2,stat2-val},
%%                   {...}|...]},
%%     {stat_entry, timestamp2,
%%                  [{stat1,stat1-val},
%%                   {stat2,stat2-val},
%%                   {...}|...]},
%%      ...]
extract_specific_stats(StatList, AllStats) ->
    ExtractAllFun = fun (OneSample, AccAll) ->
                            SV = lists:foldl(
                                   fun (StatName, Acc) ->
                                           [{StatName, proplists:get_value(StatName, OneSample#stat_entry.values, undefined)} | Acc]
                                   end, [], StatList),
                            [#stat_entry{timestamp = OneSample#stat_entry.timestamp,
                                         values = SV} | AccAll]
                    end,
    lists:reverse(lists:foldl(ExtractAllFun, [], AllStats)).

%% @doc Return the last N records starting with the given key from Tab.
fetch_latest(Bucket, Period, N) ->
    case lists:keyfind(Period, 1, stats_archiver:archives()) of
        false ->
            {error, bad_period, Period};
        {_, Interval, _} ->
            Seconds = N * Interval,
            Tab = stats_archiver:table(Bucket, Period),
            case ets:last(Tab) of
                '$end_of_table' ->
                    {ok, []};
                Key ->
                    Oldest = Key - Seconds * 1000 + 500,
                    case qlc:eval(qlc:q([Sample || {TS,Sample} <- ets:table(Tab), TS > Oldest])) of
                        {error, _, _} = Error ->
                            Error;
                        Results ->
                            {ok, Results}
                    end
            end
    end.


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


%% @doc Resample the stats in a table. Only reads the necessary number of rows.
resample(Bucket, Period, Step, N) ->
    Seconds = N * Step,
    Tab = stats_archiver:table(Bucket, Period),
    case ets:last(Tab) of
        '$end_of_table' ->
            {ok, []};
        Key ->
            Oldest = Key - Seconds * 1000 + 500,
            Handle = qlc:q([Sample || {TS, Sample}
                                          <- ets:table(Tab), TS > Oldest]),
            F = fun (#stat_entry{timestamp = T} = Sample,
                     {T1, Acc, Chunk}) ->
                        case misc:trunc_ts(T, Step) of
                            T1 ->
                                {T1, Acc, [Sample|Chunk]};
                            T2 when T1 == undefined ->
                                {T2, Acc, [Sample]};
                            T2 ->
                                {T2, [avg(T1, Chunk)|Acc], [Sample]}
                        end
                end,
            case qlc:fold(F, {undefined, [], []}, Handle) of
                {error, _, _} = Error ->
                    Error;
                {undefined, [], []} ->
                    {ok, []};
                {T, Acc, LastChunk} ->
                    {ok, lists:reverse([avg(T, LastChunk)|Acc])}
            end
    end.


%% @doc Generate a suitable name for the per-bucket gen_server.
server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).
