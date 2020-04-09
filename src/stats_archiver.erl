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
%% @doc Store and aggregate statistics collected from stats_collector into a
%% collection of ETS tables, emitting 'sample_archived' events when aggregates
%% are created. The contents of ETS table is periodically dumped to files that
%% then used to restore ETS tables after restart.
%%

-module(stats_archiver).

-include("cut.hrl").
-include("ns_common.hrl").
-include("ns_stats.hrl").

-behaviour(gen_server).

-record(state, {bucket :: bucket_name(),
                saver :: undefined | pid()}).

-export([start_link/1,
         archives/0,
         table/2,
         avg/2,
         latest_sample/2,
         wipe/0]).

-export([fake_historic_stats/0]).

-export([code_change/3, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(BACKUP_INTERVAL, ?get_param(backup_interval, 120000)).


%%
%% API
%%

start_link(Bucket) ->
    gen_server:start_link({local, server(Bucket)}, ?MODULE, Bucket, []).


%% @doc the type of statistics collected
%% {Period, Seconds, Samples}
archives() ->
    [{minute, 1,     60},
     {hour,   4,     900},
     {day,    60,    1440}, % 24 hours
     {week,   600,   1152}, % eight days (computer weeks)
     {month,  1800,  1488}, % 31 days
     {year,   21600, 1464}]. % 366 days


%% @doc Generate a suitable name for the ETS stats table.
table(Bucket, Period) ->
    list_to_atom(fmt("~s-~s-~s", [?MODULE_STRING, Bucket, Period])).

logger_file(Bucket, Period) ->
    Name = io_lib:format("~s-~s.~s.gz", [?MODULE_STRING, Bucket, Period]),
    filename:join(stats_dir(), Name).

%% Ensure directory for stats archiver ETS table backup files
ensure_stats_storage() ->
    StatsDir = stats_dir(),
    R = case filelib:ensure_dir(StatsDir) of
            ok ->
                case file:make_dir(StatsDir) of
                    ok ->
                        ok;
                    {error, eexist} ->
                        ok;
                    Error ->
                        Error
                end;
            Error ->
                Error
        end,

    case R of
        ok ->
            ok;
        _ ->
            ?log_error("Failed to create ETS stats directory with error: ~p~n", [R])
    end,

    R.

%% @doc Compute the average of a list of entries.
-spec avg(atom() | integer(), list()) -> #stat_entry{}.
avg(TS, [First|Rest]) ->
    Merge = fun(E, Acc) ->
                    orddict:merge(fun (_Key, A, B) ->
                                          A + B
                                  end,
                                  Acc, E#stat_entry.values)
            end,
    Sums = lists:foldl(Merge, First#stat_entry.values, Rest),
    Count = 1 + length(Rest),
    #stat_entry{timestamp = TS,
                values = orddict:map(fun (_Key, Value) ->
                                             Value / Count
                                     end, Sums)}.

%% @doc Fetch the latest stats sample
latest_sample(Bucket, Period) ->
    Tab = table(Bucket, Period),
    case ets:last(Tab) of
        '$end_of_table' ->
            {error, no_samples};
        Key ->
            {_, Sample} = hd(ets:lookup(Tab, Key)),
            {ok, Sample}
    end.

%% This function is called when ns_server_sup is shut down. So we don't race
%% with 'backup' handler here.
wipe() ->
    R = misc:rm_rf(stats_dir()),
    case R of
        ok ->
            ?log_info("Deleted stats directory.");
        _ ->
            ?log_error("Failed to delete stats directory: ~p", [R])
    end,
    R.

%% Populate historic stats with the data from the latest sample. That way we
%% can see how much memory all stats consume together when a node runs long
%% enough to populate all archives.
fake_historic_stats() ->
    Archivers = [Name || Name <- registered(),
                         lists:prefix(?MODULE_STRING ++ "-",
                                      atom_to_list(Name))],
    misc:parallel_map(
      fun (Archiver) ->
              ?MODULE_STRING ++ "-" ++ Bucket = atom_to_list(Archiver),
              {Bucket, gen_server:call(Archiver, fake_historic_stats, infinity)}
      end, Archivers, infinity).

%%
%% gen_server callbacks
%%

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


init(Bucket) ->
    ok = ensure_stats_storage(),

    self() ! init,

    Archives = archives(),
    lists:foreach(
      fun ({Period, Step, Samples}) ->
              Interval = 100 * Step * Samples,  % Allow to go over by 10% of the
                                                % total samples
              Msg = {truncate, Period, Samples},
              timer2:send_interval(Interval, Msg),
              self() ! Msg
      end, Archives),
    start_cascade_timers(Archives),
    timer2:send_after(rand:uniform(?BACKUP_INTERVAL), backup),

    ns_pubsub:subscribe_link(ns_stats_event,
                             fun stats_event_handler/2,
                             {self(), Bucket}),
    process_flag(trap_exit, true),
    {ok, #state{bucket=Bucket}}.


handle_call(fake_historic_stats, _From, State) ->
    handle_fake_historic_stats(State);
handle_call(Request, _From, State) ->
    {reply, {unhandled, Request}, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(init, State) ->
    create_tables(State#state.bucket),
    {noreply, State};
handle_info({stats, Bucket, Sample}, State) ->
    true = (Bucket =:= State#state.bucket),

    Tab = table(Bucket, minute),
    #stat_entry{timestamp=TS} = Sample,
    ets:insert(Tab, {TS, Sample}),
    gen_event:notify(ns_stats_event, {sample_archived, Bucket, Sample}),
    {noreply, State};
handle_info({truncate, Period, N} = Msg, #state{bucket=Bucket} = State) ->
    flush(Msg),
    Tab = table(Bucket, Period),
    truncate_logger(Tab, N),
    {noreply, State};
handle_info({cascade, Prev, Period, Step} = Msg, #state{bucket=Bucket} = State) ->
    flush(Msg),
    cascade_logger(Bucket, Prev, Period, Step),
    {noreply, State};
handle_info(backup, #state{bucket=Bucket} = State) ->
    misc:flush(backup),
    Pid = proc_lib:spawn_link(
            fun () ->
                    backup_loggers(Bucket)
            end),
    {noreply, State#state{saver = Pid}};
handle_info({'EXIT', Pid, Reason} = Exit, #state{saver = Saver} = State)
  when Pid =:= Saver ->
    case Reason of
        normal ->
            ok;
        _Other ->
            ?log_warning("Saver process terminated abnormally: ~p", [Exit])
    end,
    timer2:send_after(?BACKUP_INTERVAL, backup),
    {noreply, State#state{saver = undefined}};
handle_info({'EXIT', _, _} = Exit, State) ->
    ?log_error("Got unexpected exit message: ~p", [Exit]),
    {stop, {linked_process_died, Exit}, State};
handle_info(Msg, State) -> % Don't crash on delayed responses from calls
    ?log_warning("Got unexpected message: ~p", [Msg]),
    {noreply, State}.


terminate(_Reason, #state{bucket=Bucket} = _State) ->
    backup_loggers(Bucket),
    ok.


%%
%% Internal functions
%%

create_tables(Bucket) ->
    %% create stats logger tables
    [check_logger(Bucket, Period) || {Period, _, _} <- archives()].

read_table(Path, TableName) ->
    ets:new(TableName, [ordered_set, protected, named_table]),
    try
        ok = do_read_table(Path, TableName)
    catch
        T:E ->
            ?log_error("Failed to read table ~p from ~p:~n~p",
                       [TableName, Path, {T, E, erlang:get_stacktrace()}])
    end.

do_read_table(Path, TableName) ->
    Result = misc:with_file(Path, [raw, binary, read],
                            fun (File) ->
                                    pipes:run(pipes:read_file(File),
                                              pipes:gunzip(),
                                              pipes:unmarshal_table(TableName)),
                                    ok
                            end),

    case Result of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        Error ->
            Error
    end.

write_table(Path, TableName) ->
    ok = misc:atomic_write_file(
           Path,
           fun (File) ->
                   pipes:run(pipes:marshal_table(TableName),
                             %% prefer speed over disk space
                             pipes:gzip([{compression_level, 1}]),
                             pipes:write_file(File))
           end).

check_logger(Bucket, Period) ->
    File = logger_file(Bucket, Period),
    read_table(File, table(Bucket, Period)).

backup_logger(Bucket, Period) ->
    Tab = table(Bucket, Period),
    File = logger_file(Bucket, Period),
    write_table(File, Tab).

backup_loggers(Bucket) ->
    lists:foreach(
      fun ({Period, _, _}) ->
              backup_logger(Bucket, Period)
      end, archives()).

%% keep the last N stats samples and delete the rest
truncate_logger(Tab, NumToKeep) ->
    ets:foldr(fun ({Key, _}, I) ->
                      case I >= NumToKeep of
                          true ->
                              ets:delete(Tab, Key);
                          false ->
                              ok
                      end,
                      I + 1
              end, 0, Tab).

cascade_logger(Bucket, Prev, Period, Step) ->
    true = (Period =/= minute),

    PrevTab = table(Bucket, Prev),
    NextTab = table(Bucket, Period),
    case coalesce_stats(PrevTab, Step) of
        false ->
            ok;
        Avg ->
            #stat_entry{timestamp=TS} = Avg,
            ets:insert(NextTab, {TS, Avg})
    end.

coalesce_stats(Tab, Step) ->
    case ets:last(Tab) of
        '$end_of_table' -> false;
        LastTS -> coalesce_stats(Tab, LastTS, Step, [])
    end.

coalesce_stats(Tab, TS, Step, Samples) ->
    [{_, OneSample}] = ets:lookup(Tab, TS),
    Samples1 = [OneSample|Samples],
    PrevTS = ets:prev(Tab, TS),
    T = misc:trunc_ts(TS, Step),
    case PrevTS == '$end_of_table' orelse misc:trunc_ts(PrevTS, Step) /= T of
        false ->
            coalesce_stats(Tab, PrevTS, Step, Samples1);
        true ->
            avg(T, Samples1)
    end.

%% @doc Generate a suitable name for the per-bucket gen_server.
server(Bucket) ->
    list_to_atom(?MODULE_STRING ++ "-" ++ Bucket).


%% @doc Start the timers to cascade samples to the next resolution.
start_cascade_timers([{Prev, _, _} | [{Next, Step, _} | _] = Rest]) ->
    timer2:send_interval(200 * Step, {cascade, Prev, Next, Step}),
    start_cascade_timers(Rest);

start_cascade_timers([_]) ->
    ok.

-spec fmt(string(), list()) -> list().
fmt(Str, Args)  ->
    lists:flatten(io_lib:format(Str, Args)).

stats_dir() ->
    path_config:component_path(data, "stats").

flush(Msg) ->
    N = misc:flush(Msg),
    case N =/= 0 of
        true ->
            ?log_warning("Dropped ~b ~p messages", [N, Msg]);
        false ->
            ok
    end.

stats_event_handler(Event, {Parent, Bucket} = State) ->
    case Event of
        {stats, EventBucket, _}
          when EventBucket =:= Bucket ->
            Parent ! Event;
        _ ->
            ok
    end,
    State.

handle_fake_historic_stats(#state{bucket = Bucket} = State) ->
    Reply =
        case latest_sample(Bucket, minute) of
            {ok, Sample} ->
                lists:foreach(fake_one_archive(Bucket, Sample, _), archives()),
                backup_loggers(Bucket),
                erlang:garbage_collect(),
                ok;
            Error ->
                Error
        end,
    {reply, Reply, State}.

fake_one_archive(Bucket, Sample, {Period, Interval, NumSamples}) ->
    Table = table(Bucket, Period),
    ets:delete_all_objects(Table),

    %% Timestamp is in milliseconds.
    TS = Sample#stat_entry.timestamp,
    Values = Sample#stat_entry.values,

    lists:foreach(
      fun (I) ->
              FakeTS = TS - I * Interval * 1000,
              FakeValues =
                  lists:map(
                    fun ({Key, Value}) ->
                            case is_binary(Key) of
                                true ->
                                    %% If the key is a reference counted
                                    %% binary, we need to copy it. Otherwise
                                    %% all copies of such key are going to
                                    %% point to the same memory block and we
                                    %% won't get a true account of how much
                                    %% memory will be used in the worst case.
                                    {binary:copy(Key), Value};
                                false ->
                                    {Key, Value}
                            end
                    end, Values),
              FakeSample = #stat_entry{timestamp = FakeTS,
                                       values = FakeValues},
              true = ets:insert_new(Table, {FakeTS, FakeSample})
      end, lists:seq(0, NumSamples - 1)).
