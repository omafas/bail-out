% Copyright (c) 2009-2019, Couchbase, Inc.
% Copyright (c) 2008, Cliff Moon
% Copyright (c) 2008, Powerset, Inc
%
% Use of this software is governed by a BSD-style license that can be
% found in licenses/BSD-moon.txt.
%
% Original Author: Cliff Moon

-module(misc).

-include("ns_common.hrl").
-include_lib("kernel/include/file.hrl").

-include("cut.hrl").
-include("generic.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("triq.hrl").
-endif.

-compile(nowarn_export_all).
-compile(export_all).
-export_type([timer/0, timer/1]).

decode_unsigned_leb128(Binary) ->
    {Size, BinValue, Tail} = take_leb128_chunks(Binary),
    <<Value:Size/unsigned-integer>> = BinValue,
    {Value, Tail}.

take_leb128_chunks(<<0:1, Chunk:7/bitstring, Tail/bitstring>>) ->
    {7, Chunk, Tail};
take_leb128_chunks(<<1:1, Chunk:7/bitstring, Tail/bitstring>>) ->
    {Size2, Chunks2, Tail2} = take_leb128_chunks(Tail),
    {Size2+7, <<Chunks2/bitstring, Chunk/bitstring>>, Tail2}.

encode_unsigned_leb128(Val) ->
    rec_encode_leb128(Val).

rec_encode_leb128(Value) when Value < 16#80 ->
    <<0:1, Value:7/integer>>;

rec_encode_leb128(Value) ->
    Tail = rec_encode_leb128(Value bsr 7),
    <<1:1, Value:7/integer, Tail/binary>>.

-ifdef(TEST).
unsigned_leb128_test() ->
    Extra = <<"random_string">>,
    ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode_unsigned_leb128(624485)),
    ?assertEqual({624485, Extra}, decode_unsigned_leb128(
                                    <<16#E5, 16#8E, 16#26, Extra/binary>>)),

    ?assertEqual(<<127/integer>>, encode_unsigned_leb128(127)),
    ?assertEqual({127, <<>>}, decode_unsigned_leb128(<<127/integer>>)),

    ?assertEqual(<<16#80, 16#81, 16#2>>, encode_unsigned_leb128(16#8080)),
    ?assertEqual({16#8080, <<>>}, decode_unsigned_leb128(
                                    <<16#80, 16#81, 16#2>>)),

    ?assertEqual(<<16#80, 16#1>>, encode_unsigned_leb128(128)),
    ?assertEqual({128, <<>>}, decode_unsigned_leb128(<<16#80, 16#1>>)).
-endif.

shuffle(List) when is_list(List) ->
    [N || {_R, N} <- lists:keysort(1, [{rand:uniform(), X} || X <- List])].

get_days_list() ->
    ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"].

get_utc_offset(LocalTime, UTCTime) ->
    Diff = calendar:datetime_to_gregorian_seconds(LocalTime) -
             calendar:datetime_to_gregorian_seconds(UTCTime),
    {TZHours, TZMin, _} = calendar:seconds_to_time(abs(Diff)),
    case Diff >= 0 of
        true -> {TZHours, TZMin};
        false -> {-TZHours, TZMin}
    end.

% formats time (see erlang:localtime/0) as ISO-8601 text
iso_8601_fmt({{Year,Month,Day},{Hour,Min,Sec}}, Millis, UTCOffset) ->
    TimeS = iso_8601_fmt_datetime({{Year,Month,Day}, {Hour,Min,Sec}}, "-", ":"),
    MilliSecond = case Millis of
                     undefined -> "";
                     _ -> io_lib:format(".~3.10.0B", [Millis])
                  end,
    OffsetS =
        case UTCOffset of
            undefined -> "";
            {0, 0} -> "Z";
            {H, M} ->
                Sign = case H < 0 of
                           true -> "-";
                           false -> "+"
                       end,
                io_lib:format("~s~2.10.0B:~2.10.0B", [Sign, abs(H), M])
        end,
    lists:flatten(TimeS ++ MilliSecond ++ OffsetS).

%% Format see: https://en.wikipedia.org/wiki/ISO_8601
iso_8601_fmt_datetime({{Year,Month,Day},{Hour,Min,Sec}}, DateDelim, TimeDelim) ->
    lists:flatten(
      io_lib:format("~4.10.0B~s~2.10.0B~s~2.10.0BT~2.10.0B~s~2.10.0B~s~2.10.0B",
                    [Year, DateDelim, Month, DateDelim, Day, Hour, TimeDelim,
                     Min, TimeDelim, Sec])).

-ifdef(TEST).
iso_8601_fmt_datetime_test() ->
    ?assertEqual("2007-04-05T14:30:00",
                 iso_8601_fmt_datetime({{2007, 4, 5}, {14, 30, 00}}, "-", ":")),
    ?assertEqual("20070405T143000",
                 iso_8601_fmt_datetime({{2007, 4, 5}, {14, 30, 00}}, "", "")).
-endif.

-spec timestamp_utc_iso8601() -> string().
timestamp_utc_iso8601() ->
    iso_8601_fmt(erlang:universaltime(), undefined, {0, 0}).

%% safe for use in filenames
-spec timestamp_utc_iso8601_basic() -> string().
timestamp_utc_iso8601_basic() ->
    iso_8601_fmt_datetime(erlang:universaltime(), "", "").

%% Time is as per erlang:timestamp/0
timestamp_iso8601(Time, Where) ->
    UTCTime = calendar:now_to_universal_time(Time),
    Millis = erlang:element(3, Time) div 1000,
    case Where of
        local ->
            LocalTime = calendar:now_to_local_time(Time),
            UTCOffset = get_utc_offset(LocalTime, UTCTime),
            iso_8601_fmt(LocalTime, Millis, UTCOffset);
        utc ->
            iso_8601_fmt(UTCTime, Millis, {0,0})
    end.

-ifdef(TEST).
iso_8601_fmt_test() ->
    ?assertEqual("2007-04-05T14:30:00.040Z",
                 iso_8601_fmt({{2007, 4, 5}, {14, 30, 00}}, 40, {0, 0})),
    ?assertEqual("2007-04-05T14:30:00Z",
                 iso_8601_fmt({{2007, 4, 5}, {14, 30, 00}}, undefined, {0, 0})),
    ?assertEqual("2007-04-05T14:30:00",
                 iso_8601_fmt({{2007, 4, 5}, {14, 30, 00}}, undefined, undefined)),
    ?assertEqual("2007-04-05T14:30:00+01:00",
                 iso_8601_fmt({{2007, 4, 5}, {14, 30, 00}}, undefined, {1,0})),
    ?assertEqual("2007-04-05T14:30:00-03:45",
                 iso_8601_fmt({{2007, 4, 5}, {14, 30, 00}}, undefined, {-3,45})),
    ?assertEqual("2007-04-05T14:30:00-23:45",
                 iso_8601_fmt({{2007, 4, 5}, {14, 30, 00}}, undefined, {-23,45})).
-endif.

%% applies (catch Fun(X)) for each element of list in parallel. If
%% execution takes longer than given Timeout it'll exit with reason
%% timeout (which is consistent with behavior of gen_XXX:call
%% modules).  Care is taken to not leave any messages in calling
%% process mailbox and to correctly shutdown any worker processes
%% if/when calling process is killed.
-spec parallel_map(fun((any()) -> any()), [any()], non_neg_integer() | infinity) -> [any()].
parallel_map(Fun, List, Timeout) when is_list(List) andalso is_function(Fun) ->
    case async:run_with_timeout(
           fun () ->
                   async:map(Fun, List)
           end, Timeout) of
        {ok, R} ->
            R;
        {error, timeout} ->
            exit(timeout)
    end.

-ifdef(TEST).
prop_parallel_map_equals_map() ->
    ?FORALL({Fun, Xs}, {triq_utils:random_integer_fun(), list(int())},
            lists:map(Fun, Xs) =:= parallel_map(Fun, Xs, infinity)).
-endif.

-spec parallel_map_partial(Fun, List, Timeout) -> Result when
      Fun :: fun((any()) -> any()),
      List :: [any()],
      Timeout :: non_neg_integer() | infinity,
      Result :: [{ok, any()} | {error, timeout}].
parallel_map_partial(Fun, List, Timeout) ->
    async:with_many(
      Fun, List, [{abort_after, Timeout}],
      fun (Asyncs) ->
              [try
                   {ok, async:wait(A)}
               catch
                   exit:timeout ->
                       {error, timeout}
               end || A <- Asyncs]
      end).

-ifdef(TEST).
prop_parallel_map_partial_equals_map() ->
    ?FORALL({Fun, Xs}, {triq_utils:random_integer_fun(), list(int())},
            lists:map(?cut({ok, Fun(_)}), Xs) =:=
                parallel_map_partial(Fun, Xs, infinity)).

prop_parallel_map_partial_timeout_() ->
    {?FORALL({Fun, Xs}, {triq_utils:random_integer_fun(), list(int())},
             begin
                 TimingOutFun =
                     fun (I) ->
                             case I rem 2 =:= 0 of
                                 true ->
                                     Fun(I);
                                 false ->
                                     timer:sleep(10000)
                             end
                     end,

                 Results = parallel_map_partial(TimingOutFun, Xs, 100),
                 lists:all(fun ({I, Result}) ->
                                   case I rem 2 =:= 0 of
                                       true ->
                                           Result =:= {ok, Fun(I)};
                                       false ->
                                           Result =:= {error, timeout}
                                   end
                           end, lists:zip(Xs, Results))
             end),
     [{iters, 10}]}.
-endif.

gather_dir_info(Name) ->
    case file:list_dir(Name) of
        {ok, Filenames} ->
            [gather_link_info(filename:join(Name, N)) || N <- Filenames];
        Error ->
            Error
    end.

gather_link_info(Name) ->
    case file:read_link_info(Name) of
        {ok, Info} ->
            case Info#file_info.type of
                directory ->
                    {Name, Info, gather_dir_info(Name)};
                _ ->
                    {Name, Info}
            end;
        Error ->
            {Name, Error}
    end.

rm_rf(Name) when is_list(Name) ->
  case rm_rf_is_dir(Name) of
      {ok, false} ->
          file:delete(Name);
      {ok, true} ->
          case file:list_dir(Name) of
              {ok, Filenames} ->
                  case rm_rf_loop(Name, Filenames) of
                      ok ->
                          case file:del_dir(Name) of
                              ok ->
                                  ok;
                              {error, enoent} ->
                                  ok;
                              Error ->
                                  ?log_warning("Cannot delete ~p: ~p~nDir info: ~p",
                                               [Name, Error, gather_dir_info(Name)]),
                                  Error
                          end;
                      Error ->
                          Error
                  end;
              {error, enoent} ->
                  ok;
              {error, Reason} = Error ->
                  ?log_warning("rm_rf failed because ~p", [Reason]),
                  Error
          end;
      {error, enoent} ->
          ok;
      Error ->
          ?log_warning("stat on ~s failed: ~p", [Name, Error]),
          Error
  end.

rm_rf_is_dir(Name) ->
    case file:read_link_info(Name) of
        {ok, Info} ->
            {ok, Info#file_info.type =:= directory};
        Error ->
            Error
    end.

rm_rf_loop(_DirName, []) ->
    ok;
rm_rf_loop(DirName, [F | Files]) ->
    FileName = filename:join(DirName, F),
    case rm_rf(FileName) of
        ok ->
            rm_rf_loop(DirName, Files);
        {error, enoent} ->
            rm_rf_loop(DirName, Files);
        Error ->
            ?log_warning("Cannot delete ~p: ~p", [FileName, Error]),
            Error
    end.

generate_cookie() ->
    binary_to_atom(misc:hexify(crypto:strong_rand_bytes(32)), latin1).

nthreplace(N, E, List) ->
  lists:sublist(List, N-1) ++ [E] ++ lists:nthtail(N, List).

ceiling(X) ->
  T = erlang:trunc(X),
  case (X - T) of
    Neg when Neg < 0 -> T;
    Pos when Pos > 0 -> T + 1;
    _ -> T
  end.

position(Predicate, List) when is_function(Predicate) ->
  position(Predicate, List, 1);

position(E, List) ->
  position(E, List, 1).

position(Predicate, [], _N) when is_function(Predicate) -> false;

position(Predicate, [E|List], N) when is_function(Predicate) ->
  case Predicate(E) of
    true -> N;
    false -> position(Predicate, List, N+1)
  end;

position(_, [], _) -> false;

position(E, [E|_List], N) -> N;

position(E, [_|List], N) -> position(E, List, N+1).

msecs_to_usecs(MilliSec) ->
    MilliSec * 1000.

%% just because we need to mock node() sometimes
this_node() ->
    node().

% Returns just the node name string that's before the '@' char.
% For example, returns "test" instead of "test@myhost.com".
%
node_name_short() ->
    node_name_short(node()).

node_name_short(Node) ->
    [NodeName | _] = string:tokens(atom_to_list(Node), "@"),
    NodeName.

% Node is an atom like some_name@host.foo.bar.com

node_name_host(Node) ->
    [Name, Host | _] = string:tokens(atom_to_list(Node), "@"),
    {Name, Host}.

%% For hidden nodes (babysitter and couchdb) we use aliases as hostnames
%% This alias doesn't work outside the vm, so we need to return localhost
%% ip address in this case
extract_node_address(Node) ->
    extract_node_address(Node, get_net_family()).
extract_node_address(Node, AFamily) ->
    LocalhostAlias = localhost_alias(),
    case node_name_host(Node) of
        {_, LocalhostAlias} -> localhost(AFamily, []);
        {_, Host} -> Host
    end.

% Get an environment variable value or return a default value
getenv_int(VariableName, DefaultValue) ->
    case (catch list_to_integer(os:getenv(VariableName))) of
        EnvBuckets when is_integer(EnvBuckets) -> EnvBuckets;
        _ -> DefaultValue
    end.

% Get an application environment variable, or a defualt value.
get_env_default(Var, Def) ->
    case application:get_env(Var) of
        {ok, Value} -> Value;
        undefined -> Def
    end.

get_env_default(App, Var, Def) ->
    case application:get_env(App, Var) of
        {ok, Value} ->
            Value;
        undefined ->
            Def
    end.

ping_jointo() ->
    case application:get_env(jointo) of
        {ok, NodeName} -> ping_jointo(NodeName);
        X -> X
    end.

ping_jointo(NodeName) ->
    ?log_debug("attempting to contact ~p", [NodeName]),
    case net_adm:ping(NodeName) of
        pong -> ?log_debug("connected to ~p", [NodeName]);
        pang -> {error, io_lib:format("could not ping ~p~n", [NodeName])}
    end.

%% Wait for a process.

wait_for_process(PidOrName, Timeout) ->
    MRef = erlang:monitor(process, PidOrName),
    receive
        {'DOWN', MRef, process, _, _Reason} ->
            ok
    after Timeout ->
            erlang:demonitor(MRef, [flush]),
            {error, timeout}
    end.

-ifdef(TEST).
wait_for_process_test_() ->
    {spawn,
     fun () ->
             %% Normal
             ok = wait_for_process(spawn(fun() -> ok end), 100),
             %% Timeout
             {error, timeout} = wait_for_process(spawn(fun() ->
                                                               timer:sleep(100), ok end),
                                                 1),
             %% Process that exited before we went.
             Pid = spawn(fun() -> ok end),
             ok = wait_for_process(Pid, 100),
             ok = wait_for_process(Pid, 100)
     end}.
-endif.

-spec terminate(pid(), atom()) -> true.
terminate(Pid, normal) ->
    terminate(Pid, shutdown);
terminate(Pid, Reason) ->
    exit(Pid, Reason).

-spec terminate_and_wait(Processes :: pid() | [pid()], Reason :: term()) -> ok.
terminate_and_wait(Process, Reason) when is_pid(Process) ->
    terminate_and_wait([Process], Reason);
terminate_and_wait(Processes, Reason) ->
    [terminate(P, Reason) || P <- Processes],
    [misc:wait_for_process(P, infinity) || P <- Processes],
    ok.

-define(WAIT_FOR_NAME_SLEEP, 200).

%% waits until given name is globally registered. I.e. until calling
%% {via, leader_registry, Name} starts working
wait_for_global_name(Name) ->
    wait_for_global_name(Name, ?get_timeout(wait_for_global_name, 20000)).

wait_for_global_name(Name, TimeoutMillis) ->
    wait_for_name({via, leader_registry, Name}, TimeoutMillis).

wait_for_local_name(Name, TimeoutMillis) ->
    wait_for_name({local, Name}, TimeoutMillis).

wait_for_name(Name, TimeoutMillis) ->
    Tries = (TimeoutMillis + ?WAIT_FOR_NAME_SLEEP-1) div ?WAIT_FOR_NAME_SLEEP,
    wait_for_name_loop(Name, Tries).

wait_for_name_loop(Name, 0) ->
    case is_pid(whereis_name(Name)) of
        true ->
            ok;
        _ -> failed
    end;
wait_for_name_loop(Name, TriesLeft) ->
    case is_pid(whereis_name(Name)) of
        true ->
            ok;
        false ->
            timer:sleep(?WAIT_FOR_NAME_SLEEP),
            wait_for_name_loop(Name, TriesLeft-1)
    end.

whereis_name({global, Name}) ->
    global:whereis_name(Name);
whereis_name({local, Name}) ->
    erlang:whereis(Name);
whereis_name({via, Module, Name}) ->
    Module:whereis_name(Name).


%% Like proc_lib:start_link but allows to specify a node to spawn a process on.
-spec start_link(node(), module(), atom(), [any()]) -> any() | {error, term()}.
start_link(Node, M, F, A)
  when is_atom(Node), is_atom(M), is_atom(F), is_list(A) ->
    Pid = proc_lib:spawn_link(Node, M, F, A),
    sync_wait(Pid).

%% turns _this_ process into gen_server loop. Initializing and
%% registering it properly.
turn_into_gen_server({local, Name}, Mod, Args, GenServerOpts) ->
    erlang:register(Name, self()),
    {ok, State} = Mod:init(Args),
    proc_lib:init_ack({ok, self()}),
    gen_server:enter_loop(Mod, GenServerOpts, State, {local, Name}).

sync_wait(Pid) ->
    receive
        {ack, Pid, Return} ->
            Return;
        {'EXIT', Pid, Reason} ->
            {error, Reason}
    end.

spawn_monitor(F) ->
    Start = make_ref(),
    Parent = self(),

    Pid = proc_lib:spawn(
            fun () ->
                    MRef = erlang:monitor(process, Parent),

                    receive
                        {'DOWN', MRef, process, Parent, Reason} ->
                            exit(Reason);
                        Start ->
                            erlang:demonitor(MRef, [flush]),
                            F()
                    end
            end),

    MRef = erlang:monitor(process, Pid),
    Pid ! Start,

    {Pid, MRef}.

poll_for_condition_rec(Condition, _Sleep, 0) ->
    case Condition() of
        false -> timeout;
        Ret -> Ret
    end;
poll_for_condition_rec(Condition, Sleep, infinity) ->
    case Condition() of
        false ->
            timer:sleep(Sleep),
            poll_for_condition_rec(Condition, Sleep, infinity);
        Ret -> Ret
    end;
poll_for_condition_rec(Condition, Sleep, Counter) ->
    case Condition() of
        false ->
            timer:sleep(Sleep),
            poll_for_condition_rec(Condition, Sleep, Counter-1);
        Ret -> Ret
    end.

poll_for_condition(Condition, Timeout, Sleep) ->
    Times = case Timeout of
                infinity ->
                    infinity;
                _ ->
                    (Timeout + Sleep - 1) div Sleep
            end,
    poll_for_condition_rec(Condition, Sleep, Times).

-ifdef(TEST).
poll_for_condition_test_() ->
    {timeout, 20,
     fun () ->
             true = poll_for_condition(fun () -> true end, 0, 10),
             timeout = poll_for_condition(fun () -> false end, 100, 10),
             Ref = make_ref(),
             self() ! {Ref, 0},
             Fun  = fun() ->
                            Counter = receive
                                          {Ref, C} -> R = C + 1,
                                                      self() ! {Ref, R},
                                                      R
                                      after 0 ->
                                          erlang:error(should_not_happen)
                                      end,
                            Counter > 5
                    end,
             true = poll_for_condition(Fun, 300, 10),
             receive
                 {Ref, _} -> ok
             after 0 ->
                     erlang:error(should_not_happen)
             end
     end}.
-endif.

%% Remove matching messages from the inbox.
%% Returns a count of messages removed.

flush(Msg) -> ?flush(Msg).


%% You know, like in Python
enumerate(List) ->
    enumerate(List, 1).

enumerate([H|T], Start) ->
    [{Start, H}|enumerate(T, Start + 1)];
enumerate([], _) ->
    [].


%% Equivalent of sort|uniq -c
uniqc(List) ->
    uniqc(List, 1, []).

uniqc([], _, Acc) ->
    lists:reverse(Acc);
uniqc([H], Count, Acc) ->
    uniqc([], 0, [{H, Count}|Acc]);
uniqc([H,H|T], Count, Acc) ->
    uniqc([H|T], Count+1, Acc);
uniqc([H1,H2|T], Count, Acc) ->
    uniqc([H2|T], 1, [{H1, Count}|Acc]).

-ifdef(TEST).
uniqc_test() ->
    [{a, 2}, {b, 5}] = uniqc([a, a, b, b, b, b, b]),
    [] = uniqc([]),
    [{c, 1}] = uniqc([c]).
-endif.

unique(Xs) ->
    [X || {X, _} <- uniqc(Xs)].

groupby_map(Fun, List) ->
    Groups  = sort_and_keygroup(1, lists:map(Fun, List)),
    [{Key, [X || {_, X} <- Group]} || {Key, Group} <- Groups].

groupby(Fun, List) ->
    groupby_map(?cut({Fun(_1), _1}), List).

-ifdef(TEST).
groupby_map_test() ->
    List = [{a, 1}, {a, 2}, {b, 2}, {b, 3}],
    ?assertEqual([{a, [1, 2]}, {b, [2, 3]}],
                 groupby_map(fun functools:id/1, List)),

    ?assertEqual([{a, [-1, -2]}, {b, [-2, -3]}],
                 groupby_map(fun ({K, V}) ->
                                     {K, -V}
                             end, List)).

groupby_test() ->
    Groups = groupby(_ rem 2, lists:seq(0, 10)),

    {0, [0,2,4,6,8,10]} = lists:keyfind(0, 1, Groups),
    {1, [1,3,5,7,9]}    = lists:keyfind(1, 1, Groups).
-endif.

keygroup(Index, SortedList) ->
    keygroup(Index, SortedList, []).

keygroup(_, [], Groups) ->
    lists:reverse(Groups);
keygroup(Index, [H|T], Groups) ->
    Key = element(Index, H),
    {G, Rest} = lists:splitwith(fun (Elem) -> element(Index, Elem) == Key end, T),
    keygroup(Index, Rest, [{Key, [H|G]}|Groups]).

-ifdef(TEST).
keygroup_test() ->
    [{a, [{a, 1}, {a, 2}]},
     {b, [{b, 2}, {b, 3}]}] = keygroup(1, [{a, 1}, {a, 2}, {b, 2}, {b, 3}]),
    [] = keygroup(1, []).
-endif.

sort_and_keygroup(Index, List) ->
    keygroup(Index, lists:keysort(Index, List)).

%% Turn [[1, 2, 3], [4, 5, 6], [7, 8, 9]] info
%% [[1, 4, 7], [2, 5, 8], [3, 6, 9]]
rotate(List) ->
    rotate(List, [], [], []).

rotate([], [], [], Acc) ->
    lists:reverse(Acc);
rotate([], Heads, Tails, Acc) ->
    rotate(lists:reverse(Tails), [], [], [lists:reverse(Heads)|Acc]);
rotate([[H|T]|Rest], Heads, Tails, Acc) ->
    rotate(Rest, [H|Heads], [T|Tails], Acc);
rotate(_, [], [], Acc) ->
    lists:reverse(Acc).

-ifdef(TEST).
rotate_test() ->
    [[1, 4, 7], [2, 5, 8], [3, 6, 9]] =
        rotate([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
    [] = rotate([]).
-endif.

rewrite(Fun, Term) ->
    generic:maybe_transform(
      fun (T) ->
              case Fun(T) of
                  continue ->
                      {continue, T};
                  {stop, NewTerm} ->
                      {stop, NewTerm}
              end
      end, Term).

-ifdef(TEST).
rewrite_correctly_callbacks_on_tuples_test() ->
    executing_on_new_process(
      fun () ->
              {a, b, c} =
                  rewrite(
                    fun (Term) ->
                            self() ! {term, Term},
                            continue
                    end, {a, b, c}),
              Terms =
                  letrec(
                    [[]],
                    fun (Rec, Acc) ->
                            receive
                                X ->
                                    {term, T} = X,
                                    Rec(Rec, [T | Acc])
                            after 0 ->
                                    lists:reverse(Acc)
                            end
                    end),
              [{a, b, c}, a, b, c] = Terms
      end).
-endif.

rewrite_value(Old, New, Struct) ->
    generic:transformb(?transform(Old, New), Struct).

rewrite_key_value_tuple(Key, NewValue, Struct) ->
    generic:transformb(?transform({Key, _}, {Key, NewValue}), Struct).

rewrite_tuples(Fun, Struct) ->
    rewrite(
      fun (Term) ->
              case is_tuple(Term) of
                  true ->
                      Fun(Term);
                  false ->
                      continue
              end
      end,
      Struct).

-ifdef(TEST).
rewrite_value_test() ->
    x = rewrite_value(a, b, x),
    b = rewrite_value(a, b, a),
    b = rewrite_value(a, b, b),

    [x, y, z] = rewrite_value(a, b, [x, y, z]),

    [x, b, c, b] = rewrite_value(a, b, [x, a, c, a]),

    {x, y} = rewrite_value(a, b, {x, y}),
    {x, b} = rewrite_value(a, b, {x, a}),

    X = rewrite_value(a, b,
                      [ {"a string", 1, x},
                        {"b string", 4, a, {blah, a, b}}]),
    X = [{"a string", 1, x},
         {"b string", 4, b, {blah, b, b}}],

    % handling of improper list
    [a, [x|c]] = rewrite_value(b, x, [a, [b|c]]),
    [a, [b|x]] = rewrite_value(c, x, [a, [b|c]]).

rewrite_key_value_tuple_test() ->
    x = rewrite_key_value_tuple(a, b, x),
    {a, b} = rewrite_key_value_tuple(a, b, {a, c}),
    {b, x} = rewrite_key_value_tuple(a, b, {b, x}),

    Orig = [ {"a string", x},
             {"b string", 4, {a, x, y}, {a, c}, {a, [b, c]}}],
    X = rewrite_key_value_tuple(a, b, Orig),
    X = [{"a string", x},
         {"b string", 4, {a, x, y}, {a, b}, {a, b}}],

    X1 = rewrite_tuples(fun (T) ->
                                case T of
                                    {"a string", _} ->
                                        {stop, {a_string, xxx}};
                                    {a, _} ->
                                        {stop, {a, xxx}};
                                    _ ->
                                        continue
                                end
                        end, Orig),

    X1 = [{a_string, xxx}, {"b string", 4, {a, x, y}, {a, xxx}, {a, xxx}}].
-endif.

sanitize_url(Url) when is_binary(Url) ->
    list_to_binary(sanitize_url(binary_to_list(Url)));
sanitize_url(Url) when is_list(Url) ->
    HostIndex = string:chr(Url, $@),
    case HostIndex of
        0 ->
            Url;
        _ ->
            AfterScheme = string:str(Url, "://"),
            case AfterScheme of
                0 ->
                    "*****" ++ string:substr(Url, HostIndex);
                _ ->
                    string:substr(Url, 1, AfterScheme + 2) ++ "*****" ++
                        string:substr(Url, HostIndex)
            end
    end;
sanitize_url(Url) ->
    Url.

-ifdef(TEST).
sanitize_url_test() ->
    "blah.com/a/b/c" = sanitize_url("blah.com/a/b/c"),
    "ftp://blah.com" = sanitize_url("ftp://blah.com"),
    "http://*****@blah.com" = sanitize_url("http://user:password@blah.com"),
    "*****@blah.com" = sanitize_url("user:password@blah.com").
-endif.

ukeymergewith(Fun, N, L1, L2) ->
    ukeymergewith(Fun, N, L1, L2, []).

ukeymergewith(_, _, [], [], Out) ->
    lists:reverse(Out);
ukeymergewith(_, _, L1, [], Out) ->
    lists:reverse(Out, L1);
ukeymergewith(_, _, [], L2, Out) ->
    lists:reverse(Out, L2);
ukeymergewith(Fun, N, L1 = [T1|R1], L2 = [T2|R2], Out) ->
    K1 = element(N, T1),
    K2 = element(N, T2),
    case K1 of
        K2 ->
            ukeymergewith(Fun, N, R1, R2, [Fun(T1, T2) | Out]);
        K when K < K2 ->
            ukeymergewith(Fun, N, R1, L2, [T1|Out]);
        _ ->
            ukeymergewith(Fun, N, L1, R2, [T2|Out])
    end.

-ifdef(TEST).
ukeymergewith_test() ->
    Fun = fun ({K, A}, {_, B}) ->
                  {K, A + B}
          end,
    [{a, 3}] = ukeymergewith(Fun, 1, [{a, 1}], [{a, 2}]),
    [{a, 3}, {b, 1}] = ukeymergewith(Fun, 1, [{a, 1}], [{a, 2}, {b, 1}]),
    [{a, 1}, {b, 3}] = ukeymergewith(Fun, 1, [{b, 1}], [{a, 1}, {b, 2}]).
-endif.

%% Given two sorted lists, return a 3-tuple containing the elements
%% that appear only in the first list, only in the second list, or are
%% common to both lists, respectively.
comm([H1|T1] = L1, [H2|T2] = L2) ->
    if H1 == H2 ->
            {R1, R2, R3} = comm(T1, T2),
            {R1, R2, [H1|R3]};
       H1 < H2 ->
            {R1, R2, R3} = comm(T1, L2),
            {[H1|R1], R2, R3};
       true ->
            {R1, R2, R3} = comm(L1, T2),
            {R1, [H2|R2], R3}
    end;
comm(L1, L2) when L1 == []; L2 == [] ->
    {L1, L2, []}.


-ifdef(TEST).
comm_test() ->
    {[1], [2], [3]} = comm([1,3], [2,3]),
    {[1,2,3], [], []} = comm([1,2,3], []),
    {[], [], []} = comm([], []).
-endif.

start_singleton(Module, Name, Args, Opts) ->
    start_singleton(Module, start_link,
                    [{via, leader_registry, Name}, Name, Args, Opts]).

start_singleton(M, F, A) ->
    case erlang:apply(M, F, A) of
        {error, {already_started, Pid}} ->
            ?log_warning("start_singleton(~p, ~p, ~p) -> already started:"
                         " monitoring ~p from ~p",
                         [M, F, A, Pid, node()]),
            {ok, spawn_link(fun () ->
                                    misc:wait_for_process(Pid, infinity),
                                    ?log_info("~p saw ~p exit.",
                                              [self(), Pid])
                            end)};
        {ok, Pid} = X ->
            ?log_info("start_singleton(~p, ~p, ~p):"
                      " started as ~p on ~p~n",
                      [M, F, A, Pid, node()]),
            X;
        X -> X
    end.

key_update_rec(Key, List, Fun, Acc) ->
    case List of
        [{Key, OldValue} | Rest] ->
            %% once we found our key, compute new value and don't recurse anymore
            %% just append rest of list to reversed accumulator
            lists:reverse([{Key, Fun(OldValue)} | Acc],
                          Rest);
        [] ->
            %% if we reach here, then we didn't found our tuple
            false;
        [X | XX] ->
            %% anything that's not our pair is just kept intact
            key_update_rec(Key, XX, Fun, [X | Acc])
    end.

%% replace value of given Key with result of applying Fun on it in
%% given proplist. Preserves order of keys. Assumes Key occurs only
%% once.
key_update(Key, PList, Fun) ->
    key_update_rec(Key, PList, Fun, []).

%% replace values from OldPList with values from NewPList
update_proplist(OldPList, NewPList) ->
    NewPList ++
        lists:filter(fun ({K, _}) ->
                             case lists:keyfind(K, 1, NewPList) of
                                 false -> true;
                                 _ -> false
                             end
                     end, OldPList).

-ifdef(TEST).
update_proplist_test() ->
    [{a, 1}, {b, 2}, {c,3}] =:= update_proplist([{a,2}, {c,3}],
                                                [{a,1}, {b,2}]).
-endif.

%% Returns proplist that contains all the elements from Left and from Right.
%% Calls MergeFun(Key, LeftValue, RightValue) in case if some key
%% is part of both proplists.
merge_proplists(MergeFun, LeftProplist, RightProplist) ->
    lists:foldr(
      fun ({Key, LeftValue}, Acc) ->
          case proplists:get_all_values(Key, Acc) of
              [RightValue] ->
                  [{Key, MergeFun(Key, LeftValue, RightValue)}
                      | proplists:delete(Key, Acc)];
              [] ->
                  [{Key, LeftValue} | Acc]
          end
      end, RightProplist, LeftProplist).

-ifdef(TEST).
merge_proplists_test() ->
    ?assertEqual(
      [], merge_proplists(fun (_, _, _) -> error(never_happens) end, [], [])),
    ?assertEqual(
      [{a, 1}, {b, 2}],
      merge_proplists(fun (_, _, _) -> error(never_happens) end,
                      [], [{a, 1}, {b, 2}])),
    ?assertEqual(
      [{a, 1}, {b, 2}],
      merge_proplists(fun (_, _, _) -> error(never_happens) end,
                      [{a, 1}, {b, 2}], [])),
    ?assertEqual(
      [{a, 1}, {b, 10}, {c, 21}, {d, 11}],
      merge_proplists(fun (_, L, R) -> L * R end,
                      [{a, 1}, {b, 2}, {c, 3}], [{b, 5}, {c, 7}, {d, 11}])),
    ?assertEqual(
      [{a, 1}, {b, 5}, {c, 7}, {d, 11}],
      merge_proplists(fun (_, _L, R) -> R end,
                      [{a, 1}, {b, 2}, {c, 3}], [{b, 5}, {c, 7}, {d, 11}])).
-endif.


%% get proplist value or fail
expect_prop_value(K, List) ->
    Ref = make_ref(),
    try
        case proplists:get_value(K, List, Ref) of
            RV when RV =/= Ref -> RV
        end
    catch
        error:X -> erlang:error(X, [K, List])
    end.

%% true iff given path is absolute
is_absolute_path(Path) ->
    Normalized = filename:join([Path]),
    filename:absname(Normalized) =:= Normalized.

%% @doc Truncate a timestamp to the nearest multiple of N seconds.
trunc_ts(TS, N) ->
    TS - (TS rem (N*1000)).

%% alternative of file:read_file/1 that reads file until EOF is
%% reached instead of relying on file length. See
%% http://groups.google.com/group/erlang-programming/browse_thread/thread/fd1ec67ff690d8eb
%% for more information. This piece of code was borrowed from above mentioned URL.
raw_read_file(Path) ->
    with_file(Path, [read, binary], raw_read_loop(_, [])).
raw_read_loop(File, Acc) ->
    case file:read(File, 16384) of
        {ok, Bytes} ->
            raw_read_loop(File, [Acc | Bytes]);
        eof ->
            {ok, iolist_to_binary(Acc)};
        {error, _} = Error ->
            Error
    end.

assoc_multicall_results_rec([], _ResL, _BadNodes, SuccessAcc, ErrorAcc) ->
    {SuccessAcc, ErrorAcc};
assoc_multicall_results_rec([N | Nodes], Results, BadNodes,
                            SuccessAcc, ErrorAcc) ->
    case lists:member(N, BadNodes) of
        true ->
            assoc_multicall_results_rec(Nodes, Results, BadNodes, SuccessAcc, ErrorAcc);
        _ ->
            [Res | ResRest] = Results,

            case Res of
                {badrpc, Reason} ->
                    NewErrAcc = [{N, Reason} | ErrorAcc],
                    assoc_multicall_results_rec(Nodes, ResRest, BadNodes,
                                                SuccessAcc, NewErrAcc);
                _ ->
                    NewOkAcc = [{N, Res} | SuccessAcc],
                    assoc_multicall_results_rec(Nodes, ResRest, BadNodes,
                                                NewOkAcc, ErrorAcc)
            end
    end.

%% Returns a pair of proplists and list of nodes. First element is a
%% mapping from Nodes to return values for nodes that
%% succeeded. Second one is a mapping from Nodes to error reason for
%% failed nodes. And third tuple element is BadNodes argument unchanged.
-spec assoc_multicall_results([node()], [any() | {badrpc, any()}], [node()]) ->
                                    {OkNodeResults::[{node(), any()}],
                                     BadRPCNodeResults::[{node(), any()}],
                                     BadNodes::[node()]}.
assoc_multicall_results(Nodes, ResL, BadNodes) ->
    {OkNodeResults, BadRPCNodeResults} = assoc_multicall_results_rec(Nodes, ResL, BadNodes, [], []),
    {OkNodeResults, BadRPCNodeResults, BadNodes}.

%% Performs rpc:multicall and massages results into "normal results",
%% {badrpc, ...} results and timeouts/disconnects. Returns triple
%% produced by assoc_multicall_results/3 above.
rpc_multicall_with_plist_result(Nodes, M, F, A, Timeout) ->
    {ResL, BadNodes} = rpc:multicall(Nodes, M, F, A, Timeout),
    assoc_multicall_results(Nodes, ResL, BadNodes).

rpc_multicall_with_plist_result(Nodes, M, F, A) ->
    rpc_multicall_with_plist_result(Nodes, M, F, A, infinity).

-spec realpath(string(), string()) -> {ok, string()} |
                                      {error, atom(), string(), list(), any()}.
realpath(Path, BaseDir) ->
    case erlang:system_info(system_architecture) of
        "win32" ->
            {ok, filename:absname(Path, BaseDir)};
        _ -> case realpath_full(Path, BaseDir, 32) of
                 {ok, X, _} -> {ok, X};
                 X -> X
             end
    end.

-spec realpath_full(string(), string(), integer()) ->
                           {ok, string(), integer()} |
                           {error, atom(), string(), list(), any()}.
realpath_full(Path, BaseDir, SymlinksLimit) ->
    NormalizedPath = filename:join([Path]),
    Tokens = string:tokens(NormalizedPath, "/"),
    case Path of
        [$/ | _] ->
            %% if we're absolute path then start with root
            realpath_rec_check("/", Tokens, SymlinksLimit);
        _ ->
            %% otherwise start walking from BaseDir
            realpath_rec_info(#file_info{type = other}, BaseDir,
                              Tokens, SymlinksLimit)
    end.

%% this is called to check type of Current pathname and expand
%% it if it's symlink.
-spec realpath_rec_check(string(), [string()], integer()) ->
                                {ok, string(), integer()} |
                                {error, atom(), string(), list(), any()}.
realpath_rec_check(Current, Tokens, SymlinksLimit) ->
    case file:read_link_info(Current) of
        {ok, Info} ->
            realpath_rec_info(Info, Current, Tokens, SymlinksLimit);
        Crap -> {error, read_file_info, Current, Tokens, Crap}
    end.

%% this implements 'meat' of path name lookup
-spec realpath_rec_info(tuple(), string(), [string()], integer()) ->
                               {ok, string(), integer()} |
                               {error, atom(), string(), list(), any()}.
%% this case handles Current being symlink. Symlink is recursively
%% expanded and we continue path name walking from expanded place
realpath_rec_info(Info, Current, Tokens, SymlinksLimit)
  when Info#file_info.type =:= symlink ->
    case file:read_link(Current) of
        {error, _} = Crap -> {error, read_link, Current, Tokens, Crap};
        {ok, LinkDestination} ->
            case SymlinksLimit of
                0 -> {error, symlinks_limit_reached, Current, Tokens, undefined};
                _ ->
                    case realpath_full(LinkDestination,
                                       filename:dirname(Current),
                                       SymlinksLimit - 1) of
                        {ok, Expanded, NewSymlinksLimit} ->
                            realpath_rec_check(Expanded, Tokens,
                                               NewSymlinksLimit);
                        Error -> Error
                    end
            end
    end;
%% this case handles end of path name walking
realpath_rec_info(_, Current, [], SymlinksLimit) ->
    {ok, Current, SymlinksLimit};
%% this case just removes single dot
realpath_rec_info(Info, Current, ["." | Tokens], SymlinksLimit) ->
    realpath_rec_info(Info, Current, Tokens, SymlinksLimit);
%% this case implements ".."
realpath_rec_info(_Info, Current, [".." | Tokens], SymlinksLimit) ->
    realpath_rec_check(filename:dirname(Current), Tokens, SymlinksLimit);
%% this handles most common case of walking single level of file tree
realpath_rec_info(_Info, Current, [FirstToken | Tokens], SymlinksLimit) ->
    NewCurrent = filename:absname(FirstToken, Current),
    realpath_rec_check(NewCurrent, Tokens, SymlinksLimit).

-spec split_binary_at_char(binary(), char()) -> binary() | {binary(), binary()}.
split_binary_at_char(Binary, Chr) ->
    case binary:split(Binary, <<Chr:8>>) of
        [_] -> Binary;
        [Part1, Part2] -> {Part1, Part2}
    end.

is_binary_ends_with(Binary, Suffix) ->
    binary:longest_common_suffix([Binary, Suffix]) =:= size(Suffix).

absname(Name) ->
    PathType = filename:pathtype(Name),
    case PathType of
        absolute ->
            filename:absname(Name, "/");
        _ ->
            filename:absname(Name)
    end.

start_event_link(SubscriptionBody) ->
    proc_lib:start_link(
      erlang, apply,
      [fun () ->
               SubscriptionBody(),
               proc_lib:init_ack({ok, self()}),
               receive
                   _ -> ok
               end
       end, []]).

%% Writes to file atomically using write_file + atomic_rename
atomic_write_file(Path, BodyOrBytes)
  when is_function(BodyOrBytes);
       is_binary(BodyOrBytes);
       is_list(BodyOrBytes) ->
    DirName = filename:dirname(Path),
    FileName = filename:basename(Path),
    TmpPath = path_config:tempfile(DirName, FileName, ".tmp"),
    try
        case misc:write_file(TmpPath, BodyOrBytes) of
            ok ->
                atomic_rename(TmpPath, Path);
            X ->
                X
        end
    after
        (catch file:delete(TmpPath))
    end.

%% Rename file (more or less) atomically.
%% See https://lwn.net/Articles/351422/ for some details.
%%
%% NB: this does not work on Windows
%% (http://osdir.com/ml/racket.development/2011-01/msg00149.html).
atomic_rename(From, To) ->
    case file:open(From, [raw, binary, read, write]) of
        {ok, IO} ->
            SyncRV =
                try
                    file:sync(IO)
                after
                    ok = file:close(IO)
                end,
            case SyncRV of
                ok ->
                    %% NOTE: linux manpages also mention sync
                    %% on directory, but erlang can't do that
                    %% and that's not portable
                    file:rename(From, To);
                _ ->
                    SyncRV
            end;
        Err ->
            Err
    end.

%% Get an item from from a dict, if it doesnt exist return default
-spec dict_get(term(), dict:dict(), term()) -> term().
dict_get(Key, Dict, Default) ->
    case dict:find(Key, Dict) of
        {ok, Value} ->
            Value;
        error ->
            Default
    end.

%% like dict:update/4 but calls the function on initial value instead of just
%% storing it in the dict
dict_update(Key, Fun, Initial, Dict) ->
    try
        dict:update(Key, Fun, Dict)
    catch
        %% key not found
        error:badarg ->
            dict:store(Key, Fun(Initial), Dict)
    end.

%% Parse version of the form 1.7.0r_252_g1e1c2c0 or 1.7.0r-252-g1e1c2c0 into a
%% list {[1,7,0],candidate,252}.  1.8.0 introduces a license type suffix,
%% like: 1.8.0r-25-g1e1c2c0-enterprise.  Note that we should never
%% see something like 1.7.0-enterprise, as older nodes won't carry
%% the license type information.
-spec parse_version(string()) -> version().
parse_version(VersionStr) ->
    Parts = string:tokens(VersionStr, "_-"),
    case Parts of
        [BaseVersionStr] ->
            {BaseVersion, Type} = parse_base_version(BaseVersionStr),
            {BaseVersion, Type, 0};
        [BaseVersionStr, OffsetStr | _Rest] ->
            {BaseVersion, Type} = parse_base_version(BaseVersionStr),
            {BaseVersion, Type, list_to_integer(OffsetStr)}
    end.

-define(VERSION_REGEXP,
        "^((?:[0-9]+\.)*[0-9]+).*?(r)?$"). % unbreak font-lock "

parse_base_version(BaseVersionStr) ->
    case re:run(BaseVersionStr, ?VERSION_REGEXP,
                [{capture, all_but_first, list}]) of
        {match, [NumericVersion, "r"]} ->
            Type = candidate;
        {match, [NumericVersion]} ->
            Type = release
    end,

    {lists:map(fun list_to_integer/1,
               string:tokens(NumericVersion, ".")), Type}.

-ifdef(TEST).
parse_version_test() ->
    ?assertEqual({[1,7,0],release,252},
                 parse_version("1.7.0_252_g1e1c2c0")),
    ?assertEqual({[1,7,0],release,252},
                 parse_version("1.7.0-252-g1e1c2c0")),
    ?assertEqual({[1,7,0],candidate,252},
                 parse_version("1.7.0r_252_g1e1c2c0")),
    ?assertEqual({[1,7,0],candidate,252},
                 parse_version("1.7.0r-252-g1e1c2c0")),
    ?assertEqual({[1,7,0],release,0},
                 parse_version("1.7.0")),
    ?assertEqual({[1,7,0],candidate,0},
                 parse_version("1.7.0r")),
    ?assertEqual(true,
                 parse_version("1.7.0") >
                     parse_version("1.7.0r_252_g1e1c2c0")),
    ?assertEqual(true,
                 parse_version("1.7.0") >
                     parse_version("1.7.0r")),
    ?assertEqual(true,
                 parse_version("1.7.1r") >
                     parse_version("1.7.0")),
    ?assertEqual(true,
                 parse_version("1.7.1_252_g1e1c2c0") >
                     parse_version("1.7.1_251_g1e1c2c1")),
    ?assertEqual({[1,8,0],release,25},
                 parse_version("1.8.0_25_g1e1c2c0-enterprise")),
    ?assertEqual({[1,8,0],release,25},
                 parse_version("1.8.0-25-g1e1c2c0-enterprise")),
    ?assertEqual({[2,0,0],candidate,702},
                 parse_version("2.0.0dp4r-702")),
    ?assertEqual({[2,0,0],candidate,702},
                 parse_version("2.0.0dp4r-702-g1e1c2c0")),
    ?assertEqual({[2,0,0],candidate,702},
                 parse_version("2.0.0dp4r-702-g1e1c2c0-enterprise")),
    ?assertEqual({[2,0,0],release,702},
                 parse_version("2.0.0dp4-702")),
    ?assertEqual({[2,0,0],release,702},
                 parse_version("2.0.0dp4-702-g1e1c2c0")),
    ?assertEqual({[2,0,0],release,702},
                 parse_version("2.0.0dp4-702-g1e1c2c0-enterprise")),
    ok.

ceiling_test() ->
    ?assertEqual(4, ceiling(4)),
    ?assertEqual(4, ceiling(4.0)),
    ?assertEqual(4, ceiling(3.99)),
    ?assertEqual(4, ceiling(3.01)),
    ?assertEqual(-4, ceiling(-4)),
    ?assertEqual(-4, ceiling(-4.0)),
    ?assertEqual(-4, ceiling(-4.99)),
    ?assertEqual(-4, ceiling(-4.01)),
    ok.

-endif.

compute_map_diff(undefined, OldMap) ->
    compute_map_diff([], OldMap);
compute_map_diff(NewMap, undefined) ->
    compute_map_diff(NewMap, []);
compute_map_diff([], []) ->
    [];
compute_map_diff(NewMap, []) when NewMap =/= [] ->
    compute_map_diff(NewMap, [[] || _ <- NewMap]);
compute_map_diff([], OldMap) when OldMap =/= [] ->
    compute_map_diff([[] || _ <- OldMap], OldMap);
compute_map_diff(NewMap, OldMap) ->
    VBucketsCount = erlang:length(NewMap),
    [{I, ChainOld, ChainNew} ||
        {I, ChainOld, ChainNew} <-
            lists:zip3(lists:seq(0, VBucketsCount-1), OldMap, NewMap),
        ChainOld =/= ChainNew].

%% execute body in newly spawned process. Function returns when Body
%% returns and with it's return value. If body produced any exception
%% it will be rethrown. Care is taken to propagate exits of 'parent'
%% process to this worker process.
executing_on_new_process(Fun) ->
    executing_on_new_process(Fun, []).

executing_on_new_process(Fun, Options) ->
    {trap_exit, TrapExit} = process_info(self(), trap_exit),

    StartOptions = executing_on_new_process_handle_options(Options),
    executing_on_new_process_handle_trap_exit(TrapExit, Fun, StartOptions).

executing_on_new_process_handle_options(Options) ->
    PassThrough = [abort_after],
    proplist_keyfilter(lists:member(_, PassThrough), Options).

executing_on_new_process_handle_trap_exit(true, Fun, StartOptions) ->
    %% If the caller had trap_exit set, we can't really make the execution
    %% interruptlible, after all it was, hopefully, a deliberate choice to set
    %% trap_exit, so we need to abide by it.
    executing_on_new_process_body(Fun, StartOptions, []);
executing_on_new_process_handle_trap_exit(false, Fun, StartOptions) ->
    with_trap_exit(?cut(executing_on_new_process_body(
                          Fun, StartOptions, [interruptible]))).

executing_on_new_process_body(Fun, StartOptions, WaitOptions) ->
    async:with(
      Fun, StartOptions,
      fun (A) ->
              try
                  async:wait(A, WaitOptions)
              catch
                  throw:{interrupted, {'EXIT', _, Reason} = Exit} ->
                      true = proplists:get_bool(interruptible, WaitOptions),

                      ?log_debug("Aborting ~p (body is ~p) because "
                                 "we are interrupted by an exit message ~p",
                                 [A, Fun, Exit]),

                      async:abort(A, Reason),
                      %% will be processed by the with_trap_exit
                      self() ! Exit
              end
      end).

-ifdef(TEST).
executing_on_new_process_test_() ->
    {timeout, 200,
     fun () ->
             lists:foreach(
               fun (_) ->
                       P = spawn(?cut(misc:executing_on_new_process(
                                        fun () ->
                                                register(grandchild, self()),
                                                timer:sleep(3600 * 1000)
                                        end))),
                       timer:sleep(rand:uniform(5) - 1),
                       exit(P, shutdown),
                       ok = wait_for_process(P, 500),
                       undefined = whereis(grandchild)
               end, lists:seq(1, 1000))
     end}.

%% Check that exit signals are propagated without any mangling.
executing_on_new_process_exit_test() ->
    try
        misc:executing_on_new_process(?cut(exit(shutdown)))
    catch
        exit:shutdown ->
            ok
    end.

executing_on_new_process_abort_after_test() ->
    ?assertExit(timeout,
                misc:executing_on_new_process(
                  fun () ->
                          register(child, self()),
                          timer:sleep(10000)
                  end,
                  [{abort_after, 100}])),

    %% the child must be dead
    undefined = whereis(child),

    ok = misc:executing_on_new_process(?cut(timer:sleep(100)),
                                       [{abort_after, 1000}]),


    %% must be no messages left in mailbox
    0 = ?flush(_).

-endif.

%% returns if Reason is EXIT caused by undefined function/module
is_undef_exit(M, F, A, {undef, [{M, F, A, []} | _]}) -> true; % R15, R16
is_undef_exit(M, F, A, {undef, [{M, F, A} | _]}) -> true; % R14
is_undef_exit(_M, _F, _A, _Reason) -> false.

is_timeout_exit({'EXIT', timeout}) -> true;
is_timeout_exit({'EXIT', {timeout, _}}) -> true;
is_timeout_exit(_) -> false.

-spec sync_shutdown_many_i_am_trapping_exits(Pids :: [pid()]) -> ok.
sync_shutdown_many_i_am_trapping_exits(Pids) ->
    {trap_exit, true} = erlang:process_info(self(), trap_exit),
    [(catch erlang:exit(Pid, shutdown)) || Pid <- Pids],
    BadShutdowns = [{P, RV} || P <- Pids,
                               (RV = inner_wait_shutdown(P)) =/= shutdown],
    case BadShutdowns of
        [] -> ok;
        _ ->
            ?log_error("Shutdown of the following failed: ~p", [BadShutdowns])
    end,
    [] = BadShutdowns,
    ok.

%% NOTE: this is internal helper, despite everything being exported
%% from here
-spec inner_wait_shutdown(Pid :: pid()) -> term().
inner_wait_shutdown(Pid) ->
    MRef = erlang:monitor(process, Pid),
    MRefReason = receive
                     {'DOWN', MRef, _, _, MRefReason0} ->
                         MRefReason0
                 end,
    receive
        {'EXIT', Pid, Reason} ->
            Reason
    after 5000 ->
            ?log_error("Expected exit signal from ~p but could not get it in 5 seconds. This is a bug, but process we're waiting for is dead (~p), so trying to ignore...", [Pid, MRefReason]),
            ?log_debug("Here's messages:~n~p", [erlang:process_info(self(), messages)]),
            MRefReason
    end.

%% @doc works like try/after but when try has raised exception, any
%% exception from AfterBody is logged and ignored. I.e. when we face
%% exceptions from both try-block and after-block, exception from
%% after-block is logged and ignored and exception from try-block is
%% rethrown. Use this when exception from TryBody is more important
%% than exception from AfterBody.
try_with_maybe_ignorant_after(TryBody, AfterBody) ->
    RV =
        try TryBody()
        catch T:E:Stacktrace ->
                try AfterBody()
                catch T2:E2:S2 ->
                        ?log_error("Eating exception from ignorant after-block:~n~p", [{T2, E2, S2}])
                end,
                erlang:raise(T, E, Stacktrace)
        end,
    AfterBody(),
    RV.

letrec(Args, F) ->
    erlang:apply(F, [F | Args]).

%% Artifact of backporting enforce TLS project, MB-48047, to 6.6.4.
%% This is to workaround the cluster_compat_mode check of 7_0.
%% Should git rid off when 6.6.x is unsupported.
-spec is_strict_possible() -> true | false.
is_strict_possible() ->
    ns_config:read_key_fast(can_enable_strict_encryption, false).

-spec is_ipv6() -> true | false.
is_ipv6() ->
    get_net_family() == inet6.

-spec is_node_encryption_enabled(term(), atom()) -> true | false.
is_node_encryption_enabled(Cfg, Node) ->
    ns_config:search_node(Node, Cfg, node_encryption) =:= {value, true}.

-spec is_cluster_encryption_fully_enabled() -> true | false.
is_cluster_encryption_fully_enabled() ->
    %% Cluster wide encryption is considered to be enabled only if node
    %% level encryption is enabled on all the nodes in the cluster.
    Cfg = ns_config:latest(),
    NonEncryptNodes =
        [N || N <- ns_node_disco:nodes_wanted(),
              not is_node_encryption_enabled(Cfg, N)],

    cluster_compat_mode:is_enterprise() andalso
        NonEncryptNodes =:= [].

%% This function is not the same as (not is_cluster_encryption_fully_enabled())
%% because the cluster encryption might be in one of 3 states: enabled, disabled
%% and mixed. 'Mixed' means it's enabled for some nodes, but not for all of them
-spec is_cluster_encryption_fully_disabled() -> true | false.
is_cluster_encryption_fully_disabled() ->
    Cfg = ns_config:latest(),
    [] =:= [N || N <- ns_node_disco:nodes_wanted(),
                 misc:is_node_encryption_enabled(Cfg, N)].

%% get_cluster_encryption_level is internal, it's to avoid unnecessary restarts
%% of TLS for services when we add new nodes as there is temporary blip in
%% is_cluster_encryption_fully_enabled(true -> false -> true) due to the config
%% not being synced.
%% Used in should_cluster_data_be_encrypted fun.
-spec get_cluster_encryption_level() -> none | control | all | strict.
get_cluster_encryption_level() ->
    Default = case is_cluster_encryption_fully_enabled() of
                  true ->
                      control;
                  false ->
                      none
              end,
    ns_config:search(ns_config:latest(), cluster_encryption_level, Default).

%% get_effective_cluster_encryption_level is the experience to the user, i.e.,
%% if node to node encryption is disabled it should be "none" else can be
%% "control"/"all"/"strict".
%% We don't store "none" encryption level, we infer it when node to node
%% encryption is disabled, if enabled it defaults to control.
%% We don't care if there is temporary blip in this field while adding nodes, as
%% it is used mostly for the UI.
-spec get_effective_cluster_encryption_level(term()) -> none | control |
                                                        all | strict.
get_effective_cluster_encryption_level(Config) ->
    case is_cluster_encryption_fully_enabled() of
        true ->
            ns_config:search(Config, cluster_encryption_level, control);
        false ->
            none
    end.

-spec should_cluster_data_be_encrypted() -> true | false.
should_cluster_data_be_encrypted() ->
    case get_cluster_encryption_level() of
        all -> true;
        strict -> true;
        _ -> false
    end.

-spec disable_non_ssl_ports() -> true | false.
disable_non_ssl_ports() ->
    get_cluster_encryption_level() =:= strict.

-spec get_net_family() -> inet:address_family().
get_net_family() ->
    cb_dist:address_family().

-spec get_afamily_only() -> true | false.
get_afamily_only() ->
    get_afamily_only(ns_config:latest(), node()).

-spec get_afamily_only(term(), atom()) -> true | false.
get_afamily_only(Config, Node) ->
    ns_config:search_node_with_default(Node, Config, address_family_only, false).

-spec get_afamily_type(inet:address_family()) -> off | required | optional.
get_afamily_type(AFamily) when AFamily =:= inet orelse AFamily =:= inet6 ->
    Required = ns_config:read_key_fast({node, node(), address_family}, inet),
    AFamilyOnly = get_afamily_only(),
    case {AFamily, AFamilyOnly} of
        {Required, _} ->
            required;
        {_, false} ->
            optional;
        {_, true} ->
            off
    end.

address_family_requirement() ->
    [{AF, get_afamily_type(AF)} || AF <- [inet, inet6]].

-spec is_localhost(string()) -> true | false.
is_localhost(Addr) ->
    case inet:parse_address(Addr) of
        {ok, {127,0,0,1}} ->
            true;
        {ok, {0,0,0,0,0,0,0,1}} ->
            true;
        _ ->
            false
    end.

-spec localhost() -> string().
localhost() ->
    localhost([]).

-spec localhost([] | [url]) -> string().
localhost(Options) ->
    localhost(get_net_family(), Options).

localhost(inet, _Options) -> "127.0.0.1";
localhost(inet6, [url]) -> "[::1]";
localhost(inet6, []) -> "::1".

localhost_alias() -> "cb.local".

-spec inaddr_any() -> string().
inaddr_any() ->
    inaddr_any([]).

-spec inaddr_any([] | [url]) -> string().
inaddr_any(Options) ->
    inaddr_any(get_net_family(), Options).

inaddr_any(inet, _Options) -> "0.0.0.0";
inaddr_any(inet6, Options) ->
    case Options of
        [] ->
            "::";
        [url] ->
            "[::]"
    end.

-spec local_url(integer(),
                [] | [no_scheme | ssl |
                      {user_info, {string(), string()}}]) -> string().
local_url(Port, Options) ->
    local_url(Port, "", Options).

-spec local_url(integer(), string(),
                [] | [no_scheme | ssl |
                      {user_info, {string(), string()}}]) -> string().
local_url(Port, [H | _] = Path, Options) when H =/= $/ ->
    local_url(Port, "/" ++ Path, Options);
local_url(Port, Path, Options) ->
    Scheme = case lists:member(no_scheme, Options) of
                 true -> "";
                 false ->
                     case lists:member(ssl, Options) of
                         true  -> "https://";
                         false -> "http://"
                     end
             end,
    User = case lists:keysearch(user_info, 1, Options) of
               false -> "";
               {value, {_, {U, P}}} -> U ++ ":" ++ P ++ "@"
           end,
    Scheme ++ User ++ localhost([url]) ++ ":" ++ integer_to_list(Port) ++ Path.

-spec is_good_address(string()) ->
                ok | {cannot_resolve, {inet:posix(), inet|inet6}}
                   | {cannot_listen, inet:posix()}
                   | {address_not_allowed, string()}.
is_good_address(Address) ->
    is_good_address(Address, get_net_family()).

is_good_address(Address, AFamily) ->
    case {AFamily, is_raw_ip(Address), is_raw_ipv6(Address)} of
        {inet6, _, true} -> check_short_name(Address, ":", AFamily);
        {inet6, true, _} ->
            Msg = io_lib:format("Can't use raw ipv4 address ~s for ipv6 node. "
                                "Please use a Fully Qualified Domain Name or "
                                "ipv6 address.", [Address]),
            {address_not_allowed, lists:flatten(Msg)};
        {inet, _, true} ->
            Msg = io_lib:format("Can't use ipv6 address ~s for ipv4 node",
                                [Address]),
            {address_not_allowed, lists:flatten(Msg)};
        _ ->
            check_short_name(Address, ".", AFamily)
    end.

check_short_name(Address, Separators, AFamily) ->
    case lists:subtract(Address, Separators) of
        Address ->
            {address_not_allowed,
             "Short names are not allowed. Please use a Fully Qualified Domain Name."};
        _ ->
            is_good_address_when_allowed(Address, AFamily)
    end.

is_good_address_when_allowed(Address, NetFamily) ->
    case inet:getaddr(Address, NetFamily) of
        {error, Errno} ->
            {cannot_resolve, {Errno, NetFamily}};
        {ok, IpAddr} ->
            case gen_udp:open(0, [NetFamily, {ip, IpAddr}]) of
                {error, Errno} ->
                    {cannot_listen, Errno};
                {ok, Socket} ->
                    gen_udp:close(Socket),
                    ok
            end
    end.

is_local_port_open(Port, Timeout) ->
    case gen_tcp:connect(localhost(), Port, [get_net_family()], Timeout) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            true;
        {error, _} ->
            false
    end.

delaying_crash(DelayBy, Body) ->
    try
        Body()
    catch T:E:ST ->
            ?log_debug("Delaying crash ~p:~p by ~pms~nStacktrace: ~p", [T, E, DelayBy, ST]),
            timer:sleep(DelayBy),
            erlang:raise(T, E, ST)
    end.

%% Like erlang:memory but returns 'notsup' if it's impossible to get this
%% information.
memory() ->
    try
        erlang:memory()
    catch
        error:notsup ->
            notsup
    end.

%% based on https://www.kernel.org/doc/Documentation/cgroup-v1/cpusets.txt
%%          https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
-define(CGROUP_CPU_CFS_QUOTA_FILE, "/sys/fs/cgroup/cpu/cpu.cfs_quota_us").
-define(CGROUP_CPU_CFS_PERIOD_FILE, "/sys/fs/cgroup/cpu/cpu.cfs_period_us").
-define(CGROUP_CPUSET_CPUS_FILE, "/sys/fs/cgroup/cpuset/cpuset.cpus").
cpu_count() ->
    CPUs1 =
        case read_int_from_file(?CGROUP_CPU_CFS_QUOTA_FILE, unknown) of
            -1 -> unknown;
            unknown -> unknown;
            CPUquota ->
                case read_int_from_file(?CGROUP_CPU_CFS_PERIOD_FILE, unknown) of
                    unknown -> unknown;
                    CPUperiod -> CPUquota / CPUperiod
                end
        end,
    CPUs2 =
        case (catch raw_read_file(?CGROUP_CPUSET_CPUS_FILE)) of
            {ok, Bin} ->
                Str = string:strip(binary_to_list(Bin), right, $\n),
                try length(parse_cpuset_cpus(Str))
                catch _:_ -> unknown end;
            _ -> unknown
        end,

    CPUs3 = erlang:system_info(logical_processors),
    lists:min([CPUs1, CPUs2, CPUs3]).

parse_cpuset_cpus(Str) ->
    try
        Tokens = [string:strip(T) || T <- string:tokens(Str, ",")],
        R = lists:flatmap(
              fun (T) ->
                      case string:tokens(T, "-") of
                          [N] -> [list_to_integer(N)];
                          [N1, N2] -> lists:seq(list_to_integer(N1),
                                                list_to_integer(N2))
                      end
              end, Tokens),
        lists:usort(R)
    catch
        _:_ -> erlang:error(badarg)
    end.

-ifdef(TEST).
parse_cpuset_cpus_test() ->
    ?assertEqual([], parse_cpuset_cpus("")),
    ?assertEqual([2], parse_cpuset_cpus("2")),
    ?assertEqual([1,2], parse_cpuset_cpus("1-2")),
    ?assertEqual([1,3,5], parse_cpuset_cpus("1,3,5")),
    ?assertEqual([1,2,3,5,6,7], parse_cpuset_cpus("1-3,5-7")),
    ?assertEqual([1,2,3,4,5,6,7], parse_cpuset_cpus("1,2,5-7,3,4")),
    ?assertException(error, badarg, parse_cpuset_cpus("1,aa")).
-endif.

%% What platform are we running on?

is_linux() ->
    case os:type() of
        {unix, linux} ->
            true;
        {_, _} ->
            false
    end.

is_macos() ->
    case os:type() of
        {unix, darwin} ->
            true;
        {_, _} ->
            false
    end.

is_windows() ->
    case os:type() of
        {win32, _} ->
            true;
        {_, _} ->
            false
    end.

ensure_writable_dir(Path) ->
    filelib:ensure_dir(Path),
    case filelib:is_dir(Path) of
        true ->
            TouchPath = filename:join(Path, ".touch"),
            case misc:write_file(TouchPath, <<"">>) of
                ok ->
                    file:delete(TouchPath),
                    ok;
                _ -> error
            end;
        _ ->
            case file:make_dir(Path) of
                ok -> ok;
                _ ->
                    error
            end
    end.

ensure_writable_dirs([]) ->
    ok;
ensure_writable_dirs([Path | Rest]) ->
    case ensure_writable_dir(Path) of
        ok ->
            ensure_writable_dirs(Rest);
        X -> X
    end.

%% Like lists:split but does not fail if N > length(List).
safe_split(N, List) ->
    do_safe_split(N, List, []).

do_safe_split(_, [], Acc) ->
    {lists:reverse(Acc), []};
do_safe_split(0, List, Acc) ->
    {lists:reverse(Acc), List};
do_safe_split(N, [H|T], Acc) ->
    do_safe_split(N - 1, T, [H|Acc]).

%% Splits list into groups of given max size. It minimizes the number of groups
%% and tries to make groups equal in size when possible.
%% split(3, [1,2,3,4,5]) => [[1,2,3], [4,5]]
%% split(3, [1,2,3,4]) => [[1,2], [3,4]]
-spec split(undefined | non_neg_integer(), [A]) -> [[A]].
split(undefined, List) -> [List];
split(N, []) when N > 0 -> [[]];
split(N, List) when N > 0 ->
    Len = length(List),
    GroupsNum = ceil(Len / N),
    split_in_groups(GroupsNum, List, []).

split_in_groups(GroupsNum, List, Res) ->
    Len = length(List),
    GroupsMaxSize = ceil(Len / GroupsNum),
    case misc:safe_split(GroupsMaxSize, List) of
        {SL, []} -> lists:reverse([SL | Res]);
        {SL, Rest} -> split_in_groups(GroupsNum - 1, Rest, [SL | Res])
    end.

-ifdef(TEST).

split_test_() ->
    Test =
        fun (N, ListLen) ->
            MaxElem = ListLen - 1,
            Name = lists:flatten(io_lib:format("split(~b, lists:seq(0, ~b))",
                                               [N, MaxElem])),
            {Name,
             fun () ->
                 OrigList = lists:seq(0, MaxElem),
                 Res = split(N, OrigList),
                 ?assertEqual(OrigList, lists:concat(Res)),
                 ?assert(length(Res) > 0),
                 Max = length(hd(Res)),
                 ?assert(Max =< N),
                 lists:foreach(
                   fun (SubRes) ->
                       ?assert(lists:member(length(SubRes), [Max, Max - 1]))
                   end, Res)
             end}
        end,
    [Test(N, Len) || N <- lists:seq(1, 30), Len <- lists:seq(0, 3*N)].

-endif.

-spec run_external_tool(string(), [string()]) -> {non_neg_integer(), binary()}.
run_external_tool(Path, Args) ->
    run_external_tool(Path, Args, []).

-spec run_external_tool(string(), [string()], [{string(), string()}]) ->
                               {non_neg_integer(), binary()}.
run_external_tool(Path, Args, Env) ->
    run_external_tool(Path, Args, Env, []).

-spec run_external_tool(string(), [string()],
                        [{string(), string()}], [graceful_shutdown]) ->
                               {non_neg_integer(), binary()}.
run_external_tool(Path, Args, Env, Opts) ->
    executing_on_new_process(
      fun () ->
              GracefulShutdown = proplists:get_bool(graceful_shutdown, Opts),
              GoportOpts = [stderr_to_stdout, binary,
                            stream, exit_status,
                            {args, Args},
                            {env, Env},
                            {name, false},
                            {graceful_shutdown, GracefulShutdown}],
              {ok, Port} = goport:start_link(Path, GoportOpts),
              goport:deliver(Port),
              collect_external_tool_output(Port, [])
      end).

collect_external_tool_output(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            goport:deliver(Port),
            collect_external_tool_output(Port, [Data | Acc]);
        {Port, {exit_status, Status}} ->
            {Status, iolist_to_binary(lists:reverse(Acc))};
        Msg ->
            ?log_error("Got unexpected message"),
            exit({unexpected_message, Msg})
    end.

find_by(Pred, List) ->
    case lists:dropwhile(?cut(not Pred(_)), List) of
        [] ->
            not_found;
        [X | _] ->
            {ok, X}
    end.

min_by(Less, Items) ->
    lists:foldl(
      fun (Elem, Acc) ->
              case Less(Elem, Acc) of
                  true ->
                      Elem;
                  false ->
                      Acc
              end
      end, hd(Items), tl(Items)).

inspect_term(Value) ->
    binary_to_list(iolist_to_binary(io_lib:format("~p", [Value]))).

with_file(Path, Mode, Body) ->
    case file:open(Path, Mode) of
        {ok, F} ->
            try
                Body(F)
            after
                (catch file:close(F))
            end;
        Error ->
            Error
    end.

write_file(Path, Bytes) when is_binary(Bytes); is_list(Bytes) ->
    misc:write_file(Path,
                    fun (F) ->
                            file:write(F, Bytes)
                    end);
write_file(Path, Body) when is_function(Body) ->
    with_file(Path, [raw, binary, write], Body).

halt(Status) ->
    try
        erlang:halt(Status, [{flush, false}])
    catch
        error:undef ->
            erlang:halt(Status)
    end.

%% Ensure that directory exists. Analogous to running mkdir -p in shell.
mkdir_p(Path) ->
    case filelib:ensure_dir(Path) of
        ok ->
            case file:make_dir(Path) of
                {error, eexist} ->
                    case file:read_file_info(Path) of
                        {ok, Info} ->
                            case Info#file_info.type of
                                directory ->
                                    ok;
                                _ ->
                                    {error, eexist}
                            end;
                        Error ->
                            Error
                    end;
                %% either ok or other error
                Other ->
                    Other
            end;
        Error ->
            Error
    end.

create_marker(Path, Data)
  when is_list(Data);
       is_binary(Data) ->
    ok = atomic_write_file(Path, Data).

create_marker(Path) ->
    create_marker(Path, <<"">>).

remove_marker(Path) ->
    ok = file:delete(Path).

marker_exists(Path) ->
    case file:read_file_info(Path) of
        {ok, _} ->
            true;
        {error, enoent} ->
            false;
        Other ->
            ?log_error("Unexpected error when reading marker ~p: ~p", [Path, Other]),
            exit({failed_to_read_marker, Path, Other})
    end.

read_marker(Path) ->
    case file:read_file(Path) of
        {ok, BinaryContents} ->
            {ok, binary_to_list(BinaryContents)};
        {error, enoent} ->
            false;
        Other ->
            ?log_error("Unexpected error when reading marker ~p: ~p", [Path, Other]),
            exit({failed_to_read_marker, Path, Other})
    end.

consult_marker(Path) ->
    case file:consult(Path) of
        {ok, Terms} ->
            {ok, Terms};
        {error, enoent} ->
            false;
        {error, Other} ->
            ?log_error("Unexpected error when reading marker ~p: ~p",
                       [Path, Other]),
            exit({failed_to_read_marker, Path, Other})
    end.

take_marker(Path) ->
    Result = read_marker(Path),
    case Result of
        {ok, _} ->
            remove_marker(Path);
        false ->
            ok
    end,

    Result.

is_free_nodename(ShortName) ->
    ErlEpmd = net_kernel:epmd_module(),
    case ErlEpmd:names({127,0,0,1}) of
        {ok, Names} -> not lists:keymember(ShortName, 1, Names);
        {error, address} -> true
    end.

wait_for_nodename(ShortName) ->
    wait_for_nodename(ShortName, 5).

wait_for_nodename(ShortName, Attempts) ->
    case is_free_nodename(ShortName) of
        true ->
            ok;
        false ->
            case Attempts of
                0 ->
                    {error, duplicate_name};
                _ ->
                    ?log_info("Short name ~s is still occupied. "
                              "Will try again after a bit", [ShortName]),
                    timer:sleep(500),
                    wait_for_nodename(ShortName, Attempts - 1)
            end
    end.

is_prefix(KeyPattern, K) ->
    KPL = size(KeyPattern),
    case K of
        <<KeyPattern:KPL/binary, _/binary>> ->
            true;
        _ ->
            false
    end.

eval(Str,Binding) ->
    {ok,Ts,_} = erl_scan:string(Str),
    Ts1 = case lists:reverse(Ts) of
              [{dot,_}|_] -> Ts;
              TsR -> lists:reverse([{dot,1} | TsR])
          end,
    {ok,Expr} = erl_parse:parse_exprs(Ts1),
    erl_eval:exprs(Expr, Binding).

get_ancestors() ->
    erlang:get('$ancestors').

multi_call(Nodes, Name, Request, Timeout) ->
    multi_call(Nodes, Name, Request, Timeout, fun (_) -> true end).

%% Behaves like gen_server:multi_call except that instead of just returning a
%% list of "bad nodes" it returns some details about why a call failed.
%%
%% In addition it takes a predicate that can classify an ok reply as an
%% error. Such a reply will be put into the bad replies list.
-spec multi_call(Nodes, Name, Request, Timeout, OkPred) -> Result
  when Nodes   :: [node()],
       Name    :: atom(),
       Request :: any(),
       Timeout :: infinity | non_neg_integer(),
       Result  :: {Good, Bad},
       Good    :: [{node(), any()}],
       Bad     :: [{node(), any()}],

       OkPred   :: fun((any()) -> OkPredRV),
       OkPredRV :: boolean() | {false, ErrorTerm :: term()}.
multi_call(Nodes, Name, Request, Timeout, OkPred) ->
    NodeRequests =
        [{N, ?cut(gen_server:call({Name, N}, Request, infinity))} ||
            N <- Nodes],
    multi_call_request(NodeRequests, Timeout, OkPred).

-spec multi_call_request(NodeRequests, Timeout, OkPred) -> Result
  when NodeRequests :: [{node(), any()}],
       Timeout      :: infinity | non_neg_integer(),
       Result       :: {Good, Bad},
       Good         :: [{node(), any()}],
       Bad          :: [{node(), any()}],
       OkPred       :: fun((any()) -> OkPredRV),
       OkPredRV     :: boolean() | {false, ErrorTerm :: term()}.
multi_call_request(NodeRequests, Timeout, OkPred) ->
    Nodes = [N || {N, _Req} <- NodeRequests],
    Ref = erlang:make_ref(),
    Parent = self(),
    try
        parallel_map(
          fun ({N, Request}) ->
                  RV = try Request() of
                           Res ->
                               case OkPred(Res) of
                                   true ->
                                       {ok, Res};
                                   {true, OkRes} ->
                                       {ok, OkRes};
                                   false ->
                                       {error, Res};
                                   {false, ErrorTerm} ->
                                       {error, ErrorTerm}
                               end
                       catch T:E ->
                               {error, {T, E}}
                       end,
                  Parent ! {Ref, {N, RV}}
          end, NodeRequests, Timeout)
    catch exit:timeout ->
            ok
    end,
    multi_call_collect(ordsets:from_list(Nodes), [], [], [], Ref).

multi_call_collect(Nodes, GotNodes, AccGood, AccBad, Ref) ->
    receive
        {Ref, {N, Result}} ->
            {NewGood, NewBad} =
                case Result of
                    {ok, Good} ->
                        {[{N, Good} | AccGood], AccBad};
                    {error, Bad} ->
                        {AccGood, [{N, Bad} | AccBad]}
                end,

            multi_call_collect(Nodes, [N | GotNodes], NewGood, NewBad, Ref)
    after 0 ->
            TimeoutNodes = ordsets:subtract(Nodes, ordsets:from_list(GotNodes)),
            BadNodes = [{N, timeout} || N <- TimeoutNodes] ++ AccBad,
            {AccGood, BadNodes}
    end.

-ifdef(TEST).
multi_call_test_() ->
    {setup, fun multi_call_test_setup/0, fun multi_call_test_teardown/1,
     [fun do_test_multi_call/0]}.

multi_call_test_setup_server() ->
    meck:new(multi_call_server, [non_strict, no_link]),
    meck:expect(multi_call_server, init, fun([]) -> {ok, {}} end),
    meck:expect(multi_call_server, handle_call,
                fun(Request, _From, _State) ->
                        Reply = case Request of
                                    {echo, V} ->
                                        V;
                                    {sleep, Time} ->
                                        timer:sleep(Time);
                                    {eval, Fun} ->
                                        Fun()
                                end,
                        {reply, Reply, {}}
                end),
    {ok, _} = gen_server:start({local, multi_call_server}, multi_call_server,
                               [], []),
    ok.

multi_call_test_setup() ->
    NodeNames = [a, b, c, d, e],
    {TestNode, Host0} = misc:node_name_host(node()),
    Host = list_to_atom(Host0),

    CodePath = code:get_path(),
    Nodes = lists:map(
              fun (N) ->
                      FullName = list_to_atom(atom_to_list(N) ++ "-" ++ TestNode),
                      {ok, Node} = slave:start(Host, FullName),
                      true = rpc:call(Node, code, set_path, [CodePath]),
                      ok = rpc:call(Node, misc, multi_call_test_setup_server, []),
                      Node
              end, NodeNames),
    erlang:put(nodes, Nodes).

multi_call_test_teardown(_) ->
    Nodes = erlang:get(nodes),
    lists:foreach(
      fun (Node) ->
              ok = slave:stop(Node)
      end, Nodes).

multi_call_test_assert_bad_nodes(Bad, Expected) ->
    BadNodes = [N || {N, _} <- Bad],
    ?assertEqual(lists:sort(BadNodes), lists:sort(Expected)).

multi_call_request_test_assert_results(RVs, NodeRequests) ->
    lists:foreach(
      fun ({N, {echo, Val}}) ->
              RV = proplists:get_value(N, RVs),
              ?assertEqual(RV, Val)
      end, NodeRequests).

multi_call_test_assert_results(RVs, Nodes, Result) ->
    lists:foreach(
      fun (N) ->
              RV = proplists:get_value(N, RVs),
              ?assertEqual(RV, Result)
      end, Nodes).

do_test_multi_call() ->
    Nodes = nodes(),
    BadNodes = [bad_node],

    {R1, Bad1} = misc:multi_call(Nodes, multi_call_server, {echo, ok}, 100),

    multi_call_test_assert_results(R1, Nodes, ok),
    multi_call_test_assert_bad_nodes(Bad1, []),

    {R2, Bad2} = misc:multi_call(BadNodes ++ Nodes,
                                 multi_call_server, {echo, ok}, 100),
    multi_call_test_assert_results(R2, Nodes, ok),
    multi_call_test_assert_bad_nodes(Bad2, BadNodes),

    [FirstNode | RestNodes] = Nodes,
    catch gen_server:call({multi_call_server, FirstNode}, {sleep, 100000}, 100),

    {R3, Bad3} = misc:multi_call(Nodes, multi_call_server, {echo, ok}, 100),
    multi_call_test_assert_results(R3, RestNodes, ok),
    ?assertEqual(Bad3, [{FirstNode, timeout}]),

    true = rpc:call(FirstNode, erlang, apply,
                    [fun () ->
                             erlang:exit(whereis(multi_call_server), kill)
                     end, []]),
    {R4, Bad4} = misc:multi_call(Nodes, multi_call_server, {echo, ok}, 100),
    multi_call_test_assert_results(R4, RestNodes, ok),
    ?assertMatch([{FirstNode, {exit, {noproc, _}}}], Bad4).

multi_call_request_test_() ->
    {setup, fun multi_call_test_setup/0, fun multi_call_test_teardown/1,
     [fun do_test_multi_call_request/0]}.

do_test_multi_call_request() ->
    MultiCallRequest =
        fun (NR) ->
                Requests =
                    [{N, ?cut(gen_server:call({multi_call_server, N},
                                              R, infinity))} || {N, R} <- NR],
                multi_call_request(Requests, 100, fun (_) -> true end)
        end,
    NodeRequests = [{N, {echo, Val}} || {Val, N} <- misc:enumerate(nodes())],

    {R1, Bad1} = MultiCallRequest(NodeRequests),

    multi_call_request_test_assert_results(R1, NodeRequests),
    multi_call_test_assert_bad_nodes(Bad1, []),

    {R2, Bad2} = MultiCallRequest([{bad_node, {echo, ok}} | NodeRequests]),
    multi_call_request_test_assert_results(R2, NodeRequests),
    multi_call_test_assert_bad_nodes(Bad2, [bad_node]),

    [{FirstNode, _} | RestNodeRequests] = NodeRequests,
    catch gen_server:call({multi_call_server, FirstNode}, {sleep, 100000}, 100),

    {R3, Bad3} = MultiCallRequest(NodeRequests),
    multi_call_request_test_assert_results(R3, RestNodeRequests),
    ?assertEqual(Bad3, [{FirstNode, timeout}]),

    true = rpc:call(FirstNode, erlang, apply,
                    [fun () ->
                             erlang:exit(whereis(multi_call_server), kill)
                     end, []]),
    {R4, Bad4} = MultiCallRequest(NodeRequests),
    multi_call_request_test_assert_results(R4, RestNodeRequests),
    ?assertMatch([{FirstNode, {exit, {noproc, _}}}], Bad4).

multi_call_ok_pred_test_() ->
    {setup, fun multi_call_test_setup/0, fun multi_call_test_teardown/1,
     [fun do_test_multi_call_ok_pred/0]}.

do_test_multi_call_ok_pred() ->
    Nodes = nodes(),
    BadNodes = [bad_node],
    AllNodes = BadNodes ++ Nodes,

    {R1, Bad1} = misc:multi_call(AllNodes, multi_call_server, {echo, ok}, 100,
                                 fun (_) -> false end),

    ?assertEqual(R1, []),
    multi_call_test_assert_bad_nodes(Bad1, AllNodes),

    {R2, Bad2} = misc:multi_call(AllNodes, multi_call_server, {echo, ok}, 100,
                                 fun (_) -> {false, some_error} end),
    ?assertEqual(R2, []),
    multi_call_test_assert_bad_nodes(Bad2, AllNodes),
    multi_call_test_assert_results(Bad2, Nodes, some_error),

    {OkNodes, ErrorNodes} = lists:split(length(Nodes) div 2, misc:shuffle(Nodes)),
    {R3, Bad3} = misc:multi_call(AllNodes, multi_call_server,
                                 {eval, fun () ->
                                                case lists:member(node(), OkNodes) of
                                                    true ->
                                                        ok;
                                                    false ->
                                                        error
                                                end
                                        end},
                                 100,
                                 fun (RV) ->
                                         RV =:= ok
                                 end),

    multi_call_test_assert_results(R3, OkNodes, ok),
    multi_call_test_assert_bad_nodes(Bad3, BadNodes ++ ErrorNodes),
    multi_call_test_assert_results(Bad3, ErrorNodes, error).
-endif.

intersperse([], _) ->
    [];
intersperse([_] = List, _) ->
    List;
intersperse([X | Rest], Sep) ->
    [X, Sep | intersperse(Rest, Sep)].

-ifdef(TEST).
intersperse_test() ->
    ?assertEqual([], intersperse([], x)),
    ?assertEqual([a], intersperse([a], x)),
    ?assertEqual([a,x,b,x,c], intersperse([a,b,c], x)).
-endif.

hexify(Binary) ->
    << <<(hexify_digit(High)), (hexify_digit(Low))>>
       || <<High:4, Low:4>> <= Binary >>.

hexify_digit(0) -> $0;
hexify_digit(1) -> $1;
hexify_digit(2) -> $2;
hexify_digit(3) -> $3;
hexify_digit(4) -> $4;
hexify_digit(5) -> $5;
hexify_digit(6) -> $6;
hexify_digit(7) -> $7;
hexify_digit(8) -> $8;
hexify_digit(9) -> $9;
hexify_digit(10) -> $a;
hexify_digit(11) -> $b;
hexify_digit(12) -> $c;
hexify_digit(13) -> $d;
hexify_digit(14) -> $e;
hexify_digit(15) -> $f.

-ifdef(TEST).
hexify_test() ->
    lists:foreach(
      fun (_) ->
              R = crypto:strong_rand_bytes(256),
              Hex = hexify(R),

              Etalon0 = string:to_lower(integer_to_list(binary:decode_unsigned(R), 16)),
              Etalon1 =
                  case erlang:byte_size(R) * 2 - length(Etalon0) of
                      0 ->
                          Etalon0;
                      N when N > 0 ->
                          lists:duplicate(N, $0) ++ Etalon0
                  end,
              Etalon = list_to_binary(Etalon1),

              ?assertEqual(Hex, Etalon)
      end, lists:seq(1, 100)).
-endif.

iolist_is_empty(<<>>) ->
    true;
iolist_is_empty([]) ->
    true;
iolist_is_empty([H|T]) ->
    iolist_is_empty(H) andalso iolist_is_empty(T);
iolist_is_empty(_) ->
    false.

-ifdef(TEST).
iolist_is_empty_test() ->
    ?assertEqual(iolist_is_empty(""), true),
    ?assertEqual(iolist_is_empty(<<>>), true),
    ?assertEqual(iolist_is_empty([[[<<>>]]]), true),
    ?assertEqual(iolist_is_empty([[]|<<>>]), true),
    ?assertEqual(iolist_is_empty([<<>>|[]]), true),
    ?assertEqual(iolist_is_empty([[[]], <<"test">>]), false),
    ?assertEqual(iolist_is_empty([[<<>>]|"test"]), false).
-endif.

ejson_encode_pretty(Json) ->
    iolist_to_binary(
      pipes:run(sjson:stream_json(Json),
                sjson:encode_json([{compact, false},
                                   {strict, false}]),
                pipes:collect())).

upermutations(Xs) ->
    do_upermutations(lists:sort(Xs)).

do_upermutations([]) ->
    [[]];
do_upermutations(Xs) ->
    [[X|Ys] || X <- unique(Xs), Ys <- do_upermutations(Xs -- [X])].

-ifdef(TEST).
prop_upermutations() ->
    ?FORALL(Xs, resize(10, list(int(0,5))),
            begin
                Perms = upermutations(Xs),

                NoDups = (lists:usort(Perms) =:= Perms),

                XsSorted = lists:sort(Xs),
                ProperPerms =
                    lists:all(
                      fun (P) ->
                              lists:sort(P) =:= XsSorted
                      end, Perms),

                N = length(Xs),
                Counts = [C || {_, C} <- uniqc(XsSorted)],
                ExpectedSize =
                    fact(N) div lists:foldl(
                                  fun (X, Y) -> X * Y end,
                                  1,
                                  lists:map(fun fact/1, Counts)),
                ProperSize = (ExpectedSize =:= length(Perms)),

                NoDups andalso ProperPerms andalso ProperSize
            end).
-endif.

fact(0) ->
    1;
fact(N) ->
    N * fact(N-1).

-spec item_count(list(), term()) -> non_neg_integer().
item_count(List, Item) ->
    lists:foldl(
      fun(Ele, Acc) ->
              if Ele =:= Item -> Acc + 1;
                 true -> Acc
              end
      end, 0, List).

canonical_proplist(List) ->
    lists:usort(compact_proplist(List)).

%% This is similar to proplists:compact() in spirit, except that it
%% [1] drops the item only if it's tuple and its second field is false
%% [2] retains already compacted props & tuples whose second field is not false
%%
%% Ex:
%%  compact([{a,true}, {d,false}, 1, {"x",true}, {a,b}]) =:= [a, 1, "x", {a,b}]
compact_proplist(List) ->
    lists:filtermap(
      fun(Elem) ->
              case Elem of
                  {Key, true} ->
                      {true, Key};
                  {_Key, false} ->
                      false;
                  _ ->
                      {true, Elem}
              end
      end, List).

proplist_keyfilter(Pred, PropList) ->
    lists:filter(proplist_keyfilter_pred(_, Pred), PropList).

proplist_keyfilter_pred(Tuple, Pred)
  when is_tuple(Tuple) ->
    Pred(element(1, Tuple));
proplist_keyfilter_pred(Key, Pred) ->
    Pred(Key).

-ifdef(TEST).
compact_test() ->
    ?assertEqual([a, c], compact_proplist([{a,true}, {b,false}, {c,true}])),
    ?assertEqual(["b", {a,b}], compact_proplist([{"b",true}, {a,b}])).

canonical_proplist_test() ->
    ?assertEqual([a], canonical_proplist([{a,true}, a, {b,false}])),
    ?assertEqual([123, x, {c,"x"}, {e, "y"}],
                 canonical_proplist([{x,true}, {e,"y"}, 123, {c,"x"}])).

proplist_keyfilter_test() ->
    ?assertEqual([{a, 0}, a, {c, 2}, {e, 4}],
                 proplist_keyfilter(
                   lists:member(_, [a, c, e]),
                   [{a, 0}, b, a, {b, 23}, {c, 2}, d, {e, 4}])).
-endif.

compress(Term) ->
    zlib:compress(term_to_binary(Term)).

decompress(Blob) ->
    binary_to_term(zlib:uncompress(Blob)).

-spec split_host_port(list(), list()) -> tuple().
split_host_port("[" ++ _ = HostPort, DefaultPort) ->
    split_host_port(HostPort, DefaultPort, inet6);
split_host_port(HostPort, DefaultPort) ->
    split_host_port(HostPort, DefaultPort, inet).

-spec split_host_port(list(), list(), inet | inet6) -> tuple().
split_host_port("[" ++ Rest, DefaultPort, inet6) ->
    case string:tokens(Rest, "]") of
        [Host] ->
            {Host, DefaultPort};
        [Host, ":" ++ Port] when Port =/= [] ->
            {Host, Port};
        _ ->
            throw({error, [<<"The hostname is malformed.">>]})
    end;
split_host_port("[" ++ _Rest, _DefaultPort, inet) ->
    throw({error, [<<"Unexpected symbol '[' in IPv4 address">>]});
split_host_port(HostPort, DefaultPort, _) ->
    case item_count(HostPort, $:) > 1 of
        true ->
            throw({error, [<<"The hostname is malformed. If using an IPv6 address, "
                             "please enclose the address within '[' and ']'">>]});
        false ->
            case string:tokens(HostPort, ":") of
                [Host] ->
                    {Host, DefaultPort};
                [Host, Port] ->
                    {Host, Port};
                _ ->
                    throw({error, [<<"The hostname is malformed.">>]})
            end
    end.

-spec maybe_add_brackets(string()) -> string().
maybe_add_brackets("[" ++ _Rest = Address) ->
    Address;
maybe_add_brackets(Address) ->
    case lists:member($:, Address) of
        true -> "[" ++ Address ++ "]";
        false -> Address
    end.

-spec join_host_port(string(), string() | integer()) -> string().
join_host_port(Host, Port) when is_integer(Port) ->
    join_host_port(Host, integer_to_list(Port));
join_host_port(Host, Port) ->
    maybe_add_brackets(Host) ++ ":" ++ Port.

-ifdef(TEST).
join_host_port_test() ->
    ?assertEqual("127.0.0.1:1234", join_host_port("127.0.0.1", 1234)),
    ?assertEqual("abc.xyz.com:1234", join_host_port("abc.xyz.com", "1234")),
    ?assertEqual("[fc00::11]:1234", join_host_port("fc00::11", 1234)).
-endif.

%% Convert OTP-18+ style time to the traditional now()-like timestamp.
%%
%% Time should be the system time (as returned by erlang:system_time/1)
%% to be converted.
%%
%% Unit specifies the unit used.
time_to_timestamp(Time, Unit) ->
    Micro = erlang:convert_time_unit(Time, Unit, microsecond),

    Sec = Micro div 1000000,
    Mega = Sec div 1000000,
    {Mega, Sec - Mega * 1000000, Micro - Sec * 1000000}.

time_to_timestamp(Time) ->
    time_to_timestamp(Time, native).

%% Convert traditional now()-like timestamp to OTP-18+ system time.
%%
%% Unit designates the unit to convert to.
timestamp_to_time({MegaSec, Sec, MicroSec}, Unit) ->
    Time = MicroSec + 1000000 * (Sec + 1000000 * MegaSec),
    erlang:convert_time_unit(Time, microsecond, Unit).

timestamp_to_time(TimeStamp) ->
    timestamp_to_time(TimeStamp, native).

time_to_epoch_float(Time) when is_integer(Time) or is_float(Time) ->
    Time;

time_to_epoch_float({_, _, _} = TS) ->
    timestamp_to_time(TS, microsecond) / 1000000;

time_to_epoch_float(_) ->
    undefined.

%% Shortcut convert_time_unit/3. Always assumes that time to convert
%% is in native units.
convert_time_unit(Time, TargetUnit) ->
    erlang:convert_time_unit(Time, native, TargetUnit).

update_field(Field, Record, Fun) ->
    setelement(Field, Record, Fun(element(Field, Record))).

dump_term(Term) ->
    true = can_recover_term(Term),

    [io_lib:write(Term), $.].

can_recover_term(Term) ->
    generic:query(fun erlang:'and'/2,
                  ?cut(not (is_pid(_1) orelse is_reference(_1) orelse is_port(_1))),
                  Term).

parse_term(Term) when is_binary(Term) ->
    do_parse_term(binary_to_list(Term));
parse_term(Term) when is_list(Term) ->
    do_parse_term(lists:flatten(Term)).

do_parse_term(Term) ->
    {ok, Tokens, _} = erl_scan:string(Term),
    {ok, Parsed} = erl_parse:parse_term(Tokens),
    Parsed.

-ifdef(TEST).
forall_recoverable_terms(Body) ->
    ?FORALL(T, ?SUCHTHAT(T1, any(), can_recover_term(T1)), Body(T)).

prop_dump_parse_term() ->
    forall_recoverable_terms(?cut(_1 =:= parse_term(dump_term(_1)))).

prop_dump_parse_term_binary() ->
    forall_recoverable_terms(?cut(_1 =:= parse_term(iolist_to_binary(dump_term(_1))))).
-endif.

-record(timer, {tref, msg}).

-type timer()     :: timer(any()).
-type timer(Type) :: #timer{tref :: undefined | reference(),
                            msg  :: Type}.

-spec create_timer(Msg :: Type) -> timer(Type).
create_timer(Msg) ->
    #timer{tref = undefined,
           msg  = Msg}.

-spec create_timer(timeout(), Msg :: Type) -> timer(Type).
create_timer(Timeout, Msg) ->
    arm_timer(Timeout, create_timer(Msg)).

-spec arm_timer(timeout(), timer(Type)) -> timer(Type).
arm_timer(Timeout, Timer) ->
    do_arm_timer(Timeout, cancel_timer(Timer)).

do_arm_timer(infinity, Timer) ->
    Timer;
do_arm_timer(Timeout, #timer{msg = Msg} = Timer) ->
    TRef = erlang:send_after(Timeout, self(), Msg),
    Timer#timer{tref = TRef}.

-spec cancel_timer(timer(Type)) -> timer(Type).
cancel_timer(#timer{tref = undefined} = Timer) ->
    Timer;
cancel_timer(#timer{tref = TRef,
                    msg  = Msg} = Timer) ->
    erlang:cancel_timer(TRef),
    flush(Msg),

    Timer#timer{tref = undefined}.

-spec read_timer(timer()) -> false | non_neg_integer().
read_timer(#timer{tref = undefined}) ->
    false;
read_timer(Timer) ->
    case erlang:read_timer(Timer#timer.tref) of
        false ->
            %% Since we change tref to undefined when the timer is
            %% canceled, here we can be confident that the timer has
            %% fired. The user might or might not have processed the
            %% message, regardless, we return 0 here.
            0;
        TimeLeft when is_integer(TimeLeft) ->
            TimeLeft
    end.

is_normal_termination(normal) ->
    true;
is_normal_termination(Reason) ->
    is_shutdown(Reason).

is_shutdown(shutdown) ->
    true;
is_shutdown({shutdown, _}) ->
    true;
is_shutdown(_) ->
    false.

with_trap_exit(Fun) ->
    Old = process_flag(trap_exit, true),
    try
        Fun()
    after
        case Old of
            true ->
                ok;
            false ->
                process_flag(trap_exit, false),
                with_trap_exit_maybe_exit()
        end
    end.

with_trap_exit_maybe_exit() ->
    receive
        {'EXIT', _Pid, normal} = Exit ->
            ?log_debug("Ignoring exit message with reason normal: ~p", [Exit]),
            with_trap_exit_maybe_exit();
        {'EXIT', _Pid, Reason} = Exit ->
            ?log_debug("Terminating due to exit message ~p", [Exit]),
            exit_async(Reason)
    after
        0 ->
            ok
    end.

%% Like exit(reason), but can't be catched like such:
%%
%% try exit_async(evasive) catch exit:evasive -> ok end
exit_async(Reason) ->
    Self = self(),
    Pid = spawn(fun () ->
                        exit(Self, Reason)
                end),
    wait_for_process(Pid, infinity),
    exit(must_not_happen).

-ifdef(TEST).
with_trap_exit_test_() ->
    {spawn,
     fun () ->
             ?assertExit(
                finished,
                begin
                    with_trap_exit(fun () ->
                                           spawn_link(fun () ->
                                                              exit(crash)
                                                      end),

                                           receive
                                               {'EXIT', _, crash} ->
                                                   ok
                                           end
                                   end),

                    false = process_flag(trap_exit, false),

                    Parent = self(),
                    {_, MRef} =
                        erlang:spawn_monitor(
                          fun () ->
                                  try
                                      with_trap_exit(
                                        fun () ->
                                                spawn_link(fun () ->
                                                                   exit(blah)
                                                           end),

                                                timer:sleep(100),
                                                Parent ! msg
                                        end)
                                  catch
                                      exit:blah ->
                                          %% we must not be able to catch the
                                          %% shutdown
                                          throw(bad)
                                  end
                          end),

                    receive
                        {'DOWN', MRef, _, _, Reason} ->
                            ?assertEqual(blah, Reason)
                    end,

                    %% must still receive the message
                    1 = misc:flush(msg),

                    with_trap_exit(fun () ->
                                           spawn_link(fun () ->
                                                              exit(normal)
                                                      end),
                                           timer:sleep(100)
                                   end),

                    exit(finished)
                end)
     end}.
-endif.

%% Like a sequence of unlink(Pid) and exit(Pid, Reason). But care is taken
%% that the Pid is terminated even the caller dies right in between unlink and
%% exit.
unlink_terminate(Pid, Reason) ->
    with_trap_exit(
      fun () ->
              terminate(Pid, Reason),
              unlink(Pid),
              %% the process might have died before we unlinked
              ?flush({'EXIT', Pid, _})
      end).

%% Unlink, terminate and wait for the completion.
unlink_terminate_and_wait(Pid, Reason) ->
    unlink_terminate(Pid, Reason),

    %% keeping this out of with_trap_exit body to make sure that if somebody
    %% wants to kill us quickly, we let them
    wait_for_process(Pid, infinity).

-ifdef(TEST).
unlink_terminate_and_wait_simple_test() ->
    do_test_unlink_terminate_and_wait_simple(normal),
    do_test_unlink_terminate_and_wait_simple(shutdown),
    do_test_unlink_terminate_and_wait_simple(kill).

do_test_unlink_terminate_and_wait_simple(Reason) ->
    Pid = proc_lib:spawn_link(fun () -> timer:sleep(10000) end),
    unlink_terminate_and_wait(Pid, Reason),
    false = is_process_alive(Pid),

    %% make sure dead procecesses are handled too
    unlink_terminate_and_wait(Pid, Reason).

%% Test that if the killing process doesn't trap exits, we can still kill it
%% promptly.
unlink_terminate_and_wait_wont_block_test() ->
    One = proc_lib:spawn(
            fun () ->
                    process_flag(trap_exit, true),
                    timer:sleep(2000),
                    1 = ?flush({'EXIT', _, _})
            end),
    Two = proc_lib:spawn(
            fun () ->
                    link(One),
                    timer:sleep(50),
                    unlink_terminate_and_wait(One, shutdown)
            end),

    timer:sleep(100),
    exit(Two, shutdown),
    ok = wait_for_process(Two, 500),
    %% the other process terminates eventually
    ok = wait_for_process(One, 5000).


%% This tries to test that it's never possible to kill the killing process at
%% an unfortunate moment and leave the the linked process
%% alive. Unfortunately, timings are making it quite hard to test. I managed
%% to catch the original issue only when added explicit erlang:yield() between
%% unlink and exit. But leaving the test here anyway, it's better than nothing
%% after all.
unlink_terminate_and_wait_kill_the_killer_test_() ->
    {spawn,
     fun () ->
             lists:foreach(
               fun (_) ->
                       Self = self(),

                       Pid = proc_lib:spawn(fun () -> timer:sleep(10000) end),
                       Killer = proc_lib:spawn(
                                  fun () ->
                                          link(Pid),
                                          Self ! linked,
                                          receive
                                              kill ->
                                                  unlink_terminate_and_wait(Pid, kill)
                                          end
                                  end),

                       receive linked -> ok end,
                       Killer ! kill,
                       delay(rand:uniform(10000)),
                       exit(Killer, kill),

                       ok = wait_for_process(Killer, 1000),
                       ok = wait_for_process(Pid, 1000)
               end, lists:seq(1, 10000))
     end}.

delay(0) ->
    ok;
delay(I) ->
    delay(I-1).
-endif.

%% Compare two strings or binaries for equality without short-circuits
%% to avoid timing attacks.
compare_secure(<<X/binary>>, <<Y/binary>>) ->
    compare_secure(binary_to_list(X), binary_to_list(Y));
compare_secure(X, Y) when is_list(X), is_list(Y) ->
    case length(X) == length(Y) of
        true ->
            compare_secure(X, Y, 0);
        false ->
            false
    end.

compare_secure([X | RestX], [Y | RestY], Result) ->
    compare_secure(RestX, RestY, (X bxor Y) bor Result);
compare_secure([], [], Result) ->
    Result == 0.

bin_bxor(<<Bin1/binary>>, <<Bin2/binary>>) ->
    Size = size(Bin1),
    Size = size(Bin2),
    SizeBits = Size * 8,
    <<Int1:SizeBits>> = Bin1,
    <<Int2:SizeBits>> = Bin2,
    Int3 = Int1 bxor Int2,
    <<Int3:SizeBits>>.

duplicates(List) when is_list(List) ->
    List -- lists:usort(List).

-ifdef(TEST).
no_duplicates_test() ->
    ?assertEqual([],  duplicates([])),
    ?assertEqual([],  duplicates([1])),
    ?assertEqual([],  duplicates([1,2,3,"1"])),
    ?assertEqual([1,2,2], duplicates([1,2,1,2,3,2])).
-endif.

%% Generates a cryptographically strong seed which can be used in rand:seed/2
generate_crypto_seed() ->
    <<I1:32/unsigned-integer,
      I2:32/unsigned-integer,
      I3:32/unsigned-integer>> = crypto:strong_rand_bytes(12),
    {I1, I2, I3}.

%% Generates N: Lo =< N < Hi
rand_uniform(Lo, Hi) ->
    rand:uniform(Hi - Lo) + Lo - 1.

-ifdef(TEST).
rand_uniform_test() ->
    NTimes = fun G(N, _) when N =< 0 -> ok;
                 G(N, F) -> F(), G(N - 1, F)
             end,

    NTimes(10000, fun () ->
                          R = rand_uniform(10,100),
                          ?assert(lists:member(R, lists:seq(10,99)))
                  end).
-endif.

read_int_from_file(File) ->
    case raw_read_file(File) of
        {ok, Data} ->
            Str = string:strip(binary_to_list(Data), right, $\n),
            {ok, list_to_integer(Str)};
        Error -> Error
    end.
read_int_from_file(File, Default) ->
    try read_int_from_file(File) of
        {ok, N} -> N;
        {error, _} -> Default
    catch
        _:_ -> Default
    end.

is_valid_hostname(Address) ->
    case inet:parse_address(Address) of
        {ok, _} -> true;
        _ -> is_fqdn_basic_validation(Address)
    end.

is_fqdn_basic_validation(Address) ->
    Labels = string:split(Address, ".", all),
    lists:all(
      fun (Label) ->
              case re:run(Label, "^[a-zA-Z0-9-_]+$") of
                  {match, _} ->
                      true;
                  nomatch ->
                      false
              end
      end, Labels).

-ifdef(TEST).
is_fqdn_basic_validation_test() ->
    true = is_fqdn_basic_validation("-abc09_"),
    true = is_fqdn_basic_validation("a.b.c"),
    true = is_fqdn_basic_validation("abc.com"),
    false = is_fqdn_basic_validation("abc..com"),
    false = is_fqdn_basic_validation("ftp//"),
    false = is_fqdn_basic_validation("something:"),
    true = is_fqdn_basic_validation("ns_server33.services.co.woohoo").
-endif.

is_valid_iso_8601_utc(Time, Options) ->
    MsRegex = case Options of
                  [required_msecs] ->
                      "\.([0-9]{3}Z)$";
                  _ ->
                      "(\.[0-9]{3})?Z$"
              end,

    Pattern = "^([0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|"
              "[01][0-9]):([0-5][0-9]):([0-5][0-9])" ++ MsRegex,

    case re:run(Time, Pattern) of
        {match, _} ->
            true;
        nomatch ->
            false
    end.

-ifdef(TEST).
is_valid_iso_8601_utc_test() ->
    true = is_valid_iso_8601_utc("2021-08-02T07:35:49Z", []),
    true = is_valid_iso_8601_utc("2021-08-02T07:35:49.050Z", []),
    true = is_valid_iso_8601_utc("2021-08-02T07:35:49.150Z", []),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.15010Z", []),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.15Z", []),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.1Z", []),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.Z", []),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.150", []),
    false = is_valid_iso_8601_utc("2021-08-02T07:35Z", []),
    true = is_valid_iso_8601_utc("2021-08-02T07:35:49.050Z", [required_msecs]),
    true = is_valid_iso_8601_utc("2021-08-02T07:35:49.150Z", [required_msecs]),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.15010Z",
                                  [required_msecs]),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.15Z", [required_msecs]),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.1Z", [required_msecs]),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49.Z", [required_msecs]),
    false = is_valid_iso_8601_utc("2021-08-02T07:35:49Z", [required_msecs]),
    false = is_valid_iso_8601_utc("2021-08-02T07:35Z", [required_msecs]),
    false = is_valid_iso_8601_utc("dsdsd", [required_msecs]).

-endif.
is_valid_v4uuid(UUID) ->
    Pattern = "[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}"
              "\-[0-9a-fA-F]{12}",
    case re:run(UUID, Pattern) of
        {match, _} ->
           true;
        nomatch ->
           false
    end.

-ifdef(TEST).
is_valid_v4uuid_test() ->
    true = is_valid_v4uuid("fe27e478-38a1-4aa8-ba87-45244a0677f7"),
    true = is_valid_v4uuid("ce8631a6-a348-42c5-b97c-cfdc0a49d283"),
    false = is_valid_v4uuid("sdfsdfsdfsd").
-endif.

is_raw_ip(Host) ->
    case inet:parse_address(Host) of
        {ok, _} -> true;
        {error, einval} -> false
    end.

is_raw_ipv6(Host) ->
    case inet:parse_ipv6strict_address(Host) of
        {ok, _} -> true;
        {error, einval} -> false
    end.

afamily2str(inet) -> "IPv4";
afamily2str(inet6) -> "IPv6".

partitionmap(Fun, List) ->
    lists:foldr(
      fun (Elem, {AccLeft, AccRight}) ->
              case Fun(Elem) of
                  {left, Left} ->
                      {[Left | AccLeft], AccRight};
                  {right, Right} ->
                      {AccLeft, [Right | AccRight]}
              end
      end, {[], []}, List).

-ifdef(TEST).
partitionmap_test() ->
    ?assertEqual({[1,3,5,7,9], [-2,-4,-6,-8,-10]},
                 partitionmap(
                   fun (Num) ->
                           case Num rem 2 =:= 0 of
                               true ->
                                   {right, -Num};
                               false ->
                                   {left, Num}
                           end
                   end, lists:seq(1, 10))).
-endif.

align_list(_, 0, _) ->
    [];
align_list([H | T], LengthLeft, Pad) ->
    [H | align_list(T, LengthLeft - 1, Pad)];
align_list([], LengthLeft, PadElement) ->
    lists:duplicate(LengthLeft, PadElement).

-ifdef(TEST).
align_list_test() ->
    ?assertEqual([pad, pad, pad, pad], align_list([], 4, pad)),
    ?assertEqual([a, b, pad, pad], align_list([a, b], 4, pad)),
    ?assertEqual([a], align_list([a, b], 1, pad)),
    ?assertEqual([a, b], align_list([a, b], 2, pad)).
-endif.

%% Sort the argument if it is a non-string (not printable) list.
sort_if_non_string_list(X) when is_list(X) ->
    case io_lib:printable_list(X) of
        true ->
            X;
        _ ->
            lists:sort(X)
    end;
sort_if_non_string_list(X) ->
    X.

-ifdef(TEST).
sort_if_non_string_list_test() ->
    ?assertEqual([], sort_if_non_string_list([])),
    ?assertEqual(a, sort_if_non_string_list(a)),
    ?assertEqual([a], sort_if_non_string_list([a])),
    ?assertEqual({b, a}, sort_if_non_string_list({b, a})),
    ?assertEqual([a, b], sort_if_non_string_list([b, a])),
    ?assertEqual("foo", sort_if_non_string_list("foo")),
    ?assertEqual(<<"foo">>, sort_if_non_string_list(<<"foo">>)).
-endif.

%% Sort a key/value list by key, also sorting the values if they are
%% non-string lists.
-spec sort_kv_list([{any(), any()}]) -> [{any(), any()}].
sort_kv_list(L) ->
    lists:sort([{K, sort_if_non_string_list(V)} || {K, V} <- L]).

-ifdef(TEST).
sort_kv_list_test() ->
    ?assertEqual([], sort_kv_list([])),
    ?assertEqual([{a, b}], sort_kv_list([{a, b}])),
    ?assertEqual([{a, b}, {c, d}], sort_kv_list([{c, d}, {a, b}])),
    ?assertEqual([{a, [b, c]}, {g, [h, i]}],
                 sort_kv_list([{g, [i, h]}, {a, [c, b]}])),
    ?assertEqual([{a, "foo"}, {b, "bar"}],
                 sort_kv_list([{b, "bar"}, {a, "foo"}])).
-endif.

format_bin(F, A) ->
    iolist_to_binary(io_lib:format(F, A)).

zipwithN(_Fun, []) -> [];
zipwithN(Fun, [L | Tail]) ->
    zipwithN(Fun, Tail, [[E] || E <- L]).
zipwithN(Fun, [], Acc) ->
    [Fun(lists:reverse(L)) || L <- Acc];
zipwithN(Fun, [L | Tail], Acc) ->
    NewAcc = lists:zipwith(fun (AccL, E) -> [E | AccL] end, Acc, L),
    zipwithN(Fun, Tail, NewAcc).

-ifdef(TEST).
zipwithN_test() ->
    Z = fun (L) -> zipwithN(fun functools:id/1, L) end,
    ?assertEqual([], Z([])),
    ?assertEqual([], Z([[]])),
    ?assertEqual([], Z([[],[]])),
    ?assertEqual([[1]], Z([[1]])),
    ?assertEqual([[1,2,3]], Z([[1],[2],[3]])),
    ?assertEqual([[1,3,5],[2,4,6]], Z([[1,2],[3,4],[5,6]])).
-endif.

bin_part_near(Bin, Pos, Len) ->
    PartStart0 = Pos - Len div 2 - 1,
    PartStart =
        if
            PartStart0 < 0 -> 0;
            PartStart0 + Len > byte_size(Bin) -> max(byte_size(Bin) - Len, 0);
            true -> PartStart0
        end,
    PartLen = min(Len, byte_size(Bin) - PartStart),
    BinPart = binary:part(Bin, {PartStart, PartLen}),
    Prefix = case PartStart > 0 of
                 true -> <<"...">>;
                 false -> <<>>
             end,
    Suffix = case PartStart + PartLen < byte_size(Bin) of
                 true -> <<"...">>;
                 false -> <<>>
             end,
    <<Prefix/binary, BinPart/binary, Suffix/binary>>.

-ifdef(TEST).
bin_part_near_test() ->
    ?assertEqual(<<>>, bin_part_near(<<>>, 0, 30)),
    ?assertEqual(<<>>, bin_part_near(<<>>, 1, 30)),
    ?assertEqual(<<"0123456789">>,  bin_part_near(<<"0123456789">>, 1, 30)),
    ?assertEqual(<<"0123456789">>,  bin_part_near(<<"0123456789">>, 5, 30)),
    ?assertEqual(<<"0123456789">>,  bin_part_near(<<"0123456789">>, 10, 30)),
    ?assertEqual(<<"01234...">>,    bin_part_near(<<"0123456789">>, 1, 5)),
    ?assertEqual(<<"...23456...">>, bin_part_near(<<"0123456789">>, 5, 5)),
    ?assertEqual(<<"...56789">>,    bin_part_near(<<"0123456789">>, 10, 5)),
    ?assertEqual(<<"...56789">>,    bin_part_near(<<"0123456789">>, 11, 5)).
-endif.

uuid_v4() ->
    %% From RFC 4122
    %% 4.4.  Algorithms for Creating a UUID from Truly Random or
    %% Pseudo-Random Numbers
    %%
    %% The version 4 UUID is meant for generating UUIDs from truly-random or
    %% pseudo-random numbers.
    %% The algorithm is as follows:
    %% o  Set the two most significant bits (bits 6 and 7) of the
    %% clock_seq_hi_and_reserved to zero and one, respectively.
    %%
    %% o  Set the four most significant bits (bits 12 through 15) of the
    %% time_hi_and_version field to the 4-bit version number from
    %% Section 4.1.3.
    %% For version 4, it's 4.
    %%
    %% o  Set all the other bits to randomly (or pseudo-randomly) chosen
    %% values.
    <<B1:48, _:4, B2:12, _:2, B3:62>> = crypto:strong_rand_bytes(16),

    <<TimeLow:32, TimeMid:16, TimeHiVersion:16,
      ClockSeqHiReserved:8, ClockSeqLow:8,
      Node:48>> = <<B1:48, 0:1, 1:1, 0:1, 0:1, B2:12, 1:1, 0:1, B3:62>>,

    list_to_binary(lists:flatten(
                     io_lib:format(
                       "~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b",
                       [TimeLow, TimeMid, TimeHiVersion,
                        ClockSeqHiReserved, ClockSeqLow,
                        Node]))).

tail_of_length(List, N) ->
  case length(List) - N of
      X when X > 0 ->
          lists:nthtail(X, List);
      _ ->
          List
  end.

read_cpu_count_env() ->
    case os:getenv(?CPU_COUNT_VAR) of
        Env when Env == false; Env == "" ->
            undefined;
        CoresStr ->
            try list_to_integer(string:trim(CoresStr)) of
                0 -> undefined;
                N when N > 0 -> {ok, N}
            catch
                _:_ ->
                    ?log_error("Invalid ~s env var value: ~s",
                               [?CPU_COUNT_VAR, CoresStr]),
                    exit({invalid_cpu_count, CoresStr})
            end
    end.
