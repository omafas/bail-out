%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.

-module(ale_utils).

-compile(nowarn_export_all).
-compile(export_all).

-include("ale.hrl").

-spec loglevel_to_integer(loglevel()) -> 0..4.
loglevel_to_integer(critical) -> 0;
loglevel_to_integer(error)    -> 1;
loglevel_to_integer(warn)     -> 2;
loglevel_to_integer(info)     -> 3;
loglevel_to_integer(debug)    -> 4.

-spec loglevel_enabled(loglevel(), loglevel()) -> boolean().
loglevel_enabled(LogLevel, ThresholdLogLevel) ->
    loglevel_to_integer(LogLevel) =< loglevel_to_integer(ThresholdLogLevel).

-spec effective_loglevel(loglevel(), [loglevel()]) -> loglevel().
effective_loglevel(LoggerLogLevel, SinkLogLevels) ->
    lists:foldl(
      fun (SinkLogLevel, Acc) ->
              EffectiveLogLevel =
                  case loglevel_enabled(SinkLogLevel, LoggerLogLevel) of
                      true ->
                          SinkLogLevel;
                      false ->
                          LoggerLogLevel
                  end,

              case loglevel_enabled(EffectiveLogLevel, Acc) of
                  true ->
                      Acc;
                  false ->
                      EffectiveLogLevel
              end
      end, critical, SinkLogLevels).

-spec assemble_info(atom(), loglevel(), atom(), atom(), integer(), any()) ->
                           #log_info{}.
assemble_info(Logger, LogLevel, Module, Function, Line, UserData) ->
    Time = os:timestamp(),
    RegName =
        case erlang:process_info(self(), registered_name) of
            {registered_name, Name} ->
                Name;
            _ ->
                undefined
        end,
    Node = node(),

    #log_info{logger=Logger,
              loglevel=LogLevel,
              module=Module,
              function=Function,
              line=Line,
              time=Time,
              pid=self(),
              registered_name=RegName,
              node=Node,
              user_data=UserData}.

-spec proplists_merge([{any(), any()}], [{any(), any()}],
                      fun((any(), any(), any()) -> any())) -> [{any(), any()}].
proplists_merge(Prop1, [], _Fn) ->
    Prop1;
proplists_merge([], [_|_] = Prop2, _Fn) ->
    Prop2;
proplists_merge([{Key, Value} = H | T], Prop2, Fn) ->
    {Head, Prop2Rest} =
        case proplists:lookup(Key, Prop2) of
            none ->
                {H, Prop2};
            {Key, OtherValue} ->
                {{Key, Fn(Key, Value, OtherValue)},
                 lists:filter(fun ({PKey, _Value}) -> PKey =/= Key end, Prop2)}
        end,
    [Head | proplists_merge(T, Prop2Rest, Fn)].

proplists_merge(Prop1, Prop2) ->
    First = fun (_Key, X, _Y) -> X end,
    proplists_merge(Prop1, Prop2, First).

child_id(NameSpace, Name) ->
    NameSpaceStr = atom_to_list(NameSpace),
    NameStr = atom_to_list(Name),

    IdStr = lists:append([NameSpaceStr, "-", NameStr]),
    list_to_atom(IdStr).

sink_id(Name) ->
    child_id(sink, Name).

supported_opts(UserOpts, Supported) ->
    UserKeys = proplists:get_keys(UserOpts),
    [] =:= UserKeys -- Supported.

interesting_opts(UserOpts, Interesting) ->
    Pred = fun ({Opt, _Value}) ->
                   lists:member(Opt, Interesting);
               (Opt) ->
                   lists:member(Opt, Interesting)
           end,
    lists:filter(Pred, UserOpts).

intersperse(_X, []) ->
    [];
intersperse(_X, [H]) ->
    [H];
intersperse(X, [H | T]) ->
    [H, X | intersperse(X, T)].


%% Return offset of local time zone w.r.t UTC
get_timezone_offset(LocalTime, UTCTime) ->
    %% Do the calculations in terms of # of seconds
    DiffInSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
        calendar:datetime_to_gregorian_seconds(UTCTime),
    case DiffInSecs =:= 0 of
        true ->
            %% UTC
            utc;
        false ->
            %% Convert back to hours and minutes
            {DiffH, DiffM, _ } = calendar:seconds_to_time(abs(DiffInSecs)),
            case DiffInSecs < 0 of
                true ->
                    %% Time Zone behind UTC
                    {"-", DiffH, DiffM};
                false ->
                    %% Time Zone ahead of UTC
                    {"+", DiffH, DiffM}
            end
    end.
