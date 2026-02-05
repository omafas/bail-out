%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc helpers for validating REST API's parameters

-module(validator).

-include_lib("ns_common/include/cut.hrl").
-include("pipes.hrl").
-include("ns_common.hrl").
-include("rbac.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle/4,
         touch/2,
         validate/3,
         validate_relative/4,
         validate_multiple/3,
         validate_multi_params/3,
         post_validate_all/2,
         json_array/3,
         get_value/2,
         convert/3,
         one_of/3,
         string/2,
         trimmed_string/2,
         trimmed_string_multi_value/2,
         boolean/2,
         integer/2,
         integer/4,
         number/2,
         number/4,
         range/4,
         range/5,
         greater_or_equal/3,
         length/4,
         string/5,
         dir/2,
         email_address/2,
         time_duration/2,
         iso_8601_utc/3,
         iso_8601_parsed/2,
         v4uuid/2,
         has_params/1,
         unsupported/1,
         no_duplicate_keys/1,
         required/2,
         prohibited/2,
         prohibited/3,
         valid_in_enterprise_only/2,
         string_array/2,
         string_array/3,
         string_array/4,
         return_value/3,
         return_error/3,
         default/3,
         token_list/3,
         token_list/4,
         add_input_type/2,
         json/3,
         decoded_json/3,
         is_json/1,
         extract_internal/3,
         url/3,
         uri/3,
         regex/2,
         mutually_exclusive/3,
         non_empty_string/2,
         report_errors_for_one/3,
         validate_field_path/2,
         no_duplicate_values/1]).

%% Used for testing validators.
-ifdef(TEST).
-export([handle_proplist/2]).
-endif.

-record(state, {kv = [], touched = [], errors = []}).

handle(Fun, Req, json, Validators) ->
    handle_one(Fun, Req, with_json_object(
                           mochiweb_request:recv_body(Req), Validators));

handle(Fun, Req, json_array, Validators) ->
    handle_multiple(Fun, Req, with_json_array(
                                mochiweb_request:recv_body(Req), Validators));

handle(Fun, Req, json_map, Validators) ->
    handle_multiple(Fun, Req, with_json_map(
                                mochiweb_request:recv_body(Req), Validators));

handle(Fun, Req, form, Validators) ->
    handle(Fun, Req, add_input_type(form, mochiweb_request:parse_post(Req)),
           Validators);

handle(Fun, Req, qs, Validators) ->
    handle(Fun, Req, add_input_type(form, mochiweb_request:parse_qs(Req)),
           Validators);

handle(Fun, Req, {JSONProps} = JSONObj, Validators) when is_list(JSONProps) ->
    handle_one(Fun, Req, with_decoded_object(JSONObj, Validators));

handle(Fun, Req, {json_array, JSONArray}, Validators) when is_list(JSONArray) ->
    handle_multiple(Fun, Req, with_decoded_array(JSONArray, Validators));

handle(Fun, Req, Args, Validators) ->
    handle_one(Fun, Req, functools:chain(#state{kv = Args}, Validators)).

add_input_type(Type, Params) ->
    [{{internal, input_type}, Type} | Params].

is_json(#state{kv = Params}) ->
    json =:= proplists:get_value({internal, input_type}, Params).

validate_only(Req) ->
    proplists:get_value("just_validate",
                        mochiweb_request:parse_qs(Req)) =:= "1".

handle_multiple(Fun, Req, States) when is_list(States) ->
    Errors = [E || #state{errors = E} <- States],

    ValidationSucceed = ([] == lists:flatten(Errors)),
    case ValidationSucceed of
        true ->
            case validate_only(Req) of
                true -> report_errors_for_multiple(Req, Errors, 200);
                false -> Fun([prepare_params(S) || S <- States])
            end;
        false ->
            report_errors_for_multiple(Req, Errors, 400)
    end.

handle_one(Fun, Req, #state{errors = Errors} = State) ->
    case {validate_only(Req), Errors} of
        {false, []} ->
            Fun(prepare_params(State));
        {true, []} ->
            report_errors_for_one(Req, Errors, 200);
        {_, _} ->
            report_errors_for_one(Req, Errors, 400)
    end.

prepare_params(State) ->
    prepare_params(State, false).

prepare_params(#state{kv = Props, touched = Touched}, OnlyTouched) ->
    lists:filtermap(fun ({{internal, _}, _}) ->
                            false;
                        ({K, V}) ->
                            case lists:member(K, Touched) of
                                true ->
                                    {true, {list_to_atom(K), V}};
                                false when OnlyTouched ->
                                    false;
                                false ->
                                    {true, {K, V}}
                            end
                    end, Props).

process_fatal_errors(Req, Errors) ->
    MissingPermissions = lists:flatten([P || {_, {403, P}} <- Errors]),
    case MissingPermissions of
        [] ->
            false;
        _ ->
            ns_audit:access_forbidden(Req),
            ns_server_stats:notify_counter(<<"rest_request_access_forbidden">>),
            Resp = menelaus_web_rbac:forbidden_response(MissingPermissions),
            menelaus_util:reply_json(Req, Resp, 403),
            true
    end.

jsonify_errors(Errors) ->
    {[{Name, jsonify_error(E)} || {Name, E} <- Errors]}.

jsonify_error({json, Json}) ->
    Json;
jsonify_error(E) ->
    iolist_to_binary(E).

report_errors_for_multiple(Req, Errors, Code) ->
    case process_fatal_errors(Req, lists:flatten(Errors)) of
        true ->
            ok;
        false ->
            send_error_json(Req, [jsonify_errors(E) || E <- Errors],
                            Code)
    end.

-spec report_errors_for_one(mochiweb_request(), list(), non_neg_integer()) ->
          ok | mochiweb_response().
report_errors_for_one(Req, Errors, Code) ->
    case process_fatal_errors(Req, Errors) of
        true ->
            ok;
        false ->
            send_error_json(Req, jsonify_errors(Errors), Code)
    end.

send_error_json(Req, Errors, Code) ->
    menelaus_util:reply_json(Req, {[{errors, Errors}]}, Code).

json_array(Name, Validators, State) ->
    validate(
      fun (JsonArray) when is_list(JsonArray) ->
              States = [with_decoded_object(Elem, Validators) ||
                           Elem <- JsonArray],
              Errors = [ErrorList || #state{errors = ErrorList} <- States],
              case lists:flatten(Errors) of
                  [] ->
                      {value, [{prepare_params(St)} || St <- States]};
                  _ ->
                      {error, {json, [jsonify_errors(ErrorList) ||
                                         ErrorList <- Errors]}}
              end;
          (_) ->
              {error, "The value must be a json array"}
      end, Name, State).

with_decoded_object({KVList}, Validators) ->
    Params = [{binary_to_list(Name), Value} || {Name, Value} <- KVList],
    functools:chain(#state{kv = add_input_type(json, Params)}, Validators);
with_decoded_object(Value, Validators) when is_binary(Value) ->
    %% Instead of rejecting json which has a value as the root, store it in an
    %% internal key to possibly be parsed with extract_internal/3
    Params = [{{internal, root}, Value}],
    functools:chain(#state{kv = add_input_type(json, Params)}, Validators);
with_decoded_object(_, _) ->
    #state{errors = [{<<"_">>, <<"Unexpected Json">>}]}.

validate_decoded_object(DecodedObject, Validators) ->
    St = with_decoded_object(DecodedObject, Validators),
    case St#state.errors of
        [] ->
            {value, prepare_params(St)};
        _ ->
            {error, {json, jsonify_errors(St#state.errors)}}
    end.

decoded_json(Name, Validators, State) ->
    validate(validate_decoded_object(_, Validators), Name, State).

json(Name, Validators, State) ->
    validate(
      fun (BinJson) ->
              try ejson:decode(BinJson) of
                  Object ->
                      validate_decoded_object(Object, Validators)
              catch _:_ ->
                        {error, {json, {[{<<"_">>, <<"Invalid Json">>}]}}}
              end
      end, Name, State).

with_json_object(Body, Validators) ->
    try ejson:decode(Body) of
        Object ->
            with_decoded_object(Object, Validators)
    catch _:_ ->
            #state{errors = [{<<"_">>, <<"Invalid Json">>}]}
    end.

with_json_array(Body, Validators) ->
    try ejson:decode(Body) of
        Objects when is_list(Objects) ->
            [with_decoded_object(Object, Validators) || Object <- Objects];
        _ ->
            [#state{errors = [{<<"_">>, <<"A Json list must be specified.">>}]}]
    catch _:_ ->
            [#state{errors = [{<<"_">>, <<"Invalid Json">>}]}]
    end.

with_json_map(Body, Validators) ->
    try ejson:decode(Body) of
        {KVS} when is_list(KVS) ->
            Objects = [{[{<<"key">>, K} | Props]} || {K, {Props}} <- KVS],
            case length(Objects) < length(KVS) of
                true ->
                    [#state{
                        errors =
                            [{<<"_">>, <<"Must be a map K -> JsonObject.">>}]}];
                false ->
                    [with_decoded_object(Object, Validators) ||
                        Object <- Objects]
            end;
        _ ->
            [#state{errors = [{<<"_">>, <<"A Json map must be specified.">>}]}]
    catch _:_ ->
            [#state{errors = [{<<"_">>, <<"Invalid Json">>}]}]
    end.

with_decoded_array(JSONArray, Validators) ->
    [with_decoded_object(Object, Validators) || Object <- JSONArray].

name_to_list(Name) when is_atom(Name) ->
    atom_to_list(Name);
name_to_list(Name) when is_list(Name) ->
    Name.


get_value(Name, #state{kv = Props, errors = Errors}) ->
    LName = name_to_list(Name),
    case proplists:get_value(LName, Props) of
        undefined ->
            undefined;
        Value ->
            case lists:keymember(LName, 1, Errors) of
                true ->
                    undefined;
                false ->
                    Value
            end
    end.

touch(Name, #state{touched = Touched} = State) ->
    LName = name_to_list(Name),
    case lists:member(LName, Touched) of
        true ->
            State;
        false ->
            State#state{touched = [LName | Touched]}
    end.

return_value(Name, Value, #state{kv = Props} = State) ->
    LName = name_to_list(Name),
    State1 = touch(LName, State),
    State1#state{kv = lists:keystore(LName, 1, Props, {LName, Value})}.

return_error(Name, Error, #state{errors = Errors} = State) ->
    State#state{errors = [{name_to_list(Name), Error} | Errors]}.

validate(Fun, Name, State0) ->
    State = touch(Name, State0),
    case get_value(Name, State) of
        undefined ->
            State;
        Value when is_function(Fun, 1) ->
            case Fun(Value) of
                ok ->
                    State;
                {value, V} ->
                    return_value(Name, V, State);
                {error, Error} ->
                    return_error(Name, Error, State)
            end;
        Value when is_function(Fun, 2) ->
            case Fun(Value, State) of
                {ok, NewState} ->
                    NewState;
                {value, V, NewState} ->
                    return_value(Name, V, NewState);
                {error, Error, NewState} ->
                    return_error(Name, Error, NewState)
            end
    end.

%% This function is intended to be used to validate dependencies between
%% already parsed params. For this reason it is not called if any errors
%% happens before it. And also params that have not been touched (validated) yet
%% are not passed to the dependency checking function.
post_validate_all(Fun, State) ->
    AllValuesSoFar = prepare_params(State, true),
    case State#state.errors of
        [] ->
            if
                is_function(Fun, 1) ->
                    case Fun(AllValuesSoFar) of
                        ok -> State;
                        {error, Error} -> return_error("_", Error, State)
                    end;
                is_function(Fun, 2) ->
                    case Fun(AllValuesSoFar, State) of
                        {ok, NewState} -> NewState;
                        {error, Error, NewState} ->
                            return_error("_", Error, NewState)
                    end
            end;
        _ ->
            State
    end.

validate_relative(Fun, Name, NameRel, State) ->
    validate(
      fun (V) ->
              case get_value(NameRel, State) of
                  undefined ->
                      ok;
                  VRel ->
                      Fun(V, VRel)
              end
      end, Name, State).

validate_multiple(Fun, Names, State) ->
    {Values, NewState} = lists:mapfoldl(
                           fun (N, Acc) ->
                               Acc2 = touch(N, Acc),
                               {get_value(N, Acc2), Acc2}
                           end, State, Names),
    AllUndefined = lists:all(fun (V) -> V == undefined end, Values),
    case AllUndefined of
        true -> NewState;
        false when is_function(Fun, 1) ->
            case Fun(Values) of
                ok -> NewState;
                {error, Error} -> return_error("_", Error, NewState)
            end;
        false when is_function(Fun, 2) ->
            case Fun(Values, NewState) of
                {ok, NewState1} ->
                    NewState1
            end
    end.

convert(Name, Fun, State) ->
    validate(?cut({value, Fun(_)}), Name, State).

simple_term_to_list(X) when is_atom(X) ->
    atom_to_list(X);
simple_term_to_list(X) when is_integer(X) ->
    integer_to_list(X);
simple_term_to_list(X) when is_binary(X) ->
    binary_to_list(X);
simple_term_to_list(X) when is_list(X) ->
    X.

simple_term_to_atom(X) when is_binary(X) ->
    list_to_atom(binary_to_list(X));
simple_term_to_atom(X) when is_list(X) ->
    list_to_atom(X);
simple_term_to_atom(X) when is_atom(X) ->
    X.

simple_term_to_integer(X) when is_list(X) ->
    erlang:list_to_integer(X);
simple_term_to_integer(X) when is_integer(X) ->
    X.

simple_term_to_number(X) when is_list(X) ->
    try erlang:list_to_integer(X)
    catch _:_ ->
            erlang:list_to_float(X)
    end;
simple_term_to_number(X) when is_integer(X) ->
    X;
simple_term_to_number(X) when is_float(X) ->
    X.

one_of(Name, List, State) ->
    StringList = [simple_term_to_list(X) || X <- List],
    validate(
      fun (Value) ->
              StringValue = (catch simple_term_to_list(Value)),
              case lists:member(StringValue, StringList) of
                  true ->
                      ok;
                  false ->
                      {error,
                       io_lib:format(
                         "The value must be one of the following: [~s]",
                         [string:join(StringList, ",")])}
              end
      end, Name, State).

string_trim_logic(Trim, State) ->
    case is_json(State) of
        true ->
            fun (Binary) when is_binary(Binary), Trim ->
                    {value, string:trim(binary_to_list(Binary))};
                (Binary) when is_binary(Binary) ->
                    {value, binary_to_list(Binary)};
                (_) ->
                    {error, "Value must be json string"}
            end;
        false when Trim ->
            fun (S) -> {value, string:trim(S)} end;
        false ->
            fun (_) -> ok end
    end.

get_all_values(LName, #state{kv = Props, errors = Errors}) ->
    case lists:keymember(LName, 1, Errors) of
        true ->
            undefined;
        false ->
            case proplists:get_all_values(LName, Props) of
                [] ->
                    undefined;
                Values ->
                    Values
            end
    end.

return_multi_value(LName, Values, #state{kv = Props} = State) ->
    PrunedProps = proplists:delete(LName, Props),
    UpdateAttrs = [{LName, X} || X <- Values],
    NewKv = UpdateAttrs ++ PrunedProps,
    State#state{kv = NewKv}.

apply_multi_params(Fun, Values) ->
    EvalRes = lists:map(Fun, Values),
    case proplists:get_all_values(error, EvalRes) of
        [] ->
            V = [Y || {_, Y} <- EvalRes],
            {values, V};
        Errors ->
            {errors, Errors}
    end.

validate_multi_params(Fun, Name, State0) ->
    State = touch(Name, State0),
    LName = name_to_list(Name),
    case get_all_values(LName, State) of
        undefined ->
            State;
        Values ->
            case apply_multi_params(Fun, Values) of
                {values, V} ->
                    return_multi_value(LName, V, State);
                {errors, Errors} ->
                    [Error | _] = Errors,
                    return_error(Name, Error, State)
            end
    end.

trimmed_string_multi_value(Name, State) ->
    validate_multi_params(string_trim_logic(true, State), Name, State).

trimmed_string(Name, State) ->
    string(Name, true, State).

string(Name, State) ->
    string(Name, false, State).

string(Name, Trim, State) ->
    validate(string_trim_logic(Trim, State), Name, State).

boolean(Name, State) ->
    functools:chain(State,
                    [one_of(Name, [true, false], _),
                     convert(Name, fun simple_term_to_atom/1, _)]).

integer(Name, State) ->
    validate(
      fun (Value) ->
              Int = (catch simple_term_to_integer(Value)),
              case is_integer(Int) of
                  true ->
                      {value, Int};
                  false ->
                      {error, "The value must be an integer"}
              end
      end, Name, State).

integer(Name, Min, Max, State) ->
    functools:chain(State,
                    [integer(Name, _),
                     range(Name, Min, Max, _)]).

number(Name, State) ->
    validate(
      fun (Value) ->
              Num = (catch simple_term_to_number(Value)),
              case is_number(Num) of
                  true ->
                      {value, Num};
                  false ->
                      {error, "The value must be a number"}
              end
      end, Name, State).

number(Name, Min, Max, State) ->
    functools:chain(State,
                    [number(Name, _),
                     range(Name, Min, Max, _)]).

range_error(Min, infinity) ->
    io_lib:format("The value must be greater than or equal to ~p", [Min]);
range_error(Min, max_uint64) ->
    range_error(Min, ?MAX_64BIT_UNSIGNED_INT);
range_error(Min, Max) ->
    io_lib:format("The value must be in range from ~p to ~p (inclusive)",
                  [Min, Max]).

range(Name, Min, Max, State) ->
    range(Name, Min, Max, ?cut(range_error(Min, Max)), State).

range(Name, Min, max_uint64, ErrorFun, State) ->
    range(Name, Min, ?MAX_64BIT_UNSIGNED_INT, ErrorFun, State);
range(Name, Min, Max, ErrorFun, State) ->
    validate(
      fun (Value) ->
              case Value >= Min andalso (Max == infinity orelse Value =< Max) of
                  true ->
                      ok;
                  false ->
                      {error, lists:flatten(ErrorFun())}
              end
      end, Name, State).

-ifdef(TEST).

range_test() ->
    %% Test range validation with finite bounds
    State1 = #state{kv=[{"test", 5}]},
    ?assertEqual([], (range("test", 1, 10, State1))#state.errors),
    ?assertEqual([], (range("test", 5, 10, State1))#state.errors),
    ?assertEqual([], (range("test", 1, 5, State1))#state.errors),

    %% Test range validation failures
    State2 = #state{kv=[{"test", 0}]},
    #state{errors=[{_, Err2}]} = range("test", 1, 10, State2),
    ?assertEqual(Err2, "The value must be in range from 1 to 10 (inclusive)"),

    State3 = #state{kv=[{"test", 11}]},
    #state{errors=[{_, Err3}]} = range("test", 1, 10, State3),
    ?assertEqual(Err3, "The value must be in range from 1 to 10 (inclusive)"),

    %% Test range validation with infinity upper bound
    State4 = #state{kv=[{"test", 1000}]},
    ?assertEqual([], (range("test", 1, infinity, State4))#state.errors),

    State5 = #state{kv=[{"test", 0}]},
    #state{errors=[{_, Err5}]} = range("test", 1, infinity, State5),
    ?assertEqual(Err5, "The value must be greater than or equal to 1"),

    %% Test range validation with max_uint64 upper bound
    State6 = #state{kv=[{"test", ?MAX_64BIT_UNSIGNED_INT}]},
    ?assertEqual([], (range("test", 1, max_uint64, State6))#state.errors),

    State7 = #state{kv=[{"test", ?MAX_64BIT_UNSIGNED_INT + 1}]},
    #state{errors=[{_, Err7}]} = range("test", 1, max_uint64, State7),
    ?assertEqual(Err7, "The value must be in range from 1 to " ++
                       integer_to_list(?MAX_64BIT_UNSIGNED_INT) ++
                       " (inclusive)").

-endif.

greater_or_equal(Name1, Name2, State) ->
     validator:validate_relative(
       fun (Value1, Value2) when Value1 < Value2 ->
               {error,
                io_lib:format("should be greater than or equal to ~p", [Name2])};
           (_, _) ->
               ok
       end, Name1, Name2, State).

length(Name, Min, Max, State) ->
    validate(
      fun (Value) ->
              %% Mochiweb converts the utf8 string to a list, which isn't
              %% correct, so we need to undo that conversion here.
              case unicode:characters_to_binary(list_to_binary(Value)) of
                  {incomplete, _, _} ->
                      {error,
                       io_lib:format("Incomplete utf8 name ~p", [Value])};
                  {error, _, _} ->
                      {error,
                       io_lib:format("Ill-formed utf8 name ~p", [Value])};
                  BinaryChars ->
                      Length = string:length(BinaryChars),
                      case Length < Min orelse Length > Max of
                          true ->
                              {error,
                               io_lib:format("Name length (~p) must be in the "
                                             "range from ~p to ~p, inclusive",
                                             [Length, Min, Max])};
                          false ->
                              ok
                      end
              end
      end, Name, State).

string(Name, Regex, Options, ErrorStr, State) ->
    validate(
      fun (Value) ->
              StringValue = (catch simple_term_to_list(Value)),
              case re:run(StringValue, Regex, Options) of
                  {match, _} ->
                      ok;
                  nomatch ->
                      ErrorOut = io_lib:format("Invalid value ~p : ~p",
                                               [Value, ErrorStr]),
                      {error, ErrorOut}
              end
      end, Name, State).

dir(Name, State) ->
    validate(fun (Value) ->
                     case filelib:is_dir(Value) of
                         true ->
                             ok;
                         false ->
                             {error, "The value must be a valid directory"}
                     end
             end, Name, State).

email_address(Name, State) ->
    validate(fun (Value) ->
                     case menelaus_util:validate_email_address(Value) of
                         true ->
                             ok;
                         false ->
                             {error, "The value must be a valid email address"}
                     end
             end, Name, State).

iso_8601_utc(Name, Options, State) ->
    validate(fun (Value) ->
                case misc:is_valid_iso_8601_utc(Value, Options) of
                    true ->
                        ok;
                    false ->
                        {error, "The value must be a valid ISO 8601 UTC"}
                end
             end, Name, State).

iso_8601_parsed(Name, State) ->
    validator:validate(
      fun (DT) ->
          try
              {value, iso8601:parse(DT)}
          catch
              _:_ -> {error, "The value must be a valid ISO 8601"}
          end
      end, Name, State).

v4uuid(Name, State) ->
    validate(fun (Value) ->
                case misc:is_valid_v4uuid(Value) of
                    true ->
                        ok;
                    false ->
                        {error, "The value must be a valid v4 UUID"}
                end
             end, Name, State).

%% Validates a subset of the golang time.duration values
time_duration(Name, State) ->
    string(Name, "^[0-9]+(ns|us|ms|s|m|h)$", [dollar_endonly],
           "Must be in the form of number{ns|us|ms|s|m|h}",
           State).

%% Validate whether a request has parameters
-spec has_params(State :: #state{}) -> #state{}.
has_params(#state{kv = Kv} = State) ->
    case lists:filter(
           fun ({{internal, _}, _}) -> false;
               (_) -> true
           end,
           Kv) of
        [] -> return_error("_", "Request should have parameters", State);
        _ -> State
    end.

unsupported(#state{kv = Props, touched = Touched, errors = Errors} = State) ->
    NewErrors =
        lists:filtermap(
          fun({Name, _}) ->
                  case is_tuple(Name) orelse lists:member(Name, Touched) of
                      true ->
                          false;
                      false ->
                          {true, {Name, <<"Unsupported key">>}}
                  end
          end, Props),
    State#state{errors = NewErrors ++ Errors}.

has_duplicate_keys([{Key, _Value} | T], DupKeys) ->
    case proplists:is_defined(Key, T) of
        true ->
            has_duplicate_keys(T,lists:append(DupKeys, [name_to_list(Key)]));
        false ->
            has_duplicate_keys(T,DupKeys)
    end;
has_duplicate_keys([H | T], DupKeys) ->
    case proplists:is_defined(H, T) of
        true ->
            has_duplicate_keys(T,lists:append(DupKeys, [name_to_list(H)]));
        false ->
            has_duplicate_keys(T,DupKeys)
    end;
has_duplicate_keys([],DupKeys) -> DupKeys.

no_duplicate_keys(#state{kv = Props} = State) ->
    case has_duplicate_keys(Props, []) of
        [] ->
            State;
        DuplicateKeysList ->
            DuplicateKeysMsg = lists:join(", ", DuplicateKeysList),
            return_error("_", io_lib:format(
                                "Keys specified multiple times: ~s",
                                [DuplicateKeysMsg]), State)
    end.

value_frequencies([], Counts) ->
    maps:to_list(Counts);
value_frequencies([H|T], Counts) ->
    Inc = fun(Value) -> Value + 1 end,
    NewCounts = maps:update_with(H, Inc, 1, Counts),
    value_frequencies(T, NewCounts).

% Helper function for no_duplicate_values
has_duplicate_values(Props) ->
    Values = [V || {_, V} <- Props],
    Counts = value_frequencies(Values, #{}),
    DuplicateValues = [A || {A, B} <- Counts, B > 1],
    [name_to_list(A) || {A, B} <- Props, lists:member(B, DuplicateValues)].

no_duplicate_values(#state{kv = Props} = State) ->
    case has_duplicate_values(Props) of
        [] ->
            State;
        DuplicateKeysList ->
            DuplicateKeysMsg = lists:join(", ", DuplicateKeysList),
            return_error("_", io_lib:format(
                                "Keys specified have duplicate values: ~s",
                                [DuplicateKeysMsg]), State)
    end.

required(Name, #state{kv = Props} = State) ->
    functools:chain(
      State,
      [touch(Name, _),
       fun (St) ->
          case lists:keymember(name_to_list(Name), 1, Props) of
              false ->
                  return_error(Name, "The value must be supplied", St);
              true ->
                  St
          end
       end]).

prohibited(Name, State) ->
    prohibited(Name, "The value must not be supplied", State).

prohibited(Name, Error, #state{kv = Props} = State) ->
    case lists:keymember(name_to_list(Name), 1, Props) of
        false ->
            State;
        true ->
            return_error(Name, Error, State)
    end.

is_changeable(Name, Pred, State) ->
    PredValue = Pred(),
    validate(
        fun (_) when PredValue -> ok;
            (_) ->
                {error, PredValue}
        end,
        Name, State).

%% Validate a parameter that may only be set in enterprise edition.
valid_in_enterprise_only(Name, State) ->
    IsEnterprise = cluster_compat_mode:is_enterprise(),
    Pred = fun () when IsEnterprise -> true;
               () -> "Supported in enterprise edition only"
           end,
    is_changeable(Name, Pred, State).

string_array(Name, State) ->
    string_array(Name, fun (_) -> ok end, State).

-spec string_array(atom(), Fun, #state{}) -> #state{} when
      Fun :: fun((string()) -> ok | {value, term()} | {error, string()}).
string_array(Name, Fun, State) ->
    string_array(Name, Fun, true, State).

string_array(Name, Fun, CanBeEmpty, State) ->
    validate(
      fun ([]) when not CanBeEmpty ->
              {error, "Must contain at least one element"};
          (Array) when is_list(Array) ->
              case lists:all(?cut(is_binary(_1) andalso _1 =/= <<>>), Array) of
                  false ->
                      {error, "Must be an array of non-empty strings"};
                  true ->
                      List = [binary_to_list(B) || B <- Array],
                      validate_fun_tokens(List, Fun)
              end;
          (_) ->
              {error, "Must be an array of non-empty strings"}
      end, Name, State).

-spec validate_fun_tokens([string()], Fun) ->
          {value, list()} | {error, string()} when
      Fun :: fun((string()) -> ok | {value, term()} | {error, string()}).
validate_fun_tokens(List, Fun) ->
    List2 = lists:map(fun (V) ->
                              case Fun(V) of
                                  ok -> {value, V};
                                  {value, V2} -> {value, V2};
                                  {error, E} ->
                                      {error, V ++ " - " ++ E}
                              end
                      end, List),
    Errors = [E || {error, E} <- List2],
    case Errors of
        [] -> {value, [V || {value, V} <- List2]};
        _ -> {error, string:join(Errors, "; ")}
    end.

default(Name, Default, State) ->
    case get_value(Name, touch(Name, State)) of
        undefined when is_function(Default) ->
            return_value(Name, Default(), State);
        undefined ->
            return_value(Name, Default, State);
        _Value ->
            State
    end.

-spec token_list(atom(), string(), #state{}) -> #state{}.
token_list(Name, Separator, State) ->
    token_list(Name, Separator, fun (_) -> ok end, State).

-spec token_list(atom(), string(), Fun, #state{}) -> #state{} when
      Fun :: fun((string()) -> ok | {value, term()} | {error, string()}).
token_list(Name, Separator, Fun, State) ->
    validate(
      fun (String) ->
              validate_fun_tokens(string:lexemes(String, Separator), Fun)
      end, Name, State).

%% Get the internal value at {internal, InternalKey} and store it in NewKey
%% This can be used for instance to parse json which has a value as the root.
%% For example, '["travel-sample", "gamesim-sample"]' handled with json_array
%% would be handled with two states, each with {{internal, root}, "...-sample"}
%% in #state.kv. extract_internal(root, sample, State) would then store the
%% corresponding value at the "sample" key, allowing further validation to occur
%% as usual.
-spec extract_internal(atom(), atom() | list(), #state{}) -> #state{}.
extract_internal(InternalKey, NewKey, #state{kv = KV} = State) ->
    case proplists:get_value({internal, InternalKey}, KV) of
        undefined -> State;
        Value -> return_value(NewKey, Value, State)
    end.

url(Name, Schemes, State) ->
    validate(
      fun (Str) ->
          Validation = fun (S) ->
                           case lists:member(S, Schemes) of
                               true -> valid;
                               false -> {error, invalid_scheme}
                           end
                       end,
          case misc:parse_url(Str, [{scheme_validation_fun, Validation}]) of
              {ok, _} -> ok;
              {error, _} -> {error, "Invalid URL"}
          end
      end, Name, State).

uri(Name, Schemes, State) ->
    validate(
      fun (Str) ->
              case misc:is_valid_uri(Str, Schemes) of
                  true ->
                      ok;
                  false ->
                      {error, "Invalid URI"}
              end
      end, Name, State).

regex(Name, State) ->
    validate(
      fun (Str) ->
          case re:compile(Str) of
              {ok, _} -> ok;
              {error, {Error, At}} ->
                  Err = io_lib:format("~s (at character #~b)", [Error, At]),
                  {error, lists:flatten(Err)}
          end
      end, Name, State).

mutually_exclusive(Name1, Name2, State) ->
    validate_relative(
       fun (_, _) ->
           Err = io_lib:format("~p and ~p are mutually exclusive",
                               [Name1, Name2]),
           {error, lists:flatten(Err)}
       end, Name1, Name2, State).

non_empty_string(Name, State) ->
    functools:chain(State,
                    [string(Name, _),
                     validate(
                       fun("") -> {error, "Value must not be empty"};
                          (Value) -> {value, Value}
                       end, Name, _)]).

%% Validates a JSON field path that can be nested using dot notation.
-spec validate_field_path(atom(), #state{}) -> #state{}.
validate_field_path(Name, State) ->
    validate(
      fun(X) ->
              case re:run(X, "^[^.]+(\\.[^.]+)*$") of
                  {match, _} -> {value, X};
                  nomatch -> {error,
                              "Invalid field path format"}
                  end
      end, Name, State).

-ifdef(TEST).
%% Apply the validators to the arguments, returning the validated
%% arguments if validation succeeds or a list of errors if validation fails.
%% Used for testing validators.
-spec handle_proplist(Args, Validators) ->  {ok, Values} | {error, Errors} when
      Args :: [tuple()],
      Validators :: [fun()],
      Values :: [{atom() | string(), any()}],
      Errors :: [{string(), string()}].
handle_proplist(Args, Validators) ->
    State = functools:chain(#state{kv = Args}, Validators),
    case State#state.errors of
        [] -> {ok, prepare_params(State)};
        Errors -> {error, Errors}
    end.
-endif.

-ifdef(TEST).
%% Validates that the length of the value is in range, returning the resulting
%% error array.
length_in_range(Value, Min, Max) ->
    #state{errors = E} = length(name, Min, Max, #state{kv=[{"name", Value}]}),
    E.

assert_in_range(Value, Length, Min, Max) ->
    ?assertEqual(length_in_range(Value, Min, Max), [],
                 io:format("Length of '~ts' (~p) must be in the range "
                           "~p to ~p, inclusive",
                           [Value, Length, Min, Max])).

assert_not_in_range(Value, Length, Min, Max) ->
    ?assertNotEqual(length_in_range(Value, Min, Max), [],
                 io:format("Length of '~ts' (~p) should not fall in the "
                           "range ~p to ~p, inclusive",
                           [Value, Length, Min, Max])).

length_tester(Value, Length) ->
    %% Length is exactly right.
    assert_in_range(Value, Length, Length, Length),

    %% Length falls completely inside the range.
    assert_in_range(Value, Length, Length - 1, Length + 1),

    %% Length falls in the range, but at the lower bound.
    assert_in_range(Value, Length, Length, Length + 1),

    %% Length is less than the lower bound.
    assert_not_in_range(Value, Length, Length -1, Length - 1),

    %% Length falls in the range, but at the upper bound.
    assert_in_range(Value, Length, Length - 1, Length),

    %% Length is greater than the upper bound.
    assert_not_in_range(Value, Length, Length + 1, Length + 1).

%% We'd like this to directly determine if length/4 returns an error, but
%% that's tricky due to the way that length/4 is tied to validate/3.
assert_length_error(Value, Length) ->
    ?assertNotEqual(length_in_range(Value, Length, Length), [],
                    io:format("length/4 of '~ts' should produce an error",
                              [Value])).

length_test() ->
    length_tester(binary_to_list(<<""/utf8>>), 0),
    length_tester(binary_to_list(<<"1" /utf8>>), 1),
    length_tester(binary_to_list(<<"ß↑e̊"/utf8>>), 3),
    length_tester(binary_to_list(<<"12345" /utf8>>), 5),

    %% An ill-formed utf8 string, which we expect to produce an error.
    assert_length_error(
      binary_to_list(<<"g5DEWBlmDJhJ"/utf8, 16#EE, "Lx9Fa"/utf8>>), 18).

validators_for_testing() ->
    [validator:required(required_boolean, _),
     validator:boolean(required_boolean, _),
     validator:string(optional_string, _),
     validator:default(optional_string, "default value", _)].

handle_proplist_all_valid_test() ->
    Args =
        [{"optional_string", "some string"},
         {"required_boolean", "true"}],
    ExpectedKv =
        [{optional_string, "some string"},
         {required_boolean, true}],

    {ok, Kv} = handle_proplist(Args, validators_for_testing()),
    ?assertEqual(lists:sort(ExpectedKv), lists:sort(Kv)).

handle_proplist_invalid_value_test() ->
    Args =
        [{"optional_string", "some string"},
         {"required_boolean", "not boolean"}],
    ExpectedErrorMessage = io_lib:format(
                             "The value must be one of the following: [~s]",
                             [string:join(["true", "false"], ",")]),

    {error, [{ErrorKey, ErrorMessage}]} =
        handle_proplist(Args, validators_for_testing()),
    ?assertEqual("required_boolean", ErrorKey),
    ?assertEqual(ExpectedErrorMessage, ErrorMessage).

handle_multi_value_string_trim_valid_test() ->
    Kv = [{"key1", "  /Value1"}, {"key1", "  /Value2  "}, {"key2", "  /Value3"}],
    StateArg = #state{kv = Kv},
    RState = trimmed_string_multi_value(key1, StateArg),

    % Test validator state output for key1 with multiple values
    #state{kv = Rkv, touched = Rtouched, errors = RErrors} = RState,
    ?assertEqual([], RErrors),
    ?assertEqual(["key1"], Rtouched),
    ?assertEqual(["/Value1", "/Value2"], proplists:get_all_values("key1", Rkv)),
    ?assertEqual(["  /Value3"], proplists:get_all_values("key2", Rkv)),

    % Test validator chained state output for key2 with single value
    RState2 = trimmed_string_multi_value(key2, RState),
    #state{kv = Rkv2, touched = Rtouched2, errors = RErrors2} = RState2,
    ?assertEqual([], RErrors2),
    ?assertEqual(["key2", "key1"], Rtouched2),
    ?assertEqual(["/Value1", "/Value2"], proplists:get_all_values("key1", Rkv2)),
    ?assertEqual(["/Value3"], proplists:get_all_values("key2", Rkv2)).

handle_multi_value_string_trim_invalid_test() ->
    Kv = [{{internal,input_type},json}, {"key1", <<"Value1"/utf8>>},
          {"key1", "  /Value2"}, {"key2", "  /Value3"}],

    % Validator for key1 should produce error
    StateArg = #state{kv = Kv},
    RState = trimmed_string_multi_value(key1, StateArg),
    #state{errors = RErrors} = RState,
    ?assertEqual([{"key1", "Value must be json string"}], RErrors),

    % State should remain unchanged on same key validation
    RState2 = trimmed_string_multi_value(key1, RState),
    ?assertEqual(RState, RState2),

    % Validator for key2 should produce error
    RState3 = trimmed_string_multi_value(key2, RState2),
    #state{errors = RErrors2} = RState3,
    ?assertEqual([{"key2", "Value must be json string"},
                  {"key1", "Value must be json string"}], RErrors2).

has_params_test() ->

    %% Validate presence of params
    Kv1 = [{key, value}],
    State1 = #state{kv=Kv1},
    ?assertEqual(has_params(State1), State1),

    %% Validate internal state fields don't interfere with presence of params
    Kv2 = [{{internal,input_type},json}, {key, value}],
    State2 = #state{kv=Kv2},
    ?assertEqual(has_params(State2), State2),

    %% Validate adding new internal state fields doesn't interfere with presence
    %% of params
    Kv3 = [{{internal,input_type},json}, {{internal,other_type},other_value},
           {key, value}],
    State3 = #state{kv=Kv3},
    ?assertEqual(has_params(State3), State3),

    %% Validate internal state fields are not considered params
    Kv4 = [{{internal,input_type},json}],
    State4 = #state{kv=Kv4},
    #state{errors=RErrors4} = has_params(State4),
    ?assertEqual([{"_", "Request should have parameters"}], RErrors4),

    %% Validate new internal state fields are not considered params
    Kv5 = [{{internal,input_type},json}, {{internal,other_type},other_value}],
    State5 = #state{kv=Kv5},
    #state{errors=RErrors5} = has_params(State5),
    ?assertEqual([{"_", "Request should have parameters"}], RErrors5),

    %% Validate absense of params with no internal state fields
    Kv6 = [],
    State6 = #state{kv=Kv6},
    #state{errors=RErrors6} = has_params(State6),
    ?assertEqual([{"_", "Request should have parameters"}], RErrors6).

json_root_test() ->
    %% Root value is stored in {internal, root}
    State1 = with_decoded_object(<<"value">>, []),
    ?assertEqual(<<"value">>,
                 proplists:get_value({internal, root},
                                     State1#state.kv)),
    %% Root value can be extracted and validated
    State2 = with_decoded_object(<<"value">>,
                                 [extract_internal(root, key, _),
                                  string(key, _)]),
    ?assertEqual("value", get_value(key, State2)).

check_for_duplicate_keys_test() ->
    %% Duplicate key in tuple
    Kv1 = [{key1, value1}, {key2, value2}, {key1, value3}],
    State1 = #state{kv = Kv1},
    State1a = no_duplicate_keys(State1),
    #state{errors = Errors1} = State1a,
    ?assertEqual([{"_", io_lib:format(
                          "Keys specified multiple times: ~s",
                          [["key1"]])}], Errors1),

    %% No duplicates in tuples
    Kv2 = [{key1, value1}, {key2, value2}, {key3, value3}],
    State2 = #state{kv = Kv2},
    ?assertEqual(State2, no_duplicate_keys(State2)),

    %% Duplicate in list
    Kv3 = [apple, pie, ice_cream, apple, cake],
    State3 = #state{kv = Kv3},
    State3a = no_duplicate_keys(State3),
    #state{errors = Errors3} = State3a,
    ?assertEqual([{"_", io_lib:format(
                          "Keys specified multiple times: ~s",
                          [["apple"]])}], Errors3),

    %% No duplicate in list
    Kv4 = [apple, pie, ice_cream, cake],
    State4 = #state{kv = Kv4},
    ?assertEqual(State4, no_duplicate_keys(State4)),

    %% Duplicate tuple key in list
    Kv5 = [apple, {cherry, pie}, {party, time}, ice_cream, {party, time}, cake],
    State5 = #state{kv = Kv5},
    State5a = no_duplicate_keys(State5),
    #state{errors = Errors5} = State5a,
    ?assertEqual([{"_", io_lib:format(
                          "Keys specified multiple times: ~s",
                          [["party"]])}], Errors5),

    %% Duplicate key in list
    Kv6 = [apple, {cherry, pie}, {party, time}, ice_cream, apple, cake],
    State6 = #state{kv = Kv6},
    State6a = no_duplicate_keys(State6),
    #state{errors = Errors6} = State6a,
    ?assertEqual([{"_", io_lib:format(
                          "Keys specified multiple times: ~s",
                          [["apple"]])}], Errors6),

    %% No duplicate key in list
    Kv7 = [apple, {cherry, pie}, {party, time}, ice_cream, chocolate, cake],
    State7 = #state{kv = Kv7},
    ?assertEqual(State7, no_duplicate_keys(State7)),

    %% Duplicate single key matches key in tuple
    Kv8 = [apple, {cherry, pie}, {party, time}, ice_cream, {apple, cake}],
    State8 = #state{kv = Kv8},
    State8a = no_duplicate_keys(State8),
    #state{errors = Errors8} = State8a,
    ?assertEqual([{"_", io_lib:format(
                          "Keys specified multiple times: ~s",
                          [["apple"]])}], Errors8),

    %% Duplicate key in tuple matches single key
    Kv9 = [{apple, pie}, {cherry, pie}, {party, time}, ice_cream, apple],
    State9 = #state{kv = Kv9},
    State9a = no_duplicate_keys(State9),
    #state{errors = Errors9} = State9a,
    ?assertEqual([{"_", io_lib:format(
                          "Keys specified multiple times: ~s",
                          [["apple"]])}], Errors9),

    ok.

check_for_duplicates_values_test() ->
    %% Duplicate key in tuple
    Kv1 = [{key1, value1}, {key2, value2}, {key3, value2}],
    State1 = #state{kv = Kv1},
    State1a = no_duplicate_values(State1),
    #state{errors = Errors1} = State1a,
    DuplicateKeys = ["key2", ", ", "key3"],
    ?assertEqual(
       [{"_", io_lib:format(
                "Keys specified have duplicate values: ~s",
                [DuplicateKeys])}], Errors1),

    %% No duplicates in tuples
    Kv2 = [{key1, value1}, {key2, value2}, {key3, value3}],
    State2 = #state{kv = Kv2},
    ?assertEqual(State2, no_duplicate_values(State2)),

    Kv3 = [{key1, value1}, {"key2", value2}, {key3, value2}],
    State3 = #state{kv = Kv3},
    State3a = no_duplicate_values(State3),
    #state{errors = Errors3} = State3a,
    DuplicateKeys = ["key2", ", ", "key3"],
    ?assertEqual(
       [{"_", io_lib:format(
                "Keys specified have duplicate values: ~s",
                [DuplicateKeys])}], Errors3),
    ok.

string_array_test() ->
    %% Test with valid input and default fun
    State1 = #state{kv = [{"names", [<<"Alice">>, <<"Bob">>, <<"Charlie">>]}]},
    ResultState1 = string_array(names, State1),
    ?assertEqual(["Alice", "Bob", "Charlie"], get_value(names, ResultState1)),

    %% Test with valid input and custom fun
    Fun = fun (Name) ->
                  case re:run(Name, "^[A-Z]") of
                      {match, _} -> {value, Name};
                      nomatch -> {error, "Name must start with a capital "
                                  "letter"}
                  end
          end,
    State2 = #state{kv = [{"names", [<<"Alice">>, <<"Bob">>, <<"Charlie">>]}]},
    ResultState2 = string_array(names, Fun, State2),
    ?assertEqual(["Alice", "Bob", "Charlie"], get_value(names, ResultState2)),

    %% Test with invalid input (name not starting with a capital letter)
    State3 = #state{kv = [{"names", [<<"alice">>, <<"Bob">>, <<"Charlie">>]}]},
    ResultState3 = string_array(names, Fun, State3),
    #state{errors = Errors3} = ResultState3,
    ?assertEqual([{"names", "alice - Name must start with a capital letter"}],
                 Errors3),

    %% Test with empty input and CanBeEmpty = false
    State4 = #state{kv = [{"names", []}]},
    ResultState4 = string_array(names, Fun, false, State4),
    #state{errors = Errors4} = ResultState4,
    ?assertEqual([{"names", "Must contain at least one element"}], Errors4),

    %% Test with empty input and CanBeEmpty = true
    State5 = #state{kv = [{"names", []}]},
    ResultState5 = string_array(names, Fun, true, State5),
    ?assertEqual([], get_value(names, ResultState5)),

    %% Test with invalid input (non-list)
    State6 = #state{kv = [{"names", <<"Not a list">>}]},
    ResultState6 = string_array(names, Fun, State6),
    #state{errors = Errors6} = ResultState6,
    ?assertEqual([{"names", "Must be an array of non-empty strings"}], Errors6),

    %% Test with invalid input (empty strings in list)
    State7 = #state{kv = [{"names", [<<"Alice">>, <<"">>, <<"Charlie">>]}]},
    ResultState7 = string_array(names, Fun, State7),
    #state{errors = Errors7} = ResultState7,
    ?assertEqual([{"names", "Must be an array of non-empty strings"}], Errors7).

token_list_test() ->
    %% Test with valid input and default fun
    State1 = #state{kv = [{"names", "Alice,Bob,Charlie"}]},
    ResultState1 = token_list(names, ",", State1),
    ?assertEqual(["Alice", "Bob", "Charlie"], get_value(names, ResultState1)),

    %% Test with valid input and custom fun
    Fun = fun (Name) ->
                  case re:run(Name, "^[a-z]") of
                      {match, _} -> {value, Name};
                      nomatch -> {error, "Name must start with a lower case "
                                  "letter"}
                  end
          end,
    State2 = #state{kv = [{"names", "alice,bob,charlie"}]},
    ResultState2 = token_list(names, ",", Fun, State2),
    ?assertEqual(["alice", "bob", "charlie"], get_value(names, ResultState2)),

    %% Test with invalid input (name not starting with a lower case letter)
    State4 = #state{kv = [{"names", "Alice,bob,charlie"}]},
    ResultState4 = token_list(names, ",", Fun, State4),
    #state{errors = Errors4} = ResultState4,
    ?assertEqual([{"names", "Alice - Name must start with a lower case "
                   "letter"}], Errors4),

    %% Test with empty input
    State5 = #state{kv = [{"names", ""}]},
    ResultState5 = token_list(names, ",", State5),
    ?assertEqual([], get_value(names, ResultState5)).

-define(assertResponse(ExpectedBody, ExpectedCode, BC),
        (fun () ->
                 {Body, Code} = BC,
                 ?assertEqual(ExpectedCode, Code),
                 ?assertEqual(ExpectedBody, Body)
         end)()).

handle_json_test_() ->
    Respond = fun (Body, Code) ->
                      erlang:put(json_test_response, {Body, Code})
              end,
    Handle = fun (Type, Data) ->
                     validator:handle(Respond(_, 200), Data, Type,
                                      [validator:string(key1, _),
                                       validator:string(key2, _),
                                       validator:unsupported(_)]),
                     erlang:get(json_test_response)
             end,
    GlobalError = ?cut({[{errors, {[{<<"_">>, list_to_binary(_)}]}}]}),
    GlobalErrorList = ?cut({[{errors, [{[{<<"_">>, list_to_binary(_)}]}]}]}),
    JsonObject = <<"{\"key1\": \"v1\", \"key2\": \"v2\"}">>,
    JsonList = <<"[{\"key1\": \"v1\", \"key2\": \"v2\"}]">>,
    Rubbish = <<"fdfgjkhlkjl">>,
    {foreach,
     fun () ->
             meck:new(mochiweb_request, [passthrough]),
             meck:expect(mochiweb_request, recv_body, fun (Req) -> Req end),
             meck:expect(mochiweb_request, parse_qs, fun (_Req) -> [] end),

             meck:new(menelaus_util, [passthrough]),
             meck:expect(menelaus_util, reply_json,
                         fun (_Req, Body, Code) ->
                                 Respond(Body, Code)
                         end),
             ok
     end,
     fun (_) ->
             meck:unload(mochiweb_request),
             meck:unload(menelaus_util),
             ok
     end,
     [{"json",
       fun () ->
               ?assertResponse([{key1, "v1"}, {key2, "v2"}], 200,
                               Handle(json, JsonObject)),
               ?assertResponse(GlobalError("Unexpected Json"), 400,
                               Handle(json, JsonList)),
               ?assertResponse(GlobalError("Invalid Json"), 400,
                               Handle(json, Rubbish))
       end},
      {"json_array",
       fun () ->
               ?assertResponse([[{key1, "v1"}, {key2, "v2"}]], 200,
                               Handle(json_array, JsonList)),
               ?assertResponse(
                  GlobalErrorList("A Json list must be specified."),
                  400, Handle(json_array, JsonObject)),
               ?assertResponse(GlobalErrorList("Invalid Json"), 400,
                               Handle(json_array, Rubbish))
       end},
      {"json_map",
       fun () ->
               Validators = [validator:string(key, _),
                             validator:string(prop, _),
                             validator:unsupported(_)],
               HandleMap =
                   fun (Data) ->
                           validator:handle(Respond(_, 200), Data,
                                            json_map, Validators),
                           erlang:get(json_test_response)
                   end,
               ?assertResponse([[{key, "key1"}, {prop, "v1"}],
                                [{key, "key2"}, {prop, "v2"}]], 200,
                               HandleMap(
                                 <<"{\"key1\": {\"prop\": \"v1\"}, "
                                   " \"key2\": {\"prop\": \"v2\"}}">>)),
               ?assertResponse(
                  GlobalErrorList("Must be a map K -> JsonObject."),
                  400, HandleMap(
                         <<"{\"key1\": \"val1\", "
                           " \"key2\": {\"prop\": \"v2\"}}">>)),
               ?assertResponse(GlobalErrorList("A Json map must be specified."),
                               400, HandleMap(JsonList)),
               ?assertResponse(GlobalErrorList("Invalid Json"), 400,
                               HandleMap(Rubbish))
       end}]}.

validate_field_path_test() ->
    %% Test valid field names
    State1 = #state{kv = [{"field", "simple"}]},
    ResultState1 = validate_field_path(field, State1),
    ?assertEqual("simple", get_value(field, ResultState1)),

    State2 = #state{kv = [{"field", "user.profile.groups"}]},
    ResultState2 = validate_field_path(field, State2),
    ?assertEqual("user.profile.groups", get_value(field, ResultState2)),

    State3 = #state{kv = [{"field", "resource.client.roles"}]},
    ResultState3 = validate_field_path(field, State3),
    ?assertEqual("resource.client.roles", get_value(field, ResultState3)),

    %% Test invalid field names
    State4 = #state{kv = [{"field", ".field"}]},
    ResultState4 = validate_field_path(field, State4),
    #state{errors = Errors4} = ResultState4,
    ?assertEqual([{"field", "Invalid field path format"}], Errors4),

    State5 = #state{kv = [{"field", "field."}]},
    ResultState5 = validate_field_path(field, State5),
    #state{errors = Errors5} = ResultState5,
    ?assertEqual([{"field", "Invalid field path format"}], Errors5),

    State6 = #state{kv = [{"field", "field..subfield"}]},
    ResultState6 = validate_field_path(field, State6),
    #state{errors = Errors6} = ResultState6,
    ?assertEqual([{"field", "Invalid field path format"}], Errors6),

    State7 = #state{kv = [{"field", ""}]},
    ResultState7 = validate_field_path(field, State7),
    #state{errors = Errors7} = ResultState7,
    ?assertEqual([{"field", "Invalid field path format"}], Errors7).

-endif.
