%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc helpers for validating REST API's parameters

-module(validator).

-include("cut.hrl").
-include("pipes.hrl").
-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle/4,
         touch/2,
         validate/3,
         validate_relative/4,
         json_array/3,
         get_value/2,
         convert/3,
         one_of/3,
         string/2,
         boolean/2,
         integer/2,
         integer/4,
         range/4,
         range/5,
         greater_or_equal/3,
         length/4,
         string/5,
         dir/2,
         email_address/2,
         time_duration/2,
         has_params/1,
         unsupported/1,
         required/2,
         prohibited/2,
         changeable_in_enterprise_only/3,
         string_array/2,
         return_value/3,
         return_error/3,
         default/3,
         token_list/3,
         add_input_type/2,
         is_json/1]).

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

handle(Fun, Req, form, Validators) ->
    handle(Fun, Req, add_input_type(form, mochiweb_request:parse_post(Req)),
           Validators);

handle(Fun, Req, qs, Validators) ->
    handle(Fun, Req, add_input_type(form, mochiweb_request:parse_qs(Req)),
           Validators);

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
    case validate_only(Req) of
        true ->
            report_errors_for_multiple(Req, Errors, 200);
        false ->
            case lists:flatten(Errors) of
                [] ->
                    Fun([prepare_params(S) || S <- States]);
                _ ->
                    report_errors_for_multiple(Req, Errors, 400)
            end
    end.

handle_one(Fun, Req, #state{errors = Errors} = State) ->
    case {validate_only(Req), Errors} of
        {true, _} ->
            report_errors_for_one(Req, Errors, 200);
        {false, []} ->
            Fun(prepare_params(State));
        {false, _} ->
            report_errors_for_one(Req, Errors, 400)
    end.

prepare_params(#state{kv = Props, touched = Touched}) ->
    lists:filtermap(fun ({{internal, _}, _}) ->
                            false;
                        ({K, V}) ->
                            case lists:member(K, Touched) of
                                true ->
                                    {true, {list_to_atom(K), V}};
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
      fun (JsonArray) ->
              States = [with_decoded_object(Elem, Validators) ||
                           Elem <- JsonArray],
              Errors = [ErrorList || #state{errors = ErrorList} <- States],
              case lists:flatten(Errors) of
                  [] ->
                      {value, [{prepare_params(St)} || St <- States]};
                  _ ->
                      {error, {json, [jsonify_errors(ErrorList) ||
                                         ErrorList <- Errors]}}
              end
      end, Name, State).

with_decoded_object({KVList}, Validators) ->
    Params = [{binary_to_list(Name), Value} || {Name, Value} <- KVList],
    functools:chain(#state{kv = add_input_type(json, Params)}, Validators);
with_decoded_object(_, _) ->
    #state{errors = [{<<"_">>, <<"Unexpected Json">>}]}.

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
            [#state{errors = [{<<"_">>, <<"Unexpected Json">>}]}]
    catch _:_ ->
            [#state{errors = [{<<"_">>, <<"Invalid Json">>}]}]
    end.

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
        Value ->
            case Fun(Value) of
                ok ->
                    State;
                {value, V} ->
                    return_value(Name, V, State);
                {error, Error} ->
                    return_error(Name, Error, State)
            end
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

string(Name, State) ->
    validate(
      case is_json(State) of
          true ->
              fun (Binary) when is_binary(Binary) ->
                      {value, binary_to_list(Binary)};
                  (_) ->
                      {error, "Value must be json string"}
              end;
          false ->
              fun (_) -> ok end
      end, Name, State).

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

range(Name, Min, Max, State) ->
    ErrorFun =
        ?cut(io_lib:format("The value must be in range from ~p to ~p",
                           [Min, Max])),
    range(Name, Min, Max, ErrorFun, State).

range(Name, Min, Max0, ErrorFun, State) ->
    Max = case Max0 of
              infinity ->
                  1 bsl 64 - 1;
              _ ->
                  Max0
          end,
    validate(
      fun (Value) ->
              case Value >= Min andalso Value =< Max of
                  true ->
                      ok;
                  false ->
                      {error, ErrorFun()}
              end
      end, Name, State).

greater_or_equal(Name1, Name2, State) ->
     validator:validate_relative(
       fun (Value1, Value2) when Value1 < Value2 ->
               {error,
                io_lib:format("should be greater or equal than ~p", [Name2])};
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

%% Validates a subset of the golang time.duration values
time_duration(Name, State) ->
    string(Name, "^[0-9]+(ns|us|ms|s|m|h)$", [dollar_endonly],
           "Must be in the form of number{ns|us|ms|s|m|h}",
           State).

has_params(#state{kv = []} = State) ->
    return_error("_", "Request should have parameters", State);
has_params(State) ->
    State.

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

prohibited(Name, #state{kv = Props} = State) ->
    case lists:keymember(name_to_list(Name), 1, Props) of
        false ->
            State;
        true ->
            return_error(Name, "The value must not be supplied", State)
    end.

%% Validate a parameter that may only be set to a non-default value in
%% enterprise edition.
changeable_in_enterprise_only(Name, Default, State) ->
    IsEnterprise = cluster_compat_mode:is_enterprise(),
    validate(
      fun (Value) ->
              case {IsEnterprise, Value =:= Default} of
                  {true, _} ->
                      {value, Value};
                  {false, true} ->
                      {value, Value};
                  {false, false} ->
                      {error, "Supported in enterprise edition only"}
              end
      end, Name, State).

string_array(Name, State) ->
    validate(
      fun (Array) when is_list(Array) ->
              case lists:all(?cut(is_binary(_1) andalso _1 =/= <<>>), Array) of
                  false ->
                      {error, "Must be an array of non-empty strings"};
                  true ->
                      {value, [binary_to_list(B) || B <- Array]}
              end;
          (_) ->
              {error, "Must be an array of non-empty strings"}
      end, Name, State).

default(Name, Default, State) ->
    case get_value(Name, touch(Name, State)) of
        undefined when is_function(Default) ->
            return_value(Name, Default(), State);
        undefined ->
            return_value(Name, Default, State);
        _Value ->
            State
    end.

token_list(Name, Separator, State) ->
    validate(
      fun (String) ->
          {value, string:lexemes(String, Separator)}
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
-endif.
