%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
%% @doc helpers for validating REST API's parameters

-module(validator).

-include("cut.hrl").
-include("pipes.hrl").
-include("ns_common.hrl").


-export([handle/4,
         touch/2,
         validate/3,
         validate_relative/4,
         get_value/2,
         convert/3,
         one_of/3,
         string/2,
         boolean/2,
         integer/2,
         integer/4,
         range/4,
         range/5,
         length/4,
         string/4,
         dir/2,
         has_params/1,
         unsupported/1,
         required/2,
         prohibited/2,
         string_array/2,
         return_value/3,
         return_error/3]).

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
    case lists:keyfind(403, 1, [E || {_, E} <- Errors]) of
        false ->
            false;
        {403, Permission} ->
            menelaus_util:reply_json(
              Req, menelaus_web_rbac:forbidden_response(Permission), 403),
            true
    end.

jsonify_errors(Errors) ->
    [{Name, iolist_to_binary(E)} || {Name, E} <- Errors].

report_errors_for_multiple(Req, Errors, Code) ->
    case process_fatal_errors(Req, lists:flatten(Errors)) of
        true ->
            ok;
        false ->
            send_error_json(Req, [{struct, jsonify_errors(E)} || E <- Errors],
                            Code)
    end.

report_errors_for_one(Req, Errors, Code) ->
    case process_fatal_errors(Req, Errors) of
        true ->
            ok;
        false ->
            send_error_json(Req, {struct, jsonify_errors(Errors)}, Code)
    end.

send_error_json(Req, Errors, Code) ->
    menelaus_util:reply_json(Req, {struct, [{errors, Errors}]}, Code).

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
              functools:id(_)
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

length(Name, Min, Max, State) ->
    validate(
      fun (Value) ->
              Length = size(unicode:characters_to_binary(
                              list_to_binary(Value), utf8, utf32)) div 4,
              case Length < Min orelse Length > Max of
                  true ->
                      {error,
                       io_lib:format("Length must be in range from ~p to ~p",
                                     [Min, Max])};
                  false ->
                      ok
              end
      end, Name, State).

string(Name, Regex, ErrorStr, State) ->
    validate(
      fun (Value) ->
              StringValue = (catch simple_term_to_list(Value)),
              case re:run(StringValue, Regex) of
                  {match, _} ->
                      ok;
                  nomatch ->
                      {error, ErrorStr}
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
