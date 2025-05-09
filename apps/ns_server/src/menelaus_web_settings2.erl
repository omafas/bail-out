%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

%% @doc generic handlers for settings management endpoints
%%
%% User module is supposed to define param specs. For example:
%%
%% Params = [{"path.to.myParam1", #{cfg_name => param_name1, type => int}},
%%           {"path.to.myParam2", #{cfg_name => param_name2, type => bool}},
%%           {"path.to.myParam3", #{cfg_name => param_name3, type => my_type}}]
%%
%% Cfg_name could be either atom or list of atoms. If list of atoms is used it
%% means the value is expected to be saved in nested proplist.
%% Param type should be either standard (defined in this module), or "custom" -
%% defined in user's module. For example:
%%
%% type_spec(my_type) -> #{validators => [Validator],
%%                         formatter => Formatter}
%%
%% (note that user defined types have precedence over standard types)
%%
%% After that handle_get and handle_post functions can be called to handle get
%% and set operations for described above params.
%%
%% When http GET is received, handle_get can be used to format provided values
%% as json and send http response. For example:
%% handle_get([], Params, fun type_spec/1, [{param_name1, 123}], Req)
%% will reply 200 with the following json body:
%% {"path": {"to": {"myParam1": 123}}}
%%
%% handle_get(["path"], Params, fun type_spec/1, [{param_name1, 123}], Req)
%% will reply 200 with the following json body:
%% {"to": {"myParam1": 123}}
%%
%% handle_get(["path", "to"], Params, fun type_spec/1, [{param_name1, 123}],
%%            Req)
%% will reply 200 with the following json body:
%% {"myParam1": 123}
%%
%% handle_get(["path", "to", "myParam1"], Params, fun type_spec/1,
%%            [{param_name1, 123}], Req)
%% will reply 200 with the following json body: 123
%%
%% When POST /path is received, handle_post can be used to parse post body. It
%% calls user provided fun and passes parsed params to it. ApplyFun is supposed
%% to save the received params. For example:
%%
%% POST / with body: {"path": {"to": {"myParam2": true}}}
%% handle_post(ApplyFun, [], Params, fun type_spec/1, Req)
%%
%% POST /path with body: {"to": {"myParam2": true}}
%% handle_post(ApplyFun, ["path"], Params, fun type_spec/1, Req)
%%
%% In all the examples above handle_post will call ApplyFun with
%% [{[param_name1], true}]

-module(menelaus_web_settings2).

-export([prepare_json/4, handle_get/5, handle_post/5, handle_post/7]).

-include("ns_common.hrl").
-include_lib("ns_common/include/cut.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([param_spec/0, type_spec/0]).

-type param_spec() :: {JSONName :: string(), #{cfg_name => [atom()] | atom(),
                                               type := type_spec_name()}}.

-type type_spec_name() :: term().
-type validator() :: fun ((_Name, State) -> State) | type_spec_name().
-type formatter() :: fun ((_Term) -> _JSONTerm) | type_spec_name().
-type type_spec() :: #{validators => [validator()],
                       formatter => formatter()}.

-spec type_spec(Name :: type_spec_name()) -> type_spec().
type_spec(bool) ->
    #{validators => [fun validator:boolean/2], formatter => existing_atom};
type_spec(pos_int) ->
    #{validators => [int, greater_than(_, 0, _)], formatter => int};
type_spec(non_neg_int) ->
    #{validators => [int, greater_than(_, -1, _)], formatter => int};
type_spec(int) ->
    #{validators => [fun validator:integer/2]};
type_spec({int, Min, Max}) ->
    #{validators => [?cut(validator:integer(_1, Min, Max, _2))],
      formatter => int};
type_spec({num, Min, Max}) ->
    #{validators => [?cut(validator:number(_1, Min, Max, _2))]};
type_spec(existing_atom) ->
    #{validators => [fun existing_atom/2]};
type_spec(string) ->
    #{validators => [fun validator:string/2],
      formatter => fun (V) -> {value, list_to_binary(V)} end};
type_spec({one_of, Type, List}) ->
    #{validators => [validator:one_of(_, List, _), Type], formatter => Type};
type_spec(password) ->
    #{validators => [string, ?cut(validator:convert(_1, fun (P) ->
                                                            {password, P}
                                                        end, _2))],
      formatter => fun ({password, ""}) -> {value, <<>>};
                       ({password, _}) -> {value, <<"**********">>}
                   end};
type_spec(not_supported) ->
    #{validators => [fun not_supported/2],
      formatter => fun (_) -> ignore end};
type_spec({url, SupportedSchemes}) ->
    #{validators => [string, validator:url(_, SupportedSchemes, _)],
      formatter => string};
type_spec(certificate) ->
    #{validators => [string, fun validate_cert/2],
      formatter => fun (undefined) -> ignore;
                       (<<"redacted">>) -> {value, <<"redacted">>};
                       ({Cert, _Decoded}) -> {value, Cert}
                   end};
type_spec(certificate_chain) ->
    #{validators => [string, fun validate_cert_chain/2],
      formatter => fun (<<"redacted">>) -> {value, <<"redacted">>};
                       ({Cert, _DecodedCerts}) -> {value, Cert}
                   end};
type_spec(pkey) ->
    #{validators => [string, fun validate_key/2],
      formatter => fun (undefined) -> ignore;
                       ({password, {_, _, not_encrypted}}) ->
                           {value, <<"**********">>}
                   end};
type_spec(tls_opts) ->
    #{validators => [not_supported],
      formatter => fun (undefined) -> ignore;
                       (List) ->
                           Sanitize = fun ({password, _}) -> <<"********">>;
                                          (V) -> V
                                      end,
                           Sanitized = [{K, Sanitize(V)} || {K, V} <- List],
                           Str = io_lib:format("~p", [Sanitized]),
                           {value, iolist_to_binary(Str)}
                   end};
type_spec({string_list, Separator}) ->
    #{validators => [validate_string_list(Separator, _, _)],
      formatter => fun (L) -> {value, [list_to_binary(M) || M <- L]} end};
type_spec(empty_str) ->
    #{validators => [string, validate_empty_string(_, _)],
      formatter => string};
type_spec(datetime_iso8601) ->
    #{validators => [validator:iso_8601_parsed(_, _)],
      formatter => fun (DT) -> {value, misc:utc_to_iso8601(DT, local)} end};
type_spec({optional, Type}) ->
    OrType = {'or', [{not_set, empty_str}, {set, Type}]},
    #{validators => [OrType], formatter => OrType};
type_spec({read_only, Type}) ->
    #{validators => [not_supported],
      formatter => Type}.

validate_empty_string(Name, State) ->
    validator:validate(
      fun ("") -> ok;
          (_) -> {error, "must be empty"}
      end, Name, State).

validate_or([{TypeName, Validators} | Tail], Name, State) ->
    NewState = lists:foldl(fun (V, S) -> V(Name, S) end, State, Validators),
    case validator:get_value(Name, NewState) of
        undefined when Tail == [] ->
            NewState;
        undefined ->
            validate_or(Tail, Name, State);
        _ ->
            validator:validate(fun (V) -> {value, {TypeName, V}} end,
                               Name, NewState)
    end.

validate_string_list(Separator, Name, State) ->
    case validator:is_json(State) of
          true -> validator:string_array(Name, State);
          false -> validator:token_list(Name, Separator, State)
    end.

not_supported(Name, State) ->
    validator:validate(
      fun (_) -> {error, "modification not supported"} end,
      Name, State).

prepare_json(Path, ParamSpecs, UserTypesFun, Values) ->
    TypesFuns = [UserTypesFun || UserTypesFun /= undefined] ++
                [fun type_spec/1],
    Res = lists:filtermap(
            fun ({Name, #{type := Type} = Spec}) ->
                NameTokens = split_key(Name),
                case extract_key(Path, NameTokens) of
                    {ok, Keys} ->
                        FormattedKey = [list_to_binary(K) || K <- Keys],
                        InternalName = cfg_key(Name, Spec),
                        Formatter = extract_formatter(Type, TypesFuns),
                        case extract_value(InternalName, Values) of
                            not_found ->
                                case maps:find(default, Spec) of
                                    {ok, Default} ->
                                        {true, {FormattedKey, Default}};
                                    error ->
                                        false
                                end;
                            {value, Value} ->
                                case Formatter(Value) of
                                    {value, FormattedValue} ->
                                        {true, {FormattedKey, FormattedValue}};
                                    ignore ->
                                        false
                                end
                        end;
                    error -> false
                end
            end, ParamSpecs),
    group_elements(Res, ?cut({_})).

handle_get(Path, ParamSpecs, UserTypesFun, Values, Req) ->
    menelaus_util:reply_json(
      Req, prepare_json(Path, ParamSpecs, UserTypesFun, Values)).

handle_post(ApplyFun, Path, ParamSpecs, UserTypesFun, Req) ->
    handle_post(ApplyFun, Path, ParamSpecs, UserTypesFun, [], [], Req).

%% Note about Predefined and Defaults:
%% If some setting is mandatory, it must be present in Req or in Predefined,
%% Usually "Predefined" are the setting that were set by this endpoint
%% before.
%% Defaults are only used when values are passed to the "is mandatory" fun.
handle_post(ApplyFun, Path, ParamSpecs, UserTypesFun, Predefined, Defaults,
            Req) ->
    TypesFuns = [UserTypesFun || UserTypesFun /= undefined] ++
                [fun type_spec/1],
    ValidatorsProplist =
        lists:filtermap(
              fun ({Name, #{type := Type}}) ->
                  NameTokens = split_key(Name),
                  case extract_key(Path, NameTokens) of
                      {ok, Keys} ->
                          {true, {Keys, extract_validators(Type, TypesFuns)}};
                      error -> false
                  end
              end, ParamSpecs),

    AllPossibleKeys = [Keys || {Keys, _} <- ValidatorsProplist],
    Validators = [fun (S) -> F(join_key(Keys), S) end
                    || {Keys, Funs} <- ValidatorsProplist, F <- Funs],
    Params = parse_params(AllPossibleKeys, Req),

    %% Final touches before returning the props to the caller
    PrepareProps =
        fun (Props) ->
              lists:map(
                fun ({Key, Value}) ->
                    FullKey = join_key(Path ++ split_key(atom_to_list(Key))),
                    Spec = proplists:get_value(FullKey, ParamSpecs),
                    InternalKey = cfg_key(FullKey, Spec),
                    {InternalKey, Value}
                end, Props)
        end,

    %% Is is called "Post" because it is called after all values are parsed
    %% by Validators
    PostValidator =
        validator:post_validate_all(
          fun (Props, State) ->
              Props2 = PrepareProps(Props),
              PostValidators = post_validators(Path, Props2, ParamSpecs,
                                               Predefined, Defaults),
              {ok, functools:chain(State, PostValidators)}
          end, _),

    validator:handle(
      fun (Props) -> ApplyFun(PrepareProps(Props), Req) end, Req, Params,
      Validators ++ [validator:unsupported(_), PostValidator]).

post_validators(Path, Props, ParamSpecs, Predefined, Defaults) ->
    AllPropsIncludingPredefined =
        lists:foldl(
          fun ({Name, #{} = Spec}, Acc) ->
              InternalKey = cfg_key(Name, Spec),
              case proplists:get_all_values(InternalKey, Props) of
                  [] ->
                      case extract_value(InternalKey, Predefined) of
                          not_found ->
                              case extract_value(InternalKey, Defaults) of
                                  not_found ->
                                      Acc;
                                  {value, V} ->
                                      Acc#{cfg_key_as_is(Name, Spec) => V}
                              end;
                          {value, V} ->
                              Acc#{cfg_key_as_is(Name, Spec) => V}
                      end;
                  [V] ->
                      Acc#{cfg_key_as_is(Name, Spec) => V}
              end
          end, maps:from_list(Props), ParamSpecs),
    GetParamName =
        fun (N) -> join_key(extract_relative_key(Path, split_key(N))) end,
    mandatory_validators(ParamSpecs, Predefined, AllPropsIncludingPredefined,
                         GetParamName) ++
    depends_on_validators(Props, ParamSpecs, AllPropsIncludingPredefined,
                          GetParamName).

%% Handles depends_on field in specs
%% Format: {Name0, #{depends_on => #{Name1 => Fun1, Name2 => Fun2}}}
%% which means that Name0 depends on Name1 and Name2
%% Fun1 and Fun2 are functions that take two arguments. The first argument is
%% the value of Name0, the second argument is the value of Name1 or Name2
%% depending on which one is being checked. The dependency is only checked if
%% Name0 is being set by this POST request.
depends_on_validators(Props, ParamSpecs, AllPropsIncludingPredefined,
                      GetParamName) ->
    lists:flatmap(
      fun ({Name1, #{depends_on := DependsOnMap} = Spec}) ->
              ParamName1 = GetParamName(Name1),
              InternalKey1 = cfg_key(Name1, Spec),
              Value1 = maps:get(InternalKey1, AllPropsIncludingPredefined,
                                undefined),
              case proplists:is_defined(InternalKey1, Props) of
                  true -> %% only call the validator function if Name1 is
                          %% being set by this POST request
                      lists:flatmap(
                          fun ({Name2, DependsOn}) ->
                              Spec2 = proplists:get_value(Name2, ParamSpecs),
                              InternalKey2 = cfg_key(Name2, Spec2),
                              Value2 = maps:get(InternalKey2,
                                                AllPropsIncludingPredefined,
                                                undefined),
                              case DependsOn(Value1, Value2) of
                                  ok -> [];
                                  {error, Reason} ->
                                      [validator:validate(
                                         fun (_) -> {error, Reason} end,
                                         ParamName1, _)]
                              end
                          end, maps:to_list(DependsOnMap));
                  false ->
                      []
              end;
          ({_Name, _Spec}) ->
              []
      end, ParamSpecs).

%% Handles mandatory field in specs
%% Format: {Name, #{mandatory => true}}
%% or {Name, #{mandatory => Fun}}
%% where Fun is a function that takes a map of all properties and returns true
%% if the property is mandatory, otherwise false
mandatory_validators(ParamSpecs, Predefined, AllPropsIncludingPredefined,
                     GetParamName) ->
    lists:filtermap(
      fun ({Name, #{mandatory := F} = Spec}) ->
              case (F == true) orelse
                   (is_function(F) andalso F(AllPropsIncludingPredefined)) of
                  true ->
                      InternalKey = cfg_key(Name, Spec),
                      case extract_value(InternalKey, Predefined) of
                          not_found ->
                              {true, validator:required(GetParamName(Name), _)};
                          {value, _} ->
                              false
                      end;
                  false ->
                      false
              end;
          ({_Name, _Spec}) ->
              false
      end, ParamSpecs).

parse_params(AllPossibleKeys, Req) ->
    Type =
        case mochiweb_request:get_primary_header_value("content-type", Req) of
            "application/json" -> json;
            "application/x-www-form-urlencoded" -> form;
            "text/plain" -> text;
            undefined -> text;
            CT ->
                S = io_lib:format("Unsupported content-type: ~p", [CT]),
                menelaus_util:web_exception(400,  S)
        end,

    Params =
        case Type of
            text -> [{[],  mochiweb_request:recv_body(Req)}];
            form when AllPossibleKeys == [""] -> %% Full key is passed in path
                                                 %% and body contains only value
                Body = mochiweb_request:recv_body(Req),
                [{[], mochiweb_util:unquote(Body)}];
            form ->
                lists:map(fun ({Key, Val}) ->
                              KeyTokens = string:split(Key, "/", all),
                              {KeyTokens, Val}
                          end, mochiweb_request:parse_post(Req));
            json ->
                Body = case mochiweb_request:recv_body(Req) of
                           <<>> -> <<"{}">>; %% treat empty body as an empty
                                             %% object due to backward compat
                                             %% reasons
                           B -> B
                       end,
                JSON = try
                           ejson:decode(Body)
                       catch
                           _:_ ->
                               menelaus_util:web_exception(400, "Invalid JSON")
                       end,
                flatten_json(AllPossibleKeys, JSON, [])
        end,
    validator:add_input_type(Type, [{join_key(KL), V} || {KL, V} <- Params]).

flatten_json([], Value, Path) -> [{Path, Value}];
flatten_json([[]], Value, Path) -> [{Path, Value}];
flatten_json(AllPossibleKeys, {JSON}, Path) when is_list(JSON) ->
    lists:flatmap(
      fun ({KeyBin, Value}) ->
          KeyStr = binary_to_list(KeyBin),
          NewPossibleKeys = lists:filtermap(
                              fun ([K | Tail]) when K == KeyStr -> {true, Tail};
                                  (_) -> false
                              end, AllPossibleKeys),
          flatten_json(NewPossibleKeys, Value, Path ++ [KeyStr])
      end, JSON);
flatten_json(_AllPossibleKeys, _JSON, _Path) ->
    menelaus_util:web_exception(400, "JSON object is expected").

group_elements([{[], Value}], _) -> Value;
group_elements(Proplist, GroupFormatter) ->
    Grouped = maps:to_list(maps:groups_from_list(
                             fun ({[Head | _], _}) -> Head end,
                             fun ({[_ | Tail], Value}) -> {Tail, Value} end,
                             Proplist)),
    GroupFormatter(lists:map(
                     fun ({Key, Values}) ->
                         {Key, group_elements(Values, GroupFormatter)}
                     end, Grouped)).

extract_formatter(Type, TypesFuns) ->
    extract_formatter(Type, TypesFuns, TypesFuns, []).
extract_formatter({'or', List}, _TypesFuns, AllTypesFuns, TypesSeen) ->
    fun ({ValueType, Value}) ->
        Type = proplists:get_value(ValueType, List),
        Fun = extract_formatter(Type, AllTypesFuns, AllTypesFuns, TypesSeen),
        Fun(Value)
    end;
extract_formatter(Type, [], _, _) -> error({unknown_type, Type});
extract_formatter(Type, [F | Tail], AllTypesFuns, TypesSeen) ->
    case lists:member(Type, TypesSeen) of
        true -> error({circular_dependency, Type});
        false -> ok
    end,
    try F(Type) of
        #{formatter := Formatter} when is_function(Formatter) -> Formatter;
        #{formatter := SubType} ->
            extract_formatter(SubType, AllTypesFuns, AllTypesFuns,
                              [Type|TypesSeen]);
        #{} -> fun (V) -> {value, V} end
    catch
        error:function_clause -> extract_formatter(Type, Tail, AllTypesFuns,
                                                   TypesSeen)
    end.

extract_validators(Type, TypesFuns) ->
    extract_validators(Type, TypesFuns, []).
extract_validators({'or', List}, TypesFuns, TypesSeen) ->
    ListOfValidators =
      lists:map(
        fun ({Name, SubType}) ->
            {Name, extract_validators(SubType, TypesFuns, TypesSeen)}
        end, List),
    [validate_or(ListOfValidators, _, _)];
extract_validators(Type, [], _) -> error({unknown_type, Type});
extract_validators(Type, [F | Tail], TypesSeen) ->
    case lists:member(Type, TypesSeen) of
        true -> error({circular_dependency, Type});
        false -> ok
    end,
    try F(Type) of
        #{validators := Validators} ->
            lists:flatmap(
              fun (Fun) when is_function(Fun) -> [Fun];
                  (SubType) ->
                      extract_validators(SubType, [F | Tail], [Type | TypesSeen])
              end, Validators);
        #{} -> []
    catch
        error:function_clause -> extract_validators(Type, Tail, TypesSeen)
    end.

extract_value([], Value) -> {value, Value};
extract_value([Key | Tail], PropList) ->
    case proplists:get_all_values(Key, PropList) of
        [] -> not_found;
        [Value] -> extract_value(Tail, Value)
    end.

extract_key([], Tokens) -> {ok, Tokens};
extract_key(_Path, []) -> error;
extract_key([El | Path], [El | Tokens]) -> extract_key(Path, Tokens);
extract_key(_, _) -> error.

extract_relative_key([], Tokens) -> Tokens;
extract_relative_key([El | Path], [El | Tokens]) ->
    extract_relative_key(Path, Tokens);
extract_relative_key(Path, Tokens) ->
    ["[upper-level]" || _ <- Path] ++ Tokens.

existing_atom(Name, State) ->
    validator:convert(
      Name,
      fun (Bin) when is_binary(Bin) ->
              erlang:binary_to_existing_atom(Bin, latin1);
          (Str) when is_list(Str) ->
              erlang:list_to_existing_atom(Str);
          (A) when is_atom(A) ->
              A
      end, State).

greater_than(Name, N, State) ->
    validator:validate(
      fun (V) when V > N -> {value, V};
          (_) -> {error, io_lib:format("Value must be greater than ~p", [N])}
      end, Name, State).

split_key("") -> [];
split_key(K) -> string:split(K, ".", all).

join_key(K) -> lists:flatten(lists:join(".", K)).

cfg_key(Name, Spec) ->
    case cfg_key_as_is(Name, Spec) of
        List when is_list(List) -> List;
        Key -> [Key]
    end.

cfg_key_as_is(Name, Spec) ->
    maps:get(cfg_key, Spec, list_to_atom(Name)).

validate_key(Name, State) ->
    validator:validate(
      fun ("") -> {value, undefined};
          (Key) ->
              case ns_server_cert:validate_pkey(iolist_to_binary(Key),
                                                fun () -> undefined end) of
                  {ok, DecodedKey} -> {value, {password, DecodedKey}};
                  {error, _} -> {error, "invalid key"}
              end
      end, Name, State).

validate_cert(Name, State) ->
    validator:validate(
      fun ("") -> {value, undefined};
          (Cert) ->
              BinCert = iolist_to_binary(Cert),
              case ns_server_cert:decode_single_certificate(BinCert) of
                  {ok, Decoded} -> {value, {BinCert, Decoded}};
                  {error, _} -> {error, "invalid certificate"}
              end
      end, Name, State).

validate_cert_chain(Name, State) ->
    validator:validate(
      fun (Cert) ->
              BinCert = iolist_to_binary(Cert),
              case ns_server_cert:decode_cert_chain(BinCert) of
                  {ok, Decoded} -> {value, {BinCert, Decoded}};
                  {error, _} -> {error, "invalid certificate"}
              end
      end, Name, State).

-ifdef(TEST).

or_type_test() ->
    Type = {'or', [{type1, bool},
                   {type2, int},
                   {type3, {one_of, existing_atom, [a1, a2]}}]},
    Params = [{"t1", #{cfg_key => k1, type => Type}},
              {"t2", #{cfg_key => k2, type => Type}},
              {"t3", #{cfg_key => k3, type => Type}}],
    TypeSpec = fun (undefined) -> ok end,
    Format = fun (V) -> ejson:encode(prepare_json([], Params, TypeSpec, V)) end,
    ?assertEqual(<<"{\"t1\":true,\"t2\":5,\"t3\":\"a2\"}">>,
                 Format([{k1, {type1, true}},
                         {k2, {type2, 5}},
                         {k3, {type3, a2}}])),

    ExpectedResult = [{[k1], {type2, 10}},
                      {[k2], {type3, a1}},
                      {[k3], {type1, false}}],

    with_request(
      fun (SetContType, SetBody, Req) ->
          SetContType("application/x-www-form-urlencoded"),
          SetBody(<<"t1=10&t2=a1&t3=false">>),
          handle_post(fun (Parsed, Req2) when Req == Req2 ->
                          ?assertEqual(ExpectedResult, Parsed)
                      end, [], Params, TypeSpec, Req)
      end).

one_of_test() ->
    Type = {one_of, existing_atom, [false, a1]},
    Params = [{"t1", #{cfg_key => k1, type => Type}},
              {"t2", #{cfg_key => k2, type => Type}},
              {"t3", #{cfg_key => k3, type => Type}}],
    TypeSpec = fun (undefined) -> ok end,
    Format = fun (V) -> ejson:encode(prepare_json([], Params, TypeSpec, V)) end,

    ?assertEqual(<<"{\"t1\":false,\"t2\":\"a1\"}">>,
                 Format([{k1, false},
                         {k2, a1}])),

    with_request(
      fun (SetContType, SetBody, Req) ->
          SetContType("application/x-www-form-urlencoded"),
          SetBody(<<"t1=false&t2=a1">>),
          handle_post(fun (Parsed, Req2) when Req == Req2 ->
                          ?assertEqual([{[k1], false}, {[k2], a1}], Parsed)
                      end, [], Params, TypeSpec, Req)
      end),

    with_request(
      fun (SetContType, SetBody, Req) ->
          SetContType("application/json"),
          SetBody(<<"{\"t1\": false, \"t2\": \"false\", \"t3\": \"a1\"}">>),
          handle_post(fun (Parsed, Req2) when Req == Req2 ->
                          ?assertEqual([{[k1], false}, {[k2], false},
                                        {[k3], a1}], Parsed)
                      end, [], Params, TypeSpec, Req)
      end).

test_params() ->
    [{"key1.key2-1.key3-1", #{cfg_key => ckey1, type => int}},
     {"key1.key2-2.key3-1", #{cfg_key => [ckey2_1, ckey2_2], type => int}},
     {"key1.key2-2.key3-2", #{cfg_key => [ckey3], type => custom}}].

test_type_spec(custom) ->
    #{validators => [int, validator:validate(
                            fun (Int) ->
                                ?assertEqual(Int, 42),
                                {value, 42}
                            end, _, _)],
      formatter => fun (Int) -> ?assertEqual(Int, 42), {value, 42} end}.

prepare_json_test() ->
    Prepare =
        fun (Path, Values) ->
            J = prepare_json(Path, test_params(), fun test_type_spec/1, Values),
            binary_to_list(ejson:encode(J))
        end,
    ?assertEqual("{}", Prepare([], [])),
    ?assertEqual("{\"key1\":{\"key2-1\":{\"key3-1\":1},"
                            "\"key2-2\":{\"key3-1\":2,\"key3-2\":42}}}",
                 Prepare([],
                         [{ckey1, 1},{ckey2_1, [{ckey2_2, 2}]}, {ckey3, 42}])),
    ?assertEqual("{\"key2-1\":{\"key3-1\":1},"
                  "\"key2-2\":{\"key3-1\":2,\"key3-2\":42}}",
                 Prepare(["key1"],
                         [{ckey1, 1},{ckey2_1, [{ckey2_2, 2}]}, {ckey3, 42}])),
    ?assertEqual("{\"key3-1\":2,\"key3-2\":42}",
                 Prepare(["key1", "key2-2"],
                         [{ckey1, 1},{ckey2_1, [{ckey2_2, 2}]}, {ckey3, 42}])),
    ?assertEqual("42",
                 Prepare(["key1", "key2-2", "key3-2"],
                         [{ckey1, 1},{ckey2_1, [{ckey2_2, 2}]}, {ckey3, 42}])).

with_request(Fun) ->
    meck:new(mochiweb_request, [passthrough]),
    meck:new(menelaus_util, [passthrough]),
    Ets = ets:new(test_ets, [set, public]),
    try
        Req = make_ref(),
        meck:expect(mochiweb_request, recv_body,
                    fun (R) when R == Req ->
                        [{_, Body}] = ets:lookup(Ets, body),
                        Body
                    end),
        meck:expect(mochiweb_request, parse_post,
                    fun (R) when R == Req ->
                        [{_, ContType}] = ets:lookup(Ets, content_type),
                        case ContType == "application/x-www-form-urlencoded" of
                            true ->
                                [{_, Body}] = ets:lookup(Ets, body),
                                mochiweb_util:parse_qs(Body);
                            false -> []
                        end
                    end),
        meck:expect(mochiweb_request, get_primary_header_value,
                    fun ("content-type", R) when R == Req ->
                        [{_, ContType}] = ets:lookup(Ets, content_type),
                        ContType
                    end),
        meck:expect(mochiweb_request, parse_qs,
                    fun (R) when R == Req -> [] end),
        meck:expect(mochiweb_request, get_meta,
                    fun (resp_headers, _, R) when R == Req -> [] end),
        meck:expect(menelaus_util, reply_json,
                    fun (R, {[{errors, Errors}]}, 400) when R == Req ->
                            error({validation_failed, Errors});
                        (R1, R2, R3) ->
                            erlang:error({received_reply_json, {R1, R2, R3}})
                    end),
        Fun(fun (ContType) -> ets:insert(Ets, {content_type, ContType}) end,
            fun (Body) -> ets:insert(Ets, {body, Body}) end, Req)
    after
        ets:delete(Ets),
        meck:unload(menelaus_util),
        meck:unload(mochiweb_request)
    end.

handle_post_test() ->
    with_request(
      fun (SetContType, SetBody, Req) ->
          HandlePost =
              fun (Path, ContType, Body, ExpectedResult) ->
                  SetContType(ContType),
                  SetBody(Body),
                  handle_post(fun (Parsed, Req2) when Req == Req2 ->
                                  ?assertEqual(ExpectedResult, Parsed)
                              end, Path, test_params(),
                              fun test_type_spec/1, Req)
              end,
          HandlePost([],
                     "application/x-www-form-urlencoded",
                     <<"key1.key2-1.key3-1=1&key1.key2-2.key3-1=2&"
                       "key1.key2-2.key3-2=42">>,
                     [{[ckey1], 1}, {[ckey2_1, ckey2_2], 2}, {[ckey3], 42}]),
          HandlePost(["key1"],
                     "application/x-www-form-urlencoded",
                     <<"key2-1.key3-1=1&key2-2.key3-1=2&key2-2.key3-2=42">>,
                     [{[ckey1], 1}, {[ckey2_1, ckey2_2], 2}, {[ckey3], 42}]),
          HandlePost(["key1", "key2-2"],
                     "application/x-www-form-urlencoded",
                     <<"key3-1=2&key3-2=42">>,
                     [{[ckey2_1, ckey2_2], 2}, {[ckey3], 42}]),
          HandlePost(["key1", "key2-2", "key3-2"],
                     "application/x-www-form-urlencoded",
                     <<"42">>,
                     [{[ckey3], 42}]),

          HandlePost([],
                     "application/json",
                     <<"{\"key1\": {\"key2-1\": {\"key3-1\": 1}, "
                                   "\"key2-2\": {\"key3-1\": 2, "
                                                "\"key3-2\": 42}}}">>,
                     [{[ckey1], 1}, {[ckey2_1, ckey2_2], 2}, {[ckey3], 42}]),
          HandlePost(["key1"],
                     "application/json",
                     <<"{\"key2-1\": {\"key3-1\": 1}, "
                        "\"key2-2\": {\"key3-1\": 2, \"key3-2\": 42}}">>,
                     [{[ckey1], 1}, {[ckey2_1, ckey2_2], 2}, {[ckey3], 42}]),
          HandlePost(["key1", "key2-2"],
                     "application/json",
                     <<"{\"key3-1\": 2, \"key3-2\": 42}">>,
                     [{[ckey2_1, ckey2_2], 2}, {[ckey3], 42}]),
          HandlePost(["key1", "key2-2", "key3-2"],
                     "application/json",
                     <<"42">>,
                     [{[ckey3], 42}])
      end).

depends_on_test() ->
    GreaterThan = fun (undefined, _) -> {error, "undefined"};
                      (_, undefined) -> {error, "undefined"};
                      (V1, V2) when V1 > V2 -> ok;
                      (_, _) -> {error, "test error"}
                  end,
    LessThan = fun (undefined, _) -> {error, "undefined"};
                   (_, undefined) -> {error, "undefined"};
                   (V1, V2) when V1 < V2 -> ok;
                   (_, _) -> {error, "test error"}
               end,
    Params = [{"K1.K2.SMALL", #{cfg_key => [s1, small], type => int}},
              {"BIGGER", #{cfg_key => [super_big], type => int}},
              {"K1.BIG", #{cfg_key => [b1, b2, big], type => int,
                          %% K1.BIG must be greater than K1.K2.SMALL
                          depends_on => #{"K1.K2.SMALL" => GreaterThan,
                                          "BIGGER" => LessThan}}},
              {"K2.UNRELATED1", #{cfg_key => unrelated1, type => int}},
              {"K2.UNRELATED2", #{cfg_key => unrelated2, type => int}}],

    with_request(
      fun (SetContType, SetBody, Req) ->
          SetContType("application/x-www-form-urlencoded"),
          Succ =
              fun (#{path := Path, body := Body,
                     expected_result := ExpectedResult} = P) ->
                  SetBody(Body),
                  Predefined = maps:get(predefined, P, []),
                  Defaults = maps:get(defaults, P, []),
                  handle_post(fun (Parsed, Req2) when Req == Req2 ->
                                  ?assertEqual(ExpectedResult, Parsed)
                              end, Path, Params,
                              fun test_type_spec/1, Predefined, Defaults, Req)
              end,
          Fail =
              fun (#{path := Path, body := Body,
                     expected_error := ExpectedError} = P) ->
                  SetBody(Body),
                  Predefined = maps:get(predefined, P, []),
                  Defaults = maps:get(defaults, P, []),
                  ?assertError(
                    {validation_failed, ExpectedError},
                    handle_post(fun (_Parsed, Req2) when Req == Req2 ->
                                    ?assert(false)
                                end, Path, Params,
                                fun test_type_spec/1,
                                Predefined, Defaults, Req))
              end,

          Succ(#{path => [],
                 body => <<"K2.UNRELATED1=1&K2.UNRELATED2=2">>,
                 expected_result => [{[unrelated1], 1}, {[unrelated2], 2}]}),
          Succ(#{path => [],
                 body => <<"K2.UNRELATED1=1&K2.UNRELATED2=2">>,
                 %% If we already have inconsistent values set, we should
                 %% not return error, as we are not validating these values
                 predefined => [{b1, [{b2, [{big, 1}]}]}, {s1, [{small, 2}]}],
                 expected_result => [{[unrelated1], 1}, {[unrelated2], 2}]}),
          Succ(#{path => [],
                 body => <<"K2.UNRELATED1=1&K2.UNRELATED2=2">>,
                 %% If we already have inconsistent values set, we should
                 %% not return error, as we are not validating these values
                 predefined => [{b1, [{b2, [{big, 1}]}]}],
                 defaults => [{s1, [{small, 2}]}, {super_big, 0}],
                 expected_result => [{[unrelated1], 1}, {[unrelated2], 2}]}),
          Succ(#{path => ["K2"],
                 body => <<"UNRELATED1=1&UNRELATED2=2">>,
                 expected_result => [{[unrelated1], 1}, {[unrelated2], 2}]}),
          Succ(#{path => [],
                 body => <<"BIGGER=3&K1.BIG=2&K1.K2.SMALL=1&K2.UNRELATED1=3">>,
                 expected_result => [{[super_big], 3}, {[b1, b2, big], 2},
                                     {[s1, small], 1}, {[unrelated1], 3}]}),
          Succ(#{path => ["K1"],
                 body => <<"BIG=2&K2.SMALL=1">>,
                 defaults => [{super_big, 5}],
                 expected_result => [{[b1, b2, big], 2}, {[s1, small], 1}]}),
          Succ(#{path => ["K1"],
                 body => <<"K2.SMALL=3">>,
                 predefined => [{b1, [{b2, [{big, 5}]}]}],
                 defaults => [{s1, [{small, 6}]}, {super_big, 4}],
                 expected_result => [{[s1, small], 3}]}),
          Succ(#{path => ["K1"],
                 body => <<"BIG=3">>,
                 predefined => [{b1, [{b2, [{big, 1}]}]}],
                 defaults => [{s1, [{small, 2}]}, {super_big, 4}],
                 expected_result => [{[b1, b2, big], 3}]}),
          Fail(#{path => [],
                 body => <<"K1.BIG=1&K1.K2.SMALL=2&BIGGER=3&K2.UNRELATED1=3">>,
                 expected_error => {[{"K1.BIG", <<"test error">>}]}}),
          Fail(#{path => [],
                 body => <<"K1.BIG=3&K1.K2.SMALL=1&BIGGER=2&K2.UNRELATED1=3">>,
                 expected_error => {[{"K1.BIG", <<"test error">>}]}}),
          Fail(#{path => [],
                 body => <<"K1.BIG=3&K1.K2.SMALL=2&BIGGER=1&K2.UNRELATED1=3">>,
                 expected_error => {[{"K1.BIG", <<"test error">>}]}}),
          Succ(#{path => [], %% Successfull because K1.BIG is not being set
                 body => <<"K1.K2.SMALL=3&K2.UNRELATED1=3">>,
                 predefined => [{b1, [{b2, [{big, 2}]}]}],
                 defaults => [{s1, [{small, 1}]}, {super_big, 4}],
                 expected_result => [{[s1, small], 3}, {[unrelated1], 3}]}),
          Fail(#{path => [],
                 body => <<"K1.BIG=1&K2.UNRELATED1=3">>,
                 predefined => [{b1, [{b2, [{big, 3}]}]}],
                 defaults => [{s1, [{small, 2}]}, {super_big, 4}],
                 expected_error => {[{"K1.BIG", <<"test error">>}]}}),
          Fail(#{path => ["K1"],
                 body => <<"BIG=1&K2.SMALL=2">>,
                 predefined => [{super_big, 3}],
                 expected_error => {[{"BIG", <<"test error">>}]}}),
          Fail(#{path => [], %% Dependency fun not called
                 body => <<"K1.BIG=1&K1.K2.SMALL=S&BIGGER=3&K2.UNRELATED1=3">>,
                 expected_error => {[{"K1.K2.SMALL",
                                      <<"The value must be an integer">>}]}}),
          Fail(#{path => [], %% Dependency fun not called
                 body => <<"K1.BIG=S&K1.K2.SMALL=2&BIGGER=3&K2.UNRELATED1=3">>,
                 expected_error => {[{"K1.BIG",
                                      <<"The value must be an integer">>}]}}),
          Fail(#{path => [], %% Dependency fun not called
                 body => <<"K1.BIG=1&K1.K2.SMALL=2&BIGGER=3&K2.UNRELATED1=S">>,
                 expected_error => {[{"K2.UNRELATED1",
                                      <<"The value must be an integer">>}]}}),
          Fail(#{path => [], %% Dependency fun not called
                 body => <<"K1.BIG=1&K1.K2.SMALL=2&BIGGER=3&K2.UNKNOWN1=STR">>,
                 expected_error => {[{"K2.UNKNOWN1", <<"Unsupported key">>}]}})
      end).

mandatory_test() ->
    Params = [{"K1.K2.K3", #{cfg_key => [k1, k2], type => int}},
              {"K1.K4", #{cfg_key => [t1, t2, t3], type => int,
                          %% This key is mandatory if K1.K2.K3 == 4
                          mandatory => fun (#{[k1, k2] := 4}) -> true;
                                           (#{}) -> false
                                       end}},
              {"K2.K4", #{cfg_key => t4, type => int,
                          %% This key is mandatory if K2.K2 is undefined
                          mandatory => fun (#{t3 := _}) -> false;
                                           (#{}) -> true
                                       end}},
              {"K2.K5", #{cfg_key => t3, type => int}}],

    with_request(
      fun (SetContType, SetBody, Req) ->
          SetContType("application/x-www-form-urlencoded"),
          Succ =
              fun (Path, Body, Predefined, Defaults, ExpectedResult) ->
                  SetBody(Body),
                  handle_post(fun (Parsed, Req2) when Req == Req2 ->
                                  ?assertEqual(ExpectedResult, Parsed)
                              end, Path, Params,
                              fun test_type_spec/1, Predefined, Defaults, Req)
              end,
          Fail =
              fun (Path, Body, Predefined, Defaults, ExpectedError) ->
                  SetBody(Body),
                  ?assertError(
                    {validation_failed, ExpectedError},
                    handle_post(fun (_Parsed, Req2) when Req == Req2 ->
                                    ?assert(false)
                                end, Path, Params,
                                fun test_type_spec/1,
                                Predefined, Defaults, Req))
              end,


          Fail([], <<"">>, [], [],
               {[{"K2.K4",<<"The value must be supplied">>}]}),
          Succ([], <<"K2.K5=1">>, [], [],
               [{[t3], 1}]),
          Succ([], <<"K1.K2.K3=3&K2.K5=1">>, [], [],
               [{[k1, k2], 3}, {[t3], 1}]),
          Fail([], <<"K1.K2.K3=3">>, [], [],
               {[{"K2.K4",<<"The value must be supplied">>}]}),
          Succ([], <<"K1.K2.K3=3&K2.K4=2">>, [], [],
               [{[k1, k2], 3}, {[t4], 2}]),
          Fail([], <<"K1.K2.K3=4&K2.K5=1">>, [], [],
               {[{"K1.K4",<<"The value must be supplied">>}]}),
          Succ([], <<"K1.K2.K3=4&K2.K5=1&K1.K4=2">>, [], [],
               [{[k1, k2], 4}, {[t3], 1}, {[t1, t2, t3], 2}]),
          Fail([], <<"K1.K2.K3=4">>, [], [],
               {[{"K2.K4",<<"The value must be supplied">>},
                 {"K1.K4",<<"The value must be supplied">>}]}),
          Succ([], <<"K1.K2.K3=4&K2.K4=1&K1.K4=2">>, [], [],
               [{[k1, k2], 4}, {[t4], 1}, {[t1, t2, t3], 2}]),

          Fail(["K1"], <<"">>, [], [],
               {[{"[upper-level].K2.K4",<<"The value must be supplied">>}]}),
          Fail(["K1"], <<"K2.K3=3">>, [], [],
               {[{"[upper-level].K2.K4",<<"The value must be supplied">>}]}),
          Fail(["K1"], <<"K2.K3=4">>, [], [],
               {[{"[upper-level].K2.K4",<<"The value must be supplied">>},
                 {"K4",<<"The value must be supplied">>}]}),

          Succ(["K1"], <<"">>, [{t4, 2}], [], []),
          Fail(["K1"], <<"">>, [], [{t4, 2}],
               {[{"[upper-level].K2.K4",<<"The value must be supplied">>}]}),
          Succ(["K1"], <<"K2.K3=3">>, [{t4, 2}], [],
               [{[k1, k2], 3}]),
          Succ(["K1"], <<"K2.K3=4">>, [{t1, [{t2, [{t3, 4}]}]}, {t4, 1}], [],
               [{[k1, k2], 4}]),

          Fail([], <<"K2.K4=1">>, [{k1, [{k2, 4}]}], [],
               {[{"K1.K4",<<"The value must be supplied">>}]}),
          Fail([], <<"K2.K4=1">>, [], [{k1, [{k2, 4}]}],
               {[{"K1.K4",<<"The value must be supplied">>}]}),
          Succ([], <<"K2.K4=1">>, [{t1, [{t2, [{t3, 2}]}]}], [{k1, [{k2, 4}]}],
               [{[t4], 1}]),
          Fail([], <<"K2.K4=1">>, [], [{k1, [{k2, 4}]},
                                       {t1, [{t2, [{t3, 2}]}]}],
               {[{"K1.K4",<<"The value must be supplied">>}]})
      end).
-endif.
