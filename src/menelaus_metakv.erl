%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

-module(menelaus_metakv).

-export([handle_get/2, handle_put/2, handle_delete/2]).

-include("ns_common.hrl").
-include("cut.hrl").
-include("ns_config.hrl").

get_key(Path) ->
    "_metakv" ++ Key = Path,
    list_to_binary(uri_string:unquote(Key)).

is_directory(Key) ->
    $/ =:= binary:last(Key).

handle_get(Path, Req) ->
    Key = get_key(Path),
    case is_directory(Key) of
        true ->
            Params = mochiweb_request:parse_qs(Req),
            Continuous = proplists:get_value("feed", Params) =:= "continuous",
            case {Continuous, metakv:check_continuous_allowed(Key)} of
                {true, false} ->
                    %% Return http error - 405: Method Not Allowed
                    menelaus_util:reply(Req, 405);
                _ ->
                    handle_iterate(Req, Key, Continuous)
            end;
        false ->
            handle_normal_get(Req, Key)
    end.

handle_normal_get(Req, Key) ->
    case metakv:get(Key) of
        false ->
            menelaus_util:reply_json(Req, [], 404);
        {value, Val0} ->
            Val = base64:encode(Val0),
            menelaus_util:reply_json(Req, {[{value, Val}]});
        {value, Val0, VC} ->
            case Val0 =:= ?DELETED_MARKER of
                true ->
                    menelaus_util:reply_json(Req, [], 404);
                false ->
                    Rev = base64:encode(erlang:term_to_binary(VC)),
                    Val = base64:encode(Val0),
                    menelaus_util:reply_json(Req, {[{rev, Rev},
                                                    {value, Val}]})
            end
    end.

handle_mutate(Req, Key, Value, Params) ->
    Start = os:timestamp(),
    Rev = case proplists:get_value("rev", Params) of
              undefined ->
                  case proplists:get_value("create", Params) of
                      undefined ->
                          undefined;
                      _ ->
                          missing
                  end;
              XRev ->
                  XRevB = list_to_binary(XRev),
                  binary_to_term(XRevB)
          end,
    Sensitive = proplists:get_value("sensitive", Params) =:= "true",
    case metakv:mutate(Key, Value,
                       [{rev, Rev}, {?METAKV_SENSITIVE, Sensitive}]) of
        ok ->
            ElapsedTime = timer:now_diff(os:timestamp(), Start) div 1000,
            %% Values are already displayed by ns_config_log and simple_store.
            %% ns_config_log is smart enough to not log sensitive values
            %% and simple_store does not store senstive values.
            ?metakv_debug("Updated ~p. Elapsed time:~p ms.", [Key, ElapsedTime]),
            menelaus_util:reply(Req, 200);
        Error ->
            ?metakv_debug("Failed to update ~p (rev ~p) with error ~p.",
                          [Key, Rev, Error]),
            menelaus_util:reply(Req, 409)
    end.

handle_put(Path, Req) ->
    Key = get_key(Path),
    case is_directory(Key) of
        true ->
            ?metakv_debug("PUT is not allowed for directories. Key = ~p", [Key]),
            menelaus_util:reply(Req, 405);
        false ->
            Params = mochiweb_request:parse_post(Req),
            Value = list_to_binary(proplists:get_value("value", Params)),
            handle_mutate(Req, Key, Value, Params)
    end.

handle_delete(Path, Req) ->
    Key = get_key(Path),
    case is_directory(Key) of
        true ->
            handle_recursive_delete(Req, Key);
        false ->
            handle_mutate(Req, Key, ?DELETED_MARKER, mochiweb_request:parse_qs(Req))
    end.

handle_recursive_delete(Req, Key) ->
    ?metakv_debug("handle_recursive_delete_post for ~p", [Key]),
    case metakv:delete_matching(Key) of
        ok ->
            ?metakv_debug("Recursively deleted children of ~p", [Key]),
            menelaus_util:reply(Req, 200);
        Error ->
            ?metakv_debug("Recursive deletion failed for ~p with error ~p.",
                       [Key, Error]),
            menelaus_util:reply(Req, 409)
    end.

handle_iterate(Req, Path, Continuous) ->
    HTTPRes = menelaus_util:reply_ok(Req, "application/json; charset=utf-8", chunked),
    ?metakv_debug("Starting iteration of ~s. Continuous = ~s", [Path, Continuous]),
    case Continuous of
        true ->
            ok = mochiweb_socket:setopts(mochiweb_request:get(socket, Req), [{active, true}]);
        false ->
            ok
    end,
    RV = metakv:iterate_matching(Path, Continuous, output_kv(Req, HTTPRes, _)),
    case Continuous of
        true ->
            RV;
        false ->
            menelaus_util:write_chunk(Req, "", HTTPRes)
    end.

output_kv(Req, HTTPRes, {K, V}) ->
    write_chunk(Req, HTTPRes, null, K, base64:encode(V), false);
output_kv(Req, HTTPRes, {K, V, VC, Sensitive}) ->
    Rev0 = base64:encode(erlang:term_to_binary(VC)),
    {Rev, Value} = case V of
                       ?DELETED_MARKER ->
                           {null, null};
                       _ ->
                           {Rev0, base64:encode(V)}
                   end,
    write_chunk(Req, HTTPRes, Rev, K, Value, Sensitive).

write_chunk(Req, HTTPRes, Rev, Path, Value, Sensitive) ->
    ?metakv_debug("Sent ~s rev: ~s sensitive: ~p", [Path, Rev, Sensitive]),
    menelaus_util:write_chunk(
      Req,
      ejson:encode({[{rev, Rev}, {path, Path}, {value, Value},
                     {sensitive, Sensitive}]}),
      HTTPRes).
