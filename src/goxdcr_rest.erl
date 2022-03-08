%% @author Couchbase <info@couchbase.com>
%% @copyright 2015-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc this module implements access to goxdcr component via REST API
%%

-module(goxdcr_rest).
-include("ns_common.hrl").

-export([proxy/1, proxy/2,
         find_all_replication_docs/1,
         find_all_replication_docs/0,
         all_local_replication_infos/0,
         stats/1,
         get_replications_with_remote_info/0]).

convert_header_name(Header) when is_atom(Header) ->
    atom_to_list(Header);
convert_header_name(Header) when is_list(Header) ->
    Header.

headers_for_proxy(MochiReq, Identity) ->
    HeadersList = mochiweb_headers:to_list(
                    mochiweb_request:get(headers, MochiReq)),
    Headers = lists:filtermap(
                fun ({'Content-Length', _Value}) ->
                        false;
                    ({Name, Value}) ->
                        case menelaus_rest:is_auth_header(Name) orelse
                            menelaus_auth:is_meta_header(Name) of
                            true ->
                                false;
                            false ->
                                {true, {convert_header_name(Name), Value}}
                        end
                end, HeadersList),
    [menelaus_rest:on_behalf_header(Identity),
     menelaus_rest:special_auth_header() | Headers].

send(MochiReq, Method, Path, Headers, Body) ->
    Params = mochiweb_request:parse_qs(MochiReq),
    Timeout = list_to_integer(proplists:get_value("connection_timeout", Params, "30000")),
    send_with_timeout(Method, Path, Headers, Body, Timeout).

send_with_timeout(Method, Path, Headers, Body, Timeout) ->
    URL = misc:local_url(service_ports:get_port(xdcr_rest_port), Path, []),

    {ok, {{Code, _}, RespHeaders, RespBody}} =
        rest_utils:request(goxdcr, URL, Method, Headers, Body, Timeout),

    SafeRespHeaders = lists:filter(fun is_safe_response_header/1, RespHeaders),
    {Code, SafeRespHeaders, RespBody}.

is_safe_response_header({"Content-Length", _}) ->
    false;
is_safe_response_header({"Transfer-Encoding", _}) ->
    false;
is_safe_response_header(_) ->
    true.

proxy(MochiReq) ->
    proxy(mochiweb_request:get(raw_path, MochiReq), MochiReq).

proxy(Path, MochiReq) ->
    Identity = menelaus_auth:get_identity(MochiReq),
    Headers = headers_for_proxy(MochiReq, Identity),
    Body = case mochiweb_request:recv_body(MochiReq) of
               undefined ->
                   <<>>;
               B ->
                   B
           end,
    {Code, RespHeaders, RespBody} = send(MochiReq, mochiweb_request:get(method, MochiReq), Path, Headers, Body),
    menelaus_util:reply(MochiReq, RespBody, Code, RespHeaders).

interesting_doc_key(<<"id">>) ->
    true;
interesting_doc_key(<<"type">>) ->
    true;
interesting_doc_key(<<"source">>) ->
    true;
interesting_doc_key(<<"target">>) ->
    true;
interesting_doc_key(<<"continuous">>) ->
    true;
interesting_doc_key(<<"pauseRequested">>) ->
    true;
interesting_doc_key(<<"filter_expression">>) ->
    true;
interesting_doc_key(<<"filterBypassExpiry">>) ->
    true;
interesting_doc_key(<<"filterDeletion">>) ->
    true;
interesting_doc_key(<<"filterExpiration">>) ->
    true;
interesting_doc_key(_) ->
    false.

convert_doc_key(<<"filter_expression">>) ->
    filterExpression;
convert_doc_key(Key) ->
    list_to_atom(binary_to_list(Key)).

query_goxdcr(Fun, Method, Path, Timeout) ->
    RV = {Code, _Headers, Body} =
        send_with_timeout(
          Method, Path,
          [menelaus_rest:special_auth_header(),
           {"Accept", "application/json"}], [], Timeout),
    case Code of
        200 ->
            case Body of
                <<>> ->
                    Fun([]);
                _ ->
                    Fun(ejson:decode(Body))
            end;
        _ ->
            erlang:throw({unsuccesful_goxdcr_call,
                          {method, Method},
                          {path, Path},
                          {response,  RV}})
    end.

get_from_goxdcr(Fun, Path, Timeout) ->
    case ns_cluster_membership:get_cluster_membership(node()) of
        active ->
            try
                query_goxdcr(Fun, "GET", Path, Timeout)
            catch error:{badmatch, {error, {econnrefused, _}}} ->
                    ?log_debug("Goxdcr is temporary not available. Return empty list."),
                    []
            end;
        _ ->
            []
    end.

process_doc({Props}) ->
    [{convert_doc_key(Key), Value} ||
        {Key, Value} <- Props,
        interesting_doc_key(Key)].

-spec find_all_replication_docs() -> [Doc :: [{Key :: atom(), Value :: _}]].
find_all_replication_docs() ->
    %% Use 30s as the default timeout.
    find_all_replication_docs(30000).

-spec find_all_replication_docs(non_neg_integer() | infinity) ->
                                       [Doc :: [{Key :: atom(), Value :: _}]].
find_all_replication_docs(Timeout) ->
    get_from_goxdcr(fun (Json) ->
                            [process_doc(Doc) || Doc <- Json]
                    end, "/pools/default/replications", Timeout).

process_repl_error(Error) ->
    TimeStamp  = misc:expect_prop_value(<<"Time">>, Error),

    Time = calendar:now_to_local_time(misc:time_to_timestamp(TimeStamp, nanosecond)),

    {{Year, Month, Day}, {Hour, Minute, Second}} = Time,
    TimeFormatted = io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B ",
                                  [Year, Month, Day, Hour, Minute, Second]),
    TimeBin = iolist_to_binary(TimeFormatted),
    Message = misc:expect_prop_value(<<"ErrorMsg">>, Error),
    {Time, <<TimeBin/binary, Message/binary>>}.

process_repl_info({Info}, Acc) ->
    case misc:expect_prop_value(<<"StatsMap">>, Info) of
        null ->
            Acc;
        {StatsList} ->
            Id = misc:expect_prop_value(<<"Id">>, Info),
            Stats = [{list_to_atom(binary_to_list(K)), V} ||
                        {K, V} <- StatsList],
            ErrorList =  misc:expect_prop_value(<<"ErrorList">>, Info),
            Errors = [process_repl_error(Error) || {Error} <- ErrorList],

            [{Id, Stats, Errors} | Acc]
    end.

-spec all_local_replication_infos() -> [{Id :: binary(), [{atom(), _}],
                                         [{erlang:timestamp(), ErrorMsg :: binary()}]}].
all_local_replication_infos() ->
    get_from_goxdcr(fun (Json) ->
                            lists:foldl(fun process_repl_info/2, [], Json)
                    end, "/pools/default/replicationInfos", 30000).

grab_stats(Bucket) ->
    get_from_goxdcr(fun ({Json}) ->
                            [{Id, Stats} || {Id, {Stats}} <- Json]
                    end, "/stats/buckets/" ++ mochiweb_util:quote_plus(Bucket), 30000).

stats(Bucket) ->
    try grab_stats(Bucket) of
        Stats ->
            Stats
    catch T:E:S ->
            ?log_debug("Unable to obtain stats for bucket ~p from goxdcr:~n~p",
                       [Bucket, {T, E, S}]),
            []
    end.

parse_remote_bucket_reference(Reference) ->
    case binary:split(Reference, <<"/">>, [global]) of
        [<<>>, <<"remoteClusters">>, ClusterUUID, <<"buckets">>, BucketName] ->
            ClusterUUID1 = list_to_binary(mochiweb_util:unquote(ClusterUUID)),
            {ok, {ClusterUUID1, mochiweb_util:unquote(BucketName)}};
        _ ->
            {error, bad_reference}
    end.

get_replications_with_remote_info() ->
    RemoteClusters =
        get_from_goxdcr(
          fun (Json) ->
                  [{misc:expect_prop_value(<<"uuid">>, Cluster),
                    misc:expect_prop_value(<<"name">>, Cluster)}
                   || {Cluster} <- Json]
          end, "/pools/default/remoteClusters", 30000),

    lists:foldl(
      fun (Props, Acc) ->
              BucketName = binary_to_list(misc:expect_prop_value(source, Props)),
              Id = misc:expect_prop_value(id, Props),
              Target = misc:expect_prop_value(target, Props),
              {ok, {RemoteClusterUUID, RemoteBucket}} =
                  parse_remote_bucket_reference(Target),
              ClusterName = proplists:get_value(RemoteClusterUUID, RemoteClusters, <<"unknown">>),
              [{Id, BucketName, binary_to_list(ClusterName), RemoteBucket} | Acc]
      end, [], find_all_replication_docs()).
