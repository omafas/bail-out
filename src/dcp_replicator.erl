%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%% @doc partitions replicator that uses DCP protocol
%%
-module(dcp_replicator).

-behaviour(gen_server).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/3, start_link/6,
         get_partitions/1,
         setup_replication/2, setup_replication/3,
         takeover/2, takeover/3,
         wait_for_data_move/3,
         trim_common_prefix/2,
         get_docs_estimate/3,
         get_connections/1]).

-record(state, {proxies,
                consumer_conn :: pid(),
                connection_name :: nonempty_string(),
                producer_node :: node(),
                bucket :: bucket_name()}).

-define(VBUCKET_POLL_INTERVAL, 100).
-define(SHUT_CONSUMER_TIMEOUT, ?get_timeout(dcp_shut_consumer, 60000)).
-define(MIN_NODE_NAME, 35).

init({ConsumerNode, ProducerNode, Bucket, ConnName, RepFeatures}) ->
    process_flag(trap_exit, true),

    {ok, ConsumerConn} = dcp_consumer_conn:start_link(ConnName, ConsumerNode,
                                                      Bucket, RepFeatures),
    {ok, ProducerConn} = dcp_producer_conn:start_link(ConnName, ProducerNode,
                                                      Bucket, RepFeatures),

    Proxies = dcp_proxy:connect_proxies(ConsumerConn, ProducerConn),

    ?log_debug("initiated new dcp replication with consumer side: ~p and "
               "producer side: ~p", [ConsumerConn, ProducerConn]),

    master_activity_events:note_dcp_replicator_start(Bucket, ConnName,
                                                     ProducerNode, ConsumerConn,
                                                     ProducerConn),

    {ok, #state{
            proxies = Proxies,
            consumer_conn = ConsumerConn,
            connection_name = ConnName,
            producer_node = ProducerNode,
            bucket = Bucket
           }}.

start_link(Name, ConsumerNode, ProducerNode, Bucket, ConnName, RepFeatures) ->
    %% We (and ep-engine actually) depend on this naming.
    true = lists:prefix("replication:", ConnName),

    Args0 = [?MODULE, {ConsumerNode,
                       ProducerNode, Bucket, ConnName, RepFeatures}, []],
    Args  = case Name of
                undefined ->
                    Args0;
                _ ->
                    [{local, Name} | Args0]
            end,
    erlang:apply(gen_server, start_link, Args).

start_link(ProducerNode, Bucket, RepFeatures) ->
    ConsumerNode = node(),
    ConnName = get_connection_name(ConsumerNode, ProducerNode, Bucket),
    start_link(server_name(ProducerNode, Bucket),
               ConsumerNode, ProducerNode, Bucket, ConnName, RepFeatures).

server_name(ProducerNode, Bucket) ->
    list_to_atom(?MODULE_STRING "-" ++ Bucket ++ "-" ++ atom_to_list(ProducerNode)).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(Msg, State) ->
    ?rebalance_warning("Unhandled cast: ~p" , [Msg]),
    {noreply, State}.

terminate(Reason, #state{proxies = Proxies,
                         consumer_conn = Consumer}) ->
    try
        %% When replicator terminates normally we want to ensure that consumer
        %% connection are closed first. So if another replicator tries to
        %% replicate one of the same vbuckets, it doesn't get an EEXIST error.
        %%
        %% It's important that we close just the consumer connections. It's
        %% needed to handle failover properly. That is, we don't want the
        %% failover time to depend on slow (failed over) producer termination.
        maybe_shut_consumer(Reason, Consumer)
    after
        dcp_proxy:terminate_and_wait(Proxies, Reason)
    end,
    ok.

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info(Msg, State) ->
    ?rebalance_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_call({setup_replication, Partitions}, _From, #state{consumer_conn = Pid} = State) ->
    RV = spawn_and_wait(fun () ->
                                dcp_consumer_conn:setup_streams(Pid, Partitions)
                        end),
    {reply, RV, State};

handle_call({takeover, Partition}, _From, #state{consumer_conn = Pid} = State) ->
    RV = spawn_and_wait(fun () ->
                                dcp_consumer_conn:maybe_close_stream(Pid, Partition),
                                dcp_consumer_conn:takeover(Pid, Partition)
                        end),
    {reply, RV, State};

handle_call(get_partitions, _From, #state{consumer_conn = Pid} = State) ->
    {reply, gen_server:call(Pid, get_partitions, infinity), State};

handle_call(Command, _From, State) ->
    ?rebalance_warning("Unexpected handle_call(~p, ~p)", [Command, State]),
    {reply, refused, State}.

get_partitions(Pid) ->
    try
        gen_server:call(Pid, get_partitions, infinity)
    catch
        exit:{noproc, _} ->
            not_running
    end.

setup_replication(Pid, Partitions) ->
    gen_server:call(Pid, {setup_replication, Partitions}, infinity).

setup_replication(ProducerNode, Bucket, Partitions) ->
    setup_replication(whereis(server_name(ProducerNode, Bucket)), Partitions).

takeover(Replicator, Partition) ->
    gen_server:call(Replicator, {takeover, Partition}, infinity).

takeover(ProducerNode, Bucket, Partition) ->
    takeover(whereis(server_name(ProducerNode, Bucket)), Partition).

wait_for_data_move(Nodes, Bucket, Partition) ->
    DoneLimit = ns_config:read_key_fast(dcp_move_done_limit, 1000),
    wait_for_data_move_loop(Nodes, Bucket, Partition, DoneLimit).

wait_for_data_move_loop([], _, _, _DoneLimit) ->
    ok;
wait_for_data_move_loop([Node | Rest], Bucket, Partition, DoneLimit) ->
    Connection = get_connection_name(Node, node(), Bucket),
    case wait_for_data_move_on_one_node(0, Connection,
                                        Bucket, Partition, DoneLimit) of
        ok ->
            wait_for_data_move_loop(Rest, Bucket, Partition, DoneLimit);
        {error, _} = Error ->
            ?log_error("Error getting dcp stats "
                       "for bucket ~p, partition ~p, connection ~p: ~p",
                       [Bucket, Partition, Connection, Error]),
            Error
    end.

wait_for_data_move_on_one_node(Iterations, Connection,
                               Bucket, Partition, DoneLimit) ->
    {ok, Estimate} = ns_memcached:get_dcp_docs_estimate(Bucket,
                                                        Partition, Connection),
    case check_move_done(Estimate, DoneLimit) of
        ok ->
            ok;
        retry ->
            NewIterations =
                case Iterations of
                    300 ->
                        ?rebalance_debug(
                           "Still waiting for backfill on connection ~p, "
                           "bucket ~p, partition ~p, last estimate ~p",
                           [Connection, Bucket, Partition, Estimate]),
                        0;
                    I ->
                        I + 1
                end,
            timer:sleep(?VBUCKET_POLL_INTERVAL),
            wait_for_data_move_on_one_node(NewIterations, Connection,
                                           Bucket, Partition, DoneLimit);
        {error, _} = Error ->
            Error
    end.

check_move_done({_, _, <<"does_not_exist">>}, _DoneLimit) ->
    {error, no_stats_for_this_vbucket};
check_move_done({_, _, <<"calculating-item-count">>}, _DoneLimit) ->
    retry;
check_move_done({N, _, _}, DoneLimit)
  when N < DoneLimit ->
    ok;
check_move_done(_Estimate, _DoneLimit) ->
    retry.

-spec get_docs_estimate(bucket_name(), vbucket_id(), node()) ->
                               {ok, {non_neg_integer(), non_neg_integer(), binary()}}.
get_docs_estimate(Bucket, Partition, ConsumerNode) ->
    Connection = get_connection_name(ConsumerNode, node(), Bucket),
    ns_memcached:get_dcp_docs_estimate(Bucket, Partition, Connection).

get_connection_name(ConsumerNode, ProducerNode, Bucket) ->
    ConsumerNodeList = atom_to_list(ConsumerNode),
    ProducerNodeList = atom_to_list(ProducerNode),
    CName = "replication:" ++ ProducerNodeList ++ "->" ++ ConsumerNodeList ++
        ":" ++ Bucket,

    case length(CName) =< ?MAX_DCP_CONNECTION_NAME of
        true ->
            CName;
        false ->
            %% Trim off the common prefix to shorten the names.
            {CNode, PNode} = trim_common_prefix(ConsumerNode, ProducerNode),

            Hash = binary_to_list(base64:encode(crypto:hash(sha, CName))),
            Bkt = string:slice(Bucket, 0, 60),

            CName2 = "replication:" ++ PNode ++ "->" ++ CNode ++ ":" ++ Bkt ++
                     ":" ++ Hash,
            true = length(CName2) =< ?MAX_DCP_CONNECTION_NAME,
            CName2
    end.

trim_common_prefix(Consumer, Producer) ->
    %% Find the longest common prefix for the two nodes and chop
    %% it off (but not below a minimal length).
    LCP = binary:longest_common_prefix(
            [atom_to_binary(Consumer, latin1),
             atom_to_binary(Producer, latin1)]),
    Consumer1 = maybe_cut_name(atom_to_list(Consumer), LCP),
    Producer1 = maybe_cut_name(atom_to_list(Producer), LCP),
    {Consumer1, Producer1}.

%% Cut the specified number of bytes from the front of the name but don't
%% shorten below a minimum length.
maybe_cut_name(Name, MaxToChop) ->
    Len = length(Name),
    Start = case (Len - MaxToChop) >= ?MIN_NODE_NAME of
                       true ->
                           MaxToChop;
                       false ->
                           max(0, Len - ?MIN_NODE_NAME)
                   end,
    string:slice(Name, Start, ?MIN_NODE_NAME).

get_connections(Bucket) ->
    {ok, Connections} =
        ns_memcached:raw_stats(
          node(), Bucket, <<"dcp">>,
          fun(<<"eq_dcpq:replication:", K/binary>>, <<"consumer">>, Acc) ->
                  case binary:longest_common_suffix([K, <<":type">>]) of
                      5 ->
                          ["replication:" ++ binary_to_list(binary:part(K, {0, byte_size(K) - 5})) | Acc];
                      _ ->
                          Acc
                  end;
             (_, _, Acc) ->
                  Acc
          end, []),
    Connections.

spawn_and_wait(Body) ->
    WorkerPid = spawn_link(
                  fun () ->
                          try Body() of
                              RV ->
                                  exit({done, RV})
                          catch T:E:Stack ->
                                  exit({done, T, E, Stack})
                          end
                  end),
    receive
        {'EXIT', WorkerPid, Reason} ->
            case Reason of
                {done, RV} ->
                    RV;
                {done, T, E, Stack} ->
                    erlang:raise(T, E, Stack);
                _ ->
                    ?log_error("Got unexpected reason from ~p: ~p", [WorkerPid, Reason]),
                    erlang:error({unexpected_reason, Reason})
            end;
        {'EXIT', From, Reason} = ExitMsg ->
            ?log_debug("Received exit with reason ~p from ~p. Killing child process ~p",
                       [Reason, From, WorkerPid]),
            misc:sync_shutdown_many_i_am_trapping_exits([WorkerPid]),
            erlang:error({child_interrupted, ExitMsg})
    end.

should_shut_consumer(shutdown) ->
    true;
should_shut_consumer({shutdown, _}) ->
    true;
should_shut_consumer(_) ->
    false.

maybe_shut_consumer(Reason, Consumer) ->
    case should_shut_consumer(Reason) of
        true ->
            ok = dcp_consumer_conn:shut_connection(Consumer, ?SHUT_CONSUMER_TIMEOUT);
        false ->
            ok
    end.

-ifdef(TEST).
get_connection_name_test() ->

    %% Connection name fits into the maximum allowed

    NodeA = 'nodeA.eng.couchbase.com',
    NodeB = 'nodeB.eng.couchbase.com',
    BucketAB = "bucket1",
    ConnAB = get_connection_name(NodeA, NodeB, BucketAB),
    ?assertEqual("replication:nodeB.eng.couchbase.com->"
                 "nodeA.eng.couchbase.com:bucket1", ConnAB),
    ?assertEqual(true, length(ConnAB) =< ?MAX_DCP_CONNECTION_NAME),

    %% Test where the connection name, using the pre-NEO method, won't
    %% fit into the maximum allowed.

    Node1 = "ns_1@platform-couchbase-cluster-0000.platform-couchbase-cluster."
            "couchbase-new-pxxxxxxx.svc",
    Node2 = "ns_1@platform-couchbase-cluster-0001.platform-couchbase-cluster."
            "couchbase-new-pxxxxxxx.svc",
    Bucket12 = "com.yyyyyy.digital.ms.shoppingcart.shoppingcart.1234567890"
               "12345678901234567890",
    Conn12 = get_connection_name(list_to_atom(Node1), list_to_atom(Node2),
                                 Bucket12),
    ?assertEqual("replication:1.platform-couchbase-cluster.couchb->"
                 "0.platform-couchbase-cluster.couchb:com.yyyyyy.digital.ms."
                 "shoppingcart.shoppingcart.123456789012:"
                 "TYFMH5ZD2gPLOaLgcuA2VijsZvc=", Conn12),

    %% Test that the node names aren't shortened too much (note the only
    %% difference is the last character).

    Node3 = "ManyManyManyManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-0000",
    Node4 = "ManyManyManyManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-0001",
    LongBucket = "travel-sample-with-a-very-very-very-very-long-bucket-name",
    Conn34 = get_connection_name(list_to_atom(Node3), list_to_atom(Node4),
                                 LongBucket),
    ?assertEqual("replication:s_1@platform-couchbase-cluster-0001->"
                 "s_1@platform-couchbase-cluster-0000:travel-sample-with-a-"
                 "very-very-very-very-long-bucket-name:"
                 "D/D56MpAKsDt/0yqg6IXKBEaIcY=", Conn34),

    %% Test with unique node names but one is much longer than the other.

    Node5 = "AShortNodeName",
    Node6 = "ManyManyManyManyCommonCharacters_ns_1@platform-couchbase-cluster"
            "-AndEvenMoreCharactersToMakeThisNodeNameLongEnoughToRequireIt"
            "ToBeShortened",
    Conn56 = get_connection_name(list_to_atom(Node5), list_to_atom(Node6),
                                 LongBucket),
    ?assertEqual("replication:ManyManyManyManyCommonCharacters_ns->"
                 "AShortNodeName:travel-sample-with-a-very-very-very-very-"
                 "long-bucket-name:A3aPD1Sik+5ZIz43M6NNTGn9XFw=", Conn56).

-endif.
