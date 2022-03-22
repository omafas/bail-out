%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc partitions replicator that uses DCP protocol
%%
-module(dcp_replicator).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("cut.hrl").

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
    ConsumerSock = gen_server:call(ConsumerConn, get_socket, infinity),

    ?log_debug("Opened connection to local memcached ~p", [ConsumerConn]),

    {ok, ProducerConn} = dcp_producer_conn:start_link(ConnName, ProducerNode,
                                                      Bucket),

    self() ! {connect_to_producer, ProducerConn, RepFeatures},

    {ok, #state{
            proxies = [{ConsumerConn, ConsumerSock}],
            consumer_conn = ConsumerConn,
            connection_name = ConnName,
            producer_node = ProducerNode,
            bucket = Bucket
           }}.

connect_to_producer(ProducerConn, RepFeatures,
                    #state{
                       proxies = [{ConsumerConn, ConsumerSock}],
                       consumer_conn = ConsumerConn,
                       connection_name = ConnName,
                       producer_node = ProducerNode,
                       bucket = Bucket
                      } = State) ->
    ok = dcp_producer_conn:connect(ProducerConn, RepFeatures),

    Proxies = dcp_proxy:connect_proxies(ConsumerConn, ConsumerSock,
                                        ProducerConn),

    ?log_debug("initiated new dcp replication with consumer side: ~p and "
               "producer side: ~p", [ConsumerConn, ProducerConn]),

    master_activity_events:note_dcp_replicator_start(Bucket, ConnName,
                                                     ProducerNode, ConsumerConn,
                                                     ProducerConn),

    State#state{proxies = Proxies}.

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

handle_info({connect_to_producer, ProducerConn, RepFeatures}, State) ->
    {noreply, spawn_and_wait(
                ?cut(connect_to_producer(ProducerConn, RepFeatures, State)))};
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
    CName = "replication:" ++ atom_to_list(ProducerNode) ++ "->" ++
        atom_to_list(ConsumerNode) ++ ":" ++ Bucket,

    case length(CName) =< ?MAX_DCP_CONNECTION_NAME of
        true ->
            CName;
        false ->
            case should_truncate_name(ConsumerNode, ProducerNode) of
                true ->
                    get_truncated_connection_name(
                      CName, ConsumerNode, ProducerNode, Bucket);
                false ->
                    CName
            end
    end.

get_truncated_connection_name(LongName, ConsumerNode, ProducerNode, Bucket) ->
    %% Trim off the common prefix to shorten the names.
    {CNode, PNode} = trim_common_prefix(ConsumerNode, ProducerNode),

    Hash = binary_to_list(base64:encode(crypto:hash(sha, LongName))),
    Bkt = string:slice(Bucket, 0, 60),

    CName = "replication:" ++ PNode ++ "->" ++ CNode ++ ":" ++ Bkt ++
        ":" ++ Hash,
    true = length(CName) =< ?MAX_DCP_CONNECTION_NAME,
    CName.

node_supports_truncated_names(Node) ->
    case Node == misc:this_node() of
        true ->
            true;
        false ->
            Quirks = rebalance_quirks:get_quirks([Node], long_names),
            not rebalance_quirks:is_enabled(
                  dont_truncate_long_names,
                  rebalance_quirks:get_node_quirks(Node, Quirks))
    end.

should_truncate_name(ConsumerNode, ProducerNode) ->
    case cluster_compat_mode:is_cluster_71() of
        true ->
            true;
        false ->
            lists:all(fun node_supports_truncated_names/1,
                      [ConsumerNode, ProducerNode])
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
get_connection_name_test_() ->
    LongBucket = "travel-sample-with-a-very-very-very-very-long-bucket-name",
    ConsumerNode = list_to_atom(
                     "ns_1@platform-couchbase-cluster-0000."
                     "platform-couchbase-cluster.couchbase-new-pxxxxxxx.svc"),
    ProducerNode = list_to_atom(
                     "ns_1@platform-couchbase-cluster-0001."
                     "platform-couchbase-cluster.couchbase-new-pxxxxxxx.svc"),
    VeryLongBucket = "com.yyyyyy.digital.ms.shoppingcart."
        "shoppingcart.123456789012345678901234567890",
    TrimmedName =
        "replication:1.platform-couchbase-cluster.couchb->0.platform-couchbase"
        "-cluster.couchb:com.yyyyyy.digital.ms.shoppingcart.shoppingcart."
        "123456789012:TYFMH5ZD2gPLOaLgcuA2VijsZvc=",
    {foreach,
     fun () ->
             meck:new(cluster_compat_mode, [passthrough]),
             meck:new(rebalance_quirks, [passthrough]),
             meck:new(misc, [passthrough]),
             meck:expect(cluster_compat_mode, is_cluster_71,
                         fun () -> true end)
     end,
     fun (_) ->
             meck:unload(cluster_compat_mode),
             meck:unload(rebalance_quirks),
             meck:unload(misc)
     end,
     [{"Connection name fits into the maximum allowed",
       fun () ->
               Conn = get_connection_name(
                        'nodeA.eng.couchbase.com', 'nodeB.eng.couchbase.com',
                        "bucket1"),
               ?assertEqual("replication:nodeB.eng.couchbase.com->"
                            "nodeA.eng.couchbase.com:bucket1", Conn),
               ?assertEqual(true, length(Conn) =< ?MAX_DCP_CONNECTION_NAME)
       end},
      {"Connection name won't fit into the maximum allowed.",
       fun () ->
               ?assertEqual(
                  TrimmedName,
                  get_connection_name(ConsumerNode, ProducerNode,
                                      VeryLongBucket))
       end},
      {"Connection name won't fit into the maximum allowed. Pre 7.1 "
       "Against the node that supports trimming",
       fun () ->
               meck:expect(cluster_compat_mode, is_cluster_71,
                           fun () -> false end),
               meck:expect(rebalance_quirks, get_quirks,
                           fun (_, long_names) -> [{ProducerNode, []}] end),
               meck:expect(misc, this_node, fun () -> ConsumerNode end),
               ?assertEqual(
                  TrimmedName,
                  get_connection_name(ConsumerNode, ProducerNode,
                                      VeryLongBucket))
       end},
      {"Connection name won't fit into the maximum allowed, but name trimming "
       "is not supported",
       fun () ->
               meck:expect(cluster_compat_mode, is_cluster_71,
                           fun () -> false end),
               meck:expect(rebalance_quirks, get_quirks,
                           fun (_, long_names) ->
                                   [{ProducerNode, [dont_truncate_long_names]}]
                           end),
               meck:expect(misc, this_node, fun () -> ConsumerNode end),
               Conn = get_connection_name(ConsumerNode, ProducerNode,
                                          VeryLongBucket),
               ?assertEqual(
                  "replication:ns_1@platform-couchbase-cluster-0001.platform"
                  "-couchbase-cluster.couchbase-new-pxxxxxxx.svc->ns_1@platform"
                  "-couchbase-cluster-0000.platform-couchbase-cluster.couchbase"
                  "-new-pxxxxxxx.svc:com.yyyyyy.digital.ms.shoppingcart."
                  "shoppingcart.123456789012345678901234567890", Conn)
       end},
      {"Test that the node names aren't shortened too much (note the only "
       "difference is the last character).",
       fun () ->
               Node1 = "ManyManyManyManyCommonCharacters_ns_1@platform-"
                   "couchbase-cluster-0000",
               Node2 = "ManyManyManyManyCommonCharacters_ns_1@platform-"
                   "couchbase-cluster-0001",
               Conn = get_connection_name(
                        list_to_atom(Node1), list_to_atom(Node2), LongBucket),
               ?assertEqual(
                  "replication:s_1@platform-couchbase-cluster-0001->"
                  "s_1@platform-couchbase-cluster-0000:travel-sample-with-a-"
                  "very-very-very-very-long-bucket-name:"
                  "D/D56MpAKsDt/0yqg6IXKBEaIcY=", Conn)
       end},
      {"Test with unique node names but one is much longer than the other.",
       fun () ->
               Node1 = "AShortNodeName",
               Node2 = "ManyManyManyManyCommonCharacters_ns_1@platform-"
                   "couchbase-cluster-AndEvenMoreCharactersToMakeThisNodeName"
                   "LongEnoughToRequireItToBeShortened",
               Conn = get_connection_name(
                        list_to_atom(Node1), list_to_atom(Node2), LongBucket),
               ?assertEqual(
                  "replication:ManyManyManyManyCommonCharacters_ns->"
                  "AShortNodeName:travel-sample-with-a-very-very-very-very-"
                  "long-bucket-name:A3aPD1Sik+5ZIz43M6NNTGn9XFw=", Conn)
       end}]}.
-endif.
