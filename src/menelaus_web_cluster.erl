%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%

%% @doc implementation of cluster topology related REST API's

-module(menelaus_web_cluster).

-include("cut.hrl").
-include("ns_common.hrl").
-include("menelaus_web.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_cluster_init/1,
         handle_engage_cluster2/1,
         handle_complete_join/1,
         handle_join/1,
         serve_node_services/1,
         serve_node_services_streaming/1,
         handle_setup_services_post/1,
         handle_rebalance_progress/2,
         handle_eject_post/1,
         handle_add_node/1,
         handle_add_node_to_group/2,
         handle_hard_reset_node/1,
         handle_start_hard_failover/2,
         handle_start_graceful_failover/1,
         handle_rebalance/1,
         handle_re_add_node/1,
         handle_re_failover/1,
         handle_stop_rebalance/1,
         handle_set_recovery_type/1,
         get_rebalance_error/0,
         handle_current_rebalance_report/1]).

-import(menelaus_util,
        [reply_json/2,
         reply_json/3,
         reply/2,
         reply_text/3,
         reply_ok/3,
         parse_validate_port_number/1,
         handle_streaming/2]).

handle_cluster_init(Req) ->
    menelaus_web_rbac:assert_no_users_upgrade(),
    menelaus_util:survive_web_server_restart(
        fun () -> handle_cluster_init(Req, 10) end).

handle_cluster_init(_Req, Retries) when Retries =< 0 ->
    erlang:error(exceeded_retries);
handle_cluster_init(Req, Retries) ->
    Config = ns_config:get(),
    Snapshot = chronicle_compat:get_snapshot(
                 [ns_bucket:fetch_snapshot(all, _, [props]),
                  ns_cluster_membership:fetch_snapshot(_)],
                 #{ns_config => Config}),

    validator:handle(
      fun (Props) ->
          try
              ok = menelaus_web_node:node_init(Req, Props),
              Res = cluster_init(Req, Config, Props),
              reply_json(Req, Res)
          catch
              throw:{error, Code, Msg} ->
                  menelaus_util:global_error_exception(Code, Msg);
              throw:retry_needed ->
                  handle_cluster_init(Req, Retries - 1)
          end
      end, Req, form,
      menelaus_web_node:node_init_validators() ++
      cluster_init_validators(Config, Snapshot)).

cluster_init(Req, Config, Params) ->
    %% POST /pools/default
    menelaus_web_pools:handle_pool_settings_post_body(Req, Config, Params),
    %% POST /settings/stats
    menelaus_web_settings:apply_stats_settings(Params),
    %% POST /node/controller/setupServices
    case do_setup_services_post(Req, Params) of
        ok -> ok;
        {error, ErrorMsg} -> throw({error, 400, ErrorMsg})
    end,
    %% setting of n2n encryption
    case proplists:get_value(nodeEncryption, Params) of
        undefined -> ok;
        Encryption ->
            AFamily = cb_dist:address_family(),
            CBDistCfg = [{nodeEncryption, Encryption},
                         {externalListeners, [{AFamily, Encryption}]}],
            case netconfig_updater:apply_config(CBDistCfg) of
                ok ->
                    %% Wait for web servers to restart
                    ns_config:sync_announcements(),
                    menelaus_event:sync(
                      chronicle_compat_events:event_manager()),
                    cluster_compat_mode:is_enterprise() andalso
                        ns_ssl_services_setup:sync();
                {error, ErrorTerm} ->
                    Msg = iolist_to_binary(
                            netconfig_updater:format_error(ErrorTerm)),
                    throw({error, 400, Msg})
            end
    end,
    %% POST /settings/indexes
    IndexerParams = [{storageMode, V} || {indexerStorageMode, V} <- Params],
    menelaus_web_indexes:apply_indexes_settings(Req, IndexerParams),

    case proplists:get_value(allowedHosts, Params) of
        unchanged -> ok;
        AllowedHosts when is_list(AllowedHosts) ->
            ns_config:set(allowed_hosts, AllowedHosts)
    end,

    %% POST /settings/web
    menelaus_web_settings:handle_settings_web_post(Req, Params).

cluster_init_validators(Config, Snapshot) ->
    menelaus_web_pools:pool_settings_post_validators(Config, Snapshot) ++
    menelaus_web_settings:settings_stats_validators() ++
    setup_services_validators() ++
    menelaus_web_queries:cluster_init_validators() ++
    menelaus_web_node:node_encryption_validators() ++
    menelaus_web_settings:settings_web_post_validators() ++
    [menelaus_web_indexes:validate_storage_mode(indexerStorageMode, _),
     validator:token_list(allowedHosts, ",", _),
     validator:convert(allowedHosts,
                       ?cut(lists:map(fun iolist_to_binary/1, _)), _),
     validator:validate(
       fun (Hosts) ->
               case ns_config_auth:is_system_provisioned() of
                   true ->
                       {error, "cannot change allowedHosts after cluster is "
                        "provisioned"};
                   false ->
                       menelaus_web_settings:validate_allowed_hosts_list(Hosts)
               end
       end, allowedHosts, _),
     %% setting it to 'unchanged' to make sure validate_relative is called
     %% even if allowedHosts is not provided
     validator:default(allowedHosts, unchanged, _),
     validator:validate_relative(
       fun (Hostname, AllowedHosts0) ->
           AllowedHosts = case AllowedHosts0 of
                              unchanged -> ns_cluster:allowed_hosts();
                              _ -> AllowedHosts0
                          end,
           case ns_cluster:is_host_allowed(Hostname, AllowedHosts) of
               true -> ok;
               false ->
                   Msg = io_lib:format(
                           "Can't use '~s' as a node name because "
                           "it is not allowed by the 'allowedHosts' setting",
                           [Hostname]),
                   {error, Msg}
           end
       end, hostname, allowedHosts, _),
     validator:has_params(_),
     validator:unsupported(_)].

handle_engage_cluster2(Req) ->
    Body = mochiweb_request:recv_body(Req),
    {NodeKVList} = ejson:decode(Body),
    %% a bit kludgy, but 100% correct way to protect ourselves when
    %% everything will restart.
    process_flag(trap_exit, true),
    case ns_cluster:engage_cluster(NodeKVList) of
        {ok, _} ->
            %% NOTE: for 2.1+ cluster compat we may need
            %% something fancier. For now 2.0 is compatible only with
            %% itself and 1.8.x thus no extra work is needed.
            %%
            %% The idea is that engage_cluster/complete_join sequence
            %% is our cluster version compat negotiation. First
            %% cluster sends joinee node it's info in engage_cluster
            %% payload. Node then checks if it can work in that compat
            %% mode (node itself being single node cluster works in
            %% max compat mode it supports, which is perhaps higher
            %% then cluster's). If node supports this mode, it needs
            %% to send back engage_cluster reply with
            %% clusterCompatibility of cluster compat mode. Otherwise
            %% cluster would refuse this node as it runs in higher
            %% compat mode. That could be much much higher future
            %% compat mode. So only joinee knows if it can work in
            %% backwards compatible mode or not. Thus sending back of
            %% 'corrected' clusterCompatibility in engage_cluster
            %% response is our only option.
            %%
            %% NOTE: we don't need to actually switch to lower compat
            %% mode during engage_cluster. Because complete_join will
            %% cause full restart of joinee node, which will cause it
            %% to start back in cluster's compat mode.
            %%
            %% For now we just look if 1.8.x is asking us to join it
            %% and if it is, then we reply with clusterCompatibility
            %% of 1 which is the only thing they'll support
            %%
            %% NOTE: I was thinking about simply sending back
            %% clusterCompatibility of cluster, but that would break
            %% 10.x (i.e. much future version) check of backwards
            %% compatibility with us. I.e. because 2.0 is not checking
            %% cluster's compatibility (there's no need), lying about
            %% our cluster compat mode would not allow cluster to
            %% check we're compatible with it.
            %%
            %% 127.0.0.1 below is a bit subtle. See MB-8404. In
            %% CBSE-385 we saw how mis-configured node that was forced
            %% into 127.0.0.1 address was successfully added to
            %% cluster. And my thinking is "rename node in
            %% node-details output if node is 127.0.0.1" behavior
            %% that's needed for correct client's operation is to
            %% blame here. But given engage cluster is strictly
            %% intra-cluster thing we can return 127.0.0.1 back as
            %% 127.0.0.1 and thus join attempt in CBSE-385 would be
            %% prevented at completeJoin step which would be sent to
            %% 127.0.0.1 (joiner) and bounced.
            {Result} = menelaus_web_node:build_full_node_info(node()),
            {_, _} = CompatTuple =
                lists:keyfind(<<"clusterCompatibility">>, 1, NodeKVList),
            ThreeXCompat =
                cluster_compat_mode:effective_cluster_compat_version_for(
                  cluster_compat_mode:supported_compat_version()),
            ResultWithCompat =
                case CompatTuple of
                    {_, V} when V < ThreeXCompat ->
                        ?log_info("Lowering our advertised clusterCompatibility"
                                  " in order to enable joining older cluster"),
                        Result3 =
                            lists:keyreplace(<<"clusterCompatibility">>, 1,
                                             Result, CompatTuple),
                        lists:keyreplace(clusterCompatibility, 1, Result3,
                                         CompatTuple);
                    _ ->
                        Result
                end,
            reply_json(Req, {ResultWithCompat});
        {error, _What, Message} ->
            reply_json(Req, [Message], 400)
    end,
    exit(normal).

handle_complete_join(Req) ->
    {NodeKVList} = ejson:decode(mochiweb_request:recv_body(Req)),
    erlang:process_flag(trap_exit, true),
    case ns_cluster:complete_join(NodeKVList) of
        {ok, _} ->
            reply_json(Req, [], 200);
        {error, _What, Message} ->
            reply_json(Req, [Message], 400)
    end,
    exit(normal).

handle_join(Req) ->
    %% paths:
    %%  cluster secured, admin logged in:
    %%         after creds work and node join happens,
    %%         200 returned with Location header pointing
    %%         to new /pool/default
    %%  cluster not secured, after node join happens,
    %%         a 200 returned with Location header to new /pool/default,
    %%         401 if request had
    %%  cluster either secured or not:
    %%         a 400 with json error message when join fails for whatever reason
    %%
    %% parameter example: clusterMemberHostIp=192%2E168%2E0%2E1&
    %%                    clusterMemberPort=8091&
    %%                    user=admin&password=admin123
    %%
    case ns_config_auth:is_system_provisioned() of
        true ->
            Msg = <<"Node is already provisioned. "
                    "To join use controller/addNode api of the cluster">>,
            reply_json(Req, [Msg], 400);
        false ->
            handle_join_clean_node(Req)
    end.

parse_validate_services_list(ServicesList) ->
    KnownServices = ns_cluster_membership:supported_services(),
    ServicePairs = [{erlang:atom_to_list(S), S} || S <- KnownServices],
    ServiceStrings = string:tokens(ServicesList, ","),
    FoundServices =
        [{SN, lists:keyfind(SN, 1, ServicePairs)} || SN <- ServiceStrings],
    UnknownServices = [SN || {SN, false} <- FoundServices],
    case UnknownServices of
        [_|_] ->
            {error, io_lib:format("Unknown services: ~p", [UnknownServices])};
        [] ->
            RV = lists:usort([S || {_, {_, S}} <- FoundServices]),
            case RV of
                [] ->
                    {error, "At least one service has to be selected"};
                _ ->
                    {ok, RV}
            end
    end.

parse_join_cluster_params(Params, ThisIsJoin) ->
    Hostname =
        case proplists:get_value("hostname", Params) of
            undefined when ThisIsJoin =:= true ->
                %%  this is for backward compatibility
                CMemPort = proplists:get_value("clusterMemberPort", Params),
                CMemHostIp = proplists:get_value("clusterMemberHostIp", Params),
                case lists:member(undefined, [CMemPort, CMemHostIp]) of
                    true ->
                        "";
                    _ ->
                        lists:concat([CMemHostIp, ":", CMemPort])
                end;
            undefined -> "";
            X -> X
        end,
    OtherUser = proplists:get_value("user", Params),
    OtherPswd = proplists:get_value("password", Params),
    OtherClientCert = proplists:get_value("clientCertAuth", Params, "false"),

    ClientCertAuthErrors =
        case OtherClientCert of
            "true" -> [];
            "false" -> [];
            _ ->
                Err = io_lib:format("invalid clientCertAuth value: ~p",
                                    [OtherClientCert]),
                [iolist_to_binary(Err)]
        end,

    AddNodeErrors =
        case ThisIsJoin of
            false ->
                KnownParams = ["hostname", "user", "password", "services",
                               "clientCertAuth"],
                UnknownParams = [K || {K, _} <- Params,
                                      not lists:member(K, KnownParams)],
                case UnknownParams of
                    [_|_] ->
                        Msg = io_lib:format("Got unknown parameters: ~p",
                                            [UnknownParams]),
                        [iolist_to_binary(Msg)];
                    [] ->
                        []
                end;
            true -> []
        end,

    ServicelessAllowed =
        cluster_compat_mode:is_enterprise() andalso
        cluster_compat_mode:is_cluster_76(),

    Services = case proplists:get_value("services", Params) of
                   undefined ->
                       {ok, ns_cluster_membership:default_services()};
                   [] when ServicelessAllowed ->
                       {ok, []};
                   SvcParams ->
                       case parse_validate_services_list(SvcParams) of
                           {ok, Svcs} ->
                               {ok, Svcs};
                           {error, Error} ->
                               {error, iolist_to_binary(Error)}
                       end
               end,

    BasePList = case OtherClientCert of
                    "true" -> [{client_cert_auth, true}];
                    _ -> [{user, OtherUser}, {password, OtherPswd}]
                end,

    MissingFieldErrors = [iolist_to_binary([atom_to_list(F), <<" is missing">>])
                          || {F, V} <- BasePList,
                             V =:= undefined],

    DefaultScheme = case cluster_compat_mode:tls_supported() of
                        true -> https;
                        false -> http
                    end,

    {HostnameError, ParsedHostnameRV} =
        case (catch parse_hostname(Hostname, DefaultScheme)) of
            {error, HMsgs} ->
                {HMsgs, undefined};
            {ParsedScheme, ParsedHost, ParsedPort} when is_list(ParsedHost) ->
                {[], {ParsedScheme, ParsedHost, ParsedPort}}
        end,

    NewHostnameParams = case proplists:get_value("newNodeHostname", Params) of
                            undefined -> [];
                            NH ->
                                case string:trim(NH) of
                                    "" -> [];
                                    "127.0.0.1" -> [];
                                    "::1" -> [];
                                    _ -> [{new_node_hostname, NH}]
                                end
                        end,

    Errors = MissingFieldErrors ++ HostnameError ++ AddNodeErrors ++
        ClientCertAuthErrors ++
        case Services of
            {error, ServicesError} ->
                [ServicesError];
            _ ->
                []
        end,
    case Errors of
        [] ->
            {ok, ServicesList} = Services,
            {Scheme, Host, Port} = ParsedHostnameRV,
            {ok, [{services, ServicesList},
                  {scheme, Scheme},
                  {host, Host},
                  {port, Port}
                  | BasePList ++ NewHostnameParams]};
        _ ->
            {errors, Errors}
    end.

handle_join_clean_node(Req) ->
    Params = mochiweb_request:parse_post(Req),

    case parse_join_cluster_params(Params, true) of
        {errors, Errors} ->
            reply_json(Req, Errors, 400);
        {ok, Fields} ->
            OtherScheme = proplists:get_value(scheme, Fields),
            OtherHost = proplists:get_value(host, Fields),
            OtherPort = proplists:get_value(port, Fields),
            OtherAuth = case proplists:get_bool(client_cert_auth, Fields) of
                            true -> client_cert_auth;
                            false ->
                                User = proplists:get_value(user, Fields),
                                Pswd = proplists:get_value(password, Fields),
                                {basic_auth, User, Pswd}
                        end,
            HiddenAuth = ?HIDE(OtherAuth),
            Services = proplists:get_value(services, Fields),
            Hostname = proplists:get_value(new_node_hostname, Fields),
            handle_join_tail(Req, OtherScheme, OtherHost, OtherPort, HiddenAuth,
                             Services, Hostname)
    end.

handle_join_tail(Req, OtherScheme, OtherHost, OtherPort, HiddenAuth,
                 Services, Hostname) ->
    process_flag(trap_exit, true),
    RV = case ns_cluster:check_host_port_connectivity(OtherHost, OtherPort) of
             {ok, MyIP, AFamily} ->
                 Host =
                    case Hostname of
                        undefined ->
                            {MyPList} =
                                menelaus_web_node:build_full_node_info(
                                  {ip, MyIP}, node()),
                            HostnamePort =
                                binary_to_list(misc:expect_prop_value(hostname,
                                                                      MyPList)),
                            [H, _] = string:split(HostnamePort, ":", trailing),
                            H;
                        H -> H
                    end,

                 NodeURL = build_node_url(OtherScheme, Host),
                 call_add_node(OtherScheme, OtherHost, OtherPort,
                               HiddenAuth, AFamily, NodeURL,
                               Services);
             {error, Reason} ->
                    M = case ns_error_messages:connection_error_message(
                               Reason, OtherHost, OtherPort) of
                            undefined -> io_lib:format("~p", [Reason]);
                            Msg -> Msg
                        end,
                    URL = menelaus_rest:rest_url(OtherHost, OtherPort, "",
                                                 OtherScheme),
                    ReasonStr = io_lib:format("Failed to connect to ~s. ~s",
                                              [URL, M]),
                    {error, host_connectivity, iolist_to_binary(ReasonStr)}
         end,

    case RV of
        {ok, _} ->
            reply(Req, 200);
        {client_error, JSON} ->
            reply_json(Req, JSON, 400);
        {error, _What, Message} ->
            reply_json(Req, [Message], 400)
    end,
    exit(normal).

build_node_url(Scheme, Host) ->
    Port = case Scheme of
               http -> service_ports:get_port(rest_port);
               https -> service_ports:get_port(ssl_rest_port)
           end,
    HostWBrackets = misc:maybe_add_brackets(Host),
    URL = io_lib:format("~p://~s:~b", [Scheme, HostWBrackets, Port]),
    lists:flatten(URL).

call_add_node(OtherScheme, OtherHost, OtherPort, HiddenAuth, AFamily,
              ThisNodeURL, Services) ->

    IsClientCertAuthMandatory =
        (ns_ssl_services_setup:client_cert_auth_state() =:= "mandatory"),

    BasePayload = [{<<"hostname">>, list_to_binary(ThisNodeURL)}] ++
                   case IsClientCertAuthMandatory of
                       true ->
                           %% Letting the-cluster-node know that it should use
                           %% client cert for authentication when adding this
                           %% node
                           [{<<"clientCertAuth">>, true}];
                       false ->
                           [{<<"user">>, []},
                            {<<"password">>, []}]
                   end,

    {Payload, Endpoint} =
        case Services =:= ns_cluster_membership:default_services() of
            true ->
                {BasePayload, "/controller/addNode"};
            false ->
                ServicesStr =
                    string:join([erlang:atom_to_list(S) || S <- Services], ","),
                SVCPayload = [{"services", ServicesStr} | BasePayload],
                {SVCPayload, "/controller/addNodeV2"}
        end,

    GeneratedCerts = ns_server_cert:this_node_uses_self_generated_certs(
                       ns_config:latest()),
    Options = [{connect_options, [AFamily]},
               {server_verification, not GeneratedCerts},
               {timeout, ns_cluster:add_node_timeout()}],

    Res = menelaus_rest:json_request_hilevel(
            post,
            {OtherScheme, OtherHost, OtherPort, Endpoint,
             "application/x-www-form-urlencoded",
             mochiweb_util:urlencode(Payload)},
            HiddenAuth, Options),
    case Res of
        {error, rest_error, _M, {bad_status, 404, _Msg}} ->
            NewMsg = <<"Node attempting to join an older cluster. Some of the "
                       "selected services are not available.">>,
            {error, rest_error, NewMsg};
        {error, rest_error, M,
         {error, {{tls_alert, {certificate_required, _}}, _}}} ->
            Msg = io_lib:format("Node being added requires per-node client "
                                "certificate when client certificate "
                                "authentication is set to mandatory. ~s", [M]),
            {error, rest_error, iolist_to_binary(Msg)};
        {error, rest_error, M,
            {error, {{tls_alert, {unknown_ca, _}} = E, _}}} ->
            {error, rest_error, ns_error_messages:engage_cluster_error(
                                  {engage_cluster_failed,
                                   {"new node", E, M, node(), {}}})};
        {error, rest_error, M, _} ->
            {error, rest_error, M};
        Other -> Other
    end.

%% waits till only one node is left in cluster
do_eject_myself_rec(0, _) ->
    exit(self_eject_failed);
do_eject_myself_rec(IterationsLeft, Period) ->
    MySelf = node(),
    case ns_node_disco:nodes_actual() of
        [MySelf] -> ok;
        _ ->
            timer:sleep(Period),
            do_eject_myself_rec(IterationsLeft-1, Period)
    end.

do_eject_myself() ->
    ns_cluster:leave(),
    do_eject_myself_rec(10, 250).

handle_eject_post(Req) ->
    PostArgs = mochiweb_request:parse_post(Req),
    %
    % either Eject a running node, or eject a node which is down.
    %
    % request is a urlencoded form with otpNode
    %
    % responses are 200 when complete
    %               401 if creds were not supplied and are required
    %               403 if creds were supplied and are incorrect
    %               400 if the node to be ejected doesn't exist
    %
    OtpNodeStr = case proplists:get_value("otpNode", PostArgs) of
                     undefined -> undefined;
                     "Self" -> atom_to_list(node());
                     X -> X
                 end,
    case OtpNodeStr of
        undefined ->
            reply_text(Req, "Bad Request\n", 400);
        _ ->
            OtpNode = list_to_atom(OtpNodeStr),
            case ns_cluster_membership:get_cluster_membership(OtpNode) of
                active ->
                    reply_text(Req, "Cannot remove active server.\n", 400);
                _ ->
                    do_handle_eject_post(Req, OtpNode)
            end
    end.

try_leave_node(Req, OtpNode) ->
    try
        ns_cluster:leave(OtpNode),
        ?MENELAUS_WEB_LOG(?NODE_EJECTED,
                          "Node ejected: ~p from node: ~p",
                          [OtpNode, erlang:node()]),
        ns_audit:remove_node(Req, OtpNode),
        reply(Req, 200)
    catch T:E:Stack ->
        ?log_error("Leave failed with ~p", [{T,E,Stack}]),
        Msg = <<"Unable to leave cluster">>,
        reply_text(Req, Msg, 503)
    end.

do_handle_eject_post(Req, OtpNode) ->
    %% Verify that the server lists are consistent with cluster membership
    %% states in all buckets.
    lists:foreach(
      fun ({Bucket, BucketConfig}) ->
              ok = ns_janitor:check_server_list(Bucket, BucketConfig)
      end, ns_bucket:get_buckets()),

    case OtpNode =:= node() of
        true ->
            do_eject_myself(),
            ns_audit:remove_node(Req, node()),
            reply(Req, 200);
        false ->
            case lists:member(OtpNode, ns_node_disco:nodes_wanted()) of
                true ->
                    try_leave_node(Req, OtpNode);
                false ->
                                                % Node doesn't exist.
                    ?MENELAUS_WEB_LOG(0018, "Request to eject nonexistant "
                                      "server failed.  Requested node: ~p",
                                      [OtpNode]),
                    reply_text(Req, "Server does not exist.\n", 400)
            end
    end.

setup_services_validators() ->
    [validator:boolean(setDefaultMemQuotas, _),
     validator:default(setDefaultMemQuotas, false, _),
     validator:required(services, _),
     validator:validate(
      fun (ServicesString) ->
          case ns_config_auth:is_system_provisioned() of
              true ->
                  {error, "cannot change node services after cluster is "
                   "provisioned"};
              false ->
                  case parse_validate_services_list(ServicesString) of
                      {ok, Svcs} ->
                            case lists:member(kv, Svcs) of
                                true ->
                                    case ns_cluster:enforce_topology_limitation(
                                           Svcs) of
                                        ok -> {value, Svcs};
                                        Error -> Error
                                    end;
                                false ->
                                    {error, "cannot setup first cluster "
                                     "node without kv service"}
                            end;
                      {error, Msg} -> {error, Msg}
                  end
          end
      end, services, _)].

setup_services_check_quota(Services, SetDefaultMemQuotas) ->
    Quotas = case SetDefaultMemQuotas of
                 false ->
                     lists:map(
                       fun(Service) ->
                               {ok, Quota} = memory_quota:get_quota(Service),
                               {Service, Quota}
                       end, memory_quota:aware_services());
                 true ->
                     do_update_with_default_quotas(
                       memory_quota:default_quotas(
                         Services,
                         cluster_compat_mode:get_compat_version()))
             end,

    case Quotas of
        {error, _Msg} = E ->
            E;
        _ ->
            case memory_quota:check_this_node_quotas(Services, Quotas) of
                ok ->
                    ok;
                {error, {total_quota_too_high, _, TotalQuota, MaxAllowed}} ->
                    Msg = io_lib:format(
                            "insufficient memory to satisfy memory quota "
                            "for the services "
                            "(requested quota is ~bMB, "
                            "maximum allowed quota for the node is ~bMB)",
                            [TotalQuota, MaxAllowed]),
                    {error, iolist_to_binary(Msg)}
            end
    end.

do_update_with_default_quotas(Quotas) ->
    do_update_with_default_quotas(Quotas, 10).

do_update_with_default_quotas(_, 0) ->
    {error, <<"Could not update the config with default memory quotas">>};
do_update_with_default_quotas(Quotas, RetriesLeft) ->
    case memory_quota:set_quotas(ns_config:get(), Quotas) of
        ok ->
            Quotas;
        retry_needed ->
            do_update_with_default_quotas(Quotas, RetriesLeft - 1)
    end.

handle_setup_services_post(Req) ->
    validator:handle(
      fun (Props) ->
          case do_setup_services_post(Req, Props) of
              ok -> reply(Req, 200);
              {error, Error} -> reply_json(Req, [Error], 400)
          end
      end, Req, form, setup_services_validators()).

do_setup_services_post(Req, Props) ->
    Services = proplists:get_value(services, Props),
    SetDefaultMemQuotas = proplists:get_value(setDefaultMemQuotas, Props),
    case setup_services_check_quota(Services, SetDefaultMemQuotas) of
        ok ->
            {ok, _} = chronicle_kv:set(kv, {node, node(), services}, Services),
            ns_audit:setup_node_services(Req, node(), Services),
            ok;
        {error, Error} ->
            {error, Error}
    end.

validate_add_node_params(User, Password) ->
    Candidates =
        case lists:member(undefined, [User, Password]) of
            true -> [<<"Missing required parameter.">>];
            _ -> [case {User, Password} of
                      {[], []} -> true;
                      {[_Head | _], [_PasswordHead | _]} -> true;
                      {[], [_PasswordHead | _]} ->
                          <<"If a username is not specified, a password must "
                            "not be supplied.">>;
                      _ -> <<"A password must be supplied.">>
                  end]
        end,
    lists:filter(fun (E) -> E =/= true end, Candidates).

malformed_url_message(Hostname) ->
    list_to_binary(
      io_lib:format(
        "Malformed URL ~s; if using IPv6, enclose in square brackets",
        [Hostname])).

%% erlang R15B03 has http_uri:parse/2 that does the job
%% reimplement after support of R14B04 will be dropped
parse_hostname(Hostname, DefaultScheme) ->
    do_parse_hostname(string:trim(Hostname), DefaultScheme).

do_parse_hostname([], _) ->
    throw({error, [<<"Hostname is required.">>]});

do_parse_hostname(Hostname, DefaultScheme) ->
    WithScheme = case string:str(Hostname, "://") of
                     0 -> atom_to_list(DefaultScheme) ++ "://" ++ Hostname;
                     _ -> Hostname
                 end,
    SchemeVer = fun (<<"http">>) -> valid;
                    (<<"https">>) -> valid;
                    (S) -> {error, {invalid_scheme, S}}
                end,
    case misc:parse_url(WithScheme, [{scheme_validation_fun, SchemeVer},
                                     {ipv6_host_with_brackets, false},
                                     {scheme_defaults, [{<<"http">>, 8091},
                                                        {<<"https">>, 18091}]},
                                     {return, string}]) of
        {ok, #{scheme := SchemeStr, host := Host, port := Port, path := "/"}} ->
            Scheme = list_to_atom(SchemeStr),
            {Scheme, Host, parse_validate_port_number(integer_to_list(Port))};
        {error, {invalid_scheme, S}} ->
            throw({error, [list_to_binary("Unsupported protocol " ++ S)]});
        {error, _} ->
            throw({error, [malformed_url_message(Hostname)]})
    end.

handle_add_node(Req) ->
    do_handle_add_node(Req, undefined).

handle_add_node_to_group(GroupUUIDString, Req) ->
    do_handle_add_node(Req, list_to_binary(GroupUUIDString)).

%% Force reset of node A after it has gone through unsafe failover. The other
%% nodes aren't aware of node A, and node A's state must be wiped out before it
%% can be added back to the cluster. A's state, data are lost after hard reset.
%% If hard reset is run inadvertently on an active node, the node will have to
%% be failed over (unsafe failover).
handle_hard_reset_node(Req) ->
    ok = ns_cluster:hard_reset_init(),
    %% After the leave marker has been written, the hard reset will proceed to
    %% completion in the event of a timeout or ns_cluster process crash. If
    %% ns_cluster crashes, it processes the leave/start path (if the markers are
    %% found). So, audit the removal of the node without waiting for completion.
    ns_audit:remove_node(Req, node()),
    menelaus_util:survive_web_server_restart(
      fun() ->
              try
                  ns_cluster:hard_reset()
              catch exit:{timeout, E}:Stack ->
                      ?log_error("Hard reset exit timeout ~p", [{E,Stack}]),
                      reply_text(Req, "Request timed out\n", 500)
              end,
              reply(Req, 200)
      end).

add_node_error_code(cannot_acquire_lock) ->
    503;
add_node_error_code(unknown_group) ->
    404;
add_node_error_code(_) ->
    400.

do_handle_add_node(Req, GroupUUID) ->
    %% parameter example:
    %%    hostname=epsilon.local, user=Administrator, password=asd!23
    %% parameter example: hostname=epsilon.local, clientCertAuth=true
    Params = mochiweb_request:parse_post(Req),
    Parsed = case parse_join_cluster_params(Params, false) of
                 {ok, ParsedKV} ->
                     U = proplists:get_value(user, ParsedKV),
                     P = proplists:get_value(password, ParsedKV),
                     case proplists:get_bool(client_cert_auth, ParsedKV) orelse
                          validate_add_node_params(U, P) of
                         true ->
                             {ok, ParsedKV};
                         [] ->
                             {ok, ParsedKV};
                         CredErrors ->
                             {errors, CredErrors}
                     end;
                 {errors, ParseErrors} ->
                     {errors, ParseErrors}
             end,

    case Parsed of
        {ok, KV} ->
            {Auth, AuditUser} =
                case proplists:get_bool(client_cert_auth, KV) of
                    true -> {client_cert_auth, "<client_cert>"};
                    false ->
                        User = proplists:get_value(user, KV),
                        Password = proplists:get_value(password, KV),
                        {{basic_auth, User, Password}, User}
                end,
            Scheme = proplists:get_value(scheme, KV),
            Hostname = proplists:get_value(host, KV),
            Port = proplists:get_value(port, KV),
            Services = proplists:get_value(services, KV),

            menelaus_util:survive_web_server_restart(
              fun () ->
                  case ns_cluster:add_node_to_group(
                         Scheme, Hostname, Port,
                         ?HIDE(Auth),
                         GroupUUID,
                         Services) of
                      {ok, OtpNode} ->
                          ns_audit:add_node(Req, Hostname, Port, AuditUser,
                                            GroupUUID, Services, OtpNode),
                          ServicesJSON =
                              [ns_cluster_membership:json_service_name(S)
                               || S <- Services],
                          event_log:add_log(
                            node_join_success,
                            [{node_added, list_to_binary(Hostname)},
                             {node_services, ServicesJSON}]),

                          reply_json(Req, {[{otpNode, OtpNode}]}, 200);
                      {error, What, Message} ->
                          reply_json(Req, [Message], add_node_error_code(What))
                  end
              end);
        {errors, ErrorList} ->
            reply_json(Req, ErrorList, 400)
    end.

validate_node(NodeArg) ->
    Node = (catch list_to_existing_atom(NodeArg)),
    case Node of
        undefined ->
            {error, "No server specified."};
        _ when not is_atom(Node) ->
            {error, "Unknown server given."};
        _ ->
            {ok, Node}
    end.

parse_graceful_failover_args(Req) ->
    Params = mochiweb_request:parse_post(Req),
    parse_otp_nodes(Params).

parse_otp_nodes(Params) ->
    OtpNodes = proplists:lookup_all("otpNode", Params),
    {Good, Bad} = lists:foldl(
                    fun ({_Key, Val}, {G, B}) ->
                            case validate_node(Val) of
                                {ok, Node} -> {[Node | G], B};
                                _ -> {G, [Val | B]}
                            end
                    end, {[], []}, OtpNodes),
    case Bad of
        [] ->
            case Good of
                [] ->
                    {error, "No server specified."};
                _ ->
                    %% Remove duplicates.
                    {ok, lists:usort(Good)}
            end;
        _ ->
            {error, io_lib:format("Unknown server given: ~p", [Bad])}
    end.

parse_hard_failover_args(Req) ->
    Params = mochiweb_request:parse_post(Req),
    case parse_otp_nodes(Params) of
        {ok, Nodes} ->
            AllowUnsafe = proplists:get_value("allowUnsafe", Params),
            {ok, Nodes, AllowUnsafe =:= "true"};
        Error ->
            Error
    end.

failover_reply({incompatible_with_previous, Nodes}, Req) ->
    Hostnames = [binary_to_list(H) ||
                    {_, H} <- menelaus_web_node:get_hostnames(Req, Nodes)],
    {400, io_lib:format("Failover must include the following nodes: ~s.",
                        [string:join(Hostnames, ", ")])};
failover_reply(RV, _) ->
    failover_reply(RV).

failover_reply(ok) ->
    200;
failover_reply(in_progress) ->
    failover_reply(rebalance_running);
failover_reply(rebalance_running) ->
    {503, "Rebalance running."};
failover_reply(in_recovery) ->
    {503, "Cluster is in recovery mode."};
failover_reply(orchestration_unsafe) ->
    %% 504 is a stretch here of course, but we do
    %% need to convey the information to the client somehow.
    {504, "Cannot safely perform a failover at the moment"};
failover_reply(config_sync_failed) ->
    {500, "Failed to synchronize config to other nodes"};
failover_reply(quorum_lost) ->
    {500, "Cannot safely perform a failover at the moment"};
failover_reply({config_sync_failed, _}) ->
    failover_reply(config_sync_failed);
failover_reply(last_node) ->
    {400, "Last active node cannot be failed over."};
failover_reply({last_node_for_bucket, B}) ->
    {400, io_lib:format("Last server for bucket ~p cannot be failed over.",
                       [B])};
failover_reply(not_graceful) ->
    {400, "Failover cannot be done gracefully (would lose vbuckets)."};
failover_reply(non_kv_node) ->
    {400, "Failover cannot be done gracefully for a node without data service."
     " Use hard failover."};
failover_reply(unknown_node) ->
    {400, "Unknown server given."};
failover_reply(inactive_node) ->
    {400, "Inactive server given."};
failover_reply(stopped_by_user) ->
    {409, "Stopped by user."};
failover_reply({not_in_peers, Node, _ClusterNodes}) ->
    {400, io_lib:format("~p which is orchestrating the failover must be "
                        "one of the nodes that survive the failover", [Node])};
failover_reply({aborted, Map}) ->
    Format = [{failed_peers, "Failover could not be processed on nodes ~p"},
              {diverged_peers, "Failover is unsafe on nodes ~p due to "
                               "diverged histories"}],
    Errs = maps:fold(
             fun (K, V, Acc) ->
                     F = proplists:get_value(K, Format),
                     Err = lists:flatten(io_lib:format(F, [V])),
                     case Acc of
                         [] -> Err;
                         _ -> Acc ++ [" and "] ++ Err
                     end
             end, [], Map),
    {503, lists:flatten(Errs)};
failover_reply(Other) ->
    {500, io_lib:format("Unexpected server error: ~p", [Other])}.

failover_audit_and_reply(RV, Req, Nodes, Type) ->
    case failover_reply(RV, Req) of
        200 ->
            ns_audit:failover_nodes(Req, Nodes, Type),
            reply(Req, 200);
        {Code, Message} ->
            reply_text(Req, Message, Code)
    end.

%% When the 1st Param is true the failover is done asynchronously;
%% when false, the failover is done synchronously.

handle_start_hard_failover(true, Req) ->
    do_handle_start_hard_failover(Req, fun ns_orchestrator:start_failover/2);
handle_start_hard_failover(false, Req) ->
    do_handle_start_hard_failover(Req, fun ns_orchestrator:failover/2).

do_handle_start_hard_failover(Req, FailoverBody) ->
    case parse_hard_failover_args(Req) of
        {ok, Nodes, AllowUnsafe} ->
            failover_audit_and_reply(
              FailoverBody(Nodes, AllowUnsafe),
              Req, Nodes, hard);
        {error, ErrorMsg} ->
            reply_text(Req, ErrorMsg, 400)
    end.

handle_start_graceful_failover(Req) ->
    case parse_graceful_failover_args(Req) of
        {ok, Nodes} ->
            failover_audit_and_reply(
              ns_orchestrator:start_graceful_failover(Nodes),
              Req, Nodes, graceful);
        {error, ErrorMsg} ->
            reply_text(Req, ErrorMsg, 400)
    end.

parse_list_param(Param, Params, Default) ->
    case proplists:get_value(Param, Params) of
        undefined ->
            Default;
        Str ->
            string:tokens(Str, ",")
    end.

handle_rebalance(Req) ->
    Params = mochiweb_request:parse_post(Req),
    try parse_rebalance_params(Params) of
        Parsed -> do_handle_rebalance(Req, Parsed)
    catch Error when is_atom(Error) ->
            reply_json(Req, {[{Error, 1}]}, 400);
          Error when is_list(Error) ->
            reply_text(Req, Error, 400)
    end.

parse_rebalance_params(Params) ->
    KnownNodesS = parse_list_param("knownNodes", Params, []),
    KnownNodesS =/= [] orelse throw(empty_known_nodes),

    EjectedNodesS = parse_list_param("ejectedNodes", Params, []),
    UnknownNodes = [S || S <- EjectedNodesS ++ KnownNodesS,
                         try list_to_existing_atom(S), false
                         catch error:badarg -> true end],
    UnknownNodes =:= [] orelse throw(mismatch),

    DeltaRecoveryBuckets =
        parse_list_param("deltaRecoveryBuckets", Params, all),

    DefragmentZones =
        case parse_list_param("defragmentZones", Params, undefined) of
            undefined ->
                [];
            DefragmentZonesS ->
                bucket_placer:is_enabled() orelse
                    throw("Option defragmentZones requires bucket placer to be "
                          "enabled"),

                DefragmentZonesB = [list_to_binary(Z) || Z <- DefragmentZonesS],
                ZoneNames = [proplists:get_value(name, G) ||
                                G <- ns_cluster_membership:server_groups()],
                DefragmentZonesB -- ZoneNames =:= [] orelse
                    throw("Nonexistent groups in defragmentZones list"),
                DefragmentZonesB
        end,

    Services = case proplists:get_value("services", Params) of
                   undefined ->
                       all;
                   ServicesList ->
                       menelaus_util:assert_is_enterprise("services"),
                       menelaus_util:assert_profile_flag(
                         allow_per_service_rebalance, "services"),
                       case parse_validate_services_list(ServicesList) of
                           {ok, S} ->
                               S;
                           {error, Error} ->
                               throw(Error)
                       end
               end,

    [[list_to_existing_atom(N) || N <- KnownNodesS],
     [list_to_existing_atom(N) || N <- EjectedNodesS],
     DeltaRecoveryBuckets, DefragmentZones, Services].

do_handle_rebalance(Req, [KnownNodes, EjectedNodes, DeltaRecoveryBuckets,
                          DefragmentZones, Services] = Params) ->
    ?log_info("Starting rebalance with params ~p", [Params]),
    case rebalance:start(KnownNodes, EjectedNodes, DeltaRecoveryBuckets,
                         DefragmentZones, Services) of
        already_balanced ->
            reply(Req, 200);
        in_progress ->
            reply(Req, 200);
        nodes_mismatch ->
            reply_json(Req, {[{mismatch, 1}]}, 400);
        delta_recovery_not_possible ->
            reply_json(Req, {[{deltaRecoveryNotPossible, 1}]}, 400);
        no_active_nodes_left ->
            reply_text(Req, "No active nodes left", 400);
        in_recovery ->
            reply_text(Req, "Cluster is in recovery mode.", 503);
        in_bucket_hibernation ->
            reply_text(Req, "Cannot rebalance when another bucket is "
                            "pausing/resuming.", 503);
        no_kv_nodes_left ->
            reply_json(Req, {[{noKVNodesLeft, 1}]}, 400);
        %% pre-7.6 responses
        ok ->
            ns_audit:rebalance_initiated(Req, KnownNodes, EjectedNodes,
                                         DeltaRecoveryBuckets),
            reply(Req, 200);
        %% 7.6 and next versions response
        {ok, RebalanceId} ->
            ns_audit:rebalance_initiated(Req, KnownNodes, EjectedNodes,
                                         DeltaRecoveryBuckets),
            reply_json(Req, {[{rebalance_id, RebalanceId}]}, 200);
        OtherError ->
            reply_json(Req, {[OtherError]}, 400)
    end.

handle_rebalance_progress(_PoolId, Req) ->
    case rebalance:progress() of
        {running, PerNode} ->
            PerNodeJson = [{atom_to_binary(Node, latin1),
                            {[{progress, Progress}]}}
                           || {Node, Progress} <- PerNode],
            Status = [{status, <<"running">>} | PerNodeJson],
            reply_json(Req, {Status}, 200);
        not_running ->
            Status = [{status, <<"none">>} | get_rebalance_error()],
            reply_json(Req, {Status}, 200);
        {error, timeout} = Err ->
            reply_json(Req, {[Err]}, 503)
    end.

handle_current_rebalance_report(Req) ->
    RV = case rebalance:running() of
             true ->
                 ns_rebalance_observer:get_current_rebalance_report();
             false ->
                 not_running
         end,
    case RV of
        {ok, Report} ->
            reply_json(Req, Report, 200);
        not_running ->
            reply_json(Req, {[{is_rebalancing, false}]}, 200);
        {error, timeout} = Err ->
            reply_json(Req, {[Err]}, 503)
    end.

get_rebalance_error() ->
    [{errorMessage, iolist_to_binary(ErrorMessage)} ||
        {none, ErrorMessage} <- [rebalance:status()]].

handle_stop_rebalance(Req) ->
    validator:handle(handle_stop_rebalance(Req, _),
                     Req, form, [validator:boolean(allowUnsafe, _)]).

handle_stop_rebalance(Req, Params) ->
    AllowUnsafe = proplists:get_value(allowUnsafe, Params, false),
    case rebalance:stop(AllowUnsafe) of
        unsafe ->
            reply_text(Req,
                       "Cannot communicate to the orchestrator node. "
                       "Stopping rebalance is unsafe. "
                       "This can be overriden by passing allowUnsafe=true "
                       "in the POST form.",
                       504);
        _ ->
            reply(Req, 200)
    end.

handle_re_add_node(Req) ->
    Params = mochiweb_request:parse_post(Req),
    do_handle_set_recovery_type(Req, full, Params).

handle_re_failover(Req) ->
    Params = mochiweb_request:parse_post(Req),
    NodeString = proplists:get_value("otpNode", Params, "undefined"),
    case ns_cluster_membership:re_failover(NodeString) of
        {ok, _} ->
            ns_audit:failover_nodes(Req, [list_to_existing_atom(NodeString)],
                                    cancel_recovery),
            reply(Req, 200);
        not_possible ->
            reply(Req, 400)
    end.

serve_node_services(Req) ->
    {_Rev, _RevEpoch, Bin, _NodesExtHash} =
        bucket_info_cache:build_node_services(),
    reply_ok(Req, "application/json", Bin).

serve_node_services_streaming(Req) ->
    handle_streaming(
      fun (_, _UpdateID) ->
              {_Rev, _RevEpoc, V, _NodesExtHash} =
                bucket_info_cache:build_node_services(),
              {just_write, {write, V}}
      end, Req).

decode_recovery_type("delta") ->
    delta;
decode_recovery_type("full") ->
    full;
decode_recovery_type(_) ->
    undefined.

handle_set_recovery_type(Req) ->
    Params = mochiweb_request:parse_post(Req),
    Type = decode_recovery_type(proplists:get_value("recoveryType", Params)),
    do_handle_set_recovery_type(Req, Type, Params).

do_handle_set_recovery_type(Req, Type, Params) ->
    NodeStr = proplists:get_value("otpNode", Params),

    Node = try
               list_to_existing_atom(NodeStr)
           catch
               error:badarg ->
                   undefined
           end,

    OtpNodeErrorMsg =
        <<"invalid node name or node can't be used for delta recovery">>,

    NodeSvcs = ns_cluster_membership:node_services(Node),
    NotKVIndex = not lists:member(kv, NodeSvcs) andalso
        not lists:member(index, NodeSvcs),

    Errors =
        lists:flatten(
          [case Type of
               undefined ->
                   [{recoveryType,
                     <<"recovery type must be either 'delta' or 'full'">>}];
               _ ->
                   []
           end,

           case Node of
               undefined ->
                   [{otpNode, OtpNodeErrorMsg}];
               _ ->
                   []
           end,

           case Type =:= delta andalso NotKVIndex of
               true ->
                   [{otpNode, OtpNodeErrorMsg}];
               false ->
                   []
           end]),

    case Errors of
        [] ->
            case ns_cluster_membership:update_recovery_type(Node, Type) of
                {ok, _} ->
                    ns_audit:enter_node_recovery(Req, Node, Type),
                    reply_json(Req, [], 200);
                bad_node ->
                    reply_json(Req, {[{otpNode, OtpNodeErrorMsg}]}, 400)
            end;
        _ ->
            reply_json(Req, {Errors}, 400)
    end.


-ifdef(TEST).
parse_validate_services_list_test() ->
    meck:new(cluster_compat_mode, [passthrough]),
    meck:expect(cluster_compat_mode, is_enterprise, fun () -> true end),
    meck:new(config_profile, [passthrough]),
    meck:expect(config_profile, get,
                fun () ->
                        ?DEFAULT_EMPTY_PROFILE_FOR_TESTS
                end),
    {error, _} = parse_validate_services_list(""),
    ?assertEqual({ok, [index, kv, n1ql]},
                 parse_validate_services_list("n1ql,kv,index")),
    {ok, [kv]} = parse_validate_services_list("kv"),
    {error, _} = parse_validate_services_list("n1ql,kv,s"),
    ?assertMatch({error, _}, parse_validate_services_list("neeql,kv")),
    meck:unload(config_profile),
    meck:unload(cluster_compat_mode).

hostname_parsing_test() ->
    Urls = [" \t\r\nhttp://host:1025\n\r\t ",
            "http://host:100",
            "http://host:100000",
            "hTTp://host:8000",
            "ftp://host:600",
            "http://host",
            "127.0.0.1:6000",
            "host:port",
            "aaa:bb:cc",
            " \t\r\nhost\n",
            " ",
            "https://host:2000",
            "[::1]",
            "http://::1:10000",
            "http://[::1]:10000"],

    ExpectedResults = [{http, "host",1025},
                       {error, [<<"The port number must be greater than 1023 "
                                  "and less than 65536.">>]},
                       {error, [<<"The port number must be greater than 1023 "
                                  "and less than 65536.">>]},
                       {http, "host", 8000},
                       {error, [<<"Unsupported protocol ftp">>]},
                       {http, "host", 8091},
                       {https, "127.0.0.1", 6000},
                       {error,
                        [<<"Malformed URL host:port; "
                           "if using IPv6, enclose in square brackets">>]},
                       {error, [<<"Malformed URL aaa:bb:cc; "
                           "if using IPv6, enclose in square brackets">>]},
                       {https, "host", 18091},
                       {error, [<<"Hostname is required.">>]},
                       {https, "host", 2000},
                       {https, "::1", 18091},
                       {error, [<<"Malformed URL http://::1:10000; "
                           "if using IPv6, enclose in square brackets">>]},
                       {http, "::1", 10000}],

    Results = [(catch parse_hostname(X, https)) || X <- Urls],

    ?assertEqual(ExpectedResults, Results),
    ok.
-endif.
