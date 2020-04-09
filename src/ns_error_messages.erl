%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2018 Couchbase, Inc.
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
-module(ns_error_messages).

-include("cut.hrl").

-export([decode_json_response_error/3,
         connection_error_message/3,
         engage_cluster_json_error/1,
         bad_memory_size_error/3,
         incompatible_cluster_version_error/3,
         too_old_version_error/2,
         verify_otp_connectivity_port_error/3,
         verify_otp_connectivity_connection_error/4,
         unsupported_services_error/2,
         topology_limitation_error/1,
         cert_validation_error_message/1,
         reload_node_certificate_error/1,
         node_certificate_warning/1,
         not_absolute_path/1,
         empty_param/1,
         preview_cluster_join_error/0]).

-spec connection_error_message(term(), string(), string() | integer()) -> binary() | undefined.
connection_error_message({tls_alert, "bad record mac"}, Host, Port) ->
    list_to_binary(io_lib:format("Failed to establish TLS connection to ~s:~w. "
                                 "Please check that you are connecting to a "
                                 "TLS / HTTPS endpoint.", [Host, Port]));
connection_error_message({tls_alert, M}, Host, Port) ->
    list_to_binary(io_lib:format("Failed to establish TLS connection to ~s:~w: ~p", [Host, Port, M]));
connection_error_message({AFamily, nxdomain}, Host, _Port) ->
    list_to_binary(io_lib:format("Unable to resolve ~s address for ~p.  "
                                 "The hostname may be incorrect or not "
                                 "resolvable.",
                                 [misc:afamily2str(AFamily), Host]));
connection_error_message({Error, _}, Host, Port) ->
    connection_error_message(Error, Host, Port);
connection_error_message(nxdomain, Host, _Port) ->
    list_to_binary(io_lib:format("Failed to resolve address for ~p.  "
                                 "The hostname may be incorrect or not resolvable.", [Host]));
connection_error_message(econnrefused, Host, Port) ->
    list_to_binary(io_lib:format("Could not connect to ~p on port ~p.  "
                                 "This could be due to an incorrect host/port combination or a "
                                 "firewall in place between the servers.", [Host, Port]));
connection_error_message(timeout, Host, Port) ->
    list_to_binary(io_lib:format("Timeout connecting to ~p on port ~p.  "
                                 "This could be due to an incorrect host/port combination or a "
                                 "firewall in place between the servers.", [Host, Port]));
connection_error_message("bad certificate", Host, Port) ->
    list_to_binary(io_lib:format("Got certificate mismatch while trying to send https request to ~s:~w",
                                 [Host, Port]));
connection_error_message(_, _, _) -> undefined.

-spec decode_json_response_error({ok, term()} | {error, term()},
                                 atom(),
                                 {atom(), string(), string() | integer(),
                                  string(), string(), iolist()}) ->
                                        %% English error message and nested error
                                        {error, rest_error, binary(), {error, term()} | {bad_status, integer(), string()}}.
decode_json_response_error({ok, {{200 = _StatusCode, _} = _StatusLine,
                               _Headers, _Body} = _Result},
                         _Method, _Request) ->
    %% 200 is not error
    erlang:error(bug);

decode_json_response_error({ok, {{401 = StatusCode, _}, _, Body}},
                         _Method,
                         _Request) ->
    TrimmedBody = string:substr(erlang:binary_to_list(Body), 1, 48),
    M = <<"Authentication failed. Verify username and password.">>,
    {error, rest_error, M, {bad_status, StatusCode, list_to_binary(TrimmedBody)}};

decode_json_response_error({ok, {{StatusCode, _}, _, Body}},
                         Method,
                         {Scheme, Host, Port, Path, _MimeType, _Payload}) ->
    TrimmedBody = string:substr(erlang:binary_to_list(Body), 1, 48),
    RealPort = if is_integer(Port) -> integer_to_list(Port);
                  true -> Port
               end,
    M = list_to_binary(io_lib:format("Got HTTP status ~p from REST call ~p to ~p://~s:~s~s. Body was: ~p",
                                     [StatusCode, Method, Scheme, Host, RealPort, Path, TrimmedBody])),
    {error, rest_error, M, {bad_status, StatusCode, list_to_binary(TrimmedBody)}};

decode_json_response_error({error, Reason} = E,
                           Method,
                           {Scheme, Host, Port, Path, _MimeType, _Payload}) ->
    M = case connection_error_message(Reason, Host, Port) of
            undefined ->
                RealPort = if is_integer(Port) -> integer_to_list(Port);
                              true -> Port
                           end,
                list_to_binary(io_lib:format("Error ~p happened during REST call ~p to ~p://~s:~s~s.",
                                             [Reason, Method, Scheme, Host, RealPort, Path]));
            X -> X
        end,
    {error, rest_error, M, E}.

engage_cluster_json_error(undefined) ->
    <<"Cluster join prepare call returned invalid json.">>;
engage_cluster_json_error({unexpected_json, _Where, Field} = _Exc) ->
    list_to_binary(io_lib:format("Cluster join prepare call returned invalid json. "
                                 "Invalid field is ~s.", [Field])).

bad_memory_size_error(Services0, TotalQuota, MaxQuota) ->
    Services1 = lists:sort(Services0),
    Services = string:join([atom_to_list(S) || S <- Services1], ", "),

    Msg = io_lib:format("This server does not have sufficient memory to "
                        "support requested memory quota. "
                        "Total quota is ~bMB (services: ~s), "
                        "maximum allowed quota for the node is ~bMB.",
                        [TotalQuota, Services, MaxQuota]),
    iolist_to_binary(Msg).

incompatible_cluster_version_error(MyVersion, OtherVersion, OtherNode) ->
    case MyVersion > 1 of
        true ->
            RequiredVersion = [MyVersion div 16#10000,
                               MyVersion rem 16#10000],
            OtherVersionExpand = case OtherVersion > 1 of
                                     true ->
                                         [OtherVersion div 16#10000,
                                          OtherVersion rem 16#10000];
                                     false ->
                                         [1,8]
                                 end,
            list_to_binary(io_lib:format("This node cannot add another node (~p)"
                                         " because of cluster version compatibility mismatch. Cluster works in ~p mode and node only supports ~p",
                                         [OtherNode, RequiredVersion, OtherVersionExpand]));
        false ->
            list_to_binary(io_lib:format("This node cannot add another node (~p)"
                                         " because of cluster version compatibility mismatch (~p =/= ~p).",
                                         [OtherNode, MyVersion, OtherVersion]))
    end.

too_old_version_error(Node, Version) ->
    MinSupported0 = lists:map(fun integer_to_list/1,
                              cluster_compat_mode:min_supported_compat_version()),
    MinSupported = string:join(MinSupported0, "."),
    Msg = io_lib:format("Joining ~s node ~s is not supported. "
                        "Upgrade node to Couchbase Server "
                        "version ~s or greater and retry.",
                        [Version, Node, MinSupported]),
    iolist_to_binary(Msg).

verify_otp_connectivity_port_error(_OtpNode, Host, {error, nxdomain}) ->
    list_to_binary(
      io_lib:format("Failed to resolve address for ~p. The hostname may be "
                    "incorrect or not resolvable.", [Host]));
verify_otp_connectivity_port_error(OtpNode, _Host, _Port) ->
    list_to_binary(io_lib:format("Failed to obtain otp port from erlang port mapper for node ~p."
                                 " This can be network name resolution or firewall problem.", [OtpNode])).

verify_otp_connectivity_connection_error(Reason, OtpNode, Host, Port) ->
    Detail = case connection_error_message(Reason, Host, Port) of
                 undefined -> [];
                 X -> [" ", X]
             end,
    list_to_binary(io_lib:format("Failed to reach otp port ~p for node ~p.~s",
                                 [Port, OtpNode, Detail])).

unsupported_services_error(AvailableServices, RequestedServices) ->
    list_to_binary(io_lib:format("Node doesn't support requested services: ~s. Supported services: ~s",
                                 [services_to_iolist(RequestedServices),
                                  services_to_iolist(AvailableServices)])).

services_to_iolist(Services) ->
    OrderedServices = sort_services(Services),
    ServiceToStr = ns_cluster_membership:user_friendly_service_name(_),
    misc:intersperse([ServiceToStr(S) || S <- OrderedServices], ", ").

sort_services(Services) ->
    Order = [kv, index, n1ql, fts, cbas, eventing],
    Weights = lists:zip(Order, lists:seq(1, length(Order))),
    OrderFun = fun (S1, S2) ->
                   W1 = proplists:get_value(S1, Weights, S1),
                   W2 = proplists:get_value(S2, Weights, S2),
                   W1 =< W2
               end,
    lists:usort(OrderFun, Services).

topology_limitation_error(Combinations) ->
    CombinationsStr = misc:intersperse([[$", services_to_iolist(C), $"] ||
                                           C <- Combinations], ", "),
    Msg = io_lib:format("Unsupported service combination. "
                        "Community edition supports only the following combinations: ~s",
                        [CombinationsStr]),
    iolist_to_binary(Msg).

cert_validation_error_message(empty_cert) ->
    <<"Certificate should not be empty">>;
cert_validation_error_message(not_valid_at_this_time) ->
    <<"Certificate is not valid at this time">>;
cert_validation_error_message(malformed_cert) ->
    <<"Malformed certificate">>;
cert_validation_error_message(too_many_entries) ->
    <<"Only one certificate per request is allowed.">>;
cert_validation_error_message(encrypted_certificate) ->
    <<"Encrypted certificates are not supported.">>;
cert_validation_error_message({invalid_certificate_type, BadType}) ->
    list_to_binary(io_lib:format("Invalid certificate type: ~s", [BadType]));
cert_validation_error_message(already_in_use) ->
    <<"This certificate is already in use">>.

file_read_error(enoent) ->
    "The file does not exist.";
file_read_error(eacces) ->
    "Missing permission for reading the file, or for searching one of the parent directories.";
file_read_error(eisdir) ->
    "The named file is a directory.";
file_read_error(enotdir) ->
    "A component of the file name is not a directory.";
file_read_error(enomem) ->
    "There is not enough memory for the content of the file.";
file_read_error(Reason) ->
    atom_to_list(Reason).

reload_node_certificate_error(no_cluster_ca) ->
    <<"Cluster CA needs to be set before setting node certificate.">>;
reload_node_certificate_error({bad_cert, {invalid_root_issuer, Subject, RootSubject}}) ->
    list_to_binary(io_lib:format("Last certificate of the chain ~p is not issued by the "
                                 "cluster root certificate ~p",
                                 [Subject, RootSubject]));
reload_node_certificate_error({bad_cert, {invalid_issuer, Subject, LastSubject}}) ->
    list_to_binary(io_lib:format("Certificate ~p is not issued by the next certificate in chain ~p",
                                 [Subject, LastSubject]));
reload_node_certificate_error({bad_cert, {Error, Subject}}) ->
    list_to_binary(io_lib:format("Incorrectly configured certificate chain. Error: ~p. Certificate: ~p",
                                 [Error, Subject]));
reload_node_certificate_error({bad_cert, max_path_length_reached}) ->
    <<"The certificate chain is too long">>;
reload_node_certificate_error({bad_chain, Error}) ->
    iolist_to_binary([<<"Incorrectly configured certificate chain. ">>,
                      cert_validation_error_message(Error)]);
reload_node_certificate_error({read_pkey, Path, Reason}) ->
    list_to_binary(io_lib:format("Unable to read private key file ~s. ~s",
                                 [Path, file_read_error(Reason)]));
reload_node_certificate_error({read_chain, Path, Reason}) ->
    list_to_binary(io_lib:format("Unable to read certificate chain file ~s. ~s",
                                 [Path, file_read_error(Reason)]));
reload_node_certificate_error({invalid_pkey, BadType}) ->
    list_to_binary(io_lib:format("Invalid private key type: ~s.",
                                 [BadType]));
reload_node_certificate_error(cert_pkey_mismatch) ->
    <<"Provided certificate doesn't match provided private key">>;
reload_node_certificate_error(encrypted_pkey) ->
    <<"Encrypted keys are not supported">>;
reload_node_certificate_error(too_many_pkey_entries) ->
    <<"Provided private key contains incorrect number of entries">>;
reload_node_certificate_error(malformed_pkey) ->
    <<"Malformed or unsupported private key format">>;
reload_node_certificate_error(n2n_enabled) ->
    <<"Operation requires node-to-node encryption to be disabled">>.

node_certificate_warning(mismatch) ->
    <<"Certificate is not signed with cluster CA.">>;
node_certificate_warning(expired) ->
    <<"Certificate is expired.">>;
node_certificate_warning(expires_soon) ->
    <<"Certificate will expire soon.">>;
node_certificate_warning(self_signed) ->
    <<"Out-of-the-box certificates are self-signed. To further secure your system, you must create new X.509 certificates signed by a trusted CA.">>.

not_absolute_path(Param) ->
    Msg = io_lib:format("An absolute path is required for ~p", [Param]),
    iolist_to_binary(Msg).

empty_param(Param) ->
    iolist_to_binary(io_lib:format("~p cannot contain empty string", [Param])).

preview_cluster_join_error() ->
    <<"Can't join a developer preview cluster">>.
