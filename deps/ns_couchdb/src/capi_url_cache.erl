%% @author Couchbase <info@couchbase.com>
%% @copyright 2021-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(capi_url_cache).

-include("cut.hrl").

-export([start_link/0,
         get_capi_base_url/2,
         get_inner_capi_base_url/2]).

start_link() ->
    work_queue:start_link(?MODULE, fun init/0).

init() ->
    ets:new(?MODULE, [set, named_table]),
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({{_, _, capi_port}, _}) ->
              reset();
          ({{_, _, ssl_capi_port}, _}) ->
              reset();
          (_) ->
              ok
      end),
    reset().

reset() ->
    work_queue:submit_work(?MODULE, ?cut(ets:delete_all_objects(?MODULE))).

compute_url(Node, User, Password) ->
    work_queue:submit_sync_work(
      ?MODULE,
      fun () ->
              case capi_utils:compute_capi_port(Node) of
                  undefined ->
                      ets:insert(?MODULE, {{Node, User, Password},
                                           undefined, false}),
                      undefined;
                  Port ->
                      {RealNode, Schema} = case Node of
                                               {ssl, V} -> {V, <<"https://">>};
                                               _ -> {Node, <<"http://">>}
                                           end,
                      Auth = case {User, Password} of
                                 {undefined, undefined} ->
                                     [];
                                 {_, _} ->
                                     [User, $:, Password, $@]
                             end,

                      H = misc:extract_node_address(RealNode),
                      StorePort = case misc:is_localhost(H) of
                                      true  -> Port;
                                      false -> false
                                  end,
                      HostPort = misc:join_host_port(H, Port),
                      Url = iolist_to_binary([Schema, Auth, HostPort]),
                      ets:insert(?MODULE, {{Node, User, Password}, Url,
                                           StorePort}),
                      Url
              end
      end).

%% maps Node to http://<ip>:<capi-port> as binary
%%
%% NOTE: it's not necessarily suitable for sending outside because ip
%% can be localhost!
get_inner_capi_base_url(Node, Cookie) ->
    User = "%40ns_server",
    Password = atom_to_list(Cookie),
    case ets:lookup(?MODULE, {Node, User, Password}) of
        [] ->
            compute_url(Node, User, Password),
            get_inner_capi_base_url(Node, Cookie);
        [{_, URL, _}] ->
            URL
    end.

get_capi_base_url(Node, LocalAddr) ->
    case ets:lookup(?MODULE, {Node, undefined, undefined}) of
        [] ->
            compute_url(Node, undefined, undefined),
            get_capi_base_url(Node, LocalAddr);
        [{_, URL, false}] ->
            URL;
        [{_, _URL, Port}] ->
            Schema = case Node of
                         {ssl, _} ->
                             <<"https://">>;
                         _ ->
                             <<"http://">>
                     end,
            iolist_to_binary([Schema, LocalAddr, $:, integer_to_list(Port)])
    end.
