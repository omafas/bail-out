%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2018 Couchbase, Inc.
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
-module(json_rpc_connection_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([start_link/0, handle_rpc_connect/1, reannounce/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{json_rpc_connection, {json_rpc_connection, start_link, []},
            temporary, brutal_kill, worker, dynamic}]}}.

handle_rpc_connect(Req) ->
    "/" ++ Path = mochiweb_request:get(path, Req),
    Sock = mochiweb_request:get(socket, Req),
    menelaus_util:reply(Req, 200),
    ok = start_handler(Path, Sock),
    erlang:exit(normal).

start_handler(Label, Sock) ->
    Ref = make_ref(),
    Starter = self(),

    GetSocket =
        fun () ->
                MRef = erlang:monitor(process, Starter),

                receive
                    {Ref, S} ->
                        erlang:demonitor(MRef, [flush]),
                        S;
                    {'DOWN', MRef, _, _, Reason} ->
                        ?log_error("Starter process ~p for json rpc "
                                   "connection ~p died unexpectedly: ~p",
                                   [Starter, Label, Reason]),
                        exit({starter_died, Label, Starter, Reason})
                after
                    5000 ->
                        exit(sock_recv_timeout)
                end
        end,

    {ok, Pid} = supervisor:start_child(?MODULE, [Label, GetSocket]),
    ok = gen_tcp:controlling_process(Sock, Pid),
    Pid ! {Ref, Sock},
    ok.

reannounce() ->
    lists:foreach(
      fun ({_, Pid, _, _}) ->
              ok = json_rpc_connection:reannounce(Pid)
      end, supervisor:which_children(?MODULE)).
