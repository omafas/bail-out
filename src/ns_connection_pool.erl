%%
%% The following is largely "stolen" from lhttpc_manager. Original
%% license below.
%%
%%% ----------------------------------------------------------------------------
%%% Copyright (c) 2009, Erlang Training and Consulting Ltd.
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%    * Redistributions of source code must retain the above copyright
%%%      notice, this list of conditions and the following disclaimer.
%%%    * Redistributions in binary form must reproduce the above copyright
%%%      notice, this list of conditions and the following disclaimer in the
%%%      documentation and/or other materials provided with the distribution.
%%%    * Neither the name of Erlang Training and Consulting Ltd. nor the
%%%      names of its contributors may be used to endorse or promote products
%%%      derived from this software without specific prior written permission.
%%%
%%% THIS SOFTWARE IS PROVIDED BY Erlang Training and Consulting Ltd. ''AS IS''
%%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%%% ARE DISCLAIMED. IN NO EVENT SHALL Erlang Training and Consulting Ltd. BE
%%% LIABLE SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
%%% BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
%%% WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
%%% OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
%%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%% ----------------------------------------------------------------------------

%%% @author Oscar Hellström <oscar@hellstrom.st>
%%% @author Filipe David Manana <fdmanana@apache.org>
%%
%% @author Aliaksey Kandratsenka <alk@tut.by> (turned into
%% ns_connection_pool, all bugs are mine)
%%
%% @copyright @copyright 2013-2018 Couchbase, Inc.
%%
%%% @doc Connection manager for the more or less arbitrary protocol tcp sockets.
%%% This gen_server is responsible for keeping track of persistent
%%% connections to HTTP servers.
-module(ns_connection_pool).

-export([start_link/1,
         maybe_take_socket/2,
         put_socket/3,
         remove_socket/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-behaviour(gen_server).

-include("ns_common.hrl").

-record(ns_connection_pool, {
          destinations = dict:new(), % Dest => [Socket]
          sockets = dict:new(), % Socket => {Dest, Timer}
          clients = dict:new(), % Pid => {Dest, MonRef}
          clients_counts = dict:new(), % Dest => integer()
          queues = dict:new(),  % Dest => queue of Froms
          pool_size_per_dest = 5 :: non_neg_integer(),
          timeout = 300000 :: non_neg_integer()
         }).

maybe_take_socket(Server, Dest) ->
    gen_server:call(Server, {socket, self(), Dest}, infinity).

put_socket(Server, Dest, Socket) when is_atom(Server) ->
    case erlang:whereis(Server) of
        Pid when is_pid(Pid) ->
            put_socket(Pid, Dest, Socket)
    end;
put_socket(Server, Dest, Socket) ->
    DoneReq = {done, Dest, Socket},
    case gen_tcp:controlling_process(Socket, Server) of
        ok ->
            ok = gen_server:call(Server, DoneReq, infinity);
        _ ->
            ok
    end.

remove_socket(Server, Dest, Socket) ->
    gen_server:call(Server, {remove, Dest, Socket}, infinity).

-spec start_link([{atom(), non_neg_integer()}]) ->
                        {ok, pid()} | {error, already_started}.
start_link(Options) ->
    case proplists:get_value(name, Options) of
        undefined ->
            gen_server:start_link(?MODULE, Options, []);
        Name ->
            gen_server:start_link({local, Name}, ?MODULE, Options, [])
    end.

%% @hidden
-spec init(any()) -> {ok, #ns_connection_pool{}}.
init(Options) ->
    process_flag(priority, high),
    case lists:member({seed,1}, ssl:module_info(exports)) of
        true ->
            %% Make sure that the ssl random number generator is seeded
            %% This was new in R13 (ssl-3.10.1 in R13B vs. ssl-3.10.0 in R12B-5)
            apply(ssl, seed, [crypto:strong_rand_bytes(255)]);
        false ->
            ok
    end,
    Timeout = proplists:get_value(connection_timeout, Options),
    Size = proplists:get_value(pool_size_per_dest, Options),
    {ok, #ns_connection_pool{timeout = Timeout, pool_size_per_dest = Size}}.

%% @hidden
-spec handle_call(any(), any(), #ns_connection_pool{}) ->
                         {reply, any(), #ns_connection_pool{}}.
handle_call({socket, Pid, Dest}, {Pid, _Ref} = From,
            #ns_connection_pool{pool_size_per_dest = MaxSize} = State) ->
    {Reply0, State2} = find_socket(Dest, Pid, State),
    case Reply0 of
        {ok, _Socket} ->
            State3 = monitor_client(Dest, From, State2),
            {reply, Reply0, State3};
        no_socket ->
            case dest_clients_count(Dest, State2) >= MaxSize of
                true ->
                    {noreply, add_to_queue(Dest, From, State2)};
                false ->
                    {reply, no_socket, monitor_client(Dest, From, State2)}
            end
    end;
handle_call({done, Dest, Socket}, {Pid, _} = From, State) ->
    gen_server:reply(From, ok),
    case find_client(Pid, State) of
        {ok, {Dest, MonRef}, State2} ->
            true = erlang:demonitor(MonRef, [flush]),
            {noreply, deliver_socket(Socket, Dest, State2)};
        error ->
            %% NOTE: we don't expect that to happen often, but it is
            %% in fact possible if connection pool died and was
            %% restarted between taking socket and returning it back.
            case (catch gen_tcp:close(Socket)) of
                ok -> ok;
                CloseErr ->
                    ?log_error("Failed to close unknown socket: ~p", [CloseErr])
            end,
            {noreply, State}
    end;
handle_call({remove, Dest, Socket}, {Pid, _} = From, State) ->
    gen_server:reply(From, ok),
    State1 = remove_socket(Socket, State),

    case find_client(Pid, State1) of
        {ok, {Dest, MonRef}, State2} ->
            true = erlang:demonitor(MonRef, [flush]),
            {noreply, maybe_notify_blocked_client(Dest, State2)};
        error ->
            {noreply, State1}
    end;
handle_call(_, _, State) ->
    {reply, {error, unknown_request}, State}.

%% @hidden
-spec handle_cast(any(), #ns_connection_pool{}) -> {noreply, #ns_connection_pool{}}.
handle_cast(_, State) ->
    {noreply, State}.

%% @hidden
-spec handle_info(any(), #ns_connection_pool{}) -> {noreply, #ns_connection_pool{}}.
handle_info({tcp_closed, Socket}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({ssl_closed, Socket}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({timeout, Socket}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({tcp_error, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({ssl_error, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({tcp, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)}; % got garbage
handle_info({ssl, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)}; % got garbage
handle_info({'DOWN', MonRef, process, Pid, _Reason}, State) ->
    {ok, {Dest, MonRef}, State2} = find_client(Pid, State),
    {noreply, maybe_notify_blocked_client(Dest, State2)};
handle_info(_, State) ->
    {noreply, State}.

%% @hidden
-spec terminate(any(), #ns_connection_pool{}) -> ok.
terminate(_, _State) ->
    ok.

%% @hidden
-spec code_change(any(), #ns_connection_pool{}, any()) -> {'ok', #ns_connection_pool{}}.
code_change(_, State, _) ->
    {ok, State}.

find_socket(Dest, Pid, State) ->
    Dests = State#ns_connection_pool.destinations,
    case dict:find(Dest, Dests) of
        {ok, [Socket | Sockets]} ->
            inet:setopts(Socket, [{active, false}]),
            case gen_tcp:controlling_process(Socket, Pid) of
                ok ->
                    {_, Timer} = dict:fetch(Socket, State#ns_connection_pool.sockets),
                    cancel_timer(Timer, Socket),
                    NewState = State#ns_connection_pool{
                                 destinations = update_dest(Dest, Sockets, Dests),
                                 sockets = dict:erase(Socket, State#ns_connection_pool.sockets)
                                },
                    {{ok, Socket}, NewState};
                {error, badarg} -> % Pid has timed out, reuse for someone else
                    inet:setopts(Socket, [{active, once}]),
                    {no_socket, State};
                _ -> % something wrong with the socket; remove it, try again
                    find_socket(Dest, Pid, remove_socket(Socket, State))
            end;
        error ->
            {no_socket, State}
    end.

remove_socket(Socket, State) ->
    Dests = State#ns_connection_pool.destinations,
    case dict:find(Socket, State#ns_connection_pool.sockets) of
        {ok, {Dest, Timer}} ->
            cancel_timer(Timer, Socket),
            gen_tcp:close(Socket),
            Sockets = lists:delete(Socket, dict:fetch(Dest, Dests)),
            State#ns_connection_pool{
              destinations = update_dest(Dest, Sockets, Dests),
              sockets = dict:erase(Socket, State#ns_connection_pool.sockets)
             };
        error ->
            State
    end.

store_socket(Dest, Socket, State) ->
    Timeout = State#ns_connection_pool.timeout,
    Timer = erlang:send_after(Timeout, self(), {timeout, Socket}),
    %% the socket might be closed from the other side
    inet:setopts(Socket, [{active, once}]),
    Dests = State#ns_connection_pool.destinations,
    Sockets = case dict:find(Dest, Dests) of
                  {ok, S} -> [Socket | S];
                  error   -> [Socket]
              end,
    State#ns_connection_pool{
      destinations = dict:store(Dest, Sockets, Dests),
      sockets = dict:store(Socket, {Dest, Timer}, State#ns_connection_pool.sockets)
     }.

update_dest(Destination, [], Destinations) ->
    dict:erase(Destination, Destinations);
update_dest(Destination, Sockets, Destinations) ->
    dict:store(Destination, Sockets, Destinations).

cancel_timer(Timer, Socket) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive
                {timeout, Socket} -> ok
            after
                0 -> ok
            end;
        _     -> ok
    end.

add_to_queue(Dest, From, #ns_connection_pool{queues = Queues} = State) ->
    NewQueues =
        case dict:find(Dest, Queues) of
            error ->
                dict:store(Dest, queue:in(From, queue:new()), Queues);
            {ok, Q} ->
                dict:store(Dest, queue:in(From, Q), Queues)
        end,

    State#ns_connection_pool{queues = NewQueues}.

maybe_notify_blocked_client(Dest, State) ->
    case queue_out(Dest, State) of
        empty ->
            State;
        {ok, From, State1} ->
            gen_server:reply(From, no_socket),
            monitor_client(Dest, From, State1)
    end.

queue_out(Dest, #ns_connection_pool{queues = Queues} = State) ->
    case dict:find(Dest, Queues) of
        error ->
            empty;
        {ok, Q} ->
            {{value, From}, Q2} = queue:out(Q),
            Queues2 = case queue:is_empty(Q2) of
                          true ->
                              dict:erase(Dest, Queues);
                          false ->
                              dict:store(Dest, Q2, Queues)
                      end,
            {ok, From, State#ns_connection_pool{queues = Queues2}}
    end.

deliver_socket(Socket, Dest, State) ->
    case queue_out(Dest, State) of
        empty ->
            store_socket(Dest, Socket, State);
        {ok, {PidWaiter, _} = FromWaiter, State2} ->
            inet:setopts(Socket, [{active, false}]),
            case gen_tcp:controlling_process(Socket, PidWaiter) of
                ok ->
                    gen_server:reply(FromWaiter, {ok, Socket}),
                    monitor_client(Dest, FromWaiter, State2);
                {error, badarg} -> % Pid died, reuse for someone else
                    inet:setopts(Socket, [{active, once}]),
                    deliver_socket(Socket, Dest, State2);
                _ ->
                    %% Something wrong with the socket; remove it and reply
                    %% no_socket to the waiter
                    catch gen_tcp:close(Socket),
                    gen_server:reply(FromWaiter, no_socket),
                    monitor_client(Dest, FromWaiter, State2)
            end
    end.

monitor_client(Dest, {Pid, _} = _From,
               #ns_connection_pool{clients = Clients,
                                   clients_counts = Counts} = State) ->
    MonRef = erlang:monitor(process, Pid),
    Clients2 = dict:store(Pid, {Dest, MonRef}, Clients),
    Counts2 = dict:update_counter(Dest, 1, Counts),
    State#ns_connection_pool{clients = Clients2,
                             clients_counts = Counts2}.

find_client(Pid, #ns_connection_pool{clients = Clients,
                                     clients_counts = Counts} = State) ->
    case dict:find(Pid, Clients) of
        {ok, {Dest, _MonRef} = RV} ->
            Clients2 = dict:erase(Pid, Clients),

            true = dest_clients_count(Dest, State) >= 0,
            Counts2 = dict:update_counter(Dest, -1, Counts),

            {ok, RV, State#ns_connection_pool{clients = Clients2,
                                              clients_counts = Counts2}};
        error ->
            error
    end.

dest_clients_count(Dest, #ns_connection_pool{clients_counts = Counts}) ->
    case dict:find(Dest, Counts) of
        {ok, C} ->
            C;
        error ->
            0
    end.
