-module(cb_creds_rotation).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("cut.hrl").

%% API
-export([start_link/0, rotate_password/0, extract_protection_sleep/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type gen_server_from() :: {pid(), Tag :: term()}.

-record(idle, {timer_ref :: reference() | undefined}).

-record(full_rot_scheduled, {timer_ref :: reference(),
                             froms = [] :: [gen_server_from()]}).

-record(running, {rotation_ref :: reference(),
                  froms = [] :: [gen_server_from()]}).

-record(running_and_scheduled, {rotation_ref :: reference(),
                                froms = [] :: [gen_server_from()],
                                new_froms = [] :: [gen_server_from()]}).

-record(s, {rotation_state   :: #idle{} |
                                #full_rot_scheduled{} |
                                #running{} |
                                #running_and_scheduled{},
            last_rotation_ts :: integer() | undefined,
            interval         :: integer(),
            protection_sleep :: integer()}).

-define(ROTATE_MSG, rotate).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

rotate_password() ->
    %% In the worst case rotation just finished, so we need to wait
    %% PROTECTION_SLEEP ms before we start another rotation. At the same time
    %% full rotation will also sleep PROTECTION_SLEEP ms between phase 2 and
    %% phase 3. In total it may spend 2 * PROTECTION_SLEEP ms sleeping, plus
    %% we need some time for rotation itself.
    Timeout = 30000 + 2 * extract_protection_sleep(),

    %% If we receive noproc here, we are likely in the process of joining nodes
    %% together or changing topology. Respond with 503 (temporary failure) and
    %% tell them to try again later.
    try gen_server:call(?MODULE, on_demand_rotation, Timeout) of
        X -> X
    catch exit:{{noproc,
                 {gen_server, call,
                  [{menelaus_cbauth, _}, sync, _]}},
                {gen_server, call,
                 [cb_creds_rotation, on_demand_rotation, _]}} ->
              {error, tmp_error}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({K, _}, Pid) when K == ?INT_CREDS_ROTATION_INT_KEY;
                             K == ?INT_CREDS_PROTECTION_SLEEP_KEY ->
              Pid ! reset_timers,
              Pid;
          ({{node, Node, K}, _}, Pid)
                                when K == ?INT_CREDS_ROTATION_INT_KEY;
                                     K == ?INT_CREDS_PROTECTION_SLEEP_KEY ->
              case Node == dist_manager:this_node() of
                  true -> Pid ! reset_timers;
                  false -> ok
              end,
              Pid;
          (_, Pid) ->
              Pid
      end,
      self()),

    Interval = extract_rotation_interval(),
    {ok, #s{rotation_state = start_rotate_timer(Interval),
            interval = Interval,
            protection_sleep = extract_protection_sleep()}, hibernate}.

handle_call(on_demand_rotation, From,
            #s{rotation_state = #idle{},
               last_rotation_ts = LastTS,
               protection_sleep = ProtectionSleep} = State) ->
    cancel_timer(State),
    TimeToWait = case LastTS of
                     undefined -> 0;
                     TS when is_integer(TS) ->
                         TimeSinceLastRotation = ts() - TS,
                         max(ProtectionSleep - TimeSinceLastRotation, 0)
                 end,
    RotationState = start_on_demand_rotation_timer(TimeToWait, [From]),
    {noreply, State#s{rotation_state = RotationState}};

handle_call(on_demand_rotation, From,
            #s{rotation_state = #full_rot_scheduled{} = RS} = State) ->
    #full_rot_scheduled{froms = Froms} = RS,
    NewRS = RS#full_rot_scheduled{froms = [From | Froms]},
    {noreply, State#s{rotation_state = NewRS}};

handle_call(on_demand_rotation, From,
            #s{rotation_state = #running{} = RS} = State) ->
    #running{rotation_ref = Ref, froms = Froms} = RS,
    {noreply, State#s{rotation_state = #running_and_scheduled{
                                           rotation_ref = Ref,
                                           froms = Froms,
                                           new_froms = [From]}}};

handle_call(on_demand_rotation, From,
            #s{rotation_state = #running_and_scheduled{} = RS} = State) ->
    #running_and_scheduled{new_froms = Froms} = RS,
    NewRS = RS#running_and_scheduled{new_froms = [From | Froms]},
    {noreply, State#s{rotation_state = NewRS}};

handle_call(Request, _From, State) ->
    ?log_error("Unhandled call: ~p", [Request]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?log_error("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info(?ROTATE_MSG, #s{rotation_state = #idle{},
                            protection_sleep = Sleep} = State) ->
    cancel_timer(State),
    misc:flush(?ROTATE_MSG),
    {noreply, State#s{rotation_state = start_rotation([], Sleep)}};
handle_info(?ROTATE_MSG, #s{rotation_state = #full_rot_scheduled{froms = Froms},
                            protection_sleep = Sleep} = State) ->
    cancel_timer(State),
    misc:flush(?ROTATE_MSG),
    {noreply, State#s{rotation_state = start_rotation(Froms, Sleep)}};

handle_info({'DOWN', Ref, process, _Pid, normal},
            #s{rotation_state = #running{rotation_ref = Ref, froms = Froms},
               interval = Interval} = State) ->
    [gen_server:reply(F, ok) || F <- lists:reverse(Froms)],
    State1 = set_rotation_ts(State),
    NewRS = start_rotate_timer(Interval),
    {noreply, State1#s{rotation_state = NewRS}, hibernate};

handle_info({'DOWN', Ref, process, _Pid, normal},
            #s{rotation_state = #running_and_scheduled{
                                    rotation_ref = Ref,
                                    froms = Froms,
                                    new_froms = NewFroms},
               protection_sleep = ProtectionSleep} = State) ->
    [gen_server:reply(F, ok) || F <- lists:reverse(Froms)],
    State1 = set_rotation_ts(State),
    NewRS = start_on_demand_rotation_timer(ProtectionSleep, NewFroms),
    {noreply, State1#s{rotation_state = NewRS}};

handle_info(reset_timers, #s{interval = OldInterval,
                             protection_sleep = OldProtectionSleep} = State) ->
    misc:flush(reset_interval),
    Interval = extract_rotation_interval(),
    ProtectionSleep = extract_protection_sleep(),

    State1 = State#s{interval = Interval,
                     protection_sleep = ProtectionSleep},

    State2 =
        case State1 of
            #s{rotation_state = #idle{}} when Interval /= OldInterval ->
                cancel_timer(State1),
                State1#s{rotation_state = start_rotate_timer(Interval)};
            #s{rotation_state = #full_rot_scheduled{froms = Froms}}
                                when ProtectionSleep /= OldProtectionSleep ->
                cancel_timer(State1),
                NewRS = start_on_demand_rotation_timer(ProtectionSleep, Froms),
                State1#s{rotation_state = NewRS};
            _ ->
                State1
        end,

    {noreply, State2, hibernate};

handle_info(Info, State) ->
    ?log_error("Unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

set_rotation_ts(State) ->
    State#s{last_rotation_ts = ts()}.

start_rotate_timer(Interval) ->
    Ref =
        case Interval > 0 of
            true ->
                ?log_debug("Starting creds rotation timer (~pms)", [Interval]),
                erlang:send_after(Interval, self(), ?ROTATE_MSG);
            false ->
                ?log_debug("Skipping start of creds rotation timer"),
                undefined
        end,
    #idle{timer_ref = Ref}.

start_on_demand_rotation_timer(Time, Froms) ->
    ?log_debug("Starting on demand rotation timer (~pms)", [Time]),
    Ref = erlang:send_after(Time, self(), ?ROTATE_MSG),
    #full_rot_scheduled{timer_ref = Ref, froms = Froms}.

start_rotation(Froms, Sleep) ->
    RotationType = case Froms of
                       [] -> partial;
                       [_ | _] -> full
                   end,
    {_Pid, MonRef} = spawn_opt(
                       fun () ->
                           case cluster_compat_mode:is_cluster_76() of
                               true -> rotate_password(RotationType, Sleep);
                               false -> ok
                           end
                       end,
                       [link, monitor]),
    #running{rotation_ref = MonRef, froms = Froms}.

ts() ->
    erlang:monotonic_time(millisecond).

cancel_timer(#s{rotation_state = #idle{timer_ref = Ref}}) ->
    misc:flush(?ROTATE_MSG),
    catch erlang:cancel_timer(Ref);
cancel_timer(#s{rotation_state = #full_rot_scheduled{timer_ref = Ref}}) ->
    misc:flush(?ROTATE_MSG),
    catch erlang:cancel_timer(Ref).

extract_rotation_interval() ->
    ns_config:search_node_with_default(?INT_CREDS_ROTATION_INT_KEY,
                                       ?INT_CREDS_ROTATION_INT_DEFAULT).

extract_protection_sleep() ->
    ns_config:search_node_with_default(?INT_CREDS_PROTECTION_SLEEP_KEY,
                                       ?INT_CREDS_PROTECTION_SLEEP_DEFAULT).

rotate_password(Type, ProtectionSleep) ->
    StartTS = erlang:system_time(millisecond),
    Node = dist_manager:this_node(),
    NewPass = ns_config_default:generate_internal_pass(),
    [OldPass | _] = ns_config_auth:get_special_passwords(Node,
                                                         ns_config:latest()),

    ServerPasswordSync =
        fun () ->
            ns_config:sync_announcements(),
            chronicle_compat_events:sync(),
            memcached_passwords:sync(),
            menelaus_cbauth:sync()
        end,

    ClientPasswordSync =
        fun () ->
                ns_config:sync_announcements(),
                chronicle_compat_events:sync(),
                ns_config_rep:ensure_config_seen_by_nodes(
                  ns_node_disco:nodes_actual_other(), infinity),
                misc:parallel_map(menelaus_cbauth:sync(_),
                                  ns_node_disco:nodes_actual(), infinity)
        end,

    ?log_info("Start password rotation phase 0"),
    %% Phase0: Before removing anything we make sure that current passwords are
    %% propagated properly. If previous rotation finished unsuccessfully for
    %% any reason, it might be the case that previous sync has not finished.
    ClientPasswordSync(),

    ?log_info("Start password rotation phase 1"),
    %% Phase1: Add NewPass to all internal "servers", so all servers can now
    %% accept NewPass as a valid password. OldPass should still work as well.
    %% This change affects only "servers". At the same time we know that
    %% "servers" only use local node's admin passwords from ns_config.
    %% Which means this change doesn't actually affect remote nodes.
    update_admin_pass(Node, [OldPass, NewPass]),
    ServerPasswordSync(),

    ?log_info("Start password rotation phase 2"),
    %% Phase2: Switch internal "clients" to use NewPass when authenticating.
    %% This change affects "clients" only. "Clients" can hypothetically use
    %% passwords for all the nodes in the cluster, so this change actually
    %% affects all the nodes in the cluster.
    update_admin_pass(Node, [NewPass, OldPass]),
    ClientPasswordSync(),

    case Type of
        full ->
            ?log_info("Start password rotation phase 3"),
            %% Phase3: Since all internal "clients" and all "servers" use
            %% NewPass now, it is safe to remove OldPass completely.
            %% This change affects "servers" only, it means it affects local
            %% node only (see the comments above).
            %%
            %% Protection sleep is needed for the following reasons:
            %% 1) For every "client" there is always a lag between taking the
            %%    password from ns_config and its actual usage.
            %%    So hypothetically if rotation happens between these two events
            %%    the password will not work for that client.
            %%    By adding a pause before removing the old password we are
            %%    making this scenario almost imposible.
            %% 2) If rotation happens between revrpc connection attempts, it is
            %%    possible that that service will not be able to connect because
            %%    ns_server won't be able to notify the service about new
            %%    password. If that happens, the service will have to restart.
            %%    By adding a pause before removing the old password we are
            %%    making that scenario almost imposible.
            timer:sleep(ProtectionSleep),
            update_admin_pass(Node, [NewPass]),
            ServerPasswordSync();
        partial ->
            %% We don't need phase 3 in case if there is no strict requirement
            %% to remove the old password (for example, when it has leaked).
            %% When we do periodic rotation the old password will be removed
            %% anyway at phase 1 of next rotation.
            ok
    end,

    ?log_info("Password rotation finished (total time: ~pms)",
               [erlang:system_time(millisecond) - StartTS]),
    ok.

update_admin_pass(Node, NewPasswords) ->
    ns_config:update_key(
      {node, Node, memcached},
      fun (List) ->
          Res = misc:key_update(
                  admin_pass,
                  List,
                  fun (_) -> {v2, NewPasswords} end),
          case Res of
              false -> List;
              NewList when is_list(NewList) -> NewList
          end
      end).
