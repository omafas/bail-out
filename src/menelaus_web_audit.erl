%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
%% @doc handlers for audit related REST API's

-module(menelaus_web_audit).

-include("cut.hrl").
-include("ns_common.hrl").

-export([handle_get/1,
         handle_post/1,
         handle_get_descriptors/1,
         handle_get_non_filterable_descriptors/1]).

handle_get(Req) ->
    menelaus_util:assert_is_enterprise(),

    Props = pre_process_get(ns_audit_cfg:get_global()),

    Json =
        lists:filtermap(fun ({K, V}) ->
                                case key_config_to_api(K) of
                                    undefined ->
                                        false;
                                    ApiK ->
                                        {true,
                                         {ApiK, (jsonifier(K))(V)}}
                                end
                        end, Props),

    menelaus_util:reply_json(Req, {Json}).

handle_post(Req) ->
    menelaus_util:assert_is_enterprise(),

    Config = ns_config:get(),
    validator:handle(
      fun (Values) ->
              Settings = [{key_api_to_config(ApiK), V} ||
                             {ApiK, V} <- pre_process_post(Config, Values)],

              case proplists:get_bool(auditd_enabled, Settings) of
                  true ->
                      ns_audit_cfg:sync_set_global(Settings),
                      audit_modify_audit_settings(Req, Settings);
                  false ->
                      audit_modify_audit_settings(Req, Settings),
                      ns_audit_cfg:set_global(Settings)
              end,
              menelaus_util:reply(Req, 200)
      end, Req, form, validators(Config)).

audit_modify_audit_settings(Req, Settings) ->
    case cluster_compat_mode:is_cluster_55() of
        true -> ns_audit:modify_audit_settings(Req, Settings);
        false -> ok
    end.

reply_with_json_audit_descriptors(Req, Descriptors) ->
    Json =
        lists:map(
          fun ({Id, Props}) ->
                  {[{id, Id},
                    {name, proplists:get_value(name, Props)},
                    {module, proplists:get_value(module, Props)},
                    {description, proplists:get_value(description, Props)}]}
          end, Descriptors),
    menelaus_util:reply_json(Req, Json).

handle_get_non_filterable_descriptors(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_65(),
    Descriptors = ns_audit_cfg:get_non_filterable_descriptors(),
    reply_with_json_audit_descriptors(Req, Descriptors).

handle_get_descriptors(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_55(),
    Descriptors = ns_audit_cfg:get_descriptors(ns_config:latest()),
    reply_with_json_audit_descriptors(Req, Descriptors).

audit_user_exists({_, external}) ->
    %% Allow any external user to be specified as "disabled user",
    %% since external users might not exist in CB users database
    %% and still be able to perform auditable actions
    true;
audit_user_exists(Identity) ->
    SpecIds = [{N, local} || N <- memcached_permissions:spec_users()],
    menelaus_users:user_exists(Identity) orelse lists:member(Identity, SpecIds).

jsonifier(disabled_users) ->
    fun (UList) ->
            [{[{name, list_to_binary(N)}, {domain, D}]} ||
                 {N, D} = Identity <- UList,
                 audit_user_exists(Identity)]
    end;
jsonifier(uid) ->
    fun list_to_binary/1;
jsonifier(Key) ->
    ns_audit_cfg:jsonifier(Key).

key_api_to_config(auditdEnabled) ->
    auditd_enabled;
key_api_to_config(rotateInterval) ->
    rotate_interval;
key_api_to_config(rotateSize) ->
    rotate_size;
key_api_to_config(logPath) ->
    log_path;
key_api_to_config(disabledUsers) ->
    disabled_users;
key_api_to_config(X) when is_atom(X) ->
    X.

key_config_to_api(auditd_enabled) ->
    auditdEnabled;
key_config_to_api(rotate_interval) ->
    rotateInterval;
key_config_to_api(rotate_size) ->
    rotateSize;
key_config_to_api(log_path) ->
    logPath;
key_config_to_api(X) ->
    case cluster_compat_mode:is_cluster_55() of
        true ->
            key_config_to_api_55(X);
        false ->
            undefined
    end.

key_config_to_api_55(actually_disabled) ->
    disabled;
key_config_to_api_55(disabled_users) ->
    disabledUsers;
key_config_to_api_55(uid) ->
    uid;
key_config_to_api_55(_) ->
    undefined.

pre_process_get(Props) ->
    case cluster_compat_mode:is_cluster_55() of
        true ->
            Enabled = proplists:get_value(enabled, Props),
            Disabled = proplists:get_value(disabled, Props),
            Descriptors = ns_audit_cfg:get_descriptors(ns_config:latest()),

            %% though POST API stores all configurable events as either enabled
            %% or disabled, we anticipate that the list of configurable events
            %% might change
            ActuallyDisabled =
                lists:filtermap(
                  fun ({Id, P}) ->
                          IsEnabledByDefault = proplists:get_value(enabled, P),
                          case lists:member(Id, Enabled) orelse
                              (IsEnabledByDefault andalso
                               not lists:member(Id, Disabled)) of
                              true ->
                                  false;
                              false ->
                                  {true, Id}
                          end
                  end, Descriptors),

            [{actually_disabled, ActuallyDisabled} | Props];
        false ->
            Props
    end.

pre_process_post(Config, Props) ->
    case cluster_compat_mode:is_cluster_55(Config) of
        true ->
            case proplists:get_value(disabled, Props) of
                undefined ->
                    Props;
                Disabled ->
                    Descriptors =
                        ns_audit_cfg:get_descriptors(Config),

                    %% all configurable events are stored either in enabled or
                    %% disabled list, to reduce an element of surprise in case
                    %% if the defaults will change after the upgrade
                    Enabled = [Id || {Id, _} <- Descriptors] -- Disabled,
                    misc:update_proplist(Props,
                                         [{enabled, Enabled},
                                          {disabled, lists:sort(Disabled)}])
            end;
        false ->
            Props
    end.

validate_events(Name, Descriptors, State) ->
    validator:validate(
      fun (Value) ->
              Events = string:tokens(Value, ","),
              IntEvents = [(catch list_to_integer(E)) || E <- Events],
              case lists:all(fun is_integer/1, IntEvents) of
                  true ->
                      case lists:filter(orddict:is_key(_, Descriptors),
                                        IntEvents) of
                          IntEvents ->
                              {value, IntEvents};
                          Other ->
                              BadEvents =
                                  string:join(
                                    [integer_to_list(E) ||
                                        E <- IntEvents -- Other], ","),
                              {error,
                               io_lib:format(
                                 "Following events are either unknown or not "
                                 "modifiable: ~s", [BadEvents])}
                      end;
                  false ->
                      {error, "All event id's must be integers"}
              end
      end, Name, State).

validate_users(Name, State) ->
    validator:validate(
      fun (Value) ->
              Users = [string:trim(N) || N <- string:tokens(Value, ",")],
              UsersParsed = [{U, string:tokens(U, "/")} || U <- Users],
              UsersFound =
                  lists:map(
                    fun ({U, [N, S]}) ->
                            Identity = {N, menelaus_web_rbac:domain_to_atom(S)},
                            case audit_user_exists(Identity) of
                                true ->
                                    Identity;
                                false ->
                                    {error, U}
                            end;
                        ({U, _}) ->
                            {error, U}
                    end, UsersParsed),
              case [E || {error, E} <- UsersFound] of
                  [] ->
                      {value, UsersFound};
                  BadUsers ->
                      {error,
                       "Unrecognized users: " ++ string:join(BadUsers, ",")}
              end
      end, Name, State).

validator_55(Config, State) ->
    case cluster_compat_mode:is_cluster_55(Config) of
        false ->
            State;
        true ->
            functools:chain(State, validators_55(Config))
    end.

validators_55(Config) ->
    Descriptors = orddict:from_list(ns_audit_cfg:get_descriptors(Config)),
    [validate_events(disabled, Descriptors, _),
     validate_users(disabledUsers, _)].

validators(Config) ->
    [validator:has_params(_),
     validator:boolean(auditdEnabled, _),
     validator:dir(logPath, _),
     validator:integer(rotateInterval, _),
     validator:range(
       rotateInterval, 15*60, 60*60*24*7,
       ?cut("The value must be in range from 15 minutes to 7 days"), _),
     validator:validate(
       fun (Value) ->
               case Value rem 60 of
                   0 ->
                       ok;
                   _ ->
                       {error, "Value must not be a fraction of minute"}
               end
       end, rotateInterval, _),
     validator:integer(rotateSize, 0, 500*1024*1024, _),
     validator_55(Config, _),
     validator:unsupported(_)].
