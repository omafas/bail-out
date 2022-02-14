%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(ns_config_log).

-behaviour(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, sanitize/1, sanitize/2, sanitize_value/1,
         sanitize_value/2,
         compute_bucket_diff/2]).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("generic.hrl").

-record(state, {buckets=[]}).

%% state sanitization
-export([format_status/2, tag_user_data/1, tag_user_name/1, tag_doc_id/1,
         tag_user_props/1, tag_misc_item/1]).

format_status(_Opt, [_PDict, State]) ->
    sanitize(State).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events,
                             fun (KVList) when is_list(KVList) ->
                                     Self ! {config_change, KVList};
                                 (_) ->
                                     ok
                             end),
    {ok, #state{}}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

% Don't log values for some password/auth-related config values.

handle_call(Request, From, State) ->
    ?log_warning("Unexpected handle_call(~p, ~p, ~p)", [Request, From, State]),
    {reply, ok, State, hibernate}.

handle_cast(Request, State) ->
    ?log_warning("Unexpected handle_cast(~p, ~p)", [Request, State]),
    {noreply, State, hibernate}.

handle_info({config_change, KVList}, State) ->
    NewState =
        lists:foldl(
          fun (KV, Acc) ->
                  log_kv(KV, Acc)
          end, State, KVList),
    {noreply, NewState, hibernate};
handle_info(Info, State) ->
    ?log_warning("Unexpected handle_info(~p, ~p)", [Info, State]),
    {noreply, State, hibernate}.

%% Internal functions
compute_buckets_diff(NewBuckets, OldBuckets) ->
    OldConfigs = proplists:get_value(configs, OldBuckets, []),
    NewConfigs = proplists:get_value(configs, NewBuckets, []),

    Diffed =
        merge_bucket_configs(fun compute_bucket_diff/2, NewConfigs, OldConfigs),

    misc:update_proplist(NewBuckets, [{configs, Diffed}]).

compute_bucket_diff(NewProps, OldProps) ->
    OldMap = proplists:get_value(map, OldProps, []),
    NewMap = proplists:get_value(map, NewProps, []),
    MapDiff = misc:compute_map_diff(NewMap, OldMap),

    OldFFMap = proplists:get_value(fastForwardMap, OldProps, []),
    NewFFMap = proplists:get_value(fastForwardMap, NewProps, []),
    FFMapDiff = misc:compute_map_diff(NewFFMap, OldFFMap),

    misc:update_proplist(NewProps, [{map, MapDiff},
                                    {fastForwardMap, FFMapDiff}]).

do_tag_user_name("@" ++ _ = Name) ->
    {ok, Name};
do_tag_user_name(Name) when is_list(Name) ->
    {ok, "<ud>" ++ Name ++ "</ud>"};
do_tag_user_name(NotName) when is_atom(NotName) ->
    {ok, NotName};  %% Cases like {source, local} we don't want to tag.
do_tag_user_name(Name) when is_binary(Name) ->
    {ok, list_to_binary(tag_user_name(binary_to_list(Name)))};
do_tag_user_name(_) ->
    continue.

tag_user_data(DebugKVList) ->
    misc:rewrite_tuples(
      fun tag_user_tuples_fun/1, DebugKVList).

tag_user_tuples_fun({user, UserName}) when is_binary(UserName) ->
    {stop, {user, tag_user_name(UserName)}};
tag_user_tuples_fun({doc, {user, {U, D}}, _, _, V} = Doc) ->
    T = setelement(2, Doc, {user, {tag_user_name(U), D}}),
    {stop, setelement(5, T, tag_user_props(V))};
tag_user_tuples_fun({docv2, {user, {U, D}}, V, _} = Doc) ->
    T = setelement(2, Doc, {user, {tag_user_name(U), D}}),
    {stop, setelement(3, T, tag_user_props(V))};
tag_user_tuples_fun({full_name, FullName}) when is_binary(FullName) ->
    {stop, {full_name, tag_user_name(FullName)}};
tag_user_tuples_fun({raw_url, RawUrl}) ->
    {stop, {raw_url, tag_misc_item(RawUrl)}};
tag_user_tuples_fun({doc_id, DocId}) ->
    {stop, {doc_id, tag_user_name(DocId)}};
tag_user_tuples_fun({<<"bindDN">>, DistinguishedName}) ->
    {stop, {<<"bindDN">>, tag_user_name(DistinguishedName)}};
tag_user_tuples_fun({CertType, Certificate})
  when CertType =:= <<"cacert">> orelse
       CertType =:= <<"clientTLSCert">> ->
    {stop, {CertType, tag_misc_item(Certificate)}};
tag_user_tuples_fun({UName, Type}) when Type =:= local orelse
                                        Type =:= external orelse
                                        Type =:= admin ->
    case do_tag_user_name(UName) of
        continue ->
            continue;
        {ok, Val} ->
            {stop, {Val, Type}}
    end;
tag_user_tuples_fun(_Other) ->
    continue.

tag_user_name(UserName) ->
    {ok, Val} = do_tag_user_name(UserName),
    Val.

tag_user_props(Props) ->
    generic:transformt(?transform({name, N}, {name, tag_user_name(N)}),
                       Props).

do_tag_misc_item(Item) when is_list(Item) ->
    {ok, "<ud>" ++ Item ++ "</ud>"};
do_tag_misc_item(Item) when is_binary(Item) ->
    {ok, Val} = do_tag_misc_item(binary_to_list(Item)),
    {ok, list_to_binary(Val)};
do_tag_misc_item(_) ->
    no_change.

tag_misc_item(Item) ->
    case do_tag_misc_item(Item) of
        no_change ->
            Item;
        {ok, Val} ->
            Val
    end.

tag_doc_id(DocId) ->
    tag_misc_item(DocId).

rewrite_tuples_with_vclock(Fun, Config) ->
    misc:rewrite_tuples(
      fun ({Key, Value0} = KV) ->
          case ns_config:extract_vclock(Value0) of
              {0, []} ->
                  Fun(KV);
              {Ts, VClock} ->
                  Value = ns_config:strip_metadata(Value0),
                  case Fun({Key, Value}) of
                      continue ->
                          continue;
                      {stop, {Key, NewValue}} ->
                          {stop, {Key, [ns_config:build_vclock(Ts, VClock)
                                        | NewValue]}}
                  end
          end;
          (Other) ->
              Fun(Other)
      end, Config).

sanitize(Config) ->
    sanitize(Config, false).

sanitize(Config, TagUserTuples) ->
    Continue =
        case TagUserTuples of
            false ->
                functools:const(continue);
            true ->
                fun tag_user_tuples_fun/1
        end,
    rewrite_tuples_with_vclock(
      fun ({password, _}) ->
              {stop, {password, "*****"}};
          ({sasl_password, _}) ->
              %% Maybe present on mixed version clusters. The key is eventually
              %% deleted at some time after compat mode is elevated to 7.0.
              {stop, {sasl_password, "*****"}};
          ({admin_pass, _}) ->
              {stop, {admin_pass, "*****"}};
          ({pass, _}) ->
              {stop, {pass, "*****"}};
          ({cert_and_pkey, {Cert, PKey}}) ->
              {stop, {cert_and_pkey, {Cert, sanitize_value(PKey)}}};
          ({cert_and_pkey, {Props, Cert, PKey}}) ->
              {stop, {cert_and_pkey, {Props, Cert, sanitize_value(PKey)}}};
          ({{metakv, K}, {?METAKV_SENSITIVE, V}}) ->
              {stop, {{metakv, K}, {?METAKV_SENSITIVE, sanitize_value(V)}}};
          ({cookie, Cookie}) ->
              {stop, {cookie, ns_cookie_manager:sanitize_cookie(Cookie)}};
          ({UName, {auth, Auth}}) ->
              {stop, {tag_user_name(UName),
                      {auth, sanitize(Auth, TagUserTuples)}}};
          ({<<"h">>, V}) ->
              {stop, {<<"h">>, sanitize_value(V)}};
          ({<<"plain">>, V}) ->
              {stop, {<<"plain">>, sanitize_value(V)}};
          ({Key, ListUsers}) when Key =:= disabled_users orelse
                                  Key =:= disabled_userids ->
              TaggedUsers = [{tag_user_name(N), Src} || {N, Src} <- ListUsers],
              {stop, {Key, TaggedUsers}};
          (Other) ->
              Continue(Other)
      end, Config).

sanitize_value(Value) ->
    sanitize_value(Value, []).

sanitize_value(Value0, Options) ->
    Salt = case Options of
               [add_salt] ->
                   crypto:strong_rand_bytes(32);
               _ ->
                   <<>>
           end,
    Value = term_to_binary(Value0),
    {sanitized,
     base64:encode(crypto:hash(
                     sha256,
                     <<Value/binary, Salt/binary>>))}.

log_kv({buckets = K, ?DELETED_MARKER = V}, State) ->
    log_common(K, V),
    State#state{buckets=[]};
log_kv({buckets, RawBuckets0}, #state{buckets=OldBuckets} = State) ->
    VClock = ns_config:extract_vclock(RawBuckets0),
    {V, NewBuckets} =
        case ns_config:strip_metadata(RawBuckets0) of
            ?DELETED_MARKER ->
                {?DELETED_MARKER, []};
            RawBuckets ->
                SortedBuckets = sort_buckets(RawBuckets),
                {compute_buckets_diff(SortedBuckets, OldBuckets),
                 SortedBuckets}
        end,
    log_common(buckets, [VClock | V]),
    State#state{buckets=NewBuckets};
log_kv({K, V}, State) ->
    log_common(K, V),
    State.

log_common(K, V) ->
    %% These can get pretty big, so pre-format them for the logger.
    {_, VS} = sanitize({K, V}),
    VB = list_to_binary(io_lib:print(VS, 0, 80, 100)),
    ?log_debug("config change:~n~p ->~n~s", [K, VB]).

sort_buckets(Buckets) ->
    Configs = proplists:get_value(configs, Buckets, []),
    SortedConfigs = lists:keysort(1, Configs),
    misc:update_proplist(Buckets, [{configs, SortedConfigs}]).

%% Merge bucket configs using a function. Note that only those buckets that
%% are present in the first list will be present in the resulting list.
merge_bucket_configs(_Fun, [], _) ->
    [];
merge_bucket_configs(Fun, [X | Xs], []) ->
    {_, XValue} = X,
    [Fun(XValue, []) | merge_bucket_configs(Fun, Xs, [])];
merge_bucket_configs(Fun, [X | XRest] = Xs, [Y | YRest] = Ys) ->
    {XName, XValue} = X,
    {YName, YValue} = Y,

    if
        XName < YName ->
            [{XName, Fun(XValue, [])} | merge_bucket_configs(Fun, XRest, Ys)];
        XName > YName ->
            merge_bucket_configs(Fun, Xs, YRest);
        true ->
            [{XName, Fun(XValue, YValue)} | merge_bucket_configs(Fun, XRest, YRest)]
    end.
