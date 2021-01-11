%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
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
-module(mc_client_binary).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-define(MAX_MC_TIMEOUT, 300000).
%% we normally speak to local memcached when issuing delete
%% vbucket. Thus timeout needs to only cover ep-engine going totally
%% insane.
-define(VB_DELETE_TIMEOUT, ?MAX_MC_TIMEOUT).
-define(NO_BUCKET, "@no bucket@").

-export([auth/2,
         cmd/5,
         cmd_quiet/3,
         cmd_vocal/3,
         respond/3,
         create_bucket/4,
         delete_bucket/3,
         delete_vbucket/2,
         sync_delete_vbucket/2,
         flush/1,
         hello_features/1,
         hello/3,
         list_buckets/1,
         refresh_isasl/1,
         refresh_ssl_certs/1,
         noop/1,
         select_bucket/2,
         deselect_bucket/1,
         set_vbucket/3,
         set_vbucket/4,
         set_vbuckets/2,
         stats/1,
         stats/4,
         get_meta/3,
         update_with_rev/7,
         set_engine_param/4,
         enable_traffic/1,
         disable_traffic/1,
         get_dcp_docs_estimate/3,
         map_status/1,
         get_mass_dcp_docs_estimate/2,
         ext/2,
         set_cluster_config/4,
         get_random_key/1,
         compact_vbucket/5,
         wait_for_seqno_persistence/3,
         vbucket_state_to_atom/1,
         config_validate/2,
         config_reload/1,
         audit_put/3,
         audit_config_reload/1,
         refresh_rbac/1,
         subdoc_multi_lookup/5,
         get_failover_log/2,
         update_user_permissions/2,
         set_collections_manifest/2,
         get_collections_manifest/1
        ]).

-type recv_callback() :: fun((_, _, _) -> any()) | undefined.
-type mc_timeout() :: undefined | infinity | non_neg_integer().
-type mc_opcode() :: ?GET | ?SET | ?ADD | ?REPLACE | ?DELETE | ?INCREMENT |
                     ?DECREMENT | ?QUIT | ?FLUSH | ?GETQ | ?NOOP | ?VERSION |
                     ?GETK | ?GETKQ | ?APPEND | ?PREPEND | ?STAT | ?SETQ |
                     ?ADDQ | ?REPLACEQ | ?DELETEQ | ?INCREMENTQ | ?DECREMENTQ |
                     ?QUITQ | ?FLUSHQ | ?APPENDQ | ?PREPENDQ |
                     ?CMD_SASL_LIST_MECHS | ?CMD_SASL_AUTH | ?CMD_SASL_STEP |
                     ?CMD_CREATE_BUCKET | ?CMD_DELETE_BUCKET |
                     ?CMD_LIST_BUCKETS | ?CMD_EXPAND_BUCKET |
                     ?CMD_SELECT_BUCKET | ?CMD_SET_PARAM | ?CMD_GET_REPLICA |
                     ?CMD_SET_VBUCKET | ?CMD_GET_VBUCKET | ?CMD_DELETE_VBUCKET |
                     ?CMD_ISASL_REFRESH | ?CMD_GET_META | ?CMD_GETQ_META |
                     ?CMD_SET_WITH_META | ?CMD_SETQ_WITH_META |
                     ?CMD_SETQ_WITH_META |
                     ?CMD_DEL_WITH_META | ?CMD_DELQ_WITH_META |
                     ?RGET | ?RSET | ?RSETQ | ?RAPPEND | ?RAPPENDQ | ?RPREPEND |
                     ?RPREPENDQ | ?RDELETE | ?RDELETEQ | ?RINCR | ?RINCRQ |
                     ?RDECR | ?RDECRQ | ?SYNC | ?CMD_CHECKPOINT_PERSISTENCE |
                     ?CMD_SEQNO_PERSISTENCE | ?CMD_GET_RANDOM_KEY |
                     ?CMD_COMPACT_DB | ?CMD_AUDIT_PUT | ?CMD_AUDIT_CONFIG_RELOAD |
                     ?CMD_RBAC_REFRESH | ?CMD_SUBDOC_MULTI_LOOKUP |
                     ?CMD_GET_FAILOVER_LOG |
                     ?CMD_COLLECTIONS_SET_MANIFEST |
                     ?CMD_COLLECTIONS_GET_MANIFEST.


%% A memcached client that speaks binary protocol.
-spec cmd(mc_opcode(), port(), recv_callback(), any(),
          {#mc_header{}, #mc_entry{}}) ->
                 {ok, #mc_header{}, #mc_entry{}, any()} | {ok, quiet}.
cmd(Opcode, Sock, RecvCallback, CBData, HE) ->
    cmd(Opcode, Sock, RecvCallback, CBData, HE, undefined).

-spec cmd(mc_opcode(), port(), recv_callback(), any(),
          {#mc_header{}, #mc_entry{}}, mc_timeout()) ->
                 {ok, #mc_header{}, #mc_entry{}, any()} | {ok, quiet}.
cmd(Opcode, Sock, RecvCallback, CBData, HE, Timeout) ->
    case is_quiet(Opcode) of
        true  -> cmd_quiet(Opcode, Sock, HE);
        false -> cmd_vocal(Opcode, Sock, RecvCallback, CBData, HE,
                                  Timeout)
    end.

-spec cmd_quiet(integer(), port(),
                {#mc_header{}, #mc_entry{}}) ->
                       {ok, quiet}.
cmd_quiet(Opcode, Sock, {Header, Entry}) ->
    ok = mc_binary:send(Sock, req,
              Header#mc_header{opcode = Opcode}, ext(Opcode, Entry)),
    {ok, quiet}.

-spec respond(integer(), port(),
              {#mc_header{}, #mc_entry{}}) ->
                     {ok, quiet}.
respond(Opcode, Sock, {Header, Entry}) ->
    ok = mc_binary:send(Sock, res,
                        Header#mc_header{opcode = Opcode}, Entry),
    {ok, quiet}.

-spec cmd_vocal(integer(), port(),
                {#mc_header{}, #mc_entry{}}) ->
                       {ok, #mc_header{}, #mc_entry{}}.
cmd_vocal(Opcode, Sock, HE) ->
    {ok, RecvHeader, RecvEntry, _NCB} = cmd_vocal(Opcode, Sock, undefined, undefined, HE, undefined),
    {ok, RecvHeader, RecvEntry}.

cmd_vocal(?STAT = Opcode, Sock, RecvCallback, CBData,
                 {Header, Entry}, Timeout) ->
    ok = mc_binary:send(Sock, req, Header#mc_header{opcode = Opcode}, Entry),
    stats_recv(Sock, RecvCallback, CBData, Timeout);

cmd_vocal(Opcode, Sock, RecvCallback, CBData, {Header, Entry},
                 Timeout) ->
    ok = mc_binary:send(Sock, req,
              Header#mc_header{opcode = Opcode}, ext(Opcode, Entry)),
    cmd_vocal_recv(Opcode, Sock, RecvCallback, CBData, Timeout).

cmd_vocal_recv(Opcode, Sock, RecvCallback, CBData, Timeout) ->
    {ok, RecvHeader, RecvEntry} = mc_binary:recv(Sock, res, Timeout),
    %% Assert Opcode is what we expect.
    Opcode = RecvHeader#mc_header.opcode,
    NCB = case is_function(RecvCallback) of
              true  -> RecvCallback(RecvHeader, RecvEntry, CBData);
              false -> CBData
          end,
    {ok, RecvHeader, RecvEntry, NCB}.

% -------------------------------------------------

stats_recv(Sock, RecvCallback, State, Timeout) ->
    {ok, #mc_header{opcode = ROpcode,
                    keylen = RKeyLen} = RecvHeader, RecvEntry} =
        mc_binary:recv(Sock, res, Timeout),
    case ?STAT =:= ROpcode andalso 0 =:= RKeyLen of
        true  -> {ok, RecvHeader, RecvEntry, State};
        false -> NCB = case is_function(RecvCallback) of
                           true  -> RecvCallback(RecvHeader, RecvEntry, State);
                           false -> State
                       end,
                 stats_recv(Sock, RecvCallback, NCB, Timeout)
    end.

% -------------------------------------------------

auth(_Sock, undefined) -> ok;

auth(Sock, {<<"PLAIN">>, {AuthName, undefined}}) ->
    auth(Sock, {<<"PLAIN">>, {<<>>, AuthName, <<>>}});

auth(Sock, {<<"PLAIN">>, {AuthName, AuthPswd}}) ->
    auth(Sock, {<<"PLAIN">>, {<<>>, AuthName, AuthPswd}});

auth(Sock, {<<"PLAIN">>, {ForName, AuthName, undefined}}) ->
    auth(Sock, {<<"PLAIN">>, {ForName, AuthName, <<>>}});

auth(Sock, {<<"PLAIN">>, {ForName, AuthName, AuthPswd}}) ->
    BinForName  = mc_binary:bin(ForName),
    BinAuthName = mc_binary:bin(AuthName),
    BinAuthPswd = mc_binary:bin(AuthPswd),
    case cmd(?CMD_SASL_AUTH, Sock, undefined, undefined,
             {#mc_header{},
              #mc_entry{key = <<"PLAIN">>,
                        data = <<BinForName/binary, 0:8,
                                 BinAuthName/binary, 0:8,
                                 BinAuthPswd/binary>>
                       }}) of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Other ->
            process_error_response(Other)
    end;
auth(_Sock, _UnknownMech) ->
    {error, emech_unsupported}.

% -------------------------------------------------
create_bucket(Sock, BucketName, Engine, Config) ->
    case cmd(?CMD_CREATE_BUCKET, Sock, undefined, undefined,
             {#mc_header{},
              #mc_entry{key = BucketName,
                        data = list_to_binary([Engine, 0, Config])}}) of
        {ok, #mc_header{status=?SUCCESS}, _ME, _NCB} ->
            ok;
        Response -> process_error_response(Response)
    end.

%% This can take an arbitrary period of time.
delete_bucket(Sock, BucketName, Options) ->
    Force = proplists:get_bool(force, Options),
    Config = io_lib:format("force=~s", [Force]),
    case cmd(?CMD_DELETE_BUCKET, Sock, undefined, undefined,
             {#mc_header{},
              #mc_entry{key = BucketName,
                        data = iolist_to_binary(Config)}}, infinity) of
        {ok, #mc_header{status=?SUCCESS}, _ME, _NCB} ->
            ok;
        Response -> process_error_response(Response)
    end.

delete_vbucket(Sock, VBucket) ->
    case cmd(?CMD_DELETE_VBUCKET, Sock, undefined, undefined,
             {#mc_header{vbucket = VBucket}, #mc_entry{}},
             ?VB_DELETE_TIMEOUT) of
        {ok, #mc_header{status=?SUCCESS}, _ME, _NCB} ->
            ok;
        Response -> process_error_response(Response)
    end.

sync_delete_vbucket(Sock, VBucket) ->
    case cmd(?CMD_DELETE_VBUCKET, Sock, undefined, undefined,
             {#mc_header{vbucket = VBucket}, #mc_entry{data = <<"async=0">>}},
             infinity) of
        {ok, #mc_header{status=?SUCCESS}, _ME, _NCB} ->
            ok;
        Response -> process_error_response(Response)
    end.

flush(Sock) ->
    case cmd(?FLUSH, Sock, undefined, undefined,
             {#mc_header{},
              #mc_entry{}}) of
        {ok, #mc_header{status=?SUCCESS}, _ME, _NCB} ->
            ok;
        Response -> process_error_response(Response)
    end.

hello_features_map() ->
    [{xattr, ?MC_FEATURE_XATTR},
     {collections, ?MC_FEATURE_COLLECTIONS},
     {snappy, ?MC_FEATURE_SNAPPY},
     {duplex, ?MC_FEATURE_DUPLEX},
     {json, ?MC_FEATURE_JSON}].

hello_features(Features) ->
    FeaturesMap = hello_features_map(),
    [F || F <- Features, proplists:is_defined(F, FeaturesMap)].

hello(Sock, AgentName, ClientFeatures) ->
    FeaturesMap = hello_features_map(),
    Features = [<<V:16>> || {F, V} <- FeaturesMap,
                            proplists:get_bool(F, ClientFeatures)],
    %% AgentName is the name of the client issuing the hello command.
    case cmd(?CMD_HELLO, Sock, undefined, undefined,
             {#mc_header{},
              #mc_entry{key = AgentName, data = list_to_binary(Features)}}) of
        {ok, #mc_header{status=?SUCCESS}, #mc_entry{data = undefined}, _NCB} ->
            {ok, []};
        {ok, #mc_header{status=?SUCCESS}, #mc_entry{data = RetData}, _NCB} ->
            Negotiated = [V || <<V:16>> <= RetData],
            NegotiatedNames = [Name || {Name, Code} <- FeaturesMap,
                                       lists:member(Code, Negotiated)],
            {ok, NegotiatedNames};
        Response ->
            process_error_response(Response)
    end.

-spec vbucket_state_to_atom(int_vb_state()) -> atom().
vbucket_state_to_atom(?VB_STATE_ACTIVE) ->
    active;
vbucket_state_to_atom(?VB_STATE_REPLICA) ->
    replica;
vbucket_state_to_atom(?VB_STATE_PENDING) ->
    pending;
vbucket_state_to_atom(?VB_STATE_DEAD) ->
    dead;
vbucket_state_to_atom(_) ->
    unknown.

list_buckets(Sock) ->
    case cmd(?CMD_LIST_BUCKETS, Sock, undefined, undefined,
             {#mc_header{},
              #mc_entry{}}) of
        {ok, #mc_header{status=?SUCCESS}, #mc_entry{data=BucketsBin}, _NCB} ->
            case BucketsBin of
                undefined -> {ok, []};
                _ -> {ok, string:tokens(binary_to_list(BucketsBin), " ")}
            end;
        Response -> process_error_response(Response)
    end.

refresh_isasl(Sock) ->
    case cmd(?CMD_ISASL_REFRESH, Sock, undefined, undefined, {#mc_header{}, #mc_entry{}}) of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Response -> process_error_response(Response)
    end.

refresh_ssl_certs(Sock) ->
    case cmd(?CMD_SSL_CERTS_REFRESH, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{}}) of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Response ->
            process_error_response(Response)
    end.

noop(Sock) ->
    case cmd(?NOOP, Sock, undefined, undefined, {#mc_header{}, #mc_entry{}}) of
        {ok, #mc_header{status=?SUCCESS}, #mc_entry{}, _NCB} ->
            ok;
        Response -> process_error_response(Response)
    end.

select_bucket(Sock, BucketName) ->
    case cmd(?CMD_SELECT_BUCKET, Sock, undefined, undefined,
             {#mc_header{},
              #mc_entry{key = BucketName}}) of
        {ok, #mc_header{status=?SUCCESS}, _ME, _NCB} ->
            ok;
        Response -> process_error_response(Response)
    end.

deselect_bucket(Sock) ->
    select_bucket(Sock, ?NO_BUCKET).

engine_param_type_to_int(flush) ->
    1;
engine_param_type_to_int(tap) ->
    2;
engine_param_type_to_int(checkpoint) ->
    3;
engine_param_type_to_int(dcp) ->
    4;
engine_param_type_to_int(vbucket) ->
    5.

-spec set_engine_param(port(), binary(), binary(),
                       flush | tap | checkpoint | dcp | vbucket) -> ok | mc_error().
set_engine_param(Sock, Key, Value, Type) ->
    ParamType = engine_param_type_to_int(Type),
    Entry = #mc_entry{key = Key,
                      data = Value,
                      ext = <<ParamType:32/big>>},
    case cmd(?CMD_SET_PARAM, Sock, undefined, undefined,
             {#mc_header{}, Entry}) of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Response ->
            process_error_response(Response)
    end.

encode_vbucket_state(active)  -> <<?VB_STATE_ACTIVE:8>>;
encode_vbucket_state(replica) -> <<?VB_STATE_REPLICA:8>>;
encode_vbucket_state(pending) -> <<?VB_STATE_PENDING:8>>;
encode_vbucket_state(dead)    -> <<?VB_STATE_DEAD:8>>.

set_vbucket(Sock, VBucket, VBucketState) ->
    set_vbucket(Sock, VBucket, VBucketState, undefined).

set_vbucket(Sock, VBucket, VBucketState, VBInfo) ->
    case cmd(?CMD_SET_VBUCKET, Sock, undefined, undefined,
             construct_set_vbucket_packet({VBucket, VBucketState, VBInfo})) of
        {ok, #mc_header{status=?SUCCESS}, _ME, _NCB} -> ok;
        Response -> process_error_response(Response)
    end.

construct_set_vbucket_packet({VBucket, VBucketState, VBInfo}) ->
    State = encode_vbucket_state(VBucketState),
    Header = #mc_header{vbucket = VBucket},
    Entry = case VBInfo of
                undefined -> #mc_entry{ext = State};
                _ -> #mc_entry{data = ejson:encode(VBInfo),
                               ext = State,
                               datatype = ?MC_DATATYPE_JSON}
            end,
    {Header, Entry}.

set_vbuckets(Sock, ToSet) ->
    pipeline_send_recv(Sock, ?CMD_SET_VBUCKET,
                       fun construct_set_vbucket_packet/1, ToSet).

pipeline_send_recv(Sock, Opcode, EncodeFun, Requests) ->
    pipeline_send_recv(Sock, Opcode, EncodeFun, Requests, ?MAX_MC_TIMEOUT).

pipeline_send_recv(Sock, Opcode, EncodeFun, Requests, Timeout) ->
    ToSend = [EncodeFun(R) || R <- Requests],
    RVs = do_pipeline_send_recv(Sock, Opcode, ToSend, Timeout),
    Bad = lists:filtermap(
            fun ({_, {#mc_header{status = ?SUCCESS}, _}}) ->
                    false;
                ({Req, {Header, Entry}}) ->
                    {true, {Req, process_error_response(Header, Entry)}}
            end, lists:zip(Requests, RVs)),
    case Bad of
        [] -> ok;
        _ -> {errors, Bad}
    end.

-spec do_pipeline_send_recv(port(), integer(), [{#mc_header{}, #mc_entry{}}],
                            integer()) -> [{#mc_header{}, #mc_entry{}}].
do_pipeline_send_recv(Sock, Opcode, Requests, Timeout) ->
    EncodedStream = lists:map(
                      fun ({Header, Entry}) ->
                              NewHeader = Header#mc_header{opcode = Opcode},
                              NewEntry = ext(Opcode, Entry),
                              mc_binary:encode(req, NewHeader, NewEntry)
                      end, Requests),
    ok = mc_binary:send(Sock, EncodedStream),
    TRef = make_ref(),
    Timer = erlang:send_after(Timeout, self(), TRef),
    try
        {RV, <<>>} = lists:foldl(
                       fun (_, {Acc, Rest}) ->
                               {ok, Header, Entry, Extra} =
                                   mc_binary:quick_active_recv(Sock, Rest,
                                                               TRef),
                               %% Assert we receive the same opcode.
                               Opcode = Header#mc_header.opcode,
                               {[{Header, Entry} | Acc], Extra}
                       end, {[], <<>>}, Requests),
        lists:reverse(RV)
    after
        erlang:cancel_timer(Timer),
        misc:flush(TRef)
    end.

compact_vbucket(Sock, VBucket, PurgeBeforeTS, PurgeBeforeSeqNo, DropDeletes) ->
    DD = case DropDeletes of
             true ->
                 1;
             false ->
                 0
         end,
    Ext = <<PurgeBeforeTS:64, PurgeBeforeSeqNo:64, DD:8, 0:8, 0:16, 0:32>>,
    case cmd(?CMD_COMPACT_DB, Sock, undefined, undefined,
             {#mc_header{vbucket = VBucket}, #mc_entry{ext = Ext}},
             infinity) of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Response -> process_error_response(Response)
    end.

stats(Sock) ->
    stats(Sock, <<>>, fun (K, V, Acc) -> [{K, V}|Acc] end, []).

stats(Sock, Key, CB, CBData) ->
    case cmd(?STAT, Sock,
             fun (_MH, ME, CD) ->
                     CB(ME#mc_entry.key, ME#mc_entry.data, CD)
             end,
             CBData,
             {#mc_header{}, #mc_entry{key=Key}}) of
        {ok, #mc_header{status=?SUCCESS}, _E, Stats} ->
            {ok, Stats};
        Response -> process_error_response(Response)
    end.

get_meta(Sock, Key, VBucket) ->
    case cmd(?CMD_GET_META, Sock, undefined, undefined,
             {#mc_header{vbucket = VBucket},
              #mc_entry{key = Key}}) of
        {ok, #mc_header{status=?SUCCESS},
             #mc_entry{ext = Ext, cas = CAS}, _NCB} ->
            <<MetaFlags:32/big, ItemFlags:32/big,
              Expiration:32/big, SeqNo:64/big>> = Ext,
            RevId = <<CAS:64/big, Expiration:32/big, ItemFlags:32/big>>,
            Rev = {SeqNo, RevId},
            {ok, Rev, CAS, MetaFlags};
        {ok, #mc_header{status=?KEY_ENOENT},
             #mc_entry{cas=CAS}, _NCB} ->
            {memcached_error, key_enoent, CAS};
        Response ->
            process_error_response(Response)
    end.

subdoc_multi_lookup(Sock, Key, VBucket, Paths, Options) ->
    {SubDocFlags, SubDocDocFlags} = parse_subdoc_flags(Options),
    Ext = <<SubDocDocFlags:8>>,
    Header = #mc_header{vbucket = VBucket},
    Specs = [<<?CMD_SUBDOC_GET:8, SubDocFlags:8, (byte_size(P)):16, P/binary>>
                || P <- Paths],
    Entry = #mc_entry{ext = Ext, key = Key, data = Specs},
    case cmd(?CMD_SUBDOC_MULTI_LOOKUP, Sock, undefined, undefined,
             {Header, Entry}) of
        {ok, #mc_header{status = ?SUCCESS},
             #mc_entry{cas = CAS, data = DataResp}, _NCB} ->
            {ok, CAS, parse_multiget_res(DataResp)};
        Other ->
            process_error_response(Other)
    end.

parse_subdoc_flags(Options) ->
    lists:foldl(
        fun (mkdir_p, {F, D}) -> {F bor ?SUBDOC_FLAG_MKDIR_P, D};
            (xattr_path, {F, D}) -> {F bor ?SUBDOC_FLAG_XATTR_PATH, D};
            (expand_macros, {F, D}) -> {F bor ?SUBDOC_FLAG_EXPAND_MACROS, D};
            (mkdoc, {F, D}) -> {F, D bor ?SUBDOC_DOC_MKDOC};
            (add, {F, D}) -> {F, D bor ?SUBDOC_DOC_ADD};
            (access_deleted, {F, D}) -> {F, D bor ?SUBDOC_DOC_ACCESS_DELETED}
        end, {?SUBDOC_FLAG_NONE, ?SUBDOC_DOC_NONE}, Options).

parse_multiget_res(Binary) -> parse_multiget_res(Binary, []).
parse_multiget_res(<<>>, Res) -> lists:reverse(Res);
parse_multiget_res(<<_Status:16, Len:32, Data/binary>>, Res) ->
    <<JSON:Len/binary, Tail/binary>> = Data,
    parse_multiget_res(Tail, [JSON|Res]).

-spec update_with_rev(Sock :: port(), VBucket :: vbucket_id(),
                      Key :: binary(), Value :: binary() | undefined,
                      Rev :: rev(),
                      Deleted :: boolean(),
                      Cas :: integer()) -> {ok, #mc_header{}, #mc_entry{}} |
                                           {memcached_error, atom(), binary()}.
update_with_rev(Sock, VBucket, Key, Value, Rev, Deleted, CAS) ->
    case Deleted of
        true ->
            do_update_with_rev(Sock, VBucket, Key, <<>>, Rev, CAS, ?CMD_DEL_WITH_META);
        false ->
            do_update_with_rev(Sock, VBucket, Key, Value, Rev, CAS, ?CMD_SET_WITH_META)
    end.

%% rev is a pair. First element is RevNum (aka SeqNo). It's is tracked
%% separately inside couchbase bucket. Second part -- RevId, is
%% actually concatenation of CAS, Flags and Expiration.
%%
%% HISTORICAL/PERSONAL PERSPECTIVE:
%%
%% It can be seen that couch, xdcr and rest of ns_server are working
%% with RevIds as opaque entities. Never assuming it has CAS,
%% etc. Thus it would be possible to avoid _any_ mentions of them
%% here. We're just (re)packing some bits in the end. But I believe
%% this "across-the-project" perspective is extremely valuable. Thus
%% this informational (and, presumably, helpful) comment.
%%
%% For xxx-with-meta they're re-assembled as shown below. Apparently
%% to make flags and expiration to 'match' normal set command
%% layout.
rev_to_mcd_ext({SeqNo, <<CASPart:64, Exp:32, Flg:32>>}) ->
    %% pack the meta data in consistent order with EP_Engine protocol
    %% 32-bit flag, 32-bit exp time, 64-bit seqno and CAS
    %%
    %% Final 4 bytes is options. Currently supported options is
    %% SKIP_CONFLICT_RESOLUTION_FLAG but because we don't want to
    %% disable it we pass 0.
    <<Flg:32, Exp:32, SeqNo:64, CASPart:64, 0:32>>.


do_update_with_rev(Sock, VBucket, Key, Value, Rev, CAS, OpCode) ->
    Ext = rev_to_mcd_ext(Rev),
    Hdr = #mc_header{vbucket = VBucket},
    Entry = #mc_entry{key = Key, data = Value, ext = Ext, cas = CAS},
    Response = cmd(OpCode, Sock, undefined, undefined, {Hdr, Entry}),
    case Response of
        {ok, #mc_header{status=?SUCCESS} = RespHeader, RespEntry, _} ->
            {ok, RespHeader, RespEntry};
        _ ->
            process_error_response(Response)
    end.

enable_traffic(Sock) ->
    case cmd(?CMD_ENABLE_TRAFFIC, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{}}) of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Other ->
            process_error_response(Other)
    end.

disable_traffic(Sock) ->
    case cmd(?CMD_DISABLE_TRAFFIC, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{}}) of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Other ->
            process_error_response(Other)
    end.



%% -------------------------------------------------

is_quiet(?GETQ)       -> true;
is_quiet(?GETKQ)      -> true;
is_quiet(?SETQ)       -> true;
is_quiet(?ADDQ)       -> true;
is_quiet(?REPLACEQ)   -> true;
is_quiet(?DELETEQ)    -> true;
is_quiet(?INCREMENTQ) -> true;
is_quiet(?DECREMENTQ) -> true;
is_quiet(?QUITQ)      -> true;
is_quiet(?FLUSHQ)     -> true;
is_quiet(?APPENDQ)    -> true;
is_quiet(?PREPENDQ)   -> true;
is_quiet(?RSETQ)      -> true;
is_quiet(?RAPPENDQ)   -> true;
is_quiet(?RPREPENDQ)  -> true;
is_quiet(?RDELETEQ)   -> true;
is_quiet(?RINCRQ)     -> true;
is_quiet(?RDECRQ)     -> true;
is_quiet(?CMD_GETQ_META) -> true;
is_quiet(?CMD_SETQ_WITH_META) -> true;
is_quiet(?CMD_ADDQ_WITH_META) -> true;
is_quiet(?CMD_DELQ_WITH_META) -> true;
is_quiet(_)           -> false.

ext(?SET,        Entry) -> ext_flag_expire(Entry);
ext(?SETQ,       Entry) -> ext_flag_expire(Entry);
ext(?ADD,        Entry) -> ext_flag_expire(Entry);
ext(?ADDQ,       Entry) -> ext_flag_expire(Entry);
ext(?REPLACE,    Entry) -> ext_flag_expire(Entry);
ext(?REPLACEQ,   Entry) -> ext_flag_expire(Entry);
ext(?INCREMENT,  Entry) -> ext_arith(Entry);
ext(?INCREMENTQ, Entry) -> ext_arith(Entry);
ext(?DECREMENT,  Entry) -> ext_arith(Entry);
ext(?DECREMENTQ, Entry) -> ext_arith(Entry);
ext(_, Entry) -> Entry.

ext_flag_expire(#mc_entry{ext = Ext, flag = Flag, expire = Expire} = Entry) ->
    case Ext of
        undefined -> Entry#mc_entry{ext = <<Flag:32, Expire:32>>}
    end.

ext_arith(#mc_entry{ext = Ext, data = Data, expire = Expire} = Entry) ->
    case Ext of
        undefined ->
            Ext2 = case Data of
                       <<>>      -> <<1:64, 0:64, Expire:32>>;
                       undefined -> <<1:64, 0:64, Expire:32>>;
                       _         -> <<Data:64, 0:64, Expire:32>>
                   end,
            Entry#mc_entry{ext = Ext2, data = undefined}
    end.

map_status(?SUCCESS) ->
    success;
map_status(?KEY_ENOENT) ->
    key_enoent;
map_status(?KEY_EEXISTS) ->
    key_eexists;
map_status(?E2BIG) ->
    e2big;
map_status(?EINVAL) ->
    einval;
map_status(?NOT_STORED) ->
    not_stored;
map_status(?DELTA_BADVAL) ->
    delta_badval;
map_status(?NOT_MY_VBUCKET) ->
    not_my_vbucket;
map_status(?NO_COLL_MANIFEST) ->
    no_coll_manifest;
map_status(?UNKNOWN_COMMAND) ->
    unknown_command;
map_status(?ENOMEM) ->
    enomem;
map_status(?NOT_SUPPORTED) ->
    not_supported;
map_status(?EINTERNAL) ->
    internal;
map_status(?EBUSY) ->
    ebusy;
map_status(?ETMPFAIL) ->
    etmpfail;
map_status(?MC_AUTH_ERROR) ->
    auth_error;
map_status(?MC_AUTH_CONTINUE) ->
    auth_continue;
map_status(?ERANGE) ->
    erange;
map_status(?ROLLBACK) ->
    rollback;
map_status(?SUBDOC_PATH_NOT_EXIST) ->
    subdoc_path_not_exist;
map_status(?SUBDOC_NOT_DICT) ->
    subdoc_not_dict;
map_status(?SUBDOC_BAD_PATH_SYNTAX) ->
    subdoc_bad_path_syntax;
map_status(?SUBDOC_PATH_TOO_LARGE) ->
    subdoc_path_too_large;
map_status(?SUBDOC_MANY_LEVELS) ->
    subdoc_many_levels;
map_status(?SUBDOC_INVALID_VALUE) ->
    subdoc_invalid_value;
map_status(?SUBDOC_DOC_NOT_JSON) ->
    subdoc_doc_not_json;
map_status(?SUBDOC_BAD_ARITH) ->
    subdoc_bad_arith;
map_status(?SUBDOC_INVALID_RES_NUM) ->
    subdoc_invalid_res_num;
map_status(?SUBDOC_PATH_EXISTS) ->
    subdoc_path_exists;
map_status(?SUBDOC_RES_TOO_DEEP) ->
    subdoc_res_too_deep;
map_status(?SUBDOC_INVALID_COMMANDS) ->
    subdoc_invalid_commands;
map_status(?SUBDOC_PATH_FAILED) ->
    subdoc_path_failed;
map_status(?SUBDOC_SUCC_ON_DELETED) ->
    subdoc_succ_on_deleted;
map_status(?SUBDOC_INVALID_FLAGS) ->
    subdoc_invalid_flags;
map_status(?SUBDOC_XATTR_COMB) ->
    subdoc_xattr_comb;
map_status(?SUBDOC_UNKNOWN_MACRO) ->
    subdoc_unknown_macro;
map_status(?SUBDOC_UNKNOWN_ATTR) ->
    subdoc_unknown_attr;
map_status(?SUBDOC_VIRT_ATTR) ->
    subdoc_virt_attr;
map_status(?SUBDOC_FAILED_ON_DELETED) ->
    subdoc_failed_on_deleted;
map_status(?SUBDOC_INVALID_XATTR_ORDER) ->
    subdoc_invalid_xattr_order;
map_status(_) ->
    unknown.

-spec process_error_response(any()) -> mc_error().
process_error_response({ok, Header, Entry, _NCB}) ->
    process_error_response(Header, Entry).

-spec process_error_response(#mc_header{}, #mc_entry{}) -> mc_error().
process_error_response(#mc_header{status=Status}, #mc_entry{data=Msg}) ->
    {memcached_error, map_status(Status), Msg}.

wait_for_seqno_persistence(Sock, VBucket, SeqNo) ->
    RV = cmd(?CMD_SEQNO_PERSISTENCE, Sock, undefined, undefined,
             {#mc_header{vbucket = VBucket},
              #mc_entry{key = <<"">>,
                        ext = <<SeqNo:64/big>>}},
             infinity),
    case RV of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Other ->
            process_error_response(Other)
    end.

-spec get_dcp_docs_estimate(port(), vbucket_id(), binary() | string()) ->
                                   {ok, {non_neg_integer(), non_neg_integer(), binary()}}.
get_dcp_docs_estimate(Sock, VBucket, ConnName) ->
    Default = {0, 0, <<"does_not_exist">>},
    Key = iolist_to_binary([<<"dcp-vbtakeover ">>, integer_to_list(VBucket), $\s, ConnName]),

    RV = mc_binary:quick_stats(
           Sock, Key,
           fun (<<"estimate">>, V, {_, AccChkItems, AccStatus}) ->
                   {list_to_integer(binary_to_list(V)), AccChkItems, AccStatus};
               (<<"chk_items">>, V, {AccEstimate, _, AccStatus}) ->
                   {AccEstimate, list_to_integer(binary_to_list(V)), AccStatus};
               (<<"status">>, V, {AccEstimate, AccChkItems, _}) ->
                   {AccEstimate, AccChkItems, V};
               (_, _, Acc) ->
                   Acc
           end, Default),

    handle_docs_estimate_result(RV, Default).

handle_docs_estimate_result({ok, _} = RV, _) ->
    RV;
handle_docs_estimate_result({memcached_error, not_my_vbucket, _}, Default) ->
    {ok, Default}.

-spec get_mass_dcp_docs_estimate(port(), [vbucket_id()]) ->
                                        {ok, [{non_neg_integer(), non_neg_integer(), binary()}]}.
get_mass_dcp_docs_estimate(Sock, VBuckets) ->
    %% TODO: consider pipelining that stuff. For now it just does
    %% vbucket after vbucket sequentially
    {ok, [case get_dcp_docs_estimate(Sock, VB, <<>>) of
              {ok, V} -> V
          end || VB <- VBuckets]}.

set_cluster_config(Sock, Bucket, Rev, Blob) ->
    RV = cmd(?CMD_SET_CLUSTER_CONFIG, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{key = list_to_binary(Bucket),
                                      data = Blob,
                                      ext = <<Rev:32>>}},
             infinity),
    case RV of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Other ->
            process_error_response(Other)
    end.

get_random_key(Sock) ->
    RV = cmd(?CMD_GET_RANDOM_KEY, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{}},
             infinity),
    case RV of
        {ok, #mc_header{status=?SUCCESS}, #mc_entry{key = Key}, _} ->
            {ok, Key};
        Other ->
            process_error_response(Other)
    end.

config_validate(Sock, Body) ->
    RV = cmd(?CMD_CONFIG_VALIDATE, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{data = Body}},
             infinity),
    case process_error_response(RV) of
        {memcached_error, success, _} -> ok;
        Err -> Err
    end.

config_reload(Sock) ->
    RV = cmd(?CMD_CONFIG_RELOAD, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{}},
             infinity),
    case process_error_response(RV) of
        {memcached_error, success, _} -> ok;
        Err -> Err
    end.

audit_put(Sock, Code, Body) ->
    RV = cmd(?CMD_AUDIT_PUT, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{data = Body, ext = <<Code:32>>}},
             infinity),
    case process_error_response(RV) of
        {memcached_error, success, _} -> ok;
        Err -> Err
    end.

audit_config_reload(Sock) ->
    RV = cmd(?CMD_AUDIT_CONFIG_RELOAD, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{}},
             infinity),
    case process_error_response(RV) of
        {memcached_error, success, _} -> ok;
        Err -> Err
    end.

refresh_rbac(Sock) ->
    RV = cmd(?CMD_RBAC_REFRESH, Sock, undefined, undefined, {#mc_header{}, #mc_entry{}}),
    case process_error_response(RV) of
        {memcached_error, success, _} -> ok;
        Err -> Err
    end.

unpack_failover_log_loop(<<>>, Acc) ->
    Acc;
unpack_failover_log_loop(<<U:64/big, S:64/big, Rest/binary>>, Acc) ->
    unpack_failover_log_loop(Rest, [{U, S} | Acc]).

unpack_failover_log(Body) ->
    unpack_failover_log_loop(Body, []).

get_failover_log(Sock, VB) ->
    case cmd(?CMD_GET_FAILOVER_LOG, Sock, undefined, undefined,
             {#mc_header{vbucket = VB}, #mc_entry{}}) of
        {ok, #mc_header{status = ?SUCCESS}, ME, _NCB} ->
            unpack_failover_log(ME#mc_entry.data);
        Response ->
            process_error_response(Response)
    end.

set_collections_manifest(Sock, Blob) ->
    RV = cmd(?CMD_COLLECTIONS_SET_MANIFEST, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{data = Blob}},
             infinity),
    case RV of
        {ok, #mc_header{status=?SUCCESS}, _, _} ->
            ok;
        Other ->
            process_error_response(Other)
    end.

get_collections_manifest(Sock) ->
    case cmd(?CMD_COLLECTIONS_GET_MANIFEST, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{}}) of
        {ok, #mc_header{status = ?SUCCESS}, ME, _NCB} ->
            {ok, ME#mc_entry.data};
        Response ->
            process_error_response(Response)
    end.

update_user_permissions(Sock, RBACJson) ->
    Data = ejson:encode(RBACJson),
    case cmd(?MC_UPDATE_USER_PERMISSIONS, Sock, undefined, undefined,
             {#mc_header{}, #mc_entry{data = Data}}) of
        {ok, #mc_header{status = ?SUCCESS}, _, _} -> ok;
        Response -> process_error_response(Response)
    end.
