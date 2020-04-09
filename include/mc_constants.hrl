%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-2019 Couchbase, Inc.
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
-define(HEADER_LEN, 24).
-define(REQ_MAGIC, 16#80).
-define(RES_MAGIC, 16#81).
-define(SERVER_REQ_MAGIC,  16#82).
-define(SERVER_RESP_MAGIC, 16#83).

% Command codes.
-define(GET,         16#00).
-define(SET,         16#01).
-define(ADD,         16#02).
-define(REPLACE,     16#03).
-define(DELETE,      16#04).
-define(INCREMENT,   16#05).
-define(DECREMENT,   16#06).
-define(QUIT,        16#07).
-define(FLUSH,       16#08).
-define(GETQ,        16#09).
-define(NOOP,        16#0a).
-define(VERSION,     16#0b).
-define(GETK,        16#0c).
-define(GETKQ,       16#0d).
-define(APPEND,      16#0e).
-define(PREPEND,     16#0f).
-define(STAT,        16#10).
-define(SETQ,        16#11).
-define(ADDQ,        16#12).
-define(REPLACEQ,    16#13).
-define(DELETEQ,     16#14).
-define(INCREMENTQ,  16#15).
-define(DECREMENTQ,  16#16).
-define(QUITQ,       16#17).
-define(FLUSHQ,      16#18).
-define(APPENDQ,     16#19).
-define(PREPENDQ,    16#1a).

-define(CMD_HELLO,   16#1f).

-define(CMD_SASL_LIST_MECHS, 16#20).
-define(CMD_SASL_AUTH,       16#21).
-define(CMD_SASL_STEP,       16#22).

%% this commands are used to manage dynamic reloading of memcached.json
-define(CMD_CONFIG_VALIDATE, 16#25).
-define(CMD_CONFIG_RELOAD, 16#26).

-define(CMD_AUDIT_PUT, 16#27).
-define(CMD_AUDIT_CONFIG_RELOAD, 16#28).

-define(CMD_CREATE_BUCKET, 16#85).
-define(CMD_DELETE_BUCKET, 16#86).
-define(CMD_LIST_BUCKETS , 16#87).
-define(CMD_EXPAND_BUCKET, 16#88).
-define(CMD_SELECT_BUCKET, 16#89).

-define(CMD_SET_VBUCKET,     16#3d).
-define(CMD_GET_VBUCKET,     16#3e).
-define(CMD_DELETE_VBUCKET,  16#3f).

-define(SYNC, 16#96).
-define(CMD_SET_PARAM,       16#82).            % NOTE: Id is same as old CMD_SET_FLUSH_PARAM
-define(CMD_GET_REPLICA,     16#83).

-define(CMD_GET_FAILOVER_LOG, 16#96).

% internal mc_couch command notifying us that writes happened in
% ep-engine
-define(CMD_NOTIFY_VBUCKET_UPDATE, 16#ac).


-define(CMD_GET_META, 16#a0).
-define(CMD_GETQ_META, 16#a1).
-define(CMD_SET_WITH_META, 16#a2).
-define(CMD_SETQ_WITH_META, 16#a3).
-define(CMD_ADD_WITH_META, 16#a4).              % not used and perhaps killed in ep-engine
-define(CMD_ADDQ_WITH_META, 16#a5).
-define(CMD_DEL_WITH_META, 16#a8).
-define(CMD_DELQ_WITH_META, 16#a9).

-define(CMD_ENABLE_TRAFFIC, 16#ad).
-define(CMD_DISABLE_TRAFFIC, 16#ae).

-define(CMD_CHANGE_VB_FILTER, 16#b0).

-define(CMD_CHECKPOINT_PERSISTENCE, 16#b1).

-define(CMD_COMPACT_DB, 16#b3).

-define(CMD_SET_CLUSTER_CONFIG, 16#b4).

-define(CMD_GET_RANDOM_KEY, 16#b6).

-define(CMD_SEQNO_PERSISTENCE, 16#b7).
-define(CMD_GET_KEYS, 16#b8).

-define(CMD_COLLECTIONS_SET_MANIFEST, 16#b9).
-define(CMD_COLLECTIONS_GET_MANIFEST, 16#ba).

-define(CMD_SUBDOC_GET, 16#c5).
-define(CMD_SUBDOC_MULTI_LOOKUP, 16#d0).

-define(CMD_ISASL_REFRESH, 16#f1).
-define(CMD_SSL_CERTS_REFRESH, 16#f2).
-define(CMD_RBAC_REFRESH, 16#f7).

-define(RGET,        16#30).
-define(RSET,        16#31).
-define(RSETQ,       16#32).
-define(RAPPEND,     16#33).
-define(RAPPENDQ,    16#34).
-define(RPREPEND,    16#35).
-define(RPREPENDQ,   16#36).
-define(RDELETE,     16#37).
-define(RDELETEQ,    16#38).
-define(RINCR,       16#39).
-define(RINCRQ,      16#3a).
-define(RDECR,       16#3b).
-define(RDECRQ,      16#3c).

% Response status codes.
-define(SUCCESS,          16#00).
-define(KEY_ENOENT,       16#01).
-define(KEY_EEXISTS,      16#02).
-define(E2BIG,            16#03).
-define(EINVAL,           16#04).
-define(NOT_STORED,       16#05).
-define(DELTA_BADVAL,     16#06).
-define(NOT_MY_VBUCKET,   16#07).
-define(MC_AUTH_ERROR,    16#20).
-define(MC_AUTH_CONTINUE, 16#21).
-define(ERANGE,           16#22).
-define(ROLLBACK,         16#23).
-define(UNKNOWN_COMMAND,  16#81).
-define(ENOMEM,           16#82).
-define(NOT_SUPPORTED,    16#83).
-define(EINTERNAL,        16#84).
-define(EBUSY,            16#85).
-define(ETMPFAIL,         16#86).
-define(NO_COLL_MANIFEST, 16#89).

-define(SUBDOC_PATH_NOT_EXIST,      16#c0).
-define(SUBDOC_NOT_DICT,            16#c1).
-define(SUBDOC_BAD_PATH_SYNTAX,     16#c2).
-define(SUBDOC_PATH_TOO_LARGE,      16#c3).
-define(SUBDOC_MANY_LEVELS,         16#c4).
-define(SUBDOC_INVALID_VALUE,       16#c5).
-define(SUBDOC_DOC_NOT_JSON,        16#c6).
-define(SUBDOC_BAD_ARITH,           16#c7).
-define(SUBDOC_INVALID_RES_NUM,     16#c8).
-define(SUBDOC_PATH_EXISTS,         16#c9).
-define(SUBDOC_RES_TOO_DEEP,        16#ca).
-define(SUBDOC_INVALID_COMMANDS,    16#cb).
-define(SUBDOC_PATH_FAILED,         16#cc).
-define(SUBDOC_SUCC_ON_DELETED,     16#cd).
-define(SUBDOC_INVALID_FLAGS,       16#ce).
-define(SUBDOC_XATTR_COMB,          16#cf).
-define(SUBDOC_UNKNOWN_MACRO,       16#d0).
-define(SUBDOC_UNKNOWN_ATTR,        16#d1).
-define(SUBDOC_VIRT_ATTR,           16#d2).
-define(SUBDOC_FAILED_ON_DELETED,   16#d3).
-define(SUBDOC_INVALID_XATTR_ORDER, 16#d4).

% Vbucket States
-define(VB_STATE_ACTIVE, 1).
-define(VB_STATE_REPLICA, 2).
-define(VB_STATE_PENDING, 3).
-define(VB_STATE_DEAD, 4).

-type int_vb_state() :: ?VB_STATE_ACTIVE | ?VB_STATE_REPLICA | ?VB_STATE_PENDING | ?VB_STATE_DEAD.

%% Metadata types
-define(META_REVID, 16#01).

%% Flags passed back in get_meta call
-define(GET_META_ITEM_DELETED_FLAG, 16#01).

%% DCP commands
-define(DCP_OPEN,                  16#50).
-define(DCP_ADD_STREAM,            16#51).
-define(DCP_CLOSE_STREAM,          16#52).
-define(DCP_STREAM_REQ,            16#53).
-define(DCP_GET_FAILOVER_LOG,      16#54).
-define(DCP_STREAM_END,            16#55).
-define(DCP_SNAPSHOT_MARKER,       16#56).
-define(DCP_MUTATION,              16#57).
-define(DCP_DELETION,              16#58).
-define(DCP_EXPIRATION,            16#59).
-define(DCP_FLUSH,                 16#5a).
-define(DCP_SET_VBUCKET_STATE,     16#5b).
-define(DCP_NOP,                   16#5c).
%% window update is officially called "buffer acknowledgement"
-define(DCP_WINDOW_UPDATE,         16#5d).
-define(DCP_CONTROL,               16#5e).
-define(DCP_PREPARE,               16#60).
-define(DCP_SEQNO_ACKNOWLEDGED,    16#61).
-define(DCP_COMMIT,                16#62).
-define(DCP_ABORT,                 16#63).

%% RBAC commands
-define(MC_AUTH_PROVIDER,             16#F8).
-define(MC_UPDATE_USER_PERMISSIONS,   16#F6).
-define(MC_AUTH_REQUEST,              16#02).
-define(MC_ACTIVE_EXTERNAL_USERS,     16#03).

%% datatypes enum
-define(MC_DATATYPE_RAW_BYTES,      16#00).
-define(MC_DATATYPE_JSON,           16#01).
-define(MC_DATATYPE_COMPRESSED,     16#02).
-define(MC_DATATYPE_COMPRESSED_JSON,16#03).

%% hello features
-define(MC_FEATURE_XATTR,       16#06).
-define(MC_FEATURE_COLLECTIONS, 16#12).
-define(MC_FEATURE_SNAPPY,      16#0A).
-define(MC_FEATURE_JSON,        16#0B).
-define(MC_FEATURE_DUPLEX,      16#0C).

%% DCP Open Connection flags.
-define(DCP_CONNECTION_FLAG_CONSUMER,       16#00).
-define(DCP_CONNECTION_FLAG_PRODUCER,       16#01).
-define(DCP_CONNECTION_FLAG_NOTIFIER,       16#02).
-define(DCP_CONNECTION_FLAG_XATTR,          16#04).
-define(DCP_CONNECTION_FLAG_INCL_DEL_TIMES, 16#20).

%% Definitions of sub-document path flags (this is a bitmap)
-define(SUBDOC_FLAG_NONE, 16#00).
-define(SUBDOC_FLAG_MKDIR_P, 16#01).
-define(SUBDOC_FLAG_XATTR_PATH, 16#04).
-define(SUBDOC_FLAG_EXPAND_MACROS, 16#10).

%%  Definitions of sub-document doc flags (this is a bitmap).
-define(SUBDOC_DOC_NONE, 16#00).
-define(SUBDOC_DOC_MKDOC, 16#01).
-define(SUBDOC_DOC_ADD, 16#02).
-define(SUBDOC_DOC_ACCESS_DELETED, 16#04).
