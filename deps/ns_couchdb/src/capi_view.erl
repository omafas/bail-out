%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-module(capi_view).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").

%% Public API
-export([handle_view_req/3, all_docs_db_req/2, handle_view_merge_req/1, handle_with_auth/2]).


handle_view_req(Req, Db, DDoc) when Db#db.filepath =/= undefined ->
    couch_httpd_view:handle_view_req(Req, Db, DDoc);

handle_view_req(#httpd{method='GET',
                       path_parts=[_, _, DName, _, ViewName]}=Req,
                Db, _DDoc) ->
    capi_indexer:do_handle_view_req(mapreduce_view, Req, Db#db.name, DName, ViewName);

handle_view_req(#httpd{method='POST',
                       path_parts=[_, _, DName, _, ViewName]}=Req,
                Db, _DDoc) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    capi_indexer:do_handle_view_req(mapreduce_view, Req, Db#db.name, DName, ViewName);

handle_view_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST,HEAD").

handle_with_auth(#httpd{mochi_req = MochiReq} = Req, Module) ->
    [{cookie, Cookie}] = ns_config:read_key_fast(otp, undefined),
    CookieStr = atom_to_list(Cookie),
    Allowed =
        case menelaus_auth:extract_auth(MochiReq) of
            {"@ns_server", CookieStr} ->
                true;
            _ ->
                false
        end,
    case Allowed of
        true ->
            Module:handle_req(Req);
        false ->
            couch_httpd:send_error(Req, 401, <<"unauthorized">>,
                                   <<"Access is allowed only to ns_server.">>)
    end.

handle_view_merge_req(Req) ->
    handle_with_auth(Req, couch_httpd_view_merger).

all_docs_db_req(_Req,
                #db{filepath = undefined}) ->
    throw({bad_request, "_all_docs is no longer supported"});

all_docs_db_req(Req, Db) ->
    couch_httpd_db:db_req(Req, Db).
