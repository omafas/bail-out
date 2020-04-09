%% @author Couchbase <info@couchbase.com>
%% @copyright 2011-2018 Couchbase, Inc.
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
