% Copyright 2013-Present Couchbase, Inc.
%
% Use of this software is governed by the Business Source License included in
% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
% file, in accordance with the Business Source License, use of this software
% will be governed by the Apache License, Version 2.0, included in the file
% licenses/APL2.txt.



-record(httpdb, {
    url,
    oauth = nil,
    headers = [
        {"Accept", "application/json"},
        {"User-Agent", "CouchDB/" ++ couch_server:get_version()}
    ],
    timeout,            % milliseconds
    lhttpc_options = [],
    retries = 10,
    wait = 250,         % milliseconds
    httpc_pool = nil,
    http_connections
}).

-record(oauth, {
    consumer_key,
    token,
    token_secret,
    consumer_secret,
    signature_method
}).
