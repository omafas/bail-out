% Copyright 2009-Present Couchbase, Inc.
%
% Use of this software is governed by the Business Source License included in
% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
% file, in accordance with the Business Source License, use of this software
% will be governed by the Apache License, Version 2.0, included in the file
% licenses/APL2.txt.

-record(mc_entry, {key = undefined,
                   ext = undefined,
                   flag = 0,
                   expire = 0,
                   cas = 0,
                   data = undefined,
                   datatype = 0}).

-record(mc_header, {opcode = 0,
                    status = 0, % Used for both status & reserved field.
                    vbucket = 0,
                    keylen = undefined,
                    extlen = undefined,
                    bodylen = undefined,
                    opaque = 0}).
