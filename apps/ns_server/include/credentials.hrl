%% @author Couchbase <info@couchbase.com>
%% @copyright 2026-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-define(CREDENTIAL_TYPES,
        [aws, azure_shared, azure_ad, azure_sas, azure_managed, gcp, http,
         couchbase]).

-type credential_type() :: aws | azure_shared | azure_ad | azure_sas |
                           azure_managed | gcp | http | couchbase.
