%% @author Couchbase <info@couchbase.com>
%% @copyright 2010-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
%
% This behavior defines necessary functions making up modules that can
% categorize logging.
%

-module(ns_log_categorizing).

-callback ns_log_cat(Code :: integer()) -> Severity :: info | warn | crit.
-callback ns_log_code_string(Code :: integer()) -> Description :: string().
