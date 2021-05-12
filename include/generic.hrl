%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included
%% in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
%% in that file, in accordance with the Business Source License, use of this
%% software will be governed by the Apache License, Version 2.0, included in
%% the file licenses/APL2.txt.
%%
-define(transform(Pattern, Replacement),
        fun (__T) ->
                case __T of
                    Pattern -> Replacement;
                    _       -> __T
                end
        end).

-define(transform(Var, Pred, Replacement),
        fun (Var) ->
                case Pred(Var) of
                    true  -> Replacement
                    false -> __T
                end
        end).

-define(query(Pattern, Result, Default),
        fun (__T) ->
                case __T of
                    Pattern -> Result;
                    _       -> Default
                end
        end).

-define(query(Var, Pred, Result, Default),
        fun (Var) ->
                case Pred(Var) of
                    true  -> Result;
                    false -> Default
                end
        end).
