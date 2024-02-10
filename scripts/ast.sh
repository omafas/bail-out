#!/bin/bash
#
# @author Couchbase <info@couchbase.com>
# @copyright 2018-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
#
# Extract abstract syntax tree from a beam file compiled with
# debug_info flag.
#
#   ./ast.sh misc.beam

erl_file=$1
erl_script=$(cat <<EOF
    {ok, {_, [{abstract_code, {_, AST}}]}} =
        beam_lib:chunks("${erl_file}", [abstract_code]),
    io:format("~p~n", [AST]),
    init:stop(0).
EOF
)

erl -noinput -eval "${erl_script}"
