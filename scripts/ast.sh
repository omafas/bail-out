#!/bin/bash
#
# @author Couchbase <info@couchbase.com>
# @copyright 2018 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
