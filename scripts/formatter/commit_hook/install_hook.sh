#!/usr/bin/env bash

# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

usage () {
    cat << EOF
Usage: scripts/formatter/install_hook.sh

Installs a pre-commit hook to run

  python3 scripts/formatter/erlang_formatter.py

before each commit.

Once installed, the behaviour of the hook can be customised with

  git config couchbase.erlangformat <BEHAVIOUR>

Where the valid options are:

  "warn"   : Block the commit if there are style issues but do not make
             changes to files (default)
  "fix"    : Block the commit, Fix style issues but do not stage the
             resulting changes. Changes can be reviewed with \`git diff\`.
  "stage"  : Block, fix issues and immediately stage the changes
             ready to commit.
  "commit" : Transparently fix style issues and allow the commit.

EOF
}

if [[ ! "${@#-h}" = "$@" ]] ; then
    usage
    exit
fi

if [ -e .git/hooks/pre-commit ] ; then
    echo "Project already has a pre-commit hook, not overwriting it!"
    exit
else
    echo "Installing pre-commit hook in .git/hooks/pre-commit"
    cp scripts/formatter/commit_hook/erlang_formatter_pre_commit_hook .git/hooks/pre-commit
fi
