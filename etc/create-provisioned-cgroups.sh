#!/bin/bash
#
# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
#
# NOTE: the default base dir is usually:
#
#     /sys/fs/cgroup/system.slice/couchbase-server.service/
#
# This script is ran before starting the systemd unit file. It creates the
# necessary directories for each service. It must run before the cgroups can
# run properly.
set -e

user="couchbase"
group="couchbase"
services=("n1ql" "fts" "prometheus" "kv" "projector" "ns_server" "backup" "cont_backup" "index" "cbas" "eventing" "goxdcr" "saslauthd_port" "ns_couchdb")

Add_memory_subtree_control() {
    echo "echo '+memory' > $(pwd)/cgroup.subtree_control"
    echo "+memory" > cgroup.subtree_control
}

Create_cgroups() {
    cd $1
    Add_memory_subtree_control

    # setup services directory and create individual service folders
    mkdir services
    cd services
    Add_memory_subtree_control

    echo "Going to make directories: ${services[@]}"
    mkdir "${services[@]}"

    for svc in ${services[@]}
    do
        # n1ql has a special setup because they also have a child process
        # the "js-evaluator" which should be placed in its own cgroup
        if [ "$svc" == "n1ql" ]; then
            cd $svc
            Add_memory_subtree_control
            mkdir "evaluator" "n1ql"
            cd ..
        fi
    done
    echo "Created cgroup directory structure successfully."
}

Help() {
    # Display
    echo "Setup cgroup heirarchy for provisioned"
    echo
    echo "Syntax: ./create-provisioned-cgroups.sh [options] ROOTDIR"
    echo "ROOTDIR:     Root directory to create cgroup heirarchy in."
    echo
    echo "options:"
    echo " -h     Print the help information."
    echo " -u     Specify the user to chown the files to."
    echo " -g     Specify the user group to chown files to."
    echo
}

while getopts ":g:u:h" option; do
    case $option in
        h) # display Help
            Help
            exit
            ;;
        u)
            user=$OPTARG
            setuser=true
            ;;
        g)
            group=$OPTARG
            setuser=true
            ;;
    esac
done
shift $(expr $OPTIND - 1)

if [ "$setuser" = true ]; then
    echo "Using custom user and group: $user:$group"
fi

Create_cgroups $1
