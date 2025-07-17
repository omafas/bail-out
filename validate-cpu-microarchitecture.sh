#! /usr/bin/env sh
#
# Copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This is the global value for the x86_64 CPU microarchitecture version.
X86_64_MIN_VERSION=3

set_proc_cpuinfo() {
    proc_cpuinfo=$(cat /proc/cpuinfo)
}

set_cpu_flags() {
    cpu_flags=$(echo "${proc_cpuinfo}" | grep "^flags[[:space:]]*:" | head -n 1)
    cpu_flags="${cpu_flags#*:}"
    cpu_flags="${cpu_flags## }"
    if echo "${cpu_flags}" | grep -v -q -E "^[[:lower:][:digit:]_ ]+$"; then
        cpu_flags=""
    fi
}

has_x86_64_cpu_flags() {
    local flag
    local msg
    local cpu_name

    for flag; do
        ## Note, it's important to keep a trailing space
        case " ${cpu_flags} " in
            *" ${flag} "*)
                :
                ;;
            *)
                return 1
                ;;
        esac
    done
}

set_cpu_version() {
    ## Newer architectures should always be a subset of older ones, but for the
    ## sake of carefulness, we'll start with the lowest level and work our way
    ## up.

    ## x86-64-v1
    if ! has_x86_64_cpu_flags lm cmov cx8 fpu fxsr mmx syscall sse2; then
       cpu_version=0
       return 0
    fi

    ## x86-64-v2
    if ! has_x86_64_cpu_flags cx16 lahf_lm popcnt sse4_1 sse4_2 ssse3; then
        cpu_version=1
        return 0
    fi

    ## x86-64-v3
    if ! has_x86_64_cpu_flags avx avx2 bmi1 bmi2 f16c fma abm movbe xsave; then
        cpu_version=2
        return 0
    fi

    ## x86-64-v4
    if ! has_x86_64_cpu_flags avx512f avx512bw avx512cd avx512dq avx512vl; then
        cpu_version=3
        return 0
    fi

    cpu_version=4
}

validate_cpu_microarchitecture() {
    # Default values
    X86_64_MIN_VERSION=${1-${X86_64_MIN_VERSION}}
    DEBUG=${2-false}

    # Check to see if the check should be bypassed
    if [ -n "${COUCHBASE_DO_NOT_VALIDATE_CPU_MICROARCHITECTURE}" ]; then
        $DEBUG && echo "Skipping CPU microarchitecture validation due to COUCHBASE_DO_NOT_VALIDATE_CPU_MICROARCHITECTURE variable."
        return 0
    fi

    $DEBUG && echo "Attempting to detect CPU architecture..."
    cpu_arch=$(uname -m)
    if [ "$cpu_arch" = "aarch64" ]; then
        $DEBUG && echo "Detected ARM64 architecture, skipping x86_64 CPU microarchitecture detection."
        return 0
    elif [ "$cpu_arch" != "x86_64" ]; then
        $DEBUG && echo "Unknown CPU architecture: $cpu_arch. Continuing to boot Couchbase Server, but unexpected behavior may occur. Please report issues to Couchbase support."
        return 0
    fi

    $DEBUG && echo "Detected x86_64 architecture, proceeding with CPU microarchitecture detection..."
    set_proc_cpuinfo
    if [ -z "$proc_cpuinfo" ]; then
        echo "No data found in \"/proc/cpuinfo\". Continuing to boot Couchbase Server, but unexpected behaviour may occur. Please report issues to Couchbase support."
        return 0
    fi

    set_cpu_flags
    if [ -z "$cpu_flags" ]; then
        echo "Could not parse flags from \"/proc/cpuinfo\". Continuing to boot Couchbase Server, but unexpected behaviour may occur. Please report issues to Couchbase support."
        return 0
    fi

    set_cpu_version
    $DEBUG && echo "Detected x86_64 CPU microarchitecture version: $cpu_version"
    if [ $cpu_version -lt ${X86_64_MIN_VERSION} ]; then
        echo; echo; echo
        echo "Aborting! Couchbase Server requires x86_64 CPU microarchitecture v${X86_64_MIN_VERSION} or higher (found v$cpu_version)."
        echo; echo; echo
        exit 1
    else
        $DEBUG && echo "Couchbase Server startup can proceed. Detected x86_64 CPU microarchitecture v${cpu_version}."
        return 0
    fi
}
