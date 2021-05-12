#!/usr/bin/gawk -f
#
# @author Couchbase <info@couchbase.com>
# @copyright 2015-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

BEGIN {
    type = "current"

    argc = 0

    for (i in ARGV) {
        if (match(ARGV[i], /--(max|max-ever|current)/, m)) {
            type = m[1]
        } else {
            argv[argc] = ARGV[i]
            argc += 1
        }
    }

    ARGC = argc
    for (i in argv) {
        ARGV[i] = argv[i]
    }

    if (type == "current") {
        size_index = 0
    } else if (type == "max") {
        size_index = 1
    } else if (type == "max-ever") {
        size_index = 2
    }
}

match($0, /=allocator:(.*)/, matches) {
    allocator = matches[1]
}

match($0, /(mbcs|sbcs) (blocks|carriers) size: ([[:digit:]]+) ([[:digit:]]+) ([[:digit:]]+)/, matches) {
    if (allocator) {
        allocators[allocator] = ""
        sizes[allocator "," matches[1] "," matches[2]] = matches[3 + size_index]
    }
}

END {
    total_mbcs_blocks = 0
    total_mbcs_carriers = 0

    total_sbcs_blocks = 0
    total_sbcs_carriers = 0

    PROCINFO["sorted_in"] = "@ind_str_asc"
    for (allocator in allocators) {
        mbcs_blocks = sizes[allocator ",mbcs,blocks"]
        mbcs_carriers = sizes[allocator ",mbcs,carriers"]

        sbcs_blocks = sizes[allocator ",sbcs,blocks"]
        sbcs_carriers = sizes[allocator ",sbcs,carriers"]

        mbcs_size[allocator] = mbcs_carriers
        mbcs_overhead[allocator] = mbcs_carriers - mbcs_blocks
        mbcs_fragmentation[allocator] = fragmentation(mbcs_overhead[allocator], mbcs_size[allocator])

        sbcs_size[allocator] = sbcs_carriers
        sbcs_overhead[allocator] = sbcs_carriers - sbcs_blocks
        sbcs_fragmentation[allocator] = fragmentation(sbcs_overhead[allocator], sbcs_size[allocator])

        total_mbcs_blocks += mbcs_blocks
        total_mbcs_carriers += mbcs_carriers
        total_sbcs_blocks += sbcs_blocks
        total_sbcs_carriers += sbcs_carriers

        combined_size[allocator] = mbcs_size[allocator] + sbcs_size[allocator]
        combined_overhead[allocator] = mbcs_overhead[allocator] + sbcs_overhead[allocator]
        combined_fragmentation[allocator] = fragmentation(combined_overhead[allocator],
                                                          combined_size[allocator])

        printf("%s\n\n", allocator)
        printf("\tmbcs size: %d\n", mbcs_size[allocator])
        printf("\tmbcs overhead: %d\n", mbcs_overhead[allocator])
        printf("\tmbcs fragmentation: %d\n\n", mbcs_fragmentation[allocator])
        printf("\tsbcs size: %d\n", sbcs_size[allocator])
        printf("\tsbcs overhead: %d\n", sbcs_overhead[allocator])
        printf("\tsbcs fragmentation: %d\n\n", sbcs_fragmentation[allocator])
        printf("\tcombined size: %d\n", combined_size[allocator])
        printf("\tcombined overhead: %d\n", combined_overhead[allocator])
        printf("\tcombined fragmentation: %d\n\n", combined_fragmentation[allocator])
    }

    total_mbcs_overhead = total_mbcs_carriers - total_mbcs_blocks;
    total_mbcs_fragmentation = fragmentation(total_mbcs_overhead, total_mbcs_carriers)

    total_sbcs_overhead = total_sbcs_carriers - total_sbcs_blocks;
    total_sbcs_fragmentation = fragmentation(total_sbcs_overhead, total_sbcs_carriers)

    total_blocks = total_mbcs_blocks + total_sbcs_blocks
    total_carriers = total_mbcs_carriers + total_sbcs_carriers
    total_overhead = total_carriers - total_blocks
    total_fragmentation = fragmentation(total_overhead, total_carriers)

    top(combined_size, top_size, 10)
    top(combined_overhead, top_overhead, 10)
    top(combined_fragmentation, top_fragmentation, 10)

    PROCINFO["sorted_in"] = "@val_type_desc"

    printf("Top allocators by used memory:\n\n")
    for (allocator in top_size) {
        printf("\t%s: %d\n", allocator, top_size[allocator])
    }

    printf("\n")

    printf("Top allocators by memory overhead:\n\n")
    for (allocator in top_overhead) {
        printf("\t%s: %d\n", allocator, top_overhead[allocator])
    }

    printf("\n")

    printf("Top allocators by memory fragmentation:\n\n")
    for (allocator in top_fragmentation) {
        printf("\t%s: %d\n", allocator, top_fragmentation[allocator])
    }

    printf("\n")
    printf("Totals:\n\n")
    printf("\tmbcs size: %d\n", total_mbcs_carriers)
    printf("\tmbcs overhead: %d\n", total_mbcs_overhead)
    printf("\tmbcs fragmentation: %d\n\n", total_mbcs_fragmentation)
    printf("\tsbcs size: %d\n", total_sbcs_carriers)
    printf("\tsbcs overhead: %d\n", total_sbcs_overhead)
    printf("\tsbcs fragmentation: %d\n\n", total_sbcs_fragmentation)
    printf("\tcombined size: %d\n", total_carriers)
    printf("\tcombined overhead: %d\n", total_overhead)
    printf("\tcombined fragmentation: %d\n\n", total_fragmentation)
}

function fragmentation(overhead, size) {
    if (size == 0) {
        return 0
    }

    return 100 * overhead / size
}

function top(arr, result, count) {
    PROCINFO["sorted_in"] = "@val_type_desc"

    for (allocator in arr) {
        result[allocator] = arr[allocator]

        count -= 1
        if (count <= 0) {
            break
        }
    }
}
