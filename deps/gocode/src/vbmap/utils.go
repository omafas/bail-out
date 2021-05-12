// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"math/rand"
)

const (
	MaxInt = int(^uint(0) >> 1)
)

func Abs(v int) int {
	if v < 0 {
		return -v
	} else {
		return v
	}
}

func Shuffle(a []int) {
	for i := range a {
		j := i + rand.Intn(len(a)-i)
		a[i], a[j] = a[j], a[i]
	}
}

func SpreadSum(sum int, n int) (result []int) {
	result = make([]int, n)

	quot := sum / n
	rem := sum % n

	for i := range result {
		result[i] = quot
		if rem != 0 {
			rem -= 1
			result[i] += 1
		}
	}

	Shuffle(result)

	return
}

func B2i(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func Min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func TrivialTags(nodes int) (tags map[Node]Tag) {
	tags = make(map[Node]Tag)
	for n := 0; n < nodes; n++ {
		tags[Node(n)] = Tag(n)
	}

	return
}
