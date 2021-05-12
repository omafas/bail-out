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
	"fmt"
)

// RI generator that ignores tags. It just spreads 1s in the matrix so that
// row and column sums equal to number of slaves.
type DummyRIGenerator struct {
	DontAcceptRIGeneratorParams
}

func makeDummyRIGenerator() *DummyRIGenerator {
	return &DummyRIGenerator{}
}

func (_ DummyRIGenerator) String() string {
	return "dummy"
}

func (_ DummyRIGenerator) Generate(params VbmapParams, _ SearchParams) (ri RI, err error) {
	if params.Tags.TagsCount() != params.NumNodes {
		err = fmt.Errorf("Dummy RI generator is rack unaware and " +
			"doesn't support more than one node on the same tag")
		return
	}

	ri.TagAwarenessRank = StrictlyTagAware
	ri.Matrix = make([][]bool, params.NumNodes)
	for i := range ri.Matrix {
		ri.Matrix[i] = make([]bool, params.NumNodes)
	}

	for i, row := range ri.Matrix {
		for j := range row {
			k := (j - i + params.NumNodes - 1) % params.NumNodes
			if k < params.NumSlaves {
				ri.Matrix[i][j] = true
			}
		}
	}

	return
}
