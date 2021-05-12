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
	"log"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"
)

var (
	testSearchParams = SearchParams{5, 25, false, false, false}
)

func testBuildRI(params VbmapParams, gen RIGenerator) (RI, error) {
	return gen.Generate(params, testSearchParams)
}

func testBuildR(params VbmapParams, gen RIGenerator) (RI, R, error) {
	return tryBuildR(params, gen, testSearchParams)
}

type TestingWriter struct {
	t *testing.T
}

var allGenerators []RIGenerator = []RIGenerator{
	makeDummyRIGenerator(),
	makeMaxFlowRIGenerator(),
}

var tagAwareGenerators []RIGenerator = []RIGenerator{
	makeMaxFlowRIGenerator(),
}

func (w TestingWriter) Write(p []byte) (n int, err error) {
	w.t.Logf("%s", string(p))
	return len(p), nil
}

func setup(t *testing.T) {
	diag = log.New(TestingWriter{t}, "", 0)
	t.Parallel()
}

func TestRReplicaBalance(t *testing.T) {
	setup(t)

	for nodes := 1; nodes <= 100; nodes++ {
		tags := TrivialTags(nodes)

		for replicas := 1; replicas <= 3; replicas++ {
			seed = time.Now().UTC().UnixNano()
			rand.Seed(seed)

			t.Log("=======================================")
			t.Logf("Generating R for %d node, %d replicas (seed %d)",
				nodes, replicas, seed)

			params := VbmapParams{
				Tags:        tags,
				NumNodes:    nodes,
				NumSlaves:   10,
				NumVBuckets: 1024,
				NumReplicas: replicas,
			}

			normalizeParams(&params)

			for _, gen := range allGenerators {
				_, r, err := testBuildR(params, gen)
				if err != nil || r.Evaluation() > 0 {
					t.Error("Can't build zero-evaluation R")
				}
			}
		}
	}
}

func (_ VbmapParams) Generate(rand *rand.Rand, size int) reflect.Value {
	nodes := rand.Int()%100 + 1
	replicas := rand.Int() % 4

	params := VbmapParams{
		Tags:        TrivialTags(nodes),
		NumNodes:    nodes,
		NumSlaves:   10,
		NumVBuckets: 1024,
		NumReplicas: replicas,
	}
	normalizeParams(&params)

	return reflect.ValueOf(params)
}

func checkRIProperties(gen RIGenerator, params VbmapParams) bool {
	ri, err := testBuildRI(params, gen)
	if err != nil {
		return false
	}

	if len(ri.Matrix) != params.NumNodes {
		return false
	}

	if len(ri.Matrix[0]) != params.NumNodes {
		return false
	}

	colSums := make([]int, params.NumNodes)
	rowSums := make([]int, params.NumNodes)

	b2i := map[bool]int{false: 0, true: 1}
	for i, row := range ri.Matrix {
		for j, elem := range row {
			colSums[j] += b2i[elem]
			rowSums[i] += b2i[elem]
		}
	}

	for i := range colSums {
		if colSums[i] != params.NumSlaves {
			return false
		}

		if rowSums[i] != params.NumSlaves {
			return false
		}
	}

	return true
}

func TestRIProperties(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators {
		check := func(params VbmapParams) bool {
			return checkRIProperties(gen, params)
		}

		err := quick.Check(check, &quick.Config{MaxCount: 250})
		if err != nil {
			t.Error(err)
		}
	}
}

func checkRProperties(gen RIGenerator, params VbmapParams, seed int64) bool {
	rand.Seed(seed)

	ri, r, err := testBuildR(params, gen)
	if err != nil {
		return false
	}

	if params.NumReplicas == 0 {
		// no replicas? R should obviously be empty
		for _, row := range r.Matrix {
			for _, elem := range row {
				if elem != 0 {
					return false
				}
			}
		}
	} else {
		// check that we follow RI topology
		for i, row := range ri.Matrix {
			for j, elem := range row {
				if !elem && r.Matrix[i][j] != 0 ||
					elem && r.Matrix[i][j] == 0 {
					return false
				}
			}
		}

		totalVBuckets := 0

		// check active vbuckets balance
		for _, sum := range r.RowSums {
			if sum%params.NumReplicas != 0 {
				return false
			}

			vbuckets := sum / params.NumReplicas
			expected := params.NumVBuckets / params.NumNodes

			if vbuckets != expected && vbuckets != expected+1 {
				return false
			}

			totalVBuckets += vbuckets
		}

		if totalVBuckets != params.NumVBuckets {
			return false
		}
	}

	return true
}

func TestRProperties(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators {

		check := func(params VbmapParams, seed int64) bool {
			return checkRProperties(gen, params, seed)
		}

		err := quick.Check(check, &quick.Config{MaxCount: 250})
		if err != nil {
			t.Error(err)
		}
	}
}

type NodePair struct {
	x, y Node
}

func checkVbmapProperties(gen RIGenerator, params VbmapParams, seed int64) bool {
	rand.Seed(seed)

	ri, r, err := testBuildR(params, gen)
	if err != nil {
		return false
	}

	vbmap := buildVbmap(r)

	if len(vbmap) != params.NumVBuckets {
		return false
	}

	activeVBuckets := make([]int, params.NumNodes)
	replicaVBuckets := make([]int, params.NumNodes)

	replications := make(map[NodePair]int)

	for _, chain := range vbmap {
		if len(chain) != params.NumReplicas+1 {
			return false
		}

		master := chain[0]
		activeVBuckets[int(master)] += 1

		for _, replica := range chain[1:] {
			// all the replications correspond to ones
			// defined by R
			if !ri.Matrix[int(master)][int(replica)] {
				return false
			}

			replicaVBuckets[int(replica)] += 1

			pair := NodePair{master, replica}
			if _, present := replications[pair]; !present {
				replications[pair] = 0
			}

			replications[pair] += 1
		}
	}

	// number of replications should correspond to one defined by R
	for i, row := range r.Matrix {
		for j, elem := range row {
			if elem != 0 {
				pair := NodePair{Node(i), Node(j)}

				count, present := replications[pair]
				if !present || count != elem {
					return false
				}
			}
		}
	}

	// number of active vbuckets should correspond to one defined
	// by matrix R
	for n, sum := range r.RowSums {
		if params.NumReplicas != 0 {
			// if we have at least one replica then number
			// of active vbuckets is defined by matrix R
			if sum/params.NumReplicas != activeVBuckets[n] {
				return false
			}
		} else {
			// otherwise matrix R is empty and we just
			// need to check that active vbuckets are
			// distributed evenly
			expected := params.NumVBuckets / params.NumNodes
			if activeVBuckets[n] != expected &&
				activeVBuckets[n] != expected+1 {
				return false
			}
		}
	}

	// number of replica vbuckets should correspond to one defined
	// by R
	for n, sum := range r.ColSums {
		if sum != replicaVBuckets[n] {
			return false
		}
	}

	return true
}

func TestVbmapProperties(t *testing.T) {
	setup(t)

	for _, gen := range allGenerators {

		check := func(params VbmapParams, seed int64) bool {
			return checkVbmapProperties(gen, params, seed)
		}

		err := quick.Check(check, &quick.Config{MaxCount: 250})
		if err != nil {
			t.Error(err)
		}

	}
}

type EqualTagsR1VbmapParams struct {
	VbmapParams
}

func equalTags(numNodes int, numTags int) (tags map[Node]Tag) {
	tags = make(map[Node]Tag)
	tagSize := numNodes / numTags
	tagResidue := numNodes % numTags

	node := 0
	tag := 0
	for numTags > 0 {
		for i := 0; i < tagSize; i++ {
			tags[Node(node)] = Tag(tag)
			node++
		}

		if tagResidue != 0 {
			tags[Node(node)] = Tag(tag)
			node++
			tagResidue--
		}

		tag++
		numTags--
	}

	return
}

func (_ EqualTagsR1VbmapParams) Generate(rand *rand.Rand, size int) reflect.Value {
	numNodes := rand.Int()%100 + 2
	// number of tags is in range [2, numNodes]
	numTags := rand.Int()%(numNodes-1) + 2

	params := VbmapParams{
		Tags:        equalTags(numNodes, numTags),
		NumNodes:    numNodes,
		NumSlaves:   10,
		NumVBuckets: 1024,
		NumReplicas: 1,
	}
	normalizeParams(&params)

	return reflect.ValueOf(EqualTagsR1VbmapParams{params})
}

func checkRIPropertiesTagAware(gen RIGenerator, params VbmapParams) bool {
	ri, err := testBuildRI(params, gen)
	if err != nil {
		switch err {
		case ErrorNoSolution:
			diag.Printf("Couldn't find a solution for params %s", params)

			// cross-check using glpk generator
			gen := makeGlpkRIGenerator()

			// if glpk can find a solution then report an error
			_, err := testBuildRI(params, gen)
			if err == nil {
				return false
			}

			return true
		default:
			return false
		}
	}

	for i, row := range ri.Matrix {
		for j, elem := range row {
			if elem {
				if params.Tags[Node(i)] == params.Tags[Node(j)] {
					return false
				}
			}
		}
	}

	return true
}

func TestRIPropertiesTagAware(t *testing.T) {
	setup(t)

	for _, gen := range tagAwareGenerators {
		check := func(params EqualTagsR1VbmapParams) bool {
			return checkRIPropertiesTagAware(gen, params.VbmapParams)
		}

		err := quick.Check(check, &quick.Config{MaxCount: 250})
		if err != nil {
			t.Error(err)
		}

	}
}

type EqualTagsVbmapParams struct {
	VbmapParams
}

func (_ EqualTagsVbmapParams) Generate(rand *rand.Rand, size int) reflect.Value {
	numNodes := rand.Int()%100 + 2
	numReplicas := rand.Int()%3 + 1
	if numReplicas >= numNodes {
		numReplicas = numNodes - 1
	}

	// number of tags is in range [numReplicas+1, numNodes]
	numTags := rand.Int()%(numNodes-numReplicas) + numReplicas + 1

	params := VbmapParams{
		Tags:        equalTags(numNodes, numTags),
		NumNodes:    numNodes,
		NumSlaves:   10,
		NumVBuckets: 1024,
		NumReplicas: numReplicas,
	}
	normalizeParams(&params)

	return reflect.ValueOf(EqualTagsVbmapParams{params})
}

func checkVbmapTagAware(gen RIGenerator, params VbmapParams) bool {
	_, r, err := testBuildR(params, gen)
	if err != nil {
		if err == ErrorNoSolution {
			diag.Printf("Couldn't find a solution for params %s", params)
			return true
		}

		return false
	}

	for i, row := range r.Matrix {
		counts := make(map[Tag]int)

		for j, vbs := range row {
			if vbs == 0 {
				continue
			}

			tag := params.Tags[Node(j)]
			count, _ := counts[tag]
			counts[tag] = count + vbs
		}

		vbuckets := r.RowSums[i] / params.NumReplicas

		for _, count := range counts {
			if count > vbuckets {
				diag.Printf("Can't generate fully rack aware "+
					"vbucket map for params %s", params)
				return true
			}
		}
	}

	vbmap := buildVbmap(r)
	for _, chain := range vbmap {
		tags := make(map[Tag]bool)

		for _, node := range chain {
			tag := params.Tags[node]
			tags[tag] = true
		}

		if len(chain) != len(tags) {
			return false
		}
	}

	return true
}

func TestVbmapTagAware(t *testing.T) {
	setup(t)

	for _, gen := range tagAwareGenerators {
		check := func(params EqualTagsVbmapParams) bool {
			return checkVbmapTagAware(gen, params.VbmapParams)
		}

		err := quick.Check(check, &quick.Config{MaxCount: 250})
		if err != nil {
			t.Error(err)
		}

	}
}
