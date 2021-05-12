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
	"bytes"
	"fmt"
	"math/rand"
	"time"
)

type Vbmap [][]Node

func (vbmap Vbmap) String() string {
	buffer := &bytes.Buffer{}

	for i, nodes := range vbmap {
		fmt.Fprintf(buffer, "%4d: ", i)
		for _, n := range nodes {
			fmt.Fprintf(buffer, "%3d ", n)
		}
		fmt.Fprintf(buffer, "\n")
	}

	return buffer.String()
}

func makeVbmap(params VbmapParams) (vbmap Vbmap) {
	vbmap = make([][]Node, params.NumVBuckets)
	for v := 0; v < params.NumVBuckets; v++ {
		vbmap[v] = make([]Node, params.NumReplicas+1)
	}

	return
}

type chainCost struct {
	rawCost       int
	tagViolations int
}

var (
	inf = chainCost{MaxInt, MaxInt}
)

func (self chainCost) cmp(other chainCost) int {
	switch {
	case self.tagViolations > other.tagViolations:
		return 1
	case self.tagViolations < other.tagViolations:
		return -1
	default:
		switch {
		case self.rawCost > other.rawCost:
			return 1
		case self.rawCost < other.rawCost:
			return -1
		}
	}

	return 0
}

func (self chainCost) less(other chainCost) bool {
	return self.cmp(other) == -1
}

func (self chainCost) plus(other chainCost) (result chainCost) {
	if self == inf || other == inf {
		return inf
	}

	result.tagViolations = self.tagViolations + other.tagViolations
	result.rawCost = self.rawCost + other.rawCost

	return
}

func (self chainCost) div(denom int) (result chainCost) {
	if self == inf {
		return inf
	}

	result.tagViolations = self.tagViolations
	result.rawCost = self.rawCost / denom

	return
}

func (self chainCost) String() string {
	if self == inf {
		return "inf"
	} else {
		return fmt.Sprintf("{%d %d}", self.rawCost, self.tagViolations)
	}
}

type slavePair struct {
	x, y     Node
	distance int
}

type tagPair struct {
	x, y     Tag
	distance int
}

type pairStats struct {
	slaveStats map[slavePair]int
	tagStats   map[tagPair]int
}

func makePairStats() (stats *pairStats) {
	stats = new(pairStats)
	stats.slaveStats = make(map[slavePair]int)
	stats.tagStats = make(map[tagPair]int)

	return
}

func (s *pairStats) getSlaveStat(x, y Node, distance int) int {
	pair := slavePair{x, y, distance}
	stat, _ := s.slaveStats[pair]

	return stat
}

func (s *pairStats) getTagStat(x, y Tag, distance int) int {
	tagPair := tagPair{x, y, distance}
	stat, _ := s.tagStats[tagPair]

	return stat
}

func (s *pairStats) notePair(x, y Node, xTag, yTag Tag, distance int) {
	pair := slavePair{x, y, distance}

	count, _ := s.slaveStats[pair]
	s.slaveStats[pair] = count + 1

	tagPair := tagPair{xTag, yTag, distance}
	tagCount, _ := s.tagStats[tagPair]
	s.tagStats[tagPair] = tagCount + 1
}

type selectionCtx struct {
	params VbmapParams

	master   Node
	vbuckets int
	slaves   []Node

	slaveCounts map[Node]int
	tagCounts   map[Tag]int

	stats *pairStats
}

func makeSelectionCtx(params VbmapParams, master Node,
	vbuckets int, stats *pairStats) (ctx *selectionCtx) {

	ctx = &selectionCtx{}

	ctx.params = params
	ctx.slaveCounts = make(map[Node]int)
	ctx.tagCounts = make(map[Tag]int)
	ctx.master = master
	ctx.vbuckets = vbuckets
	ctx.stats = stats

	return
}

func (ctx *selectionCtx) getSlaveStat(x, y Node, distance int) int {
	return ctx.stats.getSlaveStat(x, y, distance)
}

func (ctx *selectionCtx) getTagStat(x, y Node, distance int) int {
	xTag := ctx.params.Tags[x]
	yTag := ctx.params.Tags[y]

	return ctx.stats.getTagStat(xTag, yTag, distance)
}

func (ctx *selectionCtx) addSlave(node Node, count int) {
	if _, present := ctx.slaveCounts[node]; present {
		panic("duplicated slave")
	}

	ctx.slaves = append(ctx.slaves, node)
	ctx.slaveCounts[node] = count

	tag := ctx.params.Tags[node]
	tagCount, _ := ctx.tagCounts[tag]
	ctx.tagCounts[tag] = tagCount + count
}

func (ctx *selectionCtx) notePair(x, y Node, distance int) {
	xTag := ctx.params.Tags[x]
	yTag := ctx.params.Tags[y]

	ctx.stats.notePair(x, y, xTag, yTag, distance)
}

func (ctx *selectionCtx) noteChain(chain []Node) {
	for i, node := range chain {
		tag := ctx.params.Tags[node]

		nodeCount := ctx.slaveCounts[node]
		tagCount := ctx.tagCounts[tag]

		if nodeCount == 0 || tagCount == 0 {
			panic("chain refers to tag or node with zero count")
		}

		ctx.slaveCounts[node] = nodeCount - 1
		ctx.tagCounts[tag] = tagCount - 1

		ctx.notePair(ctx.master, node, i)

		for j, otherNode := range chain[i+1:] {
			ctx.notePair(node, otherNode, j)
		}
	}

	ctx.vbuckets -= 1
}

func (ctx *selectionCtx) hasSlaves() bool {
	return len(ctx.slaveCounts) != 0
}

func (ctx *selectionCtx) pairCost(x, y Node, distance int) chainCost {
	stat := ctx.getSlaveStat(x, y, distance)
	tagStat := ctx.getTagStat(x, y, distance)

	xTag := ctx.params.Tags[x]
	yTag := ctx.params.Tags[y]

	viol := B2i(xTag == yTag)
	raw := stat*100 + tagStat*30

	return chainCost{raw, viol}
}

func (ctx *selectionCtx) requiredTags() (result []Tag) {
	result = make([]Tag, 0)
	for tag, count := range ctx.tagCounts {
		if count >= ctx.vbuckets {
			result = append(result, tag)
		}
	}

	return
}

func (ctx *selectionCtx) requiredNodes() (result []Node) {
	result = make([]Node, 0)
	for node, count := range ctx.slaveCounts {
		if count > ctx.vbuckets {
			panic("node has count greater than number of vbuckets left")
		} else if count == ctx.vbuckets {
			result = append(result, node)
		}
	}

	return
}

func (ctx *selectionCtx) availableSlaves() (result []Node) {
	result = make([]Node, 0)
	for _, node := range ctx.slaves {
		if ctx.slaveCounts[node] != 0 {
			result = append(result, node)
		}
	}

	return
}

func (_ *selectionCtx) restoreChain(parent [][]int,
	nodes []Node, t, i int) (chain []Node) {

	chain = make([]Node, t+1)

	for t >= 0 {
		chain[t] = nodes[i]
		i = parent[t][i]
		t--
	}

	return
}

func (ctx *selectionCtx) isFeasibleChain(requiredTags []Tag,
	requiredNodes []Node, chain []Node) bool {

	seenNodes := make(map[Node]bool)
	seenTags := make(map[Tag]bool)

	for _, node := range chain {
		if node == ctx.master {
			return false
		}

		if _, present := seenNodes[node]; present {
			return false
		}

		seenNodes[node] = true

		tag := ctx.params.Tags[node]
		seenTags[tag] = true
	}

	reqTagsCount := 0
	for _, tag := range requiredTags {
		if _, present := seenTags[tag]; present {
			reqTagsCount += 1
		}
	}

	reqNodesCount := 0
	for _, node := range requiredNodes {
		if _, present := seenNodes[node]; present {
			reqNodesCount += 1
		}
	}

	n := len(chain)
	if n-ctx.params.NumReplicas+len(requiredNodes) > reqNodesCount ||
		n-ctx.params.NumReplicas+len(requiredTags) > reqTagsCount {
		return false
	}

	for i, node := range chain {
		if node == ctx.master {
			return false
		}

		for _, other := range chain[i+1:] {
			if node == other {
				return false
			}
		}
	}

	return true
}

func (ctx *selectionCtx) nextBestChain() (result []Node) {
	requiredTags := ctx.requiredTags()
	requiredNodes := ctx.requiredNodes()

	candidates := ctx.availableSlaves()
	candidates = append(candidates, ctx.master)
	numCandidates := len(candidates)

	isFeasible := func(chain []Node) bool {
		return ctx.isFeasibleChain(requiredTags, requiredNodes, chain)
	}

	cost := make([][]chainCost, ctx.params.NumReplicas)
	parent := make([][]int, ctx.params.NumReplicas)

	for i := range cost {
		cost[i] = make([]chainCost, numCandidates)
		parent[i] = make([]int, numCandidates)
	}

	for i, node := range candidates {
		if isFeasible([]Node{node}) {
			cost[0][i] = ctx.pairCost(ctx.master, node, 0)
		} else {
			cost[0][i] = inf
		}
	}

	for t := 1; t < ctx.params.NumReplicas; t++ {
		for i, node := range candidates {
			min := inf
			var minCount int

			for j, _ := range candidates {
				c := cost[t-1][j]

				if c == inf {
					continue
				}

				chain := ctx.restoreChain(parent, candidates, t-1, j)
				chain = append(chain, candidates[i])
				if !isFeasible(chain) {
					continue
				}

				for d := 0; d < t; d++ {
					other := chain[t-d-1]
					c = c.plus(ctx.pairCost(other, node, d))
				}
				c = c.plus(ctx.pairCost(ctx.master, node, t))

				if c.less(min) {
					min = c
					minCount = 1

					parent[t][i] = j
				} else if c == min {
					minCount++

					if rand.Intn(minCount) == 0 {
						parent[t][i] = j
					}
				}
			}

			cost[t][i] = min
		}
	}

	t := ctx.params.NumReplicas - 1
	min := inf
	iMin := -1

	for i := range candidates {
		c := cost[t][i]
		if c.less(min) {
			min = c
			iMin = i
		}
	}

	if iMin == -1 {
		panic("cannot happen")
	}

	result = ctx.restoreChain(parent, candidates, t, iMin)

	return
}

// Construct vbucket map from a matrix R.
func buildVbmap(r R) (vbmap Vbmap) {
	params := r.params
	vbmap = makeVbmap(params)

	// determines how many active vbuckets each node has
	var nodeVbs []int
	if params.NumReplicas == 0 || params.NumSlaves == 0 {
		// If there's only one copy of every vbucket, then matrix R is
		// just a null matrix. So we just spread the vbuckets evenly
		// among the nodes and we're almost done.
		nodeVbs = SpreadSum(params.NumVBuckets, params.NumNodes)
	} else {
		// Otherwise matrix R defines the amount of active vbuckets
		// each node has.
		nodeVbs = make([]int, params.NumNodes)
		for i, sum := range r.RowSums {
			vbs := sum / params.NumReplicas
			if sum%params.NumReplicas != 0 {
				panic("row sum is not multiple of NumReplicas")
			}

			nodeVbs[i] = vbs
		}
	}

	stats := makePairStats()

	vbucket := 0
	for i, row := range r.Matrix {
		vbs := nodeVbs[i]
		ctx := makeSelectionCtx(params, Node(i), vbs, stats)

		for s, count := range row {
			if count != 0 {
				ctx.addSlave(Node(s), count)
			}
		}

		if !ctx.hasSlaves() {
			// Row matrix contained only zeros. This usually means
			// that replica count is zero. Other possibility is
			// that the number of vbuckets is less than number of
			// nodes and some of the nodes end up with no vbuckets
			// at all. In any case, we just mark the node as a
			// master for its vbuckets (if any).

			for vbs > 0 {
				vbmap[vbucket][0] = Node(i)
				vbs--
				vbucket++
			}

			continue
		}

		for vbs > 0 {
			vbmap[vbucket][0] = Node(i)

			chain := ctx.nextBestChain()
			ctx.noteChain(chain)

			copy(vbmap[vbucket][1:], chain)

			vbs--
			vbucket++
		}
	}

	return
}

func tryBuildRI(params *VbmapParams, gen RIGenerator,
	searchParams SearchParams) (ri RI, err error) {

	numSlaves := params.NumSlaves
	numReplicas := params.NumReplicas

	numSlavesCandidates := []int{numSlaves}
	if searchParams.RelaxNumSlaves && numSlaves > 0 {
		low := (numSlaves / numReplicas) * numReplicas
		for i := params.NumSlaves - 1; i >= low; i-- {
			numSlavesCandidates = append(numSlavesCandidates, i)
		}
	}

	var nonstrictRI RI
	nonstrictNumSlaves := -1

	for _, numSlaves := range numSlavesCandidates {
		diag.Printf("Trying to generate RI with NumSlaves=%d", numSlaves)

		params.NumSlaves = numSlaves

		ri, err = gen.Generate(*params, searchParams)
		if err != nil && err != ErrorNoSolution {
			return
		}

		if err == nil {
			if ri.TagAwarenessRank == StrictlyTagAware {
				return
			}

			if nonstrictNumSlaves == -1 ||
				nonstrictRI.TagAwarenessRank > ri.TagAwarenessRank {

				nonstrictRI = ri
				nonstrictNumSlaves = numSlaves
			}
		}
	}

	if nonstrictNumSlaves != -1 {
		params.NumSlaves = nonstrictNumSlaves
		return nonstrictRI, nil
	}

	err = ErrorNoSolution
	return
}

func tryBuildR(params VbmapParams, gen RIGenerator,
	searchParams SearchParams) (ri RI, r R, err error) {

	var nonstrictRI RI
	var nonstrictR R
	foundNonstrict := false

	for i := 0; i < searchParams.NumRIRetries; i++ {
		ri, err = tryBuildRI(&params, gen, searchParams)
		if err != nil {
			return
		}

		r, err = BuildR(params, ri, searchParams)
		if err != nil {
			if err == ErrorNoSolution {
				continue
			}

			return
		}

		if r.Strict {
			diag.Printf("Found feasible R after trying %d RI(s)", i+1)
			return
		}

		if !foundNonstrict ||
			nonstrictR.Evaluation() > r.Evaluation() {

			nonstrictRI = ri
			nonstrictR = r
			foundNonstrict = true

			if ri.TagAwarenessRank != StrictlyTagAware {
				break
			}
		}
	}

	if foundNonstrict {
		return nonstrictRI, nonstrictR, nil
	}

	err = ErrorNoSolution
	return
}

// Generate vbucket map given a generator for matrix RI and vbucket map
// parameters.
func VbmapGenerate(params VbmapParams, gen RIGenerator,
	searchParams SearchParams) (vbmap Vbmap, err error) {

	start := time.Now()

	ri, r, err := tryBuildR(params, gen, searchParams)
	if err != nil {
		return nil, err
	}

	dt := time.Since(start)
	diag.Printf("Generated matrix R in %s (wall clock)", dt)

	diag.Printf("Generated topology:\n%s", ri.String())
	diag.Printf("Final map R:\n%s", r.String())

	vbmap_start := time.Now()

	vbmap = buildVbmap(r)

	dt = time.Since(vbmap_start)
	diag.Printf("Built vbucket map from R in %s (wall clock)", dt)

	dt = time.Since(start)
	diag.Printf("Spent %s overall on vbucket map generation (wall clock)", dt)

	return
}
