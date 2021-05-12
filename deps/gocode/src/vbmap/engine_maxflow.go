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
	"math/rand"
	"sort"
)

type MaxFlowRIGenerator struct {
	dotPath    string
	dotVerbose bool
}

func makeMaxFlowRIGenerator() *MaxFlowRIGenerator {
	return &MaxFlowRIGenerator{dotPath: "", dotVerbose: false}
}

func (_ MaxFlowRIGenerator) String() string {
	return "maxflow"
}

func (gen *MaxFlowRIGenerator) SetParams(params map[string]string) error {
	for k, v := range params {
		switch k {
		case "dot":
			gen.dotPath = v
		case "dotVerbose":
			gen.dotVerbose = true
		default:
			return fmt.Errorf("unsupported parameter '%s'", k)
		}
	}

	return nil
}

func (gen MaxFlowRIGenerator) Generate(params VbmapParams,
	searchParams SearchParams) (ri RI, err error) {

	g := buildFlowGraph(params)

	diag.Print("Constructed flow graph.\n")
	diag.Print(g.graphStats)

	rank := StrictlyTagAware
	feasible, _ := g.FindFeasibleFlow()

	if !feasible && searchParams.RelaxTagConstraints {
		rank = WeaklyTagAware
		relaxMaxSlavesPerTag(g, params)
		feasible, _ = g.FindFeasibleFlow()
		if feasible {
			diag.Printf("Managed to generate RI with relaxed " +
				"number of slaves per tag")
		} else {
			rank = NonTagAware
			params.Tags = TrivialTags(params.NumNodes)
			g = buildFlowGraph(params)

			feasible, _ = g.FindFeasibleFlow()
			if feasible {
				diag.Printf("Managed to generate RI after " +
					"abandoning tag constraints")
			}
		}
	}

	if gen.dotPath != "" {
		err := g.Dot(gen.dotPath, gen.dotVerbose)
		if err != nil {
			diag.Printf("Couldn't create dot file %s: %s",
				gen.dotPath, err.Error())
		}
	}

	if !feasible {
		err = ErrorNoSolution
		return
	}

	ri = graphToRI(g, params)
	ri.TagAwarenessRank = rank
	return
}

func buildFlowGraph(params VbmapParams) (g *Graph) {
	graphName := fmt.Sprintf("Flow graph for RI (%s)", params)
	g = NewGraph(graphName)

	tags := params.Tags.TagsList()
	tagsNodes := params.Tags.TagsNodesMap()

	maxReplicationsPerTag := 0
	if params.NumReplicas != 0 {
		maxReplicationsPerTag = params.NumSlaves / params.NumReplicas
	}

	nodes := params.Nodes()
	for _, nodeIx := range rand.Perm(len(nodes)) {
		node := nodes[nodeIx]
		nodeTag := params.Tags[node]
		nodeSrcV := NodeSourceVertex(node)
		nodeSinkV := NodeSinkVertex(node)

		g.AddEdge(Source, nodeSrcV, params.NumSlaves, params.NumSlaves)
		g.AddEdge(nodeSinkV, Sink, params.NumSlaves, 0)

		for _, tagIx := range rand.Perm(len(tags)) {
			tag := tags[tagIx]

			if tag == nodeTag {
				continue
			}

			tagNodesCount := len(tagsNodes[tag])
			tagCapacity := Min(tagNodesCount, maxReplicationsPerTag)

			tagV := TagVertex(tag)
			g.AddEdge(nodeSrcV, tagV, tagCapacity, 0)
		}
	}

	for _, tagIx := range rand.Perm(len(tags)) {
		tag := tags[tagIx]
		tagNodes := tagsNodes[tag]
		tagV := TagVertex(tag)

		for _, tagNode := range tagNodes {
			tagNodeV := NodeSinkVertex(tagNode)

			g.AddEdge(tagV, tagNodeV, params.NumSlaves, 0)
		}
	}

	return
}

func relaxMaxSlavesPerTag(g *Graph, params VbmapParams) {
	tagsNodes := params.Tags.TagsNodesMap()

	for _, vertex := range g.Vertices() {
		if tagV, ok := vertex.(TagVertex); ok {
			for _, edge := range g.EdgesToVertex(tagV) {
				tagNodesCount := len(tagsNodes[Tag(tagV)])
				edge.IncreaseCapacity(tagNodesCount)
			}
		}
	}
}

type nodeCount struct {
	node  Node
	count int
}

type nodeCountSlice []nodeCount

func (a nodeCountSlice) Len() int           { return len(a) }
func (a nodeCountSlice) Less(i, j int) bool { return a[i].count > a[j].count }
func (a nodeCountSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func graphToRI(g *Graph, params VbmapParams) (ri RI) {
	ri.Matrix = make([][]bool, params.NumNodes)

	for i := range params.Nodes() {
		ri.Matrix[i] = make([]bool, params.NumNodes)
	}

	for _, tag := range params.Tags.TagsList() {
		tagV := TagVertex(tag)

		inRepsCounts := make(nodeCountSlice, 0)
		outRepsCounts := make(nodeCountSlice, 0)

		for _, edge := range g.EdgesFromVertex(tagV) {
			// edge to node sink vertex
			dstNode := Node(edge.Dst.(NodeSinkVertex))

			count := nodeCount{dstNode, edge.Flow()}
			inRepsCounts = append(inRepsCounts, count)
		}

		for _, edge := range g.EdgesToVertex(tagV) {
			srcNode := Node(edge.Src.(NodeSourceVertex))

			count := nodeCount{srcNode, edge.Flow()}
			outRepsCounts = append(outRepsCounts, count)
		}

		sort.Sort(outRepsCounts)

		slavesLeft := len(inRepsCounts)
		slaveIx := 0

		for _, pair := range outRepsCounts {
			count := pair.count
			srcNode := int(pair.node)

			for count > 0 {
				if slavesLeft == 0 {
					panic(fmt.Sprintf("Ran out of slaves "+
						"on tag %v", tag))
				}

				for inRepsCounts[slaveIx].count == 0 {
					slaveIx = (slaveIx + 1) % len(inRepsCounts)
				}

				dstNode := int(inRepsCounts[slaveIx].node)

				if ri.Matrix[srcNode][dstNode] {
					panic(fmt.Sprintf("Forced to use the "+
						"same slave %d twice (tag %v)",
						dstNode, tag))
				}

				ri.Matrix[srcNode][dstNode] = true
				count -= 1

				inRepsCounts[slaveIx].count -= 1
				if inRepsCounts[slaveIx].count == 0 {
					slavesLeft -= 1
				}

				slaveIx = (slaveIx + 1) % len(inRepsCounts)
			}
		}
	}

	return
}
