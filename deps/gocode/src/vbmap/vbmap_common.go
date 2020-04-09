// @author Couchbase <info@couchbase.com>
// @copyright 2015 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

var (
	ErrorNoSolution = errors.New("Couldn't find a solution")
)

type Node int
type NodeSlice []Node

func (s NodeSlice) Len() int           { return len(s) }
func (s NodeSlice) Less(i, j int) bool { return int(s[i]) < int(s[j]) }
func (s NodeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type Tag int
type TagSlice []Tag
type TagMap map[Node]Tag

func (s TagSlice) Len() int           { return len(s) }
func (s TagSlice) Less(i, j int) bool { return int(s[i]) < int(s[j]) }
func (s TagSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type VbmapParams struct {
	Tags TagMap

	NumNodes    int
	NumSlaves   int
	NumVBuckets int
	NumReplicas int
}

type SearchParams struct {
	NumRIRetries int
	NumRRetries  int

	RelaxTagConstraints bool
	RelaxNumSlaves      bool
	RelaxBalance        bool
}

func (params VbmapParams) Nodes() (nodes []Node) {
	for n := 0; n < params.NumNodes; n++ {
		nodes = append(nodes, Node(n))
	}

	return
}

func (params VbmapParams) String() string {
	return fmt.Sprintf("VbmapParams{Tags: %s, NumNodes: %d, "+
		"NumSlaves: %d, NumVBuckets: %d, NumReplicas: %d}",
		params.Tags, params.NumNodes, params.NumSlaves,
		params.NumVBuckets, params.NumReplicas)
}

func (tags TagMap) String() string {
	return fmt.Sprintf("%v", map[Node]Tag(tags))
}

func (tags TagMap) TagsList() (result []Tag) {
	seen := make(map[Tag]bool)

	for _, t := range tags {
		if _, present := seen[t]; !present {
			result = append(result, t)
			seen[t] = true
		}
	}

	sort.Sort(TagSlice(result))

	return
}

func (tags TagMap) TagsCount() int {
	return len(tags.TagsList())
}

func (tags TagMap) TagsNodesMap() (m map[Tag][]Node) {
	m = make(map[Tag][]Node)
	for _, tag := range tags.TagsList() {
		m[tag] = nil
	}

	for node, tag := range tags {
		m[tag] = append(m[tag], node)
		sort.Sort(NodeSlice(m[tag]))
	}

	return
}

type TagAwarenessRank int

const (
	StrictlyTagAware TagAwarenessRank = iota
	WeaklyTagAware
	NonTagAware
)

type RI struct {
	Matrix           [][]bool
	TagAwarenessRank TagAwarenessRank
}

type RIGenerator interface {
	SetParams(params map[string]string) error
	Generate(params VbmapParams, searchParams SearchParams) (RI, error)
	fmt.Stringer
}

type DontAcceptRIGeneratorParams struct{}

func (_ DontAcceptRIGeneratorParams) SetParams(params map[string]string) error {
	for k, _ := range params {
		return fmt.Errorf("unsupported parameter '%s'", k)
	}

	return nil
}

func (ri RI) String() string {
	buffer := &bytes.Buffer{}

	for _, row := range ri.Matrix {
		for _, elem := range row {
			fmt.Fprintf(buffer, "%2d ", B2i(elem))
		}
		fmt.Fprintf(buffer, "\n")
	}

	return buffer.String()
}
