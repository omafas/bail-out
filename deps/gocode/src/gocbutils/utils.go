// @author Couchbase <info@couchbase.com>
// @copyright 2019 Couchbase, Inc.
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
package gocbutils

import (
	"runtime"
)

const (
	maxCPUThreads = 4
)

// LimitCPUThreads adjusts the GOMAXPROCS setting to not exceed
// maxCPUThreads. Only meant to be used by the long running go processes
// internal to ns_server. Most of those processes do too little to warrant
// spawning tens of threads on machines that have a large number of CPUs. Yet
// that's what happens with the default GOMAXPROCS setting. This doesn't
// affect the number of threads that golang's runtime might start for doing
// IO, since there's no way to control that.
func LimitCPUThreads() {
	numCPU := runtime.NumCPU()
	if numCPU > maxCPUThreads {
		runtime.GOMAXPROCS(maxCPUThreads)
	}
}
