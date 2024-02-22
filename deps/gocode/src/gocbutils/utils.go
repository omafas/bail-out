// @author Couchbase <info@couchbase.com>
// @copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
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
