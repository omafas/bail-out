// @author Couchbase <info@couchbase.com>
// @copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package main

import (
	"fmt"
	"time"
)

type baseStoredKey struct {
	Name         string `json:"name"`
	Kind         string `json:"kind"`
	CreationTime string `json:"creationTime"`
}

func (b baseStoredKey) name() string {
	return b.Name
}

func (b baseStoredKey) kind() string {
	return b.Kind
}

func validateTimeout(timeoutMs int) error {
	maxTimeoutDuration := 5 * time.Minute
	if int64(timeoutMs) > maxTimeoutDuration.Milliseconds() {
		return fmt.Errorf("timeout greater than 5 minutes not allowed")
	}

	return nil
}
