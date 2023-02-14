// @author Couchbase <info@couchbase.com>
// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// This program generates a bcrypt hash of the specified password.
package main

import (
	"golang.org/x/crypto/bcrypt"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

func mustNoErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var password string
	var cost int
	var debug bool

	flag.IntVar(&cost, "cost", 10, "Cost (4 - 31, inclusive; default 10)")
	flag.BoolVar(&debug, "debug", false, "Debug mode")

	flag.Parse()

	password = os.Getenv("METADATA")
	start := time.Now()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	mustNoErr(err)
	duration := time.Since(start)
	if debug {
		fmt.Println("Password:", password, "Cost:", cost,
		            "Elapsed:", duration, "\n")
	}

	os.Stdout.Write(hash)
}
