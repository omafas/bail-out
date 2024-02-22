// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"gocbutils"
)

func readdir(path string) (infos []os.FileInfo, err error) {
	f, err := os.Open(".")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	infos, err = f.Readdir(0)
	return
}

var errorCount int = 0
var lastError error

func traverseTop(entry string) uint64 {
	_, err := os.Stat(entry)
	if os.IsNotExist(err) {
		return 0
	} else {
		// let traverse handle both success and other errors
		return traverse(entry)
	}
}

func traverse(entry string) uint64 {
	var infos []os.FileInfo
	var rv uint64 = 0

	old, err := os.Getwd()
	if err != nil {
		goto exit
	}
	defer os.Chdir(old)

	err = os.Chdir(entry)
	if err != nil {
		goto exit
	}

	infos, err = readdir(".")
	if err != nil {
		errorCount += 1
		lastError = err
		if infos == nil {
			goto exit
		}
	}

	for _, info := range infos {
		mode := info.Mode()
		if (mode & os.ModeType) == 0 {
			rv += (uint64)(info.Size())
		} else if (mode & os.ModeDir) != 0 {
			rv += traverse(info.Name())
		}
	}
	return rv
exit:
	errorCount += 1
	lastError = err
	return 0
}

func doRun(path string) []byte {
	before := time.Now()

	size := traverseTop(path)

	outputMap := map[string]interface{}{
		"size":       size,
		"errorCount": errorCount,
		"lastError":  nil,
	}
	if lastError != nil {
		outputMap["lastError"] = lastError.Error()
	}
	output, err := json.Marshal(outputMap)
	if err != nil {
		panic(err)
	}

	if os.Getenv("GODU_TIMING") != "" {
		after := time.Now()
		duration := after.Sub(before)
		fmt.Fprintf(os.Stderr, "\n%f %f %v\n", (float64)(before.UnixNano())*1E-9, (float64)(after.UnixNano())*1E-9, duration)
	}

	lastError = nil
	errorCount = 0

	return output
}

func readNetString(rd *bufio.Reader) (rv string, err error) {
	lengthS, err := rd.ReadString(':')
	if err != nil {
		return "", err
	}

	lengthS = lengthS[0 : len(lengthS)-1]
	lengthS = strings.TrimSpace(lengthS)
	length, err := strconv.ParseUint(lengthS, 10, 16)
	if err != nil {
		return "", err
	}

	path := make([]byte, length+1)
	_, err = io.ReadFull(rd, path)
	if err != nil {
		return "", err
	}

	if lastCH := path[length]; lastCH != ',' {
		return "", fmt.Errorf("Expected , got %c", lastCH)
	}

	return string(path[:length]), nil
}

func maybePanic(err error) {
	if err != nil {
		panic(err)
	}
}

func runPort() {
	gocbutils.LimitCPUThreads()

	rd := bufio.NewReader(os.Stdin)
	wr := bufio.NewWriter(os.Stdout)

	for {
		path, err := readNetString(rd)
		if err == io.EOF {
			break
		}
		maybePanic(err)

		output := doRun(path)

		_, err = fmt.Fprintf(wr, "%s\n", output)
		maybePanic(err)
		err = wr.Flush()
		maybePanic(err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "working as port\n")
		runPort()
		return
	}

	os.Stdout.Write(doRun(os.Args[1]))
}
