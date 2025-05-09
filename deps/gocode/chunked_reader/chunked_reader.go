// @author Couchbase <info@couchbase.com>
// @copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
// Used for the sole purpose of validating that we are properly closing the
// chunked-encoded stream(s), _even when there are crashes in ns_server_.
package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	argsWithoutProg := os.Args[1:]
	port, err := strconv.ParseInt(argsWithoutProg[0], 10, 64)
	if err != nil { // no port given, or failed to parse port number from input
		os.Exit(4)
	}
	if len(argsWithoutProg) < 2 { // no password provided
		os.Exit(5)
	}
	password := argsWithoutProg[1]
	client := http.Client{Timeout: time.Minute}
	full_url := fmt.Sprintf("http://127.0.0.1:%d/poolsStreaming/default", port)
	parsed_url, err := url.Parse(full_url)
	if err != nil {
		os.Exit(2)
	}
	request := &http.Request{
		Method:           "GET",
		URL:              parsed_url,
		Header:           map[string][]string{},
		TransferEncoding: []string{"chunked"},
		Close:            true,
		Form:             map[string][]string{},
		PostForm:         map[string][]string{},
		MultipartForm:    &multipart.Form{},
		Trailer:          map[string][]string{},
		TLS:              &tls.ConnectionState{},
		Cancel:           make(<-chan struct{}),
		Response:         &http.Response{},
	}
	request.SetBasicAuth("Administrator", password)
	response, err := client.Do(request)
	maybe_handle_error(err, 0)
	defer response.Body.Close()

	// use the same buffer for everything
	buffer := make([]byte, 4096)

	for {
		if len(response.TransferEncoding) > 0 {
			n, err := response.Body.Read(buffer)
			maybe_handle_error(err, n)
			clear(buffer)
		} else {
			os.Exit(3) // not using chunked encoding?
		}
	}
}

func maybe_handle_error(err error, n int) {
	if err != nil {
		// The reason we can ignore ErrUnexpectedEOF as well is because
		// in this case it's literally the same error as EOF.
		// See chunked.go's Read() implementation which treats EOF as
		// ErrUnexpectedEOF.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			os.Exit(0) // SUCCESS
		} else {
			fmt.Println("[FATAL] Got error: ", err)
			os.Exit(1)
		}
	}
}
