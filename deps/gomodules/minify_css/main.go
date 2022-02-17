// @author Couchbase <info@couchbase.com>
// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package main

import (
	"flag"
	"os"
	"log"
	"github.com/evanw/esbuild/pkg/api"
)

func printErrorAndExit(error string) {
	log.Printf(error)
	flag.Usage()
	os.Exit(1)
}

func main() {
	inPath := flag.String("in-path", "", "path to css root module (required)")
	outDir := flag.String("out-dir", "", "path to css output dir (required)")
	flag.Parse()
	log.SetFlags(0)

	if *inPath == "" {
		printErrorAndExit("Error: path to css root module must be specified\n")
	}

	if *outDir == "" {
		printErrorAndExit("Error: path to css out dir must be specified\n")
	}


	result := api.Build(api.BuildOptions{
		MinifyWhitespace: true,
		MinifySyntax: true,
		EntryPoints: []string{
			*inPath,
		},
		Loader: map[string]api.Loader{
			".woff": api.LoaderDataURL,
			".gif": api.LoaderDataURL,
		},
		Bundle: true,
		PreserveSymlinks: true,
		Outdir: *outDir,
		Write: true,
		LogLevel: api.LogLevelInfo,
		Engines: []api.Engine{
			{api.EngineChrome, "67"},
			{api.EngineFirefox, "67"},
			{api.EngineSafari, "11.1"},
			{api.EngineEdge, "80"},
		},
	})

	if len(result.Errors) > 0 {
		os.Exit(1)
	}
}
