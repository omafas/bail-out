// @author Couchbase <info@couchbase.com>
// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package main

import (
	"flag"
	"os"
	"log"
	"path/filepath"
	"github.com/evanw/esbuild/pkg/api"
	"io/ioutil"
	"encoding/json"
)

type ImportMap struct {
	Imports map[string]string
}

func getImportMapPlugin(importmapPath string, inDir string) (api.Plugin) {
	plan, _ := ioutil.ReadFile(importmapPath)
	var importmap ImportMap
	if err := json.Unmarshal(plan, &importmap); err != nil {
		panic(err)
	}

	return api.Plugin{
		Name: "ImportMap",
		Setup: func (build api.PluginBuild) {
			//we consider that this is bare specifier from importmap
			//if es import string doesn't start with '/' or '.'
			//or second symbol is not ':', so it covers the following
			//cases '/' and './' and '../' and 'C:\\'
			build.OnResolve(api.OnResolveOptions{Filter: `^[^\.\/][^:]`},
				func (args api.OnResolveArgs) (api.OnResolveResult, error) {
					return api.OnResolveResult{
						Path: filepath.Join(
							inDir, "ui",
							importmap.Imports[args.Path]),
					}, nil
				})
		},
	}
}

func printErrorAndExit(error string) {
	log.Printf(error)
	flag.Usage()
	os.Exit(1)
}

func main() {
	inDir := flag.String("in-dir", "", "path to js source dir (required)")
	outDir := flag.String("out-dir", "", "path to js output dir (required)")
	importmapPath := flag.String("importmap-path", "", "path to importmap.json (required)")
	flag.Parse()
	log.SetFlags(0)

	if *inDir == "" {
		printErrorAndExit("Error: path to js source dir must be specified\n")
	}

	if *outDir == "" {
		printErrorAndExit("Error: path to js source dir must be specified\n")
	}

	if *importmapPath == "" {
		printErrorAndExit("Error: path to importmap.json must be specified\n")
	}

	result := api.Build(api.BuildOptions{
		MinifyWhitespace: true,
		// TODO: figure out why does't work
		// MinifyIdentifiers: true,
		MinifySyntax: true,
		EntryPoints: []string{
			*inDir + "/ui/app/main.js",
		},
		Pure: []string{"console.log"},
		Plugins: []api.Plugin{getImportMapPlugin(*importmapPath, *inDir)},
		Sourcemap: api.SourceMapLinked,
		KeepNames: true,
		Bundle: true,
		PreserveSymlinks: true,
		Splitting: true,
		Write: true,
		Format: api.FormatESModule,
		// LogLevel: api.LogLevelWarning,
		LogLevel: api.LogLevelInfo,
		Outdir: *outDir,
		Loader: map[string]api.Loader{
			".html": api.LoaderText,
		},
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
