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
	"archive/zip"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	exitSuccess = 0
	exitFailure = 1
)

var (
	zipPath   string
	paths     []string
	prefix    string
	recursive bool
	stripPath bool

	name string
	f    *flag.FlagSet

	errorNotRecursive = errors.New("")
)

func usage(code int) {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintf(os.Stderr, "  %s [options] zipfile [file ...]\n", name)
	fmt.Fprintln(os.Stderr, "Options:")
	f.SetOutput(os.Stderr)
	f.PrintDefaults()

	os.Exit(code)
}

func maybeAddExt(name string) string {
	if filepath.Ext(name) == ".zip" {
		return name
	} else {
		return name + ".zip"
	}
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(exitFailure)
}

func zipifyPath(path string) string {
	path = filepath.Clean(path)
	volume := filepath.VolumeName(path)
	if volume != "" {
		path = path[len(volume):]
	}

	return filepath.ToSlash(strings.TrimPrefix(path, "/"))
}

type walkFn func(string, *os.File, os.FileInfo) error

func walk(root string, fn walkFn) error {
	file, err := os.Open(root)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	err = fn(root, file, info)
	if err != nil {
		return err
	}

	if info.IsDir() {
		children, err := file.Readdirnames(0)
		if err != nil {
			return err
		}

		for _, child := range children {
			err = walk(filepath.Join(root, child), fn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func isRegular(info os.FileInfo) bool {
	return info.Mode()&os.ModeType == 0
}

func compress() {
	zipFile, err := os.Create(zipPath)
	if err != nil {
		fatal("Couldn't create output file: %s", err.Error())
	}

	defer zipFile.Close()

	err = doCompress(zipFile)
	if err != nil {
		fatal("%s", err.Error())
	}
}

func doCompress(zipFile *os.File) error {
	zipInfo, err := zipFile.Stat()
	if err != nil {
		return err
	}

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	fn := func(path string, f *os.File, info os.FileInfo) error {
		if os.SameFile(zipInfo, info) || !(isRegular(info) || info.IsDir()) {
			fmt.Fprintf(os.Stderr, "skipping %s\n", path)
			return nil
		}

		zippedPath := path

		if stripPath {
			zippedPath = filepath.Base(zippedPath)
		}

		if prefix != "" {
			zippedPath = filepath.Join(prefix, zippedPath)
		}
		zippedPath = zipifyPath(zippedPath)

		var w io.Writer

		if (zippedPath != "" && zippedPath != ".") || !info.IsDir() {
			if info.IsDir() {
				zippedPath += "/"
			}

			fmt.Fprintf(os.Stderr, "adding: %s -> %s\n", path, zippedPath)

			header, err := zip.FileInfoHeader(info)
			if err != nil {
				return err
			}
			header.Name = zippedPath
			header.Method = zip.Deflate

			w, err = zipWriter.CreateHeader(header)
			if err != nil {
				return err
			}

		}

		if info.IsDir() {
			if recursive {
				return nil
			} else {
				return errorNotRecursive
			}
		}

		_, err = io.Copy(w, f)
		if err != nil {
			return fmt.Errorf("failed to copy %s: %s", path, err.Error())
		}

		return nil
	}

	for _, path := range paths {
		err = walk(path, fn)
		if err != nil && err != errorNotRecursive {
			return err
		}
	}

	return nil
}

func main() {
	name = os.Args[0]

	f = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)

	f.BoolVar(&recursive, "recursive", false, "scan directories recursively")
	f.BoolVar(&stripPath, "strip-path", false, "store just file names")
	f.StringVar(&prefix, "prefix", "", "prepend each path with prefix")

	err := f.Parse(os.Args[1:])
	if err == nil {
		switch {
		case recursive && stripPath:
			err = fmt.Errorf("-recursive and -strip-path are mutually exclusive")
		case f.NArg() == 0:
			err = fmt.Errorf("output zip file is not specified")
		default:
			zipPath = maybeAddExt(f.Arg(0))
			paths = f.Args()[1:]
		}
	}

	if err != nil {
		switch err {
		case flag.ErrHelp:
			usage(exitSuccess)
		default:
			fmt.Fprintln(os.Stderr, err)
			usage(exitFailure)
		}
	}

	compress()
}
