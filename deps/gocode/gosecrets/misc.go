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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"

	"context"
	"os/exec"
	"strings"
	"time"
)

func encrypt(key, data []byte) []byte {
	return encryptWithAD(key, data, nil)
}

func encryptWithAD(key, data, AD []byte) []byte {
	encrypted := aesgcmEncrypt(key, data, AD)
	return append([]byte{0}, encrypted...)
}

func decrypt(key, data []byte) ([]byte, error) {
	return decryptWithAD(key, data, nil)
}

func decryptWithAD(key, data, AD []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, errors.New("ciphertext is too short")
	}
	if data[0] != 0 {
		return nil, errors.New("unsupported cipher")
	}
	return aesgcmDecrypt(key, data[1:], AD)
}

func aesgcmEncrypt(key, data, AD []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	nonce := make([]byte, aesgcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		panic(err.Error())
	}
	return aesgcm.Seal(nonce[:aesgcm.NonceSize()], nonce, data, AD)
}

func aesgcmDecrypt(key, data, AD []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	if len(data) < aesgcm.NonceSize() {
		return nil, errors.New("ciphertext is too short")
	}
	nonce := data[:aesgcm.NonceSize()]
	data = data[aesgcm.NonceSize():]

	return aesgcm.Open(nil, nonce, data, AD)
}

func atomicWriteFile(path string, binary []byte, perm fs.FileMode) (err error) {
	tmpfile, err := ioutil.TempFile(
		filepath.Dir(path), filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tmpfile.Close()
			os.Remove(tmpfile.Name())
		}
	}()

	_, err = tmpfile.Write(binary)
	if err != nil {
		return err
	}

	if err := tmpfile.Chmod(perm); err != nil {
		return err
	}

	tmpfile.Sync()

	if err := tmpfile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpfile.Name(), path)
}

func callExternalScript(cmdStr string, timeoutMs int) (string, error) {
	logDbg("Calling cmd '%s'... ", cmdStr)
	cmdSlice := strings.Fields(cmdStr)
	timeoutDuration := time.Duration(timeoutMs * 1000000)
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()
	cmd := exec.CommandContext(ctx, cmdSlice[0], cmdSlice[1:]...)
	var stdoutbuf, stderrbuf bytes.Buffer
	cmd.Stdout = &stdoutbuf
	cmd.Stderr = &stderrbuf
	err := cmd.Run()
	res := stdoutbuf.String()
	if err != nil {
		return res, fmt.Errorf(
			"command '%s' finished with error (error: '%v', stderr: '%s')",
			cmdStr, err.Error(), stderrbuf.String())
	}
	return res, nil
}

func createRandomKey() []byte {
	return generateRandomBytes(keySize)
}

func generateRandomBytes(size int) []byte {
	dataKey := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, dataKey); err != nil {
		panic(err.Error())
	}
	return dataKey
}
