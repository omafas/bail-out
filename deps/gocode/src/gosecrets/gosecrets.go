// @author Couchbase <info@couchbase.com>
// @copyright 2016-2017 Couchbase, Inc.
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
package main

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/crypto/pbkdf2"

	"gocbutils"
)

const keySize = 32
const nIterations = 4096

var hmacFun = sha1.New

var salt = [8]byte{20, 183, 239, 38, 44, 214, 22, 141}

type encryptionService struct {
	lockKey          []byte
	encryptedDataKey []byte
	backupDataKey    []byte
	reader           *bufio.Reader
}

func main() {
	gocbutils.LimitCPUThreads()

	s := &encryptionService{
		reader: bufio.NewReader(os.Stdin),
	}
	for {
		s.processCommand()
	}
}

func (s *encryptionService) readCommand() (byte, []byte) {
	var size uint16
	err := binary.Read(s.reader, binary.BigEndian, &size)
	if err == io.EOF {
		// parent died. close normally
		os.Exit(0)
	}
	if err != nil {
		reportReadError(err)
	}
	if size < 1 {
		panic("Command is too short")
	}
	command, err := s.reader.ReadByte()
	if err != nil {
		reportReadError(err)
	}
	if size == 1 {
		return command, nil
	}

	buf := make([]byte, size-1)
	_, err = io.ReadFull(s.reader, buf)
	if err != nil {
		reportReadError(err)
	}
	return command, buf
}

func reportReadError(err error) {
	panic(fmt.Sprintf("Error reading input %v", err))
}

func doReply(data []byte) {
	err := binary.Write(os.Stdout, binary.BigEndian, uint16(len(data)))
	if err != nil {
		panic(fmt.Sprintf("Error writing data %v", err))
	}
	os.Stdout.Write(data)
}

func replySuccessWithData(data []byte) {
	doReply(append([]byte{'S'}, data...))
}

func replySuccess() {
	doReply([]byte{'S'})
}

func replyError(error string) {
	doReply([]byte("E" + error))
}

func encodeKey(key []byte) []byte {
	if key == nil {
		return []byte{0}
	}
	return append([]byte{byte(len(key))}, key...)
}

func combineDataKeys(key1, key2 []byte) []byte {
	return append(encodeKey(key1), encodeKey(key2)...)
}

func (s *encryptionService) replySuccessWithDataKey() {
	replySuccessWithData(combineDataKeys(s.encryptedDataKey, s.backupDataKey))
}

func (s *encryptionService) processCommand() {
	command, data := s.readCommand()

	switch command {
	case 1:
		s.cmdSetPassword(data)
	case 2:
		s.cmdCreateDataKey()
	case 3:
		s.cmdSetDataKey(data)
	case 4:
		s.cmdGetDataKey()
	case 5:
		s.cmdEncrypt(data)
	case 6:
		s.cmdDecrypt(data)
	case 7:
		s.cmdChangePassword(data)
	case 8:
		s.cmdRotateDataKey()
	case 9:
		s.cmdClearBackupKey(data)
	default:
		panic(fmt.Sprintf("Unknown command %v", command))
	}
}

func (s *encryptionService) cmdSetPassword(data []byte) {
	s.lockKey = generateLockKey(data)
	replySuccess()
}

func (s *encryptionService) createDataKey() []byte {
	dataKey := make([]byte, keySize)
	if _, err := io.ReadFull(rand.Reader, dataKey); err != nil {
		panic(err.Error())
	}
	return encrypt(s.lockKey, dataKey)
}

func (s *encryptionService) cmdCreateDataKey() {
	if s.lockKey == nil {
		panic("Password was not set")
	}
	replySuccessWithData(combineDataKeys(s.createDataKey(), nil))
}

func readField(b []byte) ([]byte, []byte) {
	size := b[0]
	return b[1 : size+1], b[size+1:]
}

func (s *encryptionService) cmdSetDataKey(data []byte) {
	if s.lockKey == nil {
		panic("Password was not set")
	}
	encryptedDataKey, data := readField(data)
	backupDataKey, _ := readField(data)

	_, err := decrypt(s.lockKey, encryptedDataKey)
	if err != nil {
		replyError(err.Error())
		return
	}
	if len(backupDataKey) == 0 {
		s.backupDataKey = nil
	} else {
		_, err = decrypt(s.lockKey, backupDataKey)
		if err != nil {
			replyError(err.Error())
			return
		}
		s.backupDataKey = backupDataKey
	}
	s.encryptedDataKey = encryptedDataKey
	replySuccess()
}

func (s *encryptionService) cmdGetDataKey() {
	s.replySuccessWithDataKey()
}

func (s *encryptionService) cmdEncrypt(data []byte) {
	if s.lockKey == nil {
		panic("Password was not set")
	}
	dataKey, err := decrypt(s.lockKey, s.encryptedDataKey)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccessWithData(encrypt(dataKey, data))
}

func (s *encryptionService) decryptWithKey(key []byte, data []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("Unable to decrypt value")
	}
	dataKey, err := decrypt(s.lockKey, key)
	if err != nil {
		return nil, err
	}
	return decrypt(dataKey, data)
}

func (s *encryptionService) cmdDecrypt(data []byte) {
	if s.lockKey == nil {
		panic("Password was not set")
	}
	plaintext, err := s.decryptWithKey(s.encryptedDataKey, data)
	if err == nil {
		replySuccessWithData(plaintext)
		return
	}
	plaintext, err = s.decryptWithKey(s.backupDataKey, data)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccessWithData(plaintext)
}

func (s *encryptionService) cmdChangePassword(data []byte) {
	if s.lockKey == nil {
		panic("Password was not set")
	}
	var backupDataKey []byte
	var err error
	if s.backupDataKey != nil {
		backupDataKey, err = decrypt(s.lockKey, s.backupDataKey)
		if err != nil {
			replyError(err.Error())
			return
		}
	}
	dataKey, err := decrypt(s.lockKey, s.encryptedDataKey)
	if err != nil {
		replyError(err.Error())
		return
	}
	s.lockKey = generateLockKey(data)
	s.encryptedDataKey = encrypt(s.lockKey, dataKey)
	if s.backupDataKey != nil {
		s.backupDataKey = encrypt(s.lockKey, backupDataKey)
	}
	s.replySuccessWithDataKey()
}

func (s *encryptionService) cmdRotateDataKey() {
	if s.lockKey == nil {
		panic("Password was not set")
	}
	if s.backupDataKey != nil {
		replyError("Data key rotation is in progress")
		return
	}
	s.backupDataKey = s.encryptedDataKey
	s.encryptedDataKey = s.createDataKey()
	s.replySuccessWithDataKey()
}

func (s *encryptionService) cmdClearBackupKey(keys []byte) {
	if !bytes.Equal(combineDataKeys(s.encryptedDataKey, s.backupDataKey), keys) {
		replyError("Key mismatch")
		return
	}
	if s.backupDataKey == nil {
		replySuccess()
		return
	}
	s.backupDataKey = nil
	s.replySuccessWithDataKey()
}

func generateLockKey(password []byte) []byte {
	return pbkdf2.Key(password, salt[:], nIterations, keySize, hmacFun)
}

func encrypt(key []byte, data []byte) []byte {
	encrypted := aesgcmEncrypt(key, data)
	return append([]byte{0}, encrypted...)
}

func decrypt(key []byte, data []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, errors.New("ciphertext is too short")
	}
	if data[0] != 0 {
		return nil, errors.New("unsupported cipher")
	}
	return aesgcmDecrypt(key, data[1:len(data)])
}

func aesgcmEncrypt(key []byte, data []byte) []byte {
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
	return aesgcm.Seal(nonce[:aesgcm.NonceSize()], nonce, data, nil)
}

func aesgcmDecrypt(key []byte, data []byte) ([]byte, error) {
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

	return aesgcm.Open(nil, nonce, data, nil)
}
