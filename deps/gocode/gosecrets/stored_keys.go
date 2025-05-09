// @author Couchbase <info@couchbase.com>
// @copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/couchbase/ns_server/deps/gocode/awsutils"
	"github.com/couchbase/ns_server/deps/gocode/kmiputils"
	"github.com/google/uuid"
)

var testData = [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
var testAD = [4]byte{255, 254, 253, 252}
var intTokenEncryptionKeyKind = "configDek"

// Tags for enconding Kmip encrypted data usage
const KMIP_USE_GET = uint8(0x00)
const KMIP_USE_ENCR_DECR = uint8(0x01)

const KMIP_MAX_IV_SIZE = 128

// Stored keys structs and interfaces:

// Configuration for stored keys, describes where to store keys, and what
// kind of key should encrypt it. It is read from main gosecrets config.
// Typical configuration can have two kinds of keys:
// [{"kind": "dek", "path": "/path/to/dek", "encryptByKind": "kek"},
//  {"kind": "kek", "path": "/path/to/kek", "encryptByKind": "encryptionService"}]

type storedKeyConfig struct {
	KeyKind       string `json:"kind"`
	Path          string `json:"path"`
	EncryptByKind string `json:"encryptByKind"`
}

// Low level interface that all kinds of stored keys should support
type storedKeyIface interface {
	name() string
	kind() string
	needRewrite(*storedKeyConfig, *StoredKeysState, *storedKeysCtx) (bool, int, error)
	ad() []byte
	encryptMe(*StoredKeysState, *storedKeysCtx) error
	decryptMe(bool, *StoredKeysState, *storedKeysCtx) error
	encryptData([]byte, []byte) ([]byte, error)
	decryptData([]byte, []byte) ([]byte, error)
	marshal() (storedKeyType, []byte, error)
	unmarshal(json.RawMessage) error
	usesSecretManagementKey() bool
	canBeCached() bool
}

// Struct for marshalling/unmarshalling of a generic stored key
type storedKeyJson struct {
	Type  storedKeyType   `json:"type"`
	Proof string          `json:"proof"`
	Raw   json.RawMessage `json:"keyData"`
}

type storedKeyType string

const (
	rawAESGCMKey storedKeyType = "raw-aes-gcm"
	awskmKey     storedKeyType = "awskm"
	kmipKey      storedKeyType = "kmip"

	// Magic string used for encrypted file headers
	encryptedFileMagicString    = "\x00Couchbase Encrypted\x00"
	encryptedFileMagicStringLen = len(encryptedFileMagicString)
	encryptedFileHeaderSize     = 80
	encryptedFileKeyNameLength  = byte(36)
	macLen                      = 101 // Vsn: 1B, UUID: 36B, MAC: 64B
)

// Struct for marshalling/unmarshalling of a raw aes-gcm stored key
type rawAesGcmStoredKeyJson struct {
	Name              string `json:"name"`
	KeyKind           string `json:"kind"`
	SealedKeyData     []byte `json:"sealedKeyData"`
	EncryptedByKind   string `json:"encryptedByKind"`
	EncryptionKeyName string `json:"encryptionKeyName"`
	CreationTime      string `json:"creationTime"`
	CanBeCached       bool   `json:"canBeCached"`
}

// Struct represents raw aes-gcm stored key
type rawAesGcmStoredKey struct {
	Name              string
	Kind              string
	DecryptedKey      []byte
	EncryptedKey      []byte
	EncryptedByKind   string
	EncryptionKeyName string
	CreationTime      string
	CanBeCached       bool
}

type awsStoredKey struct {
	Name         string `json:"name"`
	Kind         string `json:"kind"`
	KeyArn       string `json:"keyArn"`
	Region       string `json:"region"`
	ConfigFile   string `json:"configFile"`
	CredsFile    string `json:"credsFile"`
	Profile      string `json:"profile"`
	UseIMDS      bool   `jspn:"useIMDS"`
	CreationTime string `json:"creationTime"`
}

type kmipStoredKey struct {
	Name                string `json:"name"`
	Kind                string `json:"kind"`
	KmipId              string `json:"kmipId"`
	Host                string `json:"host"`
	Port                int    `json:"port"`
	ReqTimeoutMs        int    `json:"reqTimeoutMs"`
	CaSelection         string `json:"caSelection"`
	CbCaPath            string `json:"CbCaPath"`
	EncryptionApproach  string `json:"encryptionApproach"`
	KeyPath             string `json:"keyPath"`
	CertPath            string `json:"certPath"`
	EncryptedPassphrase []byte `json:"sealedPassphrase"`
	decryptedPassphrase []byte `json:"-"`
	EncryptionKeyName   string `json:"encryptionKeyName"`
	EncryptedByKind     string `json:"encryptedByKind"`
	CreationTime        string `json:"creationTime"`
}

// Note: main difference between ctx and state is that the ctx can change
// independently from stored keys functionality (in go secrets),
// while the state is created and managed by this file only
type storedKeysCtx struct {
	storedKeyConfigs           []storedKeyConfig
	encryptionServiceKey       []byte
	backupEncryptionServiceKey []byte
	keysTouched                map[string]bool
}

type StoredKeysState struct {
	readOnly          bool
	intTokensFile     string
	intTokens         []intToken
	encryptionKeyName string // Name of the encryption key that is used to encrypt the integrity tokens file
	keysCache         map[string]storedKeyIface
}

type intToken struct {
	uuid  string
	token []byte
}

type readKeyReply struct {
	Type string          `json:"type"`
	Info json.RawMessage `json:"info"`
}

type readKeyAesKeyResponse struct {
	Key             string `json:"key"`
	EncryptionKeyId string `json:"encryptionKeyId"`
	CreationTime    string `json:"creationTime"`
}

// Error types

type ErrKeyNotFound struct {
	e error
}

func (s ErrKeyNotFound) Error() string { return s.e.Error() }
func (s ErrKeyNotFound) Unwrap() error { return s.e }

// Stored keys managenement functions

func initStoredKeys(configDir string, readOnly bool, ctx *storedKeysCtx) (*StoredKeysState, error) {
	// Construct the path to the stored_keys_tokens file
	tokensPath := filepath.Join(configDir, "stored_keys_tokens")

	// Initialize empty state
	state := &StoredKeysState{
		readOnly:      readOnly,
		intTokensFile: tokensPath,
		keysCache:     make(map[string]storedKeyIface),
	}

	err := state.readIntTokensFromFile(ctx)
	if err != nil {
		return nil, err
	}

	if len(state.intTokens) == 0 {
		if !state.readOnly {
			logDbg("No integrity tokens found, generating new one")
			err = state.generateIntToken(ctx)
			if err != nil {
				return nil, err
			}
		} else {
			logDbg("No integrity tokens found, but read-only mode is enabled, not generating integrity token")
		}
	}

	return state, nil
}

func (state *StoredKeysState) rotateIntegrityTokens(keyName string, ctx *storedKeysCtx) error {
	if state.readOnly {
		return fmt.Errorf("read-only mode is enabled")
	}

	// It is important to use new key to encrypt the integrity tokens file
	// because otherwise we can store newly generated token unencrypted
	logDbg("Rotating integrity tokens, and encrypting file with key %s (old key: %s)", keyName, state.encryptionKeyName)
	oldKeyName := state.encryptionKeyName
	state.encryptionKeyName = keyName
	err := state.generateIntToken(ctx)
	if err != nil {
		state.encryptionKeyName = oldKeyName
		return err
	}

	return nil
}

func (state *StoredKeysState) removeOldIntegrityTokens(paths []string, ctx *storedKeysCtx) error {
	if state.readOnly {
		return fmt.Errorf("read-only mode is enabled")
	}

	// Before removing old integrity tokens we should regenerate proofs for
	// every key on disk. The point is to make sure all keys have proofs that
	// are generated with the most recent token, so we can remove old tokens
	for _, path := range paths {
		Names, err := scanDir(path)

		logDbg("Regenerating proofs for all keys in %s (found %d keys)", path, len(Names))
		if err != nil {
			return err
		}
		for _, name := range Names {
			ctx.keysTouched = make(map[string]bool)
			err := state.maybeRegenerateProof(path, name, ctx)
			if err != nil {
				return err
			}
		}
	}
	logDbg("Successfully regenerated proofs for all keys. Removing old tokens now")
	prevTokens := state.intTokens
	state.intTokens = state.intTokens[:1]
	err := state.writeIntTokensToFile(ctx)
	if err != nil {
		state.intTokens = prevTokens
		return err
	}
	for _, token := range prevTokens {
		logDbg("Removing old token with uuid %s", token.uuid)
	}
	return nil
}

func (state *StoredKeysState) maybeRegenerateProof(path, name string, ctx *storedKeysCtx) error {
	filePathPrefix := storedKeyPathPrefix(path, name)
	keyIface, vsn, proof, err := readKeyFromFileRaw(filePathPrefix)
	if err != nil {
		logDbg("Failed to read key %s from file %s: %s", name, filePathPrefix, err.Error())
		return err
	}
	parts := strings.Split(proof, ":")
	if len(parts) != 2 {
		return fmt.Errorf("file %s has invalid proof format", filePathPrefix)
	}
	uuid := parts[0]
	if uuid == state.intTokens[0].uuid {
		// Proof is still valid, no need to regenerate proof
		return nil
	}
	logDbg("Regenerating proof for key %s from file %s (token uuid in file: %s, expected: %s)", name, filePathPrefix, uuid, state.intTokens[0].uuid)
	// decrypting key because (1) we need to validate proof
	//                        (2) write key expects key to be unencrypted
	err = state.decryptKey(keyIface, true, proof, ctx)
	if err != nil {
		return fmt.Errorf("failed to decrypt key %s: %s", name, err.Error())
	}
	err = state.writeKeyToFile(keyIface, vsn, filePathPrefix)
	if err != nil {
		return fmt.Errorf("failed to write key with regenerated proof %s to file %s: %s", name, filePathPrefix, err.Error())
	}
	return nil
}

func (state *StoredKeysState) generateIntToken(ctx *storedKeysCtx) error {
	newToken := intToken{
		uuid:  uuid.New().String(),
		token: createRandomKey(),
	}
	prevTokens := state.intTokens
	state.intTokens = append([]intToken{newToken}, state.intTokens...)
	logDbg("Generated new token with uuid %s", newToken.uuid)
	err := state.writeIntTokensToFile(ctx)
	if err != nil {
		state.intTokens = prevTokens
		return err
	}
	return nil
}

func (state *StoredKeysState) readIntTokensFromFile(ctx *storedKeysCtx) error {
	logDbg("Reading integrity tokens from file %s", state.intTokensFile)
	state.intTokens = make([]intToken, 0)
	state.encryptionKeyName = ""
	// Read the file
	data, err := os.ReadFile(state.intTokensFile)
	if err != nil {
		if os.IsNotExist(err) {
			logDbg("Stored keys tokens file %s doesn't exist, it must be the first run", state.intTokensFile)
			return nil
		}
		return fmt.Errorf("failed to read stored keys tokens file: %s", err.Error())
	}

	baseFilename := filepath.Base(state.intTokensFile)
	data, encryptionKeyName, err := state.maybeDecryptFileData(baseFilename, data, ctx)
	if err != nil {
		return fmt.Errorf("failed to decrypt stored keys tokens file: %s", err.Error())
	}

	if !utf8.Valid(data) {
		return fmt.Errorf("invalid utf-8 in stored keys tokens file")
	}

	state.encryptionKeyName = encryptionKeyName

	// Split the file content into lines
	lines := strings.Split(string(data), "\n")

	// Process each line
	for i, line := range lines {
		// Skip empty lines
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			return fmt.Errorf("invalid token on line %d: %s", i+1, line)
		}

		uuid := strings.TrimSpace(parts[0])
		token, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return fmt.Errorf("invalid base64 token on line %d: %s", i+1, err.Error())
		}
		logDbg("Read token with uuid %s", uuid)
		state.intTokens = append(state.intTokens, intToken{
			uuid:  uuid,
			token: token,
		})
	}
	return nil
}

func (state *StoredKeysState) writeIntTokensToFile(ctx *storedKeysCtx) error {
	logDbg("Writing integrity tokens to file %s (encrypting with key %s)", state.intTokensFile, state.encryptionKeyName)
	// Create a buffer to store the encoded tokens
	var buffer bytes.Buffer

	// Encode each token in base64 and write to buffer with newline
	for _, token := range state.intTokens {
		buffer.WriteString(token.uuid)
		buffer.WriteString(",")
		buffer.WriteString(base64.StdEncoding.EncodeToString(token.token))
		buffer.WriteString("\n")
	}

	encryptedData, err := state.encryptFileData(
		buffer.Bytes(),
		state.encryptionKeyName,
		ctx,
	)
	if err != nil {
		logDbg("Failed to encrypt stored keys tokens file: %s", err.Error())
		return err
	}
	err = atomicWriteFile(state.intTokensFile, encryptedData, 0640)
	if err != nil {
		logDbg("Failed to write stored keys tokens file: %s", err.Error())
		return err
	}
	return nil
}

func (state *StoredKeysState) mac(data []byte) ([]byte, error) {
	if len(state.intTokens) == 0 {
		return nil, fmt.Errorf("no keys")
	}
	token := state.intTokens[0]
	h := hmac.New(sha512.New, token.token)
	h.Write(data)
	mac := h.Sum(nil)
	// Format: Version: 1 byte, TokenUUID, MAC
	res := append([]byte{0}, token.uuid...)
	res = append(res, mac...)
	if len(res) != macLen {
		return nil, fmt.Errorf("unexpected mac length: %d", len(res))
	}
	return res, nil
}

func (state *StoredKeysState) verifyMac(mac, data []byte) error {
	if len(mac) == 0 {
		return fmt.Errorf("mac is empty")
	}
	if mac[0] != 0 {
		return fmt.Errorf("unknown mac version: %v", mac[0])
	}

	if len(mac) != macLen {
		return fmt.Errorf("unexpected mac length: %d", len(mac))
	}
	uuidBytes := mac[1:37]
	if !utf8.Valid(uuidBytes) {
		return fmt.Errorf("invalid utf-8 in uuid: %q", uuidBytes)
	}
	uuid := string(uuidBytes)
	mac = mac[37:]
	for _, token := range state.intTokens {
		if token.uuid == uuid {
			h := hmac.New(sha512.New, token.token)
			h.Write(data)
			expectedMac := h.Sum(nil)
			if hmac.Equal(mac, expectedMac) {
				return nil
			}
			return fmt.Errorf("invalid mac")
		}
	}
	return fmt.Errorf("unknown token: %s", uuid)
}

func (state *StoredKeysState) revalidateKeyCache(ctx *storedKeysCtx) {
	for _, cachedKeyIface := range state.keysCache {
		keySettings, err := getStoredKeyConfig(cachedKeyIface.kind(), ctx.storedKeyConfigs)
		shouldInvalidate := false
		if err != nil {
			logDbg("Failed to get stored key config for %s: %s", cachedKeyIface.kind(), err.Error())
			shouldInvalidate = true
		} else {
			keyPathPrefix := storedKeyPathPrefix(keySettings.Path, cachedKeyIface.name())
			if !isKeyFile(keyPathPrefix) {
				logDbg("Key file %s not found, removing from cache", keyPathPrefix)
				shouldInvalidate = true
			}
		}
		if shouldInvalidate {
			delete(state.keysCache, cachedKeyIface.name())
		}
	}
}

func getEncryptedFileAD(header []byte, offset int) []byte {
	ad := make([]byte, len(header)+8)
	copy(ad, header)
	binary.BigEndian.PutUint64(ad[len(header):], uint64(offset))
	return ad
}

// Note: function encrypts everything as a single chunk
func (state *StoredKeysState) encryptFileData(data []byte, encryptionKeyName string, ctx *storedKeysCtx) ([]byte, error) {
	if encryptionKeyName == "" {
		// If no encryption key name provided, the data doesn't need to be encrypted
		return data, nil
	}

	const (
		version         = byte(0)
		compressionType = byte(0)
	)

	// Create header
	header := make([]byte, encryptedFileHeaderSize)
	copy(header, encryptedFileMagicString)
	header[encryptedFileMagicStringLen] = version
	header[encryptedFileMagicStringLen+1] = compressionType
	// 4 bytes reserved (zeros)
	header[encryptedFileMagicStringLen+6] = encryptedFileKeyNameLength

	// Pad or truncate encryptionKeyName to exactly 36 bytes
	keyNameBytes := make([]byte, encryptedFileKeyNameLength)
	copy(keyNameBytes, encryptionKeyName)
	copy(header[encryptedFileMagicStringLen+7:], keyNameBytes)

	salt := generateRandomBytes(16)
	copy(header[encryptedFileMagicStringLen+7+int(encryptedFileKeyNameLength):], salt)

	ad := getEncryptedFileAD(header, encryptedFileHeaderSize)

	// Read the encryption key and use it to encrypt the data
	key, err := state.readKey(encryptionKeyName, intTokenEncryptionKeyKind, true, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read config dek: %s", err.Error())
	}

	encryptedData, err := key.encryptData(data, ad)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt data: %s", err.Error())
	}

	// Create chunk (4 bytes size + encrypted data)
	chunkSize := uint32(len(encryptedData))
	chunk := make([]byte, 4+len(encryptedData))
	binary.BigEndian.PutUint32(chunk[:4], chunkSize)
	copy(chunk[4:], encryptedData)

	// Combine header and chunk
	result := make([]byte, len(header)+len(chunk))
	copy(result, header)
	copy(result[len(header):], chunk)

	return result, nil
}

// Decrypts arbitraty "Couchbase Encrypted" file data with two caveats:
// 1. It doesn't validate the proof of the key
// 2. It assumes that file contains only one chunk
func (state *StoredKeysState) maybeDecryptFileData(filename string, data []byte, ctx *storedKeysCtx) ([]byte, string, error) {
	// Check if data is long enough to contain magic string
	if len(data) < encryptedFileMagicStringLen {
		logDbg("Data is too short to contain magic string, must be unencrypted")
		return data, "", nil
	}

	logDbg("Decrypting file %s, data length: %d", filename, len(data))

	// Check for magic string
	if !bytes.Equal(data[:encryptedFileMagicStringLen], []byte(encryptedFileMagicString)) {
		logDbg("No magic string found, must be unencrypted")
		return data, "", nil
	}

	// Validate header format
	header := data[:encryptedFileHeaderSize]
	keyName, err := validateEncryptedFileHeader(header)
	if err != nil {
		return nil, "", err
	}

	logDbg("File is encrypted with key %s", keyName)

	// Chunk format is 4 bytes size + encrypted data
	chunk := data[encryptedFileHeaderSize:]
	if len(chunk) < 4 {
		return nil, "", fmt.Errorf("encrypted file too short to contain chunk size")
	}
	chunkSize := binary.BigEndian.Uint32(chunk[:4])
	if len(chunk) < 4+int(chunkSize) {
		return nil, "", fmt.Errorf("encrypted file shorter than specified chunk size")
	}

	// Read the encryption key and use it to decrypt the data
	key, err := state.readKey(keyName, intTokenEncryptionKeyKind, false, ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read config dek: %s", err.Error())
	}

	ad := getEncryptedFileAD(header, encryptedFileHeaderSize)
	decryptedData, err := key.decryptData(chunk[4:], ad)
	if err != nil {
		return nil, "", fmt.Errorf("failed to decrypt data: %s", err.Error())
	}

	return decryptedData, keyName, nil
}

func validateEncryptedFileHeader(header []byte) (string, error) {
	const (
		version         = byte(0)
		compressionType = byte(0)
	)

	if len(header) < encryptedFileHeaderSize {
		return "", fmt.Errorf("encrypted file header too short")
	}

	if header[encryptedFileMagicStringLen] != version {
		return "", fmt.Errorf("unsupported version: %d", header[encryptedFileMagicStringLen])
	}

	if header[encryptedFileMagicStringLen+1] != compressionType {
		return "", fmt.Errorf("unsupported compression type: %d", header[encryptedFileMagicStringLen+1])
	}

	if header[encryptedFileMagicStringLen+6] != encryptedFileKeyNameLength {
		return "", fmt.Errorf("invalid key name length: %d", header[encryptedFileMagicStringLen+6])
	}

	keyNameBytes := header[encryptedFileMagicStringLen+7 : encryptedFileMagicStringLen+7+int(encryptedFileKeyNameLength)]
	if !utf8.Valid(keyNameBytes) {
		return "", fmt.Errorf("invalid utf-8 in key name: %q", keyNameBytes)
	}

	keyName := string(keyNameBytes)

	return keyName, nil
}

func (state *StoredKeysState) getKeyIdInUse() (string, error) {
	// We assume that file exists here, because we always create it in init

	// Read first encryptionFileHeaderSize bytes from the file
	data := make([]byte, encryptedFileHeaderSize)
	file, err := os.Open(state.intTokensFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Read(data)
	if err != nil {
		return "", err
	}

	if len(data) < encryptedFileMagicStringLen {
		// Too short for magic string, must be unencrypted
		return "", nil
	}

	// Check for magic string
	if !bytes.Equal(data[:encryptedFileMagicStringLen], []byte(encryptedFileMagicString)) {
		// No magic string found, must be unencrypted
		return "", nil
	}

	// Try parsing as encrypted file header
	keyName, err := validateEncryptedFileHeader(data)
	if err != nil {
		return "", err
	}

	return keyName, nil
}

func (state *StoredKeysState) storeKey(
	name,
	kind,
	keyType string,
	encryptionKeyName,
	creationTime string,
	testOnly bool,
	canBeCached bool,
	otherData []byte,
	ctx *storedKeysCtx,
) error {
	if state.readOnly {
		return fmt.Errorf("read-only mode is enabled")
	}
	keySettings, err := getStoredKeyConfig(kind, ctx.storedKeyConfigs)
	if err != nil {
		return err
	}
	var keyInfo storedKeyIface
	if keyType == string(rawAESGCMKey) {
		keyInfo = newAesGcmKey(name, kind, creationTime, encryptionKeyName, canBeCached, otherData)
	} else if keyType == string(awskmKey) {
		keyInfo, err = newAwsKey(name, kind, creationTime, canBeCached, otherData)
		if err != nil {
			return err
		}
	} else if keyType == string(kmipKey) {
		keyInfo, err = newKmipKey(name, kind, creationTime, encryptionKeyName, canBeCached, otherData)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unknown type: %s", keyType)
	}

	shouldRewrite, vsn, err := keyInfo.needRewrite(keySettings, state, ctx)
	if err != nil {
		return err
	}

	if !shouldRewrite && !testOnly {
		// key is already on disk and encrypted with the correct key
		logDbg("Key %s is already on disk, will do nothing", name)
		return nil
	}

	err = state.encryptKey(keyInfo, ctx)
	if err != nil {
		return err
	}

	if testOnly {
		encryptedTestData, err := keyInfo.encryptData(testData[:], testAD[:])
		if err != nil {
			return fmt.Errorf("encryption failed: %s", err.Error())
		}
		decryptedData, err := keyInfo.decryptData(encryptedTestData, testAD[:])
		if err != nil {
			return fmt.Errorf("decryption failed: %s", err.Error())
		}
		if !bytes.Equal(testData[:], decryptedData) {
			return fmt.Errorf("encrypted and decrypted data doesn't match the original data")
		}
		logDbg("Key %s test succeeded", name)
		return nil
	}

	err = state.writeKeyToDisk(keyInfo, vsn, keySettings)
	if err != nil {
		return err
	}

	if keyInfo.canBeCached() {
		state.keysCache[keyInfo.name()] = keyInfo
	} else {
		if _, exists := state.keysCache[keyInfo.name()]; exists {
			logDbg("Key %s is not cacheable, removing from cache", keyInfo.name())
			delete(state.keysCache, keyInfo.name())
		}
	}

	return nil
}

func (state *StoredKeysState) readKeyFromFile(pathWithoutVersion string, ctx *storedKeysCtx) (storedKeyIface, int, error) {
	keyIface, vsn, proof, err := readKeyFromFileRaw(pathWithoutVersion)
	if err != nil {
		return nil, vsn, fmt.Errorf("failed to read key from file %s: %s", pathWithoutVersion, err.Error())
	}
	err = state.decryptKey(keyIface, true, proof, ctx)
	if err != nil {
		return nil, vsn, err
	}
	return keyIface, vsn, nil
}

func (state *StoredKeysState) readKey(name, kind string, validateProof bool, ctx *storedKeysCtx) (storedKeyIface, error) {
	keySettings, err := getStoredKeyConfig(kind, ctx.storedKeyConfigs)
	if err != nil {
		return nil, err
	}
	if cachedKeyIface, ok := state.keysCache[name]; ok {
		logDbg("readKey: using cached key %s", name)
		return cachedKeyIface, nil
	}
	keyIface, _, proof, err := readKeyRaw(keySettings, name)
	if err != nil {
		return nil, fmt.Errorf("failed to read key %s: %s", name, err.Error())
	}
	err = state.decryptKey(keyIface, validateProof, proof, ctx)
	if err != nil {
		return nil, err
	}
	return keyIface, nil
}

func (state *StoredKeysState) encryptKey(keyIface storedKeyIface, ctx *storedKeysCtx) error {
	// Mark it as in use, to make sure we don't try to use it for encryption
	// or decryption while we are encrypting or decrypting this key
	name := keyIface.name()
	ctx.keysTouched[name] = true
	defer delete(ctx.keysTouched, name)
	return keyIface.encryptMe(state, ctx)
}

func (state *StoredKeysState) decryptKey(keyIface storedKeyIface, validateProof bool, proof string, ctx *storedKeysCtx) error {
	// Mark it as in use, to make sure we don't try to use it for encryption
	// or decryption while we are encrypting or decrypting this key
	name := keyIface.name()
	ctx.keysTouched[name] = true
	defer delete(ctx.keysTouched, name)
	err := keyIface.decryptMe(validateProof, state, ctx)
	if err != nil {
		return err
	}
	if validateProof {
		err = validateKeyProof(keyIface, proof, state.intTokens)
		if err != nil {
			return err
		}
	}
	if keyIface.canBeCached() {
		state.keysCache[keyIface.name()] = keyIface
	}
	return nil
}

func (state *StoredKeysState) writeKeyToDisk(keyIface storedKeyIface, curVsn int, keySettings *storedKeyConfig) error {
	keyPathWithoutVersion := storedKeyPathPrefix(keySettings.Path, keyIface.name())
	return state.writeKeyToFile(keyIface, curVsn, keyPathWithoutVersion)
}

func (state *StoredKeysState) writeKeyToFile(keyIface storedKeyIface, curVsn int, pathWithoutVersion string) error {
	nextVsn := curVsn + 1
	keytype, data, err := keyIface.marshal()
	if err != nil {
		return err
	}
	proof, err := generateKeyProof(keyIface, state.intTokens)
	if err != nil {
		return err
	}
	finalData, err := json.Marshal(storedKeyJson{
		Type:  keytype,
		Raw:   data,
		Proof: proof,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal key %s: %s", keyIface.name(), err.Error())
	}

	keyPath := storedKeyPath(pathWithoutVersion, nextVsn)
	keyDir := filepath.Dir(keyPath)
	err = os.MkdirAll(keyDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create dir for file %s: %s", keyPath, err.Error())
	}
	err = atomicWriteFile(keyPath, finalData, 0640)
	if err != nil {
		return fmt.Errorf("failed to write key %s to file %s: %s", keyIface.name(), keyPath, err.Error())
	}
	if curVsn >= 0 {
		prevKeyPath := storedKeyPath(pathWithoutVersion, curVsn)
		err = os.Remove(prevKeyPath)
		if err != nil {
			// Seems like we should not return error in this case, because
			// it would lead to retry
			logDbg("failed to remove file %s: %s", prevKeyPath, err.Error())
		}
	}

	return nil
}

func generateKeyProof(keyIface storedKeyIface, intTokens []intToken) (string, error) {
	if len(intTokens) == 0 {
		return "", fmt.Errorf("failed to generate proof: empty integrity tokens")
	}
	tokenHash := tokenHash(intTokens[0], keyIface)
	encryptedTokenHash, err := keyIface.encryptData(tokenHash[:], []byte(keyIface.name()))
	if err != nil {
		return "", fmt.Errorf("failed to use key to generate proof: %s", err.Error())
	}
	proof := fmt.Sprintf("%s:%s", intTokens[0].uuid, base64.StdEncoding.EncodeToString(encryptedTokenHash))
	return proof, nil
}

func validateKeyProof(keyIface storedKeyIface, proof string, intTokens []intToken) error {
	parts := strings.Split(proof, ":")
	if len(parts) != 2 {
		return fmt.Errorf("key integrity check failed: invalid proof format")
	}
	uuid := parts[0]
	encryptedTokenHashBase64 := parts[1]
	for _, token := range intTokens {
		if token.uuid == uuid {
			encryptedTokenHash, err := base64.StdEncoding.DecodeString(encryptedTokenHashBase64)
			if err != nil {
				return fmt.Errorf("key integrity check failed: failed to decode encrypted token hash: %s", err.Error())
			}
			decryptedTokenHash, err := keyIface.decryptData(encryptedTokenHash, []byte(keyIface.name()))
			if err != nil {
				return fmt.Errorf("key integrity check failed: failed to decrypt proof: %s", err.Error())
			}
			tokenHash := tokenHash(token, keyIface)
			if bytes.Equal(tokenHash[:], decryptedTokenHash) {
				return nil
			}
			return fmt.Errorf("key integrity check failed: invalid integrity token (token uuid: %s, key name: %s)", uuid, keyIface.name())
		}
	}
	return fmt.Errorf("key integrity check failed: unknown token: %s (key name: %s)", uuid, keyIface.name())
}

func tokenHash(token intToken, keyIface storedKeyIface) [64]byte {
	return sha512.Sum512(append(token.token, []byte(keyIface.name())...))
}

func (state *StoredKeysState) encryptWithKey(keyKind, keyName string, data, AD []byte, ctx *storedKeysCtx) ([]byte, error) {
	if _, ok := ctx.keysTouched[keyName]; ok {
		return nil, fmt.Errorf("key encryption cycle")
	}
	keySettings, err := getStoredKeyConfig(keyKind, ctx.storedKeyConfigs)
	if err != nil {
		return nil, err
	}
	if cachedKeyIface, ok := state.keysCache[keyName]; ok {
		logDbg("encryptWithKey: using cached key %s", keyName)
		return cachedKeyIface.encryptData(data, AD)
	}
	keyIface, _, proof, err := readKeyRaw(keySettings, keyName)
	if err != nil {
		return nil, err
	}
	err = state.decryptKey(keyIface, true, proof, ctx)
	if err != nil {
		return nil, err
	}
	return keyIface.encryptData(data, AD)
}

func (state *StoredKeysState) decryptWithKey(keyKind, keyName string, data, AD []byte, validateKeysProof bool, ctx *storedKeysCtx) ([]byte, error) {
	if _, ok := ctx.keysTouched[keyName]; ok {
		return nil, fmt.Errorf("key encryption loop")
	}

	keySettings, err := getStoredKeyConfig(keyKind, ctx.storedKeyConfigs)
	if err != nil {
		return nil, err
	}
	if cachedKeyIface, ok := state.keysCache[keyName]; ok {
		logDbg("decryptWithKey: using cached key %s", keyName)
		return cachedKeyIface.decryptData(data, AD)
	}
	keyIface, _, proof, err := readKeyRaw(keySettings, keyName)
	if err != nil {
		return nil, err
	}
	err = state.decryptKey(keyIface, validateKeysProof, proof, ctx)
	if err != nil {
		return nil, err
	}
	return keyIface.decryptData(data, AD)
}

func getStoredKeyConfig(keyKind string, configs []storedKeyConfig) (*storedKeyConfig, error) {
	index := slices.IndexFunc(configs, func(c storedKeyConfig) bool { return c.KeyKind == keyKind })
	if index == -1 {
		return nil, fmt.Errorf("failed to find key kind \"%s\" in config", keyKind)
	}
	return &configs[index], nil
}

func readKeyRaw(keySettings *storedKeyConfig, keyName string) (storedKeyIface, int, string, error) {
	path := storedKeyPathPrefix(keySettings.Path, keyName)
	return readKeyFromFileRaw(path)
}

func scanDir(dirPath string) ([]string, error) {
	files, dirReadErr := os.ReadDir(dirPath)
	if os.IsNotExist(dirReadErr) {
		return nil, nil
	}
	if files == nil {
		return nil, dirReadErr
	}
	// Even in case of an error ReadDir can return files
	s := make(map[string]bool)
	for _, f := range files {
		keyName, _, err := parseKeyFilename(f.Name())
		if err != nil {
			logDbg("Skipping file %s as it doesn't seem to be a key file", f)
			continue
		}
		s[keyName] = true
	}
	res := make([]string, len(s))
	i := 0
	for keyName := range s {
		res[i] = keyName
		i++
	}
	return res, dirReadErr
}

func parseKeyFilename(base_filename string) (string, int, error) {
	tokens := strings.Split(base_filename, ".key.")
	if len(tokens) != 2 {
		return "", -1, fmt.Errorf("invalid key filename: %s", base_filename)
	}
	keyName := tokens[0]
	vsn, err := strconv.Atoi(tokens[1])
	if err != nil {
		return "", -1, fmt.Errorf("invalid key filename: %s", base_filename)
	}
	return keyName, vsn, nil
}

func findKeyFile(path string) (string, int, error) {
	wildcard := path + ".*"
	candidates, err := filepath.Glob(wildcard)
	if err != nil {
		return "", -1, fmt.Errorf("failed to read file list using wildcard %s: %s", wildcard, err.Error())
	}
	if len(candidates) == 0 {
		return "", -1, ErrKeyNotFound{e: fmt.Errorf("no files found matching: %s", wildcard)}
	}
	// looking for a file with max vsn (we increment it on every write)
	maxVsn := -1
	var res string
	for _, p := range candidates {
		_, vsn, err := parseKeyFilename(filepath.Base(p))
		if err != nil {
			logDbg("Unexpected key filename %s", err.Error())
			continue
		}
		if vsn > maxVsn {
			maxVsn = vsn
			res = p
		}
	}
	if maxVsn == -1 {
		return "", -1, fmt.Errorf("failed to find any key files among %v", candidates)
	}
	return res, maxVsn, nil
}

func isKeyFile(pathPrefix string) bool {
	wildcard := pathPrefix + ".*"
	candidates, err := filepath.Glob(wildcard)
	if err != nil {
		// Note that according to the docs, Glob can return error only if
		// pattern is malformed, so we should not see this error in practice
		logDbg("Failed to read file list using wildcard %s: %s", wildcard, err.Error())
		return false
	}
	return len(candidates) > 0
}

func readKeyFromFileRaw(pathWithoutVersion string) (storedKeyIface, int, string, error) {
	path, vsn, err := findKeyFile(pathWithoutVersion)
	if err != nil {
		return nil, vsn, "", err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, vsn, "", fmt.Errorf("failed to read key from file %s: %s", path, err.Error())
	}
	var keyJson storedKeyJson
	err = json.Unmarshal(data, &keyJson)
	if err != nil {
		return nil, vsn, "", fmt.Errorf("failed to unmarshal key from file %s: %s", path, err.Error())
	}

	if keyJson.Type == rawAESGCMKey {
		var k rawAesGcmStoredKey
		k.unmarshal(keyJson.Raw)
		return &k, vsn, keyJson.Proof, nil
	}

	if keyJson.Type == awskmKey {
		var k awsStoredKey
		k.unmarshal(keyJson.Raw)
		return &k, vsn, keyJson.Proof, nil
	}

	if keyJson.Type == kmipKey {
		var k kmipStoredKey
		k.unmarshal(keyJson.Raw)
		return &k, vsn, keyJson.Proof, nil
	}

	return nil, vsn, "", fmt.Errorf("unknown key type: %s", keyJson.Type)
}

// returns a key path without version, like /path/to/key.key
func storedKeyPathPrefix(path string, keyName string) string {
	return filepath.Join(path, keyName+".key")
}

func storedKeyPath(pathWithoutVersion string, vsn int) string {
	return pathWithoutVersion + "." + strconv.Itoa(vsn)
}

// Reencrypt keys that use secret management (SM) encryption key
// Needed for SM data key rotation
func reencryptStoredKeys(state *StoredKeysState, ctx *storedKeysCtx) error {
	errorsCounter := 0
	for _, cfg := range ctx.storedKeyConfigs {
		// All keys but bucketDeks can be encrypted with
		// encryption_service
		if cfg.KeyKind == "bucketDek" {
			continue
		}
		keyNames, dirReadErr := scanDir(cfg.Path)
		logDbg("Will check if the following keys in %s need reencryption: %v", cfg.Path, keyNames)
		if dirReadErr != nil {
			if os.IsNotExist(dirReadErr) {
				continue
			}
			logDbg("Could not reencrypt keys because could not read dir \"%s\": %s", cfg.Path, dirReadErr.Error())
			errorsCounter++
		}
		// Even in case of an error ReadDir can return files
		// Reencrypt everything we can
		for _, keyName := range keyNames {
			logDbg("Maybe reencrypting key \"%s\"...", keyName)
			// Reseting keysTouched to make sure we check for cycles each key
			// individually
			ctx.keysTouched = make(map[string]bool)
			keyIface, vsn, proof, err := readKeyRaw(&cfg, keyName)
			if err != nil {
				logDbg("Failed to read key %s: %s", keyName, err.Error())
				errorsCounter++
				continue
			}
			if !keyIface.usesSecretManagementKey() {
				logDbg("Skipping \"%s\", because it is not using secret management service", keyName)
				continue
			}
			err = state.decryptKey(keyIface, true, proof, ctx)
			if err != nil {
				logDbg("Failed to decrypt key %s: %s", keyName, err.Error())
				errorsCounter++
				continue
			}
			err = state.encryptKey(keyIface, ctx)
			if err != nil {
				logDbg("Failed to encrypt key %s: %s", keyName, err.Error())
				errorsCounter++
				continue
			}
			err = state.writeKeyToDisk(keyIface, vsn, &cfg)
			if err != nil {
				logDbg("Failed to write key %s to disk: %s", keyName, err.Error())
				errorsCounter++
				continue
			}
		}
	}
	if errorsCounter > 0 {
		return fmt.Errorf("could not reencrypt some keys")
	}
	return nil
}

// Helper function that is supposed to be used to encrypt data to be
// stored in the key json
func encryptKeyData(k storedKeyIface, data []byte, encryptionKeyName string, state *StoredKeysState, ctx *storedKeysCtx) ([]byte, string, error) {
	settings, err := getStoredKeyConfig(k.kind(), ctx.storedKeyConfigs)
	if err != nil {
		return nil, "", err
	}
	if data == nil {
		return nil, "", fmt.Errorf("encrypt data nil")
	}
	AD := k.ad()
	if k.usesSecretManagementKey() {
		// Encrypting with encryption service's data key
		logDbg("Will use encryption service to encrypt key %s (ad: %s)", k.name(), base64.StdEncoding.EncodeToString(AD))
		// Using encrypt instead of aesgcmEncrypt here
		// because we want to include encryption cipher (version
		// basically) information in the encrypted data for the case if
		// we want to change encryption cipher in future.
		// This doesn't apply to encryptWithKey, because that
		// information sits in the stored key (each stored key can be
		// used by one cipher only).
		return encryptWithAD(ctx.encryptionServiceKey, data, AD), settings.EncryptByKind, nil
	}
	// Encrypting with another stored key (kek) that we will read from disk
	logDbg("Will use key %s to encrypt key %s (ad: %s)", encryptionKeyName, k.name(), base64.StdEncoding.EncodeToString(AD))
	encryptedData, err := state.encryptWithKey(settings.EncryptByKind, encryptionKeyName, data, AD, ctx)
	if err != nil {
		return nil, "", err
	}
	return encryptedData, settings.EncryptByKind, nil
}

// Helper function that is supposed to be used to decrypt data that is
// stored in the key json
// Returns decrypted data and if it needs to be reencrypted
func decryptKeyData(k storedKeyIface, data []byte, encryptedByKind, encryptionKeyName string, validateKeysProof bool, state *StoredKeysState, ctx *storedKeysCtx) ([]byte, bool, error) {
	if data == nil {
		return nil, false, fmt.Errorf("decrypt data is nil")
	}
	AD := k.ad()
	if k.usesSecretManagementKey() {
		decryptedData, err := decryptWithAD(ctx.encryptionServiceKey, data, AD)
		if err != nil {
			if ctx.backupEncryptionServiceKey != nil {
				var err2 error
				decryptedData, err2 = decryptWithAD(ctx.backupEncryptionServiceKey, data, AD)
				if err2 != nil {
					logDbg("Failed to decrypt key using backup data key: %s (main key error: %s, ad: %s)", err2.Error(), err.Error(), base64.StdEncoding.EncodeToString(AD))
					return nil, false, fmt.Errorf("failed to decrypt key %s: %s", k.name(), err2.Error())
				}
			} else {
				logDbg("Failed to decrypt key using main data key, and there is no backup key: %s (ad: %s)", err.Error(), base64.StdEncoding.EncodeToString(AD))
				return nil, false, fmt.Errorf("failed to decrypt key %s: %s", k.name(), err.Error())
			}
			return decryptedData, true, nil
		}
		return decryptedData, false, nil
	}
	decryptedData, err := state.decryptWithKey(encryptedByKind, encryptionKeyName, data, AD, validateKeysProof, ctx)
	if err != nil {
		return nil, false, err
	}
	return decryptedData, false, nil
}

// Implementation of storedKeyIface for raw keys

func newAesGcmKey(name, kind, creationTime, encryptionKeyName string, canBeCached bool, data []byte) *rawAesGcmStoredKey {
	rawKeyInfo := &rawAesGcmStoredKey{
		Name:              name,
		Kind:              kind,
		EncryptionKeyName: encryptionKeyName,
		CreationTime:      creationTime,
		DecryptedKey:      data,
		CanBeCached:       canBeCached,
	}
	return rawKeyInfo
}

func (k *rawAesGcmStoredKey) name() string {
	return k.Name
}

func (k *rawAesGcmStoredKey) kind() string {
	return k.Kind
}

func (k *rawAesGcmStoredKey) needRewrite(settings *storedKeyConfig, state *StoredKeysState, ctx *storedKeysCtx) (bool, int, error) {
	keyIface, vsn, _, err := readKeyRaw(settings, k.Name)
	if err != nil {
		var keyNotFoundErr ErrKeyNotFound
		if !errors.As(err, &keyNotFoundErr) {
			logDbg("key %s read error: %s", k.Name, err.Error())
		}
		return true, vsn, nil
	}
	onDiskKey, ok := keyIface.(*rawAesGcmStoredKey)
	if !ok {
		logDbg("key %s changed type, rewriting", k.Name)
		return true, vsn, nil
	}
	needsRewrite :=
		onDiskKey.EncryptedByKind != settings.EncryptByKind ||
			onDiskKey.EncryptionKeyName != k.EncryptionKeyName ||
			onDiskKey.CanBeCached != k.CanBeCached
	return needsRewrite, vsn, nil
}

func (k *rawAesGcmStoredKey) ad() []byte {
	return []byte(string(rawAESGCMKey) + k.Name + k.Kind + k.CreationTime + k.EncryptionKeyName + strconv.FormatBool(k.CanBeCached))
}

func (k *rawAesGcmStoredKey) encryptMe(state *StoredKeysState, ctx *storedKeysCtx) error {
	if k.EncryptedKey != nil {
		// Seems like it is already encrypted
		// Checking that we can decrypt it just in case
		decryptedKey, reencryptNeeded, err := decryptKeyData(k, k.EncryptedKey, k.EncryptedByKind, k.EncryptionKeyName, true, state, ctx)
		if err != nil {
			return err
		}
		k.DecryptedKey = decryptedKey
		if !reencryptNeeded {
			return nil
		}
	}
	encryptedKey, encryptedByKind, err := encryptKeyData(k, k.DecryptedKey, k.EncryptionKeyName, state, ctx)
	if err != nil {
		return err
	}
	k.EncryptedKey = encryptedKey
	k.EncryptedByKind = encryptedByKind
	return nil
}

func (k *rawAesGcmStoredKey) decryptMe(validateKeysProof bool, state *StoredKeysState, ctx *storedKeysCtx) error {
	decryptedKey, _, err := decryptKeyData(k, k.EncryptedKey, k.EncryptedByKind, k.EncryptionKeyName, validateKeysProof, state, ctx)
	if err != nil {
		return err
	}
	k.DecryptedKey = decryptedKey
	return nil
}

func (k *rawAesGcmStoredKey) encryptData(data, AD []byte) ([]byte, error) {
	if k.DecryptedKey == nil {
		return nil, fmt.Errorf("can't encrypt because the key is encrypted")
	}
	return aesgcmEncrypt(k.DecryptedKey, data, AD), nil
}

func (k *rawAesGcmStoredKey) decryptData(data, AD []byte) ([]byte, error) {
	if k.DecryptedKey == nil {
		return nil, fmt.Errorf("can't decrypt because the key is encrypted")
	}
	return aesgcmDecrypt(k.DecryptedKey, data, AD)
}

func (k *rawAesGcmStoredKey) unmarshal(data json.RawMessage) error {
	var decoded rawAesGcmStoredKeyJson
	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return fmt.Errorf("invalid raw key json: %s", err.Error())
	}
	k.Name = decoded.Name
	k.Kind = decoded.KeyKind
	k.DecryptedKey = nil
	k.EncryptedKey = decoded.SealedKeyData
	k.EncryptedByKind = decoded.EncryptedByKind
	k.EncryptionKeyName = decoded.EncryptionKeyName
	k.CreationTime = decoded.CreationTime
	k.CanBeCached = decoded.CanBeCached
	return nil
}

func (k *rawAesGcmStoredKey) usesSecretManagementKey() bool {
	return k.EncryptionKeyName == "encryptionService"
}

func (k *rawAesGcmStoredKey) canBeCached() bool {
	return k.CanBeCached
}

func (k *rawAesGcmStoredKey) marshal() (storedKeyType, []byte, error) {
	if k.EncryptedKey == nil {
		return "", nil, fmt.Errorf("can't store key \"%s\" to disk because the key is not encrypted", k.Name)
	}
	data, err := json.Marshal(rawAesGcmStoredKeyJson{
		Name:              k.Name,
		KeyKind:           k.Kind,
		SealedKeyData:     k.EncryptedKey,
		EncryptedByKind:   k.EncryptedByKind,
		EncryptionKeyName: k.EncryptionKeyName,
		CreationTime:      k.CreationTime,
		CanBeCached:       k.CanBeCached,
	})

	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal key %s: %s", k.Name, err.Error())
	}
	return rawAESGCMKey, data, nil
}

// Implementation of storedKeyIface for aws keys

func newAwsKey(name, kind, creationTime string, canBeCached bool, data []byte) (*awsStoredKey, error) {
	if canBeCached {
		return nil, fmt.Errorf("aws keys can't be cached")
	}
	var awsk awsStoredKey
	err := json.Unmarshal(data, &awsk)
	if err != nil {
		return nil, fmt.Errorf("invalid json: %v", data)
	}
	awsk.Name = name
	awsk.Kind = kind
	awsk.CreationTime = creationTime
	return &awsk, nil
}

func (k *awsStoredKey) name() string {
	return k.Name
}

func (k *awsStoredKey) kind() string {
	return k.Kind
}

func (k *awsStoredKey) needRewrite(settings *storedKeyConfig, state *StoredKeysState, ctx *storedKeysCtx) (bool, int, error) {
	keyIface, vsn, _, err := readKeyRaw(settings, k.Name)
	if err != nil {
		var keyNotFoundErr ErrKeyNotFound
		if !errors.As(err, &keyNotFoundErr) {
			logDbg("key %s read error: %s", k.Name, err.Error())
		}
		return true, vsn, nil
	}
	onDiskKey, ok := keyIface.(*awsStoredKey)
	if !ok {
		logDbg("key %s changed type, rewriting", k.Name)
		return true, vsn, nil
	}
	return !reflect.DeepEqual(k, onDiskKey), vsn, nil
}

func (k *awsStoredKey) ad() []byte {
	return []byte("")
}

func (k *awsStoredKey) encryptMe(state *StoredKeysState, ctx *storedKeysCtx) error {
	// Nothing to encrypt here, configuration should contain no secrets
	return nil
}

func (k *awsStoredKey) decryptMe(validateKeysProof bool, state *StoredKeysState, ctx *storedKeysCtx) error {
	return nil
}

func (k *awsStoredKey) encryptData(data, AD []byte) ([]byte, error) {
	if k.KeyArn == "TEST_AWS_KEY_ARN" {
		// This code should be used for test purposes only
		logDbg("Encrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmEncrypt(zero_key, data, AD), nil
	}
	if k.KeyArn == "TEST_AWS_BAD_KEY_ARN" {
		return nil, fmt.Errorf("test encryption error")
	}

	opts := awsutils.AwsConfigOpts{
		Region:     k.Region,
		ConfigFile: k.ConfigFile,
		CredsFile:  k.CredsFile,
		Profile:    k.Profile,
		UseIMDS:    k.UseIMDS,
	}
	// AD parameters are strings in Kms :(
	strAD := base64.StdEncoding.EncodeToString(AD)
	return awsutils.KmsEncryptData(k.KeyArn, data, strAD, opts)
}

func (k *awsStoredKey) decryptData(data, AD []byte) ([]byte, error) {
	if k.KeyArn == "TEST_AWS_KEY_ARN" {
		// This code should be used for test purposes only
		logDbg("Decrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmDecrypt(zero_key, data, AD)
	}
	if k.KeyArn == "TEST_AWS_BAD_KEY_ARN" {
		return nil, fmt.Errorf("test decryption error")
	}
	opts := awsutils.AwsConfigOpts{
		Region:     k.Region,
		ConfigFile: k.ConfigFile,
		CredsFile:  k.CredsFile,
		Profile:    k.Profile,
		UseIMDS:    k.UseIMDS,
	}
	// AD parameters are strings in Kms :(
	strAD := base64.StdEncoding.EncodeToString(AD)
	return awsutils.KmsDecryptData(k.KeyArn, data, strAD, opts)
}

func (k *awsStoredKey) unmarshal(data json.RawMessage) error {
	err := json.Unmarshal(data, k)
	if err != nil {
		return fmt.Errorf("invalid raw key json: %s", err.Error())
	}
	return nil
}

func (k *awsStoredKey) marshal() (storedKeyType, []byte, error) {
	data, err := json.Marshal(k)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal key %s: %s", k.Name, err.Error())
	}
	return awskmKey, data, nil
}

func (k *awsStoredKey) usesSecretManagementKey() bool {
	return false
}

func (k *awsStoredKey) canBeCached() bool {
	return false
}

// Implementation of storedKeyIface for kmip keys

func newKmipKey(name, kind, creationTime, encryptionKeyName string, canBeCached bool, data []byte) (*kmipStoredKey, error) {
	if canBeCached {
		return nil, fmt.Errorf("kmip keys can't be cached")
	}
	type kmipKeyTmp struct {
		KmipId             string `json:"kmipId"`
		Host               string `json:"host"`
		Port               int    `json:"port"`
		ReqTimeoutMs       int    `json:"reqTimeoutMs"`
		KeyPath            string `json:"keyPath"`
		CertPath           string `json:"certPath"`
		Passphrase         []byte `json:"keyPassphrase"`
		CaSelection        string `json:"caSelection"`
		CbCaPath           string `json:"CbCaPath"`
		EncryptionApproach string `json:"encryptionApproach"`
	}
	var decoded kmipKeyTmp
	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return nil, fmt.Errorf("invalid raw key json: %s", err.Error())
	}

	rawKeyInfo := &kmipStoredKey{
		Name:                name,
		Kind:                kind,
		KmipId:              decoded.KmipId,
		Host:                decoded.Host,
		Port:                decoded.Port,
		ReqTimeoutMs:        decoded.ReqTimeoutMs,
		KeyPath:             decoded.KeyPath,
		CertPath:            decoded.CertPath,
		CaSelection:         decoded.CaSelection,
		CbCaPath:            decoded.CbCaPath,
		EncryptionApproach:  decoded.EncryptionApproach,
		EncryptionKeyName:   encryptionKeyName,
		CreationTime:        creationTime,
		decryptedPassphrase: decoded.Passphrase,
	}
	return rawKeyInfo, nil
}

func (k *kmipStoredKey) name() string {
	return k.Name
}

func (k *kmipStoredKey) kind() string {
	return k.Kind
}

func (k *kmipStoredKey) needRewrite(settings *storedKeyConfig, state *StoredKeysState, ctx *storedKeysCtx) (bool, int, error) {
	if k.decryptedPassphrase == nil {
		return false, 0, fmt.Errorf("key %s should be decrypted first", k.Name)
	}
	keyIface, vsn, proof, err := readKeyRaw(settings, k.Name)
	if err != nil {
		var keyNotFoundErr ErrKeyNotFound
		if !errors.As(err, &keyNotFoundErr) {
			logDbg("key %s read error: %s", k.Name, err.Error())
		}
		return true, vsn, nil
	}
	onDiskKey, ok := keyIface.(*kmipStoredKey)
	if !ok {
		logDbg("key %s changed type, rewriting", k.Name)
		return true, vsn, nil
	}

	if onDiskKey.decryptedPassphrase == nil {
		err = state.decryptKey(onDiskKey, true, proof, ctx)
		if err != nil {
			return false, vsn, err
		}
	}

	// copy encrypted pass because we don't want to compare them
	onDiskKey.EncryptedPassphrase = k.EncryptedPassphrase
	return !reflect.DeepEqual(k, onDiskKey), vsn, nil
}

func (k *kmipStoredKey) ad() []byte {
	return []byte(
		string(kmipKey) +
			k.Name +
			k.Kind +
			k.KmipId +
			k.Host +
			strconv.Itoa(k.Port) +
			strconv.Itoa(k.ReqTimeoutMs) +
			k.EncryptionApproach +
			k.KeyPath +
			k.CertPath +
			k.CaSelection +
			k.EncryptionKeyName +
			k.CreationTime)
}

func (k *kmipStoredKey) encryptMe(state *StoredKeysState, ctx *storedKeysCtx) error {
	if k.EncryptedPassphrase != nil {
		// Seems like it is already encrypted
		// Checking that we can decrypt it just in case
		decryptedPass, reencryptNeeded, err := decryptKeyData(k, k.EncryptedPassphrase, k.EncryptedByKind, k.EncryptionKeyName, true, state, ctx)
		if err != nil {
			return err
		}
		k.decryptedPassphrase = decryptedPass
		if !reencryptNeeded {
			return nil
		}
	}
	encryptedPass, encryptedByKind, err := encryptKeyData(k, k.decryptedPassphrase, k.EncryptionKeyName, state, ctx)
	if err != nil {
		return err
	}
	k.EncryptedPassphrase = encryptedPass
	k.EncryptedByKind = encryptedByKind
	return nil
}

func (k *kmipStoredKey) decryptMe(validateKeysProof bool, state *StoredKeysState, ctx *storedKeysCtx) error {
	decryptedPass, _, err := decryptKeyData(k, k.EncryptedPassphrase, k.EncryptedByKind, k.EncryptionKeyName, validateKeysProof, state, ctx)
	if err != nil {
		return err
	}
	k.decryptedPassphrase = decryptedPass
	return nil
}

func getKmipClientCfg(k *kmipStoredKey) kmiputils.KmipClientConfig {
	maxTimeoutDuration := 5 * time.Minute
	var timeoutDuration time.Duration
	if int64(k.ReqTimeoutMs) > maxTimeoutDuration.Milliseconds() {
		timeoutDuration = maxTimeoutDuration
	} else {
		timeoutDuration = time.Duration(k.ReqTimeoutMs) * time.Millisecond
	}

	return kmiputils.KmipClientConfig{
		Host:                k.Host,
		Port:                k.Port,
		TimeoutDuration:     timeoutDuration,
		KeyPath:             k.KeyPath,
		CertPath:            k.CertPath,
		CbCaPath:            k.CbCaPath,
		SelectCaOpt:         k.CaSelection,
		DecryptedPassphrase: k.decryptedPassphrase,
	}
}

func (k *kmipStoredKey) encryptData(data, AD []byte) ([]byte, error) {
	clientCfg := getKmipClientCfg(k)
	if k.EncryptionApproach == "use_encrypt_decrypt" {
		encrAttrs, err := kmiputils.KmipEncryptData(clientCfg, k.KmipId, data, AD)
		if err != nil {
			return nil, err
		}

		ivNonceLenSize := make([]byte, 4)
		ivCounterNonceLen := len(encrAttrs.IVCounterNonce)
		if ivCounterNonceLen > KMIP_MAX_IV_SIZE {
			return nil, fmt.Errorf("ivCounterNonceLen too large ivCounterNonceLen=%d, maxAllowed=%d", ivCounterNonceLen, KMIP_MAX_IV_SIZE)
		}

		binary.BigEndian.PutUint32(ivNonceLenSize, uint32(len(encrAttrs.IVCounterNonce)))
		dataSlice := append(ivNonceLenSize, encrAttrs.IVCounterNonce...)
		dataSlice = append(dataSlice, encrAttrs.AuthTag...)
		dataSlice = append(dataSlice, encrAttrs.EncrData...)
		return append([]byte{KMIP_USE_ENCR_DECR}, dataSlice...), nil
	} else if k.EncryptionApproach == "use_get" {
		aes256Key, err := kmiputils.KmipGetAes256Key(clientCfg, k.KmipId)
		if err != nil {
			return nil, err
		}

		encrData := aesgcmEncrypt(aes256Key, data, AD)
		return append([]byte{KMIP_USE_GET}, encrData...), nil
	} else {
		return nil, fmt.Errorf("invalid encrypton approach %s", k.EncryptionApproach)
	}
}

func (k *kmipStoredKey) decryptData(data, AD []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("invalid data length: %d", len(data))
	}

	clientCfg := getKmipClientCfg(k)
	if uint8(data[0]) == KMIP_USE_ENCR_DECR {
		dataSlice := data[1:]
		if len(dataSlice) < 4 {
			return nil, fmt.Errorf("invalid dataSlice length: %d", len(dataSlice))
		}

		authTagLen := uint32(kmiputils.KMIP_AUTH_TAG_LENGTH)
		ivNonceLen := binary.BigEndian.Uint32(dataSlice[0:4])
		if len(dataSlice) < int(ivNonceLen+authTagLen+4) {
			return nil, fmt.Errorf("invalid dataSlice length: %d", len(dataSlice))
		}

		ivNonce := dataSlice[4 : ivNonceLen+4]
		authTag := dataSlice[ivNonceLen+4 : ivNonceLen+authTagLen+4]
		encrData := dataSlice[ivNonceLen+authTagLen+4:]

		encrAttrs := kmiputils.KmipEncrAttrs{
			EncrData:       encrData,
			IVCounterNonce: ivNonce,
			AuthTag:        authTag,
			AD:             AD,
		}
		return kmiputils.KmipDecryptData(clientCfg, k.KmipId, encrAttrs)
	} else if uint8(data[0]) == KMIP_USE_GET {
		aes256Key, err := kmiputils.KmipGetAes256Key(clientCfg, k.KmipId)
		if err != nil {
			return nil, err
		}

		return aesgcmDecrypt(aes256Key, data[1:], AD)
	} else {
		return nil, fmt.Errorf("invalid usage tag for kmip encrypted data %d", uint8(data[0]))
	}
}

func (k *kmipStoredKey) unmarshal(data json.RawMessage) error {
	err := json.Unmarshal(data, k)
	if err != nil {
		return fmt.Errorf("invalid raw key json: %s", err.Error())
	}
	return nil
}

func (k *kmipStoredKey) usesSecretManagementKey() bool {
	return k.EncryptionKeyName == "encryptionService"
}

func (k *kmipStoredKey) canBeCached() bool {
	return false
}

func (k *kmipStoredKey) marshal() (storedKeyType, []byte, error) {
	if k.EncryptedPassphrase == nil {
		return "", nil, fmt.Errorf("can't store key \"%s\" to disk because the key is not encrypted", k.Name)
	}
	data, err := json.Marshal(k)

	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal key %s: %s", k.Name, err.Error())
	}
	return kmipKey, data, nil
}
