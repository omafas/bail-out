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
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/couchbase/gocbcrypto"
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
	asBytes() ([]byte, error)
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
	awskmKey     storedKeyType = "awskms-symmetric"
	gcpkmKey     storedKeyType = "gcpkms-symmetric"
	azurekmKey   storedKeyType = "azurekms"
	hashikmKey   storedKeyType = "hashikms"
	kmipKey      storedKeyType = "kmip"

	// Magic string used for encrypted file headers
	encryptedFileMagicString    = "\x00Couchbase Encrypted\x00"
	encryptedFileMagicStringLen = len(encryptedFileMagicString)
	encryptedFileHeaderSize     = 80
	encryptedFileKeyNameLength  = byte(36)
	macLen                      = 101 // Vsn: 1B, UUID: 36B, MAC: 64B
	intTokenSize                = 64
	maxTokensCount              = 100
)

// Note: main difference between ctx and state is that the ctx can change
// independently from stored keys functionality (in go secrets),
// while the state is created and managed by this file only
type storedKeysCtx struct {
	storedKeyConfigs           []storedKeyConfig
	encryptionServiceKey       []byte
	backupEncryptionServiceKey []byte
	keysTouched                map[string]bool
	useCache                   bool
}

type cachedKey struct {
	keyIface       storedKeyIface
	proof          string
	proofValidated bool
}

type StoredKeysState struct {
	readOnly          bool
	intTokensFile     string
	intTokens         []intToken
	encryptionKeyName string // Name of the encryption key that is used to encrypt the integrity tokens file
	keysCache         map[string]cachedKey
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
	Kind            string `json:"kind"`
	Name            string `json:"name"`
	Key             string `json:"key"`
	EncryptionKeyId string `json:"encryptionKeyId"`
	CreationTime    string `json:"creationTime"`
	Imported        bool   `json:"imported"`
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
		keysCache:     make(map[string]cachedKey),
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

	if len(state.intTokens) >= maxTokensCount && state.encryptionKeyName == keyName {
		// We already have too many tokens. Since the key is not changing current
		// tokens are safe and there is no need to generate new token.
		logDbg("There are too many tokens (current: %d, max: %d), no need to generate new token", len(state.intTokens), maxTokensCount)
		return nil
	}

	// It is important to use new key to encrypt the integrity tokens file
	// because otherwise we can store newly generated token unencrypted
	logDbg("Rotating integrity tokens, and encrypting tokens file with key %s (old key: %s)", keyName, state.encryptionKeyName)
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
	for _, token := range prevTokens[1:] {
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
	var uuid string
	if isLegacyProofFormat(proof) {
		// Legacy proof format is used in 7.9 only
		uuid = proof[:36]
	} else {
		proofBytes, err := base64.StdEncoding.DecodeString(proof)
		if err != nil {
			return fmt.Errorf("file %s has invalid proof format: %s", filePathPrefix, err.Error())
		}
		uuid, _, err = parseMac(proofBytes)
		if err != nil {
			return fmt.Errorf("file %s has invalid proof format: %s", filePathPrefix, err.Error())
		}
	}
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
		token: generateRandomBytes(intTokenSize),
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

	// We can't verify the key proof during reading the key file (as it is
	// usually done), because tokens are not yet read. For this reason
	// maybeDecryptFileData returns encryptionKeyProof, which is verified later,
	// when tokens are read.
	data, encryptionKeyName, encryptionKeyIface, encryptionKeyProof, err := state.maybeDecryptFileData(baseFilename, data, ctx)

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

	if encryptionKeyIface != nil {
		err = state.validateKeyProof(encryptionKeyIface, encryptionKeyProof)
		if err != nil {
			return fmt.Errorf("key integrity check failed for key %s: %s", encryptionKeyName, err.Error())
		}
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
	hmacBytes := hmacSHA512(token.token, data)
	// Format: Version: 1 byte, TokenUUID, MAC
	res := append([]byte{0}, token.uuid...)
	res = append(res, hmacBytes...)
	if len(res) != macLen {
		return nil, fmt.Errorf("unexpected mac length: %d", len(res))
	}
	return res, nil
}

func (state *StoredKeysState) verifyMac(mac, data []byte) error {
	uuid, hmacBytes, err := parseMac(mac)
	if err != nil {
		return err
	}
	for _, token := range state.intTokens {
		if token.uuid == uuid {
			expectedHmacBytes := hmacSHA512(token.token, data)
			if hmac.Equal(hmacBytes, expectedHmacBytes) {
				return nil
			}
			return fmt.Errorf("invalid mac (token uuid: %s)", uuid)
		}
	}
	return fmt.Errorf("unknown token: %s", uuid)
}

// Format: Version = 0: 1 byte, TokenUUID (36 bytes), MAC (64 bytes)
func parseMac(mac []byte) (string, []byte, error) {
	if len(mac) == 0 {
		return "", nil, fmt.Errorf("mac is empty")
	}
	if mac[0] != 0 {
		return "", nil, fmt.Errorf("unknown mac version: %v", mac[0])
	}

	if len(mac) != macLen {
		return "", nil, fmt.Errorf("unexpected mac length: %d", len(mac))
	}
	uuidBytes := mac[1:37]
	if !utf8.Valid(uuidBytes) {
		return "", nil, fmt.Errorf("invalid utf-8 in uuid: %q", uuidBytes)
	}
	uuid := string(uuidBytes)
	hmacBytes := mac[37:]
	return uuid, hmacBytes, nil
}

func (state *StoredKeysState) revalidateKeyCache(ctx *storedKeysCtx) {
	for _, cachedKey := range state.keysCache {
		keySettings, err := getStoredKeyConfig(cachedKey.keyIface.kind(), ctx.storedKeyConfigs)
		shouldInvalidate := false
		if err != nil {
			logDbg("Failed to get stored key config for %s: %s", cachedKey.keyIface.kind(), err.Error())
			shouldInvalidate = true
		} else {
			keyPathPrefix := storedKeyPathPrefix(keySettings.Path, cachedKey.keyIface.name())
			if !isKeyFile(keyPathPrefix) {
				logDbg("Key file %s not found, removing from cache", keyPathPrefix)
				shouldInvalidate = true
			}
		}
		if shouldInvalidate {
			delete(state.keysCache, cachedKey.keyIface.name())
		}
	}
}

func getEncryptedFileAD(header []byte, offset int) []byte {
	ad := make([]byte, len(header)+8)
	copy(ad, header)
	binary.BigEndian.PutUint64(ad[len(header):], uint64(offset))
	return ad
}

// formatUUID formats a 16-byte UUID as a standard UUID string
// (e.g., "550e8400-e29b-41d4-a716-446655440000")
// Uses github.com/google/uuid package for proper UUID formatting
func formatUUID(uuidBytes []byte) (string, error) {
	if len(uuidBytes) != 16 {
		return "", fmt.Errorf("UUID must be exactly 16 bytes, got %d", len(uuidBytes))
	}
	// Convert bytes to uuid.UUID and format as string
	u, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return "", fmt.Errorf("invalid UUID bytes: %w", err)
	}
	return u.String(), nil
}

// deriveFileKey derives a key using KBKDF (SP 800-108) with
// HMAC-SHA256 in counter mode. This matches the Erlang implementation.
// Note: OpenSSL KBKDF uses Info=Context and Salt=Label (swapped from SP 800-108)
// Context = "Couchbase Encrypted File/" + UUID string
// Label = "Couchbase File Encryption"
func deriveFileKey(key []byte, salt []byte) ([]byte, error) {
	saltString, err := formatUUID(salt)
	if err != nil {
		return nil, fmt.Errorf("failed to format UUID: %w", err)
	}
	context := []byte("Couchbase Encrypted File/" + saltString)
	label := []byte("Couchbase File Encryption")

	// OpenSSL KBKDF: Info=Context, Salt=Label
	// Output length is the same as input key length
	outLen := len(key)
	derivedKey := make([]byte, outLen)
	derivedKey, err = gocbcrypto.OpenSSLKBKDFDeriveKey(key, label, context, derivedKey, "SHA-256", "")
	if err != nil {
		return nil, fmt.Errorf("failed to derive key with OpenSSL KBKDF: %w", err)
	}
	return derivedKey, nil
}

// Note: function encrypts everything as a single chunk
func (state *StoredKeysState) encryptFileData(data []byte, encryptionKeyName string, ctx *storedKeysCtx) ([]byte, error) {
	if encryptionKeyName == "" {
		// If no encryption key name provided, the data doesn't need to be encrypted
		return data, nil
	}

	const (
		version            = byte(1)
		compressionType    = byte(0)
		kbkdfKeyDerivation = byte(1) // KBKDF key derivation method
	)

	// Read the encryption key
	key, _, err := state.readKey(encryptionKeyName, intTokenEncryptionKeyKind, true, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read config dek: %s", err.Error())
	}

	// Get the raw key bytes for derivation
	// Cast to raw AES key - only raw AES keys are supported for file encryption
	rawAesKey, ok := key.(*rawAesGcmStoredKey)
	if !ok {
		return nil, fmt.Errorf("file encryption only supports raw AES keys, got %T", key)
	}
	keyBytes, err := rawAesKey.getKeyMaterial()
	if err != nil {
		return nil, fmt.Errorf("failed to get key material: %s", err.Error())
	}

	// Generate salt as UUID v4 (16 bytes)
	uuidV4 := uuid.New()
	salt := uuidV4[:]

	// Derive key using KBKDF
	derivedKeyBytes, err := deriveFileKey(keyBytes, salt)
	if err != nil {
		return nil, fmt.Errorf("failed to derive key: %s", err.Error())
	}

	// Create header (version 1)
	header := make([]byte, encryptedFileHeaderSize)
	copy(header, encryptedFileMagicString)
	header[encryptedFileMagicStringLen] = version
	header[encryptedFileMagicStringLen+1] = compressionType
	// Key derivation byte: lower 4 bits = method, upper 4 bits = 0
	// (PBKDF2 iterations not used for KBKDF)
	header[encryptedFileMagicStringLen+2] = kbkdfKeyDerivation
	// 3 bytes reserved (zeros)
	header[encryptedFileMagicStringLen+6] = encryptedFileKeyNameLength

	// Pad or truncate encryptionKeyName to exactly 36 bytes
	keyNameBytes := make([]byte, encryptedFileKeyNameLength)
	copy(keyNameBytes, encryptionKeyName)
	copy(header[encryptedFileMagicStringLen+7:], keyNameBytes)

	// Copy salt to header
	copy(header[encryptedFileMagicStringLen+7+int(encryptedFileKeyNameLength):], salt)

	ad := getEncryptedFileAD(header, encryptedFileHeaderSize)

	// Encrypt the data with derived key using AES-GCM directly
	encryptedData := aesgcmEncrypt(derivedKeyBytes, data, ad)

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
func (state *StoredKeysState) maybeDecryptFileData(filename string, data []byte, ctx *storedKeysCtx) ([]byte, string, storedKeyIface, string, error) {
	// Check if data is long enough to contain magic string
	if len(data) < encryptedFileMagicStringLen {
		logDbg("Data is too short to contain magic string, must be unencrypted")
		return data, "", nil, "", nil
	}

	logDbg("Decrypting file %s, data length: %d", filename, len(data))

	// Check for magic string
	if !bytes.Equal(data[:encryptedFileMagicStringLen], []byte(encryptedFileMagicString)) {
		logDbg("No magic string found, must be unencrypted")
		return data, "", nil, "", nil
	}

	// Validate header format
	header := data[:encryptedFileHeaderSize]
	keyName, version, keyDeriv, salt, err := validateEncryptedFileHeader(header)
	if err != nil {
		return nil, "", nil, "", err
	}

	logDbg("File is encrypted with key %s (version %d)", keyName, version)

	// Chunk format is 4 bytes size + encrypted data
	chunk := data[encryptedFileHeaderSize:]
	if len(chunk) < 4 {
		return nil, "", nil, "", fmt.Errorf("encrypted file too short to contain chunk size")
	}
	chunkSize := binary.BigEndian.Uint32(chunk[:4])
	if len(chunk) < 4+int(chunkSize) {
		return nil, "", nil, "", fmt.Errorf("encrypted file shorter than specified chunk size")
	}

	// Read the encryption key
	key, keyProof, err := state.readKey(keyName, intTokenEncryptionKeyKind, false, ctx)
	if err != nil {
		return nil, "", nil, "", fmt.Errorf("failed to read config dek: %s", err.Error())
	}

	// Get the raw key bytes for derivation
	// Cast to raw AES key - only raw AES keys are supported for file encryption
	rawAesKey, ok := key.(*rawAesGcmStoredKey)
	if !ok {
		return nil, "", nil, "", fmt.Errorf("file encryption only supports raw AES keys, got %T", key)
	}
	keyBytes, err := rawAesKey.getKeyMaterial()
	if err != nil {
		return nil, "", nil, "", fmt.Errorf("failed to get key material: %s", err.Error())
	}

	// Derive key if needed (version 1 with KBKDF)
	var derivedKeyBytes []byte
	if version == 1 && keyDeriv == 1 {
		// KBKDF key derivation
		derivedKeyBytes, err = deriveFileKey(keyBytes, salt)
		if err != nil {
			return nil, "", nil, "", fmt.Errorf("failed to derive key: %s", err.Error())
		}
	} else if version == 0 || (version == 1 && keyDeriv == 0) {
		// No key derivation
		derivedKeyBytes = keyBytes
	} else {
		return nil, "", nil, "", fmt.Errorf("unsupported key derivation method: %d", keyDeriv)
	}

	ad := getEncryptedFileAD(header, encryptedFileHeaderSize)
	decryptedData, err := aesgcmDecrypt(derivedKeyBytes, chunk[4:], ad)
	if err != nil {
		return nil, "", nil, "", fmt.Errorf("failed to decrypt data: %s", err.Error())
	}

	return decryptedData, keyName, key, keyProof, nil
}

// validateEncryptedFileHeader validates the encrypted file header and returns
// key name, version, key derivation method, and salt.
// Supports both version 0 and version 1 formats.
func validateEncryptedFileHeader(header []byte) (string, byte, byte, []byte, error) {
	const (
		compressionType = byte(0)
	)

	if len(header) < encryptedFileHeaderSize {
		return "", 0, 0, nil, fmt.Errorf("encrypted file header too short")
	}

	version := header[encryptedFileMagicStringLen]
	if version != 0 && version != 1 {
		return "", 0, 0, nil, fmt.Errorf("unsupported version: %d", version)
	}

	if header[encryptedFileMagicStringLen+1] != compressionType {
		return "", 0, 0, nil, fmt.Errorf("unsupported compression type: %d", header[encryptedFileMagicStringLen+1])
	}

	var keyDeriv byte
	if version == 1 {
		// Key derivation byte: lower 4 bits = method
		keyDeriv = header[encryptedFileMagicStringLen+2] & 0x0F
	} else {
		// Version 0: no key derivation
		keyDeriv = 0
	}

	if header[encryptedFileMagicStringLen+6] != encryptedFileKeyNameLength {
		return "", 0, 0, nil, fmt.Errorf("invalid key name length: %d", header[encryptedFileMagicStringLen+6])
	}

	keyNameBytes := header[encryptedFileMagicStringLen+7 : encryptedFileMagicStringLen+7+int(encryptedFileKeyNameLength)]
	if !utf8.Valid(keyNameBytes) {
		return "", 0, 0, nil, fmt.Errorf("invalid utf-8 in key name: %q", keyNameBytes)
	}

	keyName := string(keyNameBytes)

	// Extract salt (16 bytes at the end of header)
	saltStart := encryptedFileMagicStringLen + 7 + int(encryptedFileKeyNameLength)
	salt := make([]byte, 16)
	copy(salt, header[saltStart:saltStart+16])

	return keyName, version, keyDeriv, salt, nil
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
	keyName, _, _, _, err := validateEncryptedFileHeader(data)
	if err != nil {
		return "", err
	}

	return keyName, nil
}

func (state *StoredKeysState) storeKey(
	name,
	kind,
	keyType string,
	creationTime string,
	testOnly bool,
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
		keyInfo, err = newAesGcmKey(name, kind, creationTime, otherData)
		if err != nil {
			return err
		}
	} else if keyType == string(awskmKey) {
		keyInfo, err = newAwsKey(name, kind, creationTime, otherData)
		if err != nil {
			return err
		}
	} else if keyType == string(gcpkmKey) {
		keyInfo, err = newGcpKey(name, kind, creationTime, otherData)
		if err != nil {
			return err
		}
	} else if keyType == string(azurekmKey) {
		keyInfo, err = newAzureKey(name, kind, creationTime, otherData)
		if err != nil {
			return err
		}
	} else if keyType == string(hashikmKey) {
		keyInfo, err = newHashiKey(name, kind, creationTime, otherData)
		if err != nil {
			return err
		}
	} else if keyType == string(kmipKey) {
		keyInfo, err = newKmipKey(name, kind, creationTime, otherData)
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

	return nil
}

func (state *StoredKeysState) readKeyFromFile(pathWithoutVersion string, verifyProof bool, ctx *storedKeysCtx) (storedKeyIface, int, error) {
	keyIface, vsn, proof, err := readKeyFromFileRaw(pathWithoutVersion)
	if err != nil {
		return nil, vsn, fmt.Errorf("failed to read key from file %s: %s", pathWithoutVersion, err.Error())
	}
	err = state.decryptKey(keyIface, verifyProof, proof, ctx)
	if err != nil {
		return nil, vsn, err
	}
	return keyIface, vsn, nil
}

func (state *StoredKeysState) readKeyFromCache(name string, validateProof bool, ctx *storedKeysCtx) (bool, storedKeyIface, string) {
	if cachedKey, ok := state.keysCache[name]; ok && ctx.useCache {
		if validateProof && !cachedKey.proofValidated {
			err := state.validateKeyProof(cachedKey.keyIface, cachedKey.proof)
			if err != nil {
				logDbg("key integrity check failed for cached key %s: %s, will remove from cache", name, err.Error())
				delete(state.keysCache, name)
				return false, nil, ""
			}
			cachedKey.proofValidated = true
			state.keysCache[name] = cachedKey
		}
		logDbg("readKeyFromCache: using cached key %s", name)
		return true, cachedKey.keyIface, cachedKey.proof
	}
	return false, nil, ""
}

func (state *StoredKeysState) readKey(name, kind string, validateProof bool, ctx *storedKeysCtx) (storedKeyIface, string, error) {
	keySettings, err := getStoredKeyConfig(kind, ctx.storedKeyConfigs)
	if err != nil {
		return nil, "", err
	}
	if success, keyIface, proof := state.readKeyFromCache(name, validateProof, ctx); success {
		return keyIface, proof, nil
	}
	keyIface, _, proof, err := readKeyRaw(keySettings, name)
	if err != nil {
		// It is important to not strip ErrKeyNotFound here
		return nil, "", fmt.Errorf("failed to read key %s: %w", name, err)
	}
	err = state.decryptKey(keyIface, validateProof, proof, ctx)
	if err != nil {
		return nil, "", err
	}
	return keyIface, proof, nil
}

func (state *StoredKeysState) searchKey(keyKind, keyId string, ctx *storedKeysCtx) (storedKeyIface, error) {
	for _, cfg := range ctx.storedKeyConfigs {
		if (keyKind == cfg.KeyKind) || (keyKind == "") {
			if cfg.KeyKind == "bucketDek" {
				dirs, err := filepath.Glob(filepath.Join(cfg.Path, "*"))
				if err != nil {
					logDbg("Failed to read dir list using wildcard %s: %s", cfg.Path, err.Error())
					return nil, err
				}
				logDbg("searching in buckets dirs:%v", dirs)
				for _, dir := range dirs {
					fileInfo, err := os.Stat(dir)
					if err != nil || !fileInfo.IsDir() {
						logDbg("skipping non-dir %s", dir)
						continue
					}
					baseDir := filepath.Base(dir)
					if baseDir[0] == '.' || baseDir[0] == '@' {
						logDbg("skipping dir %s", baseDir)
						continue
					}
					keyName := filepath.Join(baseDir, "deks", keyId)
					keyIface, _, err := state.readKey(keyName, cfg.KeyKind, true, ctx)
					var keyNotFoundErr ErrKeyNotFound
					if errors.As(err, &keyNotFoundErr) {
						logDbg("skipping %s - key %s not present", baseDir, keyId)
						continue
					}

					if err != nil {
						return nil, err
					}
					return keyIface, nil
				}
			} else {
				logDbg("searching in %s", cfg.Path)
				keyIface, _, err := state.readKey(keyId, cfg.KeyKind, true, ctx)
				var keyNotFoundErr ErrKeyNotFound
				if errors.As(err, &keyNotFoundErr) {
					logDbg("key %s not found in %s", keyId, cfg.Path)
					continue
				}

				if err != nil {
					return nil, err
				}
				return keyIface, nil
			}
		}
	}
	return nil, fmt.Errorf("key %s not found", keyId)
}

func (state *StoredKeysState) cachedKeysList() []string {
	keys := make([]string, 0, len(state.keysCache))
	for keyName := range state.keysCache {
		keys = append(keys, keyName)
	}
	return keys
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
		err = state.validateKeyProof(keyIface, proof)
		if err != nil {
			return fmt.Errorf("key integrity check failed for key %s: %s", keyIface.name(), err.Error())
		}
	}
	if keyIface.canBeCached() {
		state.keysCache[keyIface.name()] = cachedKey{keyIface: keyIface, proof: proof, proofValidated: validateProof}
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
	proof, err := state.generateKeyProof(keyIface)
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
	if keyIface.canBeCached() {
		// we have just generated proof, so we can mark it as validated
		state.keysCache[keyIface.name()] = cachedKey{keyIface: keyIface, proof: proof, proofValidated: true}
	} else {
		if _, exists := state.keysCache[keyIface.name()]; exists {
			logDbg("Key %s is not cacheable, removing from cache", keyIface.name())
			delete(state.keysCache, keyIface.name())
		}
	}

	return nil
}

func (state *StoredKeysState) generateKeyProof(keyIface storedKeyIface) (string, error) {
	keyBytes, err := keyIface.asBytes()
	if err != nil {
		return "", fmt.Errorf("failed to convert key to bytes: %s", err.Error())
	}
	proofBytes, err := state.mac(keyBytes)
	if err != nil {
		return "", err
	}
	proofStr := base64.StdEncoding.EncodeToString(proofBytes)
	return proofStr, nil
}

func (state *StoredKeysState) validateKeyProof(keyIface storedKeyIface, proof string) error {
	if isLegacyProofFormat(proof) {
		return validateLegacyKeyProof(keyIface, proof, state.intTokens)
	}
	proofBytes, err := base64.StdEncoding.DecodeString(proof)
	if err != nil {
		return fmt.Errorf("failed to decode proof: %s", err.Error())
	}
	keyBytes, err := keyIface.asBytes()
	if err != nil {
		return fmt.Errorf("failed to convert key to bytes: %s", err.Error())
	}
	err = state.verifyMac(proofBytes, keyBytes)
	if err != nil {
		return err
	}
	return nil
}

func isLegacyProofFormat(proof string) bool {
	return proof[36] == ':'
}

func validateLegacyKeyProof(keyIface storedKeyIface, proof string, intTokens []intToken) error {
	// Legacy proof format is used in 7.9 only
	// Can be removed after 7.9 is no longer supported.
	// Format: uuid:encryptedTokenHash
	// where uuid is 36 bytes long string
	//       encryptedTokenHash is base64 encoded sha512(token.token | key name)
	//       encrypted with keyIface
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
			tokenHash := sha512.Sum512(append(token.token, []byte(keyIface.name())...))
			if bytes.Equal(tokenHash[:], decryptedTokenHash) {
				return nil
			}
			return fmt.Errorf("key integrity check failed: invalid integrity token (token uuid: %s, key name: %s)", uuid, keyIface.name())
		}
	}
	return fmt.Errorf("key integrity check failed: unknown token: %s (key name: %s)", uuid, keyIface.name())
}

func hmacSHA512(key []byte, data []byte) []byte {
	h := hmac.New(sha512.New, key)
	h.Write(data)
	mac := h.Sum(nil)
	return mac
}

func (state *StoredKeysState) encryptWithKey(keyKind, keyName string, data, AD []byte, ctx *storedKeysCtx) ([]byte, error) {
	if _, ok := ctx.keysTouched[keyName]; ok {
		return nil, fmt.Errorf("key encryption cycle")
	}
	keySettings, err := getStoredKeyConfig(keyKind, ctx.storedKeyConfigs)
	if err != nil {
		return nil, err
	}
	if success, cachedKeyIface, _ := state.readKeyFromCache(keyName, true, ctx); success {
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
	if success, cachedKeyIface, _ := state.readKeyFromCache(keyName, validateKeysProof, ctx); success {
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
	// If the path already contains ".key." it means it already contains version
	// Example path: /path/to/<uuid>.key.3
	if strings.Contains(path, ".key.") {
		// %% Check if the file actually exists before proceeding
		if _, err := os.Stat(path); err == nil {
			parts := strings.Split(filepath.Base(path), ".")
			if len(parts) >= 3 && parts[len(parts)-2] == "key" {
				// This is a versioned key file, extract version and return as-is
				vsnStr := parts[len(parts)-1]
				vsn, err := strconv.Atoi(vsnStr)
				if err != nil {
					return "", -1, fmt.Errorf(
						"invalid version in path %s: %s", path, err.Error())
				}
				return path, vsn, nil
			}
		} else {
			return "", -1, fmt.Errorf("file %s does not exist", path)
		}
	}
	// ... otherwise, we need to find the latest version for this key
	// Example path: /path/to/<uuid>
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

func readKeyFromFileRaw(pathMaybeWithoutVersion string) (storedKeyIface, int, string, error) {
	path, vsn, err := findKeyFile(pathMaybeWithoutVersion)
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

	if keyJson.Type == gcpkmKey {
		var k gcpStoredKey
		k.unmarshal(keyJson.Raw)
		return &k, vsn, keyJson.Proof, nil
	}

	if keyJson.Type == azurekmKey {
		var k azureStoredKey
		k.unmarshal(keyJson.Raw)
		return &k, vsn, keyJson.Proof, nil
	}

	if keyJson.Type == hashikmKey {
		var k hashiStoredKey
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
