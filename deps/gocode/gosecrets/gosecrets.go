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
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/sha512"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"

	"golang.org/x/crypto/pbkdf2"

	"encoding/base64"
	"strings"

	"github.com/couchbase/ns_server/deps/gocode/gocbutils"
)

const keySize = 32
const refSaltSize = 16
const nIterations = 4096

var hmacFun = sha1.New

var backwardCompatSalt = [8]byte{20, 183, 239, 38, 44, 214, 22, 141}

const saltSize = 8

type encryptionService struct {
	initialized     bool
	readOnly        bool
	reader          *bufio.Reader
	configPath      string
	config          *Config
	encryptionKeys  secretIface
	storedKeysState *StoredKeysState
}

var ErrKeysDoNotExist = errors.New("keys do not exist")
var ErrInvalidJson = errors.New("invalid json")

type ErrReadKeysError struct {
	e error
}

func (s ErrReadKeysError) Error() string { return s.e.Error() }
func (s ErrReadKeysError) Unwrap() error { return s.e }

type secretIface interface {
	read([]byte) error
	remove() error
	changePassword([]byte) error
	getPasswordState() string
	getSecret() *secret
	setSecret(*secret) error
	setSecretWithNewPassword(*secret, []byte) error
	getStorageId() string
	sameSettings(interface{}) bool
}

type secret struct {
	key       []byte // secret itself
	backupKey []byte // backup secret (used for decryption during rotation)
}

type keysInFile struct {
	filePath string
	secret
}

type keysInEncryptedFile struct {
	settings          map[string]interface{}
	lockkey           []byte // derived from password
	lockkeySalt       []byte
	isDefaultPassword bool // true if the password is default (empty)
	keysInFile
}

type keysViaScript struct {
	writeCmd     string
	readCmd      string
	deleteCmd    string
	cmdTimeoutMs int
	secret
}

type EncryptionServiceSettings struct {
	KeyStorageType     string                 `json:"keyStorageType"`
	KeyStorageSettings map[string]interface{} `json:"keyStorageSettings"`
}

type Config struct {
	EncryptionSettings EncryptionServiceSettings `json:"encryptionService"`
	StoredKeyConfigs   []storedKeyConfig         `json:"storedKeys"`
}

type DataKeyFileJson struct {
	Version     int    `json:"version"`
	Data        []byte `json:"data"`
	Encrypted   bool   `json:"encrypted"`
	LockkeySalt []byte `json:"lockkeySalt,omitempty"`
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			logDbg("panic occurred: %v\n%s", err, string(debug.Stack()))
			panic(err)
		}
	}()

	gocbutils.LimitCPUThreads()

	var configPath string

	flag.StringVar(&configPath, "config", "", "path to configuration file")
	flag.Parse()

	config, err := readCfg(configPath)

	if err != nil {
		panic(err.Error())
	}

	s := &encryptionService{
		reader:          bufio.NewReader(os.Stdin),
		readOnly:        true,
		configPath:      configPath,
		config:          config,
		storedKeysState: nil,
	}

	for {
		s.processCommand()
	}
}

func (s *encryptionService) readCommand() (byte, []byte) {
	var size uint32
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
	err := binary.Write(os.Stdout, binary.BigEndian, uint32(len(data)))
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

func unwrapDataKeys(data []byte) ([]byte, []byte, error) {
	key, data2, err := readField(data)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid datakeys: %s", err.Error())
	}
	backup, _, err := readField(data2)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid datakeys: %s", err.Error())
	}
	return key, backup, nil
}

func (s *encryptionService) processCommand() {
	command, data := s.readCommand()

	switch command {
	case 1:
		s.cmdInitReadOnly(data)
	case 2:
		s.cmdGetKeyRef()
	case 3:
		s.cmdEncrypt(data)
	case 4:
		s.cmdDecrypt(data)
	case 5:
		s.cmdChangePassword(data)
	case 6:
		s.cmdRotateDataKey()
	case 7:
		s.cmdClearBackupKey(data)
	case 8:
		s.cmdGetState()
	case 9:
		s.cmdReloadConfig(data)
	case 10:
		s.cmdCopySecrets(data)
	case 11:
		s.cmdCleanupSecrets(data)
	case 12:
		s.cmdStoreKey(data)
	case 13:
		s.cmdEncryptWithKey(data)
	case 14:
		s.cmdDecryptWithKey(data)
	case 15:
		s.cmdReadKey(data)
	case 16:
		s.cmdReadKeyFile(data)
	case 17:
		s.cmdRotateIntegrityTokens(data)
	case 18:
		s.cmdRemoveOldIntegrityTokens(data)
	case 19:
		s.cmdGetKeyIdInUse()
	case 20:
		s.cmdInit(data)
	case 21:
		s.cmdMac(data)
	case 22:
		s.cmdVerifyMac(data)
	case 23:
		s.cmdRevalidateKeyCache()
	case 24:
		s.cmdSearchKey(data)
	default:
		panic(fmt.Sprintf("Unknown command %v", command))
	}
}

func (s *encryptionService) cmdGetState() {
	replySuccessWithData([]byte(s.encryptionKeys.getPasswordState()))
}

func (s *encryptionService) cmdInit(data []byte) {
	readOnlyBytes, data := readBigField(data)
	readOnly := (readOnlyBytes[0] != 0)
	passwordBytes, _ := readBigField(data)
	password := decodePass(passwordBytes)
	err := s.mainInit(readOnly, password)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdInitReadOnly(data []byte) {
	password := decodePass(data)
	err := s.mainInit(true, password)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) mainInit(readOnly bool, password []byte) error {
	var err error
	s.encryptionKeys, err = initEncryptionKeys(s.config)
	if err != nil {
		return err
	}

	err = readOrCreateKeys(s.encryptionKeys, password, readOnly)
	if err != nil {
		if errors.Is(err, ErrKeysDoNotExist) && readOnly {
			logDbg("keys do not exist and read-only mode is enabled, will not create data keys")
		} else {
			return err
		}
	}

	s.storedKeysState, err = initStoredKeys(filepath.Dir(s.configPath), readOnly, s.newStoredKeyCtx())
	if err != nil {
		return fmt.Errorf("failed to initialize stored keys: %s", err.Error())
	}
	s.initialized = true
	s.readOnly = readOnly
	return nil
}

func decodePass(data []byte) []byte {
	var password []byte
	if data[0] == 1 {
		password = data[1:]
	}
	return password
}

func initEncryptionKeys(config *Config) (secretIface, error) {
	if config.EncryptionSettings.KeyStorageType == "file" {
		settings := config.EncryptionSettings.KeyStorageSettings
		return initKeysFromFile(settings)
	} else if config.EncryptionSettings.KeyStorageType == "script" {
		settings := config.EncryptionSettings.KeyStorageSettings
		return initKeysViaScript(settings)
	}

	return nil, fmt.Errorf(
		"unknown encryption service key storage type: %s",
		config.EncryptionSettings.KeyStorageType)
}

func initKeysViaScript(settings map[string]interface{}) (*keysViaScript, error) {
	readCmd, found := settings["readCmd"].(string)
	if !found {
		return nil, errors.New(
			"readCmd is mandatory for this type of secret")
	}

	writeCmd, found := settings["writeCmd"].(string)
	if !found {
		writeCmd = ""
	}

	deleteCmd, found := settings["deleteCmd"].(string)
	if !found {
		deleteCmd = ""
	}

	timeoutMs, found := settings["cmdTimeoutMs"].(int)
	if !found {
		timeoutMs = 60000
	}

	return &keysViaScript{
		readCmd:      readCmd,
		writeCmd:     writeCmd,
		deleteCmd:    deleteCmd,
		cmdTimeoutMs: timeoutMs,
		secret:       secret{},
	}, nil
}

func initKeysFromFile(settings map[string]interface{}) (secretIface, error) {
	datakeyFile := settings["path"].(string)

	encryptDatakey := (settings["encryptWithPassword"] == true)

	if !encryptDatakey {
		return &keysInFile{filePath: datakeyFile, secret: secret{}}, nil
	}

	return &keysInEncryptedFile{
		settings: settings,
		keysInFile: keysInFile{
			filePath: datakeyFile,
			secret:   secret{}},
	}, nil
}

func initFilePassword(settings map[string]interface{}, password []byte) ([]byte, error) {
	passwordSource := settings["passwordSource"].(string)
	var passwordToUse []byte
	if passwordSource == "env" {
		pwdSettings, ok := settings["passwordSettings"].(map[string]interface{})
		if !ok {
			return nil, errors.New(
				"passwordSettings are missing in config")
		}

		envName, found := pwdSettings["envName"].(string)
		if !found {
			return nil, errors.New(
				"envName is missing in config")
		}
		if password != nil {
			passwordToUse = password
		} else {
			passwordToUse = []byte(os.Getenv(envName))
		}
	} else if passwordSource == "script" {
		if password != nil {
			return nil, errors.New(
				"password is not nil")
		}
		pwdSettings, ok := settings["passwordSettings"].(map[string]interface{})
		if !ok {
			return nil, errors.New(
				"passwordSettings are missing in config")
		}
		passwordCmd, found := pwdSettings["passwordCmd"].(string)
		if !found {
			return nil, errors.New(
				"passwordCmd is missing in config")
		}
		cmdTimeoutMs, found := pwdSettings["cmdTimeoutMs"].(int)
		if !found {
			cmdTimeoutMs = 60000
		}
		output, err := callExternalScript(passwordCmd, cmdTimeoutMs)
		if err != nil {
			return nil, err
		}
		passwordToUse = []byte(output)
	} else {
		return nil, fmt.Errorf("unknown password source: %s", passwordSource)
	}
	return passwordToUse, nil
}

func saveDatakey(datakeyFile string, combinedKeysData []byte, encrypted bool, lockkeySalt []byte) error {
	datakeyDir := filepath.Dir(datakeyFile)
	err := os.MkdirAll(datakeyDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create dir for data key \"%s\": %s", datakeyDir, err.Error())
	}
	toMarshal := DataKeyFileJson{
		Version:     0,
		Data:        combinedKeysData,
		Encrypted:   encrypted,
		LockkeySalt: lockkeySalt,
	}
	fileData, err := json.Marshal(toMarshal)
	if err != nil {
		return fmt.Errorf("failed to marshal datakeys: %s", err.Error())
	}
	err = atomicWriteFile(datakeyFile, fileData, 0640)
	if err != nil {
		return fmt.Errorf("failed to write datakey file \"%s\": %s",
			datakeyFile, err.Error())
	}
	return nil
}

func readDatakey(datakeyFile string) ([]byte, bool, []byte, error) {
	data, err := os.ReadFile(datakeyFile)
	if os.IsNotExist(err) {
		logDbg("file %s does not exist", datakeyFile)
		return nil, false, nil, ErrKeysDoNotExist
	} else if err != nil {
		msg := fmt.Sprintf("failed to read datakey file \"%s\": %s",
			datakeyFile, err)
		return nil, false, nil, ErrReadKeysError{e: errors.New(msg)}
	}

	var dataKeyFileJson DataKeyFileJson
	err = json.Unmarshal(data, &dataKeyFileJson)
	if err != nil {
		logDbg("Failed to parse json in data key file: %s", err.Error())
		return nil, false, nil, ErrReadKeysError{e: ErrInvalidJson}
	}

	if dataKeyFileJson.Version != 0 {
		return nil, false, nil, ErrReadKeysError{e: fmt.Errorf("not supported version of datakey file: %d", dataKeyFileJson.Version)}
	}

	return dataKeyFileJson.Data, dataKeyFileJson.Encrypted, dataKeyFileJson.LockkeySalt, nil
}

func readDatakeyBackwardCompat(datakeyFile string) ([]byte, []byte, error) {
	data, err := os.ReadFile(datakeyFile)
	if os.IsNotExist(err) {
		logDbg("file %s does not exist", datakeyFile)
		return nil, nil, ErrKeysDoNotExist
	} else if err != nil {
		msg := fmt.Sprintf("failed to read datakey file \"%s\": %s",
			datakeyFile, err)
		return nil, nil, ErrReadKeysError{e: errors.New(msg)}
	}
	key, backup, err := unwrapDataKeys(data)
	if err != nil {
		return nil, nil, ErrReadKeysError{e: err}
	}
	return key, backup, nil
}

func readField(b []byte) ([]byte, []byte, error) {
	if len(b) == 0 {
		return nil, nil, errors.New("empty data")
	}
	size := b[0]
	if len(b) < int(size)+1 {
		return nil, nil, errors.New("data too short")
	}
	if size == 0 {
		return nil, b[size+1:], nil
	}
	return b[1 : size+1], b[size+1:], nil
}

// same as readField by the size is encoded by 4 byte uint
func readBigField(data []byte) ([]byte, []byte) {
	sizeBin := data[0:4]
	size := binary.BigEndian.Uint32(sizeBin)
	return data[4 : size+4], data[(size + 4):]
}

func (s *encryptionService) cmdGetKeyRef() {
	replySuccessWithData(s.encryptionKeys.getSecret().getRef())
}

func (s *encryptionService) cmdEncrypt(data []byte) {
	if !s.initialized {
		panic("Password was not set")
	}
	if err := s.enforceReadOnly(); err != nil {
		replyError(err.Error())
		return
	}
	dataKey := s.encryptionKeys.getSecret().key
	replySuccessWithData(encrypt(dataKey, data))
}

func (s *encryptionService) enforceReadOnly() error {
	if s.readOnly && s.encryptionKeys.getSecret().key == nil {
		return fmt.Errorf("no data key found (read-only mode)")
	}
	return nil
}

func (s *encryptionService) cmdDecrypt(data []byte) {
	if !s.initialized {
		panic("Password was not set")
	}
	if err := s.enforceReadOnly(); err != nil {
		replyError(err.Error())
		return
	}

	key := s.encryptionKeys.getSecret().key

	res, mainKeyErr := decrypt(key, data)
	if mainKeyErr == nil {
		replySuccessWithData(res)
		return
	}

	backupKey := s.encryptionKeys.getSecret().backupKey

	if backupKey == nil {
		replyError(
			fmt.Sprintf(
				"Unable to decrypt value using main key: %s",
				mainKeyErr.Error()))
		return
	}

	res, backupKeyErr := decrypt(backupKey, data)
	if backupKeyErr != nil {
		replyError(
			fmt.Sprintf(
				"Unable to decrypt value using main key: '%s' "+
					"and using backup key: '%s'",
				mainKeyErr.Error(),
				backupKeyErr.Error()))
		return
	}
	replySuccessWithData(res)
}

func (s *encryptionService) cmdChangePassword(data []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	password := decodePass(data)
	if !s.initialized {
		panic("Password was not set")
	}
	err := s.encryptionKeys.changePassword(password)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdRotateDataKey() {
	if !s.initialized {
		panic("Password was not set")
	}
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	backupKey := s.encryptionKeys.getSecret().backupKey
	if backupKey != nil {
		replyError("Data key rotation is in progress")
		return
	}
	prevkey := s.encryptionKeys.getSecret().key
	newkey := createRandomKey()
	err := saveKeys(s.encryptionKeys, newkey, prevkey)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdClearBackupKey(ref []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	backupKey := s.encryptionKeys.getSecret().backupKey
	if backupKey == nil {
		replySuccess()
		return
	}

	salt := ref[:refSaltSize]
	if !bytes.Equal(s.encryptionKeys.getSecret().getRefWithSalt(salt), ref) {
		replyError("Key ref mismatch")
		return
	}
	err := reencryptStoredKeys(s.storedKeysState, s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	key := s.encryptionKeys.getSecret().key
	err = saveKeys(s.encryptionKeys, key, nil)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdReloadConfig(data []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	password := decodePass(data)
	newConfig, err := readCfg(s.configPath)
	if err != nil {
		replyError(err.Error())
		return
	}
	newEncryptionKeys, err := initEncryptionKeys(newConfig)
	if err != nil {
		replyError(err.Error())
		return
	}

	err = readOrCreateKeys(newEncryptionKeys, password, s.readOnly)
	if err != nil {
		replyError(err.Error())
		return
	}
	s.encryptionKeys = newEncryptionKeys
	s.config = newConfig
	replySuccess()
}

func (s *encryptionService) cmdCopySecrets(newCfgBytes []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	newConfig, err := readCfgBytes(newCfgBytes)
	if err != nil {
		replyError(err.Error())
		return
	}
	newEncryptionKeys, err := initEncryptionKeys(newConfig)
	if err != nil {
		replyError(err.Error())
		return
	}
	res, err := copySecret(s.encryptionKeys, newEncryptionKeys, nil)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccessWithData([]byte(res))
}

func (s *encryptionService) cmdCleanupSecrets(oldCfgBytes []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	oldConfig, err := readCfgBytes(oldCfgBytes)
	if err != nil {
		replyError(err.Error())
		return
	}
	oldKeys, err := initEncryptionKeys(oldConfig)
	if err != nil {
		replyError(err.Error())
		return
	}
	// We absolutelly must not remove secrets that are currently in use
	if oldKeys.getStorageId() == s.encryptionKeys.getStorageId() {
		replyError(fmt.Errorf(
			"can't remove secret '%s' because it is being used",
			oldKeys.getStorageId()).Error())
		return
	}
	logDbg("trying to remove secret: %s", oldKeys.getStorageId())
	err = oldKeys.remove()
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdStoreKey(data []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	keyKind, data := readBigField(data)
	keyName, data := readBigField(data)
	keyType, data := readBigField(data)
	otherData, data := readBigField(data)
	encryptionKeyNameBin, data := readBigField(data)
	creationTime, data := readBigField(data)
	testOnly, data := readBigField(data)
	canBeCachedBin, _ := readBigField(data)
	keyKindStr := string(keyKind)
	keyNameStr := string(keyName)
	keyTypeStr := string(keyType)
	encryptionKeyName := string(encryptionKeyNameBin)
	creationTimeStr := string(creationTime)
	testOnlyBool := (string(testOnly) == "true")
	canBeCached := (string(canBeCachedBin) == "true")

	if testOnlyBool {
		logDbg("Received request to test key %s (kind: %s, type: %s, encryptionKey: %s)",
			keyNameStr, keyKindStr, keyTypeStr, encryptionKeyName)
	} else {
		logDbg("Received request to store key %s (kind: %s, type: %s, encryptionKey: %s, canBeCached: %t) on disk",
			keyNameStr, keyKindStr, keyTypeStr, encryptionKeyName, canBeCached)
	}

	ctx := s.newStoredKeyCtx()
	err := s.storedKeysState.storeKey(
		keyNameStr,
		keyKindStr,
		keyTypeStr,
		encryptionKeyName,
		creationTimeStr,
		testOnlyBool,
		canBeCached,
		otherData,
		ctx)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdReadKeyFile(data []byte) {
	keyPath, _ := readBigField(data)
	keyPathStr := string(keyPath)
	keyIface, _, err := s.storedKeysState.readKeyFromFile(keyPathStr, s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	replyReadKey(keyIface)
}

func (s *encryptionService) cmdReadKey(data []byte) {
	keyKind, data := readBigField(data)
	keyName, _ := readBigField(data)
	keyKindStr := string(keyKind)
	keyNameStr := string(keyName)
	keyIface, err := s.storedKeysState.readKey(keyNameStr, keyKindStr, true, s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	replyReadKey(keyIface)
}

func (s *encryptionService) cmdRotateIntegrityTokens(data []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	keyName, _ := readBigField(data)
	keyNameStr := string(keyName)
	err := s.storedKeysState.rotateIntegrityTokens(keyNameStr, s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdRemoveOldIntegrityTokens(data []byte) {
	if s.readOnly {
		replyError("read-only mode is enabled")
		return
	}
	var paths []string
	for len(data) > 0 {
		path, dataLeft := readBigField(data)
		paths = append(paths, string(path))
		data = dataLeft
	}
	err := s.storedKeysState.removeOldIntegrityTokens(paths, s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdGetKeyIdInUse() {
	keyId, err := s.storedKeysState.getKeyIdInUse()
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccessWithData([]byte(keyId))
}

func (s *encryptionService) cmdMac(data []byte) {
	mac, err := s.storedKeysState.mac(data)
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccessWithData(mac)
}

func (s *encryptionService) cmdVerifyMac(data []byte) {
	mac, data := readBigField(data)
	data, _ = readBigField(data)

	err := s.storedKeysState.verifyMac(mac, data)

	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccess()
}

func (s *encryptionService) cmdRevalidateKeyCache() {
	s.storedKeysState.revalidateKeyCache(s.newStoredKeyCtx())
	replySuccess()
}

func (s *encryptionService) cmdSearchKey(data []byte) {
	keyKind, data := readBigField(data)
	keyName, _ := readBigField(data)
	keyKindStr := string(keyKind)
	keyNameStr := string(keyName)
	keyIface, err := s.storedKeysState.searchKey(keyKindStr, keyNameStr, s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	replyReadKey(keyIface)
}

func replyReadKey(keyIface storedKeyIface) {
	rawKey, ok := keyIface.(*rawAesGcmStoredKey)
	if !ok {
		replyError("key type not supported")
		return
	}
	keyBase64 := base64.StdEncoding.EncodeToString(rawKey.DecryptedKey)
	keyToMarshal := readKeyAesKeyResponse{
		Key:             keyBase64,
		EncryptionKeyId: rawKey.EncryptionKeyName,
		CreationTime:    rawKey.CreationTime,
	}
	keyJson, err := json.Marshal(keyToMarshal)
	if err != nil {
		replyError(fmt.Sprintf("failed to marshal key: %s", err.Error()))
		return
	}
	replyToMarshal := readKeyReply{
		Type: string(rawAESGCMKey),
		Info: keyJson,
	}
	replyJson, err := json.Marshal(replyToMarshal)
	if err != nil {
		replyError(fmt.Sprintf("failed to marshal reply: %s", err.Error()))
		return
	}
	replySuccessWithData(replyJson)
}

func (s *encryptionService) cmdEncryptWithKey(data []byte) {
	toEncrypt, data := readBigField(data)
	AD, data := readBigField(data)
	keyKindBin, data := readBigField(data)
	keyNameBin, _ := readBigField(data)
	keyKind := string(keyKindBin)
	keyName := string(keyNameBin)

	encryptedData, err := s.storedKeysState.encryptWithKey(
		keyKind,
		keyName,
		toEncrypt,
		AD,
		s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccessWithData(encryptedData)
}

func (s *encryptionService) cmdDecryptWithKey(data []byte) {
	toDecrypt, data := readBigField(data)
	AD, data := readBigField(data)
	keyKindBin, data := readBigField(data)
	keyNameBin, _ := readBigField(data)
	keyKind := string(keyKindBin)
	keyName := string(keyNameBin)

	decryptedData, err := s.storedKeysState.decryptWithKey(
		keyKind,
		keyName,
		toDecrypt,
		AD,
		true,
		s.newStoredKeyCtx())
	if err != nil {
		replyError(err.Error())
		return
	}
	replySuccessWithData(decryptedData)
}

func (s *encryptionService) newStoredKeyCtx() *storedKeysCtx {
	return &storedKeysCtx{
		storedKeyConfigs:           s.config.StoredKeyConfigs,
		encryptionServiceKey:       s.encryptionKeys.getSecret().key,
		backupEncryptionServiceKey: s.encryptionKeys.getSecret().backupKey,
		keysTouched:                map[string]bool{},
	}
}

func generateLockKey(password []byte) ([]byte, []byte) {
	salt := generateRandomBytes(saltSize)
	lockkey := generateLockKeyWithSalt(password, salt)
	return lockkey, salt
}

func generateLockKeyWithSalt(password []byte, salt []byte) []byte {
	return pbkdf2.Key(password, salt, nIterations, keySize, hmacFun)
}

func logDbg(str string, args ...interface{}) {
	doReply([]byte("L" + fmt.Sprintf(str, args...)))
}

// Implementation of secretIface for keysInFile

func (keys *keysInFile) setSecret(s *secret) error {
	combinedKeysData := combineDataKeys(s.key, s.backupKey)
	err := saveDatakey(keys.filePath, combinedKeysData, false, nil)
	if err != nil {
		return err
	}
	copySecretStruct(s, &keys.secret)
	return nil
}

func (keys *keysInFile) setSecretWithNewPassword(s *secret, password []byte) error {
	return keys.setSecret(s)
}

func (keys *keysInFile) readBackwardCompat(password []byte) error {
	key, backupKey, err := readDatakeyBackwardCompat(keys.filePath)
	if err != nil {
		return err
	}
	keys.secret.key = key
	keys.secret.backupKey = backupKey
	logDbg("backward compat read encryption keys from '%s' (%d byte main key, "+
		"and %d byte backup key)", keys.filePath, len(key), len(backupKey))
	return nil
}

func (keys *keysInFile) read(password []byte) error {
	keysData, encrypted, _, err := readDatakey(keys.filePath)
	if err != nil {
		var readErr ErrReadKeysError
		if errors.As(err, &readErr) {
			if errors.Is(errors.Unwrap(readErr), ErrInvalidJson) {
				logDbg("Failed to parse datakey file, will try old format")
				return keys.readBackwardCompat(password)
			}
		}
		return err
	}
	if encrypted {
		return errors.New("data keys are not expected to be encrypted")
	}
	key, backup, err := unwrapDataKeys(keysData)
	if err != nil {
		return ErrReadKeysError{e: err}
	}
	keys.secret.key = key
	keys.secret.backupKey = backup
	logDbg("read encryption keys from '%s' (%d byte main key, "+
		"and %d byte backup key)", keys.filePath, len(key), len(backup))
	return nil
}

func (keys *keysInFile) remove() error {
	return os.Remove(keys.filePath)
}

func (keys *keysInFile) changePassword(password []byte) error {
	return errors.New("not supported")
}

func (keys *keysInFile) getPasswordState() string {
	return "password_not_used"
}

func (secret *secret) getRefWithSalt(salt []byte) []byte {
	res := sha512.Sum512(append(salt, combineDataKeys(secret.key, secret.backupKey)...))
	return append(salt, res[:]...)
}

func (secret *secret) getRef() []byte {
	salt := generateRandomBytes(refSaltSize)
	return secret.getRefWithSalt(salt)
}

func (sec *secret) getSecret() *secret {
	return sec
}

func (keys *keysInFile) getStorageId() string {
	return "file:" + keys.filePath
}

func (keys *keysInFile) sameSettings(param interface{}) bool {
	keys2, ok := param.(*keysInFile)
	if !ok {
		return false
	}
	oldsecret := keys2.secret
	keys2.secret = keys.secret
	defer func() {
		keys2.secret = oldsecret
	}()
	return reflect.DeepEqual(keys, keys2)
}

// Implementation of secretIface for keysInEncryptedFile

func (keys *keysInEncryptedFile) setSecret(s *secret) error {
	newSecret := secret{}
	copySecretStruct(s, &newSecret)
	if len(keys.lockkey) == 0 {
		return fmt.Errorf("can't encrypt datakey, empty lockkey")
	}
	combinedKeysData := combineDataKeys(newSecret.key, newSecret.backupKey)
	encryptedKeysData := encrypt(keys.lockkey, combinedKeysData)
	err := saveDatakey(keys.filePath, encryptedKeysData, true, keys.lockkeySalt)
	if err != nil {
		return err
	}
	keys.keysInFile.secret = newSecret
	return nil
}

func (keys *keysInEncryptedFile) setSecretWithNewPassword(s *secret, password []byte) error {
	passwordToUse, err := initFilePassword(keys.settings, password)
	if err != nil {
		return err
	}
	oldLockkey := keys.lockkey
	oldLockkeySalt := keys.lockkeySalt
	keys.lockkey, keys.lockkeySalt = generateLockKey(passwordToUse)

	err = keys.setSecret(s)

	if err != nil {
		keys.lockkey = oldLockkey
		keys.lockkeySalt = oldLockkeySalt
		return err
	}
	keys.isDefaultPassword = (len(passwordToUse) == 0)
	return nil
}

func (keys *keysInEncryptedFile) readBackwardCompat(password []byte) error {
	encryptedKey, encryptedBackup, err := readDatakeyBackwardCompat(keys.filePath)
	if err != nil {
		return err
	}
	passwordToUse, err := initFilePassword(keys.settings, password)
	if err != nil {
		return err
	}
	keys.isDefaultPassword = (len(passwordToUse) == 0)
	keys.lockkeySalt = backwardCompatSalt[:]
	keys.lockkey = generateLockKeyWithSalt(passwordToUse, keys.lockkeySalt)

	key, err := decrypt(keys.lockkey, encryptedKey)
	if err != nil {
		return ErrReadKeysError{e: fmt.Errorf("key decrypt failed: %s", err.Error())}
	}
	keys.secret.key = key

	if len(encryptedBackup) > 0 {
		backup, err := decrypt(keys.lockkey, encryptedBackup)
		if err != nil {
			return fmt.Errorf("backup key decrypt failed: %s", err.Error())
		}
		keys.secret.backupKey = backup
	} else {
		keys.secret.backupKey = nil
	}

	logDbg("backward compat read encryption keys from '%s' (%d byte main key, "+
		"and %d byte backup key)",
		keys.filePath, len(encryptedKey), len(encryptedBackup))
	return nil
}

func (keys *keysInEncryptedFile) read(password []byte) error {
	keysEncryptedData, encrypted, lockkeySalt, err := readDatakey(keys.filePath)
	if err != nil {
		var readErr ErrReadKeysError
		if errors.As(err, &readErr) {
			if errors.Is(errors.Unwrap(readErr), ErrInvalidJson) {
				logDbg("Failed to parse datakey file, will try old format")
				return keys.readBackwardCompat(password)
			}
		}
		return err
	}
	if !encrypted {
		return errors.New("data keys are expected to be encrypted")
	}
	// Calling initFilePassword after reading the file because
	// in case if file doesn't exist we don't want the password script
	// to be called twice
	passwordToUse, err := initFilePassword(keys.settings, password)
	if err != nil {
		return err
	}

	keys.isDefaultPassword = (len(passwordToUse) == 0)
	keys.lockkeySalt = lockkeySalt
	keys.lockkey = generateLockKeyWithSalt(passwordToUse, keys.lockkeySalt)
	keysData, err := decrypt(keys.lockkey, keysEncryptedData)

	if err != nil {
		return ErrReadKeysError{e: fmt.Errorf("key decrypt failed: %s", err.Error())}
	}

	key, backup, err := unwrapDataKeys(keysData)
	if err != nil {
		return ErrReadKeysError{e: err}
	}

	logDbg("read encryption keys from '%s' (%d byte main key, "+
		"and %d byte backup key)",
		keys.filePath, len(key), len(backup))

	keys.secret.key = key
	keys.secret.backupKey = backup

	return nil
}

func (keys *keysInEncryptedFile) changePassword(password []byte) error {
	return keys.setSecretWithNewPassword(keys.getSecret(), password)
}

func (keys *keysInEncryptedFile) getPasswordState() string {
	if keys.isDefaultPassword {
		return "default"
	}
	return "user_configured"
}

func (keys *keysInEncryptedFile) sameSettings(param interface{}) bool {
	keys2, ok := param.(*keysInEncryptedFile)
	if !ok {
		return false
	}
	oldsecret := keys2.secret
	oldLockkey := keys2.lockkey
	oldLockkeySalt := keys2.lockkeySalt
	oldIsDefaultPass := keys2.isDefaultPassword
	keys2.secret = keys.secret
	keys2.lockkey = keys.lockkey
	keys2.lockkeySalt = keys.lockkeySalt
	keys2.isDefaultPassword = keys.isDefaultPassword
	defer func() {
		keys2.secret = oldsecret
		keys2.lockkey = oldLockkey
		keys2.lockkeySalt = oldLockkeySalt
		keys2.isDefaultPassword = oldIsDefaultPass
	}()
	return reflect.DeepEqual(keys, keys2)
}

// Implementation of secretIface for keysViaScript

func (keys *keysViaScript) setSecret(s *secret) error {
	keyBase64 := base64.StdEncoding.EncodeToString(s.getSecret().key)
	backupBase64 := base64.StdEncoding.EncodeToString(s.getSecret().backupKey)
	cmd := strings.Join([]string{keys.writeCmd, keyBase64, backupBase64}, " ")
	_, err := callExternalScript(cmd, keys.cmdTimeoutMs)
	if err != nil {
		return err
	}
	copySecretStruct(s, &keys.secret)
	return nil
}

func (keys *keysViaScript) setSecretWithNewPassword(s *secret, password []byte) error {
	return keys.setSecret(s)
}

func (keys *keysViaScript) read(password []byte) error {
	res, err := callExternalScript(keys.readCmd, keys.cmdTimeoutMs)
	if err != nil {
		return err
	}
	keysTokens := strings.Fields(res)
	var backup []byte
	if len(keysTokens) == 2 {
		backup, err = base64.StdEncoding.DecodeString(keysTokens[1])
		if err != nil {
			return fmt.Errorf("failed to decode backup key "+
				"(expected to be base64 encoded): %s", err.Error())
		}
	} else if len(keysTokens) == 1 {
		backup = nil
	} else if len(keysTokens) == 0 {
		return ErrKeysDoNotExist
	} else {
		return fmt.Errorf(
			"unexpected number of keys (%v) returned by %s",
			len(keysTokens), keys.readCmd)
	}
	key, err := base64.StdEncoding.DecodeString(keysTokens[0])
	if err != nil {
		return fmt.Errorf(
			"failed to decode key (expected to be base64 encoded): %s",
			err.Error())
	}
	keys.secret.key = key
	keys.secret.backupKey = backup
	logDbg("read keys via script '%s'", keys.readCmd)
	return nil
}

func (keys *keysViaScript) remove() error {
	_, err := callExternalScript(keys.deleteCmd, keys.cmdTimeoutMs)
	if err != nil {
		return err
	}
	return nil
}

func (keys *keysViaScript) changePassword(password []byte) error {
	return errors.New("not supported")
}

func (keys *keysViaScript) getPasswordState() string {
	return "password_not_used"
}

func (keys *keysViaScript) getStorageId() string {
	return "script:" + keys.writeCmd
}

func (keys *keysViaScript) sameSettings(param interface{}) bool {
	keys2, ok := param.(*keysViaScript)
	if !ok {
		return false
	}
	oldsecret := keys2.secret
	keys2.secret = keys.secret
	defer func() {
		keys2.secret = oldsecret
	}()
	return reflect.DeepEqual(keys, keys2)
}

// Other functions:

func copySecret(from, to secretIface, password []byte) (string, error) {
	logDbg("Trying to copy a secret\nOld cfg type: %v\nNew cfg type: %v",
		reflect.TypeOf(from), reflect.TypeOf(to))

	if from.sameSettings(to) {
		logDbg("same secret configs, nothing to do")
		return "same", nil
	}

	// Here we are trying to make sure we are not corrupting existing secrets
	// by creating secrets copy for new config.
	// For example, if new and old config use the same file on disk, we might
	// corrupt old secret file by writing to the same file using new config.
	err := to.read(password)

	if err != nil {
		if !errors.Is(err, ErrKeysDoNotExist) {
			// We can't continue even if it is caused by a different password
			// because the copy will basically change the password for
			// the secret then, and rollback will not work in case of a problem
			var readErr ErrReadKeysError
			if errors.As(err, &readErr) {
				return "", fmt.Errorf(
					"secret already exists but it can't be read (%s)",
					err.Error())
			} else {
				// Should not modify the error in this case,
				// because it can be something unrelated to
				// keys read (e.g. password extraction error)
				return "", err
			}
		}
		logDbg("New secret doesn't exist")
	} else {
		// Even if new config uses the same storage for this secret,
		// it should be safe to overwrite it using new config
		// because it should not really change the file (but we still
		// want to do the writing, because we want to test that it
		// works)
		secretsMatch := bytes.Equal(from.getSecret().key, to.getSecret().key) &&
			bytes.Equal(from.getSecret().backupKey, to.getSecret().backupKey)
		if !secretsMatch {
			logDbg(
				"New secret already exists and it doesn't " +
					"match the secret that is in use")
			oldStorage := from.getStorageId()
			newStorage := to.getStorageId()
			if oldStorage == newStorage {
				return "", fmt.Errorf(
					"can't use exactly same storage for secret "+
						"(old storage: %s, new storage: %s)",
					oldStorage, newStorage)
			}
			// that's ok, new and old configs use different storages
			// for secrets, so we will not overwrite existing secret
			// if we save new secret here
		} else {
			logDbg("New secret already exists and it matches" +
				"the secret that is in use")
		}
	}
	err = to.setSecretWithNewPassword(from.getSecret(), password)
	if err != nil {
		return "", err
	}
	// just making sure it is readable
	err = to.read(password)
	if err != nil {
		logDbg("Failed to read the secret after writing: %s", err.Error())
		return "", err
	}
	return "copied", nil
}

func saveKeys(keys secretIface, key, backup []byte) error {
	newSecret := secret{key: key, backupKey: backup}
	return keys.setSecret(&newSecret)
}

func saveKeysWithNewPassword(keys secretIface, key, backup, password []byte) error {
	newSecret := secret{key: key, backupKey: backup}
	return keys.setSecretWithNewPassword(&newSecret, password)
}

// Note: keys must be a pointer to a struct
func readOrCreateKeys(keys secretIface, password []byte, readOnly bool) error {
	err := keys.read(password)
	if errors.Is(err, ErrKeysDoNotExist) {
		if readOnly {
			return err
		}
		logDbg("Generating secret")
		key := createRandomKey()
		return saveKeysWithNewPassword(keys, key, nil, password)
	}
	return err
}

func readCfg(configPath string) (*Config, error) {
	configBytes, err := os.ReadFile(configPath)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to read config file (\"%s\"): %s",
			configPath, err.Error())
	}
	return readCfgBytes(configBytes)
}

func readCfgBytes(configBytes []byte) (*Config, error) {
	logDbg("parsing config: \n%s", string(configBytes))
	var config Config
	err := json.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to parse config file: %s\n%s",
			err.Error(), configBytes)
	}

	return &config, nil
}

func copySecretStruct(from *secret, to *secret) {
	to.key = make([]byte, len(from.key))
	copy(to.key, from.key)
	if from.backupKey == nil {
		to.backupKey = nil
	} else {
		to.backupKey = make([]byte, len(from.backupKey))
		copy(to.backupKey, from.backupKey)
	}
}
