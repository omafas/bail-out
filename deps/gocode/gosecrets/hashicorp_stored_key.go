// @author Couchbase <info@couchbase.com>
// @copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/couchbase/ns_server/deps/gocode/hashicorputils"
)

type hashiStoredKey struct {
	baseStoredKey
	KeyURL              string `json:"keyURL"`
	ReqTimeoutMs        int    `json:"reqTimeoutMs"`
	CaSelection         string `json:"caSelection"`
	CbCaPath            string `json:"CbCaPath"`
	KeyPath             string `json:"keyPath"`
	CertPath            string `json:"certPath"`
	EncryptedPassphrase []byte `json:"sealedPassphrase"`
	decryptedPassphrase []byte `json:"-"`
	EncryptionKeyName   string `json:"encryptionKeyName"`
	EncryptedByKind     string `json:"encryptedByKind"`
}

// Implementation of storedKeyIface for hashicorp keys

func newHashiKey(name, kind, creationTime string, data []byte) (*hashiStoredKey, error) {
	type hashiKeyTmp struct {
		KeyURL            string `json:"keyURL"`
		ReqTimeoutMs      int    `json:"reqTimeoutMs"`
		KeyPath           string `json:"keyPath"`
		CertPath          string `json:"certPath"`
		Passphrase        []byte `json:"keyPassphrase"`
		EncryptionKeyName string `json:"encryptionKeyName"`
		CaSelection       string `json:"caSelection"`
		CbCaPath          string `json:"CbCaPath"`
	}
	var decoded hashiKeyTmp
	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return nil, fmt.Errorf("invalid raw key json: %s", err.Error())
	}

	rawKeyInfo := &hashiStoredKey{
		baseStoredKey:       baseStoredKey{Name: name, Kind: kind, CreationTime: creationTime},
		KeyURL:              decoded.KeyURL,
		ReqTimeoutMs:        decoded.ReqTimeoutMs,
		CaSelection:         decoded.CaSelection,
		CbCaPath:            decoded.CbCaPath,
		KeyPath:             decoded.KeyPath,
		CertPath:            decoded.CertPath,
		decryptedPassphrase: decoded.Passphrase,
		EncryptionKeyName:   decoded.EncryptionKeyName,
	}
	return rawKeyInfo, nil
}

func (k *hashiStoredKey) needRewrite(settings *storedKeyConfig, state *StoredKeysState, ctx *storedKeysCtx) (bool, int, error) {
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
	onDiskKey, ok := keyIface.(*hashiStoredKey)
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

func (k *hashiStoredKey) ad() []byte {
	return []byte(
		string(hashikmKey) +
			k.Name +
			k.Kind +
			k.KeyURL +
			strconv.Itoa(k.ReqTimeoutMs) +
			k.KeyPath +
			k.CertPath +
			k.CaSelection +
			k.EncryptionKeyName +
			k.CreationTime)
}

func (k *hashiStoredKey) asBytes() ([]byte, error) {
	if k.decryptedPassphrase == nil {
		return nil, fmt.Errorf("key %s is encrypted", k.Name)
	}
	return append(k.ad(), k.decryptedPassphrase...), nil
}

func (k *hashiStoredKey) encryptMe(state *StoredKeysState, ctx *storedKeysCtx) error {
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

func (k *hashiStoredKey) decryptMe(validateKeysProof bool, state *StoredKeysState, ctx *storedKeysCtx) error {
	decryptedPass, _, err := decryptKeyData(k, k.EncryptedPassphrase, k.EncryptedByKind, k.EncryptionKeyName, validateKeysProof, state, ctx)
	if err != nil {
		return err
	}
	k.decryptedPassphrase = decryptedPass
	return nil
}

func (k *hashiStoredKey) checkHashiTestKey() (bool, error) {
	if k.KeyURL == "TEST_HASHI_KEY_URL" {
		return true, nil
	}
	return false, nil
}

func getHashiClientCfg(k *hashiStoredKey) (*hashicorputils.OperationArgs, error) {
	if err := validateTimeout(k.ReqTimeoutMs); err != nil {
		return nil, err
	}

	return &hashicorputils.OperationArgs{
		KeyURL:              k.KeyURL,
		TimeoutDuration:     time.Duration(k.ReqTimeoutMs) * time.Millisecond,
		KeyPath:             k.KeyPath,
		CertPath:            k.CertPath,
		CbCaPath:            k.CbCaPath,
		SelectCaOpt:         k.CaSelection,
		DecryptedPassphrase: k.decryptedPassphrase,
	}, nil
}

func (k *hashiStoredKey) encryptData(data, AD []byte) ([]byte, error) {
	if isTestKey, err := k.checkHashiTestKey(); isTestKey {
		if err != nil {
			return nil, err
		}
		// This code should be used for test purposes only
		logDbg("Encrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmEncrypt(zero_key, data, AD), nil
	}

	clientCfg, err := getHashiClientCfg(k)
	if err != nil {
		return nil, err
	}

	return hashicorputils.KmsEncrypt(*clientCfg, data, AD)
}

func (k *hashiStoredKey) decryptData(data, AD []byte) ([]byte, error) {
	if isTestKey, err := k.checkHashiTestKey(); isTestKey {
		if err != nil {
			return nil, err
		}
		// This code should be used for test purposes only
		logDbg("Decrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmDecrypt(zero_key, data, AD)
	}

	clientCfg, err := getHashiClientCfg(k)
	if err != nil {
		return nil, err
	}

	return hashicorputils.KmsDecrypt(*clientCfg, data, AD)
}

func (k *hashiStoredKey) unmarshal(data json.RawMessage) error {
	err := json.Unmarshal(data, k)
	if err != nil {
		return fmt.Errorf("invalid raw key json: %s", err.Error())
	}
	return nil
}

func (k *hashiStoredKey) usesSecretManagementKey() bool {
	return k.EncryptionKeyName == "encryptionService"
}

func (k *hashiStoredKey) canBeCached() bool {
	return false
}

func (k *hashiStoredKey) marshal() (storedKeyType, []byte, error) {
	if k.EncryptedPassphrase == nil {
		return "", nil, fmt.Errorf("can't store key \"%s\" to disk because the key is not encrypted", k.Name)
	}
	data, err := json.Marshal(k)

	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal key %s: %s", k.Name, err.Error())
	}
	return hashikmKey, data, nil
}
