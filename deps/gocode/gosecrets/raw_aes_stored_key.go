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
	"strconv"
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
	Imported          bool   `json:"imported"`
}

// Struct represents raw aes-gcm stored key
type rawAesGcmStoredKey struct {
	baseStoredKey
	DecryptedKey      []byte
	EncryptedKey      []byte
	EncryptedByKind   string
	EncryptionKeyName string
	CanBeCached       bool
	Imported          bool
}

// Implementation of storedKeyIface for raw keys

func newAesGcmKey(name, kind, creationTime string, data []byte) (*rawAesGcmStoredKey, error) {
	type aesKeyTmp struct {
		KeyMaterial       []byte `json:"keyMaterial"`
		EncryptionKeyName string `json:"encryptionKeyName"`
		CanBeCached       bool   `json:"canBeCached"`
		Imported          bool   `json:"imported"`
	}
	var decoded aesKeyTmp
	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return nil, fmt.Errorf("invalid aes key json: %s", err.Error())
	}
	rawKeyInfo := &rawAesGcmStoredKey{
		baseStoredKey:     baseStoredKey{Name: name, Kind: kind, CreationTime: creationTime},
		EncryptionKeyName: decoded.EncryptionKeyName,
		DecryptedKey:      decoded.KeyMaterial,
		CanBeCached:       decoded.CanBeCached,
		Imported:          decoded.Imported,
	}
	return rawKeyInfo, nil
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
			onDiskKey.CanBeCached != k.CanBeCached ||
			onDiskKey.Imported != k.Imported
	return needsRewrite, vsn, nil
}

func (k *rawAesGcmStoredKey) ad() []byte {
	var extra string
	if k.Imported {
		extra = "imported"
	} else {
		// Must be empty to stay compatible with older versions, that
		// don't have this field
		extra = ""
	}
	return []byte(string(rawAESGCMKey) + k.Name + k.Kind + k.CreationTime + k.EncryptionKeyName + strconv.FormatBool(k.CanBeCached) + extra)
}

func (k *rawAesGcmStoredKey) asBytes() ([]byte, error) {
	if k.DecryptedKey == nil {
		return nil, fmt.Errorf("key %s is encrypted", k.Name)
	}
	return append(k.ad(), k.DecryptedKey...), nil
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
	k.Imported = decoded.Imported
	return nil
}

func (k *rawAesGcmStoredKey) usesSecretManagementKey() bool {
	return k.EncryptionKeyName == "encryptionService"
}

func (k *rawAesGcmStoredKey) canBeCached() bool {
	return k.CanBeCached
}

func (k *rawAesGcmStoredKey) getKeyMaterial() ([]byte, error) {
	if k.DecryptedKey == nil {
		return nil, fmt.Errorf("key %s is encrypted", k.Name)
	}
	return k.DecryptedKey, nil
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
		Imported:          k.Imported,
	})

	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal key %s: %s", k.Name, err.Error())
	}
	return rawAESGCMKey, data, nil
}
