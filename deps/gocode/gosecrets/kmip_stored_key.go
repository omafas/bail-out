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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/couchbase/ns_server/deps/gocode/kmiputils"
)

type kmipStoredKey struct {
	baseStoredKey
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
}

// Implementation of storedKeyIface for kmip keys

func newKmipKey(name, kind, creationTime string, data []byte) (*kmipStoredKey, error) {
	type kmipKeyTmp struct {
		KmipId             string `json:"kmipId"`
		Host               string `json:"host"`
		Port               int    `json:"port"`
		ReqTimeoutMs       int    `json:"reqTimeoutMs"`
		KeyPath            string `json:"keyPath"`
		CertPath           string `json:"certPath"`
		Passphrase         []byte `json:"keyPassphrase"`
		EncryptionKeyName  string `json:"encryptionKeyName"`
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
		baseStoredKey:       baseStoredKey{Name: name, Kind: kind, CreationTime: creationTime},
		KmipId:              decoded.KmipId,
		Host:                decoded.Host,
		Port:                decoded.Port,
		ReqTimeoutMs:        decoded.ReqTimeoutMs,
		KeyPath:             decoded.KeyPath,
		CertPath:            decoded.CertPath,
		CaSelection:         decoded.CaSelection,
		CbCaPath:            decoded.CbCaPath,
		EncryptionApproach:  decoded.EncryptionApproach,
		EncryptionKeyName:   decoded.EncryptionKeyName,
		decryptedPassphrase: decoded.Passphrase,
	}
	return rawKeyInfo, nil
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

func (k *kmipStoredKey) asBytes() ([]byte, error) {
	if k.decryptedPassphrase == nil {
		return nil, fmt.Errorf("key %s is encrypted", k.Name)
	}
	return append(k.ad(), k.decryptedPassphrase...), nil
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

func (k *kmipStoredKey) checkKmipTestKey() bool {
	if k.KeyPath == "TEST_KMIP_KEY_PATH" {
		return true
	}
	return false
}

func getKmipClientCfg(k *kmipStoredKey) (*kmiputils.KmipClientConfig, error) {
	if err := validateTimeout(k.ReqTimeoutMs); err != nil {
		return nil, err
	}

	return &kmiputils.KmipClientConfig{
		Host:                k.Host,
		Port:                k.Port,
		TimeoutDuration:     time.Duration(k.ReqTimeoutMs) * time.Millisecond,
		KeyPath:             k.KeyPath,
		CertPath:            k.CertPath,
		CbCaPath:            k.CbCaPath,
		SelectCaOpt:         k.CaSelection,
		DecryptedPassphrase: k.decryptedPassphrase,
	}, nil
}

func (k *kmipStoredKey) encryptData(data, AD []byte) ([]byte, error) {
	if k.checkKmipTestKey() {
		// This code should be used for test purposes only
		logDbg("Encrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmEncrypt(zero_key, data, AD), nil
	}

	clientCfg, err := getKmipClientCfg(k)
	if err != nil {
		return nil, err
	}
	switch k.EncryptionApproach {
	case "use_encrypt_decrypt":
		encrAttrs, err := kmiputils.KmipEncryptData(*clientCfg, k.KmipId, data, AD)
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
	case "use_get":
		aes256Key, err := kmiputils.KmipGetAes256Key(*clientCfg, k.KmipId)
		if err != nil {
			return nil, err
		}

		encrData := aesgcmEncrypt(aes256Key, data, AD)
		return append([]byte{KMIP_USE_GET}, encrData...), nil
	default:
		return nil, fmt.Errorf("invalid encrypton approach %s", k.EncryptionApproach)
	}
}

func (k *kmipStoredKey) decryptData(data, AD []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("invalid data length: %d", len(data))
	}

	if k.checkKmipTestKey() {
		// This code should be used for test purposes only
		logDbg("Decrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmDecrypt(zero_key, data, AD)
	}

	clientCfg, err := getKmipClientCfg(k)
	if err != nil {
		return nil, err
	}
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
		return kmiputils.KmipDecryptData(*clientCfg, k.KmipId, encrAttrs)
	} else if uint8(data[0]) == KMIP_USE_GET {
		aes256Key, err := kmiputils.KmipGetAes256Key(*clientCfg, k.KmipId)
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
