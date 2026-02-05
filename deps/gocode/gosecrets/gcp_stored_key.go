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

	"github.com/couchbase/ns_server/deps/gocode/gcputils"
)

type gcpStoredKey struct {
	baseStoredKey
	KeyResourceId       string `json:"keyResourceId"`
	CredentialsFilePath string `json:"credentialsFile"`
	ReqTimeoutMs        int    `json:"reqTimeoutMs"`
}

// Implementation of storedKeyIface for gcp keys

func newGcpKey(name, kind, creationTime string, data []byte) (*gcpStoredKey, error) {
	var g gcpStoredKey
	err := json.Unmarshal(data, &g)
	if err != nil {
		return nil, fmt.Errorf("invalid json: %v", data)
	}
	g.baseStoredKey = baseStoredKey{Name: name, Kind: kind, CreationTime: creationTime}
	return &g, nil
}

func (k *gcpStoredKey) needRewrite(settings *storedKeyConfig, state *StoredKeysState, ctx *storedKeysCtx) (bool, int, error) {
	keyIface, vsn, _, err := readKeyRaw(settings, k.Name)
	if err != nil {
		var keyNotFoundErr ErrKeyNotFound
		if !errors.As(err, &keyNotFoundErr) {
			logDbg("key %s read error: %s", k.Name, err.Error())
		}
		return true, vsn, nil
	}
	onDiskKey, ok := keyIface.(*gcpStoredKey)
	if !ok {
		logDbg("key %s changed type, rewriting", k.Name)
		return true, vsn, nil
	}
	return !reflect.DeepEqual(k, onDiskKey), vsn, nil
}

func (k *gcpStoredKey) ad() []byte {
	return []byte("")
}

func (k *gcpStoredKey) asBytes() ([]byte, error) {
	return []byte(
		string(gcpkmKey) +
			k.Name +
			k.Kind +
			k.KeyResourceId +
			k.CredentialsFilePath +
			k.CreationTime +
			strconv.Itoa(k.ReqTimeoutMs)), nil
}

func (k *gcpStoredKey) encryptMe(state *StoredKeysState, ctx *storedKeysCtx) error {
	return nil
}

func (k *gcpStoredKey) decryptMe(validateKeysProof bool, state *StoredKeysState, ctx *storedKeysCtx) error {
	return nil
}

func (k *gcpStoredKey) checkGcpTestKey() (bool, error) {
	if k.KeyResourceId == "TEST_GCP_RESOURCE_ID" {
		return true, nil
	}
	return false, nil
}

func getGcpOperationArgs(k *gcpStoredKey) (*gcputils.OperationArgs, error) {
	if err := validateTimeout(k.ReqTimeoutMs); err != nil {
		return nil, err
	}

	return &gcputils.OperationArgs{
		KeyResourceId:     k.KeyResourceId,
		PathToServiceFile: k.CredentialsFilePath,
		TimeoutDuration:   time.Duration(k.ReqTimeoutMs) * time.Millisecond,
	}, nil
}

func (k *gcpStoredKey) encryptData(data, AD []byte) ([]byte, error) {
	if isTestKey, err := k.checkGcpTestKey(); isTestKey {
		if err != nil {
			return nil, err
		}
		// This code should be used for test purposes only
		logDbg("Encrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmEncrypt(zero_key, data, AD), nil
	}

	opArgs, err := getGcpOperationArgs(k)
	if err != nil {
		return nil, err
	}
	return gcputils.KmsEncrypt(*opArgs, data, AD)
}

func (k *gcpStoredKey) decryptData(data, AD []byte) ([]byte, error) {
	if isTestKey, err := k.checkGcpTestKey(); isTestKey {
		if err != nil {
			return nil, err
		}
		// This code should be used for test purposes only
		logDbg("Decrypting data using test key")
		zero_key := make([]byte, 32)
		return aesgcmDecrypt(zero_key, data, AD)
	}

	opArgs, err := getGcpOperationArgs(k)
	if err != nil {
		return nil, err
	}
	return gcputils.KmsDecrypt(*opArgs, data, AD)
}

func (k *gcpStoredKey) unmarshal(data json.RawMessage) error {
	err := json.Unmarshal(data, k)
	if err != nil {
		return fmt.Errorf("invalid raw key json: %s", err.Error())
	}
	return nil
}

func (k *gcpStoredKey) marshal() (storedKeyType, []byte, error) {
	data, err := json.Marshal(k)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal key %s: %s", k.Name, err.Error())
	}
	return gcpkmKey, data, nil
}

func (k *gcpStoredKey) usesSecretManagementKey() bool {
	return false
}

func (k *gcpStoredKey) canBeCached() bool {
	return false
}
