// @author Couchbase <info@couchbase.com>
// @copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package azureutils

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/couchbase/tools-common/types/v2/ptr"
)

type OperationArgs struct {
	KeyURL           string
	Algorithm        string
	CredentialsChain string
	TimeoutDuration  time.Duration
}

func KmsEncrypt(opArgs OperationArgs, plainText, AD []byte) ([]byte, string, error) {
	if len(plainText) == 0 {
		return nil, "", fmt.Errorf("no data to encrypt")
	}

	if err := validateArgs(opArgs); err != nil {
		return nil, "", err
	}

	toEncrypt := []byte(base64.URLEncoding.EncodeToString(plainText))
	options, err := getEncrOptions(toEncrypt, AD, opArgs.Algorithm)
	if err != nil {
		return nil, "", err
	}

	baseURL, keyName, err := parseAzureURL(opArgs.KeyURL)
	if err != nil {
		return nil, "", err
	}

	client, err := getAzureClient(baseURL, opArgs.CredentialsChain)
	if err != nil {
		return nil, "", err
	}

	ctx, cancel := getContextWithTimeout(opArgs.TimeoutDuration)
	defer cancel()

	res, err := client.Encrypt(ctx, keyName, "", *options, nil)
	if err != nil {
		return nil, "", fmt.Errorf("could not encrypt data: %w", err)
	}

	if res.Result == nil {
		return nil, "", fmt.Errorf("empty cipher text returned")
	}

	if res.KID == nil {
		return nil, "", fmt.Errorf("no key ID returned")
	}

	if version := res.KID.Version(); version == "" {
		return nil, "", fmt.Errorf("no key version returned")
	} else {
		return res.Result, version, nil
	}
}

func KmsDecrypt(opArgs OperationArgs, keyVersion string, cipherText, AD []byte) ([]byte, error) {
	if len(cipherText) == 0 {
		return nil, fmt.Errorf("no data to decrypt")
	}

	if err := validateArgs(opArgs); err != nil {
		return nil, err
	}

	options, err := getEncrOptions(cipherText, AD, opArgs.Algorithm)
	if err != nil {
		return nil, err
	}

	baseURL, keyName, err := parseAzureURL(opArgs.KeyURL)
	if err != nil {
		return nil, err
	}

	client, err := getAzureClient(baseURL, opArgs.CredentialsChain)
	if err != nil {
		return nil, err
	}

	ctx, cancel := getContextWithTimeout(opArgs.TimeoutDuration)
	defer cancel()

	res, err := client.Decrypt(ctx, keyName, keyVersion, *options, nil)
	if err != nil {
		return nil, fmt.Errorf("could not decrypt data: %w", err)
	}

	if res.Result == nil {
		return nil, fmt.Errorf("empty plain text returned")
	}

	plain, err := base64.URLEncoding.DecodeString(string(res.Result))
	if err != nil {
		return nil, fmt.Errorf("could not base64 decode key: %w", err)
	}

	return plain, nil
}

func validateArgs(opArgs OperationArgs) error {
	if opArgs.KeyURL == "" {
		return fmt.Errorf("no Azure Key Vault URL")
	}

	if opArgs.Algorithm == "" {
		return fmt.Errorf("no encryption algorithm")
	}

	trimmedChain := strings.TrimSpace(opArgs.CredentialsChain)
	if trimmedChain == "" {
		return fmt.Errorf("credentials chain cannot be empty")
	}
	chainParts := strings.Split(trimmedChain, ",")
	for _, part := range chainParts {
		if part == "defaultCredentialsChain" && len(chainParts) != 1 {
			return fmt.Errorf("defaultCredentialsChain must not be mixed with other credential options in chain")
		}
		if strings.TrimSpace(part) == "" {
			return fmt.Errorf("credentials chain contains an empty value")
		}
	}

	return nil
}

func getEncrOptions(value, AD []byte, encryptionAlgorithm string) (*azkeys.KeyOperationParameters, error) {
	azKeysEncrAlgorithm, err := getAzureKeysEncryptionAlgorithm(encryptionAlgorithm)
	if err != nil {
		return nil, err
	}
	return &azkeys.KeyOperationParameters{
		Algorithm:                   ptr.To(azKeysEncrAlgorithm),
		Value:                       value,
		AdditionalAuthenticatedData: AD,
	}, nil
}

func getAzureClient(baseURL string, credentialsChain string) (*azkeys.Client, error) {
	var (
		creds         azcore.TokenCredential
		err           error
		errorMessages []string
	)
	if credentialsChain == "defaultCredentialsChain" {
		creds, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("could not get credentials for Azure: %w", err)
		}
	} else {
		var chainCreds []azcore.TokenCredential
		for _, item := range strings.Split(credentialsChain, ",") {
			switch strings.TrimSpace(item) {
			case "environment":
				c, e := azidentity.NewEnvironmentCredential(nil)
				if e == nil {
					chainCreds = append(chainCreds, c)
				} else {
					errorMessages = append(errorMessages, "EnvironmentCredential: "+e.Error())
				}
			case "workloadIdentity":
				c, e := azidentity.NewWorkloadIdentityCredential(nil)
				if e == nil {
					chainCreds = append(chainCreds, c)
				} else {
					errorMessages = append(errorMessages, "WorkloadIdentityCredential: "+e.Error())
				}
			case "managedIdentity":
				c, e := azidentity.NewManagedIdentityCredential(nil)
				if e == nil {
					chainCreds = append(chainCreds, c)
				} else {
					errorMessages = append(errorMessages, "ManagedIdentityCredential: "+e.Error())
				}
			case "azureCli":
				c, e := azidentity.NewAzureCLICredential(nil)
				if e == nil {
					chainCreds = append(chainCreds, c)
				} else {
					errorMessages = append(errorMessages, "AzureCLICredential: "+e.Error())
				}
			case "azureDeveloperCli":
				c, e := azidentity.NewAzureDeveloperCLICredential(nil)
				if e == nil {
					chainCreds = append(chainCreds, c)
				} else {
					errorMessages = append(errorMessages, "AzureDeveloperCLICredential: "+e.Error())
				}
			default:
				return nil, fmt.Errorf("unsupported credential type in chain: %q", strings.TrimSpace(item))
			}
		}
		if len(chainCreds) == 0 {
			return nil, fmt.Errorf("could not setup credentials chain: %s", strings.Join(errorMessages, ";"))
		}
		creds, err = azidentity.NewChainedTokenCredential(chainCreds, nil)
		if err != nil {
			return nil, fmt.Errorf("could not create chained token credential: %w", err)
		}
	}

	client, err := azkeys.NewClient(baseURL, creds, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create an Azure KMS client: %w", err)
	}

	return client, nil
}

func getAzureKeysEncryptionAlgorithm(algorithm string) (azkeys.EncryptionAlgorithm, error) {
	if len(strings.TrimSpace(algorithm)) == 0 {
		return "", fmt.Errorf("empty encryption algorithm")
	}

	switch algorithm {
	case "A128CBC":
		return azkeys.EncryptionAlgorithmA128CBC, nil
	case "A128CBCPAD":
		return azkeys.EncryptionAlgorithmA128CBCPAD, nil
	case "A128GCM":
		return azkeys.EncryptionAlgorithmA128GCM, nil
	case "A128KW":
		return azkeys.EncryptionAlgorithmA128KW, nil
	case "A192CBC":
		return azkeys.EncryptionAlgorithmA192CBC, nil
	case "A192CBCPAD":
		return azkeys.EncryptionAlgorithmA192CBCPAD, nil
	case "A192GCM":
		return azkeys.EncryptionAlgorithmA192GCM, nil
	case "A192KW":
		return azkeys.EncryptionAlgorithmA192KW, nil
	case "A256CBC":
		return azkeys.EncryptionAlgorithmA256CBC, nil
	case "A256CBCPAD":
		return azkeys.EncryptionAlgorithmA256CBCPAD, nil
	case "A256GCM":
		return azkeys.EncryptionAlgorithmA256GCM, nil
	case "A256KW":
		return azkeys.EncryptionAlgorithmA256KW, nil
	case "CKMAESKEYWRAP":
		return azkeys.EncryptionAlgorithmCKMAESKEYWRAP, nil
	case "CKMAESKEYWRAPPAD":
		return azkeys.EncryptionAlgorithmCKMAESKEYWRAPPAD, nil
	case "RSA15":
		return azkeys.EncryptionAlgorithmRSA15, nil
	case "RSAOAEP":
		return azkeys.EncryptionAlgorithmRSAOAEP, nil
	case "RSAOAEP256":
		return azkeys.EncryptionAlgorithmRSAOAEP256, nil
	default:
		return "", fmt.Errorf("unsupported encryption algorithm: %s", algorithm)
	}
}

func checkForVaultOrHsm(host string) error {
	if strings.HasPrefix(host, ".") {
		return fmt.Errorf("no vault or managedhsm name found in domain: %s", host)
	}

	labels := strings.Split(host, ".")
	if len(labels) < 2 {
		return fmt.Errorf("no vault or managedhsm name found in domain: %s", host)
	}

	return nil
}

// The Azure Key Value URL is validated as follows:
//
// Format:
// https://{vault-or-hsm-name}.{host-domain}/keys/{key-name}
//
//  1. It must be https
//  3. It must have a {vault-or-hsm-name}
//  3. It must have a {host-domain}
//  4. It must have a path that starts with /keys/
//  5. It must have a {key-name}
//  5. The {key-name} must be the top key name and NOT refer to a specific version of key
//
// If any of these conditions are not met, an error is returned. The error message
// will indicate the specific condition that was not met.
func parseAzureURL(urlStr string) (string, string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse azure key URL: %w", err)
	}

	if u.Scheme != "https" {
		return "", "", fmt.Errorf("azure key URL must be https")
	}

	if u.Host == "" {
		return "", "", fmt.Errorf("no host found in azure key URL")
	}

	if !strings.HasPrefix(u.Path, "/keys/") {
		return "", "", fmt.Errorf("path must be exactly /keys/<name> with no additional segment")
	}

	keyName := strings.TrimPrefix(u.Path, "/keys/")
	if keyName == "" {
		return "", "", fmt.Errorf("no key name found in azure key URL")
	}

	if strings.Contains(keyName, "/") {
		return "", "", fmt.Errorf("key version not allowed in azure key URL, "+
			"use only root key name in: %s", keyName)
	}

	if err := checkForVaultOrHsm(u.Host); err != nil {
		return "", "", err
	}

	return "https://" + u.Host, keyName, nil
}

func getContextWithTimeout(timeoutDuration time.Duration) (context.Context, context.CancelFunc) {
	if timeoutDuration > 0 {
		return context.WithTimeout(context.Background(), timeoutDuration)
	}
	return context.Background(), func() {}
}
