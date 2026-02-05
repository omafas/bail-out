/**
 * Copyright (C) Couchbase, Inc 2021 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package hashicorputils

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/vault/api"
	"github.com/hashicorp/vault/api/auth/cert"
)

type OperationArgs struct {
	KeyURL              string
	TimeoutDuration     time.Duration
	KeyPath             string
	CertPath            string
	CbCaPath            string
	SelectCaOpt         string
	DecryptedPassphrase []byte
}

func KmsEncrypt(opArgs OperationArgs, data []byte, AD []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data to encrypt")
	}

	if err := validateArgs(opArgs); err != nil {
		return nil, err
	}

	ctx, cancel := getContextWithTimeout(opArgs.TimeoutDuration)
	defer cancel()

	client, keyId, err := getClientAndKeyId(ctx, opArgs)
	if err != nil {
		return nil, err
	}

	secret, err := client.Logical().WriteWithContext(
		ctx,
		path.Join("transit/encrypt", keyId),
		map[string]any{
			"plaintext":       data,
			"associated_data": AD,
		},
	)

	if err != nil {
		return nil, err
	}

	if secret == nil {
		return nil, fmt.Errorf("no secret returned")
	}

	if secret.Data["ciphertext"] == nil {
		return nil, fmt.Errorf("no ciphertext returned")
	}

	if encryptedData, ok := secret.Data["ciphertext"].(string); !ok {
		return nil, fmt.Errorf("ciphertext is not a string")
	} else {
		return []byte(encryptedData), nil
	}
}

func KmsDecrypt(opArgs OperationArgs, data []byte, AD []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data to decrypt")
	}

	if err := validateArgs(opArgs); err != nil {
		return nil, err
	}

	ctx, cancel := getContextWithTimeout(opArgs.TimeoutDuration)
	defer cancel()

	client, keyId, err := getClientAndKeyId(ctx, opArgs)
	if err != nil {
		return nil, err
	}

	out, err := client.Logical().WriteWithContext(
		ctx,
		path.Join("transit/decrypt", keyId),
		map[string]any{
			"ciphertext":      string(data),
			"associated_data": AD,
		},
	)
	if err != nil {
		return nil, err
	}

	if out.Data["plaintext"] == nil {
		return nil, fmt.Errorf("no plaintext returned")
	}

	if plaintext, ok := out.Data["plaintext"].(string); !ok {
		return nil, fmt.Errorf("plaintext is not a string")
	} else {
		return base64.StdEncoding.DecodeString(plaintext)
	}
}

func getClientSelectCaOpt(selectCaOpt string) (api.SelectCaOpt, error) {
	switch selectCaOpt {
	case "use_sys_ca":
		return api.UseSystemCAs, nil
	case "use_cb_ca":
		return api.UseCaCert, nil
	case "use_sys_and_cb_ca":
		return api.UseSystemCAsAndCaCert, nil
	case "skip_server_cert_verification":
		return api.SkipServerCertVerification, nil
	default:
		return api.UseSystemCAs, fmt.Errorf("invalid ca select option: %s", selectCaOpt)
	}
}

// The expected format of the URL is [http|https]://<Vault Host>(:<vault port>)?/<key name>
func getClientAndKeyId(ctx context.Context, opArgs OperationArgs) (*api.Client, string, error) {
	keyId, host, err := parseHashiCorpURL(opArgs.KeyURL)
	if err != nil {
		return nil, "", err
	}

	selectCaOpt, err := getClientSelectCaOpt(opArgs.SelectCaOpt)
	if err != nil {
		return nil, "", err
	}

	apiTLSConfig := &api.TLSConfigPkcs8{
		CACertPath:         opArgs.CbCaPath,
		ClientCertPath:     opArgs.CertPath,
		ClientKeyPkcs8Path: opArgs.KeyPath,
		CaSelection:        selectCaOpt,
	}

	cfg := api.DefaultConfig()
	if cfg.Error != nil {
		return nil, "", fmt.Errorf("could not create default config: %w", cfg.Error)
	}

	cfg.Address = host
	cfg.Timeout = opArgs.TimeoutDuration
	err = cfg.ConfigureTLSViaPkcs8Key(apiTLSConfig, opArgs.DecryptedPassphrase)
	if err != nil {
		return nil, "", fmt.Errorf("could not configure TLS: %w", err)
	}

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("could not create client: %w", err)
	}

	certAuth, err := cert.NewCertAuth()
	if err != nil {
		return nil, "", fmt.Errorf("could not create cert auth: %w", err)
	}

	_, err = client.Auth().Login(ctx, certAuth)
	if err != nil {
		return nil, "", fmt.Errorf("could not login with cert auth: %w", err)
	}

	return client, keyId, nil
}

// parseHashiCorpURL takes a string of the form [http|https]://<host>(:<port>)?/<key name> and separates the key from
// the rest. If the host given is invalid it will fail.“
func parseHashiCorpURL(keyURL string) (string, string, error) {
	parsed, err := url.Parse(keyURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid Hashi Corp Vault key url: %w", err)
	}

	if parsed.Host == "" {
		return "", "", fmt.Errorf("a host for the Hashi Corp Vault is required")
	}

	// In the case the path is empty or just "/"
	if len(parsed.Path) <= 1 {
		return "", "", fmt.Errorf("a key name is expected in the Hashi Corp Vault url")
	}

	key := strings.TrimPrefix(parsed.Path, "/")
	parsed.Path = ""

	return key, parsed.String(), nil
}

func validateArgs(opArgs OperationArgs) error {
	if opArgs.KeyURL == "" {
		return fmt.Errorf("key URL is required")
	}

	if len(opArgs.DecryptedPassphrase) == 0 {
		return fmt.Errorf("passphrase is required")
	}

	if opArgs.SelectCaOpt == "" {
		return fmt.Errorf("select ca option is required")
	}

	if opArgs.KeyPath == "" {
		return fmt.Errorf("key path is required")
	}

	if (opArgs.SelectCaOpt == "use_sys_and_cb_ca" || opArgs.SelectCaOpt == "use_cb_ca") &&
		opArgs.CbCaPath == "" {
		return fmt.Errorf("cbcapath is required")
	}

	return nil
}

func getContextWithTimeout(timeoutDuration time.Duration) (context.Context, context.CancelFunc) {
	if timeoutDuration > 0 {
		return context.WithTimeout(context.Background(), timeoutDuration)
	}
	return context.Background(), func() {}
}
