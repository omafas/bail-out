package kmiputils

import (
	tls "crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/akeylesslabs/go-kmip"
	tlsutils "github.com/couchbase/goutils/tls"
)

const KMIP_AUTH_TAG_LENGTH = 16

type KmipClientConfig struct {
	Host                string
	Port                int
	TimeoutDuration     time.Duration
	KeyPath             string
	CertPath            string
	CbCaPath            string
	SelectCaOpt         string
	DecryptedPassphrase []byte
}

type KmipEncrAttrs struct {
	EncrData       []byte
	IVCounterNonce []byte
	AuthTag        []byte
	AD             []byte
}

func getEncrCryptoParams() kmip.CryptoParams {
	return kmip.CryptoParams{
		CryptographicAlgorithm: kmip.CRYPTO_AES,
		BlockCipherMode:        kmip.BLOCK_MODE_GCM,
		TagLength:              KMIP_AUTH_TAG_LENGTH,
		PaddingMethod:          kmip.PADDING_METHOD_NONE,
		RandomIV:               true,
	}
}

func appendCaFromFile(cbCaPath string, certPool *x509.CertPool) error {
	cbCaCert, err := os.ReadFile(cbCaPath)
	if err != nil {
		return fmt.Errorf("failed to read CAs from file: %s: error: %w", cbCaPath, err)
	}

	ok := certPool.AppendCertsFromPEM(cbCaCert)
	if !ok {
		return fmt.Errorf("failed to append CAs to cert pool")
	}

	return nil
}

func getKmipClient(config KmipClientConfig) (*kmip.Client, error) {
	if len(config.DecryptedPassphrase) == 0 {
		return nil, fmt.Errorf("no passphrase")
	}

	cert, err := tlsutils.LoadX509KeyPair(config.CertPath, config.KeyPath, config.DecryptedPassphrase)
	if err != nil {
		if strings.Contains(err.Error(), "incorrect password") {
			return nil, fmt.Errorf("incorrect password for private key: full error: %w", err)
		}
		return nil, fmt.Errorf("could not load cert and key pair: %w", err)
	}

	var rootCAs *x509.CertPool
	var insureVerify bool
	if config.SelectCaOpt == "use_sys_and_cb_ca" || config.SelectCaOpt == "use_sys_ca" {
		rootCAs, err = x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to get sys CAs with error: %w", err)
		} else if rootCAs == nil {
			return nil, fmt.Errorf("failed to get sys CAs")
		}

		if config.SelectCaOpt == "use_sys_and_cb_ca" {
			err = appendCaFromFile(config.CbCaPath, rootCAs)
			if err != nil {
				return nil, err
			}
		}
	} else if config.SelectCaOpt == "use_cb_ca" {
		rootCAs = x509.NewCertPool()
		err = appendCaFromFile(config.CbCaPath, rootCAs)
		if err != nil {
			return nil, err
		}
	} else if config.SelectCaOpt == "skip_server_cert_verification" {
		insureVerify = true
	} else {
		return nil, fmt.Errorf("invalid ca select option: %s", config.SelectCaOpt)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            rootCAs,
		InsecureSkipVerify: insureVerify,
	}

	endPoint := fmt.Sprintf("%s:%d", config.Host, config.Port)
	client := kmip.Client{
		Endpoint:     endPoint,
		ReadTimeout:  config.TimeoutDuration,
		WriteTimeout: config.TimeoutDuration,
		DialTimeout:  config.TimeoutDuration,
		TLSConfig:    tlsConfig}

	err = client.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect client: %w", err)
	}

	return &client, nil
}

func KmipEncryptData(clientConfig KmipClientConfig, keyUid string, data []byte, AD []byte) (*KmipEncrAttrs, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data to encrypt")
	}

	client, err := getKmipClient(clientConfig)
	if err != nil {
		return nil, err
	}

	defer client.Close()

	cryptoParams := getEncrCryptoParams()
	encrReq := kmip.EncryptRequest{
		UniqueIdentifier: keyUid,
		CryptoParams:     cryptoParams,
		Data:             data,
		AdditionalData:   AD,
	}

	resp, err := client.Send(kmip.OPERATION_ENCRYPT, encrReq)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt: %w:", err)
	}

	authTagLen := len(resp.(kmip.EncryptResponse).AuthTag)
	if authTagLen != KMIP_AUTH_TAG_LENGTH {
		return nil, fmt.Errorf("invalid AuthTag length: %d, expected: %d", authTagLen, KMIP_AUTH_TAG_LENGTH)
	}

	kmipResp := KmipEncrAttrs{
		EncrData:       resp.(kmip.EncryptResponse).Data,
		IVCounterNonce: resp.(kmip.EncryptResponse).IVCounterNonce,
		AuthTag:        resp.(kmip.EncryptResponse).AuthTag,
		AD:             AD,
	}

	return &kmipResp, nil
}

func KmipDecryptData(clientConfig KmipClientConfig, keyUid string, encrAttrs KmipEncrAttrs) ([]byte, error) {
	if len(encrAttrs.EncrData) == 0 {
		return nil, fmt.Errorf("no data to decrypt")
	}

	client, err := getKmipClient(clientConfig)
	if err != nil {
		return nil, err
	}

	defer client.Close()

	decrReq := kmip.DecryptRequest{
		UniqueIdentifier: keyUid,
		CryptoParams:     getEncrCryptoParams(),
		Data:             encrAttrs.EncrData,
		AuthTag:          encrAttrs.AuthTag,
		IVCounterNonce:   encrAttrs.IVCounterNonce,
		AdditionalData:   encrAttrs.AD,
	}

	resp, err := client.Send(kmip.OPERATION_DECRYPT, decrReq)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w:", err)
	}

	decryptedData := resp.(kmip.DecryptResponse).Data
	return decryptedData, nil
}

func KmipGetAes256Key(clientConfig KmipClientConfig, keyUid string) ([]byte, error) {
	client, err := getKmipClient(clientConfig)
	if err != nil {
		return nil, err
	}

	defer client.Close()

	getReq := kmip.GetRequest{
		UniqueIdentifier: keyUid,
		KeyFormatType:    kmip.KEY_FORMAT_RAW,
	}

	resp, err := client.Send(kmip.OPERATION_GET, getReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get key with keyID %s: error %w:", keyUid, err)
	}

	key := resp.(kmip.GetResponse).SymmetricKey.KeyBlock.Value.KeyMaterial
	if len(key) != 32 {
		return nil, fmt.Errorf("invalid key size for expected key type AES-256: keyId %s", keyUid)
	}

	return key, nil
}
