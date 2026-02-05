package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"time"

	"github.com/pkg/errors"
)

type CertificateSet struct {
	CAKey  *ecdsa.PrivateKey
	CACert *x509.Certificate

	ServerKey  *ecdsa.PrivateKey
	ServerCert *x509.Certificate

	ClientKey  *ecdsa.PrivateKey
	ClientCert *x509.Certificate

	CAPool *x509.CertPool
}

func (set *CertificateSet) Generate(hostnames []string, ips []net.IP) error {
	notBefore := time.Now()
	notAfter := notBefore.Add(time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return errors.Wrapf(err, "failed to generate serial number")
	}

	set.CAKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return errors.Wrapf(err, "failt to generate CA key")
	}

	rootTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
			CommonName:   "Root CA",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &rootTemplate, &rootTemplate, &set.CAKey.PublicKey, set.CAKey)
	if err != nil {
		return errors.Wrapf(err, "error generating CA certificate")
	}

	set.CACert, err = x509.ParseCertificate(derBytes)
	if err != nil {
		return errors.Wrapf(err, "error parsing CA cert")
	}

	set.ServerKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return errors.Wrapf(err, "error generating server key")
	}

	serialNumber, err = rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return errors.Wrapf(err, "failed to generate serial number")
	}

	serverTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
			CommonName:   "test_cert_1",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		IPAddresses:           ips,
		DNSNames:              hostnames,
	}

	derBytes, err = x509.CreateCertificate(rand.Reader, &serverTemplate, &rootTemplate, &set.ServerKey.PublicKey, set.CAKey)
	if err != nil {
		return errors.Wrapf(err, "error generating server cert")
	}

	set.ServerCert, err = x509.ParseCertificate(derBytes)
	if err != nil {
		return errors.Wrapf(err, "error parsing server cert")
	}

	set.ClientKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return errors.Wrapf(err, "error generating client key")
	}

	clientTemplate := x509.Certificate{
		SerialNumber: new(big.Int).SetInt64(4),
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
			CommonName:   "client_auth_test_cert",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	derBytes, err = x509.CreateCertificate(rand.Reader, &clientTemplate, &rootTemplate, &set.ClientKey.PublicKey, set.CAKey)
	if err != nil {
		return errors.Wrapf(err, "error generating client cert")
	}

	set.ClientCert, err = x509.ParseCertificate(derBytes)
	if err != nil {
		return errors.Wrapf(err, "error parsing client cert")
	}

	set.CAPool = x509.NewCertPool()
	set.CAPool.AddCert(set.CACert)

	return nil
}
