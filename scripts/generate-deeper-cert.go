// @author Couchbase <info@couchbase.com>
// @copyright 2013 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"os"
	"time"
)

func mustNoErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var earlyNotBefore = time.Date(2013, 1, 1, 12, 0, 0, 0, time.UTC)

// that's max date that current golang x509 code supports
var earlyNotAfter = time.Date(2049, 12, 31, 23, 59, 59, 0, time.UTC)

func pemIfy(octets []byte, pemType string, out io.Writer) {
	pem.Encode(out, &pem.Block{
		Type:  pemType,
		Bytes: octets,
	})
}

func main() {
	pkey, err := rsa.GenerateKey(rand.Reader, 2048)
	mustNoErr(err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(0),
		IsCA: true,
		NotBefore: earlyNotBefore,
		NotAfter: earlyNotAfter,
		Subject: pkix.Name{
			CommonName: "ns_server",
		},
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	caCertDer, err := x509.CreateCertificate(rand.Reader, &template, &template, &pkey.PublicKey, pkey)
	mustNoErr(err)

	leafTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    earlyNotBefore,
		NotAfter:     earlyNotAfter,
		Subject: pkix.Name{
			CommonName: "beta.local",
		},
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	leafPKey, err:= rsa.GenerateKey(rand.Reader, 2048)
	mustNoErr(err)
	caCert, err := x509.ParseCertificate(caCertDer)
	mustNoErr(err)
	cert2, err := x509.CreateCertificate(rand.Reader, &leafTemplate, caCert, &leafPKey.PublicKey, pkey)
	mustNoErr(err)

	pemIfy(cert2, "CERTIFICATE", os.Stdout)
	pemIfy(caCertDer, "CERTIFICATE", os.Stdout)
	pemIfy(x509.MarshalPKCS1PrivateKey(leafPKey), "RSA PRIVATE KEY", os.Stdout)

	log.Print("Going to verify leaf certificate with 'CA' certificate")

	leafCert, err := x509.ParseCertificate(cert2)
	mustNoErr(err)

	rpool := x509.NewCertPool()
	rpool.AddCert(caCert)

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	pool.AddCert(leafCert)

	vOpts := x509.VerifyOptions{
		DNSName:       "beta.local",
		Intermediates: pool,
		Roots:         rpool,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}
	_, err = leafCert.Verify(vOpts)

	mustNoErr(err)

	log.Print("Works!")
}
