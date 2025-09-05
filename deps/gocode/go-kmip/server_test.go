package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type ServerSuite struct {
	suite.Suite

	certs  CertificateSet
	server Server
	client Client

	listenCh chan error
}

func (s *ServerSuite) SetupSuite() {
	s.Require().NoError(s.certs.Generate([]string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)}))

	s.server.Addr = "localhost:"
	s.server.TLSConfig = &tls.Config{}
	DefaultServerTLSConfig(s.server.TLSConfig)
	s.server.TLSConfig.ClientCAs = s.certs.CAPool
	s.server.TLSConfig.Certificates = []tls.Certificate{
		{
			Certificate: [][]byte{s.certs.ServerCert.Raw},
			PrivateKey:  s.certs.ServerKey,
		},
	}

	s.server.ReadTimeout = time.Second
	s.server.WriteTimeout = time.Second

	s.server.Log = log.New(os.Stderr, "[kmip] ", log.LstdFlags)

	s.listenCh = make(chan error, 1)
	initializedCh := make(chan struct{})

	go func() {
		s.listenCh <- s.server.ListenAndServe(initializedCh)
	}()

	<-initializedCh
}

func (s *ServerSuite) SetupTest() {
	s.server.mu.Lock()
	addr := s.server.l.Addr().String()
	s.server.mu.Unlock()

	_, port, err := net.SplitHostPort(addr)
	s.Require().NoError(err)

	s.client.Endpoint = "localhost:" + port
	s.client.TLSConfig = &tls.Config{}
	DefaultClientTLSConfig(s.client.TLSConfig)
	s.client.TLSConfig.RootCAs = s.certs.CAPool
	s.client.TLSConfig.Certificates = []tls.Certificate{
		{
			Certificate: [][]byte{s.certs.ClientCert.Raw},
			PrivateKey:  s.certs.ClientKey,
		},
	}

	s.client.ReadTimeout = time.Second
	s.client.WriteTimeout = time.Second
}

func (s *ServerSuite) TearDownTest() {
	s.Require().NoError(s.client.Close())

	// reset server state
	s.server.mu.Lock()
	s.server.SessionAuthHandler = nil
	s.server.initHandlers()
	s.server.mu.Unlock()
}

func (s *ServerSuite) TearDownSuite() {
	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second)
	defer ctxCancel()

	s.Require().NoError(s.server.Shutdown(ctx))
	s.Require().NoError(<-s.listenCh)
}

func (s *ServerSuite) TestDiscoverVersions() {
	s.Require().NoError(s.client.Connect())

	versions, err := s.client.DiscoverVersions(DefaultSupportedVersions)
	s.Require().NoError(err)
	s.Require().Equal(DefaultSupportedVersions, versions)

	versions, err = s.client.DiscoverVersions(nil)
	s.Require().NoError(err)
	s.Require().Equal(DefaultSupportedVersions, versions)

	versions, err = s.client.DiscoverVersions([]ProtocolVersion{{Major: 1, Minor: 2}})
	s.Require().NoError(err)
	s.Require().Equal([]ProtocolVersion{{Major: 1, Minor: 2}}, versions)

	versions, err = s.client.DiscoverVersions([]ProtocolVersion{{Major: 2, Minor: 0}})
	s.Require().NoError(err)
	s.Require().Equal([]ProtocolVersion(nil), versions)
}

func (s *ServerSuite) TestSessionAuthHandlerOkay() {
	s.server.SessionAuthHandler = func(conn net.Conn) (interface{}, error) {
		commonName := conn.(*tls.Conn).ConnectionState().PeerCertificates[0].Subject.CommonName

		if commonName != "client_auth_test_cert" {
			return nil, errors.New("wrong common name")
		}

		return commonName, nil
	}

	s.server.Handle(OPERATION_DISCOVER_VERSIONS, func(req *RequestContext, item *RequestBatchItem) (interface{}, error) {
		if req.SessionAuth.(string) != "client_auth_test_cert" {
			return nil, errors.New("wrong session auth")
		}

		return DiscoverVersionsResponse{
			ProtocolVersions: nil,
		}, nil
	})

	s.Require().NoError(s.client.Connect())

	versions, err := s.client.DiscoverVersions(nil)
	s.Require().NoError(err)
	s.Require().Equal([]ProtocolVersion(nil), versions)
}

func (s *ServerSuite) TestSessionAuthHandlerFail() {
	s.server.SessionAuthHandler = func(conn net.Conn) (interface{}, error) {
		commonName := conn.(*tls.Conn).ConnectionState().PeerCertificates[0].Subject.CommonName

		if commonName != "xxx" {
			return nil, errors.New("wrong common name")
		}

		return commonName, nil
	}

	s.Require().NoError(s.client.Connect())

	_, err := s.client.DiscoverVersions(nil)
	s.Require().Regexp("broken pipe$", errors.Cause(err).Error())

	s.client.Close()
}

func (s *ServerSuite) TestConnectTLSNoCert() {
	var savedCerts []tls.Certificate
	savedCerts, s.client.TLSConfig.Certificates = s.client.TLSConfig.Certificates, nil
	defer func() {
		s.client.TLSConfig.Certificates = savedCerts
	}()

	s.Require().EqualError(errors.Cause(s.client.Connect()), "remote error: tls: bad certificate")
}

func (s *ServerSuite) TestConnectTLSNoCA() {
	var savedPool *x509.CertPool
	savedPool, s.client.TLSConfig.RootCAs = s.client.TLSConfig.RootCAs, nil
	defer func() {
		s.client.TLSConfig.RootCAs = savedPool
	}()

	s.Require().EqualError(errors.Cause(s.client.Connect()), "x509: certificate signed by unknown authority")
}

func (s *ServerSuite) TestOperationGenericFail() {
	s.server.Handle(OPERATION_DISCOVER_VERSIONS, func(req *RequestContext, item *RequestBatchItem) (interface{}, error) {
		return nil, errors.New("oops!")
	})

	s.Require().NoError(s.client.Connect())

	_, err := s.client.DiscoverVersions(nil)
	s.Require().EqualError(errors.Cause(err), "oops!")
	s.Require().Equal(errors.Cause(err).(Error).ResultReason(), RESULT_REASON_GENERAL_FAILURE)
}

func (s *ServerSuite) TestOperationPanic() {
	s.server.Handle(OPERATION_DISCOVER_VERSIONS, func(req *RequestContext, item *RequestBatchItem) (interface{}, error) {
		panic("oops!")
	})

	s.Require().NoError(s.client.Connect())

	_, err := s.client.DiscoverVersions(nil)
	s.Require().EqualError(errors.Cause(err), "panic: oops!")
	s.Require().Equal(errors.Cause(err).(Error).ResultReason(), RESULT_REASON_GENERAL_FAILURE)
}

func (s *ServerSuite) TestOperationFailWithReason() {
	s.server.Handle(OPERATION_DISCOVER_VERSIONS, func(req *RequestContext, item *RequestBatchItem) (interface{}, error) {
		return nil, wrapError(errors.New("oops!"), RESULT_REASON_CRYPTOGRAPHIC_FAILURE)
	})

	s.Require().NoError(s.client.Connect())

	_, err := s.client.DiscoverVersions(nil)
	s.Require().EqualError(errors.Cause(err), "oops!")
	s.Require().Equal(errors.Cause(err).(Error).ResultReason(), RESULT_REASON_CRYPTOGRAPHIC_FAILURE)
}

func (s *ServerSuite) TestOperationNotSupported() {
	s.Require().NoError(s.client.Connect())

	_, err := s.client.Send(OPERATION_GET, GetRequest{})
	s.Require().EqualError(errors.Cause(err), "operation not supported")
	s.Require().Equal(errors.Cause(err).(Error).ResultReason(), RESULT_REASON_OPERATION_NOT_SUPPORTED)
}

func TestServerSuite(t *testing.T) {
	suite.Run(t, new(ServerSuite))
}
