package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type DecoderSuite struct {
	suite.Suite
}

func (s *DecoderSuite) parseSpecValue(val string) []byte {
	val = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(val, "_", ""), "|", ""), " ", "")

	res, err := hex.DecodeString(val)
	s.Require().NoError(err)

	return res
}

func (s *DecoderSuite) TestReadInteger() {
	v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 02 | 00 00 00 04 | 00 00 00 08 00 00 00 00"))).readInteger(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().EqualValues(8, v)

	// padding missing
	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 02 | 00 00 00 04 | 00 00 00 08 00 00 00 "))).readInteger(COMPROMISE_DATE)
	s.Assert().EqualError(err, "unexpected EOF")

	// no value
	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 02 | 00 00 00 04 | 00"))).readInteger(COMPROMISE_DATE)
	s.Assert().EqualError(err, "unexpected EOF")

	// no length
	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 02 | 00 00 "))).readInteger(COMPROMISE_DATE)
	s.Assert().EqualError(err, "unexpected EOF")

	// no type
	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | "))).readInteger(COMPROMISE_DATE)
	s.Assert().EqualError(err, "EOF")

	// no tag
	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42"))).readInteger(COMPROMISE_DATE)
	s.Assert().EqualError(err, "unexpected EOF")

	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 21 | 02 | 00 00 00 04 | 00 00 00 08 00 00 00 "))).readInteger(CRYPTOGRAPHIC_ALGORITHM)
	s.Assert().EqualError(err, "expecting tag 420028, but 420021 was encountered")

	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 01 | 00 00 00 04 | 00 00 00 08 00 00 00 00"))).readInteger(COMPROMISE_DATE)
	s.Assert().EqualError(err, "expecting type 2, but 1 was encountered")

	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 02 | 00 00 00 03 | 00 00 00 08 00 00 00 00"))).readInteger(COMPROMISE_DATE)
	s.Assert().EqualError(err, "expecting length 4, but 3 was encountered")
}

func (s *DecoderSuite) TestReadLongInteger() {
	v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 03 | 00 00 00 08 | 01 B6 9B 4B A5 74 92 00"))).readLongInteger(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().EqualValues(123456789000000000, v)
}

func (s *DecoderSuite) TestReadEnum() {
	v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 05 | 00 00 00 04 | 00 00 00 FF 00 00 00 00"))).readEnum(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().EqualValues(255, v)
}

func (s *DecoderSuite) TestReadBool() {
	v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 06 | 00 00 00 08 | 00 00 00 00 00 00 00 01"))).readBool(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().True(v)

	v, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 06 | 00 00 00 08 | 00 00 00 00 00 00 00 00"))).readBool(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().False(v)

	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 06 | 00 00 00 08 | 00 00 00 00 00 00 00 03"))).readBool(COMPROMISE_DATE)
	s.Assert().EqualError(err, "unexpected boolean value: [0 0 0 0 0 0 0 3]")

	_, err = NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 06 | 00 00 00 08 | 00 00 00 00 01 00 00 00"))).readBool(COMPROMISE_DATE)
	s.Assert().EqualError(err, "unexpected boolean value: [0 0 0 0 1 0 0 0]")
}

func (s *DecoderSuite) TestReadString() {
	n, v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 07 | 00 00 00 0B | 48 65 6C 6C 6F 20 57 6F 72 6C 64 00 00 00 00 00"))).readString(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().EqualValues("Hello World", v)
	s.Assert().EqualValues(24, n)
}

func (s *DecoderSuite) TestReadBytes() {
	n, v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 08 | 00 00 00 03 | 01 02 03 00 00 00 00 00"))).readBytes(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().EqualValues([]byte{1, 2, 3}, v)
	s.Assert().EqualValues(16, n)
}

func (s *DecoderSuite) TestReadTime() {
	v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 09 | 00 00 00 08 | 00 00 00 00 47 DA 67 F8"))).readTime(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().Equal("2008-03-14T11:56:40Z", v.UTC().Format(time.RFC3339))
}

func (s *DecoderSuite) TestReadDuration() {
	v, err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 0A | 00 00 00 04 | 00 0D 2F 00 00 00 00 00"))).readDuration(COMPROMISE_DATE)
	s.Assert().NoError(err)
	s.Assert().Equal(10*24*time.Hour, v)
}

func (s *DecoderSuite) TestDecodeStruct() {
	type tt struct {
		Tag   `kmip:"COMPROMISE_DATE"`
		Other string
		A     Enum   `kmip:"APPLICATION_SPECIFIC_INFORMATION,required"`
		B     int32  `kmip:"ARCHIVE_DATE,required"`
		C     string `kmip:"COMPROMISE_DATE"`
		D     []byte `kmip:"ACTIVATION_DATE"`
	}

	var v tt

	err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE 00 00 00 00 |" +
		" 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00"))).Decode(&v)
	s.Assert().NoError(err)

	s.Assert().EqualValues(254, v.A)
	s.Assert().EqualValues(255, v.B)
}

func (s *DecoderSuite) TestDecodeStructSkip() {
	type tt struct {
		Tag   `kmip:"COMPROMISE_DATE"`
		Other string
		A     Enum  `kmip:"APPLICATION_SPECIFIC_INFORMATION,required"`
		B     int32 `kmip:"ARCHIVE_DATE,skip"`
	}

	var v tt

	err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE 00 00 00 00 |" +
		" 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00"))).Decode(&v)
	s.Assert().NoError(err)

	s.Assert().EqualValues(254, v.A)
	s.Assert().EqualValues(0, v.B)
}

func (s *DecoderSuite) TestDecodeStructSkipAny() {
	type tt struct {
		Tag `kmip:"COMPROMISE_DATE"`
		A   Enum        `kmip:"APPLICATION_SPECIFIC_INFORMATION"`
		B   interface{} `kmip:"-,skip"`
	}

	var v tt

	err := NewDecoder(bytes.NewReader(s.parseSpecValue("42 00 20 | 01 | 00 00 00 20 | 42 00 04 | 05 | 00 00 00 04 | 00 00 00 FE 00 00 00 00 |" +
		" 42 00 05 | 02 | 00 00 00 04 | 00 00 00 FF 00 00 00 00"))).Decode(&v)
	s.Assert().NoError(err)

	s.Assert().EqualValues(254, v.A)
	s.Assert().EqualValues(nil, v.B)
}

func (s *DecoderSuite) TestDecodeMessageCreate() {
	var m Request

	err := NewDecoder(bytes.NewReader(messageCreate)).Decode(&m)
	s.Assert().NoError(err)
	s.Assert().Equal(
		Request{
			Header: RequestHeader{
				Version:    ProtocolVersion{Major: 1, Minor: 1},
				BatchCount: 1,
			},
			BatchItems: []RequestBatchItem{
				{
					Operation: OPERATION_CREATE,
					RequestPayload: CreateRequest{
						ObjectType: OBJECT_TYPE_SYMMETRIC_KEY,
						TemplateAttribute: TemplateAttribute{
							Attributes: []Attribute{
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_ALGORITHM,
									Value: CRYPTO_AES,
								},
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_LENGTH,
									Value: int32(128),
								},
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_USAGE_MASK,
									Value: int32(12),
								},
								{
									Name:  ATTRIBUTE_NAME_INITIAL_DATE,
									Value: time.Unix(12345, 0),
								},
							},
						},
					},
				},
			},
		}, m)
}

func (s *DecoderSuite) TestDecodeMessageCreateKeyPair() {
	var m Request

	decoder := NewDecoder(bytes.NewReader(messageCreateKeyPair))

	err := decoder.Decode(&m)
	s.Assert().NoError(err)

	s.Assert().Equal(
		Request{
			Header: RequestHeader{
				Version:    ProtocolVersion{Major: 1, Minor: 1},
				BatchCount: 1,
			},
			BatchItems: []RequestBatchItem{
				{
					Operation: OPERATION_CREATE_KEY_PAIR,
					RequestPayload: CreateKeyPairRequest{
						CommonTemplateAttribute: TemplateAttribute{
							Attributes: Attributes{
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_ALGORITHM,
									Value: CRYPTO_RSA,
								},
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_LENGTH,
									Value: int32(2048),
								},
							},
						},
						PrivateKeyTemplateAttribute: TemplateAttribute{
							Attributes: Attributes{
								{
									Name: ATTRIBUTE_NAME_NAME,
									Value: Name{
										Value: "test_private",
										Type:  NAME_TYPE_UNINTERPRETED_TEXT_STRING,
									},
								},
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_USAGE_MASK,
									Value: int32(CRYPTO_USAGE_MASK_SIGN),
								},
							},
						},
						PublicKeyTemplateAttribute: TemplateAttribute{
							Attributes: Attributes{
								{
									Name: ATTRIBUTE_NAME_NAME,
									Value: Name{
										Value: "test_public",
										Type:  NAME_TYPE_UNINTERPRETED_TEXT_STRING,
									},
								},
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_USAGE_MASK,
									Value: int32(CRYPTO_USAGE_MASK_VERIFY),
								},
							},
						},
					},
				},
			},
		}, m)
}

func (s *DecoderSuite) TestDecodeMessageCreateOpaque() {
	var m Request

	decoder := NewDecoder(bytes.NewReader(messageCreateOpaque))

	err := decoder.Decode(&m)
	s.Assert().NoError(err)

	s.Assert().Equal(
		Request{
			Header: RequestHeader{
				Version:    ProtocolVersion{Major: 1, Minor: 1},
				BatchCount: 1,
			},
			BatchItems: []RequestBatchItem{
				{
					Operation: OPERATION_CREATE,
					RequestPayload: CreateRequest{
						ObjectType: OBJECT_TYPE_OPAQUE_DATA,
						TemplateAttribute: TemplateAttribute{
							Attributes: []Attribute{
								{
									Name: ATTRIBUTE_NAME_NAME,
									Value: Name{
										Value: "test_opaque",
										Type:  NAME_TYPE_UNINTERPRETED_TEXT_STRING,
									},
								},
							},
						},
					},
				},
			},
		}, m)
}

func (s *DecoderSuite) TestDecodeMessageCreateCertificate() {
	var m Request

	decoder := NewDecoder(bytes.NewReader(messageCreateCertificate))

	err := decoder.Decode(&m)
	s.Assert().NoError(err)

	s.Assert().Equal(
		Request{
			Header: RequestHeader{
				Version:    ProtocolVersion{Major: 1, Minor: 1},
				BatchCount: 1,
			},
			BatchItems: []RequestBatchItem{
				{
					Operation: OPERATION_CREATE,
					RequestPayload: CreateRequest{
						ObjectType: OBJECT_TYPE_CERTIFICATE,
						TemplateAttribute: TemplateAttribute{
							Attributes: []Attribute{
								{
									Name: ATTRIBUTE_NAME_NAME,
									Value: Name{
										Value: "certificate",
										Type:  NAME_TYPE_UNINTERPRETED_TEXT_STRING,
									},
								},
							},
						},
					},
				},
			},
		}, m)
}

func (s *DecoderSuite) TestDecodeMessageCreateWithAuthentication() {
	var m Request
	var buf bytes.Buffer

	c := Request{
		Header: RequestHeader{
			Version:    ProtocolVersion{Major: 1, Minor: 1},
			BatchCount: 1,
			Authentication: Authentication{
				Credential: Credential{
					CredentialType:  CREDENTIAL_TYPE_DEVICE,
					CredentialValue: nil,
				},
			},
		},
		BatchItems: []RequestBatchItem{
			{
				Operation: OPERATION_CREATE,
				RequestPayload: CreateRequest{
					ObjectType: OBJECT_TYPE_SYMMETRIC_KEY,
					TemplateAttribute: TemplateAttribute{
						Attributes: []Attribute{
							{
								Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_ALGORITHM,
								Value: CRYPTO_AES,
							},
							{
								Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_LENGTH,
								Value: int32(128),
							},
							{
								Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_USAGE_MASK,
								Value: int32(12),
							},
							{
								Name:  ATTRIBUTE_NAME_INITIAL_DATE,
								Value: time.Unix(12345, 0),
							},
						},
					},
				},
			},
		},
	}
	err := NewEncoder(&buf).Encode(c)

	err = NewDecoder(bytes.NewReader(buf.Bytes())).Decode(&m)
	s.Assert().NoError(err)
	s.Assert().Equal(
		Request{
			Header: RequestHeader{
				Version:    ProtocolVersion{Major: 1, Minor: 1},
				BatchCount: 1,
			},
			BatchItems: []RequestBatchItem{
				{
					Operation: OPERATION_CREATE,
					RequestPayload: CreateRequest{
						ObjectType: OBJECT_TYPE_SYMMETRIC_KEY,
						TemplateAttribute: TemplateAttribute{
							Attributes: []Attribute{
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_ALGORITHM,
									Value: CRYPTO_AES,
								},
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_LENGTH,
									Value: int32(128),
								},
								{
									Name:  ATTRIBUTE_NAME_CRYPTOGRAPHIC_USAGE_MASK,
									Value: int32(12),
								},
								{
									Name:  ATTRIBUTE_NAME_INITIAL_DATE,
									Value: time.Unix(12345, 0),
								},
							},
						},
					},
				},
			},
		}, m)
}

func (s *DecoderSuite) TestDecodeMessageGet() {
	var m Request

	err := NewDecoder(bytes.NewReader(messageGet)).Decode(&m)
	s.Assert().NoError(err)
	s.Assert().Equal(Request{
		Header: RequestHeader{
			Version:    ProtocolVersion{Major: 1, Minor: 1},
			BatchCount: 1,
		},
		BatchItems: []RequestBatchItem{
			{
				Operation: OPERATION_GET,
				RequestPayload: GetRequest{
					UniqueIdentifier: "49a1ca88-6bea-4fb2-b450-7e58802c3038",
				},
			},
		},
	}, m)
}

func TestDecoderSuite(t *testing.T) {
	suite.Run(t, new(DecoderSuite))
}
