package kmip

import (
	"time"
)

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// CreateRequest is a Create Request Payload
type CreateRequest struct {
	ObjectType        Enum              `kmip:"OBJECT_TYPE,required"`
	TemplateAttribute TemplateAttribute `kmip:"TEMPLATE_ATTRIBUTE,required"`
}

// CreateResponse is a Create Response Payload
type CreateResponse struct {
	ObjectType        Enum              `kmip:"OBJECT_TYPE,required"`
	UniqueIdentifier  string            `kmip:"UNIQUE_IDENTIFIER,required"`
	TemplateAttribute TemplateAttribute `kmip:"TEMPLATE_ATTRIBUTE"`
}

// CreateKeyPairRequest is a Create Key Pair Request Payload
type CreateKeyPairRequest struct {
	CommonTemplateAttribute     TemplateAttribute `kmip:"COMMON_TEMPLATE_ATTRIBUTE"`
	PrivateKeyTemplateAttribute TemplateAttribute `kmip:"PRIVATE_KEY_TEMPLATE_ATTRIBUTE"`
	PublicKeyTemplateAttribute  TemplateAttribute `kmip:"PUBLIC_KEY_TEMPLATE_ATTRIBUTE"`
}

// CreateKeyPairResponse is a Create Key Pair Response Payload
type CreateKeyPairResponse struct {
	PrivateKeyUniqueIdentifier  string            `kmip:"PRIVATE_KEY_UNIQUE_IDENTIFIER,required"`
	PublicKeyUniqueIdentifier   string            `kmip:"PUBLIC_KEY_UNIQUE_IDENTIFIER,required"`
	PrivateKeyTemplateAttribute TemplateAttribute `kmip:"PRIVATE_KEY_TEMPLATE_ATTRIBUTE"`
	PublicKeyTemplateAttribute  TemplateAttribute `kmip:"PUBLIC_KEY_TEMPLATE_ATTRIBUTE"`
}

// GetRequest is a Get Request Payload
type GetRequest struct {
	UniqueIdentifier   string                   `kmip:"UNIQUE_IDENTIFIER"`
	KeyFormatType      Enum                     `kmip:"KEY_FORMAT_TYPE"`
	KeyWrapType        Enum                     `kmip:"KEY_WRAP_TYPE"`
	KeyCompressionType Enum                     `kmip:"KEY_COMPRESSION_TYPE"`
	KeyWrappingSpec    KeyWrappingSpecification `kmip:"KEY_WRAPPING_SPECIFICATION"`
}

// GetResponse is a Get Response Payload
type GetResponse struct {
	ObjectType       Enum   `kmip:"OBJECT_TYPE,required"`
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
	// Response might contain one of SymmetricKey, Certificate, ...
	SymmetricKey SymmetricKey `kmip:"SYMMETRIC_KEY"`
	PrivateKey   PrivateKey   `kmip:"PRIVATE_KEY"`
	PublicKey    PublicKey    `kmip:"PUBLIC_KEY"`
	Certificate  Certificate  `kmip:"CERTIFICATE"`
	OpaqueObject OpaqueObject `kmip:"OPAQUE_OBJECT"`
}

// GetAttributesRequest is a Get Attributes Request Payload
type GetAttributesRequest struct {
	UniqueIdentifier string   `kmip:"UNIQUE_IDENTIFIER"`
	AttributeNames   []string `kmip:"ATTRIBUTE_NAME"`
}

// GetAttributesResponse is a Get Attributes Response Payload
type GetAttributesResponse struct {
	UniqueIdentifier string     `kmip:"UNIQUE_IDENTIFIER,required"`
	Attributes       Attributes `kmip:"ATTRIBUTE"`
}

// GetAttributeListRequest is a Get Attribute List Request Payload
type GetAttributeListRequest struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER"`
}

// GetAttributeListResponse is a Get Attribute List Response Payload
type GetAttributeListResponse struct {
	UniqueIdentifier string   `kmip:"UNIQUE_IDENTIFIER,required"`
	AttributeNames   []string `kmip:"ATTRIBUTE_NAME"`
}

// ActivateRequest is a Activate Request Payload
type ActivateRequest struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER"`
}

// ActivateResponse is a Activate Response Payload
type ActivateResponse struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
}

// RevokeRequest is a Revoke Request Payload
type RevokeRequest struct {
	UniqueIdentifier string           `kmip:"UNIQUE_IDENTIFIER"`
	RevocationReason RevocationReason `kmip:"REVOCATION_REASON,required"`
	CompromiseDate   time.Time        `kmip:"COMPROMISE_DATE"`
}

// RevokeResponse is a Revoke Response Payload
type RevokeResponse struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
}

// DestroyRequest is a Destroy Request Payload
type DestroyRequest struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER"`
}

// DestroyResponse is a Destroy Response Payload
type DestroyResponse struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
}

// DiscoverVersionsRequest is a Discover Versions Request Payload
type DiscoverVersionsRequest struct {
	ProtocolVersions []ProtocolVersion `kmip:"PROTOCOL_VERSION"`
}

// DiscoverVersionsResponse is a Discover Versions Response Payload
type DiscoverVersionsResponse struct {
	ProtocolVersions []ProtocolVersion `kmip:"PROTOCOL_VERSION"`
}

// QueryRequest is a Query Request Payload
type QueryRequest struct {
	QueryFunctions []Enum `kmip:"QUERY_FUNCTION,required"`
}

// QueryResponse is a Query Response Payload
type QueryResponse struct {
	Operations           []Enum `kmip:"OPERATION"`
	ObjectTypes          []Enum `kmip:"OBJECT_TYPE"`
	VendorIdentification string `kmip:"VENDOR_IDENTIFICATION"`
}

// EncryptRequest is an Encrypt Request Payload
type EncryptRequest struct {
	UniqueIdentifier string       `kmip:"UNIQUE_IDENTIFIER"`
	CryptoParams     CryptoParams `kmip:"CRYPTOGRAPHIC_PARAMETERS"`
	Data             []byte       `kmip:"DATA"`
	IVCounterNonce   []byte       `kmip:"IV_COUNTER_NONCE"`
	CorrelationValue []byte       `kmip:"CORRELATION_VALUE"`
	InitIndicator    bool         `kmip:"INIT_INDICATOR"`
	FinalIndicator   bool         `kmip:"FINAL_INDICATOR"`
	AdditionalData   []byte       `kmip:"AUTHENTICATED_ENCRYPTION_ADDITIONAL_DATA"`
}

// EncryptResponse is a Encrypt Response Payload
type EncryptResponse struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
	Data             []byte `kmip:"DATA"`
	IVCounterNonce   []byte `kmip:"IV_COUNTER_NONCE"`
	CorrelationValue []byte `kmip:"CORRELATION_VALUE"`
	AuthTag          []byte `kmip:"AUTHENTICATED_ENCRYPTION_TAG"`
}

// DecryptRequest is a Decrypt Request Payload
type DecryptRequest struct {
	UniqueIdentifier string       `kmip:"UNIQUE_IDENTIFIER"`
	CryptoParams     CryptoParams `kmip:"CRYPTOGRAPHIC_PARAMETERS"`
	Data             []byte       `kmip:"DATA"`
	IVCounterNonce   []byte       `kmip:"IV_COUNTER_NONCE"`
	CorrelationValue []byte       `kmip:"CORRELATION_VALUE"`
	InitIndicator    bool         `kmip:"INIT_INDICATOR"`
	FinalIndicator   bool         `kmip:"FINAL_INDICATOR"`
	AdditionalData   []byte       `kmip:"AUTHENTICATED_ENCRYPTION_ADDITIONAL_DATA"`
	AuthTag          []byte       `kmip:"AUTHENTICATED_ENCRYPTION_TAG"`
}

// DecryptResponse is a Decrypt Response Payload
type DecryptResponse struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
	Data             []byte `kmip:"DATA"`
	CorrelationValue []byte `kmip:"CORRELATION_VALUE"`
}

// SignRequest is a Sign Request Payload
type SignRequest struct {
	UniqueIdentifier string       `kmip:"UNIQUE_IDENTIFIER"`
	CryptoParams     CryptoParams `kmip:"CRYPTOGRAPHIC_PARAMETERS"`
	Data             []byte       `kmip:"DATA"`
	CorrelationValue []byte       `kmip:"CORRELATION_VALUE"`
	InitIndicator    bool         `kmip:"INIT_INDICATOR"`
	FinalIndicator   bool         `kmip:"FINAL_INDICATOR"`
}

// SignResponse is a Sign Response Payload
type SignResponse struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
	SignatureData    []byte `kmip:"SIGNATURE_DATA"`
	CorrelationValue []byte `kmip:"CORRELATION_VALUE"`
}

// RegisterRequest is a Register Request Payload
type RegisterRequest struct {
	ObjectType        Enum              `kmip:"OBJECT_TYPE,required"`
	TemplateAttribute TemplateAttribute `kmip:"TEMPLATE_ATTRIBUTE,required"`
	// Request might contain one of SymmetricKey, Certificate, ...
	SymmetricKey SymmetricKey `kmip:"SYMMETRIC_KEY"`
	PrivateKey   PrivateKey   `kmip:"PRIVATE_KEY"`
	PublicKey    PublicKey    `kmip:"PUBLIC_KEY"`
	Certificate  Certificate  `kmip:"CERTIFICATE"`
	OpaqueObject OpaqueObject `kmip:"OPAQUE_OBJECT"`
}

// RegisterResponse is a Register Response Payload
type RegisterResponse struct {
	UniqueIdentifier  string            `kmip:"UNIQUE_IDENTIFIER,required"`
	TemplateAttribute TemplateAttribute `kmip:"TEMPLATE_ATTRIBUTE"`
}

// LocateRequest is a Locate Request Payload
type LocateRequest struct {
	MaximumItems      int32      `kmip:"MAXIMUM_ITEMS"`
	OffsetItems       int32      `kmip:"OFFSET_ITEMS"`
	StorageStatusMask int32      `kmip:"STORAGE_STATUS_MASK"`
	ObjectGroupMember Enum       `kmip:"OBJECT_GROUP_MEMBER"`
	Attributes        Attributes `kmip:"ATTRIBUTE"`
}

// LocateResponse is a Locate Response Payload
type LocateResponse struct {
	LocatedItems      int32    `kmip:"LOCATED_ITEMS"`
	UniqueIdentifiers []string `kmip:"UNIQUE_IDENTIFIER"`
}
type ReKeyRequest struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
}

type ReKeyResponse struct {
	UniqueIdentifier string `kmip:"UNIQUE_IDENTIFIER,required"`
}
