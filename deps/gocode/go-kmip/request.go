package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"time"

	"github.com/pkg/errors"
)

// Request is a Request Message Structure
type Request struct {
	Tag `kmip:"REQUEST_MESSAGE"`

	Header     RequestHeader      `kmip:"REQUEST_HEADER,required"`
	BatchItems []RequestBatchItem `kmip:"REQUEST_BATCH_ITEM,required"`
}

// RequestHeader is a Request Header Structure
type RequestHeader struct {
	Tag `kmip:"REQUEST_HEADER"`

	Version                     ProtocolVersion `kmip:"PROTOCOL_VERSION,required"`
	MaxResponseSize             int32           `kmip:"MAXIMUM_RESPONSE_SIZE"`
	ClientCorrelationValue      string          `kmip:"CLIENT_CORRELATION_VALUE"`
	ServerCorrelationValue      string          `kmip:"SERVER_CORRELATION_VALUE"`
	AsynchronousIndicator       bool            `kmip:"ASYNCHRONOUS_INDICATOR"`
	AttestationCapableIndicator bool            `kmip:"ATTESTATION_CAPABLE_INDICATOR"`
	AttestationType             []Enum          `kmip:"ATTESTATION_TYPE"`
	// Request authentication not implemented for now
	Authentication               Authentication `kmip:"AUTHENTICATION,skip"`
	BatchErrorContinuationOption Enum           `kmip:"BATCH_ERROR_CONTINUATION_OPTION"`
	BatchOrderOption             bool           `kmip:"BATCH_ORDER_OPTION"`
	TimeStamp                    time.Time      `kmip:"TIME_STAMP"`
	BatchCount                   int32          `kmip:"BATCH_COUNT,required"`
}

// RequestBatchItem is a Request Batch Item Structure
type RequestBatchItem struct {
	Tag `kmip:"REQUEST_BATCH_ITEM"`

	Operation        Enum             `kmip:"OPERATION,required"`
	UniqueID         []byte           `kmip:"UNIQUE_BATCH_ITEM_ID"`
	RequestPayload   interface{}      `kmip:"REQUEST_PAYLOAD,required"`
	MessageExtension MessageExtension `kmip:"MESSAGE_EXTENSION"`
}

// BuildFieldValue builds value for RequestPayload based on Operation
func (bi *RequestBatchItem) BuildFieldValue(name string) (v interface{}, err error) {
	switch bi.Operation {
	case OPERATION_CREATE:
		v = &CreateRequest{}
	case OPERATION_CREATE_KEY_PAIR:
		v = &CreateKeyPairRequest{}
	case OPERATION_GET:
		v = &GetRequest{}
	case OPERATION_GET_ATTRIBUTES:
		v = &GetAttributesRequest{}
	case OPERATION_GET_ATTRIBUTE_LIST:
		v = &GetAttributeListRequest{}
	case OPERATION_DESTROY:
		v = &DestroyRequest{}
	case OPERATION_DISCOVER_VERSIONS:
		v = &DiscoverVersionsRequest{}
	case OPERATION_REGISTER:
		v = &RegisterRequest{}
	case OPERATION_ACTIVATE:
		v = &ActivateRequest{}
	case OPERATION_LOCATE:
		v = &LocateRequest{}
	case OPERATION_REVOKE:
		v = &RevokeRequest{}
	case OPERATION_REKEY:
		v = &ReKeyRequest{}
	case OPERATION_DECRYPT:
		v = &DecryptRequest{}
	case OPERATION_ENCRYPT:
		v = &EncryptRequest{}
	case OPERATION_QUERY:
		v = &QueryRequest{}
	default:
		err = errors.Errorf("unsupported operation: %v", bi.Operation)
	}

	return
}

// ProtocolVersion is a Protocol Version structure
type ProtocolVersion struct {
	Tag `kmip:"PROTOCOL_VERSION"`

	Major int32 `kmip:"PROTOCOL_VERSION_MAJOR"`
	Minor int32 `kmip:"PROTOCOL_VERSION_MINOR"`
}

// MessageExtension is a Message Extension structure in a Batch Item
type MessageExtension struct {
	Tag `kmip:"MESSAGE_EXTENSION"`

	VendorIdentification string      `kmip:"VENDOR_IDENTIFICATION,required"`
	CriticalityIndicator bool        `kmip:"CRITICALITY_INDICATOR,required"`
	VendorExtension      interface{} `kmip:"-,skip"`
}

// RevocationReason is a Revocation Reason structure
type RevocationReason struct {
	Tag `kmip:"REVOCATION_REASON"`

	RevocationReasonCode Enum   `kmip:"REVOCATION_REASON_CODE"`
	RevocationMessage    string `kmip:"REVOCATION_REASON"`
}
