package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import (
	"time"

	"github.com/pkg/errors"
)

// Response is a Response Message Structure
type Response struct {
	Tag `kmip:"RESPONSE_MESSAGE"`

	Header     ResponseHeader      `kmip:"RESPONSE_HEADER,required"`
	BatchItems []ResponseBatchItem `kmip:"RESPONSE_BATCH_ITEM,required"`
}

// ResponseHeader is a Response Header Structure
type ResponseHeader struct {
	Tag `kmip:"RESPONSE_HEADER"`

	Version                ProtocolVersion `kmip:"PROTOCOL_VERSION,required"`
	TimeStamp              time.Time       `kmip:"TIME_STAMP,required"`
	Nonce                  Nonce           `kmip:"NONCE"`
	AttestationType        []Enum          `kmip:"ATTESTATION_TYPE"`
	ClientCorrelationValue string          `kmip:"CLIENT_CORRELATION_VALUE"`
	ServerCorrelationValue string          `kmip:"SERVER_CORRELATION_VALUE"`
	BatchCount             int32           `kmip:"BATCH_COUNT,required"`
}

// ResponseBatchItem is a Response Batch Item Structure
type ResponseBatchItem struct {
	Operation                   Enum             `kmip:"OPERATION,required"`
	UniqueID                    []byte           `kmip:"UNIQUE_BATCH_ITEM_ID"`
	ResultStatus                Enum             `kmip:"RESULT_STATUS,required"`
	ResultReason                Enum             `kmip:"RESULT_REASON"`
	ResultMessage               string           `kmip:"RESULT_MESSAGE"`
	AsyncronousCorrelationValue []byte           `kmip:"ASYNCHRONOUS_CORRELATION_VALUE"`
	ResponsePayload             interface{}      `kmip:"RESPONSE_PAYLOAD"`
	MessageExtension            MessageExtension `kmip:"MESSAGE_EXTENSION"`
}

// BuildFieldValue builds value for ResponsePayload based on Operation
func (bi *ResponseBatchItem) BuildFieldValue(name string) (v interface{}, err error) {
	switch bi.Operation {
	case OPERATION_CREATE:
		v = &CreateResponse{}
	case OPERATION_CREATE_KEY_PAIR:
		v = &CreateKeyPairResponse{}
	case OPERATION_GET:
		v = &GetResponse{}
	case OPERATION_GET_ATTRIBUTES:
		v = &GetAttributesResponse{}
	case OPERATION_GET_ATTRIBUTE_LIST:
		v = &GetAttributeListResponse{}
	case OPERATION_ACTIVATE:
		v = &ActivateResponse{}
	case OPERATION_REVOKE:
		v = &RevokeResponse{}
	case OPERATION_DESTROY:
		v = &DestroyResponse{}
	case OPERATION_DISCOVER_VERSIONS:
		v = &DiscoverVersionsResponse{}
	case OPERATION_ENCRYPT:
		v = &EncryptResponse{}
	case OPERATION_DECRYPT:
		v = &DecryptResponse{}
	case OPERATION_SIGN:
		v = &SignResponse{}
	case OPERATION_REGISTER:
		v = &RegisterResponse{}
	case OPERATION_LOCATE:
		v = &LocateResponse{}
	case OPERATION_REKEY:
		v = &ReKeyResponse{}
	case OPERATION_QUERY:
		v = &QueryResponse{}
	default:
		err = errors.Errorf("unsupported operation: %v", bi.Operation)
	}

	return
}

// Nonce object is a structure used by the server to send a random value to the client
type Nonce struct {
	Tag `kmip:"NONCE"`

	NonceID    []byte `kmip:"NONCE_ID,required"`
	NonceValue []byte `kmip:"NONCE_VALUE,required"`
}
