package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// KeyWrappingSpecification is a Key Wrapping Specification Object
type KeyWrappingSpecification struct {
	Tag `kmip:"KEY_WRAPPING_SPECIFICATION"`

	WrappingMethod             Enum                       `kmip:"WRAPPING_METHOD,required"`
	EncryptionKeyInformation   EncryptionKeyInformation   `kmip:"ENCRYPTION_KEY_INFORMATION"`
	MACSignatureKeyInformation MACSignatureKeyInformation `kmip:"MAC_SIGNATURE_KEY_INFORMATION"`
	AttributeName              []string                   `kmip:"ATTRIBUTE_NAME"`
	EncodingOption             Enum                       `kmip:"ENCODING_OPTION"`
}

// KeyWrappingData is a Key Wrapping Data Object Structure
type KeyWrappingData struct {
	Tag `kmip:"KEY_WRAPPING_DATA"`

	WrappingMethod             Enum                       `kmip:"WRAPPING_METHOD,required"`
	EncryptionKeyInformation   EncryptionKeyInformation   `kmip:"ENCRYPTION_KEY_INFORMATION"`
	MACSignatureKeyInformation MACSignatureKeyInformation `kmip:"MAC_SIGNATURE_KEY_INFORMATION"`
	MACSignature               []byte                     `kmip:"MAC_SIGNATURE"`
	IVCounterNonce             []byte                     `kmip:"IV_COUNTER_NONCE"`
	EncodingOption             Enum                       `kmip:"ENCODING_OPTION"`
}

// EncryptionKeyInformation is a Key Wrapping Specification Object
type EncryptionKeyInformation struct {
	Tag `kmip:"ENCRYPTION_KEY_INFORMATION"`

	UniqueIdentifier string       `kmip:"UNIQUE_IDENTIFIER,required"`
	CryptoParams     CryptoParams `kmip:"CRYPTOGRAPHIC_PARAMETERS"`
}

// MACSignatureKeyInformation is a MAC/Signature Key Information Object Structure
type MACSignatureKeyInformation struct {
	Tag `kmip:"MAC_SIGNATURE_KEY_INFORMATION"`

	UniqueIdentifier string       `kmip:"UNIQUE_IDENTIFIER,required"`
	CryptoParams     CryptoParams `kmip:"CRYPTOGRAPHIC_PARAMETERS"`
}

// CryptoParams is a Cryptographic Parameters Attribute Structure
type CryptoParams struct {
	Tag `kmip:"CRYPTOGRAPHIC_PARAMETERS"`

	BlockCipherMode               Enum   `kmip:"BLOCK_CIPHER_MODE"`
	PaddingMethod                 Enum   `kmip:"PADDING_METHOD"`
	HashingAlgorithm              Enum   `kmip:"HASHING_ALGORITHM"`
	KeyRoleType                   Enum   `kmip:"KEY_ROLE_TYPE"`
	DigitalSignatureAlgorithm     Enum   `kmip:"DIGITAL_SIGNATURE_ALGORITHM"`
	CryptographicAlgorithm        Enum   `kmip:"CRYPTOGRAPHIC_ALGORITHM"`
	RandomIV                      bool   `kmip:"RANDOM_IV"`
	IVLength                      int32  `kmip:"IV_LENGTH"`
	TagLength                     int32  `kmip:"TAG_LENGTH"`
	FixedFieldLength              int32  `kmip:"FIXED_FIELD_LENGTH"`
	InvocationFieldLength         int32  `kmip:"INVOCATION_FIELD_LENGTH"`
	CounterLength                 int32  `kmip:"COUNTER_LENGTH"`
	InitialCounterValue           int32  `kmip:"INITIAL_COUNTER_VALUE"`
	SaltLength                    int32  `kmip:"SALT_LENGTH"`
	MaskGenerator                 Enum   `kmip:"MASK_GENERATOR"`
	MaskGeneratorHashingAlgorithm Enum   `kmip:"MASK_GENERATOR_HASHING_ALGORITHM"`
	PSource                       []byte `kmip:"P_SOURCE"`
	TrailerFIeld                  int32  `kmip:"TRAILER_FIELD"`
}

// SymmetricKey is a Managed Cryptographic Object that is a symmetric key
type SymmetricKey struct {
	Tag `kmip:"SYMMETRIC_KEY"`

	KeyBlock KeyBlock `kmip:"KEY_BLOCK,required"`
}

// PublicKey is a Managed Cryptographic Object that is a public key
type PublicKey struct {
	Tag `kmip:"PUBLIC_KEY"`

	KeyBlock KeyBlock `kmip:"KEY_BLOCK,required"`
}

// PrivateKey is a Managed Cryptographic Object that is a private key
type PrivateKey struct {
	Tag `kmip:"PRIVATE_KEY"`

	KeyBlock KeyBlock `kmip:"KEY_BLOCK,required"`
}

// KeyBlock is a Key Block Object Structure
type KeyBlock struct {
	Tag `kmip:"KEY_BLOCK"`

	FormatType             Enum            `kmip:"KEY_FORMAT_TYPE,required"`
	CompressionType        Enum            `kmip:"KEY_COMPRESSION_TYPE"`
	Value                  KeyValue        `kmip:"KEY_VALUE,required"`
	CryptographicAlgorithm Enum            `kmip:"CRYPTOGRAPHIC_ALGORITHM"`
	CryptographicLength    int32           `kmip:"CRYPTOGRAPHIC_LENGTH"`
	WrappingData           KeyWrappingData `kmip:"KEY_WRAPPING_SPECIFICATION"`
}

// KeyValue is a Key Value Object Structure
type KeyValue struct {
	Tag `kmip:"KEY_VALUE"`

	// TODO: might be structure if wrapping is used
	KeyMaterial []byte     `kmip:"KEY_MATERIAL"`
	Attributes  Attributes `kmip:"ATTRIBUTE"`
}

type OpaqueObject struct {
	Tag `kmip:"OPAQUE_OBJECT"`

	OpaqueDataType  Enum   `kmip:"OPAQUE_DATA_TYPE"`
	OpaqueDataValue []byte `kmip:"OPAQUE_DATA_VALUE"`
}
