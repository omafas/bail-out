package kmip

type Certificate struct {
	Tag `kmip:"CERTIFICATE"`

	CertificateType  Enum   `kmip:"CERTIFICATE_TYPE,required"`
	CertificateValue []byte `kmip:"CERTIFICATE_VALUE,required"`
}
