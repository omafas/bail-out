package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import "github.com/pkg/errors"

// Authentication is an Authentication structure
type Authentication struct {
	Tag `kmip:"AUTHENTICATION"`

	Credential Credential `kmip:"CREDENTIAL"`
}

// BuildFieldValue builds value for CredentialValue based on CredentialType
func (a *Authentication) BuildFieldValue(name string) (v interface{}, err error) {
	switch a.Credential.CredentialType {
	case CREDENTIAL_TYPE_USERNAME_AND_PASSWORD:
		v = &CredentialUsernamePassword{}
	default:
		err = errors.Errorf("unsupported credential type: %v", a.Credential.CredentialType)
	}

	return
}

// CredentialUsernamePassword is a CredentialValue structure for username/password authentication
type CredentialUsernamePassword struct {
	Tag `kmip:"CREDENTIAL_VALUE"`

	Username string `kmip:"USERNAME,required"`
	Password string `kmip:"PASSWORD,required"`
}

// Credential is a structure for that contains a credentail value and type
type Credential struct {
	Tag `kmip:"CREDENTIAL"`

	CredentialType  Enum        `kmip:"CREDENTIAL_TYPE,required"`
	CredentialValue interface{} `kmip:"CREDENTIAL_VALUE,required"`
}
