package kmip

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// Error enhances error with "Result Reason" field
//
// Any Error instance is returned back to the caller with message and
// result reason set, any other Go error is returned as "General Failure"
type Error interface {
	error
	ResultReason() Enum
}

type protocolError struct {
	error
	reason Enum
}

func (e protocolError) ResultReason() Enum {
	return e.reason
}

func wrapError(err error, reason Enum) protocolError {
	return protocolError{err, reason}
}
