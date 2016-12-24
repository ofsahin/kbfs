// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package kbfsblock

import (
	"encoding"
	"encoding/json"

	"github.com/keybase/kbfs/kbfscrypto"
	"github.com/keybase/kbfs/kbfshash"
)

// ID is the (usually content-based) ID for a data block.
type ID struct {
	h kbfshash.Hash
}

var _ encoding.BinaryMarshaler = ID{}
var _ encoding.BinaryUnmarshaler = (*ID)(nil)

var _ json.Marshaler = ID{}
var _ json.Unmarshaler = (*ID)(nil)

// MaxIDStringLength is the maximum length of the string
// representation of a ID.
const MaxIDStringLength = kbfshash.MaxHashStringLength

// IDFromString creates a ID from the given string. If the
// returned error is nil, the returned ID is valid.
func IDFromString(dataStr string) (ID, error) {
	h, err := kbfshash.HashFromString(dataStr)
	if err != nil {
		return ID{}, err
	}
	return ID{h}, nil
}

// IsValid returns whether the block ID is valid. A zero block ID is
// considered invalid.
func (id ID) IsValid() bool {
	return id.h.IsValid()
}

// Bytes returns the bytes of the block ID.
func (id ID) Bytes() []byte {
	return id.h.Bytes()
}

func (id ID) String() string {
	return id.h.String()
}

// MarshalBinary implements the encoding.BinaryMarshaler interface for
// ID. Returns an error if the ID is invalid and not the zero
// ID.
func (id ID) MarshalBinary() (data []byte, err error) {
	return id.h.MarshalBinary()
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
// for ID. Returns an error if the given byte array is non-empty and
// the ID is invalid.
func (id *ID) UnmarshalBinary(data []byte) error {
	return id.h.UnmarshalBinary(data)
}

// MarshalJSON implements the encoding.json.Marshaler interface for
// ID.
func (id ID) MarshalJSON() ([]byte, error) {
	return id.h.MarshalJSON()
}

// UnmarshalJSON implements the encoding.json.Unmarshaler interface
// for ID.
func (id *ID) UnmarshalJSON(buf []byte) error {
	return id.h.UnmarshalJSON(buf)
}

// MakeTemporaryID generates a temporary block ID using a CSPRNG. This
// is used for indirect blocks before they're committed to the server.
func MakeTemporaryID() (ID, error) {
	var dh kbfshash.RawDefaultHash
	err := kbfscrypto.RandRead(dh[:])
	if err != nil {
		return ID{}, err
	}
	h, err := kbfshash.HashFromRaw(kbfshash.DefaultHashType, dh[:])
	if err != nil {
		return ID{}, err
	}
	return ID{h}, nil
}

// MakePermanentID computes the permanent ID of a block given its
// encoded and encrypted contents.
func MakePermanentID(encodedEncryptedData []byte) (ID, error) {
	h, err := kbfshash.DefaultHash(encodedEncryptedData)
	if err != nil {
		return ID{}, err
	}
	return ID{h}, nil
}

// VerifyID verifies that the given block ID is the permanent block ID
// for the given encoded and encrypted data.
func VerifyID(encodedEncryptedData []byte, id ID) error {
	return id.h.Verify(encodedEncryptedData)
}

// FakeID returns an ID derived from the given byte, suitable for
// testing.
func FakeID(b byte) ID {
	dh := kbfshash.RawDefaultHash{b}
	h, err := kbfshash.HashFromRaw(kbfshash.DefaultHashType, dh[:])
	if err != nil {
		panic(err)
	}
	return ID{h}
}

// FakeIDAdd returns an ID derived from first byte of given ID plus the
// given byte, suitable for testing.
func FakeIDAdd(id ID, b byte) ID {
	return FakeID(id.h.Bytes()[1] + b)
}

// FakeIDMul returns an ID derived from first byte of given ID times the
// given byte, suitable for testing.
func FakeIDMul(id ID, b byte) ID {
	return FakeID(id.h.Bytes()[1] * b)
}