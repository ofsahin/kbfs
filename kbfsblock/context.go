// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package kbfsblock

import (
	"encoding/hex"
	"fmt"

	"github.com/keybase/client/go/protocol/keybase1"
	"github.com/keybase/kbfs/kbfscrypto"
)

// RefNonce is a 64-bit unique sequence of bytes for identifying this
// reference of a block ID from other references to the same
// (duplicated) block.
type RefNonce [8]byte

// ZeroRefNonce is a special BlockRefNonce used for the initial
// reference to a block.
var ZeroRefNonce = RefNonce([8]byte{0, 0, 0, 0, 0, 0, 0, 0})

func (nonce RefNonce) String() string {
	return hex.EncodeToString(nonce[:])
}

// MakeRefNonce generates a block reference nonce using a CSPRNG. This
// is used for distinguishing different references to the same
// kbfsblock.ID.
func MakeRefNonce() (RefNonce, error) {
	var nonce RefNonce
	err := kbfscrypto.RandRead(nonce[:])
	if err != nil {
		return ZeroRefNonce, err
	}
	return nonce, nil
}

// Context contains all the information used by the server to identify
// blocks (other than the ID).
//
// NOTE: Don't add or modify anything in this struct without
// considering how old clients will handle them.
type Context struct {
	// Creator is the UID that was first charged for the initial
	// reference to this block.
	Creator keybase1.UID `codec:"c"`
	// Writer is the UID that should be charged for this reference to
	// the block.  If empty, it defaults to Creator.
	Writer keybase1.UID `codec:"w,omitempty"`
	// When RefNonce is all 0s, this is the initial reference to a
	// particular block.  Using a constant refnonce for the initial
	// reference allows the server to identify and optimize for the
	// common case where there is only one reference for a block.  Two
	// initial references cannot happen simultaneously, because the
	// encrypted block contents (and thus the block ID) will be
	// randomized by the server-side block crypt key half.  All
	// subsequent references to the same block must have a random
	// RefNonce (it can't be a monotonically increasing number because
	// that would require coordination among clients).
	RefNonce RefNonce `codec:"r,omitempty"`
}

// MakeFirstContext makes the initial context for a block with the
// given creator.
func MakeFirstContext(creator keybase1.UID) Context {
	return Context{Creator: creator}
}

// MakeContext makes a context with the given creator, writer, and
// nonce, where the writer is not necessarily equal to the creator,
// and the nonce is usually non-zero.
func MakeContext(creator, writer keybase1.UID, nonce RefNonce) Context {
	return Context{Creator: creator, Writer: writer, RefNonce: nonce}
}

// GetCreator returns the creator of the associated block.
func (c Context) GetCreator() keybase1.UID {
	return c.Creator
}

// GetWriter returns the writer of the associated block.
func (c Context) GetWriter() keybase1.UID {
	if !c.Writer.IsNil() {
		return c.Writer
	}
	return c.Creator
}

// SetWriter sets the Writer field, if necessary.
func (c *Context) SetWriter(newWriter keybase1.UID) {
	if c.Creator != newWriter {
		c.Writer = newWriter
	} else {
		// save some bytes by not populating the separate Writer
		// field if it matches the creator.
		c.Writer = ""
	}
}

// GetRefNonce returns the ref nonce of the associated block.
func (c Context) GetRefNonce() RefNonce {
	return c.RefNonce
}

// IsFirstRef returns whether or not p represents the first reference
// to the corresponding ID.
func (c Context) IsFirstRef() bool {
	return c.RefNonce == ZeroRefNonce
}

func (c Context) String() string {
	s := fmt.Sprintf("Context{Creator: %s", c.Creator)
	if len(c.Writer) > 0 {
		s += fmt.Sprintf(", Writer: %s", c.Writer)
	}
	if c.RefNonce != ZeroRefNonce {
		s += fmt.Sprintf(", RefNonce: %s", c.RefNonce)
	}
	s += "}"
	return s
}

// ContextMap is a map from a block ID to a list of its contexts.
type ContextMap map[ID][]Context
