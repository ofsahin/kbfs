// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"errors"
	"fmt"

	"github.com/keybase/client/go/protocol/keybase1"
	"github.com/keybase/go-codec/codec"
	"github.com/keybase/kbfs/kbfscodec"
	"github.com/keybase/kbfs/kbfscrypto"
	"github.com/keybase/kbfs/tlf"
	"golang.org/x/net/context"
)

// WriterMetadataV2 stores the metadata for a TLF that is
// only editable by users with writer permissions.
//
// NOTE: Don't add new fields to this type! Instead, add them to
// WriterMetadataExtraV2. This is because we want old clients to
// preserve unknown fields, and we're unable to do that for
// WriterMetadata directly because it's embedded in BareRootMetadata.
type WriterMetadataV2 struct {
	// Serialized, possibly encrypted, version of the PrivateMetadata
	SerializedPrivateMetadata []byte `codec:"data"`
	// The last KB user with writer permissions to this TLF
	// who modified this WriterMetadata
	LastModifyingWriter keybase1.UID
	// For public TLFs (since those don't have any keys at all).
	Writers []keybase1.UID `codec:",omitempty"`
	// For private TLFs. Writer key generations for this metadata. The
	// most recent one is last in the array. Must be same length as
	// BareRootMetadata.RKeys.
	WKeys TLFWriterKeyGenerationsV2 `codec:",omitempty"`
	// The directory ID, signed over to make verification easier
	ID tlf.ID
	// The branch ID, currently only set if this is in unmerged per-device history.
	BID BranchID
	// Flags
	WFlags WriterFlags
	// Estimated disk usage at this revision
	DiskUsage uint64

	// The total number of bytes in new blocks
	RefBytes uint64
	// The total number of bytes in unreferenced blocks
	UnrefBytes uint64

	Extra WriterMetadataExtraV2 `codec:"x,omitempty,omitemptycheckstruct"`
}

// ToWriterMetadataV3 converts the WriterMetadataV2 to a
// WriterMetadataV3.
func (wmdV2 *WriterMetadataV2) ToWriterMetadataV3() WriterMetadataV3 {
	var wmdV3 WriterMetadataV3
	wmdV3.Writers = make([]keybase1.UID, len(wmdV2.Writers))
	copy(wmdV3.Writers, wmdV2.Writers)

	wmdV3.UnresolvedWriters = make([]keybase1.SocialAssertion, len(wmdV2.Extra.UnresolvedWriters))
	copy(wmdV3.UnresolvedWriters, wmdV2.Extra.UnresolvedWriters)

	wmdV3.ID = wmdV2.ID
	wmdV3.BID = wmdV2.BID
	wmdV3.WFlags = wmdV2.WFlags
	wmdV3.DiskUsage = wmdV2.DiskUsage
	wmdV3.RefBytes = wmdV2.RefBytes
	wmdV3.UnrefBytes = wmdV2.UnrefBytes

	if wmdV2.ID.IsPublic() {
		wmdV3.LatestKeyGen = PublicKeyGen
	} else {
		wmdV3.LatestKeyGen = wmdV2.WKeys.LatestKeyGeneration()
	}
	return wmdV3
}

// WriterMetadataExtraV2 stores more fields for WriterMetadataV2. (See
// WriterMetadataV2 comments as to why this type is needed.)
type WriterMetadataExtraV2 struct {
	UnresolvedWriters []keybase1.SocialAssertion `codec:"uw,omitempty"`
	codec.UnknownFieldSetHandler
}

// BareRootMetadataV2 is the MD that is signed by the reader or
// writer. Unlike RootMetadata, it contains exactly the serializable
// metadata.
type BareRootMetadataV2 struct {
	// The metadata that is only editable by the writer.
	//
	// TODO: If we ever get a chance to update BareRootMetadata
	// without having to be backwards-compatible, WriterMetadata
	// should be unembedded; see comments to WriterMetadata as for
	// why.
	WriterMetadataV2

	// The signature for the writer metadata, to prove
	// that it's only been changed by writers.
	WriterMetadataSigInfo kbfscrypto.SignatureInfo

	// The last KB user who modified this BareRootMetadata
	LastModifyingUser keybase1.UID
	// Flags
	Flags MetadataFlags
	// The revision number
	Revision MetadataRevision
	// Pointer to the previous root block ID
	PrevRoot MdID
	// For private TLFs. Reader key generations for this metadata. The
	// most recent one is last in the array. Must be same length as
	// WriterMetadata.WKeys. If there are no readers, each generation
	// is empty.
	RKeys TLFReaderKeyGenerationsV2 `codec:",omitempty"`
	// For private TLFs. Any unresolved social assertions for readers.
	UnresolvedReaders []keybase1.SocialAssertion `codec:"ur,omitempty"`

	// ConflictInfo is set if there's a conflict for the given folder's
	// handle after a social assertion resolution.
	ConflictInfo *tlf.HandleExtension `codec:"ci,omitempty"`

	// FinalizedInfo is set if there are no more valid writer keys capable
	// of writing to the given folder.
	FinalizedInfo *tlf.HandleExtension `codec:"fi,omitempty"`

	codec.UnknownFieldSetHandler
}

// MakeInitialBareRootMetadataV2 creates a new BareRootMetadataV2
// object with revision MetadataRevisionInitial, and the given TLF ID
// and handle. Note that if the given ID/handle are private, rekeying
// must be done separately.
func MakeInitialBareRootMetadataV2(tlfID tlf.ID, h tlf.Handle) (
	*BareRootMetadataV2, error) {
	if tlfID.IsPublic() != h.IsPublic() {
		return nil, errors.New(
			"TlfID and TlfHandle disagree on public status")
	}

	var writers []keybase1.UID
	var wKeys TLFWriterKeyGenerationsV2
	var rKeys TLFReaderKeyGenerationsV2
	if tlfID.IsPublic() {
		writers = make([]keybase1.UID, len(h.Writers))
		copy(writers, h.Writers)
	} else {
		wKeys = make(TLFWriterKeyGenerationsV2, 0, 1)
		rKeys = make(TLFReaderKeyGenerationsV2, 0, 1)
	}

	var unresolvedWriters, unresolvedReaders []keybase1.SocialAssertion
	if len(h.UnresolvedWriters) > 0 {
		unresolvedWriters = make(
			[]keybase1.SocialAssertion, len(h.UnresolvedWriters))
		copy(unresolvedWriters, h.UnresolvedWriters)
	}

	if len(h.UnresolvedReaders) > 0 {
		unresolvedReaders = make(
			[]keybase1.SocialAssertion, len(h.UnresolvedReaders))
		copy(unresolvedReaders, h.UnresolvedReaders)
	}

	return &BareRootMetadataV2{
		WriterMetadataV2: WriterMetadataV2{
			Writers: writers,
			WKeys:   wKeys,
			ID:      tlfID,
			Extra: WriterMetadataExtraV2{
				UnresolvedWriters: unresolvedWriters,
			},
		},
		Revision:          MetadataRevisionInitial,
		RKeys:             rKeys,
		UnresolvedReaders: unresolvedReaders,
	}, nil
}

// TlfID implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) TlfID() tlf.ID {
	return md.ID
}

// KeyGenerationsToUpdate implements the BareRootMetadata interface
// for BareRootMetadataV2.
func (md *BareRootMetadataV2) KeyGenerationsToUpdate() (KeyGen, KeyGen) {
	latest := md.LatestKeyGeneration()
	if latest < FirstValidKeyGen {
		return 0, 0
	}
	// We keep track of all known key generations.
	return FirstValidKeyGen, latest + 1
}

// LatestKeyGeneration implements the BareRootMetadata interface for
// BareRootMetadataV2.
func (md *BareRootMetadataV2) LatestKeyGeneration() KeyGen {
	if md.ID.IsPublic() {
		return PublicKeyGen
	}
	return md.WKeys.LatestKeyGeneration()
}

func (md *BareRootMetadataV2) haveOnlyUserRKeysChanged(
	codec kbfscodec.Codec, prevMD *BareRootMetadataV2,
	user keybase1.UID) (bool, error) {
	// Require the same number of generations
	if len(md.RKeys) != len(prevMD.RKeys) {
		return false, nil
	}
	for i, gen := range md.RKeys {
		prevMDGen := prevMD.RKeys[i]
		if len(gen.RKeys) != len(prevMDGen.RKeys) {
			return false, nil
		}
		for u, keys := range gen.RKeys {
			if u != user {
				prevKeys := prevMDGen.RKeys[u]
				keysEqual, err :=
					kbfscodec.Equal(codec, keys, prevKeys)
				if err != nil {
					return false, err
				}
				if !keysEqual {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// IsValidRekeyRequest implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsValidRekeyRequest(
	codec kbfscodec.Codec, prevBareMd BareRootMetadata,
	user keybase1.UID, _, _ ExtraMetadata) (bool, error) {
	if !md.IsWriterMetadataCopiedSet() {
		// Not a copy.
		return false, nil
	}
	prevMd, ok := prevBareMd.(*BareRootMetadataV2)
	if !ok {
		// Not the same type so not a copy.
		return false, nil
	}
	writerEqual, err := kbfscodec.Equal(
		codec, md.WriterMetadataV2, prevMd.WriterMetadataV2)
	if err != nil {
		return false, err
	}
	if !writerEqual {
		// Copy mismatch.
		return false, nil
	}
	writerSigInfoEqual, err := kbfscodec.Equal(codec,
		md.WriterMetadataSigInfo, prevMd.WriterMetadataSigInfo)
	if err != nil {
		return false, err
	}
	if !writerSigInfoEqual {
		// Signature/public key mismatch.
		return false, nil
	}
	onlyUserRKeysChanged, err := md.haveOnlyUserRKeysChanged(
		codec, prevMd, user)
	if err != nil {
		return false, err
	}
	if !onlyUserRKeysChanged {
		// Keys outside of this user's reader key set have changed.
		return false, nil
	}
	return true, nil
}

// MergedStatus implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) MergedStatus() MergeStatus {
	if md.WFlags&MetadataFlagUnmerged != 0 {
		return Unmerged
	}
	return Merged
}

// IsRekeySet implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsRekeySet() bool {
	return md.Flags&MetadataFlagRekey != 0
}

// IsWriterMetadataCopiedSet implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsWriterMetadataCopiedSet() bool {
	return md.Flags&MetadataFlagWriterMetadataCopied != 0
}

// IsFinal implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsFinal() bool {
	return md.Flags&MetadataFlagFinal != 0
}

// IsWriter implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsWriter(
	user keybase1.UID, deviceKID keybase1.KID, _ ExtraMetadata) bool {
	if md.ID.IsPublic() {
		for _, w := range md.Writers {
			if w == user {
				return true
			}
		}
		return false
	}
	return md.WKeys.IsWriter(user, deviceKID)
}

// IsReader implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsReader(
	user keybase1.UID, deviceKID keybase1.KID, _ ExtraMetadata) bool {
	if md.ID.IsPublic() {
		return true
	}
	return md.RKeys.IsReader(user, deviceKID)
}

func (md *BareRootMetadataV2) deepCopy(
	codec kbfscodec.Codec) (*BareRootMetadataV2, error) {
	var newMd BareRootMetadataV2
	if err := kbfscodec.Update(codec, &newMd, md); err != nil {
		return nil, err
	}
	return &newMd, nil
}

// DeepCopy implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) DeepCopy(
	codec kbfscodec.Codec) (MutableBareRootMetadata, error) {
	return md.deepCopy(codec)
}

// MakeSuccessorCopy implements the ImmutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) MakeSuccessorCopy(
	ctx context.Context, config Config, kmd KeyMetadata,
	extra ExtraMetadata, isReadableAndWriter bool) (
	MutableBareRootMetadata, ExtraMetadata, error) {

	if !isReadableAndWriter ||
		config.MetadataVersion() < SegregatedKeyBundlesVer {
		// Continue with the current version.  If we're just a reader,
		// or can't decrypt the MD, we have to continue with v2
		// because we can't just copy a v2 signature into a v3 MD
		// blindly.
		mdCopy, err := md.makeSuccessorCopyV2(config, isReadableAndWriter)
		if err != nil {
			return nil, nil, err
		}
		return mdCopy, nil, nil
	}

	// Upconvert to the new version.
	return md.makeSuccessorCopyV3(ctx, config, kmd)
}

func (md *BareRootMetadataV2) makeSuccessorCopyV2(config Config, isReadableAndWriter bool) (
	*BareRootMetadataV2, error) {
	mdCopy, err := md.deepCopy(config.Codec())
	if err != nil {
		return nil, err
	}
	if isReadableAndWriter {
		mdCopy.WriterMetadataSigInfo = kbfscrypto.SignatureInfo{}
	}
	return mdCopy, nil
}

func (md *BareRootMetadataV2) makeSuccessorCopyV3(ctx context.Context, config Config, kmd KeyMetadata) (
	*BareRootMetadataV3, ExtraMetadata, error) {
	mdV3 := &BareRootMetadataV3{}

	// Fill out the writer metadata.
	mdV3.WriterMetadata = md.WriterMetadataV2.ToWriterMetadataV3()

	// Have this as ExtraMetadata so we return an untyped nil
	// instead of a typed nil.
	var extraCopy ExtraMetadata
	if md.LatestKeyGeneration() != PublicKeyGen {
		// Fill out the writer key bundle.
		wkbV2, wkbV3, err := md.WKeys.ToTLFWriterKeyBundleV3(
			ctx, config.Codec(), config.Crypto(),
			config.KeyManager(), kmd)
		if err != nil {
			return nil, nil, err
		}

		mdV3.WriterMetadata.WKeyBundleID, err =
			config.Crypto().MakeTLFWriterKeyBundleID(wkbV3)
		if err != nil {
			return nil, nil, err
		}

		// Fill out the reader key bundle.  wkbV2 is passed
		// because in V2 metadata ephemeral public keys for
		// readers were sometimes in the writer key bundles.
		rkbV3, err := md.RKeys.ToTLFReaderKeyBundleV3(
			config.Codec(), wkbV2)
		if err != nil {
			return nil, nil, err
		}
		mdV3.RKeyBundleID, err =
			config.Crypto().MakeTLFReaderKeyBundleID(rkbV3)
		if err != nil {
			return nil, nil, err
		}

		extraCopy = NewExtraMetadataV3(wkbV3, rkbV3, true, true)
	}

	mdV3.LastModifyingUser = md.LastModifyingUser
	mdV3.Flags = md.Flags
	mdV3.Revision = md.Revision // Incremented by the caller.
	// PrevRoot is set by the caller.
	mdV3.UnresolvedReaders = make([]keybase1.SocialAssertion, len(md.UnresolvedReaders))
	copy(mdV3.UnresolvedReaders, md.UnresolvedReaders)

	if md.ConflictInfo != nil {
		ci := *md.ConflictInfo
		md.ConflictInfo = &ci
	}

	// Metadata with finalized info is never succeeded.
	if md.FinalizedInfo != nil {
		// Shouldn't be possible.
		return nil, nil, errors.New("Non-nil finalized info")
	}

	return mdV3, extraCopy, nil
}

// CheckValidSuccessor implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) CheckValidSuccessor(
	currID MdID, nextMd BareRootMetadata) error {
	// (1) Verify current metadata is non-final.
	if md.IsFinal() {
		return MetadataIsFinalError{}
	}

	// (2) Check TLF ID.
	if nextMd.TlfID() != md.ID {
		return MDTlfIDMismatch{
			currID: md.ID,
			nextID: nextMd.TlfID(),
		}
	}

	// (3) Check revision.
	if nextMd.RevisionNumber() != md.RevisionNumber()+1 {
		return MDRevisionMismatch{
			rev:  nextMd.RevisionNumber(),
			curr: md.RevisionNumber(),
		}
	}

	// (4) Check PrevRoot pointer.
	expectedPrevRoot := currID
	if nextMd.IsFinal() {
		expectedPrevRoot = md.GetPrevRoot()
	}
	if nextMd.GetPrevRoot() != expectedPrevRoot {
		return MDPrevRootMismatch{
			prevRoot:         nextMd.GetPrevRoot(),
			expectedPrevRoot: expectedPrevRoot,
		}
	}

	// (5) Check branch ID.
	if md.MergedStatus() == nextMd.MergedStatus() && md.BID() != nextMd.BID() {
		return fmt.Errorf("Unexpected branch ID on successor: %s vs. %s",
			md.BID(), nextMd.BID())
	} else if md.MergedStatus() == Unmerged && nextMd.MergedStatus() == Merged {
		return errors.New("merged MD can't follow unmerged MD")
	}

	// (6) Check disk usage.
	expectedUsage := md.DiskUsage()
	if !nextMd.IsWriterMetadataCopiedSet() {
		expectedUsage += nextMd.RefBytes() - nextMd.UnrefBytes()
	}
	if nextMd.DiskUsage() != expectedUsage {
		return MDDiskUsageMismatch{
			expectedDiskUsage: expectedUsage,
			actualDiskUsage:   nextMd.DiskUsage(),
		}
	}

	// TODO: Check that the successor (bare) TLF handle is the
	// same or more resolved.

	return nil
}

// CheckValidSuccessorForServer implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) CheckValidSuccessorForServer(
	currID MdID, nextMd BareRootMetadata) error {
	err := md.CheckValidSuccessor(currID, nextMd)
	switch err := err.(type) {
	case nil:
		break

	case MDRevisionMismatch:
		return MDServerErrorConflictRevision{
			Expected: err.curr + 1,
			Actual:   err.rev,
		}

	case MDPrevRootMismatch:
		return MDServerErrorConflictPrevRoot{
			Expected: err.expectedPrevRoot,
			Actual:   err.prevRoot,
		}

	case MDDiskUsageMismatch:
		return MDServerErrorConflictDiskUsage{
			Expected: err.expectedDiskUsage,
			Actual:   err.actualDiskUsage,
		}

	default:
		return MDServerError{Err: err}
	}

	return nil
}

// MakeBareTlfHandle implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) MakeBareTlfHandle(_ ExtraMetadata) (
	tlf.Handle, error) {
	var writers, readers []keybase1.UID
	if md.ID.IsPublic() {
		writers = md.Writers
		readers = []keybase1.UID{keybase1.PublicUID}
	} else {
		if len(md.WKeys) == 0 {
			return tlf.Handle{}, errors.New("No writer key generations; need rekey?")
		}

		if len(md.RKeys) == 0 {
			return tlf.Handle{}, errors.New("No reader key generations; need rekey?")
		}

		wkb := md.WKeys[len(md.WKeys)-1]
		rkb := md.RKeys[len(md.RKeys)-1]
		writers = make([]keybase1.UID, 0, len(wkb.WKeys))
		readers = make([]keybase1.UID, 0, len(rkb.RKeys))
		for w := range wkb.WKeys {
			writers = append(writers, w)
		}
		for r := range rkb.RKeys {
			// TODO: Return an error instead if r is
			// PublicUID. Maybe return an error if r is in
			// WKeys also. Or do all this in
			// MakeBareTlfHandle.
			if _, ok := wkb.WKeys[r]; !ok &&
				r != keybase1.PublicUID {
				readers = append(readers, r)
			}
		}
	}

	return tlf.MakeHandle(
		writers, readers,
		md.Extra.UnresolvedWriters, md.UnresolvedReaders,
		md.TlfHandleExtensions())
}

// TlfHandleExtensions implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) TlfHandleExtensions() (
	extensions []tlf.HandleExtension) {
	if md.ConflictInfo != nil {
		extensions = append(extensions, *md.ConflictInfo)
	}
	if md.FinalizedInfo != nil {
		extensions = append(extensions, *md.FinalizedInfo)
	}
	return extensions
}

// PromoteReaders implements the BareRootMetadata interface for
// BareRootMetadataV2.
func (md *BareRootMetadataV2) PromoteReaders(
	readersToPromote map[keybase1.UID]bool,
	_ ExtraMetadata) error {
	if md.TlfID().IsPublic() {
		return InvalidPublicTLFOperation{md.TlfID(), "PromoteReaders", md.Version()}
	}

	for i, rkb := range md.RKeys {
		for reader := range readersToPromote {
			dkim, ok := rkb.RKeys[reader]
			if !ok {
				return fmt.Errorf("Could not find %s in key gen %d",
					reader, FirstValidKeyGen+KeyGen(i))
			}
			// TODO: This is incorrect, since dkim may
			// contain negative offsets, and a lot of code
			// assumes that writers will only contain
			// non-negative offsets.
			//
			// See KBFS-1719.
			md.WKeys[i].WKeys[reader] = dkim
			delete(rkb.RKeys, reader)
		}
	}

	return nil
}

// RevokeRemovedDevices implements the BareRootMetadata interface for
// BareRootMetadataV2.
func (md *BareRootMetadataV2) RevokeRemovedDevices(
	wKeys, rKeys UserDevicePublicKeys, _ ExtraMetadata) (
	ServerHalfRemovalInfo, error) {
	if md.TlfID().IsPublic() {
		return nil, InvalidPublicTLFOperation{
			md.TlfID(), "RevokeRemovedDevices", md.Version()}
	}

	var wRemovalInfo ServerHalfRemovalInfo
	for _, wkb := range md.WKeys {
		removalInfo := wkb.WKeys.removeDevicesNotIn(wKeys)
		if wRemovalInfo == nil {
			wRemovalInfo = removalInfo
		} else {
			err := wRemovalInfo.addGeneration(removalInfo)
			if err != nil {
				return nil, err
			}
		}
	}

	var rRemovalInfo ServerHalfRemovalInfo
	for _, rkb := range md.RKeys {
		removalInfo := rkb.RKeys.removeDevicesNotIn(rKeys)
		if rRemovalInfo == nil {
			rRemovalInfo = removalInfo
		} else {
			err := rRemovalInfo.addGeneration(removalInfo)
			if err != nil {
				return nil, err
			}
		}
	}

	return wRemovalInfo.mergeUsers(rRemovalInfo)
}

// getTLFKeyBundles returns the bundles for a given key generation.
// Note that it is legal a writer or a reader to have no keys in their
// bundle, if they only have a Keybase username with no device keys
// yet.
func (md *BareRootMetadataV2) getTLFKeyBundles(keyGen KeyGen) (
	*TLFWriterKeyBundleV2, *TLFReaderKeyBundleV2, error) {
	if md.ID.IsPublic() {
		return nil, nil, InvalidPublicTLFOperation{md.ID, "getTLFKeyBundles", md.Version()}
	}

	if keyGen < FirstValidKeyGen {
		return nil, nil, InvalidKeyGenerationError{md.ID, keyGen}
	}
	i := int(keyGen - FirstValidKeyGen)
	if i >= len(md.WKeys) || i >= len(md.RKeys) {
		return nil, nil, NewKeyGenerationError{md.ID, keyGen}
	}
	return &md.WKeys[i], &md.RKeys[i], nil
}

// GetDevicePublicKeys implements the BareRootMetadata interface for
// BareRootMetadataV2.
func (md *BareRootMetadataV2) GetDevicePublicKeys(
	user keybase1.UID, _ ExtraMetadata) (
	isWriter bool, keys DevicePublicKeys, err error) {
	if md.ID.IsPublic() {
		return false, nil, InvalidPublicTLFOperation{
			md.ID, "GetDevicePublicKeys", md.Version(),
		}
	}

	if len(md.WKeys) == 0 {
		return false, nil, errors.New(
			"GetDevicePublicKeys called with no key generations (V2)")
	}

	dkim := md.WKeys[len(md.WKeys)-1].WKeys[user]
	isWriter = true
	if len(dkim) == 0 {
		dkim = md.RKeys[len(md.RKeys)-1].RKeys[user]
		isWriter = false
		if len(dkim) == 0 {
			return false, nil, nil
		}
	}

	keys = make(DevicePublicKeys, len(dkim))
	for kid := range dkim {
		keys[kbfscrypto.MakeCryptPublicKey(kid)] = true
	}

	return isWriter, keys, nil
}

// HasKeyForUser implements the BareRootMetadata interface for
// BareRootMetadataV2.
func (md *BareRootMetadataV2) HasKeyForUser(
	keyGen KeyGen, user keybase1.UID, _ ExtraMetadata) (bool, error) {
	if md.ID.IsPublic() {
		return false, InvalidPublicTLFOperation{md.ID, "HasKeyForUser", md.Version()}
	}

	wkb, rkb, err := md.getTLFKeyBundles(keyGen)
	if err != nil {
		return false, err
	}

	return (len(wkb.WKeys[user]) > 0) || (len(rkb.RKeys[user]) > 0), nil
}

// GetTLFCryptKeyParams implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetTLFCryptKeyParams(
	keyGen KeyGen, user keybase1.UID, key kbfscrypto.CryptPublicKey,
	_ ExtraMetadata) (
	kbfscrypto.TLFEphemeralPublicKey, EncryptedTLFCryptKeyClientHalf,
	TLFCryptKeyServerHalfID, bool, error) {
	wkb, rkb, err := md.getTLFKeyBundles(keyGen)
	if err != nil {
		return kbfscrypto.TLFEphemeralPublicKey{},
			EncryptedTLFCryptKeyClientHalf{},
			TLFCryptKeyServerHalfID{}, false, err
	}

	dkim := wkb.WKeys[user]
	if dkim == nil {
		dkim = rkb.RKeys[user]
		if dkim == nil {
			return kbfscrypto.TLFEphemeralPublicKey{},
				EncryptedTLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{}, false, nil
		}
	}
	info, ok := dkim[key.KID()]
	if !ok {
		return kbfscrypto.TLFEphemeralPublicKey{},
			EncryptedTLFCryptKeyClientHalf{},
			TLFCryptKeyServerHalfID{}, false, nil
	}

	_, _, ePubKey, err := getEphemeralPublicKeyInfoV2(info, *wkb, *rkb)
	if err != nil {
		return kbfscrypto.TLFEphemeralPublicKey{},
			EncryptedTLFCryptKeyClientHalf{},
			TLFCryptKeyServerHalfID{}, false, err
	}

	return ePubKey, info.ClientHalf, info.ServerHalfID, true, nil
}

// IsValidAndSigned implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsValidAndSigned(
	codec kbfscodec.Codec, crypto cryptoPure, extra ExtraMetadata) error {
	// Optimization -- if the WriterMetadata signature is nil, it
	// will fail verification.
	if md.WriterMetadataSigInfo.IsNil() {
		return errors.New("Missing WriterMetadata signature")
	}

	if md.IsFinal() {
		if md.Revision < MetadataRevisionInitial+1 {
			return fmt.Errorf("Invalid final revision %d", md.Revision)
		}

		if md.Revision == (MetadataRevisionInitial + 1) {
			if md.PrevRoot != (MdID{}) {
				return fmt.Errorf("Invalid PrevRoot %s for initial final revision", md.PrevRoot)
			}
		} else {
			if md.PrevRoot == (MdID{}) {
				return errors.New("No PrevRoot for non-initial final revision")
			}
		}
	} else {
		if md.Revision < MetadataRevisionInitial {
			return fmt.Errorf("Invalid revision %d", md.Revision)
		}

		if md.Revision == MetadataRevisionInitial {
			if md.PrevRoot != (MdID{}) {
				return fmt.Errorf("Invalid PrevRoot %s for initial revision", md.PrevRoot)
			}
		} else {
			if md.PrevRoot == (MdID{}) {
				return errors.New("No PrevRoot for non-initial revision")
			}
		}
	}

	if len(md.SerializedPrivateMetadata) == 0 {
		return errors.New("No private metadata")
	}

	if (md.MergedStatus() == Merged) != (md.BID() == NullBranchID) {
		return fmt.Errorf("Branch ID %s doesn't match merged status %s",
			md.BID(), md.MergedStatus())
	}

	handle, err := md.MakeBareTlfHandle(extra)
	if err != nil {
		return err
	}

	// Make sure the last writer is valid.
	writer := md.LastModifyingWriter()
	if !handle.IsWriter(writer) {
		return fmt.Errorf("Invalid modifying writer %s", writer)
	}

	// Make sure the last modifier is valid.
	user := md.LastModifyingUser
	if !handle.IsReader(user) {
		return fmt.Errorf("Invalid modifying user %s", user)
	}

	// Verify signature. We have to re-marshal the WriterMetadata,
	// since it's embedded.
	buf, err := codec.Encode(md.WriterMetadataV2)
	if err != nil {
		return err
	}

	err = crypto.Verify(buf, md.WriterMetadataSigInfo)
	if err != nil {
		return fmt.Errorf("Could not verify writer metadata: %v", err)
	}

	return nil
}

// IsLastModifiedBy implements the BareRootMetadata interface for
// BareRootMetadataV2.
func (md *BareRootMetadataV2) IsLastModifiedBy(
	uid keybase1.UID, key kbfscrypto.VerifyingKey) error {
	// Verify the user and device are the writer.
	writer := md.LastModifyingWriter()
	if !md.IsWriterMetadataCopiedSet() {
		if writer != uid {
			return fmt.Errorf("Last writer %s != %s", writer, uid)
		}
		if md.WriterMetadataSigInfo.VerifyingKey != key {
			return fmt.Errorf(
				"Last writer verifying key %v != %v",
				md.WriterMetadataSigInfo.VerifyingKey, key)
		}
	}

	// Verify the user and device are the last modifier.
	user := md.GetLastModifyingUser()
	if user != uid {
		return fmt.Errorf("Last modifier %s != %s", user, uid)
	}

	return nil
}

// LastModifyingWriter implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) LastModifyingWriter() keybase1.UID {
	return md.WriterMetadataV2.LastModifyingWriter
}

// GetLastModifyingUser implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetLastModifyingUser() keybase1.UID {
	return md.LastModifyingUser
}

// RefBytes implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) RefBytes() uint64 {
	return md.WriterMetadataV2.RefBytes
}

// UnrefBytes implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) UnrefBytes() uint64 {
	return md.WriterMetadataV2.UnrefBytes
}

// DiskUsage implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) DiskUsage() uint64 {
	return md.WriterMetadataV2.DiskUsage
}

// SetRefBytes implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetRefBytes(refBytes uint64) {
	md.WriterMetadataV2.RefBytes = refBytes
}

// SetUnrefBytes implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetUnrefBytes(unrefBytes uint64) {
	md.WriterMetadataV2.UnrefBytes = unrefBytes
}

// SetDiskUsage implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetDiskUsage(diskUsage uint64) {
	md.WriterMetadataV2.DiskUsage = diskUsage
}

// AddRefBytes implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) AddRefBytes(refBytes uint64) {
	md.WriterMetadataV2.RefBytes += refBytes
}

// AddUnrefBytes implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) AddUnrefBytes(unrefBytes uint64) {
	md.WriterMetadataV2.UnrefBytes += unrefBytes
}

// AddDiskUsage implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) AddDiskUsage(diskUsage uint64) {
	md.WriterMetadataV2.DiskUsage += diskUsage
}

// RevisionNumber implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) RevisionNumber() MetadataRevision {
	return md.Revision
}

// BID implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) BID() BranchID {
	return md.WriterMetadataV2.BID
}

// GetPrevRoot implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetPrevRoot() MdID {
	return md.PrevRoot
}

// ClearRekeyBit implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) ClearRekeyBit() {
	md.Flags &= ^MetadataFlagRekey
}

// ClearWriterMetadataCopiedBit implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) ClearWriterMetadataCopiedBit() {
	md.Flags &= ^MetadataFlagWriterMetadataCopied
}

// IsUnmergedSet implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) IsUnmergedSet() bool {
	return (md.WriterMetadataV2.WFlags & MetadataFlagUnmerged) != 0
}

// SetUnmerged implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetUnmerged() {
	md.WriterMetadataV2.WFlags |= MetadataFlagUnmerged
}

// SetBranchID implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetBranchID(bid BranchID) {
	md.WriterMetadataV2.BID = bid
}

// SetPrevRoot implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetPrevRoot(mdID MdID) {
	md.PrevRoot = mdID
}

// GetSerializedPrivateMetadata implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetSerializedPrivateMetadata() []byte {
	return md.SerializedPrivateMetadata
}

// SetSerializedPrivateMetadata implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetSerializedPrivateMetadata(spmd []byte) {
	md.SerializedPrivateMetadata = spmd
}

// GetSerializedWriterMetadata implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetSerializedWriterMetadata(
	codec kbfscodec.Codec) ([]byte, error) {
	return codec.Encode(md.WriterMetadataV2)
}

// SignWriterMetadataInternally implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SignWriterMetadataInternally(
	ctx context.Context, codec kbfscodec.Codec,
	signer kbfscrypto.Signer) error {
	buf, err := codec.Encode(md.WriterMetadataV2)
	if err != nil {
		return err
	}

	sigInfo, err := signer.Sign(ctx, buf)
	if err != nil {
		return err
	}
	md.WriterMetadataSigInfo = sigInfo
	return nil
}

// SetLastModifyingWriter implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetLastModifyingWriter(user keybase1.UID) {
	md.WriterMetadataV2.LastModifyingWriter = user
}

// SetLastModifyingUser implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetLastModifyingUser(user keybase1.UID) {
	md.LastModifyingUser = user
}

// SetRekeyBit implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetRekeyBit() {
	md.Flags |= MetadataFlagRekey
}

// SetFinalBit implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetFinalBit() {
	md.Flags |= MetadataFlagFinal
}

// SetWriterMetadataCopiedBit implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetWriterMetadataCopiedBit() {
	md.Flags |= MetadataFlagWriterMetadataCopied
}

// SetRevision implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetRevision(revision MetadataRevision) {
	md.Revision = revision
}

// SetUnresolvedReaders implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetUnresolvedReaders(readers []keybase1.SocialAssertion) {
	md.UnresolvedReaders = readers
}

// SetUnresolvedWriters implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetUnresolvedWriters(writers []keybase1.SocialAssertion) {
	md.Extra.UnresolvedWriters = writers
}

// SetConflictInfo implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetConflictInfo(ci *tlf.HandleExtension) {
	md.ConflictInfo = ci
}

// SetFinalizedInfo implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetFinalizedInfo(fi *tlf.HandleExtension) {
	md.FinalizedInfo = fi
}

// SetWriters implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetWriters(writers []keybase1.UID) {
	md.Writers = writers
}

// SetTlfID implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) SetTlfID(tlf tlf.ID) {
	md.ID = tlf
}

// ClearFinalBit implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) ClearFinalBit() {
	md.Flags &= ^MetadataFlagFinal
}

// Version implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) Version() MetadataVer {
	// Only folders with unresolved assertions or conflict info get the
	// new version.
	if len(md.Extra.UnresolvedWriters) > 0 || len(md.UnresolvedReaders) > 0 ||
		md.ConflictInfo != nil ||
		md.FinalizedInfo != nil {
		return InitialExtraMetadataVer
	}
	// Let other types of MD objects use the older version since they
	// are still compatible with older clients.
	return PreExtraMetadataVer
}

// GetCurrentTLFPublicKey implements the BareRootMetadata interface
// for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetCurrentTLFPublicKey(
	_ ExtraMetadata) (kbfscrypto.TLFPublicKey, error) {
	if len(md.WKeys) == 0 {
		return kbfscrypto.TLFPublicKey{}, errors.New(
			"No key generations in GetCurrentTLFPublicKey")
	}
	return md.WKeys[len(md.WKeys)-1].TLFPublicKey, nil
}

// AreKeyGenerationsEqual implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) AreKeyGenerationsEqual(
	codec kbfscodec.Codec, other BareRootMetadata) (
	bool, error) {
	md2, ok := other.(*BareRootMetadataV2)
	if !ok {
		// No idea what this is.
		return false, errors.New("Unknown metadata version")
	}
	ok, err := kbfscodec.Equal(codec, md.WKeys, md2.WKeys)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	ok, err = kbfscodec.Equal(codec, md.RKeys, md2.RKeys)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// GetUnresolvedParticipants implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetUnresolvedParticipants() (readers, writers []keybase1.SocialAssertion) {
	return md.UnresolvedReaders, md.WriterMetadataV2.Extra.UnresolvedWriters
}

// GetUserDeviceKeyInfoMaps implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetUserDeviceKeyInfoMaps(
	codec kbfscodec.Codec, keyGen KeyGen, _ ExtraMetadata) (
	readers, writers UserDeviceKeyInfoMap, err error) {
	wkb, rkb, err := md.getTLFKeyBundles(keyGen)
	if err != nil {
		return nil, nil, err
	}

	rUDKIM, err := rkb.RKeys.toUDKIM(codec)
	if err != nil {
		return nil, nil, err
	}

	wUDKIM, err := wkb.WKeys.toUDKIM(codec)
	if err != nil {
		return nil, nil, err
	}

	return rUDKIM, wUDKIM, nil
}

func (md *BareRootMetadataV2) updateKeyGenerationForReader(
	crypto cryptoPure, keyGen KeyGen, rKeys UserDevicePublicKeys,
	ePubKey kbfscrypto.TLFEphemeralPublicKey,
	ePrivKey kbfscrypto.TLFEphemeralPrivateKey,
	tlfCryptKey kbfscrypto.TLFCryptKey) (UserDeviceKeyServerHalves, error) {
	_, rkb, err := md.getTLFKeyBundles(keyGen)
	if err != nil {
		return nil, err
	}

	// This is VERY ugly, but we need it in order to avoid
	// having to version the metadata. The index will be
	// strictly negative for reader ephemeral public keys.
	newIndex := -len(rkb.TLFReaderEphemeralPublicKeys) - 1

	rServerHalves, err := rkb.RKeys.fillInUserInfos(
		crypto, newIndex, rKeys, ePrivKey, tlfCryptKey)
	if err != nil {
		return nil, err
	}

	if len(rServerHalves) > 0 {
		rkb.TLFReaderEphemeralPublicKeys = append(
			rkb.TLFReaderEphemeralPublicKeys, ePubKey)
	}

	return rServerHalves, nil
}

func (md *BareRootMetadataV2) updateKeyGeneration(
	crypto cryptoPure, keyGen KeyGen, wKeys, rKeys UserDevicePublicKeys,
	ePubKey kbfscrypto.TLFEphemeralPublicKey,
	ePrivKey kbfscrypto.TLFEphemeralPrivateKey,
	tlfCryptKey kbfscrypto.TLFCryptKey) (UserDeviceKeyServerHalves, error) {
	if len(wKeys) == 0 {
		return nil, errors.New("wKeys unexpectedly non-empty")
	}

	wkb, rkb, err := md.getTLFKeyBundles(keyGen)
	if err != nil {
		return nil, err
	}

	newIndex := len(wkb.TLFEphemeralPublicKeys)

	wServerHalves, err := wkb.WKeys.fillInUserInfos(
		crypto, newIndex, wKeys, ePrivKey, tlfCryptKey)
	if err != nil {
		return nil, err
	}

	rServerHalves, err := rkb.RKeys.fillInUserInfos(
		crypto, newIndex, rKeys, ePrivKey, tlfCryptKey)
	if err != nil {
		return nil, err
	}

	serverHalves, err := wServerHalves.mergeUsers(rServerHalves)
	if err != nil {
		return nil, err
	}

	if len(serverHalves) > 0 {
		wkb.TLFEphemeralPublicKeys =
			append(wkb.TLFEphemeralPublicKeys, ePubKey)
	}

	return serverHalves, nil
}

// AddKeyGeneration implements the MutableBareRootMetadata interface
// for BareRootMetadataV2.
func (md *BareRootMetadataV2) AddKeyGeneration(codec kbfscodec.Codec,
	crypto cryptoPure, _ ExtraMetadata,
	wKeys, rKeys UserDevicePublicKeys,
	ePubKey kbfscrypto.TLFEphemeralPublicKey,
	ePrivKey kbfscrypto.TLFEphemeralPrivateKey,
	pubKey kbfscrypto.TLFPublicKey,
	currCryptKey, nextCryptKey kbfscrypto.TLFCryptKey) (
	nextExtra ExtraMetadata,
	serverHalves UserDeviceKeyServerHalves, err error) {
	if md.TlfID().IsPublic() {
		return nil, nil, InvalidPublicTLFOperation{
			md.TlfID(), "AddKeyGeneration", md.Version()}
	}

	if len(wKeys) == 0 {
		return nil, nil, errors.New("wkeys unexpectedly non-empty")
	}

	if currCryptKey != (kbfscrypto.TLFCryptKey{}) {
		return nil, nil, errors.New("currCryptKey unexpectedly non-zero")
	}

	newWriterKeys := TLFWriterKeyBundleV2{
		WKeys:        make(UserDeviceKeyInfoMapV2),
		TLFPublicKey: pubKey,
	}
	md.WKeys = append(md.WKeys, newWriterKeys)

	newReaderKeys := TLFReaderKeyBundleV2{
		RKeys: make(UserDeviceKeyInfoMapV2),
	}
	md.RKeys = append(md.RKeys, newReaderKeys)

	serverHalves, err = md.updateKeyGeneration(
		crypto, md.LatestKeyGeneration(), wKeys, rKeys,
		ePubKey, ePrivKey, nextCryptKey)
	if err != nil {
		return nil, nil, err
	}

	return nil, serverHalves, nil
}

// UpdateKeyBundles implements the MutableBareRootMetadata interface
// for BareRootMetadataV2.
func (md *BareRootMetadataV2) UpdateKeyBundles(crypto cryptoPure,
	_ ExtraMetadata, wKeys, rKeys UserDevicePublicKeys,
	ePubKey kbfscrypto.TLFEphemeralPublicKey,
	ePrivKey kbfscrypto.TLFEphemeralPrivateKey,
	tlfCryptKeys []kbfscrypto.TLFCryptKey) (
	[]UserDeviceKeyServerHalves, error) {
	if md.TlfID().IsPublic() {
		return nil, InvalidPublicTLFOperation{
			md.TlfID(), "UpdateKeyBundles", md.Version()}
	}

	expectedTLFCryptKeyCount := int(
		md.LatestKeyGeneration() - FirstValidKeyGen + 1)
	if len(tlfCryptKeys) != expectedTLFCryptKeyCount {
		return nil, fmt.Errorf(
			"(MDv2) Expected %d TLF crypt keys, got %d",
			expectedTLFCryptKeyCount, len(tlfCryptKeys))
	}

	serverHalves := make([]UserDeviceKeyServerHalves, len(tlfCryptKeys))
	if len(wKeys) == 0 {
		// Reader rekey case.
		for keyGen := FirstValidKeyGen; keyGen <= md.LatestKeyGeneration(); keyGen++ {
			serverHalvesGen, err := md.updateKeyGenerationForReader(
				crypto, keyGen, rKeys, ePubKey, ePrivKey,
				tlfCryptKeys[keyGen-FirstValidKeyGen])
			if err != nil {
				return nil, err
			}

			serverHalves[keyGen-FirstValidKeyGen] = serverHalvesGen
		}

		return serverHalves, nil
	}

	// Usual rekey case.

	for keyGen := FirstValidKeyGen; keyGen <= md.LatestKeyGeneration(); keyGen++ {
		serverHalvesGen, err := md.updateKeyGeneration(
			crypto, keyGen, wKeys, rKeys,
			ePubKey, ePrivKey,
			tlfCryptKeys[keyGen-FirstValidKeyGen])
		if err != nil {
			return nil, err
		}

		serverHalves[keyGen-FirstValidKeyGen] = serverHalvesGen
	}
	return serverHalves, nil

}

// GetTLFWriterKeyBundleID implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetTLFWriterKeyBundleID() TLFWriterKeyBundleID {
	// Since key bundles are stored internally, just return the zero value.
	return TLFWriterKeyBundleID{}
}

// GetTLFReaderKeyBundleID implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetTLFReaderKeyBundleID() TLFReaderKeyBundleID {
	// Since key bundles are stored internally, just return the zero value.
	return TLFReaderKeyBundleID{}
}

// FinalizeRekey implements the MutableBareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) FinalizeRekey(
	_ cryptoPure, _ ExtraMetadata) error {
	// Nothing to do.
	return nil
}

// StoresHistoricTLFCryptKeys implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) StoresHistoricTLFCryptKeys() bool {
	// MDv2 metadata contains only per device encrypted keys.
	return false
}

// GetHistoricTLFCryptKey implements the BareRootMetadata interface for BareRootMetadataV2.
func (md *BareRootMetadataV2) GetHistoricTLFCryptKey(
	_ cryptoPure, _ KeyGen, _ kbfscrypto.TLFCryptKey, _ ExtraMetadata) (
	kbfscrypto.TLFCryptKey, error) {
	return kbfscrypto.TLFCryptKey{}, errors.New(
		"TLF crypt key not symmetrically encrypted")
}
