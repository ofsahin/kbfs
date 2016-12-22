// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"errors"
	"fmt"

	"github.com/keybase/client/go/libkb"
	"github.com/keybase/client/go/logger"
	"github.com/keybase/client/go/protocol/keybase1"
	"github.com/keybase/kbfs/kbfscrypto"
	"github.com/keybase/kbfs/tlf"
	"golang.org/x/net/context"
)

// KeyManagerStandard implements the KeyManager interface by fetching
// keys from KeyOps and KBPKI, and computing the complete keys
// necessary to run KBFS.
type KeyManagerStandard struct {
	config   Config
	log      logger.Logger
	deferLog logger.Logger
}

// NewKeyManagerStandard returns a new KeyManagerStandard
func NewKeyManagerStandard(config Config) *KeyManagerStandard {
	log := config.MakeLogger("")
	return &KeyManagerStandard{config, log, log.CloneWithAddedDepth(1)}
}

// GetTLFCryptKeyForEncryption implements the KeyManager interface for
// KeyManagerStandard.
func (km *KeyManagerStandard) GetTLFCryptKeyForEncryption(ctx context.Context,
	kmd KeyMetadata) (tlfCryptKey kbfscrypto.TLFCryptKey, err error) {
	return km.getTLFCryptKeyUsingCurrentDevice(ctx, kmd,
		kmd.LatestKeyGeneration(), false)
}

// GetTLFCryptKeyForMDDecryption implements the KeyManager interface
// for KeyManagerStandard.
func (km *KeyManagerStandard) GetTLFCryptKeyForMDDecryption(
	ctx context.Context, kmdToDecrypt, kmdWithKeys KeyMetadata) (
	tlfCryptKey kbfscrypto.TLFCryptKey, err error) {
	return km.getTLFCryptKey(ctx, kmdWithKeys, kmdToDecrypt.LatestKeyGeneration(),
		getTLFCryptKeyAnyDevice|getTLFCryptKeyDoCache)
}

// GetTLFCryptKeyForBlockDecryption implements the KeyManager interface for
// KeyManagerStandard.
func (km *KeyManagerStandard) GetTLFCryptKeyForBlockDecryption(
	ctx context.Context, kmd KeyMetadata, blockPtr BlockPointer) (
	tlfCryptKey kbfscrypto.TLFCryptKey, err error) {
	return km.getTLFCryptKeyUsingCurrentDevice(ctx, kmd, blockPtr.KeyGen, true)
}

// GetTLFCryptKeyOfAllGenerations implements the KeyManager interface for
// KeyManagerStandard.
func (km *KeyManagerStandard) GetTLFCryptKeyOfAllGenerations(
	ctx context.Context, kmd KeyMetadata) (
	keys []kbfscrypto.TLFCryptKey, err error) {
	for g := FirstValidKeyGen; g <= kmd.LatestKeyGeneration(); g++ {
		var key kbfscrypto.TLFCryptKey
		key, err = km.getTLFCryptKeyUsingCurrentDevice(ctx, kmd, g, true)
		if err != nil {
			return keys, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (km *KeyManagerStandard) getTLFCryptKeyUsingCurrentDevice(
	ctx context.Context, kmd KeyMetadata, keyGen KeyGen, cache bool) (
	tlfCryptKey kbfscrypto.TLFCryptKey, err error) {
	flags := getTLFCryptKeyFlags(0)
	if cache {
		flags = getTLFCryptKeyDoCache
	}
	return km.getTLFCryptKey(ctx, kmd, keyGen, flags)
}

type getTLFCryptKeyFlags byte

const (
	getTLFCryptKeyAnyDevice getTLFCryptKeyFlags = 1 << iota
	getTLFCryptKeyDoCache
	getTLFCryptKeyPromptPaper
)

func (km *KeyManagerStandard) getTLFCryptKey(ctx context.Context,
	kmd KeyMetadata, keyGen KeyGen, flags getTLFCryptKeyFlags) (
	kbfscrypto.TLFCryptKey, error) {
	tlfID := kmd.TlfID()

	if tlfID.IsPublic() {
		return kbfscrypto.PublicTLFCryptKey, nil
	}

	if keyGen < FirstValidKeyGen {
		return kbfscrypto.TLFCryptKey{}, InvalidKeyGenerationError{tlfID, keyGen}
	}
	// Is this some key we don't know yet?  Shouldn't really ever happen,
	// since we must have seen the MD that led us to this block, which
	// should include all the latest keys.  Consider this a failsafe.
	if keyGen > kmd.LatestKeyGeneration() {
		return kbfscrypto.TLFCryptKey{}, NewKeyGenerationError{tlfID, keyGen}
	}

	// look in the cache first
	kcache := km.config.KeyCache()
	tlfCryptKey, err := kcache.GetTLFCryptKey(tlfID, keyGen)
	switch err := err.(type) {
	case nil:
		return tlfCryptKey, nil
	case KeyCacheMissError:
		break
	default:
		return kbfscrypto.TLFCryptKey{}, err
	}

	// Get the encrypted version of this secret key for this device
	kbpki := km.config.KBPKI()
	username, uid, err := kbpki.GetCurrentUserInfo(ctx)
	if err != nil {
		return kbfscrypto.TLFCryptKey{}, err
	}

	clientHalf, serverHalfID, cryptPublicKey, err :=
		km.getTLFCryptKeyParams(ctx, kmd, keyGen, uid, username, flags)

	var notPerDeviceEncrypted bool
	if _, notPerDeviceEncrypted = err.(TLFCryptKeyNotPerDeviceEncrypted); notPerDeviceEncrypted {
		// get the key we want using the current crypt key
		currKeyGen := kmd.LatestKeyGeneration()
		// look in the cache first
		latestKey, err := kcache.GetTLFCryptKey(tlfID, currKeyGen)
		switch err := err.(type) {
		case nil:
			break
		case KeyCacheMissError:
			// not cached, look up the params
			clientHalf, serverHalfID, cryptPublicKey, err2 :=
				km.getTLFCryptKeyParams(ctx, kmd, currKeyGen, uid, username, flags)
			if err2 != nil {
				return kbfscrypto.TLFCryptKey{}, err2
			}
			// unmask it
			latestKey, err2 = km.unmaskTLFCryptKey(ctx, serverHalfID, cryptPublicKey, clientHalf)
			if err2 != nil {
				return kbfscrypto.TLFCryptKey{}, err2
			}
			break
		default:
			return kbfscrypto.TLFCryptKey{}, err
		}
		// get the historic key we want
		tlfCryptKey, err =
			kmd.GetHistoricTLFCryptKey(km.config.Crypto(), keyGen, latestKey)
		if err != nil {
			return kbfscrypto.TLFCryptKey{}, err
		}
	} else if err != nil {
		return kbfscrypto.TLFCryptKey{}, err
	} else {
		// unmask it
		tlfCryptKey, err = km.unmaskTLFCryptKey(ctx, serverHalfID, cryptPublicKey, clientHalf)
		if err != nil {
			return kbfscrypto.TLFCryptKey{}, err
		}
	}

	if flags&getTLFCryptKeyDoCache != 0 {
		if err = kcache.PutTLFCryptKey(tlfID, keyGen, tlfCryptKey); err != nil {
			return kbfscrypto.TLFCryptKey{}, err
		}
	}

	return tlfCryptKey, nil
}

func (km *KeyManagerStandard) getTLFCryptKeyParams(
	ctx context.Context, kmd KeyMetadata,
	keyGen KeyGen, uid keybase1.UID, username libkb.NormalizedUsername,
	flags getTLFCryptKeyFlags) (
	clientHalf kbfscrypto.TLFCryptKeyClientHalf,
	serverHalfID TLFCryptKeyServerHalfID,
	cryptPublicKey kbfscrypto.CryptPublicKey, err error) {
	kbpki := km.config.KBPKI()
	crypto := km.config.Crypto()
	localMakeRekeyReadError := func() error {
		return makeRekeyReadError(ctx, km.config, kmd, uid, username)
	}

	if flags&getTLFCryptKeyAnyDevice != 0 {
		publicKeys, err := kbpki.GetCryptPublicKeys(ctx, uid)
		if err != nil {
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		}

		keys := make([]EncryptedTLFCryptKeyClientAndEphemeral, 0,
			len(publicKeys))
		serverHalfIDs := make([]TLFCryptKeyServerHalfID, 0, len(publicKeys))
		publicKeyLookup := make([]int, 0, len(publicKeys))

		for i, k := range publicKeys {
			ePublicKey, encryptedClientHalf, serverHalfID, found, err := kmd.GetTLFCryptKeyParams(keyGen, uid, k)
			if _, notPerDeviceEncrypted := err.(TLFCryptKeyNotPerDeviceEncrypted); notPerDeviceEncrypted {
				return kbfscrypto.TLFCryptKeyClientHalf{},
					TLFCryptKeyServerHalfID{},
					kbfscrypto.CryptPublicKey{}, err
			}
			if err != nil {
				km.log.CDebugf(ctx, "Got error for GetTLFCryptKeyParams(%d, %v, %v); skipping: %v", keyGen, uid, k, err)
				continue
			}
			if !found {
				km.log.CDebugf(ctx, "Could not find key info for(%d, %v, %v); skipping", keyGen, uid, k)
				continue
			}

			serverHalfIDs = append(serverHalfIDs, serverHalfID)
			keys = append(keys, EncryptedTLFCryptKeyClientAndEphemeral{
				PubKey:     k,
				ClientHalf: encryptedClientHalf,
				EPubKey:    ePublicKey,
			})
			publicKeyLookup = append(publicKeyLookup, i)
		}
		if len(keys) == 0 {
			err = localMakeRekeyReadError()
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		}
		var index int
		clientHalf, index, err = crypto.DecryptTLFCryptKeyClientHalfAny(ctx,
			keys, flags&getTLFCryptKeyPromptPaper != 0)
		_, isDecryptError := err.(libkb.DecryptionError)
		_, isNoKeyError := err.(libkb.NoSecretKeyError)
		if isDecryptError || isNoKeyError {
			km.log.CDebugf(ctx, "Got decryption error from service: %v", err)
			err = localMakeRekeyReadError()
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		} else if err != nil {
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		}
		serverHalfID = serverHalfIDs[index]
		cryptPublicKey = publicKeys[publicKeyLookup[index]]
	} else {
		cryptPublicKey, err = kbpki.GetCurrentCryptPublicKey(ctx)
		if err != nil {
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		}

		ePublicKey, encryptedClientHalf, foundServerHalfID, found, err :=
			kmd.GetTLFCryptKeyParams(keyGen, uid, cryptPublicKey)
		if _, notPerDeviceEncrypted := err.(TLFCryptKeyNotPerDeviceEncrypted); notPerDeviceEncrypted {
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		}
		if err != nil {
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		} else if !found {
			err = localMakeRekeyReadError()
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		}

		clientHalf, err = crypto.DecryptTLFCryptKeyClientHalf(
			ctx, ePublicKey, encryptedClientHalf)
		if err != nil {
			return kbfscrypto.TLFCryptKeyClientHalf{},
				TLFCryptKeyServerHalfID{},
				kbfscrypto.CryptPublicKey{}, err
		}

		serverHalfID = foundServerHalfID
	}
	return
}

func (km *KeyManagerStandard) unmaskTLFCryptKey(ctx context.Context, serverHalfID TLFCryptKeyServerHalfID,
	cryptPublicKey kbfscrypto.CryptPublicKey,
	clientHalf kbfscrypto.TLFCryptKeyClientHalf) (
	kbfscrypto.TLFCryptKey, error) {
	// get the server-side key-half, do the unmasking, possibly cache the result, return
	// TODO: can parallelize the get() with decryption
	serverHalf, err := km.config.KeyOps().GetTLFCryptKeyServerHalf(ctx, serverHalfID,
		cryptPublicKey)
	if err != nil {
		return kbfscrypto.TLFCryptKey{}, err
	}
	tlfCryptKey, err := km.config.Crypto().UnmaskTLFCryptKey(serverHalf, clientHalf)
	if err != nil {
		return kbfscrypto.TLFCryptKey{}, err
	}
	return tlfCryptKey, nil
}

func (km *KeyManagerStandard) updateKeyBundles(ctx context.Context,
	md *RootMetadata, wKeys, rKeys UserDevicePublicKeys,
	ePubKey kbfscrypto.TLFEphemeralPublicKey,
	ePrivKey kbfscrypto.TLFEphemeralPrivateKey,
	tlfCryptKeys []kbfscrypto.TLFCryptKey) error {

	serverHalves, err := md.updateKeyBundles(km.config.Crypto(),
		wKeys, rKeys, ePubKey, ePrivKey, tlfCryptKeys)
	if err != nil {
		return err
	}

	// Push new keys to the key server.
	//
	// TODO: Should accumulate the server halves across multiple
	// key generations and push them all at once right before the
	// MD push, although this only really matters for MDv2.
	for _, serverHalvesGen := range serverHalves {
		if err = km.config.KeyOps().
			PutTLFCryptKeyServerHalves(ctx, serverHalvesGen); err != nil {
			return err
		}
	}

	return nil
}

func (km *KeyManagerStandard) usersWithNewDevices(ctx context.Context,
	tlfID tlf.ID, keys, expectedKeys UserDevicePublicKeys) map[keybase1.UID]bool {
	users := make(map[keybase1.UID]bool)
	for u, expectedDeviceKeys := range expectedKeys {
		deviceKeys, ok := keys[u]
		if !ok {
			// Currently there probably shouldn't be any new users
			// in the handle, but don't error just in case we ever
			// want to support that in the future.
			km.log.CInfof(ctx, "Rekey %s: adding new user %s", tlfID, u)
			users[u] = true
			continue
		}
		for key := range expectedDeviceKeys {
			km.log.CDebugf(ctx, "Checking key %v", key)
			if !deviceKeys[key] {
				km.log.CInfof(ctx, "Rekey %s: adding new device %s for user %s",
					tlfID, key, u)
				users[u] = true
				break
			}
		}
	}
	return users
}

func (km *KeyManagerStandard) usersWithRemovedDevices(ctx context.Context,
	tlfID tlf.ID, keys, expectedKeys UserDevicePublicKeys) map[keybase1.UID]bool {
	users := make(map[keybase1.UID]bool)
	for u, deviceKeys := range keys {
		expectedDeviceKeys, ok := expectedKeys[u]
		if !ok {
			// Currently there probably shouldn't be any users removed
			// from the handle, but don't error just in case we ever
			// want to support that in the future.
			km.log.CInfof(ctx, "Rekey %s: removing user %s", tlfID, u)
			users[u] = true
			continue
		}
		for key := range deviceKeys {
			// Make sure every kid has an expected key
			if !expectedDeviceKeys[key] {
				km.log.CInfof(ctx, "Rekey %s: removing device %s for user %s",
					tlfID, key, u)
				users[u] = true
				break
			}
		}
	}
	return users
}

func (km *KeyManagerStandard) identifyUIDSets(ctx context.Context,
	tlfID tlf.ID, writersToIdentify map[keybase1.UID]bool,
	readersToIdentify map[keybase1.UID]bool) error {
	uids := make([]keybase1.UID, 0, len(writersToIdentify)+len(readersToIdentify))
	for u := range writersToIdentify {
		uids = append(uids, u)
	}
	for u := range readersToIdentify {
		uids = append(uids, u)
	}
	kbpki := km.config.KBPKI()
	return identifyUserList(ctx, kbpki, kbpki, uids, tlfID.IsPublic())
}

func (km *KeyManagerStandard) generateKeyMapForUsers(
	ctx context.Context, users []keybase1.UID) (
	UserDevicePublicKeys, error) {
	keyMap := make(UserDevicePublicKeys)

	// TODO: parallelize
	for _, w := range users {
		// HACK: clear cache
		km.config.KeybaseService().FlushUserFromLocalCache(ctx, w)
		publicKeys, err := km.config.KBPKI().GetCryptPublicKeys(ctx, w)
		if err != nil {
			return nil, err
		}
		keyMap[w] = make(DevicePublicKeys)
		for _, key := range publicKeys {
			keyMap[w][key] = true
		}
	}

	return keyMap, nil
}

// Rekey implements the KeyManager interface for KeyManagerStandard.
//
// TODO: Make this less terrible. See KBFS-1799.
func (km *KeyManagerStandard) Rekey(ctx context.Context, md *RootMetadata, promptPaper bool) (
	mdChanged bool, cryptKey *kbfscrypto.TLFCryptKey, err error) {
	km.log.CDebugf(ctx, "Rekey %s (prompt for paper key: %t)",
		md.TlfID(), promptPaper)
	defer func() { km.deferLog.CDebugf(ctx, "Rekey %s done: %#v", md.TlfID(), err) }()

	currKeyGen := md.LatestKeyGeneration()
	if md.TlfID().IsPublic() != (currKeyGen == PublicKeyGen) {
		return false, nil, fmt.Errorf(
			"ID %v has isPublic=%t but currKeyGen is %d (isPublic=%t)",
			md.TlfID(), md.TlfID().IsPublic(), currKeyGen, currKeyGen == PublicKeyGen)
	}

	if promptPaper && md.TlfID().IsPublic() {
		return false, nil, fmt.Errorf("promptPaper set for public TLF %v", md.TlfID())
	}

	handle := md.GetTlfHandle()

	username, uid, err := km.config.KBPKI().GetCurrentUserInfo(ctx)
	if err != nil {
		return false, nil, err
	}

	resolvedHandle, err := handle.ResolveAgain(ctx, km.config.KBPKI())
	if err != nil {
		return false, nil, err
	}

	isWriter := resolvedHandle.IsWriter(uid)
	if !md.TlfID().IsPublic() && !isWriter {
		// If I was already a reader, there's nothing more to do
		if handle.IsReader(uid) {
			resolvedHandle = handle
			km.log.CDebugf(ctx, "Local user is not a writer, and was "+
				"already a reader; reverting back to the original handle")
		} else {
			// Only allow yourself to change
			resolvedHandle, err =
				handle.ResolveAgainForUser(ctx, km.config.KBPKI(), uid)
			if err != nil {
				return false, nil, err
			}
		}
	}

	eq, err := handle.Equals(km.config.Codec(), *resolvedHandle)
	if err != nil {
		return false, nil, err
	}
	handleChanged := !eq
	if handleChanged {
		km.log.CDebugf(ctx, "handle for %s resolved to %s",
			handle.GetCanonicalPath(),
			resolvedHandle.GetCanonicalPath())

		// Check with the server to see if the handle became a conflict.
		latestHandle, err := km.config.MDOps().GetLatestHandleForTLF(ctx, md.TlfID())
		if latestHandle.ConflictInfo != nil {
			km.log.CDebugf(ctx, "handle for %s is conflicted",
				handle.GetCanonicalPath())
		}
		resolvedHandle, err = resolvedHandle.WithUpdatedConflictInfo(
			km.config.Codec(), latestHandle.ConflictInfo)
		if err != nil {
			return false, nil, err
		}
	}

	// For a public TLF there's no rekeying to be done, but we
	// should still update the writer list.
	if md.TlfID().IsPublic() {
		if !handleChanged {
			km.log.CDebugf(ctx,
				"Skipping rekeying %s (public): handle hasn't changed",
				md.TlfID())
			return false, nil, nil
		}
		return true, nil, md.updateFromTlfHandle(resolvedHandle)
	}

	// Decide whether we have a new device and/or a revoked device, or neither.
	// Look up all the device public keys for all writers and readers first.

	incKeyGen := currKeyGen < FirstValidKeyGen

	if !isWriter && incKeyGen {
		// Readers cannot create the first key generation
		return false, nil, NewReadAccessError(resolvedHandle, username, resolvedHandle.GetCanonicalPath())
	}

	// All writer keys in the desired keyset
	wKeys, err := km.generateKeyMapForUsers(ctx, resolvedHandle.ResolvedWriters())
	if err != nil {
		return false, nil, err
	}
	// All reader keys in the desired keyset
	rKeys, err := km.generateKeyMapForUsers(ctx, resolvedHandle.ResolvedReaders())
	if err != nil {
		return false, nil, err
	}

	addNewReaderDevice := false
	addNewWriterDevice := false
	var newReaderUsers map[keybase1.UID]bool
	var newWriterUsers map[keybase1.UID]bool
	var readersToPromote map[keybase1.UID]bool

	// Figure out if we need to add or remove any keys.
	// If we're already incrementing the key generation then we don't need to
	// figure out the key delta.
	addNewReaderDeviceForSelf := false
	if !incKeyGen {
		// See if there is at least one new device in relation to the
		// current key bundle
		writers, readers, err := md.getUserDevicePublicKeys()
		if err != nil {
			return false, nil, err
		}

		newWriterUsers = km.usersWithNewDevices(ctx, md.TlfID(), writers, wKeys)
		newReaderUsers = km.usersWithNewDevices(ctx, md.TlfID(), readers, rKeys)
		addNewWriterDevice = len(newWriterUsers) > 0
		addNewReaderDevice = len(newReaderUsers) > 0

		wRemoved := km.usersWithRemovedDevices(ctx, md.TlfID(), writers, wKeys)
		rRemoved := km.usersWithRemovedDevices(ctx, md.TlfID(), readers, rKeys)

		readersToPromote = make(map[keybase1.UID]bool, len(rRemoved))

		// Before we add the removed devices, check if we are adding a
		// new reader device for ourselves.
		_, addNewReaderDeviceForSelf = newReaderUsers[uid]

		for u := range rRemoved {
			// FIXME (potential): this could cause a reader to attempt to rekey
			// in the case of a revocation for the currently logged-in user. I
			// _think_ incKeyGen above protects against this, but I'm not
			// confident.
			newReaderUsers[u] = true
			// Track which readers have been promoted. This must happen before
			// the following line adds all the removed writers to the writer
			// set
			if newWriterUsers[u] {
				readersToPromote[u] = true
			}
		}
		for u := range wRemoved {
			newWriterUsers[u] = true
		}

		incKeyGen = len(wRemoved) > 0 || (len(rRemoved) > len(readersToPromote))

		if err := km.identifyUIDSets(ctx, md.TlfID(), newWriterUsers, newReaderUsers); err != nil {
			return false, nil, err
		}
	}

	if !addNewReaderDevice && !addNewWriterDevice && !incKeyGen &&
		!handleChanged {
		km.log.CDebugf(ctx,
			"Skipping rekeying %s (private): no new or removed devices, no new keygen, and handle hasn't changed",
			md.TlfID())
		return false, nil, nil
	}

	if !isWriter {
		// This shouldn't happen; see the code above where we
		// either use the original handle or resolve only the
		// current UID.
		if len(readersToPromote) != 0 {
			return false, nil, errors.New(
				"promoted readers unexpectedly non-empty")
		}

		if _, userHasNewKeys := newReaderUsers[uid]; userHasNewKeys {
			// Only rekey the logged-in reader.
			rKeys = UserDevicePublicKeys{
				uid: rKeys[uid],
			}
			wKeys = nil
			delete(newReaderUsers, uid)
		} else {
			// No new reader device for our user, so the reader can't do
			// anything
			return false, nil, RekeyIncompleteError{}
		}
	}

	// If promotedReader is non-empty, then isWriter is true (see
	// check above).
	err = md.promoteReaders(readersToPromote)
	if err != nil {
		return false, nil, err
	}

	// Generate ephemeral keys to be used by addNewDevice,
	// incKeygen, or both. ePrivKey will be discarded at the end
	// of the function.
	ePubKey, ePrivKey, err :=
		km.config.Crypto().MakeRandomTLFEphemeralKeys()

	// Note: For MDv3, if incKeyGen is true, then all the
	// manipulations below aren't needed, since they'll just be
	// replaced by the new key generation. However, do them
	// anyway, as they may have some side effects, e.g. removing
	// server key halves.

	// If there's at least one new device, add that device to every key bundle.
	if addNewReaderDevice || addNewWriterDevice {
		start, end := md.KeyGenerationsToUpdate()
		if start >= end {
			return false, nil, errors.New(
				"Unexpected empty range for key generations to update")
		}
		cryptKeys := make([]kbfscrypto.TLFCryptKey, end-start)
		flags := getTLFCryptKeyAnyDevice
		if promptPaper {
			flags |= getTLFCryptKeyPromptPaper
		}
		for keyGen := start; keyGen < end; keyGen++ {
			currTlfCryptKey, err := km.getTLFCryptKey(
				ctx, md.ReadOnly(), keyGen, flags)
			if err != nil {
				return false, nil, err
			}
			cryptKeys[keyGen-start] = currTlfCryptKey
		}
		err = km.updateKeyBundles(ctx, md, wKeys,
			rKeys, ePubKey, ePrivKey, cryptKeys)
		if err != nil {
			return false, nil, err
		}
	}

	// Make sure the private MD is decrypted if it wasn't already.  We
	// have to do this here, before adding a new key generation, since
	// decryptMDPrivateData assumes that the MD is always encrypted
	// using the latest key gen.
	if !md.IsReadable() && len(md.GetSerializedPrivateMetadata()) > 0 {
		pmd, err := decryptMDPrivateData(
			ctx, km.config.Codec(), km.config.Crypto(),
			km.config.BlockCache(), km.config.BlockOps(),
			km, uid, md.GetSerializedPrivateMetadata(), md, md, km.log)
		if err != nil {
			return false, nil, err
		}
		md.data = pmd
	}

	defer func() {
		// On our way back out, update the md with the
		// resolved handle if at least part of a rekey was
		// performed.  Also, if we return true for mdChanged
		// with a nil error or RekeyIncompleteError{}, we must
		// call md.finalizeRekey() first.

		_, isRekeyIncomplete := err.(RekeyIncompleteError)
		if err == nil || isRekeyIncomplete {
			updateErr := md.updateFromTlfHandle(resolvedHandle)
			if updateErr != nil {
				mdChanged = false
				cryptKey = nil
				err = updateErr
			}

			if mdChanged {
				finalizeErr := md.finalizeRekey(
					km.config.Crypto())
				if finalizeErr != nil {
					mdChanged = false
					cryptKey = nil
					err = finalizeErr
				}
			}
		}
	}()

	if !isWriter {
		if len(newReaderUsers) > 0 || addNewWriterDevice || incKeyGen {
			// If we're a reader but we haven't completed all the work, return
			// RekeyIncompleteError.
			return addNewReaderDeviceForSelf, nil, RekeyIncompleteError{}
		}
		// Otherwise, there's nothing left to do!
		return true, nil, nil
	} else if !incKeyGen {
		// we're done!
		return true, nil, nil
	}

	// Send rekey start notification once we're sure that this device
	// can perform the rekey.
	//
	// TODO: Shouldn't this happen earlier?
	km.config.Reporter().Notify(ctx, rekeyNotification(ctx, km.config, resolvedHandle,
		false))

	// Delete server-side key halves for any revoked devices, if
	// there are any previous key generations. Do this before
	// adding a new key generation, as MDv3 only keeps track of
	// the latest key generation.
	//
	// TODO: Add test coverage for this.
	if currKeyGen >= FirstValidKeyGen {
		allRemovalInfo, err := md.revokeRemovedDevices(wKeys, rKeys)
		if err != nil {
			return false, nil, err
		}

		if len(allRemovalInfo) == 0 {
			return false, nil, errors.New(
				"Didn't revoke any devices, but indicated incrementing the key generation")
		}

		kops := km.config.KeyOps()
		for uid, userRemovalInfo := range allRemovalInfo {
			if userRemovalInfo.userRemoved {
				km.log.CInfof(ctx, "Rekey %s: removed user %s entirely",
					md.TlfID(), uid)
			}
			for key, serverHalfIDs := range userRemovalInfo.deviceServerHalfIDs {
				km.log.CInfof(ctx, "Rekey %s: removing %d server key halves "+
					" for device %s of user %s", md.TlfID(),
					len(serverHalfIDs), key, uid)
				for _, serverHalfID := range serverHalfIDs {
					err := kops.DeleteTLFCryptKeyServerHalf(
						ctx, uid, key.KID(), serverHalfID)
					if err != nil {
						return false, nil, err
					}
				}
			}
		}
	}

	pubKey, privKey, tlfCryptKey, err :=
		km.config.Crypto().MakeRandomTLFKeys()
	if err != nil {
		return false, nil, err
	}

	// Get the current TLF crypt key if needed. It's
	// symmetrically encrypted and appended to a list for MDv3
	// metadata.
	var currTLFCryptKey kbfscrypto.TLFCryptKey
	if md.StoresHistoricTLFCryptKeys() && currKeyGen >= FirstValidKeyGen {
		flags := getTLFCryptKeyAnyDevice
		if promptPaper {
			flags |= getTLFCryptKeyPromptPaper
		}
		var err error
		currTLFCryptKey, err = km.getTLFCryptKey(
			ctx, md.ReadOnly(), currKeyGen, flags)
		if err != nil {
			return false, nil, err
		}
	}
	serverHalves, err := md.AddKeyGeneration(km.config.Codec(),
		km.config.Crypto(), wKeys, rKeys, ePubKey, ePrivKey,
		pubKey, privKey, currTLFCryptKey, tlfCryptKey)
	if err != nil {
		return false, nil, err
	}

	err = km.config.KeyOps().PutTLFCryptKeyServerHalves(ctx, serverHalves)
	if err != nil {
		return false, nil, err
	}

	return true, &tlfCryptKey, nil
}
