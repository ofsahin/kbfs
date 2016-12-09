// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/keybase/client/go/logger"
	"github.com/keybase/client/go/protocol/keybase1"
	"github.com/keybase/kbfs/kbfscodec"
	"github.com/keybase/kbfs/tlf"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func setupFileDataTest(t *testing.T, maxBlockSize int64,
	maxPtrsPerBlock int) (*fileData, BlockCache, DirtyBlockCache, *dirtyFile) {
	// Make a fake file.
	ptr := BlockPointer{
		ID:      fakeBlockID(42),
		KeyGen:  1,
		DataVer: 1,
	}
	id := tlf.FakeID(1, false)
	file := path{FolderBranch{Tlf: id}, []pathNode{{ptr, "file"}}}
	uid := keybase1.MakeTestUID(1)
	crypto := MakeCryptoCommon(kbfscodec.NewMsgpack())
	bsplit := &BlockSplitterSimple{maxBlockSize, maxPtrsPerBlock, 10}
	kmd := emptyKeyMetadata{id, 1}

	cleanCache := NewBlockCacheStandard(1<<10, 1<<20)
	dirtyBcache := simpleDirtyBlockCacheStandard()
	getter := func(_ context.Context, _ KeyMetadata, ptr BlockPointer,
		_ path, _ blockReqType) (*FileBlock, bool, error) {
		isDirty := true
		block, err := dirtyBcache.Get(id, ptr, MasterBranch)
		if err != nil {
			// Check the clean cache.
			block, err = cleanCache.Get(ptr)
			if err != nil {
				return nil, false, err
			}
			isDirty = false
		}
		fblock, ok := block.(*FileBlock)
		if !ok {
			return nil, false,
				fmt.Errorf("Block for %s is not a file block", ptr)
		}
		return fblock, isDirty, nil
	}
	cacher := func(ptr BlockPointer, block Block) error {
		return dirtyBcache.Put(id, ptr, MasterBranch, block)
	}

	fd := newFileData(
		file, uid, crypto, bsplit, kmd, getter, cacher, logger.NewTestLogger(t))
	df := newDirtyFile(file, dirtyBcache)
	return fd, cleanCache, dirtyBcache, df
}

type testFileDataLevel struct {
	dirty    bool
	children []testFileDataLevel
	off      int64
	size     int
}

type testFileDataHole struct {
	start int64
	end   int64
}

func testFileDataLevelFromData(maxBlockSize int64, maxPtrsPerBlock int,
	existingLevels int, fullDataLen int64, holes []testFileDataHole,
	startWrite, endWrite int64) testFileDataLevel {
	numAtLevel := int(math.Ceil(float64(fullDataLen) / float64(maxBlockSize)))
	var prevChildren []testFileDataLevel
	newLevels := 0
	for len(prevChildren) != 1 {
		prevChildIndex := 0
		var level []testFileDataLevel
		for i := 0; i < numAtLevel; i++ {
			// Split the previous children up (if any) into
			// maxPtrsPerBlock chunks.
			var children []testFileDataLevel
			off := int64(0)
			dirty := false
			size := 0
			if len(prevChildren) > 0 {
				newIndex := prevChildIndex + maxPtrsPerBlock
				if newIndex > len(prevChildren) {
					newIndex = len(prevChildren)
				}
				off = prevChildren[prevChildIndex].off
				children = prevChildren[prevChildIndex:newIndex]
				prevChildIndex = newIndex
				for _, child := range children {
					if child.dirty {
						dirty = true
						break
					}
				}
			} else {
				off = int64(i) * maxBlockSize
				dirty = off < endWrite && startWrite < off+maxBlockSize
				size = int(maxBlockSize)
				if off+maxBlockSize > fullDataLen {
					size = int(fullDataLen - off)
				}
			}
			newChild := testFileDataLevel{dirty, children, off, size}
			level = append(level, newChild)
		}
		prevChildren = level
		numAtLevel =
			int(math.Ceil(float64(len(level)) / float64(maxPtrsPerBlock)))
		newLevels++
	}

	// If we added new levels, we can expect the old topmost block to
	// be dirty, since we have to upload it with a new block ID.
	currNode := &(prevChildren[0])
	for i := 0; i <= newLevels-existingLevels; i++ {
		currNode.dirty = true
		if len(currNode.children) == 0 {
			break
		}
		currNode = &(currNode.children[0])
	}

	return prevChildren[0]
}

func (tfdl testFileDataLevel) check(t *testing.T, fd *fileData,
	ptr BlockPointer, off int64, dirtyBcache DirtyBlockCache) (
	dirtyPtrs map[BlockPointer]bool) {
	dirtyPtrs = make(map[BlockPointer]bool)
	levelString := fmt.Sprintf("ptr=%s, off=%d", ptr, off)

	require.Equal(t, tfdl.off, off, levelString)
	if tfdl.dirty {
		dirtyPtrs[ptr] = true
		require.True(
			t, dirtyBcache.IsDirty(fd.file.Tlf, ptr, MasterBranch), levelString)
	}

	fblock, isDirty, err := fd.getter(nil, nil, ptr, path{}, blockRead)
	require.NoError(t, err, levelString)
	require.Equal(t, tfdl.dirty, isDirty, levelString)
	require.NotNil(t, fblock, levelString)

	// We expect this to be a leaf block.
	if len(tfdl.children) == 0 {
		require.False(t, fblock.IsInd, levelString)
		require.Len(t, fblock.IPtrs, 0, levelString)
		require.Len(t, fblock.Contents, tfdl.size, levelString)
		return dirtyPtrs
	}

	// Otherwise it's indirect, so check all the children.
	require.True(t, fblock.IsInd, levelString)
	require.Len(t, fblock.IPtrs, len(tfdl.children), levelString)
	require.Len(t, fblock.Contents, 0, levelString)
	for i, iptr := range fblock.IPtrs {
		childDirtyPtrs := tfdl.children[i].check(
			t, fd, iptr.BlockPointer, iptr.Off, dirtyBcache)
		for ptr := range childDirtyPtrs {
			dirtyPtrs[ptr] = true
		}
	}
	return dirtyPtrs
}

func testFileDataCheckWrite(t *testing.T, fd *fileData,
	dirtyBcache DirtyBlockCache, df *dirtyFile, data []byte, off int64,
	topBlock *FileBlock, oldDe DirEntry, expectedSize uint64,
	expectedUnrefs []BlockInfo, expectedDirtiedBytes int64,
	expectedBytesExtended int64, expectedTopLevel testFileDataLevel) {
	// Do the write.
	ctx := context.Background()
	newDe, dirtyPtrs, unrefs, newlyDirtiedChildBytes, bytesExtended, err :=
		fd.write(ctx, data, off, topBlock, oldDe, df)
	require.NoError(t, err)

	// Check the basics.
	require.Equal(t, expectedSize, newDe.Size)
	require.Equal(t, expectedDirtiedBytes, newlyDirtiedChildBytes)
	require.Equal(t, expectedBytesExtended, bytesExtended)

	// Go through each expected level and make sure we have the right
	// set of dirty pointers and children.
	expectedDirtyPtrs := expectedTopLevel.check(
		t, fd, fd.file.tailPointer(), 0, dirtyBcache)
	dirtyPtrsMap := make(map[BlockPointer]bool)
	for _, ptr := range dirtyPtrs {
		dirtyPtrsMap[ptr] = true
	}
	require.True(t, reflect.DeepEqual(expectedDirtyPtrs, dirtyPtrsMap))

	require.Len(t, unrefs, len(expectedUnrefs))
}

func testFileDataWriteExtendEmptyFile(t *testing.T, maxBlockSize int64,
	maxPtrsPerBlock int, fullDataLen int64) {
	fd, _, dirtyBcache, df := setupFileDataTest(
		t, maxBlockSize, maxPtrsPerBlock)
	topBlock := NewFileBlock().(*FileBlock)
	de := DirEntry{}
	data := make([]byte, fullDataLen)
	for i := 0; i < int(fullDataLen); i++ {
		data[i] = byte(i)
	}
	expectedTopLevel := testFileDataLevelFromData(
		maxBlockSize, maxPtrsPerBlock, 0, fullDataLen, 0, fullDataLen)

	testFileDataCheckWrite(
		t, fd, dirtyBcache, df, data, 0, topBlock, de, uint64(fullDataLen),
		nil, fullDataLen, fullDataLen, expectedTopLevel)

	// Make sure we can read back the complete data.
	gotData := make([]byte, fullDataLen)
	nRead, err := fd.read(context.Background(), gotData, 0)
	require.NoError(t, err)
	require.Equal(t, nRead, fullDataLen)
	require.True(t, bytes.Equal(data, gotData))
}

func testFileDataWriteNewLevel(t *testing.T, levels float64) {
	capacity := math.Pow(2, levels)
	halfCapacity := capacity/2 + 1
	if levels == 1 {
		halfCapacity = capacity - 1
	}
	// Fills half the leaf level.
	testFileDataWriteExtendEmptyFile(t, 2, 2, int64(halfCapacity))
	// Fills whole leaf level.
	testFileDataWriteExtendEmptyFile(t, 2, 2, int64(capacity))

}

func TestFileDataWriteNewOneLevel(t *testing.T) {
	testFileDataWriteNewLevel(t, 1)
}

func TestFileDataWriteNewTwoLevels(t *testing.T) {
	testFileDataWriteNewLevel(t, 2)
}

func TestFileDataWriteNewThreeLevels(t *testing.T) {
	testFileDataWriteNewLevel(t, 3)
}

// Test when new file data all fits into ten levels.
func TestFileDataWriteNewTenLevels(t *testing.T) {
	testFileDataWriteNewLevel(t, 10)
}

func testFileDataLevelExistingBlocks(t *testing.T, fd *fileData,
	maxBlockSize int64, maxPtrsPerBlock int, existingData []byte,
	cleanBcache BlockCache) (*FileBlock, int) {
	numAtLevel := int(
		math.Ceil(float64(len(existingData)) / float64(maxBlockSize)))
	var prevChildren []*FileBlock
	numLevels := 0
	crypto := MakeCryptoCommon(kbfscodec.NewMsgpack())
	for len(prevChildren) != 1 {
		prevChildIndex := 0
		var level []*FileBlock
		for i := 0; i < numAtLevel; i++ {
			// Split the previous children up (if any) into maxNumPtr
			// chunks.
			var children []*FileBlock
			fblock := NewFileBlock().(*FileBlock)
			if len(prevChildren) > 0 {
				newIndex := prevChildIndex + maxPtrsPerBlock
				if newIndex > len(prevChildren) {
					newIndex = len(prevChildren)
				}
				children = prevChildren[prevChildIndex:newIndex]
				prevChildIndex = newIndex
				fblock.IsInd = true
				for j, child := range children {
					id, err := crypto.MakeTemporaryBlockID()
					require.NoError(t, err)
					ptr := BlockPointer{
						ID:      id,
						KeyGen:  1,
						DataVer: 1,
					}
					var off int64
					if child.IsInd {
						off = child.IPtrs[0].Off
					} else {
						off = int64(i)*maxBlockSize*int64(maxPtrsPerBlock) +
							int64(j)*maxBlockSize
					}

					fblock.IPtrs = append(fblock.IPtrs, IndirectFilePtr{
						BlockInfo: BlockInfo{ptr, 0},
						Off:       off,
					})
					cleanBcache.Put(ptr, fd.file.Tlf, child, TransientEntry)
				}
			} else {
				off := int64(i) * maxBlockSize
				endOff := off + maxBlockSize
				if endOff > int64(len(existingData)) {
					endOff = int64(len(existingData))
				}
				fblock.Contents = existingData[off:endOff]
			}
			level = append(level, fblock)
		}
		prevChildren = level
		numAtLevel =
			int(math.Ceil(float64(len(level)) / float64(maxPtrsPerBlock)))
		numLevels++
	}
	return prevChildren[0], numLevels
}

func testFileDataWriteExtendExistingFile(t *testing.T, maxBlockSize int64,
	maxPtrsPerBlock int, existingLen int64, fullDataLen int64) {
	fd, cleanBcache, dirtyBcache, df := setupFileDataTest(
		t, maxBlockSize, maxPtrsPerBlock)
	data := make([]byte, fullDataLen)
	for i := 0; i < int(fullDataLen); i++ {
		data[i] = byte(i)
	}
	topBlock, levels := testFileDataLevelExistingBlocks(
		t, fd, maxBlockSize, maxPtrsPerBlock, data[:existingLen], cleanBcache)
	de := DirEntry{
		EntryInfo: EntryInfo{
			Size: uint64(existingLen),
		},
	}
	expectedTopLevel := testFileDataLevelFromData(
		maxBlockSize, maxPtrsPerBlock, levels, fullDataLen, existingLen,
		fullDataLen)

	extendedBytes := fullDataLen - existingLen
	// Round up to find out the number of dirty bytes.
	remainder := extendedBytes % maxBlockSize
	dirtiedBytes := extendedBytes
	if remainder > 0 {
		dirtiedBytes += (maxBlockSize - remainder)
	}
	// Add a block's worth of dirty bytes if we're extending past the
	// first full level, because the original block still gets dirtied
	// because it needs to be inserted under a new ID.
	if existingLen == maxBlockSize {
		dirtiedBytes += maxBlockSize
	}
	testFileDataCheckWrite(
		t, fd, dirtyBcache, df, data[existingLen:], existingLen,
		topBlock, de, uint64(fullDataLen),
		nil, dirtiedBytes, extendedBytes, expectedTopLevel)

	// Make sure we can read back the complete data.
	gotData := make([]byte, fullDataLen)
	nRead, err := fd.read(context.Background(), gotData, 0)
	require.NoError(t, err)
	require.Equal(t, nRead, fullDataLen)
	require.True(t, bytes.Equal(data, gotData))
}

func testFileDataExtendExistingLevels(t *testing.T, levels float64) {
	capacity := math.Pow(2, levels)
	halfCapacity := capacity / 2
	// Starts with one lower level and adds a level.
	testFileDataWriteExtendExistingFile(
		t, 2, 2, int64(halfCapacity), int64(capacity))
}

func TestFileDataExtendExistingOneLevel(t *testing.T) {
	testFileDataExtendExistingLevels(t, 1)
}

func TestFileDataExtendExistingTwoLevels(t *testing.T) {
	testFileDataExtendExistingLevels(t, 2)
}

func TestFileDataExtendExistingThreeLevels(t *testing.T) {
	testFileDataExtendExistingLevels(t, 3)
}

func TestFileDataExtendExistingTenLevels(t *testing.T) {
	testFileDataExtendExistingLevels(t, 10)
}
