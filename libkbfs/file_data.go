// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"fmt"
	"time"

	"github.com/keybase/client/go/logger"
	"github.com/keybase/client/go/protocol/keybase1"
	"github.com/keybase/kbfs/kbfscodec"
	"github.com/keybase/kbfs/tlf"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

// fileBlockGetter is a function that gets a block suitable for
// reading or writing, and also returns whether the block was already
// dirty.  It may be called from new goroutines, and must handle any
// required locks accordingly.
type fileBlockGetter func(context.Context, KeyMetadata, BlockPointer,
	path, blockReqType) (fblock *FileBlock, wasDirty bool, err error)

// dirtyBlockCacher writes dirty blocks to a cache.
type dirtyBlockCacher func(ptr BlockPointer, block Block) error

// fileData is a helper struct for accessing and manipulating data
// within a file.  It's meant for use within a single scope, not for
// long-term storage.  The caller must ensure goroutine-safety.
type fileData struct {
	file   path
	uid    keybase1.UID
	crypto cryptoPure
	kmd    KeyMetadata
	bsplit BlockSplitter
	getter fileBlockGetter
	cacher dirtyBlockCacher
	log    logger.Logger
}

func newFileData(file path, uid keybase1.UID, crypto cryptoPure,
	bsplit BlockSplitter, kmd KeyMetadata, getter fileBlockGetter,
	cacher dirtyBlockCacher, log logger.Logger) *fileData {
	return &fileData{
		file:   file,
		uid:    uid,
		crypto: crypto,
		bsplit: bsplit,
		kmd:    kmd,
		getter: getter,
		cacher: cacher,
		log:    log,
	}
}

// parentBlockAndChildIndex is a node on a path down the tree to a
// particular leaf node.  `pblock` is an indirect block corresponding
// to one of that leaf node's parents, and `childIndex` is an index
// into `pblock.IPtrs` to the next node along the path.
type parentBlockAndChildIndex struct {
	pblock     *FileBlock
	childIndex int
}

func (fd *fileData) getFileBlockAtOffset(ctx context.Context,
	topBlock *FileBlock, off int64, rtype blockReqType) (
	ptr BlockPointer, parentBlocks []parentBlockAndChildIndex,
	block *FileBlock, nextBlockStartOff, startOff int64,
	wasDirty bool, err error) {
	// find the block matching the offset, if it exists
	ptr = fd.file.tailPointer()
	block = topBlock
	nextBlockStartOff = -1
	startOff = 0
	// search until it's not an indirect block
	for block.IsInd {
		nextIndex := len(block.IPtrs) - 1
		for i, iptr := range block.IPtrs {
			if iptr.Off == off {
				// small optimization to avoid iterating past the right ptr
				nextIndex = i
				break
			} else if iptr.Off > off {
				// i can never be 0, because the first ptr always has
				// an offset at the beginning of the range
				nextIndex = i - 1
				break
			}
		}
		nextPtr := block.IPtrs[nextIndex]
		parentBlocks = append(parentBlocks,
			parentBlockAndChildIndex{block, nextIndex})
		startOff = nextPtr.Off
		// there is more to read if we ever took a path through a
		// ptr that wasn't the final ptr in its respective list
		if nextIndex != len(block.IPtrs)-1 {
			nextBlockStartOff = block.IPtrs[nextIndex+1].Off
		}
		ptr = nextPtr.BlockPointer
		block, wasDirty, err = fd.getter(ctx, fd.kmd, ptr, fd.file, rtype)
		if err != nil {
			return zeroPtr, nil, nil, 0, 0, false, err
		}
	}

	return ptr, parentBlocks, block, nextBlockStartOff, startOff, wasDirty, nil
}

// getLeafBlocksForOffsetRange fetches all the leaf ("direct") blocks
// that encompass the given offset range (half-inclusive) in the file.
// If `endOff` is -1, it returns blocks until reaching the end of the
// file.  Note the range could be made up of holes, meaning that the
// last byte of a direct block doesn't immediately precede the first
// byte of the subsequent block.  If `prefixOk` is true, the function
// will ignore context deadline errors and return whatever prefix of
// the data it could fetch within the deadine.  Return params:
//
//   * pathsFromRoot is a slice, ordered by file offset, of paths from
//     the root to each block that makes up the range.  If the path is
//     empty, it indicates that pblock is a direct block and has no
//     children.
//   * blocks: a map from block pointer to a data-containing leaf node
//     in the given range of offsets.
//   * nextBlockOff is the offset of the block that follows the last
//     block given in `pathsFromRoot`.  If `pathsFromRoot` contains
//     the last block among the children, nextBlockOff is -1.
func (fd *fileData) getLeafBlocksForOffsetRange(ctx context.Context,
	ptr BlockPointer, pblock *FileBlock, startOff, endOff int64,
	prefixOk bool) (pathsFromRoot [][]parentBlockAndChildIndex,
	blocks map[BlockPointer]*FileBlock, nextBlockOffset int64, err error) {
	if !pblock.IsInd {
		// Return a single empty path and a child map with only this
		// block in it, under the assumption that the caller already
		// checked the range for this block.
		return [][]parentBlockAndChildIndex{nil},
			map[BlockPointer]*FileBlock{ptr: pblock}, -1, nil
	}

	type resp struct {
		pathsFromRoot   [][]parentBlockAndChildIndex
		blocks          map[BlockPointer]*FileBlock
		nextBlockOffset int64
	}

	// Search all of the in-range child blocks, and their child
	// blocks, etc, in parallel.
	respChans := make([]<-chan resp, 0, len(pblock.IPtrs))
	eg, groupCtx := errgroup.WithContext(ctx)
	nextBlockOffsetThisLevel := int64(-1)
	for i, iptr := range pblock.IPtrs {
		// Some byte of this block is included in the left side of the
		// range if `startOff` is less than the largest byte offset in
		// the block.
		inRangeLeft := true
		if i < len(pblock.IPtrs)-1 {
			inRangeLeft = startOff < pblock.IPtrs[i+1].Off
		}
		if !inRangeLeft {
			continue
		}
		// Some byte of this block is included in the right side of
		// the range if `endOff` is bigger than the smallest byte
		// offset in the block (or if we're explicitly reading all the
		// data to the end).
		inRangeRight := endOff == -1 || endOff > iptr.Off
		if !inRangeRight {
			// This block is the first one past the offset range
			// amount the children.
			nextBlockOffsetThisLevel = iptr.Off
			break
		}

		ptr := iptr.BlockPointer
		childIndex := i
		respCh := make(chan resp, 1)
		respChans = append(respChans, respCh)
		eg.Go(func() error {
			block, _, err := fd.getter(
				groupCtx, fd.kmd, ptr, fd.file, blockReadParallel)
			if err != nil {
				return err
			}
			// Recurse down to the level of the child.
			pfr, blocks, nextBlockOffset, err := fd.getLeafBlocksForOffsetRange(
				groupCtx, ptr, block, startOff, endOff, prefixOk)
			if err != nil {
				return err
			}

			// Append self to the front of every path.
			var r resp
			for _, p := range pfr {
				newPath := append([]parentBlockAndChildIndex{{
					pblock:     pblock,
					childIndex: childIndex,
				}}, p...)
				r.pathsFromRoot = append(r.pathsFromRoot, newPath)
			}
			r.blocks = blocks
			r.nextBlockOffset = nextBlockOffset
			respCh <- r
			return nil
		})
	}

	err = eg.Wait()
	// If we are ok with just getting the prefix, don't treat a
	// deadline exceeded error as fatal.
	if prefixOk && err == context.DeadlineExceeded {
		err = nil
	}
	if err != nil {
		return nil, nil, 0, err
	}

	blocks = make(map[BlockPointer]*FileBlock)
	minNextBlockOffsetChild := int64(-1)
outer:
	for _, respCh := range respChans {
		select {
		case r := <-respCh:
			pathsFromRoot = append(pathsFromRoot, r.pathsFromRoot...)
			for ptr, block := range r.blocks {
				blocks[ptr] = block
			}
			// We want to find the leftmost block offset that's to the
			// right of the range, the one immediately following the
			// end of the range.
			if r.nextBlockOffset != -1 &&
				(minNextBlockOffsetChild == -1 ||
					r.nextBlockOffset < minNextBlockOffsetChild) {
				minNextBlockOffsetChild = r.nextBlockOffset
			}
		default:
			// There should always be a response ready in every
			// channel, unless prefixOk is true.
			if prefixOk {
				break outer
			} else {
				panic("No response ready when !prefixOk")
			}
		}
	}

	// If this level has no offset, or one of the children has an
	// offset that's smaller than the one at this level, use the child
	// offset instead.
	if nextBlockOffsetThisLevel == -1 {
		nextBlockOffset = minNextBlockOffsetChild
	} else if minNextBlockOffsetChild != -1 &&
		minNextBlockOffsetChild < nextBlockOffsetThisLevel {
		nextBlockOffset = minNextBlockOffsetChild
	} else {
		nextBlockOffset = nextBlockOffsetThisLevel
	}

	return pathsFromRoot, blocks, nextBlockOffset, nil
}

// getByteSlicesInOffsetRange returns an ordered, continuous slice of
// byte ranges for the data described by the half-inclusive offset
// range `[startOff, endOff)`.  If `endOff` == -1, it returns data to
// the end of the file.  The caller is responsible for concatenating
// the data into a single buffer if desired. If `prefixOk` is true,
// the function will ignore context deadline errors and return
// whatever prefix of the data it could fetch within the deadine.
func (fd *fileData) getByteSlicesInOffsetRange(ctx context.Context,
	startOff, endOff int64, prefixOk bool) ([][]byte, error) {
	if startOff < 0 || endOff < -1 {
		return nil, fmt.Errorf("Bad offset range [%d, %d)", startOff, endOff)
	} else if endOff != -1 && endOff <= startOff {
		return nil, nil
	}

	topBlock, _, err := fd.getter(ctx, fd.kmd, fd.file.tailPointer(),
		fd.file, blockRead)
	if err != nil {
		return nil, err
	}

	// Find all the indirect pointers to leaf blocks in the offset range.
	var iptrs []IndirectFilePtr
	firstBlockOff := int64(-1)
	endBlockOff := int64(-1)
	nextBlockOff := int64(-1)
	var blockMap map[BlockPointer]*FileBlock
	if topBlock.IsInd {
		var pfr [][]parentBlockAndChildIndex
		pfr, blockMap, nextBlockOff, err = fd.getLeafBlocksForOffsetRange(
			ctx, fd.file.tailPointer(), topBlock, startOff, endOff, prefixOk)
		if err != nil {
			return nil, err
		}

		for i, p := range pfr {
			if len(p) == 0 {
				return nil, fmt.Errorf("Unexpected empty path to child for "+
					"file %v", fd.file.tailPointer())
			}
			lowestAncestor := p[len(p)-1]
			iptr := lowestAncestor.pblock.IPtrs[lowestAncestor.childIndex]
			iptrs = append(iptrs, iptr)
			if firstBlockOff < 0 {
				firstBlockOff = iptr.Off
			}
			if i == len(pfr)-1 {
				leafBlock := blockMap[iptr.BlockPointer]
				endBlockOff = iptr.Off + int64(len(leafBlock.Contents))
			}
		}
	} else {
		iptrs = []IndirectFilePtr{{
			BlockInfo: BlockInfo{BlockPointer: fd.file.tailPointer()},
			Off:       0,
		}}
		firstBlockOff = 0
		endBlockOff = int64(len(topBlock.Contents))
		blockMap = map[BlockPointer]*FileBlock{fd.file.tailPointer(): topBlock}
	}

	if len(iptrs) == 0 {
		return nil, nil
	}

	nRead := int64(0)
	n := endOff - startOff
	if endOff == -1 {
		n = endBlockOff - startOff
	}

	// Grab the relevant byte slices from each block described by the
	// indirect pointer, filling in holes as needed.
	var bytes [][]byte
	for _, iptr := range iptrs {
		block := blockMap[iptr.BlockPointer]
		blockLen := int64(len(block.Contents))
		nextByte := nRead + startOff
		toRead := n - nRead
		blockOff := iptr.Off
		lastByteInBlock := blockOff + blockLen

		if nextByte >= lastByteInBlock {
			if nextBlockOff > 0 {
				fill := nextBlockOff - nextByte
				if fill > toRead {
					fill = toRead
				}
				fd.log.CDebugf(ctx, "Read from hole: nextByte=%d "+
					"lastByteInBlock=%d fill=%d", nextByte, lastByteInBlock,
					fill)
				if fill <= 0 {
					fd.log.CErrorf(ctx,
						"Read invalid file fill <= 0 while reading hole")
					return nil, BadSplitError{}
				}
				bytes = append(bytes, make([]byte, fill))
				nRead += fill
				continue
			}
			return bytes, nil
		} else if toRead > lastByteInBlock-nextByte {
			toRead = lastByteInBlock - nextByte
		}

		firstByteToRead := nextByte - blockOff
		bytes = append(bytes,
			block.Contents[firstByteToRead:toRead+firstByteToRead])
		nRead += toRead
	}

	// If we didn't complete the read and there's another block, then
	// we've hit another hole and need to add a fill.
	if nRead < n && nextBlockOff > 0 {
		toRead := n - nRead
		nextByte := nRead + startOff
		fill := nextBlockOff - nextByte
		if fill > toRead {
			fill = toRead
		}
		fd.log.CDebugf(ctx, "Read from hole at end of file: nextByte=%d "+
			"fill=%d", nextByte, fill)
		if fill <= 0 {
			fd.log.CErrorf(ctx,
				"Read invalid file fill <= 0 while reading hole")
			return nil, BadSplitError{}
		}
		bytes = append(bytes, make([]byte, fill))
	}
	return bytes, nil
}

// The amount that the read timeout is smaller than the global one.
const readTimeoutSmallerBy = 2 * time.Second

// read fills the `dest` buffer with data from the file, starting at
// `startOff`.  Returns the number of bytes copied.  If the read
// operation nears the deadline set in `ctx`, it returns as big a
// prefix as possible before reaching the deadline.
func (fd *fileData) read(ctx context.Context, dest []byte, startOff int64) (
	int64, error) {
	if len(dest) == 0 {
		return 0, nil
	}

	// If we have a large enough timeout add a temporary timeout that is
	// readTimeoutSmallerBy. Use that for reading so short reads get returned
	// upstream without triggering the global timeout.
	now := time.Now()
	deadline, haveTimeout := ctx.Deadline()
	if haveTimeout {
		rem := deadline.Sub(now) - readTimeoutSmallerBy
		if rem > 0 {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, rem)
			defer cancel()
		}
	}

	bytes, err := fd.getByteSlicesInOffsetRange(ctx, startOff,
		startOff+int64(len(dest)), true)
	if err != nil {
		return 0, err
	}

	currLen := int64(0)
	for _, b := range bytes {
		bLen := int64(len(b))
		copy(dest[currLen:currLen+bLen], b)
		currLen += bLen
	}
	return currLen, nil
}

// getBytes returns a buffer containing data from the file, in the
// half-inclusive range `[startOff, endOff)`.  If `endOff` == -1, it
// returns data until the end of the file.
func (fd *fileData) getBytes(ctx context.Context, startOff, endOff int64) (
	data []byte, err error) {
	bytes, err := fd.getByteSlicesInOffsetRange(ctx, startOff, endOff, false)
	if err != nil {
		return nil, err
	}

	bufSize := 0
	for _, b := range bytes {
		bufSize += len(b)
	}
	data = make([]byte, bufSize)
	currLen := 0
	for _, b := range bytes {
		copy(data[currLen:currLen+len(b)], b)
		currLen += len(b)
	}

	return data, nil
}

// createIndirectBlock creates a new indirect block and pick a new id
// for the existing block, and use the existing block's ID for the new
// indirect block that becomes the parent.
func (fd *fileData) createIndirectBlock(
	df *dirtyFile, dver DataVer) (*FileBlock, error) {
	newID, err := fd.crypto.MakeTemporaryBlockID()
	if err != nil {
		return nil, err
	}
	fblock := &FileBlock{
		CommonBlock: CommonBlock{
			IsInd: true,
		},
		IPtrs: []IndirectFilePtr{
			{
				BlockInfo: BlockInfo{
					BlockPointer: BlockPointer{
						ID:      newID,
						KeyGen:  fd.kmd.LatestKeyGeneration(),
						DataVer: dver,
						BlockContext: BlockContext{
							Creator:  fd.uid,
							RefNonce: ZeroBlockRefNonce,
						},
					},
					EncodedSize: 0,
				},
				Off: 0,
			},
		},
	}

	fd.log.CDebugf(nil, "Promoting to indirect, new block id %v", newID)

	// Mark the old block ID as not dirty, so that we will treat the
	// old block ID as newly dirtied in cacheBlockIfNotYetDirtyLocked.
	df.setBlockNotDirty(fd.file.tailPointer())
	err = fd.cacher(fd.file.tailPointer(), fblock)
	if err != nil {
		return nil, err
	}

	return fblock, nil
}

// newRightBlock creates space for a new rightmost block,
// creating parent blocks and a new level of indirection in the tree
// as needed.  If there's no new level of indirection, it modifies the
// blocks in `parentBlocks` to include the new right-most pointers.
// If there is a new level of indirection, it returns the new top
// block.  It also returns any newly-dirtied block pointers.
func (fd *fileData) newRightBlock(
	ctx context.Context, parentBlocks []parentBlockAndChildIndex, off int64,
	df *dirtyFile, dver DataVer) (*FileBlock, []BlockPointer, error) {
	// Find the lowest block that can accommodate a new right block.
	lowestAncestorWithRoom := -1
	for i := len(parentBlocks) - 1; i >= 0; i-- {
		pb := parentBlocks[i]
		if len(pb.pblock.IPtrs) < fd.bsplit.MaxPtrsPerBlock() {
			lowestAncestorWithRoom = i
			break
		}
	}

	var newTopBlock *FileBlock
	var newDirtyPtrs []BlockPointer
	if lowestAncestorWithRoom < 0 {
		// Create a new level of indirection at the top.
		var err error
		newTopBlock, err = fd.createIndirectBlock(df, dver)
		if err != nil {
			return nil, nil, err
		}

		// The old top block needs to be cached under its new ID if it
		// was indirect.
		if len(parentBlocks) > 0 {
			ptr := newTopBlock.IPtrs[0].BlockPointer
			err = fd.cacher(ptr, parentBlocks[0].pblock)
			if err != nil {
				return nil, nil, err
			}
			newDirtyPtrs = append(newDirtyPtrs, ptr)
		}

		parentBlocks = append([]parentBlockAndChildIndex{{newTopBlock, 0}},
			parentBlocks...)
		lowestAncestorWithRoom = 0
	}

	fd.log.CDebugf(ctx, "New right block, lowestAncestor at level %d",
		lowestAncestorWithRoom)

	// Make a new right block for every parent, starting with the
	// lowest ancestor with room.
	pblock := parentBlocks[lowestAncestorWithRoom].pblock
	for i := lowestAncestorWithRoom; i < len(parentBlocks); i++ {
		newRID, err := fd.crypto.MakeTemporaryBlockID()
		if err != nil {
			return nil, nil, err
		}

		newPtr := BlockPointer{
			ID:      newRID,
			KeyGen:  fd.kmd.LatestKeyGeneration(),
			DataVer: dver,
			BlockContext: BlockContext{
				Creator:  fd.uid,
				RefNonce: ZeroBlockRefNonce,
			},
		}

		fd.log.CDebugf(ctx, "New right block, level %d, ptr %v", i, newPtr)

		pblock.IPtrs = append(pblock.IPtrs, IndirectFilePtr{
			BlockInfo: BlockInfo{
				BlockPointer: newPtr,
				EncodedSize:  0,
			},
			Off: off,
		})

		rblock := &FileBlock{}
		if i != len(parentBlocks)-1 {
			rblock.IsInd = true
			pblock = rblock
		}

		err = fd.cacher(newPtr, rblock)
		if err != nil {
			return nil, nil, err
		}

		newDirtyPtrs = append(newDirtyPtrs, newPtr)
	}

	// All parents up to and including the lowest ancestor with room
	// will have to change, so mark them as dirty
	ptr := fd.file.tailPointer()
	for i := 0; i <= lowestAncestorWithRoom; i++ {
		pb := parentBlocks[i]
		if err := fd.cacher(ptr, pb.pblock); err != nil {
			return nil, nil, err
		}
		newDirtyPtrs = append(newDirtyPtrs, ptr)
		ptr = pb.pblock.IPtrs[pb.childIndex].BlockPointer
	}

	return newTopBlock, newDirtyPtrs, nil
}

func (fd *fileData) shiftBlocksToFillHole(
	ctx context.Context, newTopBlock *FileBlock, newHoleStartOff int64) (
	newDirtyPtrs []BlockPointer, err error) {
	// Walk down the right side of the tree to find the new rightmost
	// indirect pointer, the offset of which should match
	// `newHoleStartOff`.  Keep swapping it with its sibling on the
	// left until its offset would be lower than that child's offset.
	// If there are no children to the left, continue on with the
	// children in the cousin block to the left.  If we swap a child
	// between cousin blocks, we must update the offset in the right
	// cousin's parent block.  If *that* updated pointer is the
	// leftmost pointer in its parent block, update that one as well,
	// up to the root.
	var parents []parentBlockAndChildIndex
	currBlock := newTopBlock
	for currBlock.IsInd {
		index := len(currBlock.IPtrs) - 1
		nextPtr := currBlock.IPtrs[index].BlockPointer
		parents = append(parents, parentBlockAndChildIndex{currBlock, index})
		currBlock, _, err = fd.getter(ctx, fd.kmd, nextPtr, fd.file, blockWrite)
		if err != nil {
			return nil, err
		}
	}

	immedParent := parents[len(parents)-1]
	currIndex := immedParent.childIndex
	if off := immedParent.pblock.IPtrs[currIndex].Off; off != newHoleStartOff {
		return nil, fmt.Errorf("Offset of rightmost block %d doesn't "+
			"match newHoleStartOff %d", off, newHoleStartOff)
	}

	fd.log.CDebugf(ctx, "Pre-shift parents: %v", parents)

	// Swap left as needed.
	for true {
		var leftOff int64
		var newParents []parentBlockAndChildIndex
		if currIndex != 0 {
			leftOff = immedParent.pblock.IPtrs[currIndex-1].Off
		} else {
			// Look for the next left cousin.
			newParents = make([]parentBlockAndChildIndex, len(parents))
			copy(newParents, parents)
			var level int
			for level = len(newParents) - 2; level >= 0; level-- {
				if newParents[level].childIndex > 0 {
					break
				}
				// Keep going up until we find a way back down.
			}

			if level < 0 {
				// We are already all the way on the left, we're done!
				fd.log.CDebugf(ctx, "Post-shift parents: %v", parents)
				return newDirtyPtrs, nil
			}
			newParents[level].childIndex--

			// Walk back down, shifting the new parents into position.
			for ; level < len(newParents)-1; level++ {
				nextChildPtr :=
					newParents[level].pblock.IPtrs[newParents[level].childIndex]
				childBlock, _, err := fd.getter(
					ctx, fd.kmd, nextChildPtr.BlockPointer, fd.file, blockWrite)
				if err != nil {
					return nil, err
				}

				newParents[level+1].pblock = childBlock
				newParents[level+1].childIndex = len(childBlock.IPtrs) - 1
				leftOff = childBlock.IPtrs[len(childBlock.IPtrs)-1].Off
			}
		}

		// We're done!
		if leftOff < newHoleStartOff {
			fd.log.CDebugf(ctx, "Post-shift parents: %v", parents)
			return newDirtyPtrs, nil
		}

		// Otherwise, we need to swap the indirect file pointers.
		if currIndex != 0 {
			fd.log.CDebugf(ctx, "Swapping offs %d with %d",
				immedParent.pblock.IPtrs[currIndex-1].Off,
				immedParent.pblock.IPtrs[currIndex].Off)
			immedParent.pblock.IPtrs[currIndex-1],
				immedParent.pblock.IPtrs[currIndex] =
				immedParent.pblock.IPtrs[currIndex],
				immedParent.pblock.IPtrs[currIndex-1]
			currIndex--
		} else {
			newImmedParent := newParents[len(newParents)-1]
			newCurrIndex := len(newImmedParent.pblock.IPtrs) - 1
			fd.log.CDebugf(ctx, "Swapping offs %d with %d across cousins",
				newImmedParent.pblock.IPtrs[newCurrIndex].Off,
				immedParent.pblock.IPtrs[currIndex].Off)
			newImmedParent.pblock.IPtrs[newCurrIndex],
				immedParent.pblock.IPtrs[currIndex] =
				immedParent.pblock.IPtrs[currIndex],
				newImmedParent.pblock.IPtrs[newCurrIndex]

			if len(newParents) > 1 {
				i := len(newParents) - 2
				iptr := newParents[i].pblock.IPtrs[newParents[i].childIndex]
				fd.log.CDebugf(ctx, "Cached %v", iptr.BlockPointer)
				if err := fd.cacher(
					iptr.BlockPointer, newImmedParent.pblock); err != nil {
					return nil, err
				}
			}

			// Now we need to update the parent offsets on the right
			// side, all the way up to the common ancestor (which is
			// the one with the one that doesn't have a childIndex of 0).
			newRightOff := immedParent.pblock.IPtrs[currIndex].Off
			for level := len(parents) - 2; level >= 0; level-- {
				// Cache the block below you, which was just modified.
				childPtr :=
					parents[level].pblock.IPtrs[parents[level].childIndex]
				fd.log.CDebugf(ctx, "Cached %v", childPtr.BlockPointer)
				if err := fd.cacher(childPtr.BlockPointer,
					parents[level+1].pblock); err != nil {
					return nil, err
				}
				newDirtyPtrs = append(newDirtyPtrs, childPtr.BlockPointer)

				// Update this level if needed.
				if parents[level+1].childIndex > 0 {
					fd.log.CDebugf(ctx, "Level %d parent childIndex is %d",
						level+1, parents[level+1].childIndex)
					break
				}
				index := parents[level].childIndex
				parents[level].pblock.IPtrs[index].Off = newRightOff
				fd.log.CDebugf(ctx, "Setting %d offset to %d at level %d",
					index, newRightOff, level)
			}
			immedParent = newImmedParent
			currIndex = newCurrIndex
			parents = newParents
		}
	}

	return nil, fmt.Errorf("Couldn't find where to move the hole for offset %d",
		newHoleStartOff)
}

// write sets the given data and the given offset within the file,
// making new blocks and new levels of indirection as needed. Return
// params:
// * newDe: a new directory entry with the EncodedSize cleared if the file
//   was extended.
// * dirtyPtrs: a slice of the BlockPointers that have been dirtied during
//   the write.
// * unrefs: a slice of BlockInfos that must be unreferenced as part of an
//   eventual sync of this write.  May be non-nil even if err != nil.
// * newlyDirtiedChildBytes is the total amount of block data dirtied by this
//   write, including the entire size of blocks that have had at least one
//   byte dirtied.  As above, it may be non-zero even if err != nil.
// * bytesExtended is the number of bytes the length of the file has been
//   extended as part of this write.
func (fd *fileData) write(ctx context.Context, data []byte, off int64,
	topBlock *FileBlock, oldDe DirEntry, df *dirtyFile) (
	newDe DirEntry, dirtyPtrs []BlockPointer, unrefs []BlockInfo,
	newlyDirtiedChildBytes int64, bytesExtended int64, err error) {
	n := int64(len(data))
	nCopied := int64(0)
	oldSizeWithoutHoles := oldDe.Size
	newDe = oldDe

	fd.log.CDebugf(ctx, "Writing %d bytes at off %d", n, off)

	for nCopied < n {
		oldOff := off + nCopied
		ptr, parentBlocks, block, nextBlockOff, startOff, wasDirty, err :=
			fd.getFileBlockAtOffset(ctx, topBlock, off+nCopied, blockWrite)
		if err != nil {
			return newDe, nil, unrefs, newlyDirtiedChildBytes, 0, err
		}

		oldLen := len(block.Contents)

		// Take care not to write past the beginning of the next block
		// by using max.
		max := len(data)
		if nextBlockOff > 0 {
			if room := int(nextBlockOff - off); room < max {
				max = room
			}
		}
		oldNCopied := nCopied
		nCopied += fd.bsplit.CopyUntilSplit(
			block, nextBlockOff < 0, data[nCopied:max], off+nCopied-startOff)

		// If we need another block but there are no more, then make one.
		switchToIndirect := false
		if nCopied < n {
			var newTopBlock *FileBlock
			needExtendFile := nextBlockOff < 0
			needFillHole := off+nCopied < nextBlockOff
			nextOff := startOff + int64(len(block.Contents))
			if needExtendFile || needFillHole {
				// Make a new right block and update the parent's
				// indirect block list, adding a level of indirection
				// if needed.  If we're just filling a hole, the block
				// will end up all the way to the right of the range,
				// and its offset will be smaller than the block to
				// its left -- we'll fix that up below.
				var newDirtyPtrs []BlockPointer
				newTopBlock, newDirtyPtrs, err = fd.newRightBlock(
					ctx, parentBlocks, nextOff, df,
					DefaultNewBlockDataVersion(false))
				if err != nil {
					return newDe, nil, unrefs, newlyDirtiedChildBytes, 0, err
				}
				dirtyPtrs = append(dirtyPtrs, newDirtyPtrs...)
				if newTopBlock != nil {
					if !topBlock.IsInd {
						// The whole block needs to be re-uploaded as
						// a child block with a new block pointer, so
						// track those dirty bytes and cache the block
						// as dirty.
						ptr = newTopBlock.IPtrs[0].BlockPointer
						switchToIndirect = true
					}
					// A new level of indirection.
					topBlock = newTopBlock
				}
			}
			// If we're filling a hole, swap the new right block into
			// the hole and shift everything else over.
			if needFillHole {
				newDirtyPtrs, err := fd.shiftBlocksToFillHole(
					ctx, newTopBlock, nextOff)
				if err != nil {
					return newDe, nil, unrefs, newlyDirtiedChildBytes, 0, err
				}
				dirtyPtrs = append(dirtyPtrs, newDirtyPtrs...)
				if oldSizeWithoutHoles == oldDe.Size {
					// For the purposes of calculating the newly-dirtied
					// bytes for the deferral calculation, disregard the
					// existing "hole" in the file.
					oldSizeWithoutHoles = uint64(nextOff)
				}
			}
		}

		// Nothing was copied, no need to dirty anything.  This can
		// happen when trying to append to the contents of the file
		// (i.e., either to the end of the file or right before the
		// "hole"), and the last block is already full.
		if nCopied == oldNCopied && !switchToIndirect {
			continue
		}

		// Only in the last block does the file size grow.
		if oldLen != len(block.Contents) && nextBlockOff < 0 {
			newDe.EncodedSize = 0
			// update the file info
			newDe.Size += uint64(len(block.Contents) - oldLen)
		}

		// Calculate the amount of bytes we've newly-dirtied as part
		// of this write.
		newlyDirtiedChildBytes += int64(len(block.Contents))
		fd.log.CDebugf(ctx, "Dirtied %d bytes at off %d", len(block.Contents), oldOff)
		if wasDirty {
			newlyDirtiedChildBytes -= int64(oldLen)
			fd.log.CDebugf(ctx, "Adjusted dirty bytes at off %d by %d", oldOff, oldLen)
		}

		for _, pb := range parentBlocks {
			// Remember the size of each newly-dirtied child.
			if pb.pblock.IPtrs[pb.childIndex].EncodedSize != 0 {
				unrefs = append(
					unrefs, pb.pblock.IPtrs[pb.childIndex].BlockInfo)
				pb.pblock.IPtrs[pb.childIndex].EncodedSize = 0
			}
		}

		// keep the old block ID while it's dirty
		if err = fd.cacher(ptr, block); err != nil {
			return newDe, nil, unrefs, newlyDirtiedChildBytes, 0, err
		}
		dirtyPtrs = append(dirtyPtrs, ptr)
	}

	if topBlock.IsInd {
		// Always make the top block dirty, so we will sync its
		// indirect blocks.  This has the added benefit of ensuring
		// that any write to a file while it's being sync'd will be
		// deferred, even if it's to a block that's not currently
		// being sync'd, since this top-most block will always be in
		// the dirtyFiles map.
		if err = fd.cacher(fd.file.tailPointer(), topBlock); err != nil {
			return newDe, nil, unrefs, newlyDirtiedChildBytes, 0, err
		}
		dirtyPtrs = append(dirtyPtrs, fd.file.tailPointer())
	}

	lastByteWritten := off + int64(len(data)) // not counting holes
	bytesExtended = 0
	if lastByteWritten > int64(oldSizeWithoutHoles) {
		bytesExtended = lastByteWritten - int64(oldSizeWithoutHoles)
	}

	return newDe, dirtyPtrs, unrefs, newlyDirtiedChildBytes, bytesExtended, nil
}

// truncateExtend increases file size to the given size by inserting
// "holes" into the file. Return params:
// * newDe: a new directory entry with the EncodedSize cleared if the file
//   was extended.
// * dirtyPtrs: a slice of the BlockPointers that have been dirtied during
//   the write.
func (fd *fileData) truncateExtend(ctx context.Context, size uint64,
	topBlock *FileBlock, parentBlocks []parentBlockAndChildIndex,
	oldDe DirEntry, df *dirtyFile) (
	newDe DirEntry, dirtyPtrs []BlockPointer, err error) {
	fd.log.CDebugf(ctx, "truncateExtend: extending fblock %#v", topBlock)
	switchToIndirect := !topBlock.IsInd
	oldTopBlock := topBlock
	if switchToIndirect {
		fd.log.CDebugf(ctx, "truncateExtend: making block indirect %v",
			fd.file.tailPointer())
	}

	newTopBlock, newDirtyPtrs, err := fd.newRightBlock(
		ctx, parentBlocks, int64(size), df,
		DefaultNewBlockDataVersion(true))
	if err != nil {
		return DirEntry{}, nil, err
	}
	if newTopBlock != nil {
		topBlock = newTopBlock
	}

	if switchToIndirect {
		topBlock.IPtrs[0].Holes = true
		err = fd.cacher(topBlock.IPtrs[0].BlockPointer, oldTopBlock)
		if err != nil {
			return DirEntry{}, nil, err
		}
		dirtyPtrs = append(dirtyPtrs, topBlock.IPtrs[0].BlockPointer)
		fd.log.CDebugf(ctx, "truncateExtend: new zero data block %v",
			topBlock.IPtrs[0].BlockPointer)
	}
	dirtyPtrs = append(dirtyPtrs, newDirtyPtrs...)
	newDe = oldDe
	newDe.EncodedSize = 0
	// update the file info
	newDe.Size = size

	// Mark all for presence of holes, one would be enough,
	// but this is more robust and easy.
	for i := range topBlock.IPtrs {
		topBlock.IPtrs[i].Holes = true
	}
	// Always make the top block dirty, so we will sync its
	// indirect blocks.  This has the added benefit of ensuring
	// that any write to a file while it's being sync'd will be
	// deferred, even if it's to a block that's not currently
	// being sync'd, since this top-most block will always be in
	// the fileBlockStates map.
	err = fd.cacher(fd.file.tailPointer(), topBlock)
	if err != nil {
		return DirEntry{}, nil, err
	}
	dirtyPtrs = append(dirtyPtrs, fd.file.tailPointer())
	return newDe, dirtyPtrs, nil
}

// truncateShrink shrinks the file to the given size. Return params:
// * newDe: a new directory entry with the EncodedSize cleared if the file
//   was extended.
// * unrefs: a slice of BlockInfos that must be unreferenced as part of an
//   eventual sync of this write.  May be non-nil even if err != nil.
// * newlyDirtiedChildBytes is the total amount of block data dirtied by this
//   write, including the entire size of blocks that have had at least one
//   byte dirtied.  As above, it may be non-zero even if err != nil.
func (fd *fileData) truncateShrink(ctx context.Context, size uint64,
	topBlock *FileBlock, oldDe DirEntry) (
	newDe DirEntry, unrefs []BlockInfo, newlyDirtiedChildBytes int64,
	err error) {
	iSize := int64(size) // TODO: deal with overflow
	ptr, parentBlocks, block, nextBlockOff, startOff, wasDirty, err :=
		fd.getFileBlockAtOffset(ctx, topBlock, iSize, blockWrite)
	if err != nil {
		return DirEntry{}, nil, 0, err
	}

	oldLen := len(block.Contents)
	// We need to delete some data (and possibly entire blocks).
	block.Contents = append([]byte(nil), block.Contents[:iSize-startOff]...)

	newlyDirtiedChildBytes = int64(len(block.Contents))
	if wasDirty {
		newlyDirtiedChildBytes -= int64(oldLen) // negative
	}

	if nextBlockOff > 0 {
		// TODO: if indexInParent == 0, we can remove the level of indirection.
		// TODO: support multiple levels of indirection.
		parentBlock := parentBlocks[0].pblock
		indexInParent := parentBlocks[0].childIndex
		for _, ptr := range parentBlock.IPtrs[indexInParent+1:] {
			unrefs = append(unrefs, ptr.BlockInfo)
		}
		parentBlock.IPtrs = parentBlock.IPtrs[:indexInParent+1]
		// always make the parent block dirty, so we will sync it
		if err = fd.cacher(fd.file.tailPointer(), parentBlock); err != nil {
			return DirEntry{}, nil, newlyDirtiedChildBytes, err
		}
	}

	if topBlock.IsInd {
		// Always make the top block dirty, so we will sync its
		// indirect blocks.  This has the added benefit of ensuring
		// that any truncate to a file while it's being sync'd will be
		// deferred, even if it's to a block that's not currently
		// being sync'd, since this top-most block will always be in
		// the dirtyFiles map.
		if err = fd.cacher(fd.file.tailPointer(), topBlock); err != nil {
			return DirEntry{}, nil, newlyDirtiedChildBytes, err
		}
	}

	for _, pb := range parentBlocks {
		// Remember how many bytes it was.
		unrefs = append(unrefs, pb.pblock.IPtrs[pb.childIndex].BlockInfo)
		pb.pblock.IPtrs[pb.childIndex].EncodedSize = 0
	}

	newDe = oldDe
	newDe.EncodedSize = 0
	newDe.Size = size

	// Keep the old block ID while it's dirty.
	if err = fd.cacher(ptr, block); err != nil {
		return DirEntry{}, nil, newlyDirtiedChildBytes, err
	}

	return newDe, unrefs, newlyDirtiedChildBytes, nil
}

// split, if given an indirect top block of a file, checks whether any
// of the dirty leaf blocks in that file need to be split up
// differently (i.e., if the BlockSplitter is using
// fingerprinting-based boundaries).  It returns the set of blocks
// that now need to be unreferenced.
func (fd *fileData) split(ctx context.Context, id tlf.ID,
	dirtyBcache DirtyBlockCache, topBlock *FileBlock, df *dirtyFile) (
	unrefs []BlockInfo, err error) {
	if !topBlock.IsInd {
		return nil, nil
	}

	// TODO: Verify that any getFileBlock... calls here
	// only use the dirty cache and not the network, since
	// the blocks are dirty.

	// For an indirect file:
	//   1) check if each dirty block is split at the right place.
	//   2) if it needs fewer bytes, prepend the extra bytes to the next
	//      block (making a new one if it doesn't exist), and the next block
	//      gets marked dirty
	//   3) if it needs more bytes, then use copyUntilSplit() to fetch bytes
	//      from the next block (if there is one), remove the copied bytes
	//      from the next block and mark it dirty
	//   4) Then go through once more, and ready and finalize each
	//      dirty block, updating its ID in the indirect pointer list
	for i := 0; i < len(topBlock.IPtrs); i++ {
		ptr := topBlock.IPtrs[i]
		isDirty := dirtyBcache.IsDirty(id, ptr.BlockPointer, fd.file.Branch)
		if (ptr.EncodedSize > 0) && isDirty {
			return unrefs, InconsistentEncodedSizeError{ptr.BlockInfo}
		}
		if !isDirty {
			continue
		}
		_, parentBlocks, block, nextBlockOff, _, _, err :=
			fd.getFileBlockAtOffset(ctx, topBlock, ptr.Off, blockWrite)
		if err != nil {
			return unrefs, err
		}

		splitAt := fd.bsplit.CheckSplit(block)
		switch {
		case splitAt == 0:
			continue
		case splitAt > 0:
			endOfBlock := ptr.Off + int64(len(block.Contents))
			extraBytes := block.Contents[splitAt:]
			block.Contents = block.Contents[:splitAt]
			// put the extra bytes in front of the next block
			if nextBlockOff < 0 {
				// Need to make a new block.
				if _, _, err := fd.newRightBlock(
					ctx, parentBlocks, endOfBlock, df,
					DefaultNewBlockDataVersion(false)); err != nil {
					return unrefs, err
				}
			}
			rPtr, _, rblock, _, _, _, err :=
				fd.getFileBlockAtOffset(
					ctx, topBlock, endOfBlock, blockWrite)
			if err != nil {
				return unrefs, err
			}
			rblock.Contents = append(extraBytes, rblock.Contents...)
			if err = fd.cacher(rPtr, rblock); err != nil {
				return unrefs, err
			}
			topBlock.IPtrs[i+1].Off = ptr.Off + int64(len(block.Contents))
			unrefs = append(unrefs, topBlock.IPtrs[i+1].BlockInfo)
			topBlock.IPtrs[i+1].EncodedSize = 0
		case splitAt < 0:
			if nextBlockOff < 0 {
				// end of the line
				continue
			}

			endOfBlock := ptr.Off + int64(len(block.Contents))
			rPtr, _, rblock, _, _, _, err :=
				fd.getFileBlockAtOffset(
					ctx, topBlock, endOfBlock, blockWrite)
			if err != nil {
				return unrefs, err
			}
			// copy some of that block's data into this block
			nCopied := fd.bsplit.CopyUntilSplit(block, false,
				rblock.Contents, int64(len(block.Contents)))
			rblock.Contents = rblock.Contents[nCopied:]
			if len(rblock.Contents) > 0 {
				if err = fd.cacher(rPtr, rblock); err != nil {
					return unrefs, err
				}
				topBlock.IPtrs[i+1].Off =
					ptr.Off + int64(len(block.Contents))
				unrefs = append(unrefs, topBlock.IPtrs[i+1].BlockInfo)
				topBlock.IPtrs[i+1].EncodedSize = 0
			} else {
				// TODO: delete the block, and if we're down
				// to just one indirect block, remove the
				// layer of indirection
				//
				// TODO: When we implement more than one level
				// of indirection, make sure that the pointer
				// to the parent block in the grandparent
				// block has EncodedSize 0.
				unrefs = append(unrefs, topBlock.IPtrs[i+1].BlockInfo)
				topBlock.IPtrs =
					append(topBlock.IPtrs[:i+1], topBlock.IPtrs[i+2:]...)
			}
		}
	}
	return unrefs, nil
}

// ready, if given an indirect top-block, readies all the dirty child
// blocks, and updates their block IDs in their parent block's list of
// indirect pointers.  It returns a map pointing from the new block
// info from any readied block to its corresponding old block pointer.
func (fd *fileData) ready(ctx context.Context, id tlf.ID, bcache BlockCache,
	dirtyBcache DirtyBlockCache, bops BlockOps, bps *blockPutState,
	topBlock *FileBlock, df *dirtyFile) (map[BlockInfo]BlockPointer, error) {
	if !topBlock.IsInd {
		return nil, nil
	}

	oldPtrs := make(map[BlockInfo]BlockPointer)
	// TODO: handle multiple levels of indirection.
	for i := 0; i < len(topBlock.IPtrs); i++ {
		ptr := topBlock.IPtrs[i]
		isDirty := dirtyBcache.IsDirty(id, ptr.BlockPointer, fd.file.Branch)
		if (ptr.EncodedSize > 0) && isDirty {
			return nil, InconsistentEncodedSizeError{ptr.BlockInfo}
		}
		if !isDirty {
			continue
		}

		// Ready the dirty block.
		_, _, block, _, _, _, err := fd.getFileBlockAtOffset(
			ctx, topBlock, ptr.Off, blockWrite)
		if err != nil {
			return nil, err
		}

		newInfo, _, readyBlockData, err :=
			ReadyBlock(ctx, bcache, bops, fd.crypto, fd.kmd, block, fd.uid)
		if err != nil {
			return nil, err
		}

		err = bcache.Put(newInfo.BlockPointer, id, block, PermanentEntry)
		if err != nil {
			return nil, err
		}

		bps.addNewBlock(newInfo.BlockPointer, block, readyBlockData,
			func() error {
				return df.setBlockSynced(ptr.BlockPointer)
			})

		topBlock.IPtrs[i].BlockInfo = newInfo
		oldPtrs[newInfo] = ptr.BlockPointer
	}
	return oldPtrs, nil
}

func (fd *fileData) getIndirectFileBlockInfosWithTopBlock(ctx context.Context,
	topBlock *FileBlock) (
	[]BlockInfo, error) {
	// TODO: handle multiple levels of indirection.
	if !topBlock.IsInd {
		return nil, nil
	}
	blockInfos := make([]BlockInfo, len(topBlock.IPtrs))
	for i, ptr := range topBlock.IPtrs {
		blockInfos[i] = ptr.BlockInfo
	}
	return blockInfos, nil
}

func (fd *fileData) getIndirectFileBlockInfos(ctx context.Context) (
	[]BlockInfo, error) {
	// TODO: handle multiple levels of indirection.
	topBlock, _, err := fd.getter(
		ctx, fd.kmd, fd.file.tailPointer(), fd.file, blockRead)
	if err != nil {
		return nil, err
	}
	return fd.getIndirectFileBlockInfosWithTopBlock(ctx, topBlock)
}

// findIPtrsAndClearSize looks for the given indirect pointer, and
// returns whether it could be found.  As a side effect, it also
// clears the encoded size for that indirect pointer.
func (fd *fileData) findIPtrsAndClearSize(topBlock *FileBlock,
	ptr BlockPointer) (found bool) {
	// TODO: handle multiple levels of indirection.  To make that
	// efficient, we may need to pass in more information about which
	// internal indirect pointers may have dirty child blocks.
	for i, iptr := range topBlock.IPtrs {
		if iptr.BlockPointer == ptr {
			topBlock.IPtrs[i].EncodedSize = 0
			return true
		}
	}
	return false
}

// deepCopy makes a complete copy of this file, deduping leaf blocks
// and making new random BlockPointers for all indirect blocks.  It
// returns the new top pointer of the copy, and all the new child
// pointers in the copy.
func (fd *fileData) deepCopy(ctx context.Context, codec kbfscodec.Codec,
	dataVer DataVer) (
	newTopPtr BlockPointer, allChildPtrs []BlockPointer, err error) {
	topBlock, _, err := fd.getter(ctx, fd.kmd, fd.file.tailPointer(),
		fd.file, blockRead)
	if err != nil {
		return zeroPtr, nil, err
	}

	newTopBlock, err := topBlock.DeepCopy(codec)
	if err != nil {
		return zeroPtr, nil, err
	}
	newTopPtr = fd.file.tailPointer()
	if topBlock.IsInd {
		newID, err := fd.crypto.MakeTemporaryBlockID()
		if err != nil {
			return zeroPtr, nil, err
		}
		newTopPtr = BlockPointer{
			ID:      newID,
			KeyGen:  fd.kmd.LatestKeyGeneration(),
			DataVer: dataVer,
			BlockContext: BlockContext{
				Creator:  fd.uid,
				RefNonce: ZeroBlockRefNonce,
			},
		}
	} else {
		newTopPtr.RefNonce, err = fd.crypto.MakeBlockRefNonce()
		if err != nil {
			return zeroPtr, nil, err
		}
		newTopPtr.SetWriter(fd.uid)
	}
	fd.log.CDebugf(ctx, "Deep copying file %s: %v -> %v",
		fd.file.tailName(), fd.file.tailPointer(), newTopPtr)

	// Dup all of the leaf blocks.
	// TODO: deal with multiple levels of indirection.
	if topBlock.IsInd {
		for i, iptr := range topBlock.IPtrs {
			// Generate a new nonce for each one.
			iptr.RefNonce, err = fd.crypto.MakeBlockRefNonce()
			if err != nil {
				return zeroPtr, nil, err
			}
			iptr.SetWriter(fd.uid)
			newTopBlock.IPtrs[i] = iptr
			allChildPtrs = append(allChildPtrs, iptr.BlockPointer)
		}
	}

	if err = fd.cacher(newTopPtr, newTopBlock); err != nil {
		return zeroPtr, nil, err
	}

	return newTopPtr, allChildPtrs, nil
}

// undupChildrenInCopy takes a top block that's been copied via
// deepCopy(), and un-deduplicates all leaf children of the block.  It
// adds all child blocks to the provided `bps`, including both the
// ones that were deduplicated and the ones that weren't (TODO).  It
// returns the BlockInfos for all children, including ones that
// weren't de-duplicated.
func (fd *fileData) undupChildrenInCopy(ctx context.Context,
	bcache BlockCache, bops BlockOps, bps *blockPutState,
	topBlock *FileBlock) ([]BlockInfo, error) {
	if !topBlock.IsInd {
		return nil, nil
	}

	// TODO: support multiple levels of indirection.
	// TODO: parallelize
	blockInfos := make([]BlockInfo, len(topBlock.IPtrs))
	for i, iptr := range topBlock.IPtrs {
		if iptr.RefNonce == ZeroBlockRefNonce {
			// No need to un-dup mid-level indirect blocks.  TODO:
			// fetch the blocks and add them to the bps (only needed
			// for multiple levels of indirection).
			blockInfos = append(blockInfos, iptr.BlockInfo)
			continue
		}
		childBlock, _, err := fd.getter(ctx, fd.kmd, iptr.BlockPointer,
			fd.file, blockRead)
		if err != nil {
			return nil, err
		}

		newInfo, _, readyBlockData, err :=
			ReadyBlock(ctx, bcache, bops, fd.crypto, fd.kmd, childBlock, fd.uid)
		topBlock.IPtrs[i].BlockInfo = newInfo
		bps.addNewBlock(newInfo.BlockPointer, childBlock, readyBlockData, nil)
		blockInfos = append(blockInfos, newInfo)
	}
	return blockInfos, nil
}
