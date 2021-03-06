// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"crypto/rand"
	"errors"
	"sync"
	"testing"

	"github.com/keybase/kbfs/kbfscodec"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

// blockReturner contains a block value to copy into requested blocks, and a
// channel to synchronize on with the worker.
type blockReturner struct {
	block      Block
	continueCh chan struct{}
	startCh    chan struct{}
}

// fakeBlockGetter allows specifying and obtaining fake blocks.
type fakeBlockGetter struct {
	mtx      sync.RWMutex
	blockMap map[BlockPointer]blockReturner
	codec    kbfscodec.Codec
}

// newFakeBlockGetter returns a fakeBlockGetter.
func newFakeBlockGetter() *fakeBlockGetter {
	return &fakeBlockGetter{
		blockMap: make(map[BlockPointer]blockReturner),
		codec:    kbfscodec.NewMsgpack(),
	}
}

// setBlockToReturn sets the block that will be returned for a given
// BlockPointer. Returns a writeable channel that getBlock will wait on, to
// allow synchronization of tests.
func (bg *fakeBlockGetter) setBlockToReturn(blockPtr BlockPointer, block Block) (startCh <-chan struct{}, continueCh chan<- struct{}) {
	bg.mtx.Lock()
	defer bg.mtx.Unlock()
	sCh, cCh := make(chan struct{}), make(chan struct{})
	bg.blockMap[blockPtr] = blockReturner{
		block:      block,
		startCh:    sCh,
		continueCh: cCh,
	}
	return sCh, cCh
}

// getBlock implements the interface for realBlockGetter.
func (bg *fakeBlockGetter) getBlock(ctx context.Context, kmd KeyMetadata, blockPtr BlockPointer, block Block) error {
	bg.mtx.RLock()
	defer bg.mtx.RUnlock()
	source, ok := bg.blockMap[blockPtr]
	if !ok {
		return errors.New("Block doesn't exist in fake block map")
	}
	// Wait until the caller tells us to continue
	for {
		select {
		case source.startCh <- struct{}{}:
		case <-source.continueCh:
			return kbfscodec.Update(bg.codec, block, source.block)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func makeFakeFileBlock(t *testing.T) *FileBlock {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return &FileBlock{
		Contents: buf,
	}
}

func TestBlockRetrievalWorkerBasic(t *testing.T) {
	t.Log("Test the basic ability of a worker to return a block.")
	q := newBlockRetrievalQueue(1, kbfscodec.NewMsgpack())
	require.NotNil(t, q)
	defer q.Shutdown()

	bg := newFakeBlockGetter()
	w := newBlockRetrievalWorker(bg, q)
	require.NotNil(t, w)
	defer w.Shutdown()

	ptr1 := makeFakeBlockPointer(t)
	block1 := makeFakeFileBlock(t)
	_, continueCh1 := bg.setBlockToReturn(ptr1, block1)

	block := &FileBlock{}
	ch := q.Request(context.Background(), 1, nil, ptr1, block)
	continueCh1 <- struct{}{}
	err := <-ch
	require.NoError(t, err)
	require.Equal(t, block1, block)
}

func TestBlockRetrievalWorkerMultipleWorkers(t *testing.T) {
	t.Log("Test the ability of multiple workers to retrieve concurrently.")
	q := newBlockRetrievalQueue(2, kbfscodec.NewMsgpack())
	require.NotNil(t, q)
	defer q.Shutdown()

	bg := newFakeBlockGetter()
	w1, w2 := newBlockRetrievalWorker(bg, q), newBlockRetrievalWorker(bg, q)
	require.NotNil(t, w1)
	require.NotNil(t, w2)
	defer w1.Shutdown()
	defer w2.Shutdown()

	ptr1, ptr2 := makeFakeBlockPointer(t), makeFakeBlockPointer(t)
	block1, block2 := makeFakeFileBlock(t), makeFakeFileBlock(t)
	_, continueCh1 := bg.setBlockToReturn(ptr1, block1)
	_, continueCh2 := bg.setBlockToReturn(ptr2, block2)

	t.Log("Make 2 requests for 2 different blocks")
	block := &FileBlock{}
	req1Ch := q.Request(context.Background(), 1, nil, ptr1, block)
	req2Ch := q.Request(context.Background(), 1, nil, ptr2, block)

	t.Log("Allow the second request to complete before the first")
	continueCh2 <- struct{}{}
	err := <-req2Ch
	require.NoError(t, err)
	require.Equal(t, block2, block)

	t.Log("Make another request for ptr2")
	req2Ch = q.Request(context.Background(), 1, nil, ptr2, block)
	continueCh2 <- struct{}{}
	err = <-req2Ch
	require.NoError(t, err)
	require.Equal(t, block2, block)

	t.Log("Complete the ptr1 request")
	continueCh1 <- struct{}{}
	err = <-req1Ch
	require.NoError(t, err)
	require.Equal(t, block1, block)
}

func TestBlockRetrievalWorkerWithQueue(t *testing.T) {
	t.Log("Test the ability of a worker and queue to work correctly together.")
	q := newBlockRetrievalQueue(1, kbfscodec.NewMsgpack())
	require.NotNil(t, q)
	defer q.Shutdown()

	bg := newFakeBlockGetter()
	w1 := newBlockRetrievalWorker(bg, q)
	require.NotNil(t, w1)
	defer w1.Shutdown()

	ptr1, ptr2, ptr3 := makeFakeBlockPointer(t), makeFakeBlockPointer(t), makeFakeBlockPointer(t)
	block1, block2, block3 := makeFakeFileBlock(t), makeFakeFileBlock(t), makeFakeFileBlock(t)
	startCh1, continueCh1 := bg.setBlockToReturn(ptr1, block1)
	_, continueCh2 := bg.setBlockToReturn(ptr2, block2)
	_, continueCh3 := bg.setBlockToReturn(ptr3, block3)

	t.Log("Make 3 retrievals for 3 different blocks. All retrievals after the first should be queued.")
	block := &FileBlock{}
	testBlock1 := &FileBlock{}
	testBlock2 := &FileBlock{}
	req1Ch := q.Request(context.Background(), 1, nil, ptr1, block)
	req2Ch := q.Request(context.Background(), 1, nil, ptr2, block)
	req3Ch := q.Request(context.Background(), 1, nil, ptr3, testBlock1)
	// Ensure the worker picks up the first request
	<-startCh1
	t.Log("Make a high priority request for the third block, which should complete next.")
	req4Ch := q.Request(context.Background(), 2, nil, ptr3, testBlock2)

	t.Log("Allow the ptr1 retrieval to complete.")
	continueCh1 <- struct{}{}
	err := <-req1Ch
	require.NoError(t, err)
	require.Equal(t, block1, block)

	t.Log("Allow the ptr3 retrieval to complete. Both waiting requests should complete.")
	continueCh3 <- struct{}{}
	err1 := <-req3Ch
	err2 := <-req4Ch
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, block3, testBlock1)
	require.Equal(t, block3, testBlock2)

	t.Log("Complete the ptr2 retrieval.")
	continueCh2 <- struct{}{}
	err = <-req2Ch
	require.NoError(t, err)
	require.Equal(t, block2, block)
}

func TestBlockRetrievalWorkerCancel(t *testing.T) {
	t.Log("Test the ability of a worker to handle a request cancelation.")
	q := newBlockRetrievalQueue(1, kbfscodec.NewMsgpack())
	require.NotNil(t, q)
	defer q.Shutdown()

	bg := newFakeBlockGetter()
	w := newBlockRetrievalWorker(bg, q)
	require.NotNil(t, w)
	defer w.Shutdown()

	ptr1 := makeFakeBlockPointer(t)
	block1 := makeFakeFileBlock(t)
	_, _ = bg.setBlockToReturn(ptr1, block1)

	block := &FileBlock{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := q.Request(ctx, 1, nil, ptr1, block)
	err := <-ch
	require.EqualError(t, err, context.Canceled.Error())
}

func TestBlockRetrievalWorkerShutdown(t *testing.T) {
	t.Log("Test that worker shutdown works.")
	q := newBlockRetrievalQueue(1, kbfscodec.NewMsgpack())
	require.NotNil(t, q)
	defer q.Shutdown()

	bg := newFakeBlockGetter()
	w := newBlockRetrievalWorker(bg, q)
	require.NotNil(t, w)

	ptr1 := makeFakeBlockPointer(t)
	block1 := makeFakeFileBlock(t)
	_, continueCh := bg.setBlockToReturn(ptr1, block1)

	w.Shutdown()
	block := &FileBlock{}
	ctx, cancel := context.WithCancel(context.Background())
	// Ensure the context loop is stopped so the test doesn't leak goroutines
	defer cancel()
	ch := q.Request(ctx, 1, nil, ptr1, block)
	shutdown := false
	select {
	case <-ch:
	case continueCh <- struct{}{}:
	default:
		shutdown = true
	}
	require.True(t, shutdown)
	w.Shutdown()
	require.True(t, shutdown)
}

func TestBlockRetrievalWorkerMultipleBlockTypes(t *testing.T) {
	t.Log("Test that we can retrieve the same block into different block types.")
	codec := kbfscodec.NewMsgpack()
	q := newBlockRetrievalQueue(1, codec)
	require.NotNil(t, q)
	defer q.Shutdown()

	bg := newFakeBlockGetter()
	w1 := newBlockRetrievalWorker(bg, q)
	require.NotNil(t, w1)
	defer w1.Shutdown()

	t.Log("Setup source blocks")
	ptr1 := makeFakeBlockPointer(t)
	block1 := makeFakeFileBlock(t)
	_, continueCh1 := bg.setBlockToReturn(ptr1, block1)
	testCommonBlock := &CommonBlock{}
	err := kbfscodec.Update(codec, testCommonBlock, block1)
	require.NoError(t, err)

	t.Log("Make a retrieval for the same block twice, but with a different target block type.")
	testBlock1 := &FileBlock{}
	testBlock2 := &CommonBlock{}
	req1Ch := q.Request(context.Background(), 1, nil, ptr1, testBlock1)
	req2Ch := q.Request(context.Background(), 1, nil, ptr1, testBlock2)

	t.Log("Allow the first ptr1 retrieval to complete.")
	continueCh1 <- struct{}{}
	err = <-req1Ch
	require.NoError(t, err)
	require.Equal(t, testBlock1, block1)

	t.Log("Allow the second ptr1 retrieval to complete.")
	continueCh1 <- struct{}{}
	err = <-req2Ch
	require.NoError(t, err)
	require.Equal(t, testBlock2, testCommonBlock)
}
