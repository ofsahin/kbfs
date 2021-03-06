// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

// These tests all do one conflict-free operation while a user is unstaged.

package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/keybase/kbfs/libkbfs"
)

// bob creates a file while running the journal.
func TestJournalSimple(t *testing.T) {
	test(t, journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
			mkfile("a/b", "hello"),
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/b",
			}),
			// Check the data -- this should read from the journal if
			// it hasn't flushed yet.
			lsdir("a/", m{"b$": "FILE"}),
			read("a/b", "hello"),
			resumeJournal(),
			flushJournal(),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE"}),
			read("a/b", "hello"),
		),
	)
}

// bob exclusively creates a file while running the journal.  For now
// this is treated like a normal file create.
func TestJournalExclWrite(t *testing.T) {
	test(t, journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
			mkfile("a/c", "hello"),
			mkfileexcl("a/b"),
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/c",
			}),
			lsdir("a/", m{"b$": "FILE", "c$": "FILE"}),
			resumeJournal(),
			flushJournal(),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE", "c$": "FILE"}),
		),
	)
}

// bob creates a conflicting file while running the journal.
func TestJournalCrSimple(t *testing.T) {
	test(t, journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
			mkfile("a/b", "uh oh"),
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/b",
			}),
			// Don't flush yet.
		),
		as(alice,
			mkfile("a/b", "hello"),
		),
		as(bob, noSync(),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
		),
	)
}

// bob creates many conflicting files while running the journal.
func TestJournalCrManyFiles(t *testing.T) {
	var busyWork []fileOp
	iters := 20
	for i := 0; i < iters; i++ {
		name := fmt.Sprintf("a%d", i)
		busyWork = append(busyWork, mkfile(name, "hello"), rm(name))
	}

	test(t, journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
		),
		as(bob, busyWork...),
		as(bob,
			checkUnflushedPaths([]string{
				"",
				"alice,bob",
			}),
			// Don't flush yet.
		),
		as(alice,
			mkfile("a/b", "hello"),
		),
		as(bob, noSync(),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("a/", m{"b$": "FILE"}),
			read("a/b", "hello"),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE"}),
			read("a/b", "hello"),
		),
	)
}

// bob creates a conflicting file while running the journal.
func TestJournalDoubleCrSimple(t *testing.T) {
	test(t, journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
			mkfile("a/b", "uh oh"),
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/b",
			}),
			// Don't flush yet.
		),
		as(alice,
			mkfile("a/b", "hello"),
		),
		as(bob, noSync(),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
		),
		as(bob,
			pauseJournal(),
			mkfile("a/c", "uh oh"),
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/c",
			}),
			// Don't flush yet.
		),
		as(alice,
			mkfile("a/c", "hello"),
		),
		as(bob, noSync(),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE", "c$": "FILE", crnameEsc("c", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
			read("a/c", "hello"),
			read(crname("a/c", bob), "uh oh"),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE", "c$": "FILE", crnameEsc("c", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
			read("a/c", "hello"),
			read(crname("a/c", bob), "uh oh"),
		),
	)
}

// bob writes a multi-block file that conflicts with a file created by
// alice when journaling is on.
func TestJournalCrConflictUnmergedWriteMultiblockFile(t *testing.T) {
	test(t, journal(), blockSize(20),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			disableUpdates(),
		),
		as(alice,
			write("a/b", "hello"),
		),
		as(bob, noSync(),
			pauseJournal(),
			write("a/b", ntimesString(15, "0123456789")),
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/b",
			}),
			resumeJournal(),
			flushJournal(),
			reenableUpdates(),
		),
		as(bob,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), ntimesString(15, "0123456789")),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), ntimesString(15, "0123456789")),
		),
	)
}

// bob creates a conflicting file while running the journal, but then
// its resolution also conflicts.
func testJournalCrResolutionHitsConflict(t *testing.T, options []optionOp) {
	test(t, append(options,
		journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
			mkfile("a/b", "uh oh"),
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/b",
			}),
			// Don't flush yet.
		),
		as(alice,
			mkfile("a/b", "hello"),
		),
		as(bob, noSync(),
			stallOnMDResolveBranch(),
			resumeJournal(),
			// Wait for CR to finish before introducing new commit
			// from alice.
			waitForStalledMDResolveBranch(),
		),
		as(alice,
			mkfile("a/c", "new file"),
		),
		as(bob, noSync(),
			// Wait for one more, and cause another conflict, just to
			// be sadistic.
			unstallOneMDResolveBranch(),
			waitForStalledMDResolveBranch(),
		),
		as(alice,
			mkfile("a/d", "new file2"),
		),
		as(bob, noSync(),
			// Let bob's CR proceed, which should trigger CR again on
			// top of the resolution.
			unstallOneMDResolveBranch(),
			waitForStalledMDResolveBranch(),
			undoStallOnMDResolveBranch(),
			flushJournal(),
		),
		as(bob,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE", "c$": "FILE", "d$": "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
			read("a/c", "new file"),
			read("a/d", "new file2"),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("a/", m{"b$": "FILE", crnameEsc("b", bob): "FILE", "c$": "FILE", "d$": "FILE"}),
			read("a/b", "hello"),
			read(crname("a/b", bob), "uh oh"),
			read("a/c", "new file"),
			read("a/d", "new file2"),
		),
	)...)
}

func TestJournalCrResolutionHitsConflict(t *testing.T) {
	testJournalCrResolutionHitsConflict(t, nil)
}

func TestJournalCrResolutionHitsConflictWithIndirectBlocks(t *testing.T) {
	testJournalCrResolutionHitsConflict(t,
		[]optionOp{blockChangeSize(20), blockChangeSize(5)})
}

// Check that simple quota reclamation works when journaling is enabled.
func TestJournalQRSimple(t *testing.T) {
	test(t, journal(),
		users("alice"),
		as(alice,
			mkfile("a", "hello"),
			addTime(1*time.Minute),
			enableJournal(),
			mkfile("b", "hello2"),
			rm("b"),
			addTime(2*time.Minute),
			flushJournal(),
			pauseJournal(),
			addTime(2*time.Minute),
			mkfile("c", "hello3"),
			mkfile("d", "hello4"),
			addTime(2*time.Minute),
			forceQuotaReclamation(),
			checkUnflushedPaths([]string{
				"alice",
				"alice/c",
				"alice/d",
			}),
			resumeJournal(),
			flushJournal(),
			checkUnflushedPaths(nil),
		),
	)
}

// bob creates a bunch of files in a journal and the operations get
// coalesced together.
func TestJournalCoalescingBasicCreates(t *testing.T) {
	var busyWork []fileOp
	var reads []fileOp
	listing := m{"^a$": "DIR"}
	iters := libkbfs.ForcedBranchSquashThreshold + 1
	unflushedPaths := []string{"alice,bob"}
	for i := 0; i < iters; i++ {
		name := fmt.Sprintf("a%d", i)
		contents := fmt.Sprintf("hello%d", i)
		busyWork = append(busyWork, mkfile(name, contents))
		reads = append(reads, read(name, contents))
		listing["^"+name+"$"] = "FILE"
		unflushedPaths = append(unflushedPaths, "alice,bob/"+name)
	}

	test(t, journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
		),
		as(bob, busyWork...),
		as(bob,
			checkUnflushedPaths(unflushedPaths),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("", listing),
			checkUnflushedPaths(nil),
		),
		as(bob, reads...),
		as(alice,
			lsdir("", listing),
		),
		as(alice, reads...),
	)
}

// bob creates and appends to a file in a journal and the operations
// get coalesced together.
func TestJournalCoalescingWrites(t *testing.T) {
	var busyWork []fileOp
	iters := libkbfs.ForcedBranchSquashThreshold + 1
	var contents string
	for i := 0; i < iters; i++ {
		contents += fmt.Sprintf("hello%d", i)
		busyWork = append(busyWork, write("a/b", contents))
	}

	test(t, journal(), blockSize(20), blockChangeSize(5),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
		),
		as(bob, busyWork...),
		as(bob,
			checkUnflushedPaths([]string{
				"alice,bob/a",
				"alice,bob/a/b",
			}),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("", m{"a": "DIR"}),
			lsdir("a", m{"b": "FILE"}),
			read("a/b", contents),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("", m{"a": "DIR"}),
			lsdir("a", m{"b": "FILE"}),
			read("a/b", contents),
		),
	)
}

// bob does a bunch of operations in a journal and the operations get
// coalesced together.
func TestJournalCoalescingMixedOperations(t *testing.T) {
	var busyWork []fileOp
	iters := libkbfs.ForcedBranchSquashThreshold + 1
	for i := 0; i < iters; i++ {
		name := fmt.Sprintf("a%d", i)
		busyWork = append(busyWork, mkfile(name, "hello"), rm(name))
	}

	targetMtime := time.Now().Add(1 * time.Minute)
	test(t, journal(), blockSize(20), blockChangeSize(5),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
			mkfile("a/b", "hello"),
			mkfile("a/c", "hello2"),
			mkfile("a/d", "hello3"),
			mkfile("a/e", "hello4"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
			// bob does a bunch of stuff:
			//  * writes to an existing file a/b
			//  * creates a new directory f
			//  * creates and writes to a new file f/g
			//  * creates and writes to a new file h
			//  * removes an existing file a/c
			//  * renames an existing file a/d -> a/i
			//  * sets the mtime on a/e
			//  * does a bunch of busy work to ensure we hit the squash limit
			write("a/b", "world"),
			mkdir("f"),
			mkfile("f/g", "hello5"),
			mkfile("h", "hello6"),
			rm("a/c"),
			rename("a/d", "a/i"),
			setmtime("a/e", targetMtime),
		),
		as(bob, busyWork...),
		as(bob,
			checkUnflushedPaths([]string{
				"",
				"alice,bob",
				"alice,bob/a",
				"alice,bob/a/b",
				"alice,bob/a/e",
				"alice,bob/f",
				"alice,bob/f/g",
				"alice,bob/h",
			}),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("", m{"a": "DIR", "f": "DIR", "h": "FILE"}),
			lsdir("a", m{"b": "FILE", "e": "FILE", "i": "FILE"}),
			read("a/b", "world"),
			read("a/e", "hello4"),
			mtime("a/e", targetMtime),
			read("a/i", "hello3"),
			lsdir("f", m{"g": "FILE"}),
			read("f/g", "hello5"),
			read("h", "hello6"),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("", m{"a": "DIR", "f": "DIR", "h": "FILE"}),
			lsdir("a", m{"b": "FILE", "e": "FILE", "i": "FILE"}),
			read("a/b", "world"),
			read("a/e", "hello4"),
			mtime("a/e", targetMtime),
			read("a/i", "hello3"),
			lsdir("f", m{"g": "FILE"}),
			read("f/g", "hello5"),
			read("h", "hello6"),
		),
	)
}

// bob makes a bunch of changes that cancel each other out, and get
// coalesced together.
func TestJournalCoalescingNoChanges(t *testing.T) {
	var busyWork []fileOp
	iters := libkbfs.ForcedBranchSquashThreshold + 1
	for i := 0; i < iters; i++ {
		name := fmt.Sprintf("a%d", i)
		busyWork = append(busyWork, mkfile(name, "hello"), rm(name))
	}

	test(t, journal(),
		users("alice", "bob"),
		as(alice,
			mkdir("a"),
		),
		as(bob,
			enableJournal(),
			pauseJournal(),
		),
		as(bob, busyWork...),
		as(bob,
			checkUnflushedPaths([]string{
				"",
				"alice,bob",
			}),
			resumeJournal(),
			// This should kick off conflict resolution.
			flushJournal(),
		),
		as(bob,
			lsdir("", m{"a$": "DIR"}),
			lsdir("a", m{}),
			checkUnflushedPaths(nil),
		),
		as(alice,
			lsdir("", m{"a$": "DIR"}),
			lsdir("a", m{}),
		),
	)
}
