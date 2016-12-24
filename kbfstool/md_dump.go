package main

import (
	"flag"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/keybase/client/go/protocol/keybase1"
	"github.com/keybase/kbfs/libkbfs"
	"golang.org/x/net/context"
)

func getUserString(
	ctx context.Context, config libkbfs.Config, uid keybase1.UID) string {
	username, _, err := config.KeybaseService().Resolve(
		ctx, fmt.Sprintf("uid:%s", uid))
	if err != nil {
		printError("md dump", err)
		return uid.String()
	}
	return fmt.Sprintf("%s (uid:%s)", username, uid)
}

func mdDumpReadOnlyRMD(ctx context.Context, config libkbfs.Config,
	rmd libkbfs.ReadOnlyRootMetadata) error {
	brmd := rmd.GetBareRootMetadata()
	buf, err := config.Codec().Encode(brmd)
	if err != nil {
		return err
	}

	c := spew.NewDefaultConfig()
	c.Indent = "  "
	c.DisablePointerAddresses = true
	c.DisableCapacities = true
	c.SortKeys = true

	brmdCopy, err := brmd.DeepCopy(config.Codec())
	if err != nil {
		return err
	}
	var serializedPrivateMetadata []byte
	switch brmdCopy := brmdCopy.(type) {
	case *libkbfs.BareRootMetadataV2:
		serializedPrivateMetadata = brmdCopy.SerializedPrivateMetadata
		brmdCopy.SerializedPrivateMetadata = nil
	case *libkbfs.BareRootMetadataV3:
		serializedPrivateMetadata = brmdCopy.WriterMetadata.SerializedPrivateMetadata
		brmdCopy.WriterMetadata.SerializedPrivateMetadata = nil
	default:
		// Do nothing, and let SerializedPrivateMetadata get
		// spewed, I guess.
	}
	fmt.Printf("MD size: %d bytes\n"+
		"MD version: %s\n"+
		"Private data size: %d bytes\n\n",
		len(buf), rmd.Version(), len(serializedPrivateMetadata))
	c.Dump(brmdCopy)
	fmt.Print("\n")

	fmt.Print("Extra metadata\n")
	fmt.Print("--------------\n")
	c.Dump(rmd.Extra())
	fmt.Print("\n")

	fmt.Print("Private metadata\n")
	fmt.Print("----------------\n")
	c.Dump(rmd.Data())

	return nil
}

func mdDumpImmutableRMD(ctx context.Context, config libkbfs.Config,
	rmd libkbfs.ImmutableRootMetadata) error {
	fmt.Printf("MD ID: %s\n", rmd.MdID())

	return mdDumpReadOnlyRMD(ctx, config, rmd.ReadOnly())
}

const mdDumpUsageStr = `Usage:
  kbfstool md dump input [inputs...]

Each input must be in the following format:

  TLF
  TLF:Branch
  TLF^Revision
  TLF:Branch^Revision

where TLF can be:

  - a TLF ID string (32 hex digits),
  - or a keybase TLF path (e.g., "/keybase/public/user1,user2", or
    "/keybase/private/user1,assertion2");

Branch can be:

  - a Branch ID string (32 hex digits),
  - the string "device", which indicates the unmerged branch for the
    current device, or the master branch if there is no unmerged branch,
  - the string "master", which is a shorthand for
    the ID of the master branch "00000000000000000000000000000000", or
  - omitted, in which case it is treated as if it were the string "device";

and Revision can be:

  - a hex number prefixed with "0x",
  - a decimal number with no prefix,
  - the string "latest", which indicates the latest revision for the
    branch, or
  - omitted, in which case it is treated as if it were the string "latest".

`

func mdDump(ctx context.Context, config libkbfs.Config, args []string) (exitStatus int) {
	flags := flag.NewFlagSet("kbfs md dump", flag.ContinueOnError)
	err := flags.Parse(args)
	if err != nil {
		printError("md dump", err)
		return 1
	}

	inputs := flags.Args()
	if len(inputs) < 1 {
		fmt.Print(mdDumpUsageStr)
		return 1
	}

	for _, input := range inputs {
		irmd, err := mdParseAndGet(ctx, config, input)
		if err != nil {
			printError("md dump", err)
			return 1
		}

		if irmd == (libkbfs.ImmutableRootMetadata{}) {
			fmt.Printf("No result found for %q\n\n", input)
			continue
		}

		fmt.Printf("Result for %q:\n\n", input)

		err = mdDumpImmutableRMD(ctx, config, irmd)
		if err != nil {
			printError("md dump", err)
			return 1
		}

		fmt.Print("\n")
	}

	return 0
}
