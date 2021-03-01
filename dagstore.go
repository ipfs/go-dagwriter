package dagservice

import (
	"bytes"
	"context"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	fetcher "github.com/ipfs/go-fetcher"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// DagStore is an interface for reading and writing DAGs
type DagStore interface {
	Store(lnkCtx ipld.LinkContext, lp ipld.LinkPrototype, n ipld.Node) (ipld.Link, error)
	Delete(ipld.Link) error
	NewSession(ctx context.Context) *fetcher.Fetcher
}

type dagStore struct {
	*ipld.LinkSystem
	bs blockservice.BlockService
}

// NewDagStore returns a new DagStore interface
func NewDagStore(bs *blockstore.Blockstore) DagStore {
	ds := dagStore{}
	ls := cidlink.DefaultLinkSystem()
	ls.StorageReadOpener = ds.get
	ls.StorageWriteOpener = ds.put
	return ds
}

// ipld.BlockReadOpener that reads from block store
func (ds dagStore) get(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	asCidLink, ok := lnk.(cidlink.Link)
	if !ok {
		return nil, fmt.Errorf("Unsupported Link Type")
	}
	block, err := ds.bs.GetBlock(lnkCtx.Ctx, asCidLink.Cid)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(block.RawData()), nil
}

// ipld.BlockWriterOpener that writes to block store
func (ds dagStore) put(lnkCtx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
	buffer := new(bytes.Buffer)
	committer := func(lnk ipld.Link) error {
		asCidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return fmt.Errorf("Unsupported Link Type")
		}
		block, err := blocks.NewBlockWithCid(buffer.Bytes(), asCidLink.Cid)
		if err != nil {
			return err
		}
		return ds.bs.AddBlock(block)
	}
	return buffer, committer, nil
}

func (ds dagStore) Delete(lnk ipld.Link) error {
	asCidLink, ok := lnk.(cidlink.Link)
	if !ok {
		return fmt.Errorf("Unsupported Link Type")
	}
	return ds.bs.DeleteBlock(asCidLink.Cid)
}

func (ds dagStore) NewSession(ctx context.Context) *fetcher.Fetcher {
	// does makes sense to initialize the fetcher here?
	return nil
}
