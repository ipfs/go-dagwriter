package dagwriter

import (
	"bytes"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type DagWriter interface {
	Store(lnkCtx ipld.LinkContext, lp ipld.LinkPrototype, n ipld.Node) (ipld.Link, error)
	Delete(ipld.Link) error
}

type DagMultiWriter interface {
	DagWriter
	Commit() error
}

// DagWritingService is an interface for reading and writing DAGs
type DagWritingService interface {
	DagWriter
	NewMultiWriter() DagMultiWriter
}

type dagWritingService struct {
	*ipld.LinkSystem
	bs blockstore.Blockstore
}

// NewDagWriter returns a new DagWritingService interface
func NewDagWriter(bs blockstore.Blockstore) DagWritingService {
	ds := dagWritingService{bs: bs}
	ls := cidlink.DefaultLinkSystem()
	ls.StorageWriteOpener = ds.put
	ds.LinkSystem = &ls
	return ds
}

// ipld.BlockWriterOpener that writes to block store
func (ds dagWritingService) put(lnkCtx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
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
		return ds.bs.Put(block)
	}
	return buffer, committer, nil
}

func (ds dagWritingService) Delete(lnk ipld.Link) error {
	asCidLink, ok := lnk.(cidlink.Link)
	if !ok {
		return fmt.Errorf("Unsupported Link Type")
	}
	return ds.bs.DeleteBlock(asCidLink.Cid)
}

func (ds dagWritingService) NewMultiWriter() DagMultiWriter {
	dmw := &dagMultiWriter{bs: ds.bs, cache: newCachedOperationsStore()}
	ls := ipld.LinkSystem{
		StorageWriteOpener: dmw.put,
		EncoderChooser:     ds.EncoderChooser,
		DecoderChooser:     ds.DecoderChooser,
		HasherChooser:      ds.HasherChooser,
	}
	dmw.LinkSystem = &ls
	return dmw
}
