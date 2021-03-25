package dagwriter

import (
	"bytes"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// DagWriter provides access for writing and deleting ipld.Nodes in an underlying store
// Note: on its own, the dag writer provides no methods for loading/reading nodes
type DagWriter interface {
	// Store stores the given ipld.Node in the underlying store, constructing a link/CID
	// from the data in the node and the provided LinkPrototype
	Store(lnkCtx ipld.LinkContext, lp ipld.LinkPrototype, n ipld.Node) (ipld.Link, error)
	// Delete deletes the node matching the given link from the underlying store
	Delete(ipld.Link) error
}

// DagBatchWriter is a DagWriter that allows queing up several write and delete commands
// to an underlying store, then executing them in a single commit
type DagBatchWriter interface {
	DagWriter
	// Commit executes the queued operations to the underlying data store
	Commit() error
}

// DagWritingService provides both methods for writing and deleting ipld.Nodes atomically,
// and for instantiating batch operations
type DagWritingService interface {
	DagWriter
	// NewBatchWriter instantiates a new multi-operation write/delete
	NewBatchWriter() DagBatchWriter
}

type dagWritingService struct {
	*ipld.LinkSystem
	bs blockservice.BlockService
}

// NewDagWriter returns a new DagWritingService interface
func NewDagWriter(bs blockservice.BlockService) DagWritingService {
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
		return ds.bs.AddBlock(block)
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

func (ds dagWritingService) NewBatchWriter() DagBatchWriter {
	dmw := &dagBatchWriter{bs: ds.bs, cache: newCachedOperationsStore()}
	ls := ipld.LinkSystem{
		StorageWriteOpener: dmw.put,
		EncoderChooser:     ds.EncoderChooser,
		DecoderChooser:     ds.DecoderChooser,
		HasherChooser:      ds.HasherChooser,
	}
	dmw.LinkSystem = &ls
	return dmw
}
