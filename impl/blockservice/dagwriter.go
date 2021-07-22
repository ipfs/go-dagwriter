package bsdagwriter

import (
	"bytes"
	"context"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-dagwriter"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type dagWritingService struct {
	*ipld.LinkSystem
	bs blockservice.BlockService
}

// NewDagWriter returns a new DagWritingService interface
func NewDagWriter(bs blockservice.BlockService) dagwriter.DagWritingService {
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
			return fmt.Errorf("unsupported link type %v", lnk)
		}
		block, err := blocks.NewBlockWithCid(buffer.Bytes(), asCidLink.Cid)
		if err != nil {
			return err
		}
		return ds.bs.AddBlock(block)
	}
	return buffer, committer, nil
}

func (ds dagWritingService) Delete(ctx context.Context, lnk ipld.Link) error {
	asCidLink, ok := lnk.(cidlink.Link)
	if !ok {
		return fmt.Errorf("unsupported link type %v", lnk)
	}
	return ds.bs.DeleteBlock(asCidLink.Cid)
}

func (ds dagWritingService) NewBatchWriter() dagwriter.DagBatchWriter {
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
