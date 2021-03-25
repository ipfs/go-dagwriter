package dagwriter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type dagBatchWriter struct {
	*ipld.LinkSystem
	bs    blockservice.BlockService
	cache *cachedOperationsStore
}

func (tds *dagBatchWriter) put(lnkCtx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {

	buf := bytes.Buffer{}
	return &buf, func(lnk ipld.Link) error {
		asCidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return fmt.Errorf("Unsupported Link Type")
		}
		tds.cache.write(asCidLink.Cid, buf.Bytes())
		return nil
	}, nil
}

func (tds *dagBatchWriter) Delete(ctx context.Context, lnk ipld.Link) error {
	asCidLink, ok := lnk.(cidlink.Link)
	if !ok {
		return fmt.Errorf("Unsupported Link Type")
	}
	tds.cache.delete(asCidLink.Cid)
	return nil
}

func (tds *dagBatchWriter) Commit() error {
	blks, deletes, err := tds.cache.reset()
	if err != nil {
		return err
	}
	for _, c := range deletes {
		err := tds.bs.DeleteBlock(c)
		if err != nil {
			return nil
		}
	}
	return tds.bs.AddBlocks(blks)
}

type cacheRecord struct {
	data      []byte
	tombstone bool
}

type cachedOperationsStore struct {
	cache   map[cid.Cid]cacheRecord
	cachelk sync.RWMutex
}

func newCachedOperationsStore() *cachedOperationsStore {
	return &cachedOperationsStore{
		cache: make(map[cid.Cid]cacheRecord),
	}
}

func (cos *cachedOperationsStore) write(c cid.Cid, data []byte) {
	cos.cachelk.Lock()
	cos.cache[c] = cacheRecord{data, false}
	cos.cachelk.Unlock()
}

func (cos *cachedOperationsStore) delete(c cid.Cid) {
	cos.cachelk.Lock()
	cos.cache[c] = cacheRecord{nil, true}
	cos.cachelk.Unlock()
}

func (cos *cachedOperationsStore) reset() ([]blocks.Block, []cid.Cid, error) {
	cos.cachelk.Lock()
	defer cos.cachelk.Unlock()
	blks := make([]blocks.Block, 0, len(cos.cache))
	deletes := make([]cid.Cid, 0, len(cos.cache))
	for c, record := range cos.cache {
		if record.tombstone {
			deletes = append(deletes, c)
			continue
		}
		blk, err := blocks.NewBlockWithCid(record.data, c)
		if err != nil {
			return nil, nil, nil
		}
		blks = append(blks, blk)
	}
	cos.cache = make(map[cid.Cid]cacheRecord)
	return blks, deletes, nil
}
