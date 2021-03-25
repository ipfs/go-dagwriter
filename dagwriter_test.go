package dagwriter_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-dagwriter"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/stretchr/testify/require"
)

func TestDagWriterRoundTrip(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewMapDatastore()
	bstore := blockstore.NewBlockstore(ds)
	blockService := blockservice.New(bstore, offline.Exchange(bstore))
	writer := dagwriter.NewDagWriter(blockService)

	bNode, err := qp.BuildMap(basicnode.Prototype__Map{}, 1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "three", qp.Bool(true))
	})
	require.NoError(t, err)
	pbNode, err := qp.BuildMap(dagpb.Type.PBNode, 2, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(1, func(la ipld.ListAssembler) {
			qp.ListEntry(la, qp.Map(-1, func(ma ipld.MapAssembler) {
				qp.MapEntry(ma, "Hash", qp.Link(cidlink.Link{Cid: generateRandomCid()}))
				qp.MapEntry(ma, "Name", qp.String("Applesauce"))
				qp.MapEntry(ma, "Tsize", qp.Int(rand.Int63()))
			}))
		}))
	})

	testCases := map[string]struct {
		lp          ipld.LinkPrototype
		node        ipld.Node
		expectedErr error
		np          ipld.NodePrototype
		decoder     ipld.Decoder
	}{
		"basic node": {
			lp: cidlink.LinkPrototype{cid.Prefix{
				Version:  1,
				Codec:    0x71,
				MhType:   0x17,
				MhLength: 20,
			}},
			node:    bNode,
			np:      basicnode.Prototype.Any,
			decoder: dagcbor.Decode,
		},
		"pb node": {
			lp: cidlink.LinkPrototype{cid.Prefix{
				Version:  1,
				Codec:    0x70,
				MhType:   0x17,
				MhLength: 20,
			}},
			node:    pbNode,
			np:      dagpb.Type.PBNode,
			decoder: dagpb.Decode,
		},
		"node that erros on write": {
			lp: cidlink.LinkPrototype{cid.Prefix{
				Version:  1,
				Codec:    0x70,
				MhType:   0x17,
				MhLength: 20,
			}},
			node:        bNode,
			expectedErr: errors.New("invalid key for map dagpb.PBNode: \"three\": no such field"),
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			lnk, err := writer.Store(ipld.LinkContext{Ctx: ctx}, data.lp, data.node)
			if err != nil {
				require.EqualError(t, err, data.expectedErr.Error())
			} else {
				// test write followed by load
				require.NoError(t, err)
				clnk, isCidLink := lnk.(cidlink.Link)
				require.True(t, isCidLink)
				blk, err := bstore.Get(clnk.Cid)
				require.NoError(t, err)
				nb := data.np.NewBuilder()
				err = data.decoder(nb, bytes.NewReader(blk.RawData()))
				require.NoError(t, err)
				nd := nb.Build()
				require.Equal(t, data.node, nd)

				// test delete after load
				err = writer.Delete(ctx, lnk)
				require.NoError(t, err)
				blk, err = bstore.Get(clnk.Cid)
				require.EqualError(t, err, blockstore.ErrNotFound.Error())
			}
		})
	}
}

func TestBatchWriter(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewMapDatastore()
	bstore := blockstore.NewBlockstore(ds)
	blockService := blockservice.New(bstore, offline.Exchange(bstore))
	writer := dagwriter.NewDagWriter(blockService)
	lp := cidlink.LinkPrototype{cid.Prefix{
		Version:  1,
		Codec:    0x71,
		MhType:   0x17,
		MhLength: 20,
	}}

	existing, err := qp.BuildMap(basicnode.Prototype__Map{}, 1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "applesauce", qp.String("red"))
	})
	require.NoError(t, err)
	existingLnk, err := writer.Store(ipld.LinkContext{Ctx: ctx}, lp, existing)
	require.NoError(t, err)

	batchWriter := writer.NewBatchWriter()

	nodeConstructionSeq := []func(prevLinks []ipld.Link) (ipld.Node, error){
		func(prevLinks []ipld.Link) (ipld.Node, error) {
			return qp.BuildMap(basicnode.Prototype__Map{}, 1, func(na ipld.MapAssembler) {
				qp.MapEntry(na, "three", qp.Bool(true))
			})
		},
		func(prevLinks []ipld.Link) (ipld.Node, error) {
			return qp.BuildMap(basicnode.Prototype__Map{}, 1, func(na ipld.MapAssembler) {
				qp.MapEntry(na, "four", qp.Bool(true))
			})
		},
		func(prevLinks []ipld.Link) (ipld.Node, error) {
			return qp.BuildMap(basicnode.Prototype__Map{}, 2, func(na ipld.MapAssembler) {
				qp.MapEntry(na, "link3", qp.Link(prevLinks[0]))
				qp.MapEntry(na, "link4", qp.Link(prevLinks[1]))
			})
		},
		func(prevLinks []ipld.Link) (ipld.Node, error) {
			return qp.BuildMap(basicnode.Prototype__Map{}, 3, func(na ipld.MapAssembler) {
				qp.MapEntry(na, "foo", qp.Bool(true))
				qp.MapEntry(na, "bar", qp.Bool(false))
				qp.MapEntry(na, "nested", qp.Map(2, func(na ipld.MapAssembler) {
					qp.MapEntry(na, "link2", qp.Link(prevLinks[2]))
					qp.MapEntry(na, "nonlink", qp.String("zoo"))
				}))
			})
		},
	}
	var links []ipld.Link
	for _, constructor := range nodeConstructionSeq {
		nd, err := constructor(links)
		require.NoError(t, err)
		lnk, err := batchWriter.Store(ipld.LinkContext{Ctx: ctx}, lp, nd)
		require.NoError(t, err)
		// verify the link is not in the block store
		_, err = bstore.Get(lnk.(cidlink.Link).Cid)
		require.EqualError(t, err, blockstore.ErrNotFound.Error())
		links = append(links, lnk)
	}

	// add a delete operation for the existing node and verify it's still present
	err = batchWriter.Delete(ctx, existingLnk)
	require.NoError(t, err)
	_, err = bstore.Get(existingLnk.(cidlink.Link).Cid)
	require.NoError(t, err)

	// add a delete operation for one of the nodes
	err = batchWriter.Delete(ctx, links[0])
	require.NoError(t, err)

	// commit and check:
	// written nodes are present in store
	// deleted existing node is gone
	// written then deleted node never written
	err = batchWriter.Commit()
	require.NoError(t, err)
	assertPresent(t, bstore, links[1])
	assertPresent(t, bstore, links[2])
	assertPresent(t, bstore, links[3])
	assertNotPresent(t, bstore, existingLnk)
	assertNotPresent(t, bstore, links[0])
}

func generateRandomCid() cid.Cid {
	buf := make([]byte, 100)
	rand.Read(buf)
	b := blocks.NewBlock(buf)
	return b.Cid()
}

func assertPresent(t *testing.T, bstore blockstore.Blockstore, lnk ipld.Link) {
	_, err := bstore.Get(lnk.(cidlink.Link).Cid)
	require.NoError(t, err)
}

func assertNotPresent(t *testing.T, bstore blockstore.Blockstore, lnk ipld.Link) {
	_, err := bstore.Get(lnk.(cidlink.Link).Cid)
	require.EqualError(t, err, blockstore.ErrNotFound.Error())
}
