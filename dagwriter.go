package dagwriter

import (
	"context"

	"github.com/ipld/go-ipld-prime"
)

// DagWriter provides access for writing and deleting ipld.Nodes in an underlying store
// Note: on its own, the dag writer provides no methods for loading/reading nodes
type DagWriter interface {
	// Store stores the given ipld.Node in the underlying store, constructing a link/CID
	// from the data in the node and the provided LinkPrototype
	Store(lnkCtx ipld.LinkContext, lp ipld.LinkPrototype, n ipld.Node) (ipld.Link, error)
	// Delete deletes the node matching the given link from the underlying store
	Delete(ctx context.Context, lnk ipld.Link) error
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
