# go-dagwriter

## This package has been deprecated and archived. See https://pkg.go.dev/github.com/ipld/go-ipld-prime/storage/bsrvadapter for an alternative.

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)


DagWriter is the glue for mutating (adding, removing) [ipld](https://github.com/ipld/go-ipld-prime) nodes in a [block service](https://github.com/ipfs/go-blockservice).

## Usage

```go
import (
    "context"

    "github.com/ipfs/go-dagwriter"
    "github.com/ipld/go-ipld-prime"
    cidlink "github.com/ipld/go-ipld-prime/linking/cid"
    "github.com/ipfs/go-cid"
)

...
linkPrototype := cidlink.LinkPrototype{cid.Prefix{
    Version: 1, Codec: 0x71, MhType: 0x17, MhLength: 20,
}}

writer := dagwriter.NewDagWriter(blockService)

// Store ipldNode in the block service
link, err := writer.Store(ipld.LinkContext{}, linkPrototype, ipldNode)
if err != nil {
    panic(err)
}
...

// Remove the node from the block service.
if err := writer.Delete(context.Background(), link); err != nil {
    panic(err)
}

```

## Contribute

PRs are welcome!

## License

The go-dagwriter project is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](https://github.com/ipfs/go-dagwriter/blob/main/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](https://github.com/ipfs/go-dagwriter/blob/main/LICENSE-MIT) or http://opensource.org/licenses/MIT)
