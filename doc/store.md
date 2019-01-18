Block Stores
============

The `BlockStore` protocol defines the methods necessary for implementing a block
storage backend. The interface is simple enough to map to many different kinds
of storage, and the library comes with basic in-memory and file-based stores.


## Implementation Decisions

There are a few questions you need to answer if you decide to implement a block
store.

### Laziness

Will your store return _loaded_ or _lazy_ blocks? The former requires pulling
the block data into memory when fetching the block, while the latter only
fetches the data when the block's content is read.

Different choices make sense for different backends; for example, a persistent
store backed by cloud storage can usually depend on the data being there later,
so lazy blocks are much more efficient. For a store based on a remote cache, it
would be better to pull the block content on fetch because the data might be
evicted before the block is read.

### Asynchrony

The block store methods are intended to be asynchronous, to enable efficient
usage in concurrent environments. This project uses the excellent
[manifold](https://github.com/ztellman/manifold) library for an adaptable async
framework. If there's no underlying asynchronous system to tie into in the
storage backend, you can just wrap the synchronous calls in
`manifold.deferred/future` to put them on a thread-pool.

### Metadata

Blocks support Clojure metadata, so any additional information the store needs
to communicate can be added to the blocks as metadata. This intentionally
doesn't affect block equality or semantics, but can be useful for introspection.


## Storage Protocol

Block stores must implement five methods.

### list

...

### stat

...

### get

...

### put!

...

### delete!

...



## Erasable Protocol

Stores may optionally add support for the `ErasableStore` protocol, which
provides an efficient mechanism for completely removing the backend data. If not
implemented, the `erase!` function will fall back to listing and deleting all
blocks.
