Block Stores
============

The block store protocol defines the methods necessary for implementing a block
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

For example, a common piece of useful metadata is a URI for the concrete
location of the block's data in the backing store.


## Storage Protocol

Block stores must implement five methods.

### list

The `list` method enumerates the blocks contained in the store as a
[Manifold stream](https://github.com/ztellman/manifold/blob/master/docs/stream.md),
ordered by the blocks' multihash identifier.

A few simple query criteria are supported to return ranges of blocks whose id
uses a specific algorithm, or falls before or after some given hex markers. The
implementation of this method must return _at least_ the blocks which match the
query options, and _should_ optimize the results by omitting unmatched blocks
when possible.

In practice, when this method is called the store should spin up an asynchronous
process (usually a thread via `d/future`) that interrogates the storage layer
and emits the resulting blocks on an output stream, which is returned to the
caller. This stream may be closed preemptively if the consumer is done, which
should cleanly terminate the listing process.

If the listing encounters an exception, the error should be placed on the stream
and the stream should be closed to indicate no further blocks will be coming.
Consumers must handle exceptions propagated on the stream in this fashion.

### stat

The `stat` method is used to return metadata about a block if the store contains
it. This should return a deferred which yields the info, or `nil` if the store
does not contain the requested block.

This is very similar to `get`, but returns a regular Clojure map instead of
a block. This map should have the same `:id`, `:size`, and `:stored-at` values
as well as the same metadata a block returned from `get` would. Conceptually,
this is similar to the `HEAD` vs `GET` verbs in HTTP.

This distinction is useful for stores which return loaded blocks, like a cache -
using `stat` allows the caller to avoid the extra IO for block content which
will not be used.

### get

This method fetches a block from the store. This should return a deferred
which yields the block, or nil if not present.

### put!

Putting a block persists it into the store. This should return a deferred which
yields the stored block. If the store already contains this block, then the
implementation _should_ avoid re-storing or other data transfer to the storage
layer, and return the already-stored block directly.

### delete!

Deleting a block removes it from the store. This should return a deferred which
yields true if the block was stored, false if it was not.


## Erasable Protocol

Stores may optionally add support for the `ErasableStore` protocol, which
provides an efficient mechanism for completely removing the backend data. If not
implemented, the `erase!` function will fall back to listing and deleting all
blocks.
