Block Storage
=============

[![CircleCI](https://circleci.com/gh/greglook/blocks.svg?style=shield&circle-token=d652bef14116ac200c225d12b6c7af33933f4c26)](https://circleci.com/gh/greglook/blocks)
[![codecov](https://codecov.io/gh/greglook/blocks/branch/master/graph/badge.svg)](https://codecov.io/gh/greglook/blocks)
[![cljdoc lib](https://img.shields.io/badge/cljdoc-lib-blue.svg)](https://cljdoc.org/d/mvxcvi/blocks/)

This library implements [content-addressable storage](https://en.wikipedia.org/wiki/Content-addressable_storage)
types and protocols for Clojure. Content-addressable storage has several useful properties:

- Data references are abstracted away from the knowledge of where and how the
  blocks are stored, and so can never be 'stale'.
- Blocks are immutable, so there's no concern over having the 'latest version'
  of something - you either have it, or you don't.
- References are _secure_, because a client can re-compute the digest to ensure
  they have received the original data unaltered.
- Synchronizing data between stores only requires enumerating the stored blocks
  in each and exchanging missing ones.
- Data can be structurally shared by different higher-level constructs. For
  example, a file's contents can be referenced by different versions of
  metadata without duplicating the file data.


## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project definition:

[![Clojars Project](http://clojars.org/mvxcvi/blocks/latest-version.svg)](http://clojars.org/mvxcvi/blocks)


## Block Values

A _block_ is a sequence of bytes identified by the cryptographic digest of its
content. All blocks have an `:id` and a `:size` - the block identifier is a
[multihash](//github.com/greglook/clj-multiformats) value, and the size is the
number of bytes in the block content. Blocks may also have a `:stored-at`
value, which is the instant the backing store received the block.

```clojure
=> (require '[blocks.core :as block])

;; Read a block into memory:
=> (def hello (block/read! "hello, blocks!"))
#'user/hello

=> hello
#blocks.data.Block
{:id #multi/hash "hash:sha2-256:d2eef339d508c69fb6e3e99c11c11fc4fc8c035d028973057980d41c7d162684",
 :size 14,
 :stored-at #inst "2019-02-18T07:02:28.751Z"}

=> (:id hello)
#multi/hash "hash:sha2-256:d2eef339d508c69fb6e3e99c11c11fc4fc8c035d028973057980d41c7d162684",

=> (:size hello)
14

;; Write a block to some output stream:
=> (let [baos (java.io.ByteArrayOutputStream.)]
     (block/write! hello baos)
     (String. (.toByteArray baos)))
"hello, blocks!"
```

Internally, blocks either have a buffer holding the data in memory, or a reader
which can be invoked to create new input streams for the block content.  A block
with in-memory content is a _loaded block_ while a block with a reader is a
_lazy block_.

```clojure
=> (block/loaded? hello)
true

;; Create a block from a local file:
=> (def readme (block/from-file "README.md"))
#'user/readme

;; Block is lazily backed by the file on disk:
=> (block/loaded? readme)
false

=> (block/lazy? readme)
true
```

To abstract over the loaded/lazy divide, you can create an input stream over a
block's content using `open`:

```clojure
=> (slurp (block/open hello))
"hello, blocks!"

;; You can also provide a start/end index to get a range of bytes:
=> (with-open [content (block/open readme {:start 0, :end 32})]
     (slurp content))
"Block Storage\n=============\n\n[!["
```

A block's properties and content cannot be changed after construction, but
blocks do support metadata. In order to guard against the content changing in
the underlying storage layer, blocks can be validated by re-reading their
content:

```clojure
;; In-memory blocks will never change:
=> (block/validate! hello)
nil

;; But if the README file backing the second block is changed:
=> (block/validate! readme)
; IllegalStateException Block hash:sha2-256:515c169aa0d95... has mismatched content
;   blocks.core/validate! (core.clj:115)

;; Metadata can be set and queried:
=> (meta (with-meta readme {:baz 123}))
{:baz 123}
```


## Storage Interface

A _block store_ is a system which saves and retrieves block data. Block stores
have a very simple interface: they must store, retrieve, and enumerate the
contained blocks. The simplest type of block storage is a memory store, which is
backed by a map in memory. Another basic example is a store backed by a local
filesystem, where blocks are stored as files in a directory.

The block storage protocol is comprised of five methods:
- `list` - enumerate the stored blocks as a stream
- `stat` - get metadata about a stored block
- `get` - retrieve a block from the store
- `put!` - add a block to the store
- `delete!` - remove a block from the store

These methods are asynchronous operations which return
[manifold](https://github.com/ztellman/manifold) deferred values. If you want
to treat them synchronously, deref the responses immediately.

```clojure
;; Create a new memory store:
=> (require 'blocks.store.memory)
=> (def store (block/->store "mem:-"))
#'user/store

=> store
#blocks.store.memory.MemoryBlockStore {:memory #<Ref@2573332e {}>}

;; Initially, the store is empty:
=> (block/list-seq store)
()

;; Lets put our blocks in the store so they don't get lost:
=> @(block/put! store hello)
#blocks.data.Block
{:id #multi/hash "hash:sha2-256:d2eef339d508c69fb6e3e99c11c11fc4fc8c035d028973057980d41c7d162684",
 :size 14,
 :stored-at #inst "2019-02-18T07:06:43.655Z"}

=> @(block/put! store readme)
#blocks.data.Block
{:id #multi/hash "hash:sha2-256:94d0eb8d13137ebced045b1e7ef48540af81b2abaf2cce34e924ce2cde7cfbaa",
 :size 8597,
 :stored-at #inst "2019-02-18T07:07:06.458Z"}

;; We can `stat` block ids to get metadata without content:
=> @(block/stat store (:id hello))
{:id #multi/hash "hash:sha2-256:94d0eb8d13137ebced045b1e7ef48540af81b2abaf2cce34e924ce2cde7cfbaa",
 :size 14,
 :stored-at #inst "2019-02-18T07:07:06.458Z"}

;; `list` returns the blocks, and has some basic filtering options:
=> (block/list-seq store :algorithm :sha2-256)
(#blocks.data.Block
 {:id #multi/hash "hash:sha2-256:94d0eb8d13137ebced045b1e7ef48540af81b2abaf2cce34e924ce2cde7cfbaa",
  :size 8597,
  :stored-at #inst "2019-02-18T07:07:06.458Z"}
 #blocks.data.Block
 {:id #multi/hash "hash:sha2-256:d2eef339d508c69fb6e3e99c11c11fc4fc8c035d028973057980d41c7d162684",
  :size 14,
  :stored-at #inst "2019-02-18T07:06:43.655Z"})

;; Use `get` to fetch blocks from the store:
=> @(block/get store (:id readme))
#blocks.data.Block
{:id #multi/hash "hash:sha2-256:94d0eb8d13137ebced045b1e7ef48540af81b2abaf2cce34e924ce2cde7cfbaa",
 :size 8597,
 :stored-at #inst "2019-02-18T07:07:06.458Z"}

;; You can also store them directly from a byte source like a file:
=> @(block/store! store (io/file "project.clj"))
#blocks.data.Block
{:id #multi/hash "hash:sha2-256:95344c6acadde09ecc03a7899231001455690f620f31cf8d5bbe330dcda19594",
 :size 2013,
 :stored-at #inst "2019-02-18T07:11:12.879Z"}

=> (def project-hash (:id *1))
#'user/project-hash

;; Use `delete!` to remove blocks from a store:
=> @(block/delete! store project-hash)
true

;; Checking with stat reveals the block is gone:
=> @(block/stat store project-hash)
nil
```

### Implementations

This library comes with a few block store implementations built in:

- `blocks.store.memory` provides an in-memory map of blocks for transient
  block storage.
- `blocks.store.file` provides a simple one-file-per-block store in a local
  directory.
- `blocks.store.buffer` holds blocks in one store, then flushes them to another.
- `blocks.store.replica` stores blocks in multiple backing stores for
  durability.
- `blocks.store.cache` manages two backing stores to provide an LRU cache that
  will stay under a certain size limit.

Other storage backends are provided by separate libraries:

- [blocks-s3](//github.com/greglook/blocks-s3) backed by a bucket in Amazon S3.

These storage backends exist but aren't compatible with 2.X yet:

- [blocks-adl](//github.com/amperity/blocks-adl) backed by Azure DataLake store.
- [blocks-blob](//github.com/amperity/blocks-blob) backed by Azure Blob Storage.
- [blocks-monger](//github.com/20centaurifux/blocks-monger) backed by MongoDB.


## Block Metrics

The `blocks.meter` namespace provides instrumentation for block stores to
measure data flows, call latencies, and other metrics. These measurements are
built around the notion of a _metric event_ and an associated _recording
function_ on the store which the events are passed to. Each event has a
namespaced `:type` keyword, a `:label` associated with the store, and a numeric
`:value`. The store currently measures the call latencies of the storage methods
as well as the flow of bytes into or out of a store's blocks.

To enable metering, set a `::meter/recorder` function on the store. The function
will be called with the store itself and each metric event. The `:label` on each
event is derived from the store - it will use the store's class name or an
explicit `::meter/label` value if available.


## License

This is free and unencumbered software released into the public domain.
See the UNLICENSE file for more information.
