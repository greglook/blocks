Block Storage
=============

[![CircleCI](https://circleci.com/gh/greglook/blocks.svg?style=shield&circle-token=d652bef14116ac200c225d12b6c7af33933f4c26)](https://circleci.com/gh/greglook/blocks)
[![codecov](https://codecov.io/gh/greglook/blocks/branch/develop/graph/badge.svg)](https://codecov.io/gh/greglook/blocks)
[![API codox](https://img.shields.io/badge/doc-API-blue.svg)](https://greglook.github.io/blocks/api/)
[![marginalia docs](https://img.shields.io/badge/doc-marginalia-blue.svg)](https://greglook.github.io/blocks/marginalia/uberdoc.html)

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

This library aims for compatibility with the [ipfs](//ipfs.io) block storage
layer.

## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project definition:

[![Clojars Project](http://clojars.org/mvxcvi/blocks/latest-version.svg)](http://clojars.org/mvxcvi/blocks)

## Block Values

A _block_ is a sequence of bytes identified by the cryptographic digest of its
content. All blocks have an `:id` and `:size`. The block identifier is a
[multihash](//github.com/greglook/clj-multihash) value, and the size is the
number of bytes in the block content.

```clojure
=> (require '[blocks.core :as block])

; Read a block into memory:
=> (def hello (block/read! "hello, blocks!"))
#'user/hello

=> hello
#blocks.data.Block
{:id #data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR",
 :size 14}

=> (:id hello)
#data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR"

=> (:size hello)
14

; Write a block to some output stream:
=> (let [baos (java.io.ByteArrayOutputStream.)]
     (block/write! hello baos)
     (String. (.toByteArray baos)))
"hello, blocks!"
```

Internally, blocks either have a buffer holding the data in memory, or a reader
function which can be invoked to create new input streams for the block content.
A block with in-memory content is a _literal block_ while a block with a reader
is a _lazy block_. Dereferencing a block will return `nil` if it is lazy.

```clojure
; Create a block from a local file:
=> (def readme (block/from-file "README.md"))
#'user/readme

; Block is lazily backed by the file on disk:
=> @readme
nil
```

To abstract over the literal/lazy divide, you can create an input stream over a
block's content using `open`:

```clojure
=> (slurp (block/open hello))
"hello, blocks!"

; You can also provide a start/end index to get a range of bytes:
=> (with-open [content (block/open readme 0 32)]
     (slurp content))
"Block Storage\n=============\n\n[!["
```

A block's `:id`, `:size`, and content cannot be changed after construction, so
clients can be relatively certain that the block's id is valid. Blocks support
metadata and may have additional attributes associated with them, similar to
Clojure records.

```clojure
; The block id and size are not changeable:
=> (assoc hello :id :foo)
; IllegalArgumentException Block :id cannot be changed
;   blocks.data.Block (data.clj:151)

; If you're paranoid, you can validate blocks by rehashing the content:
=> (block/validate! hello)
nil

; But if the README file backing the second block is changed:
=> (block/validate! readme)
; IllegalStateException Block hash:sha2-256:515c169aa0d95... has mismatched content
;   blocks.core/validate! (core.clj:115)

; Other attributes are associative:
=> (assoc hello :foo "bar")
#blocks.data.Block
{:foo "bar",
 :id #data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR",
 :size 14}

=> (:foo *1)
"bar"

; Metadata can be set and queried:
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
- `stat` - get metadata about a stored block
- `list` - enumerate the stored blocks
- `get` - return the bytes stored for a block
- `put!` - store a some bytes as a block
- `delete!` - remove a block from the store

```clojure
; Create a new memory store:
=> (def store (block/->store "mem:-"))
#'user/store

=> store
#blocks.store.memory.MemoryBlockStore {:memory #<Atom@2573332e {}>}

; Initially, the store is empty:
=> (block/list store)
()

; Lets put our blocks in the store so they don't get lost:
=> (block/put! store hello)
#blocks.data.Block
{:id #data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR",
 :size 14}

=> (block/put! store readme)
#blocks.data.Block
{:id #data/hash "QmVBYJ7poFrvwp1aySGtyfuh6sNz5u975hs5XGTsj7zLow",
 :size 8415}

; We can `stat` block ids to get metadata without content:
=> (block/stat store (:id hello))
{:id #data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR",
 :size 14,
 :stored-at #inst "2015-11-11T21:06:00.112-00:00"}

; `list` returns the same metadata, and has some basic filtering options:
=> (block/list store :algorithm :sha2-256)
({:id #data/hash "QmVBYJ7poFrvwp1aySGtyfuh6sNz5u975hs5XGTsj7zLow",
  :size 8415,
  :stored-at #inst "2015-11-11T21:06:37.931-00:00"}
 {:id #data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR",
  :size 14,
  :stored-at #inst "2015-11-11T21:06:00.112-00:00"})

; Use `get` to fetch blocks from the store:
=> (block/get store (:id readme))
#blocks.data.Block
{:id #data/hash "QmVBYJ7poFrvwp1aySGtyfuh6sNz5u975hs5XGTsj7zLow",
 :size 8415}

; Returned blocks may have storage stats as metadata:
=> (block/meta-stats *1)
{:stored-at #inst "2015-11-11T21:06:37.931-00:00"}

; You can also store them directly from a byte source like a file:
=> (block/store! store (io/file "project.clj"))
#blocks.data.Block
{:id #data/hash "Qmd3NMig5YeLKR13q5vV1fy55Trf3WZv1qFNdtpRw7JwBm",
 :size 1221}

=> (def project-hash (:id *1))
#'user/project-hash

; Use `delete!` to remove blocks from a store:
=> (block/delete! store project-hash)
true

; Checking with stat reveals the block is gone:
=> (block/stat store project-hash)
nil
```

## Implementations

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

- [blocks-s3](//github.com/greglook/blocks-s3) provides storage backed by a
  bucket in Amazon S3.

## License

This is free and unencumbered software released into the public domain.
See the UNLICENSE file for more information.
