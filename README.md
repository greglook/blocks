Block Storage
=============

[![Dependency Status](https://www.versioneye.com/user/projects/5639b2761d47d40015000018/badge.svg?style=flat)](https://www.versioneye.com/user/projects/5639b2761d47d40015000018)
[![Build Status](https://travis-ci.org/greglook/blocks.svg?branch=develop)](https://travis-ci.org/greglook/blocks)
[![Coverage Status](https://coveralls.io/repos/greglook/blocks/badge.svg?branch=develop&service=github)](https://coveralls.io/github/greglook/blocks?branch=develop)
[![Join the chat at https://gitter.im/greglook/blocks](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/greglook/blocks)

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

; Read a block into memory.
=> (def b1 (block/read! "hello, blocks!"))
#'user/b1

=> b1
#blocks.data.Block
{:id #data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR",
 :size 14}

=> (:id b1)
#data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR"

=> (:size b1)
14
```

Internally, blocks either have a buffer holding the data in memory, or a reader
function which can be invoked to create new input streams for the block content.
Blocks can be treated as pending values; a block with in-memory content is
considered a _literal block_ for which `realized?` returns true, while a block
with a reader function is a _lazy block_ and `realized?` will return false.
Dereferencing a realized block returns its content, while lazy blocks will give
`nil`.

```clojure
=> (realized? b1)
true

=> @b1
#<blocks.data.PersistentBytes@7dde3f9b PersistentBytes[size=14]>

=> (def b2 (block/from-file "README.md"))
#'user/b2

=> (realized? b2)
false

=> @b2
nil
```

To abstract over this, you can generically open an input stream over a block's
content:

```clojure
=> (slurp (block/open b1))
"hello, blocks!"

; Ideally you should use with-open to ensure the stream is closed:
=> (subs (with-open [content (block/open b2)] (slurp content)) 0 32)
"Block Storage\n=============\n\n[!["
```

A block's `:id`, `:size`, and content cannot be changed after construction, so
clients can be relatively certain that the block's id is valid. Blocks support
metadata and may have additional attributes associated with them, similar to
Clojure records.

```clojure
=> (assoc b1 :id :foo)
; IllegalArgumentException Block :id cannot be changed  blocks.data.Block (data.clj:151)

=> (assoc b1 :foo "bar")
#blocks.data.Block
{:foo "bar",
 :id #data/hash "QmcY3evpwX8DU4W5FsXrV4rwiHgw56HWK5g7i1zJNW6WqR",
 :size 14}

=> (:foo *1)
"bar"

=> (meta (with-meta b2 {:baz 123}))
{:baz 123}
```

## Storage Interface

A _block store_ is a system which saves and retrieves block data. Block stores
support a very simple interface; they must store, retrieve, and enumerate the
contained blocks. The simplest type of block storage is a hash map in memory.
Another simple example is a store backed by a local file system, where blocks are
stored as files in a directory.

The block storage protocol is comprised of five methods:
- `list` - enumerate the stored blocks
- `stat` - get metadata about a stored block
- `get` - return the bytes stored for a block
- `put!` - store a some bytes as a block
- `delete!` - remove a block from the store

## License

This is free and unencumbered software released into the public domain.
See the UNLICENSE file for more information.
