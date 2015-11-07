Block Storage
=============

[![Dependency Status](https://www.versioneye.com/clojure/mvxcvi:blocks/badge.svg)](https://www.versioneye.com/clojure/mvxcvi:blocks)
[![Build Status](https://travis-ci.org/greglook/blocks.svg?branch=develop)](https://travis-ci.org/greglook/blocks)
[![Coverage Status](https://coveralls.io/repos/greglook/blocks/badge.svg?branch=develop&service=github)](https://coveralls.io/github/greglook/blocks?branch=develop)
[![Join the chat at https://gitter.im/greglook/blocks](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/greglook/blocks)

This library implements [content-addressable storage](https://en.wikipedia.org/wiki/Content-addressable_storage)
types and protocols for Clojure. Content-addressable storage has several useful properties:

- Data references are abstracted away from the knowledge of where and how the
  data is stored, and so can never be 'stale'.
- Data is immutable, so there's no concern over having the 'latest version' of
  something - you either have it, or you don't.
- References are _secure_, because a client can re-compute the digest to ensure
  they have received the original data unaltered.
- Synchronizing data between stores only requires enumerating the stored blocks
  in each and exchanging missing ones.
- Content can be structurally shared by different higher-level constructs. For
  example, a file's contents can be referenced by different versions of
  metadata without duplicating the file data.

Specifically, this tries to be compatible with the
[ipfs](//ipfs.io) block storage layer.

## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project definition:

[![Clojars Project](http://clojars.org/mvxcvi/blocks/latest-version.svg)](http://clojars.org/mvxcvi/blocks)

## Block Values

A _block_ is a sequence of bytes identiied by the cryptographic digest of its
content. Digests are represented as
[multihash](//github.com/greglook/clj-multihash) values, which support many
different hashing algorithms. A given multihash securely identifies a specific
immutable piece of data, as any change in the content results in a different
identifier.

```clojure
{:id #data/hash "sha256:9e663220c60fb814a09f4dc1ecb28222eaf2d647174e60554272395bf776495a"
 :content #bin "iJwEAAECAAYFAlNMwWMACgkQkjscHEOSMYqORwQAnfJw0AX/6zabotV6yf2LbuwwJ6Mr+..."}
```

## Storage Interface

A _block store_ is a system which saves and retrieves block data. block stores
support a very simple interface; they must store, retrieve, and enumerate the
contained blocks. The simplest type of block storage is a hash map in memory.
Another simple example is a store backed by a local file system, where blocks are
stored as files.

The block storage interface is straightforward:
- `list` - enumerate the stored blocks
- `stat` - get metadata about a stored block
- `get` - return the bytes stored for a block
- `put!` - store a some bytes as a block
- `delete!` - remove a block from the store

## License

This is free and unencumbered software released into the public domain.
See the UNLICENSE file for more information.
