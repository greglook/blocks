Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

...

## [2.0.2] - 2019-07-31

### Changed
- Upgrade to Clojure 1.10.1

### Fixed
- File block stores use the platform file separator, so should be compatible
  with Windows and other non-linux systems.
  [#21](//github.com/greglook/blocks/pull/21)

## [2.0.1] - 2019-03-11

### Changed
- Various docstring updates after reviewing generated docs.
- Upgrade multiformats to 0.2.0 for the `multiformats.hash/parse` method.
- Renamed the `blocks-test` project to `blocks-tests` to better reflect the
  test namespace.

## [2.0.0] - 2019-03-05

**This is a major release with a rewritten storage interface.** Much of the
library's API has changed slightly - the core concepts are the same, but now
storage interactions are represented as asynchronous processes using
[manifold](//github.com/ztellman/manifold). Please read the notes here
carefully!

### Block Changes

The first major change is that block values are no longer open maps - they
behave more like opaque types now. Mixing in extra attributes led to some
confusing usage in practice; one example was the way extra attributes affected
equality, meaning you could not test blocks directly for content matches.
Blocks do still support metadata, so any additional information can still be
associated that way.

Second, this version upgrades from the `mvxcvi/multihash` library to the unified
`mvxcvi/multiformats` code. This changes the type of a block `:id` from
`multihash.core.Multihash` to `multiformats.hash.Multihash`, but otherwise the
identifiers have the same semantics and should behave the same way they did
before.

Blocks also have a new first-class attribute `:stored-at` which is a
`java.time.Instant` reflecting the time they were persisted. This does not
affect block equality or hashing, but is generally useful for auditing. It
_does_ impact sorting, so that earlier copies of the same block sort before
older ones.

Finally, block content is no longer represented by separate `reader` and
`content` fields on the block. Now the `content` field contains an
implementation of the new `blocks.data/ContentReader` protocol. This is
implemented for the current `PersistentBytes` values and the "reader function"
approach for lazy blocks. The protocol allows block stores to provide an
efficient mechanism for reading a sub-range of the block content, and will be
useful for any future customizations.

### Storage API Changes

The major change in this version of the library is that all block store methods
are now _asynchronous_. The `stat`, `get`, `store!`, `delete!`, `get-batch!`,
`put-batch!`, `delete-batch!`, `scan`, `erase!`, and `sync` functions now return
manifold deferred values instead of blocking.

Similarly, the `list` store method now returns a manifold stream instead of a
lazy sequence. An asynchronous process places _blocks_ on this stream for
consumption - previously, this was simple stat metadata. If an error occurs, the
store should place the exception on the stream and close it. Existing consumers
can use the `list-seq` wrapper, which returns a lazy sequence consuming from
this stream and behaves similarly to the old list method.
The `list` and `list-seq` query parameters now also accept a `:before` hex
string (in addition to the current `:after`) to halt enumeration at a certain
point.

The `blocks.store/BatchStore` protocol has been removed. It was never used in
practice and few backends could ensure atomicity. Instead, the batch methods are
now wrappers around asynchronous behavior over the normal store methods.

### Store Metrics

The library now includes a `blocks.meter` namespace which provides a common
framework for instrumenting block stores and collecting metrics. Users can opt
into metrics collection by setting a `:blocks.meter/recorder` function on each
store they want to instrument. This function will be called with the store
record and metric events, which include method elapsed times, traffic into and
out of the store, and counts of blocks enumerated by list streams.

### Block Store Changes

In addition to the notes for each store below, note that external block store
libraries like [blocks-s3](//github.com/greglook/blocks-s3) will not be
compatible with this version of the library until they upgrade!

#### BufferBlockStore

- The backing store field is now `primary` instead of `store`.
- The store checks on component startup that both `primary` and `buffer` stores
  are present.
- `clear!` and `flush!` now return deferred values.
- The store supports an arbitrary `predicate` function which can return `false`
  to indicate that a block should be stored directly in the primary store
  instead of being buffered.

#### CachingBlockStore

- On initial state scan, blocks in the cache are prioritized by their
  `:stored-at` attributes so that younger blocks are preferred.
- `reap!` now returns a deferred value.
- Instead of the `max-block-size` field, the caching store now supports an
  arbitrary `predicate` function, which can return `false` to indicate that a
  block should not be cached.

#### ReplicaBlockStore

- The sequence of replica keys is now `replicas` instead of `store-keys`.

#### FileBlockStore

The file block store has seen the most significant changes. Previously, blocks
were stored in subdirectories under the store root, like
`$ROOT/1220abcd/0123...`, with no additional metadata. Now, file stores maintain
a more sophisticated structure under the root. Block directories are now in
`$ROOT/blocks/`, and a `$ROOT/meta.properties` file contains versioning
information to make future extensibility possible. When the store starts, it
will try to detect a v0 layout; if `:auto-migrate?` is truthy on the store, it
will upgrade it to v1, otherwise it will throw an exception.

Another change is that blocks are now written to a temporary file in
`$ROOT/landing/` before being atomically renamed to their final location. This
keeps other clients from seeing partially-written blocks that are still being
stored - something that would have been difficult with the prior layout.

### Other

A few other things changed or were added:

- Added predicate `blocks.core/loaded?` which is the complement of `lazy?`.
- `blocks.core/open` now accepts a map as a second argument instead of a
  three-arity `(open block start end)`. Instead this would now be
  `(open block {:start start, :end end})`
- `blocks.core/validate!` now returns `true` instead of `nil` on success.
- The `blocks.summary` aggregates no longer contain bloom filters; these didn't
  seem to be used in practice, and clients which want that behavior can
  reimplement it without much difficulty.
- The behavior tests in `blocks.store.tests` have moved to a separate subproject
  `mvxcvi/blocks-test` to simplify usage by store implementations.
- Storage tests no longer test the batch methods, since they are no longer
  unique to store types.

## [1.1.0] - 2017-12-24

This release upgrades the library to Clojure 1.9.0.

## [1.0.0] - 2017-11-05

Finally seems like time for a 1.0 release. One very minor breaking change.

### Added
- New predicate `blocks.core/lazy?`.

### Changed
- *BREAKING:* the `:stored-at` metadata on blocks is now returned as a
  `java.time.Instant` instead of a `java.util.Date`.
- `MemoryBlockStore` uses a ref internally instead of an atom.
- Minor dependency version upgrades.
- Generative block store tests are now based on `test.carly`.

### Removed
- *BREAKING:* Blocks no longer implement `IDeref` as a way to get their internal
  content.

## [0.9.1] - 2017-05-17

### Added
- `PersistentBytes` has a `toByteArray` method to return a copy of the byte data
  as a raw array.

## [0.9.0] - 2017-03-31

This release has a couple of breaking changes, detailed below.

### Added
- `PersistentBytes` values support comparison using lexical sorting rules.
- `blocks.core/->store` initializer function to create block stores from URI
  configuration strings.
- `blocks.core/scan` function to produce a summary of the blocks contained in
  the store.
- Summary data functions which provide a count, total size, size histogram, and
  bloom filter for block id membership.
- `blocks.core/sync!` function to copy blocks between stores.
- `ErasableStore` protocol for block stores which support efficient or atomic
  data removal. There's a matching `blocks.core/erase!!` function using it,
  which falls back to deleting the blocks in the store individually.
- Buffer store supports a maximum block size limit. Storing blocks larger than
  the limit will write directly to the backing store, skipping the buffer.

### Changed
- `blocks.store.util` namespace merged into `blocks.store`. This mainly impacts
  store implementers.
- Replica store construction changed to make them better components. They now
  take a vector of keys, rather than stores.

### Removed
- Dropped `EnumerableStore` protocol and `enumerate` method. No usages have
  come up requiring it and it's easy to replace in the non-optimized case.

## [0.8.0] - 2016-08-14

### Changed
- All block stores were renamed to consistently end with `BlockStore`.
- All block store constructors are similarly renamed, e.g. `file-block-store`.
- Store constructors all follow a component pattern with variadic options.
- Blocks no longer implement `IPending`, because it is not appropriate to treat
  immutable values as asynchronous references.

### Added
- Multimethod `blocks.store/initialize` for constructing block stores from a
  URI string. The method is dispatched by URI scheme.

### Removed
- Problematic namespace `blocks.data.conversions`, which defined conversion
  paths for the `byte-streams` library.


## [0.7.1] - 2016-07-25

### Fixed
- Small number of reflection warnings in `blocks.data/clean-block`.
- Improved generative store tests.

## [0.7.0] - 2016-04-27

### Changed
- Upgrade `mvxcvi/multihash` to 2.0.0.
- Small efficiency improvements to block construction.
- Memory stores strip metadata and extra attributes from blocks `put!` in them.
- Integration tests in `blocks.store.tests` now build generative sequences of
  operations and apply them to the store under test.

### Fixed
- Reading an empty content source returns `nil` instead of an empty block.
- Check that the argument to `block/put!` is actually a block.
- Handle block merging in `block/put!` instead of requiring stores to do it.
- File stores correctly return `false` when deleting a block which is not
  contained in the store.

## [0.6.1] - 2016-01-25

### Added
- Expand `PersistentBytes` equality to include primitive byte arrays and
  `ByteBuffer` objects which have identical content.

### Fixed
- `block/store!` will no longer try to store empty files.

## [0.6.0] - 2016-01-10

### Added
- Add logical 'replica' and 'buffer' stores.
  [#2](//github.com/greglook/blocks/issues/2)
- Add a second arity to `block/open` to read a sub-range of the content in a
  block by specifying starting and ending bytes.
  [#3](//github.com/greglook/blocks/issues/3)
- Add protocol for batch block operations.
  [#5](//github.com/greglook/blocks/issues/5)
- Add protocol for efficient block enumeration.
  [#8](//github.com/greglook/blocks/issues/8)

### Changed
- Remove extra 'Block' from many store record names, for example
  `FileBlockStore` to `FileStore`.
- Change file store to match IPFS file repo behavior by restricting it to a
  single intermediate directory level.
- Move block store protocols to `blocks.store` namespace, with wrappers in
  `blocks.core`.

### Fixed
- `validate!` now checks the size of lazy blocks by using a counting input
  stream wrapper.

## [0.5.0] - 2015-11-14

### Added
- `blocks.store.cache` namespace with logical caching block store
  implementation.

### Changed
- `random-bytes` and `random-hex` now generate fixed-width data.

## [0.4.2] - 2015-11-13

### Changed
- File store now locks itself during `put!`, `delete!`, and `erase!` to
  prevent concurrent modifications.
- `select-stats` moved from core to util namespace.

### Fixed
- File store skips over malformed files instead of throwing an exception.

## [0.4.1] - 2015-11-12

### Changed
- Rename `:origin` block stat to `:source`.
- Switch argument order in `read-block` for consistency.

### Fixed
- `put!` retains extra attributes and metadata on the block argument in the
  returned block.
- Expanded integration test suite to cover `stat` and `get` on non-existent
  blocks and `put!` merging.

## [0.4.0] - 2015-11-10

Lots of high-level library changes! `blocks.data.Block` is now a custom type to
protect immutable fields like `:id` and `:size` and support the `IPending`
interface.

### Added
- Blocks can be either _literal_ or _lazy_ to support larger block sizes.
- A standard set of BlockStore integration tests are available in the
  `blocks.store.tests` namespace.

### Changed
- `BlockStore` methods `enumerate` and `get*` changed to `-list` and `-get`,
  respectively.
- `list` now returns a sequence of block stats, rather than just multihashes.
- Blocks returned by `get` and `put!` add stat information as metadata.
- File stores now keep blocks in a manner compatible with IPFS.

## 0.3.0 - 2015-11-03

Initial project release.

[Unreleased]: https://github.com/greglook/blocks/compare/2.0.2...HEAD
[2.0.2]: https://github.com/greglook/blocks/compare/2.0.1...2.0.2
[2.0.1]: https://github.com/greglook/blocks/compare/2.0.0...2.0.1
[2.0.0]: https://github.com/greglook/blocks/compare/1.1.0...2.0.0
[1.1.0]: https://github.com/greglook/blocks/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/greglook/blocks/compare/0.9.1...1.0.0
[0.9.1]: https://github.com/greglook/blocks/compare/0.9.0...0.9.1
[0.9.0]: https://github.com/greglook/blocks/compare/0.8.0...0.9.0
[0.8.0]: https://github.com/greglook/blocks/compare/0.7.1...0.8.0
[0.7.1]: https://github.com/greglook/blocks/compare/0.7.0...0.7.1
[0.7.0]: https://github.com/greglook/blocks/compare/0.6.1...0.7.0
[0.6.1]: https://github.com/greglook/blocks/compare/0.6.0...0.6.1
[0.6.0]: https://github.com/greglook/blocks/compare/0.5.0...0.6.0
[0.5.0]: https://github.com/greglook/blocks/compare/0.4.2...0.5.0
[0.4.2]: https://github.com/greglook/blocks/compare/0.4.1...0.4.2
[0.4.1]: https://github.com/greglook/blocks/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/greglook/blocks/compare/0.3.0...0.4.0
