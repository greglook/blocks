Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

...

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

[Unreleased]: https://github.com/greglook/blocks/compare/0.7.1...HEAD
[0.7.1]: https://github.com/greglook/blocks/compare/0.7.0...0.7.1
[0.7.0]: https://github.com/greglook/blocks/compare/0.6.1...0.7.0
[0.6.1]: https://github.com/greglook/blocks/compare/0.6.0...0.6.1
[0.6.0]: https://github.com/greglook/blocks/compare/0.5.0...0.6.0
[0.5.0]: https://github.com/greglook/blocks/compare/0.4.2...0.5.0
[0.4.2]: https://github.com/greglook/blocks/compare/0.4.1...0.4.2
[0.4.1]: https://github.com/greglook/blocks/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/greglook/blocks/compare/0.3.0...0.4.0
