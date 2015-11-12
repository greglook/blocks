Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

...

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

[Unreleased]: https://github.com/greglook/blocks/compare/0.4.1...HEAD
[0.4.1]: https://github.com/greglook/blocks/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/greglook/blocks/compare/0.3.0...0.4.0
