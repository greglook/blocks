Change Log
==========

All notable changes to this project will be documented in this file, which
follows the conventions of [keepachangelog.com](http://keepachangelog.com/).
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

...

## [0.4.0] - 2015-11-10

Lots of high-level (breaking) library changes! `blocks.data.Block` is now a
custom type to protect immutable fields like `:id` and `:size` and support the
`IPending` interface.

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

[Unreleased]: https://github.com/greglook/blocks/compare/0.3.0...HEAD
