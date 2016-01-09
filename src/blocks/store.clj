(ns blocks.store
  "Block storage protocols. Typically, clients of the library should use the
  API wrapper functions in `blocks.core` instead of using these methods
  directly."
  (:require
    [multihash.core :as multihash]))


;; ## Storage Protocols

(defprotocol BlockStore
  "Protocol for content-addressable storage keyed by multihash identifiers."

  (-stat
    [store id]
    "Returns a map with an `:id` and `:size` but no content. The returned map
    may contain additional data like the date stored. Returns nil if the store
    does not contain the identified block.")

  (-list
    [store opts]
    "Lists the blocks contained in the store. Returns a lazy sequence of stat
    metadata about each block. The stats should be returned in order sorted by
    multihash id. See `list` for the supported options.")

  (-get
    [store id]
    "Returns the identified block if it is stored, otherwise nil. The block
    should include stat metadata. Typically clients should use `get` instead,
    which validates arguments and the returned block record.")

  (-put!
    [store block]
    "Saves a block into the store. Returns the block record, updated with stat
    metadata.")

  (-delete!
    [store id]
    "Removes a block from the store. Returns true if the block was stored."))


(defprotocol BatchingStore
  "Protocol for stores which can perform optimized batch operations on blocks.
  Note that none of the methods in this protocol guarantee an ordering on the
  returned collections."

  (-get-batch
    [store ids]
    "Retrieves a batch of blocks identified by a collection of multihashes.
    Returns a sequence of the requested blocks which are found in the store.")

  (-put-batch!
    [store blocks]
    "Saves a collection of blocks to the store. Returns a collection of the
    stored blocks.")

  (-delete-batch!
    [store ids]
    "Removes multiple blocks from the store, identified by a collection of
    multihashes. Returns a collection of multihashes for the deleted blocks."))


(defprotocol BlockEnumerator
  "An enumerator provides a way to efficiently iterate over all the stored
  blocks."

  (-enumerate
    [store marker]
    "Returns a lazy sequence of stored blocks. Blocks are expliticly **not**
    returned in any defined order; it is assumed that the store will enumerate
    them in the most efficient order available.

    If `marker` is nil, all blocks should be returned in the sequence.
    Otherwise, it is up to the implementation to resume the enumeration at the
    marked block.

    The returned blocks should have attached metadata giving a _marker_ for the
    stream under the key `:block/marker`. This value is implementation-dependent,
    and can be used to resume the stream from a particular position."))
