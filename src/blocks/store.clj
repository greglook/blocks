(ns blocks.store
  "Block storage protocols. Typically, clients of the library should use the
  API wrapper functions in `blocks.core` instead of using these methods
  directly."
  (:require
    [bigml.sketchy.bloom :as bloom]
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
    [store]
    "Returns a lazy sequence of stored blocks. Blocks are expliticly **not**
    returned in any defined order; it is assumed that the store will enumerate
    them in the most efficient order available."))



;; ## Store Construction

(defmulti initialize
  "Constructs a new block store from a URI by dispatching on the scheme. The
  store will be returned in an initialized but not started state."
  (fn dispatch
    [uri]
    (.getScheme (java.net.URI. uri))))


(defmethod initialize :default
  [uri]
  (throw (IllegalArgumentException.
           (str "Unsupported block-store URI scheme: " (pr-str uri)))))



;; ## Storage Summaries

(defn init-summary
  []
  {:count 0
   :size 0
   :sizes {}  ; increment n for each block, where 2^n <= size < 2^(n+1)
   :membership (bloom/create 10000 0.01)})


(defn size->bucket
  "Assigns a block size to an exponential histogram bucket. Given a size `s`,
  returns `n` such that `2^n <= s < 2^(n+1)`."
  [size]
  (loop [s size
         n 0]
    (if (pos? s)
      (recur (bit-shift-right s 1) (inc n))
      n)))


(defn bucket->range
  "Returns a vector with the boundaries which a given size bucket covers."
  [n]
  [(bit-shift-left 1 (dec n))
   (bit-shift-left 1 n)])


(defn update-summary
  "Update the storage summary with the stats from the given block."
  [summary block]
  (-> summary
      (update :count inc)
      (update :size + (:size block))
      (update :sizes update (size->bucket (:size block)) (fnil inc 0))
      (update :membership bloom/insert (:id block))))


(defn merge-summaries
  "Merge two storage summaries together."
  [a b]
  (-> a
      (update :count + (:count b))
      (update :size + (:size b))
      (update :sizes (partial merge-with +) (:sizes b))
      (update :membership bloom/merge (:membership b))
      (merge (dissoc b :count :size :sizes :membership))))


(defn probably-contains?
  "Uses a summary map to check whether the the store (probably) contains the
  given block identifier. False positives may be possible, but false negatives
  are not."
  [summary id]
  (bloom/contains? (:membership summary) id))
