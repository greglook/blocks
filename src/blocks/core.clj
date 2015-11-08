(ns blocks.core
  "Block record and storage protocol functions.

  Blocks have the following primary attributes:

  - `:id`        `Multihash` with the digest identifying the content
  - `:content`   `PersistentBytes` with opaque content

  Blocks may be given other attributes describing their content. When blocks
  are returned from a block store, they may include 'stat' metadata about the
  blocks:

  - `:stored-at`   time block was added to the store
  - `:origin`      resource location for the block
  "
  (:refer-clojure :exclude [get list])
  (:require
    [blocks.data :as data]
    [blocks.data.conversions]
    [byte-streams :as bytes]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    blocks.data.PersistentBytes
    java.io.InputStream
    multihash.core.Multihash))


;; ## Block Functions

(defn with-stats
  "Adds stat information to a block's metadata."
  [block stats]
  (vary-meta block assoc :block/stats stats))


(defn meta-stats
  "Returns stat information from a block's metadata, if present."
  [block]
  (:block/stats (meta block)))


(defn open
  "Opens an input stream to read the content of the block. Returns nil for empty
  blocks."
  ^InputStream
  [^Block block]
  (if-let [content ^PersistentBytes (.content block)]
    (.open content)
    (when-let [reader (.reader block)]
      (reader))))


(defn read!
  "Reads data into memory from the given source and hashes it to identify the
  block. Defaults to sha2-256 if no algorithm is specified."
  ([source]
   (read! source :sha2-256))
  ([source algorithm]
   (data/read-literal-block source algorithm)))


; TODO: deferred-block wrapper


(defn write!
  "Writes block data to an output stream."
  [block sink]
  (when-let [content (open block)]
    (bytes/transfer content sink)))


(defn validate!
  "Checks a block to verify that it confirms to the expected schema and has a
  valid identifier for its content. Returns nil if the block is valid, or
  throws an exception on any error."
  [block]
  (let [id (:id block)]
    (if-let [stream (open block)]
      (when-not (multihash/test id stream)
        (throw (IllegalStateException.
                 (str "Invalid block " id " has mismatched content."))))
      (throw (IllegalArgumentException.
               (str "Cannot validate empty block " id))))))



;; ## Storage Interface

(defprotocol BlockStore
  "Protocol for content-addressable storage keyed by multihash identifiers."

  (-list
    [store opts]
    "Enumerates the ids of the stored blocks with some filtering options. See
    `list` for the supported options. Typically clients should use `list`
    instead.")

  (stat
    [store id]
    "Returns a map with an `:id` and `:size` but no content. The returned map
    may contain additional data like the date stored. Returns nil if the store
    does not contain the identified block.")

  (-get
    [store id]
    "Returns the identified block if it is stored, otherwise nil. The block
    should include stat metadata.

    Typically clients should use `get` instead, which validates returned block
    records.")

  (put!
    [store block]
    "Saves a block into the store. Returns the block record, updated with stat
    metadata.")

  (delete!
    [store id]
    "Removes a block from the store."))


; TODO: BlockStreamable


(defn list
  "Enumerates the stored blocks, returning a sequence of multihash values.
  Stores should support the following options:

  - `:algorithm`  only return hashes using this algorithm
  - `:sorted`     whether to return hashes in sorted order
  - `:after`      start enumerating hashes lexically following this string
  - `:limit`      return up to this many results
  "
  ([store & opts]
   (cond
     (empty? opts)
       (-list store nil)
     (and (= 1 (count opts)) (map? (first opts)))
       (-list store (first opts))
     :else
       (-list store (apply array-map opts)))))


(defn get
  "Loads content for a multihash and returns a block record. Returns nil if no
  block is stored. The block should include stat metadata.

  This function checks the digest of the loaded content against the requested multihash,
  and throws an exception if it does not match."
  [store id]
  (when-not (instance? Multihash id)
    (throw (IllegalArgumentException.
             (str "Id value must be a multihash, got: " (pr-str id)))))
  (when-let [block (-get store id)]
    (validate! block)
    block))


(defn store!
  "Stores content from a byte source in a block store and returns the block
  record. This method accepts any source which can be handled as a byte
  stream by the byte-streams library."
  [store source]
  (when-let [block (read! source)]
    (put! store block)))



;; ## Utility Functions

(defn select-hashes
  "Selects multihash identifiers from a sequence based on some criteria.

  Available options:

  - `:algorithm`  only return hashes using this algorithm
  - `:after`      start enumerating ids lexically following this string
  - `:limit`      return at most 'limit' hashes
  "
  [opts ids]
  ; TODO: this is an awkward way to search the store, think of a better approach
  (let [{:keys [algorithm prefix encoder], :or {encoder multihash/hex}} opts
        after (:after opts prefix)]
    (cond->> ids
      algorithm  (filter #(= algorithm (:algorithm %)))
      after      (drop-while #(pos? (compare after (encoder %))))
      prefix     (take-while #(.startsWith ^String (encoder %) prefix)))))


(defn scan-size
  "Scans the blocks in a store to determine the total stored content size."
  [store]
  (->> (-list store nil)
       (map (comp :size (partial stat store)))
       (reduce + 0N)))
