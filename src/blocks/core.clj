(ns blocks.core
  "Block record and storage protocol functions.

  Blocks have the following two primary attributes:

  - `:id`        `Multihash` with the digest identifying the content
  - `:content`   `PersistentBytes` with opaque content

  Blocks may be given other attributes describing their content. When blocks
  are returned from a block store, they may include 'stat' metadata about the
  blocks:

  - `:stat/size`        content size in bytes
  - `:stat/stored-at`   time block was added to the store
  - `:stat/origin`      resource location for the block"
  (:refer-clojure :exclude [get list])
  (:require
    [blocks.data.conversions]
    [byte-streams :as bytes]
    [multihash.core :as multihash])
  (:import
    blocks.data.PersistentBytes
    java.io.InputStream
    java.nio.ByteBuffer
    multihash.core.Multihash))


;; ## Utility Functions

(defn- resolve-hash!
  "Resolves an algorithm designator to a hash function. Throws an exception on
  invalid names or error."
  [algorithm]
  (cond
    (nil? algorithm)
      (throw (IllegalArgumentException.
               "Cannot find hash function without algorithm name"))

    (keyword? algorithm)
      (if-let [hf (clojure.core/get multihash/functions algorithm)]
        hf
        (throw (IllegalArgumentException.
                 (str "Cannot map algorithm name " algorithm
                      " to a supported hash function"))))

    (ifn? algorithm)
      algorithm

    :else
      (throw (IllegalArgumentException.
               (str "Hash algorithm must be keyword name or direct function, got: "
                    (pr-str algorithm))))))



;; ## Block Record

(defrecord Block
  [^Multihash id
   ^PersistentBytes content])


(defn empty-block
  "Constructs a new block record with the given multihash identifier and no
  content."
  [id]
  (when-not (instance? Multihash id)
    (throw (IllegalArgumentException.
             (str "Block identifier must be a Multihash, got: " (pr-str id)))))
  (Block. id nil))


(defn size
  "Determine the number of bytes stored in a block."
  [block]
  (if-let [content (:content block)]
    (count content)
    (:stat/size block)))


(defn open
  "Opens an input stream to read the content of the block."
  [block]
  (when-let [content (:content block)]
    (.open ^PersistentBytes content)))


(defn read!
  "Reads data into memory from the given source and hashes it to identify the
  block. Defaults to sha2-256 if no algorithm is specified."
  ([source]
   (read! source :sha2-256))
  ([source algorithm]
   (let [hash-fn (resolve-hash! algorithm)
         content (bytes/to-byte-array source)
         id (hash-fn content)]
     (when-not (empty? content)
       (when-not (instance? Multihash id)
         (throw (RuntimeException.
                  (str "Block identifier must be a Multihash, "
                       "hashing algorithm returned: " (pr-str id)))))
       (Block. id (PersistentBytes/wrap content))))))


(defn write!
  "Writes block data to an output stream."
  [block sink]
  (when-let [content (open block)]
    (bytes/transfer content sink)))


(defn validate!
  "Checks that the identifier of a block matches the actual digest of the
  content. Throws an exception if the id does not match."
  [block]
  (let [{:keys [id content]} block]
    (when-not (multihash/test id (.open content))
      (throw (IllegalStateException.
               (str "Invalid block with content " content
                    " but id " id))))))



;; ## Storage Interface

(defprotocol BlockStore
  "Protocol for content storage keyed by hash identifiers."

  (enumerate
    [store opts]
    "Enumerates the ids of the stored blocks with some filtering options. The
    'list' function provides a nicer wrapper around this protocol method.")

  (stat
    [store id]
    "Returns a block record with metadata but no content.")

  (get*
    [store id]
    "Loads content for a hash-id and returns a block record. Returns nil if no
    block is stored. The block should include stat metadata.")

  (put!
    [store block]
    "Saves a block into the store. Returns the block record, updated with stat
    metadata.")

  (delete!
    [store id]
    "Removes a block from the store.")

  (erase!!
    [store]
    "Removes all blocks from the store."))


(defn list
  "Enumerates the stored blocks, returning a sequence of multihashes.
  See `select-ids` for the available query options."
  ([store]
   (enumerate store nil))
  ([store opts]
   (enumerate store opts))
  ([store opt-key opt-val & opts]
   (enumerate store (apply hash-map opt-key opt-val opts))))


(defn get
  "Loads content for a multihash and returns a block record. Returns nil if no
  block is stored. The block should include stat metadata.

  This function checks the digest of the loaded content against the requested multihash,
  and throws an exception if it does not match."
  [store id]
  (when-not (instance? Multihash id)
    (throw (IllegalArgumentException.
             (str "Id value must be a multihash, got: " (pr-str id)))))
  (when-let [block (get* store id)]
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
  - `:encoder`    function to encode multihashes with (default: `hex`)
  - `:after`      start enumerating ids lexically following this string
  - `:prefix`     only return ids starting with the given string"
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
  (->> (list store)
       (map (comp size (partial stat store)))
       (reduce + 0N)))
