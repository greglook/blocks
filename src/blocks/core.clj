(ns blocks.core
  "Block record and storage protocol functions.

  Blocks have the following two primary attributes:

  - `:content`   read-only Java `ByteBuffer` with opaque content
  - `:id`        `Multihash` with the digest identifying the content

  Blocks may be given other attributes describing their content. When blocks
  are returned from a block store, they may include 'stat' metadata about the
  blocks:

  - `:stat/size`        content size in bytes
  - `:stat/stored-at`   time block was added to the store
  - `:stat/origin`      resource location for the block"
  (:refer-clojure :exclude [get list])
  (:require
    [byte-streams :as bytes]
    [multihash.core :as multihash])
  (:import
    java.nio.ByteBuffer
    multihash.core.Multihash))


(defn- resolve-hash!
  "Resolves an algorithm designator to a hash function. Throws an exception on
  invalid names or error."
  [algorithm]
  (cond
    (nil? algorithm)
      (throw (IllegalArgumentException.
               "Cannot find hash function without algorithm name"))

    (keyword? algorithm)
      (if-let [hf (get multihash/functions algorithm)]
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

; TODO: ideally, id and content should NOT be overwritable.
(defrecord Block
  [^Multihash id
   ^ByteBuffer content])


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
    (.capacity ^ByteBuffer content)
    (:stat/size block)))


(defn open
  "Opens an input stream to read the content of the block. This is typically
  preferable to directly accessing the content."
  [block]
  (when-let [content (:content block)]
    ; TODO: should this create a separate ByteBuffer to avoid multi-thread headaches?
    ; Need to look at how the input stream gets created.
    (.rewind ^ByteBuffer content)
    (bytes/to-input-stream content)))


(defn read!
  "Reads data into memory from the given source and hashes it to identify the
  block. This can handle any source supported by the byte-streams library.
  Defaults to sha2-256 if no algorithm is specified."
  ([source]
   (read! source :sha2-256))
  ([source algorithm]
   (let [hash-fn (resolve-hash! algorithm)
         content (bytes/to-byte-array source)]
     (when-not (empty? content)
       (Block. (hash-fn content)
               (.asReadOnlyBuffer (ByteBuffer/wrap content)))))))


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
    (when-not (multihash/test id content)
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


(defn scan-size
  "Scans the blocks in a store to determine the total stored content size."
  [store]
  (->> (list store)
       (map (comp size (partial stat store)))
       (reduce + 0N)))
