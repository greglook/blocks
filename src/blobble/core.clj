(ns blobble.core
  "Blob record and storage protocol functions.

  When blobs are returned from a blob store, they may include 'stat' metadata
  about the blobs:

  - `:stat/size`        content size in bytes
  - `:stat/stored-at`   time blob was added to the store
  - `:stat/origin`      resource location for the blob"
  (:refer-clojure :exclude [get list])
  (:require
    [byte-streams :as bytes]
    [multihash.core :as multihash])
  (:import
    java.nio.ByteBuffer
    multihash.core.Multihash))


;; ## Blob Record

;; Blobs have the following two primary attributes:
;;
;; - `:id`        `Multihash` with the digest identifying the content
;; - `:content`   read-only Java `ByteBuffer` with opaque content
;;
;; Blobs may be given other attributes describing their content. This is used
;; by blob stores to note storage-level 'stat' metadata, and in the data layer
;; to hold deserialized values and type information.
(defrecord Blob
  [^Multihash id
   ^ByteBuffer content])


(defn empty-blob
  "Constructs a new blob record with the given multihash identifier and no
  content."
  [id]
  {:pre [(instance? Multihash id)]}
  (Blob. id nil))


(def ^:dynamic *hash-fn*
  "Function to use to produce multihash values."
  multihash/sha2-256)


(defmacro with-algorithm
  "Executes body with the hash algorithm bound to the given function."
  [fn-sym & body]
  `(binding [*hash-fn* ~fn-sym]
     ~@body))


(defn identify
  "Constructs a multihash for the given content."
  [content]
  (when content
    (*hash-fn* content)))



(defn validate!
  "Checks that the identifier of a blob matches the actual digest of the
  content. Throws an exception if the id does not match."
  [blob]
  (let [{:keys [id content]} blob]
    (when-not (multihash/test id content)
      (throw (IllegalStateException.
               (str "Invalid blob with content " content
                    " but id " id))))))


(defn read!
  "Reads data into memory from the given source and hashes it to identify the
  blob. This can handle any source supported by the byte-streams library."
  [source]
  (let [content (bytes/to-byte-array source)]
    (when-not (empty? content)
      (Blob. (identify content)
             (.asReadOnlyBuffer (ByteBuffer/wrap content))))))


(defn write!
  "Writes blob data to a byte stream."
  [blob sink]
  (when-let [content (:content blob)]
    (bytes/transfer content sink)))


(defn open
  "Opens an input stream to read the content of the blob."
  [blob]
  (.rewind ^ByteBuffer (:content blob))
  (bytes/to-input-stream (:content blob)))



;; ## Storage Interface

(defprotocol BlobStore
  "Protocol for content storage keyed by hash identifiers."

  (enumerate
    [store opts]
    "Enumerates the ids of the stored blobs with some filtering options. The
    'list' function provides a nicer wrapper around this protocol method.")

  (stat
    [store id]
    "Returns a blob record with metadata but no content.")

  (get*
    [store id]
    "Loads content for a hash-id and returns a blob record. Returns nil if no
    blob is stored. The blob should include stat metadata.")

  (put!
    [store blob]
    "Saves a blob into the store. Returns the blob record, updated with stat
    metadata.")

  (delete!
    [store id]
    "Removes a blob from the store.")

  (erase!!
    [store]
    "Removes all blobs from the store."))


(defn list
  "Enumerates the stored blobs, returning a sequence of multihashes.
  See `select-ids` for the available query options."
  ([store]
   (enumerate store nil))
  ([store opts]
   (enumerate store opts))
  ([store opt-key opt-val & opts]
   (enumerate store (apply hash-map opt-key opt-val opts))))


(defn get
  "Loads content for a multihash and returns a blob record. Returns nil if no
  blob is stored. The blob should include stat metadata.

  This function checks the digest of the loaded content against the requested multihash,
  and throws an exception if it does not match."
  [store id]
  (when-not (instance? multihash.core.Multihash id)
    (throw (IllegalArgumentException.
             (str "Id value " (pr-str id) " is not a multihash."))))
  (when-let [blob (get* store id)]
    (validate! blob)
    blob))


(defn store!
  "Stores content from a byte source in a blob store and returns the blob
  record. This method accepts any source which can be handled as a byte
  stream by the byte-streams library."
  [store source]
  (when-let [blob (read! source)]
    (put! store blob)))



;; ## Utility Functions

(defn select-ids
  "Selects hash identifiers from a sequence based on input criteria.
  Available options:

  - `:encoder` function to encode multihashes with (default: `multihash/hex`)
  - `:after`   start enumerating ids lexically following this string
  - `:prefix`  only return ids starting with the given string
  - `:limit`   limit the number of results returned"
  [opts ids]
  (let [{:keys [prefix limit]} opts
        encoder (:encoder opts multihash/hex)
        after (:after opts prefix)]
    (cond->> ids
      after  (drop-while #(pos? (compare after (encoder %))))
      prefix (take-while #(.startsWith ^String (encoder %) prefix))
      limit  (take limit))))


(defn size
  "Determine the number of bytes stored in a blob."
  [blob]
  (or
    (when-let [content (:content blob)]
      (.capacity ^ByteBuffer content))
    (:stat/size blob)))


(defn scan-size
  "Scans the blobs in a store to determine the total stored content size."
  [store]
  (reduce + 0 (map (comp :stat/size (partial stat store)) (list store))))
