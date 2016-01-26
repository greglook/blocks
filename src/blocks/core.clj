(ns blocks.core
  "Block storage API. Functions which may cause IO to occur are marked with
  bangs.

  For example `(read! \"foo\")` doesn't have side-effects, but `(read!
  some-input-stream)` will consume bytes from the stream.

  When blocks are returned from a block store, they may include 'stat' metadata
  about the blocks, including:

  - `:source`      resource location for the block content
  - `:stored-at`   time block was added to the store
  "
  (:refer-clojure :exclude [get list])
  (:require
    [blocks.data :as data]
    [blocks.data.conversions]
    [blocks.store :as store]
    [byte-streams :as bytes]
    [clojure.java.io :as io]
    [clojure.set :as set]
    [clojure.string :as str]
    [multihash.core :as multihash])
  (:import
    (blocks.data
      Block
      PersistentBytes)
    (java.io
      File
      IOException
      InputStream)
    multihash.core.Multihash
    (org.apache.commons.io.input
      BoundedInputStream
      CountingInputStream)))


(def default-algorithm
  "The hashing algorithm used if not specified in functions which create blocks."
  :sha2-256)



;; ## Stat Metadata

(defn with-stats
  "Returns the given block with updated stat metadata."
  [block stats]
  (vary-meta block assoc :block/stats stats))


(defn meta-stats
  "Returns stat information from a block's metadata, if present."
  [block]
  (:block/stats (meta block)))



;; ## Block IO

(defn- bounded-input-stream
  ^java.io.InputStream
  [^InputStream input start end]
  (.skip input start)
  (BoundedInputStream. input (- end start)))


(defn from-file
  "Creates a lazy block from a local file. The file is read once to calculate
  the identifier."
  ([file]
   (from-file file default-algorithm))
  ([file algorithm]
   (let [file (io/file file)
         hash-fn (data/checked-hasher algorithm)
         reader #(io/input-stream file)
         id (hash-fn (reader))]
     (data/lazy-block id (.length file) reader))))


(defn open
  "Opens an input stream to read the contents of the block.

  If `start` and `end` are given, the input stream will only return content
  from the starting index byte to the byte before the end index. For example,
  opening a block with size _n_ with `(open block 0 n)` would return the full
  block contents."
  (^java.io.InputStream
   [^Block block]
   (let [content ^PersistentBytes (.content block)
         reader (.reader block)]
     (cond
       content (.open content)
       reader  (reader)
       :else   (throw (IOException.
                         (str "Cannot open empty block " (:id block)))))))
  (^java.io.InputStream
   [^Block block start end]
   (when-not (and (integer? start) (integer? end)
                  (<= 0 start end (:size block)))
     (throw (IllegalArgumentException.
              (str "Range bounds must be integers within block bounds: ["
                   (pr-str start) ", " (pr-str end) ")"))))
   (let [content ^PersistentBytes (.content block)
         reader (.reader block)]
     (cond
       content (bounded-input-stream (.open content) start end)
       reader  (try
                 (reader start end)
                 (catch clojure.lang.ArityException e
                   ; Native ranged open not supported, use naive approach.
                   (bounded-input-stream (reader) start end)))
       :else   (throw (IOException.
                         (str "Cannot open empty block " (:id block))))))))


(defn read!
  "Reads data into memory from the given source and hashes it to identify the
  block."
  ([source]
   (read! source default-algorithm))
  ([source algorithm]
   (data/read-block algorithm source)))


(defn write!
  "Writes block content to an output stream."
  [block out]
  (with-open [stream (open block)]
    (bytes/transfer stream out)))


(defn load!
  "Returns a literal block corresponding to the block given. If the block is
  lazy, the stream is read into memory and returned as a new literal block. If
  the block is already realized, it is returned unchanged.

  The returned block will have the same extra attributes and metadata as the one
  given."
  [^Block block]
  (if (realized? block)
    block
    (let [content (with-open [stream (open block)]
                    (bytes/to-byte-array stream))]
      (Block. (:id block)
              (count content)
              (PersistentBytes/wrap content)
              nil
              (._attrs block)
              (meta block)))))


(defn validate!
  "Checks a block to verify that it confirms to the expected schema and has a
  valid identifier for its content. Returns nil if the block is valid, or
  throws an exception on any error."
  [block]
  (let [id (:id block)
        size (:size block)]
    (when-not (instance? Multihash id)
      (throw (IllegalStateException.
               (str "Block id is not a multihash: " (pr-str id)))))
    (when (neg? size)
      (throw (IllegalStateException.
               (str "Block " id " has negative size: " size))))
    (with-open [stream (CountingInputStream. (open block))]
      (when-not (multihash/test id stream)
        (throw (IllegalStateException.
                 (str "Block " id " has mismatched content"))))
      (when (not= size (.getByteCount stream))
        (throw (IllegalStateException.
                 (str "Block " id " reports size " size " but has actual size "
                      (.getByteCount stream))))))))



;; ## Storage API

(defn stat
  "Returns a map with an `:id` and `:size` but no content. The returned map
  may contain additional data like the date stored. Returns nil if the store
  does not contain the identified block."
  [store id]
  (when id
    ; TODO: verify that id is a Multihash?
    (store/-stat store id)))


(defn list
  "Enumerates the stored blocks, returning a lazy sequence of block stats sorted
  by id. Iterating over the list may result in additional operations to read
  from the backing data store.

  - `:algorithm`  only return blocks using this hash algorithm
  - `:after`      list blocks whose id (in hex) lexically follows this string
  - `:limit`      restrict the maximum number of results returned
  "
  ([store & opts]
   (let [allowed-keys #{:algorithm :after :limit}
         opts-map (cond
                    (empty? opts) nil
                    (and (= 1 (count opts)) (map? (first opts))) (first opts)
                    :else (apply hash-map opts))
         bad-opts (set/difference (set (keys opts-map)) allowed-keys)]
     (when (not-empty bad-opts)
       (throw (IllegalArgumentException.
                (str "Invalid options passed to list: "
                     (str/join " " bad-opts)))))
     (when-let [algorithm (:algorithm opts-map)]
       (when-not (keyword? algorithm)
         (throw (IllegalArgumentException.
                  (str "Option :algorithm is not a keyword: "
                       (pr-str algorithm))))))
     (when-let [after (:after opts-map)]
       (when-not (and (string? after) (re-matches #"^[0-9a-fA-F]*$" after))
         (throw (IllegalArgumentException.
                  (str "Option :after is not a hex string: "
                       (pr-str after))))))
     (when-let [limit (:limit opts-map)]
       (when-not (and (integer? limit) (pos? limit))
         (throw (IllegalArgumentException.
                  (str "Option :limit is not a positive integer: "
                       (pr-str limit))))))
     (store/-list store opts-map))))


(defn get
  "Loads content for a multihash and returns a block record. Returns nil if no
  block is stored for that id.

  The returned block is checked to make sure the id matches the requested
  multihash."
  [store id]
  (when-not (instance? Multihash id)
    (throw (IllegalArgumentException.
             (str "Id value must be a multihash, got: " (pr-str id)))))
  (when-let [block (store/-get store id)]
    (when-not (= id (:id block))
      (throw (RuntimeException.
               (str "Asked for block " id " but got " (:id block)))))
    block))


(defn put!
  "Saves a block into the store. Returns the block record, updated with stat
  metadata."
  [store block]
  ; TODO: verify that block is a Block?
  (when block
    (store/-put! store block)))


(defn store!
  "Stores content from a byte source in a block store and returns the block
  record.

  If the source is a file, it will be streamed into the store. Otherwise, the
  content is read into memory, so this may not be suitable for large sources."
  ; TODO: protocol for efficient storage? `store/receive!` maybe?
  ; May need a protocol for turning a value into a block, as well.
  ([store source]
   (store! store source default-algorithm))
  ([store source algorithm]
   (let [block (if (instance? File source)
                 (from-file source algorithm)
                 (read! source algorithm))]
     (when (pos? (:size block))
       (store/-put! store block)))))


(defn delete!
  "Removes a block from the store. Returns true if the block was found and
  removed."
  [store id]
  (when id
    (store/-delete! store id)))



;; ## Batch API

(defn- validate-collection-of
  "Validates that the given argument is a collection of a certain class of
  entries."
  [cls xs]
  (when-not (coll? xs)
    (throw (IllegalArgumentException.
             (str "Argument must be a collection: " (pr-str xs)))))
  (when-let [bad-entries (seq (filter (complement (partial instance? cls)) xs))]
    (throw (IllegalArgumentException.
             (str "Collection entries must be " cls " values: "
                  (pr-str bad-entries))))))


(defn get-batch
  "Retrieves a batch of blocks identified by a collection of multihashes.
  Returns a sequence of the requested blocks in no particular order. Any blocks
  which were not found in the store are omitted from the result."
  [store ids]
  (validate-collection-of Multihash ids)
  (if (satisfies? store/BatchingStore store)
    (remove nil? (store/-get-batch store ids))
    (doall (keep (partial get store) ids))))


(defn put-batch!
  "Saves a collection of blocks into the store. Returns a sequence of the
  stored blocks, in no particular order."
  [store blocks]
  (validate-collection-of Block blocks)
  (if (satisfies? store/BatchingStore store)
    (seq (store/-put-batch! store blocks))
    (doall (map (partial put! store) blocks))))


(defn delete-batch!
  "Removes a batch of blocks from the store, identified by a collection of
  multihashes. Returns a set of ids for the blocks which were found and
  deleted."
  [store ids]
  (validate-collection-of Multihash ids)
  (if (satisfies? store/BatchingStore store)
    (set (store/-delete-batch! store ids))
    (set (filter (partial delete! store) ids))))
