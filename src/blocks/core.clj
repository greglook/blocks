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
  (:refer-clojure :exclude [get list sync])
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [blocks.summary :as sum]
    [byte-streams :as bytes]
    [clojure.java.io :as io]
    [clojure.set :as set]
    [clojure.string :as str]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    (blocks.data
      Block
      PersistentBytes)
    java.io.File
    multihash.core.Multihash
    org.apache.commons.io.input.CountingInputStream))


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

(defn lazy?
  "Returns true if the given block loads its content lazily. Returns false if
  all of the block's content is loaded in memory."
  [^Block block]
  (nil? (.content block)))


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
   [block]
   (data/content-stream block nil nil))
  (^java.io.InputStream
   [block start end]
   (when-not (and (integer? start)
                  (integer? end)
                  (not= start end)
                  (<= 0 start end (:size block)))
     (throw (IllegalArgumentException.
              (format "Range bounds must be distinct integers within block bounds: [0, %d]"
                      (:size block)))))
   (data/content-stream block start end)))


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
  "Returns a loaded version of the given block. If the block is lazy, the
  stream is read into memory and returned as a new block. If the block is
  already loaded, it is returned unchanged.

  The returned block will have the same extra attributes and metadata as the one
  given."
  [^Block block]
  (if (lazy? block)
    (let [content (with-open [stream (open block)]
                    (bytes/to-byte-array stream))]
      (Block. (:id block)
              (count content)
              (PersistentBytes/wrap content)
              nil
              (._attrs block)
              (meta block)))
    ; Block is already loaded.
    block))


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
      (when-not (digest/test id stream)
        (throw (IllegalStateException.
                 (str "Block " id " has mismatched content"))))
      (when (not= size (.getByteCount stream))
        (throw (IllegalStateException.
                 (str "Block " id " reports size " size " but has actual size "
                      (.getByteCount stream))))))))



;; ## Storage API

(defn ->store
  "Constructs a new block store from a URI by dispatching on the scheme. The
  store will be returned in an initialized (but not started) state."
  [uri]
  (store/initialize uri))


(defn stat
  "Returns a map with an `:id` and `:size` but no content. The returned map
  may contain additional data like the date stored. Returns nil if the store
  does not contain the identified block."
  [store id]
  (when id
    (when-not (instance? Multihash id)
      (throw (IllegalArgumentException.
               (str "Id value must be a multihash, got: " (pr-str id)))))
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
  (when block
    (when-not (instance? Block block)
      (throw (IllegalArgumentException.
               (str "Argument must be a block, got: " (pr-str block)))))
    (data/merge-blocks block (store/-put! store block))))


(defn store!
  "Stores content from a byte source in a block store and returns the block
  record.

  If the source is a file, it will be streamed into the store. Otherwise, the
  content is read into memory, so this may not be suitable for large sources."
  ([store source]
   (store! store source default-algorithm))
  ([store source algorithm]
   (let [block (if (instance? File source)
                 (from-file source algorithm)
                 (read! source algorithm))]
     (when (pos? (:size block))
       (put! store block)))))


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
  stored blocks, in no particular order.

  This is not guaranteed to be an atomic operation; readers may be able to see
  the store in a partially-updated state."
  [store blocks]
  (validate-collection-of Block blocks)
  (if-let [blocks (seq (remove nil? blocks))]
    (if (satisfies? store/BatchingStore store)
      (store/-put-batch! store blocks)
      (mapv (partial put! store) blocks))
    []))


(defn delete-batch!
  "Removes a batch of blocks from the store, identified by a collection of
  multihashes. Returns a set of ids for the blocks which were found and
  deleted.

  This is not guaranteed to be an atomic operation; readers may be able to see
  the store in a partially-deleted state."
  [store ids]
  (validate-collection-of Multihash ids)
  (if (satisfies? store/BatchingStore store)
    (set (store/-delete-batch! store ids))
    (set (filter (partial delete! store) ids))))



;; ## Storage Utilities

(defn erase!!
  "Completely removes any data associated with the store. After this call, the
  store should be empty. This is not guaranteed to be an atomic operation!"
  [store]
  (if (satisfies? store/ErasableStore store)
    (store/-erase! store)
    (run! (comp (partial delete! store) :id)
          (store/-list store nil))))


(defn scan
  "Scans all the blocks in the store, building up a store-level summary. If
  given, the predicate function will be called with each block in the store.
  By default, all blocks are scanned."
  ([store]
   (scan store nil))
  ([store p]
   (-> (store/-list store nil)
       (cond->> p (filter p))
       (->> (reduce sum/update (sum/init))))))


(defn sync!
  "Synchronize blocks from the `source` store to the `dest` store. Returns a
  summary of the copied blocks. Options may include:

  - `:filter` a function to run on every block stats before it is copied to the
    `dest` store. If the function returns a falsey value, the block will not be
    copied."
  [source dest & {:as opts}]
  (-> (store/missing-blocks
        (store/-list source nil)
        (store/-list dest nil))
      (cond->>
        (:filter opts) (filter (:filter opts)))
      (->> (reduce
             (fn copy-block
               [summary stat]
               (put! dest (get source (:id stat)))
               (sum/update summary stat))
             (sum/init)))))
