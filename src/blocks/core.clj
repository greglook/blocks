(ns blocks.core
  "Core block storage API.

  Functions which may cause side effects or IO are marked with bangs - for
  example `(read! \"foo\")` doesn't have side-effects, but
  `(read!  some-input-stream)` will consume bytes from the stream."
  (:refer-clojure :exclude [get list])
  (:require
    [blocks.data :as data]
    [blocks.meter :as meter]
    [blocks.store :as store]
    [blocks.summary :as sum]
    [byte-streams :as bytes]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.hash :as multihash])
  (:import
    blocks.data.Block
    (java.io
      File
      FileInputStream)
    java.time.Instant
    multiformats.hash.Multihash
    (org.apache.commons.io.input
      CountingInputStream)))


;; ## Utilities

(def default-algorithm
  "The hashing algorithm used if not specified in functions which create blocks."
  :sha2-256)


(defn- args->map
  "Accept arguments and return a map corresponding to the input. Accepts either
  a single map argument or kw-args."
  [args]
  (when (seq args)
    (if (and (= 1 (count args))
             (or (map? (first args))
                 (nil? (first args))))
      (first args)
      (apply hash-map args))))


(defn- hex-string?
  "True if the value is a hexadecimal string."
  [x]
  (and (string? x) (re-matches #"[0-9a-fA-F]*" x)))


(defn- multihash?
  "True if the value is a multihash."
  [x]
  (instance? Multihash x))



;; ## Block IO

(defn loaded?
  "True if the block's content is already loaded into memory."
  [block]
  (data/byte-content? block))


(defn lazy?
  "True if the given block reads its content on-demand."
  [block]
  (not (data/byte-content? block)))


(defn from-file
  "Create a lazy block from a local file. Returns the block, or nil if the file
  does not exist or is empty.

  The file is read once to calculate the identifier."
  ([file]
   (from-file file default-algorithm))
  ([file algorithm]
   (let [file (io/file file)
         hash-fn (data/hasher algorithm)]
     (io!
       (when (and (.exists file) (pos? (.length file)))
         (data/create-block
           (hash-fn (FileInputStream. file))
           (.length file)
           (Instant/ofEpochMilli (.lastModified file))
           (fn reader [] (FileInputStream. file))))))))


(defn open
  "Open an input stream to read the contents of the block.

  If an options map with `:start` or `:end` are given, the input stream will
  only return content from the starting index byte to the byte before the end
  index. For example, opening a block with size _n_ with these options would
  return the full block contents:

      (open block {:start 0, :end n})

  Omitting either boundary will read from the beginning or to the end of the
  block, respectively."
  (^java.io.InputStream
   [block]
   (open block nil))
  (^java.io.InputStream
   [block opts]
   (let [{:keys [start end]} opts]
     (when (and start (or (not (nat-int? start))
                          (<= (:size block) start)))
       (throw (IllegalArgumentException.
                (format "Range start must be an integer within block size %d: %s"
                        (:size block) start))))
     (when (and end (or (not (pos-int? end))
                        (< (:size block) end)))
       (throw (IllegalArgumentException.
                (format "Range end must be an integer within block size %d: %s"
                        (:size block) end))))
     (when (and start end (not (< start end)))
       (throw (IllegalArgumentException.
                (format "Range start %d must be less than range end %d"
                        start end))))
     (io! (data/content-stream block start end)))))


(defn read!
  "Read data into memory from the given source and hash it to identify the
  block."
  ([source]
   (read! source default-algorithm))
  ([source algorithm]
   (io! (data/read-block algorithm source))))


(defn write!
  "Write a block's content to an output stream."
  [block out]
  (with-open [stream (open block)]
    (bytes/transfer stream out)))


(defn load!
  "Ensure the block's content is loaded into memory. Returns a loaded version
  of the given block.

  If the block is lazy, the stream is read into memory and returned as a new
  block. If the block is already loaded, it is returned unchanged. The returned
  block will have the same metadata as the one given."
  [block]
  (io!
    (if (lazy? block)
      (with-meta
        (data/read-block
          (:algorithm (:id block))
          (data/content-stream block nil nil))
        (meta block))
      block)))


(defn validate!
  "Check a block to verify that it has the correct identifier and size for its
  content. Returns true if the block is valid, or throws an exception on any
  error."
  [block]
  (let [id (:id block)
        size (:size block)]
    (when-not (multihash? id)
      (throw (ex-info
               (str "Block id is not a multihash: " (pr-str id))
               {:id id})))
    (when-not (pos-int? size)
      (throw (ex-info
               (str "Block " id " has an invalid size: " (pr-str size))
               {:id id, :size size})))
    (with-open [stream (CountingInputStream. (open block))]
      (let [hash-fn (data/hasher (:algorithm id))
            actual-id (hash-fn stream)
            actual-size (.getByteCount stream)]
        (when (not= id actual-id)
          (throw (ex-info
                   (str "Block " id " has mismatched id and content")
                   {:id id, :actual-id actual-id})))
        (when (not= size actual-size)
          (throw (ex-info
                   (str "Block " id " reports size " size
                        " but has actual size " actual-size)
                   {:id id, :size size, :actual-size actual-size})))))
    true))



;; ## Storage API

(defn ->store
  "Constructs a new block store from a URI by dispatching on the scheme. The
  store will be returned in an initialized (but not started) state."
  [uri]
  (store/initialize uri))


(defn list
  "Enumerate the stored blocks, returning a stream of blocks ordered by their
  multihash id. The store will continue listing blocks until the stream is
  closed or there are no more matching blocks to return.

  - `:algorithm`
    Only return blocks identified by this hash algorithm.
  - `:after`
    Return blocks whose id (in hex) lexically follows this string. A multihash
    may also be provided and will be coerced to hex.
  - `:before`
    Return blocks whose id (in hex) lexically precedes this string. A multihash
    may also be provided and will be coerced to hex.
  - `:limit`
    Restrict the maximum number of blocks returned on the stream."
  [store & opts]
  (let [opts (args->map opts)
        opts (merge
               ; Validate algorithm option.
               (when-let [algorithm (:algorithm opts)]
                 (if (keyword? algorithm)
                   {:algorithm algorithm}
                   (throw (IllegalArgumentException.
                            (str "Option :algorithm is not a keyword: "
                                 (pr-str algorithm))))))
               ; Validate 'after' boundary.
               (when-let [after (:after opts)]
                 (cond
                   (hex-string? after)
                   {:after (str/lower-case after)}

                   (multihash? after)
                   {:after (multihash/hex after)}

                   :else
                   (throw (IllegalArgumentException.
                            (str "Option :after is not a hex string or multihash: "
                                 (pr-str after))))))
               ; Validate 'before' boundary.
               (when-let [before (:before opts)]
                 (cond
                   (hex-string? before)
                   {:before (str/lower-case before)}

                   (multihash? before)
                   {:before (multihash/hex before)}

                   :else
                   (throw (IllegalArgumentException.
                            (str "Option :before is not a hex string or multihash: "
                                 (pr-str before))))))
               ; Validate query limit.
               (when-let [limit (:limit opts)]
                 (if (pos-int? limit)
                   {:limit limit}
                   (throw (IllegalArgumentException.
                            (str "Option :limit is not a positive integer: "
                                 (pr-str limit))))))
               ; Ensure no other options.
               (when-let [bad-opts (not-empty (dissoc opts :algorithm :after :before :limit))]
                 (throw (IllegalArgumentException.
                          (str "Unknown options passed to list: " (pr-str bad-opts))))))]
    (meter/measure-stream
      store :list nil
      (io! (store/select-blocks opts (store/-list store opts))))))


(defn list-seq
  "Enumerate the stored blocks, returning a sequence of blocks ordered by their
  multihash id. This wraps the `list` method and consumes the stream lazily,
  terminating when the stream is drained, a timeout is encountered, or a list
  exception is observed on the stream.

  Accepts the same options as `list`, plus:

  - `:timeout`
    Millisecond duration to wait for new blocks to arrive on the stream.
    (default: `10000`)"
  [store & opts]
  (let [opts (args->map opts)
        timeout (:timeout opts 10000)]
    (when-not (pos-int? timeout)
      (throw (IllegalArgumentException.
               (str "Option :timeout is not a positive integer: "
                    (pr-str timeout)))))
    (letfn [(stream->seq
              [s]
              (lazy-seq
                (let [x @(s/try-take! s ::drained timeout ::timeout)]
                  (when (instance? Throwable x)
                    (throw x))
                  (when (identical? ::timeout x)
                    (throw (ex-info
                             (format "Block stream consumption timed out after %d ms"
                                     timeout)
                             {:opts opts})))
                  (when-not (identical? ::drained x)
                    (cons x (stream->seq s))))))]
      (stream->seq (list store (dissoc opts :timeout))))))


(defn stat
  "Load metadata about a block if the store contains it. Returns a deferred
  which yields a map with block information but no content, or nil if the store
  does not contain the identified block.

  The block stats include the `:id`, `:size`, and `:stored-at` fields. The
  returned map may also have additional implementation-specific storage
  metadata, similar to returned blocks."
  [store id]
  (when-not (multihash? id)
    (throw (IllegalArgumentException.
             (str "Block id must be a multihash, got: " (pr-str id)))))
  (meter/measure-method
    store :stat
    {:block-id id}
    (io! (store/-stat store id))))


(defn get
  "Load a block from the store. Returns a deferred which yields the block if
  the store contains it, or nil if no block is stored for that id."
  [store id]
  (when-not (multihash? id)
    (throw (IllegalArgumentException.
             (str "Block id must be a multihash, got: " (pr-str id)))))
  (d/chain
    (meter/measure-method
      store :get
      {:block-id id}
      (io! (store/-get store id)))
    (fn validate-block
      [block]
      (when block
        (when-not (= id (:id block))
          (throw (RuntimeException.
                   (str "Asked for block " id " but got " (:id block)))))
        (meter/metered-block store ::meter/io-read block)))))


(defn put!
  "Save a block into the store. Returns a deferred which yields the stored
  block, which may have already been present in the store."
  [store block]
  (when-not (instance? Block block)
    (throw (IllegalArgumentException.
             (str "Argument must be a block, got: " (pr-str block)))))
  (d/chain
    (meter/measure-method
      store :put!
      {:block-id (:id block)
       :block-size (:size block)}
      (->> block
           (meter/metered-block store ::meter/io-write)
           (store/-put! store)
           (io!)))
    (fn meter-block
      [block]
      (meter/metered-block store ::meter/io-read block))))


(defn store!
  "Store content from a byte source in a block store. Returns a deferred which
  yields the stored block, or nil if the source was empty.

  If the source is a file, it will be streamed into the store, otherwise the
  content is read into memory."
  ([store source]
   (store! store source default-algorithm))
  ([store source algorithm]
   (d/chain
     (io!
       (store/future'
         (if (instance? File source)
           (from-file source algorithm)
           (read! source algorithm))))
     (fn put-block
       [block]
       (when block
         (put! store block))))))


(defn delete!
  "Remove a block from the store. Returns a deferred which yields true if the
  block was found and removed."
  [store id]
  (when-not (multihash? id)
    (throw (IllegalArgumentException.
             (str "Block id must be a multihash, got: " (pr-str id)))))
  (meter/measure-method
    store :delete!
    {:block-id id}
    (io! (store/-delete! store id))))



;; ## Batch API

(defn get-batch
  "Retrieve a batch of blocks identified by a collection of multihashes.
  Returns a deferred which yields a collection of the blocks which were found.

  The blocks are returned in no particular order, and any missing blocks are
  omitted from the result."
  [store ids]
  (d/chain
    (->> (distinct ids)
         (map (partial get store))
         (apply d/zip))
    (fn omit-missing
      [blocks]
      (into [] (remove nil?) blocks))))


(defn put-batch!
  "Save a collection of blocks into the store. Returns a deferred which
  yields a collection of stored blocks.

  This is not guaranteed to be atomic; readers may see the store in a
  partially updated state."
  [store blocks]
  (if-let [blocks (seq (remove nil? blocks))]
    (apply d/zip (map (partial put! store) blocks))
    (d/success-deferred [])))


(defn delete-batch!
  "Remove a batch of blocks from the store, identified by a collection of
  multihashes. Returns a deferred which yields a set of ids for the blocks
  which were found and deleted.

  This is not guaranteed to be atomic; readers may see the store in a
  partially deleted state."
  [store ids]
  (if-let [ids (not-empty (into [] (comp (remove nil?) (distinct)) ids))]
    (d/chain
      (apply d/zip (map (partial delete! store) ids))
      (fn match-ids
        [results]
        (into #{}
              (comp
                (filter first)
                (map second))
              (map vector results ids))))
    (d/success-deferred #{})))



;; ## Storage Utilities

(defn scan
  "Scan blocks in the store, building up a summary. Returns a deferred which
  yields the summary map when the scan is complete.

  Accepts the same arguments as `list`, plus:

  - `:filter`
    A predicate function which will be used to filter blocks listed by the
    store. By default, all blocks are included."
  [store & opts]
  (let [opts (args->map opts)]
    (->
      (list store (dissoc opts :filter))
      (cond->>
        (:filter opts) (s/filter (:filter opts)))
      (->>
        (s/reduce sum/update (sum/init))))))


(defn erase!
  "Completely remove all data associated with the store. After this call, the
  store will be empty. Returns a deferred which yields true once the store has
  been erased.

  This is not guaranteed to be atomic; readers may see the store in a partially
  erased state."
  [store]
  (io!
    (if (satisfies? store/ErasableStore store)
      (meter/measure-method
        store :erase! nil
        (store/-erase! store))
      ; TODO: should be able to parallelize this - how to communicate errors?
      (s/consume-async
        (fn erase-block
          [block]
          (if (instance? Throwable block)
            (d/error-deferred block)
            (delete! store (:id block))))
        (list store)))))


(defn sync!
  "Synchronize blocks from the `source` store to the `dest` store. Returns a
  deferred which yields a summary of the copied blocks. Options may include:

  - `:filter`
    A function to run on every block before it is synchronized. The block will
    only be copied if the filter returns a truthy value."
  [source dest & opts]
  (let [opts (args->map opts)
        stream (cond->> (io! (store/missing-blocks
                               (store/-list source nil)
                               (store/-list dest nil)))
                 (:filter opts) (s/filter (:filter opts)))]
    (d/loop [summary (sum/init)]
      (d/chain'
        (s/take! stream ::drained)
        (fn copy-next
          [block]
          (cond
            (identical? ::drained block)
            summary

            (instance? Throwable block)
            (d/error-deferred block)

            :else
            (d/chain
              (put! dest block)
              (fn update-sum
                [block']
                (d/recur (sum/update summary block'))))))))))
