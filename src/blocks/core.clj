(ns blocks.core
  "Block storage protocol and utilities. Functions which may cause IO to occur
  are marked with bangs.

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
    [byte-streams :as bytes]
    [clojure.java.io :as io]
    [clojure.set :as set]
    [clojure.string :as str]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    blocks.data.PersistentBytes
    java.io.File
    java.io.IOException
    multihash.core.Multihash))


(def default-algorithm
  "The hashing algorithm used if not specified in functions which create blocks."
  :sha2-256)



;; ## Stat Metadata

(defn with-stats
  "Adds stat information to a block's metadata."
  [block stats]
  (vary-meta block assoc :block/stats stats))


(defn meta-stats
  "Returns stat information from a block's metadata, if present."
  [block]
  (:block/stats (meta block)))



;; ## Block IO

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


; TODO: support opening a byte range
(defn open
  "Opens an input stream to read the content of the block. Throws an IO
  exception on empty blocks."
  ^java.io.InputStream
  [^Block block]
  (let [content ^PersistentBytes (.content block)
        reader (.reader block)]
    (cond
      content (.open content)
      reader  (reader)
      :else   (throw (IOException.
                        (str "Cannot open empty block " (:id block)))))))


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
    ; TODO: check size correctness later with a counting-input-stream?
    (when (realized? block)
      (let [actual-size (count @block)]
        (when (not= size actual-size)
          (throw (IllegalStateException.
                   (str "Block " id " reports size " size
                        " but has actual size " actual-size))))))
    (with-open [stream (open block)]
      (when-not (multihash/test id stream)
        (throw (IllegalStateException.
                 (str "Block " id " has mismatched content")))))))



;; ## Storage Interface

(defprotocol BlockStore
  "Protocol for content-addressable storage keyed by multihash identifiers."

  (stat
    [store id]
    "Returns a map with an `:id` and `:size` but no content. The returned map
    may contain additional data like the date stored. Returns nil if the store
    does not contain the identified block.")

  (-list
    [store opts]
    "Lists the blocks contained in the store. Returns a lazy sequence of stat
    metadata about each block. See `list` for the supported options.")

  (-get
    [store id]
    "Returns the identified block if it is stored, otherwise nil. The block
    should include stat metadata. Typically clients should use `get` instead,
    which validates arguments and the returned block record.")

  (put!
    [store block]
    "Saves a block into the store. Returns the block record, updated with stat
    metadata.")

  (delete!
    [store id]
    "Removes a block from the store. Returns true if the block was stored."))


; TODO: BlockEnumerator
; Protocol which returns a lazy sequence of every block in the store, along with
; an opaque marker which can be used to resume the stream in the same position.
; Blocks are explicitly **not** returned in any defined order; it is assumed the
; store will enumerate them in the most efficient order available.


(defn list
  "Enumerates the stored blocks, returning a lazy sequence of block stats.
  Iterating over the list may result in additional operations to read from the
  backing data store.

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
     (-list store opts-map))))


(defn get
  "Loads content for a multihash and returns a block record. Returns nil if no
  block is stored. The returned block is checked to make sure the id matches the
  requested hash."
  [store id]
  (when-not (instance? Multihash id)
    (throw (IllegalArgumentException.
             (str "Id value must be a multihash, got: " (pr-str id)))))
  (when-let [block (-get store id)]
    (when-not (= id (:id block))
      (throw (RuntimeException.
               (str "Asked for block " id " but got " (:id block)))))
    block))


(defn store!
  "Stores content from a byte source in a block store and returns the block
  record.

  If the source is a file, it will be streamed into the store. Otherwise, the
  content is read into memory, so this may not be suitable for large sources."
  ([store source]
   (store! store source default-algorithm))
  ([store source algorithm]
   (put! store (if (instance? File source)
                 (from-file source algorithm)
                 (read! source algorithm)))))
