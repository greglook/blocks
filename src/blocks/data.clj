(ns ^:no-doc blocks.data
  "Block type and constructor functions.

  Blocks have two primary attributes, `:id` and `:size`. The block identifier
  is a multihash with the digest identifying the content. The size is the
  number of bytes in the block content. Blocks also have a `:stored-at` value
  giving the instant they were persisted, but this does not affect equality or
  the block's hash code.

  Internally, blocks may reference their content in-memory as a byte array, or
  a _content reader_ which constructs new input streams for the block data on
  demand. A block with in-memory content is considered a _loaded block_, while
  blocks with readers are _lazy blocks_."
  (:require
    [byte-streams :as bytes]
    [multiformats.hash :as multihash])
  (:import
    blocks.data.PersistentBytes
    (java.io
      IOException
      InputStream)
    java.time.Instant
    multiformats.hash.Multihash
    (org.apache.commons.io.input
      BoundedInputStream)))


;; ## Block Type

(deftype Block
  [^Multihash id
   ^long size
   ^Instant stored-at
   content
   _meta]

  :load-ns true


  java.lang.Object

  (toString
    [this]
    (format "Block[%s %s %s]" id size stored-at))


  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (let [that ^Block that]
              (and (= id   (.id   that))
                   (= size (.size that))))))))


  (hashCode
    [this]
    (-> (hash (class this))
        (hash-combine (hash id))
        (hash-combine size)))


  java.lang.Comparable

  (compareTo
    [this that]
    (if (= id (:id that))
      (if (= size (:size that))
        (if (= stored-at (:stored-at that))
          0
          (compare stored-at (:stored-at that)))
        (compare size (:size that)))
      (compare id (:id that))))


  clojure.lang.IObj

  (meta
    [this]
    _meta)


  (withMeta
    [this meta-map]
    (Block. id size stored-at content meta-map))


  clojure.lang.ILookup

  (valAt
    [this k]
    (.valAt this k nil))


  (valAt
    [this k not-found]
    (case k
      :id id
      :size size
      :stored-at stored-at
      not-found)))


(defmethod print-method Block
  [v ^java.io.Writer w]
  (.write w (str v)))



;; ## Content Readers

(defprotocol ContentReader
  "Content readers provide functions for repeatably reading byte streams from
  some backing data source."

  (read-all
    [reader]
    "Open an input stream that returns all bytes of the content.")

  (read-range
    [reader start end]
    "Open an input stream that reads just bytes from `start` to `end`,
    inclusive. A `nil` for either value implies the beginning or end of the
    stream, respectively."))


(defn- bounded-input-stream
  "Wrap an input stream such that it only returns a stream of bytes in the
  range `start` to `end`."
  ^java.io.InputStream
  [^InputStream input start end]
  (when (pos-int? start)
    (.skip input start))
  (if (pos-int? end)
    (BoundedInputStream. input (- end (or start 0)))
    input))


(extend-protocol ContentReader

  PersistentBytes

  (read-all
    [^PersistentBytes this]
    (.open this))


  (read-range
    [^PersistentBytes this start end]
    (bounded-input-stream (.open this) start end))


  clojure.lang.Fn

  (read-all
    [this]
    (this))


  (read-range
    [this start end]
    ; Ranged open not supported for generic functions, use naive approach.
    (bounded-input-stream (this) start end)))


(defn content-stream
  "Open an input stream to read the contents of the block."
  ^java.io.InputStream
  [^Block block start end]
  (let [content (.content block)]
    (if (or start end)
      (read-range content start end)
      (read-all content))))


(defn persistent-bytes?
  "True if the argument is a persistent byte array."
  [x]
  (instance? PersistentBytes x))


(defn byte-content?
  "True if the block has content loaded into memory as persistent bytes."
  [^Block block]
  (persistent-bytes? (.content block)))



;; ## Constructors

;; Remove automatic constructor function.
(alter-meta! #'->Block assoc :private true)


(defn- now
  "Return the current instant in time.

  This is mostly useful for rebinding during tests."
  ^Instant
  []
  (Instant/now))


(defn hasher
  "Return the hashing function for an algorithm keyword, or throw an exception
  if no supported function is available."
  [algorithm]
  (or (multihash/functions algorithm)
      (throw (IllegalArgumentException.
               (str "No digest function found for algorithm "
                    algorithm)))))


(defn create-block
  "Create a block from a content reader. The simplest version is a no-arg
  function which should return a new `InputStream` to read the full block
  content. The block is given the id and size directly, without being checked."
  ([id size content]
   (create-block id size (now) content))
  ([id size stored-at content]
   (when-not (instance? Multihash id)
     (throw (ex-info "Block id must be a multihash"
                     {:id id, :size size, :stored-at stored-at})))
   (when-not (pos-int? size)
     (throw (ex-info "Block size must be a positive integer"
                     {:id id, :size size, :stored-at stored-at})))
   (when-not (instance? Instant stored-at)
     (throw (ex-info "Block must have a stored-at instant"
                     {:id id, :size size, :stored-at stored-at})))
   (when-not content
     (throw (ex-info "Block must have a content reader"
                     {:id id, :size size, :stored-at stored-at})))
   (->Block id size stored-at content nil)))


(defn read-block
  "Create a block by reading the source into memory and hashing it."
  [algorithm source]
  (let [hash-fn (hasher algorithm)
        content (PersistentBytes/wrap (bytes/to-byte-array source))
        size (count content)]
    (when (pos? size)
      (create-block (hash-fn (read-all content)) size (now) content))))


(defn merge-blocks
  "Create a new block by merging together two blocks representing the same
  content. Block ids and sizes must match. The new block's content and
  timestamp come from the second block, and any metadata is merged together."
  [^Block left ^Block right]
  (when (not= (.id left) (.id right))
    (throw (ex-info
             (str "Cannot merge blocks with differing ids " (.id left)
                  " and " (.id right))
             {:left left, :right right})))
  (when (not= (.size left) (.size right))
    (throw (ex-info
             (str "Cannot merge blocks with differing sizes " (.size left)
                  " and " (.size right))
             {:left left, :right right})))
  (->Block
    (.id right)
    (.size right)
    (.stored-at right)
    (.content right)
    (not-empty (merge (._meta left) (._meta right)))))


(defn wrap-content
  "Wrap a block's content by calling `f` on it, returning a new block with the
  same id and size."
  ^blocks.data.Block
  [^Block block f]
  (->Block
    (.id block)
    (.size block)
    (.stored-at block)
    (f (.content block))
    (._meta block)))
