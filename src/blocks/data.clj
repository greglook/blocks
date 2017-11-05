(ns blocks.data
  "Block type and constructor functions.

  Blocks have two primary attributes, `:id` and `:size`. The block identifier
  is a `Multihash` with the digest identifying the content. The size is the
  number of bytes in the block content.

  Internally, blocks either have in-memory content holding the data, or a
  reader function which returns new input streams for the block data. A block
  with in-memory content is a _loaded block_, while a block with a reader
  function is a _lazy block_.

  A block's id, size, content, and reader cannot be changed after construction,
  so clients can be relatively certain that the block's id is valid. Blocks
  _may_ have additional attributes associated with them and support metadata,
  similar to records."
  (:require
    [byte-streams :as bytes]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    blocks.data.PersistentBytes
    (java.io
      InputStream
      IOException)
    multihash.core.Multihash
    org.apache.commons.io.input.BoundedInputStream))


(deftype Block
  [^Multihash id
   ^long size
   ^PersistentBytes content
   reader
   _attrs
   _meta]

  ;:load-ns true


  java.lang.Object

  (toString
    [this]
    (format "Block[%s %s %s]"
            id size (if content "*" (if reader "~" "!"))))

  (equals
    [this that]
    (boolean
      (or (identical? this that)
          (when (identical? (class this) (class that))
            (let [that ^Block that]
              (and (= id     (.id     that))
                   (= size   (.size   that))
                   (= _attrs (._attrs that))))))))

  (hashCode
    [this]
    (hash [(class this) id size _attrs]))


  java.lang.Comparable

  (compareTo
    [this that]
    (compare [id size _attrs]
             [(:id that) (:size that)
              (when (identical? (class this) (class that))
                (._attrs ^Block that))]))


  clojure.lang.IObj

  (meta [this] _meta)

  (withMeta
    [this meta-map]
    (Block. id size content reader _attrs meta-map))


  ; TODO: IKeywordLookup?
  clojure.lang.ILookup

  (valAt
    [this k]
    (.valAt this k nil))

  (valAt
    [this k not-found]
    (case k
      :id id
      :size size
      (get _attrs k not-found)))


  clojure.lang.IPersistentMap

  (count
    [this]
    (+ 2 (count _attrs)))

  (empty
    [this]
    (Block. id size nil nil nil _meta))

  (cons
    [this element]
    (cond
      (instance? java.util.Map$Entry element)
        (let [^java.util.Map$Entry entry element]
          (.assoc this (.getKey entry) (.getValue entry)))
      (vector? element)
        (.assoc this (first element) (second element))
      :else
        (loop [result this
               entries element]
          (if (seq entries)
            (let [^java.util.Map$Entry entry (first entries)]
              (recur (.assoc result (.getKey entry) (.getValue entry))
                     (rest entries)))
            result))))

  (equiv
    [this that]
    (.equals this that))

  (containsKey
    [this k]
    (not (identical? this (.valAt this k this))))

  (entryAt
    [this k]
    (let [v (.valAt this k this)]
      (when-not (identical? this v)
        (clojure.lang.MapEntry. k v))))

  (seq
    [this]
    (seq (concat [(clojure.lang.MapEntry. :id id)
                  (clojure.lang.MapEntry. :size size)]
                 _attrs)))

  (iterator
    [this]
    (clojure.lang.RT/iter (seq this)))

  (assoc
    [this k v]
    (case k
      (:id :size :content :reader)
        (throw (IllegalArgumentException.
                 (str "Block " k " cannot be changed")))
      (Block. id size content reader (assoc _attrs k v) _meta)))

  (without
    [this k]
    (case k
      (:id :size :content :reader)
        (throw (IllegalArgumentException.
                 (str "Block " k " cannot be changed")))
      (Block. id size content reader (not-empty (dissoc _attrs k)) _meta))))


(defmethod print-method Block
  [v ^java.io.Writer w]
  (.write w (str v)))



;; ## Utility Functions

(defn- collect-bytes
  "Collects bytes from a data source into a `PersistentBytes` object. If the
  source is already persistent, it will be reused directly."
  ^PersistentBytes
  [source]
  (if (instance? PersistentBytes source)
    source
    (PersistentBytes/wrap (bytes/to-byte-array source))))


(defn- resolve-hasher
  "Resolves an algorithm designator to a hash function. Throws an exception on
  invalid names or error."
  [algorithm]
  (cond
    (nil? algorithm)
      (throw (IllegalArgumentException.
               "Cannot find hash function without algorithm name"))

    (keyword? algorithm)
      (if-let [hf (clojure.core/get digest/functions algorithm)]
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


(defn checked-hasher
  "Constructs a function for the given hash algorithm or function which checks
  that the result is a `Multihash`."
  [algorithm]
  (let [hash-fn (resolve-hasher algorithm)]
    (fn hasher
      [source]
      (let [id (hash-fn source)]
        (when-not (instance? Multihash id)
          (throw (RuntimeException.
                   (str "Block identifier must be a Multihash, "
                        "hashing algorithm returned: " (pr-str id)))))
        id))))


(defn- bounded-input-stream
  "Wraps an input stream such that it only returns a stream of bytes in the
  range start - end."
  ^java.io.InputStream
  [^InputStream input start end]
  (.skip input start)
  (BoundedInputStream. input (- end start)))


(defn content-stream
  "Opens an input stream to read the contents of the block."
  ^java.io.InputStream
  [^Block block start end]
  (let [content ^PersistentBytes (.content block)
        reader (.reader block)]
    (cond
      content (cond-> (.open content)
                (and start end)
                  (bounded-input-stream start end))
      reader (if (and start end)
               (try
                 (reader start end)
                 (catch clojure.lang.ArityException e
                   ; Native ranged open not supported, use naive approach.
                   (bounded-input-stream (reader) start end)))
               (reader))
      :else (throw (IOException.
                     (str "Cannot open empty block " (:id block)))))))



;; ## Constructors

;; Remove automatic constructor function.
(alter-meta! #'->Block assoc :private true)


(defn lazy-block
  "Creates a block from a reader function. Each time the function is called, it
  should return a new `InputStream` to read the block contents. The block is
  given the id and size directly, without being checked."
  ^blocks.data.Block
  [id size reader]
  (->Block id size nil reader nil nil))


(defn load-block
  "Creates a block by reading a source into memory. The block is given the id
  directly, without being checked."
  ^blocks.data.Block
  [id source]
  (let [content (collect-bytes source)]
    (when (pos? (count content))
      (->Block id (count content) content nil nil nil))))


(defn read-block
  "Creates a block by reading the source into memory and hashing it."
  ^blocks.data.Block
  [algorithm source]
  (let [hash-fn (checked-hasher algorithm)
        content (collect-bytes source)]
    (when (pos? (count content))
      (->Block (hash-fn (.open content)) (count content) content nil nil nil))))


(defn clean-block
  "Creates a version of the given block without extra attributes or metadata."
  [^Block block]
  (->Block (.id block)
           (.size block)
           (.content block)
           (.reader block)
           nil
           nil))


(defn merge-blocks
  "Creates a new block by merging together two blocks representing the same
  content. Block ids and sizes must match. The new block's content or reader
  comes from the second block, and any extra attributes and metadata are merged
  together."
  [^Block a ^Block b]
  (when (not= (.id a) (.id b))
    (throw (IllegalArgumentException.
             (str "Cannot merge blocks with differing ids " (.id a)
                  " and " (.id b)))))
  (->Block (.id b)
           (.size b)
           (.content b)
           (.reader b)
           (not-empty (merge (._attrs a) (._attrs b)))
           (not-empty (merge (._meta  a) (._meta  b)))))
