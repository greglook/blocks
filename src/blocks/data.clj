(ns blocks.data
  "Block type and constructor functions.

  Blocks have the following two primary attributes:

  - `:id`       `Multihash` with the digest identifying the content
  - `:size`     number of bytes in the block content
  "
  (:require
    [byte-streams :as bytes]
    [multihash.core :as multihash])
  (:import
    blocks.data.PersistentBytes
    java.io.FilterInputStream
    java.io.InputStream
    multihash.core.Multihash))


;; ## Utility Functions

(defn resolve-hash
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


(defn checked-hash
  "Constructs a function for the given hash algorithm or function which checks
  that the result is a `Multihash`."
  [algorithm]
  (let [hash-fn (resolve-hash algorithm)]
    (fn checked-hasher
      [source]
      (let [id (hash-fn source)]
        (when-not (instance? Multihash id)
          (throw (RuntimeException.
                   (str "Block identifier must be a Multihash, "
                        "hashing algorithm returned: " (pr-str id)))))
        id))))


(defn counting-input-stream
  "Wraps the given input stream with a filter which counts the bytes passing
  through it. The byte count will be added to value in the atom argument."
  ^InputStream
  [^InputStream in counter]
  (proxy [FilterInputStream] [in]
    (read
      ([]
       (let [out (.read in)]
         (swap! counter inc)
         out))
      ([b]
       (let [size (.read in b)]
         (swap! counter + size)
         size))
      ([b off len]
       (let [size (.read in b off len)]
         (swap! counter + size)
         size)))
    (skip [n]
      (let [size (.skip in n)]
        (swap! counter + size)
        size))))



;; ## Block Type

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
            id size (if content "+" (if reader "-" " "))))

  (hashCode
    [this]
    (hash-combine (hash Block) (hash id)))

  ; TODO: this should consider attributes
  (equals
    [this that]
    (cond
      (identical? this that) true
      (instance? Block that)
        (= id  (:id that))
      :else false))


  java.lang.Comparable

  (compareTo
    [this that]
    (compare [id size _attrs]
             [(:id that) (:size that)
              (when (instance? Block that)
                      (._attrs ^Block that))]))


  clojure.lang.IHashEq

  (hasheq
    [this]
    (hash-combine (hash Block) (hash [id size _attrs])))


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
    [this other]
    (boolean (or (identical? this other)
                 (when (identical? (class this) (class other))
                   (let [other ^Block other
                         other-content (.content other)]
                     (and (= id (.id other))
                          (= size (.size other))
                          (= _attrs (._attrs other))
                          (not (and content other-content
                                    (not= content other-content)))))))))

  (containsKey
    [this k]
    (not (identical? this (.valAt this k this))))

  (entryAt
    [this k]
    (let [v (.valAt this k this)]
      (when-not (identical? this v)
        [k v])))

  (seq
    [this]
    (seq (concat [[:id id] [:size size]] _attrs)))

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
      (Block. id size content reader (not-empty (dissoc _attrs k)) _meta)))


  clojure.lang.IDeref

  ; TODO: what should this do?
  (deref
    [this]
    (merge
      _attrs
      {:id id
       :size size
       :block/state
       (cond content :literal
             reader  :lazy
             :else   :empty)}))


  clojure.lang.IPending

  (isRealized
    [this]
    (some? content)))


(defmethod print-method Block
  [v ^java.io.Writer w]
  (.write w (str v)))



;; ## Constructors

;; Remove automatic constructor function.
(ns-unmap *ns* '->Block)


(defn create-literal-block
  "Creates a block by reading a source into memory. The block is given the id
  directly, without being checked."
  ^Block
  [id source]
  (let [content (bytes/to-byte-array source)]
    (Block. id
            (count content)
            (PersistentBytes/wrap content)
            nil nil nil)))


(defn read-literal-block
  "Creates a block by reading the source into memory and hashing it. This
  creates a realized block."
  ^Block
  [source algorithm]
  (let [hash-fn (checked-hash algorithm)
        content (bytes/to-byte-array source)]
    (Block. (hash-fn content)
            (count content)
            (PersistentBytes/wrap content)
            nil nil nil)))


(defn create-lazy-block
  "Creates a block from a reader function. Each time the function is called, it
  should return a new `InputStream` to read the block contents. The block is
  given the id and size directly, without being checked."
  ^Block
  [id size reader]
  (Block. id size nil reader nil nil))


(defn read-lazy-block
  "Creates a block from a reader function. Each time the function is called, it
  should return a new `InputStream` to read the block contents. The stream will
  be read once immedately to calculate the hash and size."
  ^Block
  [reader algorithm]
  (let [hash-fn (checked-hash algorithm)
        counter (atom 0)]
    (with-open [in (counting-input-stream (reader) counter)]
      (let [id (hash-fn in)]
        (Block. id @counter nil reader nil nil)))))
