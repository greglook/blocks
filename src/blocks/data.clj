(ns blocks.data
  "Block type and constructor functions.

  Blocks have the following two primary attributes:

  - `:id`       `Multihash` with the digest identifying the content
  - `:size`     number of bytes in the block content
  "
  (:require
    [multihash.core :as multihash])
  (:import
    blocks.data.PersistentBytes
    clojure.lang.Tuple
    multihash.core.Multihash))


(deftype Block
  [^Multihash -id
   ^long -size
   ^PersistentBytes -content
   -reader
   -attributes
   -meta]

  ;:load-ns true


  java.lang.Object

  (toString
    [this]
    (format "Block[%s %s %s]"
            -id -size (if -content
                        "realized"
                        (if -reader
                          "deferred"
                          "empty"))))

  (hashCode
    [this]
    (hash-combine (hash Block) (hash -id)))

  ; TODO: this should consider attributes
  (equals
    [this that]
    (cond
      (identical? this that) true
      (instance? Block that)
        (= -id  (:id that))
      :else false))


  java.lang.Comparable

  (compareTo
    [this that]
    (compare [-id -size -attributes]
             [(:id that) (:size that)
              (when (instance? Block that)
                      (.-attributes ^Block that))]))


  clojure.lang.IHashEq

  (hasheq
    [this]
    (hash-combine (hash Block) (hash [-id -size -attributes])))


  clojure.lang.IObj

  (meta [this] -meta)

  (withMeta
    [this meta-map]
    (Block. -id -size -content -reader -attributes meta-map))


  ; TODO: IKeywordLookup?
  clojure.lang.ILookup

  (valAt
    [this k]
    (.valAt this k nil))

  (valAt
    [this k not-found]
    (case k
      :id -id
      :size -size
      not-found))


  clojure.lang.IPersistentMap

  (count
    [this]
    (+ 2 (count -attributes)))

  (empty
    [this]
    (Block. -id -size nil nil nil -meta))

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
                         other-content (. other -content)]
                     (and (= -id (. other -id))
                          (= -size (. other -size))
                          (= -attributes (. other -attributes))
                          (not (and -content other-content
                                    (not= -content other-content)))))))))

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
    (seq (concat [[:id -id] [:size -size]] -attributes)))

  (iterator
    [this]
    (.iterator (seq this)))

  (assoc
    [this k v]
    (case k
      (:id :size :content :reader)
        (throw (IllegalArgumentException.
                 (str "Block " k " cannot be changed")))
      (Block. -id -size -content -reader (assoc -attributes k v) -meta)))

  (without
    [this k]
    (case k
      (:id :size :content :reader)
        (throw (IllegalArgumentException.
                 (str "Block " k " cannot be changed")))
      (Block. -id -size -content -reader (not-empty (dissoc -attributes k)) -meta)))


  ; IDeref?
  ; (deref [this] ...)


  clojure.lang.IPending

  (isRealized
    [this]
    (some? -content))))


(defmethod print-method Block
  [v ^java.io.Writer w]
  (.write w (str v)))
