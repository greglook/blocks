(ns blocks.summary
  "A 'summary' represents a collection of blocks, including certain statistics
  about the aggregate count and sizes. These are useful for returning from
  certain operations to represent the set of blocks acted upon.

  The following fields are present in a summary:

  - `:count`
    The total number of blocks added to the summary.
  - `:size`
    The total size of blocks added to the summary, in bytes.
  - `:sizes`
    A histogram map from bucket exponent to a count of the blocks in that
    bucket (see `size->bucket` and `bucket->range`)."
  (:refer-clojure :exclude [update merge]))


(defn init
  "Construct a new, empty summary."
  []
  {:count 0
   :size 0
   :sizes {}})


(defn size->bucket
  "Assigns a block size to an exponential histogram bucket. Given a size `s`,
  returns `n` such that `2^n <= s < 2^(n+1)`."
  [size]
  (loop [s size
         n 0]
    (if (pos? s)
      (recur (bit-shift-right s 1) (inc n))
      n)))


(defn bucket->range
  "Returns a vector with the boundaries which a given size bucket covers."
  [n]
  [(bit-shift-left 1 (dec n))
   (bit-shift-left 1 n)])


(defn update
  "Update the summary with the stats from the given block."
  [summary block]
  (when (instance? Throwable block)
    (throw block))
  (-> summary
      (clojure.core/update :count inc)
      (clojure.core/update :size + (:size block))
      (clojure.core/update :sizes clojure.core/update (size->bucket (:size block)) (fnil inc 0))))


(defn merge
  "Merge two summaries together."
  [a b]
  (-> a
      (clojure.core/update :count + (:count b))
      (clojure.core/update :size + (:size b))
      (clojure.core/update :sizes (partial merge-with +) (:sizes b))
      (clojure.core/merge (dissoc b :count :size :sizes))))
