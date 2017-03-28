(ns blocks.summary
  "A 'summary' represents a collection of blocks, including certain statistics
  and an imprecise membership test in the form of a bloom filter. These are
  useful for returning from certain operations to represent the set of blocks
  acted upon."
  (:refer-clojure :exclude [update merge])
  (:require
    [bigml.sketchy.bloom :as bloom]
    [multihash.core :as multihash]))


(defn init
  "Return a new, empty summary. An optional expected population and error
  frequency may be passed in for more control. The following fields are present
  in a summary:

  - `:count` is the total number of blocks added to the summary
  - `:size` is the total size of blocks added to the summary, in bytes
  - `:sizes` gives a map from bucket exponent to a count of the blocks in that
    bucket (see `size->bucket` and `bucket->range`)
  - `:membership` a bloom filter holding the statistical membership set of
    block ids"
  ([]
   (init 10000 0.01))
  ([expected-population false-positive-rate]
   {:count 0
    :size 0
    :sizes {}
    :membership (bloom/create expected-population false-positive-rate)}))


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
  "Update the storage summary with the stats from the given block."
  [summary block]
  (-> summary
      (clojure.core/update :count inc)
      (clojure.core/update :size + (:size block))
      (clojure.core/update :sizes clojure.core/update (size->bucket (:size block)) (fnil inc 0))
      (clojure.core/update :membership bloom/insert (:id block))))


(defn merge
  "Merge two storage summaries together."
  [a b]
  (-> a
      (clojure.core/update :count + (:count b))
      (clojure.core/update :size + (:size b))
      (clojure.core/update :sizes (partial merge-with +) (:sizes b))
      (clojure.core/update :membership bloom/merge (:membership b))
      (clojure.core/merge (dissoc b :count :size :sizes :membership))))


(defn probably-contains?
  "Uses a summary map to check whether the the store (probably) contains the
  given block identifier. False positives may be possible, but false negatives
  are not."
  [summary id]
  (bloom/contains? (:membership summary) id))
