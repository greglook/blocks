(ns ^:no-doc blocks.store.tests
  "Suite of generative behavioral tests to verify that a given block store
  implementation conforms to the spec."
  (:require
    [alphabase.bytes :refer [bytes= random-bytes]]
    [alphabase.hex :as hex]
    [blocks.core :as block]
    [blocks.summary :as sum]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [clojure.test.check :as check]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]
    [multiformats.hash :as multihash]
    [puget.color.ansi :as ansi]
    [puget.printer :as puget]
    [test.carly.core :as carly :refer [defop]])
  (:import
    blocks.data.Block
    blocks.data.PersistentBytes
    java.time.Instant
    multiformats.hash.Multihash))


;; ## Block Utilities

(defn random-block
  "Creates a new block with random content at most `max-size` bytes long."
  [max-size]
  (block/read!
    (random-bytes (inc (rand-int max-size)))
    (rand-nth (keys multihash/functions))))


(defn generate-blocks!
  "Generates some test blocks and returns a map of the ids to the blocks."
  [n max-size]
  (->> (repeatedly #(random-block max-size))
       (take n)
       (map (juxt :id identity))
       (into (sorted-map))))


(defn populate-blocks!
  "Generates random blocks and puts them into the given store. Returns a map
  of multihash ids to blocks."
  [store & {:keys [n max-size], :or {n 10, max-size 1024}}]
  (let [blocks (generate-blocks! n max-size)]
    @(block/put-batch! store (vals blocks))
    blocks))



;; ## Operation Generators

(defn- choose-id
  "Returns a generator which will select a block id from the model pool."
  [blocks]
  (gen/elements (keys blocks)))


(defn- choose-block
  "Returns a generator which will select a block from the model pool."
  [blocks]
  (gen/elements (vals blocks)))


(defn- gen-sub-seq
  "Generate subsequences of the entries in the given sequence, returning some of
  the elements in the same order as given."
  [xs]
  (gen/fmap
    (fn select
      [bools]
      (sequence (comp (filter first) (map second)) (map vector bools xs)))
    (gen/vector gen/boolean (count xs))))


(defn- gen-sub-map
  "Generate subsets of the entries in the given map."
  [m]
  (gen/fmap (partial into {}) (gen-sub-seq (seq m))))


(defop ListBlocks
  [query]

  (gen-args
    [blocks]
    [(gen/bind
       (gen/hash-map
         :algorithm (gen/elements (keys multihash/functions))
         :after (gen/fmap hex/encode (gen/not-empty gen/bytes)) ; TODO: pick prefixes
         :limit (gen/large-integer* {:min 1, :max (inc (count blocks))}))
       gen-sub-map)])

  (apply-op
    [this store]
    (doall (block/list-seq store query)))

  (check
    [this model result]
    (let [expected-ids (cond->> (keys model)
                         (:algorithm query)
                           (filter #(= (:algorithm query) (:algorithm %)))
                         (:after query)
                           (filter #(pos? (compare (multihash/hex %) (:after query))))
                         (:before query)
                           (filter #(neg? (compare (multihash/hex %) (:before query))))
                         true
                           (sort)
                         (:limit query)
                           (take (:limit query)))]
      (is (sequential? result))
      (is (= (count expected-ids) (count result)))
      (doseq [[id result] (zipmap expected-ids result)]
        (if-let [block (get model id)]
          (do (is (instance? Block result))
              (is (= (:id block) (:id result)))
              (is (= (:size block) (:size result)))
              (is (instance? Instant (:stored-at result))))
          (is (nil? result)))))))


(defop StatBlock
  [id]

  (gen-args
    [ctx]
    [(choose-id ctx)])

  (apply-op
    [this store]
    @(block/stat store id))

  (check
    [this model result]
    (if-let [block (get model id)]
      (do (is (map? result))
          (is (= (:id block) (:id result)))
          (is (= (:size block) (:size result)))
          (is (instance? Instant (:stored-at result))))
      (is (nil? result)))))


(defop GetBlock
  [id]

  (gen-args
    [blocks]
    [(choose-id blocks)])

  (apply-op
    [this store]
    @(block/get store id))

  (check
    [this model result]
    (if-let [block (get model id)]
      (do (is (some? (:id result)))
          (is (integer? (:size result)))
          (is (= id (:id result)))
          (is (= (:size block) (:size result))))
      (is (nil? result)))))


(defop PutBlock
  [block]

  (gen-args
    [blocks]
    [(choose-block blocks)])

  (apply-op
    [this store]
    @(block/put! store block))

  (check
    [this model result]
    (is (= block result)))

  (update-model
    [this model]
    (assoc model (:id block) block)))


(defop DeleteBlock
  [id]

  (gen-args
    [blocks]
    [(choose-id blocks)])

  (apply-op
    [this store]
    @(block/delete! store id))

  (check
    [this model result]
    (if (contains? model id)
      (is (true? result))
      (is (false? result))))

  (update-model
    [this model]
    (dissoc model id)))


(defop EraseStore
  []

  (apply-op
    [this store]
    @(block/erase! store))

  (update-model
    [this model]
    (empty model)))


(defop ScanStore
  [p]

  (gen-args
    [blocks]
    [(gen/elements [nil (fn scan-pred
                          [stat]
                          (< (:size stat) 256))])])

  (apply-op
    [this store]
    @(block/scan store :filter p))

  (check
    [this model result]
    (let [blocks (cond->> (vals model) p (filter p))]
      (is (= (count blocks) (:count result)))
      (is (= (reduce + (map :size blocks)) (:size result)))
      (is (map? (:sizes result)))
      (is (every? integer? (keys (:sizes result))))
      (is (= (count blocks) (reduce + (vals (:sizes result))))))))


(defop OpenBlock
  [id]

  (gen-args
    [blocks]
    [(choose-id blocks)])

  (apply-op
    [this store]
    (when-let [block @(block/get store id)]
      (let [baos (java.io.ByteArrayOutputStream.)]
        (with-open [content (block/open block)]
          (io/copy content baos))
        (.toByteArray baos))))

  (check
    [this model result]
    (if-let [block (get model id)]
      (is (bytes= (.toByteArray ^PersistentBytes (.content ^Block block)) result))
      (is (nil? result)))))


(defop OpenBlockRange
  [id start end]

  (gen-args
    [blocks]
    (gen/bind
      (choose-block blocks)
      (fn [block]
        (if (< 3 (:size block))
          (gen/fmap
            (fn [positions]
              (let [[start end] (sort positions)]
                {:id (:id block)
                 :start start
                 :end end}))
            (gen/vector-distinct
              (gen/large-integer* {:min 0, :max (:size block)})
              {:num-elements 2}))
          (gen/return
            {:id (:id block)
             :start 0
             :end (:size block)})))))

  (apply-op
    [this store]
    (when-let [block @(block/get store id)]
      (let [baos (java.io.ByteArrayOutputStream.)]
        (with-open [content (block/open block {:start start, :end end})]
          (io/copy content baos))
        (.toByteArray baos))))

  (check
    [this model result]
    (if-let [block (get model id)]
      (let [baos (java.io.ByteArrayOutputStream.)
            length (- end start)
            subarray (byte-array length)]
        (block/write! block baos)
        (System/arraycopy (.toByteArray baos) start subarray 0 length)
        (is (bytes= subarray result)))
      (is (nil? result)))))


(def ^:private basic-op-generators
  (juxt gen->ListBlocks
        gen->ScanStore
        gen->StatBlock
        gen->GetBlock
        gen->OpenBlock
        gen->OpenBlockRange
        gen->PutBlock
        gen->DeleteBlock))


(def ^:private erasable-op-generators
  (juxt gen->EraseStore))


(defn- join-generators
  [ks]
  (let [op-gens (keep {:basic basic-op-generators
                       :erase erasable-op-generators}
                      ks)]
    (fn [ctx]
      (into [] (mapcat #(% ctx)) op-gens))))



;; ## Operation Testing

(defn- start-store
  [constructor]
  (let [store (component/start (constructor))]
    (when-let [extant (seq (block/list-seq store))]
      (throw (IllegalStateException.
               (str "Cannot run integration test on " (pr-str store)
                    " as it already contains blocks: "
                    (pr-str extant)))))
    (is (zero? (:count @(block/scan store))))
    store))


(defn- stop-store
  [store]
  (block/erase! store)
  (is (empty? (block/list-seq store))
      "ends empty")
  (component/stop store))


(defn- gen-blocks-context
  [test-blocks]
  (let [default-ctx (conj {} (first test-blocks))]
    (gen/fmap
      (fn [ctx] (if (seq ctx) ctx default-ctx))
      (gen-sub-map test-blocks))))


(def ^:private print-handlers
  {Multihash (puget/tagged-handler 'multi/hash str)
   Block (puget/tagged-handler 'blocks/block (juxt :id :size :stored-at))
   (class (byte-array 0)) (puget/tagged-handler 'data/bytes alphabase.hex/encode)})


(defn- type->print-handler
  [t]
  (or (print-handlers t) (puget/common-handlers t)))


(defn check-store*
  "Uses generative tests to validate the behavior of a block store
  implementation. The first argument must be a no-arg constructor function which
  will produce a new block store for testing. The remaining options control the
  behavior of the tests:

  - `blocks`
    Generate this many random blocks to test the store with.
  - `max-size`
    Maximum block size to generate, in bytes.
  - `operations`
    Kinds of operations to test - vector of `:basic`, `:erase`.
  - `concurrency`
    Maximum number of threads of operations to generate.
  - `iterations`
    Number of generative tests to perform.
  - `repetitions`
    Number of times to repeat the test for concurrency checks.

  Returns the results of the generative tests."
  [constructor
   {:keys [blocks max-size operations concurrency iterations repetitions]
    :or {blocks 20
         max-size 1024
         operations [:basic]
         concurrency 4
         iterations (or (some-> (System/getenv "BLOCKS_STORE_TEST_ITERATIONS")
                                (Integer/parseInt))
                        100)
         repetitions (or (some-> (System/getenv "BLOCKS_STORE_TEST_REPETITIONS")
                                 (Integer/parseInt))
                         10)}}]
  {:pre [(fn? constructor)]}
  (let [test-blocks (generate-blocks! blocks max-size)]
    (carly/check-system "integration testing" iterations
      (fn init-system [ctx] (start-store constructor))
      (join-generators operations)
      :on-stop stop-store
      :context-gen (gen-blocks-context test-blocks)
      :concurrency concurrency
      :repetitions repetitions
      :report {:puget {:print-handlers type->print-handler}})))


(defn check-store
  "Uses generative tests to validate the behavior of a block store
  implementation. The first argument must be a no-arg constructor function
  which will produce a new block store for testing.

  See `check-store*` for a variant with more configurable options."
  [constructor]
  (check-store*
    constructor
    {:operations [:basic :erase]
     :concurrency 1
     :repetitions 1}))
