(ns blocks.store.tests
  "Suite of tests to verify that a given block store implementation conforms to
  the spec."
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
    [multihash.core :as multihash]
    [multihash.digest :as digest]
    [puget.color.ansi :as ansi]
    [puget.printer :as puget]
    [test.carly.core :as carly :refer [defop]])
  (:import
    blocks.data.Block
    blocks.data.PersistentBytes
    multihash.core.Multihash))


;; ## Block Utilities

(defn random-block
  "Creates a new block with random content at most `max-size` bytes long."
  [max-size]
  (block/read!
    (random-bytes (inc (rand-int max-size)))
    (rand-nth (keys digest/functions))))


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
    (block/put-batch! store (vals blocks))
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


(defn- gen-sub-map
  "Generate subsets of the entries in the given map."
  [m]
  (gen/fmap
    (fn select
      [bools]
      (into {} (comp (filter first) (map second)) (map vector bools m)))
    (gen/vector gen/boolean (count m))))


(defop StatBlock
  [id]

  (gen-args
    [ctx]
    [(choose-id ctx)])

  (apply-op
    [this store]
    (block/stat store id))

  (check
    [this model result]
    (if-let [block (get model id)]
      (do (is (map? result))
          (is (= (:id block) (:id result)))
          (is (= (:size block) (:size result)))
          (is (some? (:stored-at result))))
      (is (nil? result)))))


(defop ListBlocks
  [query]

  (gen-args
    [blocks]
    [(gen/fmap
       (fn select
         [[opts selection]]
         (select-keys opts selection))
       (gen/tuple
         (gen/hash-map
           :algorithm (gen/elements (keys digest/functions))
           :after (gen/fmap hex/encode (gen/not-empty gen/bytes)) ; TODO: pick prefixes
           :limit (gen/large-integer* {:min 1, :max (inc (count blocks))}))
         (gen/set (gen/elements #{:algorithm :after :limit}))))])

  (apply-op
    [this store]
    (doall (block/list store query)))

  (check
    [this model result]
    (let [expected-ids (cond->> (keys model)
                         (:after query)
                           (filter #(pos? (compare (multihash/hex %) (:after query))))
                         (:algorithm query)
                           (filter #(= (:algorithm query) (:algorithm %)))
                         true
                           (sort)
                         (:limit query)
                           (take (:limit query)))]
      (is (sequential? result))
      (is (= (count result) (count expected-ids)))
      (doseq [[id stat] (zipmap expected-ids result)]
        (if-let [block (get model id)]
          (do (is (map? stat))
              (is (= (:id block) (:id stat)))
              (is (= (:size block) (:size stat)))
              (is (some? (:stored-at stat))))
          (is (nil? stat)))))))


(defop GetBlock
  [id]

  (gen-args
    [blocks]
    [(choose-id blocks)])

  (apply-op
    [this store]
    (block/get store id))

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
    (block/put! store block))

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
    (block/delete! store id))

  (check
    [this model result]
    (if (contains? model id)
      (is (true? result))
      (is (false? result))))

  (update-model
    [this model]
    (dissoc model id)))


(defop GetBlockBatch
  [ids]

  (gen-args
    [blocks]
    [(gen/fmap (comp set keys) (gen-sub-map blocks))])

  (apply-op
    [this store]
    (block/get-batch store ids))

  (check
    [this model result]
    (is (coll? result))
    (is (= (set (keep model ids))
           (set result)))))


(defop PutBlockBatch
  [blocks]

  (gen-args
    [blocks]
    [(gen/fmap (comp set vals) (gen-sub-map blocks))])

  (apply-op
    [this store]
    (block/put-batch! store blocks))

  (check
    [this model result]
    (is (coll? result))
    (is (= (set blocks) (set result))))

  (update-model
    [this model]
    (into model (map (juxt :id identity) blocks))))


(defop DeleteBlockBatch
  [ids]

  (gen-args
    [blocks]
    [(gen/fmap (comp set keys) (gen-sub-map blocks))])

  (apply-op
    [this store]
    (block/delete-batch! store ids))

  (check
    [this model result]
    (is (set? result))
    (is (= (set (filter (set ids) (keys model))) result)))

  (update-model
    [this model]
    (apply dissoc model ids)))


(defop EraseStore
  []

  (apply-op
    [this store]
    (block/erase!! store))

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
    ; TODO: bloom filter prints... poorly.
    (block/scan store p))

  (check
    [this model result]
    (let [blocks (cond->> (vals model) p (filter p))]
      (is (= (count blocks) (:count result)))
      (is (= (reduce + (map :size blocks)) (:size result)))
      (is (map? (:sizes result)))
      (is (every? integer? (keys (:sizes result))))
      (is (= (count blocks) (reduce + (vals (:sizes result)))))
      (is (every? (partial sum/probably-contains? result) (map :id blocks))))))


(defop OpenBlock
  [id]

  (gen-args
    [blocks]
    [(choose-id blocks)])

  (apply-op
    [this store]
    (when-let [block (block/get store id)]
      (let [baos (java.io.ByteArrayOutputStream.)]
        (with-open [content (block/open block)]
          (io/copy content baos))
        (.toByteArray baos))))

  (check
    [this model result]
    (if-let [block (get model id)]
      (is (bytes= (.toByteArray ^PersistentBytes @block) result))
      (is (nil? result)))))


(defop OpenBlockRange
  [id start end]

  (gen-args
    [blocks]
    (gen/bind
      (choose-block blocks)
      (fn [block]
        (gen/fmap
          (fn [positions]
            (let [[start end] (sort positions)]
              {:id (:id block)
               :start start
               :end end}))
          (gen/vector-distinct
            (gen/large-integer* {:min 0, :max (:size block)})
            {:num-elements 2})))))

  (apply-op
    [this store]
    (when-let [block (block/get store id)]
      (let [baos (java.io.ByteArrayOutputStream.)]
        (with-open [content (block/open block start end)]
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
  (juxt gen->StatBlock
        gen->ListBlocks
        ;gen->ScanStore
        gen->GetBlock
        gen->OpenBlock
        gen->OpenBlockRange
        gen->PutBlock
        gen->DeleteBlock))


(def ^:private batch-op-generators
  (juxt gen->GetBlockBatch
        gen->PutBlockBatch
        gen->DeleteBlockBatch))


(def ^:private erasable-op-generators
  (juxt gen->EraseStore))


(defn- join-generators
  [ks]
  (let [op-gens (keep {:basic basic-op-generators
                       :batch batch-op-generators
                       :erase erasable-op-generators}
                      ks)]
    (fn [ctx]
      (into [] (mapcat #(% ctx)) op-gens))))



;; ## Operation Testing

(defn- start-store
  [constructor]
  (let [store (component/start (constructor))]
    (when-not (empty? (block/list store))
      (throw (IllegalStateException.
               (str "Cannot run integration test on " (pr-str store)
                    " as it already contains blocks!"))))
    (is (zero? (:count (block/scan store))))
    store))


(defn- stop-store
  [store]
  (block/erase!! store)
  (is (empty? (block/list store)) "ends empty")
  (component/stop store))


(defn- gen-blocks-context
  [test-blocks]
  (let [default-ctx (conj {} (first test-blocks))]
    (gen/fmap
      (fn [ctx] (if (seq ctx) ctx default-ctx))
      (gen-sub-map test-blocks))))


(def ^:private print-handlers
  {Multihash (puget/tagged-handler 'data/hash multihash/base58)
   Block (puget/tagged-handler 'data/block (partial into {}))
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
    Kinds of operations to test - vector of `:basic`, `:batch`, `:erase`.
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
         iterations 100
         repetitions 10}}]
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
    {:operations [:basic :batch :erase]
     :concurrency 1
     :repetitions 1}))
