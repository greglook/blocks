(ns blocks.store.tests
  "Suite of tests to verify that a given block store implementation conforms to
  the spec."
  (:require
    [alphabase.bytes :refer [random-bytes]]
    [alphabase.hex :as hex]
    [blocks.core :as block]
    [blocks.summary :as sum]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [clojure.test.check :as check]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [com.stuartsierra.component :as component]
    [multihash.core :as multihash]
    [multihash.digest :as digest]
    [puget.color.ansi :as ansi]
    [puget.printer :as puget])
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



;; ## Operation Multimethods

(defmulti ^:private apply-op
  "Apply the operation to the store, returning a result value."
  (fn dispatch
    [store [op-key args]]
    op-key))


(defmethod apply-op :default
  [store [op-key args]]
  ; Try to resolve the key as a method in the blocks.core namespace.
  (if-let [method (ns-resolve 'blocks.core (symbol (name op-key)))]
    (method store args)
    (throw (ex-info (str "No apply function available for operation " (pr-str op-key))
                    {:op-key op-key}))))


(defmulti ^:private update-model
  "Apply an update to the model based on the operation."
  (fn dispatch
    [model [op-key args]]
    op-key))


(defmethod update-model :default
  [model _]
  ; By default, ops have no effect on model state.
  model)


(defmulti ^:private check-op
  "Make assertions about an operation given the operation type, the model
  state, and the args/response being tested."
  ; TODO: should this include `is` or return a boolean?
  (fn dispatch
    [model [op-key args result]]
    op-key))


(defmethod check-op :default
  [_ _]
  ; By default, checks make no assertions.
  true)


(defmacro ^:private defop
  "Defines a new specification for a store operation test."
  [op-key & forms]
  (let [form-map (into {} (map (juxt first rest)) forms)]
    `(do
       (defn- ~(symbol (str "gen-" (name op-key) "-op"))
         ~(str "Constructs a " (name op-key) " operation spec generator.")
         ~@(if-let [gen-form (get form-map 'gen-args)]
             (if (= 1 (count gen-form))
               [['blocks]
                `(gen/tuple
                   (gen/return ~op-key)
                   ~(list (first gen-form) 'blocks))]
               [(first gen-form)
                `(gen/tuple
                   (gen/return ~op-key)
                   (do ~@(rest gen-form)))])
             [['blocks]
              `(gen/return [~op-key])]))
       ~(when-let [apply-form (get form-map 'apply)]
          `(defmethod apply-op ~op-key
             [store# [op-key# args#]]
             (let [~(first apply-form) [store# args#]]
               ~@(rest apply-form))))
       ~(when-let [check-form (get form-map 'check)]
          `(defmethod check-op ~op-key
             [model# [op-key# args# result#]]
             (let [~(first check-form) [model# args# result#]]
               ~@(rest check-form))))
       ~(when-let [update-form (get form-map 'update)]
          `(defmethod update-model ~op-key
             [model# [op-key# args#]]
             (let [~(first update-form) [model# args#]]
               ~@(rest update-form)))))))



;; ## Operation Generators

(defn- choose-id
  "Returns a generator which will select a block id from the model pool."
  [blocks]
  (gen/elements (keys blocks)))


(defn- choose-block
  "Returns a generator which will select a block from the model pool."
  [blocks]
  (gen/elements (vals blocks)))


(defop :wait

  (gen-args
    [_]
    (gen/choose 1 100))

  (apply
    [store duration]
    (Thread/sleep duration)))


(defop :stat

  (gen-args choose-id)

  (check
    [model id result]
    (if-let [block (get model id)]
      (and (map? result)
           (= (:id block) (:id result))
           (= (:size block) (:size result))
           (some? (:stored-at result)))
      (nil? result))))


(defop :list

  (gen-args
    [blocks]
    (gen/fmap
      (fn select
        [[opts selection]]
        (select-keys opts selection))
      (gen/tuple
        (gen/hash-map
          :algorithm (gen/elements (keys digest/functions))
          :after (gen/fmap hex/encode (gen/not-empty gen/bytes)) ; TODO: pick prefixes
          :limit (gen/large-integer* {:min 1, :max (inc (count blocks))}))
        (gen/set (gen/elements #{:algorithm :after :limit})))))

  (apply
    [store query]
    (doall (block/list store query)))

  (check
    [model query result]
    (let [expected-ids (cond->> (keys model)
                         (:after query)
                           (filter #(pos? (compare (multihash/hex %) (:after query))))
                         (:algorithm query)
                           (filter #(= (:algorithm query) (:algorithm %)))
                         true
                           (sort)
                         (:limit query)
                           (take (:limit query)))]
      (and (is (sequential? result))
           (is (= (count result) (count expected-ids)))
           (is (every? #(check-op model (cons :stat %))
                       (map vector expected-ids result)))))))


(defop :get

  (gen-args choose-id)

  (check
    [model id result]
    (if-let [block (get model id)]
      (do
        (is (some? (:id result)))
        (is (integer? (:size result)))
        (is (= id (:id result)))
        (is (= (:size block) (:size result))))
      (nil? result))))


(defop :put!

  (gen-args choose-block)

  (check
    [model block result]
    (= block result))

  (update
    [model block]
    (assoc model (:id block) block)))


(defop :delete!

  (gen-args choose-id)

  (check
    [model id result]
    (if (contains? model id)
      (true? result)
      (false? result)))

  (update
    [model id]
    (dissoc model id)))


(defop :get-batch

  (gen-args
    [blocks]
    (gen/set (choose-id blocks)))

  (check
    [model ids result]
    (and (is (coll? result))
         (is (= (set (keep model ids))
                (set result))))))


(defop :put-batch!

  (gen-args
    [blocks]
    (gen/set (choose-block blocks)))

  (check
    [model blocks result]
    (and (coll? result)
         (= (set blocks) (set result))))

  (update
    [model blocks]
    (into model (map (juxt :id identity) blocks))))


(defop :delete-batch!

  (gen-args
    [blocks]
    (gen/set (choose-id blocks)))

  (check
    [model ids result]
    (and (set? result)
         (= result (set (filter (set ids) (keys model))))))

  (update
    [model ids]
    (apply dissoc model ids)))


(defop :erase!!

  (apply
    [store _]
    (block/erase!! store))

  (update
    [model _]
    (empty model)))


(defop :scan

  (gen-args
    [_]
    (gen/elements
      [nil
       (fn scan-pred
         [stat]
         (< (:size stat) 256))]))

  (check
    [model p result]
    (let [blocks (cond->> (vals model) p (filter p))]
      (and (= (count blocks) (:count result))
           (= (reduce + (map :size blocks)) (:size result))
           (map? (:sizes result))
           (every? integer? (keys (:sizes result)))
           (= (count blocks) (reduce + (vals (:sizes result))))
           (every? (partial sum/probably-contains? result) (map :id blocks))))))


(defop :open

  (gen-args choose-id)

  (apply
    [store id]
    (when-let [block (block/get store id)]
      (let [baos (java.io.ByteArrayOutputStream.)]
        (with-open [content (block/open block)]
          (io/copy content baos))
        (.toByteArray baos))))

  (check
    [model id result]
    (if-let [block (get model id)]
      (bytes= (.toBuffer ^PersistentBytes @block) result)
      (nil? result))))


(defop :open-range

  (gen-args
    [blocks]
    (gen/bind
      (choose-block blocks)
      (fn [block]
        (gen/fmap
          (fn [positions]
            (vec (cons (:id block) (sort positions))))
          (gen/vector-distinct
            (gen/large-integer* {:min 0, :max (:size block)})
            {:num-elements 2})))))

  (apply
    [store [id start end]]
    (when-let [block (block/get store id)]
      (let [baos (java.io.ByteArrayOutputStream.)]
        (with-open [content (block/open block start end)]
          (io/copy content baos))
        (.toByteArray baos))))

  (check
    [model [id start end] result]
    (if-let [block (get model id)]
      (let [baos (java.io.ByteArrayOutputStream.)
            length (- end start)
            subarray (byte-array length)]
        (block/write! block baos)
        (System/arraycopy (.toByteArray baos) start subarray 0 length)
        (bytes= subarray result))
      (nil? result))))


(defn- op-generators
  [blocks]
  [(gen-list-op blocks)
   (gen-scan-op blocks)
   (gen-stat-op blocks)
   (gen-get-op blocks)
   (gen-get-batch-op blocks)
   (gen-open-op blocks)
   (gen-open-range-op blocks)
   (gen-put!-op blocks)
   (gen-put-batch!-op blocks)
   (gen-delete!-op blocks)
   (gen-delete-batch!-op blocks)
   (gen-erase!!-op blocks)])


(defn- gen-op-seq
  "Generates non-empty sequences of store operations for linear testing."
  [blocks]
  (->> (op-generators blocks)
       (gen/one-of)
       (gen/list)
       (gen/not-empty)))


(defn- gen-op-seq*
  "Generates non-empty sequences of store operations for multithread testing,
  including timing wait ops."
  [blocks]
  (->> (op-generators blocks)
       (cons (gen-wait-op nil))
       (gen/one-of)
       (gen/list)
       (gen/not-empty)))



;; ## Operation Testing

(defn- apply-ops!
  "Apply a sequence of operations to a store, returning a list of op vectors
  with appended results."
  [store ops]
  (reduce
    (fn [results op]
      (conj results (conj op (apply-op store op))))
    []
    ops))


(defn- valid-ops?
  "Determines whether the given sequence of operations produced valid results
  when applied to the store. Returns true if the store behavior is valid."
  [ops]
  (loop [model {}
         ops ops]
    (if-let [op (first ops)]
      (if (check-op model op)
        (recur (update-model model op) (rest ops))
        false)
      true)))


(defn check-store
  "Uses generative tests to validate the behavior of a block store
  implementation. The first argument must be a no-arg constructor function which
  will produce a new block store for testing. The remaining options control the
  behavior of the tests:

  - `blocks`      generate this many random blocks to test the store with
  - `max-size`    maximum block size to generate, in bytes
  - `iterations`  number of generative tests to perform

  Returns the results of the generative tests."
  [constructor & {:keys [blocks max-size iterations]
                  :or {blocks 20, max-size 1024, iterations 100}}]
  {:pre [(fn? constructor)]}
  (let [test-blocks (generate-blocks! blocks max-size)]
    (check/quick-check iterations
      (prop/for-all [ops (gen-op-seq test-blocks)]
        (let [store (constructor)]
          (component/start store)
          (try
            (when-not (empty? (block/list store))
              (throw (IllegalStateException.
                       (str "Cannot run integration test on " (pr-str store)
                            " as it already contains blocks!"))))
            (is (zero? (:count (block/scan store))))
            ; TODO: multithread test
            (let [results (apply-ops! store ops)
                  validity (valid-ops? results)]
              (block/erase!! store)
              (is (empty? (block/list store)) "ends empty")
              validity)
            (finally
              (try
                (component/stop store)
                (catch Exception ex
                  (println "Error stopping store:" ex))))))))))


(defn check-store!
  "Runs tests as for `check-store` but interprets the results for
  `clojure.test`. Throws an exception if the tests do not succeed."
  [& args]
  (let [info (apply check-store args)]
    (cond
      (true? (:result info))
        true

      (or (false? (:result info))
          (instance? Throwable (:result info)))
        (do
          (puget/pprint
            info
            {:print-color true
             :print-handlers #(get {Multihash (puget/tagged-handler 'data/hash multihash/base58)
                                    Block (puget/tagged-handler 'data/block (partial into {}))}
                                   %
                                   (puget/common-handlers %))})
          (if (false? (:result info))
            (throw (ex-info (format "Tests failed after %d iterations" (:num-tests info))
                            {:info info}))
            (throw (:result info))))

      :else
        (throw (ex-info (str "Unknown info format: " (pr-str info))
                        {:info info})))))
