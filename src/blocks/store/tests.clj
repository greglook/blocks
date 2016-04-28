(ns blocks.store.tests
  "Suite of tests to verify that a given block store implementation conforms to
  the spec."
  (:require
    [alphabase.bytes :refer [random-bytes]]
    [alphabase.hex :as hex]
    [blocks.core :as block]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]
    [clojure.test.check :as check]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [com.stuartsierra.component :as component]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    blocks.data.PersistentBytes))


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



;; ## Generators

(def gen-block
  "Generator which constructs blocks with random content and one of the
  available hashing functions."
  (gen/fmap
    (partial apply block/read!)
    (gen/tuple
      (gen/not-empty (gen/scale (partial * 10) gen/bytes))
      (gen/elements (keys digest/functions)))))


(defn- gen-list-opts
  "Generator for options maps to pass into a block/list call."
  [blocks]
  ; TODO: how to test permutations of these better?
  (let [gen-limit (gen/large-integer* {:min 1, :max (inc (count blocks))})]
    (gen/one-of
      [(gen/hash-map
         :algorithm (gen/elements (keys digest/functions))
         :limit gen-limit)
       (gen/hash-map
         :after (gen/fmap hex/encode (gen/not-empty gen/bytes))
         :limit gen-limit)])))


(defn- gen-store-op
  "Test generator which creates a single operation against the store."
  [blocks]
  (let [gen-op (fn [op-key gen-args] (gen/tuple (gen/return op-key) gen-args))
        gen-block-key (gen/elements (keys blocks))
        gen-block-val (gen/elements (vals blocks))]
    (gen/one-of
      [(gen-op :stat             gen-block-key)
       (gen-op :list             (gen-list-opts blocks))
       (gen-op :get              gen-block-key)
       (gen-op :put!             gen-block-val)
       (gen-op :delete!          gen-block-key)
       (gen-op :get-batch        (gen/not-empty (gen/set gen-block-key)))
       (gen-op :put-batch!       (gen/not-empty (gen/set gen-block-val)))
       (gen-op :delete-batch!    (gen/not-empty (gen/set gen-block-key)))
       (gen-op :open-block       gen-block-key)
       (gen-op :open-block-range (gen/fmap
                                   (fn [[id a b]]
                                     (if (< a b)
                                       [id a b]
                                       [id b a]))
                                   (gen/tuple gen-block-key
                                              gen/nat
                                              gen/nat)))])))



;; ## Testing

(defn- apply-op!
  "Applies an operation to the store by using the op keyword to resolve a method
  in the `blocks.core` namespace. Returns the result of calling the method."
  [store [op-key args]]
  ;(println ">>" op-key (pr-str args))
  (case op-key
    :open-block
      (block/get store args)

    :open-block-range
      (block/get store (first args))

    (let [var-name (symbol (name op-key))
          method (ns-resolve 'blocks.core var-name)]
      (method store args))))


(defn- check-stat-result
  [block result]
  (if block
    (testing "stored block"
      (is (map? result))
      (is (= (:id block) (:id result)))
      (is (= (:size block) (:size result)))
      (is (some? (:stored-at result))))
    (testing "missing block"
      (is (nil? result)))))


(defn- check-op
  "Checks that the result of an operation matches the model of the store's
  contents. Returns true if the operation and model match."
  [model [op-key args] result]
  (case op-key
    :stat
      (check-stat-result (get model args) result)

    :list
      (let [{:keys [algorithm after limit]} args
            expected-ids
              (cond->> (keys model)
                after
                  (filter #(pos? (compare (multihash/hex %) after)))
                algorithm
                  (filter #(= algorithm (:algorithm %)))
                true
                  (sort)
                limit
                  (take limit))]
        (is (sequential? result))
        (is (= (count result) (count expected-ids)))
        (is (every?
               (fn [[id act]] (check-stat-result (get model id) act))
               (map vector expected-ids result))
            "all stat results are rturned"))

    :get
      (if-let [block (get model args)]
        (is (= block result))
        (is (nil? result)))

    :put!
      (do (is (= (:id result) (:id args)))
          (is (= (:size result) (:size args))))

    :delete!
      (if (contains? model args)
        (testing "stored block"
          (is (true? result)))
        (testing "missing block"
          (is (false? result))))

    :get-batch
      (let [expected-blocks (keep model args)]
        (is (coll? result))
        (is (= (set expected-blocks) (set result))))

    :put-batch!
      (is (= (set args) (set result)))

    :delete-batch!
      (let [contained-ids (keep (set args) (keys model))]
        (is (= (set contained-ids) result)))

    :open-block
      (if-let [block (get model args)]
        (is (bytes= (.open (.content block)) (block/open result)))
        (is (nil? result)))

    :open-block-range
      (let [[id start end] args]
        (if-let [block (get model id)]
          (is (bytes= (@#'blocks.core/bounded-input-stream
                        (.open (.content block)) start end)
                      (block/open result
                                  (min start (:size block))
                                  (min end   (:size block)))))
          (is (nil? result))))))


(defn- update-model
  [model [op-key args]]
  (case op-key
    :put! (assoc model (:id args) args)
    :delete! (dissoc model args)
    :put-batch! (into model (map (juxt :id identity) args))
    :delete-batch! (apply dissoc model args)
    model))


(defn- valid-op-seq?
  "Determines whether the given sequence of operations produces valid results
  when applied to the store."
  [store ops]
  (loop [model {}
         ops ops]
    (if (seq ops)
      (let [op (first ops)
            result (apply-op! store op)]
        (if (check-op model op result)
          (recur (update-model model op)
                 (rest ops))
          (throw (ex-info "Illegal operation result:"
                          {:op op, :result result}))))
      true)))


(defn check-store
  [constructor & {:keys [blocks max-size iterations eraser]
                  :or {blocks 20, max-size 1024, iterations 100}}]
  {:pre [(some? constructor)]}
  (let [test-blocks (generate-blocks! blocks max-size)]
    (check/quick-check iterations
      (prop/for-all [ops (gen/list (gen-store-op test-blocks))]
        (let [store (constructor)]
          (component/start store)
          (try
            (when-not (empty? (block/list store))
              (throw (IllegalStateException.
                       (str "Cannot run integration test on " (pr-str store)
                            " as it already contains blocks!"))))
            (let [result (valid-op-seq? store ops)]
              (if eraser
                (eraser store)
                (block/delete-batch! store (set (keys test-blocks))))
              (is (empty? (block/list store)) "ends empty")
              result)
            (finally
              (try
                (component/stop store)
                (catch Exception ex
                  (println "Error stopping store:" ex))))))))))


(defn check-store!
  "Interprets test results from `check-store!` for a `clojure.test` result."
  [& args]
  (let [info (apply check-store args)]
    (cond
      (true? (:result info))
        true

      (instance? Throwable (:result info))
        (throw (ex-info (str "Tests failed after " (:num-tests info) " iterations")
                        info
                        (:result info)))

      :else
        (throw (ex-info "Unknown info format" info)))))
