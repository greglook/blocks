(ns blocks.store.tests
  "Suite of tests to verify that a given block store implementation conforms to
  the spec."
  (:require
    [alphabase.bytes :refer [random-bytes]]
    [alphabase.hex :as hex]
    [blocks.core :as block]
    [blocks.summary :as summary]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]
    [clojure.test.check :as check]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [com.stuartsierra.component :as component]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    blocks.data.Block
    blocks.data.PersistentBytes))


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



;; ## Result Checkers

(defn- check-stat
  "Checker for the response for block stats."
  [model id result]
  (if-let [block (get model id)]
    (testing "for stored block"
      (is (map? result))
      (is (= (:id block) (:id result)))
      (is (= (:size block) (:size result)))
      (is (some? (:stored-at result))))
    (testing "for missing block"
      (is (nil? result)))))


(defn- check-list
  "Checker for the response for a list call."
  [model {:keys [algorithm after limit]} result]
  (let [expected-ids
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
    (is (= (count result) (count expected-ids))
        "result should match the number of blocks expected")
    (is (every? (partial apply check-stat model)
                (map vector expected-ids result))
        "all stat results are returned")))



;; ## Operation Generators

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


(defn- gen-block-range
  "Generator for a tuple containing a block id and a start and end offset, where
  the offsets are guaranteed to follow `0 <= start <= end <= size`."
  [blocks]
  (gen/bind
    (gen/elements (vals blocks))
    (fn [block]
      (gen/fmap
        (fn [positions]
          (vec (cons (:id block) (sort positions))))
        (gen/vector (gen/large-integer* {:min 0, :max (:size block)}) 2)))))


(def ^:private store-operations
  "Map of information about the various operations which the tests can perform
  on the block store. Each operation may have the following properties:

  - `:args`    A generator for arguments to the operation, called with the map
               of test blocks: `(args# blocks)`
  - `:apply`   An optional function called with `(apply# store args)` which
               should apply the operation to the store and return the store's
               response. If not provided, the operation will be resolved
               as a var, for example `:get` will become `#'blocks.core/get`.
  - `:check`   A function which checks that an operation returned a valid
               result value. Called with: `(check model args result)`
  - `:update`  An optional method to update the model after an operation is
               applied. Called with `(update model args)` and should return
               an updated model. If not provided, the op is a read which does
               not affect the model state."

  (let [choose-id (comp gen/elements keys)
        choose-block (comp gen/elements vals)]
    {:stat
     {:args choose-id
      :check check-stat}

     :list
     {:args gen-list-opts
      :check check-list}

     :get
     {:args choose-id
      :check
      (fn check-get
        [model id result]
        (if-let [block (get model id)]
          (is (= block result)
              "returned block should be equivalent to model")
          (is (nil? result)
              "missing block should return nil")))}

     :put!
     {:args choose-block
      :check
      (fn check-put
        [model block result]
        (is (= (:id block) (:id result)))
        (is (= (:size block) (:size result)))
        (is (= block result)))
      :update
      (fn update-put
        [model block]
        (assoc model (:id block) block))}

     :delete!
     {:args choose-id
      :check
      (fn check-delete
        [model id result]
        (if (contains? model id)
          (is (true? result)
              "deleting a stored block should return true")
          (is (false? result)
              "deleting a missing block should return false")))
      :update
      (fn update-delete
        [model id]
        (dissoc model id))}

     :get-batch
     {:args (comp gen/not-empty gen/set choose-id)
      :check
      (fn check-get-batch
        [model ids result]
        (let [expected-blocks (keep model (set ids))]
          (is (coll? result))
          (is (= (set expected-blocks) (set result)))))}

     :put-batch!
     {:args (comp gen/not-empty gen/set choose-block)
      :check
      (fn check-put-batch
        [model blocks result]
        (is (= (set blocks) (set result))))
      :update
      (fn update-put-batch
        [model blocks]
        (into model (map (juxt :id identity) blocks)))}

     :delete-batch!
     {:args (comp gen/not-empty gen/set choose-id)
      :check
      (fn check-delete-batch
        [model ids result]
        (let [contained-ids (keep (set ids) (keys model))]
          (is (set? result))
          (is (= (set contained-ids) result))))
      :update
      (fn update-delete-batch
        [model ids]
        (apply dissoc model ids))}

     :scan
     {:args (constantly (gen/return (constantly true)))
      :check
      (fn check-summary
        [model p result]
        (is (= (count model) (:count result)))
        (is (= (reduce + (map :size (vals model))) (:size result)))
        (is (map? (:sizes result)))
        (is (every? integer? (keys (:sizes result))))
        (is (= (count model) (reduce + (vals (:sizes result)))))
        (is (every? (partial summary/probably-contains? result) (map :id (vals model)))))}

     :open-block
     {:args choose-id
      :apply block/get
      :check
      (fn check-open-block
        [model id result]
        (if-let [block (get model id)]
          (is (bytes= (.open ^PersistentBytes (.content ^Block block)) (block/open result)))
          (is (nil? result))))}

     :open-block-range
     {:args gen-block-range
      :apply
      (fn apply-block-range
        [store args]
        (block/get store (first args)))
      :check
      (fn check-open-block-range
        [model [id start end] result]
        (if-let [block (get model id)]
          (is (bytes= (@#'blocks.core/bounded-input-stream
                        (.open ^PersistentBytes (.content ^Block block)) start end)
                      (block/open result start end)))
          (is (nil? result))))}}))


(defn- gen-store-op
  "Test generator which creates a single operation against the store."
  [blocks]
  (gen/bind
    (gen/elements (keys store-operations))
    (fn [op-key]
      (gen/tuple
        (gen/return op-key)
        ((get-in store-operations [op-key :args]) blocks)))))



;; ## Operation Testing

(defn- apply-op!
  "Applies an operation to the store by using the op keyword to resolve a method
  in the `blocks.core` namespace. Returns the result of calling the method."
  [store [op-key args]]
  ;(println ">>" op-key (pr-str args))
  (if-let [method (or (get-in store-operations [op-key :apply])
                      (ns-resolve 'blocks.core (symbol (name op-key))))]
    (method store args)
    (throw (ex-info "No apply function available for operation!"
                    {:op-key op-key}))))


(defn- check-op
  "Checks that the result of an operation matches the model of the store's
  contents. Returns true if the operation and model match."
  [model [op-key args] result]
  (if-let [checker (get-in store-operations [op-key :check])]
    (checker model args result)
    (throw (ex-info "No check function available for operation!"
                    {:op-key op-key}))))


(defn- update-model
  "Updates the model after an operation has been applied to the store under
  test. Returns an updated version of the model."
  [model [op-key args]]
  (if-let [updater (get-in store-operations [op-key :update])]
    (updater model args)
    model))


(defn- valid-op-seq?
  "Determines whether the given sequence of operations produces valid results
  when applied to the store. Returns true if the store behavior is valid."
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



;; ## Store Tests

(defn check-store
  "Uses generative tests to validate the behavior of a block store
  implementation. The first argument must be a no-arg constructor function which
  will produce a new block store for testing. The remaining options control the
  behavior of the tests:

  - `blocks`      generate this many random blocks to test the store with
  - `max-size`    maximum block size to generate, in bytes
  - `iterations`  number of generative tests to perform
  - `eraser`      optional custom function to completely remove the store

  Returns the results of the generative tests."
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
            (is (zero? (:count (block/scan store))))
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
  "Runs tests as for `check-store` but interprets the results for
  `clojure.test`. Throws an exception if the tests do not succeed."
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
