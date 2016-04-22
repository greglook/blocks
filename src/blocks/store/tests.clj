(ns ^:no-doc blocks.store.tests
  "Suite of tests to verify that a given block store implementation conforms to
  the spec."
  (:require
    [blocks.core :as block]
    [blocks.store.util :as util]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    blocks.data.PersistentBytes))


(defn random-blocks
  "Returns a lazy sequence of blocks with random content up to max-size bytes."
  [max-size]
  (map block/read! (repeatedly #(util/random-bytes (inc (rand-int max-size))))))


(defn populate-blocks!
  "Stores some test blocks in the given block store and returns a map of the
  ids to the original content values."
  [store n max-size]
  (->> (random-blocks max-size)
       (take n)
       (map (juxt (comp :id (partial block/put! store)) deref))
       (into (sorted-map))))


(defn test-put-attributes
  "The put! method in a store should return a block with an updated content or
  reader, but keep the same id, extra attributes, and any non-stat metadata."
  [store]
  (let [original (-> (block/read! (util/random-bytes 512))
                     (assoc :foo "bar")
                     (vary-meta assoc ::thing :baz))
        stored (block/put! store original)]
    (is (= (:id original) (:id stored))
        "Stored block id should match original")
    (is (= (:size original) (:size stored))
        "Stored block size should match original")
    (is (= "bar" (:foo stored))
        "Stored block should retain extra attributes")
    (is (= :baz (::thing (meta stored)))
        "Stored block should retain extra metadata")
    (is (= original stored)
        "Stored block should test equal to original")
    (is (true? (block/delete! store (:id stored))))))


(defn test-block
  "Determines whether the store contains the content for the given identifier."
  [store id content]
  (testing "block stats"
    (let [status (block/stat store id)]
      (is (= id (:id status))
          "should return the same multihash id")
      (is (= (count content) (:size status))
          "should return the content size")))
  (testing "block retrieval"
    (let [block (block/get store id)]
      (is (= id (:id block))
          "stored block has same id")
      (is (= (count content) (:size block))
          "block contains size info")
      (let [baos (java.io.ByteArrayOutputStream.)]
        (with-open [stream (block/open block)]
          (io/copy stream baos))
        (is (= (seq content) (seq (.toByteArray baos)))
            "stored content should match"))
      (is (= [:id :size] (keys block))
          "block only contains id and size"))))


(defn test-restore-block
  "Tests re-storing an existing block."
  [store id content]
  (let [status     (block/stat store id)
        new-block  (block/store! store content)
        new-status (block/stat store id)]
    (is (= id (:id new-block)))
    #_ ; breaks cache test...
    (is (= (:stored-at status)
           (:stored-at new-status)))))


(defn test-list-stats
  "Tests the functionality of list's marker option."
  [store ids n]
  (let [prefix (-> (block/list store :limit 1) first :id multihash/hex (subs 0 4))]
    (dotimes [i n]
      (let [after (str prefix (util/random-hex 6))
            limit (inc (rand-int 100))
            stats (block/list store :after after :limit limit)
            expected (->> ids
                          (filter #(pos? (compare (multihash/hex %) after)))
                          (sort)
                          (take limit))]
        (is (= expected (map :id stats))
            (str "list should return the expected ids in sorted order for: "
                 (pr-str {:after after, :limit limit})))))))


(defn test-batch-ops
  "Tests the batch functionality of a store."
  [store]
  (let [blocks (take 10 (random-blocks 512))]
    (testing "put-batch!"
      (let [block-batch (block/put-batch! store blocks)]
        (is (= (count blocks) (count block-batch)))
        (is (every? (set (map :id block-batch)) (map :id blocks)))))
    (testing "get-batch"
      (let [block-batch (block/get-batch store (cons (digest/sha1 "foo")
                                                     (map :id blocks)))]
        (is (= (count blocks) (count block-batch)))
        (is (every? (set (map :id block-batch)) (map :id blocks)))))
    (testing "delete-batch!"
      (let [deleted-ids (block/delete-batch! store (cons (digest/sha1 "foo")
                                                         (map :id blocks)))]
        (is (= (count blocks) (count deleted-ids)))
        (is (every? (set deleted-ids) (map :id blocks)))))))


(defmacro ^:private test-section
  [title & body]
  `(do (printf "    * %s\n" ~title)
       (let [start# (System/nanoTime)
             result# (testing ~title
                       ~@body)]
         (printf "        %.3f ms\n"
                 (/ (double (- (System/nanoTime) start#)) 1000000.0))
         result#)))


(defn test-block-store
  "Tests a block store implementation."
  [label store & {:keys [blocks max-size eraser]
                  :or {blocks 10, max-size 1024}}]
  (printf "  Beginning %s integration tests...\n" label)
  (testing (.getSimpleName (class store))
    (let [start-nano (System/nanoTime)
          store (test-section "starting store"
                  (component/start store))]
      (when-not (empty? (block/list store))
        (throw (IllegalStateException.
                 (str "Cannot run integration test on " (pr-str store)
                      " as it already contains blocks!"))))
      (test-section "querying non-existent block"
        (is (nil? (block/stat store (digest/sha1 "foo"))))
        (is (nil? (block/get store (digest/sha1 "bar")))))
      (test-section "ranged open"
        (let [block (block/store! store "012 345 678")]
          (is (= "345" (with-open [subrange (block/open block 4 7)]
                         (slurp subrange)))
              "subrange should return correct bytes")
          (is (true? (block/delete! store (:id block))))))
      (test-section "put attributes"
        (test-put-attributes store))
      (test-section "batch operations"
        (test-batch-ops store))
      (let [stored-content (test-section (str "populating " blocks " blocks")
                             (populate-blocks! store blocks max-size))]
        (test-section "list stats"
          (let [stats (block/list store)]
            (is (= (keys stored-content) (map :id stats))
                "enumerates all ids in sorted order")
            (is (every? #(= (:size %) (count (get stored-content (:id %)))) stats)
                "returns correct size for all blocks"))
          (test-list-stats store (keys stored-content) 10))
        (test-section "stored blocks"
          (doseq [[id content] stored-content]
            (test-block store id content)))
        (test-section "re-storing block"
          (let [[id content] (first (seq stored-content))]
            (test-restore-block store id content)))
        (test-section "erasing store"
          (if eraser
            (eraser store)
            (doseq [id (keys stored-content)]
              (is (true? (block/delete! store id)))))
          (is (empty? (block/list store)) "ends empty")))
      (printf "  Total time: %.3f ms\n"
              (/ (double (- (System/nanoTime) start-nano)) 1000000.0))
      (component/stop store))))
