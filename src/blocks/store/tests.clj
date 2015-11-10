(ns blocks.store.tests
  "This namespace contains a suite of tests to verify that a given block store
  implementation conforms to the spec."
  (:require
    [blocks.core :as block]
    [byte-streams :as bytes]
    [clojure.test :refer :all])
  (:import
    blocks.data.PersistentBytes))


(defn random-bytes
  "Returns a byte array between one and `max-size` bytes long with random
  content."
  [max-size]
  (let [size (inc (rand-int max-size))
        data (byte-array size)]
    (.nextBytes (java.security.SecureRandom.) data)
    data))


(defn populate-blocks!
  "Stores some test blocks in the given block store and returns a map of the
  ids to the original content values."
  [store n max-size]
  (->> (repeatedly #(random-bytes max-size))
       (take n)
       (map (juxt (comp :id (partial block/store! store)) identity))
       (into (sorted-map))))


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
      (with-open [stream (block/open block)]
        (is (bytes/bytes= content (bytes/to-byte-array stream))
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
    (is (= (:stored-at status)
           (:stored-at new-status)))))


(defn test-block-store
  "Tests a block store implementation."
  [label store n]
  (println "  *" label)
  (is (empty? (block/list store)) "starts empty")
  (testing (.getSimpleName (class store))
    (let [stored-content (populate-blocks! store n (* 256 1024))]
      (testing "list stats"
        (let [stats (block/list store)]
          (is (= (keys stored-content) (map :id stats))
              "enumerates all ids in sorted order")
          (is (every? #(= (:size %) (count (get stored-content (:id %)))) stats)
              "returns correct size for all blocks")))
      (doseq [[id content] stored-content]
        (test-block store id content))
      (let [[id content] (first (seq stored-content))]
        (test-restore-block store id content))
      (doseq [id (keys stored-content)]
        (block/delete! store id))
      (is (empty? (block/list store)) "ends empty"))))
