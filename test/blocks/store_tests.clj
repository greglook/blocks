(ns blocks.store-tests
  (:require
    [blocks.core :as block]
    (blocks.store
      [file :refer [file-store]]
      [memory :refer [memory-store]])
    [byte-streams :as bytes]
    [clojure.java.io :as io]
    [clojure.test :refer :all])
  (:import
    blocks.data.PersistentBytes))


;; ## Block Store Tests

(defn- store-test-blocks!
  "Stores some test blocks in the given block store and returns a map of the
  ids to the original string values."
  [store]
  (->> ["foo" "bar" "baz" "foobar" "barbaz"]
       (map (juxt (comp :id (partial block/store! store)) identity))
       (into (sorted-map))))


(defn- test-block-content
  "Determines whether the store contains the content for the given identifier."
  [store id content]
  (let [status (block/stat store id)
        block (block/get store id)
        stored-content (:content block)]
    (is (and status stored-content) "returns info and content")
    (is (instance? PersistentBytes stored-content))
    (is (= (:stat/size status) (block/size block)) "stats contain size info")
    (is (= content (bytes/convert stored-content String)) "stored content matches input")))


(defn- test-restore-block
  "Tests re-storing an existing block."
  [store id content]
  (let [status     (block/stat store id)
        new-block   (block/store! store content)
        new-status (block/stat store id)]
    (is (= id (:id new-block)))
    (is (= (:stat/stored-at status)
           (:stat/stored-at new-status)))))


(defn test-block-store
  "Tests a block store implementation."
  [store label]
  (println "  *" label)
  (is (empty? (block/list store)) "starts empty")
  (testing (str (-> store class .getSimpleName))
    (let [stored-content (store-test-blocks! store)]
      (is (= (keys stored-content) (block/list store {}))
          "enumerates all ids in sorted order")
      (doseq [[id content] stored-content]
        (test-block-content store id content))
      (let [[id content] (first (seq stored-content))]
        (test-restore-block store id content))
      (let [expected-size (reduce + 0 (map (comp count #(.getBytes %))
                                           (vals stored-content)))]
        (is (= expected-size (block/scan-size store))))
      (doseq [id (keys stored-content)]
        (block/delete! store id))
      (is (empty? (block/list store)) "ends empty"))))



;; ## Storage Implementations

(deftest test-memory-store
  (let [store (memory-store)]
    (test-block-store store "memory-store")))


(deftest test-file-store
  (let [tmpdir (io/file "target" "test" "tmp"
                        (str "file-block-store."
                          (System/currentTimeMillis)))
        store (file-store tmpdir)]
    (test-block-store store "file-store")
    (block/erase!! store)))
