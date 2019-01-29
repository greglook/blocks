(ns blocks.store.file-test
  (:require
    [blocks.core :as block]
    [blocks.store.file :as file :refer [file-block-store]]
    [blocks.store.test-harness :as test-harness]
    [clojure.java.io :as io]
    [clojure.test :refer :all]))


(defn- mk-tmpdir!
  []
  (io/file "target" "test" "tmp"
           (str "file-block-store." (System/currentTimeMillis))))


#_
(deftest file-listing
  (let [store (file-block-store (mk-tmpdir!))
        a (block/read! "larry")
        b (block/read! "curly")
        c (block/read! "moe")]
    @(block/put! store a)
    @(block/put! store b)
    @(block/put! store c)
    (is (= 3 (count (block/list store :after "12200c"))))
    (is (= 3 (count (block/list store :after "12200d0980"))))
    (is (= 1 (count (block/list store :after "12204b6f51"))))
    (is (= (:id b) (:id (first (block/list store :after "12204b6f51")))))
    (is (empty? (block/list store :after "122064")))))


(deftest ^:integration check-behavior
  (let [tmpdir (mk-tmpdir!)]
    (test-harness/check-store
      #(let [store (file-block-store tmpdir)]
         @(block/erase! store)
         store))))
