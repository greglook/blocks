(ns blocks.store.memory-test
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer [deftest is]]
    [multiformats.hash :as multihash]))


(deftest storage-realization
  (let [store (memory-block-store)
        content "foo bar baz qux"
        id (multihash/sha1 content)
        block (data/create-block
                id (count content)
                (fn lazy-reader
                  []
                  (java.io.ByteArrayInputStream. (.getBytes content))))]
    (is (block/lazy? block))
    (is (= block @(block/put! store block)))
    (is (= 15 (:size @(block/stat store id))))
    (is (block/loaded? @(block/get store id)))))


(deftest ^:integration check-behavior
  (tests/check-store memory-block-store))
