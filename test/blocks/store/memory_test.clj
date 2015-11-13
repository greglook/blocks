(ns blocks.store.memory-test
  (:require
    (blocks.store
      [memory :refer [memory-store]]
      [tests :refer [test-block-store]])
    [clojure.test :refer :all]))


(deftest ^:integration test-memory-store
  (let [store (memory-store)]
    (test-block-store
      "memory-store" store
      :max-size 1024
      :blocks 25)))
