(ns blocks.store.replica-test
  (:require
    (blocks.store
      [memory :refer [memory-store]]
      [replica :refer [replica-store]]
      [tests :refer [test-block-store]])
    [clojure.test :refer :all]))


; TODO: test that writes actually populate all backing stores.
; TODO: test that removing blocks from one store still returns all blocks.
; TODO: test that listing provides merged block list.


(deftest ^:integration test-replica-store
  (let [store (replica-store [(memory-store) (memory-store)])]
    (test-block-store
      "replica-store" store
      :max-size 1024
      :blocks 25)))
