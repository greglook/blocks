(ns blocks.store.memory-test
  (:require
    (blocks.store
      [memory :refer [memory-block-store]]
      [tests :as tests])
    [clojure.test :refer :all]))


(deftest ^:integration test-memory-block-store
  (tests/check-store! memory-block-store))
