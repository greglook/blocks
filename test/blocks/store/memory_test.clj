(ns blocks.store.memory-test
  (:require
    (blocks.store
      [memory :refer [memory-store]]
      [tests :as tests])
    [clojure.test :refer :all]))


(deftest ^:integration test-memory-store
  (tests/check-store! memory-store))
