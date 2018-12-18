(ns blocks.store.memory-test
  (:require
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.test-harness :as test-harness]
    [clojure.test :refer :all]))


(deftest ^:integration check-behavior
  (test-harness/check-store memory-block-store))
