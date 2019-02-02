(ns blocks.store.memory-test
  (:require
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.test :as bst]
    [clojure.test :refer :all]))


(deftest ^:integration check-behavior
  (bst/check-store memory-block-store))
