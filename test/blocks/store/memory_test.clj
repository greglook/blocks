(ns blocks.store.memory-test
  (:require
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]))


(deftest ^:integration check-behavior
  (tests/check-store memory-block-store))
