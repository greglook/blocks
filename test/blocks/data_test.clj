(ns blocks.data-test
  (:require
    [blocks.data :as data]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]))


(deftest hasher-resolution
  (testing "invalid algorithm name types"
    (is (thrown? IllegalArgumentException (data/checked-hasher nil)))
    (is (thrown? IllegalArgumentException (data/checked-hasher 123))))
  (testing "keyword algorithm name"
    (is (thrown? IllegalArgumentException (data/checked-hasher :sha8-4096))
        "unsupported algorithm should throw exception")
    (is (ifn? (data/checked-hasher :sha2-256))))
  (testing "direct function"
    (is (ifn? (data/checked-hasher identity))))
  (testing "hash function which returns a non-multihash value"
    (let [hasher (data/checked-hasher (constantly :bar))]
      (is (thrown? RuntimeException (hasher :foo))
          "should throw an exception when invoked"))))
