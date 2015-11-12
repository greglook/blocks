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


(deftest block-merging
  (let [a (-> (data/read-block :sha1 "foo")
              (assoc :foo 123 :bar "baz")
              (vary-meta assoc ::abc 123))
        b (-> (data/read-block :sha1 "foo")
              (assoc :foo true :qux 'thing)
              (vary-meta assoc ::abc 456 ::xyz :ok))
        c (data/read-block :sha1 "bar")]
    (testing "merging blocks with different ids"
      (is (thrown? IllegalArgumentException
                   (data/merge-blocks a c))))
    (testing "merged block"
      (let [merged (data/merge-blocks a b)]
        (is (= (:id merged) (:id b))
            "should have b's id")
        (is (= (:size merged) (:size b))
            "should have b's size")
        (is (identical? @merged @b)
            "should have b's content")
        (is (nil? (.reader merged))
            "should have no reader")
        (is (= true (:foo merged)))
        (is (= "baz" (:bar merged)))
        (is (= 'thing (:qux merged)))
        (is (= 456 (::abc (meta merged))))
        (is (= :ok (::xyz (meta merged))))))))
