(ns blocks.data-test
  (:require
    [blocks.data :as data]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]))


(deftest hasher-resolution
  (testing "invalid algorithm name types"
    (is (thrown? IllegalArgumentException (data/hasher nil)))
    (is (thrown? IllegalArgumentException (data/hasher 123))))
  (testing "keyword algorithm name"
    (is (thrown? IllegalArgumentException (data/hasher :sha8-4096))
        "unsupported algorithm should throw exception")
    (is (ifn? (data/hasher :sha2-256)))))


(deftest block-merging
  (let [a (-> (data/read-block :sha1 "foo")
              (vary-meta assoc ::abc 123))
        b (-> (data/read-block :sha1 "foo")
              (vary-meta assoc ::abc 456 ::xyz :ok))
        c (data/read-block :sha1 "bar")]
    (testing "merging blocks with different ids"
      (is (thrown-with-msg? Exception #"Cannot merge blocks with differing ids"
            (data/merge-blocks a c))))
    (testing "merged block"
      (let [merged (data/merge-blocks a b)]
        (is (= (:id merged) (:id b))
            "should have b's id")
        (is (= (:size merged) (:size b))
            "should have b's size")
        (is (= (:stored-at merged) (:stored-at b))
            "should have b's timestamp")
        (is (identical? (.content merged) (.content b))
            "should have b's content")
        (is (= 456 (::abc (meta merged))))
        (is (= :ok (::xyz (meta merged))))))))
