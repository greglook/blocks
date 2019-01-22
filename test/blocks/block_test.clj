(ns blocks.block-test
  (:require
    [blocks.data :as data]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]))


(deftest block-type
  (let [b1 (data/read-block :sha1     "howdy frobblenitz")
        b2 (data/read-block :sha2-256 "howdy frobblenitz")
        b2' (vary-meta b2 assoc ::test 123)]
    (testing "equality"
      (is (= b1 (data/load-block (:id b1) (.content b1))))
      (is (= b2 b2'))
      (is (not= b1 b2)
          "blocks with different algorithms should not be equal"))
    (testing "hash codes"
      (is (= (hash b2) (hash b2')))
      (is (not= (hash b1) (hash b2))
          "blocks with different algorithms should not be equal"))
    (testing "comparison"
      (is (zero? (compare b1 b1)))
      (is (zero? (compare b2 b2')))
      (is (neg? (compare b1 b2))
          "should sort blocks by id"))
    (testing "metadata"
      (is (nil? (meta b1))
          "should be constructed with no metadata")
      (is (= 123 (::test (meta b2'))))
      (is (= {:foo true} (meta (with-meta b1 {:foo true})))
          "should support metadata"))
    (testing "accessors"
      (is (some? (:id b1)))
      (is (number? (:size b2)))
      (is (inst? (:stored-at b1))))
    (testing "print-method"
      (is (string? (pr-str b1))))))


(deftest block-laziness
  (let [content "foo bar baz abc123"
        loaded (data/read-block :sha1 content)
        lazy (data/create-block
               (:id loaded) (:size loaded)
               #(bytes/to-input-stream (.getBytes content)))
        wrapped (data/wrap-content loaded (constantly ::wrapped))]
    (is (data/byte-content? loaded))
    (is (not (data/byte-content? lazy)))
    (is (= loaded wrapped))
    (is (not (data/byte-content? wrapped)))))
