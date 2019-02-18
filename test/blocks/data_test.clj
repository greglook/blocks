(ns blocks.data-test
  (:require
    [blocks.data :as data]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]
    [multiformats.hash :as multihash]))


(deftest block-type
  (let [b1 (data/read-block :sha1     "howdy frobblenitz")
        b2 (data/read-block :sha2-256 "howdy frobblenitz")
        b2' (vary-meta b2 assoc ::test 123)]
    (testing "equality"
      (is (= b1 (data/read-block :sha1 "howdy frobblenitz")))
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


(deftest content-reading
  (testing "persistent bytes"
    (let [block (data/read-block :sha1 "foo bar baz")]
      (is (= "foo bar baz" (slurp (data/content-stream block nil nil))))
      (is (= "bar baz" (slurp (data/content-stream block 4 nil))))
      (is (= "foo" (slurp (data/content-stream block nil 3))))
      (is (= "bar" (slurp (data/content-stream block 4 7))))))
  (testing "reader function"
    (let [content "foo bar baz"
          block (data/create-block
                  (multihash/sha1 content)
                  (count content)
                  (fn reader
                    []
                    (java.io.ByteArrayInputStream. (.getBytes content))))]
      (is (= "foo bar baz" (slurp (data/content-stream block nil nil))))
      (is (= "bar baz" (slurp (data/content-stream block 4 nil))))
      (is (= "foo" (slurp (data/content-stream block nil 3))))
      (is (= "bar" (slurp (data/content-stream block 4 7)))))))


(deftest hasher-resolution
  (testing "invalid algorithm name types"
    (is (thrown? IllegalArgumentException (data/hasher nil)))
    (is (thrown? IllegalArgumentException (data/hasher 123))))
  (testing "keyword algorithm name"
    (is (thrown? IllegalArgumentException (data/hasher :sha8-4096))
        "unsupported algorithm should throw exception")
    (is (ifn? (data/hasher :sha2-256)))))


(deftest block-construction
  (let [id (multihash/sha1 "foo")]
    (is (thrown? Exception
          (data/create-block "foo" 123 (constantly nil))))
    (is (thrown? Exception
          (data/create-block id 0 (constantly nil))))
    (is (thrown? Exception
          (data/create-block id 1 :inst (constantly nil))))
    (is (thrown? Exception
          (data/create-block id 1 nil)))))


(deftest block-merging
  (let [a (-> (data/read-block :sha1 "foo")
              (vary-meta assoc ::abc 123))
        b (-> (data/read-block :sha1 "foo")
              (vary-meta assoc ::abc 456 ::xyz :ok))
        c (data/read-block :sha1 "bar")]
    (testing "merging blocks with different ids"
      (is (thrown-with-msg? Exception #"Cannot merge blocks with differing ids"
            (data/merge-blocks a c))))
    (testing "merging blocks with different sizes"
      (is (thrown-with-msg? Exception #"Cannot merge blocks with differing sizes"
            (data/merge-blocks
              a (data/create-block (:id a) 8 (constantly nil))))))
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
