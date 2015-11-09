(ns blocks.block-test
  (:require
    [blocks.data :as data]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]))


(deftest block-type
  (let [b1 (data/read-block "howdy frobblenitz" :sha1)
        b2 (data/read-block "howdy frobblenitz" :sha2-256)]
    (testing "equality"
      (is (= b1 (empty b1))
          "empty block should still be equal")
      (is (not= b1 (assoc b1 :foo :bar))
          "extra attributes should affect equality")
      (is (not= b1 b2)
          "blocks with different algorithms should not be equal"))
    (testing "hash codes"
      (is (= (hash b1) (hash (empty b1)))
          "empty block should have same hashcode")
      (is (not= (hash b1) (hash b2))
          "blocks with different algorithms should not be equal"))
    (testing "comparison"
      (is (neg? (compare b1 b2))
          "should sort blocks by id")
      (is (pos? (compare (assoc b1 :foo true) b1))
          "extra attributes affect sort order"))
    (testing "metadata"
      (is (nil? (meta b1))
          "should be constructed with no metadata")
      (is (= {:foo true} (meta (with-meta b1 {:foo true})))
          "should support metadata"))
    (testing "accessors"
      (is (some? (:id b1)))
      (is (number? (:size b2))))
    (testing "map methods"
      (is (= 3 (count (assoc b1 :foo true)))
          "count should return base plus attributes")
      (is (= {:foo true} (meta (empty (with-meta b2 {:foo true}))))
          "empty should preserve metadata")
      (is (= :bar (:foo (conj b2 [:foo :bar])))
          "cons adds key-value vector pairs as attributes")
      (is (contains? b2 :id))
      (is (= [:size 17] (find b2 :size)))
      (is (= [[:id (:id b1)] [:size 17] [:foo "bar"]]
             (seq (assoc b1 :foo "bar"))))
      (is (= b1 (dissoc (assoc b1 :qux 123) :qux))
          "extra attributes can be dissociated"))
    (testing "fixed attributes"
      (is (thrown? IllegalArgumentException (assoc b2 :id "not-a-multihash"))
          "identifier should not be settable")
      (is (thrown? IllegalArgumentException (assoc b2 :size 1234))
          "size should not be settable")
      (is (thrown? IllegalArgumentException (assoc b2 :content nil))
          "content should not be settable")
      (is (thrown? IllegalArgumentException (assoc b2 :reader nil))
          "reader should not be settable")
      (is (thrown? IllegalArgumentException (dissoc b1 :id))
          "should not be dissociatable"))
    ))
