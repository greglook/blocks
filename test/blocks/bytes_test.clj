(ns blocks.bytes-test
  (:require
    [byte-streams :refer [bytes=]]
    [clojure.test :refer :all])
  (:import
    blocks.data.PersistentBytes))


(deftest persistent-bytes-construction
  (testing "wrap does not duplicate the array"
    (let [data (byte-array 3)
          content (PersistentBytes/wrap data)]
      (is (= 3 (count content)) "content should contain three bytes")
      (is (= 0 (first content)) "first byte should be zero")
      (aset-byte data 0 8)
      (is (= 8 (first content)) "first byte should change to eight")))
  (testing "copyFrom duplicates the array"
    (let [data (byte-array 3)
          content (PersistentBytes/copyFrom data)]
      (is (= 3 (count content)) "content should contain three bytes")
      (is (= 0 (first content)) "first byte should be zero")
      (aset-byte data 0 8)
      (is (= 0 (first content)) "first byte should still be zero"))))


(deftest persistent-bytes-identity
  (let [pb1 (PersistentBytes/wrap (.getBytes "foo"))
        pb2 (PersistentBytes/wrap (.getBytes "foo"))
        pb3 (PersistentBytes/wrap (.getBytes "bar"))]
    (is (= pb1 pb2))
    (is (not= pb1 pb3))
    (is (= (hash pb1) (hash pb2)))
    (is (not= (hash pb1) (hash pb3)))
    (is (= (.hasheq pb1) (.hasheq pb2)))
    (is (not= (.hasheq pb1) (.hasheq pb3)))
    (is (bytes= (.open pb1) "foo"))))


(deftest persistent-bytes-coll
  (let [pb (PersistentBytes/wrap (.getBytes "baz"))]
    (is (= 3 (count pb)))
    (is (= [98 97 122] (seq pb)))
    (is (= 97 (nth pb 1)))
    (is (thrown? Exception (nth pb 5)))
    (is (= ::not-found (nth pb 5 ::not-found)))))
