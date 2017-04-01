(ns blocks.bytes-test
  (:require
    [byte-streams :refer [bytes=]]
    [clojure.test :refer :all])
  (:import
    blocks.data.PersistentBytes))


(defn- ->pb
  "Construct a new `PersistentBytes` value containing the given byte data."
  [& data]
  (PersistentBytes/wrap (byte-array data)))


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
    (is (= pb1 (.getBytes "foo"))
        "should be equal to raw bytes")
    (is (= pb1 (.toBuffer pb1))
        "should be equal to byte buffer")
    (is (= pb1 pb2)
        "should be equal to persistent bytes with equal content")
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


(deftest byte-comparison
  (testing "equal arrays"
    (is (zero? (compare (->pb) (->pb))))
    (is (zero? (compare (->pb 1 2 3) (->pb 1 2 3)))))
  (testing "equal prefixes"
    (is (neg? (compare (->pb 1 2 3)
                       (->pb 1 2 3 4))))
    (is (pos? (compare (->pb 1 2 3 4)
                       (->pb 1 2 3)))))
  (testing "order-before"
    (is (neg? (compare (->pb 1 2 3)
                       (->pb 1 2 4))))
    (is (neg? (compare (->pb 1 2 3)
                       (->pb 1 3 2))))
    (is (neg? (compare (->pb 0 2 3 4)
                       (->pb 1 3 2 1))))
  (testing "order-after"
    (is (pos? (compare (->pb 1 2 4)
                       (->pb 1 2 3))))
    (is (pos? (compare (->pb 1 3 2)
                       (->pb 1 2 3))))
    (is (pos? (compare (->pb 1 3 2 1)
                       (->pb 0 2 3 4)))))))
