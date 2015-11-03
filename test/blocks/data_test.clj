(ns blocks.data-test
  (:require
    [blocks.core :as block]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all])
  (:import
    blocks.data.PersistentBytes
    java.nio.ByteBuffer))


(deftest persistent-bytes
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
      (is (= 0 (first content)) "first byte should still be zero")))

  ; hashing
  ; equality
  ; toString
  ; seq
  ; count/nth

  ; .open
  ; .toBuffer
  )

; byte-streams extensions?
