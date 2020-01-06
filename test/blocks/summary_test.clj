(ns blocks.summary-test
  (:require
    [blocks.data :as data]
    [blocks.summary :as sum]
    [clojure.test :refer [deftest is]]))


(deftest bucket-histogram
  (dotimes [i 16]
    (let [[a b] (sum/bucket->range (sum/size->bucket i))]
      (is (<= a i))
      (is (< i b)))))


(deftest storage-summaries
  (let [block-a (data/read-block :sha1 "foo")
        block-b (data/read-block :sha1 "bar")
        block-c (data/read-block :sha1 "baz")
        summary-a (sum/update (sum/init) block-a)
        summary-ab (sum/update summary-a block-b)
        summary-c (sum/update (sum/init) block-c)]
    (is (= 0 (:count (sum/init))))
    (is (= 0 (:size (sum/init))))
    (is (empty? (:sizes (sum/init))))
    (is (= 1 (:count summary-a)))
    (is (= (:size block-a) (:size summary-a)))
    (is (= 2 (:count summary-ab)))
    (is (= (+ (:size block-a) (:size block-b)) (:size summary-ab)))
    (is (seq (:sizes summary-ab)))
    (is (= (sum/merge summary-ab summary-c)
           (sum/update summary-ab block-c)))))
