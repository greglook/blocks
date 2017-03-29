(ns blocks.summary-test
  (:require
    [blocks.core :as block]
    [blocks.summary :as summary]
    [clojure.test :refer :all]))


(deftest bucket-histogram
  (let [in-range (fn [s] (let [[a b] (summary/bucket->range (summary/size->bucket s))]
                           (and (<= a s) (< s b))))]
    (dotimes [i 16]
      (is (in-range i)))))


(deftest storage-summaries
  (let [block-a (block/read! "foo")
        block-b (block/read! "bar")
        block-c (block/read! "baz")
        summary-a (summary/update (summary/init) block-a)
        summary-ab (summary/update summary-a block-b)
        summary-c (summary/update (summary/init) block-c)]
    (is (= 0 (:count (summary/init))))
    (is (= 0 (:size (summary/init))))
    (is (empty? (:sizes (summary/init))))
    (is (= 1 (:count summary-a)))
    (is (= (:size block-a) (:size summary-a)))
    (is (= 2 (:count summary-ab)))
    (is (= (+ (:size block-a) (:size block-b)) (:size summary-ab)))
    (is (not (empty? (:sizes summary-ab))))
    (is (= (summary/merge summary-ab summary-c)
           (summary/update summary-ab block-c)))
    (is (summary/probably-contains? summary-ab (:id block-a)))
    (is (summary/probably-contains? summary-ab (:id block-b)))
    (is (not (summary/probably-contains? summary-ab (:id block-c))))))
