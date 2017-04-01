(ns blocks.summary-test
  (:require
    [blocks.core :as block]
    [blocks.summary :as sum]
    [clojure.test :refer :all]))


(deftest bucket-histogram
  (let [in-range (fn [s] (let [[a b] (sum/bucket->range (sum/size->bucket s))]
                           (and (<= a s) (< s b))))]
    (dotimes [i 16]
      (is (in-range i)))))


(deftest storage-summaries
  (let [block-a (block/read! "foo")
        block-b (block/read! "bar")
        block-c (block/read! "baz")
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
    (is (not (empty? (:sizes summary-ab))))
    (is (= (sum/merge summary-ab summary-c)
           (sum/update summary-ab block-c)))
    (is (sum/probably-contains? summary-ab (:id block-a)))
    (is (sum/probably-contains? summary-ab (:id block-b)))
    (is (not (sum/probably-contains? summary-ab (:id block-c))))))
