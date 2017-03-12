(ns blocks.store-test
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [clojure.test :refer :all]))


(deftest bucket-histogram
  (let [in-range (fn [s] (let [[a b] (store/bucket->range (store/size->bucket s))]
                           (and (<= a s) (< s b))))]
    (dotimes [i 16]
      (is (in-range i)))))


(deftest storage-summaries
  (let [block-a (block/read! "foo")
        block-b (block/read! "bar")
        block-c (block/read! "baz")
        summary-a (store/update-summary (store/init-summary) block-a)
        summary-ab (store/update-summary summary-a block-b)
        summary-c (store/update-summary (store/init-summary) block-c)]
    (is (= 0 (:count (store/init-summary))))
    (is (= 0 (:size (store/init-summary))))
    (is (empty? (:sizes (store/init-summary))))
    (is (= 1 (:count summary-a)))
    (is (= (:size block-a) (:size summary-a)))
    (is (= 2 (:count summary-ab)))
    (is (= (+ (:size block-a) (:size block-b)) (:size summary-ab)))
    (is (not (empty? (:sizes summary-ab))))
    (is (= (store/merge-summaries summary-ab summary-c)
           (store/update-summary summary-ab block-c)))
    (is (store/probably-contains? summary-ab (:id block-a)))
    (is (store/probably-contains? summary-ab (:id block-b)))
    (is (not (store/probably-contains? summary-ab (:id block-c))))))
