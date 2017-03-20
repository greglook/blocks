(ns blocks.store-test
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.store.memory :refer [memory-block-store]]
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



(deftest block-syncing
  (let [block-a (block/read! "789")  ; 35a9
        block-b (block/read! "123")  ; a665
        block-c (block/read! "456")  ; b3a8
        block-d (block/read! "ABC")] ; b5d4
    (testing "empty dest"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-b)
                     (block/put! block-c))
            dest (memory-block-store)]
        (is (= 3 (count (block/list source))))
        (is (empty? (block/list dest)))
        (let [sync-summary (block/sync source dest)]
          (is (= 3 (:count sync-summary)))
          (is (= 9 (:size sync-summary))))
        (is (= 3 (count (block/list source))))
        (is (= 3 (count (block/list dest))))))
    (testing "subset source"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-c))
            dest (doto (memory-block-store)
                   (block/put! block-a)
                   (block/put! block-b)
                   (block/put! block-c))
            summary (block/sync source dest)]
        (is (zero? (:count summary)))
        (is (zero? (:size summary)))
        (is (= 2 (count (block/list source))))
        (is (= 3 (count (block/list dest))))))
    (testing "mixed blocks"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-c))
            dest (doto (memory-block-store)
                   (block/put! block-b)
                   (block/put! block-d))
            summary (block/sync source dest)]
        (is (= 2 (:count summary)))
        (is (= 6 (:size summary)))
        (is (= 2 (count (block/list source))))
        (is (= 4 (count (block/list dest))))))
    (testing "filter logic"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-c))
            dest (doto (memory-block-store)
                   (block/put! block-b)
                   (block/put! block-d))
            summary (block/sync source dest :filter (comp #{(:id block-c)} :id))]
        (is (= 1 (:count summary)))
        (is (= 3 (:size summary)))
        (is (= 2 (count (block/list source))))
        (is (= 3 (count (block/list dest))))))))
