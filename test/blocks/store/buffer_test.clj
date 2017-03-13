(ns blocks.store.buffer-test
  (:require
    [blocks.core :as block]
    (blocks.store
      [memory :refer [memory-block-store]]
      [buffer :refer [buffer-block-store] :as buffer]
      [tests :as tests])
    [clojure.test :refer :all]))


(deftest buffer-behavior
  (let [backer (memory-block-store)
        store (buffer-block-store
                :store backer
                :buffer (memory-block-store))
        buffer (:buffer store)
        a (block/read! "foo bar baz")
        b (block/read! "abracadabra")
        c (block/read! "123 xyz")]
    (block/put! store a)
    (block/put! store b)
    (is (empty? (block/list backer)))
    (block/put! backer c)
    (is (= 3 (count (block/list store))))
    (is (every? (set (map :id [a b c]))
                (map :id (block/list store))))
    (is (= (:id c) (:id (block/put! store c))))
    (is (= 2 (count (block/list buffer))))
    (let [flush-summary (buffer/flush! store)]
      (is (= 2 (:count flush-summary)))
      (is (= 22 (:size flush-summary))))
    (is (zero? (:count (buffer/clear! store))))
    (block/put! store (block/read! "XYZ"))
    (is (= 1 (:count (buffer/clear! store))))
    (is (empty? (block/list buffer)))
    (is (= 3 (count (block/list backer))))
    (block/delete! store (:id c))
    (is (= 2 (count (block/list store))))))


(deftest buffer-size-limits
  (let [backer (memory-block-store)
        buffer (memory-block-store)
        store (buffer-block-store
                :store backer
                :buffer buffer
                :max-block-size 8)
        a (block/read! "foo")
        b (block/read! "bar")
        c (block/read! "abcdefghijklmnopqrstuvwxyz")]
    (block/put! store a)
    (block/put! store b)
    (block/put! store c)
    (is (= 3 (count (block/list store))))
    (is (= 2 (count (block/list buffer))))
    (is (= 1 (count (block/list backer))))
    (is (= (:id c) (:id (first (block/list backer)))))))


(deftest ^:integration test-buffer-store
  (tests/check-store! #(buffer-block-store
                         :store (memory-block-store)
                         :buffer (memory-block-store))))
