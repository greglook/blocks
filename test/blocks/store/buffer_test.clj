(ns blocks.store.buffer-test
  (:require
    [blocks.core :as block]
    [blocks.store.buffer :refer [buffer-block-store] :as buffer]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]))


(deftest lifecycle
  (is (thrown? IllegalStateException
        (component/start (buffer-block-store
                           :buffer (memory-block-store)))))
  (is (thrown? IllegalStateException
        (component/start (buffer-block-store
                           :primary (memory-block-store)))))
  (let [store (buffer-block-store)]
    (is (identical? store (component/stop store)))))


(deftest buffer-behavior
  (let [primary (memory-block-store)
        buffer (memory-block-store)
        store (buffer-block-store
                :primary primary
                :buffer buffer)
        a (block/read! "foo bar baz")
        b (block/read! "abracadabra")
        c (block/read! "123 xyz")]
    @(block/put! store a)
    @(block/put! store b)
    (is (empty? (block/list-seq primary)))
    @(block/put! primary c)
    (is (= 3 (count (block/list-seq store))))
    (is (every? (set (map :id [a b c]))
                (map :id (block/list-seq store))))
    (is (= (:id c) (:id @(block/put! store c))))
    (is (= 2 (count (block/list-seq buffer))))
    (let [flush-summary @(buffer/flush! store)]
      (is (= 2 (:count flush-summary)))
      (is (= 22 (:size flush-summary))))
    (is (zero? (:count @(buffer/clear! store))))
    @(block/put! store (block/read! "XYZ"))
    (is (= 1 (:count @(buffer/clear! store))))
    (is (empty? (block/list-seq buffer)))
    (is (= 3 (count (block/list-seq primary))))
    @(block/delete! store (:id c))
    (is (= 2 (count (block/list-seq store))))))


(deftest buffer-predicate
  (let [primary (memory-block-store)
        buffer (memory-block-store)
        store (buffer-block-store
                :primary primary
                :buffer buffer
                :predicate #(< (:size %) 8))
        a (block/read! "foo")
        b (block/read! "bar")
        c (block/read! "abcdefghijklmnopqrstuvwxyz")]
    @(block/put! store a)
    @(block/put! store b)
    @(block/put! store c)
    (is (= 3 (count (block/list-seq store))))
    (is (= 2 (count (block/list-seq buffer))))
    (is (= 1 (count (block/list-seq primary))))
    (is @(block/stat primary (:id c)))))


(deftest ^:integration check-behavior
  (tests/check-store
    #(buffer-block-store
       :primary (memory-block-store)
       :buffer (memory-block-store))))
