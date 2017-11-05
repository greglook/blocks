(ns blocks.store.replica-test
  (:require
    [blocks.core :as block]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.replica :refer [replica-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]))


(deftest store-construction
  (is (thrown-with-msg? Exception #"missing configured keys"
        (component/start (replica-block-store [:a :b])))))


(deftest replica-behavior
  (let [replica-1 (memory-block-store)
        replica-2 (memory-block-store)
        store (replica-block-store [:a :b] :a replica-1 :b replica-2)
        a (block/read! "foo bar baz")
        b (block/read! "abracadabra")
        c (block/read! "123 xyz")]
    (block/put! store a)
    (block/put! store b)
    (block/put! store c)
    (is (= 3 (count (block/list replica-1))))
    (is (every? (partial block/get replica-1)
                (map :id [a b c]))
        "all blocks are stored in replica-1")
    (is (= 3 (count (block/list replica-2))))
    (is (every? (partial block/get replica-2)
                (map :id [a b c]))
        "all blocks are stored in replica-2")
    (is (= 3 (count (block/list store))))
    (block/delete! replica-1 (:id a))
    (block/delete! replica-2 (:id c))
    (is (= 3 (count (block/list store)))
        "replica lists all available blocks")))


(deftest ^:integration check-behavior
  (tests/check-store
    #(replica-block-store
       [:a :b]
       :a (memory-block-store)
       :b (memory-block-store))))
