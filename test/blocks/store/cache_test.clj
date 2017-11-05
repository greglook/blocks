(ns blocks.store.cache-test
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.store.cache :as cache :refer [caching-block-store]]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]))


(defn new-cache
  "Helper function to construct a fresh cache store backed by empty memory
  stores."
  [size-limit & args]
  (apply caching-block-store
    size-limit
    :primary (memory-block-store)
    :cache (memory-block-store)
    args))


(deftest store-construction
  (is (thrown? IllegalArgumentException (caching-block-store nil)))
  (is (thrown? IllegalArgumentException (caching-block-store 0)))
  (is (thrown? IllegalArgumentException (caching-block-store 512 :max-block-size "foo")))
  (is (thrown? IllegalArgumentException (caching-block-store 512 :max-block-size 0)))
  (is (satisfies? store/BlockStore (caching-block-store 512 :max-block-size 128))))


(deftest store-lifecycle
  (let [store (new-cache 1024)]
    (is (thrown? IllegalStateException
                 (component/start (assoc store :primary nil)))
        "starting cache without primary throws an exception")
    (is (thrown? IllegalStateException
                 (component/start (assoc store :cache nil)))
        "starting cache without cache throws an exception")
    (is (= store (component/start (component/start store)))
        "starting cache store again is a no-op")
    (is (= store (component/stop store))
        "stopping cache store is a no-op")))


(deftest uninitialized-store
  (let [store (new-cache 1024)]
    (testing "no primary store"
      (is (thrown? IllegalStateException (block/list (assoc store :primary nil)))))
    (testing "no cache store"
      (is (thrown? IllegalStateException (block/list (assoc store :cache nil)))))
    (testing "not started"
      (is (thrown? IllegalStateException (block/list store))))))


(deftest extant-cache-contents
  (let [store (new-cache 1024)
        blocks (tests/populate-blocks! (:cache store) :max-size 64)
        store' (component/start store)]
    (is (every? #(block/stat (:cache store) %) (keys blocks))
        "all blocks should still be present in store")
    (is (every? (:priorities @(:state store)) (keys blocks))
        "all blocks should have an entry in the priority map")))


(deftest space-reaping
  (let [store (new-cache 1024)
        blocks (tests/populate-blocks! (:cache store) :n 32)
        store (component/start store)]
    (is (< 1024 (:total-size @(:state store)))
        "has more than size-limit blocks cached")
    (cache/reap! store 512)
    (is (<= (:total-size @(:state store)) 512)
        "reap cleans up at least the desired free space")))


(deftest size-limits
  (testing "block without limit"
    (let [store (component/start (new-cache 512))
          block (block/put! store (block/read! "0123456789"))]
      (is (block/stat store (:id block)) "block is stored")
      (is (block/stat (:cache store) (:id block)) "cache should store block")))
  (testing "block under limit"
    (let [store (component/start (new-cache 512 :max-block-size 16))
          block (block/put! store (block/read! "0123456789"))]
      (is (block/stat store (:id block)) "block is stored")
      (is (block/stat (:cache store) (:id block)) "cache should store block")))
  (testing "block over limit"
    (let [store (component/start (new-cache 512 :max-block-size 16))
          block (block/put! store (block/read! "0123456789abcdef0123"))]
      (is (block/stat store (:id block)) "block is stored")
      (is (nil? (block/stat (:cache store) (:id block))) "cache should not store block")))
  (testing "block larger than cache"
    (let [store (component/start (new-cache 16))
          block (block/put! store (block/read! "0123456789abcdef0123"))]
      (is (block/stat store (:id block)) "block is stored")
      (is (nil? (block/stat (:cache store) (:id block))) "cache should not store block"))))


(deftest ^:integration check-behavior
  (tests/check-store
    #(caching-block-store 2048
       :max-block-size 512
       :primary (memory-block-store)
       :cache (memory-block-store))))
