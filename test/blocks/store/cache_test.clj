(ns blocks.store.cache-test
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.store.cache :as cache :refer [caching-block-store]]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer [deftest testing is]]
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


(deftest lifecycle
  (testing "construction validation"
    (is (thrown? IllegalArgumentException
          (caching-block-store nil)))
    (is (thrown? IllegalArgumentException
          (caching-block-store 0)))
    (is (satisfies? store/BlockStore (caching-block-store 512))))
  (testing "starting"
    (is (thrown? IllegalStateException
          (component/start (caching-block-store
                             128
                             :cache (memory-block-store)))))
    (is (thrown? IllegalStateException
          (component/start (caching-block-store
                             128
                             :primary (memory-block-store))))))
  (testing "stopping"
    (let [store (caching-block-store 256)]
      (is (identical? store (component/stop store))))))


(deftest extant-cache-contents
  (let [store (new-cache 1024)
        blocks (tests/populate-blocks! (:cache store) :max-size 64)
        store (component/start store)]
    (is (every? #(deref (block/stat (:cache store) %)) (keys blocks))
        "all blocks should still be present in store")
    (is (every? (:priorities @(:state store)) (keys blocks))
        "all blocks should have an entry in the priority map")))


(deftest space-reaping
  (let [store (new-cache 1024)
        _ (tests/populate-blocks! (:cache store) :n 32)
        store (component/start store)]
    (is (< 1024 (:total-size @(:state store)))
        "has more than size-limit blocks cached")
    (let [reaped @(cache/reap! store 512)]
      (is (pos? (:count reaped)))
      (is (< 10000 (:size reaped))))
    (is (<= (:total-size @(:state store)) 512)
        "reap cleans up at least the desired free space")))


(deftest size-limits
  (testing "block without limit"
    (let [store (component/start (new-cache 512))
          block @(block/put! store (block/read! "0123456789"))]
      (is @(block/stat store (:id block)) "block is stored")
      (is @(block/stat (:cache store) (:id block)) "cache should store block")))
  (testing "block under limit"
    (let [store (component/start (new-cache 512 :predicate #(< (:size %) 16)))
          block @(block/put! store (block/read! "0123456789"))]
      (is @(block/stat store (:id block)) "block is stored")
      (is @(block/stat (:cache store) (:id block)) "cache should store block")))
  (testing "block over limit"
    (let [store (component/start (new-cache 512 :predicate #(< (:size %) 16)))
          block @(block/put! store (block/read! "0123456789abcdef0123"))]
      (is @(block/stat store (:id block)) "block is stored")
      (is (nil? @(block/stat (:cache store) (:id block))) "cache should not store block")))
  (testing "block larger than cache"
    (let [store (component/start (new-cache 16))
          block @(block/put! store (block/read! "0123456789abcdef0123"))]
      (is @(block/stat store (:id block)) "block is stored")
      (is (nil? @(block/stat (:cache store) (:id block))) "cache should not store block"))))


;; TODO: cache behavior on get/put directly


(deftest ^:integration check-behavior
  (tests/check-store
    #(caching-block-store
       2048
       :predicate (fn [block] (< (:size block) 512))
       :primary (memory-block-store)
       :cache (memory-block-store))))
