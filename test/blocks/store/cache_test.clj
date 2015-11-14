(ns blocks.store.cache-test
  (:require
    (blocks.store
      [cache :refer [cache-store]]
      [memory :refer [memory-store]]
      [tests :as tests :refer [test-block-store]])
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]))


(deftest ^:integration test-cache-store
  (let [size-limit (* 16 1024)
        primary (memory-store)
        cache (memory-store)
        store (cache-store size-limit
                :max-block-size 1024
                :primary primary
                :cache cache)]
    (test-block-store
      "cache-store" store
      :max-size 4096
      :blocks 50)
    (is (= store (component/start (component/start store)))
        "starting cache store again is a no-op")
    (is (= store (component/stop store))
        "starting cache store again is a no-op"))
  (let [size-limit (* 16 1024)
        primary (memory-store)
        cache (memory-store)
        store (cache-store size-limit
                :primary primary
                :cache cache)]
    (tests/populate-blocks! cache 50 1536)
    (test-block-store
      "cache-store" store
      :max-size 4096
      :blocks 50)))
