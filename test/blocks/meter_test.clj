(ns blocks.meter-test
  (:require
    [blocks.core :as block]
    [blocks.meter :as meter]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]))


#_
(deftest store-behavior
  (let [underlying (memory-block-store)
        store (metered-block-store
                (fn record!
                  [store event]
                  (swap! (:events store) conj event))
                :store underlying
                :events (atom []))
        a (block/read! "foo bar baz")
        b (block/read! "abracadabra")
        c (block/read! "123 xyz")]
    (block/put! store a)
    (block/put! store b)
    (prn @(:events store))
    ; TODO: check events
    ))
