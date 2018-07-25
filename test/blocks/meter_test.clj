(ns blocks.meter-test
  (:require
    [blocks.core :as block]
    [blocks.meter :as meter]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]))


(deftest store-behavior
  (let [store (assoc (memory-block-store)
                     ::events (atom [])
                     ::meter/recorder
                     (fn record!
                       [store event]
                       (swap! (::events store) conj event)))
        a (block/read! "foo bar baz")
        b (block/read! "abracadabra")
        c (block/read! "123 xyz")]
    (block/put! store a)
    (block/put! store b)
    ; TODO: check events
    ;(prn @(::events store))
    ))
