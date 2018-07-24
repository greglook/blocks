(ns blocks.store.meter-test
  (:require
    [blocks.core :as block]
    [blocks.store.meter :refer [metered-block-store] :as meter]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]))


(deftest store-construction
  (is (thrown? IllegalArgumentException (metered-block-store :foo))
      "construction with non-function should not work"))


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


#_
(deftest ^:integration check-behavior
  (tests/check-store
    #(metered-block-store
       (constantly nil)
       :store (memory-block-store))))
