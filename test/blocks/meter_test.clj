(ns blocks.meter-test
  (:require
    [blocks.core :as block]
    [blocks.meter :as meter]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]))


(deftest store-behavior
  (let [events (atom [])
        store (assoc (memory-block-store)
                     ::events events
                     ::meter/recorder
                     (fn record!
                       [store event]
                       (swap! (::events store) conj event)))
        a (block/read! "foo bar baz")
        b (block/read! "abracadabra")]
    (block/put! store a)
    (testing "basic event attributes"
      (is (= 1 (count @events)))
      (let [event (first @events)]
        (is (= "MemoryBlockStore" (:label event)))
        (is (= ::meter/method-time (:type event)))
        (is (= ::block/put! (:method event)))
        (is (= a (:args event)))
        (is (number? (:value event)))))
    (reset! events [])
    (testing "misbehaved recorder"
      (let [store (assoc store
                         ::meter/label "memory"
                         ::meter/recorder
                         (fn record!
                           [store event]
                           (swap! (::events store) conj event)
                           (throw (ex-info "Bang!" {}))))]
        (block/put! store b)
        (is (= 1 (count @events)))
        (let [event (first @events)]
          (is (= "memory" (:label event)))
          (is (= ::meter/method-time (:type event)))
          (is (= ::block/put! (:method event)))
          (is (= b (:args event)))
          (is (number? (:value event))))))
    (reset! events [])
    (testing "more gets"
      (is (= a (block/get store (:id a))))
      (is (= 1 (count @events)))
      (let [event (first @events)]
        (is (= ::meter/method-time (:type event)))
        (is (= ::block/get (:method event)))
        (is (= (:id a) (:args event)))
        (is (number? (:value event)))))))


(deftest metered-stream
  (let [events (atom [])
        store (assoc (memory-block-store)
                     ::events events
                     ::meter/recorder
                     (fn record!
                       [store event]
                       (swap! (::events store) conj event)))
        content "the quick fox jumped over the lazy brown dog"
        abc (block/store! store content)]
    (binding [meter/*io-report-period* 0]
      (testing "read one byte"
        (reset! events [])
        (with-open [input (block/open abc)]
          (.read input)
          (is (= 1 (count @events)))
          (let [event (first @events)]
            (is (= "MemoryBlockStore" (:label event)))
            (is (= ::meter/io-read (:type event)))
            (is (= 1 (:value event))))))
      (testing "read remaining bytes"
        (reset! events [])
        (with-open [input (block/open abc)]
          (is (= content (slurp input))))
        (is (= 1 (count @events)))
        (let [event (first @events)]
          (is (= ::meter/io-read (:type event)))
          (is (= 44 (:value event))))))))
