(ns blocks.meter-test
  (:require
    [blocks.data :as data]
    [blocks.meter :as meter]
    [clojure.test :refer :all]
    [manifold.stream :as s]))


(defrecord TestStore
  [])


(defn- recording-store
  []
  (let [events (atom [])]
    (map->TestStore
      {::events events
       ::meter/label "TestStore"
       ::meter/recorder
       (fn record!
         [store event]
         (swap! (::events store) conj event))})))


(deftest block-metering
  (testing "constructor"
    (let [block (data/read-block :sha1 "foo bar baz")]
      (is (nil? (meter/metered-block {} :metric nil))
          "should be nil-tolerant")
      (is (identical? block (meter/metered-block {} :metric block))
          "disabled store should return block unaltered")))
  (testing "block reading"
    (let [store (recording-store)
          events (::events store)
          content "the quick fox jumped over the lazy brown dog"
          block (data/read-block :sha1 content)
          block' (meter/metered-block store ::io block)]
      (is (not (identical? block' block)))
      (binding [meter/*io-report-period* 0]
        (testing "read one byte"
          (reset! events [])
          (with-open [input (data/content-stream block' nil nil)]
            (.read input)
            (is (= 1 (count @events)))
            (let [event (first @events)]
              (is (= "TestStore" (:label event)))
              (is (= ::io (:type event)))
              (is (= 1 (:value event))))))
        (testing "read remaining bytes"
          (reset! events [])
          (with-open [input (data/content-stream block' nil nil)]
            (is (= content (slurp input))))
          (is (= 1 (count @events)))
          (let [event (first @events)]
            (is (= ::io (:type event)))
            (is (= 44 (:value event)))))))))


(deftest measure-method
  (testing "constructor"
    (let [d (Object.)]
      (is (identical? d (meter/measure-method {} :foo nil d))
          "disabled store should return deferred unaltered")))
  (testing "elapsed time"
    (let [store (recording-store)
          events (::events store)
          o (Object.)
          o' (meter/measure-method
               store :foo nil
               (do (Thread/sleep 3) o))]
      (is (identical? o o'))
      (is (= 1 (count @events)))
      (is (= ::meter/method-time (:type (first @events))))
      (is (<= 3.0 (:value (first @events)))))))


(deftest miscellaney
  (testing "store labeling"
    (is (= "TestStore" (#'meter/meter-label (->TestStore))))
    (is (= "FooStore" (#'meter/meter-label (map->TestStore
                                             {::meter/label "FooStore"}))))))
