(ns blocks.meter-test
  (:require
    [blocks.data :as data]
    [blocks.meter :as meter]
    [blocks.test-utils :refer [quiet-exception]]
    [clojure.test :refer [deftest testing is]]
    [manifold.deferred :as d]
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


(deftest measure-stream
  (testing "constructor"
    (let [stream (s/stream 10)]
      (is (identical? stream (meter/measure-stream {} ::flow nil stream))
          "disabled store should return stream unaltered")))
  (testing "stream flow"
    (let [store (recording-store)
          events (::events store)
          stream (s/stream 10)
          metered (binding [meter/*io-report-period* 0.02]
                    (meter/measure-stream store ::flow {} stream))]
      (is (empty? @events))
      (s/consume any? metered)
      @(s/put! stream :x)
      @(s/put! stream :x)
      (Thread/sleep 30)
      (is (or (= [{:type ::meter/list-stream
                   :label "TestStore"
                   :value 2}]
                 @events)
              (= [{:type ::meter/list-stream
                   :label "TestStore"
                   :value 1}
                  {:type ::meter/list-stream
                   :label "TestStore"
                   :value 1}]
                 @events)))
      (reset! events [])
      @(s/put! stream :x)
      (s/close! stream)
      (is (= [{:type ::meter/list-stream
               :label "TestStore"
               :value 1}]
             @events)))))


(deftest measure-method
  (testing "constructor"
    (let [d (d/deferred)]
      (is (identical? d (meter/measure-method {} :foo nil d))
          "disabled store should return deferred unaltered")))
  (testing "elapsed time"
    (let [store (recording-store)
          events (::events store)
          d (d/deferred)
          d' (meter/measure-method store :foo nil d)]
      (is (not (identical? d d')))
      (is (empty? @events))
      (Thread/sleep 3)
      (d/success! d true)
      (is (= true @d'))
      (is (= 1 (count @events)))
      (is (= ::meter/method-time (:type (first @events))))
      (is (<= 3.0 (:value (first @events)))))))


(deftest miscellaney
  (testing "store labeling"
    (is (= "TestStore" (#'meter/meter-label (->TestStore))))
    (is (= "FooStore" (#'meter/meter-label (map->TestStore
                                             {::meter/label "FooStore"})))))
  (testing "bad recorder"
    (let [store (map->TestStore
                  {::meter/recorder
                   (fn [_ _] (throw (quiet-exception)))})]
      (is (nil? (#'meter/record! store :boom! 1 nil))))))
