(ns blocks.store-test
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [blocks.test-utils :refer [quiet-exception]]
    [clojure.test :refer :all]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.hash :as multihash]))


(deftest uri-parsing
  (is (= {:scheme "mem", :name "-"} (store/parse-uri "mem:-")))
  (is (= {:scheme "file", :path "/foo/bar"} (store/parse-uri "file:///foo/bar")))
  (is (= {:scheme "file", :host "foo" :path "/bar"} (store/parse-uri "file://foo/bar")))
  (is (= {:scheme "https"
          :user-info {:id "user"
                      :secret "password"}
          :host "example.com"
          :port 443
          :path "/path/to/thing"
          :query {:foo "alpha"
                  :bar "123"}}
         (store/parse-uri "https://user:password@example.com:443/path/to/thing?foo=alpha&bar=123"))))


(deftest block-preference
  (is (nil? (store/preferred-block nil))
      "returns nil with no block arguments")
  (let [loaded (data/read-block :sha1 "foo")
        lazy-a (data/create-block
                 (multihash/sha1 "foo") 3
                 #(java.io.ByteArrayInputStream. (.getBytes "foo")))
        lazy-b (data/create-block
                 (multihash/sha1 "bar") 3
                 #(java.io.ByteArrayInputStream. (.getBytes "bar")))]
    (is (= loaded (store/preferred-block lazy-a loaded lazy-b))
        "returns loaded block if present")
    (is (= lazy-a (store/preferred-block lazy-a lazy-b))
        "returns first block if all lazy")))


(deftest block-selection
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f200000000")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e888843d7f9")
        c (multihash/create :sha1 "73fe285cedef654fccc4a4d818db4cc225932878")
        d (multihash/create :sha1 "acbd18db4cc2f856211de9ecedef654fccc4a4d8")
        e (multihash/create :sha1 "c3c23db5285662ef717963ff4ce2373df0003206")
        f (multihash/create :sha2-256 "285c3c23d662b5ef7172373df0963ff4ce003206")
        ids [a b c d e f]
        blocks (mapv #(hash-map :id % :size 1) ids)
        filtered-stream #(->> (s/->source %1)
                              (store/select-blocks %2)
                              (s/stream->seq)
                              (into []))]
    (is (= ids (map :id (filtered-stream blocks {}))))
    (testing "exception"
      (let [boom (quiet-exception)]
        (is (= [{:id a, :size 1} boom]
               (filtered-stream [{:id a, :size 1} boom {:id b, :size 1}] {}))
            "should halt stream")))
    (testing "by algorithm"
      (is (= [f] (map :id (filtered-stream blocks {:algorithm :sha2-256})))))
    (testing "rank markers"
      (is (= [c d e f] (map :id (filtered-stream blocks {:after "111473fd2"}))))
      (is (= [a b] (map :id (filtered-stream blocks {:before "111473fd2"}))))
      (is (= [b c d] (map :id (filtered-stream blocks {:after "11147", :before "1114b"})))))
    (testing "limit"
      (is (= [a b c d e f] (map :id (filtered-stream blocks {:limit 8}))))
      (is (= [a b c d e f] (map :id (filtered-stream blocks {:limit 6}))))
      (is (= [a b c] (map :id (filtered-stream blocks {:limit 3}))))
      (is (= [c d] (map :id (filtered-stream blocks {:after "111473fd", :limit 2})))))))


(deftest list-merging
  (let [list-a [{:id "aaa"}
                {:id "abb"}
                {:id "abc"}]
        list-b [{:id "aab"}
                {:id "abc"}]
        list-c [{:id "aaa"}
                {:id "xyz"}]
        try-merge (fn try-merge
                    [& lists]
                    (->> (map s/->source lists)
                         (apply store/merge-blocks)
                         (s/stream->seq)
                         (into [])))]
    (testing "single stream"
      (is (= list-a (try-merge list-a))))
    (testing "full merge"
      (is (= [{:id "aaa"}
              {:id "aab"}
              {:id "abb"}
              {:id "abc"}
              {:id "xyz"}]
             (try-merge list-a list-b list-c))))
    (testing "stream error"
      (let [boom (quiet-exception)]
        (is (= [{:id "aaa"}
                {:id "aab"}
                boom]
               (try-merge list-a [{:id "aab"} boom] list-c)))))
    (testing "preemptive close"
      (let [merged (store/merge-blocks
                     (s/->source list-b)
                     (s/->source list-c))]
        (is (= {:id "aaa"} @(s/try-take! merged ::drained 1000 ::timeout)))
        (is (= {:id "aab"} @(s/try-take! merged ::drained 1000 ::timeout)))
        (s/close! merged)
        (is (= {:id "abc"} @(s/try-take! merged ::drained 1000 ::timeout)))
        (is (identical? ::drained @(s/try-take! merged ::drained 1000 ::timeout)))))))


(deftest missing-block-detection
  (letfn [(find-missing
            [a b]
            (->> (store/missing-blocks
                   (s/->source a)
                   (s/->source b))
                 (s/stream->seq)
                 (into [])))]
    (testing "basic operation"
      (is (= [] (find-missing [] [])))
      (is (= [] (find-missing [] [{:id "abc"} {:id "cde"}])))
      (is (= [{:id "abc"} {:id "cde"}]
             (find-missing [{:id "abc"} {:id "cde"}] [])))
      (is (= [{:id "abc"} {:id "cde"}]
             (find-missing [{:id "abc"} {:id "cde"} {:id "def"}]
                           [{:id "bcd"} {:id "cab"} {:id "def"}]))))
    (testing "exceptions"
      (let [boom (quiet-exception)]
        (is (= [{:id "abc"} boom]
               (find-missing [{:id "abc"} {:id "cde"}]
                             [{:id "bad"} boom])))
        (is (= [{:id "cde"} boom]
               (find-missing [{:id "abc"} {:id "cde"} boom]
                             [{:id "abc"}])))
        (is (= [boom]
               (find-missing [{:id "abc"} boom]
                             [{:id "abc"} {:id "def"} {:id "efg"}])))
        (is (= [boom]
               (find-missing [{:id "abc"} {:id "cde"}]
                             [boom])))))))


(deftest store-utilities
  (testing "zip-stores"
    (is (= [:a :b :c]
           @(store/zip-stores
              [{:result (d/success-deferred :a)}
               {:result (d/success-deferred :b)}
               {:result (d/success-deferred :c)}]
              :result)))
    (is (thrown? RuntimeException
          @(store/zip-stores
             [{:result (d/success-deferred :a)}
              {:result (d/error-deferred (quiet-exception))}
              {:result (d/success-deferred :c)}]
             :result))))
  (testing "some-store"
    (testing "edge cases"
      (is (nil? @(store/some-store [] :result)))
      (is (= :a
             @(store/some-store
                [{:result (d/success-deferred :a)}]
                :result))))
    (testing "fallback behavior"
      (is (= :a @(store/some-store
                   [{:result (d/success-deferred :a)}
                    {:result (d/success-deferred :b)}]
                   :result)))
      (is (= :b @(store/some-store
                   [{:result (d/success-deferred nil)}
                    {:result (d/success-deferred :b)}]
                   :result)))
      (is (nil? @(store/some-store
                   [{:result (d/success-deferred nil)}
                    {:result (d/success-deferred nil)}]
                   :result))))
    (testing "errors"
      (is (= :a @(store/some-store
                   [{:result (d/success-deferred :a)}
                    {:result (d/error-deferred (quiet-exception))}]
                   :result)))
      (is (thrown? RuntimeException
            @(store/some-store
               [{:result (d/success-deferred nil)}
                {:result (d/error-deferred (quiet-exception))}]
               :result))))))
