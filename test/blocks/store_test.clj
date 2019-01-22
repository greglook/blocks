(ns blocks.store-test
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [clojure.test :refer :all]
    [multiformats.hash :as multihash]))


(deftest block-preference
  (is (nil? (store/preferred-block nil))
      "returns nil with no block arguments")
  (let [loaded (block/read! "foo")
        lazy-a (block/from-file "project.clj")
        lazy-b (block/from-file "README.md")]
    (is (= loaded (store/preferred-block lazy-a loaded lazy-b))
        "returns loaded block if present")
    (is (= lazy-a (store/preferred-block lazy-a lazy-b))
        "returns first block if all lazy")))


#_
(deftest stat-selection
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f200000000")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e888843d7f9")
        c (multihash/create :sha1 "73fe285cedef654fccc4a4d818db4cc225932878")
        d (multihash/create :sha1 "acbd18db4cc2f856211de9ecedef654fccc4a4d8")
        e (multihash/create :sha1 "c3c23db5285662ef717963ff4ce2373df0003206")
        f (multihash/create :sha2-256 "285c3c23d662b5ef7172373df0963ff4ce003206")
        ids [a b c d e f]
        stats (map #(hash-map :id % :size 1) ids)]
    (are [result opts] (= result (map :id (store/select-stats opts stats)))
         ids       {}
         [f]       {:algorithm :sha2-256}
         [c d e f] {:after "111473fd2"}
         [a b c]   {:limit 3})))


#_
(deftest stat-list-merging
  (let [list-a (list {:id "aaa", :foo :bar}
                     {:id "abb", :baz :qux}
                     {:id "abc", :key :val})
        list-b (list {:id "aab", :xyz 123}
                     {:id "abc", :ack :bar})
        list-c (list {:id "aaa", :foo 123}
                     {:id "xyz", :wqr :axo})]
    (is (= [{:id "aaa", :foo :bar}
            {:id "aab", :xyz 123}
            {:id "abb", :baz :qux}
            {:id "abc", :key :val}
            {:id "xyz", :wqr :axo}]
           (store/merge-block-lists
             list-a list-b list-c)))))


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
