(ns blocks.store.util-test
  (:require
    [blocks.store.util :as util]
    [clojure.test :refer :all]
    [multihash.core :as multihash]))


(deftest check-macro
  (testing "check with true predicate"
    (let [effects (atom [])]
      (is (= :foo (util/check :foo some?
                    (swap! effects conj [:> value])))
          "should return value")
      (is (empty? @effects) "should not cause side effects")))
  (testing "check with false predicate"
    (let [effects (atom [])]
      (is (nil? (util/check :foo (constantly false)
                  (swap! effects conj [:> value])))
          "should return nil")
      (is (= [[:> :foo]] @effects) "should cause side effects"))))


(deftest hex-validation
  (is (false? (util/hex? nil)) "nil is not valid hex")
  (is (false? (util/hex? "")) "empty string is not valid hex")
  (is (false? (util/hex? "012")) "odd length string is not hex")
  (is (false? (util/hex? "12xx")) "invalid characters are not hex")
  (is (true?  (util/hex? "00")) "single zero byte is valid hex")
  (is (true?  (util/hex? "fedcba9876543210")) "full hex alphabet is valid"))


(deftest random-generators
  (testing "random-bytes"
    (dotimes [i 20]
      (is (= (class (byte-array 0)) (class (util/random-bytes 10)))
          "should return a byte array")
      (is (pos? (count (util/random-bytes 10)))
          "should return non-empty arrays")
      (is (>= 10 (count (util/random-bytes 10)))
          "should return arrays at most a certain length")))
  (testing "random-hex"
    (dotimes [i 20]
      (is (util/hex? (util/random-hex 10))
          "should return valid hex"))))


(deftest stat-selection
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f200000000")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e888843d7f9")
        c (multihash/create :sha1 "73fe285cedef654fccc4a4d818db4cc225932878")
        d (multihash/create :sha1 "acbd18db4cc2f856211de9ecedef654fccc4a4d8")
        e (multihash/create :sha1 "c3c23db5285662ef717963ff4ce2373df0003206")
        f (multihash/create :sha2-256 "285c3c23d662b5ef7172373df0963ff4ce003206")
        ids [a b c d e f]
        stats (map #(hash-map :id % :size 1) ids)]
    (are [result opts] (= result (map :id (util/select-stats opts stats)))
         ids        {}
         [f]        {:algorithm :sha2-256}
         [c d e f]  {:after "111473fd2"}
         [a b c]    {:limit 3})))


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
           (util/merge-block-lists
             list-a list-b list-c)))))
