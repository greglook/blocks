(ns blocks.util-test
  (:require
    [blocks.util :as util]
    [clojure.test :refer :all]))


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
