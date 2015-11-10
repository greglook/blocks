(ns blocks.core-test
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    java.io.ByteArrayOutputStream
    java.io.IOException
    java.io.InputStream))


;; ## IO Tests

(deftest block-input-stream
  (testing "empty block"
    (let [block (empty (block/read! "abc"))]
      (is (thrown? IOException (block/open block)))))
  (testing "literal block"
    (let [block (block/read! "the old dog jumped")
          stream (block/open block)]
      (is (instance? InputStream stream))
      (is (= "the old dog jumped" (slurp stream)))))
  (testing "lazy block"
    (let [block (block/from-file "README.md")]
      (is (not (realized? block)) "file blocks should be lazy")
      (is (string? (slurp (block/open block)))))))


(deftest block-reading
  (testing "block construction"
    (is (empty? @(block/read! (byte-array 0)))
        "empty content reads into empty content")))


(deftest block-writing
  (let [block (block/read! "frobblenitz")
        baos (ByteArrayOutputStream.)]
    (block/write! block baos)
    (is (bytes= "frobblenitz" (.toByteArray baos)))))


(deftest block-loading
  (let [lazy-readme (block/from-file "README.md")
        literal-readme (block/load! lazy-readme)]
    (is (realized? literal-readme)
        "load returns literal block for lazy block")
    (is (identical? literal-readme (block/load! literal-readme))
        "load returns literal block unchanged")
    (is (bytes= @literal-readme (block/open lazy-readme))
        "literal block content should match lazy block")))


(deftest block-validation
  (let [base (block/read! "foo bar baz")
        fix (fn [b k v]
              (Block. (if (= k :id)      v (:id b))
                      (if (= k :size)    v (:size b))
                      (if (= k :content) v (.content b))
                      (if (= k :reader)  v (.reader b))
                      nil nil))]
    (testing "non-multihash id"
      (is (thrown? IllegalStateException
                   (block/validate! (fix base :id "foo")))))
    (testing "negative size"
      (is (thrown? IllegalStateException
                   (block/validate! (fix base :size -1)))))
    (testing "invalid size"
      (is (thrown? IllegalStateException
                   (block/validate! (fix base :size 123)))))
    (testing "incorrect identifier"
      (is (thrown? IllegalStateException
                   (block/validate! (fix base :id (multihash/sha1 "qux"))))))
    (testing "empty block"
      (is (thrown? IOException
                   (block/validate! (empty base)))))
    (testing "valid block"
      (is (nil? (block/validate! base))))))



;; ## Storage Tests

(deftest list-wrapper
  (let [store (reify block/BlockStore (-list [_ opts] opts))]
    (testing "opts-map conversion"
      (is (nil? (block/list store))
          "no arguments should return nil options map")
      (is (= {:limit 20} (block/list store {:limit 20}))
          "single map argument should pass map")
      (is (= {:limit 20} (block/list store :limit 20))
          "multiple args should convert into hash map"))
    (testing "option validation"
      (is (thrown-with-msg?
            IllegalArgumentException #":foo"
            (block/list store :foo "bar")))
      (is (thrown-with-msg?
            IllegalArgumentException #":algorithm .+ keyword.+ \"foo\""
            (block/list store :algorithm "foo")))
      (is (thrown-with-msg?
            IllegalArgumentException #":after .+ hex string.+ 123"
            (block/list store :after 123)))
      (is (thrown-with-msg?
            IllegalArgumentException #":after .+ hex string.+ \"123abx\""
            (block/list store :after "123abx")))
      (is (thrown-with-msg?
            IllegalArgumentException #":limit .+ positive integer.+ :xyz"
            (block/list store :limit :xyz)))
      (is (thrown-with-msg?
            IllegalArgumentException #":limit .+ positive integer.+ 0"
            (block/list store :limit 0)))
      (is (= {:algorithm :sha1, :after "012abc", :limit 10}
             (block/list store :algorithm :sha1, :after "012abc", :limit 10))))))


(deftest get-wrapper
  (testing "non-multihash id"
    (is (thrown? IllegalArgumentException (block/get {} "foo"))))
  (testing "no block result"
    (let [store (reify block/BlockStore (-get [_ id] nil))]
      (is (nil? (block/get store (multihash/sha1 "foo bar"))))))
  (testing "invalid block result"
    (let [store (reify block/BlockStore (-get [_ id] (block/read! "foo")))
          other-id (multihash/sha1 "baz")]
      (is (thrown? RuntimeException (block/get store other-id)))))
  (testing "valid block result"
    (let [block (block/read! "foo")
          store (reify block/BlockStore (-get [_ id] block))]
      (is (= block (block/get store (:id block)))))))


(deftest store-wrapper
  (let [store (reify block/BlockStore (put! [_ block] block))]
    (testing "file source"
      (let [block (block/store! store (io/file "README.md"))]
        (is (not (realized? block))
            "should create lazy block from file")))
    (testing "other source"
      (let [block (block/store! store "foo bar baz")]
        (is (realized? block)
            "should be read into memory")))))



;; ## Utility Tests

(deftest stat-metadata
  (let [block {:id "foo"}
        block' (block/with-stats block {:stored-at 123})]
    (testing "with-stats"
      (is (= block block') "shoudn't affect equality")
      (is (not (empty? (meta block'))) "should add metadata"))
    (testing "meta-stats"
      (is (= {:stored-at 123} (block/meta-stats block'))
          "should return the stored stats"))))


(deftest stat-selection
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f200000000")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e888843d7f9")
        c (multihash/create :sha1 "73fe285cedef654fccc4a4d818db4cc225932878")
        d (multihash/create :sha1 "acbd18db4cc2f856211de9ecedef654fccc4a4d8")
        e (multihash/create :sha1 "c3c23db5285662ef717963ff4ce2373df0003206")
        f (multihash/create :sha2-256 "285c3c23d662b5ef7172373df0963ff4ce003206")
        ids [a b c d e f]
        stats (map #(hash-map :id % :size 1) ids)]
    (are [result opts] (= result (map :id (block/select-stats opts stats)))
         ids        {}
         [f]        {:algorithm :sha2-256}
         [c d e f]  {:after "111473fd2"}
         [a b c]    {:limit 3})))
