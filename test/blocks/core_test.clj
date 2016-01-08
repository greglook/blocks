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
  (testing "ranged open validation"
    (let [block (block/read! "abcdefg")]
      (is (thrown? IllegalArgumentException (block/open block nil 4)))
      (is (thrown? IllegalArgumentException (block/open block -1 4)))
      (is (thrown? IllegalArgumentException (block/open block 0 nil)))
      (is (thrown? IllegalArgumentException (block/open block 0 -1)))
      (is (thrown? IllegalArgumentException (block/open block 3 1)))
      (is (thrown? IllegalArgumentException (block/open block 0 10)))))
  (testing "empty block"
    (let [block (empty (block/read! "abc"))]
      (is (thrown? IOException (block/open block))
          "full open should throw exception")
      (is (thrown? IOException (block/open block 0 3))
          "ranged open should throw exception")))
  (testing "literal block"
    (let [block (block/read! "the old dog jumped")]
      (is (= "the old dog jumped" (slurp (block/open block))))
      (is (= "old dog" (slurp (block/open block 4 11))))))
  (testing "lazy block"
    (let [block (block/from-file "README.md")
          readme (slurp (block/open block))]
      (is (not (realized? block)) "file blocks should be lazy")
      (is (string? readme))
      (is (= (subs readme 10 20) (slurp (block/open block 10 20)))))))


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
    (is (bytes= @literal-readme
                (with-open [content (block/open lazy-readme)]
                  (bytes/to-byte-array content)))
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


(deftest batch-operations
  (let [a (block/read! "foo")
        b (block/read! "bar")
        c (block/read! "baz")
        test-blocks {(:id a) a
                     (:id b) b
                     (:id c) c}]
    (testing "get-batch"
      (testing "validation"
        (is (thrown? IllegalArgumentException
                     (block/get-batch nil :foo))
            "with non-collection throws error")
        (is (thrown? IllegalArgumentException
                     (block/get-batch nil [(multihash/sha1 "foo") :foo]))
            "with non-multihash entry throws error"))
      (let [store (reify
                    block/BlockStore
                    (-get
                      [_ id]
                      [:get id])
                    block/BatchingStore
                    (-get-batch
                      [_ ids]
                      [:batch ids]))
            ids [(:id a) (:id b) (:id c)]]
        (is (= [:batch ids] (block/get-batch store ids))
            "should use optimized method where available"))
      (let [store (reify
                    block/BlockStore
                    (-get
                      [_ id]
                      (get test-blocks id)))
            ids [(:id a) (:id b) (:id c) (multihash/sha1 "frobble")]]
        (is (= [a b c] (block/get-batch store ids))
            "should fall back to normal get method")))
    (testing "put-batch!"
      (testing "validation"
        (is (thrown? IllegalArgumentException
                     (block/put-batch! nil :foo))
            "with non-collection throws error")
        (is (thrown? IllegalArgumentException
                     (block/put-batch! nil [(block/read! "foo") :foo]))
            "with non-block entry throws error"))
      (let [store (reify
                    block/BlockStore
                    (put!
                      [_ block]
                      [:put block])
                    block/BatchingStore
                    (-put-batch!
                      [_ blocks]
                      [:batch blocks]))]
        (is (= [:batch [a b c]]
               (block/put-batch! store [a b c]))
            "should use optimized method where available"))
      (let [store (reify
                    block/BlockStore
                    (put!
                      [_ block]
                      [:put block]))]
        (is (= [[:put a] [:put b] [:put c]]
               (block/put-batch! store [a b c]))
            "should fall back to normal put method")))
    (testing "delete-batch!"
      (testing "validation"
        (is (thrown? IllegalArgumentException
                     (block/delete-batch! nil :foo))
            "with non-collection throws error")
        (is (thrown? IllegalArgumentException
                     (block/delete-batch! nil [(multihash/sha1 "foo") :foo]))
            "with non-multihash entry throws error"))
      (let [store (reify
                    block/BlockStore
                    (delete!
                      [_ id]
                      [:delete id])
                    block/BatchingStore
                    (-delete-batch!
                      [_ ids]
                      [:batch ids]))]
        (is (= [:batch (map :id [a b c])]
               (block/delete-batch! store (map :id [a b c])))
            "should use optimized method where available"))
      (let [store (reify
                    block/BlockStore
                    (delete!
                      [_ id]
                      (contains? test-blocks id)))]
        (is (= [(:id a) (:id b)]
               (block/delete-batch! store [(:id a) (multihash/sha1 "qux") (:id b)]))
            "should fall back to normal delete method")))))



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
