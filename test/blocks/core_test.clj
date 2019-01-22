(ns blocks.core-test
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store]
    [blocks.store.memory :refer [memory-block-store]]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [multiformats.hash :as multihash])
  (:import
    blocks.data.Block
    (java.io
      ByteArrayOutputStream
      InputStream
      IOException)))


;; ## IO Tests

#_
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
  (testing "loaded block"
    (let [block (block/read! "the old dog jumped")]
      (is (= "the old dog jumped" (slurp (block/open block))))
      (is (= "old dog" (slurp (block/open block 4 11))))))
  (testing "lazy block"
    (let [block (block/from-file "README.md")
          readme (slurp (block/open block))]
      (is (true? (block/lazy? block)) "file blocks should be lazy")
      (is (string? readme))
      (is (= (subs readme 10 20) (slurp (block/open block 10 20)))))))


#_
(deftest block-reading
  (testing "block construction"
    (is (nil? (block/read! (byte-array 0)))
        "empty content reads into nil block")))


#_
(deftest block-writing
  (let [block (block/read! "frobblenitz")
        baos (ByteArrayOutputStream.)]
    (block/write! block baos)
    (is (bytes= "frobblenitz" (.toByteArray baos)))))


#_
(deftest block-loading
  (let [lazy-readme (block/from-file "README.md")
        loaded-readme (block/load! lazy-readme)]
    (is (not (block/lazy? loaded-readme))
        "load returns loaded block for lazy block")
    (is (identical? loaded-readme (block/load! loaded-readme))
        "load returns loaded block unchanged")
    (is (bytes= (block/open loaded-readme)
                (block/open lazy-readme))
        "loaded block content should match lazy block")))


#_
(deftest block-validation
  (let [base (block/read! "foo bar baz")
        fix (fn [b k v]
              (Block. (if (= k :id)      v (:id b))
                      (if (= k :size)    v (:size b))
                      (if (= k :content) v (.content b))
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

#_
(deftest list-wrapper
  (let [store (reify store/BlockStore (-list [_ opts] opts))]
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


#_
(deftest stat-wrapper
  (testing "non-multihash id"
    (is (thrown? IllegalArgumentException (block/stat {} "foo")))))


#_
(deftest get-wrapper
  (testing "non-multihash id"
    (is (thrown? IllegalArgumentException (block/get {} "foo"))))
  (testing "no block result"
    (let [store (reify store/BlockStore (-get [_ id] nil))]
      (is (nil? (block/get store (multihash/sha1 "foo bar"))))))
  (testing "invalid block result"
    (let [store (reify store/BlockStore (-get [_ id] (block/read! "foo")))
          other-id (multihash/sha1 "baz")]
      (is (thrown? RuntimeException (block/get store other-id)))))
  (testing "valid block result"
    (let [block (block/read! "foo")
          store (reify store/BlockStore (-get [_ id] block))]
      (is (= block (block/get store (:id block)))))))


#_
(deftest put-wrapper
  (let [store (reify store/BlockStore
                (-put! [_ block] (with-meta block nil)))]
    (testing "with non-block arg"
      (is (thrown? IllegalArgumentException
            (block/put! store :foo))))
    (testing "block attributes"
      (let [original (-> (block/read! "a block with some extras")
                         (assoc :foo "bar")
                         (vary-meta assoc ::thing :baz))
            stored (block/put! store original)]
        (is (= (:id original) (:id stored))
            "Stored block id should match original")
        (is (= (:size original) (:size stored))
            "Stored block size should match original")
        (is (= "bar" (:foo stored))
            "Stored block should retain extra attributes")
        (is (= :baz (::thing (meta stored)))
            "Stored block should retain extra metadata")
        (is (= original stored)
            "Stored block should test equal to original")))))


#_
(deftest store-wrapper
  (let [store (reify store/BlockStore (-put! [_ block] block))]
    (testing "file source"
      (let [block (block/store! store (io/file "README.md"))]
        (is (block/lazy? block)
            "should create lazy block from file")))
    (testing "other source"
      (let [block (block/store! store "foo bar baz")]
        (is (not (block/lazy? block))
            "should be read into memory")))))


#_
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
                    store/BlockStore
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
                    store/BlockStore
                    (-put!
                      [_ block]
                      (assoc block :put? true)))]
        (is (every? :put? (block/put-batch! store [a b c]))
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
                    store/BlockStore
                    (-delete!
                      [_ id]
                      (contains? test-blocks id)))]
        (is (= #{(:id a) (:id b)}
               (block/delete-batch! store [(:id a) (multihash/sha1 "qux") (:id b)]))
            "should fall back to normal delete method")))))



;; ## Utility Tests

(deftest store-construction
  (is (satisfies? store/BlockStore (block/->store "mem:-")))
  (is (thrown? Exception (block/->store "foo://x?z=1"))))


#_
(deftest block-syncing
  (let [block-a (block/read! "789")  ; 35a9
        block-b (block/read! "123")  ; a665
        block-c (block/read! "456")  ; b3a8
        block-d (block/read! "ABC")] ; b5d4
    (testing "empty dest"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-b)
                     (block/put! block-c))
            dest (memory-block-store)]
        (is (= 3 (count (block/list source))))
        (is (empty? (block/list dest)))
        (let [sync-summary (block/sync! source dest)]
          (is (= 3 (:count sync-summary)))
          (is (= 9 (:size sync-summary))))
        (is (= 3 (count (block/list source))))
        (is (= 3 (count (block/list dest))))))
    (testing "subset source"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-c))
            dest (doto (memory-block-store)
                   (block/put! block-a)
                   (block/put! block-b)
                   (block/put! block-c))
            summary (block/sync! source dest)]
        (is (zero? (:count summary)))
        (is (zero? (:size summary)))
        (is (= 2 (count (block/list source))))
        (is (= 3 (count (block/list dest))))))
    (testing "mixed blocks"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-c))
            dest (doto (memory-block-store)
                   (block/put! block-b)
                   (block/put! block-d))
            summary (block/sync! source dest)]
        (is (= 2 (:count summary)))
        (is (= 6 (:size summary)))
        (is (= 2 (count (block/list source))))
        (is (= 4 (count (block/list dest))))))
    (testing "filter logic"
      (let [source (doto (memory-block-store)
                     (block/put! block-a)
                     (block/put! block-c))
            dest (doto (memory-block-store)
                   (block/put! block-b)
                   (block/put! block-d))
            summary (block/sync! source dest :filter (comp #{(:id block-c)} :id))]
        (is (= 1 (:count summary)))
        (is (= 3 (:size summary)))
        (is (= 2 (count (block/list source))))
        (is (= 3 (count (block/list dest))))))))
