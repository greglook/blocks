(ns blocks.core-test
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store]
    [blocks.test-utils :refer [quiet-exception]]
    [byte-streams :refer [bytes=]]
    [clojure.java.io :as io]
    [clojure.test :refer [deftest testing is]]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.hash :as multihash])
  (:import
    blocks.data.Block
    (java.io
      ByteArrayOutputStream
      File)))


;; ## IO Tests

(deftest block-io
  (testing "from-file"
    (is (thrown? IllegalStateException
          (dosync (block/from-file "README.md"))))
    (is (nil? (block/from-file "not/a-real/file.txt")))
    (let [tmp-dir (doto (io/file "target" "test" "tmp")
                    (.mkdirs))
          tmp (doto (File/createTempFile "input" ".tmp" tmp-dir)
                (.deleteOnExit))]
      (is (nil? (block/from-file tmp))
          "empty file should return nil"))
    (let [block (block/from-file "README.md")]
      (is (pos? (:size block)))
      (is (block/lazy? block))))
  (testing "reading"
    (is (thrown? IllegalStateException
          (dosync (block/read! "foo bar baz"))))
    (is (nil? (block/read! (byte-array 0)))
        "empty content reads into nil block"))
  (testing "writing"
    (let [block (block/read! "frobblenitz")
          baos (ByteArrayOutputStream.)]
      (is (thrown? IllegalStateException
            (dosync (block/write! block baos))))
      (block/write! block baos)
      (is (bytes= "frobblenitz" (.toByteArray baos)))))
  (testing "loading"
    (let [lazy-readme (block/from-file "README.md")
          loaded-readme (block/load! lazy-readme)]
      (is (thrown? IllegalStateException
            (dosync (block/load! lazy-readme))))
      (is (block/loaded? loaded-readme)
          "load returns loaded block for lazy block")
      (is (identical? loaded-readme (block/load! loaded-readme))
          "load returns loaded block unchanged")
      (is (bytes= (block/open loaded-readme)
                  (block/open lazy-readme))
          "loaded block content should match lazy block"))))


(deftest block-opening
  (testing "ranged open validation"
    (let [block (block/read! "abcdefg")]
      (is (thrown? IllegalArgumentException (block/open block {:start -1, :end 4})))
      (is (thrown? IllegalArgumentException (block/open block {:start 0, :end -1})))
      (is (thrown? IllegalArgumentException (block/open block {:start 3, :end 1})))
      (is (thrown? IllegalArgumentException (block/open block {:start 0, :end 10})))))
  (testing "loaded block"
    (let [block (block/read! "the old dog jumped")]
      (is (= "the old dog jumped" (slurp (block/open block))))
      (is (= "the old" (slurp (block/open block {:end 7}))))
      (is (= "old dog" (slurp (block/open block {:start 4, :end 11}))))
      (is (= "jumped" (slurp (block/open block {:start 12}))))))
  (testing "lazy block"
    (let [block (block/from-file "README.md")
          readme (slurp (block/open block))]
      (is (true? (block/lazy? block)) "file blocks should be lazy")
      (is (string? readme))
      (is (= (subs readme 10 20) (slurp (block/open block {:start 10, :end 20})))))))


(deftest block-validation
  (let [base (block/read! "foo bar baz")
        fix (fn [b k v]
              (Block. (if (= k :id)      v (:id b))
                      (if (= k :size)    v (:size b))
                      (:stored-at b)
                      (if (= k :content) v (.content b))
                      nil))]
    (testing "non-multihash id"
      (is (thrown-with-msg? Exception #"id is not a multihash"
            (block/validate! (fix base :id "foo")))))
    (testing "negative size"
      (is (thrown-with-msg? Exception #"has an invalid size"
            (block/validate! (fix base :size -1)))))
    (testing "invalid size"
      (is (thrown-with-msg? Exception #"reports size 123 but has actual size 11"
            (block/validate! (fix base :size 123)))))
    (testing "incorrect identifier"
      (is (thrown-with-msg? Exception #"has mismatched id and content"
            (block/validate! (fix base :id (multihash/sha1 "qux"))))))
    (testing "valid block"
      (is (true? (block/validate! base))))))



;; ## Storage API

(deftest store-construction
  (is (satisfies? store/BlockStore (block/->store "mem:-")))
  (is (thrown? Exception (block/->store "foo://x?z=1"))))


(deftest list-wrapper
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f200000000")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e888843d7f9")
        c (multihash/create :sha1 "acbd18db4cc2f856211de9ecedef654fccc4a4d8")
        d (multihash/create :sha2-256 "285c3c23d662b5ef7172373df0963ff4ce003206")
        store (reify store/BlockStore

                (-list
                  [_ opts]
                  (s/->source [{:id a} {:id b} {:id c} {:id d}])))]
    (testing "io check"
      (is (thrown? IllegalStateException
            (dosync (block/list store)))))
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
            IllegalArgumentException #":before .+ hex string.+ 123"
            (block/list store :before 123)))
      (is (thrown-with-msg?
            IllegalArgumentException #":before .+ hex string.+ \"123abx\""
            (block/list store :before "123abx")))
      (is (thrown-with-msg?
            IllegalArgumentException #":limit .+ positive integer.+ :xyz"
            (block/list store :limit :xyz)))
      (is (thrown-with-msg?
            IllegalArgumentException #":limit .+ positive integer.+ 0"
            (block/list store :limit 0))))
    (testing "filtered behavior"
      (is (= [{:id b} {:id c}]
             (s/stream->seq
               (block/list store {:algorithm :sha1
                                  :after "111450"
                                  :before "1300"
                                  :limit 10}))))
      (is (= [{:id b}]
             (s/stream->seq
               (block/list store {:after a, :before c}))))
      (is (= [{:id c}]
             (s/stream->seq
               (block/list store {:after b, :limit 1})))))
    (testing "seq wrapper"
      (is (thrown-with-msg?
            IllegalArgumentException #":timeout is not a positive integer"
            (block/list-seq store :timeout 0)))
      (is (= [{:id a} {:id b} {:id c} {:id d}]
             (block/list-seq store)))
      (is (thrown? RuntimeException
            (doall
              (block/list-seq
                (reify store/BlockStore

                  (-list
                    [_ opts]
                    (s/->source [{:id a} (quiet-exception)]))))))
          "rethrows stream exceptions")
      (is (thrown-with-msg? Exception #"stream consumption timed out"
            (doall
              (block/list-seq
                (reify store/BlockStore

                  (-list
                    [_ opts]
                    (s/stream)))
                :timeout 10)))))))


(deftest stat-wrapper
  (testing "io check"
    (is (thrown? IllegalStateException
          (dosync (block/stat {} (multihash/sha1 "foo"))))))
  (testing "non-multihash id"
    (is (thrown? IllegalArgumentException
          (block/stat {} "foo"))))
  (testing "normal operation"
    (let [id (multihash/sha1 "foo")
          now (java.time.Instant/now)]
      (is (= {:id id, :size 123, :stored-at now}
             @(block/stat
                (reify store/BlockStore

                  (-stat
                    [_ id]
                    (d/success-deferred
                      {:id id, :size 123, :stored-at now})))
                id))))))


(deftest get-wrapper
  (testing "io check"
    (is (thrown? IllegalStateException
          (dosync (block/get {} (multihash/sha1 "foo"))))))
  (testing "non-multihash id"
    (is (thrown? IllegalArgumentException
          (block/get {} "foo"))))
  (testing "no block result"
    (let [store (reify store/BlockStore

                  (-get
                    [_ id]
                    (d/success-deferred nil)))]
      (is (nil? @(block/get store (multihash/sha1 "foo bar"))))))
  (testing "invalid block result"
    (let [store (reify store/BlockStore

                  (-get
                    [_ id]
                    (d/success-deferred (block/read! "foo"))))
          other-id (multihash/sha1 "baz")]
      (is (thrown? RuntimeException
            @(block/get store other-id)))))
  (testing "valid block result"
    (let [block (block/read! "foo")
          store (reify store/BlockStore

                  (-get
                    [_ id]
                    (d/success-deferred block)))]
      (is (= block @(block/get store (:id block)))))))


(deftest put-wrapper
  (let [original (block/read! "a block")
        store (reify store/BlockStore

                (-put!
                  [_ block]
                  (d/success-deferred block)))]
    (testing "io check"
      (is (thrown? IllegalStateException
            (dosync (block/put! store original)))))
    (testing "with non-block arg"
      (is (thrown? IllegalArgumentException
            (block/put! store :foo))))
    (testing "block handling"
      (let [stored @(block/put! store original)]
        (is (= original stored))))))


(deftest store-wrapper
  (let [store (reify store/BlockStore

                (-put!
                  [_ block]
                  (d/success-deferred block)))]
    (testing "io check"
      (is (thrown? IllegalStateException
            (dosync (block/store! store "foo")))))
    (testing "file source"
      (let [block @(block/store! store (io/file "README.md"))]
        (is (block/lazy? block)
            "should create lazy block from file")))
    (testing "other source"
      (let [block @(block/store! store "foo bar baz")]
        (is (block/loaded? block)
            "should be read into memory")))))


(deftest delete-wrapper
  (let [id (multihash/sha1 "foo")
        store (reify store/BlockStore

                (-delete!
                  [_ id']
                  (d/success-deferred (= id id'))))]
    (testing "io check"
      (is (thrown? IllegalStateException
            (dosync (block/delete! store id)))))
    (testing "non-multihash id"
      (is (thrown? IllegalArgumentException
            (block/delete! store "foo"))))
    (testing "normal operation"
      (is (true? @(block/delete! store id)))
      (is (false? @(block/delete! store (multihash/sha1 "bar")))))))


(deftest batch-operations
  (let [a (block/read! "foo")
        b (block/read! "bar")
        c (block/read! "baz")
        test-blocks {(:id a) a
                     (:id b) b
                     (:id c) c}
        store (reify store/BlockStore

                (-get
                  [_ id]
                  (d/success-deferred (get test-blocks id)))

                (-put!
                  [_ block]
                  (d/success-deferred block))

                (-delete!
                  [_ id]
                  (d/success-deferred (contains? test-blocks id))))]
    (testing "get-batch"
      (let [ids [(:id a) (:id b) (:id c) (multihash/sha1 "frobble")]]
        (is (= [a b c] @(block/get-batch store ids)))))
    (testing "put-batch!"
      (is (= [] @(block/put-batch! store [])))
      (is (= [a b] @(block/put-batch! store [a b]))))
    (testing "delete-batch!"
      (is (= #{} @(block/delete-batch! store [])))
      (is (= #{(:id a) (:id b)}
             @(block/delete-batch! store [(:id a) (multihash/sha1 "qux") (:id b)]))))))



;; ## Storage Utilities

(deftest store-scan
  (let [a (block/read! "foo")
        b (block/read! "baz")
        c (block/read! "bar")
        d (block/read! "abcdef")
        store (reify store/BlockStore

                (-list
                  [_ opts]
                  (s/->source [a b c d])))]
    (is (thrown? IllegalStateException
          (dosync (block/scan store))))
    (is (= {:count 4
            :size 15
            :sizes {2 3, 3 1}}
           @(block/scan store)))
    (is (= {:count 1
            :size 6
            :sizes {3 1}}
           @(block/scan store :filter #(< 3 (:size %)))))))


(deftest store-erasure
  (let [a (block/read! "foo")
        b (block/read! "baz")
        c (block/read! "bar")
        deleted (atom #{})
        store (reify store/BlockStore

                (-list
                  [_ opts]
                  (s/->source [a b c]))

                (-delete!
                  [_ id]
                  (swap! deleted conj id)
                  (d/success-deferred true)))]
    (is (thrown? IllegalStateException
          (dosync (block/erase! store))))
    (is (true? @(block/erase! store)))
    (is (= #{(:id a) (:id b) (:id c)} @deleted))))


(deftest block-syncing
  (let [a (block/read! "789")  ; 35a9
        b (block/read! "123")  ; a665
        c (block/read! "456")  ; b3a8
        d (block/read! "ABC")  ; b5d4
        source-store (fn [& blocks]
                       (reify store/BlockStore

                         (-list
                           [_ opts]
                           (s/->source blocks))))
        sink-store (fn [target & blocks]
                     (reify store/BlockStore

                       (-list
                         [_ opts]
                         (s/->source (vec blocks)))

                       (-put!
                         [_ block]
                         (swap! target conj block)
                         (d/success-deferred block))))]
    (testing "io check"
      (is (thrown? IllegalStateException
            (dosync (block/sync! {} {})))))
    (testing "empty dest"
      (let [transferred (atom #{})
            source (source-store a b c)
            dest (sink-store transferred)]
        (is (= 3 (count (block/list-seq source))))
        (is (empty? @transferred))
        (let [sync-summary @(block/sync! source dest)]
          (is (= 3 (:count sync-summary)))
          (is (= 9 (:size sync-summary))))
        (is (= 3 (count (block/list-seq source))))
        (is (= 3 (count @transferred)))))
    (testing "subset source"
      (let [transferred (atom #{})
            source (source-store a c)
            dest (sink-store transferred a b c)
            summary @(block/sync! source dest)]
        (is (zero? (:count summary)))
        (is (zero? (:size summary)))
        (is (= #{} @transferred))))
    (testing "mixed blocks"
      (let [transferred (atom #{})
            source (source-store a c)
            dest (sink-store transferred b d)
            summary @(block/sync! source dest)]
        (is (= 2 (:count summary)))
        (is (= 6 (:size summary)))
        (is (= #{a c} @transferred))))
    (testing "filter logic"
      (let [transferred (atom #{})
            source (source-store a c)
            dest (sink-store transferred b d)
            summary @(block/sync! source dest :filter #(= (:id c) (:id %)))]
        (is (= 1 (:count summary)))
        (is (= 3 (:size summary)))
        (is (= #{c} @transferred))))))
