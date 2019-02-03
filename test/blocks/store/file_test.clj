(ns blocks.store.file-test
  (:require
    [blocks.core :as block]
    [blocks.store.file :as file :refer [file-block-store]]
    [blocks.store.tests :as tests]
    [blocks.test-utils :refer [quiet-exception]]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]
    [multiformats.hash :as multihash]))


(defn- mk-tmpdir!
  []
  (io/file "target" "test" "tmp"
           (str "file-block-store." (System/currentTimeMillis))))


(deftest layout-init
  (testing "lifecycle"
    (let [store (file-block-store (mk-tmpdir!))]
      (is (identical? store (component/stop store)))))
  (testing "unknown version"
    (let [root (mk-tmpdir!)
          store (file-block-store root)
          meta-file (io/file root "meta.properties")]
      (io/make-parents meta-file)
      (spit meta-file "version=v2\n")
      (is (thrown-with-msg? Exception #"storage layout version \"v2\" does not match supported version \"v1\""
            (component/start store))))))


(deftest v0-layout
  (let [root (mk-tmpdir!)
        a (block/read! "one")
        b (block/read! "two")
        c (block/read! "three")]
    (doseq [block [a b c]]
      (let [hex (multihash/hex (:id block))
            head (subs hex 0 8)
            tail (subs hex 8)
            file (io/file root head tail)]
        (io/make-parents file)
        (block/write! block file)))
    (testing "with extra files"
      (let [store (file-block-store root)
            extra (io/file root "something.unknown")]
        (try
          (spit extra "What is this file for?\n")
          (is (thrown-with-msg? Exception #"unknown files in block store"
                (component/start store)))
          (finally
            (.delete extra)))))
    (testing "without migration"
      (let [store (file-block-store root)]
        (is (thrown-with-msg? Exception #"v0 file block store layout"
              (component/start store)))))
    (testing "with migration"
      (let [store (file-block-store root :auto-migrate? true)
            store (component/start store)]
        (is (= "v1" (:version store)))
        (is (= a @(block/get store (:id a))))
        (is (= b @(block/get store (:id b))))
        (is (= c @(block/get store (:id c))))))))


(deftest file-listing
  (let [store (file-block-store (mk-tmpdir!))
        a (block/read! "larry")
        b (block/read! "curly")
        c (block/read! "moe")]
    @(block/put-batch! store [a b c])
    (testing "filtering"
      (is (= 3 (count (block/list-seq store :after "12200c"))))
      (is (= 3 (count (block/list-seq store :after "12200d0980"))))
      (is (= 1 (count (block/list-seq store :after "12204b6f51"))))
      (is (= 1 (count (block/list-seq store :after "12204b" :before "12204b7"))))
      (is (= (:id b) (:id (first (block/list-seq store :after "12204b6f51")))))
      (is (empty? (block/list-seq store :after "122064"))))
    (testing "rogue content"
      (let [extra (io/file (:root store) "blocks" "12200d09" "wat")]
        (spit extra "what is this")
        (is (= 3 (count (block/list-seq store))))
        (.delete extra)))
    (testing "exception"
      (let [ex (quiet-exception)]
        (with-redefs [blocks.store.file/file->block (fn [_ _] (throw ex))]
          (is (= [ex] (manifold.stream/stream->seq (block/list store))))
          (is (thrown? Exception
                (doall (block/list-seq store)))))))))


(deftest ^:integration check-behavior
  (let [tmpdir (mk-tmpdir!)]
    (tests/check-store
      #(let [store (file-block-store tmpdir)]
         @(block/erase! store)
         store))))
