(ns blocks.store.file-test
  (:require
    [blocks.core :as block]
    (blocks.store
      [file :as file :refer [file-block-store]]
      [tests :as tests])
    [clojure.java.io :as io]
    [clojure.test :refer :all]))


(defn- mk-tmpdir!
  []
  (io/file "target" "test" "tmp"
           (str "file-block-store." (System/currentTimeMillis))))


(deftest conversion-tests
  (testing "file->id"
    (let [file->id @#'file/file->id]
      (is (nil? (file->id nil nil))
          "should return nil for nil files")
      (is (nil? (file->id "foo/bar" (io/file "foo" "baz" "abc")))
          "should return nil for non-child files")
      (is (nil? (file->id "foo/bar" (io/file "foo" "bar" "1220" "ab" "0x" "123abc")))
          "should return nil for non-hex files"))))


(deftest file-listing
  (let [store (file-block-store (mk-tmpdir!))
        a (block/read! "larry")
        b (block/read! "curly")
        c (block/read! "moe")]
    (block/put! store a)
    (block/put! store b)
    (block/put! store c)
    (is (= 3 (count (block/list store :after "12200c"))))
    (is (= 3 (count (block/list store :after "12200d0980"))))
    (is (= 1 (count (block/list store :after "12204b6f51"))))
    (is (= (:id b) (:id (first (block/list store :after "12204b6f51")))))
    (is (empty? (block/list store :after "122064")))))


(deftest ^:integration test-file-store
  (let [tmpdir (mk-tmpdir!)]
    (tests/check-store! #(file-block-store tmpdir))))
