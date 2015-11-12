(ns blocks.store-tests
  (:require
    (blocks.store
      [file :refer [file-store]]
      [memory :refer [memory-store]]
      [tests :refer [test-block-store]])
    [clojure.java.io :as io]
    [clojure.test :refer :all]))


(deftest test-memory-store
  (let [store (memory-store)]
    (test-block-store "memory-store" store
                      :blocks 25
                      :max-size 4096)))


(deftest test-file-store
  (let [tmpdir (io/file "target" "test" "tmp"
                        (str "file-block-store."
                          (System/currentTimeMillis)))
        store (file-store tmpdir)]
    (test-block-store "file-store" store
                      :blocks 10
                      :max-size 1024)
    (blocks.store.file/erase! store)))
