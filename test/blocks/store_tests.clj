(ns blocks.store-tests
  (:require
    [blocks.core :as block]
    (blocks.store
      [file :refer [file-store]]
      [memory :refer [memory-store]]
      [tests :refer [test-block-store]])
    [byte-streams :as bytes]
    [clojure.java.io :as io]
    [clojure.test :refer :all])
  (:import
    blocks.data.PersistentBytes))


(deftest test-memory-store
  (let [store (memory-store)]
    (test-block-store store "memory-store")))


(deftest test-file-store
  (let [tmpdir (io/file "target" "test" "tmp"
                        (str "file-block-store."
                          (System/currentTimeMillis)))
        store (file-store tmpdir)]
    (test-block-store store "file-store")
    (block/erase!! store)))
