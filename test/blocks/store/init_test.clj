(ns blocks.store.init-test
  (:require
    [blocks.store :as store]
    (blocks.store
      file
      memory)
    [clojure.test :refer :all])
  (:import
    blocks.store.file.FileBlockStore
    blocks.store.memory.MemoryBlockStore))


(deftest uri-initialization
  (testing "unknown"
    (is (thrown? IllegalArgumentException
                 (store/initialize "foo://something"))))
  (testing "memory init"
    (let [store (store/initialize "mem:-")]
      (is (instance? (Class/forName "blocks.store.memory.MemoryBlockStore") store))))
  (testing "file init"
    (testing "absolute path"
      (let [store (store/initialize "file:///foo/bar/baz")]
        (is (instance? (Class/forName "blocks.store.file.FileBlockStore") store))
        (is (= "/foo/bar/baz" (str (:root store))))))
    (testing "relative path"
      (let [store (store/initialize "file://foo/bar/baz")]
        (is (instance? (Class/forName "blocks.store.file.FileBlockStore") store))
        (is (= "foo/bar/baz" (str (:root store))))))))
