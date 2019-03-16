(ns blocks.store.init-test
  (:require
    [blocks.store :as store]
    [blocks.store.file]
    [blocks.store.memory]
    [clojure.test :refer :all])
  (:import
    blocks.store.file.FileBlockStore
    blocks.store.memory.MemoryBlockStore
    java.io.File))


(defn- relative-path
  [& parts]
  (str/join File/separator parts))


(defn- absolute-path
  [& parts]
  (str File/separator (apply relative-path parts)))


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
        (is (= (absolute-path "foo" "bar" "baz") (str (:root store))))))
    (testing "relative path"
      (let [store (store/initialize "file://foo/bar/baz")]
        (is (instance? (Class/forName "blocks.store.file.FileBlockStore") store))
        (is (= (relative-path "foo" "bar" "baz") (str (:root store))))))))
