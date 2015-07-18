(ns blobble.core-test
  (:require
    [blobble.core :as blob]
    (blobble.store
      [file :refer [file-store]]
      [memory :refer [memory-store]])
    [byte-streams :refer [bytes=]]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [multihash.core :as multihash]))


;; ## Storage Function Tests

(deftest list-wrapper
  (let [store (reify blob/BlobStore (enumerate [this opts] (vector :list opts)))]
    (is (= [:list nil] (blob/list store)))
    (is (= [:list {:foo "bar" :baz 3}] (blob/list store :foo "bar" :baz 3)))))


(deftest checked-get
  (let [content (.getBytes "foobarbaz")
        id (blob/identify content)
        store (reify blob/BlobStore (get* [this id] (blob/read! content)))
        blob (blob/get* store id)]
    (is (= id (:id blob)))
    (is (bytes= content (:content blob)))
    (is (thrown? RuntimeException
                 (blob/get store (blob/identify "bazbarfoo"))))))


(deftest hash-id-selection
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f2")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e88")
        c (multihash/create :sha1 "73fe285cedef654fccc4a4d818db4cc2")
        d (multihash/create :sha1 "acbd18db4cc2f85cedef654fccc4a4d8")
        e (multihash/create :sha1 "c3c23db5285662ef7172373df0003206")
        hash-ids [a b c d e]]
    (are [brs opts] (= brs (blob/select-ids opts hash-ids))
         hash-ids {}
         [c d e]  {:after "73fd2"}
         [b c]    {:prefix "73"}
         [a b]    {:limit 2})))



;; ## Blob Store Tests

(defn- store-test-blobs!
  "Stores some test blobs in the given blob store and returns a map of the
  ids to the original string values."
  [store]
  (->> ["foo" "bar" "baz" "foobar" "barbaz"]
       (map (juxt (comp :id (partial blob/store! store)) identity))
       (into (sorted-map))))


(defn- test-blob-content
  "Determines whether the store contains the content for the given identifier."
  [store id content]
  (let [status (blob/stat store id)
        stored-content (:content (blob/get store id))]
    (is (and status stored-content) "returns info and content")
    (is (= (:stat/size status) (count stored-content)) "stats contain size info")
    (is (= content (slurp stored-content)) "stored content matches input")))


(defn- test-restore-blob
  "Tests re-storing an existing blob."
  [store id content]
  (let [status     (blob/stat store id)
        new-blob   (blob/store! store content)
        new-status (blob/stat store id)]
    (is (= id (:id new-blob)))
    (is (= (:stat/stored-at status)
           (:stat/stored-at new-status)))))


(defn test-blob-store
  "Tests a blob store implementation."
  [store label]
  (println "  *" label)
  (is (empty? (blob/list store)) "starts empty")
  (testing (str (-> store class .getSimpleName))
    (let [stored-content (store-test-blobs! store)]
      (is (= (keys stored-content) (blob/list store {}))
          "enumerates all ids in sorted order")
      (doseq [[id content] stored-content]
        (test-blob-content store id content))
      (let [[id content] (first (seq stored-content))]
        (test-restore-blob store id content))
      (let [expected-size (reduce + 0 (map (comp count #(.getBytes %))
                                           (vals stored-content)))]
        (is (= expected-size (blob/scan-size store))))
      (doseq [id (keys stored-content)]
        (blob/delete! store id))
      (is (empty? (blob/list store)) "ends empty"))))



;; ## Storage Implementations

(deftest test-memory-store
  (let [store (memory-store)]
    (test-blob-store store "memory-store")))


(deftest test-file-store
  (let [tmpdir (io/file "target" "test" "tmp"
                        (str "file-blob-store."
                          (System/currentTimeMillis)))
        store (file-store tmpdir)]
    (test-blob-store store "file-store")
    (blob/erase!! store)))
