(ns blobble.core-test
  (:require
    [blobble.core :as blob]
    (blobble.store
      [file :refer [file-store]]
      [memory :refer [memory-store]])
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [multihash.core :as multihash])
  (:import
    java.nio.ByteBuffer))


;; ## Storage Function Tests

(deftest list-wrapper
  (let [store (reify blob/BlobStore (enumerate [this opts] (vector :list opts)))]
    (is (= [:list nil] (blob/list store)))
    (is (= [:list {:foo "bar" :baz 3}] (blob/list store :foo "bar" :baz 3)))))


(deftest checked-get
  (let [content "foobarbaz"
        blob (blob/read! content)
        id (multihash/sha1 "bazbarfoo")
        store (reify blob/BlobStore (get* [this id] (assoc blob :id id)))
        blob (blob/get* store id)]
    (is (= id (:id blob)))
    (is (bytes= content (:content blob)))
    (is (thrown? RuntimeException
                 (blob/get store (:id blob))))))


(deftest hash-id-selection
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f200000000")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e888843d7f9")
        c (multihash/create :sha1 "73fe285cedef654fccc4a4d818db4cc225932878")
        d (multihash/create :sha1 "acbd18db4cc2f856211de9ecedef654fccc4a4d8")
        e (multihash/create :sha1 "c3c23db5285662ef717963ff4ce2373df0003206")
        hash-ids [a b c d e]]
    (are [brs opts] (= brs (blob/select-ids opts hash-ids))
         hash-ids {}
         [c d e]  {:after "111473fd2"}
         [b c]    {:prefix "111473"}
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
        blob (blob/get store id)
        stored-content (:content blob)]
    (is (and status stored-content) "returns info and content")
    (is (instance? ByteBuffer stored-content))
    (is (= (:stat/size status) (blob/size blob)) "stats contain size info")
    (.rewind stored-content)
    (is (= content (slurp (bytes/to-input-stream stored-content))) "stored content matches input")))


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
