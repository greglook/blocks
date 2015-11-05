(ns blocks.core-test
  (:require
    [blocks.core :as block]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]
    [multihash.core :as multihash]))


;; ## Storage Function Tests

(deftest list-wrapper
  (let [store (reify block/BlockStore (enumerate [this opts] (vector :list opts)))]
    (is (= [:list nil] (block/list store)))
    (is (= [:list {:foo "bar" :baz 3}] (block/list store :foo "bar" :baz 3)))))


(deftest checked-get
  (let [content "foobarbaz"
        block (block/read! content)
        id (multihash/sha1 "bazbarfoo")
        store (reify block/BlockStore (get* [this id] (assoc block :id id)))
        block (block/get* store id)]
    (is (= id (:id block)))
    (is (bytes= content (:content block)))
    (is (thrown? RuntimeException
                 (block/get store (:id block))))))


(deftest multihash-selection
  (let [a (multihash/create :sha1 "37b51d194a7513e45b56f6524f2d51f200000000")
        b (multihash/create :sha1 "73fcffa4b7f6bb68e44cf984c85f6e888843d7f9")
        c (multihash/create :sha1 "73fe285cedef654fccc4a4d818db4cc225932878")
        d (multihash/create :sha1 "acbd18db4cc2f856211de9ecedef654fccc4a4d8")
        e (multihash/create :sha1 "c3c23db5285662ef717963ff4ce2373df0003206")
        f (multihash/create :sha2-256 "285c3c23d662b5ef7172373df0963ff4ce003206")
        hashes [a b c d e f]]
    (are [brs opts] (= brs (block/select-hashes opts hashes))
         hashes   {}
         [c d e f]  {:after "111473fd2"}
         [b c]      {:prefix "111473"}
         [f]        {:algorithm :sha2-256})))
