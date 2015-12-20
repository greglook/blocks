(ns blocks.store.replica
  "Logical block storage which writes to multiple backing stores to ensure
  durability. Lookups will try the backing stores in order to find blocks."
  (:require
    (blocks
      [core :as block])))


(defrecord ReplicaStore
  [stores]

  block/BlockStore

  (stat
    [this id]
    (some #(block/stat % id) stores))


  (-list
    [this opts]
    ; TODO: merge from all stores
    (block/-list (first stores) opts))


  (-get
    [this id]
    (some #(block/-get % id) stores))


  (put!
    [this block]
    (let [stored-block (block/put! (first stores) block)
          copy-block (if (realized? block) block stored-block)]
      (dorun (map #(block/put! % copy-block) (rest stores)))
      stored-block))


  (delete!
    [this id]
    (reduce
      (fn [existed? store]
        (let [result (block/delete! store id)]
          (or existed? result)))
      false
      stores)))


(defn replica-store
  "Creates a new replica block store."
  [stores]
  (ReplicaStore. (vec stores)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->ReplicaStore)
(ns-unmap *ns* 'map->ReplicaStore)
