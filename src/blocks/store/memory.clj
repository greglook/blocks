(ns blocks.store.memory
  "Block storage backed by an atom in memory."
  (:require
    [blocks.core :as block])
  (:import
    java.util.Date))


;; Block records in a memory store are held in a map in an atom.
(defrecord MemoryBlockStore
  [memory]

  block/BlockStore

  (-list
    [this opts]
    (block/select-hashes opts (keys @memory)))


  (stat
    [this id]
    (when-let [block (get @memory id)]
      (merge (block/meta-stats block)
             {:id (:id block)
              :size (:size block)})))


  (-get
    [this id]
    (get @memory id))


  (put!
    [this block]
    (when-let [id (:id block)]
      (or (get @memory id)
          (let [block' (block/with-stats (block/load! block)
                                         {:stored-at (Date.)})]
            (swap! memory assoc id block')
            block'))))


  (delete!
    [this id]
    (swap! memory dissoc id)))


(defn memory-store
  "Creates a new in-memory block store."
  []
  (MemoryBlockStore. (atom (sorted-map) :validator map?)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->MemoryBlockStore)
(ns-unmap *ns* 'map->MemoryBlockStore)
