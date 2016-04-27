(ns blocks.store.memory
  "Block storage backed by a map in an atom. Blocks put into this store will be
  passed to `load!` to ensure the content resides in memory.

  This store is most suitable for testing, caches, and other situations which
  call for a non-persistent block store."
  (:require
    (blocks
      [core :as block]
      [data :as data]
      [store :as store])
    [blocks.store.util :as util])
  (:import
    java.util.Date))


(defn- block-stats
  "Build a map of stat data for a stored block."
  [block]
  (merge (block/meta-stats block)
         {:id (:id block)
          :size (:size block)}))


;; Block records in a memory store are held in a map in an atom.
(defrecord MemoryStore
  [memory]

  store/BlockStore

  (-stat
    [this id]
    (when-let [block (get @memory id)]
      (block-stats block)))


  (-list
    [this opts]
    (->> @memory
         (map (comp block-stats val))
         (util/select-stats opts)))


  (-get
    [this id]
    (get @memory id))


  (-put!
    [this block]
    (when-let [id (:id block)]
      (if-let [extant (get @memory id)]
        extant
        (let [block' (-> (block/load! block)
                         (data/clean-block)
                         (block/with-stats {:stored-at (Date.)}))]
          (swap! memory assoc id block')
          block'))))


  (-delete!
    [this id]
    (let [existed? (contains? @memory id)]
      (swap! memory dissoc id)
      existed?)))


(defn memory-store
  "Creates a new in-memory block store."
  []
  (MemoryStore. (atom (sorted-map) :validator map?)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->MemoryStore)
(ns-unmap *ns* 'map->MemoryStore)
