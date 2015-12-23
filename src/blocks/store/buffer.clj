(ns blocks.store.buffer
  "Logical block storage which buffers new blocks being written to a backing
  store. Reads return a unified view of the existing and buffered blocks. The
  buffer can be _flushed_ to write all the new blocks to the backing store."
  (:require
    (blocks
      [core :as block]
      [util :as util])
    [blocks.store.memory :refer [memory-store]]))


(defrecord BufferStore
  [store buffer]

  block/BlockStore

  (stat
    [this id]
    (or (block/stat buffer id)
        (block/stat store  id)))


  (-list
    [this opts]
    (util/merge-block-lists
      (block/-list buffer opts)
      (block/-list store  opts)))


  (-get
    [this id]
    (or (block/-get buffer id)
        (block/-get store  id)))


  (put!
    [this block]
    (or (block/get store (:id block))
        (block/put! buffer block)))


  (delete!
    [this id]
    (let [buffered? (block/delete! buffer id)
          stored?   (block/delete! store  id)]
      (boolean (or buffered? stored?)))))


(defn flush!
  "Flushes the store, writing all buffered blocks to the backing store. Returns
  a seq of the flushed block ids."
  [store]
  (->> (block/list (:buffer store))
       (map (fn copy [stats]
              (->> (:id stats)
                   (block/get (:buffer store))
                   (block/put! (:store store)))
              (block/delete! (:buffer store) (:id stats))
              (:id stats)))
       (doall)))


(defn buffer-store
  "Creates a new buffering block store. If no buffer store is given, defaults to
  an in-memory store."
  ([store]
   (buffer-store store (memory-store)))
  ([store buffer]
   (BufferStore. store buffer)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->BufferStore)
(ns-unmap *ns* 'map->BufferStore)
