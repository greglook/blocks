(ns blocks.store.buffer
  "Logical block storage which buffers new blocks being written to a backing
  store. Reads return a unified view of the existing and buffered blocks. The
  buffer can be _flushed_ to write all the new blocks to the backing store."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.summary :as sum]))


(defrecord BufferBlockStore
  [max-block-size store buffer]

  store/BlockStore

  (-stat
    [this id]
    (or (store/-stat buffer id)
        (store/-stat store  id)))


  (-list
    [this opts]
    (store/merge-block-lists
      (store/-list buffer opts)
      (store/-list store  opts)))


  (-get
    [this id]
    (or (store/-get buffer id)
        (store/-get store  id)))


  (-put!
    [this block]
    (or (store/-get store (:id block))
        (if (or (nil? max-block-size)
                (<= (:size block) max-block-size))
          (store/-put! buffer block)
          (store/-put! store block))))


  (-delete!
    [this id]
    (let [buffered? (store/-delete! buffer id)
          stored?   (store/-delete! store  id)]
      (boolean (or buffered? stored?)))))


(defn clear!
  "Removes all blocks from the buffer. Returns a summary of the deleted blocks."
  [store]
  (->> (block/list (:buffer store))
       (map (fn [stats]
              (block/delete! (:buffer store) (:id stats))
              stats))
       (reduce sum/update (sum/init))))


(defn flush!
  "Flushes the store, writing all buffered blocks to the backing store. Returns
  a summary of the flushed blocks."
  ([store]
   (flush! store (map :id (block/list (:buffer store)))))
  ([store block-ids]
   (->> block-ids
        (keep (fn copy
                [id]
                (when-let [block (block/get (:buffer store) id)]
                  (block/put! (:store store) block)
                  (block/delete! (:buffer store) id)
                  block)))
        (reduce sum/update (sum/init)))))



;; ## Constructors

(store/privatize-constructors! BufferBlockStore)


(defn buffer-block-store
  "Creates a new buffering block store. If no buffer store is given, defaults to
  an in-memory store."
  [& {:as opts}]
  (map->BufferBlockStore opts))
