(ns blocks.store.buffer
  "Logical block storage which uses two backing stores to implement a buffer.

  New blocks are written to the _buffer_ store, which can be flushed to write
  all of the blocks to the _primary_ store. Reads return a unified view of the
  existing and buffered blocks."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.summary :as sum]
    [manifold.deferred :as d]
    [manifold.stream :as s]))


(defrecord BufferBlockStore
  [max-block-size buffer primary]

  ; TODO: support generalized buffer predicate?
  ; TODO: check that buffer and primary are set on start?

  store/BlockStore

  (-list
    [this opts]
    (store/merge-blocks
      (block/list buffer opts)
      (block/list primary opts)))


  (-stat
    [this id]
    (store/some-store [buffer primary] block/stat id))


  (-get
    [this id]
    (store/some-store [buffer primary] block/get id))


  (-put!
    [this block]
    (d/chain
      (block/get primary (:id block))
      (fn store-block
        [block]
        (or block
            (if (or (nil? max-block-size)
                    (<= (:size block) max-block-size))
              (block/put! buffer block)
              (block/put! primary block))))))


  (-delete!
    [this id]
    (d/chain
      (d/zip
        (block/delete! buffer id)
        (block/delete! primary id))
      (fn result
        [[buffered? stored?]]
        (boolean (or buffered? stored?))))))


(defn clear!
  "Removes all blocks from the buffer. Returns a summary of the deleted blocks."
  [store]
  (d/chain
    (s/reduce
      sum/update
      (sum/init)
      (block/list (:buffer store)))
    (fn clear-buffer
      [summary]
      (d/chain
        (block/erase! (:buffer store))
        (constantly summary)))))


(defn flush!
  "Flushes the store, writing all buffered blocks to the backing store. Returns
  a summary of the flushed blocks."
  [store]
  (->>
    (block/list (:buffer store))
    (s/map (fn copy
             [block]
             (d/chain
               (block/put! (:primary store) block)
               (fn delete
                 [block']
                 (d/chain
                   (block/delete! (:buffer store) (:id block))
                   (constantly block'))))))
    (s/realize-each)
    (s/reduce sum/update (sum/init))))



;; ## Constructors

(store/privatize-constructors! BufferBlockStore)


(defn buffer-block-store
  "Creates a new buffering block store.

  - `:buffer`
    Block store to use for new writes.
  - `:primary`
    Block store to use for flushed blocks.
  - `:max-block-size`
    Blocks over this size will not be buffered and will be written to the
    primary directly."
  [& {:as opts}]
  (map->BufferBlockStore opts))
