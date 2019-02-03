(ns blocks.store.buffer
  "Logical block storage which uses two backing stores to implement a buffer.

  New blocks are written to the _buffer_ store, which can be flushed to write
  all of the blocks to the _primary_ store. Reads return a unified view of the
  existing and buffered blocks."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.summary :as sum]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]
    [manifold.stream :as s]))


(defrecord BufferBlockStore
  [primary buffer predicate]

  component/Lifecycle

  (start
    [this]
    (when-not (satisfies? store/BlockStore primary)
      (throw (IllegalStateException.
               (str "Cannot start buffer block store without a backing primary store: "
                    (pr-str primary)))))
    (when-not (satisfies? store/BlockStore buffer)
      (throw (IllegalStateException.
               (str "Cannot start buffer block store without a backing buffer store: "
                    (pr-str buffer)))))
    this)


  (stop
    [this]
    this)


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
        [stored]
        (or stored
            (if (or (nil? predicate) (predicate block))
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
  "Remove all blocks from the buffer. Returns a deferred which yields a summary
  of the deleted blocks."
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
  "Flush the store, writing all buffered blocks to the primary store. Returns a
  deferred which yields a summary of the flushed blocks."
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
  "Create a new buffering block store.

  - `:buffer`
    Block store to use for new writes.
  - `:primary`
    Block store to use for flushed blocks.
  - `:predicate` (optional)
    A predicate function which should return false for blocks which should not
    be buffered; instead, they will be written directly to the primary store."
  [& {:as opts}]
  (map->BufferBlockStore opts))
