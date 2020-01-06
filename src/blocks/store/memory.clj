(ns blocks.store.memory
  "Memory stores provide process-local storage backed by a map in a ref. Blocks
  put into this store will be fully read into memory to ensure the content is
  present locally. Memory block stores may be constructed usin the `mem:-` URI
  form.

  This store is most suitable for testing, caches, and other situations which
  call for an ephemeral block store."
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [manifold.deferred :as d]
    [manifold.stream :as s])
  (:import
    blocks.data.Block))


(defn- load-block
  "Prepare a new block for storage based on the given block. This ensures the
  content is loaded into memory and cleans the block metadata."
  [^Block block]
  (if (data/byte-content? block)
    (data/create-block
      (:id block)
      (:size block)
      (.content block))
    (data/read-block
      (:algorithm (:id block))
      (data/read-all (.content block)))))


;; Block records in a memory store are held in a map in a ref.
(defrecord MemoryBlockStore
  [memory]

  store/BlockStore

  (-list
    [this opts]
    (s/->source (or (vals @memory) [])))


  (-stat
    [this id]
    (d/success-deferred
      (when-let [block (get @memory id)]
        {:id (:id block)
         :size (:size block)
         :stored-at (:stored-at block)})))


  (-get
    [this id]
    (d/success-deferred
      (get @memory id)))


  (-put!
    [this block]
    (let [id (:id block)]
      (store/future'
        (dosync
          (if-let [extant (get @memory id)]
            extant
            (let [block (load-block block)]
              (alter memory assoc id block)
              block))))))


  (-delete!
    [this id]
    (store/future'
      (dosync
        (let [existed? (contains? @memory id)]
          (alter memory dissoc id)
          existed?))))


  store/ErasableStore

  (-erase!
    [this]
    (store/future'
      (dosync
        (alter memory empty)
        true))))



;; ## Constructors

(store/privatize-constructors! MemoryBlockStore)


(defn memory-block-store
  "Creates a new in-memory block store."
  [& {:as opts}]
  (map->MemoryBlockStore
    (assoc opts :memory (ref (sorted-map) :validator map?))))


(defmethod store/initialize "mem"
  [_]
  (memory-block-store))
