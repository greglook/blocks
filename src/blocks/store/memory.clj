(ns blocks.store.memory
  "Block storage backed by a map in an atom. Blocks put into this store will be
  passed to `load!` to ensure the content resides in memory. Memory block stores
  may be constructed usin the `mem:-` URI form.

  This store is most suitable for testing, caches, and other situations which
  call for a non-persistent block store."
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
    (data/load-block
      (:id block)
      (data/read-all (.content block)))))


;; Block records in a memory store are held in a map in a ref.
(defrecord MemoryBlockStore
  [memory]

  store/BlockStore

  (-list
    [this opts]
    (s/->source (vals @memory)))


  (-stat
    [this id]
    (d/success-deferred
      (when-let [block (get @memory id)]
        (select-keys block [:id :size :stored-at]))))


  (-get
    [this id]
    (d/success-deferred
      (get @memory id)))


  (-put!
    [this block]
    (let [id (:id block)]
      (d/future
        (dosync
          (if-let [extant (get @memory id)]
            extant
            (let [block (load-block block)]
              (alter memory assoc id block)
              block))))))


  (-delete!
    [this id]
    (d/future
      (dosync
        (let [existed? (contains? @memory id)]
          (alter memory dissoc id)
          existed?))))


  store/ErasableStore

  (-erase!
    [this]
    (d/future
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
  [location]
  (memory-block-store))
