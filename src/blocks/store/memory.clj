(ns blocks.store.memory
  "Block storage backed by a map in an atom. Blocks put into this store will be
  passed to `load!` to ensure the content resides in memory. Memory block stores
  may be constructed usin the `mem:-` URI form.

  This store is most suitable for testing, caches, and other situations which
  call for a non-persistent block store."
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store])
  (:import
    java.time.Instant))


(defn- block-stats
  "Build a map of stat data for a stored block."
  [block]
  (merge (block/meta-stats block)
         {:id (:id block)
          :size (:size block)}))


(defn- prep-block
  "Prepare a new block for storage."
  [block]
  (-> (block/load! block)
      (data/clean-block)
      (block/with-stats {:stored-at (Instant/now)})))


;; Block records in a memory store are held in a map in an atom.
(defrecord MemoryBlockStore
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
         (store/select-stats opts)))


  (-get
    [this id]
    (get @memory id))


  (-put!
    [this block]
    (when-let [id (:id block)]
      (dosync
        (if-let [extant (get @memory id)]
          extant
          (let [block (prep-block block)]
            (alter memory assoc id block)
            block)))))


  (-delete!
    [this id]
    (dosync
      (let [existed? (contains? @memory id)]
        (alter memory dissoc id)
        existed?)))


  store/BatchingStore

  (-get-batch
    [this ids]
    (keep @memory ids))


  (-put-batch!
    [this blocks]
    (dosync
      (reduce
        (fn [acc block]
          (if-let [extant (get @memory (:id block))]
            (conj acc extant)
            (let [block (prep-block block)]
              (alter memory assoc (:id block) block)
              (conj acc block))))
        [] blocks)))


  (-delete-batch!
    [this ids]
    (dosync
      (reduce
        (fn [acc id]
          (if (contains? @memory id)
            (do (alter memory dissoc id)
                (conj acc id))
            acc))
        [] ids)))


  store/ErasableStore

  (-erase!
    [this]
    (dosync
      (alter memory empty))
    this))



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
