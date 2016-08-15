(ns blocks.store.replica
  "Logical block storage which writes to multiple backing stores to ensure
  durability. Lookups will try the backing stores in order to find blocks."
  (:require
    [blocks.store :as store]
    [blocks.store.util :as util]))


(defrecord ReplicaBlockStore
  [stores]

  store/BlockStore

  (-stat
    [this id]
    (some #(store/-stat % id) stores))


  (-list
    [this opts]
    (->> stores
         (map #(store/-list % opts))
         (doall)
         (apply util/merge-block-lists)))


  (-get
    [this id]
    (some #(store/-get % id) stores))


  (-put!
    [this block]
    (let [stored-block (store/-put! (first stores) block)
          copy-block (util/preferred-copy block stored-block)]
      (dorun (map #(store/-put! % copy-block) (rest stores)))
      stored-block))


  (-delete!
    [this id]
    (reduce
      (fn [existed? store]
        (let [result (store/-delete! store id)]
          (or existed? result)))
      false
      stores)))



;; ## Constructors

(defn replica-block-store
  "Creates a new replica block store."
  [stores & {:as opts}]
  (map->ReplicaBlockStore
    (assoc opts :stores (vec stores))))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->ReplicaBlockStore)
(ns-unmap *ns* 'map->ReplicaBlockStore)
