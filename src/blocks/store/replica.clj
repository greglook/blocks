(ns blocks.store.replica
  "Logical block storage which writes to multiple backing stores to ensure
  durability. Lookups will try the backing stores in order to find blocks."
  (:require
    [blocks.store :as store]))


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
         (apply store/merge-block-lists)))


  (-get
    [this id]
    (some #(store/-get % id) stores))


  (-put!
    [this block]
    (let [stored-block (store/-put! (first stores) block)
          copy-block (store/preferred-copy block stored-block)]
      (run! #(store/-put! % copy-block) (rest stores))
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

(store/privatize-constructors! ReplicaBlockStore)


(defn replica-block-store
  "Creates a new replica block store."
  [stores & {:as opts}]
  (map->ReplicaBlockStore
    (assoc opts :stores (vec stores))))
