(ns blocks.store.replica
  "Replica stores provide logical block storage which writes to multiple
  backing stores. Lookups will try the backing stores in order to find blocks.

  Replicas are useful for ensuring durability across stores and for shared
  caches, where some external process controls cache eviction."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]))


(defn- resolve-stores
  "Resolve the configured replica stores."
  ([store]
   (resolve-stores store (:replicas store)))
  ([store replicas]
   (mapv (partial get store) replicas)))


(defrecord ReplicaBlockStore
  [replicas]

  component/Lifecycle

  (start
    [this]
    (when-let [missing (seq (remove (partial contains? this) replicas))]
      (throw (IllegalStateException.
               (str "Replica block store is missing configured keys: "
                    (pr-str missing)))))
    this)


  (stop
    [this]
    this)


  store/BlockStore

  (-list
    [this opts]
    (->> (resolve-stores this)
         (map #(block/list % opts))
         (apply store/merge-blocks)))


  (-stat
    [this id]
    (store/some-store (resolve-stores this) block/stat id))


  (-get
    [this id]
    ;; OPTIMIZE: query in parallel, use `d/alt`?
    (store/some-store (resolve-stores this) block/get id))


  (-put!
    [this block]
    (d/chain
      (block/put! (get this (first replicas)) block)
      (fn keep-preferred
        [stored]
        (let [block (store/preferred-block block stored)]
          (d/chain
            (store/zip-stores (resolve-stores this (rest replicas)) block/put! block)
            (partial apply store/preferred-block stored))))))


  (-delete!
    [this id]
    (d/chain
      (store/zip-stores (resolve-stores this) block/delete! id)
      (partial some true?)
      boolean)))


;; ## Constructors

(store/privatize-constructors! ReplicaBlockStore)


(defn replica-block-store
  "Creates a new replica block store which will persist blocks to multiple
  backing stores. Block operations will be performed on the stores in the order
  given in `replicas`, where each key is looked up in the store record."
  [replicas & {:as opts}]
  (map->ReplicaBlockStore
    (assoc opts :replicas (vec replicas))))
