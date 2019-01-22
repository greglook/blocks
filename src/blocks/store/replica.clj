(ns blocks.store.replica
  "Logical block storage which writes to multiple backing stores to ensure
  durability. Lookups will try the backing stores in order to find blocks."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]))


(defrecord ReplicaBlockStore
  [store-keys]

  component/Lifecycle

  (start
    [this]
    (when-let [missing (seq (remove (partial contains? this) store-keys))]
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
    (->> (map this store-keys)
         (map #(block/list % opts))
         (apply store/merge-blocks)))


  (-stat
    [this id]
    (store/some-store (mapv this store-keys) block/stat id))


  (-get
    [this id]
    ; OPTIMIZE: query in parallel, use `d/alt`?
    (store/some-store (mapv this store-keys) block/get id))


  (-put!
    [this block]
    (d/chain
      (block/put! (get this (first store-keys)) block)
      (fn keep-preferred
        [stored]
        (let [block (store/preferred-block block stored)]
          (d/chain
            (store/zip-stores (mapv this (rest store-keys)) block/put! block)
            (constantly stored))))))


  (-delete!
    [this id]
    (d/chain
      (store/zip-stores (mapv this store-keys) block/delete! id)
      (partial some true?)
      boolean)))



;; ## Constructors

(store/privatize-constructors! ReplicaBlockStore)


(defn replica-block-store
  "Creates a new replica block store which will persist blocks to multiple
  backing stores. Block operations will be performed on the stores in the order
  given in `store-keys`, where each key is looked up in the store record."
  [store-keys & {:as opts}]
  (map->ReplicaBlockStore
    (assoc opts :store-keys (vec store-keys))))
