(ns blocks.store.replica
  "Logical block storage which writes to multiple backing stores to ensure
  durability. Lookups will try the backing stores in order to find blocks."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]))


(defn- zip-stores
  "Run the provided function on each of the identified store keys. Returns a
  deferred zip over the results against each store."
  ([replicas f arg]
   (zip-stores replicas f arg (:store-keys replicas)))
  ([replicas f arg store-keys]
   (->>
     store-keys
     (map #(f (get replicas %) arg))
     (apply d/zip))))


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
    (->> store-keys
         (map #(block/list (get this %) opts))
         (apply store/merge-blocks)))


  (-stat
    [this id]
    (d/loop [stores (mapv this store-keys)]
      (when-let [store (first stores)]
        (d/chain
          (block/stat store id)
          (fn check-result
            [stats]
            (or stats (d/recur (next stores))))))))


  (-get
    [this id]
    ; OPTIMIZE: query in parallel, use `d/alt`?
    (d/loop [stores (mapv this store-keys)]
      (when-let [store (first stores)]
        (d/chain
          (block/get store id)
          (fn check-result
            [block]
            (or block (d/recur (next stores))))))))


  (-put!
    [this block]
    (d/chain
      (block/put! (get this (first store-keys)) block)
      (fn keep-preferred
        [stored]
        (let [block (store/preferred-block block stored)]
          (d/chain
            (zip-stores this block/put! block (rest store-keys))
            (constantly stored))))))


  (-delete!
    [this id]
    (d/chain
      (zip-stores this block/delete! id)
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
