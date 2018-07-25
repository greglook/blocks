(ns blocks.store.replica
  "Logical block storage which writes to multiple backing stores to ensure
  durability. Lookups will try the backing stores in order to find blocks."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [com.stuartsierra.component :as component]))


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

  (-stat
    [this id]
    (some #(block/stat (get this %) id) store-keys))


  (-list
    [this opts]
    (->> store-keys
         (map #(block/list (get this %) opts))
         (apply store/merge-block-lists)))


  (-get
    [this id]
    (some #(block/get (get this %) id) store-keys))


  (-put!
    [this block]
    (let [stored-block (block/put! (get this (first store-keys)) block)
          copy-block (store/preferred-copy block stored-block)]
      (run! #(block/put! (get this %) copy-block) (rest store-keys))
      stored-block))


  (-delete!
    [this id]
    (reduce
      (fn [existed? store-key]
        (let [result (block/delete! (get this store-key) id)]
          (or existed? result)))
      false
      store-keys)))



;; ## Constructors

(store/privatize-constructors! ReplicaBlockStore)


(defn replica-block-store
  "Creates a new replica block store which will persist blocks to multiple
  backing stores. Block operations will be performed on the stores in the order
  given in `store-keys`, where each key is looked up in the store record."
  [store-keys & {:as opts}]
  (map->ReplicaBlockStore
    (assoc opts :store-keys (vec store-keys))))
