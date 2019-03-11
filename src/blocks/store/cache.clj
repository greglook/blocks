(ns blocks.store.cache
  "Cache stores provide logical block storage backed by two other stores, a
  _primary store_ and a _cache_.

  Blocks are added to the cache on reads and writes, and evicted with a
  least-recently-used strategy to keep the cache under a certain total size.
  Operations on this store will prefer to look up blocks in the cache, and fall
  back to the primary store when not available.

  Because the caching logic runs locally, the backing cache storage should not
  be shared among multiple concurrent processes."
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.summary :as sum]
    [clojure.data.priority-map :refer [priority-map]]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d])
  (:import
    java.time.Instant))


(defn- scan-state
  "Computes the state of a cache, including priorities for all stored blocks and
  the total size of block content stored."
  [store]
  (reduce
    (fn [state block]
      (let [tick (if-let [stored-at (:stored-at block)]
                   (long (/ (.toEpochMilli ^Instant stored-at) 1000))
                   0)]
        (-> state
            (update :priorities assoc (:id block) [tick (:size block)])
            (update :total-size + (:size block))
            (update :tick max tick))))
    {:priorities (priority-map)
     :total-size 0
     :tick 0}
    (block/list-seq store)))


(defn- cacheable?
  "True if the block may be cached in this store."
  [store block]
  (let [{:keys [size-limit predicate]} store]
    (and (<= (:size block) size-limit)
         (or (nil? predicate) (predicate block)))))


(defn- touch-block
  "Update the cache state to account for the usage (fetch or store) of a
  block."
  [state block]
  (let [id (:id block)
        size (:size block)
        priorities (:priorities state)]
    (-> state
        (update :tick inc)
        (update :priorities assoc id [(:tick state) size])
        (cond->
          (not (contains? priorities id))
          (update :total-size + size)))))


(defn- remove-block
  "Update the cache state to remove a block from it by id."
  [state id]
  (if-let [[tick size] (get-in state [:priorities id])]
    (-> state
        (update :total-size - size)
        (update :priorities dissoc id))
    state))


(defn reap!
  "Given a target amount of space to free and a cache store, deletes blocks from
  the cache to free up the desired amount of space. Returns a deferred which
  yields a summary of the deleted entries."
  [store target-free]
  (let [{:keys [cache state size-limit]} store]
    (d/loop [deleted (sum/init)]
      (let [{:keys [priorities total-size]} @state]
        (if (and (< (- size-limit total-size) target-free)
                 (not (empty? priorities)))
          ; Need to delete the next block.
          (let [[id [tick size]] (peek priorities)]
            (swap! state remove-block id)
            (d/chain
              (block/delete! cache id)
              (fn next-delete
                [deleted?]
                (d/recur (if deleted?
                           (sum/update deleted {:id id, :size size})
                           deleted)))))
          ; Enough free space, or no more blocks to delete.
          deleted)))))


(defn- cache-block!
  "Store a block in the cache and update the internal tracking state."
  [store block]
  (swap! (:state store) touch-block block)
  (d/chain
    (reap! store (:size block))
    (fn cache-block
      [_]
      (block/put! (:cache store) block))))


(defrecord CachingBlockStore
  [size-limit predicate primary cache state]

  component/Lifecycle

  (start
    [this]
    (when-not (satisfies? store/BlockStore primary)
      (throw (IllegalStateException.
               (str "Cannot start caching block store without a backing primary store: "
                    (pr-str primary)))))
    (when-not (satisfies? store/BlockStore cache)
      (throw (IllegalStateException.
               (str "Cannot start caching block store without a backing cache store: "
                    (pr-str cache)))))
    (when-not @state
      (let [initial-state (scan-state cache)
            cached-bytes (:total-size initial-state)]
        (reset! state initial-state)
        (when (pos? cached-bytes)
          (log/infof "Cache has %d bytes in %d blocks"
                     (:total-size initial-state)
                     (count (:priorities initial-state))))))
    this)


  (stop
    [this]
    this)


  store/BlockStore

  (-list
    [this opts]
    (store/merge-blocks
      (block/list cache opts)
      (block/list primary opts)))


  (-stat
    [this id]
    (store/some-store [cache primary] block/stat id))


  (-get
    [this id]
    (d/chain
      (block/get cache id)
      (fn check-cache
        [block]
        (if block
          (vary-meta block assoc ::cached? true)
          (block/get primary id)))
      (fn recache
        [block]
        (cond
          ; Block not present in cache or primary.
          (nil? block)
          nil

          ; Block is already cached.
          (::cached? (meta block))
          (do (swap! state touch-block block)
              block)

          ; Determine whether to cache the primary block.
          (cacheable? this block)
          (cache-block! this block)

          ; Non cacheable block from the primary store.
          :else block))))


  (-put!
    [this block]
    (d/chain
      (d/zip
        (block/put! primary block)
        (when (cacheable? this block)
          (cache-block! this block)))
      (fn return-preferred
        [[stored cached]]
        (store/preferred-block
          stored
          (when cached
            (vary-meta cached assoc ::cached? true))))))


  (-delete!
    [this id]
    (d/chain
      (d/zip
        (block/delete! primary id)
        (block/delete! cache id))
      (fn result
        [[stored? cached?]]
        (boolean (or stored? cached?)))))


  store/ErasableStore

  (-erase!
    [this]
    (d/chain
      (d/zip
        (block/erase! primary)
        (block/erase! cache))
      (constantly true))))



;; ## Constructors

(store/privatize-constructors! CachingBlockStore)


(defn caching-block-store
  "Create a new logical block store which will use one block store to cache
  up to a certain size of content for another store. This store should have a
  `:primary` and a `:cache` associated with it for backing block storage.

  - `:primary`
    Backing store with the primary block data.
  - `:cache`
    Store to cache blocks in and prefer for reads.
  - `:size-limit`
    Maximum total size of blocks to keep in the cache store.
  - `:predicate` (optional)
    A predicate function which should return false for blocks which should not
    be cached; instead, they will only be written to the primary store."
  [size-limit & {:as opts}]
  (when-not (pos-int? size-limit)
    (throw (IllegalArgumentException.
             (str "Cache store size-limit must be a positive integer: "
                  (pr-str size-limit)))))
  (map->CachingBlockStore
    (assoc opts
           :size-limit size-limit
           :state (atom nil))))
