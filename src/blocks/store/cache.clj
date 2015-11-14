(ns blocks.store.cache
  "Logical block store which is backed by two other stores, a _primary store_
  and a _cache_. Blocks will be added to the cache on reads and writes. Blocks
  will be removed from the cache based on usage to keep it below a certain total
  size."
  (:require
    [blocks.core :as block]
    [clojure.data.priority-map :refer [priority-map]]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]))


(defn- scan-state
  "Computes the state of a cache, including priorities for all stored blocks and
  the total size of block content stored."
  [store]
  (reduce
    (fn [state block]
      (let [tick (or (some-> ^java.util.Date (:stored-at block) (.getTime)) 0)]
        (-> state
            (update :priorities assoc (:id block) [tick (:size block)])
            (update :total-size + (:size block))
            (update :tick max tick))))
    {:priorities (priority-map)
     :total-size 0
     :tick 0}
    (block/list store)))


(defn- ensure-initialized!
  [store]
  (when-not (:primary store)
    (throw (IllegalStateException.
             "Cache not initialized, no :primary store set")))
  (when-not (:cache store)
    (throw (IllegalStateException.
             "Cache not initialized, no :cache store set")))
  (when-not (:state store)
    (throw (IllegalStateException.
             "Cache not initialized, no :state available"))))


(defn reap-space!
  "Given a target amount of space to free and a cache store, deletes blocks from
  the cache to free up the desired amount of space. Returns a vector of the
  deleted entries."
  [store target-free]
  (let [{:keys [cache state size-limit]} store]
    (loop [deleted []]
      (let [{:keys [priorities total-size]} @state]
        (if (and (< (- size-limit total-size) target-free)
                 (not (empty? priorities)))
          ; Need to delete the next block.
          (let [[id [tick size]] (peek priorities)]
            (block/delete! cache id)
            (swap! state assoc
                   :total-size (- total-size size)
                   :priorities (pop priorities))
            (recur (conj deleted {:id id, :tick tick, :size size})))
          ; Enough free space, or no more blocks to delete.
          deleted)))))


(defn- maybe-cache!
  [store block]
  (let [{:keys [cache state size-limit max-block-size]} store]
    ; Should we cache this block?
    (when (and (<= (:size block) size-limit)
               (or (nil? max-block-size)
                   (<= (:size block) max-block-size)))
      ; Free up enough space for the new block.
      (reap-space! store (:size block))
      ; Store the block and update cache state.
      (when-let [cached (block/put! cache block)]
        (swap! (:state store)
               (fn [{:keys [priorities total-size tick]}]
                 {:priorities (assoc priorities (:id cached) [tick (:size cached)])
                  :total-size (+ total-size (:size cached))
                  :tick (inc tick)}))
        cached))))


(defrecord CachingBlockStore
  [size-limit max-block-size primary cache state]

  component/Lifecycle

  (start
    [this]
    (if state
      (do (log/info "CachingBlockStore is already initialized")
          this)
      (do
        (when-not primary
          (throw (IllegalStateException.
                   "Cannot start caching store without backing primary store")))
        (when-not cache
          (throw (IllegalStateException.
                   "Cannot start caching store without backing cache store")))
        (log/info "Scanning cache store to build state...")
        (let [initial-state (scan-state cache)
              this' (assoc this :state (atom initial-state))]
          (log/infof "Cache has %d bytes in %d blocks"
                     (:total-size initial-state)
                     (count (:priorities initial-state)))
          (when (< size-limit (:total-size initial-state))
            (log/infof "Reaping cache down to size limit of %d bytes" size-limit)
            (let [deleted (reap-space! this' 0)]
              (log/infof "Deleted %d blocks to free %d bytes of space"
                         (count deleted) (reduce + 0 (map :size deleted)))))
          this'))))


  (stop
    [this]
    this)


  block/BlockStore

  (stat
    [this id]
    (ensure-initialized! this)
    (or (block/stat cache   id)
        (block/stat primary id)))


  (-list
    [this opts]
    (ensure-initialized! this)
    (block/-list primary opts))


  (-get
    [this id]
    (ensure-initialized! this)
    (or (block/-get cache id)
        (when-let [block (block/-get primary id)]
          (maybe-cache! this block)
          block)))


  (put!
    [this block]
    (ensure-initialized! this)
    (when-let [id (:id block)]
      (let [cached (maybe-cache! this block)
            preferred (if (or (realized? block) (nil? cached))
                        block
                        cached)
            stored (block/put! primary preferred)]
        (or cached stored))))


  (delete!
    [this id]
    (ensure-initialized! this)
    (block/delete! cache id)
    (block/delete! primary id)))


(defn cache-store
  "Creates a new logical block store which will use one block store to cache
  up to a certain size of content for another block store. The store should
  have a `:primary` and a `:cache` associated with it for backing block
  storage.

  Supported options are:

  - `:max-block-size` do not cache blocks larger than this size.
  - `:primary` set a backing primary store
  - `:cache` set a backing cache store"
  [size-limit & {:as opts}]
  ; FIXME: validate arguments and opts
  (CachingBlockStore.
    size-limit
    (:max-block-size opts)
    (:primary opts)
    (:cache opts)
    nil))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->CachingBlockStore)
(ns-unmap *ns* 'map->CachingBlockStore)
