(ns blocks.store.cache
  "Logical block store which is backed by two other stores, a _primary store_
  and a _cache_. Blocks will be added to the cache on reads and writes. Blocks
  will be removed from the cache based on usage to keep it below a certain total
  size."
  (:require
    [blocks.core :as block]
    [clojure.data.priority-map :refer [priority-map]]))


(defn- build-state
  "Computes the state of a cache, including priorities for all stored blocks and
  the total size of block content stored."
  [store tick]
  (reduce
    (fn [state block]
      (-> state
          (update :priorities assoc (:id block) [tick (:size block)])
          (update :total-size + (:size block))))
    {:priorities (priority-map)
     :total-size 0
     :tick (inc tick)}
    (block/list store)))


(defn reap-space!
  "Given a target amount of space to free and a cache store, deletes blocks from
  the cache to free up the desired amount of space. Returns a vector of the
  deleted entries."
  [store target-free]
  (let [{:keys [cache state size-limit]} store]
    (loop [deleted []]
      (let [{:keys [priorities total-size]} @state]
        (if (or (<= target-free (- size-limit total-size))
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
                   (< (:size block) max-block-size)))
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
  [primary cache state size-limit max-block-size]

  block/BlockStore

  (stat
    [this id]
    (or (block/stat cache   id)
        (block/stat primary id)))


  (-list
    [this opts]
    (block/-list primary opts))


  (-get
    [this id]
    (or (block/-get cache id)
        (when-let [block (block/-get primary id)]
          (maybe-cache! this block)
          block)))


  (put!
    [this block]
    (when-let [id (:id block)]
      (let [cached (maybe-cache! this block)
            preferred (if (or (realized? block) (nil? cached))
                        block
                        cached)
            stored (block/put! primary preferred)]
        (or cached stored))))


  (delete!
    [this id]
    (block/delete! cache id)
    (block/delete! primary id)))


(defn cache-store
  "Creates a new caching block store.

  Supported options are:

  - `:size-limit` maximum space in bytes to allocate to cache storage.
  - `:max-block-size` do not cache blocks larger than this size."
  [primary cache & {:as opts}]
  ; FIXME: validate arguments and opts
  ; FIXME: ideally constructors should not be side-effecting
  (CachingBlockStore.
    primary cache
    (atom (build-state cache 0))
    (:size-limit opts)
    (:max-block-size opts)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->CachingBlockStore)
(ns-unmap *ns* 'map->CachingBlockStore)
