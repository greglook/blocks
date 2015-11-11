(ns blocks.store.cache
  "Logical block store which is backed by two other stores, a _primary store_
  and a _cache_. Blocks will be added to the cache on reads and writes. Blocks
  will be removed from the cache based on usage to keep it below a certain total
  size."
  (:require
    [blocks.core :as block])
  (:import
    java.util.Date))


; TODO: implement
