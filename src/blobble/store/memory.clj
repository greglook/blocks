(ns blobble.store.memory
  "Blob storage backed by an atom in memory."
  (:require
    [blobble.core :as blob]))


(defn- blob-stats
  "Augments a blob with stat metadata."
  [blob]
  (assoc blob
    :stat/size (count (:content blob))
    :stat/stored-at (or (:stat/stored-at blob)
                        (java.util.Date.))))


;; Blob records in a memory store are held in a map in an atom.
(defrecord MemoryBlobStore
  [memory]

  blob/BlobStore

  (enumerate
    [this opts]
    (blob/select-ids opts (keys @memory)))


  (stat
    [this id]
    (when-let [blob (get @memory id)]
      (dissoc blob :content)))


  (get*
    [this id]
    (get @memory id))


  (put!
    [this blob]
    (if-let [id (:id blob)]
      (or (get @memory id)
          (let [blob (blob-stats blob)]
            (swap! memory assoc id blob)
            blob))))


  (delete!
    [this id]
    (swap! memory dissoc id))


  (erase!!
    [this]
    (swap! memory empty)))


(defn memory-store
  "Creates a new in-memory blob store."
  []
  (MemoryBlobStore. (atom (sorted-map) :validator map?)))
