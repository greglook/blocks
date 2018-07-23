(ns ^:no-doc blocks.store
  "Block storage protocols. Typically, clients of the library should use the
  API wrapper functions in `blocks.core` instead of using these methods
  directly."
  (:require
    [blocks.data :as data]
    [clojure.string :as str]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block))


;; ## Storage Protocols

(defprotocol BlockStore
  "Protocol for content-addressable storage keyed by multihash identifiers."

  (-stat
    [store id]
    "Returns a map with an `:id` and `:size` but no content. The returned map
    may contain additional data like the date stored. Returns nil if the store
    does not contain the identified block.")

  (-list
    [store opts]
    "Lists the blocks contained in the store. Returns a lazy sequence of stat
    metadata about each block. The stats should be returned in order sorted by
    multihash id. See `list` for the supported options.")

  (-get
    [store id]
    "Returns the identified block if it is stored, otherwise nil. The block
    should include stat metadata. Typically clients should use `get` instead,
    which validates arguments and the returned block record.")

  (-put!
    [store block]
    "Saves a block into the store. Returns the block record, updated with stat
    metadata.")

  (-delete!
    [store id]
    "Removes a block from the store. Returns true if the block was stored."))


(defprotocol BatchingStore
  "Protocol for stores which can perform optimized batch operations on blocks.
  Note that none of the methods in this protocol guarantee an ordering on the
  returned collections."

  (-get-batch
    [store ids]
    "Retrieves a batch of blocks identified by a collection of multihashes.
    Returns a sequence of the requested blocks which are found in the store.")

  (-put-batch!
    [store blocks]
    "Saves a collection of blocks to the store. Returns a collection of the
    stored blocks.")

  (-delete-batch!
    [store ids]
    "Removes multiple blocks from the store, identified by a collection of
    multihashes. Returns a collection of multihashes for the deleted blocks."))


(defprotocol ErasableStore
  "An erasable store has some notion of being removed in its entirety, usually
  also atomically. One example would be a file system unlinking the root
  directory rather than deleting each individual file."

  (-erase!
    [store]
    "Completely removes any data associated with the store."))



;; ## Store Construction

(defn parse-uri
  "Parse a URI string into a map of keywords to URI parts."
  [location]
  (let [uri (java.net.URI. location)]
    (->>
      {:scheme (.getScheme uri)
       :name (and (nil? (.getAuthority uri))
                  (nil? (.getPath uri))
                  (.getSchemeSpecificPart uri))
       :user-info (when-let [info (.getUserInfo uri)]
                    (zipmap [:id :secret] (str/split info #":" 2)))
       :host (.getHost uri)
       :port (when (not= (.getPort uri) -1)
               (.getPort uri))
       :path (.getPath uri)
       :query (when-let [query (.getQuery uri)]
                (->> (str/split query #"&")
                     (map #(let [[k v] (str/split % #"=")]
                             [(keyword k) v]))
                     (into {})))
       :fragment (.getFragment uri)}
      (filter val)
      (into {}))))


(defmulti initialize
  "Constructs a new block store from a URI by dispatching on the scheme. The
  store will be returned in an initialized but not started state."
  (comp :scheme parse-uri))


(defmethod initialize :default
  [uri]
  (throw (IllegalArgumentException.
           (str "Unsupported block-store URI scheme: " (pr-str uri)))))


(defmacro privatize!
  "Alters the metadatata on the given var symbol to change the visibility to
  private."
  [var-sym]
  `(alter-meta! #'~var-sym assoc :private true))


(defmacro privatize-constructors!
  "Alters the metadata on the automatic record constructor functions to set
  their visibility to private."
  [record-name]
  `(do (privatize! ~(symbol (str "->" record-name)))
       (privatize! ~(symbol (str "map->" record-name)))))



;; ## Utilities

(defmacro check
  "Utility macro for validating values in a threading fashion. The predicate
  `pred?` will be called with the current value; if the result is truthy, the
  value is returned. Otherwise, any forms passed in the `on-err` list are
  executed with the symbol `value` bound to the value, and the function returns
  nil."
  [value pred? & on-err]
  `(let [value# ~value]
     (if (~pred? value#)
       value#
       (let [~'value value#]
         ~@on-err
         nil))))


(defn preferred-copy
  "Chooses among multiple blocks to determine the optimal one to use for
  copying into a new store. Returns the first loaded block, if any are
  keeping in-memory content. If none are, returns the first block."
  [& blocks]
  (when-let [blocks (seq (remove nil? blocks))]
    (or (first (filter data/loaded? blocks))
        (first blocks))))


(defn select-stats
  "Selects block stats from a sequence based on the criteria spported in
  `blocks.core/list`. Helper for block store implementers."
  [opts stats]
  (let [{:keys [algorithm after limit]} opts]
    (cond->> stats
      algorithm
        (filter (comp #{algorithm} :algorithm :id))
      after
        (drop-while #(pos? (compare after (multihash/hex (:id %)))))
      limit
        (take limit))))


(defn merge-block-lists
  "Merges multiple lists of block stats (as from `block/list`) and returns a
  lazy sequence with one entry per unique id, in sorted order. The input
  sequences are consumed lazily and must already be sorted."
  [& lists]
  (lazy-seq
    (let [lists (remove empty? lists)
          earliest (first (sort-by :id (map first lists)))]
      (when earliest
        (cons earliest
              (apply
                merge-block-lists
                (map #(if (= (:id earliest) (:id (first %)))
                        (rest %)
                        %)
                     lists)))))))


(defn missing-blocks
  "Returns a lazy sequence of stats for the blocks in the list of stats from
  `source` which are not in `dest` list."
  [source-blocks dest-blocks]
  (let [s (first source-blocks)
        d (first dest-blocks)]
    (cond
      ; Source store exhausted; terminate sequence.
      (empty? source-blocks)
        nil

      ; Destination store exhausted; return remaining blocks in source.
      (empty? dest-blocks)
        source-blocks

      ; Block is already in both source and dest.
      (= (:id s) (:id d))
        (recur (next source-blocks)
               (next dest-blocks))

      :else
        (if (neg? (compare (:id s) (:id d)))
          ; Source has a block not in dest.
          (cons s (lazy-seq (missing-blocks (next source-blocks) dest-blocks)))
          ; Next source block comes after some dest blocks; skip forward.
          (recur source-blocks (next dest-blocks))))))
