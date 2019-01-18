(ns ^:no-doc blocks.store
  "Block storage protocols. Typically, clients of the library should use the
  API wrapper functions in `blocks.core` instead of using these methods
  directly."
  (:require
    [blocks.data :as data]
    [clojure.string :as str]
    [multiformats.hash :as multihash]))


;; ## Storage Protocols

(defprotocol BlockStore
  "Protocol for content-addressable storage keyed by multihash identifiers."

  (-stat
    [store id]
    "Load a block's metadata if the store contains it. Returns a deferred which
    yields a map with block information but no content, or nil if the store
    does not contain the identified block.")

  (-list
    [store opts]
    "List the blocks contained in the store. Returns a stream of blocks. The
    stats should be returned in order sorted by multihash id. See
    `blocks.core/list` for the supported options.")

  (-get
    [store id]
    "Fetch a block from the store. Returns a deferred which yields the block,
    or nil if not present.")

  (-put!
    [store block]
    "Persist a block into the store. Returns a deferred which yields the
    stored block, which may have already been present in the store.")

  (-delete!
    [store id]
    "Remove a block from the store. Returns a deferred which yields true if the
    block was stored, false if it was not."))


(defprotocol ErasableStore
  "An erasable store has some notion of being removed in its entirety, often
  atomically. For example, a file system might unlink the root directory rather
  than deleting each individual file."

  (-erase!
    [store]
    "Completely removes any data associated with the store. Returns a deferred
    value which yields when the store is erased."))



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

(defn preferred-block
  "Choose among multiple blocks to determine the optimal one to use for
  copying into a new store. Returns the first loaded block, if any are
  keeping in-memory content. If none are, returns the first block."
  [& blocks]
  (when-let [blocks (seq (remove nil? blocks))]
    (or (first (filter data/byte-content? blocks))
        (first blocks))))


; FIXME: These don't work on streams
(defn select
  "Selects blocks from a sequence based on the criteria spported in `-list`.
  Helper for block store implementers."
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
