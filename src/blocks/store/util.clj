(ns ^:no-doc blocks.store.util
  "Various utility functions for handling and testing blocks."
  (:require
    [clojure.string :as str]
    [multihash.core :as multihash]))


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
  copying into a new store. Returns the first literal block, if any are
  keeping in-memory content. If none are, returns the first block."
  [& blocks]
  (when-let [blocks (seq (remove nil? blocks))]
    (or (first (filter deref blocks))
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
