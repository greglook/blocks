(ns ^:no-doc blocks.store
  "Block storage protocols. Typically, clients of the library should use the
  API wrapper functions in `blocks.core` instead of using these methods
  directly."
  (:require
    [blocks.data :as data]
    [clojure.string :as str]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.hash :as multihash]))


;; ## Storage Protocols

(defprotocol BlockStore
  "Protocol for content-addressable storage keyed by multihash identifiers."

  (-list
    [store opts]
    "List the blocks contained in the store. This method should return a stream
    of blocks ordered by multihash id. See `blocks.core/list` for the supported
    options.

    The method must return _at least_ the blocks which match the query options,
    and _should_ optimize the results by omitting unmatched blocks. The
    returned stream may be closed preemptively if the consumer is done, which
    should terminate the list thread.

    If the listing thread encounters an exception, the error should be placed
    on the stream and the stream should be closed to indicate no further blocks
    will be coming. Consumers must handle exceptions propagated on the stream.")

  (-stat
    [store id]
    "Load a block's metadata if the store contains it. Returns a deferred which
    yields a map with block information but no content, or nil if the store
    does not contain the identified block.")

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



;; ## Implementation Utilities

(defn preferred-block
  "Choose among multiple blocks to determine the optimal one to use for
  copying into a new store. Returns the first loaded block, if any are
  keeping in-memory content. If none are, returns the first block."
  [& blocks]
  (when-let [blocks (seq (remove nil? blocks))]
    (or (first (filter data/byte-content? blocks))
        (first blocks))))


(defn select-blocks
  "Select blocks from a stream based on the criteria spported in `-list`.
  Returns a filtered view of the block streams that will close the source once
  the relevant blocks have been read."
  [opts blocks]
  (let [{:keys [algorithm after before limit]} opts
        counter (atom 0)
        out (s/stream)]
    (s/connect-via
      blocks
      (fn test-block
        [block]
        (if (instance? Throwable block)
          ; Propagate error on the stream.
          (do (s/put! out block)
              (s/close! out)
              (d/success-deferred false))
          ; Determine if block matches query criteria.
          (let [id (:id block)
                hex (multihash/hex id)]
            (cond
              ; Ignore any blocks which don't match the algorithm.
              (and algorithm (not= algorithm (:algorithm id)))
              (d/success-deferred true)

              ; Drop blocks until an id later than `after`.
              (and after (pos? (compare after hex)))
              (d/success-deferred true)

              ; Terminate the stream if block is later than `before` or `limit`
              ; blocks have already been returned.
              (or (and before (neg? (compare before hex)))
                  (and (pos-int? limit) (< limit (swap! counter inc))))
              (do (s/close! out)
                  (d/success-deferred false))

              ; Otherwise, pass the block along.
              :else
              (s/put! out block)))))
        out
        {:description {:op "select-blocks"}})
    (s/source-only out)))


(defn merge-blocks
  "Merge multiple streams of blocks and return a stream with one block per
  unique id, maintaining sorted order. The input streams are consumed
  incrementally and must already be sorted."
  [& streams]
  (if (= 1 (count streams))
    (first streams)
    (let [intermediates (mapv
                          (fn hook-up
                            [a]
                            (let [b (s/stream)]
                              (s/connect-via
                                a #(s/put! b %) b
                                {:description {:op "merge-blocks"}})
                              b))
                          streams)
          out (s/stream)]
      (d/loop [inputs (map vector intermediates (repeat nil))]
        (d/chain
          ; Take the head value from each stream we don't already have.
          (->>
            inputs
            (map (fn take-next
                   [[input head :as pair]]
                   (if (nil? head)
                     (d/chain
                       (s/take! input ::drained)
                       (partial vector input))
                     pair)))
            (apply d/zip))
          ; Remove drained streams from consideration.
          (fn remove-drained
            [inputs]
            (remove #(identical? ::drained (second %)) inputs))
          ; Find the next earliest block to return.
          (fn find-next
            [inputs]
            (if (empty? inputs)
              ; Every input is drained.
              (s/close! out)
              ; Check inputs for errors.
              (if-let [error (->> (map second inputs)
                                  (filter #(instance? Throwable %))
                                  (first))]
                ; Propagate error.
                (d/finally
                  (s/put! out error)
                  #(s/close! out))
                ; Determine the next block to output.
                (let [earliest (first (sort-by :id (map second inputs)))]
                  (d/chain
                    (s/put! out earliest)
                    (fn check-put
                      [result]
                      (if result
                        ; Remove any blocks matching the one emitted.
                        (d/recur (mapv (fn remove-earliest
                                         [[input head :as pair]]
                                         (if (= (:id earliest) (:id head))
                                           [input nil]
                                           pair))
                                       inputs))
                        ; Out was closed on us.
                        false)))))))))
      (s/source-only out))))


(defn missing-blocks
  "Compare two block streams and generate a derived stream of the blocks in
  `source` which are not present in `dest`."
  [source dest]
  (let [src (s/stream)
        dst (s/stream)
        out (s/stream)
        close-all! (fn close-all!
                     []
                     (s/close! src)
                     (s/close! dst)
                     (s/close! out))]
    (s/connect-via
      source #(s/put! src %) src
      {:description {:op "missing-blocks"}})
    (s/connect-via
      dest #(s/put! dst %) dst
      {:description {:op "missing-blocks"}})
    (d/loop [s nil
             d nil]
      (d/chain
        (d/zip
          (if (nil? s)
            (s/take! src ::drained)
            s)
          (if (nil? d)
            (s/take! dst ::drained)
            d))
        (fn compare-next
          [[s d]]
          (cond
            ; Source stream exhausted; terminate sequence.
            (identical? ::drained s)
            (close-all!)

            ; Destination stream exhausted; return remaining blocks in source.
            (identical? ::drained d)
            (-> (s/put! out s)
                (d/chain
                  (fn [_] (s/drain-into src out)))
                (d/finally close-all!))

            ; Source threw an error; propagate it.
            (instance? Throwable s)
            (d/finally
              (s/put! out s)
              close-all!)

            ; Dest threw an error; propagate it.
            (instance? Throwable d)
            (d/finally
              (s/put! out d)
              close-all!)

            ; Block is present in both streams; drop and continue.
            (= (:id s) (:id d))
            (d/recur nil nil)

            ; Source has a block not in dest.
            (neg? (compare (:id s) (:id d)))
            (d/chain
              (s/put! out s)
              (fn onwards
                [result]
                (when result
                  (d/recur nil d))))

            ; Next source block comes after some dest blocks; skip forward.
            :else
            (d/recur s nil)))))
    (s/source-only out)))


(defn zip-stores
  "Apply a function to each of the given block stores in parallel. Returns a
  deferred which yields the vector of results."
  [stores f & args]
  (apply d/zip (map #(apply f % args) stores)))


(defn some-store
  "Apply a function to each of the given block stores in order until one
  returns a non-nil result. Returns a deferred which yields the result, or nil
  if all stores returned nil."
  [stores f & args]
  (d/loop [stores stores]
    (when-let [store (first stores)]
      (d/chain
        (apply f store args)
        (fn check-result
          [result]
          (if (nil? result)
            (d/recur (rest stores))
            result))))))
